// Integration tests for the pause/resume primitive (issue #383).
//
// All tests use testcontainers so the `db` feature is required.
#![cfg(feature = "db")]
#![allow(clippy::items_after_statements)]

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::store;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, Priority, ShardId, StartWorkflowParams, WorkflowContext,
    auto_resume_expired_pauses, cancel_workflow_execution, pause_workflow_execution, queue,
    resume_workflow_execution, start_or_load_workflow_execution,
};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    "\n",
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    // issue #523: workflow-level retry policy columns.
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
);

async fn setup() -> (String, ContainerAsync<Postgres>) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect failed")
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "pause_tests",
        handler,
        execution_timeout: None,
        sla: None,
        concurrency: None,

        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

fn make_worker(registry: Arc<HandlerRegistry>) -> Worker {
    Worker::new(
        WorkerRuntimeConfig {
            worker_id: uuid::Uuid::new_v4().to_string(),
            queues: vec!["default".to_string()],
            notification_database_url: None,
            max_concurrent_workflows: 10,
            max_concurrent_activities: 20,
            poll_interval: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(2),
            cancellation_grace_period: Duration::from_secs(2),
            sticky_timeout: Duration::ZERO,
            max_local_activity_start_to_close: Duration::from_secs(60),
            shard_assignments: vec![ShardId::new(0)],
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: String::new(),
            deployment_name: None,
            workflow_cache_size: 100,
            priority_aging_secs: None,
            unknown_target_grace_window: Duration::from_secs(5),
            poison_pill_threshold: 3,

            workflow_task_timeout: std::time::Duration::from_secs(10),
            max_workflow_pause_duration: Duration::from_secs(24 * 3600),
            labels: std::collections::HashMap::new(),
            queue_weights: std::collections::HashMap::new(),
            max_workflow_history_events: None,
            shard_notification_database_urls: Vec::new(),
            sharded_pool: None,
            slot_tuner: None,
        },
        registry,
    )
    .expect("worker should build")
}

async fn start(conn: &mut AsyncPgConnection, name: &str, id: &str) -> ExecutionId {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: name,
            workflow_id: id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input: Value::Null,
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
        },
    )
    .await
    .expect("workflow start should succeed")
    .exec_id
}

async fn get_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    use autumn_harvest::schema::harvest_workflow_executions;
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::state)
        .first::<String>(conn)
        .await
        .expect("execution must exist")
}

async fn pause_columns(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> (
    Option<chrono::DateTime<chrono::Utc>>,
    Option<String>,
    Option<String>,
) {
    use autumn_harvest::schema::harvest_workflow_executions as e;
    e::table
        .filter(e::id.eq(exec_id.as_uuid()))
        .select((e::paused_at, e::pause_reason, e::pause_actor))
        .first(conn)
        .await
        .expect("execution must exist")
}

async fn history(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    store::load_history(conn, exec_id)
        .await
        .expect("load history")
        .events
}

async fn wait_for_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId, states: &[&str]) {
    for _ in 0..300 {
        let state = get_state(conn, exec_id).await;
        if states.contains(&state.as_str()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let state = get_state(conn, exec_id).await;
    panic!("execution {exec_id} never reached {states:?}; current state: {state}");
}

async fn wait_for_event<F: Fn(&WorkflowEvent) -> bool>(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    pred: F,
) {
    for _ in 0..300 {
        if history(conn, exec_id).await.iter().any(&pred) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("expected event never appeared in history for {exec_id}");
}

/// Polls until a worker has claimed the execution's workflow task (its
/// `worker_id` is set). Lets a test land a pause precisely while a decision
/// task is in-flight.
async fn wait_for_task_claimed(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    use autumn_harvest::schema::harvest_task_queue as t;
    for _ in 0..300 {
        let claimed: i64 = t::table
            .filter(t::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .filter(t::task_type.eq("workflow"))
            .filter(t::worker_id.is_not_null())
            .count()
            .get_result(conn)
            .await
            .expect("count query should succeed");
        if claimed > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("workflow task for {exec_id} was never claimed");
}

// ── Workflow handlers ──────────────────────────────────────────────────────

/// Waits on a 1-second durable timer, then completes. The sub-second window in
/// the AC is expressed here with the smallest timer the API supports; the
/// deferral mechanism is identical.
fn timer_wf<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("wait", 1).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!("done"))
    })
}

/// Sleeps before producing its `StartTimer` command so a test can land a pause
/// while this workflow decision task is still mid-flight (already claimed by a
/// worker but not yet at its suspension point). Used to exercise the
/// claimed-then-paused race (issue #383): the worker must discard the pending
/// decision and re-park rather than persist the timer.
fn slow_timer_wf<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if ctx.history_event_count() == 1 {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        ctx.timer("wait", 1).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!("done"))
    })
}

// ── Pure DB transition tests (no worker required) ──────────────────────────

#[tokio::test]
async fn pause_then_resume_transitions_and_records_events() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "p-1").await;

    let paused = pause_workflow_execution(
        &mut conn,
        exec_id,
        Some("investigating"),
        "oncall@example.com",
        &NoOpMetrics,
    )
    .await
    .expect("pause should succeed");
    assert!(paused.newly_paused);
    assert_eq!(paused.state, "PAUSED");
    assert_eq!(get_state(&mut conn, exec_id).await, "PAUSED");

    let (paused_at, reason, actor) = pause_columns(&mut conn, exec_id).await;
    assert!(paused_at.is_some(), "paused_at must be set");
    assert_eq!(reason.as_deref(), Some("investigating"));
    assert_eq!(actor.as_deref(), Some("oncall@example.com"));

    let events = history(&mut conn, exec_id).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowExecutionPaused { .. })),
        "WorkflowExecutionPaused must be appended"
    );

    let resumed = resume_workflow_execution(&mut conn, exec_id, "oncall@example.com", &NoOpMetrics)
        .await
        .expect("resume should succeed");
    assert_eq!(resumed.state, "RUNNING");
    assert_eq!(get_state(&mut conn, exec_id).await, "RUNNING");

    let (paused_at, reason, actor) = pause_columns(&mut conn, exec_id).await;
    assert!(paused_at.is_none(), "paused_at must be cleared on resume");
    assert!(reason.is_none());
    assert!(actor.is_none());

    let events = history(&mut conn, exec_id).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowExecutionResumed { .. })),
        "WorkflowExecutionResumed must be appended"
    );
}

#[tokio::test]
async fn resume_extends_deadline_by_pause_duration() {
    // Pause suspends the SLA clock (issue #383 × #243): on resume the absolute
    // `deadline_at` is pushed forward by the time spent paused so paused
    // wall-clock is not charged against the workflow's execution_timeout.
    use autumn_harvest::schema::harvest_workflow_executions as e;

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "deadline-1").await;

    pause_workflow_execution(&mut conn, exec_id, Some("hold"), "oncall", &NoOpMetrics)
        .await
        .expect("pause should succeed");

    // Establish a known pre-resume deadline and a 30-minute backdated pause so
    // the resume computes a deterministic, non-zero span.
    let now = chrono::Utc::now();
    let deadline_before = now + chrono::Duration::minutes(30);
    let paused_at = now - chrono::Duration::minutes(30);
    diesel::update(e::table.filter(e::id.eq(exec_id.as_uuid())))
        .set((
            e::deadline_at.eq(Some(deadline_before)),
            e::paused_at.eq(Some(paused_at)),
        ))
        .execute(&mut conn)
        .await
        .expect("set deadline/paused_at for test");

    resume_workflow_execution(&mut conn, exec_id, "oncall", &NoOpMetrics)
        .await
        .expect("resume should succeed");

    let deadline_after: Option<chrono::DateTime<chrono::Utc>> = e::table
        .filter(e::id.eq(exec_id.as_uuid()))
        .select(e::deadline_at)
        .first(&mut conn)
        .await
        .expect("execution must exist");

    let deadline_after = deadline_after.expect("deadline_at must remain set after resume");
    // Expected ≈ deadline_before + 30min pause span. Allow a few seconds of
    // slack for the wall-clock read inside resume.
    let expected = deadline_before + chrono::Duration::minutes(30);
    let drift = (deadline_after - expected).num_seconds().abs();
    assert!(
        drift <= 5,
        "deadline should advance by the pause span: after={deadline_after}, expected≈{expected} (drift {drift}s)"
    );
}

#[tokio::test]
async fn resume_without_deadline_leaves_it_null() {
    // A workflow with no execution_timeout (deadline_at NULL) must stay NULL
    // across a pause/resume cycle — the SLA-clock extension is a no-op.
    use autumn_harvest::schema::harvest_workflow_executions as e;

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "deadline-null-1").await;

    pause_workflow_execution(&mut conn, exec_id, None, "oncall", &NoOpMetrics)
        .await
        .expect("pause should succeed");
    resume_workflow_execution(&mut conn, exec_id, "oncall", &NoOpMetrics)
        .await
        .expect("resume should succeed");

    let deadline_after: Option<chrono::DateTime<chrono::Utc>> = e::table
        .filter(e::id.eq(exec_id.as_uuid()))
        .select(e::deadline_at)
        .first(&mut conn)
        .await
        .expect("execution must exist");
    assert!(
        deadline_after.is_none(),
        "deadline_at must stay NULL when no execution_timeout was set"
    );
}

#[tokio::test]
async fn pause_is_idempotent() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "p-idem").await;

    let first = pause_workflow_execution(&mut conn, exec_id, None, "a", &NoOpMetrics)
        .await
        .unwrap();
    assert!(first.newly_paused);
    let second = pause_workflow_execution(&mut conn, exec_id, None, "b", &NoOpMetrics)
        .await
        .unwrap();
    assert!(!second.newly_paused, "second pause must be idempotent");

    let pause_events = history(&mut conn, exec_id)
        .await
        .into_iter()
        .filter(|e| matches!(e, WorkflowEvent::WorkflowExecutionPaused { .. }))
        .count();
    assert_eq!(pause_events, 1, "only one pause event must be recorded");
}

#[tokio::test]
async fn pause_rejects_terminal_execution() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "p-term").await;
    cancel_workflow_execution(&mut conn, exec_id, "done", &NoOpMetrics)
        .await
        .unwrap();

    let err = pause_workflow_execution(&mut conn, exec_id, None, "a", &NoOpMetrics)
        .await
        .expect_err("pausing a terminal execution must fail");
    assert!(matches!(err, HarvestError::Config(_)), "got {err:?}");
}

#[tokio::test]
async fn pause_unknown_execution_is_not_found() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let missing = ExecutionId::new_for_shard(ShardId::new(0));
    let err = pause_workflow_execution(&mut conn, missing, None, "a", &NoOpMetrics)
        .await
        .expect_err("pausing a missing execution must 404");
    assert!(matches!(err, HarvestError::NotFound(_)), "got {err:?}");
}

#[tokio::test]
async fn resume_requires_paused_state() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "r-running").await;
    // RUNNING (not paused) → conflict.
    let err = resume_workflow_execution(&mut conn, exec_id, "a", &NoOpMetrics)
        .await
        .expect_err("resuming a non-paused execution must conflict");
    assert!(matches!(err, HarvestError::Config(_)), "got {err:?}");
}

#[tokio::test]
async fn cancel_beats_pause() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "cancel-beats").await;

    pause_workflow_execution(&mut conn, exec_id, Some("hold"), "a", &NoOpMetrics)
        .await
        .unwrap();
    assert_eq!(get_state(&mut conn, exec_id).await, "PAUSED");

    let cancelled = cancel_workflow_execution(&mut conn, exec_id, "kill it", &NoOpMetrics)
        .await
        .expect("cancel must beat pause");
    assert!(cancelled.newly_cancelled);
    assert_eq!(cancelled.prior_state, "PAUSED");
    assert_eq!(get_state(&mut conn, exec_id).await, "CANCELLED");

    let (paused_at, reason, actor) = pause_columns(&mut conn, exec_id).await;
    assert!(
        paused_at.is_none() && reason.is_none() && actor.is_none(),
        "cancellation must clear the pending pause record"
    );
}

#[tokio::test]
async fn update_during_pause_is_rejected() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "update-paused").await;
    pause_workflow_execution(&mut conn, exec_id, None, "a", &NoOpMetrics)
        .await
        .unwrap();

    let err = store::admit_update_event(
        &mut conn,
        exec_id,
        autumn_harvest::types::UpdateId::new(),
        "set_priority".to_string(),
        serde_json::json!({"p": 1}),
    )
    .await
    .expect_err("updates against a paused workflow must be rejected");
    assert!(
        matches!(err, HarvestError::WorkflowPaused(id) if id == exec_id),
        "got {err:?}"
    );
}

#[tokio::test]
async fn paused_workflow_task_is_not_claimed() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "claim-gate").await;

    // A fresh start enqueues a PENDING workflow task. Pause the execution and
    // assert the task is not claimable while paused.
    pause_workflow_execution(&mut conn, exec_id, None, "a", &NoOpMetrics)
        .await
        .unwrap();
    let no_breakers: &[String] = &[];
    let claimed = queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "w1",
        "",
        None,
        no_breakers,
        no_breakers,
    )
    .await
    .expect("claim query should succeed");
    assert!(
        claimed.is_none(),
        "workflow task for a paused execution must not be claimed"
    );

    // After resume the task becomes claimable again.
    resume_workflow_execution(&mut conn, exec_id, "a", &NoOpMetrics)
        .await
        .unwrap();
    let no_breakers: &[String] = &[];
    let claimed = queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "w1",
        "",
        None,
        no_breakers,
        no_breakers,
    )
    .await
    .expect("claim query should succeed");
    assert!(
        claimed.is_some(),
        "resumed execution's workflow task must be claimable"
    );
}

#[tokio::test]
async fn auto_resume_expired_pauses_force_resumes() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "auto-resume").await;
    pause_workflow_execution(&mut conn, exec_id, Some("forgot"), "a", &NoOpMetrics)
        .await
        .unwrap();

    // Backdate paused_at so the pause is well past the ceiling.
    use autumn_harvest::schema::harvest_workflow_executions as e;
    diesel::update(e::table.filter(e::id.eq(exec_id.as_uuid())))
        .set(e::paused_at.eq(Some(chrono::Utc::now() - chrono::Duration::hours(48))))
        .execute(&mut conn)
        .await
        .unwrap();

    let resumed =
        auto_resume_expired_pauses(&mut conn, Duration::from_secs(24 * 3600), &NoOpMetrics)
            .await
            .expect("auto-resume scan should succeed");
    assert_eq!(resumed, 1, "the over-long pause must be auto-resumed");
    assert_eq!(get_state(&mut conn, exec_id).await, "RUNNING");

    // The resume must be attributed to the auto-resume actor.
    let resumed_actor = history(&mut conn, exec_id)
        .await
        .into_iter()
        .find_map(|ev| match ev {
            WorkflowEvent::WorkflowExecutionResumed { actor, .. } => Some(actor),
            _ => None,
        });
    assert_eq!(resumed_actor.as_deref(), Some("auto-resume(timeout)"));
}

// ── Headline worker-driven test: pause → timer → resume → fire → replay ─────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_defers_timer_then_resume_fires_and_replays_deterministically() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![wf_info("timer_wf", timer_wf)],
        vec![],
    ));
    let exec_id = start(&mut conn, "timer_wf", "timer-pause-001").await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(20), worker_ref.run(&worker_pool)).await;
    });

    // Wait until the workflow has scheduled its timer (it is now suspended).
    wait_for_event(&mut conn, exec_id, |e| {
        matches!(e, WorkflowEvent::TimerStarted { .. })
    })
    .await;

    // Pause while the timer is pending.
    pause_workflow_execution(&mut conn, exec_id, Some("freeze"), "oncall", &NoOpMetrics)
        .await
        .expect("pause should succeed");
    assert_eq!(get_state(&mut conn, exec_id).await, "PAUSED");

    // Let the timer's fire time elapse. Despite being due, it must NOT fire
    // while paused: the workflow must still be PAUSED and not completed.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        get_state(&mut conn, exec_id).await,
        "PAUSED",
        "an elapsed timer must not fire while the workflow is paused"
    );
    assert!(
        !history(&mut conn, exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "TimerFired must be deferred until resume"
    );

    // Resume: the expired timer fires immediately and the workflow completes.
    resume_workflow_execution(&mut conn, exec_id, "oncall", &NoOpMetrics)
        .await
        .expect("resume should succeed");
    wait_for_state(&mut conn, exec_id, &["COMPLETED"]).await;

    let final_history = history(&mut conn, exec_id).await;
    assert!(
        final_history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "timer must fire after resume"
    );

    worker.shutdown();
    let _ = worker_handle.await;

    // Replay determinism: re-run the recorded history on a cold executor (no
    // cache) and confirm the pause/resume events are transparent — the workflow
    // reproduces the same terminal outcome.
    let replay_history: Vec<WorkflowEvent> = final_history
        .iter()
        .filter(|e| !matches!(e, WorkflowEvent::WorkflowCompleted { .. }))
        .cloned()
        .collect();
    let outcome = run_workflow(exec_id, replay_history, timer_wf, Value::Null).await;
    match outcome {
        WorkflowOutcome::Completed { output } => {
            assert_eq!(
                output,
                serde_json::json!("done"),
                "cold-cache replay must reproduce the same output"
            );
        }
        other => panic!("expected Completed on replay, got {other:?}"),
    }
}

// ── In-flight decision-task race: pause after claim must discard commands ────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_during_inflight_decision_task_discards_pending_commands() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![wf_info("slow_timer_wf", slow_timer_wf)],
        vec![],
    ));
    let exec_id = start(&mut conn, "slow_timer_wf", "inflight-pause-001").await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(25), worker_ref.run(&worker_pool)).await;
    });

    // Land the pause while the decision task is mid-flight: the worker has
    // claimed it (worker_id set) but the handler is still in its 800ms sleep,
    // so the StartTimer command has not yet been produced or persisted.
    wait_for_task_claimed(&mut conn, exec_id).await;
    pause_workflow_execution(
        &mut conn,
        exec_id,
        Some("mid-flight"),
        "oncall",
        &NoOpMetrics,
    )
    .await
    .expect("pause should succeed on a running execution");
    assert_eq!(get_state(&mut conn, exec_id).await, "PAUSED");

    // Give the in-flight handler ample time to finish its sleep, suspend, and
    // reach the worker's pause guard. The guard must discard the decision: no
    // TimerStarted may be appended while paused.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        get_state(&mut conn, exec_id).await,
        "PAUSED",
        "execution must remain paused while the discarded task is re-parked"
    );
    assert!(
        !history(&mut conn, exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        "an in-flight decision task must not persist new commands after pause"
    );

    // Resume: the re-parked task is re-claimed, the deterministic handler
    // re-derives the same StartTimer command, and the workflow completes.
    resume_workflow_execution(&mut conn, exec_id, "oncall", &NoOpMetrics)
        .await
        .expect("resume should succeed");
    wait_for_event(&mut conn, exec_id, |e| {
        matches!(e, WorkflowEvent::TimerStarted { .. })
    })
    .await;
    wait_for_state(&mut conn, exec_id, &["COMPLETED"]).await;

    worker.shutdown();
    let _ = worker_handle.await;
}
