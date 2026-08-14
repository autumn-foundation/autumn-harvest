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
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

async fn setup() -> (String, ContainerAsync<Postgres>) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let container = Postgres::default()
        .with_init_sql(init_sql())
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
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "pause_tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
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
            capability_miss_max_redeliveries: 5,

            workflow_task_timeout: std::time::Duration::from_secs(10),
            workflow_panic_max_attempts: 3,
            max_workflow_pause_duration: Duration::from_secs(24 * 3600),
            labels: std::collections::HashMap::new(),
            queue_weights: std::collections::HashMap::new(),
            max_workflow_history_events: None,
            shard_notification_database_urls: Vec::new(),
            sharded_pool: None,
            slot_tuner: None,
            max_concurrent_sessions: 0,
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
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
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
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
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
    assert!(
        paused.paused_at.is_some(),
        "pause result must report when the pause took effect (issue #609)"
    );
    assert_eq!(get_state(&mut conn, exec_id).await, "PAUSED");

    let (paused_at, reason, actor) = pause_columns(&mut conn, exec_id).await;
    let column_at = paused_at.expect("paused_at must be set");
    // Tolerant comparison: the column round-trips through Postgres at
    // microsecond precision while the result carries the in-memory instant.
    let reported_at = paused.paused_at.expect("checked Some above");
    assert!(
        (reported_at - column_at).abs() < chrono::Duration::milliseconds(1),
        "reported paused_at must match the persisted column"
    );
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
    assert!(
        resumed.newly_resumed,
        "a genuine PAUSED → RUNNING transition must report newly_resumed"
    );
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

// ── schedule_to_close × pause (issue #609, AC5) ─────────────────────────────

/// Inserts a PENDING activity task for `exec_id` carrying the given
/// cross-retry deadline (`schedule_to_close_at`, issue #378).
async fn insert_pending_activity_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    schedule_to_close_at: chrono::DateTime<chrono::Utc>,
) -> uuid::Uuid {
    let task_id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, state, \
          attempt, max_attempts, schedule_to_close_at) \
         VALUES ($1, 'default', 'activity', $2, 'deadline_activity', $3, '{}'::jsonb, 'PENDING', \
                 0, 10, $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(uuid::Uuid::new_v4())
    .bind::<diesel::sql_types::Timestamptz, _>(schedule_to_close_at)
    .execute(conn)
    .await
    .expect("insert pending activity task");
    task_id
}

#[tokio::test]
async fn schedule_to_close_scanner_skips_paused_executions() {
    // Pause suspends the cross-retry wall-clock deadline: an expired
    // schedule_to_close_at on a PAUSED execution's task must not be enforced
    // by the scanner, while a RUNNING execution's identical task must be.
    use autumn_harvest::timeout::{TimeoutReason, find_timed_out_tasks};

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "s2c-pause-1").await;
    let task_id = insert_pending_activity_task(
        &mut conn,
        exec_id,
        chrono::Utc::now() - chrono::Duration::seconds(1),
    )
    .await;

    pause_workflow_execution(&mut conn, exec_id, Some("contain"), "oncall", &NoOpMetrics)
        .await
        .expect("pause should succeed");

    let timed_out = find_timed_out_tasks(&mut conn)
        .await
        .expect("scan should succeed");
    assert!(
        !timed_out
            .iter()
            .any(|(t, r)| t.id == task_id && *r == TimeoutReason::ScheduleToClose),
        "the cross-retry deadline must be suspended while the owning execution is PAUSED"
    );

    // After resume the (shifted) deadline applies again. Force it back into
    // the past so the post-resume enforcement branch is observable without
    // waiting out a real pause span.
    resume_workflow_execution(&mut conn, exec_id, "oncall", &NoOpMetrics)
        .await
        .expect("resume should succeed");
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET schedule_to_close_at = NOW() - INTERVAL '1 second' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("backdate deadline");

    let timed_out = find_timed_out_tasks(&mut conn)
        .await
        .expect("scan should succeed");
    assert!(
        timed_out
            .iter()
            .any(|(t, r)| t.id == task_id && *r == TimeoutReason::ScheduleToClose),
        "a RUNNING execution's expired cross-retry deadline must still be enforced"
    );
}

#[tokio::test]
async fn resume_shifts_schedule_to_close_at_by_pause_span() {
    // AC5 (issue #609) × #378: resume pushes each open task's
    // schedule_to_close_at forward by the clamped pause span so paused
    // wall-clock is not charged against the activity's cross-retry budget.
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "s2c-shift-1").await;

    pause_workflow_execution(&mut conn, exec_id, Some("hold"), "oncall", &NoOpMetrics)
        .await
        .expect("pause should succeed");

    let now = chrono::Utc::now();
    let deadline_before = now + chrono::Duration::minutes(10);
    let task_id = insert_pending_activity_task(&mut conn, exec_id, deadline_before).await;

    // Backdate the pause 30 minutes so the resume computes a deterministic,
    // non-zero span (mirrors resume_extends_deadline_by_pause_duration).
    use autumn_harvest::schema::harvest_workflow_executions as e;
    diesel::update(e::table.filter(e::id.eq(exec_id.as_uuid())))
        .set(e::paused_at.eq(Some(now - chrono::Duration::minutes(30))))
        .execute(&mut conn)
        .await
        .expect("backdate paused_at for test");

    resume_workflow_execution(&mut conn, exec_id, "oncall", &NoOpMetrics)
        .await
        .expect("resume should succeed");

    use autumn_harvest::schema::harvest_task_queue as t;
    let deadline_after: Option<chrono::DateTime<chrono::Utc>> = t::table
        .filter(t::id.eq(task_id))
        .select(t::schedule_to_close_at)
        .first(&mut conn)
        .await
        .expect("task must exist");
    let deadline_after = deadline_after.expect("deadline must remain set");
    let shift = deadline_after - deadline_before;
    assert!(
        shift >= chrono::Duration::minutes(29) && shift <= chrono::Duration::minutes(31),
        "schedule_to_close_at must shift forward by the ~30-minute pause span, got {shift:?}"
    );
}

/// Inserts a PENDING activity task with explicit queue-timing columns for the
/// frozen-row tests (issue #609 post-review hardening, finding 3):
/// `scheduled_at`, a `schedule_to_start` window in seconds, and an optional
/// cross-retry deadline.
async fn insert_pending_activity_task_with_schedule(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    schedule_to_close_at: Option<chrono::DateTime<chrono::Utc>>,
    scheduled_at: chrono::DateTime<chrono::Utc>,
    schedule_to_start_secs: i64,
) -> uuid::Uuid {
    let task_id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, state, \
          attempt, max_attempts, schedule_to_close_at, scheduled_at, schedule_to_start) \
         VALUES ($1, 'default', 'activity', $2, 'deadline_activity', $3, '{}'::jsonb, 'PENDING', \
                 0, 10, $4, $5, $6::bigint * INTERVAL '1 second')",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(uuid::Uuid::new_v4())
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(schedule_to_close_at)
    .bind::<diesel::sql_types::Timestamptz, _>(scheduled_at)
    .bind::<diesel::sql_types::BigInt, _>(schedule_to_start_secs)
    .execute(conn)
    .await
    .expect("insert pending activity task with schedule");
    task_id
}

/// Inserts a PENDING external activity task (`harvest_external_tasks`) with
/// the given wall-clock deadline (issue #609 post-review hardening, finding 2).
async fn insert_pending_external_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    schedule_to_close_at: chrono::DateTime<chrono::Utc>,
) -> uuid::Uuid {
    let ext_id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_external_tasks \
         (id, token, workflow_exec_id, activity_id, name, queue, state, \
          schedule_to_close_at, schedule_to_close_secs) \
         VALUES ($1, $2, $3, $4, 'external_review', 'default', 'PENDING', $5, 600)",
    )
    .bind::<diesel::sql_types::Uuid, _>(ext_id)
    .bind::<diesel::sql_types::Uuid, _>(uuid::Uuid::new_v4())
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(uuid::Uuid::new_v4())
    .bind::<diesel::sql_types::Timestamptz, _>(schedule_to_close_at)
    .execute(conn)
    .await
    .expect("insert pending external task");
    ext_id
}

async fn task_timing_columns(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
) -> (
    String,
    chrono::DateTime<chrono::Utc>,
    Option<chrono::DateTime<chrono::Utc>>,
) {
    use autumn_harvest::schema::harvest_task_queue as t;
    t::table
        .filter(t::id.eq(task_id))
        .select((t::state, t::scheduled_at, t::schedule_to_close_at))
        .first(conn)
        .await
        .expect("task must exist")
}

#[tokio::test]
async fn external_task_timeout_scanner_skips_paused_executions() {
    // Finding 2 (issue #609 post-review hardening): the external-task
    // schedule_to_close scanner is pause-aware — an expired deadline on a
    // PAUSED execution's external task must not be enforced mid-pause; after
    // resume the (shifted) deadline applies again.
    use autumn_harvest::timeout::enforce_external_task_timeouts;

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "ext-pause-1").await;
    let ext_id = insert_pending_external_task(
        &mut conn,
        exec_id,
        chrono::Utc::now() - chrono::Duration::seconds(1),
    )
    .await;

    pause_workflow_execution(&mut conn, exec_id, Some("contain"), "oncall", &NoOpMetrics)
        .await
        .expect("pause should succeed");

    let timed_out = enforce_external_task_timeouts(&mut conn)
        .await
        .expect("scan should succeed");
    assert_eq!(
        timed_out, 0,
        "an external task of a PAUSED execution must not be timed out mid-pause"
    );

    use autumn_harvest::schema::harvest_external_tasks as ext;
    let state: String = ext::table
        .filter(ext::id.eq(ext_id))
        .select(ext::state)
        .first(&mut conn)
        .await
        .expect("external task must exist");
    assert_eq!(state, "PENDING", "the row must stay open while paused");
    assert!(
        !history(&mut conn, exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. })),
        "no ActivityTimedOut may be appended while the execution is paused"
    );

    // After resume the (shifted) deadline applies again. Force it back into
    // the past so the post-resume enforcement branch is observable without
    // waiting out a real pause span.
    resume_workflow_execution(&mut conn, exec_id, "oncall", &NoOpMetrics)
        .await
        .expect("resume should succeed");
    diesel::sql_query(
        "UPDATE harvest_external_tasks \
         SET schedule_to_close_at = NOW() - INTERVAL '1 second' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(ext_id)
    .execute(&mut conn)
    .await
    .expect("backdate deadline");

    let timed_out = enforce_external_task_timeouts(&mut conn)
        .await
        .expect("scan should succeed");
    assert_eq!(
        timed_out, 1,
        "a RUNNING execution's expired external deadline must still be enforced"
    );
    let state: String = ext::table
        .filter(ext::id.eq(ext_id))
        .select(ext::state)
        .first(&mut conn)
        .await
        .expect("external task must exist");
    assert_eq!(state, "TIMED_OUT");
}

#[tokio::test]
async fn resume_shifts_external_task_schedule_to_close_by_pause_span() {
    // Finding 2 (issue #609 post-review hardening): resume pushes each
    // still-open external task's schedule_to_close_at forward by the clamped
    // pause span, mirroring the harvest_task_queue treatment.
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "ext-shift-1").await;

    pause_workflow_execution(&mut conn, exec_id, Some("hold"), "oncall", &NoOpMetrics)
        .await
        .expect("pause should succeed");

    let now = chrono::Utc::now();
    let deadline_before = now + chrono::Duration::minutes(10);
    let ext_id = insert_pending_external_task(&mut conn, exec_id, deadline_before).await;

    // Backdate the pause 30 minutes so the resume computes a deterministic,
    // non-zero span (mirrors resume_shifts_schedule_to_close_at_by_pause_span).
    use autumn_harvest::schema::harvest_workflow_executions as e;
    diesel::update(e::table.filter(e::id.eq(exec_id.as_uuid())))
        .set(e::paused_at.eq(Some(now - chrono::Duration::minutes(30))))
        .execute(&mut conn)
        .await
        .expect("backdate paused_at for test");

    resume_workflow_execution(&mut conn, exec_id, "oncall", &NoOpMetrics)
        .await
        .expect("resume should succeed");

    use autumn_harvest::schema::harvest_external_tasks as ext;
    let deadline_after: chrono::DateTime<chrono::Utc> = ext::table
        .filter(ext::id.eq(ext_id))
        .select(ext::schedule_to_close_at)
        .first(&mut conn)
        .await
        .expect("external task must exist");
    let shift = deadline_after - deadline_before;
    assert!(
        shift >= chrono::Duration::minutes(29) && shift <= chrono::Duration::minutes(31),
        "external schedule_to_close_at must shift forward by the ~30-minute pause span, got {shift:?}"
    );
}

#[tokio::test]
async fn schedule_to_start_scanner_spares_frozen_rows_but_enforces_unfrozen_paused_rows() {
    // Finding 3 (issue #609 post-review hardening), option (b): a PENDING row
    // of a PAUSED execution past its (unshifted) schedule_to_close deadline
    // is frozen-unclaimable — the ScheduleToStart scanner must spare exactly
    // that row. An unfrozen pending activity of the same paused execution is
    // still claimable by design (activities are not pause-gated), so its
    // schedule_to_start signal (worker capacity) remains genuine and MUST
    // still be enforced — never a blanket paused-execution exclusion.
    use autumn_harvest::timeout::{TimeoutReason, find_timed_out_tasks};

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "s2s-frozen-1").await;
    let now = chrono::Utc::now();

    // Frozen: deadline elapsed → unclaimable while paused.
    let frozen_id = insert_pending_activity_task_with_schedule(
        &mut conn,
        exec_id,
        Some(now - chrono::Duration::minutes(1)),
        now - chrono::Duration::minutes(10),
        60,
    )
    .await;
    // Unfrozen: deadline still ahead → claimable despite the pause.
    let unfrozen_id = insert_pending_activity_task_with_schedule(
        &mut conn,
        exec_id,
        Some(now + chrono::Duration::minutes(10)),
        now - chrono::Duration::minutes(10),
        60,
    )
    .await;

    pause_workflow_execution(&mut conn, exec_id, Some("contain"), "oncall", &NoOpMetrics)
        .await
        .expect("pause should succeed");

    let timed_out = find_timed_out_tasks(&mut conn)
        .await
        .expect("scan should succeed");
    assert!(
        !timed_out.iter().any(|(t, _)| t.id == frozen_id),
        "a frozen row (paused execution + elapsed deadline) must be spared \
         by every scanner reason until resume shifts it"
    );
    assert!(
        timed_out
            .iter()
            .any(|(t, r)| t.id == unfrozen_id && *r == TimeoutReason::ScheduleToStart),
        "an unfrozen pending row of a paused execution is still claimable, \
         so its expired schedule_to_start must still be enforced"
    );
}

#[tokio::test]
async fn resume_shifts_scheduled_at_for_frozen_rows_only() {
    // Finding 3 (issue #609 post-review hardening), option (b): on resume,
    // exactly the frozen rows get scheduled_at shifted forward by the pause
    // span — restoring their schedule_to_start budget and retry-backoff
    // position — while unfrozen rows (claimable throughout the pause) keep
    // their queue position untouched.
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "s2s-shift-1").await;

    pause_workflow_execution(&mut conn, exec_id, Some("hold"), "oncall", &NoOpMetrics)
        .await
        .expect("pause should succeed");

    let now = chrono::Utc::now();
    let scheduled_before = now - chrono::Duration::minutes(40);
    let frozen_id = insert_pending_activity_task_with_schedule(
        &mut conn,
        exec_id,
        Some(now - chrono::Duration::minutes(5)),
        scheduled_before,
        60,
    )
    .await;
    let unfrozen_id = insert_pending_activity_task_with_schedule(
        &mut conn,
        exec_id,
        Some(now + chrono::Duration::minutes(10)),
        scheduled_before,
        60,
    )
    .await;

    use autumn_harvest::schema::harvest_workflow_executions as e;
    diesel::update(e::table.filter(e::id.eq(exec_id.as_uuid())))
        .set(e::paused_at.eq(Some(now - chrono::Duration::minutes(30))))
        .execute(&mut conn)
        .await
        .expect("backdate paused_at for test");

    resume_workflow_execution(&mut conn, exec_id, "oncall", &NoOpMetrics)
        .await
        .expect("resume should succeed");

    let (frozen_state, frozen_scheduled_at, frozen_deadline) =
        task_timing_columns(&mut conn, frozen_id).await;
    assert_eq!(frozen_state, "PENDING");
    let frozen_shift = frozen_scheduled_at - scheduled_before;
    assert!(
        frozen_shift >= chrono::Duration::minutes(29)
            && frozen_shift <= chrono::Duration::minutes(31),
        "the frozen row's scheduled_at must shift by the ~30-minute pause span, got {frozen_shift:?}"
    );
    let frozen_deadline_shift =
        frozen_deadline.expect("deadline must remain set") - (now - chrono::Duration::minutes(5));
    assert!(
        frozen_deadline_shift >= chrono::Duration::minutes(29)
            && frozen_deadline_shift <= chrono::Duration::minutes(31),
        "the frozen row's deadline still shifts like every open deadline, got {frozen_deadline_shift:?}"
    );

    let (unfrozen_state, unfrozen_scheduled_at, unfrozen_deadline) =
        task_timing_columns(&mut conn, unfrozen_id).await;
    assert_eq!(unfrozen_state, "PENDING");
    assert!(
        (unfrozen_scheduled_at - scheduled_before).abs() < chrono::Duration::seconds(1),
        "an unfrozen row's scheduled_at (queue position) must stay untouched"
    );
    let unfrozen_deadline_shift = unfrozen_deadline.expect("deadline must remain set")
        - (now + chrono::Duration::minutes(10));
    assert!(
        unfrozen_deadline_shift >= chrono::Duration::minutes(29)
            && unfrozen_deadline_shift <= chrono::Duration::minutes(31),
        "the unfrozen row's deadline still shifts forward, got {unfrozen_deadline_shift:?}"
    );
}

// ── Second bot-review round (issue #609 post-review hardening): scan-vs- ────
// ── enforce races in the timeout scanners ───────────────────────────────────

/// Inserts a PENDING activity task with a caller-chosen `activity_id` so a
/// matching non-terminal `ActivityScheduled` event can be appended to history
/// (the activity-timeout enforcer resolves the pending activity through
/// exactly that pairing).
async fn insert_pending_activity_task_with_id(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    activity_id: uuid::Uuid,
    schedule_to_close_at: chrono::DateTime<chrono::Utc>,
) -> uuid::Uuid {
    let task_id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, state, \
          attempt, max_attempts, schedule_to_close_at) \
         VALUES ($1, 'default', 'activity', $2, 'deadline_activity', $3, '{}'::jsonb, 'PENDING', \
                 0, 10, $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id)
    .bind::<diesel::sql_types::Timestamptz, _>(schedule_to_close_at)
    .execute(conn)
    .await
    .expect("insert pending activity task");
    task_id
}

/// Holds the execution row lock open on a dedicated connection (explicit
/// `BEGIN` + `SELECT ... FOR UPDATE`, no commit) so the test can queue a real
/// `pause_workflow_execution` behind it and release only after the scanner
/// under test has taken its non-locking scan snapshot — the exact
/// scan-then-pause-then-enforce ordering of the race. Mirrors the
/// lock-holding precedent in `integration_e2e.rs`
/// (`wake_workflow_task_retries_after_losing_the_park_row_lock_race`).
async fn hold_execution_row_lock(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    conn.batch_execute("BEGIN")
        .await
        .expect("begin should succeed");
    diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(conn)
        .await
        .expect("lock execution row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activity_schedule_to_close_enforcement_yields_to_a_pause_committed_after_the_scan() {
    // Second bot-review round, finding A (issue #609 post-review hardening,
    // P2): the ScheduleToClose scanner's PAUSED exclusion protects only the
    // `find_timed_out_tasks` SELECT snapshot. `enforce_activity_timeout`
    // then locks the execution row — but previously never re-checked whether
    // the locked row was now PAUSED, so a pause committing after the scan
    // (or while enforcement waited on the lock) still appended
    // `ActivityTimedOut { ScheduleToClose }` and failed the task mid-pause.
    // Choreography: hold the execution row lock on one connection, queue the
    // real pause behind it, then start the scanner — its scan snapshot (a
    // non-locking read) sees the still-RUNNING execution and picks up the
    // expired task, and its enforcement transaction queues on the row lock
    // BEHIND the pause. Releasing the lock lets the pause commit first; the
    // authoritative locked re-check must then skip enforcement entirely.
    use autumn_harvest::timeout::enforce_timeouts_once;

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let mut conn_lock = connect(&url).await;
    let mut conn_pause = connect(&url).await;
    let mut conn_scan = connect(&url).await;

    let exec_id = start(&mut conn, "wf", "s2c-scan-race-1").await;
    let activity_id = uuid::Uuid::new_v4();
    store::append_single_event(
        &mut conn,
        exec_id,
        WorkflowEvent::ActivityScheduled {
            activity_id: autumn_harvest::types::ActivityExecId::from_uuid(activity_id),
            name: "deadline_activity".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        },
    )
    .await
    .expect("append ActivityScheduled");
    let task_id = insert_pending_activity_task_with_id(
        &mut conn,
        exec_id,
        activity_id,
        chrono::Utc::now() - chrono::Duration::seconds(1),
    )
    .await;

    hold_execution_row_lock(&mut conn_lock, exec_id).await;

    // Queue the real pause behind the held lock so it wins the lock FIFO
    // over the scanner's enforcement transaction started below.
    let pause_handle = tokio::spawn(async move {
        pause_workflow_execution(
            &mut conn_pause,
            exec_id,
            Some("contain"),
            "oncall",
            &NoOpMetrics,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The scan snapshot runs unblocked (the execution's committed state is
    // still RUNNING, so the task is picked up as ScheduleToClose-expired);
    // the per-task enforcement transaction then blocks on the execution row
    // lock behind the queued pause.
    let scan_handle = tokio::spawn(async move {
        enforce_timeouts_once(
            &mut conn_scan,
            &NoOpMetrics,
            Duration::from_secs(5),
            &None,
            &[],
            None,
            None,
            60,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    conn_lock
        .batch_execute("COMMIT")
        .await
        .expect("release the held execution row lock");

    let paused = pause_handle
        .await
        .expect("pause task should not panic")
        .expect("pause should succeed");
    assert!(paused.newly_paused, "the pause must have won the race");
    scan_handle
        .await
        .expect("scanner task should not panic")
        .expect("timeout enforcement should succeed");

    use autumn_harvest::schema::harvest_task_queue as t;
    let (state, error): (String, Option<String>) = t::table
        .filter(t::id.eq(task_id))
        .select((t::state, t::error))
        .first(&mut conn)
        .await
        .expect("task must exist");
    assert_eq!(
        state, "PENDING",
        "the task must be left untouched (frozen by the claim gate until \
         resume), not deadline-failed against a paused execution"
    );
    assert!(
        error.is_none(),
        "a skipped enforcement must not write an error, got {error:?}"
    );
    assert!(
        !history(&mut conn, exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. })),
        "no ActivityTimedOut {{ ScheduleToClose }} may be appended while the \
         owning execution is paused"
    );
    assert_eq!(
        get_state(&mut conn, exec_id).await,
        "PAUSED",
        "the pause must survive the scanner untouched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_task_enforcement_yields_to_a_pause_committed_after_the_scan() {
    // Second bot-review round, finding B (issue #609 post-review hardening,
    // P2): the external-task scan's PAUSED check was a non-locking NOT EXISTS
    // subquery inside the claiming UPDATE — it never serialized with
    // `pause_workflow_execution` (which locks only the execution row), so a
    // pause committing between the UPDATE's snapshot and the event append
    // still flipped the external task to TIMED_OUT and appended
    // `ActivityTimedOut` mid-pause. The per-task transaction now locks the
    // external-task row first (third bot-review round: task-row-first is the
    // harvest_external_tasks convention set by the completion paths — taking
    // the execution lock first was an ABBA inversion against
    // `complete_externally`/`fail_externally`/`extend_deadline`), THEN the
    // execution row, and re-checks PAUSED under the execution lock — so the
    // state flip and the event append still serialize with the pause path.
    // Same choreography as the sibling task-queue test above.
    use autumn_harvest::timeout::enforce_external_task_timeouts;

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let mut conn_lock = connect(&url).await;
    let mut conn_pause = connect(&url).await;
    let mut conn_scan = connect(&url).await;

    let exec_id = start(&mut conn, "wf", "ext-scan-race-1").await;
    let ext_id = insert_pending_external_task(
        &mut conn,
        exec_id,
        chrono::Utc::now() - chrono::Duration::seconds(1),
    )
    .await;

    hold_execution_row_lock(&mut conn_lock, exec_id).await;

    let pause_handle = tokio::spawn(async move {
        pause_workflow_execution(
            &mut conn_pause,
            exec_id,
            Some("contain"),
            "oncall",
            &NoOpMetrics,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The scan SELECT is non-locking and sees the still-RUNNING committed
    // state, so the expired external task is claimed for enforcement; the
    // per-task transaction's execution row lock then queues behind the pause.
    let scan_handle =
        tokio::spawn(async move { enforce_external_task_timeouts(&mut conn_scan).await });
    tokio::time::sleep(Duration::from_millis(300)).await;

    conn_lock
        .batch_execute("COMMIT")
        .await
        .expect("release the held execution row lock");

    let paused = pause_handle
        .await
        .expect("pause task should not panic")
        .expect("pause should succeed");
    assert!(paused.newly_paused, "the pause must have won the race");
    let enforced = scan_handle
        .await
        .expect("scanner task should not panic")
        .expect("external timeout enforcement should succeed");
    assert_eq!(
        enforced, 0,
        "an enforcement skipped by the locked PAUSED re-check must not be \
         counted as timed out"
    );

    use autumn_harvest::schema::harvest_external_tasks as ext;
    let state: String = ext::table
        .filter(ext::id.eq(ext_id))
        .select(ext::state)
        .first(&mut conn)
        .await
        .expect("external task must exist");
    assert_eq!(
        state, "PENDING",
        "the external task must be left genuinely untouched so the \
         resume-time deadline shift covers it"
    );
    assert!(
        !history(&mut conn, exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. })),
        "no ActivityTimedOut may be appended while the execution is paused"
    );
    assert_eq!(
        get_state(&mut conn, exec_id).await,
        "PAUSED",
        "the pause must survive the scanner untouched"
    );
}

// ── Finding 1 (issue #609 post-review hardening): stale claim-time snapshot ─

/// Activity handler that simulates a pause→resume cycle completing while the
/// attempt is in flight: it shifts its own task row's `schedule_to_close_at`
/// forward (exactly what `resume_workflow_execution` does) and then fails
/// retryably. The worker's retry path then holds a claim-time snapshot whose
/// deadline is exceeded while the row-current deadline is comfortably ahead.
fn deadline_shifting_activity<'a>(
    _ctx: &'a autumn_harvest::ActivityContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let db_url = input
            .get("db_url")
            .and_then(Value::as_str)
            .expect("test input must carry db_url")
            .to_string();
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&db_url)
            .await
            .map_err(|e| e.to_string())?;
        diesel::sql_query(
            "UPDATE harvest_task_queue \
             SET schedule_to_close_at = NOW() + INTERVAL '1 hour' \
             WHERE activity_name = 'deadline_shifting_activity'",
        )
        .execute(&mut conn)
        .await
        .map_err(|e| e.to_string())?;
        Err("transient failure after the concurrent deadline shift".to_string())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn retry_path_requeues_when_a_concurrent_resume_shifted_the_deadline() {
    // Finding 1 (issue #609 post-review hardening, P1): the retry path's
    // schedule_to_close gate evaluates the claim-time TaskQueueItem snapshot;
    // resume_workflow_execution is the first post-enqueue mutator of that
    // column. An attempt claimed pre/mid-pause whose deadline is shifted by a
    // resume before it fails must be REQUEUED against the fresh (future)
    // deadline — not terminally failed against the stale snapshot, which
    // would charge paused wall-clock to the activity budget (AC5's exact
    // failure mode). The in-transaction fresh re-read under the execution row
    // lock is the guarantee; this test constructs the stale-snapshot state
    // deterministically by letting the activity itself perform the shift
    // mid-attempt.
    use autumn_harvest::RetryPolicy;
    use autumn_harvest::info::ActivityInfo;
    use autumn_harvest::queue::{EnqueueParams, TaskType};
    use autumn_harvest::types::ActivityExecId;

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    // Execution row without a workflow task: this test drives only the
    // activity retry path, so no workflow handler must ever run.
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'wf', $2, 0, 'null'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .execute(&mut conn)
    .await
    .expect("insert execution row");

    let activity_id = ActivityExecId::new();
    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "deadline_shifting_activity".to_string(),
                input: serde_json::json!({"db_url": url}),
                queue: "default".to_string(),
            },
        ],
        0,
    )
    .await
    .expect("append history");

    // Claim-time snapshot: deadline 30s ahead (claimable), retry delay 300s —
    // so the snapshot check is deterministically "exceeded" the moment the
    // attempt fails, with no sleeping.
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Activity,
        serde_json::json!({"db_url": url}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("deadline_shifting_activity".to_string());
    params.activity_id = Some(activity_id.as_uuid());
    params.max_attempts = 5;
    params.schedule_to_close_at = Some(chrono::Utc::now() + chrono::Duration::seconds(30));
    params.retry_policy = Some(
        serde_json::to_value(RetryPolicy::fixed(5, Duration::from_secs(300)))
            .expect("retry policy serializes"),
    );
    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue activity task");

    let registry = Arc::new(HandlerRegistry::new(
        vec![],
        vec![ActivityInfo {
            name: "deadline_shifting_activity",
            module: "pause_tests",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: deadline_shifting_activity,
        }],
    ));
    let worker = Arc::new(make_worker(registry));
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Wait for the attempt to fail and be re-resolved by the retry path: a
    // requeue lands the row back in PENDING with the attempt's error stored
    // (for `ActivityContext::previous_failure`) and a ~300s backoff, while
    // the buggy path lands it in FAILED with an ActivityTimedOut event.
    use autumn_harvest::schema::harvest_task_queue as t;
    let poll_deadline = std::time::Instant::now() + Duration::from_secs(30);
    let (state, error, scheduled_at) = loop {
        let (state, error, scheduled_at): (String, Option<String>, chrono::DateTime<chrono::Utc>) =
            t::table
                .filter(t::id.eq(task_id))
                .select((t::state, t::error, t::scheduled_at))
                .first(&mut conn)
                .await
                .expect("task must exist");
        let resolved = state == "FAILED" || (state == "PENDING" && error.is_some());
        if resolved || std::time::Instant::now() > poll_deadline {
            break (state, error, scheduled_at);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        state, "PENDING",
        "the task must be requeued against the fresh (shifted) deadline, \
         not terminally failed against the stale claim-time snapshot"
    );
    assert!(
        error
            .as_deref()
            .is_some_and(|e| e.contains("transient failure after the concurrent deadline shift")),
        "the requeued row must carry the attempt's error for the next attempt, got {error:?}"
    );
    assert!(
        scheduled_at > chrono::Utc::now() + chrono::Duration::seconds(200),
        "the requeue must respect the retry policy's 300s backoff, got {scheduled_at}"
    );
    assert!(
        !history(&mut conn, exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. })),
        "no ActivityTimedOut {{ ScheduleToClose }} may be recorded when the \
         row-current deadline is still ahead"
    );
    assert_eq!(
        get_state(&mut conn, exec_id).await,
        "RUNNING",
        "the owning execution must be unaffected"
    );
}

// ── Round 2 (issue #609 post-review hardening): pause after the gate ────────

/// Activity handler that reproduces an operator pause committing in the gap
/// between the retry path's non-locking `owning_execution_is_paused` gate and
/// `record_schedule_to_close_activity_timeout`'s transaction taking the
/// execution row lock. Mirroring the round-3 lock-holding precedent
/// (`wake_workflow_task_retries_after_losing_the_park_row_lock_race`): a hold
/// connection takes the execution row's `FOR UPDATE` lock in an explicit
/// open `BEGIN`, a second connection queues the *real*
/// `pause_workflow_execution` behind it, and the hold is released only after
/// this attempt's retryable failure has passed the gate (reading the still
/// committed-RUNNING snapshot) and blocked on the row lock — so the pause
/// commits first in the lock queue and the timeout transaction observes a
/// now-PAUSED execution whose row deadline IS exceeded. Only the
/// in-transaction PAUSED re-check can save the task from being
/// deadline-failed mid-pause.
fn self_pausing_activity<'a>(
    _ctx: &'a autumn_harvest::ActivityContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let db_url = input
            .get("db_url")
            .and_then(Value::as_str)
            .expect("test input must carry db_url")
            .to_string();
        let exec_id: ExecutionId = input
            .get("exec_id")
            .and_then(Value::as_str)
            .expect("test input must carry exec_id")
            .parse()
            .expect("exec_id must parse");

        // Hold the execution row lock open across an explicit BEGIN so the
        // pause below queues on it but cannot commit yet.
        let mut conn_hold = <AsyncPgConnection as AsyncConnection>::establish(&db_url)
            .await
            .map_err(|e| e.to_string())?;
        conn_hold
            .batch_execute("BEGIN")
            .await
            .map_err(|e| e.to_string())?;
        diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .execute(&mut conn_hold)
            .await
            .map_err(|e| e.to_string())?;

        // The real operator pause, queued behind the hold on its own
        // connection. It acquires the lock (and commits PAUSED) the moment
        // the hold releases.
        let pause_url = db_url.clone();
        tokio::spawn(async move {
            let mut conn_pause = <AsyncPgConnection as AsyncConnection>::establish(&pause_url)
                .await
                .expect("pause conn should connect");
            pause_workflow_execution(
                &mut conn_pause,
                exec_id,
                Some("incident containment mid-attempt"),
                "oncall",
                &NoOpMetrics,
            )
            .await
            .expect("pause should succeed once the hold releases");
        });
        // Give the spawned pause a moment to reach and block on the FOR
        // UPDATE queue before this attempt fails — so it is ahead of the
        // retry path's timeout transaction in the lock queue.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Release the hold only after the failure below has reached the
        // retry path: the non-locking gate reads the committed (RUNNING)
        // snapshot, then the timeout transaction blocks on the row lock
        // behind the pause. On a slow runner the gate may instead read an
        // already-committed PAUSED and short-circuit — the asserted end
        // state is identical either way.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            conn_hold
                .batch_execute("COMMIT")
                .await
                .expect("hold commit should succeed");
        });
        Err("transient failure after the concurrent pause".to_string())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn retry_path_requeues_when_a_concurrent_pause_committed_after_the_gate() {
    // Round 2 (issue #609 post-review hardening, P2): the retry path's
    // `owning_execution_is_paused` gate is a non-locking read taken BEFORE
    // `record_schedule_to_close_activity_timeout` opens its transaction and
    // takes the execution row lock. A pause committing in that gap used to
    // be deadline-failed anyway (the transaction only re-checked task state
    // and the row-current deadline), violating the
    // pause-suspends-schedule_to_close contract for the race window. The
    // authoritative PAUSED re-check under the execution row lock is the
    // guarantee; this test constructs the race window by letting the activity
    // itself queue a real pause of its own execution behind a held row lock
    // mid-attempt (released only after the failure reaches the retry path).
    // Both the claim-time snapshot deadline AND the row-current deadline are
    // exceeded, so neither the snapshot gate nor the resume-shift staleness
    // re-check can save the task — only the new PAUSED re-check.
    use autumn_harvest::RetryPolicy;
    use autumn_harvest::info::ActivityInfo;
    use autumn_harvest::queue::{EnqueueParams, TaskType};
    use autumn_harvest::types::ActivityExecId;

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    // Execution row without a workflow task: this test drives only the
    // activity retry path, so no workflow handler must ever run.
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'wf', $2, 0, 'null'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .execute(&mut conn)
    .await
    .expect("insert execution row");

    let activity_id = ActivityExecId::new();
    let activity_input = serde_json::json!({"db_url": url, "exec_id": exec_id.to_string()});
    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "self_pausing_activity".to_string(),
                input: activity_input.clone(),
                queue: "default".to_string(),
            },
        ],
        0,
    )
    .await
    .expect("append history");

    // Deadline 30s ahead (claimable) with a 300s retry delay: exceeded at
    // fail time both on the claim-time snapshot AND on the untouched row
    // value — deterministically, with no sleeping.
    let mut params = EnqueueParams::new("default", TaskType::Activity, activity_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("self_pausing_activity".to_string());
    params.activity_id = Some(activity_id.as_uuid());
    params.max_attempts = 5;
    params.schedule_to_close_at = Some(chrono::Utc::now() + chrono::Duration::seconds(30));
    params.retry_policy = Some(
        serde_json::to_value(RetryPolicy::fixed(5, Duration::from_secs(300)))
            .expect("retry policy serializes"),
    );
    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue activity task");

    let registry = Arc::new(HandlerRegistry::new(
        vec![],
        vec![ActivityInfo {
            name: "self_pausing_activity",
            module: "pause_tests",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: self_pausing_activity,
        }],
    ));
    let worker = Arc::new(make_worker(registry));
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Wait for the attempt to fail and be re-resolved by the retry path: a
    // requeue lands the row back in PENDING with the attempt's error stored,
    // while the buggy path lands it in FAILED with an ActivityTimedOut event.
    use autumn_harvest::schema::harvest_task_queue as t;
    let poll_deadline = std::time::Instant::now() + Duration::from_secs(30);
    let (state, error) = loop {
        let (state, error): (String, Option<String>) = t::table
            .filter(t::id.eq(task_id))
            .select((t::state, t::error))
            .first(&mut conn)
            .await
            .expect("task must exist");
        let resolved = state == "FAILED" || (state == "PENDING" && error.is_some());
        if resolved || std::time::Instant::now() > poll_deadline {
            break (state, error);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        state, "PENDING",
        "the task must be requeued (frozen by the claim gate until resume), \
         not deadline-failed against a paused execution"
    );
    assert!(
        error
            .as_deref()
            .is_some_and(|e| e.contains("transient failure after the concurrent pause")),
        "the requeued row must carry the attempt's error for the next attempt, got {error:?}"
    );
    assert!(
        !history(&mut conn, exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. })),
        "no ActivityTimedOut {{ ScheduleToClose }} may be recorded while the \
         owning execution is paused"
    );
    assert_eq!(
        get_state(&mut conn, exec_id).await,
        "PAUSED",
        "the pause must survive the retry path untouched"
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
    let first_at = first.paused_at.expect("fresh pause must report paused_at");
    let second_at = second
        .paused_at
        .expect("an idempotent repeat must still report when the pause took effect");
    // Tolerant comparison (µs column precision vs ns in-memory instant): the
    // repeat must report the original pause instant, not its own timestamp.
    assert!(
        (second_at - first_at).abs() < chrono::Duration::milliseconds(1),
        "expected the original pause instant, got {second_at} vs {first_at}"
    );

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
async fn resume_of_running_execution_is_a_success_noop() {
    // AC7 (issue #609): resuming a non-paused run is a success no-op —
    // an idempotent operator retry must not error.
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "r-running").await;

    let resumed = resume_workflow_execution(&mut conn, exec_id, "a", &NoOpMetrics)
        .await
        .expect("resuming a never-paused execution must be a success no-op");
    assert!(!resumed.newly_resumed);
    assert_eq!(resumed.state, "RUNNING");
    assert!(resumed.pause_duration_secs.abs() < f64::EPSILON);

    // Nothing was mutated: state unchanged, no WorkflowExecutionResumed event.
    assert_eq!(get_state(&mut conn, exec_id).await, "RUNNING");
    assert!(
        !history(&mut conn, exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowExecutionResumed { .. })),
        "a no-op resume must not append an event"
    );
}

#[tokio::test]
async fn resume_of_completed_execution_is_a_success_noop() {
    // AC7 (issue #609): a resume retried after the run completed post-resume
    // must not error either.
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "wf", "r-completed").await;

    use autumn_harvest::schema::harvest_workflow_executions as e;
    diesel::update(e::table.filter(e::id.eq(exec_id.as_uuid())))
        .set((
            e::state.eq("COMPLETED"),
            e::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .expect("seal execution COMPLETED for test");

    let resumed = resume_workflow_execution(&mut conn, exec_id, "a", &NoOpMetrics)
        .await
        .expect("resuming a terminal execution must be a success no-op");
    assert!(!resumed.newly_resumed);
    assert_eq!(resumed.state, "COMPLETED");
    assert!(resumed.pause_duration_secs.abs() < f64::EPSILON);
    assert_eq!(get_state(&mut conn, exec_id).await, "COMPLETED");
    assert!(
        !history(&mut conn, exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowExecutionResumed { .. })),
        "a no-op resume must not append an event"
    );
}

#[tokio::test]
async fn resume_of_unknown_execution_is_not_found() {
    // Unknown ids stay a 404 — only *existing* non-paused runs no-op.
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let missing = ExecutionId::new_for_shard(ShardId::new(0));
    let err = resume_workflow_execution(&mut conn, missing, "a", &NoOpMetrics)
        .await
        .expect_err("resuming a missing execution must 404");
    assert!(matches!(err, HarvestError::NotFound(_)), "got {err:?}");
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
        None,
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
        WorkflowOutcome::Completed { output, .. } => {
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
