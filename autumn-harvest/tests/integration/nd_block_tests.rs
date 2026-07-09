#![cfg(feature = "db")]
//! Non-terminal replay-non-determinism block tests — issue #603.
//!
//! Verifies that an engine-detected replay divergence blocks the execution
//! non-terminally (no `WorkflowFailed` event, state stays `RUNNING`, task
//! re-pended with backoff), that a rollback to the compatible build resumes
//! the execution with zero data loss and no operator action, that an author's
//! own `Err(...)` still fails terminally exactly as before, that a blocked
//! child never wakes its parent, and that re-blocks grow the backoff.
//!
//! The "deploy" is simulated by running successive workers whose registries
//! carry different handler bodies for the same workflow name: v1 arms a
//! durable timer; the divergent v2 schedules an activity instead, so replay
//! of the recorded `TimerStarted` diverges deterministically.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::context::empty_shared_state;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::schema::{harvest_task_queue, harvest_timers, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, Priority, ShardId, StartWorkflowParams, WorkflowContext,
    start_or_load_workflow_execution,
};

use diesel::prelude::*;
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
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
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
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
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
    include_str!("../../migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    include_str!("../../migrations/20260624000000_harvest_event_batches/up.sql"),
    "\n",
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    include_str!("../../migrations/20260709000000_harvest_workflow_continue_chain/up.sql"),
);

// ── Recording metrics ──────────────────────────────────────────────────────

// Mutex-based counters (not atomics): `diesel_async::RunQueryDsl`'s blanket
// `load` method shadows `AtomicUsize::load` in this file's scope.
#[derive(Debug, Default)]
struct RecordingMetrics {
    block_labels: Mutex<Vec<(String, String)>>,
    nd_detections: Mutex<usize>,
    terminal_failed: Mutex<usize>,
}

impl RecordingMetrics {
    fn nd_block_count(&self) -> usize {
        self.block_labels.lock().unwrap().len()
    }

    fn nd_detection_count(&self) -> usize {
        *self.nd_detections.lock().unwrap()
    }

    fn terminal_failed_count(&self) -> usize {
        *self.terminal_failed.lock().unwrap()
    }
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_nondeterministic_block(&self, workflow_name: &str, queue: &str) {
        self.block_labels
            .lock()
            .unwrap()
            .push((workflow_name.to_string(), queue.to_string()));
    }

    fn record_workflow_non_determinism(&self, _workflow_name: &str, _build_id: &str) {
        *self.nd_detections.lock().unwrap() += 1;
    }

    fn record_workflow_terminal(
        &self,
        _workflow_name: &str,
        _queue: &str,
        outcome: autumn_harvest::telemetry::WorkflowStatus,
    ) {
        if matches!(outcome, autumn_harvest::telemetry::WorkflowStatus::Failed) {
            *self.terminal_failed.lock().unwrap() += 1;
        }
    }
}

// ── Workflow handler functions (must be fn pointers, not closures) ─────────

/// v1 ("good build"): arm a long durable timer, then complete.
fn timer_v1_handler(
    ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        ctx.timer("nd-gate", 300).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!("v1-completed"))
    })
}

/// v2 ("divergent build"): schedules an activity where v1 recorded a timer —
/// replay of any v1-started history diverges deterministically.
fn divergent_v2_handler(
    ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        let out = ctx
            .execute_activity_raw("nd_new_activity", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(out)
    })
}

/// v1 variant used for the continue-as-new regression test: waits on the
/// same durable timer as `timer_v1_handler`, then requests continue-as-new
/// instead of completing.
fn timer_then_continue_as_new_handler(
    ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        ctx.timer("nd-gate", 300).await.map_err(|e| e.to_string())?;
        let _ = ctx.continue_as_new(serde_json::json!("v1-continued")).await;
        unreachable!("continue_as_new must not resolve");
    })
}

/// Author-error control: fails from its own logic (AC3).
fn author_fail_handler(
    _ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move { Err("author says no".to_string()) })
}

/// Completes immediately with no suspension — used to exercise the terminal
/// writers' belt-and-braces ND-block reset on a row that was never blocked.
fn immediate_complete_handler(
    _ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move { Ok(serde_json::json!("done")) })
}

/// Parent: awaits one child workflow and returns its output.
fn parent_handler(
    ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        let out = ctx
            .spawn_child_workflow_raw("nd_child", Value::Null)
            .await
            .map_err(|e| e.to_string())?;
        Ok(out)
    })
}

// ── Test helpers ───────────────────────────────────────────────────────────

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
    AsyncPgConnection::establish(url)
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
        name,
        module: "nd_block_tests",
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
        mcp: false,
    }
}

fn make_worker(
    workflows: Vec<WorkflowInfo>,
    metrics: Arc<dyn MetricsRecorder + Send + Sync>,
    build_id: &str,
) -> Worker {
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        vec![],
        empty_shared_state(),
        telemetry,
    ));
    Worker::new(
        WorkerRuntimeConfig {
            worker_id: uuid::Uuid::new_v4().to_string(),
            queues: vec!["default".to_string()],
            queue_weights: std::collections::HashMap::new(),
            notification_database_url: None,
            shard_notification_database_urls: Vec::new(),
            max_concurrent_workflows: 10,
            max_concurrent_activities: 20,
            poll_interval: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(2),
            cancellation_grace_period: Duration::from_secs(2),
            sticky_timeout: Duration::ZERO,
            max_local_activity_start_to_close: Duration::from_secs(60),
            shard_assignments: vec![ShardId::new(0)],
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: build_id.to_string(),
            deployment_name: None,
            workflow_cache_size: 100,
            priority_aging_secs: None,
            unknown_target_grace_window: Duration::from_secs(5),
            poison_pill_threshold: 3,
            workflow_task_timeout: Duration::from_secs(10),
            max_workflow_pause_duration: Duration::from_secs(24 * 3600),
            labels: std::collections::HashMap::new(),
            sharded_pool: None,
            max_workflow_history_events: None,
            slot_tuner: None,
            max_concurrent_sessions: 0,
        },
        registry,
    )
    .expect("worker should build")
}

/// Spawn a worker task bounded by a timeout; returns (worker, join handle).
fn spawn_worker(worker: Worker, pool: DbPool) -> (Arc<Worker>, tokio::task::JoinHandle<()>) {
    let worker = Arc::new(worker);
    let worker_ref = worker.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(60), worker_ref.run(&pool)).await;
    });
    (worker, handle)
}

async fn start_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    workflow_id: &str,
) -> ExecutionId {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input: serde_json::Value::Null,
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
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::state)
        .first::<String>(conn)
        .await
        .expect("execution must exist")
}

/// (`nd_blocked_at` set?, `nd_block_reason`, `nd_block_count`, `search_attrs`)
async fn get_nd_block_row(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> (bool, Option<String>, i32, Option<Value>) {
    let (blocked_at, reason, count, attrs): (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        i32,
        Option<Value>,
    ) = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select((
            harvest_workflow_executions::nd_blocked_at,
            harvest_workflow_executions::nd_block_reason,
            harvest_workflow_executions::nd_block_count,
            harvest_workflow_executions::search_attrs,
        ))
        .first(conn)
        .await
        .expect("execution must exist");
    (blocked_at.is_some(), reason, count, attrs)
}

async fn get_history(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    store::load_history(conn, exec_id)
        .await
        .expect("load history")
        .events
}

/// The single workflow task row for an execution:
/// (`state`, `scheduled_at`, `sticky_worker_id`, `error`).
async fn get_workflow_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> (
    String,
    chrono::DateTime<chrono::Utc>,
    Option<String>,
    Option<String>,
) {
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::task_type.eq("workflow"))
        .select((
            harvest_task_queue::state,
            harvest_task_queue::scheduled_at,
            harvest_task_queue::sticky_worker_id,
            harvest_task_queue::error,
        ))
        .first(conn)
        .await
        .expect("workflow task row must exist")
}

/// Wait until the execution's history contains a `TimerStarted` event —
/// i.e. the v1 build has run its first decision cycle and durably suspended.
async fn wait_for_timer_started(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    for _ in 0..400 {
        let history = get_history(conn, exec_id).await;
        if history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. }))
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("execution {exec_id} never recorded TimerStarted");
}

/// Backdate the durable timer so the next timer sweep fires it immediately.
async fn fire_timer_now(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    diesel::update(
        harvest_timers::table
            .filter(harvest_timers::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(harvest_timers::fired.eq(false)),
    )
    .set(harvest_timers::fires_at.eq(chrono::Utc::now() - chrono::Duration::seconds(1)))
    .execute(conn)
    .await
    .expect("backdate timer");
}

/// Backdate a blocked task's `scheduled_at` so it is claimable immediately
/// (skips the backoff wait in tests).
async fn make_task_claimable_now(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    diesel::update(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .filter(harvest_task_queue::task_type.eq("workflow")),
    )
    .set(harvest_task_queue::scheduled_at.eq(chrono::Utc::now() - chrono::Duration::seconds(1)))
    .execute(conn)
    .await
    .expect("backdate task scheduled_at");
}

/// Wait until `nd_block_count` reaches at least `count`.
async fn wait_for_nd_block(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    count: i32,
) -> (bool, Option<String>, i32, Option<Value>) {
    for _ in 0..400 {
        let row = get_nd_block_row(conn, exec_id).await;
        if row.2 >= count && row.0 {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let row = get_nd_block_row(conn, exec_id).await;
    panic!(
        "execution {exec_id} never reached nd_block_count >= {count}; current: blocked={} count={} reason={:?} state={}",
        row.0,
        row.2,
        row.1,
        get_state(conn, exec_id).await
    );
}

/// Wait until an execution reaches one of the given states.
async fn wait_for_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId, states: &[&str]) {
    for _ in 0..400 {
        let state = get_state(conn, exec_id).await;
        if states.contains(&state.as_str()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let state = get_state(conn, exec_id).await;
    panic!("execution {exec_id} never reached {states:?}; current state: {state}");
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// AC1 + AC5 + AC6: an engine-detected divergence blocks the execution
/// non-terminally — no `WorkflowFailed` event, state stays RUNNING, the block
/// marker + diagnostic are stamped, the task is re-pended with a future
/// `scheduled_at`, sticky affinity is cleared, and both the detection and
/// block counters fire (with zero terminal-failure counters).
#[tokio::test]
async fn divergent_replay_blocks_instead_of_failing() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let metrics = Arc::new(RecordingMetrics::default());

    let exec_id = start_workflow(&mut conn, "nd_wf", "nd-block-001").await;

    // Phase 1: v1 build runs the first decision cycle and suspends on a timer.
    let (worker1, handle1) = spawn_worker(
        make_worker(
            vec![wf_info("nd_wf", timer_v1_handler)],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    wait_for_timer_started(&mut conn, exec_id).await;
    worker1.shutdown();
    let _ = handle1.await;

    // Phase 2: the "divergent deploy" — v2 replays the same history and
    // requests an activity where v1 recorded a timer.
    fire_timer_now(&mut conn, exec_id).await;
    let (worker2, handle2) = spawn_worker(
        make_worker(
            vec![wf_info("nd_wf", divergent_v2_handler)],
            metrics.clone(),
            "v2",
        ),
        build_pool(&url),
    );
    let (blocked, reason, count, attrs) = wait_for_nd_block(&mut conn, exec_id, 1).await;
    worker2.shutdown();
    let _ = handle2.await;

    // Non-terminal: state stays RUNNING; no WorkflowFailed event was appended.
    assert!(blocked, "nd_blocked_at must be set");
    assert_eq!(count, 1);
    assert_eq!(get_state(&mut conn, exec_id).await, "RUNNING");
    let history = get_history(&mut conn, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a blocked execution must have NO terminal WorkflowFailed event"
    );
    assert!(
        reason
            .as_deref()
            .is_some_and(|r| r.contains("non-deterministic replay")),
        "nd_block_reason must carry the divergence error: {reason:?}"
    );

    // AC5 diagnostic: search_attrs carries the structured divergence fields.
    let attrs = attrs.expect("search_attrs must be stamped");
    assert_eq!(attrs["failure_cause"], "non_determinism");
    assert_eq!(attrs["actual"], "TimerStarted");
    assert!(
        attrs["expected"]
            .as_str()
            .is_some_and(|e| e.contains("ActivityScheduled")),
        "expected must name the diverging request: {attrs}"
    );
    assert!(attrs["event_index"].is_number());
    assert_eq!(attrs["workflow_type"], "nd_wf");
    assert_eq!(attrs["build_id"], "v2");

    // Re-claimable with backoff: PENDING, future scheduled_at, unpinned.
    let (task_state, scheduled_at, sticky, task_error) =
        get_workflow_task(&mut conn, exec_id).await;
    assert_eq!(task_state, "PENDING");
    assert!(
        scheduled_at > chrono::Utc::now() + chrono::Duration::seconds(2),
        "first block must defer the re-dispatch by ~5s; got {scheduled_at}"
    );
    assert!(sticky.is_none(), "sticky affinity must be cleared");
    assert!(
        task_error
            .as_deref()
            .is_some_and(|e| e.contains("non-deterministic replay")),
        "task error must carry the divergence reason"
    );

    // AC6: both counters fired; zero terminal-failure counters.
    assert_eq!(metrics.nd_block_count(), 1);
    assert_eq!(metrics.nd_detection_count(), 1);
    assert_eq!(
        metrics.terminal_failed_count(),
        0,
        "a blocked execution must NOT increment harvest.workflow.terminal{{outcome=failed}}"
    );
    assert_eq!(
        metrics.block_labels.lock().unwrap()[0],
        ("nd_wf".to_string(), "default".to_string())
    );
}

/// AC2 (the money test): after the offending build is rolled back, the next
/// dispatch replays cleanly and the workflow runs to completion — zero data
/// loss, no manual reset, block marker and diagnostic fully cleared.
#[tokio::test]
async fn blocked_execution_resumes_after_rollback() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let metrics = Arc::new(RecordingMetrics::default());

    let exec_id = start_workflow(&mut conn, "nd_wf", "nd-rollback-001").await;

    // v1 suspends on the timer.
    let (worker1, handle1) = spawn_worker(
        make_worker(
            vec![wf_info("nd_wf", timer_v1_handler)],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    wait_for_timer_started(&mut conn, exec_id).await;
    worker1.shutdown();
    let _ = handle1.await;

    // Divergent v2 blocks it.
    fire_timer_now(&mut conn, exec_id).await;
    let (worker2, handle2) = spawn_worker(
        make_worker(
            vec![wf_info("nd_wf", divergent_v2_handler)],
            metrics.clone(),
            "v2",
        ),
        build_pool(&url),
    );
    wait_for_nd_block(&mut conn, exec_id, 1).await;
    worker2.shutdown();
    let _ = handle2.await;

    // "Rollback": v1 is deployed again. Backdate the backoff so the test
    // doesn't sleep through it.
    make_task_claimable_now(&mut conn, exec_id).await;
    let (worker3, handle3) = spawn_worker(
        make_worker(
            vec![wf_info("nd_wf", timer_v1_handler)],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    wait_for_state(&mut conn, exec_id, &["COMPLETED"]).await;
    worker3.shutdown();
    let _ = handle3.await;

    // Zero data loss: the original v1 history replayed to completion with the
    // original output; no WorkflowFailed anywhere in the history.
    let history = get_history(&mut conn, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "history must never contain WorkflowFailed"
    );
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. })),
        "history must end in WorkflowCompleted"
    );
    let output: Option<Value> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::output)
        .first(&mut conn)
        .await
        .expect("execution must exist");
    assert_eq!(output, Some(serde_json::json!("v1-completed")));

    // Recovery cleared the marker and the search-attrs diagnostic.
    let (blocked, reason, count, attrs) = get_nd_block_row(&mut conn, exec_id).await;
    assert!(!blocked, "nd_blocked_at must be cleared on recovery");
    assert!(reason.is_none(), "nd_block_reason must be cleared");
    assert_eq!(count, 0, "nd_block_count must reset on recovery");
    if let Some(attrs) = attrs {
        for key in [
            "failure_cause",
            "event_index",
            "expected",
            "actual",
            "workflow_type",
            "build_id",
        ] {
            assert!(
                attrs.get(key).is_none(),
                "search-attrs diagnostic key '{key}' must be removed on recovery: {attrs}"
            );
        }
    }
}

/// AC3: a workflow whose body genuinely returns `Err(...)` still terminates
/// FAILED with a `WorkflowFailed` event, exactly as before — and never touches
/// the ND-block columns.
#[tokio::test]
async fn author_error_still_fails_terminally() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let metrics = Arc::new(RecordingMetrics::default());

    let exec_id = start_workflow(&mut conn, "author_fail_wf", "nd-author-001").await;

    let (worker, handle) = spawn_worker(
        make_worker(
            vec![wf_info("author_fail_wf", author_fail_handler)],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    wait_for_state(&mut conn, exec_id, &["FAILED"]).await;
    worker.shutdown();
    let _ = handle.await;

    let history = get_history(&mut conn, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "an author failure must still write the terminal WorkflowFailed event"
    );
    let (blocked, reason, count, _) = get_nd_block_row(&mut conn, exec_id).await;
    assert!(!blocked && reason.is_none() && count == 0);
    assert_eq!(metrics.nd_block_count(), 0);
    assert_eq!(metrics.terminal_failed_count(), 1);
}

/// A blocked child never notifies its parent: the parent stays suspended on
/// the child, and after a rollback the child completes and the parent
/// completes with the child's output.
#[tokio::test]
async fn blocked_child_does_not_notify_parent() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let metrics = Arc::new(RecordingMetrics::default());

    let parent_exec = start_workflow(&mut conn, "nd_parent", "nd-parent-001").await;

    // v1: parent spawns the child; child suspends on its timer.
    let (worker1, handle1) = spawn_worker(
        make_worker(
            vec![
                wf_info("nd_parent", parent_handler),
                wf_info("nd_child", timer_v1_handler),
            ],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    // Find the child execution (parent_id = parent).
    let child_exec = {
        let mut found = None;
        for _ in 0..400 {
            let rows: Vec<uuid::Uuid> = harvest_workflow_executions::table
                .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec.as_uuid())))
                .select(harvest_workflow_executions::id)
                .load(&mut conn)
                .await
                .expect("load children");
            // Slice pattern (not `.first()`): diesel's in-scope
            // `RunQueryDsl::first` shadows `Vec::first` here.
            if let [id, ..] = rows.as_slice() {
                found = Some(ExecutionId::from_uuid(*id));
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        found.expect("child execution must be created")
    };
    wait_for_timer_started(&mut conn, child_exec).await;
    worker1.shutdown();
    let _ = handle1.await;

    // Divergent deploy for the child only.
    fire_timer_now(&mut conn, child_exec).await;
    let (worker2, handle2) = spawn_worker(
        make_worker(
            vec![
                wf_info("nd_parent", parent_handler),
                wf_info("nd_child", divergent_v2_handler),
            ],
            metrics.clone(),
            "v2",
        ),
        build_pool(&url),
    );
    wait_for_nd_block(&mut conn, child_exec, 1).await;
    worker2.shutdown();
    let _ = handle2.await;

    // Child blocked non-terminally; parent untouched and still waiting.
    assert_eq!(get_state(&mut conn, child_exec).await, "RUNNING");
    assert_eq!(get_state(&mut conn, parent_exec).await, "RUNNING");
    let parent_history = get_history(&mut conn, parent_exec).await;
    assert!(
        !parent_history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ChildWorkflowFailed { .. })),
        "a blocked child must NOT deliver ChildWorkflowFailed to its parent"
    );

    // Rollback: child v1 again → child completes → parent completes.
    make_task_claimable_now(&mut conn, child_exec).await;
    let (worker3, handle3) = spawn_worker(
        make_worker(
            vec![
                wf_info("nd_parent", parent_handler),
                wf_info("nd_child", timer_v1_handler),
            ],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    wait_for_state(&mut conn, child_exec, &["COMPLETED"]).await;
    wait_for_state(&mut conn, parent_exec, &["COMPLETED"]).await;
    worker3.shutdown();
    let _ = handle3.await;

    let output: Option<Value> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(parent_exec.as_uuid()))
        .select(harvest_workflow_executions::output)
        .first(&mut conn)
        .await
        .expect("parent must exist");
    assert_eq!(output, Some(serde_json::json!("v1-completed")));
}

/// AC4: a still-diverging re-dispatch re-blocks — `nd_block_count` increments
/// and the backoff grows (5s, then 10s), so a permanently diverging history is
/// rate-limited, never a hot loop.
#[tokio::test]
async fn reblock_increments_count_and_grows_backoff() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let metrics = Arc::new(RecordingMetrics::default());

    let exec_id = start_workflow(&mut conn, "nd_wf", "nd-reblock-001").await;

    let (worker1, handle1) = spawn_worker(
        make_worker(
            vec![wf_info("nd_wf", timer_v1_handler)],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    wait_for_timer_started(&mut conn, exec_id).await;
    worker1.shutdown();
    let _ = handle1.await;

    fire_timer_now(&mut conn, exec_id).await;
    let (worker2, handle2) = spawn_worker(
        make_worker(
            vec![wf_info("nd_wf", divergent_v2_handler)],
            metrics.clone(),
            "v2",
        ),
        build_pool(&url),
    );
    wait_for_nd_block(&mut conn, exec_id, 1).await;
    let (_, first_scheduled_at, _, _) = get_workflow_task(&mut conn, exec_id).await;
    let first_delay = first_scheduled_at - chrono::Utc::now();

    // Skip the first backoff; the still-divergent v2 worker re-blocks.
    make_task_claimable_now(&mut conn, exec_id).await;
    wait_for_nd_block(&mut conn, exec_id, 2).await;
    let (_, second_scheduled_at, _, _) = get_workflow_task(&mut conn, exec_id).await;
    let second_delay = second_scheduled_at - chrono::Utc::now();
    worker2.shutdown();
    let _ = handle2.await;

    assert_eq!(get_state(&mut conn, exec_id).await, "RUNNING");
    // First block defers ~5s, second ~10s. Generous slop for CI clocks.
    assert!(
        second_delay > first_delay,
        "backoff must grow: first={first_delay}, second={second_delay}"
    );
    assert!(
        second_delay > chrono::Duration::seconds(6),
        "second block must defer by ~10s; got {second_delay}"
    );
    assert_eq!(metrics.nd_block_count(), 2);
    assert_eq!(
        metrics.terminal_failed_count(),
        0,
        "re-blocks must never fail the execution terminally"
    );
}

/// Regression test (code-review fix): a task row carrying a stale
/// `activity_name = 'mixed_signal_suspension'` sentinel (left behind by an
/// earlier `ctx.race()`/`receive_signal_timeout` timer+signal race, issue
/// #476/#600) must have that sentinel cleared when it is later ND-blocked.
/// Before the fix, the sentinel survived `requeue_workflow_task_nd_blocked`
/// and let `primary_repend_workflow_task_query` match the blocked row on any
/// unrelated wake, resetting `scheduled_at` early and bypassing the
/// capped-exponential backoff entirely (AC4).
#[tokio::test]
async fn nd_block_clears_stale_mixed_signal_suspension_sentinel() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let metrics = Arc::new(RecordingMetrics::default());

    let exec_id = start_workflow(&mut conn, "nd_wf", "nd-sentinel-001").await;

    let (worker1, handle1) = spawn_worker(
        make_worker(
            vec![wf_info("nd_wf", timer_v1_handler)],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    wait_for_timer_started(&mut conn, exec_id).await;
    worker1.shutdown();
    let _ = handle1.await;

    // Simulate a prior timer+signal race having stamped the sentinel on this
    // task row (the actual mechanism, `persist_started_timer`, is exercised
    // elsewhere; this test isolates the requeue-clearing behavior directly).
    diesel::update(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .filter(harvest_task_queue::task_type.eq("workflow")),
    )
    .set(harvest_task_queue::activity_name.eq(Some("mixed_signal_suspension")))
    .execute(&mut conn)
    .await
    .expect("stamp mixed_signal_suspension sentinel");

    fire_timer_now(&mut conn, exec_id).await;
    let (worker2, handle2) = spawn_worker(
        make_worker(
            vec![wf_info("nd_wf", divergent_v2_handler)],
            metrics.clone(),
            "v2",
        ),
        build_pool(&url),
    );
    wait_for_nd_block(&mut conn, exec_id, 1).await;
    worker2.shutdown();
    let _ = handle2.await;

    let activity_name: Option<String> = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::task_type.eq("workflow"))
        .select(harvest_task_queue::activity_name)
        .first(&mut conn)
        .await
        .expect("workflow task row must exist");
    assert_eq!(
        activity_name, None,
        "requeue_workflow_task_nd_blocked must clear the stale sentinel so an \
         unrelated wake_workflow_task call cannot bypass the backoff"
    );
}

/// Regression test (code-review fix): when a recovered cycle also produces
/// `ContinuedAsNew` in the same decision, the successor must NOT inherit the
/// just-cleared ND diagnostic `search_attrs` from the stale, pre-clear
/// in-memory snapshot.
#[tokio::test]
async fn continue_as_new_successor_does_not_inherit_stale_nd_search_attrs() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let metrics = Arc::new(RecordingMetrics::default());

    let exec_id = start_workflow(&mut conn, "nd_can_wf", "nd-can-001").await;

    // v1 (rollback build): suspends on the same timer, then continue-as-news.
    let (worker1, handle1) = spawn_worker(
        make_worker(
            vec![wf_info("nd_can_wf", timer_then_continue_as_new_handler)],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    wait_for_timer_started(&mut conn, exec_id).await;
    worker1.shutdown();
    let _ = handle1.await;

    // Divergent v2 blocks it, stamping the ND diagnostic into search_attrs.
    fire_timer_now(&mut conn, exec_id).await;
    let (worker2, handle2) = spawn_worker(
        make_worker(
            vec![wf_info("nd_can_wf", divergent_v2_handler)],
            metrics.clone(),
            "v2",
        ),
        build_pool(&url),
    );
    wait_for_nd_block(&mut conn, exec_id, 1).await;
    worker2.shutdown();
    let _ = handle2.await;

    // Rollback: v1 (timer_then_continue_as_new_handler) redeployed. The
    // recovered cycle both clears the ND block AND continue-as-news.
    make_task_claimable_now(&mut conn, exec_id).await;
    let (worker3, handle3) = spawn_worker(
        make_worker(
            vec![wf_info("nd_can_wf", timer_then_continue_as_new_handler)],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    wait_for_state(&mut conn, exec_id, &["CONTINUED_AS_NEW"]).await;
    worker3.shutdown();
    let _ = handle3.await;

    // Predecessor's own row: cleared (already covered by the rollback test,
    // asserted again here for completeness of this specific scenario).
    let (blocked, reason, count, _) = get_nd_block_row(&mut conn, exec_id).await;
    assert!(!blocked && reason.is_none() && count == 0);

    // Find the successor via the recorded WorkflowContinuedAsNew event.
    let history = get_history(&mut conn, exec_id).await;
    let successor_id = history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(*new_exec_id),
            _ => None,
        })
        .expect("history must contain WorkflowContinuedAsNew");

    let successor_attrs: Option<Value> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(successor_id.as_uuid()))
        .select(harvest_workflow_executions::search_attrs)
        .first(&mut conn)
        .await
        .expect("successor execution must exist");

    if let Some(attrs) = successor_attrs {
        for key in [
            "failure_cause",
            "event_index",
            "expected",
            "actual",
            "workflow_type",
            "build_id",
        ] {
            assert!(
                attrs.get(key).is_none(),
                "continue-as-new successor must NOT inherit the stale ND \
                 diagnostic key '{key}' from the pre-clear snapshot: {attrs}"
            );
        }
    }
}

/// Regression test (code-review fix): a normal completion on a row that was
/// *never* ND-blocked must not touch `search_attrs` at all. Before the fix,
/// `update_workflow_execution_completed`'s belt-and-braces reset called
/// `nd_search_attrs_clear_patch()` unconditionally on every completion,
/// which would silently delete a pre-existing user-authored `build_id`
/// business attribute written before these six keys became reserved
/// (issue #603 only reserves them going forward — it does not, and cannot,
/// revalidate rows already sitting in the database).
#[tokio::test]
async fn completion_never_touches_search_attrs_on_a_row_that_was_never_nd_blocked() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let metrics = Arc::new(RecordingMetrics::default());

    let exec_id = start_workflow(&mut conn, "immediate_wf", "nd-legacy-attrs-001").await;

    // Simulate a pre-existing legacy row: a business attribute under a name
    // that only became reserved in this deploy, written via raw SQL (as a
    // pre-migration application write would have) rather than through the
    // now-validating `upsert_search_attrs` path.
    diesel::update(harvest_workflow_executions::table)
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .set(harvest_workflow_executions::search_attrs.eq(Some(
            serde_json::json!({"build_id": "legacy-user-value", "tenant": "acme"}),
        )))
        .execute(&mut conn)
        .await
        .expect("seeding legacy search_attrs should succeed");

    let (worker, handle) = spawn_worker(
        make_worker(
            vec![wf_info("immediate_wf", immediate_complete_handler)],
            metrics.clone(),
            "v1",
        ),
        build_pool(&url),
    );
    wait_for_state(&mut conn, exec_id, &["COMPLETED"]).await;
    worker.shutdown();
    let _ = handle.await;

    let attrs: Option<Value> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::search_attrs)
        .first(&mut conn)
        .await
        .expect("execution must exist");

    let attrs = attrs.expect("search_attrs must survive a completion that was never ND-blocked");
    assert_eq!(
        attrs.get("build_id"),
        Some(&serde_json::json!("legacy-user-value")),
        "a pre-existing legacy business attribute named 'build_id' must survive an \
         ordinary completion: {attrs}"
    );
    assert_eq!(
        attrs.get("tenant"),
        Some(&serde_json::json!("acme")),
        "unrelated search_attrs keys must be untouched: {attrs}"
    );
}
