#![cfg(feature = "db")]
//! Handler-panic containment behavioural tests — issue #782 (AC7).
//!
//! Injects panicking workflow and activity handlers and drives them end-to-end
//! against a real Postgres instance + worker poll loop, proving that an
//! **uncaught handler panic is contained as a clean failure** rather than
//! crashing the worker process or wedging the task row:
//!
//! - AC1/AC3 (activity): a panicking activity handler is caught at the worker
//!   dispatch boundary, retried per the activity's retry policy, then fails
//!   terminally with a typed `HandlerPanic` `ActivityFailed` event carrying the
//!   panic message. No poison-pill DLQ row; `crash_strikes` never incremented.
//! - Local activity: a panicking `is_local = true` handler runs inline in the
//!   workflow task; the panic is caught and flattened into the same retryable
//!   `HandlerPanic` path (terminal `LocalActivityExhausted`), never unwinding
//!   the workflow-task dispatch.
//! - AC2/AC6 (workflow body): a panicking workflow handler is caught in the
//!   executor and re-dispatched (non-terminal, `RUNNING`, no event appended)
//!   with capped backoff up to `workflow_panic_max_attempts`, then failed
//!   terminally via a typed `HandlerPanic` `WorkflowFailed`. No poison-pill
//!   DLQ row; `crash_strikes` stays 0; no #523 fresh-execution retry chain.
//! - `workflow_panic_max_attempts = 0`: terminal on the first panic, no
//!   re-dispatch.
//!
//! In every case the worker process stays up (the execution reaching a terminal
//! state at all is the proof — a poisoned worker task would never process it),
//! and the panic counters (`record_activity_panic` / `record_workflow_panic`)
//! fire per panicking attempt/entry.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted
//! with `INIT_SQL`. Each test uses a unique `ExecutionId` and its own
//! capturing `MetricsRecorder`, so no cross-test scrubbing is required.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::ERROR_TYPE_HANDLER_PANIC;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_task_queue, harvest_workflow_executions,
};
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{RetryPolicy, WorkflowContext, store};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup — env-var-redirectable, otherwise testcontainers (mirrors
// retention_overrides_tests.rs so this suite EXECUTES against a local cluster
// when HARVEST_TEST_DATABASE_URL is set).
// ---------------------------------------------------------------------------

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
    "\n",
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
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
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    "\n",
    // issue #747: per-execution legal hold columns on harvest_workflow_executions.
    include_str!("../../migrations/20260709000000_harvest_legal_hold/up.sql"),
);

/// Returns a live URL to a migrated Postgres, keeping the container (if any)
/// alive for the test's duration.
async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

// ---------------------------------------------------------------------------
// Capturing metrics recorder for the two new #782 counters.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct PanicMetrics {
    activity_panics: Mutex<Vec<(String, String)>>,
    workflow_panics: Mutex<Vec<(String, String)>>,
}

impl PanicMetrics {
    fn activity_panic_count(&self) -> usize {
        self.activity_panics.lock().unwrap().len()
    }
    fn workflow_panic_count(&self) -> usize {
        self.workflow_panics.lock().unwrap().len()
    }
}

impl MetricsRecorder for PanicMetrics {
    fn record_activity_panic(&self, activity_name: &str, queue: &str) {
        self.activity_panics
            .lock()
            .unwrap()
            .push((activity_name.to_string(), queue.to_string()));
    }
    fn record_workflow_panic(&self, workflow_name: &str, queue: &str) {
        self.workflow_panics
            .lock()
            .unwrap()
            .push((workflow_name.to_string(), queue.to_string()));
    }
}

// ---------------------------------------------------------------------------
// Worker + registry construction (self-contained: the integration_e2e harness
// helpers are private to that module, and the workflow tests need a custom
// `workflow_panic_max_attempts`, which no shared helper exposes).
// ---------------------------------------------------------------------------

fn build_registry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    metrics: Arc<PanicMetrics>,
) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

fn build_worker(
    worker_id: &str,
    registry: Arc<HandlerRegistry>,
    workflow_panic_max_attempts: u32,
) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: worker_id.to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts,
                labels: HashMap::new(),
                queue_weights: HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

// ---------------------------------------------------------------------------
// Seeding + read helpers (self-contained).
// ---------------------------------------------------------------------------

/// Insert a minimal RUNNING execution row + its `WorkflowStarted` event and
/// enqueue the initial workflow task. Returns the fresh `ExecutionId`.
async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone(),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");

    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");

    exec_id
}

async fn load_execution(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = connect(url).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("reload workflow execution")
}

async fn load_tasks(url: &str, exec_id: ExecutionId) -> Vec<TaskQueueItem> {
    let mut conn = connect(url).await;
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .order(harvest_task_queue::scheduled_at.asc())
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("reload task rows")
}

async fn load_history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
}

/// Count all dead-letter rows owned by `exec_id`, plus how many carry the
/// `PoisonPill` reason discriminator.
async fn count_dead_letters(url: &str, exec_id: ExecutionId) -> (usize, usize) {
    let mut conn = connect(url).await;
    let errors: Vec<String> = harvest_dead_letters::table
        .filter(harvest_dead_letters::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .select(harvest_dead_letters::error)
        .load(&mut conn)
        .await
        .expect("load dead letters");
    let poison = errors.iter().filter(|e| e.contains("PoisonPill")).count();
    (errors.len(), poison)
}

async fn wait_for_state(
    url: &str,
    exec_id: ExecutionId,
    want: &str,
    timeout: Duration,
) -> WorkflowExecution {
    tokio::time::timeout(timeout, async {
        loop {
            let e = load_execution(url, exec_id).await;
            if e.state == want {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("execution did not reach state {want} within {timeout:?}"))
}

// ---------------------------------------------------------------------------
// Panicking handlers.
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Workflow that calls a single (regular) activity and propagates its error.
fn workflow_calls_panic_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("panic_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Regular activity handler that always panics.
fn panic_activity(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { panic!("activity boom") })
}

/// Workflow that calls a single LOCAL activity (inline) and propagates its error.
fn workflow_calls_panic_local(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw(
            "panic_local",
            input,
            Some(RetryPolicy::fixed(2, Duration::from_millis(10))),
            Some(5),
        )
        .await
        .map_err(|e| e.to_string())
    })
}

/// Local activity handler that always panics (runs inline in the workflow task).
fn panic_local(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { panic!("local activity boom") })
}

/// Workflow body that panics directly.
fn panic_workflow(_ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { panic!("workflow boom") })
}

/// Workflow whose handler panics **during future construction** — the panic
/// unwinds synchronously *before* `Box::pin(...)` is produced (issue #782 / PR
/// #1012 review). A registered hand-written handler doing synchronous work
/// before returning its boxed future must be contained identically to a body
/// panic; the poll-time `catch_unwind` cannot reach it.
fn construction_panic_workflow(_ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    panic!("workflow construction boom");
}

/// Workflow that calls a single (regular) activity that stays RUNNING for a
/// short, deterministic window before panicking (issue #782, TEST B). The
/// deliberate delay makes the RUNNING phase reliably sampleable so the test can
/// MEASURE how promptly the task leaves RUNNING once the handler panics.
fn workflow_calls_slow_panic_activity(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("slow_panic_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Regular activity that holds RUNNING for ~120ms (so the poller can sample it)
/// then panics. The RUNNING window is legitimate handler execution; the point of
/// the test is that the row leaves RUNNING promptly AFTER the panic, not that
/// the handler itself is fast.
fn slow_panic_activity(
    _ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(120)).await;
        panic!("slow activity boom")
    })
}

/// Parent workflow that spawns a single child and propagates the child's error
/// (issue #782, TEST D). The child (`panic_child_wf`) panics; after its panic
/// budget is exhausted it terminal-fails with a typed `HandlerPanic`
/// `WorkflowFailed`, which the parent observes as a typed `ChildWorkflowFailed`.
fn parent_spawns_panic_child(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("panic_child_wf", input)
            .await
            .map_err(|e| e.to_string())
    })
}

fn workflow_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "panic_containment_tests",
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

fn activity_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    is_local: bool,
    retry: Option<RetryPolicy>,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "panic_containment_tests",
        default_retry_policy: retry,
        default_start_to_close: Some(Duration::from_secs(5)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        is_local,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

// ---------------------------------------------------------------------------
// Test 1 — regular activity panic containment (AC1 + AC3 + AC7 a/b/c/d).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activity_handler_panic_is_contained_and_retried_then_terminal() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "panic_activity_wf", serde_json::json!({"n": 1})).await;

    let metrics = Arc::new(PanicMetrics::default());
    let registry = build_registry(
        vec![workflow_info(
            "panic_activity_wf",
            workflow_calls_panic_activity,
        )],
        vec![activity_info(
            "panic_activity",
            panic_activity,
            false,
            // 2 attempts: attempt 1 panics → retry; attempt 2 panics → terminal.
            Some(RetryPolicy::fixed(2, Duration::from_millis(50))),
        )],
        Arc::clone(&metrics),
    );
    let worker = build_worker("worker-activity-panic", Arc::clone(&registry), 3);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // (a) The worker process survives the panic and drives the run to a terminal
    // state — a poisoned worker task could never reach FAILED at all.
    let execution = wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(20)).await;

    worker.shutdown();
    handle.await.expect("worker task joins cleanly");

    // (c) Retried per policy (2 attempts) then terminal: exactly one terminal
    // `ActivityFailed` carrying error_type "HandlerPanic" + the panic message.
    let history = load_history(&url, exec_id).await;
    let activity_failures: Vec<_> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityFailed {
                error,
                error_type,
                attempt,
                ..
            } => Some((error.clone(), error_type.clone(), *attempt)),
            _ => None,
        })
        .collect();
    assert_eq!(
        activity_failures.len(),
        1,
        "exactly one terminal ActivityFailed expected; history={history:?}"
    );
    let (message, error_type, attempt) = &activity_failures[0];
    assert_eq!(
        error_type, ERROR_TYPE_HANDLER_PANIC,
        "error_type must be HandlerPanic"
    );
    assert!(
        message.contains("activity boom"),
        "panic message must surface in the failure detail, got {message:?}"
    );
    assert_eq!(
        *attempt, 2,
        "terminal failure should be on the 2nd (final) attempt"
    );

    // record_activity_panic fired once per panicking attempt (AC7 metric).
    assert_eq!(
        metrics.activity_panic_count(),
        2,
        "record_activity_panic should fire once per panicking attempt"
    );
    assert_eq!(
        metrics.workflow_panic_count(),
        0,
        "the workflow body itself did not panic"
    );

    // Workflow propagated the activity error → terminal WorkflowFailed, and the
    // typed HandlerPanic surfaces on the execution error.
    assert_eq!(execution.state, "FAILED");
    let err = execution.error.expect("FAILED execution carries an error");
    assert!(
        err.contains("activity boom") || err.contains("HandlerPanic"),
        "workflow error should reflect the contained panic, got {err:?}"
    );

    // (d) No poison-pill DLQ row; in fact finalize_activity_failure inserts NO
    // dead-letter at all, and crash_strikes was never incremented.
    let (total_dlq, poison_dlq) = count_dead_letters(&url, exec_id).await;
    assert_eq!(
        poison_dlq, 0,
        "a contained panic must NOT produce a PoisonPill DLQ row"
    );
    assert_eq!(
        total_dlq, 0,
        "a contained activity panic produces no DLQ row at all"
    );

    let tasks = load_tasks(&url, exec_id).await;
    let activity_task = tasks
        .iter()
        .find(|t| t.activity_name.as_deref() == Some("panic_activity"))
        .expect("activity task row exists");
    assert_eq!(
        activity_task.crash_strikes, 0,
        "crash_strikes must never be incremented by a contained handler panic"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — LOCAL activity panic containment.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_handler_panic_is_contained_and_retried_then_terminal() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "panic_local_wf", serde_json::json!({"n": 2})).await;

    let metrics = Arc::new(PanicMetrics::default());
    let registry = build_registry(
        vec![workflow_info("panic_local_wf", workflow_calls_panic_local)],
        // Retry policy is supplied at the execute_local_activity_raw call site;
        // the ActivityInfo just needs to exist and be flagged local.
        vec![activity_info("panic_local", panic_local, true, None)],
        Arc::clone(&metrics),
    );
    let worker = build_worker("worker-local-panic", Arc::clone(&registry), 3);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // A local activity runs INLINE in the workflow task; an uncaught panic would
    // unwind the whole dispatch. Reaching FAILED proves it was contained.
    let execution = wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(20)).await;

    worker.shutdown();
    handle.await.expect("worker task joins cleanly");

    let history = load_history(&url, exec_id).await;
    let local_failures = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::LocalActivityFailed { .. }))
        .count();
    assert_eq!(
        local_failures, 2,
        "each panicking local attempt records a LocalActivityFailed; history={history:?}"
    );
    let exhausted = history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::LocalActivityExhausted { error, attempt, .. } => {
                Some((error.clone(), *attempt))
            }
            _ => None,
        })
        .expect("terminal LocalActivityExhausted event expected");
    assert!(
        exhausted.0.contains("local activity boom"),
        "panic message must surface in the terminal local failure, got {:?}",
        exhausted.0
    );

    // Contained via the same retryable HandlerPanic path → panic counter fires
    // once per attempt. The workflow body did not itself panic.
    assert_eq!(
        metrics.activity_panic_count(),
        2,
        "record_activity_panic should fire once per panicking local attempt"
    );
    assert_eq!(
        metrics.workflow_panic_count(),
        0,
        "the workflow body itself did not panic"
    );

    assert_eq!(execution.state, "FAILED");

    // No DLQ row of any kind, no crash_strikes on the workflow task row.
    let (total_dlq, poison_dlq) = count_dead_letters(&url, exec_id).await;
    assert_eq!(
        poison_dlq, 0,
        "a contained local panic must NOT produce a PoisonPill DLQ row"
    );
    assert_eq!(
        total_dlq, 0,
        "a contained local activity panic produces no DLQ row"
    );

    let tasks = load_tasks(&url, exec_id).await;
    assert!(
        tasks.iter().all(|t| t.crash_strikes == 0),
        "crash_strikes must never be incremented by a contained local panic"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — workflow body panic: bounded re-dispatch then terminal
// (AC2 + AC6 + AC7 c/d).
//
// Wall clock: with workflow_panic_max_attempts = 2 the first panic re-dispatches
// with a ~1s capped-exponential backoff, the second is terminal — so the whole
// test takes roughly 1–2s.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn workflow_handler_panic_is_re_dispatched_then_terminal_within_budget() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "panic_workflow_wf", serde_json::json!({"n": 3})).await;

    let metrics = Arc::new(PanicMetrics::default());
    let registry = build_registry(
        vec![workflow_info("panic_workflow_wf", panic_workflow)],
        vec![],
        Arc::clone(&metrics),
    );
    // Small budget for speed: panic 1 re-dispatches, panic 2 is terminal.
    let worker = build_worker("worker-workflow-panic", Arc::clone(&registry), 2);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // (b/c) Observe the non-terminal re-dispatch: between the first and second
    // panic the workflow task is re-pended PENDING (state stays RUNNING, no
    // event appended, crash_strikes stays 0) with a future scheduled_at rather
    // than wedged in RUNNING. The ~1s backoff window makes this reliable.
    let observer_url = url.clone();
    let observer = tokio::spawn(async move {
        let mut saw_repended_pending = false;
        let mut saw_execution_running = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            let exec = load_execution(&observer_url, exec_id).await;
            if exec.state == "RUNNING" {
                saw_execution_running = true;
            }
            if exec.state == "FAILED" {
                break;
            }
            for t in load_tasks(&observer_url, exec_id).await {
                if t.task_type == "workflow"
                    && t.state == "PENDING"
                    && t.scheduled_at > Utc::now()
                    && t.crash_strikes == 0
                {
                    saw_repended_pending = true;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        (saw_repended_pending, saw_execution_running)
    });

    let execution = wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(30)).await;
    let (saw_repended_pending, saw_execution_running) = observer.await.expect("observer joins");

    worker.shutdown();
    handle.await.expect("worker task joins cleanly");

    assert!(
        saw_execution_running,
        "execution should stay RUNNING across the panic re-dispatch (never a mid-retry terminal)"
    );
    assert!(
        saw_repended_pending,
        "workflow task should be re-pended PENDING with a future scheduled_at during the backoff"
    );

    // record_workflow_panic fires on every panic entry (both retries + terminal),
    // so budget 2 ⇒ exactly 2 panic entries ⇒ exactly one non-terminal re-dispatch.
    assert_eq!(
        metrics.workflow_panic_count(),
        2,
        "record_workflow_panic should fire once per panic entry (max_attempts entries total)"
    );

    // Terminal via typed HandlerPanic WorkflowFailed carrying the panic message.
    let history = load_history(&url, exec_id).await;
    let workflow_failures: Vec<_> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::WorkflowFailed {
                error, error_type, ..
            } => Some((error.clone(), error_type.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        workflow_failures.len(),
        1,
        "exactly one terminal WorkflowFailed; the re-dispatch appends NO event; history={history:?}"
    );
    let (message, error_type) = &workflow_failures[0];
    assert_eq!(
        error_type.as_deref(),
        Some(ERROR_TYPE_HANDLER_PANIC),
        "WorkflowFailed.error_type must be HandlerPanic"
    );
    assert!(
        message.contains("workflow boom"),
        "panic message must surface in the terminal failure, got {message:?}"
    );

    // The re-dispatch never appended an intermediate event: history is exactly
    // WorkflowStarted → WorkflowFailed (proves state stayed RUNNING with no
    // durable footprint from the panicked cycles).
    let non_lifecycle: Vec<_> = history
        .iter()
        .filter(|e| {
            !matches!(
                e,
                WorkflowEvent::WorkflowStarted { .. } | WorkflowEvent::WorkflowFailed { .. }
            )
        })
        .collect();
    assert!(
        non_lifecycle.is_empty(),
        "a contained workflow panic re-dispatch must append no events; extra={non_lifecycle:?}"
    );

    assert_eq!(execution.state, "FAILED");

    // (d) No poison-pill DLQ; crash_strikes stays 0; and crucially NO #523
    // fresh-execution retry chain — exactly one execution row for this workflow.
    let (_total_dlq, poison_dlq) = count_dead_letters(&url, exec_id).await;
    assert_eq!(
        poison_dlq, 0,
        "a contained workflow panic must NOT produce a PoisonPill DLQ row"
    );

    let tasks = load_tasks(&url, exec_id).await;
    assert!(
        tasks.iter().all(|t| t.crash_strikes == 0),
        "crash_strikes must never be incremented by a contained workflow panic"
    );

    // #523 workflow-level retry forks a NEW execution linked back via
    // `retry_of_exec_id`; a contained HandlerPanic terminal must spawn none.
    // (Scoped to this exec_id — NOT a global workflow_name count — so it is
    // correct against a persistent shared DB reused across runs.)
    let mut conn2 = connect(&url).await;
    let retry_children: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::retry_of_exec_id.eq(Some(exec_id.as_uuid())))
        .count()
        .get_result(&mut conn2)
        .await
        .expect("count #523 retry children");
    assert_eq!(
        retry_children, 0,
        "a HandlerPanic terminal must NOT spawn a #523 fresh-execution retry chain"
    );
}

// ---------------------------------------------------------------------------
// Test 4 — workflow_panic_max_attempts = 0 → terminal on the FIRST panic.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_panic_max_attempts_zero_is_terminal_on_first_panic() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(
        &mut conn,
        "panic_workflow_zero_wf",
        serde_json::json!({"n": 4}),
    )
    .await;

    let metrics = Arc::new(PanicMetrics::default());
    let registry = build_registry(
        vec![workflow_info("panic_workflow_zero_wf", panic_workflow)],
        vec![],
        Arc::clone(&metrics),
    );
    let worker = build_worker("worker-workflow-panic-zero", Arc::clone(&registry), 0);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    let execution = wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(15)).await;

    worker.shutdown();
    handle.await.expect("worker task joins cleanly");

    // Exactly one panic entry → immediate terminal, no re-dispatch.
    assert_eq!(
        metrics.workflow_panic_count(),
        1,
        "max_attempts = 0 ⇒ terminal on the first panic, so exactly one panic entry"
    );

    let history = load_history(&url, exec_id).await;
    let workflow_failures: Vec<_> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::WorkflowFailed {
                error_type, error, ..
            } => Some((error_type.clone(), error.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(workflow_failures.len(), 1, "one terminal WorkflowFailed");
    assert_eq!(
        workflow_failures[0].0.as_deref(),
        Some(ERROR_TYPE_HANDLER_PANIC),
        "WorkflowFailed.error_type must be HandlerPanic"
    );
    assert!(workflow_failures[0].1.contains("workflow boom"));

    assert_eq!(execution.state, "FAILED");

    let (_total, poison_dlq) = count_dead_letters(&url, exec_id).await;
    assert_eq!(
        poison_dlq, 0,
        "no PoisonPill DLQ row for a contained workflow panic"
    );
    let tasks = load_tasks(&url, exec_id).await;
    assert!(
        tasks.iter().all(|t| t.crash_strikes == 0),
        "crash_strikes stays 0"
    );
}

// ---------------------------------------------------------------------------
// Test 5 (PR #1012 review) — a workflow handler that panics during future
// *construction* (before returning its boxed future) is contained end-to-end
// exactly like a body panic: HandlerPanic terminal, no poison-pill, strikes 0.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_construction_phase_panic_is_contained_as_handler_panic_terminal() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(
        &mut conn,
        "construction_panic_wf",
        serde_json::json!({"n": 1}),
    )
    .await;

    let metrics = Arc::new(PanicMetrics::default());
    let registry = build_registry(
        vec![workflow_info(
            "construction_panic_wf",
            construction_panic_workflow,
        )],
        vec![],
        Arc::clone(&metrics),
    );
    // max_attempts = 0 → terminal on the first panic (fast, no re-dispatch).
    let worker = build_worker("worker-construction-panic", Arc::clone(&registry), 0);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    let execution = wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(15)).await;

    worker.shutdown();
    handle.await.expect("worker task joins cleanly");

    // A construction panic is a genuine panic entry: the counter fires once.
    assert_eq!(
        metrics.workflow_panic_count(),
        1,
        "a construction-phase panic must be counted as a workflow panic entry"
    );

    let history = load_history(&url, exec_id).await;
    let workflow_failures: Vec<_> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::WorkflowFailed {
                error_type, error, ..
            } => Some((error_type.clone(), error.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(workflow_failures.len(), 1, "one terminal WorkflowFailed");
    assert_eq!(
        workflow_failures[0].0.as_deref(),
        Some(ERROR_TYPE_HANDLER_PANIC),
        "a contained construction panic must carry the HandlerPanic error type"
    );
    assert!(
        workflow_failures[0]
            .1
            .contains("workflow construction boom"),
        "the construction panic message must surface, got {:?}",
        workflow_failures[0].1
    );

    assert_eq!(execution.state, "FAILED");

    // The panic never reached the poison-pill path: no PoisonPill DLQ row,
    // crash_strikes never incremented.
    let (_total, poison_dlq) = count_dead_letters(&url, exec_id).await;
    assert_eq!(
        poison_dlq, 0,
        "no PoisonPill DLQ row for a contained construction panic"
    );
    let tasks = load_tasks(&url, exec_id).await;
    assert!(
        tasks.iter().all(|t| t.crash_strikes == 0),
        "crash_strikes stays 0"
    );
}

// ---------------------------------------------------------------------------
// Test B (AC7b success metric) — the panicking task leaves RUNNING promptly.
//
// #782's failure mode is a poisoned worker task that leaves the row wedged in
// RUNNING forever (and then cascades via SKIP LOCKED re-claim). This test makes
// the "off RUNNING within ~1 poll interval" success metric FALSIFIABLE rather
// than inferred: it tight-polls `harvest_task_queue.state` for the activity
// task, records the last instant it was seen RUNNING and the first instant it
// was seen off RUNNING afterward, and asserts the containment transition is
// bounded. A wedged task never leaves RUNNING, so any finite bound falsifies it.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_activity_task_leaves_running_within_one_poll_interval() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(
        &mut conn,
        "slow_panic_activity_wf",
        serde_json::json!({"n": 5}),
    )
    .await;

    let metrics = Arc::new(PanicMetrics::default());
    let registry = build_registry(
        vec![workflow_info(
            "slow_panic_activity_wf",
            workflow_calls_slow_panic_activity,
        )],
        // 1 attempt → the single panic is terminal, giving exactly one clean
        // RUNNING → FAILED transition to measure (no requeue in between).
        vec![activity_info(
            "slow_panic_activity",
            slow_panic_activity,
            false,
            Some(RetryPolicy::fixed(1, Duration::from_millis(10))),
        )],
        Arc::clone(&metrics),
    );
    let worker = build_worker("worker-slow-panic-activity", Arc::clone(&registry), 3);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // Tight-poll the activity task row. The handler holds RUNNING for ~120ms so
    // the ~5ms poll reliably samples it many times; `last_running` is therefore
    // within ~one poll of the actual panic, and `first_off` is the first sample
    // after the engine finalized the contained failure — so the measured delta
    // is the containment latency plus poll granularity, i.e. "~1 poll interval".
    let mut last_running: Option<tokio::time::Instant> = None;
    let mut first_off_after_running: Option<tokio::time::Instant> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let activity_task = load_tasks(&url, exec_id)
            .await
            .into_iter()
            .find(|t| t.activity_name.as_deref() == Some("slow_panic_activity"));
        if let Some(t) = activity_task {
            if t.state == "RUNNING" {
                last_running = Some(tokio::time::Instant::now());
            } else if last_running.is_some() && first_off_after_running.is_none() {
                first_off_after_running = Some(tokio::time::Instant::now());
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // The execution must reach FAILED (the worker process survived the panic).
    let execution = wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(20)).await;
    worker.shutdown();
    handle.await.expect("worker task joins cleanly");
    assert_eq!(execution.state, "FAILED");

    let last_running = last_running.expect(
        "the activity task must have been observed RUNNING — its handler sleeps ~120ms so the \
         ~5ms poll should sample it; a never-RUNNING task means the measurement is vacuous",
    );
    let first_off = first_off_after_running
        .expect("the activity task must transition OFF RUNNING (a wedged task never does)");

    // BOUND: the 25ms poll_interval times a generous multiple (40×). A contained
    // panic finalizes the row within a few DB-ms plus one poll of granularity, so
    // 1s is enormous headroom; a #782 wedge (RUNNING forever) makes this delta
    // unbounded, so the assertion falsifies exactly that failure mode while
    // tolerating CI scheduling jitter.
    let bound = Duration::from_secs(1);
    let transition = first_off.duration_since(last_running);
    assert!(
        transition < bound,
        "panicking activity task must leave RUNNING within {bound:?} of the panic \
         (measured {transition:?}); a larger value means the task wedged in RUNNING (#782)"
    );
}

// ---------------------------------------------------------------------------
// Test C (AC3 read-surface) — the panic message is on the task-row `error`
// column during retry backoff.
//
// This is the executable, data-layer proof for AC3's "surfaced via the stack
// API" clause without the Docker-only plugin HTTP harness. A backing-off
// (PENDING) regular activity does NOT append an `ActivityFailed` event; its last
// error lives in `harvest_task_queue.error` (written by `requeue_for_retry`) —
// the exact column `GET /workflows/{id}/stack`'s `PendingActivity.last_failure`
// (#773) reads. `requeue_for_retry` stores the unwrapped panic message (the
// typed `HandlerPanic` error_type rides the terminal `ActivityFailed` event once
// the run goes terminal — covered by the terminal test above), so during backoff
// the column carries the panic message.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backing_off_activity_task_error_column_carries_the_panic_message() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "panic_backoff_wf", serde_json::json!({"n": 6})).await;

    let metrics = Arc::new(PanicMetrics::default());
    let registry = build_registry(
        vec![workflow_info(
            "panic_backoff_wf",
            workflow_calls_panic_activity,
        )],
        // 3 attempts with a long 3s fixed backoff: attempt 1 panics → requeue
        // PENDING with a future scheduled_at, giving a wide window to observe the
        // backing-off task row before attempt 2 is claimed.
        vec![activity_info(
            "panic_activity",
            panic_activity,
            false,
            Some(RetryPolicy::fixed(3, Duration::from_secs(3))),
        )],
        Arc::clone(&metrics),
    );
    let worker = build_worker("worker-panic-backoff", Arc::clone(&registry), 3);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // Poll for the activity task in the backing-off state: PENDING, with a future
    // scheduled_at (the 3s backoff) and a populated `error` column.
    let mut observed_error: Option<String> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        let backing_off = load_tasks(&url, exec_id).await.into_iter().find(|t| {
            t.activity_name.as_deref() == Some("panic_activity")
                && t.state == "PENDING"
                && t.scheduled_at > Utc::now()
                && t.error.is_some()
        });
        if let Some(t) = backing_off {
            observed_error = t.error;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    worker.shutdown();
    handle.await.expect("worker task joins cleanly");

    // This test deliberately shuts the worker down mid-backoff, leaving a
    // claimable PENDING panic_activity task. Against a *shared* (persistent)
    // HARVEST_TEST_DATABASE_URL cluster that leftover would be re-claimed by a
    // later test's worker on the same default queue/shard and inflate its global
    // panic counter, so delete this execution's task rows for isolation. (Under
    // fresh testcontainers the whole DB is thrown away, so this is a no-op there.)
    let mut cleanup = connect(&url).await;
    diesel::delete(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid()))),
    )
    .execute(&mut cleanup)
    .await
    .expect("delete leftover backing-off task rows");

    let error = observed_error.expect(
        "a backing-off (PENDING, future scheduled_at) panic_activity task with a populated \
         error column must be observed during the 3s retry backoff",
    );
    assert!(
        error.contains("activity boom"),
        "the task-row error column (the data #773's PendingActivity.last_failure surfaces) must \
         carry the contained panic message during backoff, got {error:?}"
    );
}

// ---------------------------------------------------------------------------
// Test D (P2) — a child-workflow panic surfaces to the parent as a typed
// `ChildWorkflowFailed` carrying error_type = "HandlerPanic" (routed via the
// #767 typed-failure surface: child terminal `WorkflowFailed` → decode →
// parent `ChildWorkflowFailed`).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_workflow_panic_surfaces_to_parent_as_typed_handler_panic() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let parent_id = seed_workflow(
        &mut conn,
        "parent_panic_child_wf",
        serde_json::json!({"n": 7}),
    )
    .await;

    let metrics = Arc::new(PanicMetrics::default());
    let registry = build_registry(
        vec![
            workflow_info("parent_panic_child_wf", parent_spawns_panic_child),
            workflow_info("panic_child_wf", panic_workflow),
        ],
        vec![],
        Arc::clone(&metrics),
    );
    // Budget 1 → the child's first panic is terminal (fast, no re-dispatch).
    let worker = build_worker("worker-child-panic", Arc::clone(&registry), 1);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // The parent propagates the child error → parent reaches FAILED once the
    // child's panic has been contained, terminal-failed, and decoded upward.
    let parent = wait_for_state(&url, parent_id, "FAILED", Duration::from_secs(30)).await;

    worker.shutdown();
    handle.await.expect("worker task joins cleanly");
    assert_eq!(parent.state, "FAILED");

    // The parent observes the child failure as a typed ChildWorkflowFailed
    // carrying the engine-reserved HandlerPanic error_type (the #767 surface).
    let parent_history = load_history(&url, parent_id).await;
    let child_failures: Vec<_> = parent_history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ChildWorkflowFailed {
                error, error_type, ..
            } => Some((error.clone(), error_type.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        child_failures.len(),
        1,
        "the parent must record exactly one ChildWorkflowFailed; history={parent_history:?}"
    );
    let (message, error_type) = &child_failures[0];
    assert_eq!(
        error_type.as_deref(),
        Some(ERROR_TYPE_HANDLER_PANIC),
        "a contained child-workflow panic must surface to the parent as a typed HandlerPanic \
         (routed via decode_workflow_failure → child_workflow_failed_typed)"
    );
    assert!(
        message.contains("workflow boom"),
        "the child panic message must ride the typed ChildWorkflowFailed, got {message:?}"
    );

    // No poison-pill DLQ for the contained child panic.
    let (_total, poison_dlq) = count_dead_letters(&url, parent_id).await;
    assert_eq!(
        poison_dlq, 0,
        "no PoisonPill DLQ for a contained child panic"
    );
}
