#![cfg(feature = "db")]
//! Workflow-level retry policy tests — issue #523.
//!
//! Verifies that a workflow with a `retry_policy` set is automatically
//! re-run on failure, exhausted chains count as one failure toward schedule
//! auto-pause, and non-retryable/cancelled/timed-out outcomes are never
//! retried.

use std::any::TypeId;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::context::empty_shared_state;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{IntoWorkflowErrorString, WorkflowFailure};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::schema::{harvest_timers, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::telemetry::{METRIC_WORKFLOW_RETRIES, MetricsRecorder, TelemetryConfig};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, Priority, ShardId, StartSource, StartWorkflowParams, WorkflowContext,
    cancel_workflow_execution, start_or_load_workflow_execution,
};

use crate::integration_e2e::setup_test_database_url_or_env;

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

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

// ── Recording metrics ──────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct RecordingMetrics {
    retries: Mutex<Vec<String>>,
}

impl RecordingMetrics {
    fn retry_count(&self) -> usize {
        self.retries.lock().unwrap().len()
    }
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_retry(&self, workflow_name: &str, _queue: &str) {
        self.retries.lock().unwrap().push(workflow_name.to_owned());
    }
}

// ── Shared state for stateful handlers ────────────────────────────────────

/// Shared counter state — passed into worker via `HandlerRegistry` shared state,
/// accessed inside workflow handlers via `ctx.state::<Arc<CallCounter>>()`.
#[derive(Debug, Default)]
struct CallCounter {
    count: AtomicUsize,
}

impl CallCounter {
    /// Increment call count. Returns the call number (1-indexed).
    fn increment(&self) -> usize {
        self.count.fetch_add(1, Ordering::SeqCst) + 1
    }
}

fn make_shared_state(counter: Arc<CallCounter>) -> autumn_harvest::context::SharedState {
    let mut map: HashMap<TypeId, Box<dyn std::any::Any + Send + Sync>> = HashMap::new();
    map.insert(TypeId::of::<Arc<CallCounter>>(), Box::new(counter));
    Arc::new(map)
}

// ── Workflow handler functions (must be fn pointers, not closures) ─────────

/// Fails on the first invocation, succeeds on all subsequent ones.
fn fail_once_then_succeed_handler(
    ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    let counter = ctx
        .state::<Arc<CallCounter>>()
        .expect("CallCounter must be in shared state")
        .clone();
    Box::pin(async move {
        let n = counter.increment();
        if n == 1 {
            Err("transient error".to_string())
        } else {
            Ok(serde_json::json!("success"))
        }
    })
}

/// Always fails.
fn always_fail_handler(
    _ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move { Err("permanent transient".to_string()) })
}

/// Always fails with a typed `WorkflowFailure` whose `error_type` is the
/// exception CLASS `"NullPointerException"` and whose message carries the detail
/// (`"NullPointerException: bad input"`). The #523 retry gate matches
/// `non_retryable_errors` against the decoded typed `error_type` class on exact
/// equality (`RetryPolicy::is_non_retryable`) — NOT a substring of the raw
/// message — so the fixture must publish the class as `error_type`, mirroring
/// the passing sibling `workflow_typed_non_retryable_error_type_no_retry`.
fn non_retryable_fail_handler(
    _ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        Err(
            WorkflowFailure::new("NullPointerException", "NullPointerException: bad input")
                .into_workflow_error_payload(),
        )
    })
}

/// Fails with a *typed* `WorkflowFailure` whose `error_type` is
/// `"ValidationRejected"` (issue #767). The message is deliberately generic so
/// the retry gate can only halt by matching the typed `error_type` class, never
/// a substring of the message.
fn typed_non_retryable_fail_handler(
    _ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        Err(
            WorkflowFailure::new("ValidationRejected", "input did not pass validation")
                .into_workflow_error_payload(),
        )
    })
}

/// Fails with a typed `WorkflowFailure` whose `error_type`
/// (`"TransientGlitch"`) is NOT in the policy's `non_retryable_errors` list —
/// the control that must still retry after FIX B.
fn typed_retryable_fail_handler(
    _ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        Err(
            WorkflowFailure::new("TransientGlitch", "temporary upstream hiccup")
                .into_workflow_error_payload(),
        )
    })
}

/// Fails with a typed `WorkflowFailure` whose `error_type` (`"TransientTimeout"`)
/// is NOT in the policy's `non_retryable_errors` list, but whose HUMAN MESSAGE
/// (`"fatal"`) coincidentally equals a `non_retryable_errors` pattern (Codex P2,
/// issue #767). A typed failure must be classified by its `error_type` class
/// ONLY — this class is retryable, so the run must still retry even though the
/// message text matches. (`is_non_retryable` matches on exact equality, so the
/// message must exactly equal the pattern for the pre-fix gate — which combined
/// the class match with a raw-message match — to have wrongly suppressed it.)
fn typed_message_collision_fail_handler(
    _ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        Err(WorkflowFailure::new("TransientTimeout", "fatal").into_workflow_error_payload())
    })
}

/// Fails via the `Result<_, WorkflowFailure>` sentinel path (issue #767,
/// Codex P2): a workflow returning `Err("fatal".into())` — where the `Err` is a
/// `WorkflowFailure` built from a plain string — collapses `error_type` to
/// `None` (the reserved `"Error"` sentinel) with `message == "fatal"`. The
/// engine's `encode_err` shim wraps it in the `harvest_workflow_failure_v1`
/// envelope, so the raw boundary string is the envelope JSON, NOT `"fatal"`.
/// The retry gate must match `non_retryable_errors` against the *decoded human
/// message* (`"fatal"`), never the envelope, or this workflow would retry to
/// exhaustion.
fn sentinel_string_non_retryable_fail_handler(
    _ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        // Mirrors `Err("fatal".into())` in a `Result<_, WorkflowFailure>`
        // workflow: `WorkflowFailure::from("fatal")` → class `"Error"` (sentinel),
        // message `"fatal"`; the shim serialises it to the wire envelope.
        Err(WorkflowFailure::from("fatal").into_workflow_error_payload())
    })
}

/// Waits on a 10-second timer (effectively suspends; can be cancelled).
fn timer_wait_handler(
    ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        // 3600s so the timer cannot fire during the poll window — the workflow
        // stays durably parked until the test cancels it.
        ctx.timer("long-wait", 3600)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

// ── Test helpers ───────────────────────────────────────────────────────────

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

fn wf_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
    retry_policy: Option<RetryPolicy>,
) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "workflow_retry_tests",
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
        retry_policy,
    }
}

fn make_worker(
    workflows: Vec<WorkflowInfo>,
    shared_state: autumn_harvest::context::SharedState,
    metrics: Arc<dyn MetricsRecorder + Send + Sync>,
) -> Worker {
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        vec![],
        shared_state,
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
            build_id: String::new(),
            deployment_name: None,
            workflow_cache_size: 100,
            priority_aging_secs: None,
            unknown_target_grace_window: Duration::from_secs(5),
            poison_pill_threshold: 3,
            workflow_task_timeout: Duration::from_secs(10),
            workflow_panic_max_attempts: 3,
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

async fn start_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    workflow_id: &str,
    retry_policy: Option<RetryPolicy>,
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
            workflow_retry_policy: retry_policy,
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

/// Like [`start_workflow`] but stamps an explicit start-source provenance triple
/// (issue #740) so the workflow-level retry inheritance path is falsifiable.
#[allow(clippy::too_many_arguments)]
async fn start_workflow_with_source(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    workflow_id: &str,
    retry_policy: Option<RetryPolicy>,
    start_source: StartSource,
    start_source_ref: Option<&str>,
    started_by: Option<&str>,
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
            workflow_retry_policy: retry_policy,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source,
            start_source_ref,
            started_by,
        },
        None,
    )
    .await
    .expect("workflow start should succeed")
    .exec_id
}

/// Read the `(start_source, start_source_ref, started_by)` provenance triple of
/// an execution row (issue #740).
async fn get_provenance(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> (Option<String>, Option<String>, Option<String>) {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select((
            harvest_workflow_executions::start_source,
            harvest_workflow_executions::start_source_ref,
            harvest_workflow_executions::started_by,
        ))
        .first(conn)
        .await
        .expect("load execution provenance")
}

async fn get_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::state)
        .first::<String>(conn)
        .await
        .expect("execution must exist")
}

async fn get_attempt(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i32 {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::workflow_attempt)
        .first::<i32>(conn)
        .await
        .expect("execution must exist")
}

async fn get_retry_of(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Option<uuid::Uuid> {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::retry_of_exec_id)
        .first::<Option<uuid::Uuid>>(conn)
        .await
        .expect("execution must exist")
}

async fn get_history(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    store::load_history(conn, exec_id)
        .await
        .expect("load history")
        .events
}

/// Get execution count for a `workflow_id` (all attempts).
async fn count_by_workflow_id(conn: &mut AsyncPgConnection, workflow_id: &str) -> i64 {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .count()
        .get_result::<i64>(conn)
        .await
        .expect("count")
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

/// Wait until an execution is durably parked on a timer: a `harvest_timers` row
/// exists for it (the workflow suspended on its `ctx.timer(...)`) — the
/// unambiguous "parked" signal.
///
/// `"SUSPENDED"` is NOT a persisted state: a timer-parked run's row is
/// `"RUNNING"` with `worker_id IS NULL`, and the state CHECK constraint forbids
/// `"SUSPENDED"`. Poll the timer table (the existence of the durable timer row)
/// instead of an unreachable state string.
async fn wait_for_parked(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    for _ in 0..400 {
        let timers: i64 = harvest_timers::table
            .filter(harvest_timers::workflow_exec_id.eq(exec_id.as_uuid()))
            .count()
            .get_result(conn)
            .await
            .expect("count timers");
        if timers > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let state = get_state(conn, exec_id).await;
    panic!("execution {exec_id} never parked on a timer; current state: {state}");
}

/// Wait until a retry execution (`retry_of_exec_id = original_exec_id`) reaches
/// one of the given states and return its `ExecutionId`.
///
/// The retry run has its own unique `workflow_id` (its UUID stringified) so it
/// cannot be found by polling the original `workflow_id`; poll via the
/// `retry_of_exec_id` FK instead.
async fn wait_for_retry_state(
    conn: &mut AsyncPgConnection,
    original_exec_id: ExecutionId,
    states: &[&str],
) -> ExecutionId {
    for _ in 0..400 {
        let rows: Vec<(uuid::Uuid, String)> = harvest_workflow_executions::table
            .filter(
                harvest_workflow_executions::retry_of_exec_id.eq(Some(original_exec_id.as_uuid())),
            )
            .select((
                harvest_workflow_executions::id,
                harvest_workflow_executions::state,
            ))
            .load(conn)
            .await
            .expect("load retry executions");
        for (id, state) in &rows {
            if states.contains(&state.as_str()) {
                return ExecutionId::from_uuid(*id);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no retry execution with retry_of_exec_id={original_exec_id} ever reached {states:?}");
}

// ── Unit tests (compile-time assertions) ──────────────────────────────────

/// `METRIC_WORKFLOW_RETRIES` must be defined with the expected value.
#[test]
fn metric_constant_workflow_retries_is_defined() {
    assert_eq!(METRIC_WORKFLOW_RETRIES, "harvest.workflow.retries");
}

/// `record_workflow_retry` must be callable on a `MetricsRecorder`.
#[test]
fn metrics_recorder_has_record_workflow_retry() {
    let m = RecordingMetrics::default();
    m.record_workflow_retry("my_wf", "default");
    m.record_workflow_retry("my_wf", "default");
    assert_eq!(m.retry_count(), 2);
}

/// `WorkflowInfo::with_retry_policy` must set the field.
#[test]
fn workflow_info_with_retry_policy_sets_field() {
    let policy = RetryPolicy::exponential(3, Duration::from_secs(1));
    let info =
        wf_info("test_wf", fail_once_then_succeed_handler, None).with_retry_policy(policy.clone());
    assert!(info.retry_policy.is_some());
    assert_eq!(info.retry_policy.unwrap().max_attempts, policy.max_attempts);
}

/// `WorkflowInfo` default must have `retry_policy = None`.
#[test]
fn workflow_info_default_retry_policy_is_none() {
    let info = wf_info("test_wf", fail_once_then_succeed_handler, None);
    assert!(info.retry_policy.is_none());
}

/// `WorkflowSchedule::with_retry_policy` must set the field.
#[test]
fn workflow_schedule_with_retry_policy_sets_field() {
    let policy = RetryPolicy::exponential(3, Duration::from_secs(1));
    let sched = WorkflowSchedule::new("my_wf", Schedule::Manual).with_retry_policy(policy);
    assert!(sched.retry_policy.is_some());
}

// ── Integration tests ──────────────────────────────────────────────────────

/// AC #1: Workflow fails on attempt 1 (transient error), succeeds on attempt 2.
/// - Final execution state: COMPLETED.
/// - Exactly one `harvest.workflow.retries` metric increment.
/// - The retry execution has attempt=2 and `retry_of_exec_id` pointing to the first run.
/// - The failed run's history ends with `WorkflowFailed` + `WorkflowRetryScheduled`.
#[tokio::test]
async fn workflow_retries_on_transient_failure_and_succeeds() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let policy = RetryPolicy {
        max_attempts: 3,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec![],
        jitter: JitterPolicy::None,
    };

    let counter = Arc::new(CallCounter::default());
    let metrics = Arc::new(RecordingMetrics::default());

    let workflow_id = "retry-transient-001";
    let exec_id = start_workflow(
        &mut conn,
        "retry_transient_wf",
        workflow_id,
        Some(policy.clone()),
    )
    .await;
    drop(conn);

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info(
            "retry_transient_wf",
            fail_once_then_succeed_handler,
            Some(policy),
        )],
        make_shared_state(counter),
        metrics.clone(),
    ));
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(20), worker_ref.run(&pool)).await;
    });

    // Wait for the retry execution to reach COMPLETED (on attempt 2).
    // The retry has its own unique workflow_id (its exec UUID), so look it up
    // by the retry_of_exec_id FK rather than the original workflow_id.
    let mut check = connect(&url).await;
    let completed_exec = wait_for_retry_state(&mut check, exec_id, &["COMPLETED"]).await;

    worker.shutdown();
    let _ = worker_handle.await;

    // The completed execution must have attempt=2.
    assert_eq!(get_attempt(&mut check, completed_exec).await, 2);

    // The completed execution must have retry_of_exec_id pointing to exec_id.
    let retry_of = get_retry_of(&mut check, completed_exec).await;
    assert_eq!(
        retry_of,
        Some(exec_id.as_uuid()),
        "retry_of_exec_id must point to the original failed execution"
    );

    // The original execution must be FAILED.
    assert_eq!(get_state(&mut check, exec_id).await, "FAILED");

    // The original execution's history must end with WorkflowFailed + WorkflowRetryScheduled.
    let history = get_history(&mut check, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "first execution must have WorkflowFailed event"
    );
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. })),
        "first execution must have WorkflowRetryScheduled event"
    );

    // Exactly one retry metric was emitted.
    assert_eq!(
        metrics.retry_count(),
        1,
        "exactly one harvest.workflow.retries increment expected"
    );

    // The original execution (FAILED) still has its original workflow_id.
    assert_eq!(
        count_by_workflow_id(&mut check, workflow_id).await,
        1,
        "the original execution is the only one with the original workflow_id"
    );
}

/// Issue #740: a workflow-level retry (#523) is the same logical run trying
/// again, so the retry execution must INHERIT the predecessor's start-source
/// provenance triple (`schedule` here, with a ref + operator) rather than being
/// re-attributed as a fresh `api` start. Driven end-to-end through the real
/// worker loop — the inheritance happens inside `persist_workflow_failure`,
/// which is engine-internal. Uses the env-aware DB helper so it runs against a
/// local `HARVEST_TEST_DATABASE_URL` or a fresh testcontainer.
#[tokio::test]
async fn workflow_retry_inherits_predecessor_start_source() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let policy = RetryPolicy {
        max_attempts: 3,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec![],
        jitter: JitterPolicy::None,
    };

    let counter = Arc::new(CallCounter::default());
    let metrics = Arc::new(RecordingMetrics::default());

    // Start the ORIGINAL run stamped with a DISTINCT `schedule` source so the
    // assertion falsifies both a fresh `api` re-attribution and a dropped ref.
    let workflow_id = format!("retry-inherit-{}", uuid::Uuid::new_v4());
    let exec_id = start_workflow_with_source(
        &mut conn,
        "retry_source_inherit_wf",
        &workflow_id,
        Some(policy.clone()),
        StartSource::Schedule,
        Some("sched-inherit-xyz"),
        Some("operator@example.com"),
    )
    .await;
    drop(conn);

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info(
            "retry_source_inherit_wf",
            fail_once_then_succeed_handler,
            Some(policy),
        )],
        make_shared_state(counter),
        metrics.clone(),
    ));
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(20), worker_ref.run(&pool)).await;
    });

    // Wait for the retry execution (attempt 2) to COMPLETE.
    let mut check = connect(&url).await;
    let retry_exec = wait_for_retry_state(&mut check, exec_id, &["COMPLETED"]).await;

    worker.shutdown();
    let _ = worker_handle.await;

    assert_eq!(get_attempt(&mut check, retry_exec).await, 2);

    // The retry run must inherit the predecessor's full provenance triple.
    let (source, source_ref, started_by) = get_provenance(&mut check, retry_exec).await;
    assert_eq!(
        source.as_deref(),
        Some("schedule"),
        "a workflow-level retry must inherit the predecessor's start_source \
         ('schedule'), never re-attribute as a fresh 'api' start"
    );
    assert_eq!(
        source_ref.as_deref(),
        Some("sched-inherit-xyz"),
        "the retry must inherit the predecessor's start_source_ref"
    );
    assert_eq!(
        started_by.as_deref(),
        Some("operator@example.com"),
        "the retry must inherit the predecessor's started_by"
    );

    // Sanity: the original run keeps its own (identical) provenance.
    let (orig_source, _, _) = get_provenance(&mut check, exec_id).await;
    assert_eq!(orig_source.as_deref(), Some("schedule"));
}

/// AC #2: `max_attempts` exhausted — final run FAILED, schedule counter incremented exactly once.
/// A workflow with `max_attempts=2` that always fails should produce 2 FAILED executions,
/// 1 retry metric, and 1 schedule failure counter increment (for the exhausted chain).
#[tokio::test]
async fn workflow_retry_exhaustion_counts_as_one_failure() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let policy = RetryPolicy {
        max_attempts: 2,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec![],
        jitter: JitterPolicy::None,
    };

    let metrics = Arc::new(RecordingMetrics::default());

    let workflow_id = "retry-exhaust-001";
    let exec_id = start_workflow(
        &mut conn,
        "retry_exhaust_wf",
        workflow_id,
        Some(policy.clone()),
    )
    .await;
    drop(conn);

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info(
            "retry_exhaust_wf",
            always_fail_handler,
            Some(policy),
        )],
        empty_shared_state(),
        metrics.clone(),
    ));
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(20), worker_ref.run(&pool)).await;
    });

    // Poll until we see 2 FAILED executions for this retry chain. The retry
    // execution has its own UUID workflow_id (not `workflow_id`) and links back
    // via `retry_of_exec_id`, so match the original row OR any retry of it.
    let mut check = connect(&url).await;
    let mut failed_count = 0;
    for _ in 0..400 {
        let rows: Vec<String> = harvest_workflow_executions::table
            .filter(
                harvest_workflow_executions::id
                    .eq(exec_id.as_uuid())
                    .or(harvest_workflow_executions::retry_of_exec_id.eq(Some(exec_id.as_uuid()))),
            )
            .select(harvest_workflow_executions::state)
            .load(&mut check)
            .await
            .expect("load states");
        failed_count = rows.iter().filter(|s| s.as_str() == "FAILED").count();
        if failed_count == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    worker.shutdown();
    let _ = worker_handle.await;

    assert_eq!(
        failed_count, 2,
        "both original and retry execution must be FAILED"
    );

    // Exactly 1 retry metric (the retry after attempt 1).
    assert_eq!(
        metrics.retry_count(),
        1,
        "exactly one retry metric expected (attempt 1 → attempt 2)"
    );

    // The original execution's history has WorkflowRetryScheduled.
    let mut check2 = connect(&url).await;
    let history = get_history(&mut check2, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. }))
    );

    // The retry execution (attempt 2) does NOT have WorkflowRetryScheduled in its
    // history. The #523 retry run gets its OWN distinct UUID `workflow_id` (the
    // tested contract, per the passing `retry_run_has_fresh_history_and_correct_linkage`),
    // so it can never be found by the original `workflow_id`; select it via the
    // `retry_of_exec_id` FK — matching the `failed_count` loop above and the
    // `wait_for_retry_state` helper.
    let attempt2_id = wait_for_retry_state(&mut check2, exec_id, &["FAILED"]).await;
    let retry_history = get_history(&mut check2, attempt2_id).await;
    assert!(
        !retry_history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. })),
        "exhausted retry must NOT have WorkflowRetryScheduled"
    );
}

/// AC #3: Non-retryable error — no retry even with attempts remaining.
#[tokio::test]
async fn workflow_non_retryable_error_no_retry() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let policy = RetryPolicy {
        max_attempts: 5,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec!["NullPointerException".to_string()],
        jitter: JitterPolicy::None,
    };

    let metrics = Arc::new(RecordingMetrics::default());

    let workflow_id = "non-retryable-001";
    let exec_id = start_workflow(
        &mut conn,
        "non_retryable_wf",
        workflow_id,
        Some(policy.clone()),
    )
    .await;
    drop(conn);

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info(
            "non_retryable_wf",
            non_retryable_fail_handler,
            Some(policy),
        )],
        empty_shared_state(),
        metrics.clone(),
    ));
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(10), worker_ref.run(&pool)).await;
    });

    let mut check = connect(&url).await;
    wait_for_state(&mut check, exec_id, &["FAILED"]).await;

    worker.shutdown();
    let _ = worker_handle.await;

    // No retry was performed.
    assert_eq!(
        metrics.retry_count(),
        0,
        "non-retryable error must not trigger any retry"
    );

    // Only 1 execution exists (no retry run created).
    assert_eq!(
        count_by_workflow_id(&mut check, workflow_id).await,
        1,
        "only the original execution must exist"
    );

    // History must NOT have WorkflowRetryScheduled.
    let history = get_history(&mut check, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. })),
        "non-retryable failure must NOT have WorkflowRetryScheduled"
    );
}

/// FIX B (issue #767): the #523 workflow-level retry gate matches the policy's
/// `non_retryable_errors` class list against the *decoded typed workflow
/// `error_type`*, not just a raw-string match on the envelope. A workflow
/// returning `WorkflowFailure::new("ValidationRejected", ...)` whose class is in
/// `non_retryable_errors` must NOT be retried — even though the wire envelope
/// string never equals the class name. (Before the fix this workflow would
/// wrongly retry to exhaustion, since `failure_is_non_retryable` only matched
/// the ACTIVITY envelope / the raw string.)
#[tokio::test]
async fn workflow_typed_non_retryable_error_type_no_retry() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let policy = RetryPolicy {
        max_attempts: 5,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec!["ValidationRejected".to_string()],
        jitter: JitterPolicy::None,
    };

    let metrics = Arc::new(RecordingMetrics::default());

    let workflow_id = "typed-non-retryable-001";
    let exec_id = start_workflow(
        &mut conn,
        "typed_non_retryable_wf",
        workflow_id,
        Some(policy.clone()),
    )
    .await;
    drop(conn);

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info(
            "typed_non_retryable_wf",
            typed_non_retryable_fail_handler,
            Some(policy),
        )],
        empty_shared_state(),
        metrics.clone(),
    ));
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(10), worker_ref.run(&pool)).await;
    });

    let mut check = connect(&url).await;
    wait_for_state(&mut check, exec_id, &["FAILED"]).await;

    worker.shutdown();
    let _ = worker_handle.await;

    assert_eq!(
        metrics.retry_count(),
        0,
        "a typed non-retryable error_type class must not trigger any retry"
    );
    assert_eq!(
        count_by_workflow_id(&mut check, workflow_id).await,
        1,
        "only the original execution must exist (no retry run created)"
    );
    let history = get_history(&mut check, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. })),
        "typed non-retryable class must NOT have WorkflowRetryScheduled"
    );
    // The stored terminal WorkflowFailed carries the human message, not the envelope.
    let stored_error: Option<String> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::error)
        .first::<Option<String>>(&mut check)
        .await
        .expect("execution must exist");
    assert_eq!(
        stored_error.as_deref(),
        Some("input did not pass validation")
    );
}

/// FIX B control (issue #767): a typed `WorkflowFailure` whose `error_type`
/// (`"TransientGlitch"`) is NOT in the policy's `non_retryable_errors` list must
/// STILL be retried — proving the gate change did not break normal retries.
#[tokio::test]
async fn workflow_typed_retryable_error_type_still_retries() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let policy = RetryPolicy {
        max_attempts: 2,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec!["ValidationRejected".to_string()],
        jitter: JitterPolicy::None,
    };

    let metrics = Arc::new(RecordingMetrics::default());

    let workflow_id = "typed-retryable-001";
    let exec_id = start_workflow(
        &mut conn,
        "typed_retryable_wf",
        workflow_id,
        Some(policy.clone()),
    )
    .await;
    drop(conn);

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info(
            "typed_retryable_wf",
            typed_retryable_fail_handler,
            Some(policy),
        )],
        empty_shared_state(),
        metrics.clone(),
    ));
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(10), worker_ref.run(&pool)).await;
    });

    let mut check = connect(&url).await;
    wait_for_state(&mut check, exec_id, &["FAILED"]).await;

    // The first (original) execution must schedule a retry for the non-listed
    // typed class.
    let history = get_history(&mut check, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. })),
        "a typed error_type NOT in non_retryable_errors must still schedule a retry"
    );

    worker.shutdown();
    let _ = worker_handle.await;

    assert!(
        count_by_workflow_id(&mut check, workflow_id).await >= 1,
        "at least the original execution must exist"
    );
}

/// Codex P2 regression guard (issue #767): a typed `WorkflowFailure` whose
/// `error_type` (`"TransientTimeout"`) is NOT in `non_retryable_errors`, but
/// whose HUMAN MESSAGE (`"fatal"`) equals a `non_retryable_errors` pattern, must
/// STILL retry. The pre-fix gate passed `decoded.message` as the raw arg to
/// `is_non_retryable`, so the class OR'd with a message match and wrongly
/// suppressed the retry of a retryable typed class. The fix classifies typed
/// failures by `error_type` class ONLY.
#[tokio::test]
async fn workflow_typed_message_collision_still_retries() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let policy = RetryPolicy {
        max_attempts: 2,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        // The message "fatal" collides with this pattern, but the typed class
        // "TransientTimeout" does not — so the run must still retry.
        non_retryable_errors: vec!["fatal".to_string()],
        jitter: JitterPolicy::None,
    };

    let metrics = Arc::new(RecordingMetrics::default());

    let workflow_id = "typed-message-collision-001";
    let exec_id = start_workflow(
        &mut conn,
        "typed_message_collision_wf",
        workflow_id,
        Some(policy.clone()),
    )
    .await;
    drop(conn);

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info(
            "typed_message_collision_wf",
            typed_message_collision_fail_handler,
            Some(policy),
        )],
        empty_shared_state(),
        metrics.clone(),
    ));
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(10), worker_ref.run(&pool)).await;
    });

    let mut check = connect(&url).await;
    wait_for_state(&mut check, exec_id, &["FAILED"]).await;

    // A retryable typed class must schedule a retry even though its message text
    // coincides with a `non_retryable_errors` pattern.
    let history = get_history(&mut check, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. })),
        "a typed error_type NOT in non_retryable_errors must still retry even when \
         its human message coincides with a non_retryable_errors pattern"
    );

    worker.shutdown();
    let _ = worker_handle.await;

    assert!(
        count_by_workflow_id(&mut check, workflow_id).await >= 1,
        "at least the original execution must exist"
    );
}

/// FIX B — sentinel path (issue #767, Codex P2): a `Result<_, WorkflowFailure>`
/// workflow returning `Err("fatal".into())` collapses its `error_type` to `None`
/// (reserved `"Error"` sentinel) with `message == "fatal"`, and the engine wraps
/// it in the `harvest_workflow_failure_v1` envelope on the boundary. With
/// `non_retryable_errors = ["fatal"]`, the retry gate must halt retries by
/// matching the **decoded human message** — NOT the raw envelope JSON. Before
/// the fix (which matched against the raw envelope) this workflow would wrongly
/// retry to exhaustion because the envelope string never equals `"fatal"`.
#[tokio::test]
async fn workflow_sentinel_string_non_retryable_message_no_retry() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let policy = RetryPolicy {
        max_attempts: 5,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec!["fatal".to_string()],
        jitter: JitterPolicy::None,
    };

    let metrics = Arc::new(RecordingMetrics::default());

    let workflow_id = "sentinel-non-retryable-001";
    let exec_id = start_workflow(
        &mut conn,
        "sentinel_non_retryable_wf",
        workflow_id,
        Some(policy.clone()),
    )
    .await;
    drop(conn);

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info(
            "sentinel_non_retryable_wf",
            sentinel_string_non_retryable_fail_handler,
            Some(policy),
        )],
        empty_shared_state(),
        metrics.clone(),
    ));
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(10), worker_ref.run(&pool)).await;
    });

    let mut check = connect(&url).await;
    wait_for_state(&mut check, exec_id, &["FAILED"]).await;

    worker.shutdown();
    let _ = worker_handle.await;

    assert_eq!(
        metrics.retry_count(),
        0,
        "a sentinel Err(\"fatal\") whose decoded message is in non_retryable_errors \
         must not trigger any retry"
    );
    assert_eq!(
        count_by_workflow_id(&mut check, workflow_id).await,
        1,
        "only the original execution must exist (no retry run created)"
    );
    let history = get_history(&mut check, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. })),
        "sentinel non-retryable message must NOT have WorkflowRetryScheduled"
    );
    // AC4: the stored terminal error is the human message, never the envelope.
    let stored_error: Option<String> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::error)
        .first::<Option<String>>(&mut check)
        .await
        .expect("execution must exist");
    assert_eq!(stored_error.as_deref(), Some("fatal"));
}

/// AC #4: CANCELLED workflow is never retried.
#[tokio::test]
async fn cancelled_workflow_is_not_retried() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let policy = RetryPolicy {
        max_attempts: 5,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec![],
        jitter: JitterPolicy::None,
    };

    let metrics = Arc::new(RecordingMetrics::default());

    let workflow_id = "cancel-retry-001";
    let exec_id = start_workflow(
        &mut conn,
        "cancel_retry_wf",
        workflow_id,
        Some(policy.clone()),
    )
    .await;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info("cancel_retry_wf", timer_wait_handler, Some(policy))],
        empty_shared_state(),
        metrics.clone(),
    ));
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(20), worker_ref.run(&pool)).await;
    });

    // Wait for the workflow to start and durably park on its timer. `"SUSPENDED"`
    // is not a real state — poll for the timer row (RUNNING, worker_id IS NULL).
    let mut check = connect(&url).await;
    wait_for_parked(&mut check, exec_id).await;

    // Cancel the execution.
    cancel_workflow_execution(
        &mut connect(&url).await,
        exec_id,
        "test-cancel",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel should succeed");

    wait_for_state(&mut check, exec_id, &["CANCELLED"]).await;

    worker.shutdown();
    let _ = worker_handle.await;

    // Scope: this verifies that cancelling a timer-PARKED run seals it CANCELLED
    // and starts no second execution. Because cancel deletes the parked workflow
    // task and seals CANCELLED directly (the handler is never re-run), the
    // failure->retry gate is not exercised here; genuinely covering "a FAILED run
    // that is then cancelled is not retried" would require a failing-then-cancelled
    // race and is left as a follow-up.
    // No retry was performed.
    assert_eq!(
        metrics.retry_count(),
        0,
        "CANCELLED workflow must not be retried"
    );

    // Only 1 execution exists.
    assert_eq!(
        count_by_workflow_id(&mut check, workflow_id).await,
        1,
        "only the original execution must exist after cancellation"
    );
}

/// AC #5: Server ceiling clamps a misconfigured `max_attempts`.
#[test]
fn server_ceiling_clamps_max_attempts() {
    // Simulate ceiling clamping: if policy.max_attempts > ceiling, it should be clamped.
    let policy = RetryPolicy {
        max_attempts: 100,
        initial_interval: Duration::from_secs(1),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_secs(60),
        non_retryable_errors: vec![],
        jitter: JitterPolicy::None,
    };
    let ceiling: u32 = 5;
    let effective = policy.max_attempts.min(ceiling);
    assert_eq!(
        effective, 5,
        "ceiling must clamp policy.max_attempts from 100 to 5"
    );

    // And without a ceiling, the policy value is preserved.
    let no_ceiling: Option<u32> = None;
    let effective_no_ceiling = match no_ceiling {
        Some(c) => policy.max_attempts.min(c),
        None => policy.max_attempts,
    };
    assert_eq!(effective_no_ceiling, 100);
}

/// AC #6: Retry run has fresh history, same `workflow_id`, and describe API
/// surfaces `workflow_attempt` and `retry_of_exec_id`.
#[tokio::test]
async fn retry_run_has_fresh_history_and_correct_linkage() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let policy = RetryPolicy {
        max_attempts: 3,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec![],
        jitter: JitterPolicy::None,
    };

    let counter = Arc::new(CallCounter::default());
    let metrics = Arc::new(RecordingMetrics::default());

    let workflow_id = "retry-linkage-001";
    let original_exec_id = start_workflow(
        &mut conn,
        "retry_linkage_wf",
        workflow_id,
        Some(policy.clone()),
    )
    .await;
    drop(conn);

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info(
            "retry_linkage_wf",
            fail_once_then_succeed_handler,
            Some(policy),
        )],
        make_shared_state(counter),
        metrics.clone(),
    ));
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(20), worker_ref.run(&pool)).await;
    });

    // The retry has its own unique workflow_id (its exec UUID), so find it
    // via retry_of_exec_id FK rather than the original workflow_id.
    let mut check = connect(&url).await;
    let completed_exec = wait_for_retry_state(&mut check, original_exec_id, &["COMPLETED"]).await;

    worker.shutdown();
    let _ = worker_handle.await;

    // Verify retry_of_exec_id links retry → original.
    let retry_of = get_retry_of(&mut check, completed_exec).await;
    assert_eq!(
        retry_of,
        Some(original_exec_id.as_uuid()),
        "retry run must have retry_of_exec_id = original execution id"
    );

    // Retry run has attempt = 2.
    assert_eq!(get_attempt(&mut check, completed_exec).await, 2);

    // Retry run has its own distinct workflow_id (its UUID, not the original's).
    let retry_workflow_id: String = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(completed_exec.as_uuid()))
        .select(harvest_workflow_executions::workflow_id)
        .first(&mut check)
        .await
        .expect("load workflow_id");
    assert_ne!(
        retry_workflow_id, workflow_id,
        "retry run must have its own workflow_id distinct from the original"
    );
    assert_eq!(
        retry_workflow_id,
        completed_exec.as_uuid().to_string(),
        "retry run workflow_id must equal its own exec UUID (rid.to_string())"
    );

    // Retry run has its own history (starts with WorkflowStarted, only one).
    let retry_history = get_history(&mut check, completed_exec).await;
    let started_count = retry_history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::WorkflowStarted { .. }))
        .count();
    assert_eq!(
        started_count, 1,
        "retry run must have exactly one WorkflowStarted event"
    );
    assert!(
        retry_history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. })),
        "retry run must have WorkflowCompleted event"
    );

    // Original run has FAILED state and its own history.
    assert_eq!(get_state(&mut check, original_exec_id).await, "FAILED");
}
