#![cfg(feature = "db")]
//! Sticky routing is on by default (issue #1798).
//!
//! These tests use `WorkerConfig::default()` and set no sticky option. They
//! prove three things:
//!
//! 1. The next decision of a suspended execution runs on the same worker.
//!    That worker reports a cache hit.
//! 2. When that worker dies, a peer runs the next decision after the
//!    sticky window closes.
//! 3. A graceful shutdown releases the pins of the worker at once.
//! 4. A draining worker releases its pins before the drain, so a wake
//!    during the drain does not wait for the sticky window.
//! 5. A warm decision keeps the update results of an earlier decision.
//! 6. A warm decision resumes the resident workflow. The body does not run
//!    from the top again (issue #1798, step 2).
//! 7. A delta that the resident path cannot read falls back to a cold
//!    replay, and the run still completes.
//! 8. With sticky routing off, every decision replays from the top.
//! 9. With resident workflows off, the cache still hits, but every decision
//!    replays from the top.
//! 10. LRU eviction drops the resident workflow. The next decision is a miss
//!     and replays cold.
//!
//! A queue-level test also proves which rows the shutdown release touches.
//! Each test uses its own queue and worker ids, so the tests can share one
//! database.

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::store;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpPropagator, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, UpdateId};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, HarvestBuilder, StartWorkflowParams, StickyRoutingConfig, WorkerConfig,
    WorkflowContext, start_or_load_workflow_execution,
};
use diesel::sql_types::{Nullable, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url_or_env, wait_for_execution_state_with_timeout,
};

const WORKFLOW: &str = "sticky_default_wf";

/// Counts cache hits and misses for one worker.
#[derive(Debug, Default)]
struct CacheCounts {
    hits: AtomicU64,
    misses: AtomicU64,
}

impl CacheCounts {
    fn hits(&self) -> u64 {
        AtomicU64::load(&self.hits, Ordering::SeqCst)
    }

    fn misses(&self) -> u64 {
        AtomicU64::load(&self.misses, Ordering::SeqCst)
    }

    fn decisions(&self) -> u64 {
        self.hits() + self.misses()
    }
}

impl MetricsRecorder for CacheCounts {
    fn record_workflow_cache_hit(&self, _workflow_name: &str, _queue: &str) {
        self.hits.fetch_add(1, Ordering::SeqCst);
    }

    fn record_workflow_cache_miss(&self, _workflow_name: &str, _queue: &str) {
        self.misses.fetch_add(1, Ordering::SeqCst);
    }
}

/// Waits for two signals, then completes.
fn two_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let first: serde_json::Value = ctx
            .receive_signal("first")
            .await
            .map_err(|e| e.to_string())?;
        let second: serde_json::Value = ctx
            .receive_signal("second")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!([first, second]))
    })
}

/// Live runs of the `bump` update handler. Replay does not run it.
static UPDATE_RUNS: AtomicU64 = AtomicU64::new(0);

/// Fixed id of the update that the test admits before the first decision.
const UPDATE_UUID: uuid::Uuid = uuid::Uuid::from_u128(0x1798_0000_0000_4000_8000_0000_0000_0001);

const UPDATE_WORKFLOW: &str = "sticky_default_update_wf";

/// Completes an admitted update, signals another execution, then waits.
///
/// The update result and the external signal leave in one command batch.
/// The worker resolves the signal inline and runs the workflow again in
/// the same task. A later signal wakes it for a warm decision.
fn update_then_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.register_update_handler_no_validator("bump", |input: serde_json::Value| async move {
            UPDATE_RUNS.fetch_add(1, Ordering::SeqCst);
            Ok::<serde_json::Value, String>(input)
        });
        let _ = ctx
            .execute_admitted_update(
                UpdateId::from_uuid(UPDATE_UUID),
                "bump",
                serde_json::json!({}),
            )
            .await;
        let target = input["target"]
            .as_str()
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .ok_or("missing target")?;
        ctx.signal_external_workflow(ExecutionId::from_uuid(target), "first", "from-peer")
            .await
            .map_err(|e| e.to_string())?;
        let done: serde_json::Value = ctx
            .receive_signal("done")
            .await
            .map_err(|e| e.to_string())?;
        Ok(done)
    })
}

const SLOW_WORKFLOW: &str = "sticky_default_slow_wf";
const SLOW_ACTIVITY: &str = "sticky_default_slow_la";

/// How long the slow local activity holds its decision in flight.
const SLOW_ACTIVITY_TIME: Duration = Duration::from_secs(4);

/// Runs a slow local activity after the `go` signal, then waits for `done`.
///
/// The local activity runs inside the decision, so the decision stays in
/// flight while it sleeps. That keeps a shutdown in its drain phase.
fn slow_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _: serde_json::Value = ctx.receive_signal("go").await.map_err(|e| e.to_string())?;
        ctx.execute_local_activity_raw(SLOW_ACTIVITY, serde_json::json!({}), None, Some(30))
            .await
            .map_err(|e| e.to_string())?;
        let done: serde_json::Value = ctx
            .receive_signal("done")
            .await
            .map_err(|e| e.to_string())?;
        Ok(done)
    })
}

fn slow_activity<'a>(
    _ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        tokio::time::sleep(SLOW_ACTIVITY_TIME).await;
        Ok(input)
    })
}

fn slow_activity_info() -> ActivityInfo {
    ActivityInfo {
        name: SLOW_ACTIVITY,
        module: "sticky_default_tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: true,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: slow_activity,
    }
}

const RESIDENT_WORKFLOW: &str = "sticky_default_resident_wf";
const ECHO_ACTIVITY: &str = "sticky_default_echo";

/// Body starts of `resident_workflow`. Only one test runs it.
static RESIDENT_BODY_STARTS: AtomicU64 = AtomicU64::new(0);

/// Runs one activity, then waits for two signals.
///
/// Each await ends one decision. A resident worker runs the body from the
/// top only once.
fn resident_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        RESIDENT_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
        let queue = input["queue"].as_str().ok_or("missing queue")?;
        let echo = ctx
            .execute_activity_raw(ECHO_ACTIVITY, serde_json::json!({ "step": 1 }), queue)
            .await
            .map_err(|e| e.to_string())?;
        let one: serde_json::Value = ctx.receive_signal("one").await.map_err(|e| e.to_string())?;
        let two: serde_json::Value = ctx.receive_signal("two").await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!([echo, one, two]))
    })
}

const COUNTED_WORKFLOW: &str = "sticky_default_counted_wf";

/// Body starts of `counted_workflow`. Only one test runs it.
static COUNTED_BODY_STARTS: AtomicU64 = AtomicU64::new(0);

/// `two_signal_workflow` with a body-start counter.
fn counted_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    COUNTED_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
    two_signal_workflow(ctx, input)
}

const COLD_WORKFLOW: &str = "sticky_default_cold_wf";

/// Body starts of `cold_workflow`. Only one test runs it.
static COLD_BODY_STARTS: AtomicU64 = AtomicU64::new(0);

/// `two_signal_workflow` with a body-start counter, for the sticky-off test.
fn cold_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    COLD_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
    two_signal_workflow(ctx, input)
}

const OFF_WORKFLOW: &str = "sticky_default_off_wf";

/// Body starts of `off_workflow`. Only one test runs it.
static OFF_BODY_STARTS: AtomicU64 = AtomicU64::new(0);

/// `two_signal_workflow` with a body-start counter, for the switch test.
fn off_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    OFF_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
    two_signal_workflow(ctx, input)
}

const EVICT_WORKFLOW: &str = "sticky_default_evict_wf";

/// Body starts of `evict_workflow`. Only one test runs it.
static EVICT_BODY_STARTS: AtomicU64 = AtomicU64::new(0);

/// `two_signal_workflow` with a body-start counter, for the eviction test.
fn evict_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    EVICT_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
    two_signal_workflow(ctx, input)
}

fn echo_activity<'a>(
    _ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(input) })
}

fn echo_activity_info() -> ActivityInfo {
    ActivityInfo {
        name: ECHO_ACTIVITY,
        is_local: false,
        handler: echo_activity,
        ..slow_activity_info()
    }
}

fn workflow_info() -> WorkflowInfo {
    info_for(WORKFLOW, two_signal_workflow)
}

fn info_for(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "sticky_default_tests",
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

/// Builds a worker from `WorkerConfig::default()` with no sticky override.
///
/// The worker polls only `queue`. Pass ids from [`unique_id`].
fn build_default_worker(queue: &str, worker_id: &str, metrics: Arc<CacheCounts>) -> Arc<Worker> {
    build_worker(queue, worker_id, metrics, WorkerConfig::default())
}

/// Builds a worker from `config`. The worker polls only `queue`.
fn build_worker(
    queue: &str,
    worker_id: &str,
    metrics: Arc<CacheCounts>,
    config: WorkerConfig,
) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(vec![
            workflow_info(),
            info_for(UPDATE_WORKFLOW, update_then_signal_workflow),
            info_for(SLOW_WORKFLOW, slow_workflow),
            info_for(RESIDENT_WORKFLOW, resident_workflow),
            info_for(COUNTED_WORKFLOW, counted_workflow),
            info_for(COLD_WORKFLOW, cold_workflow),
            info_for(OFF_WORKFLOW, off_workflow),
            info_for(EVICT_WORKFLOW, evict_workflow),
        ])
        .activities(vec![slow_activity_info(), echo_activity_info()])
        .telemetry(TelemetryConfig {
            service_name: Arc::from("sticky_default_tests"),
            propagator: Arc::new(NoOpPropagator),
            metrics: metrics as Arc<dyn MetricsRecorder>,
        })
        .worker(config.with_queues([queue]))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = worker_id.to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    // Long enough that a drain outlives the slow local activity.
    runtime_config.shutdown_timeout = Duration::from_secs(10);
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

fn spawn(worker: &Arc<Worker>, pool: &DbPool) -> tokio::task::JoinHandle<()> {
    let runner = Arc::clone(worker);
    let pool = pool.clone();
    tokio::spawn(async move { runner.run(&pool).await })
}

/// Returns `prefix` with a random suffix, for worker ids and queues.
fn unique_id(prefix: &str) -> String {
    // Postgres caps a NOTIFY channel name at 63 bytes, and the queue name is
    // part of it. Twelve hex digits keep the id short and still unique.
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

fn start_params<'a>(
    exec_id: ExecutionId,
    workflow_id: &'a str,
    queue_name: &'a str,
) -> StartWorkflowParams<'a> {
    start_workflow(
        WORKFLOW,
        exec_id,
        workflow_id,
        queue_name,
        serde_json::json!({}),
    )
}

fn start_workflow<'a>(
    workflow_name: &'a str,
    exec_id: ExecutionId,
    workflow_id: &'a str,
    queue_name: &'a str,
    input: serde_json::Value,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: input.into(),
        parent_id: None,
        queue_name,
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
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: autumn_harvest::types::Priority::default(),
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
    }
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to test DB")
}

#[derive(diesel::QueryableByName, Debug)]
struct PinRow {
    #[diesel(sql_type = Nullable<Text>)]
    sticky_worker_id: Option<String>,
}

/// Returns the sticky owner of the parked workflow task, or `None` while
/// the task is not parked.
async fn parked_pin(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Option<PinRow> {
    diesel::sql_query(
        "SELECT sticky_worker_id FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow' \
           AND state = 'RUNNING' AND worker_id IS NULL",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .ok()
}

/// Waits until `counts` reports `decisions` decisions and the task is parked.
async fn wait_parked_after(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    counts: &CacheCounts,
    decisions: u64,
) -> PinRow {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if counts.decisions() >= decisions
            && let Some(row) = parked_pin(conn, exec_id).await
        {
            return row;
        }
        assert!(
            Instant::now() < deadline,
            "workflow did not park after {decisions} decision(s)"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn signal(conn: &mut AsyncPgConnection, exec_id: ExecutionId, name: &str) {
    autumn_harvest::signal::send_signal(conn, exec_id, name, serde_json::json!(name))
        .await
        .expect("send signal");
}

/// AC: the second decision lands on the same worker. When that worker
/// dies, the next decision falls back to a peer after the sticky window.
#[tokio::test]
async fn default_worker_keeps_its_execution_and_peer_takes_over_after_a_crash() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let queue = unique_id("crash-q");
    let a_id = unique_id("crash-a");

    let a_counts = Arc::new(CacheCounts::default());
    let b_counts = Arc::new(CacheCounts::default());
    let worker_a = build_default_worker(&queue, &a_id, Arc::clone(&a_counts));
    let worker_b = build_default_worker(&queue, &unique_id("crash-b"), Arc::clone(&b_counts));

    // Only worker A runs, so A runs decision 1 and parks the task.
    let handle_a = spawn(&worker_a, &pool);
    let exec_id = ExecutionId::new();
    let workflow_id = unique_id("crash-wf");
    start_or_load_workflow_execution(&mut conn, start_params(exec_id, &workflow_id, &queue), None)
        .await
        .expect("start workflow");
    let pin = wait_parked_after(&mut conn, exec_id, &a_counts, 1).await;
    assert_eq!(
        pin.sticky_worker_id.as_deref(),
        Some(a_id.as_str()),
        "a default worker must pin the parked task to itself"
    );
    assert_eq!(a_counts.misses(), 1, "decision 1 is a cold load");

    // Worker B joins. The pin keeps decision 2 on worker A.
    let handle_b = spawn(&worker_b, &pool);
    signal(&mut conn, exec_id, "first").await;
    wait_parked_after(&mut conn, exec_id, &a_counts, 2).await;
    assert_eq!(a_counts.hits(), 1, "decision 2 must be a cache hit on A");
    assert_eq!(b_counts.decisions(), 0, "B must not run a pinned decision");

    // Worker A dies without a shutdown. The pin stays until it expires.
    handle_a.abort();
    let _ = handle_a.await;
    let signalled_at = Instant::now();
    signal(&mut conn, exec_id, "second").await;
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;

    assert_eq!(b_counts.misses(), 1, "B runs decision 3 from a cold load");
    assert_eq!(
        a_counts.decisions(),
        2,
        "the dead worker must not run again"
    );
    assert!(
        signalled_at.elapsed() >= Duration::from_secs(4),
        "B must wait for the sticky window to close; waited {:?}",
        signalled_at.elapsed()
    );

    worker_b.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle_b).await;
}

/// A graceful shutdown releases the pins of the worker, so a peer does not
/// wait for the sticky window.
#[tokio::test]
async fn graceful_shutdown_releases_the_sticky_pins_of_the_worker() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let queue = unique_id("release-q");
    let a_id = unique_id("release-a");

    let a_counts = Arc::new(CacheCounts::default());
    let b_counts = Arc::new(CacheCounts::default());
    let worker_a = build_default_worker(&queue, &a_id, Arc::clone(&a_counts));

    let handle_a = spawn(&worker_a, &pool);
    let exec_id = ExecutionId::new();
    let workflow_id = unique_id("release-wf");
    start_or_load_workflow_execution(&mut conn, start_params(exec_id, &workflow_id, &queue), None)
        .await
        .expect("start workflow");
    let pin = wait_parked_after(&mut conn, exec_id, &a_counts, 1).await;
    assert_eq!(pin.sticky_worker_id.as_deref(), Some(a_id.as_str()));

    worker_a.shutdown();
    tokio::time::timeout(Duration::from_secs(10), handle_a)
        .await
        .expect("worker A stops")
        .expect("worker A task joins");

    let pin = parked_pin(&mut conn, exec_id)
        .await
        .expect("task is still parked");
    assert_eq!(
        pin.sticky_worker_id, None,
        "a graceful shutdown must release the pin"
    );

    // A peer now runs the rest of the workflow.
    let worker_b = build_default_worker(&queue, &unique_id("release-b"), Arc::clone(&b_counts));
    let handle_b = spawn(&worker_b, &pool);
    signal(&mut conn, exec_id, "first").await;
    signal(&mut conn, exec_id, "second").await;
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;
    assert!(
        b_counts.decisions() >= 1,
        "B must run the follow-up decision"
    );

    worker_b.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle_b).await;
}

/// A draining worker releases its pins before it waits for in-flight work.
/// A wake during the drain must not re-arm a pin to the draining worker.
#[tokio::test]
async fn draining_worker_releases_its_pins_before_the_drain() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let queue = unique_id("drain-q");
    let a_id = unique_id("drain-a");

    let a_counts = Arc::new(CacheCounts::default());
    let b_counts = Arc::new(CacheCounts::default());
    let worker_a = build_default_worker(&queue, &a_id, Arc::clone(&a_counts));
    let handle_a = spawn(&worker_a, &pool);

    // Worker A parks X (slow) and Y (plain). A pins both.
    let slow = ExecutionId::new();
    let slow_id = unique_id("drain-slow");
    start_or_load_workflow_execution(
        &mut conn,
        start_workflow(SLOW_WORKFLOW, slow, &slow_id, &queue, serde_json::json!({})),
        None,
    )
    .await
    .expect("start slow workflow");
    let plain = ExecutionId::new();
    let plain_id = unique_id("drain-plain");
    start_or_load_workflow_execution(&mut conn, start_params(plain, &plain_id, &queue), None)
        .await
        .expect("start plain workflow");
    wait_parked_after(&mut conn, slow, &a_counts, 2).await;
    let pin = wait_parked_after(&mut conn, plain, &a_counts, 2).await;
    assert_eq!(pin.sticky_worker_id.as_deref(), Some(a_id.as_str()));

    // X starts its slow local activity on A, so A has work in flight.
    signal(&mut conn, slow, "go").await;
    let deadline = Instant::now() + Duration::from_secs(15);
    while claimed_by(&mut conn, slow).await.as_deref() != Some(a_id.as_str()) {
        assert!(
            Instant::now() < deadline,
            "A did not claim the slow decision"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // A starts to drain. The pin of Y must go before the drain ends.
    worker_a.shutdown();
    let deadline = Instant::now() + SLOW_ACTIVITY_TIME / 2;
    loop {
        let pin = parked_pin(&mut conn, plain).await.expect("Y is parked");
        if pin.sticky_worker_id.is_none() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a draining worker must release its pins before the drain"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(!handle_a.is_finished(), "A must still be draining here");

    // A peer runs Y to completion while A still drains.
    let worker_b = build_default_worker(&queue, &unique_id("drain-b"), Arc::clone(&b_counts));
    let handle_b = spawn(&worker_b, &pool);
    signal(&mut conn, plain, "first").await;
    signal(&mut conn, plain, "second").await;
    wait_for_execution_state_with_timeout(&url, plain, "COMPLETED", Duration::from_secs(30)).await;
    assert!(b_counts.decisions() >= 1, "B must run the woken decision");

    // X parks after the drain. The release after the drain clears that pin.
    tokio::time::timeout(Duration::from_secs(20), handle_a)
        .await
        .expect("worker A stops")
        .expect("worker A task joins");
    let pin = parked_pin(&mut conn, slow).await.expect("X is parked");
    assert_eq!(
        pin.sticky_worker_id, None,
        "a pin set during the drain must go too"
    );

    worker_b.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle_b).await;
}

/// Returns the worker that holds the workflow task of `exec_id`, if any.
async fn claimed_by(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Option<String> {
    #[derive(diesel::QueryableByName)]
    struct Claim {
        #[diesel(sql_type = Nullable<Text>)]
        worker_id: Option<String>,
    }
    let row: Option<Claim> = diesel::sql_query(
        "SELECT worker_id FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow' AND state = 'RUNNING'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .ok();
    row.and_then(|r| r.worker_id)
}

/// Counts the `UpdateCompleted` events of `update_id` in stored history.
async fn stored_update_results(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    update_id: UpdateId,
) -> usize {
    store::load_history(conn, exec_id)
        .await
        .expect("load history")
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::UpdateCompleted { update_id: id, .. } if *id == update_id))
        .count()
}

/// An update result that leaves with an external signal stays in the
/// in-memory history. Otherwise the in-process re-run and the warm cache
/// both miss it, and the update handler runs a second time.
#[tokio::test]
async fn warm_decisions_keep_update_results_batched_with_an_external_signal() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let queue = unique_id("update-q");
    UPDATE_RUNS.store(0, Ordering::SeqCst);

    // The sink only receives the external signal.
    let sink = ExecutionId::new();
    let sink_id = unique_id("update-sink");
    start_or_load_workflow_execution(&mut conn, start_params(sink, &sink_id, &queue), None)
        .await
        .expect("start sink");

    // Admit the update before the first decision runs.
    let exec_id = ExecutionId::new();
    let workflow_id = unique_id("update-wf");
    let input = serde_json::json!({ "target": sink.as_uuid().to_string() });
    start_or_load_workflow_execution(
        &mut conn,
        start_workflow(UPDATE_WORKFLOW, exec_id, &workflow_id, &queue, input),
        None,
    )
    .await
    .expect("start workflow");
    let update_id = UpdateId::from_uuid(UPDATE_UUID);
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::UpdateAdmitted {
            update_id,
            name: "bump".to_string(),
            input: serde_json::json!({}),
            timestamp: chrono::Utc::now(),
        }],
        1,
    )
    .await
    .expect("admit update");

    let counts = Arc::new(CacheCounts::default());
    let worker = build_default_worker(&queue, &unique_id("update-a"), Arc::clone(&counts));
    let handle = spawn(&worker, &pool);

    // Decision 1 completes the update, signals the sink and parks.
    let deadline = Instant::now() + Duration::from_secs(15);
    while parked_pin(&mut conn, exec_id).await.is_none() {
        assert!(Instant::now() < deadline, "workflow did not park");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // The next decision is warm, so it reads the cached history.
    let hits_before = counts.hits();
    signal(&mut conn, exec_id, "done").await;
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;
    assert!(
        counts.hits() > hits_before,
        "the final decision must be warm"
    );

    assert_eq!(
        AtomicU64::load(&UPDATE_RUNS, Ordering::SeqCst),
        1,
        "the update handler must run exactly once"
    );
    assert_eq!(
        stored_update_results(&mut conn, exec_id, update_id).await,
        1,
        "history must hold exactly one result for the update"
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

#[derive(diesel::QueryableByName, Debug)]
struct IdPin {
    #[diesel(sql_type = Nullable<Text>)]
    sticky_worker_id: Option<String>,
}

/// Enqueues one workflow task on `queue`, pinned to `worker_id`.
async fn enqueue_pinned(
    conn: &mut AsyncPgConnection,
    queue: &str,
    worker_id: &str,
    session_id: Option<uuid::Uuid>,
) -> uuid::Uuid {
    let exec_id = ExecutionId::new();
    let workflow_id = unique_id("release-row");
    start_or_load_workflow_execution(conn, start_params(exec_id, &workflow_id, queue), None)
        .await
        .expect("start workflow");
    // Drop the start task so each test row is the only task of its execution.
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(conn)
        .await
        .expect("drop start task");
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, serde_json::json!(null));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    let mut params = params.with_sticky(worker_id, Duration::from_secs(60));
    if let Some(session_id) = session_id {
        params = params.with_session_id(session_id);
    }
    queue::enqueue(conn, &params).await.expect("enqueue")
}

async fn set_state(
    conn: &mut AsyncPgConnection,
    id: uuid::Uuid,
    state: &str,
    worker: Option<&str>,
) {
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET state = $2, worker_id = $3, \
             started_at = CASE WHEN $3 IS NULL THEN NULL ELSE NOW() END \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<Text, _>(state)
    .bind::<Nullable<Text>, _>(worker)
    .execute(conn)
    .await
    .expect("set task state");
}

async fn pin_of(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> Option<String> {
    let row: IdPin =
        diesel::sql_query("SELECT sticky_worker_id FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(id)
            .get_result(conn)
            .await
            .expect("read pin");
    row.sticky_worker_id
}

/// The release clears the pins of pending and parked rows of one worker.
/// It keeps session pins, pins of other workers, rows the worker still
/// runs and terminal rows. Each kept row differs from a released row in
/// one column only, so a wrong `AND`/`OR` grouping fails the test.
#[tokio::test]
async fn release_worker_sticky_pins_clears_only_idle_unsessioned_rows_of_the_worker() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("release-rows-q");
    let me = unique_id("release-me");
    let peer = unique_id("release-peer");

    let pending = enqueue_pinned(&mut conn, &queue, &me, None).await;
    let parked = enqueue_pinned(&mut conn, &queue, &me, None).await;
    set_state(&mut conn, parked, "RUNNING", None).await;

    let running = enqueue_pinned(&mut conn, &queue, &me, None).await;
    set_state(&mut conn, running, "RUNNING", Some(&me)).await;
    let completed = enqueue_pinned(&mut conn, &queue, &me, None).await;
    set_state(&mut conn, completed, "COMPLETED", None).await;
    let pending_session = enqueue_pinned(&mut conn, &queue, &me, Some(uuid::Uuid::new_v4())).await;
    let parked_session = enqueue_pinned(&mut conn, &queue, &me, Some(uuid::Uuid::new_v4())).await;
    set_state(&mut conn, parked_session, "RUNNING", None).await;
    let pending_peer = enqueue_pinned(&mut conn, &queue, &peer, None).await;
    let parked_peer = enqueue_pinned(&mut conn, &queue, &peer, None).await;
    set_state(&mut conn, parked_peer, "RUNNING", None).await;

    let released = queue::release_worker_sticky_pins(&mut conn, &me)
        .await
        .expect("release pins");

    assert_eq!(released, 2, "only the pending and parked rows are released");
    assert_eq!(pin_of(&mut conn, pending).await, None);
    assert_eq!(pin_of(&mut conn, parked).await, None);
    for (name, id, owner) in [
        ("running", running, &me),
        ("completed", completed, &me),
        ("pending session", pending_session, &me),
        ("parked session", parked_session, &me),
        ("pending peer", pending_peer, &peer),
        ("parked peer", parked_peer, &peer),
    ] {
        assert_eq!(
            pin_of(&mut conn, id).await.as_deref(),
            Some(owner.as_str()),
            "the {name} row must keep its pin"
        );
    }

    // No worker polls this queue. Delete the rows so they cannot leak.
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
        .bind::<Text, _>(&queue)
        .execute(&mut conn)
        .await
        .expect("clean up rows");
}

/// AC (issue #1798, step 2): a warm decision resumes the resident workflow.
/// The body runs from the top only in the cold first decision.
#[tokio::test]
async fn warm_decisions_resume_the_resident_workflow() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let queue = unique_id("resident-q");
    RESIDENT_BODY_STARTS.store(0, Ordering::SeqCst);

    let counts = Arc::new(CacheCounts::default());
    let worker = build_default_worker(&queue, &unique_id("resident-a"), Arc::clone(&counts));
    let handle = spawn(&worker, &pool);

    let exec_id = ExecutionId::new();
    let workflow_id = unique_id("resident-wf");
    let input = serde_json::json!({ "queue": queue });
    start_or_load_workflow_execution(
        &mut conn,
        start_workflow(RESIDENT_WORKFLOW, exec_id, &workflow_id, &queue, input),
        None,
    )
    .await
    .expect("start workflow");

    // Decision 1 schedules the activity. Decision 2 waits for `one`.
    wait_parked_after(&mut conn, exec_id, &counts, 2).await;
    signal(&mut conn, exec_id, "one").await;
    wait_parked_after(&mut conn, exec_id, &counts, 3).await;
    signal(&mut conn, exec_id, "two").await;
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;

    assert_eq!(counts.misses(), 1, "only decision 1 is a cold load");
    assert_eq!(counts.hits(), 3, "decisions 2 to 4 are cache hits");
    assert_eq!(
        AtomicU64::load(&RESIDENT_BODY_STARTS, Ordering::SeqCst),
        1,
        "a warm decision must resume the parked future, not replay the body"
    );
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load history");
    assert!(
        matches!(
            history.events.last(),
            Some(WorkflowEvent::WorkflowCompleted { output })
                if *output == serde_json::json!([{ "step": 1 }, "one", "two"])
        ),
        "the resident run must complete with the right output: {:?}",
        history.events.last()
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

/// AC (issue #1798, step 2): a delta with two results cannot resume the
/// one parked future. The worker drops the resident state and replays cold.
#[tokio::test]
async fn a_delta_the_resident_path_cannot_read_falls_back_to_a_cold_replay() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let queue = unique_id("fallback-q");
    COUNTED_BODY_STARTS.store(0, Ordering::SeqCst);

    let counts = Arc::new(CacheCounts::default());
    let worker = build_default_worker(&queue, &unique_id("fallback-a"), Arc::clone(&counts));
    let handle = spawn(&worker, &pool);

    let exec_id = ExecutionId::new();
    let workflow_id = unique_id("fallback-wf");
    start_or_load_workflow_execution(
        &mut conn,
        start_workflow(
            COUNTED_WORKFLOW,
            exec_id,
            &workflow_id,
            &queue,
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .expect("start workflow");
    wait_parked_after(&mut conn, exec_id, &counts, 1).await;

    // Both signals commit together, so decision 2 sees two results.
    conn.transaction::<_, diesel::result::Error, _>(async |tx| {
        signal(tx, exec_id, "first").await;
        signal(tx, exec_id, "second").await;
        Ok(())
    })
    .await
    .expect("commit both signals");
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;

    assert_eq!(counts.hits(), 1, "decision 2 is still a cache hit");
    assert_eq!(
        AtomicU64::load(&COUNTED_BODY_STARTS, Ordering::SeqCst),
        2,
        "decision 2 must replay cold after the resident path declines"
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

/// With sticky routing off there is no cache, so every decision is a miss
/// and runs the body from the top.
#[tokio::test]
async fn sticky_off_replays_every_decision() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let queue = unique_id("cold-q");
    COLD_BODY_STARTS.store(0, Ordering::SeqCst);

    let counts = Arc::new(CacheCounts::default());
    let config = WorkerConfig::default().with_sticky_routing(StickyRoutingConfig {
        lease_ttl: Duration::ZERO,
    });
    let worker = build_worker(&queue, &unique_id("cold-a"), Arc::clone(&counts), config);
    let handle = spawn(&worker, &pool);

    let exec_id = ExecutionId::new();
    let workflow_id = unique_id("cold-wf");
    start_or_load_workflow_execution(
        &mut conn,
        start_workflow(
            COLD_WORKFLOW,
            exec_id,
            &workflow_id,
            &queue,
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .expect("start workflow");
    wait_unclaimed_after(&mut conn, exec_id, &counts, 1).await;
    signal(&mut conn, exec_id, "first").await;
    wait_unclaimed_after(&mut conn, exec_id, &counts, 2).await;
    signal(&mut conn, exec_id, "second").await;
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;

    assert_eq!(counts.hits(), 0, "a disabled cache never hits");
    assert_eq!(counts.misses(), 3, "every decision is a miss");
    assert_eq!(
        AtomicU64::load(&COLD_BODY_STARTS, Ordering::SeqCst),
        3,
        "every decision replays the body from the top"
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

/// Waits until `counts` reports `decisions` decisions and no worker holds
/// the workflow task. A worker without sticky routing does not park.
async fn wait_unclaimed_after(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    counts: &CacheCounts,
    decisions: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while counts.decisions() < decisions || claimed_by(conn, exec_id).await.is_some() {
        assert!(
            Instant::now() < deadline,
            "workflow did not settle after {decisions} decision(s)"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// `with_resident_workflows(false)` keeps the event cache but replays every
/// decision (issue #1798).
#[tokio::test]
async fn resident_workflows_off_keeps_the_cache_but_replays_every_decision() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let queue = unique_id("off-q");
    OFF_BODY_STARTS.store(0, Ordering::SeqCst);

    let counts = Arc::new(CacheCounts::default());
    let config = WorkerConfig::default().with_resident_workflows(false);
    let worker = build_worker(&queue, &unique_id("off-a"), Arc::clone(&counts), config);
    let handle = spawn(&worker, &pool);

    let exec_id = ExecutionId::new();
    let workflow_id = unique_id("off-wf");
    start_or_load_workflow_execution(
        &mut conn,
        start_workflow(
            OFF_WORKFLOW,
            exec_id,
            &workflow_id,
            &queue,
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .expect("start workflow");
    wait_parked_after(&mut conn, exec_id, &counts, 1).await;
    signal(&mut conn, exec_id, "first").await;
    wait_parked_after(&mut conn, exec_id, &counts, 2).await;
    signal(&mut conn, exec_id, "second").await;
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;

    assert_eq!(counts.misses(), 1, "only decision 1 is a cold load");
    assert_eq!(counts.hits(), 2, "the event cache still hits");
    assert_eq!(
        AtomicU64::load(&OFF_BODY_STARTS, Ordering::SeqCst),
        3,
        "with resident workflows off, every decision replays the body"
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

/// LRU pressure evicts an entry with its resident workflow. The next decision
/// of that run is a miss and replays cold, as `docs/sticky-routing.md` says.
#[tokio::test]
async fn lru_eviction_drops_the_resident_workflow_and_counts_a_miss() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let queue = unique_id("evict-q");
    EVICT_BODY_STARTS.store(0, Ordering::SeqCst);

    let counts = Arc::new(CacheCounts::default());
    let config = WorkerConfig {
        workflow_cache_size: 1,
        ..WorkerConfig::default()
    };
    let worker = build_worker(&queue, &unique_id("evict-a"), Arc::clone(&counts), config);
    let handle = spawn(&worker, &pool);

    // A parks first, then B. The one-entry cache then holds only B.
    let a = ExecutionId::new();
    let a_id = unique_id("evict-a-wf");
    start_or_load_workflow_execution(
        &mut conn,
        start_workflow(EVICT_WORKFLOW, a, &a_id, &queue, serde_json::json!({})),
        None,
    )
    .await
    .expect("start A");
    wait_parked_after(&mut conn, a, &counts, 1).await;
    let b = ExecutionId::new();
    let b_id = unique_id("evict-b-wf");
    start_or_load_workflow_execution(
        &mut conn,
        start_workflow(EVICT_WORKFLOW, b, &b_id, &queue, serde_json::json!({})),
        None,
    )
    .await
    .expect("start B");
    wait_parked_after(&mut conn, b, &counts, 2).await;
    assert_eq!(counts.misses(), 2, "both first decisions are cold loads");

    // Each run's next decision finds the other run in the cache.
    for exec_id in [a, b] {
        signal(&mut conn, exec_id, "first").await;
        let decisions = counts.decisions() + 1;
        wait_parked_after(&mut conn, exec_id, &counts, decisions).await;
        signal(&mut conn, exec_id, "second").await;
        wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
            .await;
    }

    // A: miss, miss (evicted by B), hit. B: miss, miss (evicted by A), hit.
    assert_eq!(
        counts.misses(),
        4,
        "each first decision and each eviction is a miss"
    );
    assert_eq!(counts.hits(), 2, "each final decision is a warm hit");
    assert_eq!(
        AtomicU64::load(&EVICT_BODY_STARTS, Ordering::SeqCst),
        4,
        "an evicted run replays cold once, then resumes warm"
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}
