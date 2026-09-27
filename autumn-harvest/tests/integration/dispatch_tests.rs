#![cfg(all(feature = "db", feature = "testing"))]
//! Redis-dispatch worker integration, driven through `MemoryDispatch` (issue
//! #1312).
//!
//! The dispatch channel is process-global, so every case here takes
//! [`DISPATCH_SERIAL`] and uninstalls the channel through a guard on the way
//! out. The cases drive the real worker loop against a real Postgres, as
//! `workflow_retry_tests` does. The claim path, the release backoff and the
//! reconcile sweep are therefore the shipped ones.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::context::empty_shared_state;
use autumn_harvest::dispatch::{DispatchSettings, MemoryDispatch, TaskDispatch};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::telemetry::TelemetryConfig;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, ShardId, StartWorkflowParams, WorkflowContext, start_or_load_workflow_execution,
};

use crate::integration_e2e::setup_test_database_url_or_env;

use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Serializes every case in this module. The installed channel is
/// process-global, so two cases sharing it would read each other's references.
static DISPATCH_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Resumes a paused queue when the case ends, panic or not.
///
/// The database is shared across cases. A failed assertion inside the paused
/// window would otherwise leave the queue held for every case that follows.
struct PausedQueueGuard {
    url: String,
    queue: &'static str,
}

impl Drop for PausedQueueGuard {
    fn drop(&mut self) {
        let url = self.url.clone();
        let queue = self.queue;
        // `Drop` is not async, and the current runtime may already be stopping.
        // A short-lived thread with its own runtime answers both.
        let resumed = std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async move {
                let Ok(mut conn) = AsyncPgConnection::establish(&url).await else {
                    return;
                };
                let _ =
                    autumn_harvest::queue_pause::resume_queue(&mut conn, queue, "operator").await;
            });
        })
        .join();
        assert!(resumed.is_ok(), "the queue-resume guard panicked");
    }
}

/// Uninstalls the channel when the case ends, panic or not.
struct InstalledGuard;

impl Drop for InstalledGuard {
    fn drop(&mut self) {
        autumn_harvest::dispatch::uninstall();
    }
}

/// Install `channel` with test-paced settings and return the guard that
/// removes it again.
fn install(channel: &Arc<MemoryDispatch>) -> InstalledGuard {
    install_with(channel, dispatch_settings())
}

fn install_with(channel: &Arc<MemoryDispatch>, settings: DispatchSettings) -> InstalledGuard {
    autumn_harvest::dispatch::install(Arc::clone(channel) as Arc<dyn TaskDispatch>, settings);
    InstalledGuard
}

/// Short intervals so a case converges in seconds rather than minutes.
const fn dispatch_settings() -> DispatchSettings {
    DispatchSettings {
        poll_interval: Duration::from_millis(20),
        reconcile_interval: Duration::from_millis(200),
        reconcile_batch: 100,
        release_backoff_cap: Duration::from_millis(400),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Counts how many times the failing activity ran, and when.
#[derive(Debug, Default)]
struct ActivityLog {
    runs: AtomicUsize,
    first_run: std::sync::Mutex<Option<std::time::Instant>>,
    second_run: std::sync::Mutex<Option<std::time::Instant>>,
}

fn echo_activity(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

fn fail_once_activity(
    ctx: &autumn_harvest::ActivityContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    let log = ctx
        .state::<Arc<ActivityLog>>()
        .expect("activity log in shared state");
    Box::pin(async move {
        let run = AtomicUsize::fetch_add(&log.runs, 1, Ordering::SeqCst) + 1;
        let now = std::time::Instant::now();
        if run == 1 {
            *log.first_run.lock().expect("first run") = Some(now);
            return Err("first attempt always fails".to_string());
        }
        *log.second_run.lock().expect("second run") = Some(now);
        Ok(input)
    })
}

/// Two activities in sequence, so the run needs several claims.
fn two_activity_workflow(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        let first = ctx
            .execute_activity_raw("echo", input, &queue)
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("echo", first, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// One activity that fails on its first attempt.
fn retrying_activity_workflow(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("flaky", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// An activity that runs inline and takes a while.
///
/// A local activity runs inside the workflow task, so the workflow keeps its
/// workflow permit for the whole call. A plain sleep in the workflow body
/// cannot do this: the engine sees a suspension with no commands and fails the
/// run.
fn slow_local_activity(
    _ctx: &autumn_harvest::ActivityContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(800)).await;
        Ok(input)
    })
}

/// A workflow that holds its workflow permit for a while.
///
/// The hold makes a second concurrent workflow row observable in the table.
fn slow_workflow(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("slow_local", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

/// A workflow with no activities, so the run needs exactly one claim.
fn trivial_workflow(_ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

/// A workflow that parks on one signal, so the run needs a wake to finish.
fn signal_waiting_workflow(ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.receive_signal::<serde_json::Value>("go")
            .await
            .map_err(|e| e.to_string())
    })
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "dispatch_tests",
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

fn act_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    retry: Option<RetryPolicy>,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "dispatch_tests",
        default_retry_policy: retry,
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
        handler,
    }
}

fn shared_state_with(log: Arc<ActivityLog>) -> autumn_harvest::context::SharedState {
    let mut map: std::collections::HashMap<std::any::TypeId, Box<dyn std::any::Any + Send + Sync>> =
        std::collections::HashMap::new();
    map.insert(std::any::TypeId::of::<Arc<ActivityLog>>(), Box::new(log));
    Arc::new(map)
}

fn worker_config(queue: &str, shards: Vec<ShardId>) -> WorkerRuntimeConfig {
    WorkerRuntimeConfig {
        codec_rotation_batch_size: 0,
        dr: autumn_harvest::replication::DrConfig::default(),
        worker_id: uuid::Uuid::new_v4().to_string(),
        queues: vec![queue.to_string()],
        queue_weights: std::collections::HashMap::new(),
        notification_database_url: None,
        shard_notification_database_urls: Vec::new(),
        max_concurrent_workflows: 4,
        max_concurrent_activities: 4,
        poll_interval: Duration::from_millis(20),
        shutdown_timeout: Duration::from_secs(2),
        cancellation_grace_period: Duration::from_secs(2),
        sticky_timeout: Duration::ZERO,
        max_local_activity_start_to_close: Duration::from_secs(60),
        shard_assignments: shards,
        worker_heartbeat_interval: Duration::from_secs(5),
        build_id: String::new(),
        deployment_name: None,
        workflow_cache_size: 100,
        priority_aging_secs: None,
        unknown_target_grace_window: Duration::from_secs(5),
        poison_pill_threshold: 3,
        capability_miss_max_redeliveries: 5,
        workflow_task_timeout: Duration::from_secs(30),
        workflow_panic_max_attempts: 3,
        max_workflow_pause_duration: Duration::from_secs(24 * 3600),
        labels: std::collections::HashMap::new(),
        sharded_pool: None,
        max_workflow_history_events: None,
        slot_tuner: None,
        max_concurrent_sessions: 0,
    }
}

fn make_worker(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    shared_state: autumn_harvest::context::SharedState,
) -> Worker {
    make_worker_with(
        worker_config("default", vec![ShardId::new(0)]),
        workflows,
        activities,
        shared_state,
    )
}

/// Build a worker on a caller-supplied runtime config.
///
/// A case that needs its own queue, its own build id or its own poll interval
/// goes through here.
fn make_worker_with(
    config: WorkerRuntimeConfig,
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    shared_state: autumn_harvest::context::SharedState,
) -> Worker {
    let telemetry = Arc::new(TelemetryConfig::builder().build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        shared_state,
        telemetry,
    ));
    Worker::new(config, registry).expect("worker should build")
}

/// Records the dispatch counters of issue #1429.
#[derive(Debug, Default)]
struct DispatchCounters {
    fallbacks: std::sync::Mutex<Vec<String>>,
    recovered: std::sync::atomic::AtomicU64,
}

impl autumn_harvest::telemetry::MetricsRecorder for DispatchCounters {
    fn record_dispatch_fallback(&self, reason: &str) {
        self.fallbacks
            .lock()
            .expect("fallback log")
            .push(reason.to_owned());
    }

    fn record_dispatch_recovered(&self, count: u64) {
        self.recovered.fetch_add(count, Ordering::SeqCst);
    }
}

/// Build a worker whose telemetry records into `counters`.
fn make_worker_with_counters(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    counters: &Arc<DispatchCounters>,
) -> Worker {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(counters) as Arc<dyn autumn_harvest::telemetry::MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        empty_shared_state(),
        telemetry,
    ));
    Worker::new(worker_config("default", vec![ShardId::new(0)]), registry)
        .expect("worker should build")
}

async fn start(conn: &mut AsyncPgConnection, workflow_name: &'static str) -> ExecutionId {
    start_on(conn, workflow_name, "default").await
}

async fn start_on(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    queue: &str,
) -> ExecutionId {
    let workflow_id = format!("{workflow_name}-{}", uuid::Uuid::new_v4());
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id: &workflow_id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input: serde_json::Value::Null,
            parent_id: None,
            queue_name: queue,
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
            priority: autumn_harvest::Priority::default(),
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

async fn execution_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(dsl::state)
        .first::<String>(conn)
        .await
        .expect("execution row")
}

/// Poll until the execution reaches one of `states`, or fail after `timeout`.
async fn wait_for_state(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    states: &[&str],
    timeout: Duration,
) -> String {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let state = execution_state(conn, exec_id).await;
        if states.contains(&state.as_str()) {
            return state;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "execution {exec_id} stayed in {state}, never reached {states:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Debug, Clone, diesel::QueryableByName)]
struct TaskRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    attempt: i32,
}

async fn tasks_for(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<TaskRow> {
    diesel::sql_query(
        "SELECT id, state, attempt FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 ORDER BY created_at",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .load(conn)
    .await
    .expect("task rows")
}

/// Run `worker` against `pool` until `body` finishes, then shut it down.
async fn with_worker<F, T>(worker: Arc<Worker>, pool: DbPool, body: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let running = Arc::clone(&worker);
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(60), running.run(&pool)).await;
    });
    let out = body.await;
    worker.shutdown();
    let _ = handle.await;
    out
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/// A workflow with two activities completes with every claim driven by a
/// channel reference, and the channel is empty when it is done.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_completes_through_the_channel() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "dispatch_two_activities").await;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info("dispatch_two_activities", two_activity_workflow)],
        vec![act_info("echo", echo_activity, None)],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    })
    .await;

    // Every task row this run produced reached `RUNNING` through a channel
    // delivery. The by-id claim is the only writer of that transition on this
    // path.
    let delivered = channel.delivered_ids();
    for task in tasks_for(&mut check, exec_id).await {
        assert!(
            delivered.contains(&task.id),
            "task {} reached state {} without a channel delivery",
            task.id,
            task.state
        );
    }
    assert!(
        channel.acked_ids().len() >= delivered.len(),
        "every delivered reference must be acked or released: {} delivered, {} acked",
        delivered.len(),
        channel.acked_ids().len()
    );
    assert_eq!(
        channel.outstanding_leases(),
        0,
        "no lease may be outstanding once the run is complete"
    );
}

/// A hint raised inside a buffering scope stays out of the channel until the
/// scope flushes.
#[tokio::test]
async fn hint_publishes_after_commit_not_before() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let hint = autumn_harvest::dispatch::DispatchHint {
        task_id: uuid::Uuid::new_v4(),
        queue_name: "default".to_string(),
        scheduled_at: chrono::Utc::now(),
        priority: 0,
        shard: None,
        kind: Some(autumn_harvest::dispatch::DispatchKind::Workflow),
    };

    let observed = Arc::clone(&channel);
    let expected = hint.clone();
    let ((), leftover) = autumn_harvest::dispatch::buffered(async move {
        autumn_harvest::dispatch::record_hint(expected);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            observed.published_ids().is_empty(),
            "a scoped hint must not reach the channel before the flush"
        );
    })
    .await;

    assert_eq!(leftover, vec![hint.clone()]);
    autumn_harvest::dispatch::publish_now(leftover).await;
    assert_eq!(channel.published_ids(), vec![hint.task_id]);
}

/// A reference for a row that is already terminal is acked, and the claim
/// never runs, so `attempt` does not move.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redelivery_of_a_terminal_row_is_acked() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "dispatch_trivial").await;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(Arc::clone(&worker), pool.clone(), async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    })
    .await;

    let task = tasks_for(&mut check, exec_id)
        .await
        .into_iter()
        .next()
        .expect("one workflow task");
    assert_ne!(task.state, "PENDING");
    let attempt_before = task.attempt;

    // Republish the terminal row and let a fresh worker read it.
    let acked_before = channel.acked_ids().len();
    channel
        .publish(&[autumn_harvest::dispatch::DispatchHint {
            task_id: task.id,
            queue_name: "default".to_string(),
            scheduled_at: chrono::Utc::now(),
            priority: 0,
            shard: None,
            kind: Some(autumn_harvest::dispatch::DispatchKind::Workflow),
        }])
        .await
        .expect("republish");

    let worker2 = Arc::new(make_worker(
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));
    with_worker(worker2, pool, async {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while channel.acked_ids().len() <= acked_before {
            assert!(
                std::time::Instant::now() < deadline,
                "the terminal row's reference was never acked"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;

    let after = tasks_for(&mut check, exec_id)
        .await
        .into_iter()
        .find(|row| row.id == task.id)
        .expect("task row");
    assert_eq!(
        after.attempt, attempt_before,
        "a redelivered terminal row must not burn an attempt"
    );
    assert!(
        channel.is_drained(),
        "the reference must be acked, not held"
    );
}

/// A paused queue holds the row. The reference is released with a growing
/// delay and the row stays `PENDING`; the run completes once the queue
/// resumes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gated_row_is_released_with_backoff() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let mut conn = connect(&url).await;
    autumn_harvest::queue_pause::pause_queue(&mut conn, "default", "test", "operator", None)
        .await
        .expect("pause");
    let _resume = PausedQueueGuard {
        url: url.clone(),
        queue: "default",
    };
    let exec_id = start(&mut conn, "dispatch_trivial").await;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool.clone(), async {
        // The held row cycles through release, never through a claim.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while channel.released_ids().len() < 3 {
            assert!(
                std::time::Instant::now() < deadline,
                "a held row must be released repeatedly, got {} releases",
                channel.released_ids().len()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let held = tasks_for(&mut check, exec_id).await;
        assert!(
            held.iter().all(|row| row.state == "PENDING"),
            "a held row must stay PENDING, got {held:?}"
        );
        assert_eq!(
            execution_state(&mut check, exec_id).await,
            "RUNNING",
            "a held run must not complete while the queue is paused"
        );

        let mut resume_conn = connect(&url).await;
        autumn_harvest::queue_pause::resume_queue(&mut resume_conn, "default", "operator")
            .await
            .expect("resume");

        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    })
    .await;
}

/// The reconcile sweep republishes a row whose reference the channel lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconcile_republishes_after_channel_loss() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "dispatch_trivial").await;

    // The start published a reference. Wipe the channel, as a Redis restart
    // without persistence does. Only the reconcile sweep can recover this.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while channel.published_ids().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the start never published a hint"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    channel.drop_all();
    assert!(channel.is_drained());

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    })
    .await;
}

/// A channel that fails every call falls back to the Postgres claim path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn channel_failure_falls_back_to_postgres_claim() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "dispatch_two_activities").await;

    channel.fail_next(usize::MAX);

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info("dispatch_two_activities", two_activity_workflow)],
        vec![act_info("echo", echo_activity, None)],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(40)).await;
    })
    .await;

    assert!(
        channel.delivered_ids().is_empty(),
        "a failing channel must never deliver a reference"
    );
}

/// Every failed channel call that sends the worker to Postgres counts one
/// fallback, labelled with the call that failed (issue #1429).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_channel_failure_counts_a_fallback() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "dispatch_two_activities").await;

    channel.fail_next(usize::MAX);

    let counters = Arc::new(DispatchCounters::default());
    let pool = build_pool(&url);
    let worker = Arc::new(make_worker_with_counters(
        vec![wf_info("dispatch_two_activities", two_activity_workflow)],
        vec![act_info("echo", echo_activity, None)],
        &counters,
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(40)).await;
    })
    .await;

    let fallbacks = counters.fallbacks.lock().expect("fallback log").clone();
    assert!(
        !fallbacks.is_empty(),
        "a failing channel must count at least one fallback"
    );
    // MemoryDispatch fails `next` and `publish`, never `maintain`, and it
    // answers at once. Only these two labels can appear.
    for reason in &fallbacks {
        assert!(
            ["read", "publish"].contains(&reason.as_str()),
            "unexpected fallback reason {reason}"
        );
    }
}

/// A reference a dead consumer left unacked is recovered by maintenance, and
/// the worker counts it (issue #1429).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recovered_reference_is_counted() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::with_visibility_timeout(
        Duration::from_millis(200),
    ));
    let _guard = install(&channel);

    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "dispatch_two_activities").await;

    // The start published its reference. A consumer that dies takes it and
    // never acks.
    let taken = channel
        .next(
            &["default".to_string()],
            "dead-consumer",
            10,
            Duration::ZERO,
        )
        .await
        .expect("read the reference as the dead consumer");
    assert_eq!(taken.len(), 1, "the start publishes one reference");

    let counters = Arc::new(DispatchCounters::default());
    let pool = build_pool(&url);
    let worker = Arc::new(make_worker_with_counters(
        vec![wf_info("dispatch_two_activities", two_activity_workflow)],
        vec![act_info("echo", echo_activity, None)],
        &counters,
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::sync::atomic::AtomicU64::load(&counters.recovered, Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "maintenance never counted the recovered reference"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
}

/// An activity that fails once with a retry delay is not re-run before the
/// delay elapses, even though the channel holds a reference for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_backoff_is_honoured_by_the_channel() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let log = Arc::new(ActivityLog::default());
    let shared = shared_state_with(Arc::clone(&log));

    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "dispatch_retrying").await;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info("dispatch_retrying", retrying_activity_workflow)],
        vec![act_info(
            "flaky",
            fail_once_activity,
            Some(RetryPolicy::fixed(3, Duration::from_millis(500))),
        )],
        shared,
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(40)).await;
    })
    .await;

    assert_eq!(
        AtomicUsize::load(&log.runs, Ordering::SeqCst),
        2,
        "one failure, one retry"
    );
    let first = log.first_run.lock().expect("first").expect("first run");
    let second = log.second_run.lock().expect("second").expect("second run");
    let gap = second.duration_since(first);
    assert!(
        gap >= Duration::from_millis(450),
        "the retry ran {gap:?} after the failure; the 500 ms backoff was not honoured"
    );
}

/// A multi-shard worker cannot use the dispatch channel in v1.
#[tokio::test]
async fn multi_shard_worker_rejects_dispatch() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let channel = Arc::new(MemoryDispatch::new());
    let guard = install(&channel);

    let telemetry = Arc::new(TelemetryConfig::builder().build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
        telemetry,
    ));
    let config = worker_config("default", vec![ShardId::new(0), ShardId::new(1)]);

    let error = Worker::new(config, registry).expect_err("multi-shard dispatch must be rejected");
    assert!(
        matches!(error, autumn_harvest::HarvestError::Config(ref msg)
            if msg.contains("single-shard")),
        "expected a single-shard Config rejection, got {error:?}"
    );

    // The same config builds once the channel is gone.
    drop(guard);
    let telemetry = Arc::new(TelemetryConfig::builder().build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
        telemetry,
    ));
    Worker::new(
        worker_config("default", vec![ShardId::new(0), ShardId::new(1)]),
        registry,
    )
    .expect("multi-shard builds without a channel");
}

/// The reconcile sweep does not republish the rows of a paused queue.
#[tokio::test]
async fn the_reconcile_sweep_skips_a_paused_queue() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "dispatch_trivial").await;
    let task_ids: Vec<uuid::Uuid> = tasks_for(&mut conn, exec_id)
        .await
        .into_iter()
        .map(|row| row.id)
        .collect();
    assert_eq!(task_ids.len(), 1, "the start writes one workflow task");

    let queues = vec!["default".to_string()];
    let due = autumn_harvest::queue::due_dispatch_hints(&mut conn, &queues, 100)
        .await
        .expect("sweep read");
    assert!(
        due.iter().any(|hint| hint.task_id == task_ids[0]),
        "an unheld due row must be swept"
    );

    autumn_harvest::queue_pause::pause_queue(&mut conn, "default", "test", "operator", None)
        .await
        .expect("pause");
    let _resume = PausedQueueGuard {
        url: url.clone(),
        queue: "default",
    };

    let held = autumn_harvest::queue::due_dispatch_hints(&mut conn, &queues, 100)
        .await
        .expect("sweep read while paused");
    assert!(
        held.is_empty(),
        "a paused queue must not be swept, got {held:?}"
    );
}

/// A signal delivered inside its own transaction reaches a worker through the
/// channel, without waiting for the reconcile sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signal_reaches_the_worker_before_the_reconcile_sweep() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::new());
    // A reconcile interval far longer than the case. Anything that completes
    // here was carried by a hint, never by the sweep.
    let reconcile_interval = Duration::from_secs(30);
    let _guard = install_with(
        &channel,
        DispatchSettings {
            reconcile_interval,
            ..dispatch_settings()
        },
    );

    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "dispatch_signal_wait").await;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info("dispatch_signal_wait", signal_waiting_workflow)],
        vec![],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        // The run parks on the signal. Its task row is `RUNNING` with no owner.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let parked = tasks_for(&mut check, exec_id)
                .await
                .iter()
                .all(|row| row.state == "RUNNING");
            if parked && !channel.delivered_ids().is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the run never parked on its signal"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let mut sender = connect(&url).await;
        let sent_at = std::time::Instant::now();
        autumn_harvest::signal::send_signal(
            &mut sender,
            exec_id,
            "go",
            serde_json::json!({"ok": true}),
        )
        .await
        .expect("signal");

        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(20)).await;
        let elapsed = sent_at.elapsed();
        assert!(
            elapsed < reconcile_interval,
            "the signal took {elapsed:?}, which is the reconcile interval or more; \
             the wake hint did not reach the channel"
        );
    })
    .await;
}

/// The by-id claim runs against Postgres with the cross-region DR fence
/// spliced in, and the fence still decides the outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_by_id_claim_honours_the_dr_fence() {
    use autumn_harvest::replication::{
        FenceRegistry, ShardGeneration, bump_generation, ensure_generation_row,
    };

    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let shard = ShardId::new(0);
    let generation = ensure_generation_row(&mut conn, shard)
        .await
        .expect("generation row");

    let exec_id = start(&mut conn, "dispatch_trivial").await;
    let task_id = tasks_for(&mut conn, exec_id)
        .await
        .into_iter()
        .next()
        .expect("one workflow task")
        .id;

    // A worker pinned to the current generation claims the named row.
    FenceRegistry::clear();
    FenceRegistry::register(shard, generation).expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(shard).expect("no conflicting default shard in this test");
    let claimed = autumn_harvest::queue::claim_task_by_id_on_shard(
        &mut conn,
        task_id,
        &["default".to_string()],
        "dr-worker",
        "",
        None,
        &[],
        &[],
        Some(shard),
    )
    .await
    .expect("fenced by-id claim");
    assert_eq!(
        claimed.map(|task| task.id),
        Some(task_id),
        "a worker on the current generation must claim its own row"
    );

    // Give the row back, promote the shard, and try again with the now-stale pin.
    autumn_harvest::queue::release_terminal_workflow_claim(&mut conn, task_id, "dr-worker", 0)
        .await
        .expect("release");
    bump_generation(&mut conn, shard, "promote", "operator")
        .await
        .expect("bump");
    let fenced = autumn_harvest::queue::claim_task_by_id_on_shard(
        &mut conn,
        task_id,
        &["default".to_string()],
        "dr-worker",
        "",
        None,
        &[],
        &[],
        Some(shard),
    )
    .await
    .expect("fenced by-id claim");
    FenceRegistry::clear();
    assert!(
        fenced.is_none(),
        "a fenced worker must not claim a row by id"
    );
    // The generation moved, so the local binding is stale by construction.
    let _ = ShardGeneration::new(0);
}

/// A transactional start holds its hint until the caller commits, and
/// `TransactionalStartOutcome::finish` publishes it.
#[tokio::test]
async fn a_transactional_start_publishes_its_hint_on_finish() {
    use autumn_harvest::TransactionalStartOptions;
    use autumn_harvest::handle::WorkflowHandleClient;

    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let client = WorkflowHandleClient::single(build_pool(&url), url.clone())
        .with_workflows(vec![wf_info("dispatch_trivial", trivial_workflow)]);
    let mut conn = connect(&url).await;
    let workflow_id = format!("dispatch-tx-{}", uuid::Uuid::new_v4());

    let observed = Arc::clone(&channel);
    let outcome = Box::pin(conn.transaction::<_, autumn_harvest::HarvestError, _>({
        let client = client.clone();
        let workflow_id = workflow_id.clone();
        async move |conn| {
            let outcome = client
                .start_workflow_transactional(
                    conn,
                    "dispatch_trivial",
                    &workflow_id,
                    serde_json::Value::Null,
                    TransactionalStartOptions::new(),
                )
                .await?;
            assert!(
                observed.published_ids().is_empty(),
                "a staged start must not publish before the caller commits"
            );
            Ok(outcome)
        }
    }))
    .await
    .expect("the transaction must commit");

    let exec_id = outcome.exec_id;
    assert!(
        channel.published_ids().is_empty(),
        "the commit alone does not publish; `finish` does"
    );

    outcome.finish().await;

    let mut check = connect(&url).await;
    let task_ids: Vec<uuid::Uuid> = tasks_for(&mut check, exec_id)
        .await
        .into_iter()
        .map(|row| row.id)
        .collect();
    assert_eq!(task_ids.len(), 1, "the start writes one workflow task");
    assert_eq!(
        channel.published_ids(),
        task_ids,
        "finish must publish the hint the start raised"
    );
}

/// Deletes a case's own queue rows when the case ends, panic or not.
///
/// The database is shared across cases. A case that seeds a large gated
/// backlog must not leave it behind for the next case to sweep.
struct SeededQueueGuard {
    url: String,
    queue: String,
}

impl Drop for SeededQueueGuard {
    fn drop(&mut self) {
        let url = self.url.clone();
        let queue = self.queue.clone();
        // `Drop` is not async, and the current runtime may already be stopping.
        // A short-lived thread with its own runtime answers both.
        let cleared = std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async move {
                let Ok(mut conn) = AsyncPgConnection::establish(&url).await else {
                    return;
                };
                let _ = diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
                    .bind::<diesel::sql_types::Text, _>(&queue)
                    .execute(&mut conn)
                    .await;
            });
        })
        .join();
        assert!(cleared.is_ok(), "the seeded-queue guard panicked");
    }
}

/// A queue name no other case shares, so a seeded backlog stays local.
fn unique_queue(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

/// Seed `count` due `PENDING` rows that no worker in the case can claim.
///
/// The gate is build routing: the rows demand a build id the worker does not
/// carry, so the claim predicate rejects every one of them. The sweep cannot
/// see that gate, which is exactly the condition finding F1 describes.
async fn seed_gated_rows(conn: &mut AsyncPgConnection, queue: &str, count: i32, priority: i32) {
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (queue_name, task_type, input, state, priority, scheduled_at, \
          required_build_id, max_attempts) \
         SELECT $1, 'workflow', '{}'::jsonb, 'PENDING', $2, \
                NOW() - INTERVAL '1 minute', 'build-no-worker-carries', 1 \
         FROM generate_series(1, $3)",
    )
    .bind::<diesel::sql_types::Text, _>(queue)
    .bind::<diesel::sql_types::Integer, _>(priority)
    .bind::<diesel::sql_types::Integer, _>(count)
    .execute(conn)
    .await
    .expect("seed gated rows");
}

/// The keyset walk reaches a row that sits below a full page of gated rows
/// (issue #1312).
///
/// The first page is gated rows only. The claimable row is below them, so an
/// unpaginated sweep can never reference it. The cursor carries the walk past
/// the page, and the second page holds the claimable row.
#[tokio::test]
async fn the_reconcile_sweep_walks_past_a_full_page_of_gated_rows() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = unique_queue("f1_page");
    let _cleanup = SeededQueueGuard {
        url: url.clone(),
        queue: queue.clone(),
    };

    let mut conn = connect(&url).await;
    seed_gated_rows(&mut conn, &queue, 1500, 5).await;
    let exec_id = start_on(&mut conn, "dispatch_trivial", &queue).await;
    let claimable = tasks_for(&mut conn, exec_id)
        .await
        .into_iter()
        .next()
        .expect("one workflow task")
        .id;

    let batch = 1000;
    let first = autumn_harvest::queue::due_dispatch_hints_page(&mut conn, &queue, batch, None)
        .await
        .expect("first sweep page");
    assert_eq!(first.hints.len(), batch, "the first page must be full");
    assert!(
        !first.hints.iter().any(|hint| hint.task_id == claimable),
        "the claimable row sorts below a full page of gated rows"
    );
    let cursor = first.cursor.expect("a full page carries a cursor");

    let second =
        autumn_harvest::queue::due_dispatch_hints_page(&mut conn, &queue, batch, Some(&cursor))
            .await
            .expect("second sweep page");
    assert!(
        second.hints.len() < batch,
        "the second page must be short, so the walk wraps"
    );
    assert!(
        second.hints.iter().any(|hint| hint.task_id == claimable),
        "the keyset walk must reach the row below the gated page"
    );
    assert!(
        second.hints.iter().all(|hint| hint.task_id != cursor.id),
        "the keyset predicate is exclusive of the cursor row"
    );
}

/// A worker drains a claimable row that sits below a page of gated rows
/// (issue #1312).
///
/// Without pagination every sweep republishes the same gated page and the
/// claimable row is never referenced, so the run never completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gated_page_does_not_starve_a_claimable_row() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = unique_queue("f1_worker");
    let _cleanup = SeededQueueGuard {
        url: url.clone(),
        queue: queue.clone(),
    };

    let mut conn = connect(&url).await;
    seed_gated_rows(&mut conn, &queue, 250, 5).await;
    // The start runs before the install, so no hint reaches the channel. The
    // reconcile sweep is then the only path from this row to the worker.
    let exec_id = start_on(&mut conn, "dispatch_trivial", &queue).await;

    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install_with(
        &channel,
        DispatchSettings {
            reconcile_batch: 100,
            reconcile_interval: Duration::from_millis(200),
            release_backoff_cap: Duration::from_secs(2),
            ..dispatch_settings()
        },
    );

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker_with(
        worker_config(&queue, vec![ShardId::new(0)]),
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(45)).await;
    })
    .await;
}

/// A channel outage must not throttle the Postgres path to one claim per
/// failed read (issue #1312).
///
/// The channel fails every call. A degraded worker runs the ordinary Postgres
/// loop, so the seeded backlog drains at the Postgres rate. Without degraded
/// mode the worker claims one row per iteration and sleeps a poll interval
/// between claims.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_channel_outage_drains_the_backlog_at_the_postgres_rate() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = unique_queue("f2_outage");
    let _cleanup = SeededQueueGuard {
        url: url.clone(),
        queue: queue.clone(),
    };

    let channel = Arc::new(MemoryDispatch::new());
    channel.fail_next(usize::MAX);
    let _guard = install(&channel);

    let mut conn = connect(&url).await;
    let mut executions = Vec::new();
    for _ in 0..30 {
        executions.push(start_on(&mut conn, "dispatch_trivial", &queue).await);
    }

    // One second per claim is the cost of the old fallback: one `poll_once`
    // per failed read, then a sleep. Thirty rows would need thirty seconds.
    let poll_interval = Duration::from_secs(1);
    let config = WorkerRuntimeConfig {
        poll_interval,
        ..worker_config(&queue, vec![ShardId::new(0)])
    };
    let pool = build_pool(&url);
    let worker = Arc::new(make_worker_with(
        config,
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    let started = std::time::Instant::now();
    with_worker(worker, pool, async {
        for exec_id in &executions {
            wait_for_state(
                &mut check,
                *exec_id,
                &["COMPLETED"],
                Duration::from_secs(45),
            )
            .await;
        }
    })
    .await;
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(20),
        "the backlog took {elapsed:?} to drain; the channel outage throttled the Postgres path"
    );
}

/// A channel installed after the worker was built must not reach a multi-shard
/// loop (issue #1312).
///
/// `Worker::new` refuses the combination, but a core caller can install the
/// channel afterwards. The loop reads the process-global slot on every
/// iteration, so the refusal has to hold at run time as well.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_shard_worker_never_consumes_references() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = unique_queue("f5_span");
    let _cleanup = SeededQueueGuard {
        url: url.clone(),
        queue: queue.clone(),
    };

    let mut conn = connect(&url).await;
    let exec_id = start_on(&mut conn, "dispatch_trivial", &queue).await;
    let task_id = tasks_for(&mut conn, exec_id)
        .await
        .into_iter()
        .next()
        .expect("one workflow task")
        .id;

    // The worker is built with no channel, so `Worker::new` has nothing to
    // refuse. Two shard assignments and no sharded pool keep it on the
    // single-pool entry point.
    let worker = Arc::new(make_worker_with(
        worker_config(&queue, vec![ShardId::new(0), ShardId::new(1)]),
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));

    // The channel arrives after construction, and it already holds a reference
    // for the seeded row.
    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);
    channel
        .publish(&[autumn_harvest::dispatch::DispatchHint {
            task_id,
            queue_name: queue.clone(),
            scheduled_at: chrono::Utc::now(),
            priority: 0,
            shard: None,
            kind: Some(autumn_harvest::dispatch::DispatchKind::Workflow),
        }])
        .await
        .expect("publish");

    let pool = build_pool(&url);
    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    })
    .await;

    assert!(
        channel.delivered_ids().is_empty(),
        "a multi-shard worker must not consume references, got {:?}",
        channel.delivered_ids()
    );
}

/// A queue name the channel key space cannot carry is refused at startup
/// (issue #1312).
///
/// The channel rejects such a name on every call. A worker configured with one
/// would live on the Postgres fallback for all of its queues, and say nothing
/// about it.
#[tokio::test]
async fn a_queue_name_with_a_colon_rejects_dispatch() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let channel = Arc::new(MemoryDispatch::new());
    let guard = install(&channel);

    let telemetry = Arc::new(TelemetryConfig::builder().build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
        telemetry,
    ));

    let error = Worker::new(
        worker_config("tenant:priority", vec![ShardId::new(0)]),
        registry,
    )
    .expect_err("a colon in a queue name must be rejected under dispatch");
    assert!(
        matches!(error, autumn_harvest::HarvestError::Config(ref msg)
            if msg.contains("tenant:priority")),
        "the rejection must name the queue, got {error:?}"
    );

    // The same config builds once the channel is gone.
    drop(guard);
    let telemetry = Arc::new(TelemetryConfig::builder().build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
        telemetry,
    ));
    Worker::new(
        worker_config("tenant:priority", vec![ShardId::new(0)]),
        registry,
    )
    .expect("a colon in a queue name is fine without a channel");
}

/// Rows this worker holds `RUNNING` on `queue` right now.
///
/// A parked workflow row is `RUNNING` with no owner, so the owner test is what
/// separates a running task from a parked one.
async fn running_owned_count(conn: &mut AsyncPgConnection, queue: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_task_queue \
         WHERE queue_name = $1 AND state = 'RUNNING' AND worker_id IS NOT NULL \
           AND task_type = 'workflow'",
    )
    .bind::<diesel::sql_types::Text, _>(queue)
    .get_result(conn)
    .await
    .expect("running count");
    row.count
}

/// A worker never claims past the free permits of the pool a reference needs
/// (issue #1312).
///
/// The read is sized on the sum of both pools, so one read can hold two
/// workflow references while only one workflow permit is free. Claiming both
/// puts a row in `RUNNING` under a worker that cannot start it, and a peer with
/// capacity cannot claim it either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_reference_waits_for_a_workflow_permit() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = unique_queue("f7_permits");
    let _cleanup = SeededQueueGuard {
        url: url.clone(),
        queue: queue.clone(),
    };

    // Both starts run before the install, so the reconcile sweep publishes both
    // references in one batch and one read delivers them together.
    let mut conn = connect(&url).await;
    let first = start_on(&mut conn, "dispatch_slow", &queue).await;
    let second = start_on(&mut conn, "dispatch_slow", &queue).await;

    let channel = Arc::new(MemoryDispatch::new());
    let _guard = install(&channel);

    let config = WorkerRuntimeConfig {
        max_concurrent_workflows: 1,
        max_concurrent_activities: 8,
        ..worker_config(&queue, vec![ShardId::new(0)])
    };
    let worker = Arc::new(make_worker_with(
        config,
        vec![wf_info("dispatch_slow", slow_workflow)],
        vec![ActivityInfo {
            is_local: true,
            default_queue: None,
            ..act_info("slow_local", slow_local_activity, None)
        }],
        empty_shared_state(),
    ));

    let pool = build_pool(&url);
    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let running = running_owned_count(&mut check, &queue).await;
            assert!(
                running <= 1,
                "{running} workflow rows are RUNNING under a worker with one workflow permit"
            );
            let first_state = execution_state(&mut check, first).await;
            let second_state = execution_state(&mut check, second).await;
            if first_state == "COMPLETED" && second_state == "COMPLETED" {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the runs stayed in {first_state} and {second_state}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// Transaction owners publish after commit (issue #1429 item 2)
// ---------------------------------------------------------------------------

tokio::task_local! {
    /// Set while the test runs the transaction owner under test.
    static OWNER_TASK: ();
}

/// What the channel saw when one hint arrived.
#[derive(Debug, Clone)]
struct PublishRecord {
    task_id: uuid::Uuid,
    /// The row was `PENDING` to a separate connection at publish time.
    pending: bool,
    /// The owner's connection had no open transaction at publish time.
    owner_committed: bool,
    /// The owner's own task published the hint, not the background publisher.
    on_owner_task: bool,
}

/// A channel that checks each hint against the database as it arrives.
///
/// A separate connection reads the named row and the owner connection's
/// state in `pg_stat_activity`. A publish inside the owner's transaction sees
/// `idle in transaction` or `active` there. The task-local flag separates a
/// publish by the owner from one the background publisher sends.
#[derive(Debug)]
struct CommitCheckingDispatch {
    url: String,
    inner: MemoryDispatch,
    records: std::sync::Mutex<Vec<PublishRecord>>,
    /// Backend pid of the owner's connection. Zero until a case sets it.
    owner_pid: std::sync::atomic::AtomicI32,
}

impl CommitCheckingDispatch {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_owned(),
            inner: MemoryDispatch::new(),
            records: std::sync::Mutex::new(Vec::new()),
            owner_pid: std::sync::atomic::AtomicI32::new(0),
        }
    }

    /// Record the backend pid of the owner's connection.
    async fn watch_owner(&self, conn: &mut AsyncPgConnection) {
        #[derive(diesel::QueryableByName)]
        struct Pid {
            #[diesel(sql_type = diesel::sql_types::Integer)]
            pid: i32,
        }
        let pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
            .get_result::<Pid>(conn)
            .await
            .expect("read the owner pid")
            .pid;
        self.owner_pid.store(pid, Ordering::SeqCst);
    }

    fn records_for(&self, task_id: uuid::Uuid) -> Vec<PublishRecord> {
        self.records
            .lock()
            .expect("publish records")
            .iter()
            .filter(|record| record.task_id == task_id)
            .cloned()
            .collect()
    }

    fn clear(&self) {
        self.records.lock().expect("publish records").clear();
    }
}

#[async_trait::async_trait]
impl TaskDispatch for CommitCheckingDispatch {
    async fn publish(
        &self,
        hints: &[autumn_harvest::dispatch::DispatchHint],
    ) -> autumn_harvest::HarvestResult<()> {
        let on_owner_task = OWNER_TASK.try_with(|()| ()).is_ok();
        let mut conn = connect(&self.url).await;
        let owner_pid = std::sync::atomic::AtomicI32::load(&self.owner_pid, Ordering::SeqCst);
        let owner_state: Option<String> =
            diesel::sql_query("SELECT state AS value FROM pg_stat_activity WHERE pid = $1")
                .bind::<diesel::sql_types::Integer, _>(owner_pid)
                .get_result::<TextValue>(&mut conn)
                .await
                .optional()
                .expect("read the owner state")
                .map(|row| row.value);
        let owner_committed = owner_state.as_deref() == Some("idle");
        for hint in hints {
            let state: Option<String> =
                diesel::sql_query("SELECT state AS value FROM harvest_task_queue WHERE id = $1")
                    .bind::<diesel::sql_types::Uuid, _>(hint.task_id)
                    .get_result::<TextValue>(&mut conn)
                    .await
                    .optional()
                    .expect("read the hinted row")
                    .map(|row| row.value);
            self.records
                .lock()
                .expect("publish records")
                .push(PublishRecord {
                    task_id: hint.task_id,
                    pending: state.as_deref() == Some("PENDING"),
                    owner_committed,
                    on_owner_task,
                });
        }
        self.inner.publish(hints).await
    }

    async fn next(
        &self,
        queues: &[String],
        consumer: &str,
        max: usize,
        wait: Duration,
    ) -> autumn_harvest::HarvestResult<Vec<autumn_harvest::dispatch::DispatchLease>> {
        self.inner.next(queues, consumer, max, wait).await
    }

    async fn ack(
        &self,
        lease: &autumn_harvest::dispatch::DispatchLease,
    ) -> autumn_harvest::HarvestResult<()> {
        self.inner.ack(lease).await
    }

    async fn release(
        &self,
        lease: &autumn_harvest::dispatch::DispatchLease,
        delay: Duration,
    ) -> autumn_harvest::HarvestResult<()> {
        self.inner.release(lease, delay).await
    }

    async fn maintain(
        &self,
        queues: &[String],
    ) -> autumn_harvest::HarvestResult<autumn_harvest::dispatch::DispatchMaintenance> {
        self.inner.maintain(queues).await
    }
}

#[derive(diesel::QueryableByName)]
struct TextValue {
    #[diesel(sql_type = diesel::sql_types::Text)]
    value: String,
}

/// Install a commit-checking channel and return it with its guard.
fn install_commit_checking(url: &str) -> (Arc<CommitCheckingDispatch>, InstalledGuard) {
    let channel = Arc::new(CommitCheckingDispatch::new(url));
    autumn_harvest::dispatch::install(
        Arc::clone(&channel) as Arc<dyn TaskDispatch>,
        dispatch_settings(),
    );
    (channel, InstalledGuard)
}

/// The single workflow task row of `exec_id`.
async fn workflow_task_id(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> uuid::Uuid {
    let tasks = tasks_for(conn, exec_id).await;
    assert_eq!(tasks.len(), 1, "the start writes one workflow task");
    tasks[0].id
}

/// Wait for the background publisher to deliver anything it still holds.
async fn settle_background_publisher() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

/// Assert that `task_id` was published, and that every publish of it came
/// from the owner's task after the owner's commit.
fn assert_published_after_commit(channel: &CommitCheckingDispatch, task_id: uuid::Uuid) {
    let records = channel.records_for(task_id);
    assert!(
        !records.is_empty(),
        "the owner must publish a hint for {task_id}"
    );
    for record in records {
        assert!(
            record.on_owner_task,
            "the hint for {task_id} left through the background publisher, \
             not from the owner after its commit: {record:?}"
        );
        assert!(
            record.owner_committed,
            "the hint for {task_id} left while its owner's transaction was open: {record:?}"
        );
        assert!(
            record.pending,
            "the hint for {task_id} named a row that is not pending: {record:?}"
        );
    }
}

/// `resume_workflow_execution` owns its transaction. Its wake publishes after
/// the commit, from the caller's task (issue #1429).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resume_publishes_its_wake_after_commit() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let (channel, _guard) = install_commit_checking(&url);
    let queue = unique_queue("resume_wake");
    let _cleanup = SeededQueueGuard {
        url: url.clone(),
        queue: queue.clone(),
    };

    let mut conn = connect(&url).await;
    let exec_id = start_on(&mut conn, "dispatch_trivial", &queue).await;
    let task_id = workflow_task_id(&mut conn, exec_id).await;
    autumn_harvest::execution::pause_workflow_execution(
        &mut conn,
        exec_id,
        None,
        "operator",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("pause");
    settle_background_publisher().await;
    channel.clear();
    channel.watch_owner(&mut conn).await;

    OWNER_TASK
        .scope(
            (),
            autumn_harvest::execution::resume_workflow_execution(
                &mut conn,
                exec_id,
                "operator",
                &autumn_harvest::telemetry::NoOpMetrics,
            ),
        )
        .await
        .expect("resume");
    settle_background_publisher().await;

    assert_published_after_commit(&channel, task_id);
}

/// The timeout scanner reclaims an expired mutex lease and wakes the waiter.
/// The reclaim transaction publishes the wake after it commits (issue #1429).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mutex_reclaim_publishes_its_wake_after_commit() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let (channel, _guard) = install_commit_checking(&url);
    let queue = unique_queue("mutex_wake");
    let _cleanup = SeededQueueGuard {
        url: url.clone(),
        queue: queue.clone(),
    };

    let mut conn = connect(&url).await;
    let holder = start_on(&mut conn, "dispatch_trivial", &queue).await;
    let waiter = start_on(&mut conn, "dispatch_trivial", &queue).await;
    let waiter_task = workflow_task_id(&mut conn, waiter).await;
    settle_background_publisher().await;
    channel.clear();
    channel.watch_owner(&mut conn).await;

    // The lease is seeded right before the pass, so no other test's timeout
    // pass reclaims it first.
    let lock_key = format!("dispatch-reclaim-{}", uuid::Uuid::new_v4());
    diesel::sql_query(
        "INSERT INTO harvest_mutex_locks (lock_key, holder_exec_id, lock_seq, acquired_at, \
         lease_expires_at) \
         VALUES ($1, $2, nextval('harvest_mutex_lock_seq'), NOW() - interval '2 minutes', \
         NOW() - interval '1 second')",
    )
    .bind::<diesel::sql_types::Text, _>(&lock_key)
    .bind::<diesel::sql_types::Uuid, _>(holder.as_uuid())
    .execute(&mut conn)
    .await
    .expect("seed an expired lease");
    diesel::sql_query(
        "INSERT INTO harvest_mutex_waiters (lock_key, waiter_exec_id, requested_at) \
         VALUES ($1, $2, NOW())",
    )
    .bind::<diesel::sql_types::Text, _>(&lock_key)
    .bind::<diesel::sql_types::Uuid, _>(waiter.as_uuid())
    .execute(&mut conn)
    .await
    .expect("seed a waiter");

    OWNER_TASK
        .scope(
            (),
            autumn_harvest::timeout::enforce_timeouts_once(
                &mut conn,
                &autumn_harvest::telemetry::NoOpMetrics,
                Duration::from_secs(5),
                &None,
                &[],
                None,
                None,
                60,
                &autumn_harvest::payload_codec::PayloadCodecs::default(),
                0,
            ),
        )
        .await
        .expect("timeout pass");
    settle_background_publisher().await;

    assert_published_after_commit(&channel, waiter_task);
}

// ---------------------------------------------------------------------------
// Batched acks (issue #1429 item 8)
// ---------------------------------------------------------------------------

/// Activity runs that started, for the ack-before-start check.
static PROBE_ACTIVITY_STARTS: AtomicUsize = AtomicUsize::new(0);

fn probe_echo_activity(
    _ctx: &autumn_harvest::ActivityContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    PROBE_ACTIVITY_STARTS.fetch_add(1, Ordering::SeqCst);
    Box::pin(async move { Ok(input) })
}

/// Counts single acks and batched acks, and checks that no activity of a read
/// starts before that read is acked.
#[derive(Debug, Default)]
struct AckCountingDispatch {
    inner: MemoryDispatch,
    single: AtomicUsize,
    batches: AtomicUsize,
    /// Activity leases acked so far.
    activity_acked: AtomicUsize,
    /// Batches that found an activity already running before its ack.
    early_starts: AtomicUsize,
}

#[async_trait::async_trait]
impl TaskDispatch for AckCountingDispatch {
    async fn publish(
        &self,
        hints: &[autumn_harvest::dispatch::DispatchHint],
    ) -> autumn_harvest::HarvestResult<()> {
        self.inner.publish(hints).await
    }

    async fn next(
        &self,
        queues: &[String],
        consumer: &str,
        max: usize,
        wait: Duration,
    ) -> autumn_harvest::HarvestResult<Vec<autumn_harvest::dispatch::DispatchLease>> {
        self.inner.next(queues, consumer, max, wait).await
    }

    async fn ack(
        &self,
        lease: &autumn_harvest::dispatch::DispatchLease,
    ) -> autumn_harvest::HarvestResult<()> {
        self.single.fetch_add(1, Ordering::SeqCst);
        self.inner.ack(lease).await
    }

    async fn ack_many(
        &self,
        leases: &[autumn_harvest::dispatch::DispatchLease],
    ) -> autumn_harvest::HarvestResult<()> {
        self.batches.fetch_add(1, Ordering::SeqCst);
        // A worker that starts tasks before the ack would run them during this
        // pause, and the start count would pass the acked count.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = AtomicUsize::load(&PROBE_ACTIVITY_STARTS, Ordering::SeqCst);
        if started > AtomicUsize::load(&self.activity_acked, Ordering::SeqCst) {
            self.early_starts.fetch_add(1, Ordering::SeqCst);
        }
        let activities = leases
            .iter()
            .filter(|lease| lease.kind == Some(autumn_harvest::dispatch::DispatchKind::Activity))
            .count();
        self.activity_acked.fetch_add(activities, Ordering::SeqCst);
        self.inner.ack_many(leases).await
    }

    async fn release(
        &self,
        lease: &autumn_harvest::dispatch::DispatchLease,
        delay: Duration,
    ) -> autumn_harvest::HarvestResult<()> {
        self.inner.release(lease, delay).await
    }

    async fn maintain(
        &self,
        queues: &[String],
    ) -> autumn_harvest::HarvestResult<autumn_harvest::dispatch::DispatchMaintenance> {
        self.inner.maintain(queues).await
    }
}

/// The worker disposes of each read with one `ack_many`, never one `ack` per
/// lease. It starts no task of a read before that read is acked (issue #1429).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_worker_acks_each_read_in_one_batch() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let channel = Arc::new(AckCountingDispatch::default());
    autumn_harvest::dispatch::install(
        Arc::clone(&channel) as Arc<dyn TaskDispatch>,
        dispatch_settings(),
    );
    let _guard = InstalledGuard;

    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "dispatch_two_activities").await;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info("dispatch_two_activities", two_activity_workflow)],
        vec![act_info("echo", probe_echo_activity, None)],
        empty_shared_state(),
    ));
    PROBE_ACTIVITY_STARTS.store(0, Ordering::SeqCst);

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    })
    .await;

    assert_eq!(
        AtomicUsize::load(&channel.early_starts, Ordering::SeqCst),
        0,
        "an activity started before the read that claimed it was acked"
    );
    assert!(
        AtomicUsize::load(&PROBE_ACTIVITY_STARTS, Ordering::SeqCst) >= 2,
        "both activities must run"
    );

    assert_eq!(
        AtomicUsize::load(&channel.single, Ordering::SeqCst),
        0,
        "the worker must not ack one lease at a time"
    );
    assert!(
        AtomicUsize::load(&channel.batches, Ordering::SeqCst) > 0,
        "the worker must ack through ack_many"
    );
    assert_eq!(channel.inner.outstanding_leases(), 0);
}

// ---------------------------------------------------------------------------
// Reconcile sweep lease (issue #1429 item 10)
// ---------------------------------------------------------------------------

/// A channel whose reconcile leases a test grants, denies or fails. It keeps
/// saved cursors in memory, like a lease store would.
#[derive(Debug, Default)]
struct LeaseSwitchDispatch {
    inner: MemoryDispatch,
    grant: std::sync::atomic::AtomicBool,
    fail_lease: std::sync::atomic::AtomicBool,
    releases: AtomicUsize,
    cursors: std::sync::Mutex<std::collections::HashMap<String, String>>,
    saves: std::sync::Mutex<Vec<(String, Option<String>)>>,
    /// Task ids of each publish call, in order.
    publishes: std::sync::Mutex<Vec<Vec<uuid::Uuid>>>,
    /// Consumer that holds each granted queue.
    holders: std::sync::Mutex<std::collections::HashMap<String, String>>,
}

#[async_trait::async_trait]
impl TaskDispatch for LeaseSwitchDispatch {
    async fn publish(
        &self,
        hints: &[autumn_harvest::dispatch::DispatchHint],
    ) -> autumn_harvest::HarvestResult<()> {
        self.publishes
            .lock()
            .expect("publish log")
            .push(hints.iter().map(|hint| hint.task_id).collect());
        self.inner.publish(hints).await
    }

    async fn next(
        &self,
        queues: &[String],
        consumer: &str,
        max: usize,
        wait: Duration,
    ) -> autumn_harvest::HarvestResult<Vec<autumn_harvest::dispatch::DispatchLease>> {
        self.inner.next(queues, consumer, max, wait).await
    }

    async fn ack(
        &self,
        lease: &autumn_harvest::dispatch::DispatchLease,
    ) -> autumn_harvest::HarvestResult<()> {
        self.inner.ack(lease).await
    }

    async fn release(
        &self,
        lease: &autumn_harvest::dispatch::DispatchLease,
        delay: Duration,
    ) -> autumn_harvest::HarvestResult<()> {
        self.inner.release(lease, delay).await
    }

    async fn maintain(
        &self,
        queues: &[String],
    ) -> autumn_harvest::HarvestResult<autumn_harvest::dispatch::DispatchMaintenance> {
        self.inner.maintain(queues).await
    }

    async fn hold_reconcile_leases(
        &self,
        queues: &[String],
        consumer: &str,
        _ttl: Duration,
    ) -> autumn_harvest::HarvestResult<Vec<autumn_harvest::dispatch::ReconcileLease>> {
        if std::sync::atomic::AtomicBool::load(&self.fail_lease, Ordering::SeqCst) {
            return Err(autumn_harvest::HarvestError::Dispatch(
                "injected lease failure".to_string(),
            ));
        }
        if !std::sync::atomic::AtomicBool::load(&self.grant, Ordering::SeqCst) {
            return Ok(Vec::new());
        }
        let cursors = self.cursors.lock().expect("cursors");
        let mut holders = self.holders.lock().expect("holders");
        Ok(queues
            .iter()
            .map(|queue| {
                let previous = holders.insert(queue.clone(), consumer.to_owned());
                autumn_harvest::dispatch::ReconcileLease {
                    queue: queue.clone(),
                    cursor: cursors.get(queue).cloned(),
                    renewed: previous.as_deref() == Some(consumer),
                }
            })
            .collect())
    }

    async fn save_reconcile_cursors(
        &self,
        _consumer: &str,
        cursors: &[(String, Option<String>)],
    ) -> autumn_harvest::HarvestResult<()> {
        {
            let mut stored = self.cursors.lock().expect("cursors");
            for (queue, cursor) in cursors {
                match cursor {
                    Some(cursor) => stored.insert(queue.clone(), cursor.clone()),
                    None => stored.remove(queue),
                };
            }
        }
        self.saves
            .lock()
            .expect("saves")
            .extend(cursors.iter().cloned());
        Ok(())
    }

    async fn release_reconcile_leases(
        &self,
        _queues: &[String],
        _consumer: &str,
    ) -> autumn_harvest::HarvestResult<()> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The queue the sweep-lease cases use, so no other suite's worker claims
/// their rows.
const LEASE_QUEUE: &str = "dispatch_lease_q";

/// Start a workflow with no channel installed, so no reference names its row.
async fn start_unpublished(url: &str) -> (ExecutionId, uuid::Uuid) {
    autumn_harvest::dispatch::uninstall();
    let mut conn = connect(url).await;
    let exec_id = start_on(&mut conn, "dispatch_trivial", LEASE_QUEUE).await;
    let task_id = workflow_task_id(&mut conn, exec_id).await;
    (exec_id, task_id)
}

/// A worker that does not hold a queue's sweep lease leaves the sweep to the
/// holder. Once it holds the lease, its sweep republishes the row (issue #1429).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_sweeps_only_the_queues_whose_lease_it_holds() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let (exec_id, task_id) = start_unpublished(&url).await;

    let channel = Arc::new(LeaseSwitchDispatch::default());
    autumn_harvest::dispatch::install(
        Arc::clone(&channel) as Arc<dyn TaskDispatch>,
        dispatch_settings(),
    );
    let _guard = InstalledGuard;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker_with(
        worker_config(LEASE_QUEUE, vec![ShardId::new(0)]),
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        // Five reconcile intervals with the lease held by a peer.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            !channel.inner.published_ids().contains(&task_id),
            "a worker without the lease must not republish the row"
        );

        channel.grant.store(true, Ordering::SeqCst);
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    })
    .await;

    assert!(channel.inner.published_ids().contains(&task_id));
}

/// A sweep whose publish fails makes no further Redis call, so the fallback
/// is not delayed. The lease expires. A stopping worker then releases its
/// leases, even after its owner uninstalled the channel (issue #1429).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_publish_keeps_the_lease_and_a_stop_releases_it() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let (exec_id, _task_id) = start_unpublished(&url).await;

    let channel = Arc::new(LeaseSwitchDispatch::default());
    channel.grant.store(true, Ordering::SeqCst);
    channel.inner.fail_next(usize::MAX);
    autumn_harvest::dispatch::install(
        Arc::clone(&channel) as Arc<dyn TaskDispatch>,
        dispatch_settings(),
    );
    let _guard = InstalledGuard;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker_with(
        worker_config(LEASE_QUEUE, vec![ShardId::new(0)]),
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        // The sweep publish fails, and the Postgres fallback runs the row.
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
        assert_eq!(
            AtomicUsize::load(&channel.releases, Ordering::SeqCst),
            0,
            "a failed publish must not call Redis again to release the lease"
        );
        // `HarvestRunner::stop` uninstalls the channel before it joins the
        // worker. The release at exit must not depend on the slot.
        autumn_harvest::dispatch::uninstall();
    })
    .await;

    assert!(
        AtomicUsize::load(&channel.releases, Ordering::SeqCst) >= 1,
        "a stopping worker must release its leases"
    );
}

/// A lease call that fails does not stop the sweep. The durability floor
/// fails open and sweeps every queue (issue #1429).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_lease_call_sweeps_every_queue() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let (exec_id, task_id) = start_unpublished(&url).await;

    let channel = Arc::new(LeaseSwitchDispatch::default());
    channel.fail_lease.store(true, Ordering::SeqCst);
    autumn_harvest::dispatch::install(
        Arc::clone(&channel) as Arc<dyn TaskDispatch>,
        dispatch_settings(),
    );
    let _guard = InstalledGuard;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker_with(
        worker_config(LEASE_QUEUE, vec![ShardId::new(0)]),
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    })
    .await;

    assert!(channel.inner.published_ids().contains(&task_id));
}

/// A new lease holder resumes the walk from the cursor the last holder saved
/// (issue #1429).
///
/// The saved cursor points past the first two pages of gated rows. A fresh
/// worker's first sweep must publish the third page, which holds the
/// claimable row. A worker that starts at the top publishes the first page.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_lease_holder_resumes_the_walk_from_the_saved_cursor() {
    let _serial = DISPATCH_SERIAL.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = unique_queue("lease_cursor");
    let _cleanup = SeededQueueGuard {
        url: url.clone(),
        queue: queue.clone(),
    };

    let mut conn = connect(&url).await;
    autumn_harvest::dispatch::uninstall();
    seed_gated_rows(&mut conn, &queue, 250, 5).await;
    let exec_id = start_on(&mut conn, "dispatch_trivial", &queue).await;
    let claimable = workflow_task_id(&mut conn, exec_id).await;

    let batch = 100;
    let page_one = autumn_harvest::queue::due_dispatch_hints_page(&mut conn, &queue, batch, None)
        .await
        .expect("first page");
    let page_two = autumn_harvest::queue::due_dispatch_hints_page(
        &mut conn,
        &queue,
        batch,
        page_one.cursor.as_ref(),
    )
    .await
    .expect("second page");
    let resume_at = page_two.cursor.expect("a full page carries a cursor");

    let channel = Arc::new(LeaseSwitchDispatch::default());
    channel.grant.store(true, Ordering::SeqCst);
    channel
        .cursors
        .lock()
        .expect("cursors")
        .insert(queue.clone(), resume_at.encode());
    autumn_harvest::dispatch::install(
        Arc::clone(&channel) as Arc<dyn TaskDispatch>,
        DispatchSettings {
            reconcile_batch: batch,
            ..dispatch_settings()
        },
    );
    let _guard = InstalledGuard;

    let pool = build_pool(&url);
    let worker = Arc::new(make_worker_with(
        worker_config(&queue, vec![ShardId::new(0)]),
        vec![wf_info("dispatch_trivial", trivial_workflow)],
        vec![],
        empty_shared_state(),
    ));

    let mut check = connect(&url).await;
    with_worker(worker, pool, async {
        wait_for_state(&mut check, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    })
    .await;

    let publishes = channel.publishes.lock().expect("publish log").clone();
    assert!(!publishes.is_empty(), "the sweep publishes");
    let first_sweep = &publishes[0];
    assert!(
        first_sweep.contains(&claimable),
        "the first sweep must resume at the third page"
    );
    let page_one_ids: Vec<uuid::Uuid> = page_one.hints.iter().map(|hint| hint.task_id).collect();
    assert!(
        first_sweep.iter().all(|id| !page_one_ids.contains(id)),
        "the first sweep must not restart at the top"
    );
    let saves = channel.saves.lock().expect("saves").clone();
    assert!(
        saves.contains(&(queue.clone(), None)),
        "a sweep that wraps must clear the saved cursor"
    );
}
