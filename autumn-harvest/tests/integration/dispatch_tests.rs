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
        dr_fencing: false,
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
    let telemetry = Arc::new(TelemetryConfig::builder().build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        shared_state,
        telemetry,
    ));
    Worker::new(worker_config("default", vec![ShardId::new(0)]), registry)
        .expect("worker should build")
}

async fn start(conn: &mut AsyncPgConnection, workflow_name: &'static str) -> ExecutionId {
    let workflow_id = format!("{workflow_name}-{}", uuid::Uuid::new_v4());
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id: &workflow_id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input: serde_json::Value::Null,
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
        "every delivered reference must be acked or released"
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
    FenceRegistry::register(shard, generation);
    FenceRegistry::set_default_shard(shard);
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
