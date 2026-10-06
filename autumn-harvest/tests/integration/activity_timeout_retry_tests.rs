#![cfg(feature = "db")]
//! A timed-out activity attempt follows the retry policy (issue #1870).
//!
//! A `StartToClose` or `Heartbeat` timeout with attempts left requeues the
//! task. `ActivityTimedOut` comes only after the last attempt, as for a
//! retryable `Err`.
//!
//! A test does not wait for a real timeout. It moves `started_at` back and
//! runs the timeout scanner.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::builder::{HarvestBuilder, WorkerConfig};
use autumn_harvest::error::TimeoutType;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::timeout::{self, TimeoutReason};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{WorkflowContext, store};

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
// DB setup.
// ---------------------------------------------------------------------------

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
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
// Handlers.
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Started attempts, as `(activity name, attempt)` pairs.
static STARTED: Mutex<Option<HashSet<(String, u32)>>> = Mutex::new(None);

fn record_start(name: &str, attempt: u32) {
    STARTED
        .lock()
        .unwrap()
        .get_or_insert_with(HashSet::new)
        .insert((name.to_string(), attempt));
}

fn started(name: &str, attempt: u32) -> bool {
    STARTED
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|s| s.contains(&(name.to_string(), attempt)))
}

/// Attempt 1 never returns and never heartbeats. A later attempt succeeds.
fn hang_first(ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let info = ctx.info();
        record_start(&info.activity_type, info.attempt);
        if info.attempt == 1 {
            std::future::pending::<()>().await;
        }
        Ok(serde_json::json!("done"))
    })
}

/// No attempt ever returns or heartbeats.
fn hang_always(ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let info = ctx.info();
        record_start(&info.activity_type, info.attempt);
        std::future::pending::<()>().await;
        Ok(serde_json::Value::Null)
    })
}

/// Calls the activity named in `input["activity"]` on the workflow queue.
fn wf_calls_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        let name = input["activity"].as_str().unwrap_or_default().to_string();
        ctx.execute_activity_raw(&name, input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

const WORKFLOW: &str = "wf_1870_calls_activity";

fn wf_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: WORKFLOW,
        module: "activity_timeout_retry_tests",
        handler: wf_calls_activity,
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

/// A retry policy with an exact delay, so a test can reason about deadlines.
fn exact_policy(max_attempts: u32, interval: Duration) -> RetryPolicy {
    let mut policy = RetryPolicy::fixed(max_attempts, interval);
    policy.jitter = JitterPolicy::None;
    policy
}

/// The timeout bounds of one activity type. `None` declares nothing.
#[derive(Clone, Copy, Default)]
struct Bounds {
    start_to_close: Option<Duration>,
    heartbeat_timeout: Option<Duration>,
    schedule_to_close: Option<Duration>,
}

fn activity_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    retry: RetryPolicy,
    bounds: Bounds,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_timeout_retry_tests",
        default_retry_policy: Some(retry),
        default_start_to_close: bounds.start_to_close,
        default_heartbeat_timeout: bounds.heartbeat_timeout,
        default_schedule_to_start: None,
        default_schedule_to_close: bounds.schedule_to_close,
        default_queue: None,
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

// ---------------------------------------------------------------------------
// Registry and worker.
// ---------------------------------------------------------------------------

fn registry(activity: ActivityInfo) -> Arc<HandlerRegistry> {
    let (registry, _dags, _schedules, _config) = HarvestBuilder::new()
        .workflows(vec![wf_info()])
        .activities(vec![activity])
        .worker(WorkerConfig::default())
        .build()
        .into_worker_parts();
    Arc::new(registry)
}

fn build_worker(queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: queue.to_string(),
                queues: vec![queue.to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 2,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::ZERO,
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                resident_workflows: true,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: HashMap::new(),
                scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
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

/// A running worker. Drop does not wait, so call [`Running::stop`].
struct Running {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

impl Running {
    fn spawn(url: &str, queue: &str, activity: ActivityInfo) -> Self {
        let worker = build_worker(queue, registry(activity));
        let pool = build_pool(url);
        let runner = Arc::clone(&worker);
        let handle = tokio::spawn(async move {
            runner.run(&pool).await;
        });
        Self { worker, handle }
    }

    /// A hung attempt never completes, so do not wait forever.
    async fn stop(self) {
        self.worker.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(10), self.handle).await;
    }
}

// ---------------------------------------------------------------------------
// Seed and read helpers.
// ---------------------------------------------------------------------------

/// A queue name no earlier run used. A shared database can keep old rows.
fn unique_queue(label: &str) -> String {
    format!("q1870-{label}-{}", Uuid::new_v4().simple())
}

async fn seed_workflow(conn: &mut AsyncPgConnection, queue: &str, activity: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let input = serde_json::json!({"activity": activity});
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: WORKFLOW,
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone().into(),
        parent_id: None,
        queue_name: queue,
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
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
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
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

    let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");
    exec_id
}

async fn execution_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("reload workflow execution")
        .state
}

async fn find_activity_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Option<TaskQueueItem> {
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(TaskQueueItem::as_select())
        .first(conn)
        .await
        .optional()
        .expect("load the activity task")
}

async fn activity_task(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> TaskQueueItem {
    find_activity_task(conn, exec_id)
        .await
        .expect("the workflow must schedule its activity")
}

/// Wait until `attempt` of `activity` runs, then return its `RUNNING` row.
async fn wait_for_attempt(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    activity: &str,
    attempt: u32,
) -> TaskQueueItem {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let task = find_activity_task(conn, exec_id).await;
        if let Some(task) = &task
            && task.state == "RUNNING"
            && u32::try_from(task.attempt) == Ok(attempt)
            && started(activity, attempt)
        {
            return task.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "attempt {attempt} of {activity} must run within 30s; task={task:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId, want: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let state = execution_state(conn, exec_id).await;
        if state == want {
            return;
        }
        let history = store::load_history(conn, exec_id).await.map(|h| h.events);
        assert!(
            tokio::time::Instant::now() < deadline,
            "the workflow must reach {want}; state={state} history={history:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Make the attempt look `secs` old. Clear the heartbeat stamp too, so the
/// heartbeat scan reads `started_at`.
async fn backdate(conn: &mut AsyncPgConnection, task_id: Uuid, secs: i64) {
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET started_at = NOW() - ($2 * INTERVAL '1 second'), last_heartbeat_at = NULL \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::BigInt, _>(secs)
    .execute(conn)
    .await
    .expect("backdate the attempt");
}

async fn sweep(conn: &mut AsyncPgConnection) {
    timeout::enforce_timeouts_once(
        conn,
        &NoOpMetrics,
        Duration::from_secs(60),
        &None,
        &[ShardId::new(0)],
        None,
        None,
        60,
        &PayloadCodecs::default(),
        0,
    )
    .await
    .expect("timeout sweep");
}

/// Count `ActivityTimedOut` events of `timeout_type` and `ActivityCompleted`
/// events.
async fn outcomes(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    timeout_type: TimeoutType,
) -> (usize, usize) {
    let history = store::load_history(conn, exec_id)
        .await
        .expect("load history")
        .events;
    let timed_out = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityTimedOut { timeout_type: t, .. } if *t == timeout_type))
        .count();
    let completed = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
        .count();
    (timed_out, completed)
}

const LIMIT: Duration = Duration::from_secs(30);
const PAST_LIMIT_SECS: i64 = 31;

/// Run attempt 1 of `activity` into a timeout with attempts left. Attempt 2
/// must then complete the workflow.
async fn timeout_then_complete(
    label: &str,
    activity: &'static str,
    bounds: Bounds,
    timeout_type: TimeoutType,
) {
    let (url, _container) = setup_db().await;
    let queue = &unique_queue(label);
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, queue, activity).await;
    let info = activity_info(
        activity,
        hang_first,
        exact_policy(3, Duration::from_millis(50)),
        bounds,
    );
    let worker = Running::spawn(&url, queue, info);

    let first = wait_for_attempt(&mut conn, exec_id, activity, 1).await;
    backdate(&mut conn, first.id, PAST_LIMIT_SECS).await;
    sweep(&mut conn).await;

    wait_for_state(&mut conn, exec_id, "COMPLETED").await;
    worker.stop().await;

    let task = activity_task(&mut conn, exec_id).await;
    assert_eq!(task.attempt, 2, "the timeout must start attempt 2");
    assert_eq!(
        outcomes(&mut conn, exec_id, timeout_type).await,
        (0, 1),
        "a retried timeout appends no ActivityTimedOut"
    );
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

/// The issue repro: a `StartToClose` timeout with attempts left retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_to_close_timeout_with_attempts_left_runs_another_attempt() {
    let bounds = Bounds {
        start_to_close: Some(LIMIT),
        ..Bounds::default()
    };
    timeout_then_complete(
        "stc",
        "act_1870_stc_retry",
        bounds,
        TimeoutType::StartToClose,
    )
    .await;
}

/// A `Heartbeat` timeout with attempts left retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_timeout_with_attempts_left_runs_another_attempt() {
    let bounds = Bounds {
        heartbeat_timeout: Some(LIMIT),
        ..Bounds::default()
    };
    timeout_then_complete("hb", "act_1870_hb_retry", bounds, TimeoutType::Heartbeat).await;
}

/// The timeout of the last attempt fails the activity call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_on_the_last_attempt_fails_the_activity() {
    let (url, _container) = setup_db().await;
    let activity = "act_1870_last_attempt";
    let queue = &unique_queue("last");
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, queue, activity).await;
    let bounds = Bounds {
        start_to_close: Some(LIMIT),
        ..Bounds::default()
    };
    let info = activity_info(
        activity,
        hang_always,
        exact_policy(2, Duration::from_millis(50)),
        bounds,
    );
    let worker = Running::spawn(&url, queue, info);

    for attempt in 1..=2 {
        let task = wait_for_attempt(&mut conn, exec_id, activity, attempt).await;
        backdate(&mut conn, task.id, PAST_LIMIT_SECS).await;
        sweep(&mut conn).await;
    }

    wait_for_state(&mut conn, exec_id, "FAILED").await;
    worker.stop().await;

    let task = activity_task(&mut conn, exec_id).await;
    assert_eq!((task.state.as_str(), task.attempt), ("FAILED", 2));
    assert_eq!(
        outcomes(&mut conn, exec_id, TimeoutType::StartToClose).await,
        (1, 0),
        "only the last attempt appends ActivityTimedOut"
    );
}

/// A retry that cannot start before `schedule_to_close` is terminal at once.
/// It does not wait in the queue for the deadline scanner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_retry_past_schedule_to_close_fails_the_activity() {
    let (url, _container) = setup_db().await;
    let activity = "act_1870_past_deadline";
    let queue = &unique_queue("deadline");
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, queue, activity).await;
    let bounds = Bounds {
        start_to_close: Some(LIMIT),
        schedule_to_close: Some(Duration::from_secs(3600)),
        ..Bounds::default()
    };
    let info = activity_info(
        activity,
        hang_always,
        exact_policy(3, Duration::from_secs(7200)),
        bounds,
    );
    let worker = Running::spawn(&url, queue, info);

    let task = wait_for_attempt(&mut conn, exec_id, activity, 1).await;
    backdate(&mut conn, task.id, PAST_LIMIT_SECS).await;
    sweep(&mut conn).await;

    wait_for_state(&mut conn, exec_id, "FAILED").await;
    worker.stop().await;

    let task = activity_task(&mut conn, exec_id).await;
    assert_eq!((task.state.as_str(), task.attempt), ("FAILED", 1));
    assert_eq!(
        outcomes(&mut conn, exec_id, TimeoutType::StartToClose).await,
        (1, 0)
    );
}

/// A stale scan snapshot of attempt 1 must not time out attempt 2.
///
/// One sweeper can requeue attempt 1 while a second sweeper still holds the
/// old scan. A worker then claims attempt 2. The row is `RUNNING` again, but
/// it is a new attempt with a new `started_at`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_snapshot_does_not_time_out_the_next_attempt() {
    let (url, _container) = setup_db().await;
    let activity = "act_1870_stale_snapshot";
    let queue = &unique_queue("stale");
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, queue, activity).await;
    let bounds = Bounds {
        start_to_close: Some(LIMIT),
        ..Bounds::default()
    };
    let info = activity_info(
        activity,
        hang_always,
        exact_policy(2, Duration::from_millis(50)),
        bounds,
    );
    let worker = Running::spawn(&url, queue, info);

    let first = wait_for_attempt(&mut conn, exec_id, activity, 1).await;
    backdate(&mut conn, first.id, PAST_LIMIT_SECS).await;
    sweep(&mut conn).await;
    wait_for_attempt(&mut conn, exec_id, activity, 2).await;

    timeout::enforce_activity_timeout_for_task(
        &mut conn,
        &first,
        &TimeoutReason::StartToClose,
        &PayloadCodecs::default(),
    )
    .await
    .expect("enforce the stale snapshot");

    let task = activity_task(&mut conn, exec_id).await;
    worker.stop().await;
    assert_eq!((task.state.as_str(), task.attempt), ("RUNNING", 2));
    assert_eq!(
        outcomes(&mut conn, exec_id, TimeoutType::StartToClose).await,
        (0, 0)
    );
    assert_eq!(execution_state(&mut conn, exec_id).await, "RUNNING");
}
