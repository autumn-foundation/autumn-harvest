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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::builder::{HarvestBuilder, WorkerConfig};
use autumn_harvest::circuit_breaker::CircuitBreakerRegistry;
use autumn_harvest::error::TimeoutType;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::policy::{CircuitBreakerPolicy, JitterPolicy, RetryPolicy};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::telemetry::MetricsRecorder;
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

/// What a retry saw: the heartbeat checkpoint and the previous failure.
type Seen = (Option<serde_json::Value>, Option<String>);

/// What attempt 2 saw, by activity name.
static SEEN_BY_RETRY: Mutex<Option<HashMap<String, Seen>>> = Mutex::new(None);

fn seen_by_retry(name: &str) -> Option<Seen> {
    SEEN_BY_RETRY
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|m| m.get(name).cloned())
}

/// The checkpoint that attempt 1 sends before it hangs.
fn checkpoint() -> serde_json::Value {
    serde_json::json!({"step": 7})
}

/// Attempt 1 sends one checkpoint, then never returns. A later attempt
/// records what it sees, then succeeds.
fn hang_first(ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let info = ctx.info();
        if info.attempt == 1 {
            ctx.heartbeat(checkpoint())
                .await
                .map_err(|e| e.to_string())?;
            record_start(&info.activity_type, info.attempt);
            std::future::pending::<()>().await;
        }
        let details = ctx
            .heartbeat_details::<serde_json::Value>()
            .map_err(|e| e.to_string())?;
        let previous = ctx.previous_failure().map(str::to_string);
        SEEN_BY_RETRY
            .lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .insert(info.activity_type.clone(), (details, previous));
        record_start(&info.activity_type, info.attempt);
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
    retry: Option<RetryPolicy>,
    bounds: Bounds,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_timeout_retry_tests",
        default_retry_policy: retry,
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
                // Only the test sweeps. The worker scanner waits the longest
                // interval, so a test controls which sweep enforces.
                scanner: autumn_harvest::scanner_lease::ScannerConfig {
                    timeout_interval: Some(autumn_harvest::scanner_lease::MAX_SCANNER_INTERVAL),
                    ..autumn_harvest::scanner_lease::ScannerConfig::default()
                },
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
/// heartbeat scan reads `started_at`. Return the row as a scan reads it.
async fn backdate(conn: &mut AsyncPgConnection, task_id: Uuid, secs: i64) -> TaskQueueItem {
    diesel::update(harvest_task_queue::table.find(task_id))
        .set((
            harvest_task_queue::started_at.eq(Some(Utc::now() - chrono::Duration::seconds(secs))),
            harvest_task_queue::last_heartbeat_at.eq(None::<chrono::DateTime<Utc>>),
        ))
        .returning(TaskQueueItem::as_returning())
        .get_result(conn)
        .await
        .expect("backdate the attempt")
}

/// Record crash strikes on the task, as the orphan reclaimer does.
async fn set_crash_strikes(conn: &mut AsyncPgConnection, task_id: Uuid, strikes: i32) {
    diesel::update(harvest_task_queue::table.find(task_id))
        .set(harvest_task_queue::crash_strikes.eq(strikes))
        .execute(conn)
        .await
        .expect("set crash strikes");
}

/// Wait until the heartbeat flusher writes the checkpoint of attempt 1.
async fn wait_for_checkpoint(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while activity_task(conn, exec_id).await.heartbeat_details != Some(checkpoint()) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the checkpoint of attempt 1 must reach the task row within 30s"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Counts the metrics that a timeout retry records.
#[derive(Default)]
struct Counts {
    retried: AtomicUsize,
    tripped: AtomicUsize,
}

impl MetricsRecorder for Counts {
    fn record_activity_retried(&self, _activity: &str, _queue: &str) {
        self.retried.fetch_add(1, Ordering::SeqCst);
    }

    fn record_circuit_tripped(&self, _activity: &str) {
        self.tripped.fetch_add(1, Ordering::SeqCst);
    }
}

/// Read a counter. `AtomicUsize::load` clashes with Diesel's `load`.
fn read(counter: &AtomicUsize) -> usize {
    AtomicUsize::load(counter, Ordering::SeqCst)
}

/// A breaker that trips on the first failure of `activity`.
fn one_failure_breaker(activity: &str) -> CircuitBreakerRegistry {
    let policy = CircuitBreakerPolicy {
        failure_threshold: 1,
        window: Duration::from_secs(60),
        cooldown: Duration::from_secs(60),
    };
    CircuitBreakerRegistry::new(HashMap::from([(activity.to_string(), policy)]))
}

async fn sweep_with(
    conn: &mut AsyncPgConnection,
    metrics: &Counts,
    breakers: Option<&CircuitBreakerRegistry>,
) {
    timeout::enforce_timeouts_once(
        conn,
        metrics,
        Duration::from_secs(60),
        &None,
        &[ShardId::new(0)],
        breakers,
        None,
        60,
        &PayloadCodecs::default(),
        0,
    )
    .await
    .expect("timeout sweep");
}

async fn sweep(conn: &mut AsyncPgConnection) {
    sweep_with(conn, &Counts::default(), None).await;
}

/// The timeout types of all `ActivityTimedOut` events, and the number of
/// `ActivityCompleted` events.
async fn outcomes(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> (Vec<TimeoutType>, usize) {
    let history = store::load_history(conn, exec_id)
        .await
        .expect("load history")
        .events;
    let timed_out = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityTimedOut { timeout_type, .. } => Some(timeout_type.clone()),
            _ => None,
        })
        .collect();
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
///
/// Attempt 2 must see the checkpoint and the timeout of attempt 1. The
/// requeue must keep the crash strikes, count one retry and feed the breaker.
async fn timeout_then_complete(
    label: &str,
    activity: &'static str,
    retry: Option<RetryPolicy>,
    bounds: Bounds,
    timeout_type: TimeoutType,
) {
    let (url, _container) = setup_db().await;
    let queue = &unique_queue(label);
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, queue, activity).await;
    let worker = Running::spawn(
        &url,
        queue,
        activity_info(activity, hang_first, retry, bounds),
    );

    let first = wait_for_attempt(&mut conn, exec_id, activity, 1).await;
    wait_for_checkpoint(&mut conn, exec_id).await;
    set_crash_strikes(&mut conn, first.id, 1).await;
    backdate(&mut conn, first.id, PAST_LIMIT_SECS).await;
    let counts = Counts::default();
    let breakers = one_failure_breaker(activity);
    sweep_with(&mut conn, &counts, Some(&breakers)).await;
    assert_eq!(read(&counts.retried), 1, "one retry");
    assert_eq!(
        read(&counts.tripped),
        1,
        "a retried timeout feeds the breaker"
    );

    wait_for_state(&mut conn, exec_id, "COMPLETED").await;
    worker.stop().await;

    let task = activity_task(&mut conn, exec_id).await;
    assert_eq!(task.attempt, 2, "the timeout must start attempt 2");
    assert_eq!(
        task.crash_strikes, 1,
        "a timeout does not prove a clean run, so it keeps the crash strikes"
    );
    assert_eq!(
        outcomes(&mut conn, exec_id).await,
        (Vec::new(), 1),
        "a retried timeout appends no ActivityTimedOut"
    );
    let (details, previous) = seen_by_retry(activity).expect("attempt 2 must run");
    assert_eq!(
        details,
        Some(checkpoint()),
        "the retry keeps the checkpoint"
    );
    let previous = previous.unwrap_or_default();
    assert!(
        previous.contains(&timeout_type.to_string()),
        "attempt 2 must see the timeout of attempt 1, got {previous:?}"
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
    let retry = Some(exact_policy(3, Duration::from_millis(50)));
    let timeout_type = TimeoutType::StartToClose;
    timeout_then_complete("stc", "act_1870_stc_retry", retry, bounds, timeout_type).await;
}

/// A `Heartbeat` timeout with attempts left retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_timeout_with_attempts_left_runs_another_attempt() {
    let bounds = Bounds {
        heartbeat_timeout: Some(LIMIT),
        ..Bounds::default()
    };
    let retry = Some(exact_policy(3, Duration::from_millis(50)));
    let timeout_type = TimeoutType::Heartbeat;
    timeout_then_complete("hb", "act_1870_hb_retry", retry, bounds, timeout_type).await;
}

/// With no retry policy, a task has the default `max_attempts` of 3. A
/// timeout of attempt 1 then retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timeout_without_a_retry_policy_retries_at_the_default_attempts() {
    let bounds = Bounds {
        start_to_close: Some(LIMIT),
        ..Bounds::default()
    };
    let timeout_type = TimeoutType::StartToClose;
    timeout_then_complete("none", "act_1870_no_policy", None, bounds, timeout_type).await;
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
        Some(exact_policy(2, Duration::from_millis(50))),
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
        outcomes(&mut conn, exec_id).await,
        (vec![TimeoutType::StartToClose], 0),
        "only the last attempt appends ActivityTimedOut"
    );
}

/// A retry that cannot start before `schedule_to_close` is terminal at once.
/// It records a `ScheduleToClose` timeout, as a worker retry does. It does
/// not wait in the queue for the deadline scanner.
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
        Some(exact_policy(3, Duration::from_secs(7200))),
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
        outcomes(&mut conn, exec_id).await,
        (vec![TimeoutType::ScheduleToClose], 0)
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
        Some(exact_policy(2, Duration::from_millis(50))),
        bounds,
    );
    let worker = Running::spawn(&url, queue, info);

    let running = wait_for_attempt(&mut conn, exec_id, activity, 1).await;
    // The snapshot a second sweeper would hold: the expired attempt 1.
    let first = backdate(&mut conn, running.id, PAST_LIMIT_SECS).await;
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
    assert_eq!(outcomes(&mut conn, exec_id).await, (Vec::new(), 0));
    assert_eq!(execution_state(&mut conn, exec_id).await, "RUNNING");
}

/// A timeout after the run has ended starts no new attempt.
///
/// A workflow can fail while one of its activities still runs. A retry
/// would then run the handler again for a sealed run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timeout_after_the_run_ends_starts_no_new_attempt() {
    let (url, _container) = setup_db().await;
    let activity = "act_1870_sealed_run";
    let queue = &unique_queue("sealed");
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, queue, activity).await;
    let bounds = Bounds {
        start_to_close: Some(LIMIT),
        ..Bounds::default()
    };
    let info = activity_info(
        activity,
        hang_always,
        Some(exact_policy(3, Duration::from_millis(50))),
        bounds,
    );
    let worker = Running::spawn(&url, queue, info);

    let first = wait_for_attempt(&mut conn, exec_id, activity, 1).await;
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq("FAILED"))
        .execute(&mut conn)
        .await
        .expect("end the run");
    backdate(&mut conn, first.id, PAST_LIMIT_SECS).await;
    sweep(&mut conn).await;

    // Give a requeued task time to be claimed again.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let task = activity_task(&mut conn, exec_id).await;
    worker.stop().await;
    assert!(
        !started(activity, 2),
        "no attempt may start after the run ends"
    );
    assert_eq!((task.state.as_str(), task.attempt), ("FAILED", 1));
}
