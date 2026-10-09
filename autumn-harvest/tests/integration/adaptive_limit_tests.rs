#![cfg(feature = "db")]
//! End-to-end tests for the adaptive concurrency limit (issue #1836).
//!
//! A real worker runs workflows against Postgres. Each workflow calls one
//! activity. The handlers count their own concurrency:
//!
//! - A limited type never runs more attempts at once than its cap.
//! - A type without a limit is not capped by the limit of another type.
//! - The limit is off by default.
//! - Against a dependency whose latency grows above a knee, the worker feeds
//!   the handler latency to the limit. The cap grows from its start value.
//! - The worker exports the limit state as metrics.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly. Otherwise a fresh testcontainers Postgres boots.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use autumn_harvest::adaptive_limit::AdaptiveLimitConfig;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::policy::AdaptiveLimitPolicy;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{WorkflowContext, store};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup
// ---------------------------------------------------------------------------

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(16)
        .build()
        .expect("pool build")
}

// ---------------------------------------------------------------------------
// Concurrency probes
// ---------------------------------------------------------------------------

/// The live and the highest concurrency of one activity type.
#[derive(Default)]
struct Gauge {
    now: AtomicU32,
    peak: AtomicU32,
    done: AtomicU32,
    /// Highest attempt number a handler saw.
    max_attempt: AtomicU32,
}

static GAUGES: LazyLock<Mutex<HashMap<String, Arc<Gauge>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn gauge(activity: &str) -> Arc<Gauge> {
    Arc::clone(
        GAUGES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(activity.to_owned())
            .or_default(),
    )
}

/// Marks one running attempt. The drop marks its end.
struct Running(Arc<Gauge>);

impl Running {
    /// Start one attempt. Returns the marker and the concurrency, itself
    /// included.
    fn start(activity: &str) -> (Self, u32) {
        let g = gauge(activity);
        let n = g.now.fetch_add(1, Ordering::SeqCst) + 1;
        g.peak.fetch_max(n, Ordering::SeqCst);
        (Self(g), n)
    }
}

impl Gauge {
    // `diesel::RunQueryDsl::load` shadows the inherent `load` in method
    // syntax, so these reads use the path form.
    fn peak(&self) -> u32 {
        AtomicU32::load(&self.peak, Ordering::SeqCst)
    }

    fn done(&self) -> u32 {
        AtomicU32::load(&self.done, Ordering::SeqCst)
    }

    fn max_attempt(&self) -> u32 {
        AtomicU32::load(&self.max_attempt, Ordering::SeqCst)
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.0.now.fetch_sub(1, Ordering::SeqCst);
        self.0.done.fetch_add(1, Ordering::SeqCst);
    }
}

/// One published limit state: the cap, the in-flight count and the
/// baseline in seconds.
type LimitSample = (u32, u32, Option<f64>);

/// The last limit state that the worker published, per activity type.
#[derive(Default)]
struct LimitMetrics {
    last: Mutex<HashMap<String, LimitSample>>,
    peak_in_flight: Mutex<HashMap<String, u32>>,
    deferred: Mutex<HashMap<String, u64>>,
}

impl LimitMetrics {
    fn last(&self, activity: &str) -> Option<LimitSample> {
        self.last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(activity)
            .copied()
    }

    fn deferred(&self, activity: &str) -> u64 {
        self.deferred
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(activity)
            .copied()
            .unwrap_or(0)
    }

    fn peak_in_flight(&self, activity: &str) -> u32 {
        self.peak_in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(activity)
            .copied()
            .unwrap_or(0)
    }
}

impl MetricsRecorder for LimitMetrics {
    fn record_activity_concurrency_limit(
        &self,
        activity: &str,
        state: &autumn_harvest::adaptive_limit::LimitSnapshot,
    ) {
        let (limit, in_flight) = (state.limit, state.in_flight);
        let baseline_secs = state.baseline.map(|b| b.as_secs_f64());
        self.last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(activity.to_owned(), (limit, in_flight, baseline_secs));
        let mut peaks = self
            .peak_in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let peak = peaks.entry(activity.to_owned()).or_default();
        *peak = (*peak).max(in_flight);
        drop(peaks);
    }

    fn record_activity_concurrency_deferred(&self, activity: &str) {
        *self
            .deferred
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(activity.to_owned())
            .or_default() += 1;
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Takes 150 ms whatever the load.
fn slow_call(ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    let (running, _) = Running::start(ctx.activity_type());
    running
        .0
        .max_attempt
        .fetch_max(ctx.attempt(), Ordering::SeqCst);
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(running);
        Ok(input)
    })
}

/// Never answers in time, like a hung dependency.
fn hung_call(ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    let (running, _) = Running::start(ctx.activity_type());
    Box::pin(async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        drop(running);
        Ok(serde_json::Value::Null)
    })
}

/// Answers 20 ms after a 300 ms attempt deadline, before the timeout scan
/// and the cancel observer can act.
fn late_call(ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    let (running, _) = Running::start(ctx.activity_type());
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(320)).await;
        drop(running);
        Ok(input)
    })
}

/// Answers in 1.2 s, after a 1 s schedule-to-close budget.
fn overdue_call(ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    let (running, _) = Running::start(ctx.activity_type());
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        drop(running);
        Ok(input)
    })
}

/// Answers in 20 ms.
fn fast_call(ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    let (running, _) = Running::start(ctx.activity_type());
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(running);
        Ok(input)
    })
}

/// Concurrency above which [`knee_call`] slows down.
const KNEE: u32 = 8;

/// Answers in 60 ms up to [`KNEE`] concurrent calls. Above the knee, latency
/// grows in proportion to the concurrency, as for a pool of [`KNEE`]
/// servers.
fn knee_call(ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    let (running, n) = Running::start(ctx.activity_type());
    let load = (f64::from(n) / f64::from(KNEE)).max(1.0);
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(60).mul_f64(load)).await;
        drop(running);
        Ok(input)
    })
}

/// Calls the activity named in the input.
fn wf_call(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let activity = input["activity"].as_str().unwrap_or_default().to_owned();
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw_with_opts(&activity, input, &queue, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

fn wf_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "wf_adaptive_limit_call",
        module: "adaptive_limit_tests",
        handler: wf_call,
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

fn act_info(name: &'static str, handler: autumn_harvest::info::ActivityHandlerFn) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "adaptive_limit_tests",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(30)),
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
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

// ---------------------------------------------------------------------------
// Worker and seeding
// ---------------------------------------------------------------------------

/// A worker with 32 activity slots and the given adaptive limit config.
fn build_worker(
    worker_id: &str,
    queue: &str,
    activities: Vec<ActivityInfo>,
    metrics: Arc<LimitMetrics>,
    limit: Option<AdaptiveLimitConfig>,
    labels: HashMap<String, String>,
) -> (Arc<Worker>, Arc<HandlerRegistry>) {
    build_worker_polling(
        worker_id,
        queue,
        activities,
        metrics,
        limit,
        labels,
        Duration::from_millis(10),
    )
}

/// [`build_worker`] with an explicit idle poll interval.
fn build_worker_polling(
    worker_id: &str,
    queue: &str,
    activities: Vec<ActivityInfo>,
    metrics: Arc<LimitMetrics>,
    limit: Option<AdaptiveLimitConfig>,
    labels: HashMap<String, String>,
    poll_interval: Duration,
) -> (Arc<Worker>, Arc<HandlerRegistry>) {
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    let mut registry = HandlerRegistry::with_state_and_telemetry(
        vec![wf_info()],
        activities,
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    );
    if let Some(limit) = limit {
        registry = registry.with_adaptive_limit(limit);
    }
    let registry = Arc::new(registry);
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: worker_id.to_string(),
                queues: vec![queue.to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 8,
                max_concurrent_activities: 32,
                poll_interval,
                shutdown_timeout: Duration::from_secs(5),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::ZERO,
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
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
                labels,
                queue_weights: HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
                fairness_keys: false,
            },
            Arc::clone(&registry),
        )
        .expect("worker should build"),
    );
    (worker, registry)
}

/// Start one workflow that calls `activity` on `queue`.
async fn seed_workflow(conn: &mut AsyncPgConnection, queue: &str, activity: &str) -> ExecutionId {
    let input = serde_json::json!({ "activity": activity });
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "wf_adaptive_limit_call",
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
        tenant: None,
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

/// Poll `cond` every 20 ms until it holds, or panic after `timeout`.
async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(timeout, async {
        while !cond().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out after {timeout:?} waiting for {what}"));
}

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

/// Run `per_type` workflows for each activity until every run completes.
async fn run_to_completion(
    url: &str,
    queue: &str,
    activities: Vec<ActivityInfo>,
    limit: Option<AdaptiveLimitConfig>,
    per_type: u32,
) -> (Arc<LimitMetrics>, Arc<HandlerRegistry>) {
    run_with_labels(url, queue, activities, limit, per_type, HashMap::new()).await
}

/// [`run_to_completion`] on a worker with the given capability labels.
async fn run_with_labels(
    url: &str,
    queue: &str,
    activities: Vec<ActivityInfo>,
    limit: Option<AdaptiveLimitConfig>,
    per_type: u32,
    labels: HashMap<String, String>,
) -> (Arc<LimitMetrics>, Arc<HandlerRegistry>) {
    let names: Vec<&'static str> = activities.iter().map(|a| a.name).collect();
    let metrics = Arc::new(LimitMetrics::default());
    let (worker, registry) = build_worker(
        &format!("{queue}-worker"),
        queue,
        activities,
        Arc::clone(&metrics),
        limit,
        labels,
    );
    let pool = build_pool(url);
    let mut conn = connect(url).await;
    let mut execs = Vec::new();
    for name in &names {
        for _ in 0..per_type {
            execs.push(seed_workflow(&mut conn, queue, name).await);
        }
    }
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });
    // One connection and one count query per poll. A connection per run
    // per poll is slow under load, and its handshake competes with the
    // handlers for the runtime threads.
    let ids: Vec<Uuid> = execs.iter().map(ExecutionId::as_uuid).collect();
    let open_runs = || {
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq_any(ids.clone()))
            .filter(harvest_workflow_executions::state.ne("COMPLETED"))
            .count()
    };
    tokio::time::timeout(Duration::from_secs(120), async {
        while open_runs()
            .get_result::<i64>(&mut conn)
            .await
            .expect("count open runs")
            > 0
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("every run completes within 120 s");
    worker.shutdown();
    handle.await.expect("worker joins");
    (metrics, registry)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A fixed cap of 3 holds even though the worker has 16 slots. Every run
/// still completes, so no task is lost at the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_worker_caps_in_flight_attempts_of_a_limited_type() {
    const LIMITED: &str = "al_capped_slow";
    const FREE: &str = "al_free_slow";
    let (url, _container) = setup_db().await;
    let queue = unique_queue("al-cap");
    let config = AdaptiveLimitConfig::disabled()
        .with_activity(LIMITED, Some(AdaptiveLimitPolicy::new(3, 3)));
    let (metrics, registry) = run_to_completion(
        &url,
        &queue,
        vec![act_info(LIMITED, slow_call), act_info(FREE, slow_call)],
        Some(config),
        24,
    )
    .await;

    let limited = gauge(LIMITED);
    assert_eq!(limited.done(), 24);
    let peak = limited.peak();
    assert!(peak <= 3, "the limited type ran {peak} attempts at once");
    assert_eq!(peak, 3, "the cap is reachable");

    // The other type shares the worker, and its concurrency is not capped.
    let free = gauge(FREE).peak();
    assert!(free > 3, "the free type peaked at {free}");

    // The worker exports the limit state.
    let (limit, in_flight, baseline) = metrics.last(LIMITED).expect("limit gauges");
    assert_eq!(limit, 3);
    assert_eq!(in_flight, 0, "every slot is free after the run");
    let baseline = baseline.expect("a baseline estimate");
    assert!(baseline >= 0.15, "baseline {baseline} s");
    assert_eq!(metrics.peak_in_flight(LIMITED), 3);
    assert!(metrics.last(FREE).is_none(), "a free type has no gauges");
    assert_eq!(
        registry
            .adaptive_limits()
            .snapshot(LIMITED)
            .map(|s| s.limit),
        Some(3)
    );
}

/// The limit is off by default, so the worker slot count is the only cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_limit_is_off_by_default() {
    const ACTIVITY: &str = "al_default_slow";
    let (url, _container) = setup_db().await;
    let queue = unique_queue("al-default");
    let (metrics, _) =
        run_to_completion(&url, &queue, vec![act_info(ACTIVITY, slow_call)], None, 24).await;
    let peak = gauge(ACTIVITY).peak();
    assert!(peak > 4, "the default worker ran only {peak} at once");
    assert!(metrics.last(ACTIVITY).is_none(), "no limit, no gauges");
}

/// Regression test for issue #1836. The dependency slows down above a knee
/// of 8. The worker must feed the handler latency to the limit.
///
/// The test asserts only what holds for any worker speed. Each call also
/// pays a worker and database overhead that the sleep does not model. That
/// overhead raises the settle point: on a virtual clock, 60 ms per call
/// moves it from 14 to 18. So a wall-clock bound near the fixed point fails
/// on a slow runner. The virtual-clock tests in `adaptive_limit.rs` prove
/// the settle point itself. A moving cap cannot show a cap breach here, so
/// `the_worker_caps_in_flight_attempts_of_a_limited_type` proves that the
/// cap holds, with a fixed cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_limit_settles_near_the_knee_of_a_real_dependency() {
    const ACTIVITY: &str = "al_knee";
    let (url, _container) = setup_db().await;
    let queue = unique_queue("al-knee");
    let config = AdaptiveLimitConfig::disabled().with_default(Some(AdaptiveLimitPolicy::default()));
    let (metrics, registry) = run_to_completion(
        &url,
        &queue,
        vec![act_info(ACTIVITY, knee_call)],
        Some(config),
        400,
    )
    .await;

    let limit = registry
        .adaptive_limits()
        .snapshot(ACTIVITY)
        .expect("limit state")
        .limit;
    // The cap starts at `QUEUE_SIZE`. The gradient is at least 0.5, so each
    // full window moves the cap up while it is below the knee. Growth proves
    // that the worker feeds samples to the limit.
    assert!(
        f64::from(limit) > autumn_harvest::adaptive_limit::QUEUE_SIZE,
        "the cap did not grow from its start: {limit}"
    );
    let (published, _, baseline) = metrics.last(ACTIVITY).expect("limit gauges");
    assert_eq!(published, limit);
    // A sleep never ends early, so no window mean is below the 60 ms answer.
    let baseline = baseline.expect("a baseline estimate");
    assert!(
        baseline >= 0.06,
        "baseline {baseline} s; the no-load latency is 0.06 s"
    );
}

/// A row with capability requirements skips the ineligible-activity gate of
/// the claim. The claim must still skip a type at its cap, or each poll
/// claims a row only to defer it again. A rare race may still defer a
/// claim. That deferral uses no attempt, and every run completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_capability_type_at_its_cap_is_not_claimed() {
    const ACTIVITY: &str = "al_capability_slow";
    let (url, _container) = setup_db().await;
    let queue = unique_queue("al-backstop");
    let mut activity = act_info(ACTIVITY, slow_call);
    activity.requires = Some("gpu=true");
    let config = AdaptiveLimitConfig::disabled()
        .with_activity(ACTIVITY, Some(AdaptiveLimitPolicy::new(2, 2)));
    let labels = HashMap::from([("gpu".to_owned(), "true".to_owned())]);
    let (metrics, _) =
        run_with_labels(&url, &queue, vec![activity], Some(config), 12, labels).await;

    let deferred = metrics.deferred(ACTIVITY);
    assert!(
        deferred <= 6,
        "{deferred} claims churned through a deferral"
    );
    let g = gauge(ACTIVITY);
    assert_eq!(g.done(), 12, "each run calls the handler once");
    assert!(g.peak() <= 2, "the type ran {} attempts at once", g.peak());
    assert_eq!(g.max_attempt(), 1, "a deferral must not use an attempt");
}

/// A hung dependency never answers. Its attempts time out, and the limit
/// must read the timeouts as overload and cut the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeouts_of_a_hung_dependency_cut_the_cap() {
    const ACTIVITY: &str = "al_hung";
    let (url, _container) = setup_db().await;
    let queue = unique_queue("al-hung");
    let mut activity = act_info(ACTIVITY, hung_call);
    activity.default_start_to_close = Some(Duration::from_millis(500));
    let config = AdaptiveLimitConfig::disabled()
        .with_activity(ACTIVITY, Some(AdaptiveLimitPolicy::default()));
    let metrics = Arc::new(LimitMetrics::default());
    let (worker, registry) = build_worker(
        &format!("{queue}-worker"),
        &queue,
        vec![activity],
        Arc::clone(&metrics),
        Some(config),
        HashMap::new(),
    );
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    for _ in 0..8 {
        seed_workflow(&mut conn, &queue, ACTIVITY).await;
    }
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });
    let limits = registry.adaptive_limits();
    wait_until("a cap below the probe cap", Duration::from_secs(60), || {
        let limits = Arc::clone(&limits);
        async move { limits.snapshot(ACTIVITY).is_some_and(|s| s.limit < 4) }
    })
    .await;
    worker.shutdown();
    handle.await.expect("worker joins");
    assert!(metrics.last(ACTIVITY).is_some_and(|m| m.0 < 4));
}

/// A heartbeat timeout fires long before the attempt deadline. The limit
/// must still read it as overload and cut the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_timeouts_of_a_hung_dependency_cut_the_cap() {
    const ACTIVITY: &str = "al_hung_heartbeat";
    let (url, _container) = setup_db().await;
    let queue = unique_queue("al-hung-hb");
    let mut activity = act_info(ACTIVITY, hung_call);
    activity.default_start_to_close = Some(Duration::from_secs(120));
    activity.default_heartbeat_timeout = Some(Duration::from_millis(500));
    let config = AdaptiveLimitConfig::disabled()
        .with_activity(ACTIVITY, Some(AdaptiveLimitPolicy::default()));
    let metrics = Arc::new(LimitMetrics::default());
    let (worker, registry) = build_worker(
        &format!("{queue}-worker"),
        &queue,
        vec![activity],
        Arc::clone(&metrics),
        Some(config),
        HashMap::new(),
    );
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    for _ in 0..8 {
        seed_workflow(&mut conn, &queue, ACTIVITY).await;
    }
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });
    let limits = registry.adaptive_limits();
    wait_until("a cap below the probe cap", Duration::from_secs(40), || {
        let limits = Arc::clone(&limits);
        async move { limits.snapshot(ACTIVITY).is_some_and(|s| s.limit < 4) }
    })
    .await;
    worker.shutdown();
    handle.await.expect("worker joins");
}

/// An answer after the attempt deadline is a timeout, even when the handler
/// returns before the worker sees the lost claim. Late answers must cut the
/// cap, not grow it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn answers_after_the_deadline_cut_the_cap() {
    const ACTIVITY: &str = "al_late";
    let (url, _container) = setup_db().await;
    let queue = unique_queue("al-late");
    let mut activity = act_info(ACTIVITY, late_call);
    activity.default_start_to_close = Some(Duration::from_millis(300));
    let config = AdaptiveLimitConfig::disabled()
        .with_activity(ACTIVITY, Some(AdaptiveLimitPolicy::default()));
    let metrics = Arc::new(LimitMetrics::default());
    let (worker, registry) = build_worker(
        &format!("{queue}-worker"),
        &queue,
        vec![activity],
        Arc::clone(&metrics),
        Some(config),
        HashMap::new(),
    );
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    for _ in 0..8 {
        seed_workflow(&mut conn, &queue, ACTIVITY).await;
    }
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });
    let limits = registry.adaptive_limits();
    wait_until("a cap below the probe cap", Duration::from_secs(30), || {
        let limits = Arc::clone(&limits);
        async move { limits.snapshot(ACTIVITY).is_some_and(|s| s.limit < 4) }
    })
    .await;
    worker.shutdown();
    handle.await.expect("worker joins");
}

/// A resume after a pause moves `schedule_to_close_at` forward while an
/// attempt runs. An answer before the moved deadline is not a timeout, so
/// it must not cut the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn answers_before_a_moved_deadline_do_not_cut_the_cap() {
    const ACTIVITY: &str = "al_moved";
    const RUNS: usize = 4;
    let (url, _container) = setup_db().await;
    let queue = unique_queue("al-moved");
    // The 1 s budget leaves room for a slow claim. The 1.2 s answer still
    // ends past the deadline that the worker reads at the claim.
    let mut activity = act_info(ACTIVITY, overdue_call);
    activity.default_schedule_to_close = Some(Duration::from_secs(1));
    let config = AdaptiveLimitConfig::disabled()
        .with_activity(ACTIVITY, Some(AdaptiveLimitPolicy::default()));
    let (worker, registry) = build_worker(
        &format!("{queue}-worker"),
        &queue,
        vec![activity],
        Arc::new(LimitMetrics::default()),
        Some(config),
        HashMap::new(),
    );
    // This task stands in for the resume shift. It moves the deadline of
    // each claimed attempt 5 s forward, after the worker read it.
    let shift_sql = format!(
        "UPDATE harvest_task_queue \
         SET schedule_to_close_at = schedule_to_close_at + INTERVAL '5 seconds' \
         WHERE queue_name = '{queue}' AND activity_name = '{ACTIVITY}' \
         AND state = 'RUNNING' AND schedule_to_close_at < NOW() + INTERVAL '2 seconds'"
    );
    let shift_url = url.clone();
    let shifter = tokio::spawn(async move {
        let mut conn = connect(&shift_url).await;
        loop {
            conn.batch_execute(&shift_sql)
                .await
                .expect("shift the deadline");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });
    // One window at the start cap. Every attempt starts at once.
    let mut ids = Vec::new();
    for _ in 0..RUNS {
        ids.push(seed_workflow(&mut conn, &queue, ACTIVITY).await.as_uuid());
    }
    let open_runs = || {
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq_any(ids.clone()))
            .filter(harvest_workflow_executions::state.ne("COMPLETED"))
            .count()
    };
    tokio::time::timeout(Duration::from_secs(30), async {
        while open_runs()
            .get_result::<i64>(&mut conn)
            .await
            .expect("count open runs")
            > 0
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("every run completes before its moved deadline");
    shifter.abort();
    worker.shutdown();
    handle.await.expect("worker joins");
    let state = registry
        .adaptive_limits()
        .snapshot(ACTIVITY)
        .expect("the limit tracks the type");
    assert!(
        state.baseline.is_some(),
        "the answers set a baseline: {state:?}"
    );
    assert!(state.limit >= 4, "answers in time cut the cap: {state:?}");
}

/// A freed adaptive slot must wake the idle poll loop. Otherwise a worker
/// whose backlog holds only a saturated type waits one full poll interval
/// after each batch, and the cap stays idle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_freed_slot_wakes_the_poll_loop() {
    const ACTIVITY: &str = "al_fast_capped";
    const RUNS: u32 = 20;
    let (url, _container) = setup_db().await;
    let queue = unique_queue("al-wake");
    let config = AdaptiveLimitConfig::disabled()
        .with_activity(ACTIVITY, Some(AdaptiveLimitPolicy::new(2, 2)));
    let (worker, _registry) = build_worker_polling(
        &format!("{queue}-worker"),
        &queue,
        vec![act_info(ACTIVITY, fast_call)],
        Arc::new(LimitMetrics::default()),
        Some(config),
        HashMap::new(),
        Duration::from_secs(1),
    );
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    let mut ids = Vec::new();
    for _ in 0..RUNS {
        ids.push(seed_workflow(&mut conn, &queue, ACTIVITY).await.as_uuid());
    }
    let runner = Arc::clone(&worker);
    let started = std::time::Instant::now();
    let handle = tokio::spawn(async move { runner.run(&pool).await });
    let open_runs = || {
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq_any(ids.clone()))
            .filter(harvest_workflow_executions::state.ne("COMPLETED"))
            .count()
    };
    tokio::time::timeout(Duration::from_secs(60), async {
        while open_runs()
            .get_result::<i64>(&mut conn)
            .await
            .expect("count open runs")
            > 0
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("every run completes within 60 s");
    let elapsed = started.elapsed();
    worker.shutdown();
    handle.await.expect("worker joins");
    // Ten batches of two 20 ms calls. Without a cap the run takes about
    // 2 s here. Each missed wake adds one second of idle polling.
    assert!(
        elapsed < Duration::from_millis(3_500),
        "{RUNS} runs at a cap of 2 took {elapsed:?}"
    );
}
