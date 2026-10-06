#![cfg(feature = "db")]
//! End-to-end tests for the adaptive concurrency limit (issue #1836).
//!
//! A real worker runs workflows against Postgres. Each workflow calls one
//! activity. The handlers count their own concurrency:
//!
//! - A limited type never runs more attempts at once than its cap.
//! - A type without a limit is not capped by the limit of another type.
//! - The limit is off by default.
//! - Against a dependency whose latency grows above a knee, the cap settles
//!   near the knee, below the worker slot count.
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
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
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
}

impl LimitMetrics {
    fn last(&self, activity: &str) -> Option<LimitSample> {
        self.last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(activity)
            .copied()
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
        limit: u32,
        in_flight: u32,
        baseline_secs: Option<f64>,
    ) {
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

/// Concurrency above which [`knee_call`] slows down.
const KNEE: u32 = 4;

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

/// A worker with 16 activity slots and the given adaptive limit config.
fn build_worker(
    worker_id: &str,
    queue: &str,
    activities: Vec<ActivityInfo>,
    metrics: Arc<LimitMetrics>,
    limit: Option<AdaptiveLimitConfig>,
    labels: HashMap<String, String>,
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
                max_concurrent_workflows: 4,
                max_concurrent_activities: 16,
                poll_interval: Duration::from_millis(10),
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

async fn execution_state(url: &str, exec_id: ExecutionId) -> String {
    let mut conn = connect(url).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("load execution")
        .state
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
    let url_owned = url.to_owned();
    wait_until("every run completes", Duration::from_secs(90), || {
        let execs = execs.clone();
        let url = url_owned.clone();
        async move {
            for exec in execs {
                if execution_state(&url, exec).await != "COMPLETED" {
                    return false;
                }
            }
            true
        }
    })
    .await;
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
/// of 4. The slot tuner would see waits and grow. The adaptive limit sees
/// the latency and settles near the analytic fixed point
/// `tolerance * knee + QUEUE_SIZE`, which is 9, below the 16 worker slots.
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
        200,
    )
    .await;

    let fixed_point = AdaptiveLimitPolicy::default()
        .tolerance
        .mul_add(f64::from(KNEE), autumn_harvest::adaptive_limit::QUEUE_SIZE);
    let limit = registry
        .adaptive_limits()
        .snapshot(ACTIVITY)
        .expect("limit state")
        .limit;
    assert!(
        limit >= KNEE && f64::from(limit) <= fixed_point + 3.0,
        "settled at {limit}; knee {KNEE}; fixed point {fixed_point}"
    );
    let peak = gauge(ACTIVITY).peak();
    assert!(
        f64::from(peak) <= fixed_point + 3.0 && peak < 16,
        "the dependency saw {peak} calls at once"
    );
    assert_eq!(metrics.last(ACTIVITY).map(|m| m.0), Some(limit));
}

/// A row with capability requirements skips the claim-time exclusion, so
/// the worker can claim it at the cap. The dispatch gate must then defer
/// it. The deferral uses no attempt, and every run still completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_past_the_cap_is_deferred_without_using_an_attempt() {
    const ACTIVITY: &str = "al_capability_slow";
    let (url, _container) = setup_db().await;
    let queue = unique_queue("al-backstop");
    let mut activity = act_info(ACTIVITY, slow_call);
    activity.requires = Some("gpu=true");
    let config = AdaptiveLimitConfig::disabled()
        .with_activity(ACTIVITY, Some(AdaptiveLimitPolicy::new(2, 2)));
    let labels = HashMap::from([("gpu".to_owned(), "true".to_owned())]);
    run_with_labels(&url, &queue, vec![activity], Some(config), 12, labels).await;

    let g = gauge(ACTIVITY);
    assert_eq!(g.done(), 12, "each run calls the handler once");
    assert!(g.peak() <= 2, "the type ran {} attempts at once", g.peak());
    assert_eq!(g.max_attempt(), 1, "a deferral must not use an attempt");
}
