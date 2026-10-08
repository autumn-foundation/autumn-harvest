#![cfg(feature = "db")]
//! End-to-end tests for the per-activity-type retry budget (issue #1793).
//!
//! A real worker runs workflows against Postgres. Each workflow calls one
//! activity with a tight retry policy. The tests count the attempts that the
//! activity handlers see:
//!
//! - When one activity type fails every attempt, its retry rate stays within
//!   the default budget. No retry is lost.
//! - Exhausting that budget does not defer the retries of another type.
//! - A disabled budget never defers.
//! - A deferral keeps `crash_strikes` and refunds the claim-time rate-limit
//!   token.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly. Otherwise a fresh testcontainers Postgres boots.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::retry_budget::RetryBudgetConfig;
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{RetryPolicy, WorkflowContext, store};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// The documented default policy: `RetryBudgetPolicy::default()`.
const DEFAULT_RATIO: f64 = 0.1;
const DEFAULT_MAX_TOKENS: f64 = 10.0;
const DEFAULT_MIN_RETRIES_PER_SEC: f64 = 1.0;

/// Gap between attempts of one task. Without a budget, every failing task
/// retries about this often.
const RETRY_INTERVAL: Duration = Duration::from_millis(20);

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
// Attempt counters
// ---------------------------------------------------------------------------

/// First attempts and retries that each activity type has run.
#[derive(Debug, Default, Clone, Copy)]
struct Attempts {
    first: u32,
    retries: u32,
}

static ATTEMPTS: LazyLock<Mutex<HashMap<String, Attempts>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn record_attempt(ctx: &autumn_harvest::ActivityContext) {
    let is_retry = ctx.attempt() > 1;
    let mut map = ATTEMPTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let entry = map.entry(ctx.activity_type().to_owned()).or_default();
    if is_retry {
        entry.retries += 1;
    } else {
        entry.first += 1;
    }
    drop(map);
}

fn attempts(activity: &str) -> Attempts {
    ATTEMPTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(activity)
        .copied()
        .unwrap_or_default()
}

/// Counts the retries that the budget deferred, per activity type.
#[derive(Default)]
struct BudgetMetrics {
    exhausted: Mutex<HashMap<String, u64>>,
}

impl BudgetMetrics {
    fn exhausted(&self, activity: &str) -> u64 {
        self.exhausted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(activity)
            .copied()
            .unwrap_or(0)
    }
}

impl MetricsRecorder for BudgetMetrics {
    fn record_retry_budget_exhausted(&self, activity: &str) {
        *self
            .exhausted
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

/// Fails every attempt, like a dependency in a hard brownout.
fn always_fails(ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    record_attempt(ctx);
    Box::pin(async move { Err("dependency unavailable".to_string()) })
}

/// Fails the first attempt and succeeds on the retry.
fn fails_once(ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    record_attempt(ctx);
    let first = ctx.attempt() == 1;
    Box::pin(async move {
        if first {
            Err("transient".to_string())
        } else {
            Ok(input)
        }
    })
}

/// Calls the activity named in the input, with a tight retry policy.
fn wf_call(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let activity = input["activity"].as_str().unwrap_or_default().to_owned();
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw_with_opts(
            &activity,
            input,
            &queue,
            Some(RetryPolicy::fixed(10_000, RETRY_INTERVAL)),
            None,
        )
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
        name: "wf_retry_budget_call",
        module: "retry_budget_tests",
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
        module: "retry_budget_tests",
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

/// A worker with the default retry budget, so the tests prove it is on.
fn build_worker(
    worker_id: &str,
    queue: &str,
    activities: Vec<ActivityInfo>,
    metrics: Arc<BudgetMetrics>,
) -> Arc<Worker> {
    build_worker_with(worker_id, queue, activities, metrics, None)
}

/// A worker with an explicit retry budget config, or the default for `None`.
fn build_worker_with(
    worker_id: &str,
    queue: &str,
    activities: Vec<ActivityInfo>,
    metrics: Arc<BudgetMetrics>,
    budget: Option<RetryBudgetConfig>,
) -> Arc<Worker> {
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    let mut registry = HandlerRegistry::with_state_and_telemetry(
        vec![wf_info()],
        activities,
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    );
    if let Some(budget) = budget {
        registry = registry.with_retry_budget(budget);
    }
    let registry = Arc::new(registry);
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: worker_id.to_string(),
                queues: vec![queue.to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 8,
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
                labels: HashMap::new(),
                queue_weights: HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
                fairness_keys: false,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

/// Start one workflow that calls `activity` on `queue`.
async fn seed_workflow(conn: &mut AsyncPgConnection, queue: &str, activity: &str) -> ExecutionId {
    let input = serde_json::json!({ "activity": activity });
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "wf_retry_budget_call",
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

async fn activity_rows(url: &str, queue: &str, activity: &str) -> Vec<TaskQueueItem> {
    let mut conn = connect(url).await;
    harvest_task_queue::table
        .filter(harvest_task_queue::queue_name.eq(queue))
        .filter(harvest_task_queue::activity_name.eq(Some(activity.to_string())))
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("load activity rows")
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// When [`run_failing_window`] closes its window.
#[derive(Clone, Copy)]
enum Window {
    /// After a fixed time.
    Fixed(Duration),
    /// When the retries pass `factor` times the default budget. The run
    /// panics if that does not occur before `deadline`. A slow, loaded
    /// host then makes the run longer, not wrong.
    PastBudget { factor: f64, deadline: Duration },
}

/// What one run of [`run_failing_window`] observed.
struct WindowRun {
    /// Attempts the handler saw when the window closed.
    seen: Attempts,
    /// Seconds from worker start to the end of the window.
    elapsed: f64,
    /// Attempts the handler saw after the worker stopped.
    after: Attempts,
    execs: Vec<ExecutionId>,
    metrics: Arc<BudgetMetrics>,
}

/// Run `workflows` workflows that call `activity`, which always fails. Wait
/// until every first attempt ran, then keep the worker running until `window`
/// closes.
async fn run_failing_window(
    url: &str,
    queue: &str,
    activity: ActivityInfo,
    budget: Option<RetryBudgetConfig>,
    workflows: u32,
    window: Window,
) -> WindowRun {
    let name = activity.name;
    let metrics = Arc::new(BudgetMetrics::default());
    let worker = build_worker_with(
        &format!("{queue}-worker"),
        queue,
        vec![activity],
        Arc::clone(&metrics),
        budget,
    );
    let pool = build_pool(url);
    let mut conn = connect(url).await;
    let mut execs = Vec::new();
    for _ in 0..workflows {
        execs.push(seed_workflow(&mut conn, queue, name).await);
    }

    let started = Instant::now();
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });
    wait_until("every first attempt", Duration::from_secs(20), || async {
        attempts(name).first >= workflows
    })
    .await;
    match window {
        Window::Fixed(window) => tokio::time::sleep(window).await,
        Window::PastBudget { factor, deadline } => {
            wait_until("retries past the default budget", deadline, || async {
                let seen = attempts(name);
                let budget = default_budget(seen.first, started.elapsed().as_secs_f64());
                f64::from(seen.retries) > budget * factor
            })
            .await;
        }
    }
    let seen = attempts(name);
    let elapsed = started.elapsed().as_secs_f64();
    worker.shutdown();
    handle.await.expect("worker joins");
    // A claim taken before the snapshot can still run during shutdown, so
    // the counter checks read the handler counts again here.
    let after = attempts(name);
    WindowRun {
        seen,
        elapsed,
        after,
        execs,
        metrics,
    }
}

fn default_budget(first_attempts: u32, elapsed: f64) -> f64 {
    DEFAULT_MIN_RETRIES_PER_SEC.mul_add(
        elapsed,
        DEFAULT_RATIO.mul_add(f64::from(first_attempts), DEFAULT_MAX_TOKENS),
    )
}

fn count_started(history: &[WorkflowEvent]) -> usize {
    history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityStarted { .. }))
        .count()
}

/// Regression test for issue #1793. One activity type fails 100 % of its
/// attempts. Its retry rate must stay within the default budget:
/// `max_tokens + ratio * first_attempts + min_retries_per_sec * elapsed`.
///
/// Without a budget, each of the 20 tasks retries about every 20 ms, so the
/// worker runs hundreds of retries in the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_rate_stays_within_the_default_budget_when_one_type_always_fails() {
    const ACTIVITY: &str = "rb_always_fails_rate";
    const WORKFLOWS: u32 = 20;
    let (url, _container) = setup_db().await;
    let queue = unique_queue("rb-rate");
    let run = run_failing_window(
        &url,
        &queue,
        act_info(ACTIVITY, always_fails),
        None,
        WORKFLOWS,
        Window::Fixed(Duration::from_secs(3)),
    )
    .await;
    let seen = run.seen;

    assert_eq!(seen.first, WORKFLOWS, "every first attempt runs: {seen:?}");
    let budget = default_budget(seen.first, run.elapsed);
    assert!(
        f64::from(seen.retries) <= budget + 1.0,
        "{} retries ran in {:.2}s; the budget allows {budget:.1}",
        seen.retries,
        run.elapsed
    );
    assert!(seen.retries > 0, "the budget must still let retries run");
    assert!(
        run.metrics.exhausted(ACTIVITY) > 0,
        "deferrals must be counted"
    );

    // A deferred retry is never lost. Each task row stays live, and no
    // execution fails.
    let rows = activity_rows(&url, &queue, ACTIVITY).await;
    assert_eq!(rows.len(), run.execs.len(), "one task row per workflow");
    for row in &rows {
        assert!(
            row.state == "PENDING" || row.state == "RUNNING",
            "task {} left the queue: {}",
            row.id,
            row.state
        );
    }
    let mut started_events = 0;
    let mut conn = connect(&url).await;
    for exec in &run.execs {
        assert_eq!(execution_state(&url, *exec).await, "RUNNING");
        let history = store::load_history(&mut conn, *exec)
            .await
            .expect("load history")
            .events;
        started_events += count_started(&history);
    }

    // A deferral does not use an attempt and appends no event. The attempt
    // counters and the ActivityStarted events add up to the attempts that
    // ran, plus at most one claim per row cut off by shutdown.
    let ran = i64::from(run.after.first + run.after.retries);
    let running = i64::try_from(rows.iter().filter(|r| r.state == "RUNNING").count()).unwrap();
    let total_attempts: i64 = rows.iter().map(|r| i64::from(r.attempt)).sum();
    assert!(
        total_attempts >= ran && total_attempts <= ran + running,
        "attempt counters {total_attempts} do not match {ran} attempts that ran"
    );
    let started_events = i64::try_from(started_events).unwrap();
    assert!(
        started_events >= ran && started_events <= ran + running,
        "{started_events} ActivityStarted events for {ran} attempts that ran"
    );
}

/// `RetryBudgetConfig::disabled()` restores the old behaviour: no retry is
/// deferred, so the retry count goes far past the default budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disabled_budget_never_defers() {
    const ACTIVITY: &str = "rb_always_fails_disabled";
    let (url, _container) = setup_db().await;
    let queue = unique_queue("rb-disabled");
    let run = run_failing_window(
        &url,
        &queue,
        act_info(ACTIVITY, always_fails),
        Some(RetryBudgetConfig::disabled()),
        20,
        Window::PastBudget {
            factor: 2.0,
            deadline: Duration::from_secs(30),
        },
    )
    .await;

    assert_eq!(
        run.metrics.exhausted(ACTIVITY),
        0,
        "no deferral when disabled"
    );
    // The window closed at twice the budget. The budget at the snapshot is
    // a little larger, so this check asks only for an excess over it.
    let budget = default_budget(run.seen.first, run.elapsed);
    assert!(
        f64::from(run.seen.retries) > budget,
        "{} retries ran; without a budget the count must exceed {budget:.1}",
        run.seen.retries
    );
}

/// A budget deferral refunds the rate-limit token that the claim debited.
/// Otherwise every deferral cycle burns a token without a call, and the
/// rate limit starves the activity type.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budget_deferral_refunds_the_claim_time_rate_limit_token() {
    const ACTIVITY: &str = "rb_always_fails_rate_limited";
    const BURST: f64 = 10_000.0;
    let (url, _container) = setup_db().await;
    let queue = unique_queue("rb-refund");
    let key: &'static str = Box::leak(format!("rb-refund-{queue}").into_boxed_str());
    let mut activity = act_info(ACTIVITY, always_fails);
    // Effectively no time refill, so the bucket level counts the calls.
    activity.rate_limit_rps = Some(0.000_001);
    activity.rate_limit_burst = Some(BURST);
    activity.rate_limit_key = Some(key);
    let run = run_failing_window(
        &url,
        &queue,
        activity,
        None,
        20,
        Window::Fixed(Duration::from_secs(3)),
    )
    .await;
    assert!(run.metrics.exhausted(ACTIVITY) > 0, "the budget must defer");

    let mut conn = connect(&url).await;
    let tokens: f64 =
        diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(key)
            .get_result::<TokensRow>(&mut conn)
            .await
            .expect("load bucket")
            .tokens;
    let rows = activity_rows(&url, &queue, ACTIVITY).await;
    let running = rows.iter().filter(|r| r.state == "RUNNING").count();
    let debited = BURST - tokens;
    let ran = f64::from(run.after.first + run.after.retries);
    #[allow(clippy::cast_precision_loss)]
    let slack = running as f64 + 0.5;
    assert!(
        (debited - ran).abs() <= slack,
        "{debited:.1} tokens debited for {ran} calls; deferrals must not keep tokens"
    );
}

#[derive(diesel::QueryableByName)]
struct TokensRow {
    #[diesel(sql_type = diesel::sql_types::Double)]
    tokens: f64,
}

/// One type exhausts its budget. A second type must keep its own budget, so
/// none of its retries is deferred.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn other_activity_types_are_unaffected_when_one_type_exhausts_its_budget() {
    const FAILING: &str = "rb_always_fails_isolation";
    const HEALTHY: &str = "rb_fails_once_isolation";
    const HEALTHY_WORKFLOWS: u32 = 6;
    let (url, _container) = setup_db().await;
    let queue = unique_queue("rb-isolation");
    let metrics = Arc::new(BudgetMetrics::default());
    let worker = build_worker(
        "rb-isolation-worker",
        &queue,
        vec![
            act_info(FAILING, always_fails),
            act_info(HEALTHY, fails_once),
        ],
        Arc::clone(&metrics),
    );
    let pool = build_pool(&url);

    let mut conn = connect(&url).await;
    for _ in 0..20 {
        seed_workflow(&mut conn, &queue, FAILING).await;
    }

    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // Drain the failing type's budget before the healthy type starts.
    let exhausted = {
        let metrics = Arc::clone(&metrics);
        tokio::time::timeout(Duration::from_secs(20), async move {
            while metrics.exhausted(FAILING) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .is_ok()
    };
    if !exhausted {
        worker.shutdown();
        handle.await.expect("worker joins");
        panic!("the failing type never exhausted its budget");
    }

    let mut healthy = Vec::new();
    for _ in 0..HEALTHY_WORKFLOWS {
        healthy.push(seed_workflow(&mut conn, &queue, HEALTHY).await);
    }
    let url_for_wait = url.clone();
    let healthy_for_wait = healthy.clone();
    wait_until(
        "every healthy workflow to complete",
        Duration::from_secs(30),
        move || {
            let url = url_for_wait.clone();
            let execs = healthy_for_wait.clone();
            async move {
                for exec in execs {
                    if execution_state(&url, exec).await != "COMPLETED" {
                        return false;
                    }
                }
                true
            }
        },
    )
    .await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(
        metrics.exhausted(HEALTHY),
        0,
        "the healthy type must not be deferred by the failing type's budget"
    );
    let seen = attempts(HEALTHY);
    assert_eq!(seen.first, HEALTHY_WORKFLOWS, "{seen:?}");
    assert_eq!(seen.retries, HEALTHY_WORKFLOWS, "{seen:?}");
}

/// A budget deferral is not evidence about crashes. It must keep the crash
/// strikes, so poison-pill quarantine still trips for a task that crashes
/// workers. It also restores `attempt` and keeps `error`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_deferral_keeps_crash_strikes_attempt_and_error() {
    let (url, _container) = setup_db().await;
    let queue = unique_queue("rb-strikes");
    let mut conn = connect(&url).await;
    let exec = seed_workflow(&mut conn, &queue, "rb_strikes").await;

    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec.as_uuid());
    params.activity_name = Some("rb_strikes".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue activity");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET attempt = 1, crash_strikes = 2, error = 'boom' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("seed a crashed retry");
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1 AND id <> $2")
        .bind::<diesel::sql_types::Text, _>(&queue)
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut conn)
        .await
        .expect("leave only the activity row");

    let claimed = queue::claim_task(
        &mut conn,
        std::slice::from_ref(&queue),
        "rb-strikes-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("a claimable task");
    assert_eq!(claimed.id, task_id);
    assert_eq!(claimed.attempt, 2, "the claim raises attempt");

    let claim = queue::TaskClaim {
        task_id,
        worker_id: "rb-strikes-worker".to_string(),
        attempt: claimed.attempt,
    };
    let write =
        queue::defer_claimed_retry_for_budget(&mut conn, &claim, chrono::Duration::seconds(30))
            .await
            .expect("defer");
    assert_eq!(write, queue::ClaimWrite::Applied);

    let row: TaskQueueItem = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("reload row");
    assert_eq!(row.state, "PENDING");
    assert_eq!(row.attempt, 1, "the deferral restores attempt");
    assert_eq!(row.crash_strikes, 2, "the deferral keeps the crash strikes");
    assert_eq!(
        row.error.as_deref(),
        Some("boom"),
        "the deferral keeps error"
    );
    assert!(row.worker_id.is_none());
}

/// The deferral write and its NOTIFY are separate statements. A failed
/// NOTIFY must not make an applied deferral look unpersisted, or the worker
/// would give back a slot that a row still waits on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_deferral_reports_applied_when_only_the_notify_fails() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    // Postgres rejects a NOTIFY channel over 63 bytes, so this queue name
    // makes the NOTIFY fail after the UPDATE has applied.
    let queue = format!(
        "rb-notify-fails-{}-{}",
        Uuid::new_v4().simple(),
        "x".repeat(32)
    );
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, input, state, attempt, max_attempts, scheduled_at) \
         VALUES ($1, $2, 'activity', '{}'::jsonb, 'PENDING', 1, 10, NOW() - INTERVAL '5 seconds')",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Text, _>(&queue)
    .execute(&mut conn)
    .await
    .expect("insert a task row directly");
    let claimed = queue::claim_task(
        &mut conn,
        std::slice::from_ref(&queue),
        "rb-notify-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("a claimable task");
    assert_eq!(claimed.id, task_id, "the claim must take the inserted row");
    let claim = queue::TaskClaim {
        task_id,
        worker_id: "rb-notify-worker".to_string(),
        attempt: claimed.attempt,
    };

    let write =
        queue::defer_claimed_retry_for_budget(&mut conn, &claim, chrono::Duration::seconds(30))
            .await
            .expect("an applied deferral is not an error when only the NOTIFY fails");
    assert_eq!(write, queue::ClaimWrite::Applied);

    let row: TaskQueueItem = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("reload row");
    assert_eq!(row.state, "PENDING");
}
