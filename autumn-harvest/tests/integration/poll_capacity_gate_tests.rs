#![cfg(feature = "db")]
//! Poll-path capacity gate (issue #1787).
//!
//! The default Postgres poll path claims a task only when the worker has a
//! free local permit for that task kind. A claim stamps `started_at`, and the
//! start-to-close clock runs from `started_at`. Without the gate, an
//! over-claimed task spends its timeout budget in the local permit queue, and
//! a peer cannot take it. These tests pin that the gate prevents this.
//!
//! * `queued_activity_never_times_out_before_its_handler_starts` (AC1).
//! * `idle_peer_takes_tasks_a_saturated_worker_left_unclaimed` (AC2, AC3).
//! * `kind_filtered_claim_takes_only_that_kind`: the kind-filtered statement.
//! * `a_freed_permit_starts_the_next_task_without_a_poll_interval_wait`.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise a testcontainers Postgres 16 starts. Each test uses its own queue.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::timeout;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{RetryPolicy, WorkflowContext, store};

use chrono::Utc;
use diesel::QueryDsl;
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
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
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
// Handlers. Each activity records the execution id it receives as input.
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Execution ids whose blocking activity handler started.
static BLOCKING_STARTED: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Execution ids whose quick activity handler started.
static QUICK_STARTED: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Start instants of the short activity handler, by execution id.
static SHORT_STARTED: LazyLock<Mutex<HashMap<String, Instant>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn record(set: &Mutex<HashSet<String>>, input: &serde_json::Value) {
    let key = input.as_str().expect("activity input is an id").to_owned();
    set.lock().expect("started set").insert(key);
}

fn started_count(set: &Mutex<HashSet<String>>, ids: &[ExecutionId]) -> usize {
    let set = set.lock().expect("started set");
    ids.iter()
        .filter(|id| set.contains(&id.as_uuid().to_string()))
        .count()
}

/// Workflow: run one `gate_activity` on its own queue, with its id as input.
fn gate_workflow(ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        let id = serde_json::json!(ctx.execution_id().as_uuid().to_string());
        ctx.execute_activity_raw("gate_activity", id, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Activity that holds its permit far longer than any test waits.
fn blocking_activity(
    _ctx: &autumn_harvest::ActivityContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        record(&BLOCKING_STARTED, &input);
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(input)
    })
}

/// Activity that holds its permit for 300 ms.
fn short_activity(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let key = input.as_str().expect("activity input is an id").to_owned();
        SHORT_STARTED
            .lock()
            .expect("started map")
            .insert(key, Instant::now());
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(input)
    })
}

/// Activity that returns at once.
fn quick_activity(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        record(&QUICK_STARTED, &input);
        Ok(input)
    })
}

// ---------------------------------------------------------------------------
// Registry and worker construction.
// ---------------------------------------------------------------------------

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "gate_workflow",
        module: "poll_capacity_gate_tests",
        handler: gate_workflow,
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

/// `gate_activity` with one attempt, so a timeout is final and visible.
fn activity_info(
    handler: autumn_harvest::info::ActivityHandlerFn,
    start_to_close: Duration,
) -> ActivityInfo {
    ActivityInfo {
        name: "gate_activity",
        module: "poll_capacity_gate_tests",
        default_retry_policy: Some(RetryPolicy::fixed(1, Duration::from_millis(10))),
        default_start_to_close: Some(start_to_close),
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

fn build_registry(activity: ActivityInfo) -> Arc<HandlerRegistry> {
    build_registry_with_metrics(activity, Arc::new(NoOpMetrics))
}

fn build_registry_with_metrics(
    activity: ActivityInfo,
    metrics: Arc<dyn MetricsRecorder>,
) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![workflow_info()],
        vec![activity],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

/// A worker with one activity permit and spare workflow permits.
fn build_worker(worker_id: &str, queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    build_worker_polling(worker_id, queue, registry, Duration::from_millis(25))
}

fn build_worker_polling(
    worker_id: &str,
    queue: &str,
    registry: Arc<HandlerRegistry>,
    poll_interval: Duration,
) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: worker_id.to_string(),
                queues: vec![queue.to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 8,
                max_concurrent_activities: 1,
                poll_interval,
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
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
            },
            registry,
        )
        .expect("worker should build"),
    )
}

// ---------------------------------------------------------------------------
// Seeding and read helpers.
// ---------------------------------------------------------------------------

async fn seed_workflow(conn: &mut AsyncPgConnection, queue: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let input = serde_json::json!(null);
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "gate_workflow",
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

async fn seed_workflows(url: &str, queue: &str, n: usize) -> Vec<ExecutionId> {
    let mut conn = connect(url).await;
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        ids.push(seed_workflow(&mut conn, queue).await);
    }
    ids
}

/// Whether the history of `exec_id` holds an `ActivityTimedOut` event.
async fn activity_timed_out(url: &str, exec_id: ExecutionId) -> bool {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. }))
}

/// Poll `cond` every 50 ms until it holds or `limit` elapses.
async fn wait_until(limit: Duration, mut cond: impl FnMut() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok()
}

/// Run the timeout scan every 250 ms until `stop` fires.
fn spawn_timeout_scanner(
    pool: DbPool,
    stop: Arc<tokio::sync::Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = stop.notified() => break,
                () = tokio::time::sleep(Duration::from_millis(250)) => {
                    let mut c = pool.get().await.expect("scanner conn");
                    let _ = timeout::enforce_timeouts_once(
                        &mut c,
                        &NoOpMetrics,
                        Duration::from_secs(60),
                        &None,
                        &[ShardId::new(0)],
                        None,
                        None,
                        60,
                        &autumn_harvest::payload_codec::PayloadCodecs::default(),
                        0,
                    )
                    .await;
                }
            }
        }
    })
}

/// Records every schedule-to-start sample.
#[derive(Default)]
struct ScheduleToStartSamples(Mutex<Vec<f64>>);

impl MetricsRecorder for ScheduleToStartSamples {
    fn record_schedule_to_start(&self, _queue_name: &str, wait_secs: f64) {
        self.0.lock().expect("samples").push(wait_secs);
    }
}

/// Whether the history of `exec_id` holds an `ActivityScheduled` event.
async fn activity_scheduled(url: &str, exec_id: ExecutionId) -> bool {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
}

// ---------------------------------------------------------------------------
// AC1: no queued task times out before its handler starts.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_activity_never_times_out_before_its_handler_starts() {
    let (url, _container) = setup_db().await;
    let queue = format!("gate-ac1-{}", Uuid::new_v4());
    let ids = seed_workflows(&url, &queue, 4).await;

    let stc = Duration::from_secs(2);
    let registry = build_registry(activity_info(blocking_activity, stc));
    let worker = build_worker("gate-ac1-worker", &queue, registry);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let run_pool = pool.clone();
    let run_handle = tokio::spawn(async move { runner.run(&run_pool).await });
    let stop = Arc::new(tokio::sync::Notify::new());
    let scanner = spawn_timeout_scanner(pool.clone(), Arc::clone(&stop));

    // The first activity blocks the only permit. Wait for its start, then
    // for several start-to-close windows so every over-claimed row expires.
    assert!(
        wait_until(Duration::from_secs(20), || started_count(
            &BLOCKING_STARTED,
            &ids
        ) >= 1)
        .await,
        "the first activity handler must start"
    );
    tokio::time::sleep(stc * 4).await;

    stop.notify_one();
    let _ = scanner.await;
    worker.shutdown();
    let _ = run_handle.await;

    let started: HashSet<String> = BLOCKING_STARTED.lock().expect("started set").clone();
    let mut timed_out = Vec::new();
    for id in &ids {
        // Not vacuous: every workflow scheduled the activity under test.
        assert!(
            activity_scheduled(&url, *id).await,
            "workflow {id:?} must schedule its activity"
        );
        if activity_timed_out(&url, *id).await {
            timed_out.push(id.as_uuid().to_string());
        }
    }
    // Control: the blocked handler itself runs past start-to-close.
    assert!(
        !timed_out.is_empty(),
        "the started, blocked activity must time out (scanner control)"
    );
    let never_started: Vec<&String> = timed_out
        .iter()
        .filter(|id| !started.contains(*id))
        .collect();
    assert!(
        never_started.is_empty(),
        "activities timed out before their handler started: {never_started:?}"
    );
}

// ---------------------------------------------------------------------------
// AC2: an idle peer takes tasks that a saturated worker has not claimed.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_peer_takes_tasks_a_saturated_worker_left_unclaimed() {
    let (url, _container) = setup_db().await;
    let queue = format!("gate-ac2-{}", Uuid::new_v4());
    let ids = seed_workflows(&url, &queue, 3).await;
    let pool = build_pool(&url);

    // Worker A: its first activity holds the only permit for the whole test.
    let stc = Duration::from_secs(60);
    let busy = build_worker(
        "gate-ac2-busy",
        &queue,
        build_registry(activity_info(blocking_activity, stc)),
    );
    let busy_runner = Arc::clone(&busy);
    let busy_pool = pool.clone();
    let busy_handle = tokio::spawn(async move { busy_runner.run(&busy_pool).await });

    assert!(
        wait_until(Duration::from_secs(20), || started_count(
            &BLOCKING_STARTED,
            &ids
        ) >= 1)
        .await,
        "worker A must start one blocking activity"
    );
    // Give worker A time to over-claim the rest, if it does.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Worker B: idle, with a free permit and a handler that returns at once.
    let samples = Arc::new(ScheduleToStartSamples::default());
    let idle = build_worker(
        "gate-ac2-idle",
        &queue,
        build_registry_with_metrics(
            activity_info(quick_activity, stc),
            Arc::clone(&samples) as Arc<dyn MetricsRecorder>,
        ),
    );
    let idle_runner = Arc::clone(&idle);
    let idle_pool = pool.clone();
    let idle_handle = tokio::spawn(async move { idle_runner.run(&idle_pool).await });

    let rest = ids.len() - 1;
    let taken = wait_until(Duration::from_secs(15), || {
        started_count(&QUICK_STARTED, &ids) >= rest
    })
    .await;

    busy.shutdown();
    idle.shutdown();
    let _ = busy_handle.await;
    let _ = idle_handle.await;

    assert!(
        taken,
        "idle worker B must run the {rest} activities worker A could not start; \
         B ran {} and A started {}",
        started_count(&QUICK_STARTED, &ids),
        started_count(&BLOCKING_STARTED, &ids),
    );
    assert_eq!(
        started_count(&BLOCKING_STARTED, &ids),
        1,
        "worker A must start exactly one activity while its permit is held"
    );
    // AC3: schedule-to-start still runs from eligibility. The activities B
    // ran waited at least 1 s in `PENDING`, and the samples show that wait.
    let waited = samples
        .0
        .lock()
        .expect("samples")
        .iter()
        .filter(|secs| **secs >= 0.9)
        .count();
    assert!(
        waited >= rest,
        "schedule-to-start must include the PENDING wait; {waited} of {rest} samples did"
    );
}

// ---------------------------------------------------------------------------
// The kind-filtered claim statement against a real database.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kind_filtered_claim_takes_only_that_kind() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = format!("gate-kind-{}", Uuid::new_v4());
    let queues = [queue.clone()];

    // The workflow row is older, so an unfiltered claim takes it first.
    let mut workflow = EnqueueParams::new(&queue, TaskType::Workflow, serde_json::json!(null));
    workflow.scheduled_at = Utc::now() - chrono::Duration::seconds(10);
    let workflow_id = queue::enqueue(&mut conn, &workflow)
        .await
        .expect("enqueue workflow row");
    let mut activity = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!(null));
    activity.activity_name = Some("gate_activity".to_owned());
    activity.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    let activity_id = queue::enqueue(&mut conn, &activity)
        .await
        .expect("enqueue activity row");

    let claim = |kind| {
        let queues = queues.clone();
        let url = url.clone();
        async move {
            let mut conn = connect(&url).await;
            queue::claim_task_of_kind_on_shard(
                &mut conn,
                &queues,
                "gate-kind-worker",
                "",
                None,
                &[],
                &[],
                None,
                Some(kind),
            )
            .await
            .expect("claim")
        }
    };

    let claimed = claim(TaskType::Activity).await.expect("an activity row");
    assert_eq!(
        claimed.id, activity_id,
        "the activity claim skips the older workflow row"
    );
    assert!(
        claim(TaskType::Activity).await.is_none(),
        "no activity row is left to claim"
    );
    let claimed = claim(TaskType::Workflow).await.expect("a workflow row");
    assert_eq!(claimed.id, workflow_id);

    let state: String = autumn_harvest::schema::harvest_task_queue::table
        .find(activity_id)
        .select(autumn_harvest::schema::harvest_task_queue::state)
        .first(&mut conn)
        .await
        .expect("activity row");
    assert_eq!(state, "RUNNING");
}

// ---------------------------------------------------------------------------
// A released permit starts the next task at once (issue #1787 review).
// ---------------------------------------------------------------------------

/// With the activity pool full and the workflow pool free, the poll claims
/// workflows only. The release of the activity permit must wake the loop. A
/// 3 s `poll_interval` with no listener makes a missed wake-up visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_freed_permit_starts_the_next_task_without_a_poll_interval_wait() {
    let (url, _container) = setup_db().await;
    let queue = format!("gate-wake-{}", Uuid::new_v4());
    let ids = seed_workflows(&url, &queue, 4).await;
    let pool = build_pool(&url);

    let worker = build_worker_polling(
        "gate-wake-worker",
        &queue,
        build_registry(activity_info(short_activity, Duration::from_secs(60))),
        Duration::from_secs(3),
    );
    let runner = Arc::clone(&worker);
    let run_pool = pool.clone();
    let run_handle = tokio::spawn(async move { runner.run(&run_pool).await });

    let keys: Vec<String> = ids.iter().map(|id| id.as_uuid().to_string()).collect();
    let all_started = wait_until(Duration::from_secs(30), || {
        let map = SHORT_STARTED.lock().expect("started map");
        keys.iter().all(|k| map.contains_key(k))
    })
    .await;

    worker.shutdown();
    let _ = run_handle.await;
    assert!(all_started, "every short activity must start");

    let mut starts: Vec<Instant> = {
        let map = SHORT_STARTED.lock().expect("started map");
        keys.iter().map(|k| map[k]).collect()
    };
    starts.sort();
    let max_gap = starts
        .windows(2)
        .map(|w| w[1].duration_since(w[0]))
        .max()
        .expect("four starts");
    assert!(
        max_gap < Duration::from_millis(1500),
        "each 300 ms activity must start the next one at once; the largest gap was {max_gap:?}"
    );
}
