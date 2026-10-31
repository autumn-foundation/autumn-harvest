#![cfg(feature = "db")]
//! End-to-end tests for the cost hold (autumn#1720).
//!
//! A real worker runs workflows against Postgres. Each workflow calls one
//! activity. A switch stands in for the cost signal:
//!
//! - While the signal is high, the worker claims no task of a held type. The
//!   task stays `PENDING`, and its first attempt is still attempt 1.
//! - An activity that the hold does not name runs on the same queue.
//! - When the signal falls, the held task runs and its run completes.
//! - The worker sets the `harvest.cost.hold` gauge at each change.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly. Otherwise a fresh testcontainers Postgres boots.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::cost_hold::CostHold;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
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

const HELD: &str = "ch_rebuild_index";
const FREE: &str = "ch_charge_card";

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
// Probes
// ---------------------------------------------------------------------------

/// Runs and the highest attempt, per activity type.
static RUNS: std::sync::LazyLock<Mutex<HashMap<String, (u32, u32)>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

fn runs(activity: &str) -> (u32, u32) {
    RUNS.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(activity)
        .copied()
        .unwrap_or_default()
}

/// Each change of the cost hold gauge, in order.
#[derive(Default)]
struct HoldGauge(Mutex<Vec<bool>>);

impl HoldGauge {
    fn changes(&self) -> Vec<bool> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl MetricsRecorder for HoldGauge {
    fn record_cost_hold(&self, held: bool) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(held);
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Counts the run and the attempt, then answers.
fn counted_call(ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    let mut all = RUNS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let entry = all.entry(ctx.activity_type().to_owned()).or_default();
    entry.0 += 1;
    entry.1 = entry.1.max(ctx.attempt());
    drop(all);
    Box::pin(async move { Ok(input) })
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
        name: "wf_cost_hold_call",
        module: "cost_hold_tests",
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

fn act_info(name: &'static str) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "cost_hold_tests",
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
        handler: counted_call,
        input_schema: None,
        output_schema: None,
    }
}

// ---------------------------------------------------------------------------
// Worker and seeding
// ---------------------------------------------------------------------------

/// A worker on `queue` whose registry holds [`HELD`] while `high` is set.
fn build_worker(queue: &str, high: Arc<AtomicBool>, gauge: Arc<HoldGauge>) -> Arc<Worker> {
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(gauge).build());
    let registry = HandlerRegistry::with_state_and_telemetry(
        vec![wf_info()],
        vec![act_info(HELD), act_info(FREE)],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    )
    .with_cost_hold(Some(CostHold::new(
        move || AtomicBool::load(&high, Ordering::SeqCst),
        [HELD],
    )));
    let registry = Arc::new(registry);
    let worker_id = format!("{queue}-worker");
    let queue = queue.to_owned();
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id,
                queues: vec![queue],
                notification_database_url: None,
                max_concurrent_workflows: 8,
                max_concurrent_activities: 32,
                max_concurrent_claims: autumn_harvest::worker::DEFAULT_MAX_CONCURRENT_CLAIMS,
                poll_interval: Duration::from_millis(20),
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
            Arc::clone(&registry),
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
        workflow_name: "wf_cost_hold_call",
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

async fn run_state(conn: &mut AsyncPgConnection, exec: ExecutionId) -> String {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec.as_uuid()))
        .select(harvest_workflow_executions::state)
        .first::<String>(conn)
        .await
        .expect("read run state")
}

/// The states of the activity tasks of one run.
async fn activity_task_states(conn: &mut AsyncPgConnection, exec: ExecutionId) -> Vec<String> {
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(exec.as_uuid()))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(harvest_task_queue::state)
        .load::<String>(conn)
        .await
        .expect("read activity task states")
}

/// Poll the run state every 20 ms until it is `want`, or panic after 30 s.
async fn wait_for_run_state(conn: &mut AsyncPgConnection, exec: ExecutionId, want: &str) {
    let found = tokio::time::timeout(Duration::from_secs(30), async {
        while run_state(conn, exec).await != want {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        found.is_ok(),
        "run {exec:?} did not reach {want} within 30 s"
    );
}

/// Poll every 20 ms until the run has an activity task, or panic after 30 s.
async fn wait_for_activity_task(conn: &mut AsyncPgConnection, exec: ExecutionId) {
    let found = tokio::time::timeout(Duration::from_secs(30), async {
        while activity_task_states(conn, exec).await.is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        found.is_ok(),
        "run {exec:?} scheduled no activity within 30 s"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A high signal holds the named type and lets the other type run. When
/// the signal falls, the held task runs at attempt 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_high_signal_holds_a_deferrable_activity_until_it_falls() {
    let (url, _container) = setup_db().await;
    let queue = format!("cost-hold-{}", Uuid::new_v4().simple());
    let high = Arc::new(AtomicBool::new(true));
    let gauge = Arc::new(HoldGauge::default());
    let worker = build_worker(&queue, Arc::clone(&high), Arc::clone(&gauge));
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    let held = seed_workflow(&mut conn, &queue, HELD).await;
    let free = seed_workflow(&mut conn, &queue, FREE).await;

    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // The free run completes while the signal is high.
    let mut probe = connect(&url).await;
    wait_for_run_state(&mut probe, free, "COMPLETED").await;
    // The held activity task exists and stays PENDING over many polls.
    wait_for_activity_task(&mut probe, held).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(runs(HELD), (0, 0), "a held type must not run");
    assert_eq!(activity_task_states(&mut probe, held).await, ["PENDING"]);
    assert_eq!(run_state(&mut probe, held).await, "RUNNING");
    assert_eq!(gauge.changes(), [true]);

    // The signal falls: the next poll claims the held task.
    high.store(false, Ordering::SeqCst);
    wait_for_run_state(&mut probe, held, "COMPLETED").await;
    assert_eq!(runs(HELD), (1, 1), "the hold must not use an attempt");
    assert_eq!(runs(FREE).0, 1);
    assert_eq!(gauge.changes(), [true, false]);

    worker.shutdown();
    handle.await.expect("worker joins");
}
