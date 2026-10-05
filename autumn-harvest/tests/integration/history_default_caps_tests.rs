//! Default history caps (issue #1804).
//!
//! These tests run a real worker against Postgres. They prove these facts:
//!
//! - Under the default policy, a run at 50,000 events fails with the typed
//!   `HistoryCapExceeded` reason.
//! - Under the default policy, the early warning fires at 10,240 events and
//!   not at 10,239. That is above the `continue_as_new` threshold of 10,000,
//!   so a run that rotates on the advisory never warns.
//! - Under the default policy, a run at 50 MiB of stored history fails with
//!   the typed `HistoryBytesCapExceeded` reason.
//! - The byte sum is exact on the warm cache path and on the cold path.
//! - A run over the byte cap runs no inline local activity.
//! - A run over the byte cap can still `continue_as_new`.
//! - An explicit "unlimited" keeps a run past both default caps alive.

#![cfg(feature = "db")]

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Jsonb, Text, Uuid as SqlUuid};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

use autumn_harvest::dlq::{self, DeadLetterReason};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::queue::{self as queue_mod, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::telemetry::{METRIC_WORKFLOW_HISTORY_BLOAT, MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, DEFAULT_HISTORY_BYTE_HARD_CAP, DEFAULT_HISTORY_EVENT_HARD_CAP,
    WorkflowContext, WorkflowHistoryPolicy,
};
use chrono::Utc;

const GROWER: &str = "history_default_caps_grower";
const LOCAL_RUNNER: &str = "history_default_caps_local_runner";
const ROTATOR: &str = "history_default_caps_rotator";
const SIDE_EFFECT_ACTIVITY: &str = "history_default_caps_side_effect";

/// Time budget for a decision over a 50,000-event history.
const LARGE_HISTORY_WAIT: Duration = Duration::from_secs(90);

/// A sticky window long enough to keep follow-up tasks on the warm cache.
const WARM: Duration = Duration::from_secs(30);

/// A zero sticky window turns the workflow cache off.
const COLD: Duration = Duration::ZERO;

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

// ---------------------------------------------------------------------------
// Database and worker setup
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
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

fn build_worker(
    worker_id: &str,
    registry: Arc<HandlerRegistry>,
    sticky_timeout: Duration,
) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: worker_id.to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 2,
                max_concurrent_activities: 2,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout,
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(30),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                resident_workflows: true,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: Duration::from_secs(60),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
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

/// A running worker and the handle that joins it.
struct RunningWorker {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

impl RunningWorker {
    fn start(database_url: &str, worker: Arc<Worker>) -> Self {
        let pool = build_test_pool(database_url);
        let runner = Arc::clone(&worker);
        let handle = tokio::spawn(async move { runner.run(&pool).await });
        Self { worker, handle }
    }

    async fn stop(self) {
        self.worker.shutdown();
        self.handle.await.expect("worker joins");
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct RecordingMetrics {
    history_bloat: Mutex<Vec<String>>,
    cache_hits: AtomicU64,
}

impl RecordingMetrics {
    /// Cache hits so far. The path form avoids Diesel's `RunQueryDsl::load`.
    fn hits(&self) -> u64 {
        AtomicU64::load(&self.cache_hits, Ordering::SeqCst)
    }
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_history_bloat(&self, workflow_name: &str) {
        self.history_bloat
            .lock()
            .unwrap()
            .push(workflow_name.to_owned());
    }

    fn record_workflow_cache_hit(&self, _workflow_name: &str, _queue: &str) {
        self.cache_hits.fetch_add(1, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// Workflow and activity handlers
// ---------------------------------------------------------------------------

/// A runaway signal loop. Each `grow` signal adds history and nothing ends it.
fn grower(ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        loop {
            ctx.wait_for_signal("grow")
                .await
                .map_err(|error| error.to_string())?;
        }
    })
}

/// Counts the runs of the side-effecting local activity.
static SIDE_EFFECT_RUNS: AtomicUsize = AtomicUsize::new(0);

fn side_effect(_ctx: &ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        SIDE_EFFECT_RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::json!({}))
    })
}

/// Runs one inline local activity, then waits for signals.
fn local_runner(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw(SIDE_EFFECT_ACTIVITY, input, None, None)
            .await
            .map_err(|error| error.to_string())?;
        grower(ctx, serde_json::Value::Null).await
    })
}

/// Calls `continue_as_new` once, then waits for signals on the new run.
fn rotator(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        if input.get("rotated").is_none() {
            ctx.continue_as_new(serde_json::json!({ "rotated": true }))
                .await
                .map_err(|error| error.to_string())?;
        }
        grower(ctx, serde_json::Value::Null).await
    })
}

fn workflow_info(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "history_default_caps_tests",
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

fn side_effect_activity() -> ActivityInfo {
    ActivityInfo {
        name: SIDE_EFFECT_ACTIVITY,
        module: "history_default_caps_tests",
        default_retry_policy: None,
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
        is_local: true,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: side_effect,
    }
}

fn registry(policy: WorkflowHistoryPolicy, metrics: Arc<RecordingMetrics>) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    Arc::new(HandlerRegistry::with_state_telemetry_and_history_policy(
        vec![
            workflow_info(GROWER, grower),
            workflow_info(LOCAL_RUNNER, local_runner),
            workflow_info(ROTATOR, rotator),
        ],
        vec![side_effect_activity()],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
        policy,
    ))
}

// ---------------------------------------------------------------------------
// Seeding helpers
// ---------------------------------------------------------------------------

/// Insert a RUNNING execution of `workflow_name` with a `WorkflowStarted`
/// event.
async fn seed_execution(conn: &mut AsyncPgConnection, workflow_name: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let input = serde_json::json!({});
    let workflow_id = format!("history-default-caps-{}", Uuid::new_v4());

    diesel::insert_into(harvest_workflow_executions::table)
        .values(NewWorkflowExecution {
            quota_key: None,
            continued_from_exec_id: None,
            first_exec_id: None,
            id: exec_id.as_uuid(),
            workflow_name,
            workflow_id: &workflow_id,
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: input.clone().into(),
            parent_id: None,
            queue_name: "default",
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
            start_source: None,
            start_source_ref: None,
            started_by: None,
        })
        .execute(conn)
        .await
        .expect("insert execution row");

    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");
    exec_id
}

/// Append `count` small inert `SignalReceived` events from event id 1.
///
/// One `INSERT ... SELECT` keeps a 50,000-row seed fast.
async fn pad_history(conn: &mut AsyncPgConnection, exec_id: ExecutionId, count: i32) {
    let padding = WorkflowEvent::SignalReceived {
        signal_name: "pad".into(),
        payload: serde_json::json!({}),
    };
    let event_data = serde_json::to_value(&padding).expect("serialize padding");
    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data) \
         SELECT $1, g, $2, $3 FROM generate_series(1, $4) AS g",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(padding.type_name())
    .bind::<Jsonb, _>(event_data)
    .bind::<Integer, _>(count)
    .execute(conn)
    .await
    .expect("pad history");
}

/// Append `rows` large inert `SignalReceived` events from `start_id`.
async fn pad_bytes(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    start_id: i32,
    rows: usize,
    bytes_each: usize,
) {
    let events: Vec<WorkflowEvent> = (0..rows)
        .map(|_| WorkflowEvent::SignalReceived {
            signal_name: "pad".into(),
            payload: incompressible_payload(bytes_each),
        })
        .collect();
    store::append_events(conn, exec_id, &events, start_id)
        .await
        .expect("pad history bytes");
}

async fn enqueue_workflow_task(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    queue_mod::enqueue(conn, &params).await.expect("enqueue");
}

/// A payload that Postgres cannot compress much: random hex text.
fn incompressible_payload(approx_bytes: usize) -> serde_json::Value {
    let mut text = String::with_capacity(approx_bytes);
    while text.len() < approx_bytes {
        text.push_str(&Uuid::new_v4().simple().to_string());
    }
    serde_json::json!({ "blob": text })
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

/// Stored bytes of the run's events, without the events of `appended_types`.
///
/// The failing decision measures before it appends those events, so this is
/// the exact value the typed reason must carry.
async fn stored_bytes_before_failure(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    appended_types: &[&str],
) -> u64 {
    let appended: Vec<String> = appended_types.iter().map(|t| (*t).to_owned()).collect();
    let row: Count = diesel::sql_query(
        "SELECT COALESCE(SUM(pg_column_size(event_data)), 0)::bigint AS n \
         FROM harvest_events \
         WHERE workflow_exec_id = $1 AND NOT (event_type = ANY($2))",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Array<Text>, _>(appended)
    .get_result(conn)
    .await
    .expect("sum stored history bytes");
    u64::try_from(row.n).expect("non-negative byte sum")
}

async fn grow_signal_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_type = 'SignalReceived' \
         AND event_data->'data'->>'signal_name' = 'grow'",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .expect("count grow signals");
    row.n
}

async fn open_task_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 \
         AND (state = 'PENDING' OR (state = 'RUNNING' AND worker_id IS NOT NULL))",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .expect("count open tasks");
    row.n
}

// ---------------------------------------------------------------------------
// Polling helpers
// ---------------------------------------------------------------------------

async fn load_execution(database_url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect for reload");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("load execution")
}

async fn wait_until<F>(
    database_url: &str,
    exec_id: ExecutionId,
    what: &str,
    done: F,
) -> WorkflowExecution
where
    F: Fn(&WorkflowExecution) -> bool + Send + Sync,
{
    let polled = tokio::time::timeout(LARGE_HISTORY_WAIT, async {
        loop {
            let execution = load_execution(database_url, exec_id).await;
            if done(&execution) {
                break execution;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    if let Ok(execution) = polled {
        return execution;
    }
    let execution = load_execution(database_url, exec_id).await;
    panic!(
        "timed out waiting for {what}; state={} error={:?}",
        execution.state, execution.error
    );
}

/// Wait until no workflow task for the run is pending or claimed.
///
/// A parked task is `RUNNING` with no `worker_id`. It waits for a wake, so it
/// does not count as open.
async fn wait_for_idle(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    tokio::time::timeout(LARGE_HISTORY_WAIT, async {
        while open_task_count(conn, exec_id).await > 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the run never went idle");
}

/// Send one `grow` signal and wait until the run has ingested it and is idle.
async fn send_grow(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    payload: serde_json::Value,
    expected_total: i64,
) {
    autumn_harvest::signal::send_signal(conn, exec_id, "grow", payload)
        .await
        .expect("send grow signal");
    tokio::time::timeout(LARGE_HISTORY_WAIT, async {
        while grow_signal_count(conn, exec_id).await < expected_total {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("signal was never ingested");
    wait_for_idle(conn, exec_id).await;
}

/// The typed reason on the run's DLQ row, if any.
async fn dead_letter_reason(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Option<DeadLetterReason> {
    let rows = dlq::list_dead_letters(conn, 50, None)
        .await
        .expect("list DLQ rows");
    rows.iter()
        .find(|row| row.workflow_exec_id == Some(exec_id.as_uuid()))
        .map(|row| {
            serde_json::from_str(&row.error).unwrap_or_else(|error| {
                panic!("DLQ error is not a typed reason ({error}): {}", row.error)
            })
        })
}

/// Assert the run failed with a typed byte-cap reason that carries exactly
/// the stored bytes and `cap`.
///
/// `appended_types` names the events the failing decision appends after it
/// measures: always `WorkflowFailed`, plus any prefix events.
async fn assert_byte_cap_failure(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    expected_cap: u64,
    workflow: &str,
    appended_types: &[&str],
) {
    let stored = stored_bytes_before_failure(conn, exec_id, appended_types).await;
    match dead_letter_reason(conn, exec_id).await {
        Some(DeadLetterReason::HistoryBytesCapExceeded {
            bytes,
            cap,
            workflow_type,
        }) => {
            assert_eq!(cap, expected_cap);
            assert_eq!(
                bytes, stored,
                "the reason must carry the exact stored bytes"
            );
            assert!(bytes >= cap, "bytes {bytes} must reach the cap {cap}");
            assert_eq!(workflow_type, workflow);
        }
        other => panic!("expected HistoryBytesCapExceeded, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Default event cap and warning
// ---------------------------------------------------------------------------

/// AC1 (RED before #1804): with no cap configured, a run at 50,000 events
/// fails with the typed `HistoryCapExceeded` reason. Before the default
/// cap, the run stayed RUNNING and kept growing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_event_cap_fails_a_run_at_fifty_thousand_events() {
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn, GROWER).await;
    // WorkflowStarted + 49,999 padding = 50,000 durable events.
    pad_history(&mut conn, exec_id, 49_999).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let worker = build_worker(
        "history-default-event-cap",
        registry(WorkflowHistoryPolicy::default(), Arc::clone(&metrics)),
        WARM,
    );
    let running = RunningWorker::start(&database_url, worker);
    let execution = wait_until(&database_url, exec_id, "the default event cap", |ex| {
        ex.state != "RUNNING"
    })
    .await;
    running.stop().await;

    assert_eq!(
        execution.state, "FAILED",
        "the default cap must fail the run"
    );
    match dead_letter_reason(&mut conn, exec_id).await {
        Some(DeadLetterReason::HistoryCapExceeded {
            count,
            cap,
            workflow_type,
        }) => {
            assert_eq!(cap, DEFAULT_HISTORY_EVENT_HARD_CAP);
            assert_eq!(cap, 50_000);
            assert_eq!(count, 50_000, "the run fails at exactly the cap");
            assert_eq!(workflow_type, GROWER);
        }
        other => panic!("expected HistoryCapExceeded, got {other:?}"),
    }
    // The crossing decision also stamps the early warning.
    assert_eq!(
        *metrics.history_bloat.lock().unwrap(),
        vec![GROWER.to_owned()]
    );
}

/// AC2 (RED before #1804): with no cap configured, the early-warning metric
/// fires once a still-running history reaches 10,240 events. Before the
/// default cap, nothing fired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_warning_fires_at_10240_events() {
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn, GROWER).await;
    // WorkflowStarted + 10,239 padding = 10,240 durable events.
    pad_history(&mut conn, exec_id, 10_239).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let worker = build_worker(
        "history-default-warning",
        registry(WorkflowHistoryPolicy::default(), Arc::clone(&metrics)),
        WARM,
    );
    let running = RunningWorker::start(&database_url, worker);
    let execution = wait_until(&database_url, exec_id, "the default warning", |ex| {
        ex.history_bloat_warned_at.is_some() || ex.state != "RUNNING"
    })
    .await;
    running.stop().await;

    assert_eq!(execution.state, "RUNNING", "a warning never ends the run");
    assert!(execution.history_bloat_warned_at.is_some());
    assert_eq!(
        *metrics.history_bloat.lock().unwrap(),
        vec![GROWER.to_owned()],
        "{METRIC_WORKFLOW_HISTORY_BLOAT} must fire once"
    );
}

/// The boundary below AC2: 10,239 events do not warn. A run past the
/// 10,000-event `continue_as_new` threshold has room to rotate first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_warning_does_not_fire_at_10239_events() {
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn, GROWER).await;
    // WorkflowStarted + 10,238 padding = 10,239 durable events.
    pad_history(&mut conn, exec_id, 10_238).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let worker = build_worker(
        "history-default-no-warning",
        registry(WorkflowHistoryPolicy::default(), Arc::clone(&metrics)),
        WARM,
    );
    let running = RunningWorker::start(&database_url, worker);
    wait_for_idle(&mut conn, exec_id).await;
    let execution = load_execution(&database_url, exec_id).await;
    running.stop().await;

    assert_eq!(execution.state, "RUNNING");
    assert!(execution.history_bloat_warned_at.is_none());
    assert!(metrics.history_bloat.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Byte cap
// ---------------------------------------------------------------------------

/// Under the default policy, a run at 50 MiB of stored history fails with
/// the typed `HistoryBytesCapExceeded` reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_byte_cap_fails_a_run_at_fifty_mib() {
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn, GROWER).await;
    // 51 events of about 1 MiB each: above the 50 MiB default.
    pad_bytes(&mut conn, exec_id, 1, 51, 1024 * 1024).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let worker = build_worker(
        "history-default-byte-cap",
        registry(WorkflowHistoryPolicy::default(), Arc::clone(&metrics)),
        WARM,
    );
    let running = RunningWorker::start(&database_url, worker);
    let execution = wait_until(&database_url, exec_id, "the default byte cap", |ex| {
        ex.state != "RUNNING"
    })
    .await;
    running.stop().await;

    assert_eq!(execution.state, "FAILED");
    assert_byte_cap_failure(
        &mut conn,
        exec_id,
        DEFAULT_HISTORY_BYTE_HARD_CAP,
        GROWER,
        &["WorkflowFailed"],
    )
    .await;
}

/// A signal loop reaches a 64 KiB byte cap. The signals arrive one at a
/// time, so each decision adds bytes to the measure.
///
/// The small signals between the two large ones guard against a double
/// count: with one, 40 KiB counts twice and the run fails too early. The
/// typed reason must carry exactly the stored bytes.
async fn byte_cap_signal_loop(sticky_timeout: Duration) -> Arc<RecordingMetrics> {
    const CAP: u64 = 64 * 1024;
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn, GROWER).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let policy = WorkflowHistoryPolicy::default().with_byte_hard_cap(CAP);
    let worker = build_worker(
        "history-byte-cap",
        registry(policy, Arc::clone(&metrics)),
        sticky_timeout,
    );
    let running = RunningWorker::start(&database_url, worker);
    wait_for_idle(&mut conn, exec_id).await;

    // About 40 KiB stored: below the cap.
    send_grow(&mut conn, exec_id, incompressible_payload(40 * 1024), 1).await;
    send_grow(&mut conn, exec_id, serde_json::json!({}), 2).await;
    send_grow(&mut conn, exec_id, serde_json::json!({}), 3).await;
    let execution = load_execution(&database_url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "the stored bytes are below the cap; error={:?}",
        execution.error
    );

    // The second large signal crosses the cap.
    let hits_before = metrics.hits();
    autumn_harvest::signal::send_signal(
        &mut conn,
        exec_id,
        "grow",
        incompressible_payload(40 * 1024),
    )
    .await
    .expect("send grow signal");
    let execution = wait_until(&database_url, exec_id, "the byte cap", |ex| {
        ex.state != "RUNNING"
    })
    .await;
    running.stop().await;

    assert_eq!(execution.state, "FAILED");
    assert_byte_cap_failure(&mut conn, exec_id, CAP, GROWER, &["WorkflowFailed"]).await;
    // Report whether the crossing decision itself was a cache hit.
    metrics
        .cache_hits
        .store(metrics.hits() - hits_before, Ordering::SeqCst);
    metrics
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_cap_is_exact_on_the_warm_cache_path() {
    let metrics = byte_cap_signal_loop(WARM).await;
    assert!(
        metrics.hits() >= 1,
        "the crossing decision must take the warm cache path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_cap_is_exact_on_the_cold_path() {
    let metrics = byte_cap_signal_loop(COLD).await;
    assert_eq!(
        metrics.hits(),
        0,
        "a zero sticky window must keep every decision cold"
    );
}

/// A run already over the byte cap runs no inline local activity. A local
/// activity has real side effects, so the cap must stop it first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_cap_stops_a_run_before_an_inline_local_activity() {
    const CAP: u64 = 16 * 1024;
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn, LOCAL_RUNNER).await;
    pad_bytes(&mut conn, exec_id, 1, 1, 40 * 1024).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let policy = WorkflowHistoryPolicy::default().with_byte_hard_cap(CAP);
    let worker = build_worker(
        "history-byte-cap-local",
        registry(policy, Arc::clone(&metrics)),
        WARM,
    );
    let running = RunningWorker::start(&database_url, worker);
    let execution = wait_until(&database_url, exec_id, "the byte cap", |ex| {
        ex.state != "RUNNING"
    })
    .await;
    running.stop().await;

    assert_eq!(execution.state, "FAILED");
    assert_eq!(
        AtomicUsize::load(&SIDE_EFFECT_RUNS, Ordering::SeqCst),
        0,
        "the local activity must not run once the run is over the byte cap"
    );
    // The decision appends `LocalActivityScheduled` before the gate, as the
    // event-cap gate does, then `WorkflowFailed`.
    assert_byte_cap_failure(
        &mut conn,
        exec_id,
        CAP,
        LOCAL_RUNNER,
        &["LocalActivityScheduled", "WorkflowFailed"],
    )
    .await;
}

/// A run over the byte cap can still rotate. `continue_as_new` moves it onto
/// a fresh history, so the cap does not fail it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_cap_lets_a_run_continue_as_new() {
    const CAP: u64 = 16 * 1024;
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn, ROTATOR).await;
    pad_bytes(&mut conn, exec_id, 1, 1, 40 * 1024).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let policy = WorkflowHistoryPolicy::default().with_byte_hard_cap(CAP);
    let worker = build_worker(
        "history-byte-cap-rotate",
        registry(policy, Arc::clone(&metrics)),
        WARM,
    );
    let running = RunningWorker::start(&database_url, worker);
    let execution = wait_until(&database_url, exec_id, "the rotation", |ex| {
        ex.state != "RUNNING"
    })
    .await;
    running.stop().await;

    assert_eq!(
        execution.state, "CONTINUED_AS_NEW",
        "error={:?}",
        execution.error
    );
    assert!(dead_letter_reason(&mut conn, exec_id).await.is_none());
}

/// A codec rotation rewrites stored rows in place and can change their size.
/// The warm cache then holds a stale byte mark. A stale mark at or above the
/// cap must not fail the run: the worker re-sums the full history first.
///
/// The test shrinks an old row in place to stand in for the rotation sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_cap_rechecks_a_stale_warm_mark_before_failing() {
    const CAP: u64 = 64 * 1024;
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn, GROWER).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let policy = WorkflowHistoryPolicy::default().with_byte_hard_cap(CAP);
    let worker = build_worker(
        "history-byte-cap-stale-mark",
        registry(policy, Arc::clone(&metrics)),
        WARM,
    );
    let running = RunningWorker::start(&database_url, worker);
    wait_for_idle(&mut conn, exec_id).await;

    // The warm mark now counts about 40 KiB for this signal.
    send_grow(&mut conn, exec_id, incompressible_payload(40 * 1024), 1).await;

    // Shrink that row in place, as a rotation sweep could. A fixture rewrite
    // turns the append-only guard off for one transaction (issue #1817).
    let small = serde_json::to_value(WorkflowEvent::SignalReceived {
        signal_name: "grow".into(),
        payload: serde_json::json!({}),
    })
    .expect("serialize small signal");
    autumn_harvest::append_only::with_guard_off(&mut conn, async |c| {
        diesel::sql_query(
            "UPDATE harvest_events SET event_data = $2 \
             WHERE workflow_exec_id = $1 AND event_type = 'SignalReceived'",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .bind::<Jsonb, _>(small)
        .execute(c)
        .await
    })
    .await
    .expect("shrink the stored signal");

    // Stale mark: about 40 + 30 = 70 KiB. True stored bytes: about 30 KiB.
    send_grow(&mut conn, exec_id, incompressible_payload(30 * 1024), 2).await;
    let execution = load_execution(&database_url, exec_id).await;
    running.stop().await;

    assert!(
        metrics.hits() >= 1,
        "the second decision must take the warm cache path"
    );
    assert_eq!(
        execution.state, "RUNNING",
        "a stale mark must not fail a run whose stored bytes are below the cap; \
         error={:?}",
        execution.error
    );
    assert!(dead_letter_reason(&mut conn, exec_id).await.is_none());
}

// ---------------------------------------------------------------------------
// Unlimited
// ---------------------------------------------------------------------------

/// An explicit "unlimited" keeps the pre-#1804 behaviour. A run past both
/// default caps ingests a signal and keeps running.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlimited_caps_let_a_run_grow_past_the_defaults() {
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn, GROWER).await;
    // 50,000 small events, then 51 events of about 1 MiB each.
    pad_history(&mut conn, exec_id, 49_999).await;
    pad_bytes(&mut conn, exec_id, 50_000, 51, 1024 * 1024).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let policy = WorkflowHistoryPolicy::default()
        .without_event_hard_cap()
        .without_byte_hard_cap();
    let worker = build_worker(
        "history-unlimited",
        registry(policy, Arc::clone(&metrics)),
        WARM,
    );
    let running = RunningWorker::start(&database_url, worker);
    wait_for_idle(&mut conn, exec_id).await;
    send_grow(&mut conn, exec_id, serde_json::json!({}), 1).await;
    let execution = load_execution(&database_url, exec_id).await;
    running.stop().await;

    assert_eq!(
        execution.state, "RUNNING",
        "unlimited caps must not fail the run; error={:?}",
        execution.error
    );
    assert!(dead_letter_reason(&mut conn, exec_id).await.is_none());
    assert!(metrics.history_bloat.lock().unwrap().is_empty());
}
