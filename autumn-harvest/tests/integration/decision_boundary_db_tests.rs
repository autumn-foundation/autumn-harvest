#![cfg(feature = "db")]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
//! Decision boundaries on a live worker (issue #1833).
//!
//! Each decision that appends events also appends one `DecisionCommitted`
//! with the worker's build and worker id, in the same transaction.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use autumn_harvest::context::WorkflowHistoryPolicy;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::prelude::*;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker};
use autumn_harvest::{ExecutionId, ShardId};

use chrono::Utc;
use diesel::sql_types::{BigInt, Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::integration_e2e::{
    build_test_pool, load_history_from_url, runtime_config, setup_test_database_url_or_env,
    spawn_test_worker, wait_for_execution_state_with_timeout,
};

const BUILD_ID: &str = "build-1833";

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

/// Two activities and a side effect: three decisions that write events.
#[workflow]
async fn boundary_two_steps(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let queue = ctx.queue_name().to_string();
    let first = ctx
        .execute_activity_raw("boundary_step", input.clone(), &queue)
        .await
        .map_err(|e| e.to_string())?;
    let token = ctx.new_uuid();
    let second = ctx
        .execute_activity_raw("boundary_step", input, &queue)
        .await
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "first": first, "second": second, "token": token }))
}

/// Waits for `approve`. A wake for another signal writes no event.
#[workflow]
async fn boundary_signal_wait(
    ctx: &WorkflowContext,
    _input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    ctx.wait_for_signal("approve")
        .await
        .map_err(|e| e.to_string())
}

/// Takes the mutex in `key` and holds it until `release`.
#[workflow]
async fn boundary_mutex_holder(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let key = input["key"].as_str().unwrap_or("k").to_string();
    let guard = ctx.mutex(key).acquire().await.map_err(|e| e.to_string())?;
    ctx.wait_for_signal("release")
        .await
        .map_err(|e| e.to_string())?;
    guard.release();
    Ok(serde_json::json!("released"))
}

/// The database a waiter writes through, and whether it did so.
static FOREIGN_URL: std::sync::OnceLock<String> = std::sync::OnceLock::new();
static FOREIGN_SENT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
const FOREIGN_WORKER: &str = "another-writer";

/// Waits for the held mutex in `key`. Its first run appends one row as
/// another writer, so the row lands after the decision start.
///
/// Activity workers and the timer service append to the history directly.
/// The row stands in for such a write. It is a boundary because replay
/// ignores boundaries, so the waiter still replays clean.
#[workflow]
async fn boundary_mutex_waiter(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    if let Some(url) = FOREIGN_URL.get()
        && !FOREIGN_SENT.swap(true, Ordering::SeqCst)
    {
        let mut conn = connect(url).await;
        let foreign = WorkflowEvent::DecisionCommitted {
            build_id: autumn_harvest::types::BuildId::new(BUILD_ID),
            worker_id: autumn_harvest::types::WorkerId::new(FOREIGN_WORKER),
        };
        store::append_events(&mut conn, ctx.info().execution_id, &[foreign], 1)
            .await
            .map_err(|e| e.to_string())?;
    }
    let key = input["key"].as_str().unwrap_or("k").to_string();
    let guard = ctx.mutex(key).acquire().await.map_err(|e| e.to_string())?;
    guard.release();
    Ok(serde_json::json!("acquired"))
}

/// The database the run pauses itself through, and whether it did so.
static PAUSE_URL: std::sync::OnceLock<String> = std::sync::OnceLock::new();
static PAUSE_SENT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Runs a local activity, pauses this run once, then waits for `go`.
///
/// The local activity writes its events inline. The pause commits after
/// them and before the decision persists. So the worker parks on its
/// early pause check, outside the persist transaction.
#[workflow]
async fn boundary_pause_inline(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    ctx.execute_local_activity_raw("boundary_step", input, None, None)
        .await
        .map_err(|e| e.to_string())?;
    if let Some(url) = PAUSE_URL.get()
        && !PAUSE_SENT.swap(true, Ordering::SeqCst)
    {
        let mut conn = connect(url).await;
        autumn_harvest::execution::pause_workflow_execution(
            &mut conn,
            ctx.info().execution_id,
            Some("hold"),
            "test",
            &autumn_harvest::telemetry::NoOpMetrics,
        )
        .await
        .map_err(|e| e.to_string())?;
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

#[activity(start_to_close = "30s")]
async fn boundary_step(
    _ctx: &ActivityContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    Ok(input)
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// Counts decisions. Each decision records one cache hit or one miss.
#[derive(Default)]
struct DecisionCount(AtomicU64);

impl DecisionCount {
    fn get(&self) -> u64 {
        AtomicU64::load(&self.0, Ordering::SeqCst)
    }
}

impl autumn_harvest::telemetry::MetricsRecorder for DecisionCount {
    fn record_workflow_cache_hit(&self, _workflow_name: &str, _queue: &str) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn record_workflow_cache_miss(&self, _workflow_name: &str, _queue: &str) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn registry(policy: WorkflowHistoryPolicy, decisions: Arc<DecisionCount>) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(autumn_harvest::telemetry::TelemetryConfig {
        metrics: decisions,
        ..Default::default()
    });
    Arc::new(
        HandlerRegistry::with_state_and_telemetry(
            vec![
                boundary_two_steps_info(),
                boundary_signal_wait_info(),
                boundary_mutex_holder_info(),
                boundary_mutex_waiter_info(),
                boundary_pause_inline_info(),
            ],
            activities![boundary_step],
            autumn_harvest::context::empty_shared_state(),
            telemetry,
        )
        .with_history_policy(policy),
    )
}

/// A running worker and the task that runs it.
struct Running {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
    worker_id: String,
    decisions: Arc<DecisionCount>,
}

impl Running {
    fn start(queue: &str, pool: &DbPool, policy: WorkflowHistoryPolicy) -> Self {
        let worker_id = format!("w1833-{}", Uuid::new_v4().simple());
        let mut config = runtime_config(&worker_id, 2, 2, Duration::from_secs(10));
        config.queues = vec![queue.to_string()];
        config.build_id = BUILD_ID.to_string();
        let decisions = Arc::new(DecisionCount::default());
        let worker = Arc::new(
            Worker::new(config, registry(policy, Arc::clone(&decisions))).expect("worker builds"),
        );
        let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
        Self {
            worker,
            handle,
            worker_id,
            decisions,
        }
    }

    /// Waits until `count` decisions ran and the workflow task is parked.
    async fn wait_parked_after(
        &self,
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        count: u64,
    ) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.decisions.get() < count || !is_parked(conn, exec_id).await {
            assert!(
                Instant::now() < deadline,
                "workflow did not park after {count} decision(s)"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn stop(self) {
        self.worker.shutdown();
        tokio::time::timeout(Duration::from_secs(15), self.handle)
            .await
            .expect("the worker must stop")
            .expect("the worker task must not panic");
    }
}

/// A queue name no earlier run used. A shared database can keep old rows.
fn unique(label: &str) -> String {
    format!("q1833-{label}-{}", Uuid::new_v4().simple())
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to Postgres")
}

/// Starts a run of `workflow` on `queue`.
async fn seed(
    conn: &mut AsyncPgConnection,
    workflow: &str,
    queue: &str,
    input: serde_json::Value,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: workflow,
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
        &[WorkflowEvent::workflow_started(input.clone(), Utc::now())],
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

/// A parked workflow task is `RUNNING` with no worker.
async fn is_parked(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> bool {
    #[derive(diesel::QueryableByName)]
    struct Parked {
        #[diesel(sql_type = BigInt)]
        parked: i64,
    }
    diesel::sql_query(
        "SELECT COUNT(*) AS parked FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow' \
           AND state = 'RUNNING' AND worker_id IS NULL",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .get_result::<Parked>(conn)
    .await
    .is_ok_and(|row| row.parked == 1)
}

async fn history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    load_history_from_url(url, exec_id).await.events
}

fn type_names(events: &[WorkflowEvent]) -> Vec<&'static str> {
    events.iter().map(WorkflowEvent::type_name).collect()
}

fn boundaries(events: &[WorkflowEvent]) -> Vec<(String, String)> {
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::DecisionCommitted {
                build_id,
                worker_id,
            } => Some((build_id.to_string(), worker_id.to_string())),
            _ => None,
        })
        .collect()
}

/// Runs `boundary_two_steps` to completion and returns its id and history.
async fn completed_two_steps(
    url: &str,
    pool: &DbPool,
    policy: WorkflowHistoryPolicy,
) -> (ExecutionId, Vec<WorkflowEvent>, String) {
    let queue = unique("two");
    let running = Running::start(&queue, pool, policy);
    let worker_id = running.worker_id.clone();
    let mut conn = connect(url).await;
    let exec_id = seed(
        &mut conn,
        "boundary_two_steps",
        &queue,
        serde_json::json!({"n": 1}),
    )
    .await;
    wait_for_execution_state_with_timeout(url, exec_id, "COMPLETED", Duration::from_secs(30)).await;
    running.stop().await;
    (exec_id, history(url, exec_id).await, worker_id)
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn each_decision_ends_with_one_boundary_naming_build_and_worker() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let (_, events, worker_id) =
        completed_two_steps(&url, &pool, WorkflowHistoryPolicy::default()).await;

    assert_eq!(
        type_names(&events),
        [
            "WorkflowStarted",
            "ActivityScheduled",
            "DecisionCommitted",
            "ActivityStarted",
            "ActivityCompleted",
            "SideEffectRecorded",
            "ActivityScheduled",
            "DecisionCommitted",
            "ActivityStarted",
            "ActivityCompleted",
            "WorkflowCompleted",
            "DecisionCommitted",
        ]
    );
    let expected = (BUILD_ID.to_string(), worker_id);
    assert_eq!(boundaries(&events), vec![expected; 3]);
}

#[tokio::test]
async fn a_wake_that_appends_nothing_records_no_boundary() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("signal");
    let running = Running::start(&queue, &pool, WorkflowHistoryPolicy::default());
    let mut conn = connect(&url).await;
    let exec_id = seed(
        &mut conn,
        "boundary_signal_wait",
        &queue,
        serde_json::json!({}),
    )
    .await;

    // Decision 1 only parks on the signal wait.
    running.wait_parked_after(&mut conn, exec_id, 1).await;
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "other", serde_json::json!(1))
        .await
        .expect("send other");
    // Decision 2 reads `other` and parks again. It appends nothing.
    running.wait_parked_after(&mut conn, exec_id, 2).await;
    assert_eq!(
        type_names(&history(&url, exec_id).await),
        ["WorkflowStarted", "SignalReceived"],
        "two no-op decisions must write no boundary"
    );
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "approve", serde_json::json!(2))
        .await
        .expect("send approve");
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;
    running.stop().await;

    let events = history(&url, exec_id).await;
    assert_eq!(
        type_names(&events),
        [
            "WorkflowStarted",
            "SignalReceived",
            "SignalReceived",
            "WorkflowCompleted",
            "DecisionCommitted",
        ]
    );
}

#[tokio::test]
async fn a_contended_acquire_records_no_boundary_after_a_foreign_write() {
    // A contended acquire only enqueues. It writes no event, though the
    // history-cap estimate counts one. A row another writer commits while
    // the decision runs is not this decision's write. So no boundary of
    // this worker follows it.
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("mutex");
    let key = format!("k1833-{}", Uuid::new_v4().simple());
    let running = Running::start(&queue, &pool, WorkflowHistoryPolicy::default());
    let mut conn = connect(&url).await;
    let holder = seed(
        &mut conn,
        "boundary_mutex_holder",
        &queue,
        serde_json::json!({ "key": key }),
    )
    .await;
    running.wait_parked_after(&mut conn, holder, 1).await;

    FOREIGN_URL.set(url.clone()).expect("one test sets the URL");
    let waiter = seed(
        &mut conn,
        "boundary_mutex_waiter",
        &queue,
        serde_json::json!({ "key": key }),
    )
    .await;
    running.wait_parked_after(&mut conn, waiter, 2).await;
    assert!(
        std::sync::atomic::AtomicBool::load(&FOREIGN_SENT, Ordering::SeqCst),
        "the foreign write ran"
    );
    running.stop().await;

    let events = history(&url, waiter).await;
    assert_eq!(
        boundaries(&events),
        vec![(BUILD_ID.to_string(), FOREIGN_WORKER.to_string())],
        "an enqueue-only decision must write no boundary: {:?}",
        type_names(&events)
    );
}

#[tokio::test]
async fn a_pause_before_persist_keeps_the_boundary_of_inline_writes() {
    // The local activity writes events inline, then pauses the run. The
    // worker sees the pause before it persists and parks. Those inline
    // events are this decision's writes, so its boundary must follow.
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("pause");
    let running = Running::start(&queue, &pool, WorkflowHistoryPolicy::default());
    let worker_id = running.worker_id.clone();
    PAUSE_URL.set(url.clone()).expect("one test sets the URL");
    let mut conn = connect(&url).await;
    let exec_id = seed(
        &mut conn,
        "boundary_pause_inline",
        &queue,
        serde_json::json!({}),
    )
    .await;
    wait_for_execution_state_with_timeout(&url, exec_id, "PAUSED", Duration::from_secs(30)).await;
    // A paused park keeps a sticky hint, so `is_parked` does not apply.
    // Stopping the worker drains the decision in flight.
    running.stop().await;

    let events = history(&url, exec_id).await;
    assert_eq!(
        boundaries(&events),
        vec![(BUILD_ID.to_string(), worker_id)],
        "{:?}",
        type_names(&events)
    );
}

#[tokio::test]
async fn the_opt_out_records_no_boundary() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let policy = WorkflowHistoryPolicy::default().with_decision_boundaries(false);
    let (_, events, _) = completed_two_steps(&url, &pool, policy).await;
    assert!(boundaries(&events).is_empty(), "{:?}", type_names(&events));
    assert_eq!(events.len(), 9);
}

#[tokio::test]
async fn a_recorded_history_with_boundaries_replays_clean() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let (exec_id, events, _) =
        completed_two_steps(&url, &pool, WorkflowHistoryPolicy::default()).await;
    assert_eq!(boundaries(&events).len(), 3);

    let mut conn = connect(&url).await;
    let report = WorkflowReplayer::new()
        .register_fn("boundary_two_steps", boundary_two_steps_info().handler)
        .replay_from_db(&mut conn, exec_id)
        .await
        .expect("replay from db");
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{report}"
    );
}

/// One row of the storage measurement.
#[derive(diesel::QueryableByName, Debug)]
struct SizeRow {
    #[diesel(sql_type = Text)]
    event_type: String,
    #[diesel(sql_type = BigInt)]
    rows: i64,
    #[diesel(sql_type = BigInt)]
    data_bytes: i64,
    #[diesel(sql_type = BigInt)]
    row_bytes: i64,
}

/// Measures the storage cost of boundaries (issue #1833, AC4).
///
/// `data_bytes` is `pg_column_size(event_data)`, the figure the history
/// quota counts. `row_bytes` is `pg_column_size` of the whole row. Neither
/// counts index entries. `docs/decision-boundaries.md` records the result.
#[tokio::test]
async fn measure_boundary_storage_overhead() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let (exec_id, _, _) = completed_two_steps(&url, &pool, WorkflowHistoryPolicy::default()).await;

    let mut conn = connect(&url).await;
    let rows: Vec<SizeRow> = diesel::sql_query(
        "SELECT CASE WHEN event_type = 'DecisionCommitted' THEN event_type ELSE 'other' END \
                AS event_type, \
                COUNT(*) AS rows, \
                SUM(pg_column_size(event_data))::BIGINT AS data_bytes, \
                SUM(pg_column_size(e.*))::BIGINT AS row_bytes \
         FROM harvest_events e \
         WHERE workflow_exec_id = $1 \
         GROUP BY 1 ORDER BY 1",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("measure sizes");

    let boundary = rows
        .iter()
        .find(|row| row.event_type == "DecisionCommitted")
        .expect("boundary rows");
    let other = rows
        .iter()
        .find(|row| row.event_type == "other")
        .expect("other rows");
    println!("issue #1833 storage: {rows:#?}");
    assert_eq!(boundary.rows, 3);
    // The JSON text of one boundary here is about 115 bytes: the type tag,
    // two keys, a 10-byte build id and a 38-byte worker id. The jsonb form
    // adds a header and offsets. 160 bytes catches a field added by mistake.
    let per_boundary = boundary.data_bytes / boundary.rows;
    let per_other = other.data_bytes / other.rows;
    assert!(
        per_boundary < 160,
        "a boundary costs {per_boundary} bytes of event_data"
    );
    assert!(
        per_boundary < per_other,
        "a boundary ({per_boundary} B) must cost less than an average event ({per_other} B)"
    );
}
