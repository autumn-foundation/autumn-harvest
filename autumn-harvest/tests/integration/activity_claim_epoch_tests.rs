//! Claim-epoch fence on activity ownership writes (issue #1789).
//!
//! Every test runs one scenario. Worker A claims an activity task. A's
//! liveness row goes stale, so the orphan reclaimer requeues the row. Worker
//! B then claims the same row as the next attempt. A's late writes must not
//! change B's attempt.
//!
//! The requeue runs the reclaimer's own statement against this one row. Each
//! test uses its own queue and worker ids, and B keeps a live liveness row.
//! A background reclaimer in the same database can therefore race only on
//! A's requeue, and the helper accepts that outcome.

use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::poison_pill::requeue_orphan_stmt;
use autumn_harvest::queue::{self, ClaimWrite, EnqueueParams, TaskClaim, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use autumn_harvest::worker::{
    DbPool, append_activity_started_for_test, fail_task_and_execution_with_history,
    finalize_activity_completion, finalize_activity_failure, observe_task_cancellation,
    preload_failure_history, requeue_workflow_task_after_event_id_conflict,
};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const ACTIVITY: &str = "claim_epoch_activity";

// ---------------------------------------------------------------------------
// Setup
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
        .max_size(4)
        .build()
        .expect("pool build")
}

async fn insert_execution(conn: &mut AsyncPgConnection, queue: &str) -> ExecutionId {
    let exec_id = ExecutionId::new();
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&NewWorkflowExecution {
            quota_key: None,
            continued_from_exec_id: None,
            first_exec_id: None,
            id: exec_id.as_uuid(),
            workflow_name: "claim_epoch_wf",
            workflow_id: &format!("wf-claim-epoch-{}", Uuid::new_v4()),
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: serde_json::json!({}).into(),
            memo: None,
            search_attrs: None,
            queue_name: queue,
            parent_id: None,
            parent_close_policy: None,
            assigned_build_id: None,
            execution_timeout: None,
            deadline_at: None,
            chain_execution_timeout: None,
            chain_deadline_at: None,
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
        .expect("insert execution");
    exec_id
}

/// One scheduled activity on its own queue, with its own worker ids.
struct Fixture {
    queue: String,
    worker_a: String,
    worker_b: String,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    task_id: Uuid,
}

/// Insert or refresh a live liveness row for `worker_id`.
async fn live_worker(conn: &mut AsyncPgConnection, worker_id: &str) {
    diesel::sql_query(
        "INSERT INTO harvest_workers (worker_id, last_heartbeat_at, max_concurrency, host) \
         VALUES ($1, NOW(), 10, 'localhost') \
         ON CONFLICT (worker_id) DO UPDATE SET last_heartbeat_at = NOW()",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(conn)
    .await
    .expect("upsert live worker");
}

/// Make the liveness row of `worker_id` stale, as a partition or stall does.
async fn stale_worker(conn: &mut AsyncPgConnection, worker_id: &str) {
    diesel::sql_query(
        "UPDATE harvest_workers SET last_heartbeat_at = NOW() - INTERVAL '1 hour' \
         WHERE worker_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(conn)
    .await
    .expect("stale worker");
}

async fn seed_activity(conn: &mut AsyncPgConnection) -> Fixture {
    let run = Uuid::new_v4();
    let queue = format!("claim-epoch-{run}");
    let worker_a = format!("claim-epoch-a-{run}");
    let worker_b = format!("claim-epoch-b-{run}");
    live_worker(conn, &worker_a).await;
    live_worker(conn, &worker_b).await;
    let exec_id = insert_execution(conn, &queue).await;
    let activity_id = ActivityExecId::new();
    store::append_events(
        conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: ACTIVITY.to_string(),
                input: serde_json::json!({}),
                queue: queue.clone(),
            },
        ],
        0,
    )
    .await
    .expect("seed history");

    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some(ACTIVITY.to_string());
    params.activity_id = Some(activity_id.as_uuid());
    params.max_attempts = 5;
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    let task_id = queue::enqueue(conn, &params).await.expect("enqueue");
    Fixture {
        queue,
        worker_a,
        worker_b,
        exec_id,
        activity_id,
        task_id,
    }
}

async fn claim(conn: &mut AsyncPgConnection, fx: &Fixture, worker_id: &str) -> TaskQueueItem {
    let task = queue::claim_task(
        conn,
        std::slice::from_ref(&fx.queue),
        worker_id,
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("task is claimable");
    assert_eq!(task.id, fx.task_id);
    task
}

/// Make A's liveness stale, then run the orphan reclaimer's requeue
/// statement on this row only.
///
/// The `SELECT ... FOR UPDATE` comes first, as `requeue_orphan` requires. A
/// background reclaimer can requeue the row first. The row is then already
/// `PENDING` with no worker, which is the same outcome.
async fn requeue_orphan(conn: &mut AsyncPgConnection, task: &TaskQueueItem) {
    let task_id = task.id;
    let worker_id = task.worker_id.clone().expect("claimed task has a worker");
    let strikes = task.crash_strikes;
    stale_worker(conn, &worker_id).await;
    let requeued = conn
        .transaction::<usize, diesel::result::Error, _>(async |conn| {
            diesel::sql_query("SELECT id FROM harvest_task_queue WHERE id = $1 FOR UPDATE")
                .bind::<diesel::sql_types::Uuid, _>(task_id)
                .execute(conn)
                .await?;
            diesel::sql_query(requeue_orphan_stmt())
                .bind::<diesel::sql_types::Uuid, _>(task_id)
                .bind::<diesel::sql_types::Text, _>(&worker_id)
                .bind::<diesel::sql_types::Integer, _>(strikes)
                .bind::<diesel::sql_types::Integer, _>(strikes + 1)
                .bind::<diesel::sql_types::BigInt, _>(10_i64)
                .bind::<diesel::sql_types::Timestamptz, _>(Utc::now())
                .execute(conn)
                .await
        })
        .await
        .expect("requeue orphan");
    if requeued == 0 {
        let r = row(conn, task_id).await;
        assert_eq!(
            (r.state.as_str(), r.worker_id),
            ("PENDING", None),
            "the reclaimer must requeue A's orphaned claim"
        );
    }
}

/// A claims and starts. The reclaimer requeues. B claims and starts.
async fn a_then_b(conn: &mut AsyncPgConnection, fx: &Fixture) -> (TaskQueueItem, TaskQueueItem) {
    let codecs = PayloadCodecs::default();
    let a = claim(conn, fx, &fx.worker_a).await;
    let started =
        append_activity_started_for_test(conn, &a, fx.exec_id, ACTIVITY, &fx.worker_a, &codecs)
            .await
            .expect("A starts");
    assert_eq!(started, Some(fx.activity_id));
    requeue_orphan(conn, &a).await;
    let b = claim(conn, fx, &fx.worker_b).await;
    assert!(
        b.attempt > a.attempt,
        "a reclaim must give B a later attempt"
    );
    let started =
        append_activity_started_for_test(conn, &b, fx.exec_id, ACTIVITY, &fx.worker_b, &codecs)
            .await
            .expect("B starts");
    assert_eq!(started, Some(fx.activity_id));
    (a, b)
}

#[derive(QueryableByName, Debug, PartialEq)]
struct RowState {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    worker_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    attempt: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    heartbeat_details: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    output: Option<serde_json::Value>,
}

async fn row(conn: &mut AsyncPgConnection, task_id: Uuid) -> RowState {
    diesel::sql_query(
        "SELECT state, worker_id, attempt, last_heartbeat_at, heartbeat_details, output \
         FROM harvest_task_queue WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .get_result(conn)
    .await
    .expect("load task row")
}

async fn events(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    store::load_history(conn, exec_id)
        .await
        .expect("history")
        .events
}

fn completed_outputs(events: &[WorkflowEvent]) -> Vec<serde_json::Value> {
    events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityCompleted { output, .. } => Some(output.clone()),
            _ => None,
        })
        .collect()
}

fn count_failed(events: &[WorkflowEvent]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count()
}

fn count_started_by(events: &[WorkflowEvent], worker: &str) -> usize {
    events
        .iter()
        .filter(|e| {
            matches!(e, WorkflowEvent::ActivityStarted { worker_id, .. } if worker_id.as_str() == worker)
        })
        .count()
}

/// Make B's row carry a known heartbeat, so a change is visible.
async fn b_heartbeats(conn: &mut AsyncPgConnection, b: &TaskQueueItem) -> RowState {
    let claim_b = TaskClaim::of(b).expect("B holds a claim");
    let write = queue::record_heartbeat(conn, &claim_b, serde_json::json!({"owner": "B"}))
        .await
        .expect("B heartbeats");
    assert_eq!(write, ClaimWrite::Applied);
    row(conn, b.id).await
}

// ---------------------------------------------------------------------------
// Completion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stale_completion_while_b_runs_changes_nothing_and_b_wins() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (a, b) = a_then_b(&mut conn, &fx).await;
    let codecs = PayloadCodecs::default();
    let before = row(&mut conn, fx.task_id).await;

    finalize_activity_completion(
        &mut conn,
        &a,
        fx.exec_id,
        fx.activity_id,
        serde_json::json!("from A"),
        None,
        &codecs,
    )
    .await
    .expect("a stale completion is a no-op, not an error");

    assert!(
        completed_outputs(&events(&mut conn, fx.exec_id).await).is_empty(),
        "A's stale completion must append no ActivityCompleted"
    );
    assert_eq!(
        row(&mut conn, fx.task_id).await,
        before,
        "A's stale completion must leave B's claim untouched"
    );

    finalize_activity_completion(
        &mut conn,
        &b,
        fx.exec_id,
        fx.activity_id,
        serde_json::json!("from B"),
        None,
        &codecs,
    )
    .await
    .expect("B completes");

    assert_eq!(
        completed_outputs(&events(&mut conn, fx.exec_id).await),
        vec![serde_json::json!("from B")],
        "exactly one ActivityCompleted, and it is B's"
    );
    let after = row(&mut conn, fx.task_id).await;
    assert_eq!(after.state, "COMPLETED");
    assert_eq!(after.output, Some(serde_json::json!("from B")));
}

/// Regression guard for the order that the issue names. The history check
/// already blocks it, because B's terminal event exists, so it does not prove
/// the fence on its own.
#[tokio::test]
async fn stale_completion_after_b_completes_changes_nothing() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (a, b) = a_then_b(&mut conn, &fx).await;
    let codecs = PayloadCodecs::default();

    finalize_activity_completion(
        &mut conn,
        &b,
        fx.exec_id,
        fx.activity_id,
        serde_json::json!("from B"),
        None,
        &codecs,
    )
    .await
    .expect("B completes");
    finalize_activity_completion(
        &mut conn,
        &a,
        fx.exec_id,
        fx.activity_id,
        serde_json::json!("from A"),
        None,
        &codecs,
    )
    .await
    .expect("a late completion is a no-op, not an error");

    assert_eq!(
        completed_outputs(&events(&mut conn, fx.exec_id).await),
        vec![serde_json::json!("from B")],
        "exactly one ActivityCompleted, and it is B's"
    );
    assert_eq!(
        row(&mut conn, fx.task_id).await.output,
        Some(serde_json::json!("from B"))
    );
}

#[tokio::test]
async fn stale_claim_cannot_complete_the_row_directly() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (a, _b) = a_then_b(&mut conn, &fx).await;
    let before = row(&mut conn, fx.task_id).await;

    let claim_a = TaskClaim::of(&a).expect("A held a claim");
    let write = queue::complete_claimed_task(&mut conn, &claim_a, serde_json::json!("from A"))
        .await
        .expect("query runs");

    assert_eq!(write, ClaimWrite::LeaseLost);
    assert_eq!(row(&mut conn, fx.task_id).await, before);
}

// ---------------------------------------------------------------------------
// Failure and retry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stale_failure_while_b_runs_changes_nothing() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (a, _b) = a_then_b(&mut conn, &fx).await;
    let codecs = PayloadCodecs::default();
    let before = row(&mut conn, fx.task_id).await;

    finalize_activity_failure(
        &mut conn,
        &a,
        fx.exec_id,
        fx.activity_id,
        "A failed",
        &codecs,
    )
    .await
    .expect("a stale failure is a no-op, not an error");

    assert_eq!(count_failed(&events(&mut conn, fx.exec_id).await), 0);
    assert_eq!(row(&mut conn, fx.task_id).await, before);
}

#[tokio::test]
async fn stale_claim_cannot_fail_requeue_or_defer_the_row() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (a, _b) = a_then_b(&mut conn, &fx).await;
    let before = row(&mut conn, fx.task_id).await;
    let claim_a = TaskClaim::of(&a).expect("A held a claim");

    let failed = queue::fail_claimed_task(&mut conn, &claim_a, "A failed")
        .await
        .expect("query runs");
    let requeued =
        queue::requeue_claimed_task_for_retry(&mut conn, &claim_a, chrono::Duration::zero(), "A")
            .await
            .expect("query runs");
    let deferred = queue::defer_claimed_rate_limited_task(&mut conn, &claim_a, Utc::now())
        .await
        .expect("query runs");

    assert_eq!(failed, ClaimWrite::LeaseLost);
    assert_eq!(requeued, ClaimWrite::LeaseLost);
    assert_eq!(deferred, ClaimWrite::LeaseLost);
    assert_eq!(
        row(&mut conn, fx.task_id).await,
        before,
        "no stale write may change B's claim, and attempt must not go down"
    );
}

#[tokio::test]
async fn current_claim_writes_still_apply() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (_a, b) = a_then_b(&mut conn, &fx).await;
    let claim_b = TaskClaim::of(&b).expect("B holds a claim");

    let requeued =
        queue::requeue_claimed_task_for_retry(&mut conn, &claim_b, chrono::Duration::zero(), "B")
            .await
            .expect("query runs");

    assert_eq!(requeued, ClaimWrite::Applied);
    let after = row(&mut conn, fx.task_id).await;
    assert_eq!(after.state, "PENDING");
    assert_eq!(after.worker_id, None);
}

// ---------------------------------------------------------------------------
// Start fence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stale_start_after_b_claims_appends_nothing() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let codecs = PayloadCodecs::default();
    let a = claim(&mut conn, &fx, &fx.worker_a).await;
    requeue_orphan(&mut conn, &a).await;
    let _b = claim(&mut conn, &fx, &fx.worker_b).await;

    let started = append_activity_started_for_test(
        &mut conn,
        &a,
        fx.exec_id,
        ACTIVITY,
        &fx.worker_a,
        &codecs,
    )
    .await
    .expect("a stale start is a no-op, not an error");

    assert_eq!(started, None, "A must not start under B's claim");
    assert_eq!(
        count_started_by(&events(&mut conn, fx.exec_id).await, &fx.worker_a),
        0
    );
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stale_heartbeat_leaves_b_checkpoint_and_timestamp_unchanged() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (a, b) = a_then_b(&mut conn, &fx).await;
    let before = b_heartbeats(&mut conn, &b).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let claim_a = TaskClaim::of(&a).expect("A held a claim");
    let write = queue::record_heartbeat(&mut conn, &claim_a, serde_json::json!({"owner": "A"}))
        .await
        .expect("query runs");

    assert_eq!(write, ClaimWrite::LeaseLost);
    let after = row(&mut conn, fx.task_id).await;
    assert_eq!(after.last_heartbeat_at, before.last_heartbeat_at);
    assert_eq!(
        after.heartbeat_details,
        Some(serde_json::json!({"owner": "B"}))
    );
}

#[tokio::test]
async fn heartbeat_flusher_cancels_the_token_when_the_lease_is_lost() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (a, b) = a_then_b(&mut conn, &fx).await;
    let before = b_heartbeats(&mut conn, &b).await;
    let pool = build_pool(&url);
    let cancel = CancellationToken::new();

    let tx = autumn_harvest::heartbeat::spawn_heartbeat_flusher(
        TaskClaim::of(&a).expect("A held a claim"),
        pool,
        cancel.clone(),
    );
    tx.send(serde_json::json!({"owner": "A"}))
        .await
        .expect("send heartbeat");

    tokio::time::timeout(Duration::from_secs(10), cancel.cancelled())
        .await
        .expect("a lease-lost heartbeat must cancel the activity's token");
    let after = row(&mut conn, fx.task_id).await;
    assert_eq!(after.heartbeat_details, before.heartbeat_details);
}

#[tokio::test]
async fn heartbeat_flusher_keeps_running_while_the_claim_is_held() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (_a, b) = a_then_b(&mut conn, &fx).await;
    let pool = build_pool(&url);
    let cancel = CancellationToken::new();

    let tx = autumn_harvest::heartbeat::spawn_heartbeat_flusher(
        TaskClaim::of(&b).expect("B holds a claim"),
        pool,
        cancel.clone(),
    );
    tx.send(serde_json::json!({"owner": "B", "n": 1}))
        .await
        .expect("send heartbeat");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let r = row(&mut conn, fx.task_id).await;
        if r.heartbeat_details == Some(serde_json::json!({"owner": "B", "n": 1})) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "B's heartbeat must flush"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // One more flush interval, so a wrong verdict on the next flush shows.
    tx.send(serde_json::json!({"owner": "B", "n": 2}))
        .await
        .expect("send heartbeat");
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(
        !cancel.is_cancelled(),
        "a held claim must not cancel the token"
    );
    cancel.cancel();
}

// ---------------------------------------------------------------------------
// Cancellation observer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancellation_observer_resolves_when_the_lease_is_lost() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (a, _b) = a_then_b(&mut conn, &fx).await;
    let pool = build_pool(&url);
    let claim_a = TaskClaim::of(&a).expect("A held a claim");

    tokio::time::timeout(
        Duration::from_secs(10),
        observe_task_cancellation(&pool, &claim_a),
    )
    .await
    .expect("the observer must report a lost lease as a cancellation");
}

#[tokio::test]
async fn cancellation_observer_waits_while_the_claim_is_held() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (_a, b) = a_then_b(&mut conn, &fx).await;
    let pool = build_pool(&url);
    let claim_b = TaskClaim::of(&b).expect("B holds a claim");

    let waited = tokio::time::timeout(
        Duration::from_millis(1_500),
        observe_task_cancellation(&pool, &claim_b),
    )
    .await;

    assert!(waited.is_err(), "a held claim must not read as cancelled");
}

#[tokio::test]
async fn heartbeat_flusher_cancels_the_token_when_the_row_is_terminal() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let (_a, b) = a_then_b(&mut conn, &fx).await;
    let claim_b = TaskClaim::of(&b).expect("B holds a claim");
    let done = queue::complete_claimed_task(&mut conn, &claim_b, serde_json::json!("done"))
        .await
        .expect("B completes");
    assert_eq!(done, ClaimWrite::Applied);
    let cancel = CancellationToken::new();

    let tx = autumn_harvest::heartbeat::spawn_heartbeat_flusher(
        claim_b,
        build_pool(&url),
        cancel.clone(),
    );
    tx.send(serde_json::json!({"late": true}))
        .await
        .expect("send heartbeat");

    tokio::time::timeout(Duration::from_secs(10), cancel.cancelled())
        .await
        .expect("a heartbeat on a terminal row must cancel the activity's token");
    assert_eq!(row(&mut conn, fx.task_id).await.heartbeat_details, None);
}

// ---------------------------------------------------------------------------
// Execution failure guard
// ---------------------------------------------------------------------------

/// `crash_strikes` can return to an earlier value. Then a later claim of the
/// same worker passes the `(worker_id, crash_strikes)` checks of the
/// execution failure path. The claim epoch must still stop the stale write.
#[tokio::test]
async fn stale_execution_failure_under_a_reused_strike_count_changes_nothing() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let codecs = PayloadCodecs::default();
    let w = fx.worker_a.clone();

    let first = claim(&mut conn, &fx, &w).await;
    requeue_orphan(&mut conn, &first).await;
    live_worker(&mut conn, &w).await;
    let second = claim(&mut conn, &fx, &w).await;
    let released =
        queue::release_terminal_workflow_claim(&mut conn, second.id, &w, second.crash_strikes)
            .await
            .expect("release runs");
    assert!(released, "the release must put the row back to PENDING");
    let third = claim(&mut conn, &fx, &w).await;
    assert_eq!(
        (third.crash_strikes, third.worker_id.as_deref()),
        (first.crash_strikes, Some(w.as_str())),
        "the third claim must reuse the first claim's guard values"
    );
    assert!(third.attempt > first.attempt);
    let before = row(&mut conn, fx.task_id).await;

    let preloaded = preload_failure_history(&mut conn, &first).await;
    fail_task_and_execution_with_history(&mut conn, &first, &w, "stale", preloaded, &codecs)
        .await
        .expect("a stale execution failure is a no-op, not an error");

    assert!(
        !events(&mut conn, fx.exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a stale attempt must not fail the workflow"
    );
    assert_eq!(row(&mut conn, fx.task_id).await, before);
}

// ---------------------------------------------------------------------------
// Worker end to end
// ---------------------------------------------------------------------------

const E2E_ACTIVITY: &str = "claim_epoch_e2e_activity";

static E2E_STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static E2E_STOPPED: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

type BoxFut<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
>;

fn e2e_workflow(ctx: &autumn_harvest::WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = input["queue"].as_str().unwrap_or("default").to_string();
        ctx.execute_activity_raw(E2E_ACTIVITY, input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Heartbeat until the engine reports the lost claim, then return a result
/// that must never reach history.
fn e2e_activity(ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        E2E_STARTED.store(true, std::sync::atomic::Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut n = 0_u64;
        let stop = loop {
            n += 1;
            if let Err(e) = ctx.heartbeat(serde_json::json!({"stale": n})).await {
                break format!("heartbeat: {e}");
            }
            if ctx.is_cancelled() {
                break "token cancelled".to_string();
            }
            if tokio::time::Instant::now() > deadline {
                break "deadline".to_string();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        *E2E_STOPPED.lock().expect("lock") = Some(stop);
        Ok(serde_json::json!("from the stale attempt"))
    })
}

fn e2e_registry() -> std::sync::Arc<autumn_harvest::worker::HandlerRegistry> {
    use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
    let telemetry = std::sync::Arc::new(
        autumn_harvest::telemetry::TelemetryConfig::builder()
            .metrics(std::sync::Arc::new(autumn_harvest::telemetry::NoOpMetrics)
                as std::sync::Arc<dyn autumn_harvest::telemetry::MetricsRecorder>)
            .build(),
    );
    let workflow = WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "claim_epoch_e2e_wf",
        module: "activity_claim_epoch_tests",
        handler: e2e_workflow,
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
    };
    let activity = ActivityInfo {
        name: E2E_ACTIVITY,
        module: "activity_claim_epoch_tests",
        default_retry_policy: None,
        default_start_to_close: None,
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
        handler: e2e_activity,
    };
    std::sync::Arc::new(
        autumn_harvest::worker::HandlerRegistry::with_state_and_telemetry(
            vec![workflow],
            vec![activity],
            autumn_harvest::context::empty_shared_state(),
            telemetry,
        ),
    )
}

fn e2e_worker(
    worker_id: &str,
    queue: &str,
    registry: std::sync::Arc<autumn_harvest::worker::HandlerRegistry>,
) -> std::sync::Arc<autumn_harvest::worker::Worker> {
    use autumn_harvest::types::ShardId;
    std::sync::Arc::new(
        autumn_harvest::worker::Worker::new(
            autumn_harvest::worker::WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: worker_id.to_string(),
                queues: vec![queue.to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 2,
                max_concurrent_activities: 2,
                poll_interval: Duration::from_millis(25),
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
        .expect("worker builds"),
    )
}

#[derive(QueryableByName)]
struct ActivityTaskRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
}

/// Seed a started workflow on `queue` with one workflow task.
async fn seed_e2e_workflow(conn: &mut AsyncPgConnection, queue: &str) -> ExecutionId {
    let exec_id = insert_execution(conn, queue).await;
    let input = serde_json::json!({ "queue": queue });
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
    .expect("seed history");
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET workflow_name = 'claim_epoch_e2e_wf' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("name the workflow");
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");
    exec_id
}

/// Wait until the worker runs the activity, and return its task id.
async fn wait_for_e2e_start(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    worker_id: &str,
) -> Uuid {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let found: Option<ActivityTaskRow> = diesel::sql_query(
            "SELECT id FROM harvest_task_queue \
             WHERE workflow_exec_id = $1 AND task_type = 'activity' \
               AND state = 'RUNNING' AND worker_id = $2",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .get_result(conn)
        .await
        .optional()
        .expect("poll activity task");
        if let Some(found) = found
            && std::sync::atomic::AtomicBool::load(
                &E2E_STARTED,
                std::sync::atomic::Ordering::SeqCst,
            )
        {
            break found.id;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the activity must start"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The real worker stops an attempt whose claim moved, and drops its result.
///
/// This covers the wiring in `process_activity_task`. The flusher, the
/// cancellation observer and the durable check get the claim. The finalize
/// after the handler returns is fenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_stops_an_attempt_whose_claim_moved_and_drops_its_result() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let run = Uuid::new_v4();
    // Short: the queue name becomes a `NOTIFY` channel name.
    let queue = format!("ce-{}", &run.simple().to_string()[..12]);
    let worker_id = format!("claim-epoch-e2e-worker-{run}");
    let thief = format!("claim-epoch-e2e-thief-{run}");
    live_worker(&mut conn, &thief).await;

    let exec_id = seed_e2e_workflow(&mut conn, &queue).await;

    let worker = e2e_worker(&worker_id, &queue, e2e_registry());
    let pool = build_pool(&url);
    let runner = std::sync::Arc::clone(&worker);
    let run_pool = pool.clone();
    let run_handle = tokio::spawn(async move { runner.run(&run_pool).await });

    let task_id = wait_for_e2e_start(&mut conn, exec_id, &worker_id).await;

    // Move the claim, as an orphan reclaim followed by a re-claim does.
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET worker_id = $2, attempt = attempt + 1, \
             last_heartbeat_at = NULL, heartbeat_details = '{\"owner\": \"thief\"}'::jsonb \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Text, _>(&thief)
    .execute(&mut conn)
    .await
    .expect("move the claim");
    let moved = row(&mut conn, task_id).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let stop = loop {
        let stopped = E2E_STOPPED.lock().expect("lock").clone();
        if let Some(stop) = stopped {
            break stop;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the stale attempt must see its lost claim"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_ne!(
        stop, "deadline",
        "the stale attempt must stop on the lost claim"
    );
    // Give the worker time to run its finalize for the stale result.
    tokio::time::sleep(Duration::from_secs(2)).await;
    worker.shutdown();
    let _ = run_handle.await;

    assert!(
        completed_outputs(&events(&mut conn, exec_id).await).is_empty(),
        "the stale result must not reach history"
    );
    assert_eq!(
        row(&mut conn, task_id).await,
        moved,
        "the stale attempt must leave the new claim untouched"
    );
}

#[tokio::test]
async fn stale_execution_failure_after_another_worker_claims_is_ambiguous() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let fx = seed_activity(&mut conn).await;
    let codecs = PayloadCodecs::default();
    let (a, _b) = a_then_b(&mut conn, &fx).await;
    let before = row(&mut conn, fx.task_id).await;

    let preloaded = preload_failure_history(&mut conn, &a).await;
    let err = fail_task_and_execution_with_history(
        &mut conn,
        &a,
        &fx.worker_a,
        "stale",
        preloaded,
        &codecs,
    )
    .await
    .expect_err("a lost claim is the blameless ambiguous outcome");

    assert_eq!(err.terminal_write_claim_ambiguous(), Some(fx.task_id));
    assert!(
        !events(&mut conn, fx.exec_id)
            .await
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a stale attempt must not fail the workflow"
    );
    assert_eq!(row(&mut conn, fx.task_id).await, before);
}

// ---------------------------------------------------------------------------
// Event-id conflict re-drive (issue #1787)
// ---------------------------------------------------------------------------

/// A claimed workflow task row, as the dispatching handler loaded it.
async fn claimed_workflow_task(
    conn: &mut AsyncPgConnection,
    queue: &str,
    exec_id: ExecutionId,
    worker_id: &str,
) -> TaskQueueItem {
    let task_id = queue::enqueue(
        conn,
        &EnqueueParams::new(queue, TaskType::Workflow, serde_json::json!({})),
    )
    .await
    .expect("enqueue");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = $2, attempt = 1, \
         workflow_exec_id = $3 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("model the claim");
    harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first::<TaskQueueItem>(conn)
        .await
        .expect("load the claim")
}

/// The claim holder's re-drive re-pends the row for an immediate re-claim.
#[tokio::test]
async fn event_id_conflict_redrive_by_the_claim_holder_repends_the_row() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let run = Uuid::new_v4().simple().to_string();
    let queue = format!("ce-rd-{}", &run[..12]);
    let worker_a = format!("redrive-a-{run}");
    live_worker(&mut conn, &worker_a).await;
    let exec_id = insert_execution(&mut conn, &queue).await;
    let task = claimed_workflow_task(&mut conn, &queue, exec_id, &worker_a).await;

    requeue_workflow_task_after_event_id_conflict(
        &mut conn,
        &task,
        &worker_a,
        Duration::ZERO,
        exec_id,
    )
    .await
    .expect("the holder re-drives its own row");

    let after = row(&mut conn, task.id).await;
    assert_eq!(after.state, "PENDING", "{after:?}");
    assert_eq!(after.worker_id, None, "{after:?}");
}

/// A stale handler's re-drive leaves a peer's claim alone. The peer took the
/// row after this handler loaded it. A park without the claim fence would
/// clear the peer's ownership and let a third dispatch run.
#[tokio::test]
async fn stale_event_id_conflict_redrive_leaves_a_peer_claim_alone() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let run = Uuid::new_v4().simple().to_string();
    let queue = format!("ce-sr-{}", &run[..12]);
    let worker_a = format!("stale-redrive-a-{run}");
    let worker_b = format!("stale-redrive-b-{run}");
    live_worker(&mut conn, &worker_a).await;
    live_worker(&mut conn, &worker_b).await;
    let exec_id = insert_execution(&mut conn, &queue).await;
    let stale = claimed_workflow_task(&mut conn, &queue, exec_id, &worker_a).await;
    diesel::sql_query("UPDATE harvest_task_queue SET worker_id = $2, attempt = 2 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(stale.id)
        .bind::<diesel::sql_types::Text, _>(&worker_b)
        .execute(&mut conn)
        .await
        .expect("model the peer's claim");

    requeue_workflow_task_after_event_id_conflict(
        &mut conn,
        &stale,
        &worker_a,
        Duration::ZERO,
        exec_id,
    )
    .await
    .expect("a lost claim is a no-op");

    let after = row(&mut conn, stale.id).await;
    assert_eq!(after.state, "RUNNING", "{after:?}");
    assert_eq!(
        after.worker_id.as_deref(),
        Some(worker_b.as_str()),
        "{after:?}"
    );
    assert_eq!(after.attempt, 2, "{after:?}");
}
