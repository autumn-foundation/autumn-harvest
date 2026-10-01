//! Claim-epoch fence on activity ownership writes (issue #1789).
//!
//! Every test runs one scenario. Worker A claims an activity task. A has no
//! liveness row, so the orphan reclaimer requeues the row. Worker B then
//! claims the same row as the next attempt. A's late writes must not change
//! B's attempt.
//!
//! The requeue runs the reclaimer's own statement against this one row, so
//! the tests are safe on a shared `HARVEST_TEST_DATABASE_URL` database.

use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::poison_pill::requeue_orphan_stmt;
use autumn_harvest::queue::{self, ClaimWrite, EnqueueParams, TaskClaim, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use autumn_harvest::worker::{
    DbPool, append_activity_started_for_test, finalize_activity_completion,
    finalize_activity_failure, observe_task_cancellation,
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
            input: serde_json::json!({}),
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

/// One scheduled activity on its own queue, so a shared database is safe.
struct Fixture {
    queue: String,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    task_id: Uuid,
}

async fn seed_activity(conn: &mut AsyncPgConnection) -> Fixture {
    let queue = format!("claim-epoch-{}", Uuid::new_v4());
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
        exec_id,
        activity_id,
        task_id,
    }
}

async fn claim(conn: &mut AsyncPgConnection, fx: &Fixture, worker_id: &str) -> TaskQueueItem {
    let task = queue::claim_task(conn, &[fx.queue.clone()], worker_id, "", None, &[], &[])
        .await
        .expect("claim")
        .expect("task is claimable");
    assert_eq!(task.id, fx.task_id);
    task
}

/// Run the orphan reclaimer's requeue statement on this row only.
///
/// The `SELECT ... FOR UPDATE` comes first, as `requeue_orphan` requires.
async fn requeue_orphan(conn: &mut AsyncPgConnection, task: &TaskQueueItem) {
    let task_id = task.id;
    let worker_id = task.worker_id.clone().expect("claimed task has a worker");
    let strikes = task.crash_strikes;
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
    assert_eq!(requeued, 1, "the reclaimer must requeue A's orphaned claim");
}

/// A claims and starts. The reclaimer requeues. B claims and starts.
async fn a_then_b(conn: &mut AsyncPgConnection, fx: &Fixture) -> (TaskQueueItem, TaskQueueItem) {
    let codecs = PayloadCodecs::default();
    let a = claim(conn, fx, "worker-a").await;
    let started = append_activity_started_for_test(conn, &a, fx.exec_id, ACTIVITY, "worker-a", &codecs)
        .await
        .expect("A starts");
    assert_eq!(started, Some(fx.activity_id));
    requeue_orphan(conn, &a).await;
    let b = claim(conn, fx, "worker-b").await;
    assert!(b.attempt > a.attempt, "a reclaim must give B a later attempt");
    let started = append_activity_started_for_test(conn, &b, fx.exec_id, ACTIVITY, "worker-b", &codecs)
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

    finalize_activity_failure(&mut conn, &a, fx.exec_id, fx.activity_id, "A failed", &codecs)
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
    let a = claim(&mut conn, &fx, "worker-a").await;
    requeue_orphan(&mut conn, &a).await;
    let _b = claim(&mut conn, &fx, "worker-b").await;

    let started =
        append_activity_started_for_test(&mut conn, &a, fx.exec_id, ACTIVITY, "worker-a", &codecs)
            .await
            .expect("a stale start is a no-op, not an error");

    assert_eq!(started, None, "A must not start under B's claim");
    assert_eq!(
        count_started_by(&events(&mut conn, fx.exec_id).await, "worker-a"),
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
    assert_eq!(after.heartbeat_details, Some(serde_json::json!({"owner": "B"})));
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
        assert!(tokio::time::Instant::now() < deadline, "B's heartbeat must flush");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!cancel.is_cancelled(), "a held claim must not cancel the token");
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
