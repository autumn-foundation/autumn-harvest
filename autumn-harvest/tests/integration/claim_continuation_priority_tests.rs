#![cfg(feature = "db")]
#![allow(clippy::doc_markdown, clippy::too_many_lines)]
//! Continuations outrank new starts at claim (issue #1824).
//!
//! A continuation is any task of a run that already started: a woken
//! workflow task, or an activity task. A new start is the first workflow
//! task of a run. That row has never been claimed, so its `attempt` is 0.
//!
//! Within one priority level, a new start sorts as if it were due
//! `NEW_START_HANDICAP_SECS` later. A new start that waits longer than that
//! competes FIFO again, so a stream of continuations cannot starve it.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres.
//! Otherwise a fresh testcontainers Postgres boots with the full bundle.

use autumn_harvest::queue::{
    self, BatchedClaimConfig, EnqueueParams, NEW_START_HANDICAP_SECS, TaskType,
};
use autumn_harvest::types::ExecutionId;
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── Setup ─────────────────────────────────────────────────────────────────────

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as diesel_async::AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

async fn setup_db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (connect(&url).await, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrations");
    (conn, Some(container))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", &Uuid::new_v4().simple().to_string()[..8])
}

async fn insert_execution(conn: &mut AsyncPgConnection) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'continuation-band', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Enqueue the first workflow task of a new run.
async fn new_start(conn: &mut AsyncPgConnection, queue: &str, priority: i32) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.priority = priority;
    queue::enqueue(conn, &params).await.expect("enqueue start")
}

/// Enqueue an activity task of an existing run.
async fn activity_continuation(conn: &mut AsyncPgConnection, queue: &str) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    queue::enqueue(conn, &params).await.expect("enqueue activity")
}

/// Start a run, claim and park its workflow task, and return the row id and
/// execution id. A later [`queue::wake_workflow_task`] re-pends the same row
/// as a continuation.
async fn parked_run(conn: &mut AsyncPgConnection, queue: &str, worker: &str) -> (Uuid, Uuid) {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    let task_id = queue::enqueue(conn, &params).await.expect("enqueue run");
    let claimed = claim_one(conn, queue, worker).await;
    assert_eq!(claimed, Some(task_id), "setup claims the run's first task");
    assert!(
        queue::park_workflow_task(conn, task_id, None)
            .await
            .expect("park"),
        "setup parks the run"
    );
    (task_id, exec_id)
}

/// Move a row's due time `secs` seconds into the past.
async fn age(conn: &mut AsyncPgConnection, task_id: Uuid, secs: i64) {
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET scheduled_at = NOW() - make_interval(secs => $2) WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Double, _>(secs as f64)
    .execute(conn)
    .await
    .expect("age row");
}

async fn claim_one(conn: &mut AsyncPgConnection, queue: &str, worker: &str) -> Option<Uuid> {
    queue::claim_task(conn, &[queue.to_owned()], worker, "", None, &[], &[])
        .await
        .expect("claim")
        .map(|t| t.id)
}

async fn claim_one_batched(
    conn: &mut AsyncPgConnection,
    queue: &str,
    worker: &str,
) -> Option<Uuid> {
    queue::claim_task_batched(
        conn,
        &[queue.to_owned()],
        worker,
        "",
        None,
        &[],
        &[],
        BatchedClaimConfig {
            batch_size: 2,
            max_batches: 20,
        },
    )
    .await
    .expect("batched claim")
    .map(|t| t.id)
}

/// The single-slot backlog both claim paths share.
///
/// Holds three parked runs, four new starts and two activity continuations.
/// The new starts are the oldest rows, so plain FIFO would claim them first.
struct Backlog {
    queue: String,
    continuations: Vec<Uuid>,
    starts: Vec<Uuid>,
}

async fn backlog(conn: &mut AsyncPgConnection, worker: &str) -> Backlog {
    let queue = unique("band");
    let mut parked = Vec::new();
    for _ in 0..3 {
        parked.push(parked_run(conn, &queue, worker).await);
    }
    let mut starts = Vec::new();
    for _ in 0..4 {
        let id = new_start(conn, &queue, 0).await;
        age(conn, id, 2).await;
        starts.push(id);
    }
    let mut continuations = Vec::new();
    for _ in 0..2 {
        continuations.push(activity_continuation(conn, &queue).await);
    }
    for (task_id, exec_id) in parked {
        queue::wake_workflow_task(conn, ExecutionId::from_uuid(exec_id))
            .await
            .expect("wake");
        continuations.push(task_id);
    }
    Backlog {
        queue,
        continuations,
        starts,
    }
}

fn assert_continuations_first(order: &[Uuid], backlog: &Backlog) {
    let m = backlog.continuations.len();
    assert_eq!(order.len(), m + backlog.starts.len(), "every row is claimed");
    for id in &order[..m] {
        assert!(
            backlog.continuations.contains(id),
            "the first {m} claims are continuations; got order {order:?}"
        );
    }
    for id in &order[m..] {
        assert!(
            backlog.starts.contains(id),
            "new starts follow the continuations; got order {order:?}"
        );
    }
}

// ── AC1: continuations first, single slot ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn single_slot_claims_continuations_before_new_starts() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let backlog = backlog(&mut conn, &worker).await;

    let mut order = Vec::new();
    while let Some(id) = claim_one(&mut conn, &backlog.queue, &worker).await {
        order.push(id);
    }
    assert_continuations_first(&order, &backlog);
}

#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_also_claims_continuations_before_new_starts() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let backlog = backlog(&mut conn, &worker).await;

    let mut order = Vec::new();
    while let Some(id) = claim_one_batched(&mut conn, &backlog.queue, &worker).await {
        order.push(id);
    }
    assert_continuations_first(&order, &backlog);
}

// ── AC1: new starts still progress ────────────────────────────────────────────

/// Each round adds two fresh continuations and claims one, so continuation
/// pressure grows without bound. Every new start older than the handicap is
/// still claimed, one per round, in FIFO order.
#[tokio::test(flavor = "multi_thread")]
async fn new_starts_are_not_starved_by_a_stream_of_continuations() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let queue = unique("starve");
    let aged = i64::from(NEW_START_HANDICAP_SECS) + 5;

    let mut starts = Vec::new();
    for i in 0..3 {
        let id = new_start(&mut conn, &queue, 0).await;
        age(&mut conn, id, aged + 3 - i).await;
        starts.push(id);
    }

    let mut claimed_starts = Vec::new();
    for _ in 0..6 {
        activity_continuation(&mut conn, &queue).await;
        activity_continuation(&mut conn, &queue).await;
        let id = claim_one(&mut conn, &queue, &worker)
            .await
            .expect("a row is due every round");
        if starts.contains(&id) {
            claimed_starts.push(id);
        }
    }
    assert_eq!(
        claimed_starts, starts,
        "aged new starts progress in FIFO order under continuation load"
    );
}

/// The handicap is real but bounded. A young new start yields to a fresh
/// continuation. Once it is older than the handicap, it no longer yields.
#[tokio::test(flavor = "multi_thread")]
async fn the_handicap_expires_after_new_start_handicap_secs() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let handicap = i64::from(NEW_START_HANDICAP_SECS);

    let queue = unique("young");
    let young = new_start(&mut conn, &queue, 0).await;
    age(&mut conn, young, handicap - 5).await;
    let fresh = activity_continuation(&mut conn, &queue).await;
    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(fresh));
    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(young));

    let queue = unique("old");
    let old = new_start(&mut conn, &queue, 0).await;
    age(&mut conn, old, handicap + 5).await;
    let fresh = activity_continuation(&mut conn, &queue).await;
    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(old));
    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(fresh));
}

// ── Explicit priority still wins ──────────────────────────────────────────────

/// The band is a tie-break inside one priority level. An operator who sets
/// a higher priority on a start keeps that order.
#[tokio::test(flavor = "multi_thread")]
async fn explicit_priority_outranks_the_continuation_band() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let queue = unique("prio");

    let continuation = activity_continuation(&mut conn, &queue).await;
    age(&mut conn, continuation, 2).await;
    let urgent_start = new_start(&mut conn, &queue, 1).await;

    assert_eq!(
        claim_one(&mut conn, &queue, &worker).await,
        Some(urgent_start)
    );
    assert_eq!(
        claim_one(&mut conn, &queue, &worker).await,
        Some(continuation)
    );
}
