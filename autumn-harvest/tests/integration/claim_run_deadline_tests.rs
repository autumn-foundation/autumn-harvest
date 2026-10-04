#![cfg(feature = "db")]
#![allow(clippy::doc_markdown, clippy::too_many_lines)]
//! A task whose run deadline has passed is not executed (issue #1824).
//!
//! The claim fails such a task with a `deadline_exceeded` error and hands
//! out the next eligible task instead. The timeout scanner still times out
//! the run itself, and it keeps the task's error.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres.
//! Otherwise a fresh testcontainers Postgres boots with the full bundle.

use autumn_harvest::queue::{
    self, BatchedClaimConfig, DEADLINE_EXCEEDED_ERROR, EnqueueParams, TaskType,
};
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

/// Which deadline column a fixture run sets, and its offset from now.
#[derive(Clone, Copy)]
enum Deadline {
    None,
    Run(i64),
    Chain(i64),
}

async fn insert_execution(conn: &mut AsyncPgConnection, state: &str, deadline: Deadline) -> Uuid {
    let id = Uuid::new_v4();
    let (run, chain) = match deadline {
        Deadline::None => (None, None),
        Deadline::Run(secs) => (Some(secs), None),
        Deadline::Chain(secs) => (None, Some(secs)),
    };
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, input, state, paused_at, \
              deadline_at, chain_deadline_at) \
         VALUES ($1, 'run-deadline', $2, 0, '{}'::jsonb, $3, \
                 CASE WHEN $3 = 'PAUSED' THEN NOW() END, \
                 NOW() + make_interval(secs => $4), NOW() + make_interval(secs => $5))",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Double>, _>(run.map(|s| s as f64))
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Double>, _>(chain.map(|s| s as f64))
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

async fn enqueue(
    conn: &mut AsyncPgConnection,
    queue: &str,
    kind: TaskType,
    exec_id: Uuid,
) -> Uuid {
    let mut params = EnqueueParams::new(queue, kind, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    if kind == TaskType::Activity {
        params.activity_name = Some("noop".to_string());
        params.activity_id = Some(Uuid::new_v4());
    }
    queue::enqueue(conn, &params).await.expect("enqueue")
}

#[derive(diesel::QueryableByName, Debug)]
struct Row {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    error: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    has_completed_at: bool,
}

async fn row(conn: &mut AsyncPgConnection, id: Uuid) -> Row {
    diesel::sql_query(
        "SELECT state, error, completed_at IS NOT NULL AS has_completed_at \
         FROM harvest_task_queue WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result(conn)
    .await
    .expect("row")
}

fn assert_deadline_exceeded(row: &Row) {
    assert_eq!(row.state, "FAILED", "the task is terminal: {row:?}");
    assert!(
        row.error
            .as_deref()
            .is_some_and(|e| e.starts_with(DEADLINE_EXCEEDED_ERROR)),
        "the task records a deadline-exceeded outcome: {row:?}"
    );
    assert!(row.has_completed_at, "the outcome has a time: {row:?}");
}

async fn claim_one(conn: &mut AsyncPgConnection, queue: &str, worker: &str) -> Option<Uuid> {
    queue::claim_task(conn, &[queue.to_owned()], worker, "", None, &[], &[])
        .await
        .expect("claim")
        .map(|t| t.id)
}

// ── AC2: past the run deadline, the task is not executed ──────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_task_past_its_run_deadline_is_not_claimed() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-wf");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(-1)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Workflow, exec).await;

    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, None);
    assert_deadline_exceeded(&row(&mut conn, task).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn activity_task_past_its_run_deadline_is_not_claimed() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-act");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(-1)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Activity, exec).await;

    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, None);
    assert_deadline_exceeded(&row(&mut conn, task).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn chain_deadline_also_stops_the_claim() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-chain");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Chain(-1)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Activity, exec).await;

    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, None);
    assert_deadline_exceeded(&row(&mut conn, task).await);
}

/// One claim call skips the expired row and returns the live row behind
/// it, so an expired backlog does not idle the slot for a poll interval.
#[tokio::test(flavor = "multi_thread")]
async fn an_expired_task_does_not_block_the_live_task_behind_it() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-skip");
    let mut expired = Vec::new();
    for _ in 0..3 {
        let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(-1)).await;
        expired.push(enqueue(&mut conn, &queue, TaskType::Activity, exec).await);
    }
    let live_exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(3600)).await;
    let live = enqueue(&mut conn, &queue, TaskType::Activity, live_exec).await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET scheduled_at = NOW() - INTERVAL '1 minute' \
         WHERE id = ANY($1)",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&expired)
    .execute(&mut conn)
    .await
    .expect("age expired rows");

    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, Some(live));
    for id in expired {
        assert_deadline_exceeded(&row(&mut conn, id).await);
    }
}

// ── The check does not overreach ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tasks_with_a_future_or_no_deadline_are_claimed() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    for deadline in [Deadline::None, Deadline::Run(3600), Deadline::Chain(3600)] {
        let queue = unique("dl-live");
        let exec = insert_execution(&mut conn, "RUNNING", deadline).await;
        let task = enqueue(&mut conn, &queue, TaskType::Activity, exec).await;
        assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(task));
        assert_eq!(row(&mut conn, task).await.state, "RUNNING");
    }
}

/// Resume moves a paused run's deadline forward, and the scanner skips
/// paused runs. So the claim must not fail a paused run's activity either.
#[tokio::test(flavor = "multi_thread")]
async fn a_paused_run_past_its_deadline_keeps_its_activity() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-paused");
    let exec = insert_execution(&mut conn, "PAUSED", Deadline::Run(-1)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Activity, exec).await;

    assert_eq!(
        claim_one(&mut conn, &queue, &unique("w")).await,
        Some(task)
    );
    assert_eq!(row(&mut conn, task).await.state, "RUNNING");
}

// ── Every claim path applies the check ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn by_id_claim_does_not_claim_an_expired_task() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-byid");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(-1)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Workflow, exec).await;

    let claimed = queue::claim_task_by_id_on_shard(
        &mut conn,
        task,
        &[queue.clone()],
        &unique("w"),
        "",
        None,
        &[],
        &[],
        None,
    )
    .await
    .expect("by-id claim");
    assert!(claimed.is_none(), "an expired task is not handed out");
    assert_deadline_exceeded(&row(&mut conn, task).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_skips_an_expired_task() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-batch");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(-1)).await;
    let expired = enqueue(&mut conn, &queue, TaskType::Activity, exec).await;
    let live_exec = insert_execution(&mut conn, "RUNNING", Deadline::None).await;
    let live = enqueue(&mut conn, &queue, TaskType::Activity, live_exec).await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET scheduled_at = NOW() - INTERVAL '1 minute' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(expired)
    .execute(&mut conn)
    .await
    .expect("age expired row");

    let claimed = queue::claim_task_batched(
        &mut conn,
        &[queue.clone()],
        &unique("w"),
        "",
        None,
        &[],
        &[],
        BatchedClaimConfig::default(),
    )
    .await
    .expect("batched claim")
    .map(|t| t.id);
    assert_eq!(claimed, Some(live));
    assert_deadline_exceeded(&row(&mut conn, expired).await);
}

// ── The timeout scanner keeps the outcome ─────────────────────────────────────

/// The scanner times out the run later. It only rewrites open rows, so the
/// task keeps its deadline-exceeded error.
#[tokio::test(flavor = "multi_thread")]
async fn the_timeout_scanner_times_out_the_run_and_keeps_the_task_outcome() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-scan");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(-1)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Workflow, exec).await;
    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, None);

    autumn_harvest::timeout::enforce_workflow_execution_timeouts(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("scanner");

    #[derive(diesel::QueryableByName)]
    struct State {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    let run: State = diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec)
        .get_result(&mut conn)
        .await
        .expect("run state");
    assert_eq!(run.state, "TIMED_OUT");
    assert_deadline_exceeded(&row(&mut conn, task).await);
}
