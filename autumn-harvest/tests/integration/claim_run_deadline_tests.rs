#![cfg(feature = "db")]
#![allow(
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::too_many_lines
)]
//! A task whose run deadline has passed is not executed (issue #1824).
//!
//! The claim skips a task of a `RUNNING` run that is past `deadline_at` or
//! `chain_deadline_at`. The task stays `PENDING` and no worker gets it. The
//! timeout scanner then times out the run. It fails the task with an error
//! that starts with `deadline_exceeded`.
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

/// A past deadline, in seconds from now.
///
/// The claim compares against the database clock. The scanner compares
/// against the host clock. A full minute keeps a small skew from hiding
/// the deadline from either one.
const EXPIRED: i32 = -60;

/// Which deadline column a fixture run sets, and its offset from now.
#[derive(Clone, Copy)]
enum Deadline {
    None,
    Run(i32),
    Chain(i32),
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
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(run)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(chain)
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Enqueue a task of `exec_id`. An aged row sorts ahead of fresh rows.
async fn enqueue(
    conn: &mut AsyncPgConnection,
    queue: &str,
    kind: TaskType,
    exec_id: Uuid,
    aged: bool,
) -> Uuid {
    let mut params = EnqueueParams::new(queue, kind, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    if kind == TaskType::Activity {
        params.activity_name = Some("noop".to_string());
        params.activity_id = Some(Uuid::new_v4());
    }
    let id = queue::enqueue(conn, &params).await.expect("enqueue");
    if aged {
        age(conn, id).await;
    }
    id
}

async fn age(conn: &mut AsyncPgConnection, id: Uuid) {
    diesel::sql_query(
        "UPDATE harvest_task_queue SET scheduled_at = scheduled_at - INTERVAL '1 minute' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("age row");
}

#[derive(diesel::QueryableByName, Debug)]
struct Row {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    attempt: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    error: Option<String>,
}

async fn row(conn: &mut AsyncPgConnection, id: Uuid) -> Row {
    diesel::sql_query("SELECT state, attempt, error FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result(conn)
        .await
        .expect("row")
}

/// The claim did not hand the task out: it is still `PENDING`, unclaimed.
async fn assert_skipped(conn: &mut AsyncPgConnection, id: Uuid) {
    let row = row(conn, id).await;
    assert_eq!(row.state, "PENDING", "the task is not executed: {row:?}");
    assert_eq!(row.attempt, 0, "no claim consumed an attempt: {row:?}");
}

/// Run the timeout scanner, then check the task and the run outcome.
async fn assert_recorded_as_deadline_exceeded(conn: &mut AsyncPgConnection, task: Uuid) {
    autumn_harvest::timeout::enforce_workflow_execution_timeouts(
        conn,
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("scanner");

    let row = row(conn, task).await;
    assert_eq!(row.state, "FAILED", "the task is terminal: {row:?}");
    assert!(
        row.error
            .as_deref()
            .is_some_and(|e| e.starts_with(DEADLINE_EXCEEDED_ERROR)),
        "the task records a deadline-exceeded outcome: {row:?}"
    );

    #[derive(diesel::QueryableByName)]
    struct Run {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    let run: Run = diesel::sql_query(
        "SELECT e.state FROM harvest_workflow_executions e \
         JOIN harvest_task_queue t ON t.workflow_exec_id = e.id WHERE t.id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task)
    .get_result(conn)
    .await
    .expect("run state");
    assert_eq!(run.state, "TIMED_OUT", "the scanner times out the run");
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
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(EXPIRED)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Workflow, exec, false).await;

    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, None);
    assert_skipped(&mut conn, task).await;
    assert_recorded_as_deadline_exceeded(&mut conn, task).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn activity_task_past_its_run_deadline_is_not_claimed() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-act");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(EXPIRED)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Activity, exec, false).await;

    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, None);
    assert_skipped(&mut conn, task).await;
    assert_recorded_as_deadline_exceeded(&mut conn, task).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn chain_deadline_also_stops_the_claim() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-chain");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Chain(EXPIRED)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Activity, exec, false).await;

    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, None);
    assert_skipped(&mut conn, task).await;
    assert_recorded_as_deadline_exceeded(&mut conn, task).await;
}

/// The worker's own claim passes a task kind (issue #1787). That variant
/// skips an expired run too.
#[tokio::test(flavor = "multi_thread")]
async fn the_kind_filtered_claim_skips_an_expired_task() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-kind");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(EXPIRED)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Workflow, exec, false).await;

    let claimed = queue::claim_task_of_kind_on_shard(
        &mut conn,
        std::slice::from_ref(&queue),
        &unique("w"),
        "",
        None,
        &[],
        &[],
        None,
        Some(TaskType::Workflow),
    )
    .await
    .expect("kind claim");
    assert!(claimed.is_none(), "an expired task is not handed out");
    assert_skipped(&mut conn, task).await;
}

/// One claim call passes over the expired rows and returns the live row
/// behind them. So an expired backlog does not idle the slot.
#[tokio::test(flavor = "multi_thread")]
async fn an_expired_task_does_not_block_the_live_task_behind_it() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-skip");
    let mut expired = Vec::new();
    for _ in 0..12 {
        let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(EXPIRED)).await;
        expired.push(enqueue(&mut conn, &queue, TaskType::Activity, exec, true).await);
    }
    let live_exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(3600)).await;
    let live = enqueue(&mut conn, &queue, TaskType::Activity, live_exec, false).await;

    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, Some(live));
    for id in expired {
        assert_skipped(&mut conn, id).await;
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
        let task = enqueue(&mut conn, &queue, TaskType::Activity, exec, false).await;
        assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(task));
        assert_eq!(row(&mut conn, task).await.state, "RUNNING");
    }
}

/// Resume moves a paused run's deadline forward, and the scanner skips
/// paused runs. So the claim does not skip a paused run's activity either.
#[tokio::test(flavor = "multi_thread")]
async fn a_paused_run_past_its_deadline_keeps_its_activity() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-paused");
    let exec = insert_execution(&mut conn, "PAUSED", Deadline::Run(EXPIRED)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Activity, exec, false).await;

    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, Some(task));
    assert_eq!(row(&mut conn, task).await.state, "RUNNING");
}

// ── Every claim path applies the check ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn by_id_claim_does_not_claim_an_expired_task() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-byid");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(EXPIRED)).await;
    let task = enqueue(&mut conn, &queue, TaskType::Workflow, exec, false).await;

    let claimed = queue::claim_task_by_id_on_shard(
        &mut conn,
        task,
        std::slice::from_ref(&queue),
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
    assert_skipped(&mut conn, task).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_skips_an_expired_task() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-batch");
    let exec = insert_execution(&mut conn, "RUNNING", Deadline::Run(EXPIRED)).await;
    let expired = enqueue(&mut conn, &queue, TaskType::Activity, exec, true).await;
    let live_exec = insert_execution(&mut conn, "RUNNING", Deadline::None).await;
    let live = enqueue(&mut conn, &queue, TaskType::Activity, live_exec, false).await;

    assert_eq!(claim_one_batched(&mut conn, &queue).await, Some(live));
    assert_skipped(&mut conn, expired).await;
}

async fn claim_one_batched(conn: &mut AsyncPgConnection, queue: &str) -> Option<Uuid> {
    queue::claim_task_batched(
        conn,
        &[queue.to_owned()],
        &unique("w"),
        "",
        None,
        &[],
        &[],
        BatchedClaimConfig::default(),
    )
    .await
    .expect("batched claim")
    .map(|t| t.id)
}

// ── A skipped task spends no rate-limit token ─────────────────────────────────

/// Enqueue an aged or fresh activity task that spends a token from `bucket`.
async fn rate_limited(
    conn: &mut AsyncPgConnection,
    queue: &str,
    bucket: &str,
    deadline: Deadline,
    aged: bool,
) -> Uuid {
    let exec = insert_execution(conn, "RUNNING", deadline).await;
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(bucket.to_owned());
    let id = queue::enqueue(conn, &params).await.expect("enqueue");
    if aged {
        age(conn, id).await;
    }
    id
}

/// A one-token bucket. An expired task ahead of a live task must not spend
/// the token, or the live task cannot run until the bucket refills.
async fn one_token_backlog(conn: &mut AsyncPgConnection, queue: &str) -> (Uuid, Uuid) {
    let bucket = unique("dl-bucket");
    queue::ensure_rate_limit_bucket(conn, &bucket, 0.0, 1.0)
        .await
        .expect("bucket");
    let expired = rate_limited(conn, queue, &bucket, Deadline::Run(EXPIRED), true).await;
    let live = rate_limited(conn, queue, &bucket, Deadline::None, false).await;
    (expired, live)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_skipped_task_spends_no_rate_limit_token() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-rl");
    let (expired, live) = one_token_backlog(&mut conn, &queue).await;

    assert_eq!(claim_one(&mut conn, &queue, &unique("w")).await, Some(live));
    assert_skipped(&mut conn, expired).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batched_skip_spends_no_rate_limit_token() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-rl-b");
    let (expired, live) = one_token_backlog(&mut conn, &queue).await;

    assert_eq!(claim_one_batched(&mut conn, &queue).await, Some(live));
    assert_skipped(&mut conn, expired).await;
}

/// The batched scan and each candidate attempt are separate statements in
/// one transaction. A run can expire between them. The attempt re-checks
/// the run on a fresh clock, so it neither claims the task nor spends its
/// token.
#[tokio::test(flavor = "multi_thread")]
async fn the_batched_attempt_rechecks_the_run_deadline() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("dl-attempt");
    let bucket = unique("dl-bucket-a");
    queue::ensure_rate_limit_bucket(&mut conn, &bucket, 0.0, 1.0)
        .await
        .expect("bucket");
    let task = rate_limited(&mut conn, &queue, &bucket, Deadline::Run(EXPIRED), false).await;

    let claimed: Vec<autumn_harvest::models::TaskQueueItem> =
        diesel::sql_query(queue::claim_batched_candidate_attempt_query())
            .bind::<diesel::sql_types::Text, _>(unique("w"))
            .bind::<diesel::sql_types::Uuid, _>(task)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(None::<String>)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(None::<i32>)
            .bind::<diesel::sql_types::Text, _>("activity")
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(bucket.clone()))
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some("noop"))
            .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(Vec::<String>::new())
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                None::<chrono::DateTime<chrono::Utc>>,
            )
            .bind::<diesel::sql_types::Text, _>("")
            .load(&mut conn)
            .await
            .expect("attempt");
    assert!(
        claimed.is_empty(),
        "the attempt does not claim an expired run's task"
    );
    assert_skipped(&mut conn, task).await;

    #[derive(diesel::QueryableByName)]
    struct Tokens {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    let bucket_row: Tokens =
        diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(&bucket)
            .get_result(&mut conn)
            .await
            .expect("bucket row");
    assert!(
        (bucket_row.tokens - 1.0).abs() < f64::EPSILON,
        "the attempt spends no token"
    );
}
