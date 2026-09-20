#![cfg(feature = "db")]
//! Regression tests for issue #1389 — quota retry backoff via
//! `requeue_for_retry` (and its two siblings) can be defeated by host/Postgres
//! clock skew.
//!
//! Each retry-requeue function used to compute `scheduled_at` on the
//! **host** clock, but `claim_task` checks `scheduled_at <= NOW()` on
//! **Postgres's** clock. A host-computed deadline can already be due by the
//! time `claim_task` checks it, when the host clock trails Postgres's.
//!
//! The fix computes `scheduled_at` as `clock_timestamp() + make_interval(secs
//! => ...)` inside the `UPDATE` statement itself. This stamps it on
//! Postgres's own clock, the same clock `claim_task` later checks it
//! against. `clock_timestamp()`, not `NOW()`: some callers run this inside a
//! transaction that already did other work, and `NOW()` is frozen at that
//! transaction's start.
//!
//! These tests compare the written `scheduled_at` against a query using the
//! database's own `NOW()`, never the host clock. Each checks that the held
//! duration lands close to `delay`, not merely at or past it. A regression
//! that erases the delay fails this. So does one that re-inflates it, such
//! as a reintroduced host-side padding constant.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted
//! with the full migration bundle.

use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use chrono::{DateTime, Duration, Utc};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── DB setup ─────────────────────────────────────────────────────────────────

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

// ── Helpers ──────────────────────────────────────────────────────────────────

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

async fn insert_execution(conn: &mut AsyncPgConnection) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'retry-skew', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

async fn enqueue_activity_task(conn: &mut AsyncPgConnection, queue: &str) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue activity")
}

async fn enqueue_workflow_task(conn: &mut AsyncPgConnection, queue: &str) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow")
}

/// Claim the one eligible task on `queue`, returning its id.
async fn claim_one(conn: &mut AsyncPgConnection, queue: &str) -> Uuid {
    queue::claim_task(conn, &[queue.to_string()], "w1", "", None, &[], &[])
        .await
        .expect("claim")
        .expect("a task must be claimable")
        .id
}

#[derive(diesel::QueryableByName)]
struct ScheduledAtRow {
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    scheduled_at: DateTime<Utc>,
}

async fn scheduled_at(conn: &mut AsyncPgConnection, id: Uuid) -> DateTime<Utc> {
    diesel::sql_query("SELECT scheduled_at FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<ScheduledAtRow>(conn)
        .await
        .expect("scheduled_at")
        .scheduled_at
}

#[derive(diesel::QueryableByName)]
struct NowRow {
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    now: DateTime<Utc>,
}

/// Postgres's own clock, queried directly — never the host clock.
async fn db_now(conn: &mut AsyncPgConnection) -> DateTime<Utc> {
    diesel::sql_query("SELECT NOW() AS now")
        .get_result::<NowRow>(conn)
        .await
        .expect("probe database clock")
        .now
}

/// Round-trip tolerance for the two DB queries this test issues around the
/// requeue call. One writes `scheduled_at`; the other reads the database's
/// own `NOW()` back. This absorbs real round-trip latency. It still fails a
/// multi-second regression, padding or erasure alike.
const TOLERANCE_MS: i64 = 750;

/// Assert `scheduled_at` lands close to `delay` past Postgres's own clock,
/// not the host clock that requested it.
///
/// The check is two-sided (issue #1389). Too little held time means the
/// clock-skew defect is back: the deadline is computed on the host clock
/// again, or the delay is dropped. Too much means an unwarranted host-side
/// padding crept back in, inflating every retry's backoff.
async fn assert_scheduled_at_matches_delay_on_db_clock(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    delay: Duration,
) {
    let now = db_now(conn).await;
    let deadline = scheduled_at(conn, task_id).await;
    let held = deadline - now;
    let drift = (held - delay).num_milliseconds().abs();
    assert!(
        drift <= TOLERANCE_MS,
        "expected scheduled_at ({deadline}) to land within {TOLERANCE_MS}ms of \
         delay {delay} past the database's own NOW() ({now}), but held {held} \
         -- drift {drift}ms"
    );
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn requeue_for_retry_computes_scheduled_at_on_the_db_clock() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("retry-skew");
    let task = enqueue_activity_task(&mut conn, &q).await;
    assert_eq!(claim_one(&mut conn, &q).await, task);

    let delay = Duration::seconds(2);
    queue::requeue_for_retry(&mut conn, task, delay, "boom")
        .await
        .expect("requeue for retry");

    assert_scheduled_at_matches_delay_on_db_clock(&mut conn, task, delay).await;
}

#[tokio::test]
async fn requeue_workflow_task_nd_blocked_computes_scheduled_at_on_the_db_clock() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("nd-blocked-skew");
    let task = enqueue_workflow_task(&mut conn, &q).await;
    assert_eq!(claim_one(&mut conn, &q).await, task);

    let delay = Duration::seconds(2);
    queue::requeue_workflow_task_nd_blocked(&mut conn, task, delay, "non-determinism")
        .await
        .expect("requeue nd-blocked");

    assert_scheduled_at_matches_delay_on_db_clock(&mut conn, task, delay).await;
}

/// Inside a transaction with elapsed prior work, `NOW()` is frozen at the
/// transaction's start (issue #1389). `clock_timestamp()` is not.
///
/// `block_workflow_for_non_determinism` calls
/// `requeue_workflow_task_nd_blocked` after a row lock and a prior write in
/// the same transaction. This reproduces that shape, with `pg_sleep`
/// standing in for that prior work. The backoff must still hold `delay`
/// from the requeue's real execution time, not from the transaction's
/// start.
#[tokio::test]
async fn requeue_workflow_task_nd_blocked_uses_the_live_clock_inside_a_transaction() {
    use diesel_async::AsyncConnection as _;

    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("nd-txn-skew");
    let task = enqueue_workflow_task(&mut conn, &q).await;
    assert_eq!(claim_one(&mut conn, &q).await, task);

    let delay = Duration::seconds(2);
    Box::pin(
        conn.transaction::<(), autumn_harvest::error::HarvestError, _>(async |conn| {
            // Stand-in for real prior work inside the same transaction (a row
            // lock, a preceding write) taking a full second before the
            // requeue itself runs.
            diesel::sql_query("SELECT pg_sleep(1)")
                .execute(conn)
                .await
                .expect("simulate prior transaction work");
            queue::requeue_workflow_task_nd_blocked(conn, task, delay, "non-determinism").await
        }),
    )
    .await
    .expect("requeue nd-blocked");

    assert_scheduled_at_matches_delay_on_db_clock(&mut conn, task, delay).await;
}

#[tokio::test]
async fn requeue_workflow_task_after_panic_computes_scheduled_at_on_the_db_clock() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("panic-retry-skew");
    let task = enqueue_workflow_task(&mut conn, &q).await;
    assert_eq!(claim_one(&mut conn, &q).await, task);

    let delay = Duration::seconds(2);
    queue::requeue_workflow_task_after_panic(&mut conn, task, delay, "handler panic")
        .await
        .expect("requeue after panic");

    assert_scheduled_at_matches_delay_on_db_clock(&mut conn, task, delay).await;
}

/// A zero delay must land at (not after) the database's own `NOW()`.
///
/// `make_interval(secs => 0)` is a no-op. This needs no special case in
/// production code. The earlier host-side padding fix needed one: it had to
/// carve zero out, or strand a queue-pause reset (issue #1389).
#[tokio::test]
async fn requeue_for_retry_zero_delay_computes_scheduled_at_on_the_db_clock() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("retry-skew-zero");
    let task = enqueue_activity_task(&mut conn, &q).await;
    assert_eq!(claim_one(&mut conn, &q).await, task);

    queue::requeue_for_retry(&mut conn, task, Duration::zero(), "boom")
        .await
        .expect("requeue for retry");

    assert_scheduled_at_matches_delay_on_db_clock(&mut conn, task, Duration::zero()).await;
}
