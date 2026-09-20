#![cfg(feature = "db")]
//! Regression tests for issue #1389 — quota retry backoff via
//! `requeue_for_retry` (and its two siblings) can be defeated by host/Postgres
//! clock skew.
//!
//! Each retry-requeue function computes `scheduled_at` on the **host**
//! clock, but `claim_task` checks `scheduled_at <= NOW()` on **Postgres's**
//! clock. These tests compare the written `scheduled_at` against a query
//! using the database's own `NOW()`, never the host clock. This catches a
//! skew regression locally, not only on a clock-skewed production host.
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

/// Mirrors the private `RETRY_SCHEDULE_SKEW_ALLOWANCE` in `queue.rs` (issue
/// #1389). It is not importable across the crate boundary, so it is pinned
/// here as a plain duration. This test then catches a regression in the
/// padding itself, not just in the literal it happens to share with
/// production code.
const EXPECTED_SKEW_ALLOWANCE_SECS: i64 = 5;

/// Round-trip tolerance for the two DB queries this test issues around the
/// requeue call. Well under [`EXPECTED_SKEW_ALLOWANCE_SECS`], so a missing
/// skew allowance still fails this check by several seconds.
const ROUND_TRIP_TOLERANCE_SECS: i64 = 1;

/// Assert `scheduled_at` still holds `delay` in the future, measured against
/// Postgres's own clock rather than the host clock that computed it.
///
/// Without the skew-allowance padding this issue fixes, `scheduled_at` sits
/// at roughly `delay` past the DB's `NOW()`. That is short of this floor by
/// close to the missing allowance. A regression here fails loudly, not by a
/// flaky few milliseconds.
async fn assert_retry_deadline_holds_against_db_clock(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    delay: Duration,
) {
    let now = db_now(conn).await;
    let deadline = scheduled_at(conn, task_id).await;
    let held = deadline - now;
    let floor = delay + Duration::seconds(EXPECTED_SKEW_ALLOWANCE_SECS)
        - Duration::seconds(ROUND_TRIP_TOLERANCE_SECS);
    assert!(
        held >= floor,
        "expected scheduled_at ({deadline}) to hold at least {floor} past the \
         database's own NOW() ({now}) — delay {delay} plus the skew allowance, \
         minus round-trip tolerance — but held only {held}"
    );
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn requeue_for_retry_holds_the_full_delay_against_the_db_clock() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("retry-skew");
    let task = enqueue_activity_task(&mut conn, &q).await;
    assert_eq!(claim_one(&mut conn, &q).await, task);

    let delay = Duration::seconds(2);
    queue::requeue_for_retry(&mut conn, task, delay, "boom")
        .await
        .expect("requeue for retry");

    assert_retry_deadline_holds_against_db_clock(&mut conn, task, delay).await;
}

#[tokio::test]
async fn requeue_workflow_task_nd_blocked_holds_the_full_delay_against_the_db_clock() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("nd-blocked-skew");
    let task = enqueue_workflow_task(&mut conn, &q).await;
    assert_eq!(claim_one(&mut conn, &q).await, task);

    let delay = Duration::seconds(2);
    queue::requeue_workflow_task_nd_blocked(&mut conn, task, delay, "non-determinism")
        .await
        .expect("requeue nd-blocked");

    assert_retry_deadline_holds_against_db_clock(&mut conn, task, delay).await;
}

#[tokio::test]
async fn requeue_workflow_task_after_panic_holds_the_full_delay_against_the_db_clock() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("panic-retry-skew");
    let task = enqueue_workflow_task(&mut conn, &q).await;
    assert_eq!(claim_one(&mut conn, &q).await, task);

    let delay = Duration::seconds(2);
    queue::requeue_workflow_task_after_panic(&mut conn, task, delay, "handler panic")
        .await
        .expect("requeue after panic");

    assert_retry_deadline_holds_against_db_clock(&mut conn, task, delay).await;
}
