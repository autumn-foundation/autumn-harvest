#![cfg(feature = "db")]
//! Regression tests for issue #1807: host-clock writes into columns that the
//! timeout scans compare with the database `NOW()`.
//!
//! A host that runs behind the database stamps a heartbeat in the past. The
//! heartbeat scan then times out a healthy activity. Each write now takes its
//! value from `clock_timestamp()` in SQL, so the host clock does not matter.
//!
//! The suite checks two things. A stored stamp must lie between two database
//! clock readings, one before the write and one after it. The heartbeat scan must not select a task that has
//! just heartbeated. To reproduce a real skew, run the built test binary under
//! `faketime -f -60s`.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise a testcontainers Postgres boots with the full migration bundle.

use autumn_harvest::poison_pill::reclaim_orphaned_tasks;
use autumn_harvest::queue::{self, EnqueueParams, TaskClaim, TaskType};
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::timeout::heartbeat_timeout_query;
use chrono::{DateTime, Duration, Utc};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// Slack for a sub-millisecond clock step. A 60 s skew is far outside it.
const SLACK_MS: i64 = 50;

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
         VALUES ($1, 'host-clock', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

async fn enqueue_activity(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    heartbeat_timeout: Option<Duration>,
) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue_name, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.heartbeat_timeout = heartbeat_timeout;
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue activity")
}

#[derive(diesel::QueryableByName)]
struct TimestampRow {
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    at: DateTime<Utc>,
}

/// Postgres's own clock, queried directly. Never the host clock.
async fn db_now(conn: &mut AsyncPgConnection) -> DateTime<Utc> {
    diesel::sql_query("SELECT clock_timestamp() AS at")
        .get_result::<TimestampRow>(conn)
        .await
        .expect("probe database clock")
        .at
}

async fn read_column(conn: &mut AsyncPgConnection, column: &str, id: Uuid) -> DateTime<Utc> {
    diesel::sql_query(format!(
        "SELECT {column} AS at FROM harvest_task_queue WHERE id = $1"
    ))
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result::<TimestampRow>(conn)
    .await
    .expect("read column")
    .at
}

/// Assert that `stamp` lies between `before` and a fresh database reading.
///
/// `before` is read before the write. The second reading comes after it. A
/// stamp from the database clock always lies between them, however long the
/// test process pauses. A stamp from a skewed host clock does not.
async fn assert_on_db_clock(
    conn: &mut AsyncPgConnection,
    what: &str,
    before: DateTime<Utc>,
    stamp: DateTime<Utc>,
) {
    let after = db_now(conn).await;
    let slack = Duration::milliseconds(SLACK_MS);
    assert!(
        stamp >= before - slack && stamp <= after + slack,
        "{what} ({stamp}) is outside the database clock window [{before}, {after}]; \
         the write used the host clock"
    );
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

/// Count rows of `id` that the heartbeat-timeout scan selects.
async fn heartbeat_scan_hits(conn: &mut AsyncPgConnection, id: Uuid) -> i64 {
    diesel::sql_query(format!(
        "SELECT count(*) AS count FROM ({}) AS scan WHERE scan.id = $1",
        heartbeat_timeout_query()
    ))
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result::<CountRow>(conn)
    .await
    .expect("run heartbeat scan")
    .count
}

struct NoMetrics;
impl MetricsRecorder for NoMetrics {}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn record_heartbeat_stamps_the_database_clock() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("hb-clock");
    let task = enqueue_activity(&mut conn, &q, Some(Duration::seconds(30))).await;
    let claimed = queue::claim_task(&mut conn, &[q], "w1", "", None, &[], &[])
        .await
        .expect("claim")
        .expect("claimable");
    assert_eq!(claimed.id, task);
    let claim = TaskClaim::of(&claimed).expect("claim");

    let before = db_now(&mut conn).await;
    let write = queue::record_heartbeat(&mut conn, &claim, serde_json::json!({"step": 1}))
        .await
        .expect("heartbeat");
    assert!(matches!(write, queue::ClaimWrite::Applied));

    let stamp = read_column(&mut conn, "last_heartbeat_at", task).await;
    assert_on_db_clock(&mut conn, "last_heartbeat_at", before, stamp).await;
}

/// A fresh heartbeat must keep a long-running task out of the timeout scan.
/// A host 60 s behind the database used to push the stamp 60 s into the past.
/// That is older than the 30 s timeout, so the scan selected a healthy task.
#[tokio::test]
async fn a_fresh_heartbeat_is_not_a_false_heartbeat_timeout() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("hb-scan");
    let task = enqueue_activity(&mut conn, &q, Some(Duration::seconds(30))).await;
    let claimed = queue::claim_task(&mut conn, &[q], "w1", "", None, &[], &[])
        .await
        .expect("claim")
        .expect("claimable");
    // Age the claim on the database clock so only a heartbeat can save it.
    diesel::sql_query(
        "UPDATE harvest_task_queue SET started_at = clock_timestamp() - INTERVAL '10 minutes' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task)
    .execute(&mut conn)
    .await
    .expect("age claim");
    assert_eq!(
        heartbeat_scan_hits(&mut conn, task).await,
        1,
        "setup: an aged claim with no heartbeat must time out"
    );

    let claim = TaskClaim::of(&claimed).expect("claim");
    let write = queue::record_heartbeat(&mut conn, &claim, serde_json::json!({}))
        .await
        .expect("heartbeat");
    assert!(matches!(write, queue::ClaimWrite::Applied));

    assert_eq!(
        heartbeat_scan_hits(&mut conn, task).await,
        0,
        "a task that just heartbeated must not time out"
    );
}

/// `NOW()` freezes at transaction start. The stamp must still be live.
#[tokio::test]
async fn record_heartbeat_uses_the_live_clock_inside_a_transaction() {
    use diesel_async::AsyncConnection as _;

    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("hb-txn");
    let task = enqueue_activity(&mut conn, &q, Some(Duration::seconds(30))).await;
    let claimed = queue::claim_task(&mut conn, &[q], "w1", "", None, &[], &[])
        .await
        .expect("claim")
        .expect("claimable");
    let claim = TaskClaim::of(&claimed).expect("claim");

    let before = db_now(&mut conn).await;
    Box::pin(
        conn.transaction::<(), autumn_harvest::error::HarvestError, _>(async |conn| {
            diesel::sql_query("SELECT pg_sleep(1)")
                .execute(conn)
                .await
                .expect("simulate prior transaction work");
            queue::record_heartbeat(conn, &claim, serde_json::json!({}))
                .await
                .map(|_| ())
        }),
    )
    .await
    .expect("heartbeat in transaction");

    let stamp = read_column(&mut conn, "last_heartbeat_at", task).await;
    assert_on_db_clock(&mut conn, "last_heartbeat_at", before, stamp).await;
}

/// The stuck-running backstop re-pends a workflow task. Its new
/// `scheduled_at` must come from the database clock.
#[tokio::test]
async fn stuck_task_requeue_stamps_scheduled_at_on_the_database_clock() {
    let (mut conn, _c) = setup_db().await;
    let exec_id = insert_execution(&mut conn).await;
    let worker = format!("live-{}", Uuid::new_v4().simple());
    let task = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workers (worker_id, last_heartbeat_at, max_concurrency, host) \
         VALUES ($1, NOW(), 10, 'localhost')",
    )
    .bind::<diesel::sql_types::Text, _>(&worker)
    .execute(&mut conn)
    .await
    .expect("insert live worker");
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, input, state, worker_id, \
          attempt, max_attempts, started_at, crash_strikes) \
         VALUES ($1, $2, 'workflow', $3, '{}'::jsonb, 'RUNNING', $4, 1, 3, \
                 clock_timestamp() - INTERVAL '1 hour', 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(task)
    .bind::<diesel::sql_types::Text, _>(unique_queue("stuck"))
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .bind::<diesel::sql_types::Text, _>(&worker)
    .execute(&mut conn)
    .await
    .expect("insert stuck task");

    let before = db_now(&mut conn).await;
    let summary = reclaim_orphaned_tasks(
        &mut conn,
        3,
        10,
        Some(60),
        &NoMetrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("reclaim");
    assert_eq!(summary.stuck_requeued, 1, "setup: the backstop must fire");

    let stamp = read_column(&mut conn, "scheduled_at", task).await;
    assert_on_db_clock(&mut conn, "scheduled_at", before, stamp).await;
}

/// `force_retry_activity_now` moves a backing-off row to immediate
/// eligibility. The new `scheduled_at` must come from the database clock.
#[tokio::test]
async fn force_retry_stamps_scheduled_at_on_the_database_clock() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("force-retry");
    let task = enqueue_activity(&mut conn, &q, None).await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET scheduled_at = clock_timestamp() + INTERVAL '1 hour' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task)
    .execute(&mut conn)
    .await
    .expect("push the retry into the future");
    let exec_id: Uuid = {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = diesel::sql_types::Uuid)]
            workflow_exec_id: Uuid,
        }
        diesel::sql_query("SELECT workflow_exec_id FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(task)
            .get_result::<Row>(&mut conn)
            .await
            .expect("exec id")
            .workflow_exec_id
    };

    let before = db_now(&mut conn).await;
    let outcome = queue::force_retry_activity_now(&mut conn, exec_id, task)
        .await
        .expect("force retry");
    assert!(outcome.advanced, "setup: the row must advance");

    let stamp = read_column(&mut conn, "scheduled_at", task).await;
    assert_on_db_clock(&mut conn, "scheduled_at", before, stamp).await;
    assert_eq!(
        outcome.scheduled_at, stamp,
        "outcome reports the stored value"
    );
}
