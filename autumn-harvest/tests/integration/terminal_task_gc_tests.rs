#![cfg(feature = "db")]
//! Terminal-task janitor (issue #1811).
//!
//! Finished `harvest_task_queue` rows stayed forever when history retention was
//! off. These tests drive the real retention janitor against Postgres. They
//! pin this contract:
//!
//! - Old `COMPLETED`, `FAILED` and `CANCELLED` rows are deleted.
//! - `PENDING` and `RUNNING` rows are never deleted, at any age.
//! - Terminal rows younger than the window stay.
//! - One tick deletes at most `batch_size` x the batch cap. The next tick
//!   continues.
//! - A terminal workflow row stays while its execution is live.
//! - A row locked by another transaction is skipped, not waited on.
//! - `dry_run` deletes nothing and records no metric.
//! - The migration sets the table's reloptions and reshapes its indexes.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise the suite starts a testcontainers Postgres.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::queue::{self, MAX_TERMINAL_TASK_SWEEP_BATCHES_PER_TICK};
use autumn_harvest::retention::{
    RetentionConfig, RetentionRuntime, RetentionTickResult, TerminalTaskGcOutcome,
};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Captures `record_terminal_tasks_deleted` calls.
#[derive(Default)]
struct CapturingMetrics {
    deleted: Mutex<Vec<(String, u64)>>,
}

impl CapturingMetrics {
    fn by_state(&self) -> BTreeMap<String, u64> {
        let mut out = BTreeMap::new();
        for (state, count) in self.deleted.lock().unwrap().iter() {
            *out.entry(state.clone()).or_insert(0) += count;
        }
        out
    }
}

impl MetricsRecorder for CapturingMetrics {
    fn record_terminal_tasks_deleted(&self, state: &str, count: u64) {
        self.deleted
            .lock()
            .unwrap()
            .push((state.to_string(), count));
    }

    fn record_scanner_tick(&self, _scanner: &str, _shard: &str) {}
}

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
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

/// Every suite shares one database, so each test starts from empty tables.
async fn scrub(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "DELETE FROM harvest_task_queue; \
         DELETE FROM harvest_workflow_executions;",
    )
    .await
    .expect("scrub");
}

fn days_ago(days: i64) -> DateTime<Utc> {
    Utc::now() - chrono::Duration::days(days)
}

const WEEK: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Every pass off except the terminal-task janitor.
fn task_gc_only(window: Duration) -> RetentionConfig {
    RetentionConfig {
        max_age_secs: None,
        audit_retention_days: 0,
        schedule_decision_retention_days: 0,
        ..RetentionConfig::default()
    }
    .without_rate_limit_bucket_gc()
    .with_terminal_task_retention(window)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Insert `count` activity rows in `state`, finished at `completed_at`.
async fn insert_activity_rows(
    conn: &mut AsyncPgConnection,
    state: &str,
    completed_at: Option<DateTime<Utc>>,
    count: i64,
) {
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
            (queue_name, task_type, activity_name, input, state, completed_at) \
         SELECT 'janitor-q', 'activity', 'send_email', '{}'::jsonb, $1, $2 \
         FROM generate_series(1, $3)",
    )
    .bind::<Text, _>(state)
    .bind::<Nullable<Timestamptz>, _>(completed_at)
    .bind::<BigInt, _>(count)
    .execute(conn)
    .await
    .expect("insert activity rows");
}

/// Insert one execution in `state` and return its id.
async fn insert_execution(conn: &mut AsyncPgConnection, state: &str) -> uuid::Uuid {
    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = SqlUuid)]
        id: uuid::Uuid,
    }
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
            (workflow_name, workflow_id, shard_id, state, input, queue_name) \
         VALUES ('janitor_wf', $1, 0, $2, '{}'::jsonb, 'janitor-q') \
         RETURNING id",
    )
    .bind::<Text, _>(format!("janitor-{}", uuid::Uuid::new_v4()))
    .bind::<Text, _>(state)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id
}

/// Insert one task row for `exec_id` and return its id.
async fn insert_task_for(
    conn: &mut AsyncPgConnection,
    exec_id: uuid::Uuid,
    task_type: &str,
    state: &str,
    completed_at: DateTime<Utc>,
) -> uuid::Uuid {
    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = SqlUuid)]
        id: uuid::Uuid,
    }
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
            (queue_name, task_type, workflow_exec_id, activity_name, input, state, completed_at) \
         VALUES ('janitor-q', $1, $2, \
                 CASE WHEN $1 = 'activity' THEN 'send_email' END, \
                 '{}'::jsonb, $3, $4) \
         RETURNING id",
    )
    .bind::<Text, _>(task_type)
    .bind::<SqlUuid, _>(exec_id)
    .bind::<Text, _>(state)
    .bind::<Timestamptz, _>(completed_at)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert task")
    .id
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// Row counts by state.
async fn counts_by_state(conn: &mut AsyncPgConnection) -> BTreeMap<String, i64> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        state: String,
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT state, COUNT(*) AS n FROM harvest_task_queue GROUP BY state")
        .load::<Row>(conn)
        .await
        .expect("count by state")
        .into_iter()
        .map(|r| (r.state, r.n))
        .collect()
}

async fn task_exists(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> bool {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_task_queue WHERE id = $1")
        .bind::<SqlUuid, _>(id)
        .get_result::<Row>(conn)
        .await
        .expect("count task")
        .n
        > 0
}

// ---------------------------------------------------------------------------
// Tick driver
// ---------------------------------------------------------------------------

/// Run exactly one janitor iteration and return shard 0's result.
///
/// The loop runs only on `run_now` or after `tick_interval` (1 h). So one
/// `run_now` on a fresh runtime is one iteration.
async fn run_one_tick(
    pool: DbPool,
    config: RetentionConfig,
    metrics: Arc<CapturingMetrics>,
) -> RetentionTickResult {
    let runtime = RetentionRuntime::spawn(
        ShardedDbPool::single(pool),
        config,
        Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
        None,
        None,
    )
    .expect("the runtime spawns when the task janitor is active");
    runtime.run_now();
    let mut result = None;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if runtime.monitor().iterations_completed() == 0 {
            continue;
        }
        let snap = runtime.monitor().snapshot();
        result = snap.per_shard.iter().find(|r| r.shard == 0).cloned();
        break;
    }
    runtime.shutdown();
    result.expect("the retention tick did not complete in time")
}

fn outcome(result: &RetentionTickResult) -> &TerminalTaskGcOutcome {
    result
        .terminal_task_gc
        .as_ref()
        .expect("an active janitor reports an outcome")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn old_terminal_rows_are_deleted_and_live_rows_are_untouched() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    // Old rows in every state. Live rows are old too, so age alone never
    // protects them.
    for state in ["COMPLETED", "FAILED", "CANCELLED"] {
        insert_activity_rows(&mut conn, state, Some(days_ago(30)), 3).await;
        insert_activity_rows(&mut conn, state, Some(days_ago(1)), 2).await;
    }
    insert_activity_rows(&mut conn, "PENDING", None, 4).await;
    insert_activity_rows(&mut conn, "RUNNING", None, 4).await;
    // A live row with a stale `completed_at` from an earlier attempt.
    insert_activity_rows(&mut conn, "PENDING", Some(days_ago(30)), 1).await;
    insert_activity_rows(&mut conn, "RUNNING", Some(days_ago(30)), 1).await;

    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(build_pool(&url), task_gc_only(WEEK), Arc::clone(&metrics)).await;

    let gc = outcome(&result);
    assert_eq!(gc.error, None);
    assert!(!gc.dry_run);
    assert_eq!(gc.deleted, 9);
    let expected: BTreeMap<String, u64> = ["CANCELLED", "COMPLETED", "FAILED"]
        .into_iter()
        .map(|s| (s.to_string(), 3))
        .collect();
    assert_eq!(gc.deleted_by_state, expected);
    assert_eq!(
        metrics.by_state(),
        expected,
        "the metric counts real deletes"
    );

    let left = counts_by_state(&mut conn).await;
    assert_eq!(left.get("PENDING"), Some(&5), "no PENDING row is deleted");
    assert_eq!(left.get("RUNNING"), Some(&5), "no RUNNING row is deleted");
    for state in ["COMPLETED", "FAILED", "CANCELLED"] {
        assert_eq!(left.get(state), Some(&2), "young {state} rows stay");
    }
}

#[tokio::test]
async fn one_tick_deletes_in_bounded_batches_and_the_next_tick_continues() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let batch = 10_usize;
    let budget = i64::try_from(batch * MAX_TERMINAL_TASK_SWEEP_BATCHES_PER_TICK).unwrap();
    insert_activity_rows(&mut conn, "COMPLETED", Some(days_ago(30)), budget + 25).await;
    insert_activity_rows(&mut conn, "PENDING", None, 50).await;
    insert_activity_rows(&mut conn, "RUNNING", None, 50).await;

    let config = RetentionConfig {
        batch_size: batch,
        ..task_gc_only(WEEK)
    };

    let first = run_one_tick(
        build_pool(&url),
        config.clone(),
        Arc::new(CapturingMetrics::default()),
    )
    .await;
    assert_eq!(
        outcome(&first).deleted,
        u64::try_from(budget).unwrap(),
        "one tick stops at batch_size x the batch cap"
    );
    assert_eq!(counts_by_state(&mut conn).await.get("COMPLETED"), Some(&25));

    let second = run_one_tick(
        build_pool(&url),
        config,
        Arc::new(CapturingMetrics::default()),
    )
    .await;
    assert_eq!(outcome(&second).deleted, 25, "the next tick continues");

    let left = counts_by_state(&mut conn).await;
    assert_eq!(left.get("COMPLETED"), None);
    assert_eq!(left.get("PENDING"), Some(&50));
    assert_eq!(left.get("RUNNING"), Some(&50));
}

#[tokio::test]
async fn the_sweep_returns_per_state_counts_for_any_batch_size() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    insert_activity_rows(&mut conn, "COMPLETED", Some(days_ago(30)), 17).await;
    insert_activity_rows(&mut conn, "FAILED", Some(days_ago(30)), 5).await;
    insert_activity_rows(&mut conn, "RUNNING", None, 3).await;

    // A batch size that does not divide the row count exercises the keyset
    // boundary between pages.
    let deleted = queue::sweep_terminal_tasks(&mut conn, days_ago(7), 3, false)
        .await
        .expect("sweep");
    assert_eq!(deleted.get("COMPLETED"), Some(&17));
    assert_eq!(deleted.get("FAILED"), Some(&5));
    assert_eq!(counts_by_state(&mut conn).await.get("RUNNING"), Some(&3));

    // A zero batch size is clamped to 1 and still terminates.
    insert_activity_rows(&mut conn, "CANCELLED", Some(days_ago(30)), 2).await;
    let deleted = queue::sweep_terminal_tasks(&mut conn, days_ago(7), 0, false)
        .await
        .expect("sweep");
    assert_eq!(deleted.get("CANCELLED"), Some(&2));
}

#[tokio::test]
async fn dry_run_previews_without_deleting_or_metering() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    insert_activity_rows(&mut conn, "COMPLETED", Some(days_ago(30)), 25).await;
    insert_activity_rows(&mut conn, "PENDING", None, 2).await;

    let metrics = Arc::new(CapturingMetrics::default());
    let config = RetentionConfig {
        dry_run: true,
        batch_size: 10,
        ..task_gc_only(WEEK)
    };
    let result = run_one_tick(build_pool(&url), config, Arc::clone(&metrics)).await;

    let gc = outcome(&result);
    assert!(gc.dry_run);
    assert_eq!(gc.deleted, 25, "the preview pages past its first batch");
    assert!(metrics.by_state().is_empty(), "a preview records no metric");
    assert_eq!(counts_by_state(&mut conn).await.get("COMPLETED"), Some(&25));
}

#[tokio::test]
async fn a_disabled_janitor_deletes_nothing_and_reports_none() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    insert_activity_rows(&mut conn, "COMPLETED", Some(days_ago(30)), 3).await;

    // Audit purge keeps the runtime alive, so the tick still runs.
    let config = RetentionConfig {
        max_age_secs: None,
        schedule_decision_retention_days: 0,
        ..RetentionConfig::default()
    }
    .without_rate_limit_bucket_gc()
    .without_terminal_task_gc();
    let result = run_one_tick(
        build_pool(&url),
        config,
        Arc::new(CapturingMetrics::default()),
    )
    .await;

    assert!(result.terminal_task_gc.is_none());
    assert_eq!(counts_by_state(&mut conn).await.get("COMPLETED"), Some(&3));
}

#[tokio::test]
async fn a_live_executions_terminal_workflow_row_is_kept() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    // `concurrency.rs` finds a live execution through its workflow row, in
    // any row state. That row must stay while the execution is live.
    let running = insert_execution(&mut conn, "RUNNING").await;
    let paused = insert_execution(&mut conn, "PAUSED").await;
    let done = insert_execution(&mut conn, "COMPLETED").await;

    let kept_running =
        insert_task_for(&mut conn, running, "workflow", "FAILED", days_ago(30)).await;
    let kept_paused =
        insert_task_for(&mut conn, paused, "workflow", "COMPLETED", days_ago(30)).await;
    let gone_activity =
        insert_task_for(&mut conn, running, "activity", "COMPLETED", days_ago(30)).await;
    let gone_done = insert_task_for(&mut conn, done, "workflow", "COMPLETED", days_ago(30)).await;

    let result = run_one_tick(
        build_pool(&url),
        task_gc_only(WEEK),
        Arc::new(CapturingMetrics::default()),
    )
    .await;
    assert_eq!(outcome(&result).deleted, 2);

    assert!(task_exists(&mut conn, kept_running).await);
    assert!(task_exists(&mut conn, kept_paused).await);
    assert!(!task_exists(&mut conn, gone_activity).await);
    assert!(!task_exists(&mut conn, gone_done).await);
}

#[tokio::test]
async fn a_locked_row_is_skipped_not_waited_on() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let exec = insert_execution(&mut conn, "COMPLETED").await;
    let locked = insert_task_for(&mut conn, exec, "activity", "COMPLETED", days_ago(30)).await;
    insert_activity_rows(&mut conn, "COMPLETED", Some(days_ago(30)), 4).await;

    // Hold a row lock in an open transaction on a second connection.
    let mut holder = connect(&url).await;
    holder.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query("SELECT id FROM harvest_task_queue WHERE id = $1 FOR UPDATE")
        .bind::<SqlUuid, _>(locked)
        .execute(&mut holder)
        .await
        .expect("lock the row");

    let deleted = tokio::time::timeout(
        Duration::from_secs(10),
        queue::sweep_terminal_tasks(&mut conn, days_ago(7), 100, false),
    )
    .await
    .expect("the sweep must not wait on a locked row")
    .expect("sweep");
    assert_eq!(deleted.get("COMPLETED"), Some(&4));
    assert!(
        task_exists(&mut conn, locked).await,
        "the locked row is skipped"
    );

    holder.batch_execute("COMMIT").await.expect("commit");
    let deleted = queue::sweep_terminal_tasks(&mut conn, days_ago(7), 100, false)
        .await
        .expect("sweep");
    assert_eq!(deleted.get("COMPLETED"), Some(&1), "the next pass takes it");
}

#[tokio::test]
async fn the_migration_tunes_the_table_and_reshapes_its_indexes() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    #[derive(diesel::QueryableByName)]
    struct OptRow {
        #[diesel(sql_type = Text)]
        opt: String,
    }
    let opts: Vec<String> = diesel::sql_query(
        "SELECT unnest(reloptions) AS opt FROM pg_class \
         WHERE oid = 'harvest_task_queue'::regclass",
    )
    .load::<OptRow>(&mut conn)
    .await
    .expect("read reloptions")
    .into_iter()
    .map(|r| r.opt)
    .collect();
    for expected in [
        "fillfactor=80",
        "autovacuum_vacuum_scale_factor=0.02",
        "autovacuum_analyze_scale_factor=0.02",
        "autovacuum_vacuum_cost_limit=2000",
    ] {
        assert!(
            opts.iter().any(|o| o == expected),
            "missing reloption {expected}: {opts:?}"
        );
    }

    #[derive(diesel::QueryableByName)]
    struct IndexRow {
        #[diesel(sql_type = Text)]
        indexname: String,
        #[diesel(sql_type = Text)]
        indexdef: String,
    }
    let indexes: BTreeMap<String, String> = diesel::sql_query(
        "SELECT indexname::text AS indexname, indexdef FROM pg_indexes \
         WHERE tablename = 'harvest_task_queue' AND schemaname = current_schema()",
    )
    .load::<IndexRow>(&mut conn)
    .await
    .expect("read indexes")
    .into_iter()
    .map(|r| (r.indexname, r.indexdef))
    .collect();

    let janitor = indexes
        .get("idx_harvest_tq_terminal_completed_at")
        .expect("the janitor's index exists");
    assert!(janitor.contains("(completed_at, id)"), "{janitor}");
    assert!(janitor.contains("COMPLETED"), "{janitor}");

    // The RUNNING index no longer keys on `last_heartbeat_at`.
    assert!(!indexes.contains_key("idx_harvest_tq_running"));
    let running = indexes
        .get("idx_harvest_tq_running_started")
        .expect("the RUNNING index exists");
    assert!(running.contains("(started_at)"), "{running}");
    assert!(running.contains("'RUNNING'"), "{running}");
    for (name, def) in &indexes {
        assert!(
            !def.contains("last_heartbeat_at") && !def.contains("heartbeat_details"),
            "{name} indexes a heartbeat column, so a heartbeat cannot be HOT: {def}"
        );
    }

    // The audit drops two indexes that no query can use.
    assert!(!indexes.contains_key("idx_harvest_task_queue_rate_limit_key"));
    assert!(!indexes.contains_key("harvest_task_queue_session_id_pending"));
    // The superset index that replaces the first one stays.
    assert!(indexes.contains_key("idx_harvest_task_queue_rate_limit_key_live"));
}

#[tokio::test]
async fn a_heartbeat_update_is_hot() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    #[derive(diesel::QueryableByName)]
    struct HotRow {
        #[diesel(sql_type = BigInt)]
        hot: i64,
    }

    // One transaction, so the per-transaction counter is exact.
    conn.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
            (queue_name, task_type, activity_name, input, state, worker_id, started_at) \
         VALUES ('janitor-q', 'activity', 'send_email', '{}'::jsonb, 'RUNNING', 'w-1', NOW())",
    )
    .execute(&mut conn)
    .await
    .expect("insert running row");
    // The same SET list as `queue::record_heartbeat`.
    diesel::sql_query(
        "UPDATE harvest_task_queue \
            SET last_heartbeat_at = clock_timestamp(), heartbeat_details = '{\"p\": 1}'::jsonb \
          WHERE queue_name = 'janitor-q'",
    )
    .execute(&mut conn)
    .await
    .expect("heartbeat update");
    let hot = diesel::sql_query(
        "SELECT pg_stat_get_xact_tuples_hot_updated('harvest_task_queue'::regclass) AS hot",
    )
    .get_result::<HotRow>(&mut conn)
    .await
    .expect("read HOT counter")
    .hot;
    conn.batch_execute("ROLLBACK").await.expect("rollback");

    assert_eq!(
        hot, 1,
        "a heartbeat changes no indexed column, so it is HOT"
    );
}
