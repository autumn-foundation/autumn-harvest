#![cfg(feature = "db")]
#![allow(clippy::items_after_statements)]
//! Terminal-task janitor (issue #1811).
//!
//! Finished `harvest_task_queue` rows stayed forever when history retention was
//! off. These tests drive the real retention janitor against Postgres. They
//! pin this contract:
//!
//! - Old `COMPLETED`, `FAILED` and `CANCELLED` rows are deleted.
//! - `PENDING` and `RUNNING` rows are never deleted, at any age.
//! - Terminal rows younger than the window stay.
//! - Each statement deletes at most `batch_size` rows, and one tick runs at
//!   most the batch cap of statements. The next tick continues.
//! - The sweep takes no lock on a live row.
//! - A terminal workflow row stays while its execution is live, or while a
//!   dead letter exists for its execution.
//! - A row locked by another transaction is skipped, not waited on.
//! - `dry_run` deletes nothing and records no metric.
//! - A disabled janitor deletes nothing and reports `None`.
//! - A role without `DELETE` gets an error in the outcome, not a silent zero.
//! - The migration sets the table's reloptions and reshapes its indexes. It
//!   round-trips through `down.sql`, and a heartbeat update is HOT after it.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it,
//! with `--test-threads=1`: each test empties the shared tables. Otherwise
//! the suite starts a testcontainers Postgres.

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
        let entries = self.deleted.lock().unwrap().clone();
        let mut out = BTreeMap::new();
        for (state, count) in entries {
            *out.entry(state).or_insert(0) += count;
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

/// Tests can share one database, so each test starts from empty tables.
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

/// Every pass off except the terminal-task janitor. The runtime then spawns
/// for the janitor alone.
fn task_gc_only(window: Duration) -> RetentionConfig {
    RetentionConfig {
        max_age_secs: None,
        audit_retention_days: 0,
        schedule_decision_retention_days: 0,
        partitions: autumn_harvest::retention::PartitionMaintenanceConfig {
            enabled: false,
            ..autumn_harvest::retention::PartitionMaintenanceConfig::default()
        },
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
    // A `MIGRATED` seal must carry its forwarding pointer.
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
            (workflow_name, workflow_id, shard_id, state, input, queue_name, \
             migrated_to_shard, migrated_at) \
         VALUES ('janitor_wf', $1, 0, $2, '{}'::jsonb, 'janitor-q', \
                 CASE WHEN $2 = 'MIGRATED' THEN 1 END, \
                 CASE WHEN $2 = 'MIGRATED' THEN NOW() END) \
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

const fn outcome(result: &RetentionTickResult) -> &TerminalTaskGcOutcome {
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
async fn aliased_shards_on_one_database_are_swept_once_per_tick() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let batch = 10_usize;
    let budget = i64::try_from(batch * MAX_TERMINAL_TASK_SWEEP_BATCHES_PER_TICK).unwrap();
    insert_activity_rows(&mut conn, "COMPLETED", Some(days_ago(30)), budget + 25).await;

    // Two logical shards on one physical pool, as in a pre-split rollout.
    let pool = build_pool(&url);
    let pools = ShardedDbPool::from_map(
        BTreeMap::from([
            (autumn_harvest::types::ShardId::new(0), pool.clone()),
            (autumn_harvest::types::ShardId::new(1), pool),
        ]),
        autumn_harvest::types::ShardId::new(0),
    );
    let metrics = Arc::new(CapturingMetrics::default());
    let runtime = RetentionRuntime::spawn(
        pools,
        RetentionConfig {
            batch_size: batch,
            ..task_gc_only(WEEK)
        },
        Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
        None,
        None,
    )
    .expect("the runtime spawns");
    runtime.run_now();
    let mut snapshot = Vec::new();
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if runtime.monitor().iterations_completed() > 0 {
            snapshot = runtime.monitor().snapshot().per_shard;
            break;
        }
    }
    runtime.shutdown();

    let budget = u64::try_from(budget).unwrap();
    assert_eq!(
        counts_by_state(&mut conn).await.get("COMPLETED"),
        Some(&25),
        "one tick spends one budget on the shared database"
    );
    assert_eq!(metrics.by_state().get("COMPLETED"), Some(&budget));
    assert_eq!(snapshot.len(), 2);
    for shard in &snapshot {
        assert_eq!(
            outcome(shard).deleted,
            budget,
            "each alias reports the one database-wide pass"
        );
    }
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

    // Audit purge and partition maintenance keep the runtime alive, so the
    // tick still runs.
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
async fn a_dead_lettered_executions_workflow_row_is_kept() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    conn.batch_execute("DELETE FROM harvest_dead_letters;")
        .await
        .expect("scrub dead letters");

    // A DLQ redrive can move a FAILED execution back to RUNNING. Until the
    // dead letter is gone, the execution's workflow row must stay.
    let parked = insert_execution(&mut conn, "FAILED").await;
    let plain = insert_execution(&mut conn, "FAILED").await;
    let kept = insert_task_for(&mut conn, parked, "workflow", "FAILED", days_ago(30)).await;
    let gone = insert_task_for(&mut conn, plain, "workflow", "FAILED", days_ago(30)).await;
    diesel::sql_query(
        "INSERT INTO harvest_dead_letters \
            (original_task_id, queue_name, task_type, workflow_exec_id, input, error, attempts) \
         VALUES (gen_random_uuid(), 'janitor-q', 'activity', $1, '{}'::jsonb, 'boom', 3)",
    )
    .bind::<SqlUuid, _>(parked)
    .execute(&mut conn)
    .await
    .expect("insert dead letter");

    let deleted = queue::sweep_terminal_tasks(&mut conn, days_ago(7), 100, false)
        .await
        .expect("sweep");
    assert_eq!(deleted.get("FAILED"), Some(&1));
    assert!(
        task_exists(&mut conn, kept).await,
        "the dead letter keeps the row"
    );
    assert!(!task_exists(&mut conn, gone).await);

    conn.batch_execute("DELETE FROM harvest_dead_letters;")
        .await
        .expect("scrub dead letters");
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

    // The dead-letter guard reads its own index.
    let dead_letters: Vec<IndexRow> = diesel::sql_query(
        "SELECT indexname::text AS indexname, indexdef FROM pg_indexes \
         WHERE indexname = 'idx_harvest_dl_workflow_exec_id'",
    )
    .load::<IndexRow>(&mut conn)
    .await
    .expect("read dead-letter index");
    assert_eq!(dead_letters.len(), 1, "the dead-letter index exists");
    assert!(dead_letters[0].indexdef.contains("(workflow_exec_id)"));

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
    #[derive(diesel::QueryableByName)]
    struct ClaimRow {
        #[diesel(sql_type = SqlUuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
    }
    async fn hot_updates(conn: &mut AsyncPgConnection) -> i64 {
        diesel::sql_query(
            "SELECT pg_stat_get_xact_tuples_hot_updated('harvest_task_queue'::regclass) AS hot",
        )
        .get_result::<HotRow>(conn)
        .await
        .expect("read HOT counter")
        .hot
    }

    // One transaction, so the per-transaction counter reads this work.
    conn.batch_execute("BEGIN").await.expect("begin");
    let row = diesel::sql_query(
        "INSERT INTO harvest_task_queue \
            (queue_name, task_type, activity_name, input, state, worker_id, started_at) \
         VALUES ('janitor-q', 'activity', 'send_email', '{}'::jsonb, 'RUNNING', 'w-1', NOW()) \
         RETURNING id, attempt",
    )
    .get_result::<ClaimRow>(&mut conn)
    .await
    .expect("insert running row");
    let before = hot_updates(&mut conn).await;
    let claim = queue::TaskClaim::new(row.id, "w-1", row.attempt);
    let write = queue::record_heartbeat(&mut conn, &claim, serde_json::json!({"p": 1}))
        .await
        .expect("record heartbeat");
    assert_eq!(write, queue::ClaimWrite::Applied, "the heartbeat must land");
    let after = hot_updates(&mut conn).await;
    conn.batch_execute("ROLLBACK").await.expect("rollback");

    assert_eq!(
        after - before,
        1,
        "a heartbeat changes no indexed column, so it is HOT"
    );
}

/// Record the rows each `DELETE` statement removes from the task table.
///
/// A statement-level trigger sees one statement's deleted rows as a
/// transition table. The log then proves the per-statement bound.
async fn install_delete_log(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "CREATE TABLE IF NOT EXISTS janitor_delete_log (seq BIGSERIAL, n BIGINT); \
         TRUNCATE janitor_delete_log; \
         CREATE OR REPLACE FUNCTION janitor_log_delete() RETURNS trigger AS $$ \
         BEGIN \
             INSERT INTO janitor_delete_log (n) SELECT COUNT(*) FROM old_rows; \
             RETURN NULL; \
         END $$ LANGUAGE plpgsql; \
         DROP TRIGGER IF EXISTS janitor_log_delete ON harvest_task_queue; \
         CREATE TRIGGER janitor_log_delete AFTER DELETE ON harvest_task_queue \
             REFERENCING OLD TABLE AS old_rows \
             FOR EACH STATEMENT EXECUTE FUNCTION janitor_log_delete();",
    )
    .await
    .expect("install the delete log");
}

async fn remove_delete_log(conn: &mut AsyncPgConnection) -> Vec<i64> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let rows = diesel::sql_query("SELECT n FROM janitor_delete_log ORDER BY seq")
        .load::<Row>(conn)
        .await
        .expect("read the delete log")
        .into_iter()
        .map(|r| r.n)
        .collect();
    conn.batch_execute(
        "DROP TRIGGER IF EXISTS janitor_log_delete ON harvest_task_queue; \
         DROP FUNCTION IF EXISTS janitor_log_delete(); \
         DROP TABLE IF EXISTS janitor_delete_log;",
    )
    .await
    .expect("remove the delete log");
    rows
}

#[tokio::test]
async fn each_statement_deletes_at_most_one_batch_and_no_live_row_is_locked() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let batch = 7_usize;
    let rows = i64::try_from(batch * MAX_TERMINAL_TASK_SWEEP_BATCHES_PER_TICK).unwrap() + 4;
    insert_activity_rows(&mut conn, "COMPLETED", Some(days_ago(30)), rows).await;
    insert_activity_rows(&mut conn, "PENDING", None, 5).await;
    insert_activity_rows(&mut conn, "RUNNING", None, 5).await;

    install_delete_log(&mut conn).await;
    let deleted = queue::sweep_terminal_tasks(&mut conn, days_ago(7), batch, false)
        .await
        .expect("sweep");
    let statements = remove_delete_log(&mut conn).await;

    let budget = u64::try_from(batch * MAX_TERMINAL_TASK_SWEEP_BATCHES_PER_TICK).unwrap();
    assert_eq!(deleted.get("COMPLETED"), Some(&budget));
    assert_eq!(
        statements.len(),
        MAX_TERMINAL_TASK_SWEEP_BATCHES_PER_TICK,
        "one tick runs the batch cap of statements"
    );
    let batch = i64::try_from(batch).unwrap();
    assert!(
        statements.iter().all(|n| *n == batch),
        "every statement deletes one full batch: {statements:?}"
    );

    // `xmax` stays 0 unless a transaction locked or updated the row.
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let touched = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue \
         WHERE state IN ('PENDING', 'RUNNING') AND xmax::text <> '0'",
    )
    .get_result::<Row>(&mut conn)
    .await
    .expect("read xmax")
    .n;
    assert_eq!(touched, 0, "the sweep must not lock a live row");
}

#[tokio::test]
async fn a_batch_size_above_the_cap_is_clamped() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let cap = i64::try_from(queue::MAX_TERMINAL_TASK_SWEEP_BATCH).unwrap();
    insert_activity_rows(&mut conn, "COMPLETED", Some(days_ago(30)), cap + 3).await;

    install_delete_log(&mut conn).await;
    let deleted = queue::sweep_terminal_tasks(&mut conn, days_ago(7), usize::MAX, false)
        .await
        .expect("sweep");
    let statements = remove_delete_log(&mut conn).await;

    assert_eq!(
        deleted.get("COMPLETED"),
        Some(&u64::try_from(cap + 3).unwrap())
    );
    assert_eq!(statements, vec![cap, 3], "no statement exceeds the cap");
}

#[tokio::test]
async fn a_row_without_completed_at_or_at_the_cutoff_stays() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let cutoff = days_ago(7);
    insert_activity_rows(&mut conn, "COMPLETED", None, 2).await;
    insert_activity_rows(&mut conn, "FAILED", Some(cutoff), 3).await;
    insert_activity_rows(
        &mut conn,
        "CANCELLED",
        Some(cutoff - chrono::Duration::microseconds(1)),
        1,
    )
    .await;

    let deleted = queue::sweep_terminal_tasks(&mut conn, cutoff, 100, false)
        .await
        .expect("sweep");
    assert_eq!(
        deleted.get("CANCELLED"),
        Some(&1),
        "a row before the cutoff goes"
    );
    let left = counts_by_state(&mut conn).await;
    assert_eq!(left.get("COMPLETED"), Some(&2), "a NULL completed_at stays");
    assert_eq!(left.get("FAILED"), Some(&3), "a row at the cutoff stays");
}

#[tokio::test]
async fn every_execution_state_decides_whether_its_workflow_row_stays() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    // Every state that the execution state check allows.
    let states = [
        "RUNNING",
        "PAUSED",
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMED_OUT",
        "CONTINUED_AS_NEW",
        "TERMINATED",
        "MIGRATING",
        "MIGRATED",
    ];
    let mut rows = Vec::new();
    for state in states {
        let exec = insert_execution(&mut conn, state).await;
        let row = insert_task_for(&mut conn, exec, "workflow", "COMPLETED", days_ago(30)).await;
        rows.push((state, row));
    }
    // A workflow row with no execution has nothing to keep it.
    let orphan: uuid::Uuid = {
        #[derive(diesel::QueryableByName)]
        struct IdRow {
            #[diesel(sql_type = SqlUuid)]
            id: uuid::Uuid,
        }
        diesel::sql_query(
            "INSERT INTO harvest_task_queue \
                (queue_name, task_type, input, state, completed_at) \
             VALUES ('janitor-q', 'workflow', '{}'::jsonb, 'COMPLETED', $1) \
             RETURNING id",
        )
        .bind::<Timestamptz, _>(days_ago(30))
        .get_result::<IdRow>(&mut conn)
        .await
        .expect("insert orphan workflow row")
        .id
    };

    queue::sweep_terminal_tasks(&mut conn, days_ago(7), 100, false)
        .await
        .expect("sweep");

    for (state, row) in rows {
        let terminal = autumn_harvest::erase::is_terminal_state(state);
        assert_eq!(
            task_exists(&mut conn, row).await,
            !terminal,
            "an execution in {state} must {} its workflow row",
            if terminal { "release" } else { "keep" }
        );
    }
    assert!(!task_exists(&mut conn, orphan).await, "an orphan row goes");
}

#[tokio::test]
async fn a_role_without_delete_reports_the_error() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    insert_activity_rows(&mut conn, "COMPLETED", Some(days_ago(30)), 3).await;

    // A least-privilege role that can read and update but not delete.
    let role = format!("janitor_nodel_{}", uuid::Uuid::new_v4().simple());
    conn.batch_execute(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'pw'; \
         GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO {role};"
    ))
    .await
    .expect("create role");
    let (rest, db) = url.rsplit_once('/').expect("url has a database");
    let host = rest.rsplit_once('@').expect("url has a host").1;
    let role_url = format!("postgres://{role}:pw@{host}/{db}");

    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(
        build_pool(&role_url),
        task_gc_only(WEEK),
        Arc::clone(&metrics),
    )
    .await;

    let gc = outcome(&result);
    assert!(
        gc.error
            .as_deref()
            .is_some_and(|e| e.contains("permission denied")),
        "the error is reported, not hidden as zero rows: {gc:?}"
    );
    assert_eq!(gc.deleted, 0);
    assert!(metrics.by_state().is_empty());
    assert_eq!(counts_by_state(&mut conn).await.get("COMPLETED"), Some(&3));

    conn.batch_execute(&format!(
        "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {role}; DROP ROLE {role};"
    ))
    .await
    .expect("drop role");
}

#[tokio::test]
async fn the_migration_round_trips() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    const DOWN: &str =
        include_str!("../../migrations/20261003201739_harvest_task_queue_hygiene/down.sql");
    const UP: &str =
        include_str!("../../migrations/20261003201739_harvest_task_queue_hygiene/up.sql");

    async fn index_names(conn: &mut AsyncPgConnection) -> Vec<String> {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = Text)]
            name: String,
        }
        diesel::sql_query(
            "SELECT indexname::text AS name FROM pg_indexes \
             WHERE tablename IN ('harvest_task_queue', 'harvest_dead_letters') \
             ORDER BY 1",
        )
        .load::<Row>(conn)
        .await
        .expect("read indexes")
        .into_iter()
        .map(|r| r.name)
        .collect()
    }

    let migrated = index_names(&mut conn).await;
    conn.batch_execute("BEGIN").await.expect("begin");
    conn.batch_execute(DOWN).await.expect("down.sql");
    let reverted = index_names(&mut conn).await;
    conn.batch_execute(UP).await.expect("up.sql after down.sql");
    let again = index_names(&mut conn).await;
    // A second `up.sql` finds valid indexes and changes nothing.
    conn.batch_execute(UP).await.expect("up.sql is idempotent");
    let twice = index_names(&mut conn).await;
    conn.batch_execute("ROLLBACK").await.expect("rollback");

    for name in [
        "idx_harvest_tq_running",
        "idx_harvest_task_queue_rate_limit_key",
        "harvest_task_queue_session_id_pending",
    ] {
        assert!(
            reverted.iter().any(|n| n == name),
            "down.sql restores {name}"
        );
    }
    assert!(
        !reverted
            .iter()
            .any(|n| n == "idx_harvest_tq_terminal_completed_at")
    );
    assert_eq!(again, migrated);
    assert_eq!(twice, migrated);
}
