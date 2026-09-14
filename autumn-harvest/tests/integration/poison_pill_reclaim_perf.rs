#![cfg(feature = "db")]
//! Ledger performance investigation: `poison_pill::reclaim_orphaned_tasks`.
//!
//! The scanner's broad candidate scan (`orphaned_running_tasks_query`) is
//! already one batched statement. It is a `NOT EXISTS` anti-join against
//! `harvest_workers`, not a per-row round trip. The per-row loop after it
//! is the target here. For every candidate row, `requeue_orphan` (and
//! `quarantine_orphan`) re-verified the claiming worker was still dead
//! with its own dedicated `SELECT ... FROM harvest_workers WHERE
//! worker_id = $1`, immediately before the row's own write.
//!
//! That re-check exists for a real reason: a worker can resurrect between
//! the broad scan and this row's own lock. But it is keyed on
//! `worker_id`, not on the task row. A single crashed worker process
//! ordinarily holds many concurrently-claimed `RUNNING` tasks at once --
//! that is the entire motivation for `max_concurrency`. So one crash
//! produces many orphan rows that all re-ask the identical question
//! against `harvest_workers`. The statement count for that check scales
//! with the orphan count, not with the number of distinct dead workers.
//! That is exactly the "individually trivial, collectively dominant"
//! bookkeeping shape `docs/performance.md` calls out.
//!
//! `requeue_orphan` folds that dedicated liveness `SELECT` into its own
//! write. See `requeue_orphan_stmt`'s doc comment for the one subtlety
//! that makes this safe. The liveness check must run in a fresh
//! statement, issued only after the row's own lock is already held. It
//! must never share a statement with the lock acquisition itself.
//! `quarantine_orphan` cannot fold its own liveness check the same way.
//! It still needs a separate dead-letter insert between the check and
//! the write. Its statement count is unchanged by this investigation;
//! see the parent `docs/performance-poison-pill-orphan-recheck.md`
//! page's "Known limitations" for why.
//!
//! This mirrors `mutex_lease_reclaim_perf.rs`'s harness and
//! evidence-capture structure: same tool (`pg_stat_statements`,
//! `calls`/`total_buffers`), same fixture-then-snapshot flow, same
//! three-size sweep.

#![allow(clippy::too_many_lines)]

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::poison_pill::reclaim_orphaned_tasks;
use autumn_harvest::telemetry::MetricsRecorder;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

#[derive(Debug, Default)]
struct NoopMetrics;
impl MetricsRecorder for NoopMetrics {}

// ── DB bootstrap (mirrors mutex_lease_reclaim_perf.rs) ─────────────────────

type DbGuard = Option<ContainerAsync<Postgres>>;

async fn setup_server() -> (String, DbGuard) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

async fn create_fresh_db(admin_url: &str, name: &str) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await;

    let (prefix, _) = admin_url.rsplit_once('/').expect("url has a db segment");
    let url = format!("{prefix}/{name}");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migration bundle");
    drop(conn);
    url
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

// ── Fixture generation ──────────────────────────────────────────────────────

/// Seeds `n` orphaned `RUNNING` activity tasks, round-robined across
/// `num_workers` distinct crashed worker ids. None of those worker ids has a
/// `harvest_workers` row at all -- a fully crashed process, never
/// re-heartbeating, same convention `poison_pill_tests.rs::insert_running_task`
/// uses. `crash_strikes` is left at 0 for every row. The sweep is driven
/// with a high quarantine threshold, so every row takes the `Requeue` path
/// this investigation targets -- none is quarantined.
///
/// Pure set-based SQL, not a per-row Rust loop, mirroring
/// `mutex_lease_reclaim_perf.rs`'s `seed_fixture` convention.
async fn seed_fixture(conn: &mut AsyncPgConnection, n: i64, num_workers: i64) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_task_queue (
             id, queue_name, task_type, workflow_exec_id, input, state, worker_id,
             attempt, max_attempts, started_at, last_heartbeat_at, crash_strikes
         )
         SELECT
             ('00000000-0000-0000-0003-' || lpad(gs::text, 12, '0'))::uuid,
             'default', 'activity', NULL, '{{}}'::jsonb, 'RUNNING',
             'poison_pill_perf_dead_worker_' || (gs % {num_workers}),
             1, 3,
             NOW() - INTERVAL '1 hour',
             NOW() - INTERVAL '1 hour',
             0
         FROM generate_series(1, {n}) AS gs;

         ANALYZE harvest_task_queue;
         ANALYZE harvest_workers;"
    ))
    .await
    .expect("seed poison-pill orphan-reclaim perf fixture");
}

// ── pg_stat_statements capture (mirrors mutex_lease_reclaim_perf.rs) ───────

#[derive(diesel::QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    total_buffers: i64,
}

async fn ensure_pg_stat_statements(conn: &mut AsyncPgConnection) {
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(conn)
        .await;
}

async fn reset_stats_for_db(conn: &mut AsyncPgConnection, db_name: &str) {
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(conn)
    .await
    .expect(
        "pg_stat_statements_reset(...) failed -- the HARVEST_TEST_DATABASE_URL role must be \
         able to reset statistics (superuser, or granted EXECUTE on this function)",
    );
}

async fn snapshot_statements(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
                (shared_blks_hit + shared_blks_read) AS total_buffers \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
         ORDER BY total_buffers DESC"
    ))
    .load(conn)
    .await
    .expect(
        "pg_stat_statements query failed -- it must be preloaded via shared_preload_libraries \
         for this capture to produce real evidence rather than fail outright",
    )
}

/// The standalone per-row worker-liveness re-check this investigation
/// targets: a statement that reads `harvest_workers` but does not also
/// touch `harvest_task_queue`. That excludes the broad candidate scan,
/// which reads both tables in one `NOT EXISTS` statement. It also
/// excludes every row-lock or write statement against
/// `harvest_task_queue` alone. What remains is exactly
/// `worker_still_dead`'s dedicated round trip.
fn is_worker_recheck_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("harvest_workers") && !q.contains("harvest_task_queue")
}

/// The one-time broad scan (`orphaned_running_tasks_query`): a `SELECT`
/// against `harvest_task_queue` carrying a `NOT EXISTS` against
/// `harvest_workers`. Distinguished from `requeue_orphan_stmt`'s combined
/// `UPDATE` (below) by statement kind, not just the tables it touches.
/// Both are `harvest_task_queue` plus `NOT EXISTS`, so a filter that
/// ignored `SELECT` vs `UPDATE` would silently merge them. That was this
/// harness's own bug on its first version. `requeue_orphan_stmt` also
/// matched this predicate, so `candidate_scan_calls` read `n + 1` instead
/// of the true, constant `1`. The printed *total* across all buckets was
/// still correct by construction: every statement fell into exactly one
/// of the buckets that existed then.
fn is_candidate_scan_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    let q = q.trim_start();
    q.starts_with("select") && q.contains("harvest_task_queue") && q.contains("not exists")
}

/// `requeue_orphan_stmt`: the combined per-row `UPDATE` this
/// investigation adds, folding the liveness re-check into the write.
/// Matches the same two substrings [`is_candidate_scan_statement`] does,
/// so distinguishing them by statement kind (`UPDATE` vs `SELECT`) is
/// what keeps the two buckets disjoint.
fn is_combined_update_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    let q = q.trim_start();
    q.starts_with("update") && q.contains("harvest_task_queue") && q.contains("not exists")
}

/// The row-lock-only `SELECT ... FOR UPDATE` every `requeue_orphan` call
/// still issues first. Touches `harvest_task_queue` alone, with no
/// `NOT EXISTS`, so it cannot be confused with either statement above.
fn is_row_lock_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("harvest_task_queue") && !q.contains("not exists")
}

struct SizePoint {
    n: i64,
    worker_recheck_calls: i64,
    worker_recheck_buffers: i64,
    candidate_scan_calls: i64,
    combined_update_calls: i64,
    combined_update_buffers: i64,
    row_lock_calls: i64,
    row_lock_buffers: i64,
}

async fn measure_one_pass(admin: &str, label: &str, n: i64, num_workers: i64) -> SizePoint {
    let db_name = unique(&format!("poison_pill_reclaim_perf_{label}_{n}"));
    let url = create_fresh_db(admin, &db_name).await;

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;
    seed_fixture(&mut seed_conn, n, num_workers).await;

    // The one real public entry point: the exact function every worker's
    // periodic timeout tick calls (`timeout::enforce_timeouts_once`'s call
    // site), driven directly against a real Postgres.
    let mut pass_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("pass connection");

    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    let metrics = NoopMetrics;
    // Threshold far above any crash_strikes value seeded here (1). Every
    // orphan takes the `Requeue` path this investigation targets -- none
    // is quarantined, keeping the measurement isolated to one code path.
    let summary = reclaim_orphaned_tasks(
        &mut pass_conn,
        1_000_000,
        10,
        None,
        &metrics,
        &PayloadCodecs::default(),
    )
    .await
    .expect("reclaim_orphaned_tasks should succeed");
    assert_eq!(
        i64::try_from(summary.requeued).unwrap(),
        n,
        "every seeded orphan (all crashed, all below the quarantine threshold) \
         must be re-queued -- exactly once"
    );
    assert_eq!(summary.quarantined, 0, "threshold is set unreachably high");

    let all_rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let worker_recheck_calls: i64 = all_rows
        .iter()
        .filter(|r| is_worker_recheck_statement(r))
        .map(|r| r.calls)
        .sum();
    let worker_recheck_buffers: i64 = all_rows
        .iter()
        .filter(|r| is_worker_recheck_statement(r))
        .map(|r| r.total_buffers)
        .sum();
    let candidate_scan_calls: i64 = all_rows
        .iter()
        .filter(|r| is_candidate_scan_statement(r))
        .map(|r| r.calls)
        .sum();
    let combined_update_calls: i64 = all_rows
        .iter()
        .filter(|r| is_combined_update_statement(r))
        .map(|r| r.calls)
        .sum();
    let combined_update_buffers: i64 = all_rows
        .iter()
        .filter(|r| is_combined_update_statement(r))
        .map(|r| r.total_buffers)
        .sum();
    let row_lock_calls: i64 = all_rows
        .iter()
        .filter(|r| is_row_lock_statement(r))
        .map(|r| r.calls)
        .sum();
    let row_lock_buffers: i64 = all_rows
        .iter()
        .filter(|r| is_row_lock_statement(r))
        .map(|r| r.total_buffers)
        .sum();

    assert!(
        worker_recheck_calls > 0 || candidate_scan_calls > 0 || combined_update_calls > 0,
        "pg_stat_statements returned zero rows matching the expected shapes -- check \
         pg_stat_statements.track and shared_preload_libraries",
    );
    assert_eq!(
        candidate_scan_calls, 1,
        "the broad candidate scan runs exactly once per tick, regardless of n -- a count \
         other than 1 here means is_candidate_scan_statement is (again) conflating it with \
         the combined per-row UPDATE"
    );

    SizePoint {
        n,
        worker_recheck_calls,
        worker_recheck_buffers,
        candidate_scan_calls,
        combined_update_calls,
        combined_update_buffers,
        row_lock_calls,
        row_lock_buffers,
    }
}

// ── Evidence capture (not a CI assertion) ───────────────────────────────────

/// Fixed at 3 distinct dead workers across every sweep point. The table
/// then demonstrates the call-count shape's insensitivity to worker
/// cardinality directly. If the recheck were already O(distinct
/// workers), this column would stay flat near 3 as `n` grows. It does
/// not.
const NUM_DISTINCT_WORKERS: i64 = 3;

/// Sweeps three fixture sizes so the artifact demonstrates the call-count
/// shape directly, not just one point on the curve. Run once against the
/// pre-fix code (`PERF_LABEL=before`), once against the post-fix code
/// (`PERF_LABEL=after`).
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-poison-pill-orphan-recheck.md"]
async fn zz_capture_poison_pill_reclaim_perf_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("poison-pill-orphan-recheck");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut lines = vec![format!(
        "-- {label}: reclaim_orphaned_tasks, pg_stat_statements sweep \
         (num_distinct_dead_workers={NUM_DISTINCT_WORKERS}) --\n\
         n\tworker_recheck_calls\tworker_recheck_buffers\tcandidate_scan_calls\t\
         combined_update_calls\tcombined_update_buffers\trow_lock_calls\trow_lock_buffers"
    )];
    for n in [60_i64, 300, 1_500] {
        let point = measure_one_pass(&admin, &label, n, NUM_DISTINCT_WORKERS).await;
        eprintln!(
            "label={label} n={} worker_recheck_calls={} worker_recheck_buffers={} \
             candidate_scan_calls={} combined_update_calls={} combined_update_buffers={} \
             row_lock_calls={} row_lock_buffers={}",
            point.n,
            point.worker_recheck_calls,
            point.worker_recheck_buffers,
            point.candidate_scan_calls,
            point.combined_update_calls,
            point.combined_update_buffers,
            point.row_lock_calls,
            point.row_lock_buffers,
        );
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            point.n,
            point.worker_recheck_calls,
            point.worker_recheck_buffers,
            point.candidate_scan_calls,
            point.combined_update_calls,
            point.combined_update_buffers,
            point.row_lock_calls,
            point.row_lock_buffers,
        ));
    }
    std::fs::write(
        out_dir.join(format!("{label}-sweep.txt")),
        lines.join("\n") + "\n",
    )
    .expect("write sweep artifact");
    eprintln!("evidence capture complete: label={label}");
}

// ── Correctness: folding the liveness re-check into one statement must not ─
// ── change what gets reclaimed, or how ──────────────────────────────────────

#[derive(diesel::QueryableByName, Debug, PartialEq, Eq)]
struct TaskRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    worker_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    crash_strikes: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    last_heartbeat_at: Option<chrono::DateTime<chrono::Utc>>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    error: Option<String>,
}

async fn task_row(conn: &mut AsyncPgConnection, id: Uuid) -> TaskRow {
    diesel::sql_query(
        "SELECT state, worker_id, crash_strikes, last_heartbeat_at, error \
         FROM harvest_task_queue WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result(conn)
    .await
    .expect("load task row")
}

async fn dead_letter_count_for(conn: &mut AsyncPgConnection, original_task_id: Uuid) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_dead_letters WHERE original_task_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(original_task_id)
    .get_result::<CountRow>(conn)
    .await
    .expect("count dead letters")
    .count
}

/// Exercises the same three cases `poison_pill_tests.rs` covers end to
/// end. A dead-worker orphan below the quarantine threshold, one at it,
/// and a live-worker task that must be left alone. `poison_pill_tests.rs`
/// itself needs a Docker daemon (`testcontainers`) and is not duplicated
/// here. This is the same semantics, driven through the
/// `HARVEST_TEST_DATABASE_URL` harness this file already uses, so it
/// also runs where Docker is unavailable.
#[tokio::test]
async fn requeue_and_quarantine_semantics_are_unchanged_by_the_combined_statement() {
    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("poison_pill_reclaim_equiv")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let dead_worker = "poison_pill_equiv_dead_worker";
    let live_worker = "poison_pill_equiv_live_worker";
    let requeue_id = Uuid::new_v4();
    let quarantine_id = Uuid::new_v4();
    let live_id = Uuid::new_v4();
    let quarantine_threshold = 3_i32;

    conn.batch_execute(&format!(
        "INSERT INTO harvest_workers (worker_id, last_heartbeat_at, max_concurrency, host)
         VALUES ('{live_worker}', NOW(), 10, 'localhost');

         INSERT INTO harvest_task_queue (
             id, queue_name, task_type, workflow_exec_id, input, state, worker_id,
             attempt, max_attempts, started_at, last_heartbeat_at, crash_strikes, error
         ) VALUES
             ('{requeue_id}', 'default', 'activity', NULL, '{{}}'::jsonb, 'RUNNING',
              '{dead_worker}', 1, 3, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
              0, 'stale error from a previous clean failure'),
             ('{quarantine_id}', 'default', 'activity', NULL, '{{}}'::jsonb, 'RUNNING',
              '{dead_worker}', 1, 3, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
              {}, NULL),
             ('{live_id}', 'default', 'activity', NULL, '{{}}'::jsonb, 'RUNNING',
              '{live_worker}', 1, 3, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
              0, NULL);",
        quarantine_threshold - 1,
    ))
    .await
    .expect("seed equivalence fixture");

    let metrics = NoopMetrics;
    let summary = reclaim_orphaned_tasks(
        &mut conn,
        quarantine_threshold,
        10,
        None,
        &metrics,
        &PayloadCodecs::default(),
    )
    .await
    .expect("reclaim_orphaned_tasks should succeed");

    assert_eq!(summary.requeued, 1, "exactly the below-threshold orphan");
    assert_eq!(summary.quarantined, 1, "exactly the at-threshold orphan");

    let requeued = task_row(&mut conn, requeue_id).await;
    assert_eq!(requeued.state, "PENDING");
    assert_eq!(requeued.worker_id, None, "dead worker's claim cleared");
    assert_eq!(requeued.crash_strikes, 1);
    assert_eq!(
        requeued.last_heartbeat_at, None,
        "stale heartbeat cleared on requeue"
    );
    assert_eq!(
        requeued.error, None,
        "stale error from the dead attempt cleared on requeue"
    );

    let quarantined = task_row(&mut conn, quarantine_id).await;
    assert_eq!(quarantined.state, "FAILED");
    assert_eq!(quarantined.crash_strikes, quarantine_threshold);
    assert!(
        quarantined.error.is_some(),
        "quarantine records a reason on the row"
    );
    assert_eq!(
        dead_letter_count_for(&mut conn, quarantine_id).await,
        1,
        "quarantine writes exactly one dead-letter entry"
    );

    let live = task_row(&mut conn, live_id).await;
    assert_eq!(
        live.state, "RUNNING",
        "a task whose worker is still heartbeating must never be reclaimed"
    );
    assert_eq!(live.crash_strikes, 0);
}
