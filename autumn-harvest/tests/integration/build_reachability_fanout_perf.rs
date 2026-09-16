#![cfg(feature = "db")]
//! Evidence for `build_routing::all_build_reachability`'s per-build fan-out.
//!
//! `all_build_reachability` backs the Vantage UI Builds page and its
//! `GET /admin/builds` twin (`ui.rs`, `api.rs`). Before the fix this page
//! documents, it looped over every distinct build id known to the fleet and
//! called `build_reachability` once per id. Each call re-scanned
//! `harvest_workflow_executions`, `harvest_task_queue`, and
//! `harvest_workers` with a fresh `WHERE build_id = $1` predicate.
//! `harvest_workers` carries no index on `build_id` at all, so its two
//! per-build subqueries were a full sequential scan on every iteration.
//!
//! `build_reachability` (the per-build helper) is unchanged by this fix.
//! It stays a correct, simple building block for its own single-build
//! callers (the CLI, single-build API routes). It also gives this file a
//! stable "before" reproduction. Looping it once per distinct build id, here
//! in the test, is exactly what the pre-fix `all_build_reachability` did.
//! So this file reproduces the pre-fix cost regardless of which commit is
//! checked out.
//!
//! Two tests:
//! - [`all_build_reachability_agrees_with_the_per_build_helper`] -- fast,
//!   always-run correctness check. It ties the batched function's output to
//!   the trusted per-build helper's output, row by row. The fixture covers a
//!   build present in only one of the three source tables.
//! - [`zz_capture_build_reachability_fanout_evidence`] -- `#[ignore]`d. It
//!   seeds a production-shaped fixture into a throwaway database. It then
//!   captures a `pg_stat_statements` snapshot for two strategies: the "loop
//!   over `build_reachability`" reproduction ("before"), and the real,
//!   shipped `all_build_reachability` ("after"). It also compares the two
//!   result sets for equivalence.

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use autumn_harvest::build_routing::{all_build_reachability, build_reachability};

use crate::integration_e2e::setup_test_database_url_or_env;

const STALE_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Fast, always-run correctness check
// ---------------------------------------------------------------------------

async fn seed_execution(
    conn: &mut AsyncPgConnection,
    build_id: &str,
    state: &str,
    workflow_id: &str,
) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (workflow_name, workflow_id, shard_id, state, input, queue_name, assigned_build_id) \
         VALUES ('fanout_wf', '{workflow_id}', 0, '{state}', '{{}}'::jsonb, 'default', '{build_id}')",
    ))
    .await
    .expect("seed execution");
}

async fn seed_task(conn: &mut AsyncPgConnection, build_id: &str, state: &str) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_task_queue (queue_name, task_type, input, state, required_build_id) \
         VALUES ('default', 'activity', '{{}}'::jsonb, '{state}', '{build_id}')",
    ))
    .await
    .expect("seed task");
}

async fn seed_worker(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    build_id: &str,
    status: &str,
    heartbeat_age: chrono::Duration,
) {
    let heartbeat_at = chrono::Utc::now() - heartbeat_age;
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workers \
             (worker_id, max_concurrency, host, build_id, status, last_heartbeat_at) \
         VALUES ('{worker_id}', 4, 'h', '{build_id}', '{status}', '{}')",
        heartbeat_at.to_rfc3339(),
    ))
    .await
    .expect("seed worker");
}

/// The batched function must report exactly what the trusted per-build
/// helper reports, for every build the fixture touches. That includes a
/// build present in only one of the three source tables. The batched
/// function's three independent lookups must each default to zero for it,
/// rather than dropping the build entirely.
#[tokio::test]
async fn all_build_reachability_agrees_with_the_per_build_helper() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let build_open = format!("fanout_open_{run_id}");
    let build_retired = format!("fanout_retired_{run_id}");
    let build_worker_only = format!("fanout_worker_only_{run_id}");

    // build_open: one open execution, one pending task, one fresh active worker.
    seed_execution(
        &mut conn,
        &build_open,
        "RUNNING",
        &format!("{build_open}_e"),
    )
    .await;
    seed_task(&mut conn, &build_open, "PENDING").await;
    seed_worker(
        &mut conn,
        &format!("w_{build_open}"),
        &build_open,
        "Active",
        chrono::Duration::seconds(5),
    )
    .await;

    // build_retired: only a terminal execution and a stale worker, no pending
    // tasks -- exercises `safe_to_retire` and the stale-worker branch.
    seed_execution(
        &mut conn,
        &build_retired,
        "COMPLETED",
        &format!("{build_retired}_e"),
    )
    .await;
    seed_worker(
        &mut conn,
        &format!("w_{build_retired}"),
        &build_retired,
        "Active",
        chrono::Duration::seconds(600),
    )
    .await;

    // build_worker_only: appears in harvest_workers alone.
    seed_worker(
        &mut conn,
        &format!("w_{build_worker_only}"),
        &build_worker_only,
        "Active",
        chrono::Duration::seconds(1),
    )
    .await;

    conn.batch_execute(
        "ANALYZE harvest_workflow_executions; ANALYZE harvest_task_queue; \
         ANALYZE harvest_workers;",
    )
    .await
    .expect("analyze");

    let all = all_build_reachability(&mut conn, STALE_THRESHOLD)
        .await
        .expect("all_build_reachability");

    for build_id in [&build_open, &build_retired, &build_worker_only] {
        let batched = all
            .iter()
            .find(|r| &r.build_id == build_id)
            .unwrap_or_else(|| panic!("{build_id} missing from the batched result"));
        let direct = build_reachability(&mut conn, build_id, STALE_THRESHOLD)
            .await
            .expect("build_reachability");
        assert_eq!(
            batched.open_executions, direct.open_executions,
            "open_executions mismatch for {build_id}"
        );
        assert_eq!(
            batched.pending_tasks, direct.pending_tasks,
            "pending_tasks mismatch for {build_id}"
        );
        assert_eq!(
            batched.active_workers, direct.active_workers,
            "active_workers mismatch for {build_id}"
        );
        assert_eq!(
            batched.stale_workers, direct.stale_workers,
            "stale_workers mismatch for {build_id}"
        );
        assert_eq!(
            batched.safe_to_retire, direct.safe_to_retire,
            "safe_to_retire mismatch for {build_id}"
        );
    }
}

// ---------------------------------------------------------------------------
// Evidence capture: pg_stat_statements before/after
// ---------------------------------------------------------------------------

/// Distinct build ids the fixture below produces.
///
/// Mirrors `build_routing::all_build_ids_query`'s catalog query, duplicated
/// here. This file then measures the same catalog scan regardless of which
/// commit -- pre- or post-fix -- is checked out.
const DISTINCT_BUILD_IDS_QUERY: &str = "SELECT DISTINCT build_id FROM ( \
         SELECT assigned_build_id AS build_id FROM harvest_workflow_executions \
         WHERE assigned_build_id IS NOT NULL AND assigned_build_id <> '' \
         UNION \
         SELECT required_build_id AS build_id FROM harvest_task_queue \
         WHERE required_build_id IS NOT NULL AND required_build_id <> '' \
         UNION \
         SELECT build_id FROM harvest_workers WHERE build_id IS NOT NULL AND build_id <> '' \
     ) sub ORDER BY build_id";

#[derive(QueryableByName)]
struct BuildIdRow {
    #[diesel(sql_type = Text)]
    build_id: String,
}

async fn distinct_build_ids(conn: &mut AsyncPgConnection) -> Vec<String> {
    let rows: Vec<BuildIdRow> = diesel::sql_query(DISTINCT_BUILD_IDS_QUERY)
        .load(conn)
        .await
        .expect("distinct build ids");
    rows.into_iter().map(|r| r.build_id).collect()
}

/// 50 distinct build ids, 60,000 executions, 30,000 queued tasks, and 3,000
/// workers -- a long-lived fleet's worth of deploy history, not a toy input.
/// Every count is a deterministic function of the row index, not
/// `random()`. The fixture is therefore byte-identical on every run, in
/// every commit. The "before" and "after" result sets below are a genuine
/// equivalence check, not two different random draws.
const N_BUILDS: i64 = 50;
const N_EXECUTIONS: i64 = 60_000;
const N_TASKS: i64 = 30_000;
const N_WORKERS: i64 = 3_000;

async fn seed_production_shaped_fixture(conn: &mut AsyncPgConnection) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (workflow_name, workflow_id, shard_id, state, input, queue_name, \
              assigned_build_id, started_at, created_at)
         SELECT
             'build_fanout_wf',
             'build_fanout_exec_' || gs::text,
             0,
             -- Cycles through the 10-slot state array once per build's own
             -- slot index (gs/{N_BUILDS}), not once per row -- so every
             -- build gets the same even mix of states instead of one state
             -- each, which {N_BUILDS} rows-per-build-divides-evenly-by-10
             -- would otherwise collapse to.
             (ARRAY['RUNNING','PAUSED','COMPLETED','COMPLETED','COMPLETED', \
                    'COMPLETED','FAILED','FAILED','CANCELLED','COMPLETED'])
                 [1 + ((gs / {N_BUILDS}) % 10)],
             '{{}}'::jsonb,
             'default',
             'build-' || lpad((gs % {N_BUILDS})::text, 4, '0'),
             NOW() - (((gs / {N_BUILDS}) % 180) * interval '1 day'),
             NOW() - (((gs / {N_BUILDS}) % 180) * interval '1 day')
         FROM generate_series(1, {N_EXECUTIONS}) AS gs;

         INSERT INTO harvest_task_queue (queue_name, task_type, input, state, required_build_id)
         SELECT
             'default', 'activity', '{{}}'::jsonb,
             CASE WHEN (gs / {N_BUILDS}) % 3 = 0 THEN 'PENDING' ELSE 'RUNNING' END,
             'build-' || lpad((gs % {N_BUILDS})::text, 4, '0')
         FROM generate_series(1, {N_TASKS}) AS gs;

         INSERT INTO harvest_workers
             (worker_id, max_concurrency, host, build_id, status, last_heartbeat_at)
         SELECT
             'build_fanout_worker_' || gs::text,
             4,
             'host-' || (gs % 20)::text,
             'build-' || lpad((gs % {N_BUILDS})::text, 4, '0'),
             CASE WHEN (gs / {N_BUILDS}) % 7 = 0 THEN 'Draining' ELSE 'Active' END,
             CASE
                 WHEN (gs / {N_BUILDS}) % 11 < 8
                     THEN NOW() - (((gs / {N_BUILDS}) % 20) * interval '1 second')
                 ELSE NOW() - (interval '10 minutes' \
                               + ((gs / {N_BUILDS}) % 100) * interval '1 second')
             END
         FROM generate_series(1, {N_WORKERS}) AS gs;

         ANALYZE harvest_workflow_executions;
         ANALYZE harvest_task_queue;
         ANALYZE harvest_workers;"
    ))
    .await
    .expect("seed production-shaped build-reachability fixture");
}

#[derive(QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = Text)]
    query: String,
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = BigInt)]
    total_buffers: i64,
}

/// One line per distinct build, sorted, so the file diffs cleanly between
/// "before" and "after" and a mismatch points straight at the build id.
fn format_result_rows(rows: &[(String, i64, i64, i64, i64, bool)]) -> String {
    use std::fmt::Write as _;

    let mut sorted = rows.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = format!("rows={}\n", sorted.len());
    for (build_id, open, pending, active, stale, safe) in sorted {
        let _ = writeln!(
            out,
            "{build_id} open={open} pending={pending} active={active} stale={stale} safe={safe}"
        );
    }
    out
}

async fn reset_pg_stat_statements(conn: &mut AsyncPgConnection) {
    // Scoped to this run's own database, not the bare zero-argument form.
    // The bare form would reset statistics for every database on a shared
    // cluster (see docs/performance-usage-report-activity-lookback.md for
    // the same reasoning this harness copies).
    diesel::sql_query(
        "SELECT pg_stat_statements_reset(0, \
         (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
    )
    .execute(conn)
    .await
    .expect("pg_stat_statements_reset failed");
}

async fn capture_pg_stat_statements(conn: &mut AsyncPgConnection) -> Vec<StatRow> {
    let stats: Vec<StatRow> = diesel::sql_query(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
         (shared_blks_hit + shared_blks_read) AS total_buffers \
         FROM pg_stat_statements \
         WHERE query LIKE '%build_id%' \
           AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         ORDER BY total_buffers DESC LIMIT 10",
    )
    .load(conn)
    .await
    .expect("pg_stat_statements query failed");
    assert!(
        !stats.is_empty(),
        "pg_stat_statements returned no matching rows -- the LIKE filter or \
         the reset above is not scoped correctly"
    );
    stats
}

/// One build's reachability counters, flattened to a plain tuple.
///
/// The "before" and "after" strategies below build the same shape from two
/// different sources. A shared tuple lets them share one comparison and one
/// formatter.
type ReachRow = (String, i64, i64, i64, i64, bool);

fn reach_tuple(r: autumn_harvest::build_routing::BuildReachability) -> ReachRow {
    (
        r.build_id,
        r.open_executions,
        r.pending_tasks,
        r.active_workers,
        r.stale_workers,
        r.safe_to_retire,
    )
}

fn write_capture_artifacts(
    out_dir: &std::path::Path,
    label: &str,
    header: &str,
    stats: &[StatRow],
    rows: &[ReachRow],
) {
    std::fs::write(
        out_dir.join(format!("{label}.pg_stat_statements.txt")),
        format!("{header}{stats:#?}\n"),
    )
    .unwrap_or_else(|e| panic!("write {label} pg_stat_statements artifact: {e}"));
    std::fs::write(
        out_dir.join(format!("{label}.result-rows.txt")),
        format_result_rows(rows),
    )
    .unwrap_or_else(|e| panic!("write {label} result-rows artifact: {e}"));
}

/// "Before": look up the catalog, then loop `build_reachability` once per
/// distinct build id -- exactly what the pre-fix `all_build_reachability`
/// did internally. The catalog lookup runs inside this reset/capture window.
/// It therefore carries the same one-time catalog cost `capture_after`
/// incurs internally, so the comparison between the two isolates the
/// per-build loop.
async fn capture_before(conn: &mut AsyncPgConnection, out_dir: &std::path::Path) -> Vec<ReachRow> {
    reset_pg_stat_statements(conn).await;
    let build_ids = distinct_build_ids(conn).await;
    let mut rows = Vec::with_capacity(build_ids.len());
    for build_id in &build_ids {
        let r = build_reachability(conn, build_id, STALE_THRESHOLD)
            .await
            .expect("build_reachability");
        rows.push(reach_tuple(r));
    }
    let stats = capture_pg_stat_statements(conn).await;
    write_capture_artifacts(
        out_dir,
        "before",
        &format!(
            "-- before: build_reachability() looped once per distinct build id \
             (N={}) --\n",
            build_ids.len()
        ),
        &stats,
        &rows,
    );
    rows
}

/// "After": the real, shipped `all_build_reachability`.
async fn capture_after(conn: &mut AsyncPgConnection, out_dir: &std::path::Path) -> Vec<ReachRow> {
    reset_pg_stat_statements(conn).await;
    let rows: Vec<ReachRow> = all_build_reachability(conn, STALE_THRESHOLD)
        .await
        .expect("all_build_reachability")
        .into_iter()
        .map(reach_tuple)
        .collect();
    let stats = capture_pg_stat_statements(conn).await;
    write_capture_artifacts(
        out_dir,
        "after",
        &format!(
            "-- after: all_build_reachability() (N={} distinct builds) --\n",
            rows.len()
        ),
        &stats,
        &rows,
    );
    rows
}

/// Regenerates `docs/perf-artifacts/build-reachability-fanout/`. `#[ignore]`d
/// -- seeds tens of thousands of rows and takes real time. Needs
/// `HARVEST_TEST_DATABASE_URL` (an admin connection string) or a reachable
/// Docker daemon for `claim_bench_support::db::setup_bench_db`'s
/// testcontainer fallback.
#[tokio::test]
#[ignore = "seeds a production-shaped build-reachability fixture; run explicitly"]
async fn zz_capture_build_reachability_fanout_evidence() {
    use super::claim_bench_support::db;

    let bench = match db::setup_bench_db().await {
        Ok(b) => b,
        Err(reason) => {
            eprintln!("no database reachable; nothing captured: {}", reason.0);
            return;
        }
    };

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("build-reachability-fanout");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut conn = db::connect(&bench.url).await;
    diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await
        .ok();
    seed_production_shaped_fixture(&mut conn).await;

    let expected_builds = usize::try_from(N_BUILDS).expect("N_BUILDS fits usize");
    assert_eq!(
        distinct_build_ids(&mut conn).await.len(),
        expected_builds,
        "fixture must produce exactly N_BUILDS distinct build ids"
    );

    let mut before_rows = capture_before(&mut conn, &out_dir).await;
    let mut after_rows = capture_after(&mut conn, &out_dir).await;

    before_rows.sort_by(|a, b| a.0.cmp(&b.0));
    after_rows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        before_rows, after_rows,
        "before (looped build_reachability) and after (all_build_reachability) \
         must report byte-identical counters for every build"
    );

    eprintln!("equivalence confirmed: {expected_builds} builds agree across before and after");
    eprintln!("== done. Artifacts in {} ==", out_dir.display());
}
