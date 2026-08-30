#![cfg(feature = "db")]
//! Correctness and buffer-cost evidence for
//! `timeout::workflow_history_ceiling_query()` (issue #493) -- the scanner
//! query behind `enforce_workflow_history_ceiling`, run on every timeout-
//! scanner tick (`enforce_timeouts_once`) whenever an operator configures
//! `HarvestBuilder::max_workflow_history_events`.
//!
//! The query needs each RUNNING execution's durable `harvest_events` row
//! count in two places -- the `SELECT` list (to report it) and the `WHERE`
//! clause (to filter on it, since a `SELECT`-list alias is not visible to the
//! `WHERE` clause of the same query). It currently gets there by writing the
//! correlated `COUNT(*)` subquery out twice, which Postgres evaluates
//! independently at each site -- so every RUNNING row pays for the count
//! twice. This commit adds only the harness and the baseline evidence for
//! that shape, no fix -- see `docs/performance-history-ceiling.md` (added
//! alongside the fix in a follow-up commit) for the full writeup.
//!
//! Two tests:
//! - [`enforce_workflow_history_ceiling_terminates_only_oversized_running_rows`]
//!   -- fast, always-run functional correctness check against the real
//!   scanner function and the shared test database (state transitions only,
//!   filtered to this test's own uniquely-named rows -- the scanner is a
//!   GLOBAL scan, exactly like `enforce_workflow_execution_timeouts` in
//!   `chain_timeout_tests.rs`).
//! - [`zz_capture_history_ceiling_claim_evidence`] -- `#[ignore]`d. Seeds a
//!   production-shaped fixture into a throwaway database
//!   ([`claim_bench_support::db::setup_bench_db`]), then captures
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` and a
//!   `pg_stat_statements` snapshot for the current (pre-fix)
//!   `workflow_history_ceiling_query()`.

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Nullable, Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::timeout::{enforce_workflow_history_ceiling, workflow_history_ceiling_query};
use autumn_harvest::types::ExecutionId;

use crate::integration_e2e::{insert_workflow_execution_with_id, setup_test_database_url_or_env};

#[derive(QueryableByName, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct OversizedRow {
    #[diesel(sql_type = SqlUuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = BigInt)]
    event_count: i64,
}

/// Bulk-insert `n` events for `exec_id`, bypassing `store::append_single_event`'s
/// per-row locked round trip -- this file seeds up to hundreds of thousands of
/// events per fixture, and only the *count* matters, not durable-log realism.
async fn seed_events(conn: &mut AsyncPgConnection, exec_id: ExecutionId, n: i64) {
    if n == 0 {
        return;
    }
    conn.batch_execute(&format!(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data) \
         SELECT '{}'::uuid, gs - 1, 'ActivityCompleted', '{{}}'::jsonb \
         FROM generate_series(1, {n}) AS gs",
        exec_id.as_uuid(),
    ))
    .await
    .expect("bulk-seed events");
}

// ---------------------------------------------------------------------------
// Fast, always-run correctness check
// ---------------------------------------------------------------------------

/// Exercises the real `enforce_workflow_history_ceiling` end to end: a RUNNING
/// row over the ceiling must FAIL with the `history_ceiling_exceeded` message,
/// a RUNNING row exactly AT the ceiling must also FAIL (`>=`, not `>`), a
/// RUNNING row one event short must stay RUNNING untouched, and a COMPLETED
/// row with more events than the ceiling must never be touched at all (the
/// query filters on `state = 'RUNNING'` before it ever counts events).
///
/// The ceiling (750) and ceiling-straddling event counts are chosen well
/// above what any other test in this shared-database suite plausibly seeds,
/// so this test's assertions -- scoped to its own uniquely-named rows, since
/// the scanner performs a GLOBAL scan exactly like
/// `enforce_workflow_execution_timeouts` (see `chain_timeout_tests.rs`) --
/// cannot be tripped by unrelated concurrent tests, and this test cannot trip
/// theirs.
const FUNCTIONAL_TEST_CEILING: i64 = 750;

#[derive(QueryableByName)]
struct StateRow {
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Nullable<Text>)]
    error: Option<String>,
}

async fn load_state(conn: &mut AsyncPgConnection, id: ExecutionId) -> StateRow {
    diesel::sql_query("SELECT state, error FROM harvest_workflow_executions WHERE id = $1")
        .bind::<SqlUuid, _>(id.as_uuid())
        .get_result::<StateRow>(conn)
        .await
        .expect("reload execution")
}

#[tokio::test]
async fn enforce_workflow_history_ceiling_terminates_only_oversized_running_rows() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect to test database");

    let suffix = uuid::Uuid::new_v4().simple().to_string();

    let over_id =
        insert_workflow_execution_with_id(&mut conn, &format!("hist-ceiling-over-{suffix}")).await;
    seed_events(&mut conn, over_id, FUNCTIONAL_TEST_CEILING + 50).await;

    let at_id =
        insert_workflow_execution_with_id(&mut conn, &format!("hist-ceiling-at-{suffix}")).await;
    seed_events(&mut conn, at_id, FUNCTIONAL_TEST_CEILING).await;

    let under_id =
        insert_workflow_execution_with_id(&mut conn, &format!("hist-ceiling-under-{suffix}")).await;
    seed_events(&mut conn, under_id, FUNCTIONAL_TEST_CEILING - 1).await;

    let completed_id =
        insert_workflow_execution_with_id(&mut conn, &format!("hist-ceiling-completed-{suffix}"))
            .await;
    seed_events(&mut conn, completed_id, FUNCTIONAL_TEST_CEILING + 50).await;
    diesel::sql_query("UPDATE harvest_workflow_executions SET state = 'COMPLETED' WHERE id = $1")
        .bind::<SqlUuid, _>(completed_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("mark control row COMPLETED");

    enforce_workflow_history_ceiling(&mut conn, FUNCTIONAL_TEST_CEILING as u64, &NoOpMetrics)
        .await
        .expect("scanner run");

    let over = load_state(&mut conn, over_id).await;
    assert_eq!(over.state, "FAILED", "over-ceiling RUNNING row must FAIL");
    assert!(
        over.error
            .as_deref()
            .is_some_and(|e| e.contains("history_ceiling_exceeded")),
        "got error: {:?}",
        over.error
    );

    let at = load_state(&mut conn, at_id).await;
    assert_eq!(
        at.state, "FAILED",
        "exactly-at-ceiling RUNNING row must FAIL ('>=', not '>')"
    );

    let under = load_state(&mut conn, under_id).await;
    assert_eq!(
        under.state, "RUNNING",
        "one-under-ceiling RUNNING row must be left alone"
    );

    let completed = load_state(&mut conn, completed_id).await;
    assert_eq!(
        completed.state, "COMPLETED",
        "a non-RUNNING row must never be touched, however many events it has"
    );
}

// ---------------------------------------------------------------------------
// Evidence capture: EXPLAIN + pg_stat_statements baseline
// ---------------------------------------------------------------------------

/// Seed a production-shaped fixture into `conn`'s (already-migrated,
/// already-empty) database: 100,000 workflow executions, 3,000 (3%) of them
/// RUNNING -- a plausible steady-state fraction for a busy fleet where most
/// work finishes quickly -- and roughly 4,000,000 `harvest_events` rows
/// spread across them with a skewed, per-state event-count distribution.
///
/// RUNNING rows get a heavier tail than non-RUNNING ones (10% land in a
/// 1,000-8,000 event band that straddles the 5,000-event `CEILING` below):
/// still-running executions over-represent hung/looping workflows -- exactly
/// the population this scanner exists to catch. Non-RUNNING rows get a much
/// smaller heavy tail (1%, capped at 3,000) since most of them already
/// finished normally.
///
/// Pure set-based SQL (`generate_series` + `LATERAL`), not a per-row Rust
/// loop: seeding ~4M events row-by-row from the client would dominate this
/// test's own wall clock and says nothing about the query under test.
const FIXTURE_TOTAL_EXECUTIONS: i64 = 100_000;
const FIXTURE_RUNNING_EXECUTIONS: i64 = 3_000;
const CEILING: i64 = 5_000;

async fn seed_production_shaped_fixture(conn: &mut AsyncPgConnection) {
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE tmp_hist_exec (id UUID, state TEXT, event_count INT);

         INSERT INTO tmp_hist_exec (id, state, event_count)
         SELECT
             gen_random_uuid(),
             (ARRAY['COMPLETED','COMPLETED','COMPLETED','COMPLETED','COMPLETED','COMPLETED',
                    'COMPLETED','COMPLETED','FAILED','FAILED','CANCELLED','TIMED_OUT'])
                 [1 + floor(random()*12)::int],
             CASE
                 WHEN random() < 0.90 THEN 5 + floor(random()*26)::int
                 WHEN random() < 0.99 THEN 50 + floor(random()*251)::int
                 ELSE 1000 + floor(random()*2001)::int
             END
         FROM generate_series(1, {non_running});

         INSERT INTO tmp_hist_exec (id, state, event_count)
         SELECT
             gen_random_uuid(),
             'RUNNING',
             CASE
                 WHEN random() < 0.70 THEN 5 + floor(random()*26)::int
                 WHEN random() < 0.90 THEN 50 + floor(random()*251)::int
                 ELSE 1000 + floor(random()*7001)::int
             END
         FROM generate_series(1, {running});

         INSERT INTO harvest_workflow_executions
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name,
              started_at, created_at)
         SELECT
             id,
             'hist_ceiling_bench_wf_' || (1 + floor(random()*40))::text,
             'hist_ceiling_bench_' || id::text,
             gen_random_uuid(),
             0,
             state,
             '{{}}'::jsonb,
             'default',
             NOW() - (random() * interval '30 days'),
             NOW() - (random() * interval '30 days')
         FROM tmp_hist_exec;

         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT
             t.id,
             gs.n - 1,
             (ARRAY['ActivityScheduled','ActivityCompleted','TimerFired','SignalReceived'])
                 [1 + floor(random()*4)::int],
             '{{}}'::jsonb,
             NOW() - (random() * interval '30 days')
         FROM tmp_hist_exec t
         CROSS JOIN LATERAL generate_series(1, t.event_count) AS gs(n);

         ANALYZE harvest_workflow_executions;
         ANALYZE harvest_events;",
        non_running = FIXTURE_TOTAL_EXECUTIONS - FIXTURE_RUNNING_EXECUTIONS,
        running = FIXTURE_RUNNING_EXECUTIONS,
    ))
    .await
    .expect("seed production-shaped history-ceiling fixture");
}

#[derive(QueryableByName)]
struct ExplainRow {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    query_plan: String,
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

async fn explain_and_result_set(
    conn: &mut AsyncPgConnection,
    sql: &str,
) -> (String, Vec<OversizedRow>) {
    let explained: Vec<ExplainRow> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) {sql}"
    ))
    .bind::<BigInt, _>(CEILING)
    .load(conn)
    .await
    .unwrap_or_else(|e| panic!("EXPLAIN failed for:\n{sql}\n\nerror: {e}"));
    let plan_text = explained
        .into_iter()
        .map(|r| r.query_plan)
        .collect::<Vec<_>>()
        .join("\n");

    let mut rows: Vec<OversizedRow> = diesel::sql_query(sql)
        .bind::<BigInt, _>(CEILING)
        .load(conn)
        .await
        .unwrap_or_else(|e| panic!("query failed for:\n{sql}\n\nerror: {e}"));
    rows.sort();

    (plan_text, rows)
}

/// Regenerates `docs/perf-artifacts/history-ceiling-scanner/before-*`:
/// `EXPLAIN` and a `pg_stat_statements` snapshot for the query as it stands
/// in this commit (pre-fix). `#[ignore]`d -- seeds ~4,000,000 rows and takes
/// well over a minute; not part of the default suite.
///
/// Needs `HARVEST_TEST_DATABASE_URL` (an admin connection string) or a
/// reachable Docker daemon for `claim_bench_support::db::setup_bench_db`'s
/// testcontainer fallback.
#[tokio::test]
#[ignore = "seeds ~4M rows; run explicitly via scripts/history_ceiling_claim_perf_repro.sh"]
async fn zz_capture_history_ceiling_claim_evidence() {
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
        .join("history-ceiling-scanner");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut conn = db::connect(&bench.url).await;
    seed_production_shaped_fixture(&mut conn).await;

    diesel::sql_query("SELECT pg_stat_statements_reset()")
        .execute(&mut conn)
        .await
        .ok(); // best-effort; absent extension must not fail the capture

    let sql = workflow_history_ceiling_query();
    let (plan_text, rows) = explain_and_result_set(&mut conn, sql).await;

    std::fs::write(
        out_dir.join("before-history-ceiling.explain.txt"),
        format!(
            "-- before: workflow_history_ceiling_query() @ \
             {FIXTURE_TOTAL_EXECUTIONS} executions ({FIXTURE_RUNNING_EXECUTIONS} RUNNING), \
             ceiling={CEILING} --\n{plan_text}\n"
        ),
    )
    .expect("write explain artifact");

    let stats: Vec<StatRow> = diesel::sql_query(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
         (shared_blks_hit + shared_blks_read) AS total_buffers \
         FROM pg_stat_statements \
         WHERE query LIKE '%harvest_workflow_executions%harvest_events%' \
         ORDER BY total_buffers DESC LIMIT 5",
    )
    .load(&mut conn)
    .await
    .unwrap_or_default();
    std::fs::write(
        out_dir.join("before-pg_stat_statements.txt"),
        format!("{stats:#?}\n"),
    )
    .expect("write pg_stat_statements artifact");

    std::fs::write(
        out_dir.join("before-result-rows.txt"),
        format!(
            "rows={}\n{}\n",
            rows.len(),
            rows.iter()
                .map(|r| format!("{} {}", r.id, r.event_count))
                .collect::<Vec<_>>()
                .join("\n")
        ),
    )
    .expect("write result-set artifact");

    eprintln!("captured 'before': {} oversized rows", rows.len());
    eprintln!("== done. Artifacts in {} ==", out_dir.display());
}
