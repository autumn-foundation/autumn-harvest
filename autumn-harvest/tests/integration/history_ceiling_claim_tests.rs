#![cfg(feature = "db")]
//! Correctness and buffer-cost evidence for
//! `timeout::workflow_history_ceiling_query()` (issue #493).
//! `enforce_workflow_history_ceiling` runs it on every timeout-scanner tick
//! when `HarvestBuilder::max_workflow_history_events` is set.
//!
//! The query needs each RUNNING execution's `harvest_events` row count in the
//! `SELECT` list and in the `WHERE` clause. A `SELECT`-list alias is not
//! visible to the `WHERE` clause of the same query. The pre-fix query wrote
//! the correlated `COUNT(*)` subquery twice. Postgres evaluates each
//! occurrence independently, so every RUNNING row paid for the count twice.
//! The fix wraps it in a `MATERIALIZED` CTE, so the count runs once per row.
//! The evidence test regenerates `docs/performance-history-ceiling.md`.
//!
//! Two tests:
//! - [`enforce_workflow_history_ceiling_terminates_only_oversized_running_rows`]
//!   -- a fast functional check against the shared test database. The scanner
//!   is a GLOBAL scan, like `enforce_workflow_execution_timeouts` in
//!   `chain_timeout_tests.rs`. So the test checks only its own rows.
//! - [`zz_capture_history_ceiling_claim_evidence`] -- `#[ignore]`d. Seeds a
//!   production-shaped fixture into a throwaway database
//!   ([`claim_bench_support::db::setup_bench_db`]). It captures
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` and a
//!   `pg_stat_statements` snapshot for the pre-fix query and the current one.
//!   Both run against the same fixture in the same run. The pre-fix query
//!   below is byte-for-byte the previous baseline. The test asserts that both
//!   queries return byte-identical result sets (same executions, same
//!   `event_count`). That equivalence is the correctness proof of the rewrite.

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Nullable, Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::timeout::{enforce_workflow_history_ceiling, workflow_history_ceiling_query};
use autumn_harvest::types::ExecutionId;

use crate::integration_e2e::{insert_workflow_execution_with_id, setup_test_database_url_or_env};

/// The exact query `workflow_history_ceiling_query()` replaced. Kept here
/// verbatim (not reconstructed) so the evidence-capture test's "before" half
/// is byte-for-byte what the previous commit measured, not a paraphrase that
/// could drift.
const BEFORE_SQL: &str = "SELECT id, workflow_id, workflow_name, queue_name, parent_id, parent_close_policy, \
     (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id)::bigint AS event_count \
     FROM harvest_workflow_executions \
     WHERE state = 'RUNNING' \
     AND (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id) >= $1";

#[derive(QueryableByName, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct OversizedRow {
    #[diesel(sql_type = SqlUuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = BigInt)]
    event_count: i64,
}

/// Bulk-insert `n` events for `exec_id`, bypassing `store::append_single_event`.
///
/// That path costs a per-row locked round trip. A fixture holds up to hundreds
/// of thousands of events, and only the count matters.
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

/// Exercises the real `enforce_workflow_history_ceiling` end to end.
///
/// - A RUNNING row over the ceiling must FAIL with `history_ceiling_exceeded`.
/// - A RUNNING row exactly AT the ceiling must also FAIL (`>=`, not `>`).
/// - A RUNNING row one event short must stay RUNNING.
/// - A COMPLETED row over the ceiling is never touched. The query filters on
///   `state = 'RUNNING'` before it counts events.
///
/// The scanner is a GLOBAL scan, like `enforce_workflow_execution_timeouts`
/// in `chain_timeout_tests.rs`. The ceiling (750) is well above what other
/// tests in this shared database seed. The assertions check only this test's
/// own rows. So concurrent tests cannot trip this test, and it cannot trip them.
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

/// Seed a production-shaped fixture into `conn`'s migrated, empty database.
///
/// It holds 100,000 workflow executions, of which 3,000 (3%) are RUNNING.
/// That is a plausible steady state for a busy fleet where most work finishes
/// quickly. About 4,000,000 `harvest_events` rows are spread across them, with
/// a skewed per-state count.
///
/// RUNNING rows get a heavier tail: 10% land in a 1,000-8,000 event band
/// that straddles the 5,000-event `CEILING` below. Still-running executions
/// over-represent hung or looping workflows, which this scanner exists to
/// catch. Non-RUNNING rows get a smaller tail (1%, capped at 3,000), because
/// most of them finished normally.
///
/// The seed is set-based SQL (`generate_series` + `LATERAL`). A per-row
/// client loop over ~4M events would dominate the wall clock of this test.
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

/// Runs `sql` once (`EXPLAIN` + a plain execution). It writes the plan, a
/// `pg_stat_statements` snapshot and the result set to `{label}-*` files under
/// `out_dir`. It returns the sorted rows for the equivalence check.
async fn capture(
    conn: &mut AsyncPgConnection,
    out_dir: &std::path::Path,
    label: &str,
    sql: &str,
) -> Vec<OversizedRow> {
    diesel::sql_query("SELECT pg_stat_statements_reset()")
        .execute(conn)
        .await
        .ok(); // best-effort; absent extension must not fail the capture

    let (plan_text, rows) = explain_and_result_set(conn, sql).await;

    std::fs::write(
        out_dir.join(format!("{label}-history-ceiling.explain.txt")),
        format!(
            "-- {label}: workflow_history_ceiling_query() @ \
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
    .load(conn)
    .await
    .unwrap_or_default();
    std::fs::write(
        out_dir.join(format!("{label}-pg_stat_statements.txt")),
        format!("{stats:#?}\n"),
    )
    .expect("write pg_stat_statements artifact");

    std::fs::write(
        out_dir.join(format!("{label}-result-rows.txt")),
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

    eprintln!("captured '{label}': {} oversized rows", rows.len());
    rows
}

/// Regenerates `docs/perf-artifacts/history-ceiling-scanner/`: before/after
/// `EXPLAIN`, a `pg_stat_statements` snapshot for each, and a result-set
/// equivalence check between the two query forms against the identical
/// fixture. `#[ignore]`d -- seeds ~4,000,000 rows and takes well over a
/// minute; not part of the default suite.
///
/// Needs `HARVEST_TEST_DATABASE_URL` (an admin connection string) or a
/// reachable Docker daemon for `claim_bench_support::db::setup_bench_db`'s
/// testcontainer fallback.
#[tokio::test]
#[ignore = "seeds ~4M rows; run explicitly via cargo xtask perf history-ceiling-claim"]
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

    let after_sql = workflow_history_ceiling_query();
    assert_ne!(
        after_sql, BEFORE_SQL,
        "the current query text is identical to the hardcoded pre-fix text -- \
         update BEFORE_SQL (or this assertion) before trusting this capture"
    );

    let before_rows = capture(&mut conn, &out_dir, "before", BEFORE_SQL).await;
    let after_rows = capture(&mut conn, &out_dir, "after", after_sql).await;

    assert_eq!(
        before_rows, after_rows,
        "before/after query forms must return byte-identical result sets \
         (same executions flagged, same reported event_count) against the \
         same fixture -- this is the correctness proof the rewrite depends on"
    );
    eprintln!(
        "equivalence confirmed: before and after agree on all {} oversized rows",
        before_rows.len()
    );

    eprintln!("== done. Artifacts in {} ==", out_dir.display());
}
