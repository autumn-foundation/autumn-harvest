#![cfg(feature = "db")]
//! Buffer-cost evidence for the tenant sum of `llm_budget::load_llm_spend`.
//!
//! Each LLM step of a workflow type with a tenant LLM cap calls
//! `ActivityContext::check_llm_budget` before its model call. That check runs
//! `llm_budget::LLM_SPEND_SQL` once. The tenant sum finds the runs of the
//! tenant that are active or that ended in the window. It then sums the ledger
//! rows of each run that fall in the window.
//!
//! The ledger has two indexes: the primary key `(workflow_exec_id, event_id,
//! call_index)` and `recorded_at`. Neither serves the per-run window read. The
//! read takes every row of each run through the primary key. It fetches each
//! row from the heap, and then drops the rows outside the window. A heap page holds
//! the rows of many runs, because the ledger takes its rows in time order. So
//! the read touches about one heap page for each ledger row of each run.
//!
//! The candidate is a covering index on `(workflow_exec_id, recorded_at)` that
//! includes the three summed columns. The window is then a range in the index
//! key, and the sum reads the index only.
//!
//! Two tests:
//!
//! - [`llm_spend_covering_index_does_not_change_the_spend`] always runs. It
//!   seeds the edge cases into a small fixture. It compares each spend with
//!   and without the candidate index, in one transaction, so `NOW()` is the
//!   same instant for both reads.
//! - [`zz_capture_llm_spend_tenant_window_evidence`] is `#[ignore]`d. It seeds
//!   a production-shaped fixture and writes the artifacts in
//!   `docs/perf-artifacts/llm-spend-tenant-window/`. `cargo xtask perf
//!   llm-spend-tenant-window` runs it.

use std::fmt::Write as _;

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use autumn_harvest::llm_budget::{LLM_SPEND_SQL, LlmSpend, load_llm_spend};
use autumn_harvest::types::ExecutionId;

use crate::throwaway_db::ThrowawayDb;

/// The candidate index. The migration builds the same definition.
const CANDIDATE_INDEX: &str = "harvest_llm_ledger_run_recorded";

/// The definition of the candidate index.
const CANDIDATE_INDEX_SQL: &str = "CREATE INDEX IF NOT EXISTS harvest_llm_ledger_run_recorded \
     ON harvest_llm_ledger (workflow_exec_id, recorded_at) \
     INCLUDE (input_tokens, output_tokens, cost_usd_micros)";

/// The default tenant window, as `llm_budget::DEFAULT_TENANT_LLM_WINDOW_SECS`.
const WINDOW_SECS: u32 = autumn_harvest::llm_budget::DEFAULT_TENANT_LLM_WINDOW_SECS;

/// Runs in the production-shaped fixture.
const FIXTURE_RUNS: i64 = 200_000;

/// Spend reads for each target in one capture: one per LLM step.
const STEPS_PER_TARGET: usize = 10;

/// Ledger inserts in one capture, to measure the write cost of the index.
const LEDGER_INSERTS: usize = 200;

/// The run that takes the ledger inserts. It is in no target scope.
const WRITER_RUN: &str = "00000000-0000-4000-9000-00000000ffff";

/// One spend read target: a running run of one tenant scope.
struct Target {
    label: &'static str,
    workflow_name: &'static str,
    quota_key: &'static str,
    id: &'static str,
}

/// The targets of the capture. They differ only in the number of runs of
/// their tenant in the window, so the three sizes show how the read scales.
const TARGETS: [Target; 3] = [
    Target {
        label: "large",
        workflow_name: "support_agent",
        quota_key: "tenant-0",
        id: "00000000-0000-4000-9000-000000000001",
    },
    Target {
        label: "medium",
        workflow_name: "support_agent",
        quota_key: "tenant-1",
        id: "00000000-0000-4000-9000-000000000002",
    },
    Target {
        label: "small",
        workflow_name: "support_agent",
        quota_key: "tenant-3",
        id: "00000000-0000-4000-9000-000000000003",
    },
];

fn exec_id(id: &str) -> ExecutionId {
    ExecutionId::from_uuid(uuid::Uuid::parse_str(id).expect("a valid target id"))
}

/// Read the spend of each target, with and without the tenant sum.
async fn spends(conn: &mut AsyncPgConnection, ids: &[&str]) -> Vec<(String, bool, LlmSpend)> {
    let mut out = Vec::new();
    for id in ids {
        for with_tenant in [true, false] {
            let spend = load_llm_spend(conn, exec_id(id), WINDOW_SECS, with_tenant)
                .await
                .expect("load_llm_spend");
            out.push(((*id).to_owned(), with_tenant, spend));
        }
    }
    out
}

/// The spends of each run without and with the candidate index.
type SpendPair = (Vec<(String, bool, LlmSpend)>, Vec<(String, bool, LlmSpend)>);

/// The error of a transaction that rolls back on purpose.
///
/// `Done` carries the result out of the transaction. The rollback keeps the
/// schema and the rows of the caller as they were.
#[derive(Debug)]
enum Rollback<T> {
    Done(T),
    Db(diesel::result::Error),
}

impl<T> From<diesel::result::Error> for Rollback<T> {
    fn from(error: diesel::result::Error) -> Self {
        Self::Db(error)
    }
}

impl<T: std::fmt::Debug> Rollback<T> {
    fn done(outcome: Result<(), Self>) -> T {
        match outcome {
            Err(Self::Done(value)) => value,
            other => panic!("the transaction failed: {other:?}"),
        }
    }
}

/// Compare the spends without and with the candidate index.
///
/// The whole comparison runs in one transaction. `NOW()` is the start of the
/// transaction, so both reads use the same window. The transaction rolls
/// back, so the index state of the caller does not change. With
/// `check_plans`, the comparison also checks that only the second read uses
/// the index. A tiny fixture can plan either way, so it skips that check.
async fn compare_with_and_without_index(
    conn: &mut AsyncPgConnection,
    ids: &[&str],
    check_plans: bool,
) -> SpendPair {
    let outcome = conn
        .transaction::<(), Rollback<SpendPair>, _>(async |conn| {
            conn.batch_execute(&format!("DROP INDEX IF EXISTS {CANDIDATE_INDEX}"))
                .await?;
            let without = spends(conn, ids).await;
            let plan_without = tenant_plan(conn, ids[0]).await;
            conn.batch_execute(CANDIDATE_INDEX_SQL).await?;
            conn.batch_execute("ANALYZE harvest_llm_ledger").await?;
            let with = spends(conn, ids).await;
            let plan_with = tenant_plan(conn, ids[0]).await;
            if check_plans {
                assert!(
                    !plan_without.contains(CANDIDATE_INDEX),
                    "the read without the index must not use it:\n{plan_without}"
                );
                assert!(
                    plan_with.contains(CANDIDATE_INDEX),
                    "the read with the index must use it:\n{plan_with}"
                );
            }
            Err(Rollback::Done((without, with)))
        })
        .await;
    Rollback::done(outcome)
}

/// `EXPLAIN` of the spend read of one run, in text form.
async fn tenant_plan(conn: &mut AsyncPgConnection, id: &str) -> String {
    explain(conn, id, "EXPLAIN (COSTS OFF)").await
}

#[derive(QueryableByName)]
struct PlanLine {
    #[diesel(sql_type = Text)]
    #[diesel(column_name = "QUERY PLAN")]
    line: String,
}

/// Run one `EXPLAIN` form of `LLM_SPEND_SQL` for one run.
///
/// The statement text and the bind types are those of `load_llm_spend`. JIT
/// is off, as `load_llm_spend` sets it for the tenant read.
async fn explain(conn: &mut AsyncPgConnection, id: &str, form: &str) -> String {
    conn.batch_execute("SET jit = off").await.expect("jit off");
    let lines: Vec<PlanLine> = diesel::sql_query(format!("{form} {LLM_SPEND_SQL}"))
        .bind::<diesel::sql_types::Uuid, _>(exec_id(id).as_uuid())
        .bind::<BigInt, _>(i64::from(WINDOW_SECS))
        .bind::<diesel::sql_types::Bool, _>(true)
        .load(conn)
        .await
        .expect("explain the spend read");
    conn.batch_execute("RESET jit").await.expect("reset jit");
    lines.into_iter().fold(String::new(), |mut out, l| {
        out.push_str(&l.line);
        out.push('\n');
        out
    })
}

// ---------------------------------------------------------------------------
// Fast, always-run equivalence check
// ---------------------------------------------------------------------------

/// The edge cases of the tenant sum, all in the scope of the first run.
///
/// Each timestamp is relative to `NOW()`, and the seed runs in the comparison
/// transaction, so each boundary row sits exactly on its boundary.
const EDGE_FIXTURE_SQL: &str = "
INSERT INTO harvest_workflow_executions
    (id, workflow_name, workflow_id, shard_id, state, input, started_at, completed_at,
     quota_key, migrated_to_shard, migrated_at)
VALUES
    -- The reading run: active, with rows on both sides of the window start.
    ('10000000-0000-4000-8000-000000000001', 'edge_agent', 'e1', 0, 'RUNNING', '{}',
     NOW() - INTERVAL '3 days', NULL, 'edge', NULL, NULL),
    -- Ended exactly at the window start: it is in range, and its last row counts.
    ('10000000-0000-4000-8000-000000000002', 'edge_agent', 'e2', 0, 'COMPLETED', '{}',
     NOW() - INTERVAL '2 days', NOW() - INTERVAL '1 day', 'edge', NULL, NULL),
    -- Ended one microsecond before the window start: it never counts.
    ('10000000-0000-4000-8000-000000000003', 'edge_agent', 'e3', 0, 'COMPLETED', '{}',
     NOW() - INTERVAL '2 days', NOW() - INTERVAL '1 day' - INTERVAL '1 microsecond',
     'edge', NULL, NULL),
    -- Unpriced calls: a NULL cost adds no cost.
    ('10000000-0000-4000-8000-000000000004', 'edge_agent', 'e4', 0, 'FAILED', '{}',
     NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 hour', 'edge', NULL, NULL),
    -- A staged shard-move copy: it never counts.
    ('10000000-0000-4000-8000-000000000005', 'edge_agent', 'e5', 0, 'MIGRATING', '{}',
     NOW() - INTERVAL '2 hours', NULL, 'edge', NULL, NULL),
    -- A sealed source with a committed move: it counts.
    ('10000000-0000-4000-8000-000000000006', 'edge_agent', 'e6', 0, 'MIGRATED', '{}',
     NOW() - INTERVAL '2 hours', NULL, 'edge', 1, NOW()),
    -- A sealed source with no committed move: it does not count.
    ('10000000-0000-4000-8000-000000000007', 'edge_agent', 'e7', 0, 'MIGRATED', '{}',
     NOW() - INTERVAL '2 hours', NULL, 'edge', 1, NOW()),
    -- No quota key: no tenant spend, and never in the scope of another run.
    ('10000000-0000-4000-8000-000000000008', 'edge_agent', 'e8', 0, 'RUNNING', '{}',
     NOW() - INTERVAL '2 hours', NULL, NULL, NULL, NULL),
    -- Another tenant of the same type: never in the edge scope.
    ('10000000-0000-4000-8000-000000000009', 'edge_agent', 'e9', 0, 'RUNNING', '{}',
     NOW() - INTERVAL '2 hours', NULL, 'other', NULL, NULL),
    -- A run with no ledger rows.
    ('10000000-0000-4000-8000-00000000000a', 'edge_agent', 'e10', 0, 'RUNNING', '{}',
     NOW() - INTERVAL '1 hour', NULL, 'edge', NULL, NULL);

INSERT INTO harvest_shard_migrations (execution_id, source_shard, target_shard, phase)
VALUES ('10000000-0000-4000-8000-000000000006', 0, 1, 'COMMITTED'),
       ('10000000-0000-4000-8000-000000000007', 0, 1, 'COPIED');

INSERT INTO harvest_llm_ledger
    (workflow_exec_id, event_id, call_index, activity_name, model,
     input_tokens, output_tokens, cost_usd_micros, latency_ms, recorded_at)
VALUES
    -- e1: before the window, one microsecond before, on the start, inside.
    ('10000000-0000-4000-8000-000000000001', 1, 0, 'a', 'm', 1, 1, 1, 1, NOW() - INTERVAL '2 days'),
    ('10000000-0000-4000-8000-000000000001', 2, 0, 'a', 'm', 10, 10, 10, 1,
     NOW() - INTERVAL '1 day' - INTERVAL '1 microsecond'),
    ('10000000-0000-4000-8000-000000000001', 3, 0, 'a', 'm', 100, 100, 100, 1, NOW() - INTERVAL '1 day'),
    ('10000000-0000-4000-8000-000000000001', 4, 0, 'a', 'm', 1000, 1000, 1000, 1, NOW() - INTERVAL '1 hour'),
    ('10000000-0000-4000-8000-000000000001', 4, 1, 'a', 'm', 2000, 2000, NULL, 1, NOW() - INTERVAL '1 hour'),
    ('10000000-0000-4000-8000-000000000001', 5, 0, 'a', 'm', 1000000000000, 1000000000000,
     1000000000000000, 1, NOW()),
    -- e2: one row outside, one on the window start.
    ('10000000-0000-4000-8000-000000000002', 1, 0, 'a', 'm', 3, 3, 3, 1, NOW() - INTERVAL '36 hours'),
    ('10000000-0000-4000-8000-000000000002', 2, 0, 'a', 'm', 30, 30, 30, 1, NOW() - INTERVAL '1 day'),
    -- e3: on its own end, one microsecond before the window start.
    ('10000000-0000-4000-8000-000000000003', 1, 0, 'a', 'm', 5, 5, 5, 1,
     NOW() - INTERVAL '1 day' - INTERVAL '1 microsecond'),
    -- e4: unpriced calls.
    ('10000000-0000-4000-8000-000000000004', 1, 0, 'a', 'm', 7, 7, NULL, 1, NOW() - INTERVAL '90 minutes'),
    ('10000000-0000-4000-8000-000000000004', 1, 1, 'a', 'm', 7, 7, NULL, 1, NOW() - INTERVAL '90 minutes'),
    ('10000000-0000-4000-8000-000000000005', 1, 0, 'a', 'm', 11, 11, 11, 1, NOW() - INTERVAL '1 hour'),
    ('10000000-0000-4000-8000-000000000006', 1, 0, 'a', 'm', 13, 13, 13, 1, NOW() - INTERVAL '1 hour'),
    ('10000000-0000-4000-8000-000000000007', 1, 0, 'a', 'm', 17, 17, 17, 1, NOW() - INTERVAL '1 hour'),
    ('10000000-0000-4000-8000-000000000008', 1, 0, 'a', 'm', 19, 19, 19, 1, NOW() - INTERVAL '1 hour'),
    ('10000000-0000-4000-8000-000000000009', 1, 0, 'a', 'm', 23, 23, 23, 1, NOW() - INTERVAL '1 hour');
";

/// Each run of the edge fixture, as a reading run.
const EDGE_RUNS: [&str; 10] = [
    "10000000-0000-4000-8000-000000000001",
    "10000000-0000-4000-8000-000000000002",
    "10000000-0000-4000-8000-000000000003",
    "10000000-0000-4000-8000-000000000004",
    "10000000-0000-4000-8000-000000000005",
    "10000000-0000-4000-8000-000000000006",
    "10000000-0000-4000-8000-000000000007",
    "10000000-0000-4000-8000-000000000008",
    "10000000-0000-4000-8000-000000000009",
    "10000000-0000-4000-8000-00000000000a",
];

#[tokio::test]
async fn llm_spend_covering_index_does_not_change_the_spend() {
    let Some(db) = ThrowawayDb::create("llm_spend_idx_t").await else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL is unset");
        return;
    };
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connect to the test database");

    let outcome = conn
        .transaction::<(), Rollback<SpendPair>, _>(async |conn| {
            conn.batch_execute(EDGE_FIXTURE_SQL).await?;
            let pair = compare_with_and_without_index(conn, &EDGE_RUNS, false).await;
            Err(Rollback::Done(pair))
        })
        .await;
    let (without, with) = Rollback::done(outcome);
    assert_eq!(without, with, "the index must not change any spend");

    // Pin the edge semantics, so a vacuous pass is impossible. The reading
    // run sums e1 rows 3 to 5, e2 row 2, e4 and e6.
    let edge = with
        .iter()
        .find(|(id, with_tenant, _)| id == EDGE_RUNS[0] && *with_tenant)
        .map(|(_, _, spend)| *spend)
        .expect("the reading run");
    let e1_window = 200 + 2000 + 4000 + 2_000_000_000_000_i64;
    assert_eq!(edge.tenant_tokens, e1_window + 60 + 28 + 26);
    assert_eq!(
        edge.tenant_cost_usd_micros,
        100 + 1000 + 1_000_000_000_000_000_i64 + 30 + 13
    );
    assert_eq!(edge.run_tokens, 2 + 20 + e1_window);
    let no_key = with
        .iter()
        .find(|(id, with_tenant, _)| id == EDGE_RUNS[7] && *with_tenant)
        .map(|(_, _, spend)| *spend)
        .expect("the run with no key");
    assert_eq!((no_key.tenant_tokens, no_key.run_tokens), (0, 38));
    eprintln!(
        "equivalence confirmed: {} spends agree on the edge fixture",
        with.len()
    );
}

// ---------------------------------------------------------------------------
// Production-shaped evidence capture
// ---------------------------------------------------------------------------

/// The production-shaped fixture, before its in-window ledger rows.
///
/// - 200,000 runs over 30 days, the retention window of a busy agent fleet.
///   Three workflow types take 70%, 20% and 10% of the runs.
/// - 90% of the runs have a quota key. The key follows a cubic skew over 500
///   tenants, so `tenant-0` takes about 13% of the keyed runs.
/// - 90% of the runs last 1 to 30 minutes, 9.7% last 1 to 12 hours, and 0.3%
///   last 1 to 10 days. A run that has not ended by now is `RUNNING`.
/// - Calls per run: 70% make 1 to 5, 25% make 6 to 40, and 5% make 100 to
///   600. A multi-day run makes 500 to 3,000. They spread evenly over the run.
///
/// One `random()` draw per column, in one row source, so a later `CASE` arm
/// never draws again. The ledger rows go in in time order, as the engine
/// writes them, so a heap page holds the rows of many runs.
const SEED_RUNS_SQL: &str = "
SELECT setseed(0.42);
CREATE TEMP TABLE seed_runs AS
SELECT g AS n, random() AS u_name, random() AS u_key, random() AS u_start,
       random() AS u_dur, random() AS u_calls, random() AS u_dur2
FROM generate_series(1, $RUNS) g;
INSERT INTO harvest_workflow_executions
  (id, workflow_name, workflow_id, shard_id, state, input, queue_name,
   started_at, created_at, completed_at, quota_key)
SELECT
  ('00000000-0000-4000-8000-' || lpad(to_hex(n), 12, '0'))::uuid,
  CASE WHEN u_name < 0.7 THEN 'support_agent'
       WHEN u_name < 0.9 THEN 'research_agent' ELSE 'triage_agent' END,
  'wf-' || n, 0,
  CASE WHEN s + d > now() THEN 'RUNNING' WHEN u_calls < 0.05 THEN 'FAILED' ELSE 'COMPLETED' END,
  '{}'::jsonb, 'agents', s, s,
  CASE WHEN s + d > now() THEN NULL ELSE s + d END,
  CASE WHEN u_key < 0.9 THEN 'tenant-' || floor(500 * power(u_key / 0.9, 3))::int END
FROM (
  SELECT n, u_name, u_key, u_calls,
    now() - interval '30 days' * u_start AS s,
    CASE WHEN u_dur < 0.90 THEN interval '1 minute' * (1 + 29 * u_dur2)
         WHEN u_dur < 0.997 THEN interval '1 hour' * (1 + 11 * u_dur2)
         ELSE interval '1 day' * (1 + 9 * u_dur2) END AS d
  FROM seed_runs) x;
-- The targets: one running run in each scope, with 20 calls over 2 hours.
INSERT INTO harvest_workflow_executions
  (id, workflow_name, workflow_id, shard_id, state, input, queue_name,
   started_at, created_at, quota_key)
VALUES
  ('$T0', 'support_agent', 'target-large', 0, 'RUNNING', '{}', 'agents',
   now() - interval '2 hours', now() - interval '2 hours', 'tenant-0'),
  ('$T1', 'support_agent', 'target-medium', 0, 'RUNNING', '{}', 'agents',
   now() - interval '2 hours', now() - interval '2 hours', 'tenant-1'),
  ('$T2', 'support_agent', 'target-small', 0, 'RUNNING', '{}', 'agents',
   now() - interval '2 hours', now() - interval '2 hours', 'tenant-3'),
  ('$WRITER', 'writer_agent', 'writer', 0, 'RUNNING', '{}', 'agents',
   now(), now(), NULL);
CREATE TEMP TABLE seed_calls AS
SELECT w.id, w.started_at, COALESCE(w.completed_at, now()) AS ended_at, c.calls
FROM harvest_workflow_executions w
JOIN seed_runs r ON w.workflow_id = 'wf-' || r.n
CROSS JOIN LATERAL (SELECT CASE
   WHEN r.u_dur >= 0.997 THEN 500 + floor(2500 * r.u_dur2)::int
   WHEN r.u_calls < 0.70 THEN 1 + floor(5 * r.u_calls / 0.70)::int
   WHEN r.u_calls < 0.95 THEN 6 + floor(35 * (r.u_calls - 0.70) / 0.25)::int
   ELSE 100 + floor(500 * (r.u_calls - 0.95) / 0.05)::int END AS calls) c
UNION ALL
SELECT id, started_at, now(), 20 FROM harvest_workflow_executions
WHERE workflow_id LIKE 'target-%';
CREATE TEMP TABLE seed_rows AS
SELECT c.id, k, 1000 + (k * 7919) % 20000 AS input_tokens,
       100 + (k * 104729) % 2000 AS output_tokens,
       500 + (k * 31) % 50000 AS cost,
       c.started_at + (c.ended_at - c.started_at) * (k::float8 / (c.calls + 1)) AS recorded_at
FROM seed_calls c CROSS JOIN LATERAL generate_series(1, c.calls) k;
ALTER TABLE harvest_llm_ledger SET (autovacuum_enabled = false);
";

/// Insert the seeded ledger rows of one side of the window start, in time order.
fn seed_ledger_sql(predicate: &str) -> String {
    format!(
        "INSERT INTO harvest_llm_ledger
           (workflow_exec_id, event_id, call_index, activity_name, model,
            input_tokens, output_tokens, cost_usd_micros, latency_ms, recorded_at)
         SELECT id, k, 0, 'call_model',
                CASE WHEN k % 4 = 0 THEN 'claude-haiku' ELSE 'claude-sonnet' END,
                input_tokens, output_tokens, cost, 300 + (k * 13) % 5000, recorded_at
         FROM seed_rows WHERE {predicate} ORDER BY recorded_at"
    )
}

/// Seed the production-shaped fixture.
///
/// The rows before the window go in first, and a `VACUUM` marks their pages
/// all-visible. The rows in the window go in after it, and autovacuum is off
/// for the ledger. So the in-window pages are not all-visible, as on a live
/// shard where autovacuum has not reached the newest rows yet. That is the
/// worst case for an index-only scan.
async fn seed_production_shaped_fixture(conn: &mut AsyncPgConnection) {
    let sql = SEED_RUNS_SQL
        .replace("$RUNS", &FIXTURE_RUNS.to_string())
        .replace("$T0", TARGETS[0].id)
        .replace("$T1", TARGETS[1].id)
        .replace("$T2", TARGETS[2].id)
        .replace("$WRITER", WRITER_RUN);
    conn.batch_execute(&sql).await.expect("seed the runs");
    let window = format!("now() - interval '{WINDOW_SECS} seconds'");
    conn.batch_execute(&seed_ledger_sql(&format!("recorded_at < {window}")))
        .await
        .expect("seed the ledger rows before the window");
    conn.batch_execute("VACUUM ANALYZE harvest_workflow_executions")
        .await
        .expect("vacuum the runs");
    conn.batch_execute("VACUUM ANALYZE harvest_llm_ledger")
        .await
        .expect("vacuum the old ledger rows");
    conn.batch_execute(&seed_ledger_sql(&format!("recorded_at >= {window}")))
        .await
        .expect("seed the ledger rows in the window");
    conn.batch_execute("ANALYZE harvest_llm_ledger")
        .await
        .expect("analyze the ledger");
}

#[derive(QueryableByName)]
struct FixtureRow {
    #[diesel(sql_type = Text)]
    item: String,
    #[diesel(sql_type = BigInt)]
    value: i64,
}

/// The shape of the seeded fixture, for the artifact.
async fn fixture_summary(conn: &mut AsyncPgConnection) -> String {
    let rows: Vec<FixtureRow> = diesel::sql_query(format!(
        "SELECT 'runs' AS item, COUNT(*)::BIGINT AS value FROM harvest_workflow_executions
         UNION ALL SELECT 'runs RUNNING', COUNT(*) FROM harvest_workflow_executions WHERE state = 'RUNNING'
         UNION ALL SELECT 'runs with a quota key', COUNT(*) FROM harvest_workflow_executions WHERE quota_key IS NOT NULL
         UNION ALL SELECT 'ledger rows', COUNT(*) FROM harvest_llm_ledger
         UNION ALL SELECT 'ledger rows in the window', COUNT(*) FROM harvest_llm_ledger
           WHERE recorded_at >= now() - interval '{WINDOW_SECS} seconds'
         UNION ALL SELECT 'ledger heap pages', pg_relation_size('harvest_llm_ledger') / 8192
         UNION ALL SELECT 'ledger all-visible pages', relallvisible::BIGINT FROM pg_class
           WHERE relname = 'harvest_llm_ledger'"
    ))
    .load(conn)
    .await
    .expect("fixture summary");
    let mut out = String::from("item|value\n");
    for r in rows {
        let _ = writeln!(out, "{}|{}", r.item, r.value);
    }
    for t in &TARGETS {
        let scope: Vec<FixtureRow> = diesel::sql_query(format!(
            "SELECT 'scope {label}: runs in the window' AS item, COUNT(*)::BIGINT AS value
             FROM harvest_workflow_executions
             WHERE workflow_name = '{wf}' AND quota_key = '{key}'
               AND COALESCE(completed_at, 'infinity') >= now() - interval '{WINDOW_SECS} seconds'
             UNION ALL
             SELECT 'scope {label}: ledger rows of those runs', COUNT(*)::BIGINT
             FROM harvest_llm_ledger l JOIN harvest_workflow_executions w ON w.id = l.workflow_exec_id
             WHERE w.workflow_name = '{wf}' AND w.quota_key = '{key}'
               AND COALESCE(w.completed_at, 'infinity') >= now() - interval '{WINDOW_SECS} seconds'
             UNION ALL
             SELECT 'scope {label}: ledger rows in the window', COUNT(*)::BIGINT
             FROM harvest_llm_ledger l JOIN harvest_workflow_executions w ON w.id = l.workflow_exec_id
             WHERE w.workflow_name = '{wf}' AND w.quota_key = '{key}'
               AND l.recorded_at >= now() - interval '{WINDOW_SECS} seconds'",
            label = t.label,
            wf = t.workflow_name,
            key = t.quota_key,
        ))
        .load(conn)
        .await
        .expect("scope summary");
        for r in scope {
            let _ = writeln!(out, "{}|{}", r.item, r.value);
        }
    }
    out
}

#[derive(QueryableByName)]
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
    #[diesel(sql_type = BigInt)]
    temp_blks_written: i64,
    #[diesel(sql_type = BigInt)]
    wal_bytes: i64,
    #[diesel(sql_type = BigInt)]
    rows: i64,
}

#[derive(QueryableByName)]
struct IndexStat {
    #[diesel(sql_type = Text)]
    indexrelname: String,
    #[diesel(sql_type = BigInt)]
    idx_scan: i64,
    #[diesel(sql_type = BigInt)]
    size_bytes: i64,
}

#[derive(QueryableByName)]
struct TableStat {
    #[diesel(sql_type = BigInt)]
    seq_scan: i64,
    #[diesel(sql_type = BigInt)]
    idx_scan: i64,
    #[diesel(sql_type = BigInt)]
    heap_blks_read: i64,
    #[diesel(sql_type = BigInt)]
    heap_blks_hit: i64,
    #[diesel(sql_type = BigInt)]
    idx_blks_read: i64,
    #[diesel(sql_type = BigInt)]
    idx_blks_hit: i64,
}

/// The `pg_stat_statements` scope of this run's database.
const DB_SCOPE: &str = "dbid = (SELECT oid FROM pg_database WHERE datname = current_database())";

/// One ledger insert, in the shape of `llm_ledger::insert_ledger_rows`.
async fn insert_ledger_row(conn: &mut AsyncPgConnection, event_id: i32) {
    diesel::sql_query(
        "INSERT INTO harvest_llm_ledger
           (workflow_exec_id, event_id, call_index, activity_name, model,
            input_tokens, output_tokens, cost_usd_micros, latency_ms)
         VALUES ($1, $2, 0, 'call_model', 'claude-sonnet', $3, $4, $5, $6)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id(WRITER_RUN).as_uuid())
    .bind::<diesel::sql_types::Integer, _>(event_id)
    .bind::<BigInt, _>(12_000_i64)
    .bind::<BigInt, _>(900_i64)
    .bind::<Nullable<BigInt>, _>(Some(4_200_i64))
    .bind::<BigInt, _>(1_500_i64)
    .execute(conn)
    .await
    .expect("insert one ledger row");
}

/// The index and table statistics of the ledger, for the snapshot.
async fn ledger_table_stats(conn: &mut AsyncPgConnection) -> String {
    let mut pss = String::new();
    let indexes: Vec<IndexStat> = diesel::sql_query(
        "SELECT indexrelname::TEXT AS indexrelname, idx_scan,
                pg_relation_size(indexrelid) AS size_bytes
         FROM pg_stat_user_indexes WHERE relname = 'harvest_llm_ledger'
         ORDER BY indexrelname",
    )
    .load(conn)
    .await
    .expect("index statistics");
    pss.push_str(
        "\n-- pg_stat_user_indexes, harvest_llm_ledger --\nindexrelname|idx_scan|size_bytes\n",
    );
    for i in &indexes {
        let _ = writeln!(pss, "{}|{}|{}", i.indexrelname, i.idx_scan, i.size_bytes);
    }
    let table: Vec<TableStat> = diesel::sql_query(
        "SELECT t.seq_scan, COALESCE(t.idx_scan, 0) AS idx_scan,
                COALESCE(s.heap_blks_read, 0) AS heap_blks_read,
                COALESCE(s.heap_blks_hit, 0) AS heap_blks_hit,
                COALESCE(s.idx_blks_read, 0) AS idx_blks_read,
                COALESCE(s.idx_blks_hit, 0) AS idx_blks_hit
         FROM pg_stat_user_tables t JOIN pg_statio_user_tables s USING (relid)
         WHERE t.relname = 'harvest_llm_ledger'",
    )
    .load(conn)
    .await
    .expect("table statistics");
    pss.push_str(
        "\n-- pg_stat_user_tables + pg_statio_user_tables, harvest_llm_ledger --\n\
                  seq_scan|idx_scan|heap_blks_read|heap_blks_hit|idx_blks_read|idx_blks_hit\n",
    );
    for t in &table {
        let _ = writeln!(
            pss,
            "{}|{}|{}|{}|{}|{}",
            t.seq_scan,
            t.idx_scan,
            t.heap_blks_read,
            t.heap_blks_hit,
            t.idx_blks_read,
            t.idx_blks_hit
        );
    }
    pss
}

/// Run the workload once, and write the artifacts of one label.
///
/// The workload is the database half of the LLM steps of an agent fleet. Each
/// step reads the spend of its run and its tenant. Each completed step writes
/// one ledger row.
async fn capture(
    conn: &mut AsyncPgConnection,
    out_dir: &std::path::Path,
    label: &str,
    first_event: i32,
) {
    diesel::sql_query(
        "SELECT pg_stat_statements_reset(0, \
         (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
    )
    .execute(conn)
    .await
    .expect("pg_stat_statements_reset() failed; stale counts would leak into this capture");
    conn.batch_execute("SELECT pg_stat_reset()")
        .await
        .expect("reset the table statistics");

    for t in &TARGETS {
        for _ in 0..STEPS_PER_TARGET {
            load_llm_spend(conn, exec_id(t.id), WINDOW_SECS, true)
                .await
                .expect("load_llm_spend");
        }
    }
    for i in 0..LEDGER_INSERTS {
        insert_ledger_row(conn, first_event + i32::try_from(i).expect("small")).await;
    }
    // The cumulative statistics flush at the end of a transaction, at most
    // once a second, so wait before the snapshot.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    conn.batch_execute("SELECT pg_stat_force_next_flush()")
        .await
        .expect("flush the table statistics");

    let stats: Vec<StatRow> = diesel::sql_query(format!(
        "SELECT query, calls, shared_blks_hit, shared_blks_read,
                (shared_blks_hit + shared_blks_read) AS total_buffers,
                temp_blks_written, wal_bytes::BIGINT AS wal_bytes, rows
         FROM pg_stat_statements
         WHERE {DB_SCOPE}
           AND (query LIKE '%harvest_llm_ledger%' OR query LIKE 'SET LOCAL jit%'
                OR query IN ('BEGIN', 'COMMIT'))
           AND query NOT LIKE '%pg_stat_statements%'
         ORDER BY total_buffers DESC, calls DESC"
    ))
    .load(conn)
    .await
    .expect("pg_stat_statements snapshot");
    assert!(
        !stats.is_empty(),
        "pg_stat_statements has no row for {label}"
    );
    let workload_buffers: i64 = stats.iter().map(|s| s.total_buffers).sum();
    let workload_calls: i64 = stats.iter().map(|s| s.calls).sum();
    let mut pss = format!(
        "-- {label}: {} targets x {STEPS_PER_TARGET} spend reads + {LEDGER_INSERTS} ledger inserts --\n\
         -- workload total: {workload_buffers} buffers, {workload_calls} calls --\n\
         calls|shared_blks_hit|shared_blks_read|total_buffers|pct_buffers|pct_calls|temp_blks_written|wal_bytes|rows|query\n",
        TARGETS.len()
    );
    for s in &stats {
        #[allow(clippy::cast_precision_loss)]
        let pct_b = 100.0 * s.total_buffers as f64 / workload_buffers.max(1) as f64;
        #[allow(clippy::cast_precision_loss)]
        let pct_c = 100.0 * s.calls as f64 / workload_calls.max(1) as f64;
        let query: String = s.query.split_whitespace().collect::<Vec<_>>().join(" ");
        let _ = writeln!(
            pss,
            "{}|{}|{}|{}|{pct_b:.1}|{pct_c:.1}|{}|{}|{}|{}",
            s.calls,
            s.shared_blks_hit,
            s.shared_blks_read,
            s.total_buffers,
            s.temp_blks_written,
            s.wal_bytes,
            s.rows,
            query.chars().take(160).collect::<String>()
        );
    }

    pss.push_str(&ledger_table_stats(conn).await);
    std::fs::write(out_dir.join(format!("{label}.pg_stat_statements.txt")), pss)
        .expect("write the statement snapshot");

    let mut explain_text = String::new();
    for t in &TARGETS {
        let plan = explain(
            conn,
            t.id,
            "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)",
        )
        .await;
        let _ = write!(
            explain_text,
            "-- {label}: LLM_SPEND_SQL, scope {} ({} / {}), window {WINDOW_SECS} s --\n{plan}\n",
            t.label, t.workflow_name, t.quota_key
        );
    }
    std::fs::write(out_dir.join(format!("{label}.explain.txt")), explain_text)
        .expect("write the plans");
}

#[tokio::test]
#[ignore = "evidence capture: seeds 200,000 runs and 6M ledger rows"]
async fn zz_capture_llm_spend_tenant_window_evidence() {
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
        .join("llm-spend-tenant-window");
    std::fs::create_dir_all(&out_dir).expect("create the artifact directory");

    let mut conn = db::connect(&bench.url).await;
    conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await
        .expect("pg_stat_statements must be in shared_preload_libraries");
    // The migration builds the candidate once it ships. Drop it, so that the
    // baseline is the pre-fix schema on every checkout.
    conn.batch_execute(&format!("DROP INDEX IF EXISTS {CANDIDATE_INDEX}"))
        .await
        .expect("drop the candidate index");
    seed_production_shaped_fixture(&mut conn).await;
    std::fs::write(
        out_dir.join("fixture.txt"),
        fixture_summary(&mut conn).await,
    )
    .expect("write the fixture summary");

    let target_ids: Vec<&str> = TARGETS.iter().map(|t| t.id).collect();
    let (without, with) = compare_with_and_without_index(&mut conn, &target_ids, true).await;
    let mut rows = String::from("run|with_tenant|run_tokens|run_cost|tenant_tokens|tenant_cost\n");
    for (id, with_tenant, s) in &without {
        let _ = writeln!(
            rows,
            "{id}|{with_tenant}|{}|{}|{}|{}",
            s.run_tokens, s.run_cost_usd_micros, s.tenant_tokens, s.tenant_cost_usd_micros
        );
    }
    std::fs::write(out_dir.join("result-rows.txt"), &rows).expect("write the result rows");

    capture(&mut conn, &out_dir, "before", 1).await;

    let started = std::time::Instant::now();
    conn.batch_execute(CANDIDATE_INDEX_SQL)
        .await
        .expect("create the candidate index");
    let build = started.elapsed();
    conn.batch_execute("ANALYZE harvest_llm_ledger")
        .await
        .expect("analyze after the build");
    std::fs::write(
        out_dir.join("index-build.txt"),
        format!(
            "plain CREATE INDEX on the fixture: {} ms\n",
            build.as_millis()
        ),
    )
    .expect("write the build time");

    capture(
        &mut conn,
        &out_dir,
        "after",
        1 + i32::try_from(LEDGER_INSERTS).expect("small"),
    )
    .await;

    // The best case: autovacuum has reached every page.
    conn.batch_execute("VACUUM harvest_llm_ledger")
        .await
        .expect("vacuum the ledger");
    let plan = explain(
        &mut conn,
        TARGETS[0].id,
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)",
    )
    .await;
    std::fs::write(
        out_dir.join("after-vacuumed.explain.txt"),
        format!("-- after, all pages all-visible: scope large --\n{plan}\n"),
    )
    .expect("write the vacuumed plan");

    assert_eq!(without, with, "the index must not change any spend");
    eprintln!(
        "equivalence confirmed: {} spends agree on the production-shaped fixture",
        with.len()
    );
    eprintln!("== done. Artifacts in {} ==", out_dir.display());
}
