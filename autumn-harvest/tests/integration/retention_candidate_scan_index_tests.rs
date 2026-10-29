#![cfg(feature = "db")]
//! Plan gate for the retention candidate scan (issue #2395).
//!
//! The janitor runs [`candidate_scan_sql`] on every retention tick, on every
//! shard. The scan selects six terminal states. The partial index
//! `idx_harvest_we_retention_scan` once covered only five of them, and left
//! out `TERMINATED`. Postgres uses a partial index only when the query
//! predicate implies the index predicate. A sixth state breaks that
//! implication, so each tick read the whole executions table. The migration
//! `20261029034500_harvest_retention_candidates_index` replaces it with
//! `idx_harvest_we_retention_candidates`, which lists all six.
//!
//! The gate turns `enable_seqscan` off. The planner then prefers any usable
//! index to a sequential scan. So a plan without the retention index means
//! that the index cannot serve the query.

use std::fmt::Write as _;

use autumn_harvest::retention::candidate_scan_sql;
use diesel::QueryableByName;
use diesel::sql_types::Text;
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use crate::claim_bench_support::db as claim_bench_db;

/// The index that must serve the candidate scan.
const RETENTION_INDEX: &str = "idx_harvest_we_retention_candidates";

/// Terminal executions per candidate state.
const ROWS_PER_STATE: usize = 500;

/// The states that the candidate scan selects. `MIGRATED` is not one of them.
const CANDIDATE_STATES: [&str; 6] = [
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMED_OUT",
    "CONTINUED_AS_NEW",
    "TERMINATED",
];

#[derive(QueryableByName, Debug)]
struct PlanLine {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    line: String,
}

/// Provision an isolated, fully migrated database for the gate.
///
/// A plan gate that skips itself reports success over the defect it exists to
/// catch. So a database that cannot be provisioned fails the gate.
async fn plan_gate_db() -> claim_bench_db::BenchDb {
    claim_bench_db::setup_bench_db().await.unwrap_or_else(|r| {
        panic!(
            "the retention plan gate needs an isolated database, from \
             HARVEST_TEST_DATABASE_URL (an admin connection string) or a reachable \
             Docker daemon: {}",
            r.0
        )
    })
}

/// Seed terminal executions in every candidate state, and a live population.
async fn seed_executions(conn: &mut AsyncPgConnection) {
    let mut sql = String::new();
    for state in CANDIDATE_STATES.iter().chain(std::iter::once(&"RUNNING")) {
        let completed_at = if *state == "RUNNING" {
            "NULL"
        } else {
            "NOW() - (gs || ' minutes')::interval"
        };
        write!(
            sql,
            "INSERT INTO harvest_workflow_executions \
                 (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
                  started_at, created_at, completed_at) \
             SELECT gen_random_uuid(), 'retention_plan_wf', '{state}_' || gs, \
                    gen_random_uuid(), 0, '{state}', '{{}}'::jsonb, 'default', \
                    NOW(), NOW(), {completed_at} \
             FROM generate_series(1, {ROWS_PER_STATE}) gs;"
        )
        .expect("write to a String");
    }
    sql.push_str("ANALYZE harvest_workflow_executions;");
    conn.batch_execute(&sql).await.expect("seed executions");
}

/// Replace each bind placeholder with a typed literal.
///
/// The global-age variant binds eight parameters. The other variant binds
/// seven, because its age fallback is the `-infinity` literal.
fn with_literal_binds(sql: &str, has_global_age: bool) -> String {
    let binds: &[&str] = if has_global_age {
        &[
            "'{}'::text[]",
            "'{}'::timestamptz[]",
            "NOW()",
            "NULL::timestamptz",
            "NULL::uuid",
            "100",
            "'{}'::text[]",
            "'{}'::timestamptz[]",
        ]
    } else {
        &[
            "'{}'::text[]",
            "'{}'::timestamptz[]",
            "NULL::timestamptz",
            "NULL::uuid",
            "100",
            "'{}'::text[]",
            "'{}'::timestamptz[]",
        ]
    };
    let mut out = sql.to_string();
    for (index, literal) in binds.iter().enumerate().rev() {
        out = out.replace(&format!("${}", index + 1), literal);
    }
    assert!(!out.contains('$'), "an unbound placeholder remains:\n{out}");
    out
}

async fn explain(conn: &mut AsyncPgConnection, sql: &str) -> String {
    let lines: Vec<PlanLine> = diesel::sql_query(format!("EXPLAIN (COSTS OFF) {sql}"))
        .load(conn)
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}\n{sql}"));
    lines
        .into_iter()
        .map(|l| l.line)
        .collect::<Vec<_>>()
        .join("\n")
}

/// The candidate scan uses the retention index in both of its variants.
#[tokio::test(flavor = "multi_thread")]
async fn retention_candidate_scan_uses_its_partial_index() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    seed_executions(&mut conn).await;
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .expect("disable sequential scans");

    for has_global_age in [true, false] {
        let sql = with_literal_binds(candidate_scan_sql(has_global_age), has_global_age);
        let plan = explain(&mut conn, &sql).await;
        assert!(
            plan.contains(RETENTION_INDEX),
            "the candidate scan (global age: {has_global_age}) must use \
             {RETENTION_INDEX}. Its state list must imply the index predicate.\n{plan}"
        );
        assert!(
            !plan.contains("Seq Scan on harvest_workflow_executions"),
            "the candidate scan (global age: {has_global_age}) read the whole table:\n{plan}"
        );
    }
}
