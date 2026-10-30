#![cfg(feature = "db")]
//! Plan gates for the plugin census findings (issue #2424).
//!
//! The Ledger census found four statements that read a whole table:
//!
//! - the per-queue task counts that an autoscaler polls;
//! - the canary rows of `GET /admin/canary`;
//! - the cross-shard child check of a shard rebalance;
//! - the timer copy and delete of a shard rebalance.
//!
//! Each gate plans the statement text that the engine runs, with
//! `enable_seqscan` off. The cutover gate plans `QUIESCENCE_SQL` inside a
//! `SELECT`, not the whole cutover. The erasure gate plans the Diesel query
//! that `erase.rs` builds. The planner then takes any usable index. So a `Seq Scan` that remains
//! means that no index can serve the statement. The gates use a generic plan,
//! so a bound value cannot make a partial index usable.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::canary::CANARY_ROWS_SQL;
use autumn_harvest::queue::{QUEUE_TASK_COUNTS_SQL, QueueTaskCounts, queue_task_counts};
use autumn_harvest::shard_rebalance::{
    OBSERVE_SQL, QUIESCENCE_SQL, STAGED_CHILD_DELETES, STAGED_TIMERS_SQL,
};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz};
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use crate::claim_bench_support::db as claim_bench_db;

#[derive(QueryableByName, Debug)]
struct PlanLine {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    line: String,
}

/// Provision an isolated, fully migrated database.
///
/// A plan gate that skips itself reports success over the defect it exists to
/// catch. So a database that cannot be provisioned fails the gate.
async fn plan_gate_db() -> claim_bench_db::BenchDb {
    claim_bench_db::setup_bench_db().await.unwrap_or_else(|r| {
        panic!(
            "the census plan gates need an isolated database, from \
             HARVEST_TEST_DATABASE_URL (an admin connection string) or a reachable \
             Docker daemon: {}",
            r.0
        )
    })
}

/// Return the generic plan of `sql` with sequential scans off.
///
/// Diesel sends a query with the extended protocol, which needs a value for
/// each parameter. So the gate prepares the statement with `PREPARE` and
/// forces its generic plan.
async fn generic_plan(conn: &mut AsyncPgConnection, sql: &str) -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let name = format!("census_gate_{}", NEXT.fetch_add(1, Ordering::Relaxed));
    // Postgres numbers parameters from `$1` with no gap. A gated statement
    // binds fewer than 10.
    let params = (1..10)
        .take_while(|n| sql.contains(&format!("${n}")))
        .count();
    conn.batch_execute(&format!(
        "SET enable_seqscan = off; \
         SET plan_cache_mode = force_generic_plan; \
         PREPARE {name} AS {sql}"
    ))
    .await
    .unwrap_or_else(|e| panic!("PREPARE failed: {e}\n{sql}"));
    let args = if params == 0 {
        String::new()
    } else {
        format!("({})", vec!["NULL"; params].join(", "))
    };
    let lines: Vec<PlanLine> =
        diesel::sql_query(format!("EXPLAIN (COSTS OFF) EXECUTE {name}{args}"))
            .load(conn)
            .await
            .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}\n{sql}"));
    conn.batch_execute(&format!(
        "DEALLOCATE {name}; RESET enable_seqscan; RESET plan_cache_mode"
    ))
    .await
    .expect("deallocate the gate statement");
    lines
        .into_iter()
        .map(|l| l.line)
        .collect::<Vec<_>>()
        .join("\n")
}

/// Assert that `plan` reads `table` through `index` and never scans it.
fn assert_served(plan: &str, table: &str, index: &str, what: &str) {
    assert!(
        !plan.contains(&format!("Seq Scan on {table}")),
        "{what} reads the whole of {table}:\n{plan}"
    );
    assert!(
        plan.contains(index),
        "{what} must read {table} through {index}:\n{plan}"
    );
}

/// The autoscaler count reads live task rows only.
#[tokio::test(flavor = "multi_thread")]
async fn queue_task_counts_read_only_live_tasks() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let plan = generic_plan(&mut conn, QUEUE_TASK_COUNTS_SQL).await;
    assert_served(
        &plan,
        "harvest_task_queue",
        "idx_harvest_tq_live_created",
        "queue_task_counts",
    );
}

/// Append to `sql` an `INSERT` of `count` tasks of one queue in one state.
fn seed_tasks(sql: &mut String, queue: &str, state: &str, scheduled_at: &str, count: usize) {
    write!(
        sql,
        "INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, input, state, priority, max_attempts, scheduled_at) \
         SELECT gen_random_uuid(), '{queue}', 'workflow', '{{}}'::jsonb, '{state}', 0, 1, \
                {scheduled_at} \
         FROM generate_series(1, {count});"
    )
    .expect("write to a String");
}

/// The counts of each live state stay exact. A queue with only finished tasks
/// has no task row to report.
#[tokio::test(flavor = "multi_thread")]
async fn queue_task_counts_count_each_live_state() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let due = "NOW() - INTERVAL '1 minute'";
    let future = "NOW() + INTERVAL '1 hour'";
    let mut sql = String::new();
    seed_tasks(&mut sql, "alpha", "PENDING", due, 2);
    seed_tasks(&mut sql, "alpha", "PENDING", future, 1);
    seed_tasks(&mut sql, "alpha", "RUNNING", due, 3);
    for state in ["COMPLETED", "FAILED", "CANCELLED"] {
        seed_tasks(&mut sql, "alpha", state, due, 2);
        seed_tasks(&mut sql, "finished_only", state, due, 2);
    }
    seed_tasks(&mut sql, "beta", "RUNNING", due, 1);
    conn.batch_execute(&sql).await.expect("seed tasks");

    let mut counts = queue_task_counts(&mut conn)
        .await
        .expect("queue_task_counts");
    counts.sort_by(|a, b| a.queue.cmp(&b.queue));

    let expected = vec![
        QueueTaskCounts {
            queue: "alpha".to_owned(),
            backlog: 2,
            in_flight: 3,
            scheduled: 1,
        },
        QueueTaskCounts {
            queue: "beta".to_owned(),
            backlog: 0,
            in_flight: 1,
            scheduled: 0,
        },
    ];
    assert_eq!(counts, expected);
}

/// The canary read stops after the newest canary rows.
#[tokio::test(flavor = "multi_thread")]
async fn canary_rows_read_the_canary_index() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let plan = generic_plan(&mut conn, CANARY_ROWS_SQL).await;
    assert_served(
        &plan,
        "harvest_workflow_executions",
        "idx_harvest_we_canary_completed",
        "the canary rows",
    );
}

#[derive(QueryableByName, Debug)]
struct CanaryRow {
    #[diesel(sql_type = Text)]
    queue_name: String,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// The canary read returns canary runs only, newest first, and honours its cap.
///
/// A `LIKE` form treats each `_` as a wildcard. So the lookalike names below
/// catch a predicate that matches more than the prefix.
#[tokio::test(flavor = "multi_thread")]
async fn canary_rows_match_the_prefix_newest_first() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let prefix = autumn_harvest::canary::CANARY_WORKFLOW_NAME_PREFIX;
    let rows = [
        (
            format!("{prefix}__q1"),
            "q1",
            "COMPLETED",
            "NOW() - INTERVAL '3 minutes'",
        ),
        (
            format!("{prefix}__q2"),
            "q2",
            "COMPLETED",
            "NOW() - INTERVAL '1 minute'",
        ),
        (format!("{prefix}__q3"), "q3", "RUNNING", "NULL"),
        (
            format!("{prefix}__q4"),
            "q4",
            "FAILED",
            "NOW() - INTERVAL '2 minutes'",
        ),
        (
            "xxharvest_canary_probe__q5".to_owned(),
            "q5",
            "COMPLETED",
            "NOW()",
        ),
        (
            "__harvestXcanaryXprobe__q6".to_owned(),
            "q6",
            "COMPLETED",
            "NOW()",
        ),
        ("ordinary".to_owned(), "q7", "COMPLETED", "NOW()"),
    ];
    let mut sql = String::new();
    for (index, (name, queue, state, completed_at)) in rows.iter().enumerate() {
        write!(
            sql,
            "INSERT INTO harvest_workflow_executions \
                 (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
                  started_at, created_at, completed_at) \
             VALUES (gen_random_uuid(), '{name}', 'canary_{index}', gen_random_uuid(), 0, \
                     '{state}', '{{}}'::jsonb, '{queue}', NOW(), NOW(), {completed_at});"
        )
        .expect("write to a String");
    }
    conn.batch_execute(&sql).await.expect("seed executions");

    let read = |cap: i64| diesel::sql_query(CANARY_ROWS_SQL).bind::<BigInt, _>(cap);
    let all: Vec<CanaryRow> = read(500).load(&mut conn).await.expect("canary rows");
    let queues: Vec<&str> = all.iter().map(|r| r.queue_name.as_str()).collect();
    assert_eq!(queues, ["q2", "q4", "q1", "q3"]);
    assert!(all[3].completed_at.is_none(), "a running canary sorts last");

    let capped: Vec<CanaryRow> = read(2).load(&mut conn).await.expect("capped canary rows");
    let queues: Vec<&str> = capped.iter().map(|r| r.queue_name.as_str()).collect();
    assert_eq!(queues, ["q2", "q4"]);
}

/// The rebalance reads cross-shard children by parent, and timers by execution.
#[tokio::test(flavor = "multi_thread")]
async fn rebalance_reads_children_and_timers_by_execution() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let children = "harvest_cross_shard_children";
    let children_index = "idx_harvest_cross_shard_children_parent";
    let plan = generic_plan(&mut conn, OBSERVE_SQL).await;
    assert_served(&plan, children, children_index, "observe_quiescence");

    let cutover = format!(
        "SELECT e.id FROM harvest_workflow_executions e WHERE e.id = $1 AND {QUIESCENCE_SQL}"
    );
    let plan = generic_plan(&mut conn, &cutover).await;
    assert_served(&plan, children, children_index, "the cutover re-check");

    let timers = "harvest_timers";
    let timers_index = "idx_harvest_timers_exec";
    let plan = generic_plan(&mut conn, STAGED_TIMERS_SQL).await;
    assert_served(&plan, timers, timers_index, "the staged timer copy");

    let delete = STAGED_CHILD_DELETES
        .iter()
        .find(|sql| sql.contains(timers))
        .expect("the staged deletes remove timers");
    let plan = generic_plan(&mut conn, delete).await;
    assert_served(&plan, timers, timers_index, "the staged timer delete");
}

/// The PII erasure reads the cross-shard children of one parent by index.
///
/// This is the query of `erase::collect_cross_shard_child_ids`.
#[tokio::test(flavor = "multi_thread")]
async fn erasure_reads_children_by_parent() {
    use autumn_harvest::schema::harvest_cross_shard_children as x;
    use diesel::prelude::*;

    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let query = x::table
        .filter(x::parent_exec_id.eq(uuid::Uuid::nil()))
        .select((x::child_exec_id, x::target_shard, x::status));
    let sql = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
    let sql = sql.split(" -- binds:").next().expect("a query text");
    let plan = generic_plan(&mut conn, sql).await;
    assert_served(
        &plan,
        "harvest_cross_shard_children",
        "idx_harvest_cross_shard_children_parent",
        "the erasure child lookup",
    );
}
