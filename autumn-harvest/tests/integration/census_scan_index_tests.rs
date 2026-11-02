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
//! The same gates cover the `harvest_signals` finding (issue #2367). A
//! workflow retry, a PII erasure, a shard rebalance and an execution delete
//! each reach every signal of one execution, consumed rows included. The
//! pending index skips consumed rows. The idempotency index skips rows with no
//! key. So neither index can serve these statements.
//!
//! The same gates cover the `harvest_dead_letters` finding (issue #2401). No
//! index held `failed_at`, `original_task_id` or `queue_name`. So the DLQ
//! list, the callback redrive delete, each queue filter and the newest-age
//! probe of the status summary read the whole table.
//!
//! Most gates plan the statement text that the engine runs, with
//! `enable_seqscan` off and a generic plan. The planner then takes any usable
//! index, and a bound value cannot make a partial index usable. The deciding
//! check is the index name. With no usable index, the planner can also choose
//! a full scan of the primary key instead of a `Seq Scan`.
//!
//! These gates differ:
//!
//! - The cutover gate plans `QUIESCENCE_SQL` inside a `SELECT`, not the whole
//!   cutover.
//! - The erasure child gate plans the Diesel query that `erase.rs` builds.
//! - The retry, erasure signal, pending-signal and DLQ gates capture the SQL
//!   that the engine function sends. The newest-age gate plans
//!   `DLQ_NEWEST_AGE_SQL`, which the plugin sends.
//! - The cascade gate deletes an execution and counts the scans of the index.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use autumn_harvest::canary::CANARY_ROWS_SQL;
use autumn_harvest::queue::{QUEUE_TASK_COUNTS_SQL, QueueTaskCounts, queue_task_counts};
use autumn_harvest::shard_rebalance::{
    OBSERVE_SQL, QUIESCENCE_SQL, STAGED_CHILD_DELETES, STAGED_SIGNALS_SQL, STAGED_TIMERS_SQL,
};
use autumn_harvest::types::ExecutionId;
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

/// The table of the issue #2367 gates.
const SIGNALS: &str = "harvest_signals";

/// The index that serves each read of all signals of one execution.
const SIGNALS_EXEC_INDEX: &str = "idx_harvest_signals_exec";

/// Record the SQL of each statement that `conn` sends from now on.
fn record_statements(conn: &mut AsyncPgConnection) -> Arc<Mutex<Vec<String>>> {
    use diesel::connection::InstrumentationEvent;
    use diesel_async::AsyncConnection;
    let log = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&log);
    conn.set_instrumentation(move |event: InstrumentationEvent<'_>| {
        if let InstrumentationEvent::StartQuery { query, .. } = event {
            sink.lock().expect("statement log").push(query.to_string());
        }
    });
    log
}

/// Take the one recorded statement that starts with `prefix`, with no binds.
fn take_statement(log: &Mutex<Vec<String>>, prefix: &str) -> String {
    let [sql] = take_statements(log, [prefix]);
    sql
}

/// Take one recorded statement for each prefix, with no binds.
///
/// The call empties the log. So a test that needs two statements of one call
/// takes both here.
fn take_statements<const N: usize>(log: &Mutex<Vec<String>>, prefixes: [&str; N]) -> [String; N] {
    let statements = std::mem::take(&mut *log.lock().expect("statement log"));
    prefixes.map(|prefix| {
        let matches: Vec<&str> = statements
            .iter()
            .filter(|sql| sql.starts_with(prefix))
            .map(|sql| sql.split(" -- binds:").next().expect("a query text"))
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "expected one statement that starts with {prefix}:\n{statements:#?}"
        );
        matches[0].to_owned()
    })
}

/// A workflow retry moves every signal of the failed attempt by index.
///
/// The gate plans the statement that `forward_signals_to_retry_attempt` sends.
/// `retry_chain_routing_tests` checks that consumed rows move too.
#[tokio::test(flavor = "multi_thread")]
async fn retry_forwarding_reads_signals_by_execution() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let log = record_statements(&mut conn);
    autumn_harvest::signal::forward_signals_to_retry_attempt(
        &mut conn,
        ExecutionId::new(),
        ExecutionId::new(),
    )
    .await
    .expect("forward an empty mailbox");
    let sql = take_statement(&log, "UPDATE \"harvest_signals\"");

    let plan = generic_plan(&mut conn, &sql).await;
    assert_served(&plan, SIGNALS, SIGNALS_EXEC_INDEX, "the retry signal move");
}

#[derive(QueryableByName, Debug)]
struct SignalPayload {
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    payload: serde_json::Value,
}

/// A PII erasure tombstones every signal of an execution by index.
///
/// The erasure must reach a consumed signal too. Its payload is still PII.
#[tokio::test(flavor = "multi_thread")]
async fn erasure_scrubs_every_signal_by_execution() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let exec_id = ExecutionId::new();
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at, completed_at) \
         VALUES ('{exec_id}', 'erase_signals', 'erase_signals', gen_random_uuid(), 0, \
                 'COMPLETED', '{{}}'::jsonb, 'default', NOW(), NOW(), NOW()); \
         INSERT INTO harvest_signals (workflow_exec_id, signal_name, payload, consumed) \
         VALUES ('{exec_id}', 'seen', '{{\"pii\": 1}}'::jsonb, true), \
                ('{exec_id}', 'queued', '{{\"pii\": 2}}'::jsonb, false);"
    ))
    .await
    .expect("seed a completed execution with two signals");

    let log = record_statements(&mut conn);
    let outcome = autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id, "gate")
        .await
        .expect("erase the execution");
    let sql = take_statement(&log, "UPDATE \"harvest_signals\"");
    assert_eq!(outcome.signals_scrubbed, 2, "both signals are scrubbed");

    let payloads: Vec<SignalPayload> =
        diesel::sql_query("SELECT payload FROM harvest_signals WHERE workflow_exec_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .load(&mut conn)
            .await
            .expect("load the signal payloads");
    assert_eq!(payloads.len(), 2, "the erasure keeps each signal row");
    for row in &payloads {
        assert!(
            autumn_harvest::erase::is_erasure_tombstone(&row.payload),
            "a signal payload survives the erasure: {}",
            row.payload
        );
    }

    let plan = generic_plan(&mut conn, &sql).await;
    assert_served(
        &plan,
        SIGNALS,
        SIGNALS_EXEC_INDEX,
        "the erasure signal scrub",
    );
}

/// The rebalance copies and deletes the signals of one execution by index.
#[tokio::test(flavor = "multi_thread")]
async fn rebalance_reads_signals_by_execution() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let plan = generic_plan(&mut conn, STAGED_SIGNALS_SQL).await;
    assert_served(&plan, SIGNALS, SIGNALS_EXEC_INDEX, "the staged signal copy");

    let delete = STAGED_CHILD_DELETES
        .iter()
        .find(|sql| sql.contains(SIGNALS))
        .expect("the staged deletes remove signals");
    let plan = generic_plan(&mut conn, delete).await;
    assert_served(
        &plan,
        SIGNALS,
        SIGNALS_EXEC_INDEX,
        "the staged signal delete",
    );
}

#[derive(QueryableByName, Debug)]
struct ScanCount {
    #[diesel(sql_type = BigInt)]
    scans: i64,
}

/// The `ON DELETE CASCADE` from an execution deletes its signals by index.
///
/// Retention deletes executions. The foreign key trigger then deletes the
/// signals of each one. The gate runs a real delete with `enable_seqscan` off.
/// It then counts the scans of the index in the same transaction.
#[tokio::test(flavor = "multi_thread")]
async fn execution_delete_cascades_to_signals_by_execution() {
    use diesel_async::AsyncConnection;

    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let exec_id = ExecutionId::new();
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at, completed_at) \
         VALUES ('{exec_id}', 'cascade_signals', 'cascade_signals', gen_random_uuid(), 0, \
                 'COMPLETED', '{{}}'::jsonb, 'default', NOW(), NOW(), NOW()); \
         INSERT INTO harvest_signals (workflow_exec_id, signal_name, payload, consumed) \
         VALUES ('{exec_id}', 'seen', '{{}}'::jsonb, true), \
                ('{exec_id}', 'queued', '{{}}'::jsonb, false);"
    ))
    .await
    .expect("seed a completed execution with two signals");

    let scans = conn
        .transaction::<i64, diesel::result::Error, _>(async |conn| {
            conn.batch_execute("SET LOCAL enable_seqscan = off").await?;
            diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
                .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
                .execute(conn)
                .await?;
            let row: ScanCount = diesel::sql_query(format!(
                "SELECT COALESCE(pg_stat_get_xact_numscans(\
                     to_regclass('{SIGNALS_EXEC_INDEX}')), 0) AS scans"
            ))
            .get_result(conn)
            .await?;
            Ok(row.scans)
        })
        .await
        .expect("delete the execution");

    assert!(
        scans > 0,
        "the execution delete cascade reads the whole of {SIGNALS}, not {SIGNALS_EXEC_INDEX}"
    );
    let left: Vec<SignalPayload> =
        diesel::sql_query("SELECT payload FROM harvest_signals WHERE workflow_exec_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .load(&mut conn)
            .await
            .expect("load the signals left");
    assert!(left.is_empty(), "the cascade deletes each signal");
}

/// A pending-signal read keeps the partial index under a generic plan.
///
/// A bound `consumed = $2` cannot match the predicate `WHERE NOT consumed`.
/// After five calls, Postgres can switch a cached statement to its generic
/// plan. That plan then reads every signal of the run through
/// `idx_harvest_signals_exec`. A literal `NOT consumed` keeps the small index.
#[tokio::test(flavor = "multi_thread")]
async fn pending_signal_reads_keep_the_partial_index() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let log = record_statements(&mut conn);
    let exec_id = ExecutionId::new();
    autumn_harvest::signal::has_pending_signals(&mut conn, exec_id)
        .await
        .expect("has_pending_signals");
    let exists = take_statement(&log, "SELECT EXISTS");
    autumn_harvest::signal::load_pending_signals(&mut conn, exec_id)
        .await
        .expect("load_pending_signals");
    let load = take_statement(&log, "SELECT");

    for (sql, what) in [
        (exists, "has_pending_signals"),
        (load, "load_pending_signals"),
    ] {
        let plan = generic_plan(&mut conn, &sql).await;
        assert_served(&plan, SIGNALS, "idx_harvest_signals_pending", what);
        assert!(
            !plan.contains(SIGNALS_EXEC_INDEX),
            "{what} reads every signal of the run:\n{plan}"
        );
    }
}

/// No query binds the value of `harvest_signals.consumed` in a filter.
///
/// Only `harvest_signals` has a `consumed` column. The literal form
/// `diesel::dsl::not(consumed)` matches the partial indexes in a generic plan.
#[test]
fn no_query_binds_the_consumed_flag() {
    fn scan(dir: &std::path::Path, found: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).expect("read a source directory") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                scan(&path, found);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).expect("read a source file");
                for (number, line) in text.lines().enumerate() {
                    if line.contains(".filter(") && line.contains("consumed.eq(false)") {
                        found.push(format!("{}:{}", path.display(), number + 1));
                    }
                }
            }
        }
    }

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the workspace root");
    let mut found = Vec::new();
    for entry in std::fs::read_dir(root).expect("read the workspace root") {
        let src = entry.expect("a directory entry").path().join("src");
        if src.is_dir() {
            scan(&src, &mut found);
        }
    }
    assert!(
        found.is_empty(),
        "use diesel::dsl::not(consumed), not consumed.eq(false):\n{}",
        found.join("\n")
    );
}

/// The table of the issue #2401 gates.
const DLQ: &str = "harvest_dead_letters";

/// The index that serves the newest-first list and the newest-age probe.
const DLQ_FAILED_AT_INDEX: &str = "idx_harvest_dl_failed_at";

/// The index that serves the callback redrive delete.
const DLQ_ORIGINAL_TASK_INDEX: &str = "idx_harvest_dl_original_task";

/// The index that serves each filter on `queue_name`.
const DLQ_QUEUE_INDEX: &str = "idx_harvest_dl_queue_failed";

/// Append to `sql` an `INSERT` of one dead letter.
///
/// `error` names the row, so a test can check the order of a read.
fn seed_dead_letter(
    sql: &mut String,
    original_task_id: uuid::Uuid,
    queue: &str,
    task_type: &str,
    error: &str,
    failed_at: &str,
) {
    write!(
        sql,
        "INSERT INTO harvest_dead_letters \
             (original_task_id, queue_name, task_type, input, error, attempts, failed_at) \
         VALUES ('{original_task_id}', '{queue}', '{task_type}', '{{}}'::jsonb, '{error}', 1, \
                 {failed_at});"
    )
    .expect("write to a String");
}

/// Append to `sql` an `INSERT` of the dead letter of one callback delivery.
///
/// The row has the queue name and the execution id that an exhausted
/// delivery writes.
fn seed_callback_dead_letter(
    sql: &mut String,
    delivery_id: uuid::Uuid,
    exec_id: ExecutionId,
    error: &str,
) {
    write!(
        sql,
        "INSERT INTO harvest_dead_letters \
             (original_task_id, queue_name, task_type, workflow_exec_id, input, error, attempts) \
         VALUES ('{delivery_id}', 'completion-callback', 'CALLBACK', '{exec_id}', '{{}}'::jsonb, \
                 '{error}', 1);"
    )
    .expect("write to a String");
}

/// Assert that `plan` stops early: a backward index scan under a `Limit`.
///
/// `assert_served` alone also accepts a walk of the whole index.
fn assert_newest_first_and_bounded(plan: &str, what: &str) {
    assert!(
        plan.contains("Limit") && plan.contains("Backward"),
        "{what} must read a backward scan under a Limit:\n{plan}"
    );
}

/// The DLQ list reads the newest rows by index, newest first, up to its limit.
#[tokio::test(flavor = "multi_thread")]
async fn dead_letter_list_reads_the_newest_rows_by_index() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let mut sql = String::new();
    for (error, age) in [("old", "3"), ("newest", "1"), ("middle", "2")] {
        let failed_at = format!("NOW() - INTERVAL '{age} minutes'");
        seed_dead_letter(
            &mut sql,
            uuid::Uuid::new_v4(),
            "alpha",
            "activity",
            error,
            &failed_at,
        );
    }
    conn.batch_execute(&sql).await.expect("seed dead letters");

    let log = record_statements(&mut conn);
    let rows = autumn_harvest::dlq::list_dead_letters(&mut conn, 2, None)
        .await
        .expect("list dead letters");
    let list = take_statement(&log, "SELECT \"harvest_dead_letters\"");
    let errors: Vec<&str> = rows.iter().map(|r| r.error.as_str()).collect();
    assert_eq!(errors, ["newest", "middle"]);

    let plan = generic_plan(&mut conn, &list).await;
    assert_served(&plan, DLQ, DLQ_FAILED_AT_INDEX, "list_dead_letters");
    assert_newest_first_and_bounded(&plan, "list_dead_letters");
}

#[derive(QueryableByName, Debug)]
struct DeadLetterAge {
    #[diesel(sql_type = Nullable<BigInt>)]
    age: Option<i64>,
}

/// The newest-age probe of the status summary reads one row by index.
///
/// The probe gives `NULL` for an empty DLQ, and the age of the newest row.
#[tokio::test(flavor = "multi_thread")]
async fn dead_letter_newest_age_reads_one_row_by_index() {
    use autumn_harvest::dlq::DLQ_NEWEST_AGE_SQL;

    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let empty: DeadLetterAge = diesel::sql_query(DLQ_NEWEST_AGE_SQL)
        .get_result(&mut conn)
        .await
        .expect("the age of an empty DLQ");
    assert_eq!(empty.age, None, "an empty DLQ has no age");

    let mut sql = String::new();
    for (error, failed_at) in [
        ("old", "NOW() - INTERVAL '1 hour'"),
        ("new", "NOW() - INTERVAL '2 minutes'"),
    ] {
        seed_dead_letter(
            &mut sql,
            uuid::Uuid::new_v4(),
            "alpha",
            "activity",
            error,
            failed_at,
        );
    }
    conn.batch_execute(&sql).await.expect("seed dead letters");
    let newest: DeadLetterAge = diesel::sql_query(DLQ_NEWEST_AGE_SQL)
        .get_result(&mut conn)
        .await
        .expect("the age of the newest dead letter");
    let age = newest.age.expect("a DLQ with rows has an age");
    assert!(
        (120..1800).contains(&age),
        "the age comes from the newest row: {age}"
    );

    let plan = generic_plan(&mut conn, DLQ_NEWEST_AGE_SQL).await;
    assert_served(&plan, DLQ, DLQ_FAILED_AT_INDEX, "the newest-age probe");
    assert_newest_first_and_bounded(&plan, "the newest-age probe");
}

#[derive(QueryableByName, Debug)]
struct DeadLetterError {
    #[diesel(sql_type = Text)]
    error: String,
}

/// A callback redrive deletes its own dead letter by index, and no other row.
///
/// The seed shares the delivery id with an activity row. It also has a
/// callback row of another delivery. Both rows stay.
#[tokio::test(flavor = "multi_thread")]
async fn callback_redrive_deletes_its_dead_letter_by_index() {
    use autumn_harvest::completion_callback::{DeliveryRedriveOutcome, redrive_delivery_at};

    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let exec_id = ExecutionId::new();
    let delivery_id = uuid::Uuid::new_v4();
    let mut sql = format!(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at, completed_at) \
         VALUES ('{exec_id}', 'callback_redrive', 'callback_redrive', gen_random_uuid(), 0, \
                 'FAILED', '{{}}'::jsonb, 'default', NOW(), NOW(), NOW()); \
         INSERT INTO harvest_completion_deliveries \
             (id, workflow_exec_id, shard_id, callback_index, workflow_name, workflow_id, \
              target_url, event_filter, terminal_state, payload, state, attempt, max_attempts, \
              retry_policy, next_attempt_at) \
         VALUES ('{delivery_id}', '{exec_id}', 0, 0, 'callback_redrive', 'callback_redrive', \
                 'https://example.test/hook', '{{}}'::jsonb, 'failed', '{{}}'::jsonb, \
                 'FAILED', 1, 1, '{{}}'::jsonb, NOW());"
    );
    seed_callback_dead_letter(&mut sql, delivery_id, exec_id, "this delivery");
    seed_dead_letter(
        &mut sql,
        delivery_id,
        "alpha",
        "activity",
        "same id, activity",
        "NOW()",
    );
    seed_callback_dead_letter(&mut sql, uuid::Uuid::new_v4(), exec_id, "another delivery");
    conn.batch_execute(&sql)
        .await
        .expect("seed a failed delivery and three dead letters");

    let log = record_statements(&mut conn);
    let outcome = redrive_delivery_at(&mut conn, exec_id, delivery_id, None)
        .await
        .expect("redrive the delivery");
    let delete = take_statement(&log, "DELETE FROM \"harvest_dead_letters\"");
    assert_eq!(outcome, DeliveryRedriveOutcome::Redriven);

    let mut left: Vec<String> = diesel::sql_query("SELECT error FROM harvest_dead_letters")
        .load::<DeadLetterError>(&mut conn)
        .await
        .expect("load the dead letters left")
        .into_iter()
        .map(|r| r.error)
        .collect();
    left.sort();
    assert_eq!(left, ["another delivery", "same id, activity"]);

    let plan = generic_plan(&mut conn, &delete).await;
    assert_served(
        &plan,
        DLQ,
        DLQ_ORIGINAL_TASK_INDEX,
        "the callback redrive delete",
    );
}

/// Seed two queues of dead letters. `alpha` holds three, `beta` holds one.
async fn seed_two_queues(conn: &mut AsyncPgConnection) {
    let mut sql = String::new();
    for (queue, error, age) in [
        ("alpha", "alpha middle", "2"),
        ("beta", "beta oldest", "9"),
        ("alpha", "alpha oldest", "3"),
        ("alpha", "alpha newest", "1"),
    ] {
        let failed_at = format!("NOW() - INTERVAL '{age} minutes'");
        seed_dead_letter(
            &mut sql,
            uuid::Uuid::new_v4(),
            queue,
            "activity",
            error,
            &failed_at,
        );
    }
    conn.batch_execute(&sql).await.expect("seed dead letters");
}

/// Return the `error` of each dead letter in `ids`, in the order of `ids`.
async fn errors_of(conn: &mut AsyncPgConnection, ids: &[String]) -> Vec<String> {
    let mut errors = Vec::with_capacity(ids.len());
    for id in ids {
        let id: uuid::Uuid = id.parse().expect("a dead-letter id");
        let row: DeadLetterError =
            diesel::sql_query("SELECT error FROM harvest_dead_letters WHERE id = $1")
                .bind::<diesel::sql_types::Uuid, _>(id)
                .get_result(conn)
                .await
                .expect("load a dead letter");
        errors.push(row.error);
    }
    errors
}

/// Assert that the queue filter of each statement reads the queue index.
async fn assert_queue_served(conn: &mut AsyncPgConnection, statements: [(String, &str); 2]) {
    for (sql, what) in statements {
        let plan = generic_plan(conn, &sql).await;
        assert_served(&plan, DLQ, DLQ_QUEUE_INDEX, what);
    }
}

/// A redrive by queue counts and reads that queue by index, oldest first.
#[tokio::test(flavor = "multi_thread")]
async fn queue_redrive_reads_the_queue_by_index() {
    use autumn_harvest::dlq::{RedriveFilter, redrive_dead_letters};

    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    seed_two_queues(&mut conn).await;

    let filter = RedriveFilter {
        queue: Some("alpha".to_owned()),
        max: Some(2),
        dry_run: true,
        ..RedriveFilter::default()
    };
    let log = record_statements(&mut conn);
    let result = redrive_dead_letters(
        &mut conn,
        &filter,
        None,
        None,
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("a dry-run redrive");
    let [count, read] =
        take_statements(&log, ["SELECT COUNT(*)", "SELECT \"harvest_dead_letters\""]);
    assert_eq!(result.matched, 3, "the count ignores the cap");

    let errors = errors_of(&mut conn, &result.ids).await;
    assert_eq!(errors, ["alpha oldest", "alpha middle"]);

    assert_queue_served(
        &mut conn,
        [
            (count, "count_redrive_filter_matches"),
            (read, "query_dead_letters_for_redrive"),
        ],
    )
    .await;
}

/// A bulk replay by queue counts and reads that queue by index, oldest first.
#[tokio::test(flavor = "multi_thread")]
async fn queue_bulk_replay_reads_the_queue_by_index() {
    use autumn_harvest::dlq::{BulkDlqFilter, bulk_replay_dead_letters};

    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    seed_two_queues(&mut conn).await;

    let filter = BulkDlqFilter {
        queue_name: Some("alpha".to_owned()),
        limit: Some(2),
        dry_run: true,
        ..BulkDlqFilter::default()
    };
    let log = record_statements(&mut conn);
    let result = bulk_replay_dead_letters(&mut conn, &filter, None)
        .await
        .expect("a dry-run bulk replay");
    let [count, read] =
        take_statements(&log, ["SELECT COUNT(*)", "SELECT \"harvest_dead_letters\""]);
    assert_eq!(result.matched, 3, "the count ignores the limit");
    let errors = errors_of(&mut conn, &result.ids).await;
    assert_eq!(errors, ["alpha oldest", "alpha middle"]);

    assert_queue_served(
        &mut conn,
        [
            (count, "count_bulk_filter_matches"),
            (read, "query_dead_letters_for_bulk"),
        ],
    )
    .await;
}

/// A DLQ aggregate by queue reads that queue by index.
///
/// The unfiltered total still counts every row. That count is inherent.
#[tokio::test(flavor = "multi_thread")]
async fn queue_aggregate_reads_the_queue_by_index() {
    use autumn_harvest::dlq::{DlqAggregateParams, aggregate_dead_letters};

    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    seed_two_queues(&mut conn).await;

    let params = DlqAggregateParams {
        queue_name: Some("alpha".to_owned()),
        ..DlqAggregateParams::default()
    };
    let log = record_statements(&mut conn);
    let partial = aggregate_dead_letters(&mut conn, &params)
        .await
        .expect("aggregate the DLQ");
    let read = take_statement(&log, "SELECT \"harvest_dead_letters\"");
    assert_eq!(partial.total, 4, "the total counts every queue");
    assert_eq!(partial.filtered_total, 3, "the filter keeps one queue");

    let plan = generic_plan(&mut conn, &read).await;
    assert_served(&plan, DLQ, DLQ_QUEUE_INDEX, "aggregate_dead_letters");
}
