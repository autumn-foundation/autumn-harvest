#![cfg(feature = "db")]
//! Gates for `idx_harvest_tq_concurrency_live`.
//!
//! `queue::concurrency_key_stats` feeds the concurrency gauges. It reads the
//! live rows that hold a concurrency key and a cap. Before the index, no
//! index held the `PENDING` rows with a key. So each call read the whole task
//! queue. `docs/performance-concurrency-live-index.md` records the cost.
//!
//! The plan gates plan the statement text that the engine sends, with
//! `enable_seqscan` off and a generic plan. The deciding check is the index
//! name.
//!
//! The buffer gates plan each statement as the engine does. The gauge read is
//! an unnamed statement, so it gets a custom plan. The claim applies
//! `CLAIM_PLAN_SETTINGS_SQL`, so its pending keys get a generic plan with
//! bitmap scans off. The gates seed many rows with no key and a few keyed
//! rows. One call must then read a number of buffers near the keyed rows, not
//! near the table. A plan gate cannot cover the pending keys of the full
//! claim. On an empty table, other indexes can serve that step too, so the
//! planner may pick any of them.

use std::fmt::Write as _;

use autumn_harvest::queue::{
    self, CLAIM_PLAN_SETTINGS_SQL, CONCURRENCY_ATTRIBUTION_SQL, CONCURRENCY_STATS_SQL,
};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};

use crate::census_scan_index_tests::{assert_served, generic_plan, plan_gate_db};
use crate::claim_bench_support::db as claim_bench_db;

/// The index under test.
const INDEX: &str = "idx_harvest_tq_concurrency_live";

/// The gauge read uses the live keyed index.
#[tokio::test(flavor = "multi_thread")]
async fn concurrency_stats_read_the_live_keyed_index() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let plan = generic_plan(&mut conn, CONCURRENCY_STATS_SQL).await;
    assert_served(&plan, "harvest_task_queue", INDEX, "concurrency_key_stats");
}

/// The admin attribution read shares the row filter, so it uses the same
/// index.
#[tokio::test(flavor = "multi_thread")]
async fn concurrency_attribution_reads_the_live_keyed_index() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;

    let plan = generic_plan(&mut conn, CONCURRENCY_ATTRIBUTION_SQL).await;
    assert_served(
        &plan,
        "harvest_task_queue",
        INDEX,
        "concurrency_key_workflows",
    );
}

/// Return the body of the `concurrency_pending_keys` CTE of the full claim.
///
/// The body binds the queue list as `$2`. A prepared statement needs each
/// parameter from `$1`, so the body binds it as `$1` here.
fn pending_keys_sql() -> String {
    let full = queue::claim_task_query();
    let head = "concurrency_pending_keys AS MATERIALIZED (";
    let start = full
        .find(head)
        .expect("the full claim has the pending-keys CTE")
        + head.len();
    let mut depth = 1;
    let end = full[start..]
        .char_indices()
        .find_map(|(i, c)| {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                _ => {}
            }
            (depth == 0).then_some(start + i)
        })
        .expect("the pending-keys CTE closes");
    let body = full[start..end].to_owned();
    let only_queue_list = body.match_indices('$').all(|(i, _)| {
        let mut after = body[i + 1..].chars();
        after.next() == Some('2') && !after.next().is_some_and(|c| c.is_ascii_digit())
    });
    assert!(
        body.contains("$2") && only_queue_list,
        "the pending-keys CTE binds only the queue list: {body}"
    );
    body.replace("$2", "$1")
}

#[derive(QueryableByName)]
struct PlanLine {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    line: String,
}

#[derive(QueryableByName)]
struct Pages {
    #[diesel(sql_type = BigInt)]
    pages: i64,
}

/// Return the SQL of the gate fixture.
///
/// Queue `w` holds 40,000 `PENDING` and 20,000 `COMPLETED` rows with no key.
/// The keyed rows below give the groups that
/// [`concurrency_stats_count_only_live_capped_rows`] expects.
fn fixture_sql() -> String {
    let rows = [
        // (key, task type, state, worker, cap, queue, count)
        ("ck-a", "activity", "PENDING", "NULL", "5", "w", 20),
        ("ck-a", "activity", "RUNNING", "'w1'", "5", "w", 3),
        ("ck-a", "activity", "RUNNING", "NULL", "5", "w", 2),
        ("ck-a", "activity", "COMPLETED", "NULL", "5", "w", 2),
        ("ck-a", "workflow", "PENDING", "NULL", "7", "w", 4),
        ("ck-b", "activity", "PENDING", "NULL", "2", "w", 6),
        ("ck-b", "activity", "PENDING", "NULL", "9", "w", 1),
        ("ck-b", "activity", "RUNNING", "'w2'", "2", "w", 7),
        ("ck-nocap", "activity", "PENDING", "NULL", "NULL", "w", 5),
        ("ck-other", "activity", "PENDING", "NULL", "3", "x", 5),
    ];
    let mut sql = String::from(
        "INSERT INTO harvest_task_queue (queue_name, task_type, input, state) \
         SELECT 'w', 'activity', '{}'::jsonb, 'PENDING' FROM generate_series(1, 40000); \
         INSERT INTO harvest_task_queue (queue_name, task_type, input, state) \
         SELECT 'w', 'activity', '{}'::jsonb, 'COMPLETED' FROM generate_series(1, 20000); ",
    );
    for (key, task_type, state, worker, cap, queue_name, count) in rows {
        write!(
            sql,
            "INSERT INTO harvest_task_queue \
                 (queue_name, task_type, input, state, worker_id, concurrency_key, \
                  concurrency_cap) \
             SELECT '{queue_name}', '{task_type}', '{{}}'::jsonb, '{state}', {worker}, \
                    '{key}', {cap} \
             FROM generate_series(1, {count}); "
        )
        .expect("write to a String");
    }
    sql
}

/// Seed the gate fixture, then refresh the statistics and the visibility map.
async fn seed(conn: &mut diesel_async::AsyncPgConnection) {
    conn.batch_execute(&fixture_sql())
        .await
        .expect("seed the gate fixture");
    conn.batch_execute("VACUUM ANALYZE harvest_task_queue")
        .await
        .expect("vacuum the task queue");
}

/// Return the shared buffers that one call of `sql` reads on queue `w`, and
/// its plan.
///
/// `settings` holds `SET LOCAL` statements. They apply to the call only, as
/// the claim applies `CLAIM_PLAN_SETTINGS_SQL`. They choose the plan mode.
async fn call_buffers(
    conn: &mut diesel_async::AsyncPgConnection,
    settings: &str,
    sql: &str,
) -> (i64, String) {
    conn.batch_execute(&format!(
        "BEGIN; {settings}; \
         PREPARE concurrency_live_gate AS {sql}"
    ))
    .await
    .unwrap_or_else(|e| panic!("PREPARE failed: {e}\n{sql}"));
    let lines: Vec<PlanLine> = diesel::sql_query(
        "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) \
         EXECUTE concurrency_live_gate(ARRAY['w'])",
    )
    .load(conn)
    .await
    .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}\n{sql}"));
    conn.batch_execute("DEALLOCATE concurrency_live_gate; COMMIT")
        .await
        .expect("deallocate the gated statement");
    let plan = lines
        .into_iter()
        .map(|l| l.line)
        .collect::<Vec<_>>()
        .join("\n");
    // The top node has the lines before the first child arrow. Its
    // `Buffers:` line includes the buffers of each child node.
    let top = plan
        .lines()
        .take_while(|l| !l.trim_start().starts_with("->"))
        .find(|l| l.trim_start().starts_with("Buffers:"))
        .unwrap_or_else(|| panic!("the top node has no buffer line:\n{plan}"));
    let buffers = ["hit=", "read="]
        .iter()
        .filter_map(|field| {
            let rest = &top[top.find(field)? + field.len()..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse::<i64>().ok()
        })
        .sum();
    (buffers, plan)
}

/// Seed the gate fixture and return the size of the table in pages.
async fn seeded_pages(conn: &mut diesel_async::AsyncPgConnection) -> i64 {
    seed(conn).await;
    let table: Pages =
        diesel::sql_query("SELECT pg_relation_size('harvest_task_queue') / 8192 AS pages")
            .get_result(conn)
            .await
            .expect("table size");
    assert!(
        table.pages > 400,
        "the fixture must be large enough to tell a scan apart: {} pages",
        table.pages
    );
    table.pages
}

/// The most buffers that one gated call may read.
///
/// The index holds 48 entries: 43 on queue `w`, with 2 `RUNNING` rows that
/// have no worker, and 5 on queue `x`. The worst case reads one heap page per
/// entry and 2 index pages, so 50 buffers. A scan of the fixture reads more
/// than 400 pages.
const BUFFER_BOUND: i64 = 60;

/// One stats call reads buffers in proportion to the keyed rows, not to
/// the table.
#[tokio::test(flavor = "multi_thread")]
async fn concurrency_stats_buffers_follow_the_keyed_rows() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    let pages = seeded_pages(&mut conn).await;

    let (buffers, plan) = call_buffers(
        &mut conn,
        "SET LOCAL plan_cache_mode = force_custom_plan",
        CONCURRENCY_STATS_SQL,
    )
    .await;
    assert!(
        buffers <= BUFFER_BOUND,
        "one stats call read {buffers} buffers of a {pages}-page table:\n{plan}"
    );
}

/// The `concurrency_pending_keys` CTE of the full claim reads the due keyed
/// rows, not each due row of the queue. The window claim drops that CTE, so
/// only a fallback claim runs it. The gate plans it with the settings of the
/// claim, which force a generic plan and turn off bitmap scans.
#[tokio::test(flavor = "multi_thread")]
async fn claim_fallback_pending_keys_follow_the_keyed_rows() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    let pages = seeded_pages(&mut conn).await;

    let (buffers, plan) =
        call_buffers(&mut conn, CLAIM_PLAN_SETTINGS_SQL, &pending_keys_sql()).await;
    assert!(
        buffers <= BUFFER_BOUND,
        "the pending keys of one fallback claim read {buffers} buffers of a \
         {pages}-page table:\n{plan}"
    );
    assert!(
        plan.contains(INDEX),
        "the pending keys must read {INDEX}:\n{plan}"
    );
}

/// The stats count only live rows with a key and a cap on the polled queues.
/// The index must not change the groups.
#[tokio::test(flavor = "multi_thread")]
async fn concurrency_stats_count_only_live_capped_rows() {
    let bench = plan_gate_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    seed(&mut conn).await;

    let mut stats: Vec<(String, String, i32, i64, i64)> =
        queue::concurrency_key_stats(&mut conn, &["w".to_owned()])
            .await
            .expect("concurrency stats")
            .into_iter()
            .map(|s| (s.key, s.task_type, s.max_concurrent, s.in_flight, s.pending))
            .collect();
    stats.sort();
    let expected = vec![
        ("ck-a".to_owned(), "activity".to_owned(), 5, 3, 20),
        ("ck-a".to_owned(), "workflow".to_owned(), 7, 0, 4),
        ("ck-b".to_owned(), "activity".to_owned(), 9, 7, 7),
    ];
    assert_eq!(stats, expected);
}
