#![cfg(feature = "db")]
//! Gates for the running-count CTEs of the claim.
//!
//! `claim_running_counts_sql!` builds three CTEs: `seek_running_counts`,
//! `seek_deep_running_counts` and `concurrency_running_counts`. Each counts
//! the `RUNNING` rows of each pending pair of key and task type. The gate of
//! each candidate scan compares that count with the cap.
//!
//! Each gate plans the CTE text from `claim_task_query`. It plans the text as
//! the claim does, with `CLAIM_PLAN_SETTINGS_SQL` and a generic plan. A small
//! CTE in front gives the pending pairs, so the gates measure the count step
//! only. The fixture holds many running rows of other keys and with no key.
//! One count must read buffers in proportion to the running rows of its own
//! pairs. `docs/performance-keyed-claim-running-counts.md` records the cost.

use std::fmt::Write as _;

use autumn_harvest::queue::{self, CLAIM_PLAN_SETTINGS_SQL};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use crate::claim_bench_support::db as claim_bench_db;

/// Each count CTE of the claim, with the pending-keys CTE that it reads.
const COUNT_CTES: [(&str, &str); 3] = [
    ("seek_running_counts", "seek_pending_keys"),
    ("seek_deep_running_counts", "seek_deep_pending_keys"),
    ("concurrency_running_counts", "concurrency_pending_keys"),
];

/// The index that serves a count of one key.
const KEY_INDEX: &str = "harvest_task_queue_concurrency_key_running";

/// The index that holds every running row. A count must not scan it.
const WORKER_INDEX: &str = "harvest_task_queue_running_worker_idx";

/// The pending pairs of each gated call, with the expected running count.
///
/// The fixture gives each count. Rows with no worker, and rows in another
/// state, do not count. A row with a key and no cap counts.
const PAIRS: [(&str, &str, i64); 5] = [
    ("ck-a", "activity", 3),
    ("ck-a", "workflow", 1),
    ("ck-b", "activity", 5),
    ("ck-c", "activity", 0),
    ("ck-nocap", "activity", 2),
];

/// The most buffers that one gated count may read.
///
/// The key index holds 14 entries for the 5 pairs: 8 for `ck-a`, 5 for `ck-b`
/// and 2 for `ck-nocap`. `ck-a` has two pairs, so 2 probes read its 8
/// entries. A probe reads at most 3 index pages. So the worst case is
/// 5 * 3 + 8 + 8 + 5 + 2 = 38 buffers. A scan of every running row reads more
/// than 6,000.
const BUFFER_BOUND: i64 = 50;

/// Running rows that the growth gate adds for other keys and with no key.
const ADDED_FOREIGN_RUNNING: i64 = 5_000;

#[derive(QueryableByName)]
struct PlanLine {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    line: String,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = Text)]
    concurrency_key: String,
    #[diesel(sql_type = Text)]
    task_type: String,
    #[diesel(sql_type = BigInt)]
    running_count: i64,
}

/// Return the text of the CTE `name` in `sql`, from its name to its closing
/// parenthesis.
fn cte_text(sql: &str, name: &str) -> String {
    let head = format!("{name} AS MATERIALIZED (");
    let start = sql
        .find(&head)
        .unwrap_or_else(|| panic!("the claim defines {name}"));
    let mut depth = 0_i32;
    let end = sql[start..]
        .char_indices()
        .find_map(|(i, c)| {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(start + i);
                    }
                }
                _ => {}
            }
            None
        })
        .unwrap_or_else(|| panic!("{name} closes"));
    sql[start..=end].to_owned()
}

/// Return a statement that runs the count CTE `name` of the full claim.
///
/// The CTE `keys` in front gives the pending pairs from `$1` and `$2`. The
/// count CTE binds nothing, so these are the only binds.
fn count_sql(name: &str, keys: &str) -> String {
    let cte = cte_text(queue::claim_task_query(), name);
    assert!(
        !cte.contains('$'),
        "the count CTE {name} binds nothing: {cte}"
    );
    format!(
        "WITH {keys} AS MATERIALIZED ( \
             SELECT * FROM unnest($1::text[], $2::text[]) AS k(concurrency_key, task_type) \
         ), {cte} \
         SELECT concurrency_key, task_type, running_count FROM {name}"
    )
}

/// The `$1` and `$2` arrays of [`PAIRS`], as SQL literals.
fn pair_args() -> String {
    let keys: Vec<String> = PAIRS.iter().map(|(k, _, _)| format!("'{k}'")).collect();
    let types: Vec<String> = PAIRS.iter().map(|(_, t, _)| format!("'{t}'")).collect();
    format!(
        "ARRAY[{}]::text[], ARRAY[{}]::text[]",
        keys.join(", "),
        types.join(", ")
    )
}

/// Seed the gate fixture.
///
/// The background is the one of `concurrency_live_index_perf`: 100,000
/// `PENDING` rows with no key on 20 queues, and 1,000 `RUNNING` rows of 40
/// other keys. With these statistics the old count scanned every running
/// row. The rows of the gated pairs come last.
fn fixture_sql() -> String {
    let mut sql = String::from(
        "INSERT INTO harvest_task_queue (queue_name, task_type, input, state) \
         SELECT 'bg' || (g % 20), 'activity', '{}'::jsonb, 'PENDING' \
         FROM generate_series(1, 100000) g; \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, worker_id, concurrency_key, concurrency_cap) \
         SELECT 'bg' || (g % 20), 'activity', '{}'::jsonb, 'RUNNING', 'bg' || (g % 50), \
                'fk' || (g % 40), 10 \
         FROM generate_series(1, 1000) g; ",
    );
    let rows = [
        // (key, task type, state, worker, cap, count)
        ("ck-a", "activity", "RUNNING", "'w1'", "5", 3),
        ("ck-a", "activity", "RUNNING", "NULL", "5", 2),
        ("ck-a", "activity", "PENDING", "NULL", "5", 4),
        ("ck-a", "workflow", "RUNNING", "'w1'", "7", 1),
        ("ck-a", "activity", "COMPLETED", "'w1'", "5", 2),
        ("ck-b", "activity", "RUNNING", "'w2'", "2", 5),
        ("ck-c", "activity", "PENDING", "NULL", "2", 3),
        ("ck-nocap", "activity", "RUNNING", "'w3'", "NULL", 2),
    ];
    for (key, task_type, state, worker, cap, count) in rows {
        write!(
            sql,
            "INSERT INTO harvest_task_queue \
                 (queue_name, task_type, input, state, worker_id, concurrency_key, \
                  concurrency_cap) \
             SELECT 'w', '{task_type}', '{{}}'::jsonb, '{state}', {worker}, '{key}', {cap} \
             FROM generate_series(1, {count}); "
        )
        .expect("write to a String");
    }
    sql
}

/// Add running rows of other keys and with no key.
fn foreign_growth_sql() -> String {
    let half = ADDED_FOREIGN_RUNNING / 2;
    format!(
        "INSERT INTO harvest_task_queue (queue_name, task_type, input, state, worker_id) \
         SELECT 'w', 'activity', '{{}}'::jsonb, 'RUNNING', 'gx' || (g % 50) \
         FROM generate_series(1, {half}) g; \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, worker_id, concurrency_key, concurrency_cap) \
         SELECT 'w', 'activity', '{{}}'::jsonb, 'RUNNING', 'gx' || (g % 50), 'gk' || (g % 80), 10 \
         FROM generate_series(1, {half}) g; "
    )
}

async fn analyze(conn: &mut AsyncPgConnection) {
    conn.batch_execute("VACUUM ANALYZE harvest_task_queue")
        .await
        .expect("vacuum the task queue");
}

/// Return the shared buffers that one call of the count CTE `name` reads,
/// and its plan.
async fn call_buffers(conn: &mut AsyncPgConnection, name: &str, keys: &str) -> (i64, String) {
    let sql = count_sql(name, keys);
    conn.batch_execute(&format!(
        "BEGIN; {CLAIM_PLAN_SETTINGS_SQL}; PREPARE running_count_gate AS {sql}"
    ))
    .await
    .unwrap_or_else(|e| panic!("PREPARE failed: {e}\n{sql}"));
    let lines: Vec<PlanLine> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) \
         EXECUTE running_count_gate({})",
        pair_args()
    ))
    .load(conn)
    .await
    .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}\n{sql}"));
    conn.batch_execute("DEALLOCATE running_count_gate; COMMIT")
        .await
        .expect("deallocate the gated statement");
    let plan = lines
        .into_iter()
        .map(|l| l.line)
        .collect::<Vec<_>>()
        .join("\n");
    // The top node has the lines before the first child arrow. Its
    // `Buffers:` line includes the buffers of each child node.
    let buffers = plan
        .lines()
        .take_while(|l| !l.trim_start().starts_with("->"))
        .find(|l| l.trim_start().starts_with("Buffers:"))
        .map_or(0, |top| {
            ["hit=", "read="]
                .iter()
                .filter_map(|field| {
                    let rest = &top[top.find(field)? + field.len()..];
                    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                    digits.parse::<i64>().ok()
                })
                .sum()
        });
    (buffers, plan)
}

async fn seeded_db() -> (claim_bench_db::BenchDb, AsyncPgConnection) {
    let bench = claim_bench_db::setup_bench_db().await.unwrap_or_else(|r| {
        panic!(
            "the running-count gates need an isolated database, from \
             HARVEST_TEST_DATABASE_URL or a reachable Docker daemon: {}",
            r.0
        )
    });
    let mut conn = claim_bench_db::connect(&bench.url).await;
    conn.batch_execute(&fixture_sql())
        .await
        .expect("seed the gate fixture");
    analyze(&mut conn).await;
    (bench, conn)
}

/// Each count reads the running rows of its own pairs through the key
/// index. It does not read every running row.
#[tokio::test(flavor = "multi_thread")]
async fn each_running_count_reads_only_the_rows_of_its_own_keys() {
    let (_bench, mut conn) = seeded_db().await;
    for (name, keys) in COUNT_CTES {
        let (buffers, plan) = call_buffers(&mut conn, name, keys).await;
        assert!(
            buffers <= BUFFER_BOUND,
            "{name} read {buffers} buffers for {} pairs:\n{plan}",
            PAIRS.len()
        );
        assert!(
            plan.contains(KEY_INDEX),
            "{name} must read {KEY_INDEX}:\n{plan}"
        );
        assert!(
            !plan.contains(WORKER_INDEX),
            "{name} must not scan every running row:\n{plan}"
        );
    }
}

/// More running rows of other keys, or with no key, do not add reads to a
/// count. Only the index can grow by one level, so each probe may read one
/// more page.
#[tokio::test(flavor = "multi_thread")]
async fn foreign_running_rows_add_no_reads_to_a_count() {
    let (_bench, mut conn) = seeded_db().await;
    let mut before = Vec::new();
    for (name, keys) in COUNT_CTES {
        before.push(call_buffers(&mut conn, name, keys).await.0);
    }
    conn.batch_execute(&foreign_growth_sql())
        .await
        .expect("add foreign running rows");
    analyze(&mut conn).await;
    let slack = i64::try_from(PAIRS.len()).expect("a small count");
    for ((name, keys), before) in COUNT_CTES.into_iter().zip(before) {
        let (after, plan) = call_buffers(&mut conn, name, keys).await;
        assert!(
            after <= before + slack,
            "{name} read {before} buffers, then {after} after \
             {ADDED_FOREIGN_RUNNING} more foreign running rows:\n{plan}"
        );
    }
}

/// Each count equals a direct count of the running rows with a worker, for
/// its pair. A pair with no row counts as 0, as the gate reads it.
#[tokio::test(flavor = "multi_thread")]
async fn each_running_count_matches_a_direct_count_per_pair() {
    let (_bench, mut conn) = seeded_db().await;
    for (name, keys) in COUNT_CTES {
        let sql = count_sql(name, keys);
        conn.batch_execute(&format!(
            "BEGIN; {CLAIM_PLAN_SETTINGS_SQL}; PREPARE running_count_rows AS {sql}"
        ))
        .await
        .unwrap_or_else(|e| panic!("PREPARE failed: {e}\n{sql}"));
        let rows: Vec<CountRow> =
            diesel::sql_query(format!("EXECUTE running_count_rows({})", pair_args()))
                .load(&mut conn)
                .await
                .unwrap_or_else(|e| panic!("EXECUTE failed: {e}\n{sql}"));
        conn.batch_execute("DEALLOCATE running_count_rows; COMMIT")
            .await
            .expect("deallocate the count statement");
        for (key, task_type, expected) in PAIRS {
            let found: Vec<i64> = rows
                .iter()
                .filter(|r| r.concurrency_key == key && r.task_type == task_type)
                .map(|r| r.running_count)
                .collect();
            assert!(
                found.len() <= 1,
                "{name} has one row per pair at most: {key} {task_type} {found:?}"
            );
            let count = found.first().copied().unwrap_or(0);
            assert_eq!(count, expected, "{name}: {key} {task_type}");
        }
    }
}
