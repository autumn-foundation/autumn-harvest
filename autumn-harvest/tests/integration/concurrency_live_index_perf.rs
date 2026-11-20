#![cfg(feature = "db")]
//! Ledger evidence: the write cost of `idx_harvest_tq_concurrency_live`.
//!
//! The index holds each live task row with a concurrency key and a cap. So an
//! enqueue, a claim and a completion of such a row can write an index entry.
//! A row with no key never enters the index.
//!
//! The harness runs one workload twice, on two fresh databases. One database
//! has the index. The other drops it. Each workload has two phases. The keyed
//! phase enqueues, claims and completes tasks with a key and a high cap. The
//! plain phase does the same with no key. The harness reads
//! `pg_stat_statements` for each phase: calls, shared buffers, dirtied
//! buffers, WAL records and WAL bytes. Evidence is counts, not wall-clock.
//!
//! The background holds 100,000 pending rows with no key and 3,000 live keyed
//! rows on other queues. So the index has depth, and each claim reads a real
//! backlog.
//!
//! The capture is `#[ignore]`d. It needs a superuser and a preloaded
//! `pg_stat_statements`. `HARVEST_TEST_DATABASE_URL` names an admin URL.
//! Without it the harness starts a Postgres container. Output goes to
//! `docs/perf-artifacts/concurrency-live-index/`.

#![allow(clippy::too_many_lines, clippy::cast_precision_loss)]

use std::fmt::Write as _;

use autumn_harvest::queue::{self, EnqueueParams, TaskClaim, TaskType};
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::json;

use crate::queue_age_gauge_perf::setup_server;
use crate::throwaway_db::ThrowawayDb;

/// The index under measurement.
const INDEX: &str = "idx_harvest_tq_concurrency_live";

/// Tasks per phase.
const TASKS: usize = 2_000;

/// The workload queue.
const QUEUE: &str = "w";

/// One write statement, matched by a fragment of its text.
struct Kind {
    label: &'static str,
    fragment: &'static str,
}

/// The statements of the write path. Every other statement falls into
/// "other".
const KINDS: [Kind; 4] = [
    Kind {
        label: "enqueue (insert)",
        fragment: "INSERT INTO \"harvest_task_queue\"",
    },
    Kind {
        label: "claim",
        fragment: "worker_info AS",
    },
    Kind {
        label: "claim cap re-check",
        fragment: ") > harvest_task_queue.concurrency_cap",
    },
    Kind {
        label: "complete",
        fragment: "UPDATE \"harvest_task_queue\"",
    },
];

/// Totals of one statement kind.
#[derive(Default, Clone, Copy)]
struct Cost {
    calls: i64,
    buffers: i64,
    dirtied: i64,
    wal_records: i64,
    wal_fpi: i64,
    wal_bytes: i64,
}

impl Cost {
    fn per_call(self, value: i64) -> f64 {
        if self.calls == 0 {
            0.0
        } else {
            value as f64 / self.calls as f64
        }
    }

    fn add(&mut self, row: &StatRow) {
        self.calls += row.calls;
        self.buffers += row.buffers;
        self.dirtied += row.dirtied;
        self.wal_records += row.wal_records;
        self.wal_fpi += row.wal_fpi;
        self.wal_bytes += row.wal_bytes;
    }
}

#[derive(diesel::QueryableByName)]
struct StatRow {
    #[diesel(sql_type = Text)]
    query: String,
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    buffers: i64,
    #[diesel(sql_type = BigInt)]
    dirtied: i64,
    #[diesel(sql_type = BigInt)]
    wal_records: i64,
    #[diesel(sql_type = BigInt)]
    wal_fpi: i64,
    #[diesel(sql_type = BigInt)]
    wal_bytes: i64,
}

fn artifact_dir() -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("docs/perf-artifacts/concurrency-live-index");
    std::fs::create_dir_all(&dir).expect("create artifact dir");
    dir
}

/// Seed the background: pending rows with no key on 20 queues, and live keyed
/// rows on the same queues.
async fn seed(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "SELECT setseed(0.42); \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, activity_name, scheduled_at) \
         SELECT 'bg' || (g % 20), 'activity', '{}'::jsonb, 'PENDING', 'act' || (g % 60), \
                now() - (random() * interval '6 hours') \
         FROM generate_series(1, 100000) g; \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, activity_name, concurrency_key, \
              concurrency_cap, scheduled_at) \
         SELECT 'bg' || (g % 20), 'activity', '{}'::jsonb, 'PENDING', 'act' || (g % 60), \
                'bgk' || (g % 40), 10, now() - (random() * interval '6 hours') \
         FROM generate_series(1, 2000) g; \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, worker_id, activity_name, concurrency_key, \
              concurrency_cap, started_at) \
         SELECT 'bg' || (g % 20), 'activity', '{}'::jsonb, 'RUNNING', 'w' || (g % 50), \
                'act' || (g % 60), 'bgk' || (g % 40), 10, now() \
         FROM generate_series(1, 1000) g;",
    )
    .await
    .expect("seed the background");
    conn.batch_execute("VACUUM ANALYZE harvest_task_queue")
        .await
        .expect("vacuum");
}

async fn reset(conn: &mut AsyncPgConnection, db_name: &str) {
    conn.batch_execute("CHECKPOINT").await.expect("checkpoint");
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(conn)
    .await
    .expect("pg_stat_statements_reset needs a superuser");
}

async fn snapshot(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, (shared_blks_hit + shared_blks_read) AS buffers, \
                shared_blks_dirtied AS dirtied, wal_records, wal_fpi, \
                wal_bytes::BIGINT AS wal_bytes \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
           AND query NOT ILIKE 'CHECKPOINT%' \
         ORDER BY wal_bytes DESC, query"
    ))
    .load(conn)
    .await
    .expect("pg_stat_statements must be preloaded")
}

/// Enqueue, claim and complete `TASKS` tasks on the workload queue.
async fn run_phase(conn: &mut AsyncPgConnection, keyed: bool) {
    for i in 0..TASKS {
        let mut params = EnqueueParams::new(QUEUE, TaskType::Activity, json!({"i": i}));
        params.activity_name = Some("act".to_owned());
        if keyed {
            params.concurrency_key = Some(format!("ck{}", i % 20));
            params.max_concurrent = Some(1_000_000);
        }
        queue::enqueue(conn, &params).await.expect("enqueue");
    }
    let queues = vec![QUEUE.to_owned()];
    let mut done = 0;
    while let Some(task) = queue::claim_task(conn, &queues, "perf-w", "", None, &[], &[])
        .await
        .expect("claim")
    {
        let claim = TaskClaim::of(&task).expect("a claimed task has a claim");
        let _ = queue::complete_claimed_task(conn, &claim, json!({}))
            .await
            .expect("complete");
        done += 1;
    }
    assert_eq!(done, TASKS, "each enqueued task is claimed and completed");
}

/// The costs of one phase, by kind, and the raw statement dump.
fn classify(rows: &[StatRow]) -> (Vec<Cost>, Cost, String) {
    let mut kinds = vec![Cost::default(); KINDS.len()];
    let mut other = Cost::default();
    let mut raw = String::new();
    for row in rows {
        let slot = KINDS
            .iter()
            .position(|k| row.query.contains(k.fragment))
            .map_or(&mut other, |i| &mut kinds[i]);
        slot.add(row);
        let query: String = row.query.chars().take(140).collect();
        let _ = writeln!(
            raw,
            "calls={} buffers={} dirtied={} wal_records={} wal_fpi={} wal_bytes={} | {query}",
            row.calls, row.buffers, row.dirtied, row.wal_records, row.wal_fpi, row.wal_bytes
        );
    }
    (kinds, other, raw)
}

/// Writes the per-call write costs, with and without the index.
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-concurrency-live-index.md"]
async fn zz_capture_concurrency_live_index_write_costs() {
    let (admin, _guard) = setup_server().await;
    let dir = artifact_dir();
    let mut table = format!(
        "-- per call, {TASKS} tasks per phase\n\
         | index | phase | statement | calls | buffers | dirtied | WAL records | WAL FPI | WAL bytes |\n\
         |:--|:--|:--|--:|--:|--:|--:|--:|--:|\n"
    );
    for with_index in [false, true] {
        let variant = if with_index { "with" } else { "without" };
        let db = ThrowawayDb::create_on(&admin, "concurrency_live_index").await;
        let db_name = db.name().to_owned();
        let mut conn = AsyncPgConnection::establish(&db.url())
            .await
            .expect("connection");
        conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .await
            .expect("pg_stat_statements extension");
        if !with_index {
            conn.batch_execute(&format!("DROP INDEX {INDEX}"))
                .await
                .expect("drop the index for the baseline");
        }
        seed(&mut conn).await;
        // One untimed phase warms the plan cache and the buffer cache.
        run_phase(&mut conn, true).await;
        for keyed in [true, false] {
            let phase = if keyed { "keyed" } else { "plain" };
            reset(&mut conn, &db_name).await;
            run_phase(&mut conn, keyed).await;
            let rows = snapshot(&mut conn, &db_name).await;
            let (kinds, other, raw) = classify(&rows);
            for (kind, cost) in KINDS.iter().zip(&kinds).chain([(
                &Kind {
                    label: "other",
                    fragment: "",
                },
                &other,
            )]) {
                let _ = writeln!(
                    table,
                    "| {variant} | {phase} | {} | {} | {:.1} | {:.2} | {:.2} | {:.3} | {:.0} |",
                    kind.label,
                    cost.calls,
                    cost.per_call(cost.buffers),
                    cost.per_call(cost.dirtied),
                    cost.per_call(cost.wal_records),
                    cost.per_call(cost.wal_fpi),
                    cost.per_call(cost.wal_bytes),
                );
            }
            std::fs::write(
                dir.join(format!("{variant}-{phase}-pg_stat_statements.txt")),
                raw,
            )
            .expect("write raw");
        }
        drop(conn);
        drop(db);
    }
    std::fs::write(dir.join("write-costs.md"), &table).expect("write table");
    eprintln!("{table}");
}
