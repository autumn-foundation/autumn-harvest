#![cfg(feature = "db")]
//! Ledger performance investigation: the timeout-checker refill (issue #2126).
//!
//! Each task-timeout lane refills its queue from one page of live rows. Four
//! lanes at one position read the same page. Before issue #2126, each lane
//! read it with its own statement. Now one statement reads it for them all.
//!
//! The harness seeds a fresh database, runs batched passes, and reads
//! `pg_stat_statements`. Evidence is call counts, buffers, temp blocks, JIT
//! counts and the server's execution time. Output goes to
//! `docs/perf-artifacts/` under the label in `PERF_LABEL`.
//!
//! Two tests:
//! - [`zz_capture_timeout_refill_evidence`] is the `#[ignore]`d evidence
//!   generator. It needs `pg_stat_statements`.
//! - [`a_seeded_pass_gives_each_row_its_first_reason`] is an always-run
//!   correctness check on the same fixture.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use autumn_harvest::timeout::{self, TimeoutScanCursor};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::throwaway_db::ThrowawayDb;

/// Passes per measurement.
const PASSES: usize = 8;

/// The batch limit of the spawned checker.
const LIMIT: u32 = timeout::DEFAULT_TIMEOUT_SCAN_BATCH_SIZE;

/// Prefer an operator-supplied server and fall back to testcontainers.
async fn setup_server() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

/// Seeds `live` live rows and `2 * live` terminal rows.
///
/// Of the live rows, 30% are `RUNNING` and 70% are `PENDING`. 1% of the
/// live rows miss a start-to-close deadline, and 1% miss a schedule-to-start
/// deadline. The seed is fixed, so each run gets the same table.
async fn seed(conn: &mut AsyncPgConnection, live: i64) {
    conn.batch_execute(&format!(
        "SELECT setseed(0.1956);\
         INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, input, state, attempt, max_attempts, \
              scheduled_at, started_at, last_heartbeat_at, heartbeat_timeout, \
              start_to_close, schedule_to_start, schedule_to_close_at, created_at) \
         SELECT gen_random_uuid(), 'perf', 'activity', '{{}}'::jsonb, \
                CASE WHEN g % 10 < 3 THEN 'RUNNING' ELSE 'PENDING' END, 1, 3, \
                NOW() - random() * INTERVAL '1 hour', \
                CASE WHEN g % 10 < 3 THEN NOW() - random() * INTERVAL '1 hour' END, \
                CASE WHEN g % 10 < 3 THEN NOW() - random() * INTERVAL '1 minute' END, \
                INTERVAL '10 minutes', \
                CASE WHEN g % 100 = 0 THEN INTERVAL '1 second' ELSE INTERVAL '2 hours' END, \
                CASE WHEN g % 100 = 5 THEN INTERVAL '1 second' ELSE INTERVAL '2 hours' END, \
                NOW() + INTERVAL '1 day', \
                NOW() - INTERVAL '1 day' + (g::double precision / {live}) * INTERVAL '23 hours' \
         FROM generate_series(1, {live}) AS g;\
         INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, input, state, attempt, max_attempts, created_at) \
         SELECT gen_random_uuid(), 'perf', 'activity', '{{}}'::jsonb, 'COMPLETED', 1, 3, \
                NOW() - random() * INTERVAL '1 day' \
         FROM generate_series(1, 2 * {live});\
         ANALYZE harvest_task_queue;"
    ))
    .await
    .expect("seed the task queue");
}

#[derive(diesel::QueryableByName)]
struct StatRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    buffers: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    temp_written: i64,
    #[diesel(sql_type = diesel::sql_types::Double)]
    exec_ms: f64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    jit_inlined: i64,
}

async fn snapshot(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, (shared_blks_hit + shared_blks_read) AS buffers, \
                temp_blks_written AS temp_written, total_exec_time AS exec_ms, \
                (jit_inlining_count + jit_optimization_count) AS jit_inlined \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
         ORDER BY buffers DESC, query"
    ))
    .load(conn)
    .await
    .expect("pg_stat_statements must be preloaded")
}

/// Totals for one fixture size.
struct Point {
    live: i64,
    refill_calls: i64,
    refill_buffers: i64,
    refill_temp: i64,
    refill_ms: f64,
    refill_jit_inlined: i64,
    clock_calls: i64,
    pass_buffers: i64,
    rows_found: usize,
}

async fn measure(admin: &str, label: &str, live: i64) -> (Point, String) {
    let db = ThrowawayDb::create_on(admin, "timeout_refill_perf").await;
    let db_name = db.name().to_owned();
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await
        .expect("pg_stat_statements extension");
    seed(&mut conn, live).await;
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(&mut conn)
    .await
    .expect("pg_stat_statements_reset needs a superuser");

    let mut cursor = TimeoutScanCursor::default();
    let mut rows_found = 0;
    for _ in 0..PASSES {
        rows_found += timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, i64::from(LIMIT))
            .await
            .expect("batch scan")
            .len();
    }
    let rows = snapshot(&mut conn, &db_name).await;
    let refill = rows
        .iter()
        .filter(|r| r.query.contains("WITH page AS MATERIALIZED"));
    let point = Point {
        live,
        refill_calls: refill.clone().map(|r| r.calls).sum(),
        refill_buffers: refill.clone().map(|r| r.buffers).sum(),
        refill_temp: refill.clone().map(|r| r.temp_written).sum(),
        refill_ms: refill.clone().map(|r| r.exec_ms).sum(),
        refill_jit_inlined: refill.map(|r| r.jit_inlined).sum(),
        clock_calls: rows
            .iter()
            .filter(|r| r.query.contains("AS as_of"))
            .map(|r| r.calls)
            .sum(),
        pass_buffers: rows.iter().map(|r| r.buffers).sum(),
        rows_found,
    };
    let mut raw = format!("-- {label}: live={live}, {PASSES} passes, limit {LIMIT}\n");
    for row in &rows {
        let query: String = row.query.chars().take(120).collect();
        let _ = writeln!(
            raw,
            "calls={} buffers={} temp_written={} exec_ms={:.1} jit_inlined={} | {query}",
            row.calls, row.buffers, row.temp_written, row.exec_ms, row.jit_inlined
        );
    }
    drop(conn);
    drop(db);
    (point, raw)
}

/// Writes the evidence for one label (`before` or `after`).
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-timeout-refill-shared-page.md"]
async fn zz_capture_timeout_refill_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "after".to_owned());
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("docs/perf-artifacts/timeout-refill-shared-page");
    std::fs::create_dir_all(&dir).expect("create artifact dir");
    let mut table = format!(
        "-- {label}: timeout refill, {PASSES} passes, limit {LIMIT}\n\
         live | refill calls | refill buffers | refill temp blocks | refill ms | \
         refill JIT inline | clock reads | all buffers | rows found\n"
    );
    let mut raw = String::new();
    for live in [1_000, 100_000] {
        let (point, rows) = measure(&admin, &label, live).await;
        let _ = writeln!(
            table,
            "{} | {} | {} | {} | {:.0} | {} | {} | {} | {}",
            point.live,
            point.refill_calls,
            point.refill_buffers,
            point.refill_temp,
            point.refill_ms,
            point.refill_jit_inlined,
            point.clock_calls,
            point.pass_buffers,
            point.rows_found
        );
        raw.push_str(&rows);
    }
    println!("{table}");
    std::fs::write(dir.join(format!("{label}-summary.txt")), &table).expect("write summary");
    std::fs::write(dir.join(format!("{label}-pg_stat_statements.txt")), raw)
        .expect("write raw statements");
}

/// One pass on the seeded fixture gives each row its first reason.
///
/// The unbounded scan tests the reasons one by one and keeps the first
/// match. At the default batch, one pass from a new cursor returns every
/// expired row of the fixture through one shared refill.
#[tokio::test]
async fn a_seeded_pass_gives_each_row_its_first_reason() {
    let (admin, _guard) = setup_server().await;
    let db = ThrowawayDb::create_on(&admin, "timeout_refill_first").await;
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    seed(&mut conn, 1_000).await;
    let reasons = |rows: Vec<(
        autumn_harvest::models::TaskQueueItem,
        timeout::TimeoutReason,
    )>| {
        rows.into_iter()
            .map(|(task, reason)| (task.id, reason.to_string()))
            .collect::<BTreeMap<_, _>>()
    };
    let unbounded = reasons(
        timeout::find_timed_out_tasks(&mut conn)
            .await
            .expect("unbounded scan"),
    );
    let batched = reasons(
        timeout::find_timed_out_tasks_batch(
            &mut conn,
            &mut TimeoutScanCursor::default(),
            i64::from(LIMIT),
        )
        .await
        .expect("batch scan"),
    );
    assert_eq!(unbounded.len(), 20, "{unbounded:?}");
    assert_eq!(batched, unbounded);
}
