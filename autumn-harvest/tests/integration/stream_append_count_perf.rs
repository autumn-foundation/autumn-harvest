#![cfg(feature = "db")]
//! Ledger performance investigation: the durable stream append (issue #2083).
//!
//! `store::append_stream_chunks` admits chunks against a per-run cap. It reads
//! the stored row count of the run on every append, so one append of ten chunks
//! to a long stream reads every stored chunk of that run.
//!
//! The harness seeds running executions with no chunks. It then replays the
//! whole lifetime of every stream through the public `append_stream_chunks`:
//! round by round, each live stream appends ten chunks, until it has emitted its
//! length. A stream of `n` chunks makes `n / 10` appends, and append `k` finds
//! `10 k` stored chunks. So the long streams dominate the count cost, as they do
//! in production. Stream lengths are heavy-tailed. The harness reads
//! `pg_stat_statements`. Evidence is call counts, buffers and WAL bytes.
//! Wall-clock is not evidence.
//!
//! The visibility map decides how the stored-row count is read. The `vacuumed`
//! variant runs `VACUUM` whenever autovacuum's insert rule would fire: more than
//! `1000 + 0.2 * reltuples` inserts since the last pass. That is the best case
//! for the count, since a real autovacuum lags. The other variant never
//! vacuums. Autovacuum is off on the table, so both runs are deterministic.
//! Output goes to `docs/perf-artifacts/stream-append-count/` under the label in
//! `PERF_LABEL`.
//!
//! Two tests:
//! - [`zz_capture_stream_append_evidence`] is the `#[ignore]`d evidence
//!   generator. It needs `pg_stat_statements` and a superuser.
//! - [`append_admission_edge_cases`] is an always-run check of the admission
//!   rule. It holds the same expectations before and after the change.

use std::fmt::Write as _;

use autumn_harvest::store::{self, DURABLE_STREAM_TRUNCATION_OFFSET, DurableStreamChunk};
use autumn_harvest::types::ExecutionId;
use diesel::sql_types::{BigInt, Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::throwaway_db::ThrowawayDb;

/// The production cap per run (`context::DURABLE_STREAM_MAX_CHUNKS`).
const CAP: u32 = 10_000;

/// Chunks per append, as one decision cycle emits them.
const BATCH: i64 = 10;

/// Prefer an operator-supplied server and fall back to testcontainers.
async fn setup_server() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .with_cmd(["-c", "shared_preload_libraries=pg_stat_statements"])
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

/// Seeds `execs` running executions and returns each run's stream length.
///
/// Every value derives from `md5` of the ordinal, so one size gives one table.
/// The length is heavy-tailed: the median run emits about 90 chunks and the p95
/// run about 770. One run in fifty emits just over the cap, so it meets the
/// cap in the replay.
async fn seed(conn: &mut AsyncPgConnection, execs: i64) -> Vec<(ExecutionId, i64)> {
    #[derive(diesel::QueryableByName)]
    struct Run {
        #[diesel(sql_type = SqlUuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE ledger_exec AS \
         SELECT g, md5('stream-ledger-id-' || g)::uuid AS id, \
                CASE WHEN g % 50 = 0 THEN {CAP} + 5 + (g / 50) % 5 \
                     ELSE LEAST({CAP}, (50.0 / power( \
                         ((('x' || substr(md5('stream-ledger-len-' || g), 1, 8))::bit(32)::bigint) \
                           / 4294967296.0) + 0.0005, 0.9))::int) END::bigint AS n \
         FROM generate_series(1, {execs}) AS g;\
         INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, started_at, queue_name, shard_id) \
         SELECT id, 'stream_ledger', 'stream_ledger-' || id::text, 'RUNNING', '{{}}'::jsonb, \
                NOW(), 'default', 0 \
         FROM ledger_exec;\
         ALTER TABLE harvest_stream_chunks SET (autovacuum_enabled = false);"
    ))
    .await
    .expect("seed the executions");
    conn.batch_execute("ANALYZE harvest_workflow_executions")
        .await
        .expect("analyze executions");
    diesel::sql_query("SELECT id, n FROM ledger_exec ORDER BY g")
        .load::<Run>(conn)
        .await
        .expect("run lengths")
        .into_iter()
        .map(|r| (ExecutionId::from_uuid(r.id), r.n))
        .collect()
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
    wal_bytes: i64,
}

#[derive(diesel::QueryableByName)]
struct Digest {
    #[diesel(sql_type = BigInt)]
    chunks: i64,
    #[diesel(sql_type = Text)]
    md5: String,
}

async fn snapshot(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, (shared_blks_hit + shared_blks_read) AS buffers, \
                wal_bytes::bigint AS wal_bytes \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
         ORDER BY buffers DESC, query"
    ))
    .load(conn)
    .await
    .expect("pg_stat_statements must be preloaded")
}

/// A fingerprint of every stored chunk, in a total order.
async fn digest(conn: &mut AsyncPgConnection) -> Digest {
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS chunks, \
                md5(string_agg(workflow_exec_id::text || ':' || stream_offset::text || ':' \
                               || chunk::text, ',' ORDER BY workflow_exec_id, stream_offset)) AS md5 \
         FROM harvest_stream_chunks",
    )
    .get_result(conn)
    .await
    .expect("digest")
}

/// One summed row of the measurement.
struct Point {
    execs: i64,
    appends: i64,
    statements: i64,
    buffers: i64,
    count_calls: i64,
    count_buffers: i64,
    wal_bytes: i64,
    inserted: usize,
    digest: Digest,
}

async fn measure(admin: &str, label: &str, execs: i64, vacuumed: bool) -> (Point, String) {
    let db = ThrowawayDb::create_on(admin, "stream_append_perf").await;
    let db_name = db.name().to_owned();
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await
        .expect("pg_stat_statements extension");
    let runs = seed(&mut conn, execs).await;
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(&mut conn)
    .await
    .expect("pg_stat_statements_reset needs a superuser");

    // Round by round across all live runs, so appends of different runs
    // interleave in the heap as concurrent streams write them.
    let mut emitted = vec![0_i64; runs.len()];
    let (mut appends, mut inserted) = (0, 0);
    let (mut rows_at_vacuum, mut since_vacuum) = (0_i64, 0_i64);
    let mut total_rows = 0_i64;
    loop {
        let mut progressed = false;
        for ((exec_id, n), done) in runs.iter().zip(&mut emitted) {
            let take = BATCH.min(n - *done);
            if take <= 0 {
                continue;
            }
            progressed = true;
            let chunks: Vec<DurableStreamChunk> = (0..take)
                .map(|i| DurableStreamChunk {
                    offset: *done + i,
                    chunk: json!({"t": format!("tok-{}", *done + i), "i": *done + i}),
                })
                .collect();
            *done += take;
            let written = store::append_stream_chunks(&mut conn, *exec_id, &chunks, CAP)
                .await
                .expect("append");
            inserted += written;
            appends += 1;
            let written = i64::try_from(written).expect("small");
            total_rows += written;
            since_vacuum += written;
        }
        if !progressed {
            break;
        }
        // Autovacuum's insert rule: 1000 + 0.2 * reltuples inserts since the last pass.
        #[allow(clippy::cast_precision_loss)]
        let due = since_vacuum as f64 > 1000.0 + 0.2 * rows_at_vacuum as f64;
        if vacuumed && due {
            conn.batch_execute("VACUUM harvest_stream_chunks")
                .await
                .expect("vacuum");
            (rows_at_vacuum, since_vacuum) = (total_rows, 0);
        }
    }
    let rows = snapshot(&mut conn, &db_name).await;
    let is_count = |q: &str| {
        let q = q.to_lowercase();
        q.contains("count(*)") && q.contains("harvest_stream_chunks")
    };
    let point = Point {
        execs,
        appends,
        statements: rows.iter().map(|r| r.calls).sum(),
        buffers: rows.iter().map(|r| r.buffers).sum(),
        count_calls: rows.iter().filter(|r| is_count(&r.query)).map(|r| r.calls).sum(),
        count_buffers: rows
            .iter()
            .filter(|r| is_count(&r.query))
            .map(|r| r.buffers)
            .sum(),
        wal_bytes: rows.iter().map(|r| r.wal_bytes).sum(),
        inserted,
        digest: digest(&mut conn).await,
    };
    let mut raw = format!(
        "-- {label}: execs={execs}, vacuumed={vacuumed}, full lifetime replay of {BATCH}-chunk \
         appends, cap {CAP}\n"
    );
    for row in &rows {
        let query: String = row.query.chars().take(140).collect();
        let _ = writeln!(
            raw,
            "calls={} buffers={} wal_bytes={} | {query}",
            row.calls, row.buffers, row.wal_bytes
        );
    }
    drop(conn);
    drop(db);
    (point, raw)
}

/// Writes the evidence for one label (`before` or `after`).
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-stream-append-count.md"]
async fn zz_capture_stream_append_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "after".to_owned());
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("docs/perf-artifacts/stream-append-count");
    std::fs::create_dir_all(&dir).expect("create artifact dir");
    let mut table = format!(
        "-- {label}: durable stream append, full lifetime replay in {BATCH}-chunk appends, cap {CAP}\n\
         execs | vacuumed | appends | statements | all buffers | count calls | count buffers | \
         WAL bytes | chunks inserted | stored chunks | table md5\n"
    );
    let mut raw = String::new();
    for vacuumed in [true, false] {
        for execs in [100, 300, 1_000] {
            let (p, rows) = measure(&admin, &label, execs, vacuumed).await;
            let _ = writeln!(
                table,
                "{} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}",
                p.execs,
                vacuumed,
                p.appends,
                p.statements,
                p.buffers,
                p.count_calls,
                p.count_buffers,
                p.wal_bytes,
                p.inserted,
                p.digest.chunks,
                p.digest.md5
            );
            raw.push_str(&rows);
        }
    }
    println!("{table}");
    std::fs::write(dir.join(format!("{label}-summary.txt")), &table).expect("write summary");
    std::fs::write(dir.join(format!("{label}-pg_stat_statements.txt")), raw)
        .expect("write raw statements");
}

/// Appends `batches` to one fresh run at `cap`. Returns the stored offsets.
async fn run_scenario(
    admin: &str,
    seed_offsets: &[i64],
    batches: &[&[i64]],
    cap: u32,
) -> (Vec<usize>, Vec<i64>) {
    let db = ThrowawayDb::create_on(admin, "stream_append_edge").await;
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    let exec_id = ExecutionId::new();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, started_at, queue_name, shard_id) \
         VALUES ($1, 'edge', 'edge-' || $1::text, 'RUNNING', '{}'::jsonb, NOW(), 'default', 0)",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("insert execution");
    // The seed goes in with a high cap, so only the scenario batches face `cap`.
    let seed_chunks: Vec<_> = seed_offsets
        .iter()
        .map(|&offset| DurableStreamChunk {
            offset,
            chunk: json!({"seed": offset}),
        })
        .collect();
    store::append_stream_chunks(&mut conn, exec_id, &seed_chunks, 1_000_000)
        .await
        .expect("seed append");
    let mut written = Vec::new();
    for batch in batches {
        let chunks: Vec<_> = batch
            .iter()
            .map(|&offset| DurableStreamChunk {
                offset,
                chunk: json!({"new": offset}),
            })
            .collect();
        written.push(
            store::append_stream_chunks(&mut conn, exec_id, &chunks, cap)
                .await
                .expect("append"),
        );
    }
    let stored = store::load_stream_chunks(&mut conn, exec_id, None, 1_000_000)
        .await
        .expect("load")
        .into_iter()
        .map(|c| c.stream_offset)
        .collect();
    (written, stored)
}

/// The admission rule, edge by edge.
///
/// The cap counts real stored rows. A hole in the offsets never fills. A
/// negative offset still counts. A re-driven offset is dropped and writes no
/// marker. These expectations hold for the count-always rule and for any
/// rewrite that skips the count.
#[tokio::test]
async fn append_admission_edge_cases() {
    let (admin, _guard) = setup_server().await;
    let marker = DURABLE_STREAM_TRUNCATION_OFFSET;

    // Holes: offsets 3 and 4 are lost. Offsets at or below the highest never fill.
    let (written, stored) = run_scenario(
        &admin,
        &[0, 1, 2, 5],
        &[&[3, 4, 6, 7, 8, 9, 10, 11], &[12]],
        10,
    )
    .await;
    assert_eq!(written, [6, 0]);
    assert_eq!(stored, [0, 1, 2, 5, 6, 7, 8, 9, 10, 11, marker]);

    // Negative offsets count against the cap: 5 negative and 3 real rows stored.
    let (written, stored) =
        run_scenario(&admin, &[-5, -4, -3, -2, -1, 0, 1, 2], &[&[3, 4, 5]], 10).await;
    assert_eq!(written, [2]);
    assert_eq!(stored, [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, marker]);

    // Plenty of room: every new offset is admitted, no marker.
    let (written, stored) = run_scenario(&admin, &[0, 1, 2, 3, 4], &[&[5, 6, 7, 8, 9]], 100).await;
    assert_eq!(written, [5]);
    assert_eq!(stored, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // A re-drive offers stored offsets only: nothing written, no marker.
    let (written, stored) = run_scenario(&admin, &[0, 1, 2], &[&[0, 1, 2], &[1, 2]], 3).await;
    assert_eq!(written, [0, 0]);
    assert_eq!(stored, [0, 1, 2]);

    // The exact cap: two fit, no marker. The next offset latches the marker.
    let (written, stored) =
        run_scenario(&admin, &[0, 1, 2, 3, 4, 5, 6, 7], &[&[8, 9], &[10]], 10).await;
    assert_eq!(written, [2, 0]);
    assert_eq!(stored, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, marker]);

    // A batch bigger than the room is cut at the room, then the marker latches.
    let (written, stored) =
        run_scenario(&admin, &[0, 1, 2, 3, 4, 5, 6, 7, 8], &[&[9, 10, 11], &[12]], 10).await;
    assert_eq!(written, [1, 0]);
    assert_eq!(stored, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, marker]);

    // An empty run takes a batch up to the cap.
    let (written, stored) = run_scenario(&admin, &[], &[&[0, 1, 2, 3]], 3).await;
    assert_eq!(written, [3]);
    assert_eq!(stored, [0, 1, 2, marker]);
}
