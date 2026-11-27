#![cfg(feature = "db")]
//! The due-row read of the unsharded event-batch scanner.
//!
//! Each scanner tick calls `event_batch::fire_due_event_batches`. Without a
//! sharded pool, it fires one due row per transaction. Each transaction runs
//! `WHERE fire_at <= $1 ORDER BY fire_at LIMIT 1 FOR UPDATE SKIP LOCKED`.
//! The only scanner index is `(shard_id, fire_at)`. Its leading column is not
//! in that predicate, so the index cannot give rows in `fire_at` order. Each
//! read then fetches every due row from the heap and sorts them to return
//! one. A tick that fires `D` rows reads about `D * D / 2` heap rows.
//!
//! The candidate is a plain index on `(fire_at)`. The read then takes the
//! first index entry and stops. The query text does not change.
//! `docs/performance-event-batch-fire-scan.md` records the measurement.
//!
//! Two tests:
//!
//! - [`event_batch_fire_index_does_not_change_what_fires`] always runs when
//!   `HARVEST_TEST_DATABASE_URL` is set. It seeds the edge cases into two
//!   copies of one database, one without the candidate index and one with it.
//!   It fires both and compares the runs, the fire order and the rows left.
//! - [`zz_capture_event_batch_fire_scan_evidence`] is `#[ignore]`d. It seeds
//!   a production-shaped fixture at three sizes, once without the index and
//!   once with it. It writes the artifacts in
//!   `docs/perf-artifacts/event-batch-fire-scan/`. `cargo xtask perf
//!   event-batch-fire-scan` runs it.
//!
//! Each arm runs in its own copy of one migrated template database. So no
//! arm sees the runs or the statistics of another arm. The capture needs a
//! superuser and a preloaded `pg_stat_statements`.

#![allow(clippy::too_many_lines, clippy::cast_precision_loss)]

use std::fmt::Write as _;
use std::time::Duration;

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use jiff_diesel::ToDiesel;

use autumn_harvest::debounce::DebounceStartOptions;
use autumn_harvest::event_batch::{AdmitBatchParams, admit_batched_start, fire_due_event_batches};
use autumn_harvest::telemetry::NoOpMetrics;

use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::throwaway_db::ThrowawayDb;

/// The candidate index. The migration builds the same definition.
const CANDIDATE_INDEX: &str = "idx_harvest_event_batches_fire_at";

/// The definition of the candidate index.
const CANDIDATE_INDEX_SQL: &str = "CREATE INDEX IF NOT EXISTS idx_harvest_event_batches_fire_at \
     ON harvest_event_batches (fire_at)";

/// The due-row read of the unsharded scanner, as `fire_due_on_conn` sends it.
const DUE_SQL: &str = "SELECT id, workflow_name, batch_key, workflow_id, queue_name, \
     buffered_payloads, start_options, fire_at, max_size, shard_id \
     FROM harvest_event_batches WHERE fire_at <= $1 ORDER BY fire_at ASC LIMIT $2 \
     FOR UPDATE SKIP LOCKED";

/// Opens every harness query, so the statement snapshot can drop them.
const HARNESS: &str = "/* harness */ ";

/// The open batches of the fixture at each capture size.
const SIZES: [i64; 3] = [1_000, 10_000, 100_000];

/// Admissions in each capture: half append to an open batch, half open one.
const ADMISSIONS: usize = 200;

/// The `max_size` of each seeded and admitted batch.
const MAX_SIZE: usize = 100;

/// The production-shaped fixture. `$BATCHES` is the number of open batches.
///
/// - 20 batched workflow types with a cubic skew, so `batch_wf_0` takes
///   about 37% of the rows.
/// - 2% of the rows are due. Their `fire_at` falls in the last two minutes,
///   as after a scanner pause or a slow tick. The rest fall due in 10 to 15
///   minutes. Each `fire_at` is distinct, so the fire order is total.
/// - Buffered payloads: 70% of the rows hold 1 to 5 events, 25% hold 6 to
///   50, and 5% hold 51 to 98. An event is about 230 bytes, so a large row
///   goes to TOAST.
/// - 30% of the rows carry a memo in their start options.
/// - Half of the rows then take one more event, as an append does. Autovacuum
///   is off for the table, so about a third of the heap tuples are dead.
///
/// One `random()` draw per column, in one row source.
const FIXTURE_SQL: &str = "
SELECT setseed(0.42);
ALTER TABLE harvest_event_batches SET (autovacuum_enabled = false);
DROP TABLE IF EXISTS seed_batches;
CREATE TEMP TABLE seed_batches AS
SELECT g AS n, random() AS u_name, random() AS u_size, random() AS u_size2,
       random() AS u_fire, random() AS u_memo
FROM generate_series(1, $BATCHES) g;
INSERT INTO harvest_event_batches
  (id, workflow_name, batch_key, workflow_id, queue_name, buffered_payloads,
   start_options, fire_at, max_size, shard_id, created_at, updated_at)
SELECT
  ('00000000-0000-4000-8000-' || lpad(to_hex(n), 12, '0'))::uuid,
  'batch_wf_' || floor(20 * power(u_name, 3))::int,
  'key-' || n,
  'batch-run-' || n,
  'batches',
  (SELECT jsonb_agg(jsonb_build_object(
      'event', n * 1000 + k, 'kind', 'order.updated',
      'body', md5((n * 1000 + k)::text) || md5((n * 1000 + k + 1)::text)
              || md5((n * 1000 + k + 2)::text) || md5((n * 1000 + k + 3)::text)
              || md5((n * 1000 + k + 4)::text) || md5((n * 1000 + k + 5)::text)))
   FROM generate_series(1, CASE
       WHEN u_size < 0.70 THEN 1 + floor(5 * u_size2)::int
       WHEN u_size < 0.95 THEN 6 + floor(45 * u_size2)::int
       ELSE 51 + floor(48 * u_size2)::int END) k),
  CASE WHEN u_memo < 0.3 THEN jsonb_build_object('memo', jsonb_build_object('source', 'webhook'))
       ELSE '{}'::jsonb END,
  CASE WHEN u_fire < 0.02
       THEN now() - interval '2 minutes' + interval '100 seconds' * (u_fire / 0.02)
       ELSE now() + interval '10 minutes' + interval '5 minutes' * u_fire END
    + n * interval '1 microsecond',
  $MAX_SIZE, 0, now() - interval '3 minutes', now() - interval '3 minutes'
FROM seed_batches;
UPDATE harvest_event_batches
   SET buffered_payloads = buffered_payloads
         || jsonb_build_array(jsonb_build_object('event', 0, 'kind', 'order.appended')),
       updated_at = now() - interval '1 minute'
 WHERE (get_byte(uuid_send(id), 15) % 2) = 0;
ANALYZE harvest_event_batches;
DROP TABLE seed_batches;
";

/// The edge cases of the due read, for the always-run check.
///
/// - Due rows on four shard ids, since an unsharded scan fires every shard id.
/// - Three due rows that tie on `fire_at`. The read orders on `fire_at` only,
///   so their relative order is not part of its contract.
/// - A row due one millisecond before the seed, and a row due one minute
///   after it, which must stay.
/// - A row at `max_size - 1` events.
const EDGE_FIXTURE_SQL: &str = "
INSERT INTO harvest_event_batches
  (id, workflow_name, batch_key, workflow_id, queue_name, buffered_payloads,
   start_options, fire_at, max_size, shard_id)
VALUES
  ('20000000-0000-4000-8000-000000000001', 'edge_wf', 'e1', 'edge-1', 'batches',
   '[{\"n\":1}]', '{}', now() - interval '50 seconds', 10, 0),
  ('20000000-0000-4000-8000-000000000002', 'edge_wf', 'e2', 'edge-2', 'batches',
   '[{\"n\":2},{\"n\":3}]', '{}', now() - interval '40 seconds', 10, 1),
  ('20000000-0000-4000-8000-000000000003', 'edge_wf', 'e3', 'edge-3', 'batches',
   '[{\"n\":4}]', '{\"memo\":{\"k\":\"v\"}}', now() - interval '30 seconds', 10, 2),
  ('20000000-0000-4000-8000-000000000004', 'edge_wf', 'e4', 'edge-4', 'batches',
   '[{\"n\":5}]', '{}', now() - interval '20 seconds', 10, 3),
  ('20000000-0000-4000-8000-000000000005', 'edge_wf', 'e5', 'edge-5', 'batches',
   '[{\"n\":6}]', '{}', date_trunc('second', now()) - interval '10 seconds', 10, 0),
  ('20000000-0000-4000-8000-000000000006', 'edge_wf', 'e6', 'edge-6', 'batches',
   '[{\"n\":7}]', '{}', date_trunc('second', now()) - interval '10 seconds', 10, 2),
  ('20000000-0000-4000-8000-000000000007', 'edge_wf', 'e7', 'edge-7', 'batches',
   '[{\"n\":8}]', '{}', date_trunc('second', now()) - interval '10 seconds', 10, 0),
  ('20000000-0000-4000-8000-000000000008', 'edge_wf', 'e8', 'edge-8', 'batches',
   '[{\"n\":9}]', '{}', now() - interval '1 millisecond', 10, 1),
  ('20000000-0000-4000-8000-000000000009', 'edge_wf', 'e9', 'edge-9', 'batches',
   '[1,2,3,4,5,6,7,8,9]', '{}', now() - interval '5 seconds', 10, 0),
  ('20000000-0000-4000-8000-00000000000a', 'edge_wf', 'e10', 'edge-10', 'batches',
   '[{\"n\":10}]', '{}', now() + interval '1 minute', 10, 0),
  ('20000000-0000-4000-8000-00000000000b', 'edge_wf', 'e11', 'edge-11', 'batches',
   '[{\"n\":11}]', '{}', now() + interval '1 hour', 10, 3);
ANALYZE harvest_event_batches;
";

#[derive(QueryableByName)]
struct NameRow {
    #[diesel(sql_type = Text)]
    item: String,
}

#[derive(QueryableByName)]
struct PlanLine {
    #[diesel(sql_type = Text)]
    #[diesel(column_name = "QUERY PLAN")]
    line: String,
}

#[derive(QueryableByName)]
struct FixtureRow {
    #[diesel(sql_type = Text)]
    item: String,
    #[diesel(sql_type = BigInt)]
    value: i64,
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
    #[diesel(sql_type = Text)]
    relname: String,
    #[diesel(sql_type = BigInt)]
    seq_scan: i64,
    #[diesel(sql_type = BigInt)]
    seq_tup_read: i64,
    #[diesel(sql_type = BigInt)]
    idx_scan: i64,
    #[diesel(sql_type = BigInt)]
    idx_tup_fetch: i64,
    #[diesel(sql_type = BigInt)]
    n_live_tup: i64,
    #[diesel(sql_type = BigInt)]
    n_dead_tup: i64,
    #[diesel(sql_type = BigInt)]
    heap_blks: i64,
    #[diesel(sql_type = BigInt)]
    idx_blks: i64,
    #[diesel(sql_type = BigInt)]
    toast_blks: i64,
}

/// The `pg_stat_statements` scope of this database.
const DB_SCOPE: &str = "dbid = (SELECT oid FROM pg_database WHERE datname = current_database())";

/// One line for each row of a text query.
async fn lines(conn: &mut AsyncPgConnection, sql: &str) -> Vec<String> {
    let rows: Vec<NameRow> = diesel::sql_query(format!("{HARNESS}{sql}"))
        .load(conn)
        .await
        .unwrap_or_else(|e| panic!("harness query failed: {e}\n{sql}"));
    rows.into_iter().map(|r| r.item).collect()
}

/// What a fire left behind, in a form that two arms can compare.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    /// Each started run, in fire order: type, id and an input digest.
    runs: Vec<String>,
    /// Each batch row left, by key: id, size and a payload digest.
    left: Vec<String>,
}

/// Read the started runs in fire order, and the batch rows left.
///
/// Each fire commits its own transaction, so `created_at` orders the runs.
async fn outcome(conn: &mut AsyncPgConnection) -> Outcome {
    let runs = lines(
        conn,
        "SELECT workflow_name || '|' || workflow_id || '|' || md5(input::text) AS item
         FROM harvest_workflow_executions ORDER BY created_at, workflow_id",
    )
    .await;
    let left = lines(
        conn,
        "SELECT batch_key || '|' || workflow_id || '|' || shard_id || '|'
                || jsonb_array_length(buffered_payloads) || '|' || md5(buffered_payloads::text)
                || '|' || md5(start_options::text) AS item
         FROM harvest_event_batches ORDER BY workflow_name, batch_key",
    )
    .await;
    Outcome { runs, left }
}

/// `EXPLAIN` of the due read, in a transaction that rolls back.
///
/// `FOR UPDATE` locks the row that it returns, so the read runs in a
/// transaction that rolls back. No row changes.
async fn explain_due(conn: &mut AsyncPgConnection, form: &str) -> String {
    conn.batch_execute("BEGIN").await.expect("begin");
    let plan: Vec<PlanLine> = diesel::sql_query(format!("{HARNESS}{form} {DUE_SQL}"))
        .bind::<diesel::sql_types::Timestamptz, _>(jiff::Timestamp::now().to_diesel())
        .bind::<BigInt, _>(1_i64)
        .load(conn)
        .await
        .expect("explain the due read");
    conn.batch_execute("ROLLBACK").await.expect("rollback");
    plan.into_iter().fold(String::new(), |mut out, l| {
        out.push_str(&l.line);
        out.push('\n');
        out
    })
}

/// Admit one start through the public batch path.
async fn admit(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    batch_key: &str,
    workflow_id: &str,
    event: usize,
) {
    let params = AdmitBatchParams {
        workflow_name: workflow_name.to_string(),
        batch_key: batch_key.to_string(),
        workflow_id: workflow_id.to_string(),
        queue_name: "batches".to_string(),
        payload: serde_json::json!({ "event": event, "kind": "order.created" }),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(300),
        max_size: MAX_SIZE,
        shard_id: 0,
    };
    admit_batched_start(conn, params, None)
        .await
        .expect("admit a batched start");
}

/// One migrated template and the admin URL of its server.
///
/// The fields drop in order, so the template database goes before its
/// container.
struct Template {
    admin_url: String,
    db: ThrowawayDb,
    _container: Option<ContainerAsync<Postgres>>,
}

/// Create the template on the `HARVEST_TEST_DATABASE_URL` server. Without
/// that variable, start a Postgres container, as the other event-batch
/// suites do. Returns `None` only when neither is available.
async fn template(prefix: &str) -> Option<Template> {
    let (admin_url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = match Postgres::default().with_tag("16").start().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("no Postgres container: {e}");
                return None;
            }
        };
        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("container port");
        (
            format!("postgresql://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let db = ThrowawayDb::create_on(&admin_url, prefix).await;
    Some(Template {
        admin_url,
        db,
        _container: container,
    })
}

/// Copy the template into a new database for one arm, with or without the
/// candidate index. Returns the database and a connection to it.
async fn arm(t: &Template, prefix: &str, with_index: bool) -> (ThrowawayDb, AsyncPgConnection) {
    let db = ThrowawayDb::clone_on(&t.admin_url, prefix, t.db.name()).await;
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connect to the arm database");
    // The migration builds the candidate once it ships. Drop it in the
    // baseline arm, so the baseline is the pre-fix schema on every checkout.
    let ddl = if with_index {
        CANDIDATE_INDEX_SQL.to_string()
    } else {
        format!("DROP INDEX IF EXISTS {CANDIDATE_INDEX}")
    };
    conn.batch_execute(&ddl).await.expect("set the index state");
    (db, conn)
}

/// Assert that the due read takes the candidate index only in its arm.
async fn assert_plan(conn: &mut AsyncPgConnection, with_index: bool) -> String {
    let plan = explain_due(conn, "EXPLAIN (COSTS OFF)").await;
    assert_eq!(
        plan.contains(CANDIDATE_INDEX),
        with_index,
        "the due read must use the candidate index only in the arm with it:\n{plan}"
    );
    plan
}

// ---------------------------------------------------------------------------
// Always-run equivalence check
// ---------------------------------------------------------------------------

/// Fire the edge fixture in one arm and return what fired.
async fn fire_edge_arm(t: &Template, with_index: bool) -> Outcome {
    let (_db, mut conn) = arm(t, "ebfire_edge_arm", with_index).await;
    conn.batch_execute(EDGE_FIXTURE_SQL)
        .await
        .expect("seed the edge fixture");
    // A tiny table can plan either way, so do not check the plan here. The
    // capture checks it on the production-shaped fixture.
    let fired = fire_due_event_batches(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("fire the due batches");
    assert_eq!(fired, 9, "nine edge rows are due");
    outcome(&mut conn).await
}

/// Drop the tied rows from a fire order, and return them sorted.
fn split_ties(runs: &[String]) -> (Vec<String>, Vec<String>) {
    let tied_ids = ["|edge-5|", "|edge-6|", "|edge-7|"];
    let (mut ties, rest): (Vec<String>, Vec<String>) = runs
        .iter()
        .cloned()
        .partition(|r| tied_ids.iter().any(|t| r.contains(t)));
    ties.sort();
    (rest, ties)
}

#[tokio::test]
async fn event_batch_fire_index_does_not_change_what_fires() {
    let Some(t) = template("ebfire_edge_tpl").await else {
        eprintln!("SKIP: no HARVEST_TEST_DATABASE_URL and no Postgres container");
        return;
    };
    let without = fire_edge_arm(&t, false).await;
    let with = fire_edge_arm(&t, true).await;

    assert_eq!(without.left, with.left, "the same rows must stay");
    assert_eq!(without.left.len(), 2, "the two rows not yet due stay");
    let (order_without, ties_without) = split_ties(&without.runs);
    let (order_with, ties_with) = split_ties(&with.runs);
    assert_eq!(
        order_without, order_with,
        "the untied fire order must agree"
    );
    assert_eq!(ties_without, ties_with, "the tied rows must all fire");
    assert_eq!(ties_with.len(), 3);
    // The fire order follows `fire_at`, over every shard id.
    let ids: Vec<&str> = order_with
        .iter()
        .map(|r| r.split('|').nth(1).expect("an id"))
        .collect();
    assert_eq!(
        ids,
        ["edge-1", "edge-2", "edge-3", "edge-4", "edge-9", "edge-8"]
    );
    eprintln!(
        "equivalence confirmed: {} runs and {} rows agree on the edge fixture",
        with.runs.len(),
        with.left.len()
    );
}

// ---------------------------------------------------------------------------
// Production-shaped evidence capture
// ---------------------------------------------------------------------------

/// The shape of the seeded fixture, for the artifact.
async fn fixture_summary(conn: &mut AsyncPgConnection, label: &str) -> String {
    let rows: Vec<FixtureRow> = diesel::sql_query(format!(
        "{HARNESS}SELECT 'open batches' AS item, COUNT(*)::BIGINT AS value FROM harvest_event_batches
         UNION ALL SELECT 'due batches', COUNT(*) FROM harvest_event_batches WHERE fire_at <= now()
         UNION ALL SELECT 'batches over 50 events', COUNT(*) FROM harvest_event_batches
           WHERE jsonb_array_length(buffered_payloads) > 50
         UNION ALL SELECT 'rows of batch_wf_0', COUNT(*) FROM harvest_event_batches
           WHERE workflow_name = 'batch_wf_0'
         UNION ALL SELECT 'heap pages', pg_relation_size('harvest_event_batches') / 8192
         UNION ALL SELECT 'toast pages', COALESCE(pg_relation_size(reltoastrelid) / 8192, 0)
           FROM pg_class WHERE relname = 'harvest_event_batches'
         UNION ALL SELECT 'dead tuples', n_dead_tup FROM pg_stat_user_tables
           WHERE relname = 'harvest_event_batches'
         UNION ALL SELECT 'live tuples', n_live_tup FROM pg_stat_user_tables
           WHERE relname = 'harvest_event_batches'"
    ))
    .load(conn)
    .await
    .expect("fixture summary");
    let mut out = format!("-- {label} --\nitem|value\n");
    for r in rows {
        let _ = writeln!(out, "{}|{}", r.item, r.value);
    }
    out
}

/// The table and index statistics of the start path, for the snapshot.
async fn table_stats(conn: &mut AsyncPgConnection) -> String {
    let mut out = String::new();
    let indexes: Vec<IndexStat> = diesel::sql_query(format!(
        "{HARNESS}SELECT indexrelname::TEXT AS indexrelname, idx_scan,
                pg_relation_size(indexrelid) AS size_bytes
         FROM pg_stat_user_indexes WHERE relname = 'harvest_event_batches'
         ORDER BY indexrelname"
    ))
    .load(conn)
    .await
    .expect("index statistics");
    out.push_str(
        "\n-- pg_stat_user_indexes, harvest_event_batches --\nindexrelname|idx_scan|size_bytes\n",
    );
    for i in &indexes {
        let _ = writeln!(out, "{}|{}|{}", i.indexrelname, i.idx_scan, i.size_bytes);
    }
    let tables: Vec<TableStat> = diesel::sql_query(format!(
        "{HARNESS}SELECT t.relname::TEXT AS relname, t.seq_scan, t.seq_tup_read,
                COALESCE(t.idx_scan, 0) AS idx_scan, COALESCE(t.idx_tup_fetch, 0) AS idx_tup_fetch,
                t.n_live_tup, t.n_dead_tup,
                COALESCE(s.heap_blks_read, 0) + COALESCE(s.heap_blks_hit, 0) AS heap_blks,
                COALESCE(s.idx_blks_read, 0) + COALESCE(s.idx_blks_hit, 0) AS idx_blks,
                COALESCE(s.toast_blks_read, 0) + COALESCE(s.toast_blks_hit, 0) AS toast_blks
         FROM pg_stat_user_tables t JOIN pg_statio_user_tables s USING (relid)
         WHERE COALESCE(s.heap_blks_read, 0) + COALESCE(s.heap_blks_hit, 0)
             + COALESCE(s.idx_blks_read, 0) + COALESCE(s.idx_blks_hit, 0) > 0
         ORDER BY t.seq_tup_read DESC, heap_blks DESC, t.relname"
    ))
    .load(conn)
    .await
    .expect("table statistics");
    out.push_str(
        "\n-- pg_stat_user_tables + pg_statio_user_tables, tables the workload touched --\n\
         relname|seq_scan|seq_tup_read|idx_scan|idx_tup_fetch|n_live_tup|n_dead_tup|heap_blks|idx_blks|toast_blks\n",
    );
    for t in &tables {
        let _ = writeln!(
            out,
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            t.relname,
            t.seq_scan,
            t.seq_tup_read,
            t.idx_scan,
            t.idx_tup_fetch,
            t.n_live_tup,
            t.n_dead_tup,
            t.heap_blks,
            t.idx_blks,
            t.toast_blks
        );
    }
    out
}

/// The statement snapshot of one workload, ranked by buffers and by calls.
async fn statement_snapshot(conn: &mut AsyncPgConnection, label: &str, fired: usize) -> String {
    let stats: Vec<StatRow> = diesel::sql_query(format!(
        "{HARNESS}SELECT query, calls, shared_blks_hit, shared_blks_read,
                (shared_blks_hit + shared_blks_read) AS total_buffers,
                temp_blks_written, wal_bytes::BIGINT AS wal_bytes, rows
         FROM pg_stat_statements
         WHERE {DB_SCOPE}
           AND query NOT LIKE '%harness%'
           AND query NOT LIKE '%pg_stat%'
         ORDER BY total_buffers DESC, calls DESC, query"
    ))
    .load(conn)
    .await
    .expect("pg_stat_statements snapshot");
    assert!(
        !stats.is_empty(),
        "pg_stat_statements has no row for {label}"
    );
    let buffers: i64 = stats.iter().map(|s| s.total_buffers).sum();
    let calls: i64 = stats.iter().map(|s| s.calls).sum();
    let wal: i64 = stats.iter().map(|s| s.wal_bytes).sum();
    let temp: i64 = stats.iter().map(|s| s.temp_blks_written).sum();
    let mut out = format!(
        "-- {label}: one scanner tick that fires {fired} batches, then {ADMISSIONS} admissions --\n\
         -- workload total: {buffers} buffers, {calls} calls, {wal} WAL bytes, {temp} temp blocks written --\n"
    );
    let mut render = |title: &str, rows: &[&StatRow]| {
        let _ = writeln!(
            out,
            "\n-- {title} --\n\
             calls|shared_blks_hit|shared_blks_read|total_buffers|pct_buffers|pct_calls|temp_blks_written|wal_bytes|rows|query"
        );
        for s in rows {
            let pct_b = 100.0 * s.total_buffers as f64 / buffers.max(1) as f64;
            let pct_c = 100.0 * s.calls as f64 / calls.max(1) as f64;
            let query: String = s.query.split_whitespace().collect::<Vec<_>>().join(" ");
            let _ = writeln!(
                out,
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
    };
    let by_buffers: Vec<&StatRow> = stats.iter().take(10).collect();
    render("top 10 by buffers", &by_buffers);
    let mut by_calls: Vec<&StatRow> = stats.iter().collect();
    by_calls.sort_by(|a, b| b.calls.cmp(&a.calls).then(a.query.cmp(&b.query)));
    render("top 10 by calls", &by_calls[..by_calls.len().min(10)]);
    let mut by_wal: Vec<&StatRow> = stats.iter().filter(|s| s.wal_bytes > 0).collect();
    by_wal.sort_by(|a, b| b.wal_bytes.cmp(&a.wal_bytes).then(a.query.cmp(&b.query)));
    render("top 10 by WAL bytes", &by_wal[..by_wal.len().min(10)]);
    out
}

/// Run one arm at one size, and return its outcome and artifact text.
///
/// The workload is one scanner tick through the public entry point, then
/// [`ADMISSIONS`] admissions. Half of them append to a batch that is not due,
/// and half open a new batch. The admissions measure the write cost of the
/// index.
async fn capture_arm(t: &Template, size: i64, with_index: bool) -> (Outcome, String, String) {
    let label = format!(
        "{} index, {size} open batches",
        if with_index { "with" } else { "without" }
    );
    let (_db, mut conn) = arm(t, "ebfire_cap_arm", with_index).await;
    conn.batch_execute(
        &FIXTURE_SQL
            .replace("$BATCHES", &size.to_string())
            .replace("$MAX_SIZE", &MAX_SIZE.to_string()),
    )
    .await
    .expect("seed the fixture");
    // The tuple counts flush at most once a second, so wait for them.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    conn.batch_execute("SELECT pg_stat_force_next_flush()")
        .await
        .expect("flush the table statistics");
    let mut summary = fixture_summary(&mut conn, &label).await;
    let plan = assert_plan(&mut conn, with_index).await;
    let mut explain = format!("-- {label}: plan shape --\n{plan}\n");
    let analyzed = explain_due(&mut conn, "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)").await;
    let _ = write!(
        explain,
        "-- {label}: the first due read of the tick --\n{analyzed}\n"
    );
    let append_keys = lines(
        &mut conn,
        &format!(
            "SELECT workflow_name || '|' || batch_key AS item FROM harvest_event_batches
             WHERE fire_at > now() AND jsonb_array_length(buffered_payloads) < 50
             ORDER BY id LIMIT {}",
            ADMISSIONS / 2
        ),
    )
    .await;
    assert_eq!(
        append_keys.len(),
        ADMISSIONS / 2,
        "enough open batches to append to"
    );

    diesel::sql_query(
        "SELECT pg_stat_statements_reset(0, \
         (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
    )
    .execute(&mut conn)
    .await
    .expect("pg_stat_statements_reset() failed; stale counts would leak into this capture");
    conn.batch_execute("SELECT pg_stat_reset()")
        .await
        .expect("reset the table statistics");

    let fired = fire_due_event_batches(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("fire the due batches");
    // Each append names the workflow type of its batch, since a batch is
    // keyed on the type and the key together.
    for (i, target) in append_keys.iter().enumerate() {
        let (workflow_name, key) = target.split_once('|').expect("type|key");
        admit(&mut conn, workflow_name, key, &format!("unused-{i}"), i).await;
    }
    for i in 0..ADMISSIONS / 2 {
        let key = format!("new-key-{i}");
        admit(&mut conn, "batch_wf_0", &key, &format!("new-run-{i}"), i).await;
    }
    // Each fired row leaves, each append keeps its row, and each new key adds one.
    let left = lines(
        &mut conn,
        "SELECT COUNT(*)::TEXT AS item FROM harvest_event_batches",
    )
    .await;
    let fired_rows = i64::try_from(fired).expect("a small count");
    let opened = i64::try_from(ADMISSIONS / 2).expect("a small count");
    assert_eq!(
        left,
        [(size - fired_rows + opened).to_string()],
        "the admissions must append to {opened} batches and open {opened} new ones"
    );

    // The cumulative statistics flush at the end of a transaction, at most
    // once a second, so wait before the snapshot.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    conn.batch_execute("SELECT pg_stat_force_next_flush()")
        .await
        .expect("flush the table statistics");
    let mut pss = statement_snapshot(&mut conn, &label, fired).await;
    pss.push_str(&table_stats(&mut conn).await);
    let _ = writeln!(summary, "fired in the tick|{fired}");
    let out = outcome(&mut conn).await;
    (out, format!("{summary}\n{pss}\n"), explain)
}

#[tokio::test]
#[ignore = "evidence capture: seeds up to 100,000 open batches, six times"]
async fn zz_capture_event_batch_fire_scan_evidence() {
    let Some(t) = template("ebfire_cap_tpl").await else {
        eprintln!("SKIP: no HARVEST_TEST_DATABASE_URL and no Postgres container; nothing captured");
        return;
    };
    {
        let mut conn = AsyncPgConnection::establish(&t.db.url())
            .await
            .expect("connect to the template");
        conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .await
            .expect("pg_stat_statements must be in shared_preload_libraries");
    }
    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("event-batch-fire-scan");
    std::fs::create_dir_all(&out_dir).expect("create the artifact directory");

    let mut results = Vec::new();
    for with_index in [false, true] {
        let name = if with_index { "after" } else { "before" };
        let mut pss = String::new();
        let mut explain = String::new();
        for size in SIZES {
            let (out, text, plan) = capture_arm(&t, size, with_index).await;
            pss.push_str(&text);
            explain.push_str(&plan);
            results.push((with_index, size, out));
        }
        std::fs::write(out_dir.join(format!("{name}.pg_stat_statements.txt")), pss)
            .expect("write the statement snapshot");
        std::fs::write(out_dir.join(format!("{name}.explain.txt")), explain)
            .expect("write the plans");
    }

    let mut rows = String::from("size|runs started|rows left|outcome digest\n");
    for size in SIZES {
        let pick = |w: bool| {
            results
                .iter()
                .find(|(with, s, _)| *with == w && *s == size)
                .map(|(_, _, o)| o)
                .expect("an arm")
        };
        let (without, with) = (pick(false), pick(true));
        assert_eq!(
            without.runs, with.runs,
            "the index must not change the runs or their order at {size}"
        );
        assert_eq!(
            without.left, with.left,
            "the index must not change the rows left at {size}"
        );
        let digest = format!(
            "{:x}",
            fnv_digest(&format!(
                "{}\n{}",
                with.runs.join("\n"),
                with.left.join("\n")
            ))
        );
        let _ = writeln!(
            rows,
            "{size}|{}|{}|{digest}",
            with.runs.len(),
            with.left.len()
        );
    }
    std::fs::write(out_dir.join("result-rows.txt"), &rows).expect("write the result rows");
    eprintln!(
        "equivalence confirmed: both arms fire the same runs in the same order at every size"
    );
    eprintln!("== done. Artifacts in {} ==", out_dir.display());
}

/// A stable 64-bit FNV-1a digest of `text`, for the result artifact.
fn fnv_digest(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}
