#![cfg(feature = "db")]
//! Ledger performance investigation: the write-path cost of
//! `harvest_audit_log_unexported_idx` on a deployment that never configures
//! audit export.
//!
//! Issue #1272 (closed via PR #1518, documentation only) established a gap.
//! `harvest_audit_log_unexported_idx` — `(occurred_at, id) WHERE export_seq
//! IS NULL` — matches every row in `harvest_audit_log` when no audit-export
//! sink is configured. `export_seq` then stays `NULL` forever. The issue
//! text is explicit about the real fixes: (1) lazy index creation on opt-in,
//! and (4) a separate operator-applied migration. The issue calls both
//! real fixes, but only "if the cost proves material on a large audit
//! table". That, the issue says, "wants a measurement, not a guess."
//!
//! `audit_export_tests.rs`'s own module doc repeats the same deferral:
//! "The insert-path index cost is separate (issue #1272)."
//!
//! This harness is that measurement. It seeds a production-shaped
//! `harvest_audit_log` once. The fixture has 500,000 pre-existing rows,
//! realistic operation-name and actor cardinality, and `occurred_at`
//! spread across the 90-day default retention window. `export_seq` stays
//! `NULL` throughout -- the exact steady state of an unconfigured
//! deployment.
//!
//! It then physically clones that one seeded database, with `CREATE
//! DATABASE ... TEMPLATE`, into two byte-identical scenario databases.
//! One keeps the shipped schema, index present. The other drops the
//! index -- the "lazily created, only on opt-in" counterfactual. The
//! clone, not independent re-seeding, is what makes the two scenarios
//! comparable. Independent seeding would build two physically different
//! B-trees, for reasons unrelated to the index under test.
//!
//! The harness then drives the REAL public entry point every mutating
//! management-API handler calls, `audit::insert_audit`, for a batch of
//! new rows in both databases. It
//! reports the buffer/WAL delta `pg_stat_statements` attributes to the
//! `INSERT INTO harvest_audit_log` statement itself, plus the index's own
//! size and its `idx_scan` count. `idx_scan` is observed across a
//! representative read workload: `audit::list_audit`, the real `GET /audit`
//! handler's own entry point.
//!
//! Evidence is `pg_stat_statements` buffers, WAL bytes and `pg_relation_size`.
//! Wall-clock alone is never evidence, per this persona's charter.

// `cast_precision_loss`: every `as f64` cast below is a small, bounded
// evidence count (buffers, WAL bytes, insert calls), for ratio/percentage
// reporting. None comes near `f64`'s 2^52 mantissa limit.
#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use super::claim_bench_support::with_db_name;
use autumn_harvest::audit::{self, AuditFilters};
use autumn_harvest::models::NewAuditRecord;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── DB bootstrap (mirrors activity_enqueue_batch_perf.rs) ──────────────────

type DbGuard = Option<ContainerAsync<Postgres>>;

async fn setup_server() -> (String, DbGuard) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    // Preload `pg_stat_statements` so the fallback path works too, not
    // just an external `HARVEST_TEST_DATABASE_URL` that happens to
    // already have it configured. Review finding on PR #1666, mirroring
    // `claim_bench_support.rs`'s own identical fix: the extension's C
    // hooks only exist once preloaded at postmaster start. A later
    // `CREATE EXTENSION` (which `ensure_pg_stat_statements` still runs,
    // to register the SQL-level objects) cannot retroactively enable
    // them. `.with_cmd(...)` replaces `Image::cmd()` rather than merging
    // with it. So the image's own `fsync=off` default is repeated here
    // explicitly. Otherwise fsync would silently re-enable, slowing
    // down every Docker-fallback run.
    let container = Postgres::default()
        .with_tag("16")
        .with_cmd([
            "-c",
            "shared_preload_libraries=pg_stat_statements",
            "-c",
            "fsync=off",
        ])
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

async fn create_fresh_db(admin_url: &str, name: &str) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await;

    // `with_db_name`, not a naive `rsplit_once('/')` -- review finding on
    // PR #1666. A raw split panics on a legal libpq keyword/value DSN,
    // which has no `/` at all. It also silently drops query-string
    // options (`sslmode`, `application_name`, certs) from a URL-form
    // DSN that carries them.
    let url = with_db_name(admin_url, name).expect("admin url selects a database");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migration bundle");
    drop(conn);
    url
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

/// Best-effort `DROP DATABASE IF EXISTS` for every name given. A
/// persistent `HARVEST_TEST_DATABASE_URL` server would otherwise
/// accumulate this harness's large scenario databases across repeated
/// runs -- review finding on PR #1666. One connection error, or one
/// drop failure, does not stop the rest. Each name gets its own
/// attempt. A failure is logged, not panicked: this cleanup step runs
/// after the evidence has already been captured.
async fn drop_databases(admin_url: &str, names: &[&str]) {
    let Ok(mut admin) = AsyncPgConnection::establish(admin_url).await else {
        eprintln!("cleanup: could not connect to admin database, leaving {names:?} in place");
        return;
    };
    for name in names {
        if let Err(e) = diesel::sql_query(format!("DROP DATABASE IF EXISTS \"{name}\""))
            .execute(&mut admin)
            .await
        {
            eprintln!("cleanup: failed to drop database {name}: {e}");
        }
    }
}

async fn ensure_pg_stat_statements(conn: &mut AsyncPgConnection) {
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(conn)
        .await;
}

async fn reset_stats_for_db(conn: &mut AsyncPgConnection, db_name: &str) {
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(conn)
    .await
    .expect(
        "pg_stat_statements_reset(...) failed -- the HARVEST_TEST_DATABASE_URL role must be \
         able to reset statistics (superuser, or granted EXECUTE on this function)",
    );
}

#[derive(diesel::QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    total_buffers: i64,
}

async fn snapshot_statements(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
                (shared_blks_hit + shared_blks_read) AS total_buffers \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
         ORDER BY total_buffers DESC"
    ))
    .load(conn)
    .await
    .expect(
        "pg_stat_statements query failed -- it must be preloaded via shared_preload_libraries \
         for this capture to produce real evidence rather than fail outright",
    )
}

fn is_audit_insert_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("insert into") && q.contains("harvest_audit_log") && !q.contains("select")
}

/// Current WAL insert position, in bytes since the log's start. See
/// `activity_enqueue_batch_perf.rs::wal_bytes` for why this, not wall-clock,
/// is the admissible write-path measurement.
async fn wal_bytes(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct WalRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        bytes: i64,
    }
    let row: WalRow =
        diesel::sql_query("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint AS bytes")
            .get_result(conn)
            .await
            .expect("read WAL insert position");
    row.bytes
}

async fn relation_size(conn: &mut AsyncPgConnection, name: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct SizeRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        bytes: i64,
    }
    let row: SizeRow = diesel::sql_query(format!("SELECT pg_relation_size('{name}') AS bytes"))
        .get_result(conn)
        .await
        .unwrap_or(SizeRow { bytes: -1 });
    row.bytes
}

#[derive(diesel::QueryableByName, Debug)]
struct IdxScanRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    indexrelname: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    idx_scan: i64,
}

/// Reads `pg_stat_user_indexes` for `harvest_audit_log`, first forcing this
/// backend's own counters to flush.
///
/// PG16's statistics collector batches updates. Querying
/// `pg_stat_user_indexes` right after a scan, on the same backend, can
/// still read the pre-scan counters. That would let a real index use hide
/// behind a stale zero -- review finding on PR #1666. `SELECT
/// pg_stat_force_next_flush()` makes the read authoritative. Issue #1511's
/// own measurement used the identical mechanism for `pg_stat_database`
/// counters, for the identical reason.
async fn index_scan_counts(conn: &mut AsyncPgConnection) -> Vec<IdxScanRow> {
    diesel::sql_query("SELECT pg_stat_force_next_flush()")
        .execute(conn)
        .await
        .expect("force this backend's statistics to flush before reading them");
    diesel::sql_query(
        "SELECT indexrelname, idx_scan FROM pg_stat_user_indexes \
         WHERE relname = 'harvest_audit_log' ORDER BY indexrelname",
    )
    .load(conn)
    .await
    .expect("pg_stat_user_indexes query failed")
}

// ── Production-shaped fixture ───────────────────────────────────────────────

/// Realistic operation-name cardinality and skew. `workflow.start` and
/// `workflow.signal` dominate a real deployment's mutating traffic. The rest
/// of the management surface (schedules, DLQ, batch, audit-export admin) is
/// the long tail. 14 distinct operations, weighted, not a uniform draw.
const OPERATION_WEIGHTS: &[(&str, u32)] = &[
    (audit::OP_WORKFLOW_START, 40),
    (audit::OP_WORKFLOW_SIGNAL, 20),
    (audit::OP_WORKFLOW_CANCEL, 8),
    (audit::OP_WORKFLOW_TERMINATE, 3),
    (audit::OP_WORKFLOW_RESET, 2),
    (audit::OP_WORKFLOW_PAUSE, 4),
    (audit::OP_WORKFLOW_RESUME, 4),
    (audit::OP_SCHEDULE_CREATE, 3),
    (audit::OP_SCHEDULE_UPDATE, 3),
    (audit::OP_SCHEDULE_DELETE, 1),
    (audit::OP_DLQ_REPLAY, 5),
    (audit::OP_DLQ_REPLAY_BULK, 1),
    (audit::OP_BATCH_SUBMIT, 5),
    (audit::OP_RETENTION_RUN_NOW, 1),
];

fn weighted_operation(i: usize) -> &'static str {
    let total: u32 = OPERATION_WEIGHTS.iter().map(|(_, w)| w).sum();
    // Reduce modulo `total` in `usize` first, so the result fits `u32`
    // (`total` itself is a `u32`) before the narrowing cast below.
    let mut n = u32::try_from(i % (total as usize)).expect("value taken modulo a u32 fits u32");
    for (op, w) in OPERATION_WEIGHTS {
        if n < *w {
            return op;
        }
        n -= w;
    }
    OPERATION_WEIGHTS[0].0
}

fn fixture_row(i: usize) -> (String, String, String, String, String, String) {
    let op = weighted_operation(i);
    let actor = format!("operator-{}@example.com", i % 47);
    let target_type = if op.starts_with("workflow") {
        "workflow"
    } else if op.starts_with("schedule") {
        "schedule"
    } else if op.starts_with("dlq") {
        "dead_letter"
    } else {
        "batch"
    };
    // Deterministic, not `Uuid::new_v4()` -- review finding on PR #1666.
    // `fixture_row(i)` must be a pure function of `i`. That way the
    // measured window draws byte-identical `target_id` values in both
    // scenarios. `harvest_audit_log.id`, the actual primary key, is a
    // separate, irreducible source of randomness. This harness cannot
    // control it without bypassing `audit::insert_audit`'s reliance on
    // the schema's own `gen_random_uuid()` default. See that function's
    // call site below for why that reliance is not optional here.
    let target_id = format!("{}-{i}", uuid::Uuid::from_u128(i as u128));
    let route = format!(
        "POST /{target_type}s/{{id}}/{}",
        op.rsplit('.').next().unwrap()
    );
    let status = if i.is_multiple_of(20) {
        "failed"
    } else {
        "succeeded"
    };
    (
        actor,
        op.to_string(),
        target_type.to_string(),
        target_id,
        route,
        status.to_string(),
    )
}

/// Seeds `n` pre-existing audit rows through the REAL batched entry point,
/// `audit::insert_audit_batch`. It then backdates `occurred_at` with a
/// single bulk `UPDATE`, to spread the rows across the 90-day default
/// retention window. `occurred_at` is DB-defaulted to `NOW()` at insert
/// time and is not a `NewAuditRecord` field. Backdating afterward is the
/// only way to give the fixture realistic time-density without
/// hand-writing `INSERT` statements that bypass the code path under test.
/// The backdating `UPDATE` runs once, outside the measured window.
///
/// `occurred_at` is a key column of four of this table's five indexes,
/// `harvest_audit_log_unexported_idx` among them. A non-`HOT` update on
/// an indexed column touches both the heap and the index. So the
/// backdating `UPDATE` leaves one dead entry per row in each of those
/// four indexes. It also leaves one dead heap tuple per row in the
/// table itself. Without cleanup, the measurements below would read a
/// transient, bloated shape. That bloat's exact size would depend on
/// autovacuum's own timing, not on the fixture -- review finding on PR
/// #1666.
///
/// A plain `VACUUM` marks the dead entries reusable, in the heap and
/// in every index. It does not shrink either one. `pg_relation_size`
/// would still report the growth from the update. A later insert
/// could reuse a freed page instead of paying a real allocation cost.
/// That is a second review finding on PR #1666, for the indexes. A
/// third finding, for the heap, followed once the index-only fix did
/// not close the gap on its own. `VACUUM FULL` rewrites the heap into
/// a compact file. As part of that rewrite it always rebuilds every
/// index too. That gives the fixture the shape a table built directly
/// from this data would have. Heap and indexes both, not just the
/// indexes.
///
/// `VACUUM FULL` takes `ACCESS EXCLUSIVE` on this table. That is
/// acceptable only because this connection is the sole client of this
/// disposable per-scenario database at this point in setup, before
/// `op_conn`/`stats_conn` exist.
async fn seed_fixture(conn: &mut AsyncPgConnection, n: usize) {
    const CHUNK: usize = 4999;
    let mut done = 0;
    while done < n {
        let take = CHUNK.min(n - done);
        let rows: Vec<(String, String, String, String, String, String)> =
            (done..done + take).map(fixture_row).collect();
        let records: Vec<NewAuditRecord<'_>> = rows
            .iter()
            .map(
                |(actor, operation, target_type, target_id, route, status)| NewAuditRecord {
                    actor,
                    operation,
                    target_type,
                    target_id: Some(target_id),
                    route_or_command: route,
                    request_id: None,
                    idempotency_key: None,
                    status,
                    error_summary: None,
                    shard_id: Some(0),
                    source: "api",
                },
            )
            .collect();
        audit::insert_audit_batch(conn, &records)
            .await
            .expect("seed batch insert");
        done += take;
    }
    diesel::sql_query(
        "UPDATE harvest_audit_log SET occurred_at = NOW() - (random() * interval '90 days')",
    )
    .execute(conn)
    .await
    .expect("backdate fixture rows across the retention window");
    // `VACUUM FULL`, not plain `VACUUM` or a separate `REINDEX TABLE`.
    // See this function's own doc comment above for why. It compacts
    // both the heap and every index in one pass. The disposable,
    // single-connection database this runs against is why the
    // `ACCESS EXCLUSIVE` lock it takes is acceptable here.
    diesel::sql_query("VACUUM FULL harvest_audit_log")
        .execute(conn)
        .await
        .expect("compact the heap and every index after the backdating UPDATE");
}

const FIXTURE_ROWS: usize = 500_000;
const MEASURED_INSERTS: usize = 3_000;

struct Measurement {
    label: &'static str,
    db_name: String,
    insert_calls: i64,
    insert_buffers: i64,
    insert_wal_bytes: i64,
    unexported_idx_bytes: i64,
    unexported_idx_scans_after_reads: i64,
    occurred_at_idx_scans_after_reads: i64,
}

/// Seeds one shared, fully-prepared fixture database: the real batched
/// insert entry point, the backdating `UPDATE`, `VACUUM`, `REINDEX TABLE`
/// -- everything `seed_fixture` does, run exactly once.
///
/// Review finding on PR #1666: the earlier revision called `seed_fixture`
/// independently inside each of the two `measure` calls. Each call draws
/// its own `random()` timestamps. The database assigns its own
/// `gen_random_uuid()` primary keys too. So the two runs built
/// physically different B-trees, even before the target index was added
/// or dropped. Page occupancy and split points could differ for reasons
/// that have nothing to do with the index under test.
///
/// Seeding once here, then physically cloning this exact database with
/// `CREATE DATABASE ... TEMPLATE` for each scenario below, fixes that.
/// It makes the two scenario databases byte-identical up to the one
/// index difference this harness measures.
async fn seed_base_db(admin: &str) -> String {
    let base_name = unique("audit_write_cost_base");
    let url = create_fresh_db(admin, &base_name).await;
    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;
    seed_fixture(&mut seed_conn, FIXTURE_ROWS).await;
    // `CREATE DATABASE ... TEMPLATE` requires zero other backends
    // connected to the source. Drop this connection now so the clones
    // below never race it.
    drop(seed_conn);
    base_name
}

async fn measure(
    admin: &str,
    base_name: &str,
    label: &'static str,
    drop_unexported_idx: bool,
    owns_server: bool,
) -> Measurement {
    let db_name = unique(&format!("audit_write_cost_{label}"));
    let mut admin_conn = AsyncPgConnection::establish(admin)
        .await
        .expect("connect to admin database");
    diesel::sql_query(format!(
        "CREATE DATABASE \"{db_name}\" TEMPLATE \"{base_name}\""
    ))
    .execute(&mut admin_conn)
    .await
    .expect("clone the shared, already-seeded fixture for this scenario");
    drop(admin_conn);
    // `with_db_name`, not a naive `rsplit_once('/')` -- same review
    // finding on PR #1666 as `create_fresh_db` above.
    let url = with_db_name(admin, &db_name).expect("admin url selects a database");

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");

    if drop_unexported_idx {
        diesel::sql_query("DROP INDEX harvest_audit_log_unexported_idx")
            .execute(&mut seed_conn)
            .await
            .expect("drop the counterfactual index");
    }

    let unexported_idx_bytes = if drop_unexported_idx {
        0
    } else {
        relation_size(&mut seed_conn, "harvest_audit_log_unexported_idx").await
    };

    let mut op_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("op connection");
    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    // A checkpoint right before the measured window, in both scenarios.
    // The first write to a page after a checkpoint carries a full-page
    // image. Under this fixture's write load, that image can arrive from
    // an autovacuum-triggered checkpoint firing unpredictably mid-window,
    // rather than from the code under test. Forcing one here puts both
    // scenarios' measured windows on the same footing.
    //
    // Only when this harness owns the whole server -- the testcontainer
    // path. Review finding on PR #1666: `CHECKPOINT` flushes every
    // database on the server, not just this scenario's. The
    // `HARVEST_TEST_DATABASE_URL` path may point at a shared server.
    // Skip the checkpoint there instead. That trades away some
    // checkpoint-timing noise control. The alternative -- an I/O spike,
    // and a WAL-delta confound from unrelated activity -- would land on
    // a server this harness does not own.
    if owns_server {
        diesel::sql_query("CHECKPOINT")
            .execute(&mut stats_conn)
            .await
            .expect("checkpoint before the measured window");
    } else {
        eprintln!(
            "note: HARVEST_TEST_DATABASE_URL set, server not owned by this harness -- \
             skipping CHECKPOINT; WAL numbers may carry more checkpoint-timing noise"
        );
    }

    // The REAL public entry point every mutating management-API handler
    // calls once per request (`audit::insert_audit`'s own doc comment:
    // "Called after every covered management mutation").
    //
    // `harvest_audit_log.id`, the primary key, is DB-defaulted
    // (`gen_random_uuid()`), not a `NewAuditRecord` field. So each row
    // inserted here still gets its own random `id`, unlike the now-
    // deterministic `target_id` above. That randomness cannot be
    // removed without inserting through something other than
    // `audit::insert_audit` itself. It is also not a testing artifact.
    // A real deployment's own inserts get the identical random `id`
    // distribution. Measuring its effect is measuring the real
    // workload, not adding noise to it. `fixture_row`'s own determinism
    // fix still matters: it isolates this one irreducible source from
    // every other, avoidable one.
    let wal_before = wal_bytes(&mut stats_conn).await;
    for i in 0..MEASURED_INSERTS {
        let (actor, operation, target_type, target_id, route, status) =
            fixture_row(FIXTURE_ROWS + i);
        let record = NewAuditRecord {
            actor: &actor,
            operation: &operation,
            target_type: &target_type,
            target_id: Some(&target_id),
            route_or_command: &route,
            request_id: None,
            idempotency_key: None,
            status: &status,
            error_summary: None,
            shard_id: Some(0),
            source: "api",
        };
        audit::insert_audit(&mut op_conn, &record)
            .await
            .expect("insert_audit should succeed");
    }
    let wal_after = wal_bytes(&mut stats_conn).await;

    let stat_rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let insert_rows: Vec<&StatRow> = stat_rows
        .iter()
        .filter(|r| is_audit_insert_statement(r))
        .collect();
    assert!(
        !insert_rows.is_empty(),
        "pg_stat_statements returned zero rows matching the audit INSERT shape -- check \
         pg_stat_statements.track and shared_preload_libraries",
    );
    let insert_calls: i64 = insert_rows.iter().map(|r| r.calls).sum();
    let insert_buffers: i64 = insert_rows.iter().map(|r| r.total_buffers).sum();

    // Representative read workload: the real `GET /audit` entry point,
    // `audit::list_audit`, with filters an operator actually uses --
    // recency, actor, operation. None of these touch
    // `harvest_audit_log_unexported_idx`. It is not eligible for any of
    // them, exactly as it would not be for the real handler in production.
    let _ = audit::list_audit(&mut op_conn, &AuditFilters::default())
        .await
        .expect("list_audit default");
    let _ = audit::list_audit(
        &mut op_conn,
        &AuditFilters {
            actor: Some("operator-3@example.com".to_string()),
            ..AuditFilters::default()
        },
    )
    .await
    .expect("list_audit by actor");
    let _ = audit::list_audit(
        &mut op_conn,
        &AuditFilters {
            operation: Some(audit::OP_WORKFLOW_CANCEL.to_string()),
            ..AuditFilters::default()
        },
    )
    .await
    .expect("list_audit by operation");

    let idx_scans = index_scan_counts(&mut op_conn).await;
    let unexported_idx_scans_after_reads = idx_scans
        .iter()
        .find(|r| r.indexrelname == "harvest_audit_log_unexported_idx")
        .map_or(-1, |r| r.idx_scan);
    let occurred_at_idx_scans_after_reads = idx_scans
        .iter()
        .find(|r| r.indexrelname == "harvest_audit_occurred_at_idx")
        .map_or(-1, |r| r.idx_scan);

    Measurement {
        label,
        db_name,
        insert_calls,
        insert_buffers,
        insert_wal_bytes: wal_after - wal_before,
        unexported_idx_bytes,
        unexported_idx_scans_after_reads,
        occurred_at_idx_scans_after_reads,
    }
}

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- Ledger findings issue, \
            harvest_audit_log_unexported_idx write-path cost (issue #1272)"]
async fn zz_capture_audit_log_unexported_idx_write_cost_evidence() {
    let (admin, guard) = setup_server().await;
    let owns_server = guard.is_some();
    let base_name = seed_base_db(&admin).await;

    let before = measure(
        &admin,
        &base_name,
        "before_index_present",
        false,
        owns_server,
    )
    .await;
    let after = measure(&admin, &base_name, "after_index_dropped", true, owns_server).await;

    for m in [&before, &after] {
        eprintln!(
            "label={} insert_calls={} insert_buffers={} insert_wal_bytes={} \
             unexported_idx_bytes={} unexported_idx_scans_after_reads={} \
             occurred_at_idx_scans_after_reads={}",
            m.label,
            m.insert_calls,
            m.insert_buffers,
            m.insert_wal_bytes,
            m.unexported_idx_bytes,
            m.unexported_idx_scans_after_reads,
            m.occurred_at_idx_scans_after_reads,
        );
    }

    let buffers_per_insert_before = before.insert_buffers as f64 / before.insert_calls as f64;
    let buffers_per_insert_after = after.insert_buffers as f64 / after.insert_calls as f64;
    let wal_per_insert_before = before.insert_wal_bytes as f64 / MEASURED_INSERTS as f64;
    let wal_per_insert_after = after.insert_wal_bytes as f64 / MEASURED_INSERTS as f64;

    eprintln!(
        "buffers/insert: before={buffers_per_insert_before:.3} after={buffers_per_insert_after:.3} \
         delta={:.2}%",
        (buffers_per_insert_after - buffers_per_insert_before) / buffers_per_insert_before * 100.0
    );
    eprintln!(
        "wal_bytes/insert: before={wal_per_insert_before:.1} after={wal_per_insert_after:.1} \
         delta={:.2}%",
        (wal_per_insert_after - wal_per_insert_before) / wal_per_insert_before * 100.0
    );
    eprintln!(
        "harvest_audit_log_unexported_idx size at {FIXTURE_ROWS} rows: {} bytes ({:.2} MiB)",
        before.unexported_idx_bytes,
        before.unexported_idx_bytes as f64 / (1024.0 * 1024.0)
    );
    eprintln!(
        "idx_scan(harvest_audit_log_unexported_idx) after seed+{MEASURED_INSERTS} inserts+3 \
         real list_audit reads: {}",
        before.unexported_idx_scans_after_reads
    );

    // Best-effort cleanup, ahead of the assertions below (review finding
    // on PR #1666). On the `HARVEST_TEST_DATABASE_URL` path this
    // harness creates one 500,000-row base database plus two full
    // clones. Every name carries a fresh UUID. So repeated runs
    // against a persistent server would otherwise accumulate large
    // databases forever. Placed before the assertions, so a positive-
    // control or zero-scan failure still leaves the server clean. A
    // hard crash inside `measure` itself, before reaching here, is the
    // one case this does not cover.
    drop_databases(&admin, &[&base_name, &before.db_name, &after.db_name]).await;

    // Positive control (review finding on PR #1666). If the read path
    // never registers on ANY index here, a zero on the target index is
    // worthless. It would mean the flush/read mechanism itself is not
    // seeing scans, not that this index specifically went unused.
    // `list_audit`'s default call orders by `occurred_at DESC` with no
    // filter. `harvest_audit_occurred_at_idx` exists to serve exactly that
    // call. So it must show at least one real scan across the same three
    // `list_audit` calls whose `harvest_audit_log_unexported_idx` count is
    // asserted zero below.
    assert!(
        before.occurred_at_idx_scans_after_reads > 0,
        "harvest_audit_occurred_at_idx must show at least one real scan from the same \
         list_audit reads -- a zero here means the stats-flush/read mechanism itself is not \
         visible, which would make the unexported-index zero below meaningless rather than a \
         real finding (occurred_at_idx_scans_after_reads={})",
        before.occurred_at_idx_scans_after_reads,
    );

    assert_eq!(
        before.unexported_idx_scans_after_reads, 0,
        "harvest_audit_log_unexported_idx must never be scanned by the real read path \
         (audit::list_audit) when export is unconfigured -- if this fails, the index is not \
         the pure write tax this investigation assumes",
    );
}
