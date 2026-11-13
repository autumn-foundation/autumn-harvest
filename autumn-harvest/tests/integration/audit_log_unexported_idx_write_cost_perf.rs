#![cfg(feature = "db")]
//! Ledger performance investigation: the write-path cost of
//! `harvest_audit_log_unexported_idx` on a deployment that never configures
//! audit export.
//!
//! Issue #1272 (closed via PR #1518, documentation only) established a gap.
//! `harvest_audit_log_unexported_idx`, `(occurred_at, id) WHERE export_seq
//! IS NULL`, matches every row of `harvest_audit_log` when no audit-export
//! sink is configured. `export_seq` then stays `NULL` forever. The issue
//! names the real fixes: (1) lazy index creation on opt-in, and (4) a
//! separate operator-applied migration. It wants them only "if the cost
//! proves material on a large audit table". That, the issue says, "wants a
//! measurement, not a guess."
//!
//! `audit_export_tests.rs`'s own module doc repeats the same deferral:
//! "The insert-path index cost is separate (issue #1272)."
//!
//! Issue #1667 shipped option (1): the migration drops the index and the exporter
//! builds it lazily. This harness now builds it for the with-index arm.
//!
//! This harness is that measurement. It seeds a production-shaped
//! `harvest_audit_log` once: 500,000 rows, realistic operation-name and
//! actor cardinality, and `occurred_at` spread across the 90-day default
//! retention window. `export_seq` stays `NULL` throughout, as on an
//! unconfigured deployment.
//!
//! `CREATE DATABASE ... TEMPLATE` then clones that database into two
//! byte-identical scenario databases. One keeps the shipped schema, index
//! present. The other drops the index: the "lazily created, only on opt-in"
//! counterfactual. The clone makes the two scenarios comparable. Independent
//! seeding would build two physically different B-trees, for reasons
//! unrelated to the index under test.
//!
//! The harness then calls `audit::insert_audit`, the entry point of every
//! mutating management-API handler, for a batch of new rows in both
//! databases. It reports the buffer and WAL delta that `pg_stat_statements`
//! attributes to the `INSERT INTO harvest_audit_log` statement. It also
//! reports the index size, and its `idx_scan` count across a read workload
//! of `audit::list_audit`, the entry point of `GET /audit`.
//!
//! Evidence is `pg_stat_statements` buffers, WAL bytes and `pg_relation_size`.
//! Wall-clock time alone is never evidence.

// `cast_precision_loss`: each `as f64` cast is a small evidence count
// (buffers, WAL bytes, insert calls) for a ratio. None nears the 2^52
// mantissa limit of `f64`.
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
    // Preload `pg_stat_statements`, as `claim_bench_support.rs` does. Its C
    // hooks exist only when preloaded at postmaster start. A later `CREATE
    // EXTENSION` registers the SQL objects but cannot enable the hooks.
    // `.with_cmd(...)` replaces `Image::cmd()`, so it repeats the image's
    // `fsync=off` default. Otherwise fsync would slow every Docker run.
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

async fn create_fresh_db(admin_url: &str, name: &str, cleanup: &DbCleanupGuard) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await;
    // Register the name whether or not the statement succeeded. A panic in
    // the migration must not skip cleanup of a database that exists. An
    // unmade name is harmless: `drop_databases_impl` uses `IF EXISTS`.
    cleanup.register(name);

    // Not `rsplit_once('/')`: a raw split panics on a legal libpq
    // keyword/value DSN, which has no `/`. It also drops query-string
    // options (`sslmode`, `application_name`, certs) from a URL DSN.
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

/// A unique database name of 63 bytes or fewer.
///
/// Postgres truncates every identifier, quoted or not, to `NAMEDATALEN - 1`
/// (63) bytes. A longer name makes a database that `with_db_name` cannot
/// reach. A full 32-hex `Uuid::simple()` pushed the scenario names to 69-70
/// bytes. 16 hex characters (64 bits) keep each prefix under the limit and
/// are collision-free for a few names. The assertion catches a longer prefix.
fn unique(prefix: &str) -> String {
    let suffix = &uuid::Uuid::new_v4().simple().to_string()[..16];
    let name = format!("{prefix}_{suffix}");
    assert!(
        name.len() <= 63,
        "database name {name:?} ({} bytes) exceeds Postgres's 63-byte identifier limit -- \
         shorten the prefix",
        name.len(),
    );
    name
}

/// Best-effort `DROP DATABASE IF EXISTS` for every name given.
///
/// One failure does not stop the rest. A failure is logged, not panicked.
/// By cleanup time the evidence is captured, or the run has already failed.
async fn drop_databases_impl(admin_url: &str, names: &[String]) {
    let Ok(mut admin) = AsyncPgConnection::establish(admin_url).await else {
        eprintln!("cleanup: could not connect to admin database, leaving {names:?} in place");
        return;
    };
    for name in names {
        // `DROP DATABASE` can race a dropped `AsyncPgConnection` whose
        // backend has not closed yet. It then fails with "is being accessed
        // by other users". A few short retries absorb that race, and the
        // cleanup still cannot block forever.
        let mut attempts_left = 30;
        loop {
            let result = diesel::sql_query(format!("DROP DATABASE IF EXISTS \"{name}\""))
                .execute(&mut admin)
                .await;
            match result {
                Ok(_) => break,
                Err(_) if attempts_left > 1 => {
                    attempts_left -= 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(e) => {
                    eprintln!("cleanup: failed to drop database {name}: {e}");
                    break;
                }
            }
        }
    }
}

/// Registers every scenario database this harness creates.
///
/// `register` runs right after each `CREATE DATABASE`, before anything else
/// can fail. So a panic in `seed_base_db` or `measure` still leaves the guard
/// a full list. Otherwise a panic on the `HARVEST_TEST_DATABASE_URL` path
/// leaks the databases made so far.
///
/// `cleanup` and `Drop::drop` run the cleanup for two different cases. Their
/// doc comments say why they differ.
struct DbCleanupGuard {
    admin_url: String,
    names: std::sync::Mutex<Vec<String>>,
}

impl DbCleanupGuard {
    const fn new(admin_url: String) -> Self {
        Self {
            admin_url,
            names: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn register(&self, name: &str) {
        self.names
            .lock()
            .expect("cleanup registry mutex")
            .push(name.to_string());
    }

    fn take_names(&self) -> Vec<String> {
        std::mem::take(&mut *self.names.lock().expect("cleanup registry mutex"))
    }

    /// The success-path cleanup.
    ///
    /// A blocking `Drop` self-deadlocks here. The default `#[tokio::test]`
    /// runtime is single-threaded. A connection closes its socket only when
    /// that thread polls its I/O driver task again. Only then does Postgres
    /// see the backend go away. A plain `.await` lets that happen, so the
    /// retries in `drop_databases_impl` succeed quickly. `Drop::drop` cannot
    /// `.await`, so it cannot work this way.
    async fn cleanup(&self) {
        let names = self.take_names();
        if !names.is_empty() {
            drop_databases_impl(&self.admin_url, &names).await;
        }
    }
}

impl Drop for DbCleanupGuard {
    /// A last-resort fallback only.
    ///
    /// The test spawns the measurement work, so a panic there arrives as an
    /// `Err(JoinError)`, and the test awaits `cleanup()`. This path covers
    /// the rest: a panic inside `cleanup()`, or a path that reaches neither.
    /// `Drop::drop` is synchronous, so it cannot await the connections that
    /// close before its `DROP DATABASE`.
    ///
    /// Do not block on a spawned thread with `JoinHandle::join`. `join`
    /// blocks the one thread of the `current_thread` runtime. The
    /// connection-shutdown tasks that the retries wait on need that thread.
    /// That is a real deadlock, not a slow retry. It reproduced at every
    /// retry budget tried, up to 15 seconds.
    ///
    /// So this spawns a detached thread and returns at once. It cannot
    /// deadlock. The cost is that cleanup may not finish before the process
    /// exits. That cost applies only to the narrow set of failures above.
    fn drop(&mut self) {
        let names = self.take_names();
        if names.is_empty() {
            return;
        }
        let admin_url = self.admin_url.clone();
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build a runtime for panic-safe cleanup")
                .block_on(drop_databases_impl(&admin_url, &names));
        });
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
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    wal_bytes: i64,
}

async fn snapshot_statements(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
                (shared_blks_hit + shared_blks_read) AS total_buffers, \
                wal_bytes::bigint AS wal_bytes \
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
/// PG16 batches its statistics updates. A read of `pg_stat_user_indexes`
/// right after a scan, on the same backend, can still see the pre-scan
/// counters. A real index use could then hide behind a stale zero. `SELECT
/// pg_stat_force_next_flush()` makes the read authoritative. The measurement
/// of issue #1511 used it on `pg_stat_database` for the same reason.
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
    // Deterministic, not `Uuid::new_v4()`. `fixture_row(i)` must be a pure
    // function of `i`, so both scenarios draw the same `target_id` values.
    // The primary key `harvest_audit_log.id` stays random. Only a bypass of
    // `audit::insert_audit` and its `gen_random_uuid()` default could control it.
    // The call site in `measure` says why that bypass is wrong.
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

/// Seeds `n` audit rows through `audit::insert_audit_batch`, then
/// backdates them across the 90-day default retention window.
///
/// `occurred_at` defaults to `NOW()` in the database and is not a
/// `NewAuditRecord` field. A hand-written `INSERT` would bypass the code
/// under test, so one bulk `UPDATE` backdates the rows afterward. It runs
/// once, outside the measured window.
///
/// `occurred_at` is a key column of four of the five indexes of this table,
/// `harvest_audit_log_unexported_idx` among them. A non-`HOT` update of an
/// indexed column touches the heap and the index. So the `UPDATE` leaves a
/// dead heap tuple per row, and a dead entry per row in each of those
/// indexes. That bloat would make the measurements depend on autovacuum
/// timing, not on the fixture.
///
/// A plain `VACUUM` marks dead entries reusable but shrinks nothing.
/// `pg_relation_size` would still report the growth. A later insert could
/// reuse a freed page instead of paying a real allocation. `VACUUM FULL`
/// rewrites the heap into a compact file and rebuilds every index. The
/// fixture then has the shape of a table built directly from this data.
///
/// `VACUUM FULL` takes `ACCESS EXCLUSIVE` on this table. That is acceptable
/// only because this connection is the sole client of the disposable
/// database at this point, before `op_conn` and `stats_conn` exist.
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
    // `VACUUM FULL`, not plain `VACUUM` or a separate `REINDEX TABLE`: the
    // doc comment says why.
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

/// Seeds one shared fixture database with `seed_fixture`, exactly once.
///
/// A seed per `measure` call would draw its own `random()` timestamps and
/// `gen_random_uuid()` primary keys. The two runs would build physically
/// different B-trees, so page occupancy and split points could differ. Each
/// scenario clones this database with `CREATE DATABASE ... TEMPLATE`
/// instead. The two databases then differ only in the measured index.
async fn seed_base_db(admin: &str, cleanup: &DbCleanupGuard) -> String {
    let base_name = unique("audit_write_cost_base");
    let url = create_fresh_db(admin, &base_name, cleanup).await;
    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;
    seed_fixture(&mut seed_conn, FIXTURE_ROWS).await;
    // `CREATE DATABASE ... TEMPLATE` needs a source with no other backend,
    // so the clones must not race this connection.
    drop(seed_conn);
    base_name
}

async fn measure(
    admin: &str,
    base_name: &str,
    label: &'static str,
    drop_unexported_idx: bool,
    owns_server: bool,
    cleanup: &DbCleanupGuard,
) -> Measurement {
    let db_name = unique(&format!("audit_write_cost_{label}"));
    let mut admin_conn = AsyncPgConnection::establish(admin)
        .await
        .expect("connect to admin database");
    let create_result = diesel::sql_query(format!(
        "CREATE DATABASE \"{db_name}\" TEMPLATE \"{base_name}\""
    ))
    .execute(&mut admin_conn)
    .await;
    // Register before `.expect()` can panic, as in `create_fresh_db`.
    cleanup.register(&db_name);
    create_result.expect("clone the shared, already-seeded fixture for this scenario");
    drop(admin_conn);
    // Not `rsplit_once('/')`, for the reason in `create_fresh_db`.
    let url = with_db_name(admin, &db_name).expect("admin url selects a database");

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");

    // Issue #1667: no migration creates the index. The shipped-schema arm
    // builds it, as the exporter does on its first tick.
    if !drop_unexported_idx {
        diesel::sql_query(
            "CREATE INDEX IF NOT EXISTS harvest_audit_log_unexported_idx \
             ON harvest_audit_log (occurred_at, id) WHERE export_seq IS NULL",
        )
        .execute(&mut seed_conn)
        .await
        .expect("build the index for the with-index arm");
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
    // image. An autovacuum checkpoint could fire mid-window and add one.
    // A forced checkpoint here puts both windows on the same footing.
    //
    // Only on the testcontainer path, where this harness owns the server.
    // `CHECKPOINT` flushes every database on the server. The
    // `HARVEST_TEST_DATABASE_URL` server may be shared, so skip it there.
    // That costs some noise control. The alternative puts an I/O spike and
    // a WAL confound on a server this harness does not own.
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

    // Every mutating management-API handler calls `audit::insert_audit`
    // once per request. Its doc comment says "Called after every covered
    // management mutation".
    //
    // The primary key `harvest_audit_log.id` defaults to
    // `gen_random_uuid()` and is not a `NewAuditRecord` field. So each row
    // gets a random `id`, unlike the deterministic `target_id`. Only an
    // insert that bypasses `audit::insert_audit` could remove it. A real
    // deployment has the same random `id` distribution, so it is part of
    // the workload, not noise. `fixture_row` still removes every other,
    // avoidable source.
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
    // Not `pg_current_wal_lsn()`: it advances for every database on the
    // server, and the `HARVEST_TEST_DATABASE_URL` server may be shared.
    // Other traffic could dwarf or reverse the delta. The `wal_bytes` of
    // `pg_stat_statements` counts only the matched `INSERT` calls.
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
    let insert_wal_bytes: i64 = insert_rows.iter().map(|r| r.wal_bytes).sum();

    // A read workload through `audit::list_audit`, the `GET /audit` entry
    // point, with the filters an operator uses: recency, actor, operation.
    // `harvest_audit_log_unexported_idx` is not eligible for any of them,
    // as in production.
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
        insert_wal_bytes,
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
    // An `Arc`, so `cleanup` survives a panic in the spawned block and the
    // `Err` arm can await `cleanup()`. A plain local would drop while that
    // block unwinds, and reach only the detached thread of `Drop::drop`.
    // The doc comment of `Drop for DbCleanupGuard` says why that thread is
    // not reliable.
    let cleanup = std::sync::Arc::new(DbCleanupGuard::new(admin.clone()));

    // The measurement runs as its own task. `tokio::spawn` reports a panic
    // as `Err(JoinError)` and does not unwind this function. So the `Err`
    // arm can await cleanup, wherever the panic came from. The task runs
    // on the same runtime, not on another OS thread, so it cannot deadlock
    // as a blocking `Drop` does.
    let spawned_admin = admin.clone();
    let spawned_cleanup = std::sync::Arc::clone(&cleanup);
    let measured = tokio::spawn(async move {
        let base_name = seed_base_db(&spawned_admin, &spawned_cleanup).await;
        let before = measure(
            &spawned_admin,
            &base_name,
            "before_index_present",
            false,
            owns_server,
            &spawned_cleanup,
        )
        .await;
        let after = measure(
            &spawned_admin,
            &base_name,
            "after_index_dropped",
            true,
            owns_server,
            &spawned_cleanup,
        )
        .await;
        (before, after)
    })
    .await;

    let (before, after) = match measured {
        Ok(pair) => pair,
        Err(join_err) => {
            cleanup.cleanup().await;
            std::panic::resume_unwind(join_err.into_panic());
        }
    };

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

    // Await cleanup before the assertions, not at the end-of-scope `Drop`.
    // A failed assertion then still leaves the server clean at once.
    cleanup.cleanup().await;

    // Positive control. If the reads register on no index, a zero on the
    // target index proves nothing: the flush and read may see no scans.
    // The default `list_audit` call orders by `occurred_at DESC` with no
    // filter, which `harvest_audit_occurred_at_idx` serves. So that index
    // must show a scan across the same three reads.
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
