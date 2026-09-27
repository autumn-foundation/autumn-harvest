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

async fn create_fresh_db(admin_url: &str, name: &str, cleanup: &DbCleanupGuard) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await;
    // Registered right after the statement above, succeeded or not --
    // review finding on PR #1666. A panic in migration below must not
    // skip cleanup for a database that `CREATE DATABASE` already made.
    // Registering an unmade name is harmless: `drop_databases_impl` runs
    // `DROP DATABASE IF EXISTS`.
    cleanup.register(name);

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

/// Postgres truncates every identifier -- quoted or not -- to
/// `NAMEDATALEN - 1` (63) bytes at the lexer level. `CREATE DATABASE
/// "<name>"` with a longer `name` then creates a shorter database than
/// this function returns. Every later `with_db_name(admin, name)` call
/// builds a connection URL for the untruncated name, so it fails to
/// connect to the database `CREATE DATABASE` actually made. Review
/// finding on PR #1666: the full 32-hex-character `Uuid::simple()` form
/// pushed both scenario labels' names to 69-70 bytes. 16 hex characters
/// (64 bits) keeps every current prefix well under the limit. That is
/// still, for the two or three names one test run creates, effectively
/// collision-free. The assertion below still catches it explicitly, not
/// silently via a Postgres-side truncation, if a future longer prefix
/// pushes the total over the limit again.
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

/// Best-effort `DROP DATABASE IF EXISTS` for every name given. One
/// connection error, or one drop failure, does not stop the rest. Each
/// name gets its own attempt. A failure is logged, not panicked. This
/// runs during cleanup. By then the evidence it would report on is
/// already captured, or the run has already failed.
async fn drop_databases_impl(admin_url: &str, names: &[String]) {
    let Ok(mut admin) = AsyncPgConnection::establish(admin_url).await else {
        eprintln!("cleanup: could not connect to admin database, leaving {names:?} in place");
        return;
    };
    for name in names {
        // Self-found running this fix. `DROP DATABASE` can race a
        // just-dropped `AsyncPgConnection` whose own backend has not
        // finished closing on the server yet. It then fails with "is
        // being accessed by other users", even though nothing in this
        // process still holds it open. A few short retries absorb that
        // race without turning a best-effort cleanup step into one that
        // blocks indefinitely.
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

/// Registers every scenario database this harness creates. `register`
/// is called immediately after each `CREATE DATABASE` succeeds. That
/// is before anything else in that function can fail. So a panic
/// partway through `seed_base_db`/`measure` still leaves this guard
/// knowing what to clean up, not just the success path. Review finding
/// on PR #1666: an earlier revision cleaned up only after both `measure`
/// calls returned normally. A mid-measurement panic on the
/// `HARVEST_TEST_DATABASE_URL` path still leaked whatever had already
/// been created.
///
/// Two ways to actually run that cleanup, for two different cases --
/// see `cleanup` and `Drop::drop` below for why they differ.
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

    /// The success-path cleanup call. Self-found running the first
    /// version of this guard: a blocking `Drop`, below, self-deadlocks
    /// here. This test's default `#[tokio::test]` runtime is
    /// single-threaded. A connection's own graceful async shutdown
    /// needs that same thread to be polled again. Only then can its
    /// background I/O driver task notice the handle was dropped, and
    /// actually close the socket. Only then does Postgres see the
    /// backend go away. A plain `.await` here does that naturally. That
    /// is why the retries inside `drop_databases_impl` then succeed
    /// quickly, instead of timing out. `Drop::drop` cannot `.await`.
    /// That is exactly why it cannot do this the same way -- see its
    /// own doc comment.
    async fn cleanup(&self) {
        let names = self.take_names();
        if !names.is_empty() {
            drop_databases_impl(&self.admin_url, &names).await;
        }
    }
}

impl Drop for DbCleanupGuard {
    /// A last-resort fallback only. The main test body no longer relies
    /// on this path for its own panics -- review finding on PR #1666.
    /// `tokio::spawn`ing the measurement work catches a panic there as
    /// an `Err(JoinError)`, instead of unwinding past this guard. So the
    /// test function itself can `.await` `cleanup()` in response. What
    /// is left for `Drop::drop` to cover is narrower: a panic inside
    /// `cleanup()` itself, or a path that reaches neither `cleanup()`
    /// nor the `tokio::spawn`/`Err` arm. `Drop::drop` is synchronous, so
    /// it cannot `.await` the connections above closing before it
    /// attempts `DROP DATABASE` itself.
    ///
    /// An earlier revision spawned a thread and blocked on it with
    /// `JoinHandle::join`. That was an attempt to get a synchronous
    /// call out of an async cleanup -- review finding on PR #1666's
    /// own testing, not Codex's. `join` blocks this OS thread. That
    /// thread is the only one the `current_thread` runtime has, to run
    /// anything on. That includes the very connection-shutdown tasks
    /// the spawned thread's retries were waiting on. That is a real
    /// deadlock, not a slow retry. It reproduced identically at every
    /// retry budget tried, up to 15 seconds.
    ///
    /// The fix here does not block. It spawns a detached thread and
    /// returns immediately, so it cannot deadlock. The cost: cleanup
    /// is no longer guaranteed to finish before the process can exit.
    /// This is now a narrow, last-resort path rather than the main
    /// panic-path handler, so that cost applies to a correspondingly
    /// narrower set of failures.
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
async fn seed_base_db(admin: &str, cleanup: &DbCleanupGuard) -> String {
    let base_name = unique("audit_write_cost_base");
    let url = create_fresh_db(admin, &base_name, cleanup).await;
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
    // Registered before `.expect()` below can panic -- same reasoning
    // as `create_fresh_db`'s own registration.
    cleanup.register(&db_name);
    create_result.expect("clone the shared, already-seeded fixture for this scenario");
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
    // Review finding on PR #1666: `pg_current_wal_lsn()` (the earlier
    // approach) advances for writes to every database on the server,
    // not just this one. On the `HARVEST_TEST_DATABASE_URL` path that
    // server may be shared. Unrelated concurrent traffic could then
    // dwarf or reverse the delta this harness reports. `wal_bytes`,
    // read below from the same `pg_stat_statements` snapshot the
    // buffer counters already come from, is per-statement instead --
    // attributable only to the matched `INSERT` calls.
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
    // `Arc`, not a plain local -- review finding on PR #1666. `cleanup`
    // must survive a panic anywhere in the `tokio::spawn`ed block below,
    // so the `Err` arm can still call its real `async fn cleanup(&self)`.
    // A plain local would itself be dropped while unwinding that block,
    // reaching only `Drop::drop`'s detached, un-joined thread -- exactly
    // the gap this restructuring closes. See `Drop for DbCleanupGuard`'s
    // own doc comment for why that thread cannot be a reliable substitute.
    let cleanup = std::sync::Arc::new(DbCleanupGuard::new(admin.clone()));

    // The measurement work runs as its own task -- review finding on PR
    // #1666. `tokio::spawn` catches a panic internally and reports it as
    // `Err(JoinError)` instead of unwinding this function's own stack.
    // That leaves this `async fn` itself un-panicked. It can still
    // `.await` real cleanup in the `Err` arm below, no matter where
    // inside the spawned block the panic came from. A plain
    // `std::thread` would not help here. This is `tokio::spawn`,
    // another task on the same single-threaded runtime, not another OS
    // thread. So it carries none of `Drop`'s own self-deadlock risk.
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

    // Explicit, awaited cleanup here, ahead of the assertions below,
    // rather than waiting for `cleanup`'s own end-of-scope `Drop`. A
    // positive-control or zero-scan assertion failure below should still
    // leave the server clean immediately, not just eventually.
    // `DbCleanupGuard::drop` remains a last-resort fallback only.
    // Panics from the measurement work itself are already handled by
    // the `tokio::spawn`/`Err` arm above, which awaits this same method.
    cleanup.cleanup().await;

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
