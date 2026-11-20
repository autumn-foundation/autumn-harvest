#![cfg(feature = "db")]
//! The DLQ depth counter.
//!
//! `harvest_dead_letter_counts` holds the count of `harvest_dead_letters`.
//! Triggers keep it exact, so the depth gauge reads a few rows, not the DLQ.
//! See `docs/performance-dlq-depth-counter.md`.
//!
//! Each test compares the counter with an exact count. So each test runs in
//! its own database, and no other suite writes there.

use std::time::Duration;

use autumn_harvest::dlq::{self, NewDeadLetterEntry};
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

use crate::queue_age_gauge_perf::setup_server;
use crate::throwaway_db::ThrowawayDb;

/// A migrated database of its own. The container, if any, lives as long.
struct Fixture {
    conn: AsyncPgConnection,
    db: ThrowawayDb,
    _server: Option<ContainerAsync<Postgres>>,
}

async fn fixture(prefix: &str) -> Fixture {
    let (admin, server) = setup_server().await;
    let db = ThrowawayDb::create_on(&admin, prefix).await;
    let conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connect to the throwaway database");
    Fixture {
        conn,
        db,
        _server: server,
    }
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct PlanRow {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    plan: String,
}

#[derive(diesel::QueryableByName)]
struct Reg {
    #[diesel(sql_type = Nullable<Text>)]
    reg: Option<String>,
}

async fn scalar(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(sql)
        .get_result::<Count>(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .n
}

/// The exact count, read the slow way.
async fn exact(conn: &mut AsyncPgConnection) -> i64 {
    scalar(
        conn,
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_dead_letters",
    )
    .await
}

/// The gauge value.
async fn gauge(conn: &mut AsyncPgConnection) -> i64 {
    dlq::dead_letter_count(conn).await.expect("gauge read")
}

async fn counter_rows(conn: &mut AsyncPgConnection) -> i64 {
    scalar(
        conn,
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_dead_letter_counts",
    )
    .await
}

async fn assert_exact(conn: &mut AsyncPgConnection, step: &str) {
    let (gauge, exact) = (gauge(conn).await, exact(conn).await);
    assert_eq!(gauge, exact, "after {step}, the gauge must equal COUNT(*)");
}

fn entry(n: usize) -> NewDeadLetterEntry {
    NewDeadLetterEntry {
        original_task_id: Uuid::new_v4(),
        queue_name: "default".to_owned(),
        task_type: "activity".to_owned(),
        workflow_exec_id: None,
        activity_name: Some("act".to_owned()),
        input: serde_json::json!({ "n": n }),
        error: "activity failed".to_owned(),
        attempts: 3,
        owner: None,
        severity: None,
    }
}

/// Inserts `n` dead letters in one statement.
async fn insert_rows(conn: &mut AsyncPgConnection, n: i64) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_dead_letters \
             (original_task_id, queue_name, task_type, input, error, attempts) \
         SELECT gen_random_uuid(), 'default', 'activity', \
                jsonb_build_object('n', g), 'activity failed', 3 \
         FROM generate_series(1, {n}) g"
    ))
    .await
    .expect("insert dead letters");
}

/// Each `Relation Name` of a JSON plan, and the buffers of its top node.
fn plan_relations_and_buffers(plan: &serde_json::Value) -> (Vec<String>, i64) {
    fn walk(node: &serde_json::Value, out: &mut Vec<String>) {
        if let Some(name) = node.get("Relation Name").and_then(|v| v.as_str()) {
            out.push(name.to_owned());
        }
        for child in node
            .get("Plans")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            walk(child, out);
        }
    }
    let top = &plan[0]["Plan"];
    let mut relations = Vec::new();
    walk(top, &mut relations);
    let buffers = ["Shared Hit Blocks", "Shared Read Blocks"]
        .iter()
        .map(|k| top.get(*k).and_then(serde_json::Value::as_i64).unwrap_or(0))
        .sum();
    (relations, buffers)
}

#[tokio::test]
async fn the_gauge_read_does_not_scale_with_the_dlq() {
    let mut f = fixture("dlq_depth_read").await;
    insert_rows(&mut f.conn, 50_000).await;
    f.conn
        .batch_execute("VACUUM ANALYZE harvest_dead_letters")
        .await
        .expect("analyze");

    // The first call on a connection loads the function from the catalog. A
    // pooled sampler connection pays that once.
    gauge(&mut f.conn).await;
    let row: PlanRow = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}",
        dlq::DLQ_DEPTH_SQL
    ))
    .get_result(&mut f.conn)
    .await
    .expect("explain the gauge read");
    let plan: serde_json::Value = serde_json::from_str(&row.plan).expect("JSON plan");
    let (relations, buffers) = plan_relations_and_buffers(&plan);

    assert!(
        !relations.iter().any(|r| r == "harvest_dead_letters"),
        "the gauge read must not scan the DLQ: {relations:?}"
    );
    assert!(
        buffers <= 16,
        "the gauge read must stay a few buffers at 50,000 dead letters, read {buffers}"
    );
    assert_eq!(gauge(&mut f.conn).await, 50_000);
}

#[tokio::test]
async fn the_gauge_matches_count_after_each_write_path() {
    let mut f = fixture("dlq_depth_paths").await;
    let conn = &mut f.conn;
    assert_eq!(gauge(conn).await, 0, "a new database has no dead letters");

    let first = dlq::dead_letter(conn, &entry(0)).await.expect("insert");
    assert_exact(conn, "one insert").await;
    assert_eq!(gauge(conn).await, 1);

    insert_rows(conn, 25).await;
    assert_exact(conn, "a multi-row insert").await;

    let mut ids: Vec<Uuid> = Vec::new();
    for n in 1..=10 {
        ids.push(dlq::dead_letter(conn, &entry(n)).await.expect("insert"));
    }
    ids.push(first);
    let discarded = dlq::discard_dead_letters_batch(conn, &ids)
        .await
        .expect("batch discard");
    assert_eq!(discarded.len(), 11);
    assert_exact(conn, "a batch discard").await;
    assert_eq!(gauge(conn).await, 25);

    conn.batch_execute("DELETE FROM harvest_dead_letters WHERE false")
        .await
        .expect("empty delete");
    assert_exact(conn, "an empty delete").await;

    conn.batch_execute("UPDATE harvest_dead_letters SET error = 'rewritten'")
        .await
        .expect("update");
    assert_exact(conn, "an update").await;

    let rolled_back = conn
        .transaction::<(), diesel::result::Error, _>(async |conn| {
            insert_rows(conn, 5).await;
            conn.batch_execute(
                "DELETE FROM harvest_dead_letters WHERE id IN \
                 (SELECT id FROM harvest_dead_letters LIMIT 3)",
            )
            .await?;
            Err(diesel::result::Error::RollbackTransaction)
        })
        .await;
    assert!(rolled_back.is_err());
    assert_exact(conn, "a rollback").await;
    assert_eq!(gauge(conn).await, 25);

    conn.batch_execute(
        "WITH gone AS (DELETE FROM harvest_dead_letters WHERE id IN \
             (SELECT id FROM harvest_dead_letters LIMIT 4) RETURNING *) \
         INSERT INTO harvest_dead_letters \
             (original_task_id, queue_name, task_type, input, error, attempts) \
         SELECT original_task_id, queue_name, task_type, input, error, attempts \
         FROM gone CROSS JOIN generate_series(1, 2)",
    )
    .await
    .expect("delete and insert in one statement");
    assert_exact(conn, "a delete and an insert in one statement").await;
    assert_eq!(gauge(conn).await, 29);

    conn.batch_execute("TRUNCATE harvest_dead_letters")
        .await
        .expect("truncate");
    assert_exact(conn, "a truncate").await;
    assert_eq!(gauge(conn).await, 0);

    insert_rows(conn, 2).await;
    assert_exact(conn, "an insert after a truncate").await;
}

#[tokio::test]
async fn open_writers_never_wait_for_the_counter() {
    const WRITERS: usize = 12;
    const WRITERS_I64: i64 = 12;
    let mut f = fixture("dlq_depth_writers").await;
    insert_rows(&mut f.conn, 3).await;

    let url = f.db.url();
    let mut writers = Vec::with_capacity(WRITERS);
    for n in 0..WRITERS {
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("writer connection");
        conn.batch_execute("BEGIN").await.expect("begin");
        // Each earlier writer still holds its open transaction here. A
        // counter row that two writers share would block this insert.
        tokio::time::timeout(
            Duration::from_secs(5),
            dlq::dead_letter(&mut conn, &entry(n)),
        )
        .await
        .unwrap_or_else(|_| panic!("writer {n} waited for an open writer"))
        .expect("insert");
        writers.push(conn);
    }
    // An open delete must not wait either.
    let mut deleter = AsyncPgConnection::establish(&url)
        .await
        .expect("deleter connection");
    deleter.batch_execute("BEGIN").await.expect("begin");
    tokio::time::timeout(
        Duration::from_secs(5),
        deleter.batch_execute(
            "DELETE FROM harvest_dead_letters WHERE id = \
             (SELECT id FROM harvest_dead_letters ORDER BY failed_at LIMIT 1)",
        ),
    )
    .await
    .expect("the delete waited for an open writer")
    .expect("delete");

    for (n, conn) in writers.iter_mut().enumerate() {
        let end = if n % 2 == 0 { "COMMIT" } else { "ROLLBACK" };
        conn.batch_execute(end).await.expect("end the writer");
    }
    deleter.batch_execute("COMMIT").await.expect("commit");

    assert_exact(&mut f.conn, "open writers").await;
    assert_eq!(gauge(&mut f.conn).await, 3 + WRITERS_I64 / 2 - 1);
}

#[tokio::test]
async fn recount_repairs_a_drift() {
    let mut f = fixture("dlq_depth_recount").await;
    let conn = &mut f.conn;
    insert_rows(conn, 5).await;
    for n in 0..4 {
        dlq::dead_letter(conn, &entry(n)).await.expect("insert");
    }
    // A write with the triggers off makes the counter drift.
    autumn_harvest::append_only::with_guard_off(conn, async |conn| {
        insert_rows(conn, 7).await;
        Ok(())
    })
    .await
    .expect("insert with the triggers off");
    assert_eq!(gauge(conn).await, 9);
    assert_eq!(exact(conn).await, 16);

    let recounted = dlq::recount_dead_letters(conn).await.expect("recount");
    assert_eq!(recounted, 16);
    assert_exact(conn, "a recount").await;
    assert_eq!(counter_rows(conn).await, 1, "a recount folds the rows");

    dlq::dead_letter(conn, &entry(99)).await.expect("insert");
    assert_exact(conn, "an insert after a recount").await;
}

/// The directory of the counter migration.
fn counter_migration_dir() -> std::path::PathBuf {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    std::fs::read_dir(&root)
        .expect("read the migrations directory")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("_harvest_dead_letter_counts"))
        })
        .expect("a *_harvest_dead_letter_counts migration must exist")
}

async fn run_migration_file(conn: &mut AsyncPgConnection, path: &std::path::Path) {
    let sql = std::fs::read_to_string(path).expect("read the migration file");
    conn.batch_execute(&format!("BEGIN;\n{sql}\n;\nCOMMIT;"))
        .await
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
}

#[tokio::test]
async fn the_migration_seeds_the_counter_from_existing_rows() {
    let mut f = fixture("dlq_depth_migrate").await;
    let conn = &mut f.conn;
    let dir = counter_migration_dir();

    run_migration_file(conn, &dir.join("down.sql")).await;
    let reg: Reg =
        diesel::sql_query("SELECT to_regclass('harvest_dead_letter_counts')::text AS reg")
            .get_result(conn)
            .await
            .expect("regclass");
    assert_eq!(reg.reg, None, "down.sql must drop the counter table");

    insert_rows(conn, 40).await;
    run_migration_file(conn, &dir.join("up.sql")).await;
    assert_eq!(gauge(conn).await, 40, "up.sql must seed the existing rows");

    insert_rows(conn, 1).await;
    assert_exact(conn, "an insert after the migration").await;
}

/// Opens a transaction at `level` and takes its snapshot. The snapshot query
/// reads no table, so at `SERIALIZABLE` it takes no predicate lock of its own.
async fn begin_with_snapshot(url: &str, level: &str) -> AsyncPgConnection {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connection");
    conn.batch_execute(&format!("BEGIN ISOLATION LEVEL {level}; SELECT 1;"))
        .await
        .expect("begin and take a snapshot");
    conn
}

/// A session default of `REPEATABLE READ` or `SERIALIZABLE` must not turn
/// overlapping dead-letter writes into serialization failures.
#[tokio::test]
async fn writers_above_read_committed_never_conflict_on_the_counter() {
    let mut f = fixture("dlq_depth_isolation").await;
    let url = f.db.url();
    for level in ["REPEATABLE READ", "SERIALIZABLE"] {
        let mut base = Vec::new();
        for n in 0..3 {
            base.push(
                dlq::dead_letter(&mut f.conn, &entry(n))
                    .await
                    .expect("insert"),
            );
        }
        let mut first = begin_with_snapshot(&url, level).await;
        let mut second = begin_with_snapshot(&url, level).await;
        // Other writers commit after both snapshots.
        for n in 0..5 {
            dlq::dead_letter(&mut f.conn, &entry(n))
                .await
                .expect("insert");
        }
        dlq::discard_dead_letters_batch(&mut f.conn, &base[..2])
            .await
            .expect("delete");
        dlq::dead_letter(&mut first, &entry(10))
            .await
            .unwrap_or_else(|e| panic!("{level}: the first writer conflicted: {e}"));
        dlq::dead_letter(&mut second, &entry(11))
            .await
            .unwrap_or_else(|e| panic!("{level}: the second writer conflicted: {e}"));
        // The second writer deletes a row that no other writer touches.
        dlq::discard_dead_letters_batch(&mut second, &base[2..])
            .await
            .unwrap_or_else(|e| panic!("{level}: the delete conflicted: {e}"));
        first
            .batch_execute("COMMIT")
            .await
            .unwrap_or_else(|e| panic!("{level}: the first commit failed: {e}"));
        second
            .batch_execute("COMMIT")
            .await
            .unwrap_or_else(|e| panic!("{level}: the second commit failed: {e}"));
        assert_exact(&mut f.conn, level).await;
    }
}

/// A role with grants on `harvest_dead_letters` alone writes, deletes and
/// reads the depth. So an upgrade needs no new grant, and an older binary
/// keeps its dead-letter writes.
#[tokio::test]
async fn a_role_without_a_counter_grant_writes_and_reads_the_depth() {
    let mut f = fixture("dlq_depth_grants").await;
    let role = format!("dlq_depth_{}", Uuid::new_v4().simple());
    f.conn
        .batch_execute(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD 'pw'; \
             GRANT SELECT, INSERT, DELETE ON harvest_dead_letters TO {role}; \
             GRANT SELECT ON harvest_workflow_executions TO {role};"
        ))
        .await
        .expect("create a role");
    let url = crate::throwaway_db::with_credentials(&f.db.url(), &role, "pw");
    let outcome = async {
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .map_err(|e| e.to_string())?;
        let id = dlq::dead_letter(&mut conn, &entry(1))
            .await
            .map_err(|e| e.to_string())?;
        dlq::dead_letter(&mut conn, &entry(2))
            .await
            .map_err(|e| e.to_string())?;
        dlq::discard_dead_letters_batch(&mut conn, &[id])
            .await
            .map_err(|e| e.to_string())?;
        dlq::dead_letter_count(&mut conn)
            .await
            .map_err(|e| e.to_string())
    }
    .await;
    f.conn
        .batch_execute(&format!(
            "REVOKE ALL ON harvest_dead_letters, harvest_workflow_executions FROM {role}; \
             DROP OWNED BY {role}; DROP ROLE {role};"
        ))
        .await
        .expect("drop the role");
    assert_eq!(outcome, Ok(1), "a role without a counter grant must work");
    assert_exact(&mut f.conn, "writes of a role without a counter grant").await;
}

/// A recount on an old snapshot would miss rows that committed before its
/// lock. It must refuse, not drift.
#[tokio::test]
async fn recount_refuses_a_snapshot_older_than_its_lock() {
    let f = fixture("dlq_depth_recount_rr").await;
    let mut conn = begin_with_snapshot(&f.db.url(), "REPEATABLE READ").await;
    let refused = conn
        .batch_execute("SELECT harvest_dead_letter_recount()")
        .await;
    assert!(
        refused.is_err(),
        "a recount at REPEATABLE READ must raise, not count an old snapshot"
    );
}

/// Each dead-letter statement adds a delta row. The writers and the fold keep
/// the live row count small, so the gauge read stays a few rows.
#[tokio::test]
async fn the_counter_stays_small_under_many_writes() {
    const WRITES: i64 = 2_000;
    let mut f = fixture("dlq_depth_fold").await;
    for _ in 0..WRITES {
        insert_rows(&mut f.conn, 1).await;
    }
    assert_exact(&mut f.conn, "many single-row writes").await;
    let rows = counter_rows(&mut f.conn).await;
    // A writer folds with a chance of 1 in 32. The chance of 600 writes with
    // no fold is below 1 in 10^8.
    assert!(rows < 600, "the writers must fold the counter: {rows} rows");

    let folded = dlq::fold_dead_letter_counts(&mut f.conn)
        .await
        .expect("fold");
    assert!(folded <= rows, "the fold removes free rows only: {folded}");
    assert_eq!(counter_rows(&mut f.conn).await, 1);
    assert_exact(&mut f.conn, "a fold").await;
    assert_eq!(gauge(&mut f.conn).await, WRITES);
}

/// `TRUNCATE` removes each dead letter, also one that committed after the
/// snapshot of the truncating transaction. The counter reset must match it.
#[tokio::test]
async fn a_truncate_on_an_old_snapshot_resets_every_counter_row() {
    let mut f = fixture("dlq_depth_truncate_rr").await;
    insert_rows(&mut f.conn, 2).await;
    let mut truncator = begin_with_snapshot(&f.db.url(), "REPEATABLE READ").await;
    // This delta row commits after the snapshot of the truncating transaction.
    dlq::dead_letter(&mut f.conn, &entry(1))
        .await
        .expect("insert");
    truncator
        .batch_execute("TRUNCATE harvest_dead_letters; COMMIT;")
        .await
        .expect("truncate");
    assert_exact(&mut f.conn, "a truncate on an old snapshot").await;
    assert_eq!(gauge(&mut f.conn).await, 0);

    // A truncate of many tables can name the counter too, in either order.
    for tables in [
        "harvest_dead_letters, harvest_dead_letter_counts",
        "harvest_dead_letter_counts, harvest_dead_letters",
    ] {
        insert_rows(&mut f.conn, 3).await;
        f.conn
            .batch_execute(&format!("TRUNCATE {tables}"))
            .await
            .unwrap_or_else(|e| panic!("TRUNCATE {tables}: {e}"));
        assert_exact(&mut f.conn, tables).await;
        insert_rows(&mut f.conn, 1).await;
        assert_exact(&mut f.conn, "an insert after a truncate of both tables").await;
        f.conn
            .batch_execute("TRUNCATE harvest_dead_letters")
            .await
            .expect("truncate");
    }
}
