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
async fn the_gauge_reads_a_few_rows_at_any_dlq_size() {
    let mut f = fixture("dlq_depth_read").await;
    insert_rows(&mut f.conn, 50_000).await;
    f.conn
        .batch_execute("VACUUM ANALYZE harvest_dead_letters")
        .await
        .expect("analyze");

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
    let rows = counter_rows(&mut f.conn).await;
    assert!(
        rows <= WRITERS_I64 + 2,
        "the counter grows to the peak count of open writers at most: {rows} rows"
    );
}

#[tokio::test]
async fn recount_repairs_a_drift() {
    let mut f = fixture("dlq_depth_recount").await;
    let conn = &mut f.conn;
    insert_rows(conn, 5).await;
    for n in 0..4 {
        dlq::dead_letter(conn, &entry(n)).await.expect("insert");
    }
    // A replica-role session fires no trigger, so the counter drifts.
    conn.transaction::<(), diesel::result::Error, _>(async |conn| {
        conn.batch_execute("SET LOCAL session_replication_role = replica")
            .await?;
        insert_rows(conn, 7).await;
        Ok(())
    })
    .await
    .expect("insert with the triggers off");
    assert_eq!(gauge(conn).await, 9);
    assert_eq!(exact(conn).await, 16);

    let recounted = scalar(conn, "SELECT harvest_dead_letter_recount() AS n").await;
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
    conn.batch_execute(&format!("BEGIN; {sql}; COMMIT;"))
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
