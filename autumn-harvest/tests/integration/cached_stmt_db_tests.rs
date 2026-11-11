#![cfg(feature = "db")]
//! Cached prepared statements against a live Postgres (issue #2651).
//!
//! `pg_prepared_statements` lists the prepared statements of the session
//! that reads it. The driver closes an unnamed statement before the next
//! statement of the connection runs. So a statement that the view still
//! lists after later statements is one that the connection cached.
//!
//! Each test proves one part of the claim:
//!
//! - a marked statement is prepared once, and an unmarked one is not kept,
//! - a load of `WorkflowExecution` by primary key is cached,
//! - a lookup of `WorkflowExecution` by key is not, because its generic plan
//!   cannot use the partial index of the active key,
//! - the hot engine statements of the claim path are cached, and
//! - a cached statement with named columns survives an added column.

use autumn_harvest::cached_stmt::CachePreparedDsl as _;

use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

use crate::throwaway_db::ThrowawayDb;

/// One connection to a fresh, migrated database.
///
/// The connection is the first field, so it closes before the guard drops
/// the database.
struct Db {
    conn: AsyncPgConnection,
    _throwaway: Option<ThrowawayDb>,
    _container: Option<ContainerAsync<Postgres>>,
}

/// Connect to a database of this test alone.
///
/// With `HARVEST_TEST_DATABASE_URL` set, [`ThrowawayDb`] creates a migrated
/// database on that server, with a unique name, and drops it again. Without
/// it, the test gets a container.
async fn connect() -> Db {
    if let Some(throwaway) = ThrowawayDb::create("cached_stmt_t").await {
        let conn = AsyncPgConnection::establish(&throwaway.url())
            .await
            .expect("connect to the test database");
        return Db {
            conn,
            _throwaway: Some(throwaway),
            _container: None,
        };
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("start the Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to the test database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply the migrations");
    Db {
        conn,
        _throwaway: None,
        _container: Some(container),
    }
}

/// The prepared statements of this session whose text matches `pattern`.
async fn prepared_like(conn: &mut AsyncPgConnection, pattern: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS n FROM pg_prepared_statements WHERE statement LIKE $1")
        .bind::<diesel::sql_types::Text, _>(pattern)
        .get_result::<Count>(conn)
        .await
        .expect("read pg_prepared_statements")
        .n
}

#[derive(diesel::QueryableByName)]
struct Probe {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    answer: i32,
}

#[tokio::test]
async fn a_marked_statement_is_prepared_once_and_an_unmarked_one_is_not_kept() {
    let mut db = connect().await;
    let conn = &mut db.conn;

    for expected in [41, 42, 43] {
        let row: Probe = diesel::sql_query("SELECT $1::int4 + 0 AS answer /* marked */")
            .bind::<diesel::sql_types::Integer, _>(expected)
            .cache_prepared()
            .get_result(conn)
            .await
            .expect("run the marked statement");
        assert_eq!(row.answer, expected);
    }
    assert_eq!(
        prepared_like(conn, "%/* marked */").await,
        1,
        "three runs of a marked statement keep one prepared statement"
    );

    for expected in [1, 2, 3] {
        let row: Probe = diesel::sql_query("SELECT $1::int4 + 0 AS answer /* unmarked */")
            .bind::<diesel::sql_types::Integer, _>(expected)
            .get_result(conn)
            .await
            .expect("run the unmarked statement");
        assert_eq!(row.answer, expected);
    }
    assert_eq!(
        prepared_like(conn, "%/* unmarked */").await,
        0,
        "an unmarked raw statement is closed after each run"
    );

    // The marked statement is still there after other statements ran.
    assert_eq!(prepared_like(conn, "%/* marked */").await, 1);
}

#[tokio::test]
async fn a_false_condition_keeps_no_prepared_statement() {
    let mut db = connect().await;
    let conn = &mut db.conn;

    for _ in 0..2 {
        diesel::sql_query("SELECT 1 AS answer /* conditional */")
            .cache_prepared_if(false)
            .execute(conn)
            .await
            .expect("run the statement");
    }
    assert_eq!(prepared_like(conn, "%/* conditional */").await, 0);

    for _ in 0..2 {
        diesel::sql_query("SELECT 1 AS answer /* conditional */")
            .cache_prepared_if(true)
            .execute(conn)
            .await
            .expect("run the statement");
    }
    assert_eq!(prepared_like(conn, "%/* conditional */").await, 1);
}

/// The unmapped columns of `harvest_workflow_executions` are `sql::<T>()`
/// fragments, so a load of the model is unnamed unless its call site marks
/// it. A signal locks its run by primary key, and that load is marked.
#[tokio::test]
async fn a_workflow_execution_load_by_primary_key_is_cached() {
    let mut db = connect().await;
    let conn = &mut db.conn;
    let pattern = r#"SELECT "harvest_workflow_executions"."id"%WHERE ("harvest_workflow_executions"."id" = $1)%FOR UPDATE%"#;

    for _ in 0..3 {
        let missing = autumn_harvest::signal::send_signal(
            conn,
            autumn_harvest::types::ExecutionId::from_uuid(Uuid::new_v4()),
            "poke",
            serde_json::json!(null),
        )
        .await;
        assert!(missing.is_err(), "a signal to no run fails");
    }
    assert_eq!(
        prepared_like(conn, pattern).await,
        1,
        "the load of the run by primary key is one cached statement"
    );
}

/// A lookup by workflow name and id binds the sealed states as an array.
/// A generic plan cannot prove the predicate of the partial active-key
/// index from a bind, so it reads every sealed row of a continue-as-new
/// chain. The lookup must stay unnamed, so Postgres plans it with its
/// values.
#[tokio::test]
async fn a_workflow_execution_lookup_by_key_is_not_cached() {
    let mut db = connect().await;
    let conn = &mut db.conn;
    let pattern = r#"SELECT "harvest_workflow_executions"."id"%"workflow_id" = $2%"#;

    for _ in 0..3 {
        let missing = autumn_harvest::execution::try_load_by_key(conn, "no_flow", "no-id")
            .await
            .expect("look the key up");
        assert!(missing.is_none());
    }
    assert_eq!(
        prepared_like(conn, pattern).await,
        0,
        "the lookup by key keeps no prepared statement"
    );
}

/// Each claim runs these statements. They ran unnamed before issue #2651.
#[tokio::test]
async fn the_claim_rechecks_and_the_probes_are_cached() {
    let mut db = connect().await;
    let conn = &mut db.conn;
    let task = Uuid::new_v4();

    for _ in 0..3 {
        assert!(
            autumn_harvest::queue_pause::try_lock_queue_for_claim(conn, "cached-queue")
                .await
                .expect("take the queue lock")
        );
        assert!(
            !autumn_harvest::queue_pause::release_claim_if_queue_paused(conn, task, "worker-1")
                .await
                .expect("run the queue pause recheck")
        );
        assert!(
            !autumn_harvest::activity_pause::release_claim_if_activity_paused(
                conn, task, "worker-1"
            )
            .await
            .expect("run the activity pause recheck")
        );
        assert!(
            autumn_harvest::mutex::table_present(conn)
                .await
                .expect("probe the mutex table")
        );
    }

    for (what, pattern) in [
        (
            "the queue lock",
            "SELECT pg_try_advisory_xact_lock_shared($1, $2)%",
        ),
        (
            "the queue pause recheck",
            "UPDATE harvest_task_queue%FROM harvest_queue_pauses qp%",
        ),
        (
            "the activity pause recheck",
            "UPDATE harvest_task_queue%FROM harvest_activity_pauses ap%",
        ),
        (
            "the mutex table probe",
            "SELECT to_regclass('harvest_mutex_locks')%",
        ),
    ] {
        assert_eq!(
            prepared_like(conn, pattern).await,
            1,
            "{what} is one cached statement"
        );
    }
}

/// Read the probe row through a cached statement with named columns.
async fn read_probe(conn: &mut AsyncPgConnection) -> i32 {
    diesel::sql_query("SELECT answer FROM cached_stmt_probe WHERE id = $1")
        .bind::<diesel::sql_types::Integer, _>(1)
        .cache_prepared()
        .get_result::<Probe>(conn)
        .await
        .expect("read the probe row")
        .answer
}

/// A cached statement that names its columns keeps its result type when a
/// migration adds a column. That is why `cache_prepared` refuses `*`.
#[tokio::test]
async fn a_cached_statement_with_named_columns_survives_an_added_column() {
    let mut db = connect().await;
    let conn = &mut db.conn;
    conn.batch_execute(
        "CREATE TABLE cached_stmt_probe (id int4 PRIMARY KEY, answer int4 NOT NULL); \
         INSERT INTO cached_stmt_probe VALUES (1, 42)",
    )
    .await
    .expect("create the probe table");

    // Six runs, so Postgres is past its five custom plans.
    for _ in 0..6 {
        assert_eq!(read_probe(conn).await, 42);
    }

    conn.batch_execute("ALTER TABLE cached_stmt_probe ADD COLUMN added text")
        .await
        .expect("add a column");

    assert_eq!(read_probe(conn).await, 42);
    assert_eq!(
        prepared_like(conn, "SELECT answer FROM cached_stmt_probe%").await,
        1
    );
}

/// The claim opens its transaction with one batch: `BEGIN`, then the planner
/// settings. A statement of that batch can fail after the `BEGIN` ran. The
/// server then holds an aborted transaction that the transaction manager
/// does not know. Without a `ROLLBACK`, each later statement on the
/// connection fails.
#[tokio::test]
async fn a_failed_claim_begin_batch_leaves_no_open_transaction() {
    let mut db = connect().await;
    let conn = &mut db.conn;

    let failed = autumn_harvest::queue::begin_claim_transaction_for_test(
        conn,
        "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; SELECT 1 / 0",
    )
    .await;
    assert!(failed.is_err(), "the batch fails on its second statement");

    let row: Probe = diesel::sql_query("SELECT 42 AS answer")
        .get_result(conn)
        .await
        .expect("the session is not in an aborted transaction");
    assert_eq!(row.answer, 42);

    // A claim transaction opens and commits on the same connection.
    autumn_harvest::queue::begin_claim_transaction_for_test(
        conn,
        autumn_harvest::queue::CLAIM_BEGIN_WITH_SETTINGS_SQL,
    )
    .await
    .expect("open a claim transaction");
    conn.transaction::<(), diesel::result::Error, _>(async |conn| {
        diesel::sql_query("SELECT 1 AS answer")
            .execute(conn)
            .await
            .map(drop)
    })
    .await
    .expect("a nested transaction runs inside the claim transaction");
    {
        use diesel_async::TransactionManager as _;
        diesel_async::AnsiTransactionManager::commit_transaction(conn)
            .await
            .expect("commit the claim transaction");
    }
}
