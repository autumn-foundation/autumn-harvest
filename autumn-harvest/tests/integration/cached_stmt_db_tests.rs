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
//! - a load of `WorkflowExecution` is cached with no mark at the call site,
//! - the hot engine statements of the claim path are cached, and
//! - a cached statement with named columns survives an added column.

use std::sync::atomic::{AtomicU64, Ordering};

use autumn_harvest::cached_stmt::CachePreparedDsl as _;
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions;

use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

static DB_SEQ: AtomicU64 = AtomicU64::new(0);

/// `url` with its database name replaced by `db`.
fn with_db_name(url: &str, db: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let prefix = base.rsplit_once('/').map_or(base, |(prefix, _)| prefix);
    query.map_or_else(
        || format!("{prefix}/{db}"),
        |query| format!("{prefix}/{db}?{query}"),
    )
}

/// One connection to a fresh, migrated database.
///
/// `HARVEST_TEST_DATABASE_URL` is an admin URL. Each test gets a database of
/// its own on that server. Without it, each test gets a container.
async fn connect() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    let (url, container) = if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect to the admin database");
        let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
        let db = format!("cached_stmt_t_{}_{}", std::process::id(), n);
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create the test database");
        (with_db_name(&admin_url, &db), None)
    } else {
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
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to the test database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply the migrations");
    (conn, container)
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
    let (mut conn, _container) = connect().await;

    for expected in [41, 42, 43] {
        let row: Probe = diesel::sql_query("SELECT $1::int4 + 0 AS answer /* marked */")
            .bind::<diesel::sql_types::Integer, _>(expected)
            .cache_prepared()
            .get_result(&mut conn)
            .await
            .expect("run the marked statement");
        assert_eq!(row.answer, expected);
    }
    assert_eq!(
        prepared_like(&mut conn, "%/* marked */").await,
        1,
        "three runs of a marked statement keep one prepared statement"
    );

    for expected in [1, 2, 3] {
        let row: Probe = diesel::sql_query("SELECT $1::int4 + 0 AS answer /* unmarked */")
            .bind::<diesel::sql_types::Integer, _>(expected)
            .get_result(&mut conn)
            .await
            .expect("run the unmarked statement");
        assert_eq!(row.answer, expected);
    }
    assert_eq!(
        prepared_like(&mut conn, "%/* unmarked */").await,
        0,
        "an unmarked raw statement is closed after each run"
    );

    // The marked statement is still there after other statements ran.
    assert_eq!(prepared_like(&mut conn, "%/* marked */").await, 1);
}

#[tokio::test]
async fn a_false_condition_keeps_no_prepared_statement() {
    let (mut conn, _container) = connect().await;

    for _ in 0..2 {
        diesel::sql_query("SELECT 1 AS answer /* conditional */")
            .cache_prepared_if(false)
            .execute(&mut conn)
            .await
            .expect("run the statement");
    }
    assert_eq!(prepared_like(&mut conn, "%/* conditional */").await, 0);

    for _ in 0..2 {
        diesel::sql_query("SELECT 1 AS answer /* conditional */")
            .cache_prepared_if(true)
            .execute(&mut conn)
            .await
            .expect("run the statement");
    }
    assert_eq!(prepared_like(&mut conn, "%/* conditional */").await, 1);
}

/// The unmapped columns of `harvest_workflow_executions` were `sql::<T>()`
/// fragments. They made every load of the model parse and plan again.
#[tokio::test]
async fn a_workflow_execution_load_is_cached_with_no_mark() {
    let (mut conn, _container) = connect().await;
    let pattern = r#"SELECT "harvest_workflow_executions"."id"%"migrated_from_shards"%"#;

    for _ in 0..3 {
        let missing: Option<WorkflowExecution> = harvest_workflow_executions::table
            .find(Uuid::new_v4())
            .select(WorkflowExecution::as_select())
            .first(&mut conn)
            .await
            .optional()
            .expect("load an execution");
        assert!(missing.is_none());
    }
    assert_eq!(
        prepared_like(&mut conn, pattern).await,
        1,
        "the load of the execution model is one cached statement"
    );
}

/// Each claim runs these statements. They ran unnamed before issue #2651.
#[tokio::test]
async fn the_claim_rechecks_and_the_probes_are_cached() {
    let (mut conn, _container) = connect().await;
    let task = Uuid::new_v4();

    for _ in 0..3 {
        assert!(
            autumn_harvest::queue_pause::try_lock_queue_for_claim(&mut conn, "cached-queue")
                .await
                .expect("take the queue lock")
        );
        assert!(
            !autumn_harvest::queue_pause::release_claim_if_queue_paused(
                &mut conn, task, "worker-1"
            )
            .await
            .expect("run the queue pause recheck")
        );
        assert!(
            !autumn_harvest::activity_pause::release_claim_if_activity_paused(
                &mut conn, task, "worker-1"
            )
            .await
            .expect("run the activity pause recheck")
        );
        assert!(
            autumn_harvest::mutex::table_present(&mut conn)
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
            prepared_like(&mut conn, pattern).await,
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
    let (mut conn, _container) = connect().await;
    conn.batch_execute(
        "CREATE TABLE cached_stmt_probe (id int4 PRIMARY KEY, answer int4 NOT NULL); \
         INSERT INTO cached_stmt_probe VALUES (1, 42)",
    )
    .await
    .expect("create the probe table");

    // Six runs, so Postgres is past its five custom plans.
    for _ in 0..6 {
        assert_eq!(read_probe(&mut conn).await, 42);
    }

    conn.batch_execute("ALTER TABLE cached_stmt_probe ADD COLUMN added text")
        .await
        .expect("add a column");

    assert_eq!(read_probe(&mut conn).await, 42);
    assert_eq!(
        prepared_like(&mut conn, "SELECT answer FROM cached_stmt_probe%").await,
        1
    );
}
