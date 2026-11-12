#![cfg(feature = "db")]
//! The terminal mutex sweep reads every key of a run in one statement.
//!
//! - A run with no lock and no waiter row costs two statements: the table
//!   probe and the key read.
//! - A run that holds a key and waits on others frees each key, drops each
//!   waiter row and wakes each new head of line, as the two separate sweeps
//!   did.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::types::ExecutionId;
use diesel::connection::{Instrumentation, InstrumentationEvent};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

use crate::throwaway_db::ThrowawayDb;

/// One connection to a fresh, migrated database.
struct Db {
    conn: AsyncPgConnection,
    _throwaway: Option<ThrowawayDb>,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn connect() -> Db {
    if let Some(throwaway) = ThrowawayDb::create("mutex_sweep_t").await {
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

/// Count the statements that a connection starts.
struct Counter(Arc<AtomicUsize>);

impl Instrumentation for Counter {
    fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
        if matches!(event, InstrumentationEvent::StartQuery { .. }) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Insert a running run with a parked workflow task.
async fn run(conn: &mut AsyncPgConnection, name: &str) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'sweep', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(format!("{name}-{id}"))
    .execute(conn)
    .await
    .expect("insert the run");
    diesel::sql_query(
        "INSERT INTO harvest_task_queue (queue_name, task_type, workflow_exec_id, input, state, \
         priority, max_attempts, scheduled_at) \
         VALUES ('default', 'workflow', $1, '{}'::jsonb, 'RUNNING', 0, 3, NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert the parked task");
    id
}

async fn hold(conn: &mut AsyncPgConnection, key: &str, holder: Uuid) {
    diesel::sql_query(
        "INSERT INTO harvest_mutex_locks (lock_key, holder_exec_id, lock_seq, acquired_at, \
         lease_expires_at) VALUES ($1, $2, 1, NOW(), NOW() + interval '1 hour')",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Uuid, _>(holder)
    .execute(conn)
    .await
    .expect("insert the lock");
}

async fn wait(conn: &mut AsyncPgConnection, key: &str, waiter: Uuid) {
    diesel::sql_query(
        "INSERT INTO harvest_mutex_waiters (lock_key, waiter_exec_id, requested_at) \
         VALUES ($1, $2, clock_timestamp())",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Uuid, _>(waiter)
    .execute(conn)
    .await
    .expect("insert the waiter");
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn count(conn: &mut AsyncPgConnection, sql: &str, id: Uuid) -> i64 {
    diesel::sql_query(sql)
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<Count>(conn)
        .await
        .expect("count rows")
        .n
}

async fn task_state(conn: &mut AsyncPgConnection, exec: Uuid) -> String {
    #[derive(diesel::QueryableByName)]
    struct State {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    diesel::sql_query("SELECT state FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec)
        .get_result::<State>(conn)
        .await
        .expect("read the task state")
        .state
}

/// A run with no mutex rows pays one probe and one key read.
#[tokio::test]
async fn a_run_with_no_mutex_rows_costs_two_statements() {
    let mut db = connect().await;
    let conn = &mut db.conn;
    let holder = run(conn, "idle").await;

    let started = Arc::new(AtomicUsize::new(0));
    conn.set_instrumentation(Counter(Arc::clone(&started)));
    autumn_harvest::mutex::sweep_terminal_holder_and_wake(conn, ExecutionId::from_uuid(holder))
        .await
        .expect("sweep");
    assert_eq!(AtomicUsize::load(&started, Ordering::Relaxed), 2);
}

/// The holder frees its key, drops its own waiter row on that key and its
/// waiter row on another key. The waiter of each key wakes.
#[tokio::test]
async fn the_sweep_frees_held_keys_and_drops_waiter_rows() {
    let mut db = connect().await;
    let conn = &mut db.conn;
    let holder = run(conn, "holder").await;
    let first = run(conn, "first").await;
    let second = run(conn, "second").await;
    let held = format!("held-{holder}");
    let waited = format!("waited-{holder}");

    hold(conn, &held, holder).await;
    // A holder keeps its waiter row after its grant.
    wait(conn, &held, holder).await;
    wait(conn, &held, first).await;
    hold(conn, &waited, second).await;
    wait(conn, &waited, holder).await;

    autumn_harvest::mutex::sweep_terminal_holder_and_wake(conn, ExecutionId::from_uuid(holder))
        .await
        .expect("sweep");

    assert_eq!(
        count(
            conn,
            "SELECT count(*) AS n FROM harvest_mutex_locks WHERE holder_exec_id = $1",
            holder
        )
        .await,
        0,
        "the held key is free"
    );
    assert_eq!(
        count(
            conn,
            "SELECT count(*) AS n FROM harvest_mutex_waiters WHERE waiter_exec_id = $1",
            holder
        )
        .await,
        0,
        "no waiter row of the holder stays"
    );
    assert_eq!(
        count(
            conn,
            "SELECT count(*) AS n FROM harvest_mutex_locks WHERE holder_exec_id = $1",
            second
        )
        .await,
        1,
        "a key of another run stays held"
    );
    assert_eq!(
        task_state(conn, first).await,
        "PENDING",
        "the next waiter wakes"
    );
}
