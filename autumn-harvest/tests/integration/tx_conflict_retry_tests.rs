#![cfg(feature = "db")]
//! Conflict retry for deadlock and serialization aborts (issue #1822).
//!
//! The first test drives two real workers through a forced deadlock. Two
//! workflows signal each other in the same cycle. Each persist transaction
//! locks its own execution row, then the row of its peer. A test trigger
//! parks both transactions after the first lock, so the cycle always forms.
//! Postgres aborts one of them. The retry runs it again, and both commit.
//!
//! Without the retry, the victim fails its workflow. The first test then
//! fails on the `COMPLETED` assertion.
//!
//! The other tests drive [`run_with_conflict_retry`] directly.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres. Otherwise
//! a testcontainers Postgres starts.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_harvest::context::empty_shared_state;
use autumn_harvest::error::{HarvestError, HarvestResult};
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::tx_retry::{
    SITE_PERSIST, SITE_SCANNER, TxRetryPolicy, run_with_conflict_retry,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{ExecutionId, ShardId, StartWorkflowParams};
use diesel::sql_types::{BigInt, Integer, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, load_execution_from_url, setup_test_database_url_or_env,
    spawn_test_worker,
};

/// Advisory-lock class of the test gate. The two-key form never collides
/// with a one-key engine lock.
const GATE_CLASS: i32 = 1822;
/// Advisory-lock class of the second gate. The trigger takes it after the
/// first gate. Only the terminal-persist test ever holds it.
const SECOND_GATE_CLASS: i32 = 1823;

#[derive(Default)]
struct RetryMetrics {
    retries: Mutex<Vec<(String, String)>>,
}

impl RetryMetrics {
    fn count(&self, site: &str, reason: &str) -> usize {
        self.retries
            .lock()
            .unwrap()
            .iter()
            .filter(|(s, r)| s == site && r == reason)
            .count()
    }

    fn total(&self) -> usize {
        self.retries.lock().unwrap().len()
    }
}

impl MetricsRecorder for RetryMetrics {
    fn record_db_transaction_retry(&self, site: &str, reason: &str) {
        self.retries
            .lock()
            .unwrap()
            .push((site.to_owned(), reason.to_owned()));
    }
}

/// Signals the execution named in `input["peer"]`, then completes.
///
/// The signal result is ignored. The peer can complete first, so the
/// retried side sees `target_terminal`. That outcome is correct.
#[workflow]
async fn tx1822_signal_peer(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let peer: ExecutionId = input["peer"]
        .as_str()
        .ok_or("missing peer")?
        .parse()
        .map_err(|e: uuid::Error| e.to_string())?;
    let _ = ctx
        .signal_external_workflow(peer, "ping", serde_json::json!({}))
        .await;
    Ok(serde_json::json!("signalled"))
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to test DB")
}

fn quick_policy(max_attempts: u32) -> TxRetryPolicy {
    TxRetryPolicy {
        max_attempts,
        base_delay: Duration::from_millis(2),
        max_delay: Duration::from_millis(10),
    }
}

/// Count backends that wait on the test gate.
async fn gate_waiters(conn: &mut AsyncPgConnection, gate_key: i32) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Waiters {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query(
        "SELECT count(*) AS n FROM pg_locks
         WHERE locktype = 'advisory' AND NOT granted
           AND classid = $1::oid AND objid = $2::oid AND objsubid = 2",
    )
    .bind::<Integer, _>(GATE_CLASS)
    .bind::<Integer, _>(gate_key)
    .get_result::<Waiters>(conn)
    .await
    .expect("read pg_locks")
    .n
}

/// Install a trigger that parks the persist of `ids` on the gate lock.
///
/// The trigger fires on the insert of an `event_type` event. By then the
/// persist transaction holds the execution row lock of its own run.
async fn install_gate(
    conn: &mut AsyncPgConnection,
    gate_key: i32,
    event_type: &str,
    ids: &[ExecutionId],
) {
    let sql = format!(
        "CREATE OR REPLACE FUNCTION harvest_test_tx1822_gate() RETURNS trigger AS $gate$
         BEGIN
           IF NEW.event_type = '{event_type}'
              AND NEW.workflow_exec_id IN ({ids}) THEN
             PERFORM pg_advisory_xact_lock_shared({GATE_CLASS}, {gate_key});
             PERFORM pg_advisory_xact_lock_shared({SECOND_GATE_CLASS}, {gate_key});
           END IF;
           RETURN NEW;
         END
         $gate$ LANGUAGE plpgsql;
         DROP TRIGGER IF EXISTS harvest_test_tx1822_gate ON harvest_events;
         CREATE TRIGGER harvest_test_tx1822_gate AFTER INSERT ON harvest_events
           FOR EACH ROW EXECUTE FUNCTION harvest_test_tx1822_gate();",
        ids = ids
            .iter()
            .map(|id| format!("'{}'::uuid", id.as_uuid()))
            .collect::<Vec<_>>()
            .join(", "),
    );
    conn.batch_execute(&sql)
        .await
        .expect("install gate trigger");
}

async fn set_gate(conn: &mut AsyncPgConnection, gate_key: i32, closed: bool) {
    let sql = if closed {
        "SELECT pg_advisory_lock($1, $2)"
    } else {
        "SELECT pg_advisory_unlock($1, $2)"
    };
    diesel::sql_query(sql)
        .bind::<Integer, _>(GATE_CLASS)
        .bind::<Integer, _>(gate_key)
        .execute(conn)
        .await
        .expect("set gate");
}

async fn remove_gate(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "DROP TRIGGER IF EXISTS harvest_test_tx1822_gate ON harvest_events;
         DROP FUNCTION IF EXISTS harvest_test_tx1822_gate();",
    )
    .await
    .expect("remove gate trigger");
}

async fn wait_for_terminal(url: &str, exec_id: ExecutionId) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let state = load_execution_from_url(url, exec_id).await.state;
        if matches!(
            state.as_str(),
            "COMPLETED" | "FAILED" | "CANCELLED" | "TERMINATED" | "TIMED_OUT"
        ) {
            return state;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "execution {exec_id} did not reach a terminal state; last state {state}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// AC: a forced deadlock between two persist transactions. Both commit, one
/// after a retry. The retry shows in the metric.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_persist_transactions_deadlock_and_both_commit() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let mut gate = connect(&url).await;
    let gate_key = i32::try_from(std::process::id() % 1_000_000).expect("fits i32");

    let a = ExecutionId::new_for_shard(ShardId::new(0));
    let b = ExecutionId::new_for_shard(ShardId::new(0));
    install_gate(&mut conn, gate_key, "ExternalSignalRequested", &[a, b]).await;
    set_gate(&mut gate, gate_key, true).await;

    let suffix = uuid::Uuid::new_v4();
    let (a_id, b_id) = (format!("tx1822-a-{suffix}"), format!("tx1822-b-{suffix}"));
    for (exec_id, workflow_id, peer) in [(a, &a_id, b), (b, &b_id, a)] {
        autumn_harvest::execution::start_or_load_workflow_execution(
            &mut conn,
            StartWorkflowParams::new(
                "tx1822_signal_peer",
                workflow_id,
                exec_id,
                serde_json::json!({ "peer": peer.to_string() }),
                "default",
            ),
            None,
        )
        .await
        .expect("start workflow");
    }

    let metrics = Arc::new(RetryMetrics::default());
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics.clone()).build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![tx1822_signal_peer_info()],
        vec![],
        empty_shared_state(),
        telemetry,
    ));
    let worker = build_runtime_worker("tx1822-worker", 4, 4, registry);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // Both persist transactions hold their own row and wait on the gate.
    let parked = tokio::time::timeout(Duration::from_secs(20), async {
        while gate_waiters(&mut conn, gate_key).await < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if parked.is_err() {
        remove_gate(&mut conn).await;
        panic!("both persist transactions must park on the gate");
    }

    // Open the gate. Each transaction now asks for the row of its peer.
    set_gate(&mut gate, gate_key, false).await;

    let a_state = wait_for_terminal(&url, a).await;
    let b_state = wait_for_terminal(&url, b).await;
    worker.shutdown();
    let _ = handle.await;
    remove_gate(&mut conn).await;

    assert_eq!(
        (a_state.as_str(), b_state.as_str()),
        ("COMPLETED", "COMPLETED"),
        "both persist transactions must commit"
    );
    assert_eq!(
        metrics.count(SITE_PERSIST, "deadlock"),
        1,
        "exactly one persist retry after the deadlock"
    );
    // The rollback removes the first run of the victim, so no event repeats.
    for exec_id in [a, b] {
        assert_eq!(
            requested_events(&mut conn, exec_id).await,
            1,
            "one ExternalSignalRequested per run"
        );
    }
}

async fn requested_events(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query(
        "SELECT count(*) AS n FROM harvest_events
         WHERE workflow_exec_id = $1 AND event_type = 'ExternalSignalRequested'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result::<Count>(conn)
    .await
    .expect("count events")
    .n
}

/// Upserts a search attribute, then completes in the same cycle.
///
/// The pending upsert makes the outcome terminal with commands.
#[workflow]
#[allow(clippy::unused_async)] // The `#[workflow]` macro requires an `async fn`.
async fn tx1822_tag_and_complete(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let _ = input;
    ctx.upsert_search_attrs([("tx1822".to_owned(), Some(serde_json::json!("x")))])
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!("tagged"))
}

async fn backend_pid(conn: &mut AsyncPgConnection) -> i32 {
    #[derive(diesel::QueryableByName)]
    struct Pid {
        #[diesel(sql_type = Integer)]
        pid: i32,
    }
    diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Pid>(conn)
        .await
        .expect("read backend pid")
        .pid
}

async fn waits_on_a_lock(conn: &mut AsyncPgConnection, pid: i32) -> bool {
    #[derive(diesel::QueryableByName)]
    struct Waiting {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        waiting: bool,
    }
    diesel::sql_query(
        "SELECT coalesce(bool_or(wait_event_type = 'Lock'), false) AS waiting
         FROM pg_stat_activity WHERE pid = $1",
    )
    .bind::<Integer, _>(pid)
    .get_result::<Waiting>(conn)
    .await
    .expect("read pg_stat_activity")
    .waiting
}

/// A deadlock in the terminal persist resets the task, so the cycle runs
/// again. Before issue #1822 the engine failed the workflow instead.
///
/// The persist holds the execution row and parks on the first gate. A
/// blocker takes the second gate, then waits on the execution row. The first
/// gate opens, and the persist waits on the second gate. That closes the
/// cycle. The blocker has a long `deadlock_timeout`, so Postgres aborts the
/// persist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deadlocked_terminal_persist_runs_the_cycle_again() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let mut gate = connect(&url).await;
    let gate_key = i32::try_from(std::process::id() % 1_000_000 + 1_000_000).expect("fits i32");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    install_gate(&mut conn, gate_key, "WorkflowCompleted", &[exec_id]).await;
    set_gate(&mut gate, gate_key, true).await;

    let workflow_id = format!("tx1822-tag-{}", uuid::Uuid::new_v4());
    autumn_harvest::execution::start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams::new(
            "tx1822_tag_and_complete",
            &workflow_id,
            exec_id,
            serde_json::json!({}),
            "default",
        ),
        None,
    )
    .await
    .expect("start workflow");

    let metrics = Arc::new(RetryMetrics::default());
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics.clone()).build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![tx1822_tag_and_complete_info()],
        vec![],
        empty_shared_state(),
        telemetry,
    ));
    let worker = build_runtime_worker("tx1822-terminal-worker", 2, 2, registry);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    let parked = tokio::time::timeout(Duration::from_secs(20), async {
        while gate_waiters(&mut conn, gate_key).await < 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if parked.is_err() {
        remove_gate(&mut conn).await;
        panic!("the terminal persist must park on the gate");
    }

    let mut blocker = connect(&url).await;
    let blocker_pid = backend_pid(&mut blocker).await;
    blocker
        .batch_execute("BEGIN; SET LOCAL deadlock_timeout = '30s';")
        .await
        .expect("begin blocker");
    diesel::sql_query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind::<Integer, _>(SECOND_GATE_CLASS)
        .bind::<Integer, _>(gate_key)
        .execute(&mut blocker)
        .await
        .expect("blocker takes the second gate");
    let blocked = tokio::spawn(async move {
        diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .execute(&mut blocker)
            .await
            .expect("blocker gets the execution row after the abort");
        blocker
            .batch_execute("ROLLBACK")
            .await
            .expect("rollback blocker");
    });
    while !waits_on_a_lock(&mut conn, blocker_pid).await {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    set_gate(&mut gate, gate_key, false).await;
    blocked.await.expect("join blocker");

    let state = wait_for_terminal(&url, exec_id).await;
    worker.shutdown();
    let _ = handle.await;
    remove_gate(&mut conn).await;

    assert_eq!(state, "COMPLETED", "the cycle runs again and commits");
    assert_eq!(
        metrics.count(SITE_PERSIST, "deadlock"),
        1,
        "the persist conflict is counted once"
    );
}

/// Two helper transactions take two advisory locks in opposite order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forced_deadlock_between_two_helper_transactions_retries_once() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let metrics = Arc::new(RetryMetrics::default());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let base = i64::from(std::process::id()) << 20;

    let run = |first: i64, second: i64| {
        let url = url.clone();
        let metrics = Arc::clone(&metrics);
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let runs = AtomicU32::new(0);
            let result = run_with_conflict_retry(
                &mut conn,
                SITE_SCANNER,
                metrics.as_ref(),
                quick_policy(5),
                async |conn| {
                    let run = AtomicU32::fetch_add(&runs, 1, Ordering::SeqCst);
                    let barrier = &barrier;
                    conn.transaction::<(), HarvestError, _>(async |conn| {
                        lock_xact(conn, first).await?;
                        if run == 0 {
                            barrier.wait().await;
                        }
                        lock_xact(conn, second).await
                    })
                    .await
                },
            )
            .await;
            (result, runs.into_inner())
        })
    };

    let left = run(base + 1, base + 2);
    let right = run(base + 2, base + 1);
    let (left, left_runs) = left.await.expect("join left");
    let (right, right_runs) = right.await.expect("join right");

    left.expect("left commits");
    right.expect("right commits");
    assert_eq!(left_runs + right_runs, 3, "one side runs twice");
    assert_eq!(metrics.count(SITE_SCANNER, "deadlock"), 1);
    assert_eq!(metrics.total(), 1);
}

async fn lock_xact(conn: &mut AsyncPgConnection, key: i64) -> HarvestResult<()> {
    diesel::sql_query("SELECT pg_advisory_xact_lock($1)")
        .bind::<BigInt, _>(key)
        .execute(conn)
        .await
        .map(|_| ())
        .map_err(autumn_harvest::error::database_error)
}

/// A `REPEATABLE READ` write after a concurrent commit aborts with `40001`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_serialization_failure_is_retried_on_a_fresh_snapshot() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut setup = connect(&url).await;
    let table = format!("harvest_test_tx1822_{}", std::process::id());
    setup
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (id INT PRIMARY KEY, v INT NOT NULL);
             INSERT INTO {table} VALUES (1, 0);"
        ))
        .await
        .expect("create table");

    let metrics = RetryMetrics::default();
    let read_done = Arc::new(tokio::sync::Notify::new());
    let writer_done = Arc::new(tokio::sync::Notify::new());
    let writer = {
        let (url, table) = (url.clone(), table.clone());
        let (read_done, writer_done) = (Arc::clone(&read_done), Arc::clone(&writer_done));
        tokio::spawn(async move {
            read_done.notified().await;
            let mut conn = connect(&url).await;
            conn.batch_execute(&format!("UPDATE {table} SET v = v + 10 WHERE id = 1"))
                .await
                .expect("concurrent update");
            writer_done.notify_one();
        })
    };

    let mut conn = connect(&url).await;
    let runs = AtomicU32::new(0);
    let select = format!("SELECT v FROM {table} WHERE id = 1");
    let update = format!("UPDATE {table} SET v = v + 1 WHERE id = 1");
    run_with_conflict_retry(
        &mut conn,
        SITE_PERSIST,
        &metrics,
        quick_policy(5),
        async |conn| {
            let run = AtomicU32::fetch_add(&runs, 1, Ordering::SeqCst);
            let (select, update) = (&select, &update);
            let (read_done, writer_done) = (&read_done, &writer_done);
            let mut tx = conn.build_transaction().repeatable_read();
            tx.run(async |conn: &mut AsyncPgConnection| -> HarvestResult<()> {
                conn.batch_execute(select)
                    .await
                    .map_err(autumn_harvest::error::database_error)?;
                if run == 0 {
                    read_done.notify_one();
                    writer_done.notified().await;
                }
                conn.batch_execute(update)
                    .await
                    .map_err(autumn_harvest::error::database_error)
            })
            .await
        },
    )
    .await
    .expect("the retry commits");
    writer.await.expect("join writer");

    #[derive(diesel::QueryableByName)]
    struct Value {
        #[diesel(sql_type = Integer)]
        v: i32,
    }
    let value = diesel::sql_query(format!("SELECT v FROM {table} WHERE id = 1"))
        .get_result::<Value>(&mut setup)
        .await
        .expect("read value")
        .v;
    setup
        .batch_execute(&format!("DROP TABLE {table}"))
        .await
        .expect("drop table");

    assert_eq!(value, 11, "both writes land");
    assert_eq!(AtomicU32::load(&runs, Ordering::SeqCst), 2);
    assert_eq!(metrics.count(SITE_PERSIST, "serialization_failure"), 1);
}

fn synthetic_deadlock() -> HarvestError {
    HarvestError::Database("deadlock detected".to_owned())
}

/// The helper stops after `max_attempts` and returns the conflict error.
#[tokio::test]
async fn retries_are_bounded_and_the_last_error_surfaces() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let metrics = RetryMetrics::default();
    let runs = AtomicU32::new(0);

    let result: HarvestResult<()> = run_with_conflict_retry(
        &mut conn,
        SITE_PERSIST,
        &metrics,
        quick_policy(3),
        async |_conn| {
            AtomicU32::fetch_add(&runs, 1, Ordering::SeqCst);
            Err(synthetic_deadlock())
        },
    )
    .await;

    assert!(matches!(result, Err(HarvestError::Database(ref m)) if m.contains("deadlock")));
    assert_eq!(AtomicU32::load(&runs, Ordering::SeqCst), 3);
    assert_eq!(metrics.count(SITE_PERSIST, "deadlock"), 2, "one per retry");
}

/// Any other error returns at once.
#[tokio::test]
async fn a_non_conflict_error_is_not_retried() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let metrics = RetryMetrics::default();
    let runs = AtomicU32::new(0);

    let result: HarvestResult<()> = run_with_conflict_retry(
        &mut conn,
        SITE_PERSIST,
        &metrics,
        quick_policy(5),
        async |_conn| {
            AtomicU32::fetch_add(&runs, 1, Ordering::SeqCst);
            Err(HarvestError::Database("lock timeout".to_owned()))
        },
    )
    .await;

    assert!(result.is_err());
    assert_eq!(AtomicU32::load(&runs, Ordering::SeqCst), 1);
    assert_eq!(metrics.total(), 0);
}

/// Inside an open transaction the helper runs once. The outer caller owns
/// the retry.
#[tokio::test]
async fn a_nested_call_is_not_retried() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let metrics = RetryMetrics::default();
    let runs = AtomicU32::new(0);

    let result = conn
        .transaction::<(), HarvestError, _>(async |conn| {
            run_with_conflict_retry(
                conn,
                SITE_PERSIST,
                &metrics,
                quick_policy(5),
                async |_conn| {
                    AtomicU32::fetch_add(&runs, 1, Ordering::SeqCst);
                    Err::<(), _>(synthetic_deadlock())
                },
            )
            .await
        })
        .await;

    assert!(result.is_err());
    assert_eq!(AtomicU32::load(&runs, Ordering::SeqCst), 1);
    assert_eq!(metrics.total(), 0);
}

/// A success on the first run records nothing.
#[tokio::test]
async fn a_clean_commit_records_no_retry() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let metrics = RetryMetrics::default();

    let value = run_with_conflict_retry(
        &mut conn,
        SITE_PERSIST,
        &metrics,
        TxRetryPolicy::default(),
        async |conn| {
            #[derive(diesel::QueryableByName)]
            struct One {
                #[diesel(sql_type = Text)]
                s: String,
            }
            diesel::sql_query("SELECT 'ok'::text AS s")
                .get_result::<One>(conn)
                .await
                .map(|row| row.s)
                .map_err(autumn_harvest::error::database_error)
        },
    )
    .await
    .expect("commit");

    assert_eq!(value, "ok");
    assert_eq!(metrics.total(), 0);
}
