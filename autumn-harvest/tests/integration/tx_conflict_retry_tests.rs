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
//! Two tests cover the claim and scanner sites. A trigger raises one
//! synthetic `40P01` inside the claim or the fire batch. A sequence counts
//! the raises, and a rollback does not undo `nextval`, so the retry passes.
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
    SITE_CLAIM, SITE_PERSIST, SITE_SCANNER, SITE_WORKFLOW_TASK, TxRetryPolicy,
    run_with_conflict_retry,
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
    exhausted: Mutex<Vec<(String, String)>>,
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

    fn exhausted(&self) -> Vec<(String, String)> {
        self.exhausted.lock().unwrap().clone()
    }
}

impl MetricsRecorder for RetryMetrics {
    fn record_db_transaction_retry(&self, site: &str, reason: &str) {
        self.retries
            .lock()
            .unwrap()
            .push((site.to_owned(), reason.to_owned()));
    }

    fn record_db_transaction_retry_exhausted(&self, site: &str, reason: &str) {
        self.exhausted
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

const fn quick_policy(max_attempts: u32) -> TxRetryPolicy {
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
/// The trigger fires on the insert of an `event_type` event. It first takes
/// `FOR KEY SHARE` on the execution row of the run, then waits on the gates.
/// The persist therefore always holds a lock on its own row while it waits.
async fn install_gate(
    conn: &mut AsyncPgConnection,
    name: &str,
    gate_key: i32,
    event_type: &str,
    ids: &[ExecutionId],
) {
    let sql = format!(
        "CREATE OR REPLACE FUNCTION {name}() RETURNS trigger AS $gate$
         BEGIN
           IF NEW.event_type = '{event_type}'
              AND NEW.workflow_exec_id IN ({ids}) THEN
             PERFORM 1 FROM harvest_workflow_executions
               WHERE id = NEW.workflow_exec_id FOR KEY SHARE;
             PERFORM pg_advisory_xact_lock_shared({GATE_CLASS}, {gate_key});
             PERFORM pg_advisory_xact_lock_shared({SECOND_GATE_CLASS}, {gate_key});
           END IF;
           RETURN NEW;
         END
         $gate$ LANGUAGE plpgsql;
         DROP TRIGGER IF EXISTS {name} ON harvest_events;
         CREATE TRIGGER {name} AFTER INSERT ON harvest_events
           FOR EACH ROW EXECUTE FUNCTION {name}();",
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

async fn remove_gate(conn: &mut AsyncPgConnection, name: &str) {
    conn.batch_execute(&format!(
        "DROP TRIGGER IF EXISTS {name} ON harvest_events;
         DROP FUNCTION IF EXISTS {name}();"
    ))
    .await
    .expect("remove gate trigger");
}

/// Serializes the trigger tests.
///
/// The gate DDL takes a table lock on `harvest_events`. A parked persist of
/// another trigger test holds a conflicting lock until its gate opens. Two
/// trigger tests in parallel would then wait on each other.
static TRIGGER_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Drop every test trigger, function and sequence that a failed run left.
///
/// A leftover trigger on `harvest_events` makes partition enable and disable
/// refuse on a shared database. Call this under [`TRIGGER_TESTS`].
async fn sweep_stale_test_objects(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        r"DO $sweep$
          DECLARE r record;
          BEGIN
            FOR r IN SELECT tgname, tgrelid::regclass AS rel FROM pg_trigger
                     WHERE tgname LIKE 'harvest\_test\_tx1822\_%' AND tgparentid = 0
            LOOP
              EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', r.tgname, r.rel);
            END LOOP;
            FOR r IN SELECT oid::regprocedure AS f FROM pg_proc
                     WHERE proname LIKE 'harvest\_test\_tx1822\_%'
            LOOP
              EXECUTE format('DROP FUNCTION IF EXISTS %s', r.f);
            END LOOP;
            FOR r IN SELECT relname FROM pg_class
                     WHERE relkind = 'S' AND relname LIKE 'harvest\_test\_tx1822\_%'
            LOOP
              EXECUTE format('DROP SEQUENCE IF EXISTS %I', r.relname);
            END LOOP;
          END
          $sweep$;",
    )
    .await
    .expect("sweep stale test objects");
}

async fn is_superuser(conn: &mut AsyncPgConnection) -> bool {
    #[derive(diesel::QueryableByName)]
    struct Super {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        on: bool,
    }
    diesel::sql_query("SELECT current_setting('is_superuser') = 'on' AS on")
        .get_result::<Super>(conn)
        .await
        .expect("read is_superuser")
        .on
}

/// Wait until `count` persist transactions park on the gate.
///
/// On a timeout, open the gate first. The trigger DDL would otherwise wait
/// behind the parked transactions.
async fn wait_for_parked(
    conn: &mut AsyncPgConnection,
    gate: &mut AsyncPgConnection,
    name: &str,
    gate_key: i32,
    count: i64,
) {
    let parked = tokio::time::timeout(Duration::from_secs(20), async {
        while gate_waiters(conn, gate_key).await < count {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if parked.is_err() {
        set_gate(gate, gate_key, false).await;
        remove_gate(conn, name).await;
        panic!("{count} persist transaction(s) must park on the gate");
    }
}

/// Start one run of `workflow_name` on the `default` queue.
async fn start_run(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    exec_id: ExecutionId,
    input: serde_json::Value,
) {
    let workflow_id = format!("{workflow_name}-{}", exec_id.as_uuid());
    autumn_harvest::execution::start_or_load_workflow_execution(
        conn,
        StartWorkflowParams::new(workflow_name, &workflow_id, exec_id, input, "default"),
        None,
    )
    .await
    .expect("start workflow");
}

/// Spawn a worker for `workflow` with a recording metrics sink.
fn spawn_worker(
    url: &str,
    worker_id: &str,
    workflow: autumn_harvest::info::WorkflowInfo,
) -> (
    Arc<RetryMetrics>,
    Arc<autumn_harvest::worker::Worker>,
    tokio::task::JoinHandle<()>,
) {
    let metrics = Arc::new(RetryMetrics::default());
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics.clone()).build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![workflow],
        vec![],
        empty_shared_state(),
        telemetry,
    ));
    let worker = build_runtime_worker(worker_id, 4, 4, registry);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(url));
    (metrics, worker, handle)
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
    const GATE: &str = "harvest_test_tx1822_mutual_gate";
    let _serial = TRIGGER_TESTS.lock().await;
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let mut gate = connect(&url).await;
    sweep_stale_test_objects(&mut conn).await;
    let gate_key = i32::try_from(std::process::id() % 1_000_000).expect("fits i32");

    let a = ExecutionId::new_for_shard(ShardId::new(0));
    let b = ExecutionId::new_for_shard(ShardId::new(0));
    install_gate(
        &mut conn,
        GATE,
        gate_key,
        "ExternalSignalRequested",
        &[a, b],
    )
    .await;
    set_gate(&mut gate, gate_key, true).await;
    for (exec_id, peer) in [(a, b), (b, a)] {
        let input = serde_json::json!({ "peer": peer.to_string() });
        start_run(&mut conn, "tx1822_signal_peer", exec_id, input).await;
    }
    let (metrics, worker, handle) =
        spawn_worker(&url, "tx1822-mutual-worker", tx1822_signal_peer_info());

    // Both persist transactions hold their own row and wait on the gate.
    wait_for_parked(&mut conn, &mut gate, GATE, gate_key, 2).await;

    // Open the gate. Each transaction now asks for the row of its peer.
    set_gate(&mut gate, gate_key, false).await;

    let a_state = wait_for_terminal(&url, a).await;
    let b_state = wait_for_terminal(&url, b).await;
    worker.shutdown();
    let _ = handle.await;
    remove_gate(&mut conn, GATE).await;

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
    assert!(
        metrics.exhausted().is_empty(),
        "the retry resolves the cycle"
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
    const GATE: &str = "harvest_test_tx1822_terminal_gate";
    let _serial = TRIGGER_TESTS.lock().await;
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let mut gate = connect(&url).await;
    sweep_stale_test_objects(&mut conn).await;
    // `SET LOCAL deadlock_timeout` below needs a superuser. Without it the
    // blocker can become the victim, so the test cannot force the cycle.
    if !is_superuser(&mut conn).await {
        eprintln!("skipped: the test role is not a superuser");
        return;
    }
    let gate_key = i32::try_from(std::process::id() % 1_000_000 + 1_000_000).expect("fits i32");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    install_gate(&mut conn, GATE, gate_key, "WorkflowCompleted", &[exec_id]).await;
    set_gate(&mut gate, gate_key, true).await;
    start_run(
        &mut conn,
        "tx1822_tag_and_complete",
        exec_id,
        serde_json::json!({}),
    )
    .await;
    let (metrics, worker, handle) = spawn_worker(
        &url,
        "tx1822-terminal-worker",
        tx1822_tag_and_complete_info(),
    );

    wait_for_parked(&mut conn, &mut gate, GATE, gate_key, 1).await;

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
    let waiter = tokio::spawn(async move {
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
    tokio::time::timeout(Duration::from_secs(20), async {
        while !waits_on_a_lock(&mut conn, blocker_pid).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the blocker must wait on the execution row");

    set_gate(&mut gate, gate_key, false).await;
    waiter.await.expect("join blocker");

    let state = wait_for_terminal(&url, exec_id).await;
    worker.shutdown();
    let _ = handle.await;
    remove_gate(&mut conn, GATE).await;

    assert_eq!(state, "COMPLETED", "the cycle runs again and commits");
    assert_eq!(
        metrics.count(SITE_WORKFLOW_TASK, "deadlock"),
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
                            tokio::time::timeout(Duration::from_secs(20), barrier.wait())
                                .await
                                .expect("both sides reach the barrier");
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
    #[derive(diesel::QueryableByName)]
    struct Value {
        #[diesel(sql_type = Integer)]
        v: i32,
    }
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
                    tokio::time::timeout(Duration::from_secs(20), writer_done.notified())
                        .await
                        .expect("the concurrent writer commits");
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
    assert_eq!(
        metrics.exhausted(),
        vec![(SITE_PERSIST.to_owned(), "deadlock".to_owned())],
        "the last conflict counts as exhausted"
    );
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

/// Completes at once.
#[workflow]
#[allow(clippy::unused_async)] // The `#[workflow]` macro requires an `async fn`.
async fn tx1822_noop(
    _ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    Ok(input)
}

/// Install a trigger that raises one synthetic `40P01`.
///
/// `name` names the trigger, its function and its sequence. `timing_event`
/// is the trigger timing and event, and `table` is the table. `condition`
/// selects the row. The sequence counts raises. A rollback does not undo
/// `nextval`, so only the first matching run fails.
async fn install_one_deadlock(
    conn: &mut AsyncPgConnection,
    name: &str,
    timing_event: &str,
    table: &str,
    condition: &str,
) {
    let sql = format!(
        "CREATE SEQUENCE {name};
         CREATE FUNCTION {name}() RETURNS trigger AS $once$
         BEGIN
           IF ({condition}) AND nextval('{name}') = 1 THEN
             RAISE EXCEPTION 'deadlock detected' USING ERRCODE = '40P01';
           END IF;
           RETURN COALESCE(NEW, OLD);
         END
         $once$ LANGUAGE plpgsql;
         CREATE TRIGGER {name} {timing_event} ON {table}
           FOR EACH ROW EXECUTE FUNCTION {name}();"
    );
    conn.batch_execute(&sql)
        .await
        .expect("install one-deadlock trigger");
}

/// The claim site retries a conflict inside the claim transaction.
///
/// Without the claim wrapper, the poll logs the error and a later poll claims
/// the task. The workflow still completes, but the claim counter stays at 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflict_in_a_claim_is_retried_at_the_claim_site() {
    let _serial = TRIGGER_TESTS.lock().await;
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    sweep_stale_test_objects(&mut conn).await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    install_one_deadlock(
        &mut conn,
        "harvest_test_tx1822_claim_once",
        "BEFORE UPDATE",
        "harvest_task_queue",
        &format!(
            "NEW.workflow_exec_id = '{}'::uuid AND NEW.state = 'RUNNING' \
             AND OLD.state <> 'RUNNING'",
            exec_id.as_uuid()
        ),
    )
    .await;
    start_run(&mut conn, "tx1822_noop", exec_id, serde_json::json!({})).await;
    let (metrics, worker, handle) = spawn_worker(&url, "tx1822-claim-worker", tx1822_noop_info());

    let state = wait_for_terminal(&url, exec_id).await;
    worker.shutdown();
    let _ = handle.await;
    sweep_stale_test_objects(&mut conn).await;

    assert_eq!(state, "COMPLETED");
    assert_eq!(metrics.count(SITE_CLAIM, "deadlock"), 1, "one claim retry");
    assert!(metrics.exhausted().is_empty());
}

/// The scanner site retries a conflict inside the debounce fire batch.
#[tokio::test]
async fn a_conflict_in_a_debounce_fire_batch_is_retried_at_the_scanner_site() {
    use autumn_harvest::debounce::{
        AdmitDebounceParams, DebounceStartOptions, admit_debounced_start, fire_due_debounced_starts,
    };

    let _serial = TRIGGER_TESTS.lock().await;
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    sweep_stale_test_objects(&mut conn).await;

    let key = format!("tx1822:{}", uuid::Uuid::new_v4());
    let workflow_id = format!("tx1822-debounce-{}", uuid::Uuid::new_v4());
    admit_debounced_start(
        &mut conn,
        AdmitDebounceParams {
            workflow_name: "tx1822_noop",
            debounce_key: &key,
            workflow_id: &workflow_id,
            queue_name: "tx1822-scanner-q",
            last_input: serde_json::json!({}),
            start_options: DebounceStartOptions::default(),
            window: Duration::from_millis(1),
            max_wait: Duration::from_secs(1),
            shard_id: 0,
        },
        false,
    )
    .await
    .expect("admit")
    .expect("an ungated admission returns an outcome");
    install_one_deadlock(
        &mut conn,
        "harvest_test_tx1822_scanner_once",
        "BEFORE DELETE",
        "harvest_debounce",
        &format!("OLD.debounce_key = '{key}'"),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let metrics = RetryMetrics::default();
    let fired = fire_due_debounced_starts(&mut conn, &None, &[], &metrics).await;
    sweep_stale_test_objects(&mut conn).await;

    assert!(fired.expect("the batch commits after one retry") >= 1);
    assert_eq!(
        metrics.count(SITE_SCANNER, "deadlock"),
        1,
        "one scanner retry"
    );
    assert!(metrics.exhausted().is_empty());
}

/// A conflict that reaches `fail_execution_on_error` does not fail the run.
///
/// This covers a conflict that outlasts the retries of a wired site. The
/// dispatcher then resets the task, and the cycle runs again.
#[tokio::test]
async fn a_conflict_error_does_not_fail_the_execution() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = format!("tx1822-pass-{}", uuid::Uuid::new_v4());
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_id = format!("tx1822-pass-{}", exec_id.as_uuid());
    autumn_harvest::execution::start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams::new(
            "tx1822_noop",
            &workflow_id,
            exec_id,
            serde_json::json!({}),
            &queue,
        ),
        None,
    )
    .await
    .expect("start workflow");
    let task = autumn_harvest::queue::claim_task(
        &mut conn,
        std::slice::from_ref(&queue),
        "tx1822-pass-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("the task is claimable");

    let result = autumn_harvest::worker::fail_execution_on_error(
        &mut conn,
        &task,
        "tx1822-pass-worker",
        Err::<(), _>(synthetic_deadlock()),
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert!(
        matches!(result, Err(HarvestError::Database(_))),
        "the error passes through"
    );
    assert_eq!(
        load_execution_from_url(&url, exec_id).await.state,
        "RUNNING"
    );
}
