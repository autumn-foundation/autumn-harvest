//! Post-commit NOTIFY tests (issue #1796).
//!
//! A failed `pg_notify` must never fail a history append or an enqueue.
//! Each test makes `pg_notify` fail on one session only. A function with the
//! same name in the `notify_fail` schema raises an error, and the session puts
//! that schema before `pg_catalog` in its `search_path`.

use std::time::{Duration, Instant};

use autumn_harvest::error::HarvestError;
use autumn_harvest::notify::{QueueListener, WorkflowEventListener, WorkflowEventWaitOutcome};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::worker::DbPool;
use autumn_harvest::{ExecutionId, WorkflowEvent, store};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// SQL that installs a `pg_notify` that always fails.
const SHADOW_SQL: &str = "CREATE SCHEMA IF NOT EXISTS notify_fail; \
    CREATE OR REPLACE FUNCTION notify_fail.pg_notify(text, text) RETURNS void \
    LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'pg_notify is disabled on this session'; END $$;";

/// The `search_path` that selects the failing `pg_notify`.
const SHADOW_SEARCH_PATH: &str = "notify_fail, pg_catalog, public";

/// Start a migrated Postgres with the failing `pg_notify` installed.
async fn setup() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrations");
    conn.batch_execute(SHADOW_SQL)
        .await
        .expect("shadow pg_notify");
    (url, container)
}

/// Open a connection on which every `pg_notify` call fails.
async fn shadowed_conn(url: &str) -> AsyncPgConnection {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    conn.batch_execute(&format!("SET search_path = {SHADOW_SEARCH_PATH}"))
        .await
        .expect("set search_path");
    let probe = diesel::sql_query("SELECT pg_notify('harvest_probe', 'x')")
        .execute(&mut conn)
        .await;
    assert!(probe.is_err(), "the test session must not reach pg_notify");
    conn
}

/// A pool whose connections reach the real `pg_notify`.
fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

/// Insert a minimal execution row for the events to reference.
async fn insert_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'notify_test', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(Uuid::new_v4().to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    exec_id
}

/// Count the stored events of one execution.
async fn event_count(url: &str, exec_id: ExecutionId) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    diesel::sql_query("SELECT count(*) AS n FROM harvest_events WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<Count>(&mut conn)
        .await
        .expect("count events")
        .n
}

/// Count the queued tasks with one id.
async fn task_count(url: &str, task_id: Uuid) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    diesel::sql_query("SELECT count(*) AS n FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<Count>(&mut conn)
        .await
        .expect("count tasks")
        .n
}

/// Count the `PENDING` tasks with one id.
async fn pending_count(url: &str, task_id: Uuid) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    diesel::sql_query(
        "SELECT count(*) AS n FROM harvest_task_queue WHERE id = $1 AND state = 'PENDING'",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .get_result::<Count>(&mut conn)
    .await
    .expect("count pending tasks")
    .n
}

fn started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn completed() -> WorkflowEvent {
    WorkflowEvent::WorkflowCompleted {
        output: serde_json::json!({"ok": true}),
    }
}

fn enqueue_params(queue_name: &str, exec_id: ExecutionId) -> EnqueueParams {
    let mut params = EnqueueParams::new(queue_name, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params
}

// ── RED: a failed pg_notify never fails the write ──────────────────────

#[tokio::test]
async fn append_in_a_transaction_commits_when_pg_notify_fails() {
    let (url, _container) = setup().await;
    let mut conn = shadowed_conn(&url).await;
    let exec_id = insert_execution(&mut conn).await;

    let result = Box::pin(conn.transaction::<usize, HarvestError, _>(async |conn| {
        store::append_events(conn, exec_id, &[started(), completed()], 0).await
    }))
    .await;

    assert_eq!(result.expect("the append must commit"), 2);
    assert_eq!(event_count(&url, exec_id).await, 2);
}

#[tokio::test]
async fn append_outside_a_transaction_succeeds_when_pg_notify_fails() {
    let (url, _container) = setup().await;
    let mut conn = shadowed_conn(&url).await;
    let exec_id = insert_execution(&mut conn).await;

    let result = store::append_events(&mut conn, exec_id, &[started()], 0).await;

    assert_eq!(result.expect("the append must succeed"), 1);
    assert_eq!(event_count(&url, exec_id).await, 1);
}

#[tokio::test]
async fn enqueue_in_a_transaction_commits_when_pg_notify_fails() {
    let (url, _container) = setup().await;
    let mut conn = shadowed_conn(&url).await;
    let exec_id = insert_execution(&mut conn).await;
    let params = enqueue_params("default", exec_id);

    let result =
        Box::pin(conn.transaction::<Uuid, HarvestError, _>(async |conn| {
            queue::enqueue(conn, &params).await
        }))
        .await;

    let task_id = result.expect("the enqueue must commit");
    assert_eq!(task_count(&url, task_id).await, 1);
}

/// Enqueue, claim and park a workflow task, so a wake has a row to re-pend.
async fn park_task(url: &str) -> (ExecutionId, Uuid) {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = enqueue_params("default", exec_id);
    params.scheduled_at = chrono::Utc::now() - chrono::Duration::seconds(1);
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");
    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "notify-test", "", None, &[], &[])
        .await
        .expect("claim")
        .expect("a claimable task");
    assert_eq!(claimed.id, task_id);
    queue::park_workflow_task(&mut conn, task_id, None)
        .await
        .expect("park");
    (exec_id, task_id)
}

#[tokio::test]
async fn enqueue_outside_a_transaction_succeeds_when_pg_notify_fails() {
    let (url, _container) = setup().await;
    let mut conn = shadowed_conn(&url).await;
    let exec_id = insert_execution(&mut conn).await;

    let task_id = queue::enqueue(&mut conn, &enqueue_params("default", exec_id))
        .await
        .expect("the enqueue must succeed");

    assert_eq!(task_count(&url, task_id).await, 1);
}

#[tokio::test]
async fn append_in_a_raw_begin_block_commits_when_pg_notify_fails() {
    let (url, _container) = setup().await;
    let mut conn = shadowed_conn(&url).await;
    let exec_id = insert_execution(&mut conn).await;

    // Diesel does not track a transaction that a raw `BEGIN` opens.
    conn.batch_execute("BEGIN").await.expect("begin");
    store::append_events(&mut conn, exec_id, &[started()], 0)
        .await
        .expect("the append must succeed");
    // The fallback releases its savepoint. Probe behind a savepoint of our
    // own, so the failed probe does not abort the block.
    conn.batch_execute("SAVEPOINT probe").await.expect("probe");
    let leftover = conn
        .batch_execute("RELEASE SAVEPOINT harvest_notify_fallback")
        .await;
    assert!(leftover.is_err(), "the fallback must release its savepoint");
    conn.batch_execute("ROLLBACK TO SAVEPOINT probe")
        .await
        .expect("undo the probe");
    conn.batch_execute("COMMIT").await.expect("commit");

    assert_eq!(event_count(&url, exec_id).await, 1, "the write must commit");
}

#[tokio::test]
async fn a_failed_probe_in_a_raw_begin_block_fails_the_append() {
    let (url, _container) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = insert_execution(&mut conn).await;
    // Make the raw-block probe fail on this session only.
    conn.batch_execute(
        "CREATE SCHEMA IF NOT EXISTS probe_fail; \
         CREATE OR REPLACE FUNCTION probe_fail.txid_current_if_assigned() RETURNS bigint \
         LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'probe disabled'; END $$; \
         SET search_path = probe_fail, pg_catalog, public",
    )
    .await
    .expect("shadow the probe");

    conn.batch_execute("BEGIN").await.expect("begin");
    let result = store::append_events(&mut conn, exec_id, &[started()], 0).await;
    conn.batch_execute("ROLLBACK").await.expect("rollback");

    // The failed probe aborted the block, so a false `Ok` would let the
    // caller's `COMMIT` roll back silently.
    assert!(result.is_err(), "the append must report the aborted block");
}

#[tokio::test]
async fn wake_in_a_transaction_commits_when_pg_notify_fails() {
    let (url, _container) = setup().await;
    let (exec_id, task_id) = park_task(&url).await;

    let mut conn = shadowed_conn(&url).await;
    let result = Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        queue::wake_workflow_task(conn, exec_id).await
    }))
    .await;

    result.expect("the wake must commit");
    assert_eq!(pending_count(&url, task_id).await, 1);
}

#[tokio::test]
async fn wake_outside_a_transaction_succeeds_when_pg_notify_fails() {
    let (url, _container) = setup().await;
    let (exec_id, task_id) = park_task(&url).await;

    let mut conn = shadowed_conn(&url).await;
    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("the wake must succeed");

    assert_eq!(pending_count(&url, task_id).await, 1);
}

#[tokio::test]
async fn a_long_queue_name_commits_and_wakes_its_listener() {
    let (url, _container) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = insert_execution(&mut conn).await;
    // `harvest_queue_` plus 60 bytes is longer than a Postgres identifier.
    let queue_name = "q".repeat(60);
    let mut listener = QueueListener::connect(&url, std::slice::from_ref(&queue_name))
        .await
        .expect("listener");
    let params = enqueue_params(&queue_name, exec_id);

    let result =
        Box::pin(conn.transaction::<Uuid, HarvestError, _>(async |conn| {
            queue::enqueue(conn, &params).await
        }))
        .await;

    let task_id = result.expect("the enqueue must commit");
    assert_eq!(task_count(&url, task_id).await, 1);
    let wake = listener
        .wait_for_notification(Duration::from_secs(5))
        .await
        .expect("payload parses")
        .expect("the listener of the long queue must wake");
    assert_eq!(wake.task_id, task_id);
}

// ── Fallback delivery without a registered pool ────────────────────────

#[tokio::test]
async fn an_unregistered_database_still_gets_the_wake_after_commit() {
    let (url, _container) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = insert_execution(&mut conn).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    Box::pin(conn.transaction::<usize, HarvestError, _>(async |conn| {
        store::append_events(conn, exec_id, &[started()], 0).await
    }))
    .await
    .expect("append");

    let outcome = listener
        .wait_for_notification_timeout(Duration::from_secs(5))
        .await
        .expect("payload parses");
    assert!(
        matches!(&outcome, WorkflowEventWaitOutcome::Notification(p) if p.workflow_exec_id == exec_id.as_uuid()),
        "{outcome:?}"
    );
}

// ── Post-commit delivery from a registered pool ────────────────────────

/// How long a test waits for a registered sender to become ready.
const READY: Duration = Duration::from_secs(10);

/// How long a test waits for a wake that must arrive.
const WAIT: Duration = Duration::from_secs(5);

/// The extra wake latency the sender may add over an in-transaction NOTIFY.
///
/// The sender reads an open transaction again at most 25 ms later. The rest
/// of the bound absorbs a loaded CI host.
const WAKE_TOLERANCE: Duration = Duration::from_millis(250);

/// Samples per mode in the latency test.
const LATENCY_SAMPLES: usize = 5;

/// How long each latency sample holds its transaction open.
const HOLD: Duration = Duration::from_millis(100);

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

/// Wait for the wake of `exec_id` and return when it arrived.
async fn wake_of(listener: &mut WorkflowEventListener, exec_id: ExecutionId) -> Instant {
    loop {
        match listener
            .wait_for_notification_timeout(WAIT)
            .await
            .expect("payload parses")
        {
            WorkflowEventWaitOutcome::Notification(p)
                if p.workflow_exec_id == exec_id.as_uuid() =>
            {
                return Instant::now();
            }
            WorkflowEventWaitOutcome::Notification(_) => {}
            other => panic!("no wake for {exec_id}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn wake_latency_stays_within_tolerance_of_an_in_transaction_notify() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    assert!(
        autumn_harvest::notify::register_pool(&pool)
            .wait_ready(READY)
            .await
    );
    let mut conn = pool.get().await.expect("pool connection");
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    // Before issue #1796: `pg_notify` inside the write transaction.
    let mut before = Vec::new();
    for _ in 0..LATENCY_SAMPLES {
        let exec_id = insert_execution(&mut conn).await;
        let payload = serde_json::to_string(&autumn_harvest::notify::WorkflowEventNotifyPayload {
            workflow_exec_id: exec_id.as_uuid(),
            event_count: 1,
            last_event_type: "WorkflowStarted".to_string(),
        })
        .expect("payload");
        Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
            diesel::sql_query("SELECT pg_notify('harvest_events', $1)")
                .bind::<diesel::sql_types::Text, _>(&payload)
                .execute(conn)
                .await
                .map_err(autumn_harvest::error::database_error)?;
            tokio::time::sleep(HOLD).await;
            Ok(())
        }))
        .await
        .expect("in-transaction notify");
        let committed_at = Instant::now();
        before.push(wake_of(&mut listener, exec_id).await - committed_at);
    }

    // After issue #1796: the sender sends after commit.
    let mut after = Vec::new();
    for _ in 0..LATENCY_SAMPLES {
        let exec_id = insert_execution(&mut conn).await;
        Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
            store::append_events(conn, exec_id, &[started()], 0).await?;
            tokio::time::sleep(HOLD).await;
            Ok(())
        }))
        .await
        .expect("append");
        let committed_at = Instant::now();
        after.push(wake_of(&mut listener, exec_id).await - committed_at);
    }

    let (before, after) = (median(before), median(after));
    assert!(
        after <= before + WAKE_TOLERANCE,
        "median wake latency {after:?} after the change, {before:?} before"
    );
}

#[tokio::test]
async fn a_registered_pool_wakes_after_commit_and_not_before() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    assert!(
        autumn_harvest::notify::register_pool(&pool)
            .wait_ready(READY)
            .await,
        "the sender must become ready"
    );
    let mut conn = pool.get().await.expect("pool connection");
    let exec_id = insert_execution(&mut conn).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        store::append_events(conn, exec_id, &[started()], 0).await?;
        let early = listener
            .wait_for_notification_timeout(Duration::from_millis(500))
            .await
            .expect("payload parses");
        assert_eq!(
            early,
            WorkflowEventWaitOutcome::TimedOut,
            "no wake may arrive before commit"
        );
        Ok(())
    }))
    .await
    .expect("append");

    wake_of(&mut listener, exec_id).await;
}

#[tokio::test]
async fn a_raw_begin_block_wakes_after_commit_and_not_before() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    assert!(
        autumn_harvest::notify::register_pool(&pool)
            .wait_ready(READY)
            .await
    );
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = insert_execution(&mut conn).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    // Diesel does not track a transaction that a raw `BEGIN` opens.
    conn.batch_execute("BEGIN").await.expect("begin");
    store::append_events(&mut conn, exec_id, &[started()], 0)
        .await
        .expect("append");
    assert_eq!(
        listener
            .wait_for_notification_timeout(Duration::from_millis(500))
            .await
            .expect("payload parses"),
        WorkflowEventWaitOutcome::TimedOut,
        "no wake may arrive before commit"
    );
    conn.batch_execute("COMMIT").await.expect("commit");

    wake_of(&mut listener, exec_id).await;
}

#[tokio::test]
async fn a_registered_pool_never_notifies_on_the_write_connection() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    assert!(
        autumn_harvest::notify::register_pool(&pool)
            .wait_ready(READY)
            .await
    );
    let mut conn = shadowed_conn(&url).await;
    let exec_id = insert_execution(&mut conn).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    Box::pin(conn.transaction::<usize, HarvestError, _>(async |conn| {
        store::append_events(conn, exec_id, &[started()], 0).await
    }))
    .await
    .expect("append");

    let outcome = listener
        .wait_for_notification_timeout(WAIT)
        .await
        .expect("payload parses");
    assert!(
        matches!(&outcome, WorkflowEventWaitOutcome::Notification(p) if p.workflow_exec_id == exec_id.as_uuid()),
        "the sender must deliver on its own connection: {outcome:?}"
    );
}

#[tokio::test]
async fn a_rolled_back_write_sends_no_wake() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    assert!(
        autumn_harvest::notify::register_pool(&pool)
            .wait_ready(READY)
            .await
    );
    let mut conn = pool.get().await.expect("pool connection");
    let exec_id = insert_execution(&mut conn).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    let result = Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        store::append_events(conn, exec_id, &[started()], 0).await?;
        Err(HarvestError::Cancelled("roll back".to_string()))
    }))
    .await;
    assert!(result.is_err());

    let outcome = listener
        .wait_for_notification_timeout(Duration::from_secs(1))
        .await
        .expect("payload parses");
    assert_eq!(outcome, WorkflowEventWaitOutcome::TimedOut);
}

#[tokio::test]
async fn three_enqueues_in_one_commit_send_one_merged_wake() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    assert!(
        autumn_harvest::notify::register_pool(&pool)
            .wait_ready(READY)
            .await
    );
    let mut conn = pool.get().await.expect("pool connection");
    let exec_id = insert_execution(&mut conn).await;
    let queues = vec!["default".to_string()];
    let mut listener = QueueListener::connect(&url, &queues)
        .await
        .expect("listener");

    let params = enqueue_params("default", exec_id);
    Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        for _ in 0..3 {
            queue::enqueue(conn, &params).await?;
        }
        // Hold the commit until the sender holds all three notes. A note that
        // is still pending at the gate read goes in a later tick.
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(())
    }))
    .await
    .expect("enqueue");

    let first = listener
        .wait_for_notification(WAIT)
        .await
        .expect("payload parses")
        .expect("one wake");
    assert_eq!(first.task_id, Uuid::nil(), "a merged wake names no task");
    let second = listener
        .wait_for_notification(Duration::from_millis(500))
        .await
        .expect("payload parses");
    assert_eq!(second, None, "three enqueues in one commit send one wake");
}

#[tokio::test]
async fn a_failed_send_is_counted_and_the_append_commits() {
    let (url, _container) = setup().await;
    // Every connection of this pool reaches the failing `pg_notify`.
    let failing_url = format!("{url}?options=-c%20search_path%3Dnotify_fail%2Cpg_catalog%2Cpublic");
    let pool = build_pool(&failing_url);
    let sink = autumn_harvest::notify::register_pool(&pool);
    assert!(sink.wait_ready(READY).await);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = insert_execution(&mut conn).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    let appended = store::append_events(&mut conn, exec_id, &[started()], 0)
        .await
        .expect("the append must succeed");
    assert_eq!(appended, 1);

    let deadline = Instant::now() + READY;
    while sink.send_failures() == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        sink.send_failures() > 0,
        "the sender must count its failed send"
    );
    assert_eq!(
        listener
            .wait_for_notification_timeout(Duration::from_millis(500))
            .await
            .expect("payload parses"),
        WorkflowEventWaitOutcome::TimedOut,
        "the failing sender, not the write connection, owned the wake"
    );
    assert_eq!(event_count(&url, exec_id).await, 1);
}

#[tokio::test]
async fn a_sender_samples_the_notification_queue_usage() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let sink = autumn_harvest::notify::register_pool(&pool);
    assert!(sink.wait_ready(READY).await);
    let usage = sink.queue_usage().expect("a ready sender reads the usage");
    assert!((0.0..=1.0).contains(&usage), "usage {usage}");
}

#[tokio::test]
async fn a_handle_client_registers_its_pool() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let _client = autumn_harvest::WorkflowHandleClient::single(pool.clone(), url.clone());
    assert!(
        autumn_harvest::notify::register_pool(&pool)
            .wait_ready(READY)
            .await,
        "the client must have started the sender"
    );
    let mut conn = shadowed_conn(&url).await;
    let exec_id = insert_execution(&mut conn).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    Box::pin(conn.transaction::<usize, HarvestError, _>(async |conn| {
        store::append_events(conn, exec_id, &[started()], 0).await
    }))
    .await
    .expect("append");

    wake_of(&mut listener, exec_id).await;
}

#[tokio::test]
async fn a_pool_registered_outside_a_runtime_starts_its_sender_later() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let sink = {
        let pool = pool.clone();
        std::thread::spawn(move || autumn_harvest::notify::register_pool(&pool))
            .join()
            .expect("register outside a runtime")
    };
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let mut shadowed = shadowed_conn(&url).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    // The first note staged in a runtime starts the sender.
    let exec_id = insert_execution(&mut conn).await;
    store::append_events(&mut conn, exec_id, &[started()], 0)
        .await
        .expect("append");
    assert!(sink.wait_ready(READY).await, "the sender must start");

    // A parallel test can start the sender on its own runtime, which ends
    // with that test. The next note then starts the sender again here.
    assert_wake_from_sender(&mut shadowed, &mut listener, &sink).await;
}

/// Append on `shadowed` until a wake for the append arrives, up to three
/// times.
///
/// `shadowed` cannot send a wake itself, so a wake proves the sender sent it.
/// A sender that is not running when a note is staged starts again at that
/// note, so a later attempt succeeds.
async fn assert_wake_from_sender(
    shadowed: &mut AsyncPgConnection,
    listener: &mut WorkflowEventListener,
    sink: &autumn_harvest::notify::NotifySink,
) {
    for attempt in 1..=3 {
        let exec_id = insert_execution(shadowed).await;
        Box::pin(
            shadowed.transaction::<usize, HarvestError, _>(async |conn| {
                store::append_events(conn, exec_id, &[started()], 0).await
            }),
        )
        .await
        .expect("append");
        let woke = loop {
            match listener
                .wait_for_notification_timeout(Duration::from_secs(2))
                .await
                .expect("payload parses")
            {
                WorkflowEventWaitOutcome::Notification(p)
                    if p.workflow_exec_id == exec_id.as_uuid() =>
                {
                    break true;
                }
                WorkflowEventWaitOutcome::Notification(_) => {}
                _ => break false,
            }
        };
        if woke {
            return;
        }
        assert!(sink.wait_ready(READY).await, "attempt {attempt}");
    }
    panic!("the sender never delivered a wake");
}

#[tokio::test]
async fn a_sender_starts_again_after_its_runtime_ends() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let sink = {
        let pool = pool.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().expect("runtime");
            let sink = runtime.block_on(async {
                let sink = autumn_harvest::notify::register_pool(&pool);
                assert!(sink.wait_ready(READY).await);
                sink
            });
            drop(runtime);
            sink
        })
        .join()
        .expect("a short-lived runtime")
    };
    let mut shadowed = shadowed_conn(&url).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    assert_wake_from_sender(&mut shadowed, &mut listener, &sink).await;
}

#[tokio::test]
async fn a_sender_cancelled_before_its_first_poll_starts_again() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let sink = {
        let pool = pool.clone();
        std::thread::spawn(move || {
            // The block finishes at its first poll, so the runtime never
            // polls the sender task. The drop cancels that task.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            let sink = runtime.block_on(async { autumn_harvest::notify::register_pool(&pool) });
            drop(runtime);
            sink
        })
        .join()
        .expect("a short-lived runtime")
    };
    let mut shadowed = shadowed_conn(&url).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    assert_wake_from_sender(&mut shadowed, &mut listener, &sink).await;
}

#[tokio::test]
async fn a_scheduler_runtime_registers_its_pool() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let registry = std::sync::Arc::new(autumn_harvest::worker::HandlerRegistry::new(
        Vec::new(),
        Vec::new(),
    ));
    let scheduler = autumn_harvest::scheduler::SchedulerRuntime::spawn(
        pool.clone(),
        registry,
        std::sync::Arc::new(autumn_harvest::scheduler::DagCatalog::new()),
        std::sync::Arc::new(Vec::new()),
    );
    let mut shadowed = shadowed_conn(&url).await;
    let mut listener = WorkflowEventListener::connect(&url)
        .await
        .expect("listener");

    // Nothing else registers this pool, and `shadowed` cannot send a wake.
    // A wake therefore proves that the scheduler started the sender.
    let mut woke = false;
    for _ in 0..10 {
        let exec_id = insert_execution(&mut shadowed).await;
        Box::pin(
            shadowed.transaction::<usize, HarvestError, _>(async |conn| {
                store::append_events(conn, exec_id, &[started()], 0).await
            }),
        )
        .await
        .expect("append");
        if let WorkflowEventWaitOutcome::Notification(p) = listener
            .wait_for_notification_timeout(Duration::from_millis(500))
            .await
            .expect("payload parses")
        {
            assert_eq!(p.workflow_exec_id, exec_id.as_uuid());
            woke = true;
            break;
        }
    }
    scheduler.shutdown();
    scheduler.join().await.expect("scheduler stops");
    assert!(woke, "the scheduler must register its pool");
}
