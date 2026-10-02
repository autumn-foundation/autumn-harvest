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

#[tokio::test]
async fn wake_in_a_transaction_commits_when_pg_notify_fails() {
    let (url, _container) = setup().await;
    let mut setup_conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = insert_execution(&mut setup_conn).await;
    let mut params = enqueue_params("default", exec_id);
    params.scheduled_at = chrono::Utc::now() - chrono::Duration::seconds(1);
    let task_id = queue::enqueue(&mut setup_conn, &params)
        .await
        .expect("enqueue");
    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut setup_conn, &queues, "notify-test", "", None, &[], &[])
        .await
        .expect("claim")
        .expect("a claimable task");
    assert_eq!(claimed.id, task_id);
    queue::park_workflow_task(&mut setup_conn, task_id, None)
        .await
        .expect("park");

    let mut conn = shadowed_conn(&url).await;
    let result = Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        queue::wake_workflow_task(conn, exec_id).await
    }))
    .await;

    result.expect("the wake must commit");
    assert_eq!(pending_count(&url, task_id).await, 1);
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

/// The tolerance on the wake latency after commit.
///
/// An in-transaction NOTIFY reaches the listener at commit. The sender adds
/// one tick. The bound is wide, so a loaded CI host does not flake.
const WAKE_TOLERANCE: Duration = Duration::from_secs(2);

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
    let committed_at = Instant::now();

    let outcome = listener
        .wait_for_notification_timeout(WAKE_TOLERANCE)
        .await
        .expect("payload parses");
    let latency = committed_at.elapsed();
    assert!(
        matches!(&outcome, WorkflowEventWaitOutcome::Notification(p) if p.workflow_exec_id == exec_id.as_uuid()),
        "{outcome:?}"
    );
    assert!(latency < WAKE_TOLERANCE, "wake latency {latency:?}");
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
        .wait_for_notification_timeout(WAKE_TOLERANCE)
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
async fn one_tick_sends_one_wake_per_queue() {
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
        // Hold the commit, so the sender holds all three notes when it sees
        // the commit. A note whose write committed before the sender read it
        // goes in its own tick.
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(())
    }))
    .await
    .expect("enqueue");

    let first = listener
        .wait_for_notification(WAKE_TOLERANCE)
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
    assert!(
        autumn_harvest::notify::register_pool(&pool)
            .wait_ready(READY)
            .await
    );
    let before = autumn_harvest::notify::send_failures();
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = insert_execution(&mut conn).await;

    let appended = store::append_events(&mut conn, exec_id, &[started()], 0)
        .await
        .expect("the append must succeed");
    assert_eq!(appended, 1);

    let deadline = Instant::now() + READY;
    while autumn_harvest::notify::send_failures() == before && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        autumn_harvest::notify::send_failures() > before,
        "a failed send must be counted"
    );
    assert_eq!(event_count(&url, exec_id).await, 1);
}

#[tokio::test]
async fn a_sender_samples_the_notification_queue_usage() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    assert!(
        autumn_harvest::notify::register_pool(&pool)
            .wait_ready(READY)
            .await
    );
    let usage = autumn_harvest::notify::queue_usage().expect("a ready sender reads the usage");
    assert!((0.0..=1.0).contains(&usage), "usage {usage}");
}
