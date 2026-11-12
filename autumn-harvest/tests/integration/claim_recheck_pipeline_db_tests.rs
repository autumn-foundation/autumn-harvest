#![cfg(feature = "db")]
//! The claim rechecks run in one pipeline. These tests prove the order.
//!
//! The queue pause recheck must see the shared queue lock. A trigger on
//! `harvest_task_queue` records whether the session holds that lock when the
//! recheck updates the row. Each test runs on a cold connection, on a warm
//! connection and on a connection that prepared the recheck before the lock.

use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

use autumn_harvest::models::TaskQueueItem;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};

use crate::throwaway_db::ThrowawayDb;

/// A migrated database of this test alone.
struct Db {
    url: String,
    _throwaway: Option<ThrowawayDb>,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn database() -> Db {
    if let Some(throwaway) = ThrowawayDb::create("claim_pipeline_t").await {
        return Db {
            url: throwaway.url(),
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
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply the migrations");
    Db {
        url,
        _throwaway: None,
        _container: Some(container),
    }
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to the test database")
}

/// Record, for each update of a task row, whether the session holds a
/// shared advisory lock.
async fn install_probe(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "CREATE TABLE IF NOT EXISTS recheck_probe (task_id uuid, saw_lock bool); \
         CREATE OR REPLACE FUNCTION recheck_probe_fn() RETURNS trigger AS $$ \
         BEGIN \
           IF OLD.state = 'RUNNING' AND NEW.state = 'PENDING' THEN \
             INSERT INTO recheck_probe SELECT NEW.id, EXISTS ( \
               SELECT 1 FROM pg_locks WHERE pid = pg_backend_pid() \
                 AND locktype = 'advisory' AND mode = 'ShareLock' AND granted); \
           END IF; \
           RETURN NEW; \
         END $$ LANGUAGE plpgsql; \
         DROP TRIGGER IF EXISTS recheck_probe_trg ON harvest_task_queue; \
         CREATE TRIGGER recheck_probe_trg BEFORE UPDATE ON harvest_task_queue \
           FOR EACH ROW EXECUTE FUNCTION recheck_probe_fn();",
    )
    .await
    .expect("install the probe trigger");
}

/// Enqueue a workflow task on `queue` and claim it as `w1`.
async fn claimed_task(conn: &mut AsyncPgConnection, queue: &str) -> Uuid {
    let exec_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'pipeline', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .execute(conn)
    .await
    .expect("insert the run");
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    let task_id = queue::enqueue(conn, &params).await.expect("enqueue");
    let claimed = queue::claim_task(conn, &[queue.to_string()], "w1", "", None, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.map(|t| t.id), Some(task_id));
    task_id
}

async fn load_task(conn: &mut AsyncPgConnection, task_id: Uuid) -> TaskQueueItem {
    diesel::sql_query("SELECT * FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(conn)
        .await
        .expect("load the task")
}

/// Run the rechecks in a claim transaction. True means that the claim keeps
/// its row.
async fn recheck(conn: &mut AsyncPgConnection, task_id: Uuid) -> bool {
    use diesel_async::TransactionManager as _;
    let task = load_task(conn, task_id).await;
    queue::begin_claim_transaction_for_test(conn, queue::CLAIM_BEGIN_WITH_SETTINGS_SQL)
        .await
        .expect("open a claim transaction");
    let kept = queue::apply_post_claim_rechecks_for_test(conn, task, "w1")
        .await
        .expect("run the rechecks");
    diesel_async::AnsiTransactionManager::commit_transaction(conn)
        .await
        .expect("commit");
    kept
}

#[derive(diesel::QueryableByName)]
struct Probe {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    saw_lock: bool,
}

async fn probes(conn: &mut AsyncPgConnection, task_id: Uuid) -> Vec<bool> {
    diesel::sql_query("SELECT saw_lock FROM recheck_probe WHERE task_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .load::<Probe>(conn)
        .await
        .expect("read the probe")
        .into_iter()
        .map(|p| p.saw_lock)
        .collect()
}

async fn task_state(conn: &mut AsyncPgConnection, task_id: Uuid) -> String {
    #[derive(diesel::QueryableByName)]
    struct State {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<State>(conn)
        .await
        .expect("read the task state")
        .state
}

/// Claim a task on a paused queue, recheck it on `conn`, and assert that
/// the pause recheck released it under the queue lock.
async fn assert_release_under_the_lock(
    admin: &mut AsyncPgConnection,
    conn: &mut AsyncPgConnection,
) {
    let queue = format!("pipeline-{}", Uuid::new_v4().simple());
    let task_id = claimed_task(admin, &queue).await;
    autumn_harvest::queue_pause::pause_queue(admin, &queue, "test", "alice", None)
        .await
        .expect("pause the queue");

    assert!(
        !recheck(conn, task_id).await,
        "a paused queue gives the row back"
    );
    assert_eq!(task_state(admin, task_id).await, "PENDING");
    assert_eq!(
        probes(admin, task_id).await,
        vec![true],
        "one recheck released the row, and it ran under the queue lock"
    );
}

/// A cold connection prepares each statement inside the pipeline.
#[tokio::test]
async fn the_pause_recheck_runs_under_the_lock_on_a_cold_connection() {
    let db = database().await;
    let mut admin = connect(&db.url).await;
    install_probe(&mut admin).await;
    let mut conn = connect(&db.url).await;
    assert_release_under_the_lock(&mut admin, &mut conn).await;
}

/// A warm connection sends every statement of the pipeline at once.
#[tokio::test]
async fn the_pause_recheck_runs_under_the_lock_on_a_warm_connection() {
    let db = database().await;
    let mut admin = connect(&db.url).await;
    install_probe(&mut admin).await;
    let mut conn = connect(&db.url).await;
    for _ in 0..3 {
        assert_release_under_the_lock(&mut admin, &mut conn).await;
    }
}

/// The recheck is prepared and the lock is not. The lock still runs first.
#[tokio::test]
async fn the_pause_recheck_runs_under_the_lock_when_only_the_recheck_is_prepared() {
    let db = database().await;
    let mut admin = connect(&db.url).await;
    install_probe(&mut admin).await;
    let mut conn = connect(&db.url).await;
    assert!(
        !autumn_harvest::queue_pause::release_claim_if_queue_paused(&conn, Uuid::new_v4(), "w1")
            .await
            .expect("prepare the recheck")
    );
    assert_release_under_the_lock(&mut admin, &mut conn).await;
}

/// An unpaused queue keeps its claim.
#[tokio::test]
async fn an_unpaused_queue_keeps_its_claim() {
    let db = database().await;
    let mut admin = connect(&db.url).await;
    let mut conn = connect(&db.url).await;
    let queue = format!("pipeline-{}", Uuid::new_v4().simple());
    let task_id = claimed_task(&mut admin, &queue).await;
    assert!(recheck(&mut conn, task_id).await);
    assert_eq!(task_state(&mut admin, task_id).await, "RUNNING");
}

/// A pause that holds the exclusive queue lock makes the lock fail. No
/// recheck releases the row then, so the claim gives it back itself.
#[tokio::test]
async fn a_failed_queue_lock_gives_the_row_back() {
    use diesel_async::TransactionManager as _;
    let db = database().await;
    let mut admin = connect(&db.url).await;
    let mut conn = connect(&db.url).await;
    let mut pauser = connect(&db.url).await;
    let queue = format!("pipeline-{}", Uuid::new_v4().simple());
    let task_id = claimed_task(&mut admin, &queue).await;

    diesel_async::AnsiTransactionManager::begin_transaction(&mut pauser)
        .await
        .expect("open the pause transaction");
    autumn_harvest::queue_pause::lock_queue_for_pause(&mut pauser, &queue)
        .await
        .expect("take the exclusive queue lock");

    assert!(
        !recheck(&mut conn, task_id).await,
        "a failed lock keeps no claim"
    );
    assert_eq!(task_state(&mut admin, task_id).await, "PENDING");

    diesel_async::AnsiTransactionManager::rollback_transaction(&mut pauser)
        .await
        .expect("end the pause transaction");
}
