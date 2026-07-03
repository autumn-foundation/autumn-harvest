#![cfg(feature = "db")]
//! Integration tests for transactional activities (issue #352).
//!
//! These tests prove that `ctx.run_transactional` atomically commits user
//! domain writes and the `ActivityCompleted` event in one Postgres
//! transaction; on error the writes are rolled back and the activity fails.

use std::sync::Arc;
use std::time::Duration;

use diesel::prelude::*;
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::prelude::*;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::types::Priority;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    StartWorkflowParams, WorkflowIdReusePolicy, start_or_load_workflow_execution,
};

/// Holds either a live testcontainers handle (keeps the container running) or
/// nothing when a pre-existing database is used via `TEST_DATABASE_URL`.
#[allow(dead_code)]
enum DbHandle {
    Container(Box<testcontainers::ContainerAsync<Postgres>>),
    External,
}

/// Serializes integration tests that share a `user_records` table when run
/// against a single Postgres instance (i.e. when `TEST_DATABASE_URL` is set).
static DB_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---------------------------------------------------------------------------
// Migration SQL (same set as integration_e2e.rs)
// ---------------------------------------------------------------------------

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    // issue #523: workflow-level retry policy columns.
    include_str!("../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../migrations/20260704000000_harvest_build_policy_ramp/up.sql")
);

const CREATE_USER_RECORDS: &str = "
    DROP TABLE IF EXISTS user_records;
    CREATE TABLE user_records (
        id BIGSERIAL PRIMARY KEY,
        value TEXT NOT NULL
    );
";

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Set up a test database. If `TEST_DATABASE_URL` is set in the environment,
/// use that Postgres instance and apply migrations directly (tests are
/// responsible for isolation via unique IDs and a fresh `user_records` table).
/// Otherwise spin up a throwaway testcontainers Postgres container.
async fn setup_db() -> (String, DbHandle) {
    if let Ok(url) = std::env::var("TEST_DATABASE_URL") {
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        conn.batch_execute(INIT_SQL).await.unwrap_or(()); // ignore "already exists" errors on repeat runs
        return (url, DbHandle::External);
    }

    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container.get_host().await.expect("get host");
    let port = container.get_host_port_ipv4(5432).await.expect("get port");
    let db_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (db_url, DbHandle::Container(Box::new(container)))
}

fn make_pool(db_url: &str) -> DbPool {
    let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
        diesel_async::AsyncPgConnection,
    >::new(db_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(10)
        .build()
        .expect("pool build failed")
}

async fn load_execution(db_url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = AsyncPgConnection::establish(db_url).await.expect("connect");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("load execution")
}

async fn wait_for_state(db_url: &str, exec_id: ExecutionId, target: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let execution = load_execution(db_url, exec_id).await;
            if execution.state == target {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("workflow did not reach '{target}' within timeout"));
}

fn make_worker(_db_url: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: uuid::Uuid::new_v4().to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::ZERO,
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(30),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 100,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn count_user_records(db_url: &str) -> i64 {
    let mut conn = AsyncPgConnection::establish(db_url).await.expect("connect");
    let rows = diesel::sql_query("SELECT COUNT(*)::bigint AS n FROM user_records")
        .load::<CountRow>(&mut conn)
        .await
        .expect("count user_records");
    rows[0].n
}

async fn count_activity_completed_events(db_url: &str, exec_id: ExecutionId) -> i64 {
    let mut conn = AsyncPgConnection::establish(db_url).await.expect("connect");
    let rows = diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_data->>'type' = 'ActivityCompleted'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .load::<CountRow>(&mut conn)
    .await
    .expect("count ActivityCompleted events");
    rows[0].n
}

// ---------------------------------------------------------------------------
// Activity and workflow definitions
// ---------------------------------------------------------------------------

/// Inserts a row inside `run_transactional` — commits atomically with
/// `ActivityCompleted`.
#[activity(start_to_close = "30s")]
async fn insert_record_txn(ctx: &ActivityContext, value: String) -> Result<(), String> {
    ctx.run_transactional(|conn| {
        Box::pin(async move {
            diesel::sql_query("INSERT INTO user_records (value) VALUES ($1)")
                .bind::<Text, _>(value.as_str())
                .execute(conn)
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        })
    })
    .await
}

/// Inserts a row but returns `Err` — the transaction must roll back.
#[activity(start_to_close = "30s")]
async fn insert_then_fail_txn(ctx: &ActivityContext, value: String) -> Result<(), String> {
    let _ = value;
    ctx.run_transactional(|conn| {
        Box::pin(async move {
            diesel::sql_query("INSERT INTO user_records (value) VALUES ('should_be_rolled_back')")
                .execute(conn)
                .await
                .map_err(|e| e.to_string())?;
            // Deliberately fail after the insert
            Err("intentional failure".to_string())
        })
    })
    .await
}

#[workflow]
async fn happy_txn_workflow(ctx: &WorkflowContext, input: ()) -> Result<(), String> {
    let () = input;
    ctx.execute_activity(&insert_record_txn_info(), "hello".to_string())
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn failing_txn_workflow(ctx: &WorkflowContext, input: ()) -> Result<(), String> {
    let () = input;
    ctx.execute_activity(&insert_then_fail_txn_info(), "fail".to_string())
        .await
        .map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

/// Happy path: exactly one `user_records` row and one `ActivityCompleted`
/// event after a successful transactional activity.
#[tokio::test]
async fn transactional_activity_happy_path_atomic_commit() {
    let _guard = DB_TEST_LOCK.lock().await;
    let (db_url, _container) = setup_db().await;
    let mut setup_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("connect");
    setup_conn
        .batch_execute(CREATE_USER_RECORDS)
        .await
        .expect("create user_records");

    let pool = make_pool(&db_url);
    let registry = Arc::new(HandlerRegistry::new(
        vec![happy_txn_workflow_info()],
        vec![insert_record_txn_info()],
    ));
    let worker = make_worker(&db_url, registry);
    let worker_pool = pool.clone();
    let worker_handle = tokio::spawn({
        let w = worker.clone();
        async move { w.run(&worker_pool).await }
    });

    let exec_id = {
        let mut conn = pool.get().await.unwrap();
        start_or_load_workflow_execution(
            &mut conn,
            StartWorkflowParams {
                workflow_name: "happy_txn_workflow",
                workflow_id: &format!("txn-happy-{}", uuid::Uuid::new_v4()),
                exec_id: ExecutionId::new(),
                input: serde_json::json!(null),
                parent_id: None,
                queue_name: "default",
                execution_timeout: None,
                memo: None,
                search_attrs: None,
                reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                trace_context: None,
                max_execution_timeout_ceiling: None,
                concurrency_key: None,
                concurrency_limit: None,
                priority: Priority::default(),
                max_workflow_input_bytes: 0,
                start_at: None,
                delay: None,
                max_workflow_start_delay: None,

                owner: None,
                runbook_url: None,
                severity: None,
                context_headers: None,

                sla: None,
                schedule_id: None,
                scheduled_for: None,
                workflow_attempt: 1,
                workflow_retry_policy: None,
                retry_of_exec_id: None,
                max_workflow_attempts_ceiling: None,
                origin: None,
            },
        )
        .await
        .expect("start workflow")
        .exec_id
    };

    wait_for_state(&db_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    let _ = worker_handle.await;

    assert_eq!(
        count_user_records(&db_url).await,
        1,
        "exactly one user row — user write committed atomically with ActivityCompleted"
    );
    assert_eq!(
        count_activity_completed_events(&db_url, exec_id).await,
        1,
        "exactly one ActivityCompleted event in harvest_events"
    );
}

/// Error path: when the closure returns `Err`, user writes are rolled back.
/// The workflow fails (no retry policy configured on the activity).
#[tokio::test]
async fn transactional_activity_err_rolls_back_user_writes() {
    let _guard = DB_TEST_LOCK.lock().await;
    let (db_url, _container) = setup_db().await;
    let mut setup_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("connect");
    setup_conn
        .batch_execute(CREATE_USER_RECORDS)
        .await
        .expect("create user_records");

    let pool = make_pool(&db_url);
    let registry = Arc::new(HandlerRegistry::new(
        vec![failing_txn_workflow_info()],
        vec![insert_then_fail_txn_info()],
    ));
    let worker = make_worker(&db_url, registry);
    let worker_pool = pool.clone();
    let worker_handle = tokio::spawn({
        let w = worker.clone();
        async move { w.run(&worker_pool).await }
    });

    let exec_id = {
        let mut conn = pool.get().await.unwrap();
        start_or_load_workflow_execution(
            &mut conn,
            StartWorkflowParams {
                workflow_name: "failing_txn_workflow",
                workflow_id: &format!("txn-fail-{}", uuid::Uuid::new_v4()),
                exec_id: ExecutionId::new(),
                input: serde_json::json!(null),
                parent_id: None,
                queue_name: "default",
                execution_timeout: None,
                memo: None,
                search_attrs: None,
                reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                trace_context: None,
                max_execution_timeout_ceiling: None,
                concurrency_key: None,
                concurrency_limit: None,
                priority: Priority::default(),
                max_workflow_input_bytes: 0,
                start_at: None,
                delay: None,
                max_workflow_start_delay: None,

                owner: None,
                runbook_url: None,
                severity: None,
                context_headers: None,

                sla: None,
                schedule_id: None,
                scheduled_for: None,
                workflow_attempt: 1,
                workflow_retry_policy: None,
                retry_of_exec_id: None,
                max_workflow_attempts_ceiling: None,
                origin: None,
            },
        )
        .await
        .expect("start workflow")
        .exec_id
    };

    // The workflow will fail because the activity returns Err with no retry
    wait_for_state(&db_url, exec_id, "FAILED").await;
    worker.shutdown();
    let _ = worker_handle.await;

    assert_eq!(
        count_user_records(&db_url).await,
        0,
        "zero user rows — transactional rollback discarded the insert"
    );
}

/// `run_transactional` on a test context (no DB pool attached) returns a
/// descriptive error rather than panicking.
#[cfg(feature = "testing")]
#[tokio::test]
async fn run_transactional_without_pool_returns_descriptive_error() {
    let ctx = ActivityContext::new_test();
    let result: Result<(), String> = ctx
        .run_transactional(|_conn| Box::pin(async { Ok(()) }))
        .await;
    assert!(result.is_err(), "expected Err when no transactional pool");
    let msg = result.unwrap_err();
    assert!(
        msg.contains("transactional") || msg.contains("not supported"),
        "error message should describe the restriction; got: {msg}"
    );
}
