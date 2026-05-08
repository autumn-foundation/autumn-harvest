#![cfg(feature = "db")]

//! End-to-end integration tests using testcontainers for a real Postgres instance.
//!
//! These tests spin up a throwaway Postgres container per test, run the harvest
//! migration SQL via `with_init_sql`, and exercise the full store/queue/DLQ stack
//! against a real database.

use autumn_harvest::dlq::{self, NewDeadLetterEntry};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{
    HarvestTimer, NewWorkflowExecution, TaskQueueItem, WorkflowExecution,
};
use autumn_harvest::queue::{EnqueueParams, StickyHint, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_timers, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, DagCatalog, HarvestBuilder, HarvestError, Schedule, SchedulerMonitor,
    SchedulerRuntime, StartWorkflowParams, TimeoutType, WorkerConfig, WorkflowContext,
    WorkflowIdReusePolicy, WorkflowSchedule, cancel_workflow_execution, queue,
    register_workflow_schedules, start_or_load_workflow_execution, tick_once, timeout,
};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use scoped_futures::ScopedFutureExt;
use std::any::TypeId;
use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// The migration SQL embedded at compile time.
///
/// Combines the initial schema with every forward-compatible schema-addition
/// migration that ships in `migrations/`. The
/// `20260410010000_harvest_workflow_start_uniqueness` migration is
/// deliberately excluded because one test (see
/// `legacy_workflow_uniqueness_schema_can_be_upgraded_for_idempotent_starts`)
/// applies it on a legacy schema to verify the upgrade path.
const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
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
);

/// The minimal "legacy" migration set used by the upgrade-path regression
/// test. Excludes both the workflow-start uniqueness upgrade *and* the
/// continue-as-new migration so the test can drive the database through the
/// historical upgrade sequence: legacy -> uniqueness fix -> continue-as-new.
///
/// The `concurrency_key` migration is included because `enqueue` (called by
/// `start_or_load_workflow_execution`) writes the `concurrency_key` and
/// `concurrency_cap` columns that it added; without them the INSERT fails.
const LEGACY_INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
);

/// Start a Postgres container with the harvest schema applied and return
/// an `AsyncPgConnection` ready for use.
///
/// CRITICAL: the returned `ContainerAsync` must be held alive for the duration
/// of the test -- dropping it kills the container.
async fn setup_test_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    (conn, container)
}

/// Start a Postgres container with the harvest schema applied and return
/// the database URL plus the live container handle.
async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    (database_url, container)
}

async fn setup_blank_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    (database_url, container)
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn load_execution_from_url(database_url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for execution query");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("failed to reload workflow execution")
}

async fn load_task_from_url(database_url: &str, task_id: Uuid) -> TaskQueueItem {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for task query");
    harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("failed to reload task queue row")
}

async fn load_history_from_url(database_url: &str, exec_id: ExecutionId) -> store::EventHistory {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for history query");
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history failed")
}

async fn load_tasks_for_execution_from_url(
    database_url: &str,
    exec_id: ExecutionId,
) -> Vec<TaskQueueItem> {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for task list query");
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .order(harvest_task_queue::scheduled_at.asc())
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("failed to reload task queue rows")
}

async fn load_timers_for_execution_from_url(
    database_url: &str,
    exec_id: ExecutionId,
) -> Vec<HarvestTimer> {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for timer list query");
    harvest_timers::table
        .filter(harvest_timers::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_timers::fires_at.asc())
        .select(HarvestTimer::as_select())
        .load(&mut conn)
        .await
        .expect("failed to reload timer rows")
}

async fn load_child_executions_from_url(
    database_url: &str,
    parent_exec_id: ExecutionId,
) -> Vec<WorkflowExecution> {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for child execution query");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .order(harvest_workflow_executions::started_at.asc())
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("failed to reload child workflow executions")
}

/// Insert a minimal `harvest_workflow_executions` row and return its UUID.
async fn insert_workflow_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "e2e_test_workflow",
        workflow_id: "e2e-wf-001",
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({"test": true}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
    };

    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert workflow execution");

    exec_id
}

#[tokio::test]
async fn legacy_workflow_uniqueness_schema_can_be_upgraded_for_idempotent_starts() {
    let (database_url, _container) = setup_blank_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");
    // Build a "legacy" schema: the original initial migration whose
    // uniqueness key was `(workflow_id, run_id)` rather than the modern
    // `(workflow_name, workflow_id)`. The continue-as-new migration is
    // deliberately *not* applied yet so this test exercises the historical
    // upgrade path one step at a time.
    let legacy_init_sql = LEGACY_INIT_SQL.replacen(
        "UNIQUE (workflow_name, workflow_id)",
        "UNIQUE (workflow_id, run_id)",
        1,
    );
    conn.batch_execute(&legacy_init_sql)
        .await
        .expect("failed to apply legacy harvest schema");

    let request = StartWorkflowParams {
        workflow_name: "upgrade_test",
        workflow_id: "workflow-42",
        exec_id: autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0)),
        input: serde_json::json!({ "workflow_id": 42 }),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
        trace_context: None,
    };

    // On the legacy schema there is no `(workflow_name, workflow_id)`
    // uniqueness anywhere, so the first start succeeds — but the schema
    // alone does not yet enforce idempotency. The point of the upgrade
    // migration is to add that enforcement.
    let initial_start = start_or_load_workflow_execution(&mut conn, request.clone())
        .await
        .expect("first start should succeed even on legacy schema");
    assert!(
        initial_start.created,
        "first start should create a workflow execution row on the legacy schema",
    );

    let upgrade_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql");
    let upgrade_sql = std::fs::read_to_string(&upgrade_path).unwrap_or_else(|error| {
        panic!(
            "failed to read harvest upgrade migration at {}: {error}",
            upgrade_path.display()
        )
    });
    conn.batch_execute(&upgrade_sql)
        .await
        .expect("failed to apply harvest upgrade migration");

    // After the start-uniqueness upgrade, repeated starts must collapse onto
    // the originally-created row.
    let after_uniqueness = start_or_load_workflow_execution(&mut conn, request.clone())
        .await
        .expect("post-upgrade start should reuse the legacy row idempotently");
    assert!(
        !after_uniqueness.created,
        "post-upgrade start should not create a second row",
    );
    assert_eq!(
        initial_start.exec_id, after_uniqueness.exec_id,
        "post-upgrade start should resolve to the same execution as the legacy start",
    );

    // Apply the continue-as-new migration on top to make sure the partial
    // unique index it installs is compatible with the upgraded schema and
    // continues to enforce idempotent starts.
    let continue_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations/20260427000000_harvest_continue_as_new/up.sql");
    let continue_sql = std::fs::read_to_string(&continue_path).unwrap_or_else(|error| {
        panic!(
            "failed to read continue-as-new migration at {}: {error}",
            continue_path.display()
        )
    });
    conn.batch_execute(&continue_sql)
        .await
        .expect("failed to apply continue-as-new migration");

    let after_continue_as_new = start_or_load_workflow_execution(&mut conn, request)
        .await
        .expect("start should remain idempotent after the continue-as-new migration");
    assert!(
        !after_continue_as_new.created,
        "continue-as-new migration should not break idempotent starts",
    );
    assert_eq!(
        initial_start.exec_id, after_continue_as_new.exec_id,
        "idempotent starts should still resolve to the same execution after continue-as-new",
    );
}

async fn enqueue_started_workflow_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    workflow_input: serde_json::Value,
) {
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task failed");
}

fn build_runtime_worker(
    worker_id: &str,
    max_concurrent_workflows: usize,
    max_concurrent_activities: usize,
    registry: Arc<HandlerRegistry>,
) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: worker_id.to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows,
                max_concurrent_activities,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
            },
            registry,
        )
        .expect("worker should build"),
    )
}

fn spawn_test_worker(worker: Arc<Worker>, pool: DbPool) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        worker.run(&pool).await;
    })
}

async fn wait_for_execution_state(
    database_url: &str,
    exec_id: ExecutionId,
    expected_state: &str,
) -> WorkflowExecution {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let execution = load_execution_from_url(database_url, exec_id).await;
            if execution.state == expected_state {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow should reach expected state within timeout")
}

fn child_round_trip_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: parent_workflow_with_child,
            },
            WorkflowInfo {
                name: "child_echo_workflow",
                module: "integration_e2e",
                handler: child_echo_workflow,
            },
        ],
        vec![],
    ))
}

fn child_continue_as_new_rejection_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: parent_workflow_with_continue_as_new_child,
            },
            WorkflowInfo {
                name: "child_continue_as_new_workflow",
                module: "integration_e2e",
                handler: continue_as_new_workflow,
            },
        ],
        vec![],
    ))
}

fn echo_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(input) })
}

fn failing_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Err("workflow exploded on purpose".to_string()) })
}

fn workflow_with_activity<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("send_email", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

fn workflow_with_checkpointed_activity<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("checkpointed_import", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

fn send_email_activity<'a>(
    _ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let to = input
            .get("to")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        Ok(serde_json::json!({
            "sent": true,
            "to": to,
        }))
    })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ImportCheckpoint {
    next_offset: usize,
}

#[derive(Debug, Default)]
struct HeartbeatResumeStats {
    attempts: AtomicUsize,
    processed_steps: AtomicUsize,
    resume_offsets: Mutex<Vec<usize>>,
}

fn heartbeat_resume_state(
    stats: Arc<HeartbeatResumeStats>,
) -> autumn_harvest::context::SharedState {
    let mut shared_state_map = HashMap::new();
    shared_state_map.insert(
        TypeId::of::<Arc<HeartbeatResumeStats>>(),
        Box::new(stats) as Box<dyn std::any::Any + Send + Sync>,
    );
    Arc::new(shared_state_map)
}

fn checkpointed_import_activity<'a>(
    ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let total = input
            .get("total")
            .and_then(serde_json::Value::as_u64)
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(6);
        let fail_after = input
            .get("fail_after")
            .and_then(serde_json::Value::as_u64)
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(3);

        let checkpoint = ctx
            .heartbeat_details::<ImportCheckpoint>()
            .map_err(|e| e.to_string())?;
        let start = checkpoint.map_or(0, |details| details.next_offset);
        let stats = Arc::clone(
            ctx.state::<Arc<HeartbeatResumeStats>>()
                .expect("test stats should be registered"),
        );
        let attempt = stats.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        stats
            .resume_offsets
            .lock()
            .expect("resume offset lock poisoned")
            .push(start);

        for offset in start..total {
            stats.processed_steps.fetch_add(1, Ordering::SeqCst);
            let next_offset = offset + 1;
            ctx.heartbeat(ImportCheckpoint { next_offset })
                .await
                .map_err(|e| e.to_string())?;

            if attempt == 1 && next_offset == fail_after {
                tokio::time::sleep(Duration::from_millis(1_200)).await;
                return Err(format!("fail after checkpoint {next_offset}"));
            }
        }

        Ok(serde_json::json!({
            "attempt": attempt,
            "resumed_from": start,
            "processed_total": AtomicUsize::load(&stats.processed_steps, Ordering::SeqCst),
        }))
    })
}

fn workflow_with_timer<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("cooldown", 1).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "timer": "fired",
        }))
    })
}

fn workflow_with_slow_activity<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("slow_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

fn slow_activity<'a>(
    _ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        Ok(input)
    })
}

fn signal_waiting_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let payload = ctx
            .wait_for_signal("approve")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "approved_by": payload }))
    })
}

fn activity_then_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity_result = ctx
            .execute_activity_raw("send_email", input, "default")
            .await
            .map_err(|e| e.to_string())?;
        let signal_payload = ctx
            .wait_for_signal("approve")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "activity": activity_result,
            "signal": signal_payload,
        }))
    })
}

fn parent_workflow_with_child<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("child_echo_workflow", input)
            .await
            .map_err(|e| e.to_string())
    })
}

fn child_echo_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let value = input
            .get("value")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        Ok(serde_json::json!({
            "child": value,
        }))
    })
}

fn parent_workflow_with_continue_as_new_child<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("child_continue_as_new_workflow", input)
            .await
            .map_err(|e| e.to_string())
    })
}

/// First-generation handler used by the continue-as-new e2e test: it
/// branches on the input. When invoked with `{"phase": "init"}` it requests
/// continue-as-new with `{"phase": "next"}`. When invoked with the
/// post-continuation payload it returns it directly.
fn continue_as_new_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let phase = input
            .get("phase")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if phase == "init" {
            let _ = ctx
                .continue_as_new(serde_json::json!({"phase": "next", "ran_init": true}))
                .await;
            unreachable!("continue_as_new must not resolve");
        }
        Ok(input)
    })
}

fn workflow_with_builder_state<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity_output = ctx
            .execute_activity_raw("stateful_activity", input.clone(), "default")
            .await
            .map_err(|error| error.to_string())?;
        let workflow_prefix = ctx
            .state::<String>()
            .cloned()
            .ok_or_else(|| "workflow missing shared state".to_string())?;

        Ok(serde_json::json!({
            "workflow_prefix": workflow_prefix,
            "activity": activity_output,
        }))
    })
}

fn stateful_activity<'a>(
    ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity_prefix = ctx
            .state::<String>()
            .cloned()
            .ok_or_else(|| "activity missing shared state".to_string())?;

        Ok(serde_json::json!({
            "activity_prefix": activity_prefix,
            "payload": input,
        }))
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_workflow_lifecycle() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    // 1. Append WorkflowStarted event
    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({"user": "alice"}),
        timestamp: Utc::now(),
    }];
    let inserted = store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");
    assert_eq!(inserted, 1);

    // 2. Load history -- verify 1 event
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history failed");
    assert_eq!(history.events.len(), 1);
    assert!(matches!(
        history.events[0],
        WorkflowEvent::WorkflowStarted { .. }
    ));
    assert_eq!(history.next_event_id, 1);

    // 3. Enqueue an activity task
    //    Set scheduled_at slightly in the past to avoid clock skew between
    //    the host (where Utc::now() runs) and the Docker container (where
    //    Postgres NOW() runs).
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Activity,
        serde_json::json!({"to": "bob@example.com"}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("send_email".into());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue failed");

    // 4. Claim the task
    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "worker-e2e-1", "")
        .await
        .expect("claim_task failed");
    let claimed = claimed.expect("no task claimed");
    assert_eq!(claimed.id, task_id);
    assert_eq!(claimed.activity_name.as_deref(), Some("send_email"));
    assert_eq!(claimed.state, "RUNNING");

    // 5. Complete the task
    queue::complete_task(&mut conn, task_id, serde_json::json!({"sent": true}))
        .await
        .expect("complete_task failed");

    // 6. Append activity completion + workflow completion events
    let activity_id = ActivityExecId::new();
    let completion_events = vec![
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "send_email".into(),
            input: serde_json::json!({"to": "bob@example.com"}),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!({"sent": true}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"status": "ok"}),
        },
    ];
    let inserted = store::append_events(&mut conn, exec_id, &completion_events, 1)
        .await
        .expect("append completion events failed");
    assert_eq!(inserted, 3);

    // 7. Load final history -- verify 4 events total
    //    (Started + ActivityScheduled + ActivityCompleted + WorkflowCompleted)
    let final_history = store::load_history(&mut conn, exec_id)
        .await
        .expect("final load_history failed");
    assert_eq!(final_history.events.len(), 4);
    assert!(matches!(
        final_history.events[0],
        WorkflowEvent::WorkflowStarted { .. }
    ));
    assert!(matches!(
        final_history.events[1],
        WorkflowEvent::ActivityScheduled { .. }
    ));
    assert!(matches!(
        final_history.events[2],
        WorkflowEvent::ActivityCompleted { .. }
    ));
    assert!(matches!(
        final_history.events[3],
        WorkflowEvent::WorkflowCompleted { .. }
    ));
    assert_eq!(final_history.next_event_id, 4);

    // 8. Verify the completed task in the queue has COMPLETED state
    let task: Vec<autumn_harvest::models::TaskQueueItem> = harvest_task_queue::table
        .filter(harvest_task_queue::id.eq(task_id))
        .load(&mut conn)
        .await
        .expect("failed to query task");
    assert_eq!(task.len(), 1);
    assert_eq!(task[0].state, "COMPLETED");
}

#[tokio::test]
async fn claim_task_returns_none_on_empty_queue() {
    let (mut conn, _container) = setup_test_db().await;

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "worker-empty-1", "")
        .await
        .expect("claim_task failed");
    assert!(
        claimed.is_none(),
        "expected None from empty queue, got {claimed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_completes_workflow_task_and_persists_result() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"status": "ok"});

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: workflow_input.clone(),
        timestamp: Utc::now(),
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: echo_workflow,
        }],
        vec![],
    ));
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "worker-e2e-complete".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
            },
            registry,
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let completed_execution = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;

            if execution.state == "COMPLETED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution =
        completed_execution.expect("worker should complete workflow task within timeout");
    assert_eq!(execution.output, Some(workflow_input.clone()));
    assert!(execution.completed_at.is_some());

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.last(),
        Some(WorkflowEvent::WorkflowCompleted { output }) if *output == workflow_input
    ));

    let task = load_task_from_url(&database_url, task_id).await;
    assert_eq!(task.state, "COMPLETED");
    assert_eq!(task.output, Some(workflow_input));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_marks_workflow_failed_when_handler_errors() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"status": "boom"});

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: workflow_input.clone(),
        timestamp: Utc::now(),
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: failing_workflow,
        }],
        vec![],
    ));
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "worker-e2e-fail".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
            },
            registry,
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let failed_execution = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;

            if execution.state == "FAILED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution = failed_execution.expect("worker should fail workflow task within timeout");
    assert_eq!(execution.state, "FAILED");
    assert!(
        execution
            .error
            .as_deref()
            .is_some_and(|e| e.contains("workflow exploded"))
    );
    assert!(execution.completed_at.is_some());

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.last(),
        Some(WorkflowEvent::WorkflowFailed { error }) if error.contains("workflow exploded")
    ));

    let task = load_task_from_url(&database_url, task_id).await;
    assert_eq!(task.state, "FAILED");
    assert!(
        task.error
            .as_deref()
            .is_some_and(|e| e.contains("workflow exploded"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_completes_workflow_with_activity_round_trip() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"to": "alice@example.com"});
    let activity_output = serde_json::json!({
        "sent": true,
        "to": "alice@example.com",
    });

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: workflow_input.clone(),
        timestamp: Utc::now(),
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let workflow_task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: workflow_with_activity,
        }],
        vec![ActivityInfo {
            name: "send_email",
            module: "integration_e2e",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            handler: send_email_activity,
        }],
    ));
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "worker-e2e-activity-round-trip".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
            },
            registry,
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let completed_execution = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;
            if execution.state == "COMPLETED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution =
        completed_execution.expect("worker should complete workflow-with-activity within timeout");
    assert_eq!(execution.output, Some(activity_output.clone()));

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::ActivityScheduled { .. },
            WorkflowEvent::ActivityStarted { .. },
            WorkflowEvent::ActivityCompleted { .. },
            WorkflowEvent::WorkflowCompleted { .. },
        ]
    ));

    let tasks = load_tasks_for_execution_from_url(&database_url, exec_id).await;
    assert_eq!(tasks.len(), 2, "workflow + activity task rows should exist");
    assert!(tasks.iter().all(|task| task.state == "COMPLETED"));
    assert!(tasks.iter().any(|task| task.id == workflow_task_id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activity_retry_resumes_from_persisted_heartbeat_details() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({
        "total": 6,
        "fail_after": 3,
    });
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let stats = Arc::new(HeartbeatResumeStats::default());
    let registry = Arc::new(HandlerRegistry::with_state(
        vec![WorkflowInfo {
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: workflow_with_checkpointed_activity,
        }],
        vec![ActivityInfo {
            name: "checkpointed_import",
            module: "integration_e2e",
            default_retry_policy: Some(autumn_harvest::RetryPolicy::fixed(
                2,
                Duration::from_millis(10),
            )),
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            handler: checkpointed_import_activity,
        }],
        heartbeat_resume_state(Arc::clone(&stats)),
    ));
    let worker = build_runtime_worker("worker-heartbeat-resume", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let execution = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        execution.output,
        Some(serde_json::json!({
            "attempt": 2,
            "resumed_from": 3,
            "processed_total": 6,
        }))
    );
    assert_eq!(AtomicUsize::load(&stats.attempts, Ordering::SeqCst), 2);
    assert_eq!(
        AtomicUsize::load(&stats.processed_steps, Ordering::SeqCst),
        6
    );
    assert_eq!(
        *stats
            .resume_offsets
            .lock()
            .expect("resume offset lock poisoned"),
        vec![0, 3],
        "second attempt must start from the checkpoint persisted by the first attempt",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_fails_orphaned_activity_task_without_scheduled_event() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_input = serde_json::json!({"step": "send_email"});

    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({"workflow": "activity-only"}),
        timestamp: Utc::now(),
    }];
    store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Activity, activity_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("send_email".to_string());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue activity task failed");

    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "worker-e2e-activity-orphaned".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
            },
            Arc::new(HandlerRegistry::new(
                vec![],
                vec![ActivityInfo {
                    name: "send_email",
                    module: "integration_e2e",
                    default_retry_policy: None,
                    default_start_to_close: None,
                    default_heartbeat_timeout: None,
                    default_schedule_to_start: None,
                    default_queue: Some("default"),
                    max_concurrent: None,
                    concurrency_key: None,
                    is_local: false,
                    handler: send_email_activity,
                }],
            )),
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let failed_task = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let task = load_task_from_url(&database_url, task_id).await;

            if task.state == "FAILED" {
                break task;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let task = failed_task.expect("worker should fail orphaned activity task within timeout");
    assert_eq!(task.state, "FAILED");
    assert!(
        task.error
            .as_deref()
            .is_some_and(|e| e.contains("no pending scheduled activity"))
    );

    let execution = load_execution_from_url(&database_url, exec_id).await;
    assert_eq!(execution.state, "FAILED");
    assert!(
        execution
            .error
            .as_deref()
            .is_some_and(|e| e.contains("no pending scheduled activity"))
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.last(),
        Some(WorkflowEvent::WorkflowFailed { error })
            if error.contains("no pending scheduled activity")
    ));
}

#[tokio::test]
async fn timeout_enforcement_fails_pending_activity_and_wakes_workflow() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();

    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({"timeout": "schedule_to_start"}),
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"to": "alice@example.com"}),
                queue: "stuck-queue".into(),
            },
        ],
        0,
    )
    .await
    .expect("append initial history failed");

    let mut workflow_params = EnqueueParams::new(
        "default",
        TaskType::Workflow,
        serde_json::json!({"workflow": true}),
    );
    workflow_params.workflow_exec_id = Some(exec_id.as_uuid());
    workflow_params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    let workflow_task_id = queue::enqueue(&mut conn, &workflow_params)
        .await
        .expect("enqueue parked workflow task failed");

    let default_queues = vec!["default".to_string()];
    let claimed_workflow = queue::claim_task(&mut conn, &default_queues, "parked-worker", "")
        .await
        .expect("claim parked workflow task failed")
        .expect("workflow task should be claimable");
    assert_eq!(claimed_workflow.id, workflow_task_id);
    assert_eq!(claimed_workflow.state, "RUNNING");
    queue::park_workflow_task(&mut conn, workflow_task_id, None)
        .await
        .expect("park workflow task failed");

    let mut activity_params = EnqueueParams::new(
        "stuck-queue",
        TaskType::Activity,
        serde_json::json!({"to": "alice@example.com"}),
    );
    activity_params.workflow_exec_id = Some(exec_id.as_uuid());
    activity_params.activity_name = Some("send_email".to_string());
    activity_params.schedule_to_start = Some(chrono::Duration::milliseconds(50));
    activity_params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    let activity_task_id = queue::enqueue(&mut conn, &activity_params)
        .await
        .expect("enqueue timed-out activity task failed");

    let enforced = timeout::enforce_timeouts_once(&mut conn)
        .await
        .expect("timeout enforcement should succeed");
    assert_eq!(enforced, 1);

    let workflow_task = load_task_from_url(&database_url, workflow_task_id).await;
    assert_eq!(workflow_task.state, "PENDING");

    let activity_task = load_task_from_url(&database_url, activity_task_id).await;
    assert_eq!(activity_task.state, "FAILED");
    assert!(activity_task.error.as_deref().is_some_and(|error| {
        error.contains("ScheduleToStart") && error.contains("send_email")
    }));

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history after timeout enforcement failed");
    assert!(matches!(
        history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::ActivityScheduled { .. },
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::ScheduleToStart,
                ..
            },
        ]
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_fails_workflow_when_activity_start_to_close_timeout_elapses() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"slow": true});

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "worker-e2e-activity-timeout".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
            },
            Arc::new(HandlerRegistry::new(
                vec![WorkflowInfo {
                    name: "e2e_test_workflow",
                    module: "integration_e2e",
                    handler: workflow_with_slow_activity,
                }],
                vec![ActivityInfo {
                    name: "slow_activity",
                    module: "integration_e2e",
                    default_retry_policy: None,
                    default_start_to_close: Some(Duration::from_millis(50)),
                    default_heartbeat_timeout: None,
                    default_schedule_to_start: None,
                    default_queue: Some("default"),
                    max_concurrent: None,
                    concurrency_key: None,
                    is_local: false,
                    handler: slow_activity,
                }],
            )),
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let failed_execution = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;
            if execution.state == "FAILED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let execution = failed_execution.expect("worker should fail timed-out workflow within timeout");
    assert_eq!(execution.state, "FAILED");
    assert!(execution.error.as_deref().is_some_and(|error| {
        error.contains("StartToClose") && error.contains("slow_activity")
    }));

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::ActivityScheduled { .. },
            WorkflowEvent::ActivityStarted { .. },
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::StartToClose,
                ..
            },
            WorkflowEvent::WorkflowFailed { .. },
        ]
    ));

    let tasks = load_tasks_for_execution_from_url(&database_url, exec_id).await;
    assert_eq!(tasks.len(), 2);
    assert!(tasks.iter().all(|task| task.state == "FAILED"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_completes_workflow_with_timer_round_trip() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"timer": true});

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "worker-e2e-timer-round-trip".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
            },
            Arc::new(HandlerRegistry::new(
                vec![WorkflowInfo {
                    name: "e2e_test_workflow",
                    module: "integration_e2e",
                    handler: workflow_with_timer,
                }],
                vec![],
            )),
        )
        .expect("worker should build"),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let completed_execution = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;
            if execution.state == "COMPLETED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution =
        completed_execution.expect("worker should complete workflow-with-timer within timeout");
    assert_eq!(
        execution.output,
        Some(serde_json::json!({"timer": "fired"}))
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(matches!(
        history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::TimerStarted { .. },
            WorkflowEvent::TimerFired { .. },
            WorkflowEvent::WorkflowCompleted { .. },
        ]
    ));

    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert_eq!(timers.len(), 1, "a durable timer row should be created");
    assert!(timers[0].fired, "timer should be marked fired once resumed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_completes_parent_workflow_after_child_workflow_round_trip() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"value": "from-parent"});
    enqueue_started_workflow_task(&mut conn, parent_exec_id, workflow_input).await;

    let worker = build_runtime_worker(
        "worker-e2e-child-round-trip",
        2,
        1,
        child_round_trip_registry(),
    );
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent_execution =
        wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        parent_execution.output,
        Some(serde_json::json!({"child": "from-parent"}))
    );

    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    assert!(matches!(
        parent_history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::ChildWorkflowStarted { .. },
            WorkflowEvent::ChildWorkflowCompleted { .. },
            WorkflowEvent::WorkflowCompleted { .. },
        ]
    ));

    let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(
        child_execs.len(),
        1,
        "exactly one child execution should be created"
    );
    let child_execution = &child_execs[0];
    assert_eq!(child_execution.workflow_name, "child_echo_workflow");
    assert_eq!(
        child_execution.output,
        Some(serde_json::json!({"child": "from-parent"}))
    );

    let child_history = load_history_from_url(
        &database_url,
        child_execution
            .id
            .to_string()
            .parse()
            .expect("child execution id should parse"),
    )
    .await;
    assert!(matches!(
        child_history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::WorkflowCompleted { .. },
        ]
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_continue_as_new_rejection_wakes_parent_with_child_failure() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"phase": "init"});
    enqueue_started_workflow_task(&mut conn, parent_exec_id, workflow_input).await;

    let worker = build_runtime_worker(
        "worker-e2e-child-continue-reject",
        2,
        1,
        child_continue_as_new_rejection_registry(),
    );
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent_execution = wait_for_execution_state(&database_url, parent_exec_id, "FAILED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    assert!(
        matches!(
            parent_history.events.as_slice(),
            [
                WorkflowEvent::WorkflowStarted { .. },
                WorkflowEvent::ChildWorkflowStarted { .. },
                WorkflowEvent::ChildWorkflowFailed { .. },
                WorkflowEvent::WorkflowFailed { .. },
            ]
        ),
        "parent should observe a terminal child failure instead of staying parked: {:?}",
        parent_history.events
    );
    let child_failure = parent_history
        .events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ChildWorkflowFailed { child_id, error } => {
                Some((*child_id, error.clone()))
            }
            _ => None,
        })
        .expect("parent history should include ChildWorkflowFailed");
    assert!(
        child_failure
            .1
            .contains("continue_as_new is not supported in child workflows"),
        "parent should receive the rejection reason from the child"
    );

    let child_execution = load_execution_from_url(&database_url, child_failure.0).await;
    assert_eq!(child_execution.state, "FAILED");
    assert_eq!(
        child_execution.error.as_deref(),
        Some("continue_as_new is not supported in child workflows in this release"),
    );

    let child_history = load_history_from_url(&database_url, child_failure.0).await;
    assert!(
        matches!(
            child_history.events.as_slice(),
            [
                WorkflowEvent::WorkflowStarted { .. },
                WorkflowEvent::WorkflowFailed { .. },
            ]
        ),
        "child should fail cleanly after the rejected continue-as-new: {:?}",
        child_history.events
    );

    assert!(
        parent_execution.error.as_deref().is_some_and(
            |error| error.contains("continue_as_new is not supported in child workflows")
        ),
        "parent should fail with the propagated child error",
    );
}

// ── Parallel child workflow handler functions ────────────────────────────────

/// Parent that spawns two children concurrently via `tokio::join!` and returns
/// a merged result.
fn parent_workflow_parallel_children<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let (a, b) = tokio::join!(
            ctx.spawn_child_workflow_raw("child_alpha", serde_json::json!({"item": "alpha"})),
            ctx.spawn_child_workflow_raw("child_beta", serde_json::json!({"item": "beta"})),
        );
        let a = a.map_err(|e| e.to_string())?;
        let b = b.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"alpha": a, "beta": b}))
    })
}

fn child_alpha_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        Ok(serde_json::json!({"result": input.get("item").and_then(|v| v.as_str()).unwrap_or("?")}))
    })
}

fn child_beta_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        Ok(serde_json::json!({"result": input.get("item").and_then(|v| v.as_str()).unwrap_or("?")}))
    })
}

fn parallel_children_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                name: "e2e_test_workflow",
                module: "integration_e2e",
                handler: parent_workflow_parallel_children,
            },
            WorkflowInfo {
                name: "child_alpha",
                module: "integration_e2e",
                handler: child_alpha_workflow,
            },
            WorkflowInfo {
                name: "child_beta",
                module: "integration_e2e",
                handler: child_beta_workflow,
            },
        ],
        vec![],
    ))
}

/// RED test: parent spawns two child workflows in parallel via `tokio::join!`.
///
/// Both children must complete and the parent must produce a merged result
/// containing both outputs.  With the current single-child dispatch the
/// worker fails because it cannot handle two simultaneous `StartChildWorkflow`
/// commands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_completes_parent_workflow_with_parallel_child_workflows() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({})).await;

    let worker = build_runtime_worker(
        "worker-e2e-parallel-children",
        4,
        2,
        parallel_children_registry(),
    );
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent_execution =
        wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        parent_execution.output,
        Some(serde_json::json!({
            "alpha": {"result": "alpha"},
            "beta":  {"result": "beta"},
        })),
        "parent output must contain merged results from both children"
    );

    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    let child_started_count = parent_history
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowStarted { .. }))
        .count();
    let child_completed_count = parent_history
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowCompleted { .. }))
        .count();
    assert_eq!(
        child_started_count, 2,
        "parent history must record both child starts"
    );
    assert_eq!(
        child_completed_count, 2,
        "parent history must record both child completions"
    );

    let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(
        child_execs.len(),
        2,
        "exactly two child executions must be stored with parent_id set"
    );
    for child_exec in &child_execs {
        assert_eq!(
            child_exec.state, "COMPLETED",
            "each child execution must be COMPLETED"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_builder_state_is_visible_to_workflow_and_activity() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"job": "shared-state"});

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let built = HarvestBuilder::new()
        .workflows(vec![WorkflowInfo {
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: workflow_with_builder_state,
        }])
        .activities(vec![ActivityInfo {
            name: "stateful_activity",
            module: "integration_e2e",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            handler: stateful_activity,
        }])
        .state(String::from("haunted"))
        .worker(WorkerConfig::default())
        .build();
    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-e2e-builder-state".to_string();
    runtime_config.poll_interval = Duration::from_millis(25);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();

    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let completed_execution = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let execution = load_execution_from_url(&database_url, exec_id).await;
            if execution.state == "COMPLETED" {
                break execution;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let execution =
        completed_execution.expect("worker should complete shared-state workflow within timeout");
    assert_eq!(
        execution.output,
        Some(serde_json::json!({
            "workflow_prefix": "haunted",
            "activity": {
                "activity_prefix": "haunted",
                "payload": workflow_input,
            }
        }))
    );
}

#[tokio::test]
async fn queue_listener_receives_enqueue_notification() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let queues = vec!["default".to_string()];
    let mut listener = autumn_harvest::notify::QueueListener::connect(&database_url, &queues)
        .await
        .expect("listener should connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Workflow,
        serde_json::json!({"notify": true}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");

    let notification = listener
        .wait_for_notification(Duration::from_secs(2))
        .await
        .expect("listener wait should succeed")
        .expect("listener should receive a notification");

    assert_eq!(notification.task_id, task_id);
}

#[tokio::test]
async fn queue_listener_handles_quoted_queue_names() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let queue_name = "priority\"queue";
    let queues = vec![queue_name.to_string()];
    let mut listener = autumn_harvest::notify::QueueListener::connect(&database_url, &queues)
        .await
        .expect("listener should connect for quoted queue names");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let mut params = EnqueueParams::new(
        queue_name,
        TaskType::Workflow,
        serde_json::json!({"notify": "quoted"}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed for quoted queue names");

    let notification = listener
        .wait_for_notification(Duration::from_secs(2))
        .await
        .expect("listener wait should succeed")
        .expect("listener should receive a notification");

    assert_eq!(notification.task_id, task_id);
}

#[tokio::test]
async fn wake_workflow_task_emits_notification() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Workflow,
        serde_json::json!({"wake": true}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "wake-test-worker", "")
        .await
        .expect("claim should succeed")
        .expect("workflow task should be claimable");
    assert_eq!(claimed.id, task_id);
    queue::park_workflow_task(&mut conn, task_id, None)
        .await
        .expect("park workflow task should succeed");

    let mut listener = autumn_harvest::notify::QueueListener::connect(&database_url, &queues)
        .await
        .expect("listener should connect");

    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake_workflow_task should succeed");

    let notification = listener
        .wait_for_notification(Duration::from_secs(2))
        .await
        .expect("listener wait should succeed")
        .expect("listener should receive a wake notification");

    assert_eq!(notification.task_id, Uuid::nil());
}

#[tokio::test]
async fn wake_workflow_task_does_not_requeue_active_running_task() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Workflow,
        serde_json::json!({"wake": false}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");
    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "active-worker", "")
        .await
        .expect("claim should succeed")
        .expect("workflow task should be claimable");
    assert_eq!(claimed.id, task_id);
    assert_eq!(claimed.state, "RUNNING");
    assert!(claimed.worker_id.is_some());
    assert!(claimed.started_at.is_some());

    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake_workflow_task should succeed");

    let task = load_task_from_url(&database_url, task_id).await;
    assert_eq!(task.state, "RUNNING");
    assert_eq!(task.worker_id.as_deref(), Some("active-worker"));
    assert!(task.started_at.is_some());
}

#[tokio::test]
async fn reschedule_task_clears_stale_heartbeat_timestamp() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Activity,
        serde_json::json!({"retry": true}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("flaky_step".to_string());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");
    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "retry-worker", "")
        .await
        .expect("claim should succeed")
        .expect("activity task should be claimable");
    assert_eq!(claimed.id, task_id);

    let checkpoint = serde_json::json!({"next_offset": 7});
    queue::record_heartbeat(&mut conn, task_id, checkpoint.clone())
        .await
        .expect("record heartbeat should succeed");
    let heartbeating = load_task_from_url(&database_url, task_id).await;
    assert!(
        heartbeating.last_heartbeat_at.is_some(),
        "heartbeat should be recorded before reschedule"
    );
    assert_eq!(
        heartbeating.heartbeat_details,
        Some(checkpoint.clone()),
        "heartbeat payload should be recorded before reschedule"
    );

    queue::reschedule_task(
        &mut conn,
        task_id,
        Utc::now() + chrono::Duration::seconds(30),
    )
    .await
    .expect("reschedule_task should succeed");

    let task = load_task_from_url(&database_url, task_id).await;
    assert_eq!(task.state, "PENDING");
    assert!(task.worker_id.is_none());
    assert!(task.started_at.is_none());
    assert!(
        task.last_heartbeat_at.is_none(),
        "rescheduling should clear stale heartbeat timestamps"
    );
    assert_eq!(
        task.heartbeat_details,
        Some(checkpoint),
        "rescheduling should preserve checkpoint payload for the retry attempt"
    );
}

#[tokio::test]
async fn enqueue_inside_transaction_emits_notification_on_commit() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let queues = vec!["default".to_string()];
    let mut listener = autumn_harvest::notify::QueueListener::connect(&database_url, &queues)
        .await
        .expect("listener should connect");

    let mut params = EnqueueParams::new(
        "default",
        TaskType::Activity,
        serde_json::json!({"tx": true}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("send_email".to_string());

    let task_id = conn
        .transaction::<Uuid, HarvestError, _>(|conn| {
            let params = params.clone();
            async move { queue::enqueue(conn, &params).await }.scope_boxed()
        })
        .await
        .expect("transactional enqueue should succeed");

    let notification = listener
        .wait_for_notification(Duration::from_secs(2))
        .await
        .expect("listener wait should succeed")
        .expect("listener should receive transactional enqueue notification");

    assert_eq!(notification.task_id, task_id);
}

#[tokio::test]
async fn dead_letter_queue_lifecycle() {
    let (mut conn, _container) = setup_test_db().await;

    // Verify DLQ starts empty
    let initial_count = dlq::dead_letter_count(&mut conn)
        .await
        .expect("dead_letter_count failed");
    assert_eq!(initial_count, 0);

    // Insert a dead letter entry
    let entry = NewDeadLetterEntry {
        original_task_id: Uuid::new_v4(),
        queue_name: "default".into(),
        task_type: "ACTIVITY".into(),
        workflow_exec_id: None,
        activity_name: Some("flaky_step".into()),
        input: serde_json::json!({"attempt": 3}),
        error: "SMTP connection refused after 3 retries".into(),
        attempts: 3,
    };

    let dlq_id = dlq::dead_letter(&mut conn, &entry)
        .await
        .expect("dead_letter insert failed");
    assert!(!dlq_id.is_nil(), "DLQ entry should have a valid UUID");

    // Verify count is now 1
    let count = dlq::dead_letter_count(&mut conn)
        .await
        .expect("dead_letter_count failed");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn event_store_round_trip() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let activity_id_1 = ActivityExecId::new();
    let activity_id_2 = ActivityExecId::new();

    // Append 3 events in one batch
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"batch": "round_trip"}),
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: activity_id_1,
            name: "step_1".into(),
            input: serde_json::json!(1),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: activity_id_1,
            output: serde_json::json!({"result": "done"}),
        },
    ];

    let inserted = store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("append failed");
    assert_eq!(inserted, 3);

    // Load and verify count
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history failed");
    assert_eq!(history.events.len(), 3);
    assert_eq!(history.next_event_id, 3);

    // Verify deserialization fidelity
    assert!(matches!(
        history.events[0],
        WorkflowEvent::WorkflowStarted { .. }
    ));
    if let WorkflowEvent::WorkflowStarted { ref input, .. } = history.events[0] {
        assert_eq!(input, &serde_json::json!({"batch": "round_trip"}));
    }

    assert!(matches!(
        history.events[1],
        WorkflowEvent::ActivityScheduled { .. }
    ));
    if let WorkflowEvent::ActivityScheduled { ref name, .. } = history.events[1] {
        assert_eq!(name, "step_1");
    }

    assert!(matches!(
        history.events[2],
        WorkflowEvent::ActivityCompleted { .. }
    ));
    if let WorkflowEvent::ActivityCompleted { ref output, .. } = history.events[2] {
        assert_eq!(output, &serde_json::json!({"result": "done"}));
    }

    // Append more events and verify continuity
    let more_events = vec![
        WorkflowEvent::ActivityScheduled {
            activity_id: activity_id_2,
            name: "step_2".into(),
            input: serde_json::json!(2),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: activity_id_2,
            output: serde_json::json!({"result": "also done"}),
        },
    ];

    let inserted = store::append_events(&mut conn, exec_id, &more_events, 3)
        .await
        .expect("second append failed");
    assert_eq!(inserted, 2);

    let full_history = store::load_history(&mut conn, exec_id)
        .await
        .expect("full load_history failed");
    assert_eq!(full_history.events.len(), 5);
    assert_eq!(full_history.next_event_id, 5);
}

/// Worker delivers a signal to a waiting workflow and the workflow completes.
///
/// This tests the full signal delivery path:
/// 1. Workflow runs, hits `wait_for_signal` → no signal yet → requeued
/// 2. Signal is written to the `harvest_signals` table
/// 3. Worker retries the task, ingests the pending signal, replays the workflow
/// 4. `wait_for_signal` replays with the ingested signal → workflow completes
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_completes_workflow_after_signal_delivery() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: signal_waiting_workflow,
        }],
        vec![],
    ));
    let worker = build_runtime_worker("worker-e2e-signal-wait", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // Let the worker pick up the task and reach the signal-wait state.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Deliver the signal.
    autumn_harvest::signal::send_signal(
        &mut conn,
        exec_id,
        "approve",
        serde_json::json!({"user": "alice"}),
    )
    .await
    .expect("send_signal should succeed");

    // Wake the parked task immediately so the test doesn't wait for the 1 s
    // requeue delay.
    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake_workflow_task should succeed");

    let execution = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        execution.output,
        Some(serde_json::json!({"approved_by": {"user": "alice"}}))
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        matches!(
            history.events.as_slice(),
            [
                WorkflowEvent::WorkflowStarted { .. },
                WorkflowEvent::SignalReceived { .. },
                WorkflowEvent::WorkflowCompleted { .. },
            ]
        ),
        "unexpected history: {:?}",
        history.events
    );
}

/// Signal arrives before the workflow's activity is scheduled.
///
/// `ingest_pending_signals` can append a `SignalReceived` event at the very
/// start of history (right after `WorkflowStarted`) if the signal arrives
/// while the workflow task is queued but not yet running.  The replay engine
/// must skip those early signals when looking for `ActivityScheduled` and
/// then deliver them when the workflow later calls `wait_for_signal`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_handles_early_ingested_signal_before_activity() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let exec_id = insert_workflow_execution(&mut conn).await;
    let workflow_input = serde_json::json!({"to": "bob@example.com"});

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    // Insert the signal into history BEFORE the workflow runs its activity,
    // simulating the race where a signal arrives right after the workflow
    // task is enqueued but before the worker picks it up.
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::SignalReceived {
            signal_name: "approve".into(),
            payload: serde_json::json!({"early": true}),
        }],
        1,
    )
    .await
    .expect("append early SignalReceived failed");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);

    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue workflow task failed");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: activity_then_signal_workflow,
        }],
        vec![ActivityInfo {
            name: "send_email",
            module: "integration_e2e",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            handler: send_email_activity,
        }],
    ));
    let worker = build_runtime_worker("worker-e2e-early-signal", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let execution = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let expected_activity_output = serde_json::json!({
        "sent": true,
        "to": "bob@example.com",
    });
    assert_eq!(
        execution.output,
        Some(serde_json::json!({
            "activity": expected_activity_output,
            "signal": {"early": true},
        }))
    );
}

#[tokio::test]
async fn duplicate_event_id_is_rejected() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let events = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: Utc::now(),
    }];

    // First insert succeeds
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("first append should succeed");

    // Second insert with same start_id should fail (unique constraint)
    let result = store::append_events(&mut conn, exec_id, &events, 0).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HarvestError::Database(_)));
}

// ---------------------------------------------------------------------------
// Sticky cross-worker routing tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn enqueue_with_sticky_pin_stores_worker_and_expiry() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let params = EnqueueParams::new(
        "default",
        TaskType::Workflow,
        serde_json::json!({"go": true}),
    )
    .with_sticky("worker-sticky-1", Duration::from_secs(3));
    let mut enqueue = params.clone();
    enqueue.workflow_exec_id = Some(exec_id.as_uuid());

    let task_id = queue::enqueue(&mut conn, &enqueue)
        .await
        .expect("enqueue should succeed");

    let row = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("row should exist");

    assert_eq!(row.sticky_worker_id.as_deref(), Some("worker-sticky-1"));
    assert!(row.sticky_until.is_some(), "sticky_until should be set");
    let stored_timeout = row.sticky_timeout.expect("sticky_timeout should be set");
    assert_eq!(
        stored_timeout.num_seconds(),
        3,
        "sticky_timeout interval should round-trip as 3 seconds (got {stored_timeout})",
    );
}

async fn insert_named_workflow_execution(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "e2e_sticky_test",
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert sticky test workflow execution");
    exec_id
}

#[tokio::test]
async fn claim_task_prefers_sticky_worker_within_window() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_pinned = insert_named_workflow_execution(&mut conn, "pinned-1").await;
    let exec_free = insert_named_workflow_execution(&mut conn, "free-1").await;

    // Free task is higher priority AND enqueued first so it would ordinarily be
    // claimed ahead of the pinned task by any worker. Sticky routing must
    // reshuffle the order so the pinned worker sees its pinned row first.
    let mut free = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    free.priority = 10;
    free.workflow_exec_id = Some(exec_free.as_uuid());
    let free_id = queue::enqueue(&mut conn, &free)
        .await
        .expect("enqueue free task failed");

    let mut pinned = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}))
        .with_sticky("sticky-worker", Duration::from_secs(30));
    pinned.priority = 0;
    pinned.workflow_exec_id = Some(exec_pinned.as_uuid());
    let pinned_id = queue::enqueue(&mut conn, &pinned)
        .await
        .expect("enqueue pinned task failed");

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "sticky-worker", "")
        .await
        .expect("claim should succeed")
        .expect("sticky worker should get its pinned task");
    assert_eq!(
        claimed.id, pinned_id,
        "sticky worker should claim its pinned task ahead of the higher-priority free task",
    );

    let claimed_other = queue::claim_task(&mut conn, &queues, "other-worker", "")
        .await
        .expect("second claim should succeed")
        .expect("other worker should pick up the free task");
    assert_eq!(
        claimed_other.id, free_id,
        "other worker should still claim the unpinned free task",
    );
}

#[tokio::test]
async fn claim_task_excludes_other_workers_while_sticky_active() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let pinned = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}))
        .with_sticky("owner-worker", Duration::from_secs(30));
    let mut pinned = pinned;
    pinned.workflow_exec_id = Some(exec_id.as_uuid());
    queue::enqueue(&mut conn, &pinned)
        .await
        .expect("enqueue should succeed");

    let queues = vec!["default".to_string()];
    // Different worker must not steal a fresh sticky pin.
    let claimed = queue::claim_task(&mut conn, &queues, "interloper", "")
        .await
        .expect("claim should succeed");
    assert!(
        claimed.is_none(),
        "non-sticky worker should not claim a pinned task while sticky_until is in the future",
    );

    // The owner can still claim it.
    let owner_claim = queue::claim_task(&mut conn, &queues, "owner-worker", "")
        .await
        .expect("owner claim should succeed")
        .expect("owner should be able to claim its pinned task");
    assert_eq!(
        owner_claim.sticky_worker_id.as_deref(),
        Some("owner-worker")
    );
}

#[tokio::test]
async fn claim_task_falls_back_to_any_worker_after_sticky_expires() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    // Pin with a short window so we can observe fallback without sleeping long.
    // The sleep is generously larger than the window to tolerate DB/host clock
    // skew inside testcontainers on CI runners.
    let mut pinned = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}))
        .with_sticky("crashed-worker", Duration::from_millis(100));
    pinned.workflow_exec_id = Some(exec_id.as_uuid());
    queue::enqueue(&mut conn, &pinned)
        .await
        .expect("enqueue should succeed");

    // Allow the sticky window to elapse comfortably.
    tokio::time::sleep(Duration::from_millis(800)).await;

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "rescue-worker", "")
        .await
        .expect("claim should succeed")
        .expect("any worker may claim after sticky_until expires");
    assert_eq!(
        claimed.worker_id.as_deref(),
        Some("rescue-worker"),
        "fallback worker should own the row after expiry",
    );
}

#[tokio::test]
async fn claim_task_treats_expired_sticky_rows_like_unpinned_rows() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_expired = insert_named_workflow_execution(&mut conn, "expired-sticky-1").await;
    let exec_free = insert_named_workflow_execution(&mut conn, "free-after-expiry-1").await;

    let mut expired = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}))
        .with_sticky("offline-worker", Duration::from_secs(30));
    expired.priority = 0;
    expired.workflow_exec_id = Some(exec_expired.as_uuid());
    let expired_id = queue::enqueue(&mut conn, &expired)
        .await
        .expect("enqueue expired-sticky task failed");

    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET sticky_until = NOW() - INTERVAL '1 second' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(expired_id)
    .execute(&mut conn)
    .await
    .expect("failed to expire sticky window");

    let mut free = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    free.priority = 10;
    free.workflow_exec_id = Some(exec_free.as_uuid());
    let free_id = queue::enqueue(&mut conn, &free)
        .await
        .expect("enqueue free task failed");

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "rescue-worker", "")
        .await
        .expect("claim should succeed")
        .expect("one of the eligible tasks should be claimed");
    assert_eq!(
        claimed.id, free_id,
        "expired sticky rows should compete with unpinned rows by priority instead of jumping ahead",
    );
}

#[tokio::test]
async fn park_workflow_task_with_sticky_hint_pins_to_worker() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    // Seed and claim a workflow task so the row is in RUNNING state.
    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");
    let queues = vec!["default".to_string()];
    let _claimed = queue::claim_task(&mut conn, &queues, "park-worker", "")
        .await
        .expect("claim should succeed")
        .expect("row should be claimable");

    queue::park_workflow_task(
        &mut conn,
        task_id,
        Some(StickyHint::new("park-worker", Duration::from_secs(10))),
    )
    .await
    .expect("park with sticky should succeed");

    let row = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("row should exist");
    assert_eq!(row.state, "RUNNING", "parked row keeps RUNNING state");
    assert!(row.worker_id.is_none(), "worker ownership cleared on park");
    assert_eq!(row.sticky_worker_id.as_deref(), Some("park-worker"));
    assert!(row.sticky_until.is_some());
    let stored_timeout = row.sticky_timeout.expect("sticky_timeout should be set");
    assert_eq!(
        stored_timeout.num_seconds(),
        10,
        "sticky_timeout should round-trip as 10 seconds (got {stored_timeout})",
    );
}

#[tokio::test]
async fn wake_workflow_task_refreshes_sticky_until() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    // Seed, claim, and park a workflow task with a short sticky window.
    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue should succeed");
    let queues = vec!["default".to_string()];
    let _claimed = queue::claim_task(&mut conn, &queues, "wake-refresh-worker", "")
        .await
        .expect("claim should succeed");
    // Use a 5s window so both the park's sticky_until and the wake's refreshed
    // sticky_until land comfortably in the future even under DB/host clock skew
    // on CI runners. The test asserts the value was REFRESHED by comparing
    // before/after timestamps, not by waiting for expiry.
    queue::park_workflow_task(
        &mut conn,
        task_id,
        Some(StickyHint::new(
            "wake-refresh-worker",
            Duration::from_secs(5),
        )),
    )
    .await
    .expect("park should succeed");

    let parked_until = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("parked row should exist")
        .sticky_until
        .expect("sticky_until should be set at park");

    // Wait long enough that NOW() has moved noticeably (well above typical
    // sub-millisecond timer resolution) before triggering the refresh.
    tokio::time::sleep(Duration::from_millis(250)).await;

    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake should succeed");

    let row = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("row should exist");

    assert_eq!(row.state, "PENDING");
    assert_eq!(row.sticky_worker_id.as_deref(), Some("wake-refresh-worker"));
    let refreshed_until = row.sticky_until.expect("sticky_until should be refreshed");
    assert!(
        refreshed_until > parked_until,
        "sticky_until should be pushed forward on wake (parked_until={parked_until}, \
         refreshed_until={refreshed_until})",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_continues_as_new_with_fresh_history_and_same_workflow_id() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let original_exec_id = insert_workflow_execution(&mut conn).await;
    let initial_input = serde_json::json!({"phase": "init"});
    enqueue_started_workflow_task(&mut conn, original_exec_id, initial_input.clone()).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: continue_as_new_workflow,
        }],
        vec![],
    ));
    let worker = build_runtime_worker("worker-e2e-continue-as-new", 2, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // The original run is sealed by the worker as soon as it drains the
    // continue-as-new command. We wait for that terminal transition before
    // asserting on the chain.
    let sealed_execution =
        wait_for_execution_state(&database_url, original_exec_id, "CONTINUED_AS_NEW").await;

    // The successor run is identified by the WorkflowContinuedAsNew event
    // appended to the original run's history. Walking the history is the
    // public contract operators rely on; loading a fresh row by joining on
    // the parent_id chain would not work because continue-as-new
    // intentionally does NOT set parent_id (parent_id is reserved for child
    // workflows).
    let original_history = load_history_from_url(&database_url, original_exec_id).await;
    let new_exec_id = original_history
        .events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(*new_exec_id),
            _ => None,
        })
        .expect("original history should contain a WorkflowContinuedAsNew event");

    assert_ne!(
        new_exec_id, original_exec_id,
        "continue-as-new must mint a fresh ExecutionId"
    );

    // The new run completes on its second execution by returning the
    // post-continuation payload it was started with.
    let completed = wait_for_execution_state(&database_url, new_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    assert_eq!(
        sealed_execution.state, "CONTINUED_AS_NEW",
        "original run should be sealed in the continue-as-new terminal state"
    );
    assert_eq!(
        sealed_execution.workflow_id, completed.workflow_id,
        "logical workflow_id must be preserved across the continue-as-new transition"
    );
    assert_eq!(
        sealed_execution.workflow_name, completed.workflow_name,
        "workflow_name must be preserved across the continue-as-new transition"
    );
    assert_eq!(
        completed.output,
        Some(serde_json::json!({"phase": "next", "ran_init": true})),
        "the new run should observe the input passed to continue_as_new"
    );

    // The successor run starts with an empty history apart from
    // WorkflowStarted plus its own terminal completion event — this is the
    // whole point of continue-as-new, bounding history growth.
    let new_history = load_history_from_url(&database_url, new_exec_id).await;
    assert!(
        matches!(
            new_history.events.as_slice(),
            [
                WorkflowEvent::WorkflowStarted { .. },
                WorkflowEvent::WorkflowCompleted { .. },
            ]
        ),
        "new run history should be bounded; got {:?}",
        new_history
            .events
            .iter()
            .map(WorkflowEvent::type_name)
            .collect::<Vec<_>>(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continue_as_new_down_migration_rewrites_historical_runs_for_rollback() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let original_exec_id = insert_workflow_execution(&mut conn).await;
    let initial_input = serde_json::json!({"phase": "init"});
    enqueue_started_workflow_task(&mut conn, original_exec_id, initial_input.clone()).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "e2e_test_workflow",
            module: "integration_e2e",
            handler: continue_as_new_workflow,
        }],
        vec![],
    ));
    let worker = build_runtime_worker("worker-e2e-continue-down", 2, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let _sealed_execution =
        wait_for_execution_state(&database_url, original_exec_id, "CONTINUED_AS_NEW").await;
    let original_history = load_history_from_url(&database_url, original_exec_id).await;
    let successor_exec_id = original_history
        .events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(*new_exec_id),
            _ => None,
        })
        .expect("original history should contain a WorkflowContinuedAsNew event");
    let _completed = wait_for_execution_state(&database_url, successor_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    let down_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations/20260427000000_harvest_continue_as_new/down.sql");
    let down_sql = std::fs::read_to_string(&down_path).unwrap_or_else(|error| {
        panic!(
            "failed to read continue-as-new down migration at {}: {error}",
            down_path.display()
        )
    });
    conn.batch_execute(&down_sql).await.expect(
        "continue-as-new down migration should succeed after continue-as-new has been used",
    );

    let executions = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("e2e_test_workflow"))
        .order(harvest_workflow_executions::started_at.asc())
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("failed to reload workflow executions after down migration");
    assert_eq!(
        executions.len(),
        2,
        "rollback should preserve both runs while making them compatible with the pre-continue schema",
    );
    assert!(
        executions
            .iter()
            .all(|execution| execution.state != "CONTINUED_AS_NEW"),
        "down migration must eliminate CONTINUED_AS_NEW before restoring the old state check",
    );

    let original_row = executions
        .iter()
        .find(|execution| execution.id == original_exec_id.as_uuid())
        .expect("original execution should still exist after rollback rewrite");
    assert_eq!(
        original_row.state, "COMPLETED",
        "sealed historical runs should be rewritten to an old-schema terminal state",
    );
    assert!(
        original_row
            .workflow_id
            .starts_with("e2e-wf-001::continued-as-new:"),
        "sealed historical runs should get a synthetic workflow_id during rollback",
    );

    let successor_row = executions
        .iter()
        .find(|execution| execution.id == successor_exec_id.as_uuid())
        .expect("successor execution should still exist after rollback rewrite");
    assert_eq!(
        successor_row.workflow_id, "e2e-wf-001",
        "the latest run should retain the original logical workflow_id",
    );

    let rows_on_original_key = executions
        .iter()
        .filter(|execution| execution.workflow_id == "e2e-wf-001")
        .count();
    assert_eq!(
        rows_on_original_key, 1,
        "rollback should leave exactly one row on the original logical key before restoring uniqueness",
    );
}

// ── WorkflowIdReusePolicy integration matrix ─────────────────────────────────
//
// 4 policies × 5 prior states (none, RUNNING, COMPLETED, FAILED, CANCELLED)
// = 20 cells, each asserting: (a) which exec_id the second start observes,
// (b) whether a new exec_id was minted, (c) the error variant when applicable.

/// Helpers for the reuse-policy matrix tests.
mod reuse_policy_helpers {
    use super::*;

    pub fn base_params(
        workflow_id: &'static str,
        exec_id: ExecutionId,
    ) -> StartWorkflowParams<'static> {
        StartWorkflowParams {
            workflow_name: "reuse_policy_wf",
            workflow_id,
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            assigned_build_id: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
        }
    }

    /// Force a workflow row into a terminal state by direct UPDATE.
    pub async fn force_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId, state: &str) {
        diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
            .set(harvest_workflow_executions::state.eq(state))
            .execute(conn)
            .await
            .expect("force_state UPDATE failed");
    }
}

#[tokio::test]
async fn reuse_policy_allow_duplicate_no_prior_creates() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-allow-none", exec_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let result = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("AllowDuplicate with no prior should succeed");
    assert!(result.created, "should create when no prior exists");
    assert_eq!(result.exec_id, exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_duplicate_running_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-allow-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    assert!(first.created);

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("AllowDuplicate on RUNNING should return existing");
    assert!(!second.created);
    assert_eq!(
        second.exec_id, first.exec_id,
        "must return the first exec_id"
    );
    assert_eq!(second.state, "RUNNING");
}

#[tokio::test]
async fn reuse_policy_allow_duplicate_completed_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-allow-cmp", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "COMPLETED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("AllowDuplicate on COMPLETED should return existing");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_duplicate_failed_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-allow-fail", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "FAILED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("AllowDuplicate on FAILED should return existing");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_duplicate_cancelled_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-allow-can", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    cancel_workflow_execution(&mut conn, first.exec_id, "test cancel")
        .await
        .expect("cancel should succeed");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("AllowDuplicate on CANCELLED should return existing");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
    assert_eq!(second.state, "CANCELLED");
}

#[tokio::test]
async fn reuse_policy_reject_duplicate_no_prior_creates() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-reject-none", exec_id);
    params.reuse_policy = WorkflowIdReusePolicy::RejectDuplicate;
    let result = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("RejectDuplicate with no prior should succeed");
    assert!(result.created);
}

#[tokio::test]
async fn reuse_policy_reject_duplicate_running_errors() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-reject-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::RejectDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let err = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect_err("RejectDuplicate on RUNNING must error");
    match err {
        HarvestError::AlreadyExists {
            existing_exec_id,
            existing_state,
        } => {
            assert_eq!(existing_exec_id, first.exec_id);
            assert_eq!(existing_state, "RUNNING");
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
}

#[tokio::test]
async fn reuse_policy_reject_duplicate_completed_errors() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-reject-cmp", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::RejectDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "COMPLETED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let err = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect_err("RejectDuplicate on COMPLETED must error");
    assert!(matches!(err, HarvestError::AlreadyExists { .. }));
}

#[tokio::test]
async fn reuse_policy_reject_duplicate_failed_errors() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-reject-fail", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::RejectDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "FAILED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let err = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect_err("RejectDuplicate on FAILED must error");
    assert!(matches!(err, HarvestError::AlreadyExists { .. }));
}

#[tokio::test]
async fn reuse_policy_reject_duplicate_cancelled_errors() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-reject-can", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::RejectDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    cancel_workflow_execution(&mut conn, first.exec_id, "test cancel")
        .await
        .expect("cancel should succeed");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let err = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect_err("RejectDuplicate on CANCELLED must error");
    assert!(matches!(err, HarvestError::AlreadyExists { .. }));
}

#[tokio::test]
async fn reuse_policy_allow_failed_only_no_prior_creates() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-afo-none", exec_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let result = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("AllowDuplicateFailedOnly with no prior should create");
    assert!(result.created);
}

#[tokio::test]
async fn reuse_policy_allow_failed_only_running_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-afo-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("AllowDuplicateFailedOnly on RUNNING should return existing");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_failed_only_completed_returns_existing() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-afo-cmp", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "COMPLETED").await;

    params.exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("AllowDuplicateFailedOnly on COMPLETED should return existing");
    assert!(!second.created);
    assert_eq!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_failed_only_failed_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-afo-fail", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "FAILED").await;

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("AllowDuplicateFailedOnly on FAILED should start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id, "must use the new exec_id");
    assert_ne!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_allow_failed_only_cancelled_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-afo-can", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    cancel_workflow_execution(&mut conn, first.exec_id, "test cancel")
        .await
        .expect("cancel should succeed");

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("AllowDuplicateFailedOnly on CANCELLED should start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_terminate_if_running_no_prior_creates() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-none", exec_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let result = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("TerminateIfRunning with no prior should create");
    assert!(result.created);
    assert_eq!(result.exec_id, exec_id);
}

#[tokio::test]
async fn reuse_policy_terminate_if_running_running_cancels_and_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-run", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    assert_eq!(first.state, "RUNNING");

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("TerminateIfRunning on RUNNING should cancel and start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
    assert_eq!(second.state, "RUNNING");

    // Verify prior run was cancelled
    let prior = harvest_workflow_executions::table
        .find(first.exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("prior execution row must still exist");
    assert_eq!(
        prior.state, "CONTINUED_AS_NEW",
        "prior run should be sealed as CONTINUED_AS_NEW"
    );
}

#[tokio::test]
async fn reuse_policy_terminate_if_running_completed_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-cmp", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "COMPLETED").await;

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("TerminateIfRunning on COMPLETED should start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_terminate_if_running_failed_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-fail", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    reuse_policy_helpers::force_state(&mut conn, first.exec_id, "FAILED").await;

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("TerminateIfRunning on FAILED should start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

#[tokio::test]
async fn reuse_policy_terminate_if_running_cancelled_starts_fresh() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-can", first_id);
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("first start should succeed");
    cancel_workflow_execution(&mut conn, first.exec_id, "test cancel")
        .await
        .expect("cancel should succeed");

    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("TerminateIfRunning on CANCELLED should start fresh");
    assert!(second.created, "must mint a new execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

/// `TerminateIfRunning` is idempotent: if the prior run is CANCELLED (e.g. from
/// a previous `TerminateIfRunning` that crashed between the two transactions),
/// the retry starts fresh without requiring manual intervention.
#[tokio::test]
async fn reuse_policy_terminate_if_running_retry_after_partial_failure_is_idempotent() {
    let (mut conn, _container) = setup_test_db().await;
    let first_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let mut params = reuse_policy_helpers::base_params("rp-tir-retry", first_id);

    // Simulate transaction 1 completing (cancel) but transaction 2 failing
    // by manually cancelling the first run and then not starting a new one.
    params.reuse_policy = WorkflowIdReusePolicy::AllowDuplicate;
    let first = start_or_load_workflow_execution(&mut conn, params.clone())
        .await
        .expect("initial start should succeed");
    cancel_workflow_execution(&mut conn, first.exec_id, "simulated T1 complete")
        .await
        .expect("cancel should succeed");

    // Now retry with TerminateIfRunning. Prior run is CANCELLED → start fresh.
    let second_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    params.exec_id = second_id;
    params.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let second = start_or_load_workflow_execution(&mut conn, params)
        .await
        .expect("retry with TerminateIfRunning on CANCELLED should start fresh");
    assert!(second.created, "retry must mint a fresh execution");
    assert_eq!(second.exec_id, second_id);
    assert_ne!(second.exec_id, first.exec_id);
}

// ---------------------------------------------------------------------------
// Concurrency-cap integration tests (issue #88)
// ---------------------------------------------------------------------------

/// (a) Baseline: a `max_concurrent = 2` cap is enforced cluster-wide.
///
/// Six activity tasks are enqueued with the same concurrency key and cap.
/// Only 2 should be claimable at once. After completing one in-flight task, a
/// third slot opens and one more can be claimed.
#[tokio::test]
async fn concurrency_cap_limits_concurrent_claims_cluster_wide() {
    let (mut conn, _container) = setup_test_db().await;
    let queues = vec!["default".to_string()];

    for i in 0..6_u32 {
        let mut params =
            EnqueueParams::new("default", TaskType::Activity, serde_json::json!({ "i": i }));
        params.activity_name = Some("capped_activity".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        params.concurrency_key = Some("capped_activity".to_string());
        params.max_concurrent = Some(2);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue failed");
    }

    let t1 = queue::claim_task(&mut conn, &queues, "worker-cc-1", "")
        .await
        .expect("claim 1 query failed");
    let t2 = queue::claim_task(&mut conn, &queues, "worker-cc-1", "")
        .await
        .expect("claim 2 query failed");
    assert!(t1.is_some(), "first claim should succeed");
    assert!(t2.is_some(), "second claim should succeed");

    // Cap is now saturated — third claim must be deferred.
    let t3 = queue::claim_task(&mut conn, &queues, "worker-cc-1", "")
        .await
        .expect("claim 3 query failed");
    assert!(
        t3.is_none(),
        "third claim must be deferred while cap is saturated"
    );

    // Complete one in-flight task to free a slot.
    queue::complete_task(&mut conn, t1.unwrap().id, serde_json::json!(null))
        .await
        .expect("complete_task failed");

    // Now a slot is free; one more task should be claimable.
    let t4 = queue::claim_task(&mut conn, &queues, "worker-cc-1", "")
        .await
        .expect("claim after complete query failed");
    assert!(
        t4.is_some(),
        "claim should succeed after one in-flight task completes"
    );
}

/// (b) Shared-key: two distinct activities sharing `concurrency_key = "stripe"`
/// and `max_concurrent = 3` consume from a single shared budget — not two
/// independent budgets of 3 each.
#[tokio::test]
async fn concurrency_cap_shared_key_budget_is_not_doubled() {
    let (mut conn, _container) = setup_test_db().await;
    let queues = vec!["default".to_string()];

    // 3 tasks for activity A
    for i in 0..3_u32 {
        let mut params = EnqueueParams::new(
            "default",
            TaskType::Activity,
            serde_json::json!({ "act": "charge", "i": i }),
        );
        params.activity_name = Some("charge_stripe".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        params.concurrency_key = Some("stripe".to_string());
        params.max_concurrent = Some(3);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue charge_stripe failed");
    }

    // 3 tasks for activity B, same key and cap
    for i in 0..3_u32 {
        let mut params = EnqueueParams::new(
            "default",
            TaskType::Activity,
            serde_json::json!({ "act": "refund", "i": i }),
        );
        params.activity_name = Some("refund_stripe".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        params.concurrency_key = Some("stripe".to_string());
        params.max_concurrent = Some(3);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue refund_stripe failed");
    }

    // Attempt to claim all 6; the shared budget of 3 should cap the total.
    let mut claimed = 0usize;
    for _ in 0..6 {
        if queue::claim_task(&mut conn, &queues, "worker-sk-1", "")
            .await
            .expect("claim query failed")
            .is_some()
        {
            claimed += 1;
        }
    }
    assert_eq!(
        claimed, 3,
        "shared 'stripe' budget of 3 must cap the combined in-flight count to 3, not 6"
    );
}

/// (c) Failure path: in-flight tasks that fail transition out of RUNNING,
/// freeing their slots so that pending peers can be claimed. The queue must
/// not wedge when the saturating tasks all fail.
#[tokio::test]
async fn concurrency_cap_failure_frees_slot_and_does_not_wedge_queue() {
    let (mut conn, _container) = setup_test_db().await;
    let queues = vec!["default".to_string()];

    for i in 0..4_u32 {
        let mut params =
            EnqueueParams::new("default", TaskType::Activity, serde_json::json!({ "i": i }));
        params.activity_name = Some("fragile_activity".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        params.concurrency_key = Some("fragile".to_string());
        params.max_concurrent = Some(2);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue failed");
    }

    // Claim 2 (saturating the cap).
    let t1 = queue::claim_task(&mut conn, &queues, "worker-fp-1", "")
        .await
        .expect("claim 1 query failed")
        .expect("first task should be claimable");
    let t2 = queue::claim_task(&mut conn, &queues, "worker-fp-1", "")
        .await
        .expect("claim 2 query failed")
        .expect("second task should be claimable");

    // Cap is now saturated.
    let t3 = queue::claim_task(&mut conn, &queues, "worker-fp-1", "")
        .await
        .expect("claim 3 query failed");
    assert!(t3.is_none(), "cap must be saturated after 2 claims");

    // Fail both in-flight tasks — this transitions them out of RUNNING.
    queue::fail_task(&mut conn, t1.id, "intentional test failure")
        .await
        .expect("fail t1 failed");
    queue::fail_task(&mut conn, t2.id, "intentional test failure")
        .await
        .expect("fail t2 failed");

    // The queue must not be wedged; the remaining pending tasks must be claimable.
    let t4 = queue::claim_task(&mut conn, &queues, "worker-fp-1", "")
        .await
        .expect("claim after fail query failed");
    let t5 = queue::claim_task(&mut conn, &queues, "worker-fp-1", "")
        .await
        .expect("claim 5 query failed");
    assert!(
        t4.is_some(),
        "pending task must be claimable after in-flight tasks fail"
    );
    assert!(
        t5.is_some(),
        "second pending task must also be claimable after in-flight tasks fail"
    );
}

/// (d) Backward compatibility: activities without a `concurrency_key` (NULL in
/// the queue row) are completely unaffected by a saturated key — they can be
/// claimed freely even when another key is at its cap.
#[tokio::test]
async fn concurrency_cap_null_key_tasks_are_unaffected_by_saturated_key() {
    let (mut conn, _container) = setup_test_db().await;
    let queues = vec!["default".to_string()];

    // Saturate a key with 2 RUNNING tasks (cap = 2).
    for i in 0..2_u32 {
        let mut params =
            EnqueueParams::new("default", TaskType::Activity, serde_json::json!({ "i": i }));
        params.activity_name = Some("capped_activity".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(2);
        params.concurrency_key = Some("saturated_key".to_string());
        params.max_concurrent = Some(2);
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue capped task failed");
        // Immediately claim each to put it in RUNNING state.
        queue::claim_task(&mut conn, &queues, "worker-bc-1", "")
            .await
            .expect("claim capped task failed");
    }

    // Verify the key is saturated (third claim returns None).
    let saturated_check = queue::claim_task(&mut conn, &queues, "worker-bc-1", "")
        .await
        .expect("saturation check query failed");
    assert!(
        saturated_check.is_none(),
        "key must be saturated before backward-compat check"
    );

    // Enqueue 3 tasks with NO concurrency key (null — the pre-#88 baseline).
    for i in 0..3_u32 {
        let mut params =
            EnqueueParams::new("default", TaskType::Activity, serde_json::json!({ "i": i }));
        params.activity_name = Some("uncapped_activity".into());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        // concurrency_key left as None (default) — backward-compat path.
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue uncapped task failed");
    }

    // All 3 uncapped tasks must be claimable even though the saturated key
    // is at its cap — the NULL check-path must not be constrained by other keys.
    let mut claimed = 0usize;
    for _ in 0..3 {
        if queue::claim_task(&mut conn, &queues, "worker-bc-1", "")
            .await
            .expect("uncapped claim query failed")
            .is_some()
        {
            claimed += 1;
        }
    }
    assert_eq!(
        claimed, 3,
        "uncapped tasks must not be blocked by a saturated concurrency key"
    );
}

// ---------------------------------------------------------------------------
// Issue #91: per-workflow cron schedule integration tests
// ---------------------------------------------------------------------------

/// Helper: count executions for a given workflow name in any non-terminal or
/// completed state.
async fn count_executions_for_workflow(database_url: &str, workflow_name: &str) -> i64 {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for execution count query");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .count()
        .get_result::<i64>(&mut conn)
        .await
        .expect("count query failed")
}

/// Helper: count executions in RUNNING state for a given workflow name.
async fn count_running_executions(database_url: &str, workflow_name: &str) -> i64 {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for running-count query");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .count()
        .get_result::<i64>(&mut conn)
        .await
        .expect("running count query failed")
}

/// Instant-return workflow handler used for schedule baseline tests.
fn instant_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::Value::Null) })
}

/// Slow workflow handler that sleeps for 30 s — used to saturate `max_active_runs`.
fn slow_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(serde_json::Value::Null)
    })
}

/// (a) Baseline: a `*/2 * * * * *` schedule dispatches >=3 executions in a
/// 10-second window and each execution carries the deterministic `workflow_id`
/// `sched:{name}:{ts}`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_schedule_baseline_dispatches_multiple_runs() {
    use autumn_harvest::schema::harvest_workflow_executions::dsl as exec_dsl;

    let (database_url, _container) = setup_test_database_url().await;

    let wf_name = "scheduled_instant_workflow";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: wf_name,
            module: "integration_e2e",
            handler: instant_workflow,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    // Register the schedule row before starting the scheduler.
    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()));
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }

    // Start scheduler + worker.
    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "worker-sched-baseline".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(100),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
            },
            Arc::clone(&registry),
        )
        .expect("worker should build"),
    );
    let worker_pool = pool.clone();
    let worker_ref = Arc::clone(&worker);
    let worker_handle = tokio::spawn(async move { worker_ref.run(&worker_pool).await });

    // Wait up to 10 seconds for at least 3 executions to appear.
    let dispatched = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let n = count_executions_for_workflow(&database_url, wf_name).await;
            if n >= 3 {
                break n;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("should observe >=3 scheduled dispatches within 10 seconds");

    assert!(dispatched >= 3, "expected >=3 executions, got {dispatched}");

    // Verify that execution workflow_ids follow the deterministic `sched:{name}:{ts}` pattern.
    let mut conn = pool.get().await.expect("pool get failed");
    let workflow_ids: Vec<String> = exec_dsl::harvest_workflow_executions
        .filter(exec_dsl::workflow_name.eq(wf_name))
        .select(exec_dsl::workflow_id)
        .load(&mut conn)
        .await
        .expect("load workflow_ids failed");
    for id in &workflow_ids {
        assert!(
            id.starts_with(&format!("sched:{wf_name}:")),
            "workflow_id '{id}' does not match expected sched: prefix"
        );
    }

    scheduler.shutdown();
    worker.shutdown();
    let _ = scheduler.join().await;
    let _ = worker_handle.await;
}

/// (b) `max_active_runs = 1` with a slow handler: the second cron firing must
/// be skipped — the in-flight run count must never exceed 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_schedule_max_active_runs_enforced() {
    let (database_url, _container) = setup_test_database_url().await;

    let wf_name = "scheduled_slow_workflow";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: wf_name,
            module: "integration_e2e",
            handler: slow_workflow,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_max_active_runs(1);
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }

    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "worker-sched-maxruns".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(100),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
            },
            Arc::clone(&registry),
        )
        .expect("worker should build"),
    );
    let worker_pool = pool.clone();
    let worker_ref = Arc::clone(&worker);
    let worker_handle = tokio::spawn(async move { worker_ref.run(&worker_pool).await });

    // Let at least one execution start, then observe multiple ticks.
    // After the first dispatch, subsequent ticks must skip because the slow
    // workflow is RUNNING. Allow 8 seconds so we see 3–4 ticks at 2s cadence.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // At most 1 RUNNING execution at any point in time.
    let running = count_running_executions(&database_url, wf_name).await;
    assert!(
        running <= 1,
        "expected at most 1 running execution, found {running}"
    );

    // At least 1 execution was dispatched (the first tick).
    let total = count_executions_for_workflow(&database_url, wf_name).await;
    assert!(total >= 1, "expected at least 1 execution, got {total}");

    scheduler.shutdown();
    worker.shutdown();
    let _ = scheduler.join().await;
    let _ = worker_handle.await;
}

/// (c) Pause / resume: no dispatches after pause; dispatches resume after unpause.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_schedule_pause_and_resume() {
    use autumn_harvest::schema::harvest_schedules::dsl as sched_dsl;

    let (database_url, _container) = setup_test_database_url().await;

    let wf_name = "scheduled_pause_resume_workflow";
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: wf_name,
            module: "integration_e2e",
            handler: instant_workflow,
        }],
        vec![],
    ));
    let pool = build_test_pool(&database_url);

    // Register the schedule paused so no runs fire initially.
    let ws = WorkflowSchedule::new(wf_name, Schedule::Cron("*/2 * * * * *".to_string()))
        .with_paused(true);
    {
        let mut conn = pool.get().await.expect("pool get failed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
            .await
            .expect("register_workflow_schedules failed");
    }

    let workflow_schedules = Arc::new(vec![ws]);
    let scheduler = SchedulerRuntime::spawn(
        pool.clone(),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::clone(&workflow_schedules),
    );
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "worker-sched-pause".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(100),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
            },
            Arc::clone(&registry),
        )
        .expect("worker should build"),
    );
    let worker_pool = pool.clone();
    let worker_ref = Arc::clone(&worker);
    let worker_handle = tokio::spawn(async move { worker_ref.run(&worker_pool).await });

    // Let 3 ticks pass — schedule is paused so no executions should start.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let count_while_paused = count_executions_for_workflow(&database_url, wf_name).await;
    assert_eq!(
        count_while_paused, 0,
        "no executions should fire while schedule is paused"
    );

    // Resume: flip is_paused to false and update next_run_at so the scheduler
    // will pick it up on the next tick.
    {
        let mut conn = pool.get().await.expect("pool get failed");
        diesel::update(sched_dsl::harvest_schedules.filter(sched_dsl::workflow_name.eq(wf_name)))
            .set((
                sched_dsl::is_paused.eq(false),
                sched_dsl::next_run_at.eq(Utc::now() - chrono::Duration::seconds(1)),
                sched_dsl::updated_at.eq(Utc::now()),
            ))
            .execute(&mut conn)
            .await
            .expect("resume update failed");
    }

    // After resuming, wait up to 6 seconds for at least 1 execution.
    let dispatched = tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let n = count_executions_for_workflow(&database_url, wf_name).await;
            if n >= 1 {
                break n;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("should observe executions after resume within 6 seconds");

    assert!(
        dispatched >= 1,
        "expected >=1 execution after resume, got {dispatched}"
    );

    scheduler.shutdown();
    worker.shutdown();
    let _ = scheduler.join().await;
    let _ = worker_handle.await;
}

/// (d) Backward compatibility: a deployment with only DAG schedules in the
/// `harvest_schedules` table sees no interaction from the workflow-schedule
/// tick branch. This verifies the `workflow_name IS NOT NULL` filter keeps
/// DAG-only rows untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_schedule_dag_only_deployment_unaffected() {
    use autumn_harvest::schema::harvest_schedules::dsl as sched_dsl;

    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // There are no workflow-schedule rows; the workflow_schedules list is empty.
    let empty_workflow_schedules: Arc<Vec<WorkflowSchedule>> = Arc::new(Vec::new());
    let empty_dags: Arc<autumn_harvest::DagCatalog> = Arc::new(DagCatalog::default());
    let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));

    // Run several ticks against an otherwise-empty database.
    for _ in 0..3 {
        tick_once(
            pool.clone(),
            Arc::clone(&registry),
            Arc::clone(&empty_dags),
            Arc::clone(&empty_workflow_schedules),
            SchedulerMonitor::offline(),
        )
        .await
        .expect("tick_once must not error on an empty workflow-schedule list");
    }

    // No schedule rows should have been inserted.
    let mut conn = pool.get().await.expect("pool get failed");
    let schedule_count: i64 = sched_dsl::harvest_schedules
        .count()
        .get_result(&mut conn)
        .await
        .expect("count query failed");
    assert_eq!(
        schedule_count, 0,
        "no schedule rows should exist when workflow_schedules is empty"
    );

    // No workflow executions should have been started.
    let exec_count: i64 = harvest_workflow_executions::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("execution count query failed");
    assert_eq!(
        exec_count, 0,
        "no workflow executions should have been started"
    );
}

// ---------------------------------------------------------------------------
// Search-attribute lifecycle tests (issue #159, AC #11 and #12)
// ---------------------------------------------------------------------------

/// Workflow used by search-attribute tests.
///
/// - Immediately sets `phase=awaiting_approval`.
/// - Suspends on a `charge` signal.
/// - On receipt, overwrites `phase=charged`.
/// - Completes.
fn approval_search_attrs_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.upsert_search_attrs([(
            "phase".to_string(),
            Some(serde_json::json!("awaiting_approval")),
        )])
        .map_err(|e| e.to_string())?;

        ctx.wait_for_signal("charge")
            .await
            .map_err(|e| e.to_string())?;

        ctx.upsert_search_attrs([("phase".to_string(), Some(serde_json::json!("charged")))])
            .map_err(|e| e.to_string())?;

        Ok(serde_json::json!({"status": "charged"}))
    })
}

/// Query `harvest_workflow_executions` rows whose `search_attrs` contains all
/// key-value pairs in `predicate` (Postgres `@>` containment operator).
async fn find_by_search_attrs(
    database_url: &str,
    predicate: serde_json::Value,
) -> Vec<WorkflowExecution> {
    use diesel::dsl::sql;
    use diesel::sql_types::{Bool, Jsonb};

    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("fresh connection for search_attrs query");

    harvest_workflow_executions::table
        .filter(sql::<Bool>("search_attrs @> ").bind::<Jsonb, _>(predicate))
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("search_attrs containment query failed")
}

/// AC #11 — mutable search-attribute lifecycle:
///
/// 1. Workflow starts with `tenant=acme`.
/// 2. First execution cycle: `upsert_search_attrs` sets `phase=awaiting_approval`.
/// 3. Workflow suspends on the `charge` signal.
/// 4. DB query with `tenant=acme AND phase=awaiting_approval` finds the execution.
/// 5. `charge` signal is delivered; second cycle sets `phase=charged`.
/// 6. `phase=awaiting_approval` filter returns nothing; `phase=charged` filter finds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_attrs_upsert_visible_after_update_and_filterable() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("connect to test DB");

    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));

    // Start the workflow with tenant=acme in initial search_attrs.
    let start = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "approval_search_attrs_workflow",
            workflow_id: "acme-approval-001",
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: Some(serde_json::json!({"tenant": "acme"})),
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
        },
    )
    .await
    .expect("start_or_load_workflow_execution failed");
    assert!(start.created);

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "approval_search_attrs_workflow",
            module: "integration_e2e",
            handler: approval_search_attrs_workflow,
        }],
        vec![],
    ));
    let worker = build_runtime_worker("worker-sa-lifecycle", 1, 1, registry);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // Wait for the workflow to reach the signal-wait suspension (phase should
    // now be awaiting_approval in the DB).
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = find_by_search_attrs(
                &database_url,
                serde_json::json!({"tenant": "acme", "phase": "awaiting_approval"}),
            )
            .await;
            if !rows.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("search_attrs should show phase=awaiting_approval within timeout");

    // Confirm the old filter now has a hit.
    let awaiting = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "awaiting_approval"}),
    )
    .await;
    assert_eq!(
        awaiting.len(),
        1,
        "should find one execution awaiting approval"
    );
    assert_eq!(awaiting[0].id, exec_id.as_uuid());

    // Confirm it is NOT yet in the charged filter.
    let charged_before = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "charged"}),
    )
    .await;
    assert!(
        charged_before.is_empty(),
        "should not appear in charged filter before signal"
    );

    // Deliver the charge signal and wake the task.
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "charge", serde_json::json!({}))
        .await
        .expect("send_signal failed");
    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake_workflow_task failed");

    // Wait for completion.
    wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    // After completion: phase=awaiting_approval filter returns nothing.
    let awaiting_after = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "awaiting_approval"}),
    )
    .await;
    assert!(
        awaiting_after.is_empty(),
        "awaiting_approval filter must be empty after phase update"
    );

    // phase=charged filter now finds the execution.
    let charged_after = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "charged"}),
    )
    .await;
    assert_eq!(
        charged_after.len(),
        1,
        "charged filter should find the execution after phase update"
    );
    assert_eq!(charged_after[0].id, exec_id.as_uuid());
}

/// AC #12 — search attributes survive a worker crash and resume:
///
/// 1. Worker runs the first cycle, sets `phase=awaiting_approval`, suspends.
/// 2. Worker is shut down (simulating a crash).
/// 3. A fresh query confirms the attribute is still in the DB.
/// 4. A new worker picks up the task from where it left off.
/// 5. Signal is delivered; workflow completes with `phase=charged` in the DB.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_attrs_survive_worker_crash_and_resume() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("connect to test DB");

    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));

    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "approval_search_attrs_workflow",
            workflow_id: "acme-crash-resume-001",
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: Some(serde_json::json!({"tenant": "acme"})),
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
        },
    )
    .await
    .expect("start_or_load_workflow_execution failed");

    let make_registry = || {
        Arc::new(HandlerRegistry::new(
            vec![WorkflowInfo {
                name: "approval_search_attrs_workflow",
                module: "integration_e2e",
                handler: approval_search_attrs_workflow,
            }],
            vec![],
        ))
    };
    let pool = build_test_pool(&database_url);

    // --- First worker: run until phase=awaiting_approval is persisted ---
    let worker1 = build_runtime_worker("worker-crash-1", 1, 1, make_registry());
    let handle1 = spawn_test_worker(Arc::clone(&worker1), pool.clone());

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = find_by_search_attrs(
                &database_url,
                serde_json::json!({"phase": "awaiting_approval"}),
            )
            .await;
            if !rows.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("phase=awaiting_approval should be visible within timeout");

    // Simulate crash: shut down the first worker.
    worker1.shutdown();
    handle1.await.expect("worker1 join");

    // Confirm the attribute is durable after the crash.
    let after_crash = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "awaiting_approval"}),
    )
    .await;
    assert_eq!(
        after_crash.len(),
        1,
        "search_attrs must survive worker crash"
    );

    // --- Second worker: resume and complete ---
    let worker2 = build_runtime_worker("worker-crash-2", 1, 1, make_registry());
    let handle2 = spawn_test_worker(Arc::clone(&worker2), pool.clone());

    // Wait for the task to be re-claimed by the new worker (sticky timeout
    // elapses or the task is re-enqueued after the signal below).
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "charge", serde_json::json!({}))
        .await
        .expect("send_signal failed");
    queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("wake_workflow_task failed");

    wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;

    worker2.shutdown();
    handle2.await.expect("worker2 join");

    // Final check: phase=charged is now in the DB.
    let final_state = find_by_search_attrs(
        &database_url,
        serde_json::json!({"tenant": "acme", "phase": "charged"}),
    )
    .await;
    assert_eq!(
        final_state.len(),
        1,
        "phase=charged must be set after resume and completion"
    );
}

/// (e) Builder validation: scheduling an unregistered workflow name fails at
/// `build()` with `HarvestBuilderError::UnknownWorkflowSchedule`.
#[test]
fn workflow_schedule_builder_rejects_unregistered_workflow() {
    let ws = WorkflowSchedule::new(
        "nonexistent_workflow",
        Schedule::Cron("0 * * * *".to_string()),
    );

    let result = HarvestBuilder::new()
        .workflows(vec![WorkflowInfo {
            name: "some_other_workflow",
            module: "integration_e2e",
            handler: echo_workflow,
        }])
        .workflow_schedule(ws)
        .worker(WorkerConfig::default())
        .try_build();

    assert!(
        matches!(
            result,
            Err(autumn_harvest::HarvestBuilderError::UnknownWorkflowSchedule {
                ref workflow_name, ..
            }) if workflow_name == "nonexistent_workflow"
        ),
        "expected UnknownWorkflowSchedule error, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Worker drain controls (issue #170)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drain_accepted_sets_status_to_draining() {
    use autumn_harvest::workers::{DrainOutcome, register_worker, request_drain};

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    register_worker(
        &mut conn,
        "w-drain-1",
        &["default".to_string()],
        &[0],
        4,
        "test-host",
        None,
        "",
        None,
    )
    .await
    .unwrap();

    // Supply an explicit deadline; the default is computed by the HTTP handler
    // layer, not request_drain itself. The integration test verifies the DB
    // round-trip for a caller-supplied deadline.
    let deadline = Utc::now() + chrono::Duration::minutes(1);
    let resp = request_drain(
        &mut conn,
        "w-drain-1",
        Some(deadline),
        true,
        stale_threshold,
    )
    .await
    .unwrap();

    assert_eq!(
        resp.outcome,
        DrainOutcome::Accepted,
        "first drain must be Accepted"
    );
    assert!(
        resp.drain_deadline_at.is_some(),
        "drain_deadline_at must be set when a deadline is supplied"
    );
    assert_eq!(resp.worker_id, "w-drain-1");
    assert!(resp.unavailable_shards.is_empty());
}

#[tokio::test]
async fn drain_already_draining_on_second_call() {
    use autumn_harvest::workers::{DrainOutcome, register_worker, request_drain};

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    register_worker(
        &mut conn,
        "w-drain-2",
        &["default".to_string()],
        &[],
        2,
        "test-host",
        None,
        "",
        None,
    )
    .await
    .unwrap();

    let first_deadline = Utc::now() + chrono::Duration::minutes(1);
    request_drain(
        &mut conn,
        "w-drain-2",
        Some(first_deadline),
        true,
        stale_threshold,
    )
    .await
    .unwrap();

    // Re-drain with a new deadline — should return AlreadyDraining and
    // persist the updated deadline (operators extending a drain window).
    let new_deadline = Utc::now() + chrono::Duration::minutes(5);
    let resp2 = request_drain(
        &mut conn,
        "w-drain-2",
        Some(new_deadline),
        true,
        stale_threshold,
    )
    .await
    .unwrap();

    assert_eq!(
        resp2.outcome,
        DrainOutcome::AlreadyDraining,
        "second drain on already-draining worker must return AlreadyDraining"
    );
    // Deadline must reflect the refreshed value, not the original.
    let stored = resp2.drain_deadline_at.expect("deadline must be echoed");
    let diff = (stored - new_deadline).num_seconds().abs();
    assert!(diff <= 2, "refreshed deadline differs by {diff}s");
}

#[tokio::test]
async fn drain_already_stopped_after_transition() {
    use autumn_harvest::workers::{
        DrainOutcome, WorkerStatus, register_worker, request_drain, transition_status,
    };

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    register_worker(
        &mut conn,
        "w-drain-3",
        &[],
        &[],
        1,
        "test-host",
        None,
        "",
        None,
    )
    .await
    .unwrap();
    transition_status(&mut conn, "w-drain-3", WorkerStatus::Stopped)
        .await
        .unwrap();

    let resp = request_drain(&mut conn, "w-drain-3", None, false, stale_threshold)
        .await
        .unwrap();

    assert_eq!(
        resp.outcome,
        DrainOutcome::AlreadyStopped,
        "draining a stopped worker must return AlreadyStopped"
    );
}

#[tokio::test]
async fn drain_not_found_for_unknown_worker() {
    use autumn_harvest::workers::request_drain;

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    let resp = request_drain(&mut conn, "w-does-not-exist", None, false, stale_threshold)
        .await
        .unwrap();

    assert_eq!(
        resp.outcome,
        autumn_harvest::workers::DrainOutcome::NotFound,
        "unknown worker must return NotFound"
    );
}

#[tokio::test]
async fn drain_with_explicit_deadline_is_stored() {
    use autumn_harvest::workers::{DrainOutcome, register_worker, request_drain};

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    register_worker(
        &mut conn,
        "w-drain-deadline",
        &[],
        &[],
        1,
        "test-host",
        None,
        "",
        None,
    )
    .await
    .unwrap();

    let explicit_deadline = Utc::now() + chrono::Duration::minutes(5);
    let resp = request_drain(
        &mut conn,
        "w-drain-deadline",
        Some(explicit_deadline),
        true,
        stale_threshold,
    )
    .await
    .unwrap();

    assert_eq!(resp.outcome, DrainOutcome::Accepted);
    let stored = resp.drain_deadline_at.expect("deadline must be set");
    let diff = (stored - explicit_deadline).num_seconds().abs();
    assert!(diff <= 2, "stored deadline differs by {diff}s");
}

#[tokio::test]
async fn drain_preview_returns_active_workers() {
    use autumn_harvest::workers::{WorkerFilters, drain_preview, register_worker};

    let (mut conn, _container) = setup_test_db().await;
    let stale_threshold = Duration::from_secs(10);

    for i in 0..3_u8 {
        register_worker(
            &mut conn,
            &format!("w-preview-{i}"),
            &["default".to_string()],
            &[],
            4,
            "test-host",
            None,
            "",
            None,
        )
        .await
        .unwrap();
    }

    let filters = WorkerFilters {
        queue: Some("default".to_string()),
        ..WorkerFilters::new()
    };
    let items = drain_preview(&mut conn, &filters, stale_threshold)
        .await
        .unwrap();

    assert_eq!(items.len(), 3, "drain-preview should return all 3 workers");
    for item in &items {
        assert_eq!(item.status, "Active");
    }
}
