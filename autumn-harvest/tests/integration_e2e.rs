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
use autumn_harvest::queue::{EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_timers, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, HarvestBuilder, HarvestError, StartWorkflowParams, TimeoutType, WorkerConfig,
    WorkflowContext, queue, start_or_load_workflow_execution, timeout,
};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use scoped_futures::ScopedFutureExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// The migration SQL embedded at compile time.
const INIT_SQL: &str = include_str!("../migrations/20260409000000_harvest_initial/up.sql");

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
    let legacy_init_sql = INIT_SQL.replacen(
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
        shard_id: 0,
        input: serde_json::json!({ "workflow_id": 42 }),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
    };

    let legacy_error = start_or_load_workflow_execution(&mut conn, request.clone())
        .await
        .expect_err("legacy schema should reject the new ON CONFLICT target");
    assert!(
        matches!(legacy_error, HarvestError::Database(_)),
        "legacy schema should fail with a database error, got {legacy_error:?}",
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

    let first = start_or_load_workflow_execution(&mut conn, request.clone())
        .await
        .expect("upgrade migration should restore workflow start");
    let second = start_or_load_workflow_execution(&mut conn, request)
        .await
        .expect("upgrade migration should restore idempotent workflow start");

    assert!(
        first.created,
        "first start should create a workflow execution"
    );
    assert!(
        !second.created,
        "second start should reuse the existing workflow execution",
    );
    assert_eq!(
        first.exec_id, second.exec_id,
        "idempotent starts should point at the same execution after the upgrade migration",
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
    let claimed = queue::claim_task(&mut conn, &queues, "worker-e2e-1")
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
    let claimed = queue::claim_task(&mut conn, &queues, "worker-empty-1")
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
    let claimed_workflow = queue::claim_task(&mut conn, &default_queues, "parked-worker")
        .await
        .expect("claim parked workflow task failed")
        .expect("workflow task should be claimable");
    assert_eq!(claimed_workflow.id, workflow_task_id);
    assert_eq!(claimed_workflow.state, "RUNNING");
    queue::park_workflow_task(&mut conn, workflow_task_id)
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
            handler: stateful_activity,
        }])
        .state(String::from("haunted"))
        .worker(WorkerConfig::default())
        .build();
    let (registry, _dags, worker_config) = built.into_worker_parts();
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
    let claimed = queue::claim_task(&mut conn, &queues, "wake-test-worker")
        .await
        .expect("claim should succeed")
        .expect("workflow task should be claimable");
    assert_eq!(claimed.id, task_id);
    queue::park_workflow_task(&mut conn, task_id)
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
    let claimed = queue::claim_task(&mut conn, &queues, "active-worker")
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
    let claimed = queue::claim_task(&mut conn, &queues, "retry-worker")
        .await
        .expect("claim should succeed")
        .expect("activity task should be claimable");
    assert_eq!(claimed.id, task_id);

    queue::record_heartbeat(&mut conn, task_id)
        .await
        .expect("record heartbeat should succeed");
    let heartbeating = load_task_from_url(&database_url, task_id).await;
    assert!(
        heartbeating.last_heartbeat_at.is_some(),
        "heartbeat should be recorded before reschedule"
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
