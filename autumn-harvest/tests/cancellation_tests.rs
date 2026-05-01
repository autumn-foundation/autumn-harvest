#![cfg(feature = "db")]

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::{TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::signal;
use autumn_harvest::store;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, HarvestError, StartWorkflowParams, WorkflowContext, cancel_workflow_execution,
    queue, start_or_load_workflow_execution,
};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel::sql_query;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::Value;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
);

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

    let conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    (conn, container)
}

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

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn start_test_workflow(conn: &mut AsyncPgConnection) -> autumn_harvest::ExecutionId {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "cancel_me",
            workflow_id: "cancel-me-001",
            exec_id: autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0)),
            input: serde_json::json!({ "request_id": "cancel-me-001" }),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
        },
    )
    .await
    .expect("workflow start should succeed")
    .exec_id
}

async fn load_execution(
    conn: &mut AsyncPgConnection,
    exec_id: autumn_harvest::ExecutionId,
) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("workflow execution should exist")
}

async fn load_tasks(
    conn: &mut AsyncPgConnection,
    exec_id: autumn_harvest::ExecutionId,
) -> Vec<TaskQueueItem> {
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .order(harvest_task_queue::scheduled_at.asc())
        .select(TaskQueueItem::as_select())
        .load(conn)
        .await
        .expect("task queue rows should load")
}

#[derive(Clone)]
struct HeartbeatCancellationProbe {
    activity_started: Arc<tokio::sync::Notify>,
    activity_saw_cancel: Arc<AtomicBool>,
}

fn heartbeat_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("heartbeat_activity", input, "default")
            .await
            .map_err(|error| error.to_string())
    })
}

fn heartbeat_activity<'a>(
    ctx: &'a ActivityContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let probe = ctx
            .state::<HeartbeatCancellationProbe>()
            .expect("probe state must be registered")
            .clone();
        probe.activity_started.notify_waiters();

        loop {
            match ctx.heartbeat(serde_json::json!({ "tick": true })).await {
                Ok(()) => tokio::time::sleep(Duration::from_millis(25)).await,
                Err(error) => {
                    probe.activity_saw_cancel.store(true, Ordering::SeqCst);
                    return Err(error.to_string());
                }
            }
        }
    })
}

fn heartbeat_registry(probe: HeartbeatCancellationProbe) -> Arc<HandlerRegistry> {
    let mut state: HashMap<TypeId, Box<dyn Any + Send + Sync>> = HashMap::new();
    state.insert(TypeId::of::<HeartbeatCancellationProbe>(), Box::new(probe));

    Arc::new(HandlerRegistry::with_state(
        vec![autumn_harvest::info::WorkflowInfo {
            name: "heartbeat_workflow",
            module: "cancellation_tests",
            handler: heartbeat_workflow,
        }],
        vec![autumn_harvest::info::ActivityInfo {
            name: "heartbeat_activity",
            module: "cancellation_tests",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            handler: heartbeat_activity,
        }],
        Arc::new(state),
    ))
}

#[tokio::test]
async fn cancel_running_workflow_marks_execution_cancelled_and_fails_open_tasks() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = start_test_workflow(&mut conn).await;

    let mut activity = EnqueueParams::new(
        "default",
        TaskType::Activity,
        serde_json::json!({ "work": "expensive" }),
    );
    activity.workflow_exec_id = Some(exec_id.as_uuid());
    activity.activity_name = Some("expensive_activity".to_string());
    queue::enqueue(&mut conn, &activity)
        .await
        .expect("activity task should enqueue");

    let cancelled = cancel_workflow_execution(&mut conn, exec_id, "operator requested shutdown")
        .await
        .expect("workflow cancellation should succeed");

    assert_eq!(cancelled.exec_id, exec_id);
    assert_eq!(cancelled.state, "CANCELLED");
    assert!(cancelled.newly_cancelled);
    assert_eq!(
        cancelled.failed_task_count, 2,
        "initial workflow task and activity task should both be failed"
    );

    let execution = load_execution(&mut conn, exec_id).await;
    assert_eq!(execution.state, "CANCELLED");
    assert_eq!(
        execution.error.as_deref(),
        Some("operator requested shutdown")
    );
    assert!(execution.completed_at.is_some());

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history should load");
    assert!(matches!(
        history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::WorkflowCancelled { reason }
        ] if reason == "operator requested shutdown"
    ));

    let tasks = load_tasks(&mut conn, exec_id).await;
    assert_eq!(tasks.len(), 2);
    assert!(tasks.iter().all(|task| task.state == "FAILED"));
    assert!(tasks.iter().all(|task| {
        task.error
            .as_deref()
            .is_some_and(|error| error.contains("operator requested shutdown"))
    }));
}

#[tokio::test]
async fn cancelling_an_already_cancelled_workflow_is_idempotent() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = start_test_workflow(&mut conn).await;

    cancel_workflow_execution(&mut conn, exec_id, "first reason")
        .await
        .expect("first cancellation should succeed");
    let second = cancel_workflow_execution(&mut conn, exec_id, "second reason")
        .await
        .expect("second cancellation should be idempotent");

    assert!(!second.newly_cancelled);
    assert_eq!(second.reason, "first reason");
    assert_eq!(second.failed_task_count, 0);

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history should load");
    let cancellation_events = history
        .events
        .iter()
        .filter(|event| matches!(event, WorkflowEvent::WorkflowCancelled { .. }))
        .count();
    assert_eq!(
        cancellation_events, 1,
        "idempotent cancellation must not append a second terminal event"
    );
}

#[tokio::test]
async fn signals_to_cancelled_workflows_are_rejected() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = start_test_workflow(&mut conn).await;
    cancel_workflow_execution(&mut conn, exec_id, "no more signals")
        .await
        .expect("workflow cancellation should succeed");

    let error = signal::send_signal(
        &mut conn,
        exec_id,
        "approved",
        serde_json::json!({ "approved": true }),
    )
    .await
    .expect_err("terminal cancelled workflows must reject new signals");

    assert!(
        matches!(error, HarvestError::Cancelled(message) if message.contains("no more signals"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signal_insert_waits_for_concurrent_cancellation_lock() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut locker = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect locker to Postgres container");
    let mut sender = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect sender to Postgres container");
    let exec_id = start_test_workflow(&mut locker).await;

    sql_query("BEGIN")
        .execute(&mut locker)
        .await
        .expect("begin cancellation simulation");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .for_update()
        .select(WorkflowExecution::as_select())
        .first::<WorkflowExecution>(&mut locker)
        .await
        .expect("lock workflow execution row");
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("CANCELLED"),
            harvest_workflow_executions::error.eq(Some("racing cancel")),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut locker)
        .await
        .expect("stage cancellation update");

    let send_task = tokio::spawn(async move {
        signal::send_signal(
            &mut sender,
            exec_id,
            "approved",
            serde_json::json!({ "approved": true }),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !send_task.is_finished(),
        "signal delivery should wait on the execution row lock before deciding terminal state"
    );

    sql_query("COMMIT")
        .execute(&mut locker)
        .await
        .expect("commit cancellation simulation");

    let error = send_task
        .await
        .expect("signal task should not panic")
        .expect_err("signal should be rejected after cancellation commits");
    assert!(matches!(error, HarvestError::Cancelled(message) if message.contains("racing cancel")));

    let pending = signal::load_pending_signals(&mut locker, exec_id)
        .await
        .expect("pending signal query should succeed");
    assert!(
        pending.is_empty(),
        "cancelled workflow must not receive a signal from the race window"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_activity_heartbeat_observes_workflow_cancellation() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let probe = HeartbeatCancellationProbe {
        activity_started: Arc::new(tokio::sync::Notify::new()),
        activity_saw_cancel: Arc::new(AtomicBool::new(false)),
    };
    let registry = heartbeat_registry(probe.clone());
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "heartbeat-cancel-worker".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
            },
            registry,
        )
        .expect("worker config should be valid"),
    );
    let worker_task = {
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        tokio::spawn(async move {
            worker.run(&pool).await;
        })
    };

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");
    let exec_id = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "heartbeat_workflow",
            workflow_id: "heartbeat-cancel-001",
            exec_id: autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0)),
            input: serde_json::json!({ "request_id": "heartbeat-cancel-001" }),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
        },
    )
    .await
    .expect("workflow start should succeed")
    .exec_id;

    tokio::time::timeout(Duration::from_secs(10), probe.activity_started.notified())
        .await
        .expect("activity should start before cancellation");

    cancel_workflow_execution(
        &mut conn,
        exec_id,
        "operator cancelled while activity was running",
    )
    .await
    .expect("workflow cancellation should succeed");

    tokio::time::timeout(Duration::from_secs(10), async {
        while !probe.activity_saw_cancel.as_ref().load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("activity heartbeat should observe cancellation");

    worker.shutdown();
    worker_task.await.expect("worker should stop cleanly");
}

#[derive(Clone)]
struct UncooperativeActivityProbe {
    activity_started: Arc<tokio::sync::Notify>,
    activity_aborted_early: Arc<AtomicBool>,
}

fn uncooperative_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("uncooperative_activity", input, "default")
            .await
            .map_err(|error| error.to_string())
    })
}

fn uncooperative_activity<'a>(
    ctx: &'a ActivityContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let probe = ctx
            .state::<UncooperativeActivityProbe>()
            .expect("probe state must be registered")
            .clone();
        probe.activity_started.notify_waiters();

        // Simulate an activity that never heartbeats and never consults
        // `ctx.is_cancelled()`. We await a long sleep that the worker must
        // interrupt by dropping the future after the grace period expires.
        tokio::time::sleep(Duration::from_secs(30)).await;

        // If the worker dropped the future during its long sleep this flag
        // will remain true. Setting it to false means the grace-period abort
        // did not actually stop us from running to completion.
        probe
            .activity_aborted_early
            .as_ref()
            .store(false, Ordering::SeqCst);
        Ok(serde_json::json!({ "ran_to_completion": true }))
    })
}

fn uncooperative_registry(probe: UncooperativeActivityProbe) -> Arc<HandlerRegistry> {
    let mut state: HashMap<TypeId, Box<dyn Any + Send + Sync>> = HashMap::new();
    state.insert(TypeId::of::<UncooperativeActivityProbe>(), Box::new(probe));

    Arc::new(HandlerRegistry::with_state(
        vec![autumn_harvest::info::WorkflowInfo {
            name: "uncooperative_workflow",
            module: "cancellation_tests",
            handler: uncooperative_workflow,
        }],
        vec![autumn_harvest::info::ActivityInfo {
            name: "uncooperative_activity",
            module: "cancellation_tests",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            handler: uncooperative_activity,
        }],
        Arc::new(state),
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncooperative_activity_is_hard_aborted_after_grace_period() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let probe = UncooperativeActivityProbe {
        activity_started: Arc::new(tokio::sync::Notify::new()),
        activity_aborted_early: Arc::new(AtomicBool::new(true)),
    };
    let registry = uncooperative_registry(probe.clone());
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "uncooperative-worker".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(2),
                // Short grace period so the test completes quickly.
                cancellation_grace_period: Duration::from_millis(500),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
            },
            registry,
        )
        .expect("worker config should be valid"),
    );
    let worker_task = {
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        tokio::spawn(async move {
            worker.run(&pool).await;
        })
    };

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");
    let exec_id = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "uncooperative_workflow",
            workflow_id: "uncooperative-001",
            exec_id: autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0)),
            input: serde_json::json!({ "request_id": "uncooperative-001" }),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
        },
    )
    .await
    .expect("workflow start should succeed")
    .exec_id;

    tokio::time::timeout(Duration::from_secs(10), probe.activity_started.notified())
        .await
        .expect("activity should start before cancellation");

    cancel_workflow_execution(&mut conn, exec_id, "operator hard-stop")
        .await
        .expect("workflow cancellation should succeed");

    // Give the worker time to: observe cancellation (up to ~500ms poll),
    // cancel the token, elapse the 500ms grace period, then abort the
    // handler and record the activity failure.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let tasks = load_tasks(&mut conn, exec_id).await;
            if tasks
                .iter()
                .any(|task| task.task_type == "activity" && task.state == "FAILED")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("activity task should be failed by the worker within the grace window");

    let tasks = load_tasks(&mut conn, exec_id).await;
    let activity_task = tasks
        .iter()
        .find(|task| task.task_type == "activity")
        .expect("activity task should exist");
    assert_eq!(activity_task.state, "FAILED");
    let activity_error = activity_task.error.as_deref().unwrap_or_default();
    assert!(
        activity_error.contains("cancellation grace period")
            || activity_error.contains("workflow cancelled"),
        "activity error should indicate cancellation, got: {activity_error}"
    );

    assert!(
        probe.activity_aborted_early.as_ref().load(Ordering::SeqCst),
        "uncooperative activity should have been hard-aborted before it ran to completion"
    );

    worker.shutdown();
    worker_task.await.expect("worker should stop cleanly");
}
