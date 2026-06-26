#![cfg(feature = "db")]

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::{TaskQueueItem, WorkflowExecution};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    HarvestError, Priority, StartWorkflowParams, WorkflowContext, cancel_workflow_execution,
    start_or_load_workflow_execution,
};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

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
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
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
    include_str!("../migrations/20260615000001_harvest_context_headers/up.sql")
);

async fn setup_test_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
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

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

fn delay_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(input) })
}

fn delay_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![autumn_harvest::info::WorkflowInfo {
            name: "delay_workflow",
            module: "delayed_start_tests",
            handler: delay_workflow,
            execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ))
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

#[tokio::test]
async fn test_delayed_start_validation() {
    let (mut conn, _container) = setup_test_db().await;

    // 1. Conflicting parameters (both start_at and delay specified)
    let err = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "delay_workflow",
            workflow_id: "conflict-001",
            exec_id: autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0)),
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: Some(chrono::Utc::now() + chrono::Duration::seconds(10)),
            delay: Some(chrono::Duration::seconds(10)),
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
        },
    )
    .await
    .unwrap_err();

    assert!(
        matches!(err, HarvestError::Config(ref msg) if msg.contains("Cannot specify both start_at and delay")),
        "Expected config error about conflicting parameters, got: {err:?}",
    );

    // 2. Exceeding maximum permissible delay
    // We configure WorkerConfig or retrieve from it. To test execution.rs directly:
    // Actually, execution.rs validates against the configured max delay of the WorkerConfig.
    // In our implementation plan, HarvestBuilder or WorkerConfig holds the configured delay.
    // For unit/core tests, we want to see that exceeding the cap fails.

    // 3. Past start_at timestamps are rejected
    let err = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "delay_workflow",
            workflow_id: "past-001",
            exec_id: autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0)),
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: Some(chrono::Utc::now() - chrono::Duration::seconds(10)),
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
        },
    )
    .await
    .unwrap_err();

    assert!(
        matches!(err, HarvestError::Config(ref msg) if msg.contains("Requested start_at is in the past")),
        "Expected config error about past start_at, got: {err:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn test_delayed_start_no_premature_dispatch() {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let pool = build_test_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let registry = delay_registry();
    let worker = Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "delay-worker".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(50),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                sharded_pool: None,
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

    let exec_id = autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let delay_duration = chrono::Duration::seconds(3);

    let _started = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "delay_workflow",
            workflow_id: "delay-001",
            exec_id,
            input: serde_json::json!({ "val": 42 }),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: Some(delay_duration),
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
        },
    )
    .await
    .expect("should start successfully");

    // Immediately verify it is RUNNING, and task is PENDING
    let exec = load_execution(&mut conn, exec_id).await;
    assert_eq!(exec.state, "RUNNING");

    let tasks = load_tasks(&mut conn, exec_id).await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].state, "PENDING");

    // Scheduled at should be approximately 3 seconds in the future
    let scheduled_at = tasks[0].scheduled_at;
    let now = chrono::Utc::now();
    assert!(scheduled_at > now + chrono::Duration::seconds(1));
    assert!(scheduled_at <= now + chrono::Duration::seconds(4));

    // Verify worker hasn't claimed it immediately
    tokio::time::sleep(Duration::from_millis(500)).await;
    let tasks_mid = load_tasks(&mut conn, exec_id).await;
    assert_eq!(tasks_mid.len(), 1);
    assert_eq!(tasks_mid[0].state, "PENDING");

    // Sleep/poll until scheduled time passes and execution finishes (up to 10 seconds)
    let exec_final = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let exec = load_execution(&mut conn, exec_id).await;
            if exec.state == "COMPLETED" {
                break exec;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Workflow should complete within 10 seconds");
    assert_eq!(exec_final.state, "COMPLETED");

    worker.shutdown();
    worker_task.await.expect("worker stopped");
}

#[tokio::test]
async fn test_delayed_start_cancel_before_firing() {
    let (mut conn, _container) = setup_test_db().await;

    let exec_id = autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "delay_workflow",
            workflow_id: "delay-cancel-001",
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: Some(chrono::Duration::seconds(10)),
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
        },
    )
    .await
    .expect("should start successfully");

    // Verify task exists and is pending
    let tasks = load_tasks(&mut conn, exec_id).await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].state, "PENDING");

    // Cancel workflow execution
    let cancelled = cancel_workflow_execution(
        &mut conn,
        exec_id,
        "user request before start",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("should cancel");

    assert_eq!(cancelled.state, "CANCELLED");
    assert!(cancelled.newly_cancelled);
    // Since task has not fired/started, it was deleted entirely from the queue.
    // Our design says: "If the row was successfully deleted, we know the workflow was cancelled *before* it fired.
    // In this case, we avoid calling queue::fail_open_tasks_for_execution, set execution state to CANCELLED, and append event"
    // So tasks list should be completely empty for this execution.
    let tasks_post = load_tasks(&mut conn, exec_id).await;
    assert!(
        tasks_post.is_empty(),
        "Pending task should have been deleted"
    );

    // Verify execution row
    let exec = load_execution(&mut conn, exec_id).await;
    assert_eq!(exec.state, "CANCELLED");
    assert_eq!(exec.error.as_deref(), Some("user request before start"));

    // Verify history events
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    assert!(matches!(
        history.events.as_slice(),
        [
            WorkflowEvent::WorkflowStarted { .. },
            WorkflowEvent::WorkflowCancelled { reason }
        ] if reason == "user request before start"
    ));
}

#[tokio::test]
async fn test_delayed_start_workflow_started_event_timestamp() {
    let (mut conn, _container) = setup_test_db().await;

    let exec_id = autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let target_future = chrono::Utc::now() + chrono::Duration::hours(2);

    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "delay_workflow",
            workflow_id: "delay-timestamp-001",
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: Some(target_future),
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
        },
    )
    .await
    .expect("should start successfully");

    // Verify history events
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    match history.events.as_slice() {
        [WorkflowEvent::WorkflowStarted { timestamp, .. }] => {
            // Timestamp should be exactly target_future (stamped with delayed time)
            assert_eq!(*timestamp, target_future);
        }
        other => panic!("expected one WorkflowStarted event, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_immediate_start_skew_tolerance() {
    let (mut conn, _container) = setup_test_db().await;

    let exec_id = autumn_harvest::ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let now = chrono::Utc::now();

    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "delay_workflow",
            workflow_id: "immediate-skew-001",
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
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
        },
    )
    .await
    .expect("should start successfully");

    let tasks = load_tasks(&mut conn, exec_id).await;
    assert_eq!(tasks.len(), 1);

    // Scheduled_at should be approximately now - 5 seconds to tolerate skew
    let scheduled_at = tasks[0].scheduled_at;
    let expected_skew_scheduled = now - chrono::Duration::seconds(5);

    // It should be within a 2 second window of expected_skew_scheduled
    let diff = (scheduled_at - expected_skew_scheduled).num_seconds().abs();
    assert!(
        diff <= 2,
        "scheduled_at={scheduled_at:?} expected_skew_scheduled={expected_skew_scheduled:?}"
    );
}

#[test]
fn test_builder_honors_worker_config_max_start_delay() {
    use autumn_harvest::builder::{HarvestBuilder, WorkerConfig};
    use std::time::Duration;

    let custom_delay = Duration::from_secs(12345);
    let worker_config = WorkerConfig::default().with_max_workflow_start_delay(custom_delay);

    let built = HarvestBuilder::new()
        .worker(worker_config)
        .try_build()
        .expect("build should succeed");

    assert_eq!(built.worker_config().max_workflow_start_delay, custom_delay);
    assert_eq!(built.max_workflow_start_delay, custom_delay);
}
