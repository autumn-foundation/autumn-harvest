// Integration tests for ParentClosePolicy and detached child workflow spawns
// (issue #347). All tests use testcontainers so the `db` feature is required.
#![cfg(feature = "db")]
#![allow(clippy::items_after_statements)]

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use autumn_harvest::concurrency::ConcurrencyPolicy;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::types::ParentClosePolicy;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, Priority, ShardId, StartWorkflowParams, TelemetryConfig, TraceContextCarrier,
    TraceContextPropagator, WorkflowContext, WorkflowHistoryPolicy, WorkflowResetRequest,
    cancel_workflow_execution, reset_workflow_execution, start_or_load_workflow_execution, timeout,
};
use chrono::Utc;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

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
    include_str!("../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../migrations/20260504000000_harvest_workflow_parent_children/up.sql"),
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

async fn setup_test_db_url() -> (String, ContainerAsync<Postgres>) {
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
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

fn make_worker(registry: Arc<HandlerRegistry>) -> Worker {
    Worker::new(
        WorkerRuntimeConfig {
            worker_id: uuid::Uuid::new_v4().to_string(),
            queues: vec!["default".to_string()],
            notification_database_url: None,
            max_concurrent_workflows: 10,
            max_concurrent_activities: 20,
            poll_interval: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(2),
            cancellation_grace_period: Duration::from_secs(2),
            sticky_timeout: Duration::ZERO,
            max_local_activity_start_to_close: Duration::from_secs(60),
            shard_assignments: vec![ShardId::new(0)],
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: String::new(),
            deployment_name: None,
            workflow_cache_size: 100,
            priority_aging_secs: None,
            unknown_target_grace_window: Duration::from_secs(5),
            poison_pill_threshold: 3,
            max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
            #[cfg(feature = "db")]
            labels: std::collections::HashMap::new(),
            max_workflow_history_events: None,
            sharded_pool: None,
        },
        registry,
    )
    .expect("worker should build")
}

async fn start_workflow(
    conn: &mut AsyncPgConnection,
    name: &str,
    id: &str,
    input: Value,
) -> ExecutionId {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: name,
            workflow_id: id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input,
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .expect("workflow start should succeed")
    .exec_id
}

/// Load execution state by ID.
async fn get_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    use autumn_harvest::schema::harvest_workflow_executions;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::state)
        .first::<String>(conn)
        .await
        .expect("execution must exist")
}

/// Poll until the execution reaches one of the expected states.
async fn wait_for_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId, states: &[&str]) {
    for _ in 0..300 {
        let state = get_state(conn, exec_id).await;
        if states.contains(&state.as_str()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let state = get_state(conn, exec_id).await;
    panic!("execution {exec_id} never reached {states:?}; current state: {state}");
}

async fn wait_for_workflow_task_parked(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    use autumn_harvest::schema::harvest_task_queue;
    use diesel::ExpressionMethods;
    use diesel::OptionalExtension;
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;

    for _ in 0..300 {
        if let Some((state, worker_id, started_at)) = harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .filter(harvest_task_queue::task_type.eq("workflow"))
            .select((
                harvest_task_queue::state,
                harvest_task_queue::worker_id,
                harvest_task_queue::started_at,
            ))
            .first::<(String, Option<String>, Option<chrono::DateTime<Utc>>)>(conn)
            .await
            .optional()
            .expect("workflow task query failed")
            && state == "RUNNING"
            && worker_id.is_none()
            && started_at.is_none()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!("workflow task for {exec_id} never parked");
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        name,
        module: "child_policy_tests",
        handler,
        execution_timeout: None,
        sla: None,
        concurrency: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
    }
}

fn wf_info_with_concurrency(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
    concurrency: ConcurrencyPolicy,
) -> WorkflowInfo {
    WorkflowInfo {
        name,
        module: "child_policy_tests",
        handler,
        execution_timeout: None,
        sla: None,
        concurrency: Some(concurrency),
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
    }
}

fn local_activity_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "child_policy_tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        is_local: true,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        requires: None,
        handler,
    }
}

struct FixedTracePropagator;

impl TraceContextPropagator for FixedTracePropagator {
    fn capture(&self) -> Option<TraceContextCarrier> {
        Some(TraceContextCarrier::from_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        ))
    }

    fn install(&self, _carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
        Box::new(())
    }
}

async fn load_history_events(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Vec<WorkflowEvent> {
    autumn_harvest::store::load_history(conn, exec_id)
        .await
        .expect("history should load")
        .events
}

async fn insert_detached_child_execution(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
    workflow_name: &'static str,
    workflow_id: &'static str,
    policy: ParentClosePolicy,
) -> ExecutionId {
    use autumn_harvest::schema::harvest_workflow_executions;
    use diesel_async::RunQueryDsl;

    let child_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&NewWorkflowExecution {
            id: child_exec_id.as_uuid(),
            workflow_name,
            workflow_id,
            run_id: uuid::Uuid::new_v4(),
            shard_id: 0,
            input: Value::Null,
            parent_id: Some(parent_exec_id.as_uuid()),
            queue_name: "default",
            execution_timeout: None,
            deadline_at: None,
            memo: None,
            search_attrs: None,
            assigned_build_id: None,
            parent_close_policy: Some(policy.as_str().to_string()),

            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,

            sla_deadline_at: None,
            schedule_id: None,
            scheduled_for: None,
        })
        .execute(conn)
        .await
        .expect("detached child row should insert");

    autumn_harvest::store::append_events(
        conn,
        child_exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        }],
        0,
    )
    .await
    .expect("detached child started event should append");

    child_exec_id
}

async fn force_parent_close_transition_to_lose(
    conn: &mut AsyncPgConnection,
    child_exec_id: ExecutionId,
) {
    let suffix = child_exec_id.as_uuid().simple().to_string();
    let sql = format!(
        r"
CREATE OR REPLACE FUNCTION skip_parent_close_transition_{suffix}()
RETURNS trigger AS $$
BEGIN
    IF OLD.id = '{child_id}'::uuid
       AND OLD.state = 'RUNNING'
       AND NEW.state IN ('CANCELLED', 'FAILED')
       AND NEW.error IN ('parent closed', 'ParentClosed') THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER skip_parent_close_transition_{suffix}
BEFORE UPDATE ON harvest_workflow_executions
FOR EACH ROW
EXECUTE FUNCTION skip_parent_close_transition_{suffix}();
",
        suffix = suffix,
        child_id = child_exec_id.as_uuid(),
    );

    conn.batch_execute(&sql)
        .await
        .expect("race trigger should install");
}

async fn wait_for_child_execution(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
) -> ExecutionId {
    use autumn_harvest::schema::harvest_workflow_executions;
    use diesel::ExpressionMethods;
    use diesel::OptionalExtension;
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;

    for _ in 0..100 {
        if let Some(child_uuid) = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
            .select(harvest_workflow_executions::id)
            .first::<uuid::Uuid>(conn)
            .await
            .optional()
            .expect("child query failed")
        {
            return ExecutionId::from_uuid(child_uuid);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!("parent {parent_exec_id} never spawned a detached child");
}

async fn wait_for_child_task_trace_context(
    conn: &mut AsyncPgConnection,
    child_exec_id: ExecutionId,
) -> Option<Value> {
    use autumn_harvest::schema::harvest_task_queue;
    use diesel::ExpressionMethods;
    use diesel::OptionalExtension;
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;

    for _ in 0..100 {
        if let Some(trace_context) = harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(child_exec_id.as_uuid())))
            .select(harvest_task_queue::trace_context)
            .first::<Option<Value>>(conn)
            .await
            .optional()
            .expect("child task trace query failed")
        {
            return trace_context;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!("detached child {child_exec_id} never received a workflow task");
}

async fn wait_for_child_task_concurrency(
    conn: &mut AsyncPgConnection,
    child_exec_id: ExecutionId,
) -> (Option<String>, Option<i32>) {
    use autumn_harvest::schema::harvest_task_queue;
    use diesel::ExpressionMethods;
    use diesel::OptionalExtension;
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;

    for _ in 0..100 {
        if let Some(policy) = harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(child_exec_id.as_uuid())))
            .select((
                harvest_task_queue::concurrency_key,
                harvest_task_queue::concurrency_cap,
            ))
            .first::<(Option<String>, Option<i32>)>(conn)
            .await
            .optional()
            .expect("child task query failed")
        {
            return policy;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!("detached child {child_exec_id} never received a workflow task");
}

// ── Test 1: Detached child with Abandon policy outlives parent completion ─────

static CHILD_WAS_STARTED: AtomicBool = AtomicBool::new(false);

#[tokio::test]
async fn child_workflow_detached_abandon_outlives_parent_completion() {
    CHILD_WAS_STARTED.store(false, Ordering::SeqCst);

    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);

    // Parent spawns a detached child with Abandon policy and completes immediately.
    fn parent_wf<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let _child_id = ctx
                .spawn_child_workflow_detached_raw(
                    "long_running_child",
                    Value::Null,
                    ParentClosePolicy::Abandon,
                )
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!("parent_done"))
        })
    }

    // Child runs a timer to simulate long work, then sets a flag.
    fn long_running_child_wf<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            CHILD_WAS_STARTED.store(true, Ordering::SeqCst);
            ctx.timer("short_wait", 1)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!("child_done"))
        })
    }

    let registry = Arc::new(HandlerRegistry::new(
        vec![
            wf_info("parent_abandon", parent_wf),
            wf_info("long_running_child", long_running_child_wf),
        ],
        vec![],
    ));

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "parent_abandon",
        "abandon-parent-001",
        Value::Null,
    )
    .await;

    // Start a worker and let it run.
    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(15), worker_ref.run(&worker_pool)).await;
    });

    // Parent should complete.
    wait_for_state(&mut conn, parent_exec_id, &["COMPLETED"]).await;

    // Wait a bit to give the child time to start.
    tokio::time::sleep(Duration::from_secs(3)).await;

    worker_handle.abort();

    // Child was started (parent_close_policy = Abandon → no cascade).
    assert!(
        CHILD_WAS_STARTED.load(Ordering::SeqCst),
        "detached child should have been started"
    );
}

#[tokio::test]
async fn detached_child_workflow_uses_registered_concurrency_policy() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);

    fn parent_wf<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let _child_id = ctx
                .spawn_child_workflow_detached_raw(
                    "capped_detached_child",
                    serde_json::json!({ "tenant_id": "acme" }),
                    ParentClosePolicy::Abandon,
                )
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!("parent_done"))
        })
    }

    fn capped_child_wf<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.timer("hold", 3600).await.map_err(|e| e.to_string())?;
            Ok(serde_json::json!("child_done"))
        })
    }

    let registry = Arc::new(HandlerRegistry::new(
        vec![
            wf_info("parent_spawns_capped_detached", parent_wf),
            wf_info_with_concurrency(
                "capped_detached_child",
                capped_child_wf,
                ConcurrencyPolicy {
                    key_expr: "input.tenant_id",
                    limit: 2,
                },
            ),
        ],
        vec![],
    ));

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "parent_spawns_capped_detached",
        "detached-concurrency-parent-001",
        Value::Null,
    )
    .await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), worker_ref.run(&worker_pool)).await;
    });

    wait_for_state(&mut conn, parent_exec_id, &["COMPLETED"]).await;
    let child_exec_id = wait_for_child_execution(&mut conn, parent_exec_id).await;
    let (concurrency_key, concurrency_cap) =
        wait_for_child_task_concurrency(&mut conn, child_exec_id).await;

    worker_handle.abort();

    assert_eq!(concurrency_key.as_deref(), Some("acme"));
    assert_eq!(concurrency_cap, Some(2));
}

#[tokio::test]
async fn detached_child_continue_as_new_failure_does_not_wake_parent() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);

    fn parent_waits<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.spawn_child_workflow_detached_raw(
                "detached_continue_as_new_child",
                serde_json::json!({ "phase": "init" }),
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;
            let _signal = ctx
                .wait_for_signal("release")
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!("parent_done"))
        })
    }

    fn child_continues<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let _ = ctx
                .continue_as_new(serde_json::json!({ "phase": "next" }))
                .await;
            unreachable!("continue_as_new must not resolve")
        })
    }

    let registry = Arc::new(HandlerRegistry::new(
        vec![
            wf_info("detached_continue_parent", parent_waits),
            wf_info("detached_continue_as_new_child", child_continues),
        ],
        vec![],
    ));

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "detached_continue_parent",
        "detached-continue-parent-001",
        Value::Null,
    )
    .await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), worker_ref.run(&worker_pool)).await;
    });

    let child_exec_id = wait_for_child_execution(&mut conn, parent_exec_id).await;
    wait_for_state(&mut conn, child_exec_id, &["FAILED"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    worker_handle.abort();

    let parent_history = load_history_events(&mut conn, parent_exec_id).await;
    assert!(
        !parent_history
            .iter()
            .any(|event| matches!(event, WorkflowEvent::ChildWorkflowFailed { .. })),
        "detached child continue-as-new failure must not be recorded on parent history: {parent_history:?}"
    );
    assert_eq!(get_state(&mut conn, parent_exec_id).await, "RUNNING");
}

#[tokio::test]
async fn signal_then_detached_only_remainder_persists_spawn_and_completes() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);

    fn target_waits<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let payload = ctx
                .wait_for_signal("poke")
                .await
                .map_err(|e| e.to_string())?;
            Ok(payload)
        })
    }

    fn parent_signals_after_detached<'a>(
        ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let target = ExecutionId::from_uuid(
                uuid::Uuid::parse_str(input["target"].as_str().ok_or("missing target")?)
                    .map_err(|e| e.to_string())?,
            );
            ctx.spawn_child_workflow_detached_raw(
                "signal_remainder_detached_child",
                Value::Null,
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;
            ctx.signal_external_workflow(target, "poke", serde_json::json!({ "ok": true }))
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!("parent_done"))
        })
    }

    fn long_child<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.timer("hold", 3600).await.map_err(|e| e.to_string())?;
            Ok(serde_json::json!("child_done"))
        })
    }

    let registry = Arc::new(HandlerRegistry::new(
        vec![
            wf_info("signal_target_waits", target_waits),
            wf_info("signal_then_detached_parent", parent_signals_after_detached),
            wf_info("signal_remainder_detached_child", long_child),
        ],
        vec![],
    ));

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let target_exec_id = start_workflow(
        &mut conn,
        "signal_target_waits",
        "signal-target-001",
        Value::Null,
    )
    .await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), worker_ref.run(&worker_pool)).await;
    });

    wait_for_workflow_task_parked(&mut conn, target_exec_id).await;
    let parent_exec_id = start_workflow(
        &mut conn,
        "signal_then_detached_parent",
        "signal-detached-parent-001",
        serde_json::json!({ "target": target_exec_id.as_uuid().to_string() }),
    )
    .await;

    wait_for_state(&mut conn, parent_exec_id, &["COMPLETED"]).await;
    wait_for_state(&mut conn, target_exec_id, &["COMPLETED"]).await;
    let child_exec_id = wait_for_child_execution(&mut conn, parent_exec_id).await;
    assert_eq!(get_state(&mut conn, child_exec_id).await, "RUNNING");

    worker_handle.abort();
}

#[tokio::test]
async fn detached_spawn_before_local_activity_is_persisted() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);

    fn parent_runs_local<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.spawn_child_workflow_detached_raw(
                "local_detached_child",
                Value::Null,
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;
            let local = ctx
                .execute_local_activity_raw("local_ping", Value::Null, None, None)
                .await
                .map_err(|e| e.to_string())?;
            Ok(local)
        })
    }

    fn local_detached_child<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.timer("hold", 3600).await.map_err(|e| e.to_string())?;
            Ok(serde_json::json!("child_done"))
        })
    }

    fn local_ping<'a>(
        _ctx: &'a autumn_harvest::ActivityContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Ok(serde_json::json!("pong")) })
    }

    let registry = Arc::new(HandlerRegistry::new(
        vec![
            wf_info("local_detached_parent", parent_runs_local),
            wf_info("local_detached_child", local_detached_child),
        ],
        vec![local_activity_info("local_ping", local_ping)],
    ));

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "local_detached_parent",
        "local-detached-parent-001",
        Value::Null,
    )
    .await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(20), worker_ref.run(&worker_pool)).await;
    });

    wait_for_state(&mut conn, parent_exec_id, &["COMPLETED"]).await;
    let child_exec_id = wait_for_child_execution(&mut conn, parent_exec_id).await;
    assert_eq!(get_state(&mut conn, child_exec_id).await, "RUNNING");

    worker_handle.abort();
}

#[tokio::test]
async fn detached_child_execution_timeout_does_not_wake_parent() {
    use autumn_harvest::schema::harvest_workflow_executions;
    use chrono::Utc;
    use diesel_async::RunQueryDsl;

    let (url, _container) = setup_test_db_url().await;
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "manual_timeout_parent",
        "manual-timeout-parent-001",
        Value::Null,
    )
    .await;
    let child_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let child_workflow_id = child_exec_id.to_string();
    let timeout = chrono::Duration::milliseconds(1);

    diesel::insert_into(harvest_workflow_executions::table)
        .values(&NewWorkflowExecution {
            id: child_exec_id.as_uuid(),
            workflow_name: "manual_timeout_child",
            workflow_id: &child_workflow_id,
            run_id: uuid::Uuid::new_v4(),
            shard_id: 0,
            input: Value::Null,
            parent_id: Some(parent_exec_id.as_uuid()),
            queue_name: "default",
            execution_timeout: Some(timeout),
            deadline_at: Some(Utc::now() - chrono::Duration::seconds(1)),
            memo: None,
            search_attrs: None,
            assigned_build_id: None,
            parent_close_policy: Some(ParentClosePolicy::Abandon.as_str().to_string()),

            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,

            sla_deadline_at: None,
            schedule_id: None,
            scheduled_for: None,
        })
        .execute(&mut conn)
        .await
        .expect("child row should insert");
    autumn_harvest::store::append_events(
        &mut conn,
        child_exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        }],
        0,
    )
    .await
    .expect("child started event should append");

    let enforced =
        timeout::enforce_workflow_execution_timeouts(&mut conn, &autumn_harvest::NoOpMetrics)
            .await
            .expect("timeout enforcement should succeed");

    assert_eq!(enforced, 1);
    wait_for_state(&mut conn, child_exec_id, &["TIMED_OUT"]).await;
    let parent_history = load_history_events(&mut conn, parent_exec_id).await;
    assert!(
        !parent_history
            .iter()
            .any(|event| matches!(event, WorkflowEvent::ChildWorkflowFailed { .. })),
        "detached child timeout must not be recorded on parent history: {parent_history:?}"
    );
}

#[tokio::test]
async fn terminal_detached_spawn_setup_error_fails_workflow() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);

    fn parent_spawns_missing_detached<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.spawn_child_workflow_detached_raw(
                "missing_detached_child",
                Value::Null,
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;
            Ok(Value::Null)
        })
    }

    let registry = Arc::new(HandlerRegistry::new(
        vec![wf_info(
            "terminal_missing_detached_parent",
            parent_spawns_missing_detached,
        )],
        vec![],
    ));

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "terminal_missing_detached_parent",
        "terminal-missing-detached-parent-001",
        Value::Null,
    )
    .await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), worker_ref.run(&worker_pool)).await;
    });

    wait_for_state(&mut conn, parent_exec_id, &["FAILED"]).await;
    worker_handle.abort();

    let parent_history = load_history_events(&mut conn, parent_exec_id).await;
    assert!(
        parent_history.iter().any(|event| {
            matches!(
                event,
                WorkflowEvent::WorkflowFailed { error }
                    if error.contains("no workflow handler registered for 'missing_detached_child'")
            )
        }),
        "missing detached child handler should be recorded as workflow failure: {parent_history:?}"
    );
}

#[tokio::test]
async fn parent_terminate_cascade_applies_child_descendant_policy() {
    let (url, _container) = setup_test_db_url().await;

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "manual_parent_for_descendant_cascade",
        "manual-parent-descendant-cascade-001",
        Value::Null,
    )
    .await;
    let child_exec_id = insert_detached_child_execution(
        &mut conn,
        parent_exec_id,
        "manual_child_for_descendant_cascade",
        "manual-child-descendant-cascade-001",
        ParentClosePolicy::Terminate,
    )
    .await;
    let grandchild_exec_id = insert_detached_child_execution(
        &mut conn,
        child_exec_id,
        "manual_grandchild_for_descendant_cascade",
        "manual-grandchild-descendant-cascade-001",
        ParentClosePolicy::RequestCancel,
    )
    .await;

    cancel_workflow_execution(
        &mut conn,
        parent_exec_id,
        "operator closed parent",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("parent cancellation should cascade");

    wait_for_state(&mut conn, child_exec_id, &["FAILED"]).await;
    wait_for_state(&mut conn, grandchild_exec_id, &["CANCELLED"]).await;
}

#[tokio::test]
async fn cascade_records_child_events_only_after_winning_child_transition() {
    let (url, _container) = setup_test_db_url().await;

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "manual_parent_for_lost_cascade",
        "manual-parent-lost-cascade-001",
        Value::Null,
    )
    .await;
    let child_exec_id = insert_detached_child_execution(
        &mut conn,
        parent_exec_id,
        "manual_child_for_lost_cascade",
        "manual-child-lost-cascade-001",
        ParentClosePolicy::RequestCancel,
    )
    .await;
    force_parent_close_transition_to_lose(&mut conn, child_exec_id).await;

    cancel_workflow_execution(
        &mut conn,
        parent_exec_id,
        "operator closed parent",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("parent cancellation should succeed despite lost child transition");

    let child_history = load_history_events(&mut conn, child_exec_id).await;
    assert!(
        !child_history
            .iter()
            .any(|event| matches!(event, WorkflowEvent::WorkflowCancelled { .. })),
        "cascade must not append child cancellation when the guarded child update affects zero rows: {child_history:?}"
    );

    let parent_history = load_history_events(&mut conn, parent_exec_id).await;
    assert!(
        !parent_history.iter().any(|event| matches!(
            event,
            WorkflowEvent::ChildWorkflowCascadeApplied { child_id, .. } if *child_id == child_exec_id
        )),
        "parent must not record a cascade that did not transition the child: {parent_history:?}"
    );
}

#[tokio::test]
async fn workflow_reset_cascades_detached_children() {
    let (url, _container) = setup_test_db_url().await;

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "manual_reset_parent",
        "manual-reset-parent-001",
        Value::Null,
    )
    .await;
    let child_exec_id = insert_detached_child_execution(
        &mut conn,
        parent_exec_id,
        "manual_reset_child",
        "manual-reset-child-001",
        ParentClosePolicy::RequestCancel,
    )
    .await;

    reset_workflow_execution(
        &mut conn,
        parent_exec_id,
        WorkflowResetRequest {
            reset_to_event_id: 0,
            reason: "operator reset".to_string(),
            operator_id: "test-operator".to_string(),
            signal_reapply: autumn_harvest::ResetSignalReapplyPolicy::default(),
            allow_terminal_source: false,
        },
        None,
    )
    .await
    .expect("reset should succeed");

    wait_for_state(&mut conn, parent_exec_id, &["TERMINATED"]).await;
    wait_for_state(&mut conn, child_exec_id, &["CANCELLED"]).await;
}

#[tokio::test]
async fn workflow_task_timeout_cascades_detached_children() {
    let (url, _container) = setup_test_db_url().await;

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "manual_task_timeout_parent",
        "manual-task-timeout-parent-001",
        Value::Null,
    )
    .await;
    let child_exec_id = insert_detached_child_execution(
        &mut conn,
        parent_exec_id,
        "manual_task_timeout_child",
        "manual-task-timeout-child-001",
        ParentClosePolicy::RequestCancel,
    )
    .await;

    {
        use autumn_harvest::schema::harvest_task_queue;
        use diesel::ExpressionMethods;
        use diesel::QueryDsl;
        use diesel_async::RunQueryDsl;

        diesel::update(
            harvest_task_queue::table
                .filter(harvest_task_queue::workflow_exec_id.eq(Some(parent_exec_id.as_uuid())))
                .filter(harvest_task_queue::task_type.eq("workflow")),
        )
        .set((
            harvest_task_queue::scheduled_at.eq(Utc::now() - chrono::Duration::seconds(5)),
            harvest_task_queue::schedule_to_start.eq(Some(chrono::Duration::milliseconds(1))),
        ))
        .execute(&mut conn)
        .await
        .expect("parent workflow task should be made timeout-eligible");
    }

    let sharded_pool = Option::<autumn_harvest::ShardedDbPool>::None;
    let enforced = timeout::enforce_timeouts_once(
        &mut conn,
        &autumn_harvest::NoOpMetrics,
        Duration::from_secs(5),
        &sharded_pool,
        &[ShardId::new(0)],
        None,
        None,
    )
    .await
    .expect("timeout enforcement should succeed");

    assert!(enforced >= 1);
    wait_for_state(&mut conn, parent_exec_id, &["TIMED_OUT"]).await;
    wait_for_state(&mut conn, child_exec_id, &["CANCELLED"]).await;
}

#[tokio::test]
async fn detached_child_task_receives_trace_context() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);

    fn parent_spawns_traced_detached<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.spawn_child_workflow_detached_raw(
                "traced_detached_child",
                Value::Null,
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;
            Ok(Value::Null)
        })
    }

    fn traced_child<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let _ = ctx
                .wait_for_signal("release")
                .await
                .map_err(|e| e.to_string())?;
            Ok(Value::Null)
        })
    }

    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .propagator(Arc::new(FixedTracePropagator))
            .build(),
    );
    let state: autumn_harvest::context::SharedState = Arc::new(HashMap::new());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![
            wf_info("traced_detached_parent", parent_spawns_traced_detached),
            wf_info("traced_detached_child", traced_child),
        ],
        vec![],
        state,
        telemetry,
    ));

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "traced_detached_parent",
        "traced-detached-parent-001",
        Value::Null,
    )
    .await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), worker_ref.run(&worker_pool)).await;
    });

    let child_exec_id = wait_for_child_execution(&mut conn, parent_exec_id).await;
    let trace_context = wait_for_child_task_trace_context(&mut conn, child_exec_id).await;
    worker_handle.abort();

    assert_eq!(
        trace_context
            .and_then(|json| TraceContextCarrier::from_json(&json))
            .and_then(|carrier| carrier.traceparent),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string())
    );
}

// ── Test 2: RequestCancel cascade when parent is cancelled ────────────────────

static CHILD_SAW_CANCEL: AtomicBool = AtomicBool::new(false);

#[tokio::test]
async fn child_workflow_cancel_cascade_request_cancel() {
    CHILD_SAW_CANCEL.store(false, Ordering::SeqCst);

    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);

    // Parent spawns a detached child with RequestCancel policy and waits.
    fn parent_request_cancel<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let _child_id = ctx
                .spawn_child_workflow_detached_raw(
                    "cancellable_child",
                    Value::Null,
                    ParentClosePolicy::RequestCancel,
                )
                .map_err(|e| e.to_string())?;
            // Parent blocks on a very long timer so the operator can cancel it.
            ctx.timer("wait_forever", 3600)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!("never"))
        })
    }

    // Child loops on a short timer and checks cancellation.
    fn cancellable_child_wf<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            for _ in 0..10u32 {
                if ctx.is_cancelled() {
                    CHILD_SAW_CANCEL.store(true, Ordering::SeqCst);
                    return Err("cancelled by cascade".to_string());
                }
                ctx.timer("poll", 1).await.map_err(|e| e.to_string())?;
            }
            Ok(serde_json::json!("completed_normally"))
        })
    }

    let registry = Arc::new(HandlerRegistry::new(
        vec![
            wf_info("parent_request_cancel", parent_request_cancel),
            wf_info("cancellable_child", cancellable_child_wf),
        ],
        vec![],
    ));

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "parent_request_cancel",
        "rc-parent-001",
        Value::Null,
    )
    .await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(20), worker_ref.run(&worker_pool)).await;
    });

    let child_exec_id = wait_for_child_execution(&mut conn, parent_exec_id).await;

    // Operator cancels the parent.
    cancel_workflow_execution(
        &mut conn,
        parent_exec_id,
        "operator cancel test",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel should succeed");

    // Parent should become CANCELLED.
    wait_for_state(&mut conn, parent_exec_id, &["CANCELLED"]).await;
    wait_for_state(&mut conn, child_exec_id, &["CANCELLED", "FAILED"]).await;

    worker_handle.abort();
}

#[tokio::test]
async fn history_cap_failure_cascades_detached_children() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);

    fn parent_waits_for_signal<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let _child_id = ctx
                .spawn_child_workflow_detached_raw(
                    "history_cap_child",
                    Value::Null,
                    ParentClosePolicy::RequestCancel,
                )
                .map_err(|e| e.to_string())?;
            let _signal = ctx
                .wait_for_signal("release")
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!("parent_done"))
        })
    }

    fn history_cap_child_wf<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.timer("long_wait", 3600)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!("child_done"))
        })
    }

    let registry = HandlerRegistry::new(
        vec![
            wf_info("history_cap_parent", parent_waits_for_signal),
            wf_info("history_cap_child", history_cap_child_wf),
        ],
        vec![],
    )
    .with_history_policy(WorkflowHistoryPolicy::default().with_event_hard_cap(3));
    let registry = Arc::new(registry);

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "history_cap_parent",
        "history-cap-parent-001",
        Value::Null,
    )
    .await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), worker_ref.run(&worker_pool)).await;
    });

    let child_exec_id = wait_for_child_execution(&mut conn, parent_exec_id).await;
    autumn_harvest::signal::send_signal(
        &mut conn,
        parent_exec_id,
        "release",
        serde_json::json!({ "ok": true }),
    )
    .await
    .expect("signal should enqueue");

    wait_for_state(&mut conn, parent_exec_id, &["FAILED"]).await;
    wait_for_state(&mut conn, child_exec_id, &["CANCELLED", "FAILED"]).await;

    worker_handle.abort();
}

// ── Test 3: Terminate cascade on parent failure ───────────────────────────────

#[tokio::test]
async fn child_workflow_terminate_cascade_on_parent_failure() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);

    fn parent_fails<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let _child_id = ctx
                .spawn_child_workflow_detached_raw(
                    "child_to_terminate",
                    Value::Null,
                    ParentClosePolicy::Terminate,
                )
                .map_err(|e| e.to_string())?;
            // Parent fails immediately after spawning the child.
            Err("parent_error".to_string())
        })
    }

    fn child_to_terminate_wf<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.timer("long_work", 3600)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!("completed_normally"))
        })
    }

    let registry = Arc::new(HandlerRegistry::new(
        vec![
            wf_info("parent_fails", parent_fails),
            wf_info("child_to_terminate", child_to_terminate_wf),
        ],
        vec![],
    ));

    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    let parent_exec_id = start_workflow(
        &mut conn,
        "parent_fails",
        "terminate-parent-001",
        Value::Null,
    )
    .await;

    let worker = Arc::new(make_worker(registry));
    let worker_pool = pool.clone();
    let worker_ref = worker.clone();
    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(15), worker_ref.run(&worker_pool)).await;
    });

    // Parent should fail.
    wait_for_state(&mut conn, parent_exec_id, &["FAILED"]).await;

    // Give the cascade time to apply.
    tokio::time::sleep(Duration::from_secs(3)).await;

    worker_handle.abort();

    use autumn_harvest::schema::harvest_workflow_executions;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;
    let child_rows: Vec<(String, Option<String>)> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .select((
            harvest_workflow_executions::state,
            harvest_workflow_executions::error,
        ))
        .load(&mut conn)
        .await
        .expect("child query failed");

    assert!(!child_rows.is_empty(), "at least one child should exist");

    // All children with Terminate policy should be in a terminal state.
    let all_terminal = child_rows
        .iter()
        .all(|(s, _)| s == "FAILED" || s == "TERMINATED" || s == "CANCELLED");
    assert!(
        all_terminal,
        "all Terminate-policy children should be terminal after parent failure; rows: {child_rows:?}"
    );

    // At least one child's error should contain "ParentClosed".
    let parent_closed_error = child_rows
        .iter()
        .any(|(_, err)| err.as_deref().is_some_and(|e| e.contains("ParentClosed")));
    assert!(
        parent_closed_error,
        "Terminate-policy child should have ParentClosed error; rows: {child_rows:?}"
    );
}

// ── Unit test: ParentClosePolicy serde round-trip ─────────────────────────────

#[test]
fn parent_close_policy_serde_round_trip() {
    for policy in [
        ParentClosePolicy::Abandon,
        ParentClosePolicy::RequestCancel,
        ParentClosePolicy::Terminate,
    ] {
        let json = serde_json::to_string(&policy).unwrap();
        let back: ParentClosePolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, policy);
    }
}

#[test]
fn parent_close_policy_default_is_request_cancel() {
    assert_eq!(
        ParentClosePolicy::default(),
        ParentClosePolicy::RequestCancel
    );
}

#[test]
fn parent_close_policy_as_str() {
    assert_eq!(ParentClosePolicy::Abandon.as_str(), "abandon");
    assert_eq!(ParentClosePolicy::RequestCancel.as_str(), "request_cancel");
    assert_eq!(ParentClosePolicy::Terminate.as_str(), "terminate");
}

#[test]
fn parent_close_policy_from_str() {
    use std::str::FromStr;
    assert_eq!(
        ParentClosePolicy::from_str("abandon").unwrap(),
        ParentClosePolicy::Abandon
    );
    assert_eq!(
        ParentClosePolicy::from_str("request_cancel").unwrap(),
        ParentClosePolicy::RequestCancel
    );
    assert_eq!(
        ParentClosePolicy::from_str("terminate").unwrap(),
        ParentClosePolicy::Terminate
    );
    assert!(ParentClosePolicy::from_str("unknown").is_err());
}
