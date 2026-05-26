// Integration tests for ParentClosePolicy and detached child workflow spawns
// (issue #347). All tests use testcontainers so the `db` feature is required.
#![cfg(feature = "db")]
#![allow(clippy::items_after_statements)]

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::types::ParentClosePolicy;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, Priority, ShardId, StartWorkflowParams, WorkflowContext,
    cancel_workflow_execution, start_or_load_workflow_execution,
};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::Value;
use testcontainers::ContainerAsync;
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
    include_str!("../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
);

async fn setup_test_db_url() -> (String, ContainerAsync<Postgres>) {
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
            #[cfg(feature = "db")]
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

/// Poll until the execution reaches one of the expected terminal states (up to
/// `max_attempts * 100ms`).
async fn wait_for_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId, states: &[&str]) {
    for _ in 0..100 {
        let state = get_state(conn, exec_id).await;
        if states.contains(&state.as_str()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let state = get_state(conn, exec_id).await;
    panic!("execution {exec_id} never reached {states:?}; current state: {state}");
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        name,
        module: "child_policy_tests",
        handler,
        execution_timeout: None,
        concurrency: None,
        max_input_bytes: None,
    }
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

    // Give the worker time to start both the parent and child.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Operator cancels the parent.
    cancel_workflow_execution(&mut conn, parent_exec_id, "operator cancel test")
        .await
        .expect("cancel should succeed");

    // Parent should become CANCELLED.
    wait_for_state(&mut conn, parent_exec_id, &["CANCELLED"]).await;

    // Give the cascade time to apply.
    tokio::time::sleep(Duration::from_secs(5)).await;

    worker_handle.abort();

    // Find the child execution and check its state.
    use autumn_harvest::schema::harvest_workflow_executions;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;
    let child_states: Vec<String> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .select(harvest_workflow_executions::state)
        .load::<String>(&mut conn)
        .await
        .expect("child query failed");

    assert!(!child_states.is_empty(), "at least one child should exist");

    // At least one child should be CANCELLED or FAILED (cascade applied).
    let cascade_applied = child_states
        .iter()
        .any(|s| s == "CANCELLED" || s == "FAILED");
    assert!(
        cascade_applied,
        "cascade should have cancelled/failed the child; states: {child_states:?}"
    );
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
