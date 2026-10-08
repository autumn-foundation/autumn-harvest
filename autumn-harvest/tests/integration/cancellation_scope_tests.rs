#![cfg(feature = "db")]
//! Cancellation scopes through a real worker (issue #1984).
//!
//! - A scope cancel tears down an activity, a timer and a child workflow.
//! - A non-cancellable block runs to completion after a workflow cancel.
//! - A full replay of each recorded history adds no command.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::TaskQueueItem;
use autumn_harvest::schema::harvest_task_queue;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{HarvestError, WorkflowContext, cancel_workflow_execution};
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task,
    insert_workflow_execution, load_child_executions_from_url, load_execution_from_url,
    load_history_from_url, load_timers_for_execution_from_url, setup_test_database_url_or_env,
    spawn_test_worker, wait_for_execution_state, wait_for_execution_state_with_timeout,
};

type WfFuture<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;
type WfHandler = for<'a> fn(&'a WorkflowContext, Value) -> WfFuture<'a>;
type ActFuture = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send>>;

fn wf_info(name: &'static str, handler: WfHandler) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "cancellation_scope_tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

fn act_info(name: &'static str, handler: autumn_harvest::info::ActivityHandlerFn) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "cancellation_scope_tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

/// Runs until the worker cancels it.
fn slow_activity(_ctx: &autumn_harvest::ActivityContext, _input: Value) -> ActFuture {
    Box::pin(async move {
        tokio::time::sleep(std::time::Duration::from_secs(120)).await;
        Ok(json!("slow"))
    })
}

fn fast_activity(_ctx: &autumn_harvest::ActivityContext, _input: Value) -> ActFuture {
    Box::pin(async move { Ok(json!("fast")) })
}

/// Opens when the test sets it. One test uses it.
static CLEANUP_GATE: AtomicBool = AtomicBool::new(false);

fn gated_cleanup(_ctx: &autumn_harvest::ActivityContext, _input: Value) -> ActFuture {
    Box::pin(async move {
        while !CLEANUP_GATE.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok(json!("cleaned"))
    })
}

/// A child that parks on a long timer.
fn parked_child(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        ctx.timer("child_wait", 3600)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("child done"))
    })
}

/// Starts one member of each kind in a scope. The `abort` signal cancels it.
async fn scope_members(ctx: &WorkflowContext) -> Result<Value, String> {
    let scope = ctx.cancellation_scope();
    let (members, ()) = tokio::join!(
        scope.run(async {
            tokio::join!(
                ctx.execute_activity_raw("slow_activity", Value::Null, "default"),
                ctx.timer("scope_deadline", 3600),
                ctx.spawn_child_workflow_raw("parked_child", Value::Null),
            )
        }),
        async {
            let _ = ctx.wait_for_signal("abort").await;
            scope.cancel();
        }
    );
    match members {
        Err(HarvestError::Cancelled(_)) => Ok(json!("cancelled")),
        Ok(_) => Ok(json!("completed")),
        Err(e) => Err(e.to_string()),
    }
}

fn scope_members_wf(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(scope_members(ctx))
}

/// Cleanup in a non-cancellable block, then one more activity.
async fn shielded_cleanup(ctx: &WorkflowContext) -> Result<Value, String> {
    let cleaned = ctx
        .non_cancellable(ctx.execute_activity_raw("gated_cleanup", Value::Null, "default"))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    ctx.execute_activity_raw("fast_activity", Value::Null, "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(cleaned)
}

fn shielded_cleanup_wf(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(shielded_cleanup(ctx))
}

async fn wait_for_event(
    database_url: &str,
    exec_id: ExecutionId,
    what: &str,
    pred: impl Fn(&WorkflowEvent) -> bool,
) {
    for _ in 0..200 {
        let history = load_history_from_url(database_url, exec_id).await;
        if history.events.iter().any(&pred) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn activity_tasks(database_url: &str, exec_id: ExecutionId) -> Vec<TaskQueueItem> {
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect");
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("load activity tasks")
}

/// Replays `events` in process and asserts that replay adds nothing.
async fn assert_replay_is_clean(
    exec_id: ExecutionId,
    events: Vec<WorkflowEvent>,
    handler: WfHandler,
    expected: &Value,
) {
    let ctx = WorkflowContext::for_replay(exec_id, events);
    let replayed = handler(&ctx, Value::Null).await;
    assert_eq!(replayed.as_ref(), Ok(expected), "replay result");
    assert!(ctx.take_nd_details().is_none(), "replay must be deterministic");
    let commands = ctx.drain_commands();
    assert!(commands.is_empty(), "replay must add nothing: {commands:?}");
}

/// AC1: a scope cancels the activity, the timer and the child it started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scope_cancel_tears_down_an_activity_a_timer_and_a_child() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = Arc::new(HandlerRegistry::new(
        vec![
            wf_info("e2e_test_workflow", scope_members_wf),
            wf_info("parked_child", parked_child),
        ],
        vec![act_info("slow_activity", slow_activity)],
    ));
    let worker = build_runtime_worker("worker-1984-scope-members", 4, 2, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));

    wait_for_event(&database_url, exec_id, "the child start", |e| {
        matches!(e, WorkflowEvent::ChildWorkflowStarted { .. })
    })
    .await;
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "abort", Value::Null)
        .await
        .expect("send abort");

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    let children = load_child_executions_from_url(&database_url, exec_id).await;
    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    let tasks = activity_tasks(&database_url, exec_id).await;
    let history = load_history_from_url(&database_url, exec_id).await;
    worker.shutdown();
    handle.await.expect("join");

    assert_eq!(parent.output, Some(json!("cancelled")));
    assert_eq!(children.len(), 1, "{children:?}");
    assert_eq!(children[0].state, "CANCELLED", "the child is cancelled");
    assert!(timers.is_empty(), "the timer row is deleted: {timers:?}");
    assert_eq!(tasks.len(), 1, "{tasks:?}");
    assert_eq!(tasks[0].state, "CANCELLED", "the activity task is cancelled");
    assert!(
        history.events.iter().any(|e| matches!(
            e,
            WorkflowEvent::MarkerRecorded { name, .. } if name == "cancel_scope:1"
        )),
        "the cancel decision is recorded: {:?}",
        history.events
    );

    // AC3: the recorded history replays deterministically.
    assert_replay_is_clean(
        exec_id,
        history.events,
        scope_members_wf,
        &json!("cancelled"),
    )
    .await;
}

/// AC2: a non-cancellable block runs to completion after a workflow cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_cancellable_block_completes_after_the_workflow_is_cancelled() {
    CLEANUP_GATE.store(false, Ordering::SeqCst);
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = Arc::new(HandlerRegistry::new(
        vec![wf_info("e2e_test_workflow", shielded_cleanup_wf)],
        vec![
            act_info("gated_cleanup", gated_cleanup),
            act_info("fast_activity", fast_activity),
        ],
    ));
    let worker = build_runtime_worker("worker-1984-shield", 4, 2, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));

    wait_for_event(&database_url, exec_id, "the cleanup start", |e| {
        matches!(e, WorkflowEvent::ActivityStarted { .. })
    })
    .await;
    let cancelled = cancel_workflow_execution(&mut conn, exec_id, "operator abort", &NoOpMetrics)
        .await
        .expect("cancel");

    assert!(cancelled.deferred, "an open block defers the cancel");
    assert_eq!(cancelled.state, "RUNNING");
    assert_eq!(
        load_execution_from_url(&database_url, exec_id).await.state,
        "RUNNING",
        "the run keeps running while the block is open"
    );

    CLEANUP_GATE.store(true, Ordering::SeqCst);
    let parent = wait_for_execution_state_with_timeout(
        &database_url,
        exec_id,
        "CANCELLED",
        std::time::Duration::from_secs(20),
    )
    .await;
    let history = load_history_from_url(&database_url, exec_id).await;
    let tasks = activity_tasks(&database_url, exec_id).await;
    worker.shutdown();
    handle.await.expect("join");

    assert_eq!(parent.error.as_deref(), Some("operator abort"));
    let position = |pred: &dyn Fn(&WorkflowEvent) -> bool| {
        history
            .events
            .iter()
            .position(pred)
            .unwrap_or_else(|| panic!("event missing: {:?}", history.events))
    };
    let requested = position(&|e| matches!(e, WorkflowEvent::WorkflowCancelRequested { .. }));
    let cleaned = position(&|e| {
        matches!(e, WorkflowEvent::ActivityCompleted { output, .. } if output == &json!("cleaned"))
    });
    let closed = position(&|e| {
        matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name == "non_cancellable_close:1")
    });
    let terminal = position(&|e| matches!(e, WorkflowEvent::WorkflowCancelled { .. }));
    assert!(requested < cleaned, "the cleanup completes after the cancel");
    assert!(cleaned < closed && closed < terminal, "{:?}", history.events);
    assert!(
        tasks
            .iter()
            .any(|t| t.activity_name.as_deref() == Some("gated_cleanup") && t.state == "COMPLETED"),
        "the shielded activity completes: {tasks:?}"
    );
}

/// A cancel with no open block stays terminal at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_after_the_block_closes_is_terminal_at_once() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let exec_id = insert_workflow_execution(&mut conn).await;
    let history = [
        WorkflowEvent::MarkerRecorded {
            name: "non_cancellable_open:1".into(),
            details: Value::Null,
        },
        WorkflowEvent::MarkerRecorded {
            name: "non_cancellable_close:1".into(),
            details: Value::Null,
        },
    ];
    autumn_harvest::store::append_events(&mut conn, exec_id, &history, 0)
        .await
        .expect("seed history");

    let cancelled = cancel_workflow_execution(&mut conn, exec_id, "operator abort", &NoOpMetrics)
        .await
        .expect("cancel");

    assert!(!cancelled.deferred);
    assert_eq!(cancelled.state, "CANCELLED");
}
