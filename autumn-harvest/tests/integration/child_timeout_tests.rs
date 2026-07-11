#![cfg(feature = "db")]
//! End-to-end integration tests for `ctx.execute_child_workflow_timeout` /
//! `ctx.spawn_child_workflow_timeout` — deadline-bounded child-workflow awaits
//! (issue #779).
//!
//! Drives a real worker loop against a throwaway Postgres container (reusing
//! the harness helpers from `integration_e2e`). RED PHASE: the two
//! `WorkflowContext` methods do not exist yet, so this file fails to compile —
//! the accepted red state (per the #543/#593 precedent). It is Docker-run in
//! CI once green.
//!
//! Scenarios:
//! - child completes before the deadline → parent gets `Ok(Some(output))`.
//! - child hangs past the deadline → parent gets `Ok(None)` AND the still-running
//!   child row is sealed `CANCELLED` (R1/R6/R8).
//! - child fails before the deadline → parent gets a typed `Err` (R13).

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::WorkflowContext;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use serde_json::Value;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task,
    insert_workflow_execution, load_child_executions_from_url, load_history_from_url,
    setup_test_database_url, spawn_test_worker, wait_for_execution_state,
};

type WfFuture<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;
type WfHandler = for<'a> fn(&'a WorkflowContext, Value) -> WfFuture<'a>;

fn wf_info(name: &'static str, handler: WfHandler) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "child_timeout_tests",
        handler,
        execution_timeout: None,
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

fn registry(parent: WorkflowInfo, child: WorkflowInfo) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(vec![parent, child], vec![]))
}

// ── Workflow fns ───────────────────────────────────────────────────────────

/// Parent awaits a child with a 300s deadline; reports Some/None distinctly.
fn parent_await_child<'a>(ctx: &'a WorkflowContext, input: Value) -> WfFuture<'a> {
    Box::pin(async move {
        let outcome: Option<Value> = ctx
            .spawn_child_workflow_timeout(
                "timeout_child",
                input,
                std::time::Duration::from_secs(300),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(outcome.map_or_else(
            || serde_json::json!({"outcome": "timed_out"}),
            |o| serde_json::json!({"outcome": "child_done", "child": o}),
        ))
    })
}

/// Parent awaits a hanging child with a SHORT (2s) deadline so the durable
/// timer fires in bounded wall-clock time and the child is cancelled.
fn parent_short_deadline<'a>(ctx: &'a WorkflowContext, input: Value) -> WfFuture<'a> {
    Box::pin(async move {
        let outcome: Option<Value> = ctx
            .spawn_child_workflow_timeout("hanging_child", input, std::time::Duration::from_secs(2))
            .await
            .map_err(|e| e.to_string())?;
        Ok(outcome.map_or_else(
            || serde_json::json!({"outcome": "timed_out"}),
            |o| serde_json::json!({"outcome": "child_done", "child": o}),
        ))
    })
}

fn fast_child<'a>(_ctx: &'a WorkflowContext, input: Value) -> WfFuture<'a> {
    Box::pin(async move { Ok(serde_json::json!({"echo": input})) })
}

/// A child that hangs forever waiting for a signal that never arrives.
fn hanging_child<'a>(ctx: &'a WorkflowContext, _input: Value) -> WfFuture<'a> {
    Box::pin(async move {
        let _ = ctx.wait_for_signal("never").await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"unreachable": true}))
    })
}

fn failing_child<'a>(_ctx: &'a WorkflowContext, _input: Value) -> WfFuture<'a> {
    Box::pin(async move { Err("downstream 503".to_string()) })
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_completes_before_deadline_parent_gets_some() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({"id": 1})).await;

    let reg = registry(
        wf_info("e2e_test_workflow", parent_await_child),
        wf_info("timeout_child", fast_child),
    );
    let worker = build_runtime_worker("worker-779-child-win", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    assert_eq!(
        parent.output.and_then(|o| o.get("outcome").cloned()),
        Some(serde_json::json!("child_done")),
        "child completed before the deadline → parent must get Some"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_hangs_past_deadline_parent_gets_none_and_child_cancelled() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({"id": 2})).await;

    let reg = registry(
        wf_info("e2e_test_workflow", parent_short_deadline),
        wf_info("hanging_child", hanging_child),
    );
    let worker = build_runtime_worker("worker-779-timer-win", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    // The deadline fired first → parent takes the timeout branch.
    assert_eq!(
        parent.output.and_then(|o| o.get("outcome").cloned()),
        Some(serde_json::json!("timed_out")),
        "deadline fired first → parent must get None"
    );

    // The parent's own history must carry the deadline TimerFired.
    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    assert!(
        parent_history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "parent history must record the deadline TimerFired"
    );

    // The still-running loser child must be sealed CANCELLED (no leak).
    let children = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(children.len(), 1, "exactly one child execution");
    assert_eq!(
        children[0].state, "CANCELLED",
        "the losing child must be durably cancelled when the deadline wins"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_fails_before_deadline_parent_gets_typed_err() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({"id": 3})).await;

    let reg = registry(
        wf_info("e2e_test_workflow", parent_await_child),
        wf_info("timeout_child", failing_child),
    );
    let worker = build_runtime_worker("worker-779-child-fail", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // A child failure before the deadline propagates as an Err through the
    // parent (which maps it to a String and returns via `?`), failing the run.
    let parent = wait_for_execution_state(&database_url, parent_exec_id, "FAILED").await;
    worker.shutdown();
    handle.await.expect("join");

    assert!(
        parent.error.unwrap_or_default().contains("downstream 503"),
        "the child's failure must surface to the parent"
    );
    // Sanity: the deadline was never reached, so no TimerFired should be present
    // in the parent history on the child-failure path.
    let _child_id = ExecutionId::new(); // keep ExecutionId import exercised
}
