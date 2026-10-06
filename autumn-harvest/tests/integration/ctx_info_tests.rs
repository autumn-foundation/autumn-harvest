//! DB/worker integration test for `ctx.info()` run metadata (issue #698).
//!
//! Exercises the real worker → executor → `WorkflowContext` threading of the
//! spawning parent's execution id: a top-level run reports no parent, and a
//! child spawned by it reports the parent's execution id via
//! `ctx.info().parent_execution_id`. The child captures its `info()` into its
//! own output so the test can assert on it after the run.
//!
//! Uses testcontainers, so the `db` feature is required.
#![cfg(feature = "db")]

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::worker::HandlerRegistry;
use diesel_async::AsyncPgConnection;
use serde_json::json;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task,
    insert_workflow_execution, load_child_executions_from_url, load_history_from_url,
    setup_test_database_url_or_env, spawn_test_worker, wait_for_execution_state,
};

/// Parent workflow: spawns a child and folds its own (absent) parent id plus the
/// child's output into the result. A top-level run has no parent → `null`.
fn parent_info_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_out = ctx
            .spawn_child_workflow_raw("child_info_workflow", input)
            .await
            .map_err(|e| e.to_string())?;
        let info = ctx.info();
        Ok(json!({
            "my_parent": info.parent_execution_id.map(|id| id.to_string()),
            "my_exec": info.execution_id.to_string(),
            "child": child_out,
        }))
    })
}

/// Child workflow: returns its own `info()` so the test can assert the parent
/// linkage was threaded from the execution row into the `WorkflowContext`.
fn child_info_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let info = ctx.info();
        Ok(json!({
            "parent": info.parent_execution_id.map(|id| id.to_string()),
            "exec": info.execution_id.to_string(),
            "type": info.workflow_type,
        }))
    })
}

fn ctx_info_registry() -> Arc<HandlerRegistry> {
    // Match the field set of the e2e `child_round_trip_registry` WorkflowInfo
    // literals exactly.
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "e2e_test_workflow",
                module: "ctx_info_tests",
                handler: parent_info_workflow,
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
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "child_info_workflow",
                module: "ctx_info_tests",
                handler: child_info_workflow,
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
            },
        ],
        vec![],
    ))
}

/// AC2/AC4 (DB, real worker loop): a child spawned by a parent reports the
/// parent's `execution_id` via `ctx.info().parent_execution_id`, and the
/// top-level parent reports no parent. Proves the worker → executor → context
/// threading of the execution row's `parent_id` end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_threads_parent_execution_id_into_child_ctx_info() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, json!({"cart": "cart-42"})).await;

    let worker = build_runtime_worker("worker-e2e-ctx-info", 2, 1, ctx_info_registry());
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent_execution =
        wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker task should join");

    // Top-level parent reports no parent.
    let parent_output = parent_execution.output.expect("parent should have output");
    assert_eq!(
        parent_output.get("my_parent"),
        Some(&serde_json::Value::Null),
        "a top-level run must report no parent, got: {parent_output}"
    );
    assert_eq!(
        parent_output
            .get("my_exec")
            .and_then(serde_json::Value::as_str),
        Some(parent_exec_id.to_string().as_str()),
        "the parent must report its own execution id"
    );

    // Exactly one child was created; it reports the parent's execution id.
    let child_execs = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(child_execs.len(), 1, "exactly one child execution expected");
    let child = &child_execs[0];
    assert_eq!(child.workflow_name, "child_info_workflow");

    let child_output = parent_output
        .get("child")
        .expect("parent must fold child output");
    assert_eq!(
        child_output
            .get("parent")
            .and_then(serde_json::Value::as_str),
        Some(parent_exec_id.to_string().as_str()),
        "AC2/AC4: child.info().parent_execution_id must equal the spawning parent's exec id"
    );
    // The child's reported execution id equals its own DB row id.
    assert_eq!(
        child_output.get("exec").and_then(serde_json::Value::as_str),
        Some(child.id.to_string().as_str()),
        "child.info().execution_id must equal the child execution row id"
    );
    assert_eq!(
        child_output.get("type").and_then(serde_json::Value::as_str),
        Some("child_info_workflow"),
    );

    // Sanity: no unexpected events; ctx.info() left no footprint.
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
    let child_history = load_history_from_url(
        &database_url,
        child.id.to_string().parse().expect("child id should parse"),
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
