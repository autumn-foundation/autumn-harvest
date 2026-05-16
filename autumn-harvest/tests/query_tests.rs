//! Tests for Issue #234: Read-only Query handlers for live workflow state inspection.
//!
//! Covers:
//!   - New `HarvestError` variants: `QueryHandlerNotFound`, `WorkflowNotRunning`,
//!     `QueryHandlerPanicked`, `QueryTimedOut`
//!   - `QueryRegistry::list_names` and `execute_with_args`
//!   - `WorkflowContext::register_query_handler` (typed Req/Resp)
//!   - `WorkflowContext::execute_query_with_args`
//!   - `WorkflowContext::list_query_names`
//!   - Replay safety: no `WorkflowCommand`s emitted by query ops
//!   - `WorkerConfig::query_timeout` (default 5 s, configurable)
//!   - `telemetry::METRIC_QUERY_DURATION` constant

use std::sync::Arc;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::error::HarvestError;
use autumn_harvest::query::QueryRegistry;
use autumn_harvest::telemetry;
use autumn_harvest::types::ExecutionId;
use serde_json::{Value, json};
use std::time::Duration;

// ── HarvestError new variants ─────────────────────────────────────────────

#[test]
fn error_query_handler_not_found_displays_name() {
    let e = HarvestError::QueryHandlerNotFound("my_query".to_string());
    let msg = e.to_string();
    assert!(
        msg.contains("my_query"),
        "error message should contain handler name, got: {msg}"
    );
}

#[test]
fn error_workflow_not_running_displays_exec_id() {
    let exec_id = ExecutionId::new();
    let e = HarvestError::WorkflowNotRunning(exec_id);
    let msg = e.to_string();
    assert!(
        msg.contains(&exec_id.to_string()),
        "error message should contain exec_id, got: {msg}"
    );
}

#[test]
fn error_query_handler_panicked_displays_message() {
    let e = HarvestError::QueryHandlerPanicked("handler blew up".to_string());
    let msg = e.to_string();
    assert!(
        msg.contains("handler blew up"),
        "error message should contain panic message, got: {msg}"
    );
}

#[test]
fn error_query_timed_out_displays_query_name_and_timeout() {
    let e = HarvestError::QueryTimedOut {
        query_name: "check_progress".to_string(),
        timeout_ms: 5000,
    };
    let msg = e.to_string();
    assert!(
        msg.contains("check_progress"),
        "error message should contain query name, got: {msg}"
    );
    assert!(
        msg.contains("5000"),
        "error message should contain timeout value, got: {msg}"
    );
}

// ── QueryRegistry::list_names ─────────────────────────────────────────────

#[test]
fn query_registry_list_names_returns_registered_names() {
    let mut registry = QueryRegistry::new();
    registry.register(
        "status",
        Arc::new(|_args: Value| -> Result<Value, String> { Ok(json!("ok")) }),
    );
    registry.register(
        "progress",
        Arc::new(|_args: Value| -> Result<Value, String> { Ok(json!(42)) }),
    );

    let names = registry.list_names();
    assert!(
        names.contains(&"status".to_string()),
        "should contain 'status'"
    );
    assert!(
        names.contains(&"progress".to_string()),
        "should contain 'progress'"
    );
    assert_eq!(names.len(), 2, "should have exactly 2 names");
}

#[test]
fn query_registry_list_names_empty_when_nothing_registered() {
    let registry = QueryRegistry::new();
    assert!(
        registry.list_names().is_empty(),
        "empty registry should return empty list"
    );
}

// ── QueryRegistry::execute_with_args ─────────────────────────────────────

#[test]
fn query_registry_execute_with_args_passes_args_to_handler() {
    let mut registry = QueryRegistry::new();
    registry.register(
        "echo",
        Arc::new(|args: Value| -> Result<Value, String> { Ok(args) }),
    );

    let result = registry
        .execute_with_args("echo", json!({"hello": "world"}))
        .expect("echo query should succeed");
    assert_eq!(result, json!({"hello": "world"}));
}

#[test]
fn query_registry_execute_with_args_returns_handler_not_found_for_unknown() {
    let registry = QueryRegistry::new();
    let result = registry.execute_with_args("unknown", json!(null));
    assert!(
        matches!(result, Err(HarvestError::QueryHandlerNotFound(_))),
        "should return QueryHandlerNotFound, got: {result:?}"
    );
}

#[test]
fn query_registry_execute_with_args_propagates_handler_error() {
    let mut registry = QueryRegistry::new();
    registry.register(
        "failing",
        Arc::new(|_args: Value| -> Result<Value, String> { Err("handler error".to_string()) }),
    );

    let result = registry.execute_with_args("failing", json!(null));
    // The handler error should propagate as a specific error variant
    assert!(result.is_err(), "handler error should propagate");
}

// ── WorkflowContext::register_query_handler (typed) ──────────────────────

#[test]
fn register_query_handler_typed_deserializes_request_and_serializes_response() {
    #[derive(serde::Deserialize)]
    struct ProgressQuery {
        include_details: bool,
    }
    #[derive(serde::Serialize)]
    struct ProgressResponse {
        processed: u32,
        details: Option<String>,
    }

    let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    ctx.register_query_handler("progress", |req: &ProgressQuery| {
        Ok(ProgressResponse {
            processed: 42,
            details: if req.include_details {
                Some("step 3 of 10".to_string())
            } else {
                None
            },
        })
    });

    let result = ctx
        .execute_query_with_args("progress", json!({"include_details": true}))
        .expect("query should succeed");
    assert_eq!(result["processed"], 42);
    assert_eq!(result["details"], "step 3 of 10");
}

#[test]
fn register_query_handler_with_no_args_type() {
    // Handler with serde_json::Value as the Req type (generic pass-through)
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    ctx.register_query_handler(
        "echo",
        |req: &serde_json::Value| -> Result<serde_json::Value, String> { Ok(req.clone()) },
    );

    let result = ctx
        .execute_query_with_args("echo", json!({"ping": true}))
        .expect("echo should succeed");
    assert_eq!(result["ping"], true);
}

#[test]
fn register_query_handler_is_idempotent() {
    // Registering the same name twice should not panic; first registration wins.
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    ctx.register_query_handler(
        "status",
        |_: &serde_json::Value| -> Result<serde_json::Value, String> { Ok(json!("first")) },
    );
    ctx.register_query_handler(
        "status",
        |_: &serde_json::Value| -> Result<serde_json::Value, String> { Ok(json!("second")) },
    );

    let result = ctx
        .execute_query_with_args("status", json!(null))
        .expect("query should succeed");
    // First registration wins (idempotent like UpdateRegistry)
    assert_eq!(result, json!("first"), "first registration must win");
}

// ── WorkflowContext::execute_query_with_args ──────────────────────────────

#[test]
fn execute_query_with_args_passes_args_to_no_arg_handler() {
    // register_query (no-arg) ignores args; execute_query_with_args should still work.
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    ctx.register_query("simple", || json!("simple_result"));

    let result = ctx
        .execute_query_with_args("simple", json!({"any": "args"}))
        .expect("simple query should succeed");
    assert_eq!(result, json!("simple_result"));
}

#[test]
fn execute_query_with_args_returns_query_handler_not_found_for_unknown() {
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    let result = ctx.execute_query_with_args("nonexistent", json!(null));
    assert!(
        matches!(result, Err(HarvestError::QueryHandlerNotFound(_))),
        "should return QueryHandlerNotFound, got: {result:?}"
    );
}

// ── WorkflowContext::list_query_names ─────────────────────────────────────

#[test]
fn list_query_names_returns_all_registered_handlers() {
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    ctx.register_query("status", || json!("running"));
    ctx.register_query_handler(
        "progress",
        |_: &serde_json::Value| -> Result<serde_json::Value, String> { Ok(json!(42)) },
    );

    let names = ctx.list_query_names();
    assert!(
        names.contains(&"status".to_string()),
        "should include 'status'"
    );
    assert!(
        names.contains(&"progress".to_string()),
        "should include 'progress'"
    );
    assert_eq!(names.len(), 2);
}

#[test]
fn list_query_names_empty_before_any_registration() {
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    assert!(
        ctx.list_query_names().is_empty(),
        "should be empty before any registration"
    );
}

// ── Replay safety: queries must not emit WorkflowCommands ─────────────────

#[test]
fn register_query_handler_emits_no_commands() {
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    ctx.register_query_handler(
        "status",
        |_: &serde_json::Value| -> Result<serde_json::Value, String> { Ok(json!("ok")) },
    );
    let cmds = ctx.drain_commands();
    assert!(
        cmds.is_empty(),
        "query registration must not emit any commands"
    );
}

#[test]
fn execute_query_with_args_emits_no_commands() {
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    ctx.register_query("status", || json!("running"));
    let _ = ctx.execute_query_with_args("status", json!(null));
    let cmds = ctx.drain_commands();
    assert!(
        cmds.is_empty(),
        "query execution must not emit any commands: {cmds:?}"
    );
}

#[test]
fn execute_query_with_args_on_unknown_handler_emits_no_commands() {
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    let _ = ctx.execute_query_with_args("nonexistent", json!(null));
    let cmds = ctx.drain_commands();
    assert!(
        cmds.is_empty(),
        "failed query lookup must not emit any commands"
    );
}

// ── WorkerConfig::query_timeout ───────────────────────────────────────────

#[test]
fn worker_config_query_timeout_defaults_to_5s() {
    let config = WorkerConfig::default();
    assert_eq!(
        config.query_timeout,
        Duration::from_secs(5),
        "default query_timeout should be 5 seconds"
    );
}

#[test]
fn worker_config_with_query_timeout_builder_method() {
    let config = WorkerConfig::default().with_query_timeout(Duration::from_millis(500));
    assert_eq!(
        config.query_timeout,
        Duration::from_millis(500),
        "with_query_timeout should update the field"
    );
}

// ── telemetry::METRIC_QUERY_DURATION constant ─────────────────────────────

#[test]
fn metric_query_duration_constant_has_correct_name() {
    assert_eq!(
        telemetry::METRIC_QUERY_DURATION,
        "harvest.query.duration",
        "METRIC_QUERY_DURATION must follow the harvest.<noun>.<instrument> naming convention"
    );
}
