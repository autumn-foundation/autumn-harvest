//! Integration tests for per-execution context headers (issue #481).

#![cfg(feature = "testing")]

use std::collections::HashMap;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::ExecutionId;
use chrono::Utc;
use serde_json::Value;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_started_history(input: Value) -> Vec<WorkflowEvent> {
    vec![WorkflowEvent::WorkflowStarted {
        input,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }]
}

fn echo_headers_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let tenant = ctx.header("tenant_id").unwrap_or("").to_string();
        Ok(Value::String(tenant))
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn context_headers_empty_map_on_default_workflow_context() {
    // A WorkflowContext without headers exposes empty map; no panic.
    let exec_id = ExecutionId::new();
    let ctx = WorkflowContext::for_replay(exec_id, make_started_history(Value::Null));
    assert!(ctx.headers().is_empty());
    assert!(ctx.header("anything").is_none());
}

#[test]
fn context_headers_accessible_after_with_context_headers() {
    let exec_id = ExecutionId::new();
    let mut headers = HashMap::new();
    headers.insert("tenant_id".to_string(), "acme".to_string());
    let ctx = WorkflowContext::for_replay(exec_id, make_started_history(Value::Null))
        .with_context_headers(headers);
    assert_eq!(ctx.header("tenant_id"), Some("acme"));
    assert!(ctx.header("missing").is_none());
}

#[tokio::test]
async fn context_headers_replay_succeeds_with_headers() {
    // WorkflowReplayer with headers injected must complete with ReplaySucceeded.
    let mut headers = HashMap::new();
    headers.insert("tenant_id".to_string(), "acme-corp".to_string());

    let history = make_started_history(Value::Null);

    let report = WorkflowReplayer::new()
        .register_fn("echo_headers_workflow", echo_headers_workflow)
        .with_context_headers(headers)
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay must succeed with headers: {report}"
    );
}

#[tokio::test]
async fn context_headers_replay_succeeds_without_headers() {
    // No headers injected → still succeeds (backward compat).
    let history = make_started_history(Value::Null);

    let report = WorkflowReplayer::new()
        .register_fn("echo_headers_workflow", echo_headers_workflow)
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay must succeed without headers: {report}"
    );
}

#[tokio::test]
async fn history_snapshot_context_headers_round_trips() {
    // HistorySnapshot with context_headers serializes/deserializes correctly.
    let mut headers = HashMap::new();
    headers.insert("tenant_id".to_string(), "round-trip-tenant".to_string());

    let exec_id = ExecutionId::new();
    let snapshot = HistorySnapshot {
        workflow_name: "echo_headers_workflow".to_string(),
        execution_id: exec_id,
        events: make_started_history(Value::Null),
        context_headers: Some(headers.clone()),
    };

    let json = serde_json::to_string(&snapshot).expect("serialize snapshot");
    let recovered: HistorySnapshot = serde_json::from_str(&json).expect("deserialize snapshot");

    assert_eq!(
        recovered
            .context_headers
            .as_ref()
            .and_then(|m| m.get("tenant_id"))
            .map(String::as_str),
        Some("round-trip-tenant")
    );
}

#[tokio::test]
async fn history_snapshot_without_context_headers_deserializes_to_empty() {
    // Old JSON snapshots without the context_headers field must still parse.
    let json = r#"{
        "workflow_name": "echo_headers_workflow",
        "execution_id": "00000000-0000-0000-0000-000000000001",
        "events": []
    }"#;

    let snapshot: HistorySnapshot =
        serde_json::from_str(json).expect("old snapshot must still deserialize");

    assert!(
        snapshot.context_headers.is_none(),
        "missing context_headers field must deserialize to None"
    );
}
