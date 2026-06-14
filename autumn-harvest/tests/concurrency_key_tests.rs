//! Unit tests for per-key concurrency limits (issue #247).
//!
//! RED-phase tests — all expected to fail until the feature is implemented.

use autumn_harvest::concurrency::{ConcurrencyPolicy, resolve_concurrency_key};
use autumn_harvest::info::WorkflowInfo;

// ---------------------------------------------------------------------------
// WorkflowInfo concurrency fields
// ---------------------------------------------------------------------------

#[test]
fn workflow_info_has_concurrency_fields() {
    let info = WorkflowInfo {
        name: "run_report",
        module: "my_app::workflows",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
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
    };
    assert!(info.concurrency.is_none());
}

#[test]
fn workflow_info_with_concurrency_policy() {
    let info = WorkflowInfo {
        name: "run_report",
        module: "my_app::workflows",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        sla: None,
        concurrency: Some(ConcurrencyPolicy {
            key_expr: "input.tenant_id",
            limit: 10,
        }),
        max_input_bytes: None,

        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
    };
    let policy = info.concurrency.expect("should have concurrency policy");
    assert_eq!(policy.key_expr, "input.tenant_id");
    assert_eq!(policy.limit, 10);
}

// ---------------------------------------------------------------------------
// Key expression resolver
// ---------------------------------------------------------------------------

#[test]
fn resolve_top_level_field() {
    let input = serde_json::json!({ "tenant_id": "acme" });
    let resolved = resolve_concurrency_key("tenant_id", &input);
    assert_eq!(resolved, Some("acme".to_string()));
}

#[test]
fn resolve_input_prefixed_field() {
    let input = serde_json::json!({ "tenant_id": "acme" });
    // "input.tenant_id" is a common shorthand — the "input." prefix is stripped
    // before resolving against the payload.
    let resolved = resolve_concurrency_key("input.tenant_id", &input);
    assert_eq!(resolved, Some("acme".to_string()));
}

#[test]
fn resolve_nested_field() {
    let input = serde_json::json!({ "user": { "id": 42 } });
    let resolved = resolve_concurrency_key("user.id", &input);
    assert_eq!(resolved, Some("42".to_string()));
}

#[test]
fn resolve_missing_field_returns_none() {
    let input = serde_json::json!({ "other": "value" });
    let resolved = resolve_concurrency_key("tenant_id", &input);
    assert_eq!(resolved, None);
}

#[test]
fn resolve_null_value_returns_none() {
    let input = serde_json::json!({ "tenant_id": null });
    let resolved = resolve_concurrency_key("tenant_id", &input);
    assert_eq!(resolved, None);
}

#[test]
fn resolve_integer_value_as_string() {
    let input = serde_json::json!({ "tenant_id": 123 });
    let resolved = resolve_concurrency_key("tenant_id", &input);
    assert_eq!(resolved, Some("123".to_string()));
}

#[test]
fn resolve_against_non_object_input() {
    // Input is a plain string, not an object
    let input = serde_json::json!("plain_string");
    let resolved = resolve_concurrency_key("tenant_id", &input);
    assert_eq!(resolved, None);
}

// ---------------------------------------------------------------------------
// ConcurrencyPolicy
// ---------------------------------------------------------------------------

#[test]
fn concurrency_policy_debug() {
    let policy = ConcurrencyPolicy {
        key_expr: "input.tenant_id",
        limit: 5,
    };
    let s = format!("{policy:?}");
    assert!(s.contains("tenant_id"));
    assert!(s.contains('5'));
}
