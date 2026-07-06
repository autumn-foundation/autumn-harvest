//! Core descriptor tests for the inbound webhook trigger primitive (issue #344).
//!
//! Pure, no-DB tests for `WebhookTriggerInfo`/`WebhookCtx`/`WebhookHandlerFn`/
//! `validate_webhook_triggers`. These exercise the shape the `#[webhook]` macro
//! (issue #344, Layer 2) generates and the plugin route layer (Layer 3) consumes.
//! `WebhookHandlerFn` takes its payload by reference (the real dispatch shim
//! deserializes from a borrowed `&Value` to avoid cloning the verified body),
//! so these hand-written fixtures do too.

use autumn_harvest::types::WorkflowId;
use autumn_harvest::webhook_trigger::{
    WebhookCtx, WebhookHandlerError, WebhookTarget, WebhookTriggerInfo, validate_webhook_triggers,
};

fn ctx_fixture(path: &'static str) -> WebhookCtx {
    WebhookCtx::new(
        path,
        "orders",
        "generic",
        Some("dlv_123".to_string()),
        Some("order.created".to_string()),
        br#"{"id":"dlv_123","order_id":"o-1"}"#.to_vec(),
    )
}

fn starts_handler(
    _ctx: &WebhookCtx,
    payload: &serde_json::Value,
) -> Result<WorkflowId, WebhookHandlerError> {
    let order_id = payload
        .get("order_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| WebhookHandlerError::Deserialize("missing order_id".to_string()))?;
    Ok(WorkflowId::new(format!("order-{order_id}")))
}

fn always_rejects(
    _ctx: &WebhookCtx,
    _payload: &serde_json::Value,
) -> Result<WorkflowId, WebhookHandlerError> {
    Err(WebhookHandlerError::Rejected("nope".to_string()))
}

#[test]
fn webhook_ctx_exposes_verified_metadata() {
    let ctx = ctx_fixture("/hooks/orders");
    assert_eq!(ctx.path, "/hooks/orders");
    assert_eq!(ctx.endpoint, "orders");
    assert_eq!(ctx.provider, "generic");
    assert_eq!(ctx.delivery_id.as_deref(), Some("dlv_123"));
    assert_eq!(ctx.event_type.as_deref(), Some("order.created"));
    assert!(!ctx.raw_body.is_empty());
}

#[test]
fn webhook_target_starts_reports_its_workflow() {
    let target = WebhookTarget::Starts {
        workflow: "order_flow",
    };
    assert_eq!(target.workflow(), "order_flow");
}

#[test]
fn webhook_target_signals_with_start_reports_its_workflow() {
    let target = WebhookTarget::SignalsWithStart {
        workflow: "subscription_flow",
        signal_name: "payment_succeeded",
    };
    assert_eq!(target.workflow(), "subscription_flow");
}

#[test]
fn handler_shim_returns_deterministic_workflow_id_on_success() {
    let ctx = ctx_fixture("/hooks/orders");
    let payload = serde_json::json!({"order_id": "o-1"});
    let id = starts_handler(&ctx, &payload).expect("mapping should succeed");
    assert_eq!(id.as_str(), "order-o-1");
}

#[test]
fn handler_shim_classifies_deserialize_failure() {
    let ctx = ctx_fixture("/hooks/orders");
    let payload = serde_json::json!({"not_order_id": "o-1"});
    let err = starts_handler(&ctx, &payload).unwrap_err();
    assert_eq!(
        err,
        WebhookHandlerError::Deserialize("missing order_id".to_string())
    );
}

#[test]
fn handler_shim_classifies_user_rejection() {
    let ctx = ctx_fixture("/hooks/orders");
    let err = always_rejects(&ctx, &serde_json::json!({})).unwrap_err();
    assert_eq!(err, WebhookHandlerError::Rejected("nope".to_string()));
}

#[test]
fn webhook_handler_error_display_distinguishes_variants() {
    let d = WebhookHandlerError::Deserialize("bad json".to_string());
    let r = WebhookHandlerError::Rejected("no".to_string());
    assert!(d.to_string().contains("bad json"));
    assert!(r.to_string().contains("no"));
    assert_ne!(d.to_string(), r.to_string());
}

fn trigger(name: &'static str, path: &'static str) -> WebhookTriggerInfo {
    WebhookTriggerInfo {
        name,
        module: module_path!(),
        path,
        target: WebhookTarget::Starts { workflow: "wf" },
        handler: starts_handler,
        queue: None,
    }
}

#[test]
fn validate_accepts_disjoint_paths() {
    let triggers = vec![trigger("a", "/hooks/a"), trigger("b", "/hooks/b")];
    assert!(validate_webhook_triggers(&triggers).is_ok());
}

#[test]
fn validate_rejects_duplicate_paths_naming_both_triggers() {
    let triggers = vec![trigger("a", "/hooks/shared"), trigger("b", "/hooks/shared")];
    let err = validate_webhook_triggers(&triggers).unwrap_err();
    assert!(
        err.contains("/hooks/shared"),
        "error should name the path: {err}"
    );
    assert!(
        err.contains('a') && err.contains('b'),
        "error should name both triggers: {err}"
    );
}

#[test]
fn validate_rejects_path_missing_leading_slash() {
    let triggers = vec![trigger("a", "hooks/a")];
    let err = validate_webhook_triggers(&triggers).unwrap_err();
    assert!(err.contains("hooks/a"));
}

#[test]
fn validate_rejects_root_path() {
    let triggers = vec![trigger("a", "/")];
    assert!(validate_webhook_triggers(&triggers).is_err());
}

#[test]
fn validate_accepts_empty_trigger_set() {
    assert!(validate_webhook_triggers(&[]).is_ok());
}

#[test]
fn webhook_trigger_info_carries_optional_queue_override() {
    let mut t = trigger("a", "/hooks/a");
    t.queue = Some("billing");
    assert_eq!(t.queue, Some("billing"));
}
