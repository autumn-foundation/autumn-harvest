//! Macro expansion tests for `#[webhook]` and `webhooks![]` (issue #344).
//!
//! `#[webhook]` mapping functions must take their typed payload parameter by
//! value (the dispatch shim deserializes an owned `T` from JSON) and some
//! fixture functions here never take the error branch -- both are inherent
//! to exercising the macro's calling convention, not style issues.
#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use autumn_harvest::prelude::*;
use autumn_harvest::webhook_trigger::WebhookTarget;

#[derive(serde::Deserialize)]
struct OrderEvent {
    order_id: String,
}

#[webhook(path = "/hooks/orders", starts = "order_flow")]
fn map_order(_ctx: &WebhookCtx, evt: OrderEvent) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new(format!("order-{}", evt.order_id)))
}

#[derive(serde::Deserialize)]
struct PaymentEvent {
    amount_cents: i64,
}

#[webhook(
    path = "/hooks/stripe",
    signals = "subscription_flow",
    signal_name = "payment_succeeded",
    queue = "billing"
)]
fn map_payment(_ctx: &WebhookCtx, evt: PaymentEvent) -> Result<WorkflowId, String> {
    if evt.amount_cents <= 0 {
        return Err("amount must be positive".to_string());
    }
    Ok(WorkflowId::new("subscription-shared"))
}

#[test]
fn starts_variant_companion_carries_expected_fields() {
    let info = map_order_info();
    assert_eq!(info.name, "map_order");
    assert_eq!(info.path, "/hooks/orders");
    assert_eq!(info.queue, None);
    assert_eq!(
        info.target,
        WebhookTarget::Starts {
            workflow: "order_flow"
        }
    );
}

#[test]
fn signals_variant_companion_carries_signal_name_and_queue() {
    let info = map_payment_info();
    assert_eq!(info.name, "map_payment");
    assert_eq!(info.path, "/hooks/stripe");
    assert_eq!(info.queue, Some("billing"));
    assert_eq!(
        info.target,
        WebhookTarget::SignalsWithStart {
            workflow: "subscription_flow",
            signal_name: "payment_succeeded",
        }
    );
}

#[test]
fn hidden_companion_and_public_alias_agree() {
    let via_alias = map_order_info();
    let via_companion = __autumn_webhook_info_map_order();
    assert_eq!(via_alias.name, via_companion.name);
    assert_eq!(via_alias.path, via_companion.path);
}

#[test]
fn handler_shim_round_trips_typed_payload_to_workflow_id() {
    let info = map_order_info();
    let ctx = WebhookCtx::new("/hooks/orders", "orders", "generic", None, None, vec![]);
    let payload = serde_json::json!({"order_id": "o-42"});
    let id = (info.handler)(&ctx, payload).expect("mapping should succeed");
    assert_eq!(id.as_str(), "order-o-42");
}

#[test]
fn handler_shim_classifies_deserialize_failure_for_malformed_payload() {
    let info = map_order_info();
    let ctx = WebhookCtx::new("/hooks/orders", "orders", "generic", None, None, vec![]);
    let payload = serde_json::json!({"not_order_id": "o-42"});
    let err = (info.handler)(&ctx, payload).unwrap_err();
    assert!(matches!(err, WebhookHandlerError::Deserialize(_)));
}

#[test]
fn handler_shim_classifies_user_rejection() {
    let info = map_payment_info();
    let ctx = WebhookCtx::new("/hooks/stripe", "stripe", "stripe", None, None, vec![]);
    let payload = serde_json::json!({"amount_cents": -5});
    let err = (info.handler)(&ctx, payload).unwrap_err();
    assert_eq!(
        err,
        WebhookHandlerError::Rejected("amount must be positive".to_string())
    );
}

#[test]
fn webhooks_bang_macro_collects_in_declared_order() {
    let hooks: Vec<WebhookTriggerInfo> = webhooks![map_order, map_payment];
    assert_eq!(hooks.len(), 2);
    assert_eq!(hooks[0].name, "map_order");
    assert_eq!(hooks[1].name, "map_payment");
}
