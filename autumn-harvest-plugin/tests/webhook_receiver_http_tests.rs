//! No-database HTTP tests for the inbound webhook receiver (issue #344).
//!
//! Builds a real autumn-web app with the generated webhook receiver routes
//! and a `WebhookRegistry` extension (mirroring `install_registry_from_config`
//! but built directly from a `WebhookConfig`, since no `autumn.toml` is
//! loaded in this harness), then drives genuinely HMAC-signed requests
//! through the real `SignedWebhook` extractor -- verification runs exactly
//! as it would in production before any of this crate's handler code executes.
//!
//! NOTE: `TestApp::plugin(HarvestPlugin)` cannot be used here -- `TestApp`
//! replays plugin startup hooks and `start_harvest_runtime` requires a live
//! Postgres. The fail-closed (runtime-not-started) path is exactly what this
//! harness *can* prove without a database; the full dispatch flow (idempotent
//! redelivery, exactly-one-execution) is covered by the testcontainers
//! integration test (`webhook_receiver_integration.rs`, compile-checked in
//! this sandbox per the #543/#544 precedent).

#![cfg(feature = "webhooks")]
#![allow(
    clippy::unused_async,
    clippy::used_underscore_binding,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps
)]

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestApiState;
use autumn_harvest_plugin::webhook_receiver::build_webhook_routes;
use autumn_web::security::hmac_sha256_hex;
use autumn_web::test::{TestApp, TestClient};
use autumn_web::webhook::{WebhookConfig, WebhookEndpointConfig, WebhookRegistry};

const SECRET: &str = "test-webhook-secret-at-least-16-bytes";

#[workflow]
async fn order_flow(_ctx: &WorkflowContext, _order_id: String) -> Result<String, String> {
    Ok("done".into())
}

#[workflow]
async fn subscription_flow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[derive(serde::Deserialize)]
struct OrderEvent {
    order_id: String,
}

#[webhook(path = "/hooks/orders", starts = "order_flow")]
fn map_order(_ctx: &WebhookCtx, evt: OrderEvent) -> Result<WorkflowId, String> {
    if evt.order_id == "reject-me" {
        return Err("order id is blocklisted".to_string());
    }
    Ok(WorkflowId::new(format!("order-{}", evt.order_id)))
}

#[webhook(
    path = "/hooks/subscriptions",
    signals = "subscription_flow",
    signal_name = "payment_succeeded"
)]
fn map_subscription(_ctx: &WebhookCtx, evt: serde_json::Value) -> Result<WorkflowId, String> {
    let _ = evt;
    Ok(WorkflowId::new("subscription-shared"))
}

fn generic_endpoint(name: &str, path: &str, replay_protection: bool) -> WebhookEndpointConfig {
    let config = WebhookEndpointConfig::generic(name, path, SECRET);
    if replay_protection {
        config
    } else {
        config.without_replay_protection()
    }
}

fn registered_workflows() -> Vec<autumn_harvest::WorkflowInfo> {
    vec![
        __autumn_workflow_info_order_flow(),
        __autumn_workflow_info_subscription_flow(),
    ]
}

/// Assemble a no-DB test app: descriptor -> routes -> `WebhookRegistry`
/// extension. Mirrors what `HarvestPlugin::webhooks(...)` does inside
/// `Plugin::build`, minus the runtime startup.
fn build_client(endpoints: Vec<WebhookEndpointConfig>) -> TestClient {
    let triggers = vec![map_order_info(), map_subscription_info()];
    let routes = build_webhook_routes(&triggers, &registered_workflows(), &HarvestApiState::new());
    let config = WebhookConfig {
        endpoints,
        ..Default::default()
    };
    let registry = WebhookRegistry::from_config(&config).expect("valid webhook config");
    TestApp::new()
        .routes(routes)
        .state_initializer(move |state| state.insert_extension(registry))
        .build()
}

fn sign(body: &[u8]) -> String {
    format!("sha256={}", hmac_sha256_hex(SECRET.as_bytes(), body))
}

#[tokio::test]
async fn fails_closed_when_runtime_is_not_started() {
    let client = build_client(vec![generic_endpoint("orders", "/hooks/orders", false)]);
    let body = serde_json::to_vec(&serde_json::json!({"order_id": "o-1"})).unwrap();
    let resp = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sign(&body))
        .header("X-Webhook-Delivery", "dlv-1")
        .body(body)
        .send()
        .await;
    // The signature verified successfully (proving the request reached this
    // crate's handler code), which then fails closed because no
    // HarvestApiState::install(...) ever ran in this no-DB harness.
    resp.assert_status(400);
    resp.assert_body_contains("harvest runtime is not started");
}

#[tokio::test]
async fn bad_signature_is_rejected_before_any_dispatch_code_runs() {
    let client = build_client(vec![generic_endpoint("orders", "/hooks/orders", false)]);
    let body = serde_json::to_vec(&serde_json::json!({"order_id": "o-1"})).unwrap();
    let resp = client
        .post("/hooks/orders")
        .header(
            "X-Webhook-Signature",
            "sha256=0000000000000000000000000000000000000000000000000000000000000000",
        )
        .header("X-Webhook-Delivery", "dlv-1")
        .body(body)
        .send()
        .await;
    resp.assert_status(401);
}

#[tokio::test]
async fn missing_signature_header_returns_400() {
    let client = build_client(vec![generic_endpoint("orders", "/hooks/orders", false)]);
    let body = serde_json::to_vec(&serde_json::json!({"order_id": "o-1"})).unwrap();
    let resp = client.post("/hooks/orders").body(body).send().await;
    resp.assert_status(400);
}

#[tokio::test]
async fn duplicate_delivery_with_replay_protection_is_rejected_409() {
    let client = build_client(vec![generic_endpoint("orders", "/hooks/orders", true)]);
    let body = serde_json::to_vec(&serde_json::json!({"order_id": "o-1"})).unwrap();
    let sig = sign(&body);

    let first = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-replay-1")
        .body(body.clone())
        .send()
        .await;
    // Runtime isn't started, so even the *first* delivery 400s past
    // verification -- but that alone proves verification (and thus the
    // replay-protection insert) already ran and committed the delivery id.
    first.assert_status(400);

    let second = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-replay-1")
        .body(body)
        .send()
        .await;
    second.assert_status(409);
}

fn panic_message(result: std::thread::Result<Vec<autumn_web::Route>>) -> String {
    let Err(err) = result else {
        panic!("expected a panic, got Ok");
    };
    err.downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_default()
}

#[test]
fn duplicate_binding_paths_panic_at_build_time() {
    let triggers = vec![map_order_info(), map_order_info()];
    let result = std::panic::catch_unwind(|| {
        build_webhook_routes(&triggers, &registered_workflows(), &HarvestApiState::new())
    });
    let message = panic_message(result);
    assert!(
        message.contains("duplicate webhook binding path"),
        "{message}"
    );
}

#[test]
fn trigger_targeting_unregistered_workflow_panics_at_build_time() {
    let triggers = vec![map_order_info()];
    let result =
        std::panic::catch_unwind(|| build_webhook_routes(&triggers, &[], &HarvestApiState::new()));
    let message = panic_message(result);
    assert!(message.contains("which is not registered"), "{message}");
}

#[tokio::test]
async fn webhook_routes_coexist_with_a_second_binding() {
    let client = build_client(vec![
        generic_endpoint("orders", "/hooks/orders", false),
        generic_endpoint("subscriptions", "/hooks/subscriptions", false),
    ]);
    let order_body = serde_json::to_vec(&serde_json::json!({"order_id": "o-1"})).unwrap();
    let sub_body = serde_json::to_vec(&serde_json::json!({"id": "evt-1"})).unwrap();

    let order_resp = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sign(&order_body))
        .header("X-Webhook-Delivery", "dlv-a")
        .body(order_body)
        .send()
        .await;
    order_resp.assert_status(400); // fail-closed, but reached this crate's code

    let sub_resp = client
        .post("/hooks/subscriptions")
        .header("X-Webhook-Signature", &sign(&sub_body))
        .body(sub_body)
        .send()
        .await;
    sub_resp.assert_status(400); // fail-closed, but reached this crate's code
}
