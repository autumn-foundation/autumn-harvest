//! A standalone mount can reach the inbound webhook receiver (issue #1612).
//!
//! `build_webhook_routes` returns `Vec<autumn_web::Route>`, which only
//! composes with an `autumn_web::AppBuilder`. So before this file's subject
//! (`build_webhook_router`) existed, a `#[webhook]` binding was reachable only
//! through `HarvestPlugin`. This is the compile-time and runtime guard for the
//! standalone counterpart. It builds the exact shape a non-`HarvestPlugin`
//! embedder builds: a bare `axum::Router` with no `autumn_web::AppBuilder`,
//! and no `.with_state(...)` call at the embedder's own call site. And it
//! drives a genuinely HMAC-signed request through the real `SignedWebhook`
//! extractor, proving that extractor runs with no autumn-web app in the test.
//!
//! No database is required. `fails_closed_when_runtime_is_not_started` proves
//! the request reaches this crate's handler code. The signature is verified,
//! then it fails closed because no `HarvestApiState::install(...)` ever ran.
//! This is the same boot-window proof `webhook_receiver_http_tests.rs` uses
//! for the plugin path. Full dispatch (idempotent redelivery,
//! exactly-one-execution) is unchanged and already covered there and by the
//! testcontainers integration test. Both entry points share one handler
//! (`webhook_method_router`), so this file only needs to prove the mount, not
//! re-prove dispatch.

#![cfg(feature = "webhooks")]
#![allow(
    clippy::unused_async,
    clippy::used_underscore_binding,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps
)]

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestApiState;
use autumn_harvest_plugin::webhook_receiver::build_webhook_router;
use autumn_web::reexports::axum;
use autumn_web::security::hmac_sha256_hex;
use autumn_web::webhook::{WebhookConfig, WebhookConfigError, WebhookEndpointConfig};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

const SECRET: &str = "test-webhook-secret-at-least-16-bytes";

#[workflow]
async fn order_flow(_ctx: &WorkflowContext, _order_id: String) -> Result<String, String> {
    Ok("done".into())
}

#[derive(serde::Deserialize)]
struct OrderEvent {
    order_id: String,
}

#[webhook(path = "/hooks/orders", starts = "order_flow")]
fn map_order(_ctx: &WebhookCtx, evt: OrderEvent) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new(format!("order-{}", evt.order_id)))
}

fn sign(body: &[u8]) -> String {
    format!("sha256={}", hmac_sha256_hex(SECRET.as_bytes(), body))
}

/// The standalone composition under test: `build_webhook_router` mounted
/// directly on a bare `axum::Router`, with no `autumn_web::AppState` and no
/// `autumn_web::AppBuilder` anywhere in this file.
fn standalone_router(replay_protection: bool) -> Result<axum::Router, WebhookConfigError> {
    let endpoint = WebhookEndpointConfig::generic("orders", "/hooks/orders", SECRET);
    let endpoint = if replay_protection {
        endpoint
    } else {
        endpoint.without_replay_protection()
    };
    let config = WebhookConfig {
        endpoints: vec![endpoint],
        ..Default::default()
    };
    build_webhook_router(
        &[map_order_info()],
        &[__autumn_workflow_info_order_flow()],
        &[],
        &HarvestApiState::new(),
        &config,
    )
}

async fn post_signed(app: axum::Router, body: Vec<u8>, signature: &str) -> StatusCode {
    app.oneshot(
        Request::builder()
            .method(axum::http::Method::POST)
            .uri("/hooks/orders")
            .header("X-Webhook-Signature", signature)
            .header("X-Webhook-Delivery", "dlv-1")
            .header("Content-Type", "application/json")
            .body(Body::from(body))
            .expect("request should build"),
    )
    .await
    .expect("router should serve the request")
    .status()
}

#[tokio::test]
async fn signature_verification_runs_with_no_autumn_web_app_in_the_test() {
    let body = serde_json::to_vec(&serde_json::json!({"order_id": "o-1"})).unwrap();
    let sig = sign(&body);

    let status = post_signed(standalone_router(false).unwrap(), body, &sig).await;

    // The signature verified, proving SignedWebhook's extractor ran against
    // the AppState::detached() this function installs, with no
    // autumn_web::AppBuilder involved anywhere. It then fails closed because
    // no HarvestApiState::install(...) ever ran in this no-DB test -- 503,
    // matching the plugin path's identical boot-window behavior.
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn bad_signature_is_rejected_before_any_dispatch_code_runs() {
    let body = serde_json::to_vec(&serde_json::json!({"order_id": "o-1"})).unwrap();

    let status = post_signed(
        standalone_router(false).unwrap(),
        body,
        "sha256=0000000000000000000000000000000000000000000000000000000000000000",
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// `WebhookReplayCleanupLayer` (what releases a reserved replay key on a
/// `5xx`) is `pub(crate)` inside autumn-web, installed only by its own
/// `AppBuilder`. A standalone mount has no way to install it. Rather than
/// silently mounting a route that would leak a stuck replay key on every
/// failure, `build_webhook_router` refuses a config that asks for it.
#[test]
fn replay_protection_is_rejected_at_build_time_not_at_the_first_stuck_key() {
    let err = standalone_router(true).unwrap_err();

    let WebhookConfigError::InvalidEndpoint { name, message } = err else {
        panic!("expected InvalidEndpoint, got {err:?}");
    };
    assert_eq!(name, "orders");
    assert!(message.contains("replay_protection"), "{message}");
}
