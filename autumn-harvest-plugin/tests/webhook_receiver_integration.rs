//! Testcontainers integration tests for the inbound webhook receiver
//! (issue #344).
//!
//! Requires Docker. In sandboxes without a Docker daemon these tests are
//! compile-checked with `cargo test --no-run` (repo precedent: #543/#544).
//!
//! Mirrors `webhook_durable_integration.rs`'s harness shape
//! (`TestApp::plugin` + `TestDb::shared()` +
//! `autumn_web::migrate::run_pending`) rather than the hand-rolled
//! per-migration `INIT_SQL` pattern some other integration tests use --
//! that pattern needs the full harvest migration set enumerated by hand
//! and buys nothing extra here.

#![cfg(feature = "webhooks")]
#![allow(
    clippy::unused_async,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps
)]

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::security::hmac_sha256_hex;
use autumn_web::test::{TestApp, TestDb};
use autumn_web::webhook::{WebhookConfig, WebhookEndpointConfig};
use diesel_async::RunQueryDsl;

const SECRET: &str = "test-webhook-secret-at-least-16-bytes";

#[workflow]
async fn order_flow(_ctx: &WorkflowContext, order_id: String) -> Result<String, String> {
    Ok(order_id)
}

#[workflow]
async fn subscription_flow(ctx: &WorkflowContext) -> Result<String, String> {
    let payment: serde_json::Value = ctx
        .receive_signal("payment_succeeded")
        .await
        .map_err(|e| e.to_string())?;
    Ok(payment.to_string())
}

#[derive(serde::Deserialize)]
struct OrderEvent {
    order_id: String,
}

#[webhook(path = "/hooks/orders", starts = "order_flow")]
fn map_order(_ctx: &WebhookCtx, evt: OrderEvent) -> Result<WorkflowId, String> {
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

fn sign(body: &[u8]) -> String {
    format!("sha256={}", hmac_sha256_hex(SECRET.as_bytes(), body))
}

fn webhook_config() -> autumn_web::config::AutumnConfig {
    let mut config = autumn_web::config::AutumnConfig::default();
    config.security.webhooks = WebhookConfig {
        endpoints: vec![
            WebhookEndpointConfig::generic("orders", "/hooks/orders", SECRET)
                .without_replay_protection(),
            WebhookEndpointConfig::generic("subscriptions", "/hooks/subscriptions", SECRET)
                .without_replay_protection(),
        ],
        ..Default::default()
    };
    config
}

async fn count_executions(
    pool: &diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>,
    workflow_name: &str,
) -> i64 {
    let mut conn = pool.get().await.expect("pool conn");
    diesel::sql_query(
        "SELECT COUNT(*) as count FROM harvest_workflow_executions WHERE workflow_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .get_result::<CountRow>(&mut conn)
    .await
    .expect("count query")
    .count
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

/// AC (issue #344): "a synthetic vendor sends two identical webhook
/// deliveries; exactly one workflow execution is created; both responses
/// return the same `workflow_exec_id`."
#[tokio::test]
#[ignore = "requires Docker (testcontainers)"]
async fn duplicate_webhook_delivery_creates_exactly_one_execution_with_same_exec_id() {
    let _ = tracing_subscriber::fmt::try_init();

    let db = TestDb::shared().await;
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", db.url());
    }
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    db.execute_sql("TRUNCATE TABLE harvest_workflow_executions CASCADE")
        .await;
    db.execute_sql("TRUNCATE TABLE harvest_audit_log CASCADE")
        .await;

    let client = TestApp::new()
        .config(webhook_config())
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![order_flow, subscription_flow])
                .webhooks(webhooks![map_order, map_subscription])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .with_db(db.pool())
        .build();

    let body = serde_json::to_vec(&serde_json::json!({
        "id": "dlv-order-1",
        "order_id": "o-42"
    }))
    .unwrap();
    let sig = sign(&body);

    let first = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-order-1")
        .body(body.clone())
        .send()
        .await;
    first.assert_status(202);
    let first_json: serde_json::Value = first.json();
    assert_eq!(first_json["status"], "accepted");
    let exec_id = first_json["workflow_exec_id"]
        .as_str()
        .expect("workflow_exec_id present")
        .to_string();

    let second = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-order-1")
        .body(body)
        .send()
        .await;
    second.assert_status(200);
    let second_json: serde_json::Value = second.json();
    assert_eq!(second_json["status"], "idempotent_replay");
    assert_eq!(
        second_json["workflow_exec_id"].as_str().unwrap(),
        exec_id,
        "redelivery must resolve to the same execution id"
    );

    let count = count_executions(&db.pool(), "order_flow").await;
    assert_eq!(
        count, 1,
        "exactly one execution must exist after redelivery"
    );

    // Audit: a dispatch attempt is recorded (issue #344, OP_WEBHOOK_TRIGGER).
    let mut conn = db.pool().get().await.expect("pool conn");
    let audit_count: i64 = diesel::sql_query(
        "SELECT COUNT(*) as count FROM harvest_audit_log WHERE operation = 'webhook.trigger'",
    )
    .get_result::<CountRow>(&mut conn)
    .await
    .expect("audit count query")
    .count;
    assert!(
        audit_count >= 1,
        "expected at least one webhook.trigger audit row"
    );
}

/// The `signals` target variant dedupes on the verified delivery ID: two
/// identical deliveries admit exactly one `SignalReceived` event, not two.
#[tokio::test]
#[ignore = "requires Docker (testcontainers)"]
async fn duplicate_webhook_delivery_to_signals_target_delivers_signal_exactly_once() {
    let _ = tracing_subscriber::fmt::try_init();

    let db = TestDb::shared().await;
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", db.url());
    }
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    db.execute_sql("TRUNCATE TABLE harvest_workflow_executions CASCADE")
        .await;
    db.execute_sql("TRUNCATE TABLE harvest_signals CASCADE")
        .await;

    let client = TestApp::new()
        .config(webhook_config())
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![order_flow, subscription_flow])
                .webhooks(webhooks![map_order, map_subscription])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .with_db(db.pool())
        .build();

    let body = serde_json::to_vec(&serde_json::json!({
        "id": "dlv-payment-1",
        "amount_cents": 500
    }))
    .unwrap();
    let sig = sign(&body);

    let first = client
        .post("/hooks/subscriptions")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-payment-1")
        .body(body.clone())
        .send()
        .await;
    first.assert_status(202);

    let second = client
        .post("/hooks/subscriptions")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-payment-1")
        .body(body)
        .send()
        .await;
    second.assert_status(200);

    let count = count_executions(&db.pool(), "subscription_flow").await;
    assert_eq!(
        count, 1,
        "exactly one execution must exist after redelivery"
    );

    // The idempotency key is namespaced by binding (path + signal_name), not
    // the raw provider delivery id -- see webhook_receiver.rs::handle_webhook.
    let mut conn = db.pool().get().await.expect("pool conn");
    let signal_count: i64 = diesel::sql_query(
        "SELECT COUNT(*) as count FROM harvest_signals WHERE idempotency_key = \
         '/hooks/subscriptions:payment_succeeded:dlv-payment-1'",
    )
    .get_result::<CountRow>(&mut conn)
    .await
    .expect("signal count query")
    .count;
    assert_eq!(signal_count, 1, "the signal must be admitted exactly once");
}

/// A blank top-level `"id"` field (`autumn_web::webhook`'s JSON-body
/// delivery-id fallback has no non-empty guard) must be treated as "no
/// delivery id resolved" -- not a genuine, if empty, id that would let two
/// unrelated `SignalsWithStart` deliveries collide on the same namespaced
/// idempotency key (Codex review, PR #918).
#[tokio::test]
#[ignore = "requires Docker (testcontainers)"]
async fn blank_delivery_id_is_rejected_as_missing_for_signals_target() {
    let _ = tracing_subscriber::fmt::try_init();

    let db = TestDb::shared().await;
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", db.url());
    }
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");

    let client = TestApp::new()
        .config(webhook_config())
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![order_flow, subscription_flow])
                .webhooks(webhooks![map_order, map_subscription])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .with_db(db.pool())
        .build();

    let body = serde_json::to_vec(&serde_json::json!({
        "id": "",
        "amount_cents": 500
    }))
    .unwrap();
    let sig = sign(&body);

    let resp = client
        .post("/hooks/subscriptions")
        .header("X-Webhook-Signature", &sig)
        .body(body)
        .send()
        .await;
    resp.assert_status(400);
    resp.assert_body_contains("missing_idempotency");
}
