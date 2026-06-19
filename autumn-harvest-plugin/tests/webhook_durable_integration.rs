#![cfg(feature = "webhooks")]

use autumn_harvest::prelude::WorkerConfig;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::config::AutumnConfig;
use autumn_web::test::{TestApp, TestDb};
use autumn_web::webhook_outbound::{
    InMemoryOutboundWebhookHandler, OutboundWebhookPlugin, WebhookOutboundManager,
    WebhookSubscription, WebhookSubscriptionStatus,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires Docker (testcontainers)"]
async fn test_durable_signed_webhook_via_harvest_workflow() {
    let _ = tracing_subscriber::fmt::try_init();

    // 1. Initialize TestDb and run pending migrations
    let db = TestDb::shared().await;
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");

    // Clean tables before test run
    db.execute_sql("TRUNCATE TABLE autumn_jobs CASCADE").await;
    db.execute_sql("TRUNCATE TABLE harvest_workflow_executions CASCADE")
        .await;

    // 2. Setup the Webhook Outbound Plugin with a process-local InMemory handler
    let handler = Arc::new(InMemoryOutboundWebhookHandler::new());
    let webhook_plugin = OutboundWebhookPlugin::new(handler.clone());

    // Create a subscription targeting the mock receiver
    let sub = WebhookSubscription {
        id: "sub_durable".to_owned(),
        target_url: "http://mock-receiver/webhooks/durable".to_owned(),
        event_topics: vec!["order.completed".to_owned()],
        secret: "my_webhook_signing_secret_32_bytes!!".to_owned(),
        status: WebhookSubscriptionStatus::Active,
        consecutive_failures: 0,
    };
    handler.create_subscription(sub).await.unwrap();

    // 3. Build the TestApp, mounting both OutboundWebhookPlugin and HarvestPlugin
    // with the "webhooks" feature enabled (which autowires the webhook workflow/activity)
    let mut config = AutumnConfig::default();
    config.database.url = Some(db.url().to_string());

    let mut app_builder = TestApp::new()
        .config(config)
        .plugin(webhook_plugin)
        .plugin(
            HarvestPlugin::new()
                .worker(WorkerConfig::default().with_queues(["webhooks"]))
                .api("/api/harvest"),
        )
        .with_db(db.pool());

    // Register HTTP mock for the outbound signed webhook target
    let mock = app_builder
        .http_mock("http://mock-receiver/webhooks/durable")
        .post("/webhooks/durable")
        .respond_with(200, serde_json::json!({ "received": true }));

    let app = app_builder.build();
    let state = app.state();

    // 4. Dispatch the webhook using the WebhookOutboundManager
    let manager = state
        .extension::<WebhookOutboundManager>()
        .expect("WebhookOutboundManager should be registered");

    let payload = serde_json::json!({
        "order_id": "ord_100",
        "amount": 9900
    });

    manager
        .dispatch(state, "order.completed", &payload)
        .await
        .unwrap();

    // 5. Wait for the Harvest workflow and activity to execute in the background
    let mut logs = Vec::new();
    for _ in 0..50 {
        logs = handler.get_delivery_logs().await.unwrap();
        if let Some(log) = logs.first() {
            // Wait until response status is logged (indicating HTTP request completed)
            if log.response_status.is_some() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 6. Verify mock receiver was called and signature is correct
    mock.expect_called(1);

    // Verify delivery logs were recorded successfully
    assert!(!logs.is_empty());
    let log = &logs[0];
    assert_eq!(log.subscription_id, "sub_durable");
    assert_eq!(log.topic, "order.completed");
    assert_eq!(log.response_status, Some(200));
    assert!(log.request_headers.contains_key("Autumn-Signature"));
    let sig_header = log.request_headers.get("Autumn-Signature").unwrap();
    assert!(sig_header.starts_with("t="));
    assert!(sig_header.contains(",v1="));
}
