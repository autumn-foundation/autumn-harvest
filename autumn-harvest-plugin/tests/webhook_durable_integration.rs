//! Durable outbound webhook delivery through a Harvest workflow.
//!
//! Requires Docker. CI runs this suite from a `linux` manifest row (issue #1959).

#![cfg(feature = "webhooks")]

use autumn_harvest::prelude::WorkerConfig;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::test::TestApp;
use autumn_web::webhook_outbound::{
    InMemoryOutboundWebhookHandler, OutboundWebhookPlugin, WebhookOutboundManager,
    WebhookSubscription, WebhookSubscriptionStatus,
};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// Multi-thread runtime: `TestApp::plugin` blocks on plugin startup, and that
// deadlocks a current-thread runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_durable_signed_webhook_via_harvest_workflow() {
    let _ = tracing_subscriber::fmt::try_init();

    // 1. Start Postgres 16 and run the migrations. `TestDb` starts Postgres 11.
    // There, the worker claim query fails on `MATERIALIZED`, so no task runs.
    // `test_init_sql()` loads the Harvest schema. A second `run_pending` call
    // skips six Harvest migrations that share a framework migration version.
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let db_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&db_url, autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    let pool = Pool::builder(AsyncDieselConnectionManager::<AsyncPgConnection>::new(
        &db_url,
    ))
    .max_size(5)
    .build()
    .expect("failed to build pool");

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
    let config = autumn_web::config::AutumnConfig {
        profile: Some("test".into()),
        security: autumn_web::security::SecurityConfig {
            csrf: autumn_web::security::CsrfConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        },
        database: autumn_web::config::DatabaseConfig {
            url: Some(db_url.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    let mut app_builder = TestApp::new()
        .config(config)
        .plugin(webhook_plugin)
        .plugin(
            HarvestPlugin::new()
                .worker(WorkerConfig::default().with_queues(["webhooks"]))
                .api("/api/harvest"),
        )
        .with_db(pool);

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

    // 5. Wait up to 30 s for the Harvest workflow and activity to run.
    let mut logs = Vec::new();
    for _ in 0..300 {
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
