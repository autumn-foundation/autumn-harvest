use std::net::SocketAddr;

use autumn_harvest_plugin::HarvestEmbedding;
use autumn_harvest_plugin::api::{HarvestApiState, StandaloneAdminAuth};
use autumn_harvest_plugin::embedding::ambient_deployment_profile;
use autumn_harvest_plugin::metrics_scrape::HarvestMetricsRecorder;
use autumn_harvest_plugin::prelude::*;
use autumn_harvest_plugin::webhook_receiver::{
    WebhookConfig, WebhookEndpointConfig, build_webhook_router,
};
use axum::{Json, routing::get};
use serde_json::json;

use crate::db;
use crate::runtime::{standalone_builder, standalone_runtime_config};
use crate::{webhooks, workflows};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const DEFAULT_DATABASE_URL: &str = "postgres://runner:runner@localhost:5434/runner";
const DEFAULT_ADDR: &str = "127.0.0.1:8082";

/// Assemble the raw Axum app the runner listens on.
///
/// It holds the runner health route and a Prometheus scrape route fed by
/// `metrics`. It nests `harvest` under `/api/harvest` and merges `webhooks`
/// at the root.
///
/// Every input is a `Router<()>`, so the app carries no framework state.
pub fn build_router(
    harvest: axum::Router,
    metrics: HarvestMetricsRecorder,
    webhooks: Option<axum::Router>,
) -> axum::Router {
    let app = axum::Router::new()
        .route(
            "/",
            get(|| async { Json(json!({ "service": "standalone-runner" })) }),
        )
        .route(
            "/metrics",
            get(move || {
                let metrics = metrics.clone();
                async move {
                    (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "text/plain; version=0.0.4",
                        )],
                        metrics.render_prometheus(),
                    )
                }
            }),
        )
        .nest("/api/harvest", harvest);
    match webhooks {
        Some(webhooks) => app.merge(webhooks),
        None => app,
    }
}

/// The signed order webhook, verified with `secret`.
///
/// Replay protection is off. The mapped workflow id dedupes a redelivery.
pub fn order_webhook_router(
    api_state: &HarvestApiState,
    secret: &str,
) -> Result<axum::Router, BoxError> {
    let config = WebhookConfig {
        endpoints: vec![
            WebhookEndpointConfig::generic("orders", webhooks::ORDER_WEBHOOK_PATH, secret)
                .without_replay_protection(),
        ],
        ..WebhookConfig::default()
    };
    let router = build_webhook_router(
        &webhooks::webhooks(),
        &workflows::workflows(),
        &[],
        api_state,
        &config,
    )?;
    Ok(router)
}

/// Migrate in `dev`, start Harvest, and serve until Ctrl-C.
///
/// Each step that can fail runs before `start`. After `start`, the runtime
/// always stops, also when the server fails.
pub async fn run() -> Result<(), BoxError> {
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned());
    let address: SocketAddr = std::env::var("STANDALONE_RUNNER_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_owned())
        .parse()?;

    // `with_ambient_profile` below reads the same profile.
    if ambient_deployment_profile().as_deref() == Some("dev") {
        let report = db::run_pending_migrations(&database_url).await?;
        tracing::info!(applied = report.applied.len(), "applied Harvest migrations");
    }
    let pool = db::create_pool(&database_url)?;

    // The webhook router and the runtime share this state.
    let api_state = HarvestApiState::new();
    let webhooks = std::env::var("STANDALONE_RUNNER_WEBHOOK_SECRET")
        .ok()
        .map(|secret| order_webhook_router(&api_state, &secret))
        .transpose()?;
    let listener = tokio::net::TcpListener::bind(address).await?;

    let metrics = HarvestMetricsRecorder::new();
    let harvest = HarvestEmbedding::new(
        standalone_builder(metrics.clone()).try_build()?,
        standalone_runtime_config(database_url),
        HarvestRunnerResources::new(pool),
    )
    .with_api_state(api_state)
    .with_admin_auth(StandaloneAdminAuth::new().with_api_tokens())
    .with_ambient_profile()
    .start()
    .await
    .map_err(|error| format!("failed to start Harvest: {error}"))?;

    let app = build_router(harvest.router(), metrics, webhooks);
    tracing::info!(%address, "standalone Harvest runner listening");
    let served = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    harvest.stop().await;
    Ok(served?)
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to listen for shutdown signal");
    }
}
