use std::future::IntoFuture as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

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

/// How long open responses may run after a stop signal. A stream never ends
/// on its own, so the server stops waiting after this.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

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
///
/// `build_webhook_router` checks only that the secret is not empty. With
/// `is_production`, this also refuses a short or demo secret.
pub fn order_webhook_router(
    api_state: &HarvestApiState,
    secret: &str,
    is_production: bool,
) -> Result<axum::Router, BoxError> {
    let config = WebhookConfig {
        endpoints: vec![
            WebhookEndpointConfig::generic("orders", webhooks::ORDER_WEBHOOK_PATH, secret)
                .without_replay_protection(),
        ],
        ..WebhookConfig::default()
    };
    config.validate(is_production)?;
    let router = build_webhook_router(
        &webhooks::webhooks(),
        &workflows::workflows(),
        &[],
        api_state,
        &config,
    )?;
    Ok(router)
}

/// Migrate in `dev`, start Harvest, and serve until Ctrl-C or SIGTERM.
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
    let is_dev = ambient_deployment_profile().as_deref() == Some("dev");
    if is_dev {
        let report = db::run_pending_migrations(&database_url).await?;
        tracing::info!(applied = report.applied.len(), "applied Harvest migrations");
    }
    let pool = db::create_pool(&database_url)?;

    // The webhook router and the runtime share this state.
    let api_state = HarvestApiState::new();
    let webhooks = std::env::var("STANDALONE_RUNNER_WEBHOOK_SECRET")
        .ok()
        .map(|secret| order_webhook_router(&api_state, &secret, !is_dev))
        .transpose()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    // Port 0 picks a free port, so log the address the socket really has.
    let address = listener.local_addr()?;

    let metrics = HarvestMetricsRecorder::new();
    let harvest = HarvestEmbedding::new(
        standalone_builder(metrics.clone()).try_build()?,
        standalone_runtime_config(database_url),
        HarvestRunnerResources::new(pool),
    )
    .with_api_state(api_state)
    .with_admin_auth(admin_auth(is_dev))
    .with_ambient_profile()
    .start()
    .await
    .map_err(|error| format!("failed to start Harvest: {error}"))?;

    let app = build_router(harvest.router(), metrics, webhooks);
    tracing::info!(%address, "standalone Harvest runner listening");
    let signalled = Arc::new(tokio::sync::Notify::new());
    let notify = Arc::clone(&signalled);
    let drain_state = harvest.api_state().clone();
    let serve = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            // Mark the drain before the listener closes. A probe in flight
            // then sees 503. The pod `preStop` sleep covers new traffic.
            drain_state.begin_draining();
            notify.notify_one();
        })
        .into_future();
    let served = tokio::select! {
        served = serve => served,
        () = async {
            signalled.notified().await;
            tokio::time::sleep(SHUTDOWN_GRACE).await;
        } => {
            tracing::warn!(grace = ?SHUTDOWN_GRACE, "closing responses still open at shutdown");
            Ok(())
        }
    };
    harvest.stop().await;
    Ok(served?)
}

/// The auth for the Harvest routes.
///
/// Outside `dev`, every route except the public ones needs a Harvest API
/// token. `harvest token bootstrap` seeds the first one.
///
/// In `dev`, there is no token layer, so the README quickstart works
/// without a token. The embedding logs that the API is then open.
fn admin_auth(is_dev: bool) -> StandaloneAdminAuth {
    if is_dev {
        StandaloneAdminAuth::new()
    } else {
        StandaloneAdminAuth::new().with_api_tokens()
    }
}

/// Resolve on Ctrl-C, or on SIGTERM from a process manager.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "failed to listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
