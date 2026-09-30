use std::net::SocketAddr;

use autumn_harvest_plugin::HarvestEmbedding;
use autumn_harvest_plugin::metrics_scrape::HarvestMetricsRecorder;
use autumn_harvest_plugin::prelude::*;
use autumn_web::config::DatabaseConfig;
use autumn_web::reexports::axum::{self, Json, routing::get};
use serde_json::json;

use crate::runtime::{standalone_builder, standalone_runtime_config};

/// Assemble the raw Axum app the runner listens on.
///
/// It holds the runner health route and a Prometheus scrape route fed by
/// `metrics`. It nests the `harvest` router under `/api/harvest`.
///
/// Split out of [`run`] so a test can drive it with `tower::ServiceExt::oneshot`
/// without a database (see `tests.rs`).
///
/// `harvest` is `Router<()>`, so the mount carries no autumn-web state (issue
/// #1607). `/metrics` needs none either (issue #1611).
pub fn build_router(harvest: axum::Router, metrics: HarvestMetricsRecorder) -> axum::Router {
    axum::Router::new()
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
        .nest("/api/harvest", harvest)
}

/// Build a pool, start Harvest through `HarvestEmbedding`, and serve.
///
/// `HarvestEmbedding` runs the whole startup sequence (issue #1613). It
/// applies the operator's startup config and reads `AUTUMN_PROFILE`. It loads
/// the persisted admission gates and installs the pool and the runtime.
pub async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://runner:runner@localhost:5434/runner".to_owned());
    if std::env::var("AUTUMN_PROFILE").as_deref() == Ok("dev") {
        autumn_web::migrate::run_pending(&database_url, autumn_harvest::MIGRATIONS)?;
    }

    let pool = autumn_web::db::create_pool(&DatabaseConfig {
        url: Some(database_url.clone()),
        ..DatabaseConfig::default()
    })?
    .ok_or("DATABASE_URL must create a Postgres pool")?;

    let metrics = HarvestMetricsRecorder::new();
    let harvest = HarvestEmbedding::new(
        standalone_builder(metrics.clone()).try_build()?,
        standalone_runtime_config(database_url),
        HarvestRunnerResources::new(pool),
    )
    .start()
    .await
    .map_err(|error| format!("failed to start Harvest: {error}"))?;

    let app = build_router(harvest.router(), metrics);
    let address = SocketAddr::from(([127, 0, 0, 1], 8082));
    tracing::info!(%address, "standalone Harvest runner listening");
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    harvest.stop().await;
    Ok(())
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to listen for shutdown signal");
    }
}
