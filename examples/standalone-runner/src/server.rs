use std::net::SocketAddr;

use autumn_harvest_plugin::prelude::*;
use autumn_web::config::DatabaseConfig;
use autumn_web::reexports::axum::{self, Json, routing::get};
use serde_json::json;

use crate::runtime::{standalone_builder, standalone_runtime_config};

/// Assemble the raw Axum app the runner listens on: the runner health route
/// plus `harvest_api_router` nested under `/api/harvest`.
///
/// Split out of [`run`] so a test can drive it with `tower::ServiceExt::oneshot`
/// without binding a socket or starting a `HarvestRunner` (see `tests.rs`).
pub fn build_router(api_state: HarvestApiState, web_state: autumn_web::AppState) -> axum::Router {
    axum::Router::new()
        .route(
            "/",
            get(|| async { Json(json!({ "service": "standalone-runner" })) }),
        )
        .nest("/api/harvest", harvest_api_router(api_state))
        .with_state(web_state)
}

/// Declare the deployment posture the README's own `Run` command sets
/// (`AUTUMN_PROFILE=dev`), so the documented `harvest preflight` step is
/// not gated forever (issue #1609).
///
/// A `HarvestPlugin` mount gets this call for free at startup
/// (`plugin.rs`'s `start_harvest_runtime`). A standalone mount builds its
/// own `HarvestApiState` and must call it directly, or the admin gate
/// (`require_admin`) stays fail-closed against every caller.
///
/// Split out of [`run`] so a test can drive the exact startup posture
/// without a process-wide `AUTUMN_PROFILE` env var (see `tests.rs`).
pub fn declare_deployment_profile(api_state: &HarvestApiState, autumn_profile: Option<&str>) {
    if autumn_profile == Some("dev") {
        api_state.set_deployment_profile("dev");
    }
}

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

    let config = standalone_runtime_config(database_url);
    let built = standalone_builder().try_build()?;
    let runner = HarvestRunner::start(built, &config, HarvestRunnerResources::new(pool.clone()))
        .await
        .map_err(|error| format!("failed to start Harvest runner: {error}"))?;

    let api_state = HarvestApiState::new();
    declare_deployment_profile(&api_state, std::env::var("AUTUMN_PROFILE").ok().as_deref());
    api_state.install_storage_pool(runner.storage_pool());
    api_state.install(runner.api_runtime());

    let web_state = autumn_web::AppState::for_test().with_pool(pool);
    let app = build_router(api_state, web_state);

    let address = SocketAddr::from(([127, 0, 0, 1], 8082));
    tracing::info!(%address, "standalone Harvest runner listening");
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    runner.stop().await;
    Ok(())
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to listen for shutdown signal");
    }
}
