use std::net::SocketAddr;

use autumn_harvest_plugin::prelude::*;
use autumn_web::config::DatabaseConfig;
use autumn_web::reexports::axum::{self, Json, routing::get};
use serde_json::json;

use crate::runtime::{standalone_builder, standalone_runtime_config};

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
    api_state.install_storage_pool(runner.storage_pool());
    api_state.install(runner.api_runtime());

    let web_state = autumn_web::AppState::for_test().with_pool(pool);
    let app = axum::Router::new()
        .route(
            "/",
            get(|| async { Json(json!({ "service": "standalone-runner" })) }),
        )
        .nest("/api/harvest", harvest_api_router(api_state))
        .with_state(web_state);

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
