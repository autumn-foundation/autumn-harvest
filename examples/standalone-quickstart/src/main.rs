//! The Chapter 2 workflow on a plain Axum server, with no `HarvestPlugin`.

mod workflows;

use autumn_harvest::diesel_async::AsyncPgConnection;
use autumn_harvest::diesel_async::pooled_connection::AsyncDieselConnectionManager;
use autumn_harvest::diesel_async::pooled_connection::deadpool::Pool;
use autumn_harvest_plugin::prelude::*;
use autumn_web::reexports::axum;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // Print the engine's log lines, for example the dev-profile warning.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let database_url = std::env::var("DATABASE_URL").map_err(|_| "set DATABASE_URL")?;
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&database_url);
    let pool = Pool::builder(manager).build()?;

    // The default config runs the worker and the scheduler in this process.
    // The outbox relay needs autumn-web, so `HarvestEmbedding` does not run it.
    let config = HarvestRuntimeConfig {
        database: HarvestDatabaseConfig {
            url: Some(database_url),
        },
        outbox: HarvestOutboxConfig {
            enabled: false,
            ..HarvestOutboxConfig::default()
        },
        ..HarvestRuntimeConfig::default()
    };

    // Run the startup sequence of `HarvestPlugin` without autumn-web's `AppBuilder`.
    let harvest = HarvestEmbedding::new(
        workflows::harvest_builder().try_build()?,
        config,
        HarvestRunnerResources::new(pool),
    )
    .with_ambient_profile()
    .start()
    .await
    .map_err(|error| format!("Harvest did not start: {error}"))?;

    let app = axum::Router::new().nest("/api/harvest", harvest.router());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    tracing::info!("listening on http://127.0.0.1:3000");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    // Drain the worker and remove the process globals.
    harvest.stop().await;
    Ok(())
}
