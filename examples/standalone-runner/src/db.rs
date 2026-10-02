//! The Harvest database, without `autumn-web`.
//!
//! `autumn_harvest::migrate` is the code behind `harvest migrate run`. The
//! pool is the `deadpool` pool that `diesel-async` builds.
//!
//! `diesel-async` connects without TLS. Every connection here goes through
//! `autumn_harvest::pg_tls` instead, so the `sslmode` picks the transport.
//! `prefer`, the default, uses TLS when the server offers it. A managed
//! Postgres such as Fly hands out a URL with no `sslmode` and refuses
//! plaintext, so the default must negotiate TLS.

use std::time::Duration;

use autumn_harvest::migrate::{self, MigrationReport};
use autumn_harvest::pg_tls::connect;
use autumn_harvest::worker::DbPool;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::deadpool::{BuildError, Pool};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use futures::FutureExt as _;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Connections the pool may hold. The worker, the scheduler and the API
/// share them.
const POOL_SIZE: usize = 16;

/// How long a checkout waits for a free or a new connection. Without a
/// limit, an unreachable database hangs startup and every request.
const CHECKOUT_TIMEOUT: Duration = Duration::from_secs(5);

/// Build the Harvest storage pool. It opens no connection until first use.
pub fn create_pool(database_url: &str) -> Result<DbPool, BuildError> {
    let mut config = ManagerConfig::<AsyncPgConnection>::default();
    config.custom_setup = Box::new(|url| connect(url).boxed());
    let manager =
        AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(database_url, config);
    Pool::builder(manager)
        .max_size(POOL_SIZE)
        .wait_timeout(Some(CHECKOUT_TIMEOUT))
        .create_timeout(Some(CHECKOUT_TIMEOUT))
        .runtime(deadpool::Runtime::Tokio1)
        .build()
}

/// Apply every pending Harvest migration to `database_url`.
pub async fn run_pending_migrations(database_url: &str) -> Result<MigrationReport, BoxError> {
    let mut conn = connect(database_url).await?;
    let report = migrate::apply_to_connection(&mut conn, &migrate::embedded()).await?;
    Ok(report)
}
