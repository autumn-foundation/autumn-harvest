//! The Harvest database, without `autumn-web`.
//!
//! `autumn_harvest::migrate` is the code behind `harvest migrate run`. The
//! pool is the `deadpool` pool that `diesel-async` builds.

use autumn_harvest::migrate::{self, MigrationReport, PartialMigration};
use autumn_harvest::worker::DbPool;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::{BuildError, Pool};

/// Connections the pool may hold. The worker, the scheduler and the API
/// share them.
const POOL_SIZE: usize = 16;

/// Build the Harvest storage pool. It opens no connection until first use.
pub fn create_pool(database_url: &str) -> Result<DbPool, BuildError> {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    Pool::builder(manager).max_size(POOL_SIZE).build()
}

/// Apply every pending Harvest migration to `database_url`.
pub async fn run_pending_migrations(
    database_url: &str,
) -> Result<MigrationReport, Box<PartialMigration>> {
    migrate::apply(database_url, &migrate::embedded()).await
}
