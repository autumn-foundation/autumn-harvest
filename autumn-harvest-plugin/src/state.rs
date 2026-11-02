//! Global shared state and database pools used by the Harvest plugin.

use std::ops::Deref;

use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;

/// The app pool as a Postgres pool, for the Postgres path (issue #2467).
///
/// Under `sqlite` the app pool is SQLite, so this returns `None`. Split and
/// external mode run the Postgres path with its own Harvest pool (issue
/// #2472).
#[cfg_attr(
    feature = "sqlite",
    allow(
        clippy::missing_const_for_fn,
        reason = "only the sqlite body could be const; the Postgres body cannot"
    )
)]
pub(crate) fn app_postgres_pool(state: &autumn_web::AppState) -> Option<DbPool> {
    #[cfg(not(feature = "sqlite"))]
    {
        state.pool().cloned()
    }
    #[cfg(feature = "sqlite")]
    {
        let _ = state;
        None
    }
}

/// A Postgres pool from `config`.
///
/// The default build asks `autumn-web`. Under `sqlite`, `autumn-web` builds
/// SQLite pools only, so the pool comes from [`postgres_pool_on_sqlite`]
/// (issue #2472).
///
/// # Errors
///
/// Returns the pool error, or a refusal of a URL that is not Postgres.
pub(crate) fn create_postgres_pool(
    config: &autumn_web::config::DatabaseConfig,
) -> Result<Option<DbPool>, String> {
    #[cfg(not(feature = "sqlite"))]
    {
        autumn_web::db::create_pool(config).map_err(|error| error.to_string())
    }
    #[cfg(feature = "sqlite")]
    {
        postgres_pool_on_sqlite(config)
    }
}

/// A Postgres pool in a build where `autumn-web` is SQLite (issue #2472).
///
/// It keeps the `autumn-web` pool settings: the size, the wait and create
/// timeouts, and fast recycling. Each connection opens with
/// [`autumn_harvest::pg_tls::connect`]. Its TLS rules differ from those of
/// `autumn-web`: `sslmode=require` checks the certificate chain and the host
/// name, and `sslrootcert` is not read. `SSL_CERT_FILE` names a private CA.
#[cfg(feature = "sqlite")]
fn postgres_pool_on_sqlite(
    config: &autumn_web::config::DatabaseConfig,
) -> Result<Option<DbPool>, String> {
    use diesel_async::AsyncPgConnection;
    use diesel_async::pooled_connection::{
        AsyncDieselConnectionManager, ManagerConfig, RecyclingMethod,
    };

    let Some(url) = config.effective_primary_url() else {
        return Ok(None);
    };
    if autumn_web::config::DatabaseBackend::detect(url)
        != Some(autumn_web::config::DatabaseBackend::Postgres)
    {
        return Err(
            "harvest.database.url is not a Postgres URL. Split and external mode keep \
             Harvest in Postgres (autumn-harvest#2472)"
                .to_owned(),
        );
    }
    let mut manager = ManagerConfig::<AsyncPgConnection>::default();
    manager.recycling_method = RecyclingMethod::Fast;
    manager.custom_setup = Box::new(|url| Box::pin(autumn_harvest::pg_tls::connect(url)));
    let timeout = std::time::Duration::from_secs(config.connect_timeout_secs);
    deadpool::managed::Pool::builder(AsyncDieselConnectionManager::new_with_config(url, manager))
        .max_size(config.effective_primary_pool_size().max(1))
        .wait_timeout(Some(timeout))
        .create_timeout(Some(timeout))
        .runtime(deadpool::Runtime::Tokio1)
        .build()
        .map(Some)
        .map_err(|error| format!("cannot build the Postgres Harvest pool: {error}"))
}

/// Application/business database role exposed to Harvest activities.
///
/// In embedded mode this may point to the same underlying Postgres pool as
/// Harvest system storage. In split mode it intentionally stays bound to the
/// app database so activities can touch business tables explicitly.
#[derive(Clone)]
pub struct AppDbPool(DbPool);

impl AppDbPool {
    #[must_use]
    pub fn clone_inner(&self) -> DbPool {
        self.0.clone()
    }
}

impl From<DbPool> for AppDbPool {
    fn from(pool: DbPool) -> Self {
        Self(pool)
    }
}

impl Deref for AppDbPool {
    type Target = DbPool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for AppDbPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppDbPool")
            .field("max_size", &self.0.status().max_size)
            .finish()
    }
}

/// Harvest system-storage database role used for queue, history, and scheduler
/// tables.
///
/// In single-shard deployments this wraps exactly one Postgres pool; all
/// lookups resolve to the same database. In sharded deployments it carries a
/// [`ShardedDbPool`] with one entry per shard and routes lookups either by an
/// explicit [`ShardId`] or by the shard encoded in an [`ExecutionId`].
#[derive(Clone)]
pub struct HarvestDbPool {
    sharded: ShardedDbPool,
}

impl HarvestDbPool {
    /// Build a single-shard pool from a plain [`DbPool`].
    ///
    /// Equivalent to the pre-sharding shape: the wrapped pool lives at
    /// `ShardId(0)` and all lookups resolve to it.
    #[must_use]
    pub fn single(pool: DbPool) -> Self {
        Self {
            sharded: ShardedDbPool::single(pool),
        }
    }

    /// Build a pool from a pre-populated [`ShardedDbPool`].
    #[must_use]
    pub const fn sharded(sharded: ShardedDbPool) -> Self {
        Self { sharded }
    }

    /// Clone the underlying deadpool handle for the default shard.
    ///
    /// This is the pool worker and scheduler tasks run against in
    /// single-shard deployments. Multi-shard workers should instead iterate
    /// [`HarvestDbPool::iter_shards`] and spawn one task per assigned shard.
    #[must_use]
    pub fn clone_inner(&self) -> DbPool {
        self.default_pool().clone()
    }

    /// Return the pool for the configured default shard.
    #[must_use]
    pub fn default_pool(&self) -> &DbPool {
        self.sharded.pool_for(self.sharded.default_shard())
    }

    /// The configured default shard's id.
    #[must_use]
    pub const fn default_shard(&self) -> ShardId {
        self.sharded.default_shard()
    }

    /// Return the pool that owns a given workflow execution.
    ///
    /// `ExecutionId` carries its shard identifier in the first two bytes so
    /// this is an O(1) lookup that never reaches the database.
    #[must_use]
    pub fn pool_for_execution(&self, exec_id: ExecutionId) -> &DbPool {
        self.sharded.pool_for_execution(exec_id)
    }

    /// Return the pool for an explicit shard.
    ///
    /// Falls back to the default shard's pool when `shard` has no pool of its
    /// own. Use [`HarvestDbPool::exact_pool_for`] when that fallback would be
    /// incorrect.
    #[must_use]
    pub fn pool_for(&self, shard: ShardId) -> &DbPool {
        self.sharded.pool_for(shard)
    }

    /// Return the pool for an explicit shard, with **no** default fallback.
    ///
    /// Required wherever silently using the default shard's database would be
    /// wrong rather than merely suboptimal — notably an explicitly pinned
    /// workflow start (issue #697), where falling back would write
    /// residency-bound data to the wrong database while still reporting the
    /// pinned shard to the caller. The router's shard set and the pool map are
    /// configured independently, so a shard known to the router but not yet
    /// pooled (mid a shard-add rollout) is a real state.
    #[must_use]
    pub fn exact_pool_for(&self, shard: ShardId) -> Option<&DbPool> {
        self.sharded.exact_pool_for(shard)
    }

    /// Iterate `(shard, pool)` pairs.
    pub fn iter_shards(&self) -> impl Iterator<Item = (ShardId, &DbPool)> {
        self.sharded.iter_shards()
    }

    /// Access the underlying [`ShardedDbPool`].
    #[must_use]
    pub const fn sharded_pool(&self) -> &ShardedDbPool {
        &self.sharded
    }
}

impl From<DbPool> for HarvestDbPool {
    fn from(pool: DbPool) -> Self {
        Self::single(pool)
    }
}

impl From<ShardedDbPool> for HarvestDbPool {
    fn from(sharded: ShardedDbPool) -> Self {
        Self::sharded(sharded)
    }
}

impl Deref for HarvestDbPool {
    type Target = DbPool;

    fn deref(&self) -> &Self::Target {
        self.default_pool()
    }
}

impl std::fmt::Debug for HarvestDbPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HarvestDbPool")
            .field("shards", &self.sharded.shard_ids())
            .field("default_shard", &self.sharded.default_shard())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_web::config::DatabaseConfig;

    fn config(url: Option<&str>, pool_size: usize) -> DatabaseConfig {
        DatabaseConfig {
            url: url.map(str::to_owned),
            pool_size,
            ..DatabaseConfig::default()
        }
    }

    /// A SQLite app in split mode still gets a Postgres Harvest pool. The
    /// pool connects lazily, so no server is needed here (issue #2472).
    #[tokio::test]
    async fn a_postgres_pool_builds_in_every_build() {
        let pool = create_postgres_pool(&config(Some("postgres://h:h@localhost:5432/h"), 4))
            .expect("the pool builds")
            .expect("a url gives a pool");
        assert_eq!(pool.status().max_size, 4);
    }

    #[test]
    fn no_url_gives_no_pool() {
        assert!(create_postgres_pool(&config(None, 4)).unwrap().is_none());
    }

    /// A SQLite URL is not a Postgres Harvest database.
    #[test]
    fn a_sqlite_url_is_refused() {
        let Err(error) = create_postgres_pool(&config(Some("sqlite://h.db"), 4)) else {
            panic!("a SQLite URL must not give a Postgres pool");
        };
        assert!(error.contains("Postgres"), "{error}");
    }

    /// The SQLite app pool never reaches a Postgres path (issue #2472).
    #[cfg(feature = "sqlite")]
    #[test]
    fn the_sqlite_app_pool_is_never_a_postgres_pool() {
        assert!(app_postgres_pool(&autumn_web::AppState::for_test()).is_none());
    }
}
