//! Global shared state and database pools used by the Harvest plugin.

use std::ops::Deref;

use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;

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
