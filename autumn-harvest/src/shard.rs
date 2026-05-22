//! Shard routing and per-shard database pools.
//!
//! Autumn-Harvest can spread workflow state across several independent
//! Postgres databases. Each workflow execution lives entirely on a single
//! shard — the event log, task queue rows, timers, signals, and dead-letter
//! entries for an execution all join back to the same database — so per-
//! workflow ACID guarantees are preserved without cross-shard transactions.
//!
//! This module provides the two primitives used to wire that design into the
//! runtime:
//!
//! * [`ShardRouter`] picks a [`ShardId`] for a *new* workflow. For existing
//!   workflows the shard is already encoded in the [`ExecutionId`] UUID and
//!   no routing decision is required.
//! * [`ShardedDbPool`] owns one [`crate::worker::DbPool`] per shard and
//!   resolves a pool from either a `ShardId` or an `ExecutionId`. Single-DB
//!   deployments use [`ShardedDbPool::single`], which places the only pool at
//!   `ShardId(0)` and behaves identically to the pre-sharding code path.
//!
//! Routing is deliberately directory-less. Because every `ExecutionId` writes
//! its shard into the UUID's first two bytes, any holder of an
//! `ExecutionId` can resolve to the correct pool in O(1). Lookups for ids that
//! were minted before sharding (or in tests) produce
//! [`ShardId::UNENCODED`]; the pool falls back to a configured default shard
//! for those cases.

#[cfg(feature = "db")]
use std::collections::BTreeMap;
use std::hash::Hasher;

use crate::types::{ExecutionId, ShardId};
#[cfg(feature = "db")]
use crate::worker::DbPool;

/// Decides which shard a newly started workflow should live on.
///
/// The router carries two lists:
///
/// * `readable_shards`: the superset of shards the deployment can load from.
///   Rendezvous-hashing is performed over this set so the hash is stable
///   across deployments where the writable set is being widened or narrowed
///   (e.g. adding a new shard for new workflows only).
/// * `writable_shards`: the subset that accepts *new* workflows. When the
///   initial rendezvous pick lands outside this subset the router re-hashes
///   among the writable subset.
///
/// Hashes use `seahash` rather than `std::hash` because `std::hash::BuildHasher`
/// is seeded randomly per-process and would produce different placements on
/// every boot, breaking idempotent outbox retries.
#[derive(Debug, Clone)]
pub struct ShardRouter {
    readable_shards: Vec<ShardId>,
    writable_shards: Vec<ShardId>,
    default_shard: ShardId,
}

impl ShardRouter {
    /// Build a router from an explicit list of readable and writable shards.
    ///
    /// `default_shard` is returned when a lookup is asked to resolve an
    /// `ExecutionId` that carries [`ShardId::UNENCODED`] in its shard bits.
    ///
    /// # Panics
    ///
    /// Panics if `readable_shards` is empty or if any entry in
    /// `writable_shards` is absent from `readable_shards`.
    #[must_use]
    pub fn new(
        readable_shards: Vec<ShardId>,
        writable_shards: Vec<ShardId>,
        default_shard: ShardId,
    ) -> Self {
        assert!(
            !readable_shards.is_empty(),
            "ShardRouter requires at least one readable shard"
        );
        for writable in &writable_shards {
            assert!(
                readable_shards.contains(writable),
                "writable shard {writable} is not in the readable set"
            );
        }
        Self {
            readable_shards,
            writable_shards,
            default_shard,
        }
    }

    /// Build a router for a single-shard deployment.
    ///
    /// Equivalent to the pre-sharding runtime: all workflows land on
    /// `ShardId(0)` and all reads resolve to the same database.
    #[must_use]
    pub fn single() -> Self {
        let shard = ShardId::new(0);
        Self::new(vec![shard], vec![shard], shard)
    }

    /// Shards this router accepts reads from.
    #[must_use]
    pub fn readable_shards(&self) -> &[ShardId] {
        &self.readable_shards
    }

    /// Shards this router accepts *new* workflows on.
    #[must_use]
    pub fn writable_shards(&self) -> &[ShardId] {
        &self.writable_shards
    }

    /// The shard returned when an `ExecutionId` carries the unencoded sentinel.
    #[must_use]
    pub const fn default_shard(&self) -> ShardId {
        self.default_shard
    }

    /// Pick a shard for a brand new workflow using rendezvous hashing.
    ///
    /// The input is `(workflow_name, workflow_id)` which uniquely identifies
    /// the logical workflow independent of its execution UUID — the same
    /// `(name, id)` therefore always hashes to the same shard, making outbox
    /// retries idempotent.
    #[must_use]
    pub fn pick_for_new_workflow(&self, workflow_name: &str, workflow_id: &str) -> ShardId {
        let initial = rendezvous_pick(&self.readable_shards, workflow_name, workflow_id);
        if self.writable_shards.contains(&initial) {
            return initial;
        }
        if self.writable_shards.is_empty() {
            return self.default_shard;
        }
        rendezvous_pick(&self.writable_shards, workflow_name, workflow_id)
    }

    /// Resolve the shard for an arbitrary `ExecutionId`.
    ///
    /// Returns the encoded shard when present, or the configured default
    /// shard for ids carrying [`ShardId::UNENCODED`].
    #[must_use]
    pub fn shard_for_execution(&self, exec_id: ExecutionId) -> ShardId {
        let encoded = exec_id.shard();
        if encoded.is_unencoded() {
            return self.default_shard;
        }
        if self.readable_shards.contains(&encoded) {
            encoded
        } else {
            self.default_shard
        }
    }

    /// Pick a shard for a DAG at catalog-compile time.
    ///
    /// DAG schedules (`harvest_schedules`) are scoped per database, so each
    /// DAG must be pinned to a single shard that owns it.
    /// The same name always maps to the same shard because rendezvous hashing
    /// is stable.
    #[must_use]
    pub fn pick_for_dag(&self, dag_name: &str) -> ShardId {
        let primary = if self.writable_shards.is_empty() {
            &self.readable_shards
        } else {
            &self.writable_shards
        };
        rendezvous_pick(primary, dag_name, "")
    }
}

impl Default for ShardRouter {
    fn default() -> Self {
        Self::single()
    }
}

fn rendezvous_pick(shards: &[ShardId], primary: &str, secondary: &str) -> ShardId {
    debug_assert!(!shards.is_empty(), "rendezvous pick requires candidates");
    let mut best = shards[0];
    let mut best_hash = rendezvous_hash(best, primary, secondary);
    for shard in &shards[1..] {
        let candidate = rendezvous_hash(*shard, primary, secondary);
        if candidate > best_hash {
            best = *shard;
            best_hash = candidate;
        }
    }
    best
}

fn rendezvous_hash(shard: ShardId, primary: &str, secondary: &str) -> u64 {
    let mut hasher = seahash::SeaHasher::new();
    hasher.write_i32(shard.as_i32());
    hasher.write(primary.as_bytes());
    hasher.write_u8(0);
    hasher.write(secondary.as_bytes());
    hasher.finish()
}

/// Collection of [`DbPool`] handles keyed by [`ShardId`].
///
/// In single-shard deployments the map has one entry and
/// [`ShardedDbPool::pool_for`] / [`ShardedDbPool::pool_for_execution`] always
/// return it. Multi-shard deployments populate one pool per shard and rely on
/// the encoded shard bits in each [`ExecutionId`] for routing.
#[cfg(feature = "db")]
#[derive(Clone)]
pub struct ShardedDbPool {
    pools: BTreeMap<ShardId, DbPool>,
    default_shard: ShardId,
}

#[cfg(feature = "db")]
impl std::fmt::Debug for ShardedDbPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedDbPool")
            .field("shards", &self.pools.keys())
            .field("default_shard", &self.default_shard)
            .finish()
    }
}

#[cfg(feature = "db")]
pub static GLOBAL_SHARDED_POOL: std::sync::RwLock<Option<ShardedDbPool>> =
    std::sync::RwLock::new(None);

#[cfg(feature = "db")]
impl ShardedDbPool {
    /// Wrap an existing single pool as a one-shard sharded pool at `ShardId(0)`.
    ///
    /// This is the shape used by every pre-sharding deployment. All lookups
    /// resolve to the same pool and no behavior changes.
    #[must_use]
    pub fn single(pool: DbPool) -> Self {
        let shard = ShardId::new(0);
        let mut pools = BTreeMap::new();
        pools.insert(shard, pool);
        let this = Self {
            pools,
            default_shard: shard,
        };
        if let Ok(mut lock) = GLOBAL_SHARDED_POOL.write() {
            *lock = Some(this.clone());
        }
        this
    }

    /// Build a sharded pool from a pre-computed map of shard → pool.
    ///
    /// # Panics
    ///
    /// Panics if `pools` is empty or does not contain `default_shard`.
    #[must_use]
    pub fn from_map(pools: BTreeMap<ShardId, DbPool>, default_shard: ShardId) -> Self {
        assert!(
            !pools.is_empty(),
            "ShardedDbPool requires at least one pool"
        );
        assert!(
            pools.contains_key(&default_shard),
            "default_shard {default_shard} has no configured pool"
        );
        let this = Self {
            pools,
            default_shard,
        };
        if let Ok(mut lock) = GLOBAL_SHARDED_POOL.write() {
            *lock = Some(this.clone());
        }
        this
    }

    /// The default shard used when an `ExecutionId` carries the unencoded
    /// sentinel or references a shard that isn't configured locally.
    #[must_use]
    pub const fn default_shard(&self) -> ShardId {
        self.default_shard
    }

    /// Look up the pool for a shard. Falls back to the default shard when the
    /// requested shard is not present in this map.
    ///
    /// # Panics
    ///
    /// Panics only if the pool was constructed by bypassing the public API
    /// and the default shard entry has been removed; [`ShardedDbPool::single`]
    /// and [`ShardedDbPool::from_map`] guarantee a default entry exists.
    #[must_use]
    pub fn pool_for(&self, shard: ShardId) -> &DbPool {
        self.pools
            .get(&shard)
            .or_else(|| self.pools.get(&self.default_shard))
            .expect("default shard pool is always present")
    }

    /// Resolve the pool that owns a given `ExecutionId`.
    #[must_use]
    pub fn pool_for_execution(&self, exec_id: ExecutionId) -> &DbPool {
        let shard = exec_id.shard();
        if shard.is_unencoded() {
            return self.pool_for(self.default_shard);
        }
        self.pool_for(shard)
    }

    /// Iterate over `(shard, pool)` pairs in ascending shard order.
    pub fn iter_shards(&self) -> impl Iterator<Item = (ShardId, &DbPool)> {
        self.pools.iter().map(|(shard, pool)| (*shard, pool))
    }

    /// Shards this pool serves, in ascending order.
    #[must_use]
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.pools.keys().copied().collect()
    }

    /// How many shards are represented.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pools.len()
    }

    /// Is this an empty pool map?
    ///
    /// Always `false` for values constructed through the public API but
    /// exposed for completeness.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router_with(shards: &[i32]) -> ShardRouter {
        let ids: Vec<ShardId> = shards.iter().copied().map(ShardId::new).collect();
        ShardRouter::new(ids.clone(), ids.clone(), ids[0])
    }

    #[test]
    fn single_router_always_returns_shard_zero() {
        let router = ShardRouter::single();
        assert_eq!(
            router.pick_for_new_workflow("onboarding", "user-1"),
            ShardId::new(0)
        );
        assert_eq!(
            router.pick_for_new_workflow("etl", "nightly"),
            ShardId::new(0)
        );
        assert_eq!(router.default_shard(), ShardId::new(0));
    }

    #[test]
    fn rendezvous_hash_is_stable_across_runs() {
        let router = router_with(&[0, 1, 2, 3]);
        let a = router.pick_for_new_workflow("onboarding", "user-42");
        let b = router.pick_for_new_workflow("onboarding", "user-42");
        assert_eq!(a, b);
    }

    #[test]
    fn rendezvous_hash_distributes_across_shards() {
        let router = router_with(&[0, 1, 2]);
        let mut counts = [0usize; 3];
        for i in 0..300 {
            let shard = router.pick_for_new_workflow("onboarding", &format!("user-{i}"));
            counts[usize::try_from(shard.as_i32()).unwrap()] += 1;
        }
        // With 300 samples across 3 shards we expect ~100 each; allow a wide
        // band to stay robust against hash skew.
        for count in counts {
            assert!(count > 50, "shard counts too imbalanced: {counts:?}");
            assert!(count < 200, "shard counts too imbalanced: {counts:?}");
        }
    }

    #[test]
    fn writable_subset_redirects_when_initial_pick_is_read_only() {
        let readable = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
        let writable = vec![ShardId::new(1)];
        let router = ShardRouter::new(readable, writable, ShardId::new(0));

        for i in 0..20 {
            let picked = router.pick_for_new_workflow("wf", &format!("id-{i}"));
            assert_eq!(picked, ShardId::new(1));
        }
    }

    #[test]
    fn shard_for_execution_falls_back_on_unencoded_sentinel() {
        let router = router_with(&[0, 1, 2]);
        let unencoded = ExecutionId::new();
        assert_eq!(router.shard_for_execution(unencoded), ShardId::new(0));
    }

    #[test]
    fn shard_for_execution_honours_encoded_shard() {
        let router = router_with(&[0, 1, 2]);
        let id = ExecutionId::new_for_shard(ShardId::new(2));
        assert_eq!(router.shard_for_execution(id), ShardId::new(2));
    }

    #[test]
    fn shard_for_execution_falls_back_when_shard_is_unknown() {
        let router = router_with(&[0, 1]);
        let id = ExecutionId::new_for_shard(ShardId::new(7));
        assert_eq!(router.shard_for_execution(id), ShardId::new(0));
    }

    #[test]
    fn pick_for_dag_is_stable() {
        let router = router_with(&[0, 1, 2, 3]);
        let a = router.pick_for_dag("daily_etl");
        let b = router.pick_for_dag("daily_etl");
        assert_eq!(a, b);
    }
}
