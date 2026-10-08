//! Issue #1146 review (Codex P1): the runtime's *effective* pool and the
//! process global must not diverge.
//!
//! `ShardedDbPool::single` / `from_map` self-install into `GLOBAL_SHARDED_POOL`
//! at construction, so the global reflects whichever pool was built **last**.
//! `resolve_runtime_storage_pool` picks by precedence — a
//! `HarvestRunnerResources::sharded_pool` override beats a
//! `WorkerConfig::with_sharded_pool` — and its sharded arm used to wrap that
//! choice without re-installing it. Build the multi-shard override first and a
//! single-shard worker-config pool second, and the runtime runs multi-shard
//! while the global says single-shard.
//!
//! That divergence is not cosmetic. Every consumer of the global reads the
//! wrong topology, including `deployment_is_multi_shard`, which gates inline
//! by-id delivery: it answers "single shard", inline delivery is allowed, and a
//! caller whose shard holds a stale terminal copy of the key records a
//! permanent `not_running` against a run that is live on another shard — the
//! exact wrong terminal issue #1146 exists to remove.
//!
//! Own binary: `GLOBAL_SHARDED_POOL` is a process global, so a parallel test
//! constructing any `ShardedDbPool` would race the assertions below.

use autumn_harvest::shard::{GLOBAL_SHARDED_POOL, ShardedDbPool};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::runner::resolve_runtime_storage_pool;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use std::collections::BTreeMap;

/// Build a pool tagged by its `max_size`, readable without connecting.
fn tagged_pool(max_size: usize) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
        "postgres://unused@127.0.0.1:1/none",
    );
    deadpool::managed::Pool::builder(manager)
        .max_size(max_size)
        .build()
        .expect("build tagged pool")
}

/// The shard ids currently installed in the process global.
fn global_shard_ids() -> Vec<ShardId> {
    GLOBAL_SHARDED_POOL
        .read()
        .expect("read global sharded pool")
        .as_ref()
        .map(ShardedDbPool::shard_ids)
        .unwrap_or_default()
}

/// The `max_size` tag of the global's shard-0 pool, identifying *which* pool.
fn global_shard0_tag() -> Option<usize> {
    GLOBAL_SHARDED_POOL
        .read()
        .expect("read global sharded pool")
        .as_ref()
        .map(|sp| sp.pool_for(ShardId::new(0)).status().max_size)
}

#[test]
fn resolving_a_multi_shard_override_installs_it_over_a_later_single_shard_pool() {
    let harvest = tagged_pool(3);

    // The runner-level override: multi-shard, built FIRST.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), tagged_pool(11));
    pools.insert(ShardId::new(1), tagged_pool(12));
    let resources_override = ShardedDbPool::from_map(pools, ShardId::new(0));

    // A single-shard worker-config pool, built SECOND — self-installing over
    // the multi-shard global. This is the divergence the fix closes.
    let worker_config_sharded = ShardedDbPool::single(tagged_pool(7));

    assert_eq!(
        global_shard_ids().len(),
        1,
        "precondition: the later single-shard construction owns the global, \
         so the global disagrees with the pool precedence will select",
    );

    let resolved = resolve_runtime_storage_pool(
        Some(&resources_override),
        Some(&worker_config_sharded),
        &harvest,
    );

    // Precedence is unchanged: the runner-level override still wins.
    assert_eq!(
        resolved.clone_inner().status().max_size,
        11,
        "runner-level sharded_pool override must win over WorkerConfig",
    );

    // ...and the global now agrees with it.
    assert_eq!(
        global_shard_ids(),
        vec![ShardId::new(0), ShardId::new(1)],
        "the global must carry the topology the runtime resolved, not the \
         topology of whichever pool happened to be constructed last",
    );
    assert_eq!(
        global_shard0_tag(),
        Some(11),
        "the installed pool must be the selected one, not a same-shaped clone",
    );

    // The consequence that matters: the inline by-id gate reads this global.
    // Before the fix it saw one shard and allowed inline delivery.
    assert!(
        autumn_harvest::external_target_location::deployment_is_multi_shard_for_tests(),
        "a runtime running multi-shard must gate by-id delivery as multi-shard",
    );
}
