//! Issue #700: the boot-time orphaned-workflow gate must be side-effect-free
//! on `Abort`. It must mutate NO process global. The single case of
//! `resolve_runtime_storage_pool` goes through `HarvestDbPool::from` →
//! `ShardedDbPool::single`, which writes `GLOBAL_SHARDED_POOL`. The
//! pool-precedence fix added that latent side effect. Suppose the gate used
//! that install helper. An aborted second-runtime startup in the same process
//! would then leave completion-trigger and timeout code on the aborted
//! runtime's DB.
//!
//! The gate uses a READ-ONLY selector instead. It reads pools from an existing
//! `ShardedDbPool`, or returns `harvest_pool`, and installs nothing. Since
//! issue #1128 that selector is the cross-shard `select_runtime_gate_shards`.
//! Its single-shard public sibling `select_runtime_shard0_pool` has the same
//! precedence and no-install guarantee, so this test pins it too. The test has
//! its own binary, so `GLOBAL_SHARDED_POOL` starts unset and no parallel test
//! races it.

use autumn_harvest::shard::{GLOBAL_SHARDED_POOL, ShardedDbPool};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::runner::{
    resolve_runtime_storage_pool, select_runtime_gate_shards, select_runtime_shard0_pool,
};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;

/// Build a pool tagged by its `max_size` (readable without connecting).
fn tagged_pool(max_size: usize) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
        "postgres://unused@127.0.0.1:1/none",
    );
    deadpool::managed::Pool::builder(manager)
        .max_size(max_size)
        .build()
        .expect("build tagged pool")
}

/// The `max_size` tag of the shard-0 pool currently installed in the process
/// global, if any.
fn global_shard0_tag() -> Option<usize> {
    GLOBAL_SHARDED_POOL
        .read()
        .expect("read global sharded pool")
        .as_ref()
        .map(|sp| sp.pool_for(ShardId::new(0)).status().max_size)
}

#[test]
fn gate_pool_selection_does_not_install_global_pool() {
    // Fresh process: nothing installed yet.
    assert_eq!(
        global_shard0_tag(),
        None,
        "a fresh process must start with no installed global sharded pool",
    );

    let harvest = tagged_pool(31);

    // Constructing the sharded input DOES install (this is the ONLY install in
    // the test) — tag 71.
    let sharded = ShardedDbPool::single(tagged_pool(71));
    assert_eq!(
        global_shard0_tag(),
        Some(71),
        "ShardedDbPool::single installs the global (sanity: it is the writer)",
    );

    // Sharded case: the gate's selector reads shard 0 of the sharded pool and
    // must NOT touch the global.
    let selected = select_runtime_shard0_pool(None, Some(&sharded), &harvest);
    assert_eq!(
        selected.status().max_size,
        71,
        "select must return shard-0 of the sharded pool (the DB the workers poll)",
    );
    assert_eq!(
        global_shard0_tag(),
        Some(71),
        "select_runtime_shard0_pool (sharded case) must install/overwrite nothing",
    );

    // Single case: the gate's selector returns harvest_pool directly and must
    // NOT install it as a single-shard global.
    let selected = select_runtime_shard0_pool(None, None, &harvest);
    assert_eq!(
        selected.status().max_size,
        31,
        "select must return the harvest_pool handle directly",
    );
    assert_eq!(
        global_shard0_tag(),
        Some(71),
        "select_runtime_shard0_pool (single case) must NOT install harvest_pool globally",
    );

    // Issue #1128: the multi-shard gate selector must be side-effect free too.
    // It reads every shard of an existing pool, or returns `harvest_pool` as
    // shard 0. It never constructs a `ShardedDbPool`, which installs the global.
    let shards = select_runtime_gate_shards(None, Some(&sharded), &harvest, None);
    assert_eq!(
        shards
            .get(&0)
            .expect("shard 0 enumerated")
            .as_ref()
            .expect("shard 0 pool present")
            .status()
            .max_size,
        71,
        "the gate selector must enumerate the sharded pool's own shard-0 pool",
    );
    assert_eq!(
        global_shard0_tag(),
        Some(71),
        "select_runtime_gate_shards (sharded case) must install/overwrite nothing",
    );

    let shards = select_runtime_gate_shards(None, None, &harvest, None);
    assert_eq!(
        shards
            .get(&0)
            .expect("shard 0 enumerated")
            .as_ref()
            .expect("shard 0 pool present")
            .status()
            .max_size,
        31,
        "the gate selector must return the harvest_pool handle as shard 0",
    );
    assert_eq!(
        global_shard0_tag(),
        Some(71),
        "select_runtime_gate_shards (single case) must NOT install harvest_pool globally",
    );

    // Contrast: the INSTALL helper of the normal startup path DOES overwrite
    // the global (single case). So the two helpers differ in this side effect,
    // and the gate uses the read-only one.
    let _installed = resolve_runtime_storage_pool(None, None, &harvest);
    assert_eq!(
        global_shard0_tag(),
        Some(31),
        "resolve_runtime_storage_pool (the install helper) DOES install harvest_pool globally",
    );
}
