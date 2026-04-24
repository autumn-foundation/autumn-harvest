//! End-to-end-style tests for the shard routing primitives that do not
//! require a live Postgres instance.
//!
//! The integration tests that exercise multi-shard Postgres setups live under
//! `tests/multi_shard.rs` (planned follow-up) and require Docker; the checks
//! here validate the routing contract in isolation so the logic is covered
//! even on hosts that cannot spin up testcontainers.

use autumn_harvest::{ExecutionId, ShardId, ShardRouter};

#[test]
fn round_trip_preserves_encoded_shard_for_every_shard_in_a_three_shard_layout() {
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        ShardId::new(0),
    );

    for shard in [ShardId::new(0), ShardId::new(1), ShardId::new(2)] {
        let id = ExecutionId::new_for_shard(shard);
        assert_eq!(id.shard(), shard);
        assert_eq!(router.shard_for_execution(id), shard);
    }
}

#[test]
fn router_distributes_new_workflows_across_three_shards() {
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        ShardId::new(0),
    );

    let mut counts = [0usize; 3];
    for i in 0..300 {
        let shard = router.pick_for_new_workflow("onboarding", &format!("user-{i}"));
        counts[usize::try_from(shard.as_i32()).unwrap()] += 1;
    }

    // Rendezvous hashing will rarely be perfectly uniform; assert only that
    // every shard received a meaningful chunk of the load.
    for count in counts {
        assert!(count > 40, "uneven distribution: {counts:?}");
    }
}

#[test]
fn narrowing_writable_shards_redirects_new_workflows() {
    // Three shards are readable but only shard 1 is writable.
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        vec![ShardId::new(1)],
        ShardId::new(0),
    );

    for i in 0..25 {
        let shard = router.pick_for_new_workflow("wf", &format!("id-{i}"));
        assert_eq!(shard, ShardId::new(1));
    }

    // But reads for executions encoded on shard 2 still resolve there.
    let id_on_two = ExecutionId::new_for_shard(ShardId::new(2));
    assert_eq!(router.shard_for_execution(id_on_two), ShardId::new(2));
}

#[test]
fn unencoded_executions_fall_back_to_default_shard() {
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(1),
    );
    let legacy = ExecutionId::new();
    assert!(legacy.shard().is_unencoded());
    assert_eq!(router.shard_for_execution(legacy), ShardId::new(1));
}

#[test]
fn outbox_retry_picks_same_shard_for_same_workflow_key() {
    let router = ShardRouter::new(
        vec![
            ShardId::new(0),
            ShardId::new(1),
            ShardId::new(2),
            ShardId::new(3),
        ],
        vec![
            ShardId::new(0),
            ShardId::new(1),
            ShardId::new(2),
            ShardId::new(3),
        ],
        ShardId::new(0),
    );

    let first = router.pick_for_new_workflow("onboarding", "user-42");
    for _ in 0..1_000 {
        assert_eq!(router.pick_for_new_workflow("onboarding", "user-42"), first);
    }
}
