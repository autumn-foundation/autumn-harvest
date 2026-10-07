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

// ── Reserved (cell) shards, issue #1837 ─────────────────────────────────────

/// Three writable shards. Shard 1 is reserved for one tenant cell.
fn cell_router() -> ShardRouter {
    let all = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
    ShardRouter::new(all.clone(), all, ShardId::new(0))
        .with_residency_map([("cell-a".to_string(), ShardId::new(1))])
        .with_reserved_shards([ShardId::new(1)])
}

#[test]
fn auto_placement_never_picks_a_reserved_shard() {
    let router = cell_router();
    for i in 0..2_000 {
        let id = format!("tenant-b-{i}");
        assert_ne!(router.pick_for_new_workflow("wf", &id), ShardId::new(1));
        assert_ne!(router.pick_for_idempotency_key("wf", &id), ShardId::new(1));
        assert_ne!(router.pick_for_dag(&id), ShardId::new(1));
    }
}

#[test]
fn a_pin_still_reaches_a_reserved_shard() {
    use autumn_harvest::shard::ShardPlacement;
    let router = cell_router();
    let by_key = ShardPlacement::residency_key("cell-a");
    assert_eq!(
        router.resolve_placement(&by_key, "wf", "a-1"),
        Ok(ShardId::new(1))
    );
    let by_shard = ShardPlacement::Shard(ShardId::new(1));
    assert_eq!(
        router.resolve_placement(&by_shard, "wf", "a-2"),
        Ok(ShardId::new(1))
    );
    assert!(router.is_writable(ShardId::new(1)));
}

/// Only keys that hashed to the reserved shard move. All other keys keep
/// their shard, so a reservation does not reshuffle shared tenants.
#[test]
fn reserving_a_shard_moves_only_the_keys_that_hashed_to_it() {
    let all = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
    let plain = ShardRouter::new(all.clone(), all, ShardId::new(0));
    let cells = cell_router();
    let mut moved = 0;
    for i in 0..2_000 {
        let id = format!("k-{i}");
        let before = plain.pick_for_new_workflow("wf", &id);
        let after = cells.pick_for_new_workflow("wf", &id);
        if before == ShardId::new(1) {
            moved += 1;
        } else {
            assert_eq!(before, after, "key {id} moved off a shared shard");
        }
    }
    assert!(moved > 0, "the reserved shard received no keys before");
}

#[test]
fn no_reservation_leaves_placement_byte_identical() {
    let all = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
    let plain = ShardRouter::new(all.clone(), all, ShardId::new(0));
    let empty = plain.clone().with_reserved_shards([]);
    assert_eq!(empty.reserved_shards(), &[] as &[ShardId]);
    for i in 0..500 {
        let id = format!("k-{i}");
        assert_eq!(
            plain.pick_for_new_workflow("wf", &id),
            empty.pick_for_new_workflow("wf", &id)
        );
    }
}

#[test]
fn reserved_shards_are_sorted_and_deduplicated() {
    let all = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
    let router = ShardRouter::new(all.clone(), all, ShardId::new(0)).with_reserved_shards([
        ShardId::new(2),
        ShardId::new(1),
        ShardId::new(2),
    ]);
    assert_eq!(
        router.reserved_shards(),
        &[ShardId::new(1), ShardId::new(2)]
    );
    assert!(router.is_reserved(ShardId::new(2)));
    assert!(!router.is_reserved(ShardId::new(0)));
}

#[test]
#[should_panic(expected = "reserved shard 9 is not in the readable set")]
fn reserving_an_unknown_shard_panics_at_boot() {
    let all = vec![ShardId::new(0), ShardId::new(1)];
    let _ =
        ShardRouter::new(all.clone(), all, ShardId::new(0)).with_reserved_shards([ShardId::new(9)]);
}

#[test]
#[should_panic(expected = "leaves no writable shard for unpinned starts")]
fn reserving_every_writable_shard_panics_at_boot() {
    // Shard 0 is the default but drained, so shards 1 and 2 hold all writes.
    let readable = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
    let writable = vec![ShardId::new(1), ShardId::new(2)];
    let _ = ShardRouter::new(readable, writable.clone(), ShardId::new(0))
        .with_reserved_shards(writable);
}

#[test]
#[should_panic(expected = "reserved shard 0 is the default shard")]
fn reserving_the_default_shard_panics_at_boot() {
    let all = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
    let _ =
        ShardRouter::new(all.clone(), all, ShardId::new(0)).with_reserved_shards([ShardId::new(0)]);
}

#[test]
fn accepts_unpinned_excludes_reserved_and_drained_shards() {
    let readable = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
    let writable = vec![ShardId::new(0), ShardId::new(1)];
    let router = ShardRouter::new(readable, writable, ShardId::new(0))
        .with_reserved_shards([ShardId::new(1)]);
    assert!(router.accepts_unpinned(ShardId::new(0)));
    assert!(!router.accepts_unpinned(ShardId::new(1)), "reserved");
    assert!(!router.accepts_unpinned(ShardId::new(2)), "drained");
    assert!(
        router.is_writable(ShardId::new(1)),
        "a pin still reaches it"
    );
}

/// No shard is writable, and the default shard is not readable. Reserving
/// every readable shard would leave DAG placement only cell shards. Boot
/// refuses it.
#[test]
#[should_panic(expected = "leaves no readable shard for unpinned work")]
fn reserving_every_readable_shard_panics_at_boot() {
    let readable = vec![ShardId::new(1), ShardId::new(2)];
    let _ = ShardRouter::new(readable.clone(), Vec::new(), ShardId::new(0))
        .with_reserved_shards(readable);
}
