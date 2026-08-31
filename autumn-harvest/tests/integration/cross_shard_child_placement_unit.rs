//! Pure-logic coverage for cross-shard child placement (issue #956).
//!
//! No database, no feature gate — these run in the cheap
//! `cargo test -p autumn-harvest --no-default-features` CI step on every OS.
//!
//! Two surfaces are covered here:
//!
//! 1. **Placement resolution** (`shard::resolve_child_placement`) — the pure
//!    function every `spawn_child_workflow*` call site routes through to decide
//!    which shard a child lands on. The default (`ParentShard`) must never
//!    consult the router at all, so a deployment with no installed router keeps
//!    working byte-for-byte.
//! 2. **The relay state machine** (`shard::next_cross_shard_child_action`) — the
//!    decision the cross-shard scanner makes for one outbox row, factored out of
//!    the database so every branch (start, cancel, terminal delivery, close
//!    cascade, retire) is exhaustively testable without Postgres.
//!
//! The multi-shard *runtime* coverage (real child rows on a second database,
//! real parent wakes) lives in `cross_shard_children_tests.rs`, which needs
//! Docker/Postgres.

use autumn_harvest::shard::{
    ChildPlacement, CrossShardChildAction, CrossShardChildObservation, CrossShardChildStatus,
    next_cross_shard_child_action, resolve_child_placement,
};
use autumn_harvest::types::{ExecutionId, ParentClosePolicy, ShardId};
use autumn_harvest::{HarvestError, ShardRouter};

fn four_shard_router() -> ShardRouter {
    ShardRouter::new(
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
    )
}

// ── AC1: the default is parent-shard pinning, unchanged ───────────────────────

#[test]
fn parent_shard_is_the_default_placement() {
    assert_eq!(ChildPlacement::default(), ChildPlacement::ParentShard);
}

#[test]
fn parent_shard_placement_resolves_to_the_parent_shard_with_no_router() {
    // The load-bearing property: `ParentShard` must resolve without a router,
    // because that is what every single-shard deployment (and every existing
    // test) runs with. Passing `None` here is the strongest possible assertion
    // that the router is not consulted.
    for shard in [0, 1, 7, 65534] {
        let parent = ShardId::new(shard);
        let resolved =
            resolve_child_placement(None, &ChildPlacement::ParentShard, parent, "child_wf", "k")
                .expect("parent-shard placement never fails");
        assert_eq!(resolved, parent);
    }
}

#[test]
fn parent_shard_placement_ignores_the_router_even_when_one_is_installed() {
    let router = four_shard_router();
    let parent = ShardId::new(2);
    let resolved = resolve_child_placement(
        Some(&router),
        &ChildPlacement::ParentShard,
        parent,
        "child_wf",
        "parent#0",
    )
    .expect("parent-shard placement never fails");
    assert_eq!(resolved, parent);
}

// ── AC2: rendezvous over writable_shards, restart-stable ─────────────────────

#[test]
fn distributed_placement_matches_the_top_level_start_rendezvous_pick() {
    let router = four_shard_router();
    let key = "01234567-89ab-cdef-0123-456789abcdef#3";
    let resolved = resolve_child_placement(
        Some(&router),
        &ChildPlacement::Distributed,
        ShardId::new(0),
        "child_wf",
        key,
    )
    .expect("distributed placement resolves");
    // Identical to what a *top-level* start with the same (name, id) would get:
    // one rendezvous function, one restart-stability contract.
    assert_eq!(resolved, router.pick_for_new_workflow("child_wf", key));
}

#[test]
fn distributed_placement_is_deterministic_for_the_same_key() {
    let router = four_shard_router();
    let first = resolve_child_placement(
        Some(&router),
        &ChildPlacement::Distributed,
        ShardId::new(1),
        "child_wf",
        "parent#7",
    )
    .unwrap();
    for _ in 0..32 {
        let again = resolve_child_placement(
            Some(&router),
            &ChildPlacement::Distributed,
            ShardId::new(1),
            "child_wf",
            "parent#7",
        )
        .unwrap();
        assert_eq!(first, again, "placement must be restart-stable");
    }
}

#[test]
fn distributed_placement_never_picks_a_drained_shard() {
    // Shard 3 is readable but drained out of the writable set. A child must
    // never be placed there, exactly like a top-level start.
    let router = ShardRouter::new(
        vec![
            ShardId::new(0),
            ShardId::new(1),
            ShardId::new(2),
            ShardId::new(3),
        ],
        vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        ShardId::new(0),
    );
    for i in 0..500 {
        let resolved = resolve_child_placement(
            Some(&router),
            &ChildPlacement::Distributed,
            ShardId::new(0),
            "child_wf",
            &format!("parent#{i}"),
        )
        .unwrap();
        assert_ne!(resolved, ShardId::new(3), "drained shard was selected");
    }
}

#[test]
fn distributed_placement_on_a_single_shard_router_stays_on_the_parent_shard() {
    // AC1's "every existing single-shard deployment is unaffected" also has to
    // hold for code that opts *in*: with one writable shard the rendezvous pick
    // is that shard, which is the parent's, so nothing goes cross-shard.
    let router = ShardRouter::single();
    for i in 0..64 {
        let resolved = resolve_child_placement(
            Some(&router),
            &ChildPlacement::Distributed,
            ShardId::new(0),
            "child_wf",
            &format!("parent#{i}"),
        )
        .unwrap();
        assert_eq!(resolved, ShardId::new(0));
    }
}

// ── Success metric: ±10% of the rendezvous-uniform distribution ──────────────

const FAN_OUT_N: usize = 10_000;

#[test]
#[allow(clippy::cast_precision_loss)] // counts are bounded by FAN_OUT_N
fn a_ten_thousand_child_fan_out_spreads_within_ten_percent_of_uniform() {
    let router = four_shard_router();
    let parent = ExecutionId::new_for_shard(ShardId::new(0));
    let mut counts = [0usize; 4];
    let n = FAN_OUT_N;
    for seq in 0..n {
        let key = autumn_harvest::shard::child_placement_key(parent, u32::try_from(seq).unwrap());
        let shard = resolve_child_placement(
            Some(&router),
            &ChildPlacement::Distributed,
            ShardId::new(0),
            "child_wf",
            &key,
        )
        .unwrap();
        counts[usize::try_from(shard.as_i32()).unwrap()] += 1;
    }

    let expected = n as f64 / 4.0;
    for (shard, &count) in counts.iter().enumerate() {
        let deviation = (count as f64 - expected).abs() / expected;
        assert!(
            deviation <= 0.10,
            "shard {shard} got {count} of {n} children ({:.2}% off uniform); \
             the success metric allows ±10%",
            deviation * 100.0
        );
    }
}

#[test]
fn child_placement_keys_are_distinct_per_sequence_and_stable_per_parent() {
    let parent = ExecutionId::new_for_shard(ShardId::new(1));
    let other = ExecutionId::new_for_shard(ShardId::new(1));
    assert_eq!(
        autumn_harvest::shard::child_placement_key(parent, 4),
        autumn_harvest::shard::child_placement_key(parent, 4),
        "same (parent, seq) must re-derive the same key after a crash"
    );
    assert_ne!(
        autumn_harvest::shard::child_placement_key(parent, 4),
        autumn_harvest::shard::child_placement_key(parent, 5),
    );
    assert_ne!(
        autumn_harvest::shard::child_placement_key(parent, 4),
        autumn_harvest::shard::child_placement_key(other, 4),
        "two parents' Nth children must not collide onto one shard by construction"
    );
}

// ── Out of scope carve-out: an explicit pin is honoured ──────────────────────

#[test]
fn an_explicit_shard_pin_is_honoured_verbatim() {
    let router = four_shard_router();
    let resolved = resolve_child_placement(
        Some(&router),
        &ChildPlacement::Shard(ShardId::new(2)),
        ShardId::new(0),
        "child_wf",
        "parent#0",
    )
    .unwrap();
    assert_eq!(resolved, ShardId::new(2));
}

#[test]
fn an_explicit_residency_key_is_honoured_verbatim() {
    let router = four_shard_router().with_residency_map([("eu".to_string(), ShardId::new(3))]);
    let resolved = resolve_child_placement(
        Some(&router),
        &ChildPlacement::ResidencyKey("eu".to_string()),
        ShardId::new(0),
        "child_wf",
        "parent#0",
    )
    .unwrap();
    assert_eq!(resolved, ShardId::new(3));
}

#[test]
fn an_undeclared_residency_key_is_rejected_never_hashed() {
    let router = four_shard_router().with_residency_map([("eu".to_string(), ShardId::new(3))]);
    let err = resolve_child_placement(
        Some(&router),
        &ChildPlacement::ResidencyKey("mars".to_string()),
        ShardId::new(0),
        "child_wf",
        "parent#0",
    )
    .expect_err("an unmapped residency key must never silently hash");
    assert!(
        matches!(err, HarvestError::Config(ref m) if m.contains("mars")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn a_pin_to_a_drained_shard_is_rejected() {
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    );
    let err = resolve_child_placement(
        Some(&router),
        &ChildPlacement::Shard(ShardId::new(1)),
        ShardId::new(0),
        "child_wf",
        "parent#0",
    )
    .expect_err("a drained shard must not accept new children");
    assert!(matches!(err, HarvestError::Config(_)), "got {err:?}");
}

#[test]
fn opting_in_without_an_installed_router_is_a_typed_error_not_a_silent_fallback() {
    // AC8's anti-goal: never a silent fallback that breaks the placement
    // contract without trace.
    for placement in [
        ChildPlacement::Distributed,
        ChildPlacement::Shard(ShardId::new(1)),
        ChildPlacement::ResidencyKey("eu".to_string()),
    ] {
        let err =
            resolve_child_placement(None, &placement, ShardId::new(0), "child_wf", "parent#0")
                .expect_err("no router installed must fail, not fall back");
        assert!(matches!(err, HarvestError::Config(_)), "got {err:?}");
    }
}

// ── AC8: an unreachable target shard is typed and retryable ──────────────────

#[test]
fn shard_unavailable_is_a_distinct_retryable_error() {
    let err = HarvestError::ShardUnavailable {
        shard_id: 3,
        reason: "no pool configured on this node".to_string(),
    };
    assert!(err.is_shard_unavailable());
    assert!(
        err.to_string().contains('3'),
        "the message must name the shard: {err}"
    );
    assert!(
        !HarvestError::Config("x".into()).is_shard_unavailable(),
        "the predicate must not over-match"
    );
}

// ── The relay state machine ──────────────────────────────────────────────────

const fn awaited(status: CrossShardChildStatus) -> CrossShardChildObservation<'static> {
    CrossShardChildObservation {
        status,
        cancel_requested: false,
        parent_close_policy: None,
        parent_terminal: false,
        child_state: None,
    }
}

const fn detached(
    status: CrossShardChildStatus,
    policy: ParentClosePolicy,
) -> CrossShardChildObservation<'static> {
    CrossShardChildObservation {
        status,
        cancel_requested: false,
        parent_close_policy: Some(policy),
        parent_terminal: false,
        child_state: None,
    }
}

#[test]
fn a_pending_row_starts_the_child_on_the_target_shard() {
    assert_eq!(
        next_cross_shard_child_action(&awaited(CrossShardChildStatus::PendingStart)),
        CrossShardChildAction::StartChild
    );
    assert_eq!(
        next_cross_shard_child_action(&detached(
            CrossShardChildStatus::PendingStart,
            ParentClosePolicy::Abandon
        )),
        CrossShardChildAction::StartChild
    );
}

#[test]
fn a_started_child_with_no_news_waits() {
    let mut obs = awaited(CrossShardChildStatus::Started);
    obs.child_state = Some("RUNNING");
    assert_eq!(
        next_cross_shard_child_action(&obs),
        CrossShardChildAction::Wait
    );
}

#[test]
fn a_requested_cancel_is_delivered_before_anything_else() {
    let mut obs = awaited(CrossShardChildStatus::Started);
    obs.cancel_requested = true;
    obs.child_state = Some("RUNNING");
    assert_eq!(
        next_cross_shard_child_action(&obs),
        CrossShardChildAction::CancelChild
    );
}

#[test]
fn every_terminal_child_state_delivers_the_terminal_to_an_awaited_parent() {
    for state in [
        "COMPLETED",
        "FAILED",
        "TIMED_OUT",
        "CANCELLED",
        "TERMINATED",
    ] {
        let mut obs = awaited(CrossShardChildStatus::Started);
        obs.child_state = Some(state);
        assert_eq!(
            next_cross_shard_child_action(&obs),
            CrossShardChildAction::DeliverTerminal,
            "state {state} must wake the awaiting parent"
        );
    }
}

#[test]
fn a_detached_child_never_delivers_a_terminal_to_its_parent() {
    for policy in [
        ParentClosePolicy::Abandon,
        ParentClosePolicy::RequestCancel,
        ParentClosePolicy::Terminate,
    ] {
        let mut obs = detached(CrossShardChildStatus::Started, policy);
        obs.child_state = Some("COMPLETED");
        assert_eq!(
            next_cross_shard_child_action(&obs),
            CrossShardChildAction::Retire,
            "a detached child's terminal is not the parent's business"
        );
    }
}

#[test]
fn a_closed_parent_cascades_to_its_running_cross_shard_detached_children() {
    for policy in [
        ParentClosePolicy::RequestCancel,
        ParentClosePolicy::Terminate,
    ] {
        let mut obs = detached(CrossShardChildStatus::Started, policy);
        obs.parent_terminal = true;
        obs.child_state = Some("RUNNING");
        assert_eq!(
            next_cross_shard_child_action(&obs),
            CrossShardChildAction::ApplyCloseCascade,
            "policy {policy:?} must reach a cross-shard child"
        );
    }
}

#[test]
fn abandon_never_cascades() {
    let mut obs = detached(CrossShardChildStatus::Started, ParentClosePolicy::Abandon);
    obs.parent_terminal = true;
    obs.child_state = Some("RUNNING");
    assert_eq!(
        next_cross_shard_child_action(&obs),
        CrossShardChildAction::Retire,
        "Abandon means the child keeps running and the row is owed nothing"
    );
}

#[test]
fn an_awaited_child_outliving_a_closed_parent_retires_the_row() {
    // Parity with the same-shard contract: an awaited child can outlive a
    // cancelled or terminated parent. There is nobody left to wake, so the row
    // must be dropped rather than polled forever.
    let mut obs = awaited(CrossShardChildStatus::Started);
    obs.parent_terminal = true;
    obs.child_state = Some("RUNNING");
    assert_eq!(
        next_cross_shard_child_action(&obs),
        CrossShardChildAction::Retire
    );
}

#[test]
fn a_terminal_child_still_delivers_even_when_the_parent_has_closed() {
    // The delivery step itself re-checks the parent under `FOR UPDATE` and
    // skips the append when it is terminal (mirroring
    // `notify_awaited_parent_of_child_terminal`), so preferring delivery here
    // is the safe ordering: it can degrade to a row-delete, whereas retiring
    // first would drop a wake the parent could still legitimately consume.
    let mut obs = awaited(CrossShardChildStatus::Started);
    obs.parent_terminal = true;
    obs.child_state = Some("COMPLETED");
    assert_eq!(
        next_cross_shard_child_action(&obs),
        CrossShardChildAction::DeliverTerminal
    );
}

#[test]
fn a_child_row_not_yet_visible_on_the_target_shard_waits() {
    let obs = awaited(CrossShardChildStatus::Started);
    assert_eq!(obs.child_state, None);
    assert_eq!(
        next_cross_shard_child_action(&obs),
        CrossShardChildAction::Wait
    );
}

#[test]
fn status_round_trips_through_its_database_representation() {
    for status in [
        CrossShardChildStatus::PendingStart,
        CrossShardChildStatus::Started,
    ] {
        assert_eq!(
            CrossShardChildStatus::from_db(status.as_db_str()),
            Some(status)
        );
    }
    assert_eq!(CrossShardChildStatus::from_db("NONSENSE"), None);
}
