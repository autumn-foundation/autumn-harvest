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

#[cfg(feature = "db")]
use autumn_harvest::cross_shard_child::preflight_target_shard;
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

/// A drain is a *transient* operational state, so it must not be rejected inside
/// the workflow handler — where the ABI erases the error type and the executor
/// turns it into a terminal failure. The resolver therefore returns the shard
/// the caller named, and the rejection happens at the persist boundary.
// `preflight_target_shard` takes a `ShardedDbPool`, which is `db`-gated.
#[cfg(feature = "db")]
#[test]
fn a_pin_to_a_drained_shard_resolves_and_is_rejected_at_the_persist_boundary() {
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    );
    // The resolver does NOT reject it — rejecting here would be terminal.
    let resolved = resolve_child_placement(
        Some(&router),
        &ChildPlacement::Shard(ShardId::new(1)),
        ShardId::new(0),
        "child_wf",
        "parent#0",
    )
    .expect("a drain must not fail the handler");
    assert_eq!(
        resolved,
        ShardId::new(1),
        "the resolver must never quietly swap the requested shard"
    );

    // The preflight does, retryably. `None` for the pool map exercises the
    // fail-closed arm; the drain check is asserted separately below.
    let err = preflight_target_shard(None, Some(&router), resolved)
        .expect_err("a drained shard must not accept new children");
    assert!(err.is_shard_unavailable(), "got {err:?}");
}

/// A fully-drained fleet must not silently place a `Distributed` child on the
/// default shard.
///
/// `ShardRouter::pick_for_new_workflow` falls back to `default_shard` when the
/// writable set is empty — correct for a top-level start, and exactly the silent
/// fallback AC8 forbids for an opt-in placement. With the writable set empty the
/// default shard is by definition not writable, so the preflight stops the
/// fallback before anything is recorded.
// `preflight_target_shard` takes a `ShardedDbPool`, which is `db`-gated.
#[cfg(feature = "db")]
#[test]
fn distributed_placement_with_no_writable_shard_is_stopped_by_the_preflight() {
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![],
        ShardId::new(0),
    );
    let resolved = resolve_child_placement(
        Some(&router),
        &ChildPlacement::Distributed,
        ShardId::new(0),
        "child_wf",
        "parent#1",
    )
    .expect("resolution itself must not fail the handler");
    assert!(
        !router.is_writable(resolved),
        "with no writable shard the pick is the non-writable default"
    );
    let err = preflight_target_shard(None, Some(&router), resolved)
        .expect_err("a fully-drained fleet must not silently use the default shard");
    assert!(err.is_shard_unavailable(), "got {err:?}");
}

/// The preflight fails closed with no pool map at all — a reachable
/// misconfiguration, since the router and the pool map are independent globals.
// `preflight_target_shard` takes a `ShardedDbPool`, which is `db`-gated.
#[cfg(feature = "db")]
#[test]
fn the_preflight_fails_closed_with_no_pool_map() {
    let router = four_shard_router();
    let err = preflight_target_shard(None, Some(&router), ShardId::new(2))
        .expect_err("no pool map must not admit a cross-shard placement");
    assert!(err.is_shard_unavailable(), "got {err:?}");
    assert!(
        err.to_string().contains('2'),
        "the message must name the shard: {err}"
    );
}

/// A genuine misconfiguration stays a terminal `Config` error: retrying an
/// unknown shard or an undeclared residency key never helps.
#[test]
fn static_misconfiguration_stays_a_terminal_config_error() {
    let router = four_shard_router();
    for placement in [
        ChildPlacement::Shard(ShardId::new(99)),
        ChildPlacement::ResidencyKey("mars".to_string()),
    ] {
        let err =
            resolve_child_placement(Some(&router), &placement, ShardId::new(0), "child_wf", "k")
                .expect_err("must be rejected");
        assert!(matches!(err, HarvestError::Config(_)), "got {err:?}");
        assert!(!err.is_shard_unavailable(), "got {err:?}");
    }
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
        parent_terminal: Some(false),
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
        parent_terminal: Some(false),
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
        obs.parent_terminal = Some(true);
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
    obs.parent_terminal = Some(true);
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
    obs.parent_terminal = Some(true);
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
    obs.parent_terminal = Some(true);
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

/// **Regression (Codex round 1, P1).** A sweep that could not read the parents
/// must decide nothing destructive.
///
/// `Retire` deletes the outbox row outright with no second look at the parent,
/// so collapsing a transient parent-read failure into "the parent is terminal"
/// permanently loses the terminal wake of every awaited cross-shard child in the
/// batch — and cascade-cancels detached children whose parents are alive. The
/// unknown state is `None`, and only `Some(true)` retires or cascades.
#[test]
fn an_unreadable_parent_state_never_retires_or_cascades() {
    // Awaited, child still running, parent unknown -> wait, never retire.
    let mut obs = awaited(CrossShardChildStatus::Started);
    obs.parent_terminal = None;
    obs.child_state = Some("RUNNING");
    assert_eq!(
        next_cross_shard_child_action(&obs),
        CrossShardChildAction::Wait,
        "an unread parent must not retire an awaited child's row"
    );

    // Detached, child still running, parent unknown -> wait, never cascade.
    for policy in [
        ParentClosePolicy::RequestCancel,
        ParentClosePolicy::Terminate,
        ParentClosePolicy::Abandon,
    ] {
        let mut obs = detached(CrossShardChildStatus::Started, policy);
        obs.parent_terminal = None;
        obs.child_state = Some("RUNNING");
        assert_eq!(
            next_cross_shard_child_action(&obs),
            CrossShardChildAction::Wait,
            "an unread parent must not cascade {policy:?} onto a live child"
        );
    }
}

/// The steps that do not depend on the parent still make progress while the
/// parent read is failing — a parent-side blip must not stall child creation or
/// a pending cancel.
#[test]
fn an_unreadable_parent_state_still_starts_and_cancels() {
    let mut pending = awaited(CrossShardChildStatus::PendingStart);
    pending.parent_terminal = None;
    assert_eq!(
        next_cross_shard_child_action(&pending),
        CrossShardChildAction::StartChild
    );

    let mut cancelling = awaited(CrossShardChildStatus::Started);
    cancelling.parent_terminal = None;
    cancelling.cancel_requested = true;
    cancelling.child_state = Some("RUNNING");
    assert_eq!(
        next_cross_shard_child_action(&cancelling),
        CrossShardChildAction::CancelChild
    );
}

/// A terminal child is still delivered while the parent read is failing: the
/// delivery step re-reads the parent under `FOR UPDATE` and skips the append
/// itself if it has sealed, so this is safe and avoids stalling every wake.
#[test]
fn an_unreadable_parent_state_still_delivers_a_terminal_child() {
    let mut obs = awaited(CrossShardChildStatus::Started);
    obs.parent_terminal = None;
    obs.child_state = Some("COMPLETED");
    assert_eq!(
        next_cross_shard_child_action(&obs),
        CrossShardChildAction::DeliverTerminal
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
