//! Pure-logic coverage for shard rebalancing (issue #964).
//!
//! No database, no feature gate — these run in the cheap
//! `cargo test -p autumn-harvest --no-default-features` CI step on every OS.
//!
//! Three surfaces are covered here:
//!
//! 1. **The quiescence predicate** (`shard_rebalance::assess_quiescence`) — the
//!    single decision that says whether an execution may move at all. It is a
//!    pure function over an explicitly-gathered observation precisely so every
//!    blocker is exhaustively testable without Postgres; the first draft of it
//!    ("no task rows at all") would have refused to migrate every timer-parked
//!    workflow, i.e. the entire population the feature exists to move.
//! 2. **The phase machine** (`shard_rebalance::next_migration_action`) — what a
//!    resume sweep does with a migration row it finds after a crash, for every
//!    phase. This is the kill-point contract in pure form.
//! 3. **Forward-chain resolution** (`shard_rebalance::resolve_forward_chain`) —
//!    the bounded A→B→C hop following that makes a pre-migration `ExecutionId`
//!    keep resolving.
//!
//! The multi-shard *runtime* coverage (real copies onto a second database, real
//! cutover, real kill points) lives in `shard_rebalance_db_tests.rs`, which
//! needs Postgres.

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::shard_rebalance::{
    MAX_FORWARD_HOPS, MigrationAction, MigrationObservation, MigrationPhase, Quiescence,
    QuiescenceBlocker, QuiescenceObservation, assess_quiescence, history_fingerprint,
    next_migration_action, resolve_forward_chain,
};
use autumn_harvest::types::{ExecutionId, ShardId};

/// The canonical migratable shape: a long-lived entity workflow parked on a
/// timer, with nothing in flight anywhere.
fn timer_parked() -> QuiescenceObservation {
    QuiescenceObservation {
        state: "RUNNING".to_string(),
        parent_id: None,
        claimed_workflow_tasks: 0,
        due_pending_tasks: 0,
        parked_workflow_tasks: 1,
        wake_requested: false,
        live_activity_tasks: 0,
        unconsumed_signals: 0,
        inflight_completion_deliveries: 0,
        active_sessions: 0,
        live_external_tasks: 0,
        live_children: 0,
        cross_shard_child_rows: 0,
        held_mutex_locks: 0,
        queued_mutex_waiters: 0,
        dead_letter_rows: 0,
        nd_blocked: false,
    }
}

/// The other common long-lived shape: parked on a signal, with no timer at all.
///
/// Structurally identical to [`timer_parked`] as far as the predicate is
/// concerned — both leave exactly one workflow task row in a parked shape — and
/// that identity is the point. The observation deliberately does **not** record
/// *which* park it is, because the migration treats them the same: the row is
/// copied verbatim either way. The DB suite
/// (`a_signal_parked_execution_migrates_with_its_parked_task_row`) is what pins
/// the two physically-different row shapes.
fn signal_parked() -> QuiescenceObservation {
    timer_parked()
}

fn blockers(obs: &QuiescenceObservation) -> Vec<QuiescenceBlocker> {
    match assess_quiescence(obs) {
        Quiescence::Eligible => vec![],
        Quiescence::Blocked(blockers) => blockers,
    }
}

// ── AC1: the eligible population ──────────────────────────────────────────────

#[test]
fn a_timer_parked_execution_is_migratable() {
    // The load-bearing case. An execution waiting on a timer keeps its workflow
    // task row in the *parked* shape (`RUNNING`, no worker, no start time), so a
    // naive "no task rows" predicate would refuse exactly the population this
    // feature exists to move.
    assert_eq!(assess_quiescence(&timer_parked()), Quiescence::Eligible);
}

#[test]
fn a_signal_parked_execution_is_migratable() {
    assert_eq!(assess_quiescence(&signal_parked()), Quiescence::Eligible);
}

#[test]
fn an_execution_with_no_task_row_at_all_is_migratable() {
    // A run between wakes may legitimately have no queue row.
    let obs = QuiescenceObservation {
        parked_workflow_tasks: 0,
        ..timer_parked()
    };
    assert_eq!(assess_quiescence(&obs), Quiescence::Eligible);
}

// ── AC1: every blocker, named ────────────────────────────────────────────────

#[test]
fn a_claimed_workflow_task_blocks_migration() {
    let obs = QuiescenceObservation {
        claimed_workflow_tasks: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::ClaimedWorkflowTask]);
}

#[test]
fn a_due_pending_task_blocks_migration() {
    // A wake that is dispatchable *right now* is in-flight work even though no
    // worker holds it yet.
    let obs = QuiescenceObservation {
        due_pending_tasks: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::DuePendingTask]);
}

#[test]
fn a_pending_wake_request_blocks_migration() {
    // `wake_requested` is the durable "a wake raced the park" flag. Migrating
    // under it would strand the wake on the sealed source.
    let obs = QuiescenceObservation {
        wake_requested: true,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::WakeRequested]);
}

#[test]
fn a_live_activity_task_blocks_migration() {
    let obs = QuiescenceObservation {
        live_activity_tasks: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::LiveActivityTask]);
}

#[test]
fn an_unconsumed_signal_blocks_migration() {
    let obs = QuiescenceObservation {
        unconsumed_signals: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::UnconsumedSignal]);
}

#[test]
fn an_inflight_completion_delivery_blocks_migration() {
    let obs = QuiescenceObservation {
        inflight_completion_deliveries: 1,
        ..timer_parked()
    };
    assert_eq!(
        blockers(&obs),
        vec![QuiescenceBlocker::InflightCompletionDelivery]
    );
}

#[test]
fn an_active_session_blocks_migration() {
    let obs = QuiescenceObservation {
        active_sessions: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::ActiveSession]);
}

#[test]
fn a_live_external_task_blocks_migration() {
    let obs = QuiescenceObservation {
        live_external_tasks: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::LiveExternalTask]);
}

#[test]
fn a_non_root_execution_blocks_migration() {
    // A child's terminal appends to its parent's history in a shard-local
    // transaction; moving the child away would break that edge.
    let obs = QuiescenceObservation {
        parent_id: Some(ExecutionId::new_for_shard(ShardId::new(0))),
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::NotARoot]);
}

#[test]
fn a_live_child_blocks_migration() {
    let obs = QuiescenceObservation {
        live_children: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::LiveChild]);
}

#[test]
fn a_cross_shard_child_row_blocks_migration() {
    let obs = QuiescenceObservation {
        cross_shard_child_rows: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::CrossShardChildRow]);
}

#[test]
fn holding_a_durable_mutex_blocks_migration() {
    // The lock row is shard-local and keyed by the holder. Moving the holder
    // away would leave the key held by an execution that no longer lives here,
    // and every waiter on it blocked forever.
    let obs = QuiescenceObservation {
        held_mutex_locks: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::HoldsMutexLock]);
}

#[test]
fn being_queued_for_a_durable_mutex_blocks_migration() {
    // The sharper of the two: a waiter looks perfectly parked — no task in
    // flight, nothing claimed — but its grant is delivered by waking it on
    // THIS shard. Migrate it and the grant lands on a sealed row: a lost wake,
    // which is the exact failure the quiescence bar exists to prevent.
    let obs = QuiescenceObservation {
        queued_mutex_waiters: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::QueuedForMutex]);
}

#[test]
fn a_dead_letter_row_blocks_migration() {
    let obs = QuiescenceObservation {
        dead_letter_rows: 1,
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::HasDeadLetterRow]);
}

#[test]
fn a_non_determinism_blocked_execution_blocks_migration() {
    let obs = QuiescenceObservation {
        nd_blocked: true,
        ..timer_parked()
    };
    assert_eq!(
        blockers(&obs),
        vec![QuiescenceBlocker::NonDeterminismBlocked]
    );
}

#[test]
fn every_non_running_state_blocks_migration() {
    for state in [
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMED_OUT",
        "CONTINUED_AS_NEW",
        "TERMINATED",
        "MIGRATING",
        "MIGRATED",
    ] {
        let obs = QuiescenceObservation {
            state: state.to_string(),
            ..timer_parked()
        };
        assert_eq!(
            blockers(&obs),
            vec![QuiescenceBlocker::NotRunning],
            "state {state} must not be migratable"
        );
    }
}

#[test]
fn a_paused_execution_is_not_migratable_and_says_so() {
    // `PAUSED` is a STATE, not a column — the first draft of this design note
    // had it the other way round. The predicate admits `RUNNING` only, so a
    // paused root is skipped with a named reason rather than silently moved
    // under a half-supported code path (the copy and the cutover both key on
    // `RUNNING`, and activation would have to carry the original state through).
    // The decommission runbook lists the remedy: resume, migrate, re-pause.
    let obs = QuiescenceObservation {
        state: "PAUSED".to_string(),
        ..timer_parked()
    };
    assert_eq!(blockers(&obs), vec![QuiescenceBlocker::NotRunning]);
}

#[test]
fn multiple_blockers_are_all_reported_not_just_the_first() {
    // A dry-run has to explain itself completely, or an operator fixes one
    // blocker at a time and re-runs forever.
    let obs = QuiescenceObservation {
        claimed_workflow_tasks: 1,
        unconsumed_signals: 2,
        active_sessions: 1,
        ..timer_parked()
    };
    let found = blockers(&obs);
    assert!(
        found.contains(&QuiescenceBlocker::ClaimedWorkflowTask),
        "{found:?}"
    );
    assert!(
        found.contains(&QuiescenceBlocker::UnconsumedSignal),
        "{found:?}"
    );
    assert!(
        found.contains(&QuiescenceBlocker::ActiveSession),
        "{found:?}"
    );
    assert_eq!(found.len(), 3, "{found:?}");
}

#[test]
fn more_than_one_parked_workflow_task_blocks_migration() {
    // The engine's invariant is at most one workflow task row per execution.
    // Seeing two means something we do not understand is going on; refuse
    // rather than copy an ambiguous parked set.
    let obs = QuiescenceObservation {
        parked_workflow_tasks: 2,
        ..timer_parked()
    };
    assert_eq!(
        blockers(&obs),
        vec![QuiescenceBlocker::AmbiguousParkedTaskSet]
    );
}

// ── AC7: the phase machine, i.e. the kill-point contract ─────────────────────

const fn obs_at(phase: MigrationPhase) -> MigrationObservation {
    MigrationObservation {
        phase,
        source_still_quiescent: true,
    }
}

#[test]
fn a_pending_migration_restages_the_copy() {
    assert_eq!(
        next_migration_action(&obs_at(MigrationPhase::Pending)),
        MigrationAction::StageCopy
    );
}

#[test]
fn a_copied_migration_verifies() {
    assert_eq!(
        next_migration_action(&obs_at(MigrationPhase::Copied)),
        MigrationAction::Verify
    );
}

#[test]
fn a_verified_migration_cuts_over() {
    assert_eq!(
        next_migration_action(&obs_at(MigrationPhase::Verified)),
        MigrationAction::Cutover
    );
}

#[test]
fn a_verified_migration_whose_source_woke_up_aborts() {
    // R2: a wake landing between verification and cutover must abort cleanly and
    // leave the source authoritative, never be silently cut over.
    let obs = MigrationObservation {
        phase: MigrationPhase::Verified,
        source_still_quiescent: false,
    };
    assert_eq!(next_migration_action(&obs), MigrationAction::Abort);
}

#[test]
fn a_committed_migration_activates_the_target() {
    // Past the cutover the source is sealed, so quiescence of the *source* is
    // no longer a reason to stop: the only correct move is forward.
    for still_quiescent in [true, false] {
        let obs = MigrationObservation {
            phase: MigrationPhase::Committed,
            source_still_quiescent: still_quiescent,
        };
        assert_eq!(
            next_migration_action(&obs),
            MigrationAction::ActivateTarget,
            "a committed migration must never roll back"
        );
    }
}

#[test]
fn terminal_phases_retire_the_row() {
    for phase in [MigrationPhase::Done, MigrationPhase::Aborted] {
        assert_eq!(
            next_migration_action(&obs_at(phase)),
            MigrationAction::Retire
        );
    }
}

#[test]
fn a_pre_cutover_phase_aborts_when_the_source_is_no_longer_quiescent() {
    for phase in [MigrationPhase::Pending, MigrationPhase::Copied] {
        let obs = MigrationObservation {
            phase,
            source_still_quiescent: false,
        };
        assert_eq!(
            next_migration_action(&obs),
            MigrationAction::Abort,
            "{phase:?} must abort rather than continue copying a woken run"
        );
    }
}

#[test]
fn migration_phase_round_trips_through_its_database_string() {
    for phase in [
        MigrationPhase::Pending,
        MigrationPhase::Copied,
        MigrationPhase::Verified,
        MigrationPhase::Committed,
        MigrationPhase::Done,
        MigrationPhase::Aborted,
    ] {
        assert_eq!(MigrationPhase::from_db(phase.as_db()), Some(phase));
    }
    assert_eq!(MigrationPhase::from_db("NOT_A_PHASE"), None);
}

// ── AC4: forward-chain resolution ────────────────────────────────────────────

#[test]
fn an_unmigrated_execution_resolves_to_its_encoded_shard() {
    let id = ExecutionId::new_for_shard(ShardId::new(3));
    let resolved = resolve_forward_chain(id.shard(), |_| None).expect("no hops needed");
    assert_eq!(resolved, ShardId::new(3));
}

#[test]
fn a_single_hop_resolves_to_the_target() {
    let id = ExecutionId::new_for_shard(ShardId::new(0));
    let resolved = resolve_forward_chain(id.shard(), |s| {
        (s == ShardId::new(0)).then(|| ShardId::new(1))
    })
    .expect("one hop");
    assert_eq!(resolved, ShardId::new(1));
}

#[test]
fn a_chain_of_hops_resolves_to_the_final_shard() {
    // A→B→C, the shape a run migrated twice leaves behind before the
    // best-effort chain collapse catches up.
    let resolved = resolve_forward_chain(ShardId::new(0), |s| match s.as_i32() {
        0 => Some(ShardId::new(1)),
        1 => Some(ShardId::new(2)),
        _ => None,
    })
    .expect("two hops");
    assert_eq!(resolved, ShardId::new(2));
}

#[test]
fn an_over_long_chain_fails_closed_rather_than_looping() {
    // A cycle (or a pathologically long chain) must be a typed error, never an
    // infinite loop inside a routing call.
    let error = resolve_forward_chain(ShardId::new(0), |s| {
        Some(ShardId::new((s.as_i32() + 1) % 2))
    })
    .expect_err("a cycle must fail closed");
    assert!(
        error.is_shard_unavailable(),
        "expected a retryable shard-unavailable classification, got {error:?}"
    );
}

#[test]
fn the_hop_bound_is_small_enough_to_bound_a_routing_call() {
    // Not a restatement of the literal: the property that matters is that the
    // bound is small enough that following it is cheap on a routing path, and
    // at least long enough for the chains a real deployment produces (a run
    // migrated twice before the collapse catches up needs 2).
    assert!(
        (2..=8).contains(&MAX_FORWARD_HOPS),
        "the hop bound must leave room for a real chain without making a \
         cycle expensive to detect: {MAX_FORWARD_HOPS}"
    );
}

// ── AC4 (level 2): router-declared forwards for a decommissioned shard ───────

use autumn_harvest::ShardRouter;

fn successor_router() -> ShardRouter {
    // Shard 0 has been drained and REMOVED from the readable set; shard 2 is
    // its successor. The successor is deliberately NOT the default shard (1):
    // with `default == successor` every assertion below would also pass for a
    // `with_shard_forwards` that did nothing at all, since the unknown-shard
    // fallback already answers the default.
    ShardRouter::new(
        vec![ShardId::new(1), ShardId::new(2)],
        vec![ShardId::new(1), ShardId::new(2)],
        ShardId::new(1),
    )
    .with_shard_forwards([(ShardId::new(0), ShardId::new(2))])
}

#[test]
fn an_id_minted_on_a_retired_shard_resolves_to_its_successor() {
    let router = successor_router();
    let minted_on_the_retired_shard = ExecutionId::new_for_shard(ShardId::new(0));
    assert_eq!(
        router.shard_for_execution(minted_on_the_retired_shard),
        ShardId::new(2),
        "a decommissioned shard's ids must resolve to its declared successor, \
         never fall through to the default shard"
    );
    assert_ne!(
        router.shard_for_execution(minted_on_the_retired_shard),
        ShardId::new(1),
        "and the successor must be reached BECAUSE it was declared, not because \
         it happens to be the default"
    );
}

#[test]
fn without_a_forward_a_retired_shards_ids_fall_through_to_the_default() {
    // The pre-#964 behaviour, and exactly the silent misroute the forward
    // exists to prevent: shard 0's ids answer from shard 1, which does not
    // host them. Pinned against the SAME topology as `successor_router` so the
    // contrast is the forward and nothing else.
    let router = ShardRouter::new(
        vec![ShardId::new(1), ShardId::new(2)],
        vec![ShardId::new(1), ShardId::new(2)],
        ShardId::new(1),
    );
    assert_eq!(
        router.shard_for_execution(ExecutionId::new_for_shard(ShardId::new(0))),
        ShardId::new(1),
        "without a declared forward the id silently resolves to the default shard"
    );
}

#[test]
fn a_live_shards_ids_are_unaffected_by_a_forward_for_another_shard() {
    let router = successor_router();
    assert_eq!(
        router.shard_for_execution(ExecutionId::new_for_shard(ShardId::new(1))),
        ShardId::new(1)
    );
    assert_eq!(
        router.shard_for_execution(ExecutionId::new_for_shard(ShardId::new(2))),
        ShardId::new(2)
    );
}

#[test]
fn the_forward_map_is_empty_by_default() {
    let router = ShardRouter::new(
        vec![ShardId::new(0)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    );
    assert!(
        router.shard_forwards().is_empty(),
        "no deployment gets forwarding behaviour it did not ask for"
    );
}

#[test]
fn the_forward_map_is_surfaced_in_the_router_parts_projection() {
    // `parts()` is the coverage guard the admin config snapshot destructures
    // exhaustively, so a placement-affecting field that is missing here is a
    // compile error rather than a silently unobservable divergence between
    // replicas.
    let router = successor_router();
    let parts = router.parts();
    assert_eq!(parts.shard_forwards.len(), 1);
    assert_eq!(
        parts.shard_forwards.get(&ShardId::new(0)),
        Some(&ShardId::new(2))
    );
}

#[test]
#[should_panic(expected = "cannot be forwarded to itself")]
fn a_self_forward_panics_at_construction() {
    let _ = ShardRouter::new(
        vec![ShardId::new(1)],
        vec![ShardId::new(1)],
        ShardId::new(1),
    )
    .with_shard_forwards([(ShardId::new(1), ShardId::new(1))]);
}

#[test]
#[should_panic(expected = "is still in the readable set")]
fn forwarding_a_still_readable_shard_panics_at_construction() {
    // Forwarding a LIVE shard would shadow its own rows: every id minted there
    // would be answered from another database.
    let _ = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(1)],
        ShardId::new(1),
    )
    .with_shard_forwards([(ShardId::new(0), ShardId::new(1))]);
}

#[test]
#[should_panic(expected = "which is not in the readable set")]
fn forwarding_to_an_unreadable_shard_panics_at_construction() {
    let _ = ShardRouter::new(
        vec![ShardId::new(1)],
        vec![ShardId::new(1)],
        ShardId::new(1),
    )
    .with_shard_forwards([(ShardId::new(0), ShardId::new(9))]);
}

#[test]
#[should_panic(expected = "is still in the readable set")]
fn a_chain_of_router_forwards_is_structurally_impossible() {
    // A chain needs no dedicated rule, and asserting one would be dead code: a
    // forward's SOURCE must be outside `readable_shards` while its TARGET must
    // be inside it, so the middle shard of a chain would have to be both. The
    // readable-set assertion is what actually rejects it, and that is exactly
    // why `shard_for_execution`'s single hop is exhaustive by construction
    // rather than by a hop counter.
    let _ = ShardRouter::new(
        vec![ShardId::new(1), ShardId::new(2)],
        vec![ShardId::new(2)],
        ShardId::new(2),
    )
    .with_shard_forwards([
        (ShardId::new(0), ShardId::new(1)),
        (ShardId::new(1), ShardId::new(2)),
    ]);
}

#[test]
#[should_panic(expected = "forwarded more than once with conflicting targets")]
fn a_conflicting_duplicate_forward_panics_at_construction() {
    let _ = ShardRouter::new(
        vec![ShardId::new(1), ShardId::new(2)],
        vec![ShardId::new(1)],
        ShardId::new(1),
    )
    .with_shard_forwards([
        (ShardId::new(0), ShardId::new(1)),
        (ShardId::new(0), ShardId::new(2)),
    ]);
}

#[test]
fn an_unencoded_execution_id_still_resolves_to_the_default_shard() {
    // `ExecutionId::new()` carries the UNENCODED sentinel. Forwarding must not
    // change what that means.
    let router = successor_router();
    assert_eq!(
        router.shard_for_execution(ExecutionId::new()),
        ShardId::new(1)
    );
}

// ── AC2: the replay fingerprint must actually discriminate ───────────────────
//
// Verification compares a fingerprint of the source's history against the
// target's. A fingerprint that returned a constant would pass every migration
// and every test that merely compares it to itself, so these pin that it
// *separates* histories that replay to different next-command states.

use chrono::Utc;
use serde_json::json;

fn started(input: serde_json::Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn signal(name: &str) -> WorkflowEvent {
    WorkflowEvent::SignalReceived {
        signal_name: name.to_string(),
        payload: json!({}),
    }
}

fn timer(id: &str) -> WorkflowEvent {
    WorkflowEvent::TimerStarted {
        timer_id: autumn_harvest::types::TimerId::new(id),
        duration_secs: 60,
    }
}

#[test]
fn the_fingerprint_is_stable_for_the_same_history() {
    let events = vec![started(json!({"a": 1})), timer("t"), signal("go")];
    assert_eq!(
        history_fingerprint(&events),
        history_fingerprint(&events.clone()),
        "verification would be useless if the fingerprint were not deterministic"
    );
}

#[test]
fn the_fingerprint_separates_histories_that_differ_in_content() {
    let a = vec![started(json!({"a": 1}))];
    let b = vec![started(json!({"a": 2}))];
    assert_ne!(history_fingerprint(&a), history_fingerprint(&b));
}

#[test]
fn the_fingerprint_separates_a_reordered_history() {
    // The append-only invariant's whole point: order is meaning. A copy that
    // preserved every event but reordered two of them must not verify.
    let a = vec![started(json!({})), timer("t1"), timer("t2")];
    let b = vec![started(json!({})), timer("t2"), timer("t1")];
    assert_ne!(history_fingerprint(&a), history_fingerprint(&b));
}

#[test]
fn the_fingerprint_separates_an_appended_event() {
    let a = vec![started(json!({})), timer("t")];
    let mut b = a.clone();
    b.push(signal("extra"));
    assert_ne!(history_fingerprint(&a), history_fingerprint(&b));
}

#[test]
fn the_fingerprint_separates_a_truncated_history() {
    let a = vec![started(json!({})), timer("t"), signal("go")];
    let b = a[..2].to_vec();
    assert_ne!(history_fingerprint(&a), history_fingerprint(&b));
}

#[test]
fn the_fingerprint_separates_signal_multiplicity() {
    // Two deliveries of the SAME signal name replay to a different
    // next-command state than one, and the cursor half of the fingerprint is
    // what has to notice: the event list alone would differ too, but this pins
    // that the unconsumed-signal counts are part of what is hashed.
    let one = vec![started(json!({})), signal("go")];
    let two = vec![started(json!({})), signal("go"), signal("go")];
    assert_ne!(history_fingerprint(&one), history_fingerprint(&two));
}

#[test]
fn the_fingerprint_separates_an_empty_history_from_a_started_one() {
    assert_ne!(
        history_fingerprint(&[]),
        history_fingerprint(&[started(json!({}))])
    );
}
