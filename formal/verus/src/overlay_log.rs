//! The overlay log keeps every version and moves its pointer once
//! (issue #2371).
//!
//! The copies of `next_overlay_version` and `decide_activate` below are the
//! kernel of the overlay stores. The in-memory and `SQLite` stores call both.
//! The Postgres store calls `decide_activate`, and its SQL computes
//! `latest + 1` as `next_overlay_version` does.
//!
//! The spec `Log` models one kind: a sequence of version bodies and an
//! active pointer. Version `v` is the entry at index `v - 1`. A body is an
//! integer, because Verus cannot check bytes. The model makes two
//! assumptions:
//!
//! - The versions are dense. The model has no delete. A raw delete of a
//!   version that is not active leaves a gap. A trigger keeps the head row,
//!   so the number of that version never comes back.
//! - The calls of one kind run one after another. The lock of the head row
//!   gives that order in each store.
//!
//! The shared suite of the stores checks that the SQL does the same.
//!
//! Issue #2374 adds the activation log and the rollback. The copies of
//! `logs_activation` and `rollback_target` are its kernel. The spec `Heads`
//! models the pointer of one kind and its log of moves. Each move records
//! the pointer before it and the version after it. A rollback activates the
//! pointer before the last move, over the pointer after it.
//!
//! Issue #2394 adds `decide_auto_move`, the kernel of an auto adoption
//! under the head lock. It refuses a stale pointer, then a full daily cap.
//! The spec `spec_auto_day` shows that the auto moves of one day never pass
//! the cap.

use vstd::prelude::*;

verus! {

/// `next_overlay_version` as a spec.
pub open spec fn spec_next_overlay_version(latest: Option<u64>) -> Option<u64> {
    match latest {
        None => Some(1u64),
        Some(last) => if last < u64::MAX {
            Some((last + 1) as u64)
        } else {
            None
        },
    }
}

// mirror: autumn-harvest/src/overlay_log.rs next_overlay_version
pub fn next_overlay_version(latest: Option<u64>) -> (r: Option<u64>)
    ensures
        r == spec_next_overlay_version(latest),
        latest.is_none() ==> r == Some(1u64),
        latest.is_some() && r.is_some() ==> r.unwrap() == latest.unwrap() + 1,
        r.is_none() ==> latest == Some(u64::MAX),
{
    match latest {
        None => Some(1),
        Some(last) => {
            if last < u64::MAX {
                Some(last + 1)
            } else {
                None
            }
        }
    }
}

/// The three outcomes of an activation.
pub enum ActivateDecision {
    Activate,
    UnknownVersion,
    Conflict,
}

/// `decide_activate` as a spec.
pub open spec fn spec_decide_activate(known: bool, expected: Option<u64>, actual: Option<u64>) -> ActivateDecision {
    if !known {
        ActivateDecision::UnknownVersion
    } else if expected == actual {
        ActivateDecision::Activate
    } else {
        ActivateDecision::Conflict
    }
}

// mirror: autumn-harvest/src/overlay_log.rs decide_activate
pub fn decide_activate(known: bool, expected: Option<u64>, actual: Option<u64>) -> (r:
    ActivateDecision)
    ensures
        r == spec_decide_activate(known, expected, actual),
        !known <==> r == ActivateDecision::UnknownVersion,
        r == ActivateDecision::Activate <==> known && expected == actual,
{
    if !known {
        return ActivateDecision::UnknownVersion;
    }
    let same = match (expected, actual) {
        (None, None) => true,
        (Some(want), Some(have)) => want == have,
        _ => false,
    };
    if same {
        ActivateDecision::Activate
    } else {
        ActivateDecision::Conflict
    }
}

/// The log of one kind.
pub struct Log {
    pub bodies: Seq<int>,
    pub active: Option<u64>,
}

/// The last version, or `None` for an empty log.
pub open spec fn latest(log: Log) -> Option<u64> {
    if log.bodies.len() == 0 {
        None
    } else {
        Some(log.bodies.len() as u64)
    }
}

/// Whether the log holds version `v`.
pub open spec fn holds(log: Log, v: u64) -> bool {
    1 <= v && v <= log.bodies.len()
}

/// The pointer is unset or names a stored version. The log fits the
/// counter.
pub open spec fn inv(log: Log) -> bool {
    log.bodies.len() <= u64::MAX && match log.active {
        None => true,
        Some(v) => holds(log, v),
    }
}

/// A put of `body`. A full counter refuses it and changes nothing.
pub open spec fn put(log: Log, body: int) -> Log {
    if spec_next_overlay_version(latest(log)).is_some() {
        Log { bodies: log.bodies.push(body), active: log.active }
    } else {
        log
    }
}

/// An activation of `v` over the expected pointer `expected`.
pub open spec fn activate(log: Log, v: u64, expected: Option<u64>) -> Log {
    if spec_decide_activate(holds(log, v), expected, log.active) == ActivateDecision::Activate {
        Log { bodies: log.bodies, active: Some(v) }
    } else {
        log
    }
}

/// One store call.
pub enum Op {
    Put(int),
    Activate(u64, Option<u64>),
}

/// One call on `log`.
pub open spec fn step(log: Log, op: Op) -> Log {
    match op {
        Op::Put(body) => put(log, body),
        Op::Activate(v, expected) => activate(log, v, expected),
    }
}

/// The calls `ops` on `log`, one after another.
pub open spec fn run(log: Log, ops: Seq<Op>) -> Log
    decreases ops.len(),
{
    if ops.len() == 0 {
        log
    } else {
        run(step(log, ops[0]), ops.drop_first())
    }
}

/// A put returns a version above each stored version. It keeps each stored
/// body, and it adds `body` as that version.
pub proof fn theorem_a_put_appends_a_new_top_version(log: Log, body: int)
    requires
        inv(log),
        log.bodies.len() < u64::MAX,
    ensures
        spec_next_overlay_version(latest(log)) == Some((log.bodies.len() + 1) as u64),
        forall|v: u64| holds(log, v) ==> v < log.bodies.len() + 1,
        put(log, body).bodies.len() == log.bodies.len() + 1,
        put(log, body).bodies.subrange(0, log.bodies.len() as int) == log.bodies,
        put(log, body).bodies[log.bodies.len() as int] == body,
        put(log, body).active == log.active,
        inv(put(log, body)),
{
    assert(put(log, body).bodies.subrange(0, log.bodies.len() as int) =~= log.bodies);
}

/// A refused activation changes nothing. An unknown version is refused.
pub proof fn theorem_a_refused_activate_changes_nothing(log: Log, v: u64, expected: Option<u64>)
    ensures
        !holds(log, v) ==> activate(log, v, expected) == log,
        expected != log.active ==> activate(log, v, expected) == log,
        activate(log, v, expected).bodies == log.bodies,
{
}

/// Each call keeps the invariant.
pub proof fn theorem_a_step_keeps_the_invariant(log: Log, op: Op)
    requires
        inv(log),
    ensures
        inv(step(log, op)),
{
}

/// No sequence of calls changes or drops a stored body, and each sequence
/// keeps the invariant.
pub proof fn theorem_no_call_rewrites_a_version(log: Log, ops: Seq<Op>)
    requires
        inv(log),
    ensures
        inv(run(log, ops)),
        log.bodies.len() <= run(log, ops).bodies.len(),
        run(log, ops).bodies.subrange(0, log.bodies.len() as int) == log.bodies,
    decreases ops.len(),
{
    if ops.len() > 0 {
        let next = step(log, ops[0]);
        theorem_a_step_keeps_the_invariant(log, ops[0]);
        assert(next.bodies.subrange(0, log.bodies.len() as int) =~= log.bodies);
        theorem_no_call_rewrites_a_version(next, ops.drop_first());
        let end = run(next, ops.drop_first());
        assert(end.bodies.subrange(0, log.bodies.len() as int) =~= end.bodies.subrange(
            0,
            next.bodies.len() as int,
        ).subrange(0, log.bodies.len() as int));
    }
}

/// Two activations read the same pointer. Each names a stored version, and
/// the first names a new one. The lock of the head row runs them in turn.
/// The first moves the pointer, and the second is a conflict. So exactly
/// one wins, in either order.
pub proof fn theorem_exactly_one_of_two_activates_wins(log: Log, first: u64, second: u64)
    requires
        inv(log),
        holds(log, first),
        holds(log, second),
        log.active != Some(first),
    ensures
        spec_decide_activate(holds(log, first), log.active, log.active)
            == ActivateDecision::Activate,
        activate(log, first, log.active).active == Some(first),
        spec_decide_activate(
            holds(activate(log, first, log.active), second),
            log.active,
            activate(log, first, log.active).active,
        ) == ActivateDecision::Conflict,
        activate(activate(log, first, log.active), second, log.active) == activate(
            log,
            first,
            log.active,
        ),
{
}

// mirror: autumn-harvest/src/overlay_log.rs logs_activation
pub fn logs_activation(actual: Option<u64>, version: u64) -> (r: bool)
    ensures
        r <==> actual != Some(version),
{
    match actual {
        Some(have) => have != version,
        None => true,
    }
}

/// The plan of a rollback: activate `target` over the pointer `expected`.
pub struct RollbackPlan {
    pub target: u64,
    pub expected: u64,
}

/// `rollback_target` as a spec.
pub open spec fn spec_rollback_target(from: Option<u64>, to: u64) -> Option<RollbackPlan> {
    match from {
        Some(target) => Some(RollbackPlan { target, expected: to }),
        None => None,
    }
}

// mirror: autumn-harvest/src/overlay_log.rs rollback_target
pub fn rollback_target(from: Option<u64>, to: u64) -> (r: Option<RollbackPlan>)
    ensures
        r == spec_rollback_target(from, to),
        r.is_none() <==> from.is_none(),
        r.is_some() ==> r.unwrap().target == from.unwrap() && r.unwrap().expected == to,
{
    match from {
        Some(target) => Some(RollbackPlan { target, expected: to, }),
        None => None,
    }
}

/// `decide_rollback` as a spec.
pub open spec fn spec_decide_rollback(
    known: bool,
    expected: u64,
    actual: Option<u64>,
    read_seq: u64,
    last_seq: Option<u64>,
) -> ActivateDecision {
    if spec_decide_activate(known, Some(expected), actual) != ActivateDecision::Activate {
        spec_decide_activate(known, Some(expected), actual)
    } else if last_seq == Some(read_seq) {
        ActivateDecision::Activate
    } else {
        ActivateDecision::Conflict
    }
}

// mirror: autumn-harvest/src/overlay_log.rs decide_rollback
pub fn decide_rollback(
    known: bool,
    expected: u64,
    actual: Option<u64>,
    read_seq: u64,
    last_seq: Option<u64>,
) -> (r: ActivateDecision)
    ensures
        r == spec_decide_rollback(known, expected, actual, read_seq, last_seq),
        r == ActivateDecision::Activate ==> last_seq == Some(read_seq),
        r == ActivateDecision::Activate ==> known && actual == Some(expected),
{
    match decide_activate(known, Some(expected), actual) {
        ActivateDecision::Activate => match last_seq {
            Some(seq) if seq == read_seq => ActivateDecision::Activate,
            _ => ActivateDecision::Conflict,
        },
        other => other,
    }
}

/// One move of the pointer: the pointer before it and the version after it.
pub struct Move {
    pub from: Option<u64>,
    pub to: u64,
}

/// The pointer of one kind and its log of moves. The kind holds the
/// versions 1 to `stored`.
pub struct Heads {
    pub stored: u64,
    pub active: Option<u64>,
    pub moves: Seq<Move>,
}

/// The pointer is the version after the last move, or unset with no move.
/// Each move starts where the move before it ended, and ends on a stored
/// version. No move stays in place.
pub open spec fn chained(h: Heads) -> bool {
    &&& (h.moves.len() == 0 ==> h.active.is_none())
    &&& (h.moves.len() > 0 ==> h.active == Some(h.moves.last().to))
    &&& (h.moves.len() > 0 ==> h.moves[0].from.is_none())
    &&& forall|i: int| 0 < i < h.moves.len() ==> #[trigger] h.moves[i].from == Some(h.moves[i - 1].to)
    &&& forall|i: int| 0 <= i < h.moves.len() ==> #[trigger] h.moves[i].from != Some(h.moves[i].to)
    &&& forall|i: int| 0 <= i < h.moves.len() ==> 1 <= #[trigger] h.moves[i].to <= h.stored
}

/// An activation of `v` over `expected`. A move to a new version logs it.
/// An activation of the active version succeeds and logs nothing.
pub open spec fn activate_logged(h: Heads, v: u64, expected: Option<u64>) -> Heads {
    let known = 1 <= v && v <= h.stored;
    if spec_decide_activate(known, expected, h.active) != ActivateDecision::Activate {
        h
    } else if h.active == Some(v) {
        h
    } else {
        Heads { stored: h.stored, active: Some(v), moves: h.moves.push(Move { from: h.active, to: v }) }
    }
}

/// A rollback: read the last move, then activate its start over its end.
/// With no move, or a last move from an unset pointer, it changes nothing.
pub open spec fn rollback(h: Heads) -> Heads {
    if h.moves.len() == 0 {
        h
    } else {
        match spec_rollback_target(h.moves.last().from, h.moves.last().to) {
            None => h,
            Some(plan) => activate_logged(h, plan.target, Some(plan.expected)),
        }
    }
}

/// An activation keeps the chain.
pub proof fn theorem_an_activation_keeps_the_chain(h: Heads, v: u64, expected: Option<u64>)
    requires
        chained(h),
    ensures
        chained(activate_logged(h, v, expected)),
        activate_logged(h, v, expected).stored == h.stored,
{
    let next = activate_logged(h, v, expected);
    if next.moves.len() > h.moves.len() {
        assert forall|i: int| 0 < i < next.moves.len() implies #[trigger] next.moves[i].from
            == Some(next.moves[i - 1].to) by {
            if i == h.moves.len() {
                assert(next.moves[i].from == h.active);
            } else {
                assert(next.moves[i] == h.moves[i]);
                assert(next.moves[i - 1] == h.moves[i - 1]);
            }
        }
        assert forall|i: int| 0 <= i < next.moves.len() implies #[trigger] next.moves[i].from
            != Some(next.moves[i].to) by {
            if i < h.moves.len() {
                assert(next.moves[i] == h.moves[i]);
            }
        }
        assert forall|i: int| 0 <= i < next.moves.len() implies 1 <= #[trigger] next.moves[i].to
            <= next.stored by {
            if i < h.moves.len() {
                assert(next.moves[i] == h.moves[i]);
            }
        }
    }
}

/// A rollback keeps the chain.
pub proof fn theorem_a_rollback_keeps_the_chain(h: Heads)
    requires
        chained(h),
    ensures
        chained(rollback(h)),
{
    if h.moves.len() > 0 {
        match spec_rollback_target(h.moves.last().from, h.moves.last().to) {
            None => {},
            Some(plan) => theorem_an_activation_keeps_the_chain(h, plan.target, Some(plan.expected)),
        }
    }
}

/// A rollback with no earlier activation changes nothing: no move, or a
/// last move from an unset pointer.
pub proof fn theorem_a_rollback_with_no_earlier_activation_changes_nothing(h: Heads)
    requires
        chained(h),
        h.moves.len() == 0 || h.moves.last().from.is_none(),
    ensures
        rollback(h) == h,
{
}

/// A rollback activates the version that was active before the current
/// one. It logs that move, so a second rollback undoes it.
pub proof fn theorem_a_rollback_restores_the_version_before(h: Heads)
    requires
        chained(h),
        h.moves.len() > 0,
        h.moves.last().from.is_some(),
        1 <= h.moves.last().from.unwrap() <= h.stored,
    ensures
        rollback(h).active == h.moves.last().from,
        rollback(h).moves.len() == h.moves.len() + 1,
        rollback(h).moves.last() == (Move { from: h.active, to: h.moves.last().from.unwrap() }),
        rollback(rollback(h)).active == h.active,
{
    let back = rollback(h);
    theorem_a_rollback_keeps_the_chain(h);
    assert(back.moves.last() == Move { from: h.active, to: h.moves.last().from.unwrap() });
    assert(h.active.is_some());
    assert(1 <= h.active.unwrap() <= h.stored);
}

/// The issue case: activate v1, then v2 over v1, then roll back. v1 is
/// active again.
pub proof fn theorem_a_rollback_after_v1_then_v2_makes_v1_active(stored: u64)
    requires
        stored >= 2,
    ensures
        rollback(
            activate_logged(
                activate_logged(Heads { stored, active: None, moves: Seq::empty() }, 1, None),
                2,
                Some(1u64),
            ),
        ).active == Some(1u64),
{
    let empty = Heads { stored, active: None, moves: Seq::empty() };
    let one = activate_logged(empty, 1, None);
    let two = activate_logged(one, 2, Some(1u64));
    assert(one.moves.len() == 1);
    assert(two.moves.len() == 2);
    assert(two.moves.last() == Move { from: Some(1u64), to: 2 });
}

/// Two rollbacks read the same last move. The lock of the head row runs
/// them in turn. The first moves the pointer, and the second is a
/// conflict, so it changes nothing.
pub proof fn theorem_exactly_one_of_two_rollbacks_wins(h: Heads)
    requires
        chained(h),
        h.moves.len() > 0,
        h.moves.last().from.is_some(),
        1 <= h.moves.last().from.unwrap() <= h.stored,
    ensures
        rollback(h).active != h.active,
        activate_logged(rollback(h), h.moves.last().from.unwrap(), Some(h.moves.last().to))
            == rollback(h),
{
}

/// The `seq` of the last move, or `None` with no move. The `seq` of a move
/// is its index plus 1.
pub open spec fn last_seq(h: Heads) -> Option<u64> {
    if h.moves.len() == 0 {
        None
    } else {
        Some(h.moves.len() as u64)
    }
}

/// The swap of a rollback that read `plan` at the move `read_seq`. It runs
/// later, on `h`, under the lock.
pub open spec fn rollback_swap(h: Heads, plan: RollbackPlan, read_seq: u64) -> Heads {
    let known = 1 <= plan.target && plan.target <= h.stored;
    if spec_decide_rollback(known, plan.expected, h.active, read_seq, last_seq(h))
        != ActivateDecision::Activate {
        h
    } else if h.active == Some(plan.target) {
        h
    } else {
        Heads {
            stored: h.stored,
            active: Some(plan.target),
            moves: h.moves.push(Move { from: h.active, to: plan.target }),
        }
    }
}

/// A swap over a stale read changes nothing: a move after the read changed
/// the last `seq`. That holds also when the pointer moved back to the value
/// that the rollback read.
pub proof fn theorem_a_stale_rollback_changes_nothing(h: Heads, plan: RollbackPlan, read_seq: u64)
    requires
        h.moves.len() <= u64::MAX,
        last_seq(h) != Some(read_seq),
    ensures
        rollback_swap(h, plan, read_seq) == h,
{
}

/// A swap over a fresh read is the rollback.
pub proof fn theorem_a_fresh_rollback_swap_is_the_rollback(h: Heads)
    requires
        chained(h),
        h.moves.len() > 0,
        h.moves.len() <= u64::MAX,
        h.moves.last().from.is_some(),
    ensures
        rollback_swap(
            h,
            spec_rollback_target(h.moves.last().from, h.moves.last().to).unwrap(),
            h.moves.len() as u64,
        ) == rollback(h),
{
}

/// The ABA case: a rollback reads v1 -> v2. The pointer then moves to v3
/// and back to v2. The pointer matches the read, but the swap changes
/// nothing.
pub proof fn theorem_a_move_away_and_back_fails_a_stale_rollback(h: Heads)
    requires
        chained(h),
        h.stored >= 3,
        h.moves.len() > 0,
        h.moves.len() < 1000,
        h.active == Some(2u64),
        h.moves.last().from == Some(1u64),
    ensures
        rollback_swap(
            activate_logged(activate_logged(h, 3, Some(2u64)), 2, Some(3u64)),
            RollbackPlan { target: 1, expected: 2 },
            h.moves.len() as u64,
        ) == activate_logged(activate_logged(h, 3, Some(2u64)), 2, Some(3u64)),
        activate_logged(activate_logged(h, 3, Some(2u64)), 2, Some(3u64)).active == Some(2u64),
{
    let away = activate_logged(h, 3, Some(2u64));
    let back = activate_logged(away, 2, Some(3u64));
    assert(away.moves.len() == h.moves.len() + 1);
    assert(back.moves.len() == h.moves.len() + 2);
    theorem_a_stale_rollback_changes_nothing(back, RollbackPlan { target: 1, expected: 2 }, h.moves.len() as u64);
}


/// The three outcomes of an auto adoption under the head lock (issue #2394).
pub enum AutoMoveDecision {
    Move,
    Conflict,
    CapReached,
}

/// `decide_auto_move` as a spec.
pub open spec fn spec_decide_auto_move(
    expected: Option<u64>,
    actual: Option<u64>,
    used: u64,
    cap: Option<u32>,
) -> AutoMoveDecision {
    if expected != actual {
        AutoMoveDecision::Conflict
    } else {
        match cap {
            Some(limit) => if used >= limit as u64 {
                AutoMoveDecision::CapReached
            } else {
                AutoMoveDecision::Move
            },
            None => AutoMoveDecision::Move,
        }
    }
}

// mirror: autumn-harvest/src/overlay_log.rs decide_auto_move
pub fn decide_auto_move(expected: Option<u64>, actual: Option<u64>, used: u64, cap: Option<u32>) -> (r:
    AutoMoveDecision)
    ensures
        r == spec_decide_auto_move(expected, actual, used, cap),
        r is Move ==> expected == actual,
        r is Move && cap is Some ==> used < cap->0 as u64,
{
    let same = match (expected, actual) {
        (None, None) => true,
        (Some(want), Some(have)) => want == have,
        _ => false,
    };
    if !same {
        return AutoMoveDecision::Conflict;
    }
    match cap {
        Some(limit) if used >= limit as u64 => AutoMoveDecision::CapReached,
        _ => AutoMoveDecision::Move,
    }
}

/// The auto moves of one UTC day, from the count `used`, after a sequence
/// of attempts. Each attempt reads the pointer that it expects, so only the
/// cap can refuse it. Each move adds one.
pub open spec fn spec_auto_day(cap: Option<u32>, used: u64, attempts: nat) -> u64
    decreases attempts,
{
    if attempts == 0 {
        used
    } else {
        let next = if spec_decide_auto_move(None, None, used, cap) is Move {
            (used + 1) as u64
        } else {
            used
        };
        spec_auto_day(cap, next, (attempts - 1) as nat)
    }
}

/// With a cap of `limit`, a day that starts at or under the cap ends at or
/// under it, after any number of attempts. The store counts under the head
/// lock, so the attempts of one kind run one after another.
pub proof fn theorem_an_auto_day_never_passes_the_cap(limit: u32, used: u64, attempts: nat)
    requires
        used <= limit as u64,
    ensures
        spec_auto_day(Some(limit), used, attempts) <= limit as u64,
    decreases attempts,
{
    if attempts > 0 {
        let next = if spec_decide_auto_move(None, None, used, Some(limit)) is Move {
            (used + 1) as u64
        } else {
            used
        };
        theorem_an_auto_day_never_passes_the_cap(limit, next, (attempts - 1) as nat);
    }
}

/// A stale pointer never moves, whatever the cap.
pub proof fn theorem_a_stale_auto_move_conflicts(
    expected: Option<u64>,
    actual: Option<u64>,
    used: u64,
    cap: Option<u32>,
)
    requires
        expected != actual,
    ensures
        spec_decide_auto_move(expected, actual, used, cap) is Conflict,
{
}

} // verus!
