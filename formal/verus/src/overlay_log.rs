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

} // verus!
