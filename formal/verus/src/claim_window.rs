//! The split claim takes the row that the one full statement takes.
//!
//! Each scan of the full statement is an input: the row it would take if it
//! ran, and the tests of its gate. The copy below proves the kernel in
//! `autumn-harvest/src/queue.rs`.

use vstd::prelude::*;

verus! {

pub enum WindowStep {
    Claimed,
    Empty,
    Fallback,
}

pub open spec fn spec_window_step(claimed: bool, fallback: bool) -> WindowStep {
    if claimed {
        WindowStep::Claimed
    } else if fallback {
        WindowStep::Fallback
    } else {
        WindowStep::Empty
    }
}

// mirror: autumn-harvest/src/queue.rs window_step
pub fn window_step(claimed: bool, fallback: bool) -> (r: WindowStep)
    ensures
        r == spec_window_step(claimed, fallback),
        r is Claimed <==> claimed,
        r is Fallback <==> !claimed && fallback,
        r is Empty <==> !claimed && !fallback,
{
    if claimed {
        WindowStep::Claimed
    } else if fallback {
        WindowStep::Fallback
    } else {
        WindowStep::Empty
    }
}

/// One snapshot of a claim. Each `Option` is the row that a scan takes if
/// it runs. `None` is a scan that finds no free row.
pub struct Snapshot {
    /// `seek_candidate`.
    pub window: Option<int>,
    /// `seek_deep_candidate`, without its gate.
    pub deep: Option<int>,
    /// `legacy_candidate`, without its gate.
    pub full: Option<int>,
    /// `seek_contended` holds a row.
    pub contended: bool,
    /// `seek_bounds` holds a row.
    pub bounds: bool,
    /// `seek_deep_bounds` holds a row.
    pub deep_bounds: bool,
    pub ageing: bool,
}

/// The deep scan of the full statement, behind its gate.
pub open spec fn deep_candidate(s: Snapshot) -> Option<int> {
    if s.window is None && s.contended {
        s.deep
    } else {
        None
    }
}

pub open spec fn full_statement(s: Snapshot) -> Option<int> {
    if s.window is Some {
        s.window
    } else if deep_candidate(s) is Some {
        deep_candidate(s)
    } else if (s.bounds && (!s.contended || s.deep_bounds)) || s.ageing {
        s.full
    } else {
        None
    }
}

pub open spec fn window_flag(s: Snapshot) -> bool {
    s.window is None && (s.contended || s.bounds || s.ageing)
}

/// The row that the split claim takes. The window form reads `first`. The
/// full statement, when it runs, reads `second`, a later snapshot.
pub open spec fn split_claim(first: Snapshot, second: Snapshot) -> Option<int> {
    match spec_window_step(first.window is Some, window_flag(first)) {
        WindowStep::Claimed => first.window,
        WindowStep::Empty => None,
        WindowStep::Fallback => full_statement(second),
    }
}

/// A clear flag with no window row proves that the full statement takes
/// no row from the same snapshot.
proof fn lemma_a_clear_flag_means_no_row(s: Snapshot)
    requires
        s.window is None,
        !window_flag(s),
    ensures
        full_statement(s) is None,
{
}

/// A set flag means that the window form took no row. So the full
/// statement after it is a fresh claim attempt.
proof fn lemma_a_set_flag_took_no_row(s: Snapshot)
    requires
        window_flag(s),
    ensures
        s.window is None,
{
}

/// The split claim takes the row of the one full statement when both read
/// the same snapshot. With a later snapshot, it takes the row of a full
/// statement that starts at that later point.
pub proof fn theorem_the_split_claim_takes_the_full_statement_row(
    first: Snapshot,
    second: Snapshot,
)
    ensures
        split_claim(first, first) == full_statement(first),
        window_flag(first) ==> split_claim(first, second) == full_statement(second),
        !window_flag(first) ==> split_claim(first, second) == full_statement(first),
{
    if first.window is None && !window_flag(first) {
        lemma_a_clear_flag_means_no_row(first);
    }
    if window_flag(first) {
        lemma_a_set_flag_took_no_row(first);
    }
}

} // verus!
