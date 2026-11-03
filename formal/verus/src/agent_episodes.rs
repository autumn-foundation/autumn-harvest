//! The episode log of an agent keeps its bound (issue #2454).
//!
//! `episode_evictions` gives the episodes that one append evicts.
//! `consolidation_due` asks whether a pass is due. The copies below prove
//! that no step overflows and that each one equals its spec. The theorems
//! show that an append never passes the bound and evicts only stored
//! episodes. A log within the bound loses at most one episode per append.
//! No sequence of appends takes the log past the bound. A pass is due only
//! at a positive multiple of the trigger, and a trigger of 0 never fires.

use vstd::prelude::*;

verus! {

/// The bound that the log uses: 0 counts as 1.
pub open spec fn bound(max: int) -> int {
    if max == 0 {
        1
    } else {
        max
    }
}

/// The episodes that one append evicts from a log of `len`.
pub open spec fn evicted(len: int, max: int) -> int {
    if len < bound(max) {
        0
    } else {
        len - bound(max) + 1
    }
}

/// The length of the log after one append.
pub open spec fn after(len: int, max: int) -> int {
    len - evicted(len, max) + 1
}

// mirror: autumn-harvest-agent/src/bounds.rs episode_evictions
pub fn episode_evictions(len: u64, max: u64) -> (r: u64)
    ensures
        r == evicted(len as int, max as int),
{
    let max = if max == 0 {
        1
    } else {
        max
    };
    if len < max {
        0
    } else {
        len - max + 1
    }
}

/// A pass is due at each positive multiple of the trigger.
pub open spec fn due(pending: int, every: int) -> bool {
    every > 0 && pending >= every && pending % every == 0
}

// mirror: autumn-harvest-agent/src/bounds.rs consolidation_due
pub fn consolidation_due(pending: u64, every: u32) -> (r: bool)
    ensures
        r == due(pending as int, every as int),
{
    every > 0 && pending >= every as u64 && pending % every as u64 == 0
}

/// An append never passes the bound, and it evicts only stored episodes.
pub proof fn theorem_an_append_keeps_the_bound(len: int, max: int)
    requires
        0 <= len,
        0 <= max,
    ensures
        0 <= evicted(len, max) <= len,
        1 <= after(len, max) <= bound(max),
{
}

/// A log within the bound loses at most one episode per append.
pub proof fn theorem_a_log_in_its_bound_evicts_at_most_one(len: int, max: int)
    requires
        0 <= len <= bound(max),
        0 <= max,
    ensures
        evicted(len, max) <= 1,
        len < bound(max) ==> evicted(len, max) == 0,
{
}

/// The length of the log after `n` appends to a log of `len`.
pub open spec fn run(len: int, max: int, n: nat) -> int
    decreases n,
{
    if n == 0 {
        len
    } else {
        run(after(len, max), max, (n - 1) as nat)
    }
}

/// No sequence of appends takes a log in its bound past it.
pub proof fn theorem_no_sequence_passes_the_bound(len: int, max: int, n: nat)
    requires
        0 <= len <= bound(max),
        0 <= max,
    ensures
        0 <= run(len, max, n) <= bound(max),
    decreases n,
{
    if n > 0 {
        theorem_an_append_keeps_the_bound(len, max);
        theorem_no_sequence_passes_the_bound(after(len, max), max, (n - 1) as nat);
    }
}

/// A trigger of 0 never fires.
pub proof fn theorem_a_zero_trigger_never_fires(pending: int)
    ensures
        !due(pending, 0),
{
}

/// A due pass has a positive trigger and at least that many episodes.
pub proof fn theorem_a_due_pass_has_a_full_trigger(pending: int, every: int)
    requires
        due(pending, every),
    ensures
        pending >= every,
        every > 0,
{
}

/// Under the trigger, no pass is due.
pub proof fn theorem_no_pass_is_due_under_the_trigger(pending: int, every: int)
    requires
        0 <= pending < every,
    ensures
        !due(pending, every),
{
}

} // verus!
