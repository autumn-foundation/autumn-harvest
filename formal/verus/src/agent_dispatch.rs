//! One consolidation firing starts a bounded number of passes (issue #2540).
//!
//! `dispatch_passes` gives the passes that one firing of
//! `agent_consolidate_all` starts. The copy below proves that no step
//! overflows and that it equals its spec. The theorems show that a firing
//! starts at most the bound and at most the due agents. A firing with a due
//! agent starts at least one pass. When the due agents fit the bound, none
//! waits. With no new due agent, the queue drains within the number of
//! firings that the bound gives.

use vstd::prelude::*;

verus! {

/// The most passes of one firing.
pub const MAX_DISPATCH_PASSES: u32 = 256;

/// The bound of one firing: 0 counts as 1, and the cap is 256.
pub open spec fn cap(max: int) -> int {
    if max == 0 {
        1
    } else if max > 256 {
        256
    } else {
        max
    }
}

/// The passes that one firing starts for `due` agents.
pub open spec fn started(due: int, max: int) -> int {
    if due < cap(max) {
        due
    } else {
        cap(max)
    }
}

// mirror: autumn-harvest-agent/src/bounds.rs dispatch_passes
pub fn dispatch_passes(due: u64, max: u32) -> (r: u64)
    ensures
        r == started(due as int, max as int),
{
    let max = if max == 0 {
        1
    } else if max > MAX_DISPATCH_PASSES {
        MAX_DISPATCH_PASSES
    } else {
        max
    };
    if due < max as u64 {
        due
    } else {
        max as u64
    }
}

/// A firing starts at most the bound and at most the due agents.
pub proof fn theorem_a_firing_keeps_its_bound(due: int, max: int)
    requires
        0 <= due,
        0 <= max,
    ensures
        1 <= cap(max) <= 256,
        0 <= started(due, max) <= cap(max),
        started(due, max) <= due,
{
}

/// A firing with a due agent starts at least one pass.
pub proof fn theorem_a_due_agent_makes_progress(due: int, max: int)
    requires
        0 < due,
        0 <= max,
    ensures
        1 <= started(due, max),
{
}

/// When the due agents fit the bound, none waits.
pub proof fn theorem_agents_in_the_bound_never_wait(due: int, max: int)
    requires
        0 <= due <= cap(max),
        0 <= max,
    ensures
        started(due, max) == due,
{
}

/// The due agents that wait after `n` firings with no new due agent.
pub open spec fn left(due: int, max: int, n: nat) -> int
    decreases n,
{
    if n == 0 {
        due
    } else {
        left(due - started(due, max), max, (n - 1) as nat)
    }
}

/// With no new due agent, `n` firings that cover the queue drain it.
pub proof fn theorem_the_queue_drains(due: int, max: int, n: nat)
    requires
        0 <= due,
        0 <= max,
        due <= n * cap(max),
    ensures
        left(due, max, n) == 0,
    decreases n,
{
    if n > 0 {
        let c = cap(max);
        let m = (n - 1) as nat;
        assert(m * c == n * c - c) by (nonlinear_arith)
            requires
                m == n - 1,
        ;
        assert(0 <= m * c) by (nonlinear_arith)
            requires
                0 <= m,
                0 <= c,
        ;
        theorem_the_queue_drains(due - started(due, max), max, m);
        assert(left(due, max, n) == left(due - started(due, max), max, m));
    } else {
        assert(n * cap(max) == 0) by (nonlinear_arith)
            requires
                n == 0,
        ;
    }
}

} // verus!
