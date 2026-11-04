//! A strategy run never passes the draw caps of its policy (issue #2439).
//!
//! `draw_plan` admits the draw count and the concurrency of one run.
//! `wave_end` splits the draws into waves. `next_policy_revision` counts
//! the changes of a policy row. The copies below are the kernels in
//! `sampling/admit.rs` of the agent crate and in `agent_policy_log.rs`.
//!
//! The proofs show:
//!
//! - An admitted run has from 1 to `max_draws` draws.
//! - Its concurrency is from 1 to the cap of the policy, to
//!   `MAX_PARALLEL_CHILDREN` and to the draw count.
//! - Each wave holds from 1 to `concurrency` draws, and the waves cover
//!   each draw once. So no more than `concurrency` draws run at one time.
//! - A revision moves up by one and never wraps.

use vstd::prelude::*;

verus! {

/// The bound on concurrent children in the agent crate.
pub const MAX_PARALLEL_CHILDREN: u32 = 16;

/// The integer part of an admission.
pub enum DrawPlan {
    Run { draws: u32, concurrency: u32 },
    NoDraws,
    TooManyDraws,
    TooManyConcurrent,
}

/// The cap on the concurrency: the policy cap, at most
/// `MAX_PARALLEL_CHILDREN` and at least 1.
pub open spec fn spec_cap(max_concurrent: u32) -> int {
    if max_concurrent > MAX_PARALLEL_CHILDREN {
        MAX_PARALLEL_CHILDREN as int
    } else if max_concurrent == 0 {
        1
    } else {
        max_concurrent as int
    }
}

// mirror: autumn-harvest-agent/src/sampling/admit.rs draw_plan
pub fn draw_plan(max_draws: u32, max_concurrent: u32, draws: u32, asked: u32) -> (r: DrawPlan)
    ensures
        draws == 0 <==> r is NoDraws,
        r is TooManyDraws <==> (draws > 0 && draws > max_draws),
        r is TooManyConcurrent <==> (draws > 0 && draws <= max_draws && asked > spec_cap(
            max_concurrent,
        )),
        r is Run ==> r->Run_draws == draws,
        r is Run ==> 1 <= r->Run_draws <= max_draws,
        r is Run ==> 1 <= r->Run_concurrency,
        r is Run ==> r->Run_concurrency <= r->Run_draws,
        r is Run ==> r->Run_concurrency <= MAX_PARALLEL_CHILDREN,
        r is Run ==> r->Run_concurrency <= spec_cap(max_concurrent),
        r is Run && asked > 0 && asked <= draws ==> r->Run_concurrency == asked,
{
    if draws == 0 {
        return DrawPlan::NoDraws;
    }
    if draws > max_draws {
        return DrawPlan::TooManyDraws;
    }
    let mut cap = max_concurrent;
    if cap > MAX_PARALLEL_CHILDREN {
        cap = MAX_PARALLEL_CHILDREN;
    }
    if cap == 0 {
        cap = 1;
    }
    if asked > cap {
        return DrawPlan::TooManyConcurrent;
    }
    let mut concurrency = if asked == 0 { cap } else { asked };
    if concurrency > draws {
        concurrency = draws;
    }
    DrawPlan::Run { draws, concurrency }
}

/// `wave_end` as a spec.
pub open spec fn spec_wave_end(draws: int, concurrency: int, start: int) -> int {
    let width = if concurrency == 0 { 1 } else { concurrency };
    if start >= draws {
        draws
    } else if draws - start < width {
        draws
    } else {
        start + width
    }
}

// mirror: autumn-harvest-agent/src/sampling/admit.rs wave_end
pub fn wave_end(draws: u32, concurrency: u32, start: u32) -> (r: u32)
    ensures
        r == spec_wave_end(draws as int, concurrency as int, start as int),
        start < draws ==> start < r <= draws,
        start < draws && concurrency > 0 ==> r - start <= concurrency,
        start < draws && concurrency == 0 ==> r == start + 1,
        start >= draws ==> r == draws,
{
    if start >= draws {
        return draws;
    }
    let width = if concurrency == 0 { 1 } else { concurrency };
    let left = draws - start;
    if left < width {
        draws
    } else {
        start + width
    }
}

/// The number of waves from `start`, and the draws that they cover.
pub open spec fn covered(draws: int, concurrency: int, start: int) -> int
    decreases draws - start,
{
    if start >= draws || start < 0 {
        0
    } else {
        let end = spec_wave_end(draws, concurrency, start);
        if end <= start {
            0
        } else {
            (end - start) + covered(draws, concurrency, end)
        }
    }
}

/// The waves from `start` cover each draw after it exactly once.
pub proof fn theorem_the_waves_cover_each_draw_once(draws: int, concurrency: int, start: int)
    requires
        0 <= start <= draws,
        concurrency >= 0,
    ensures
        covered(draws, concurrency, start) == draws - start,
    decreases draws - start,
{
    if start < draws {
        let end = spec_wave_end(draws, concurrency, start);
        assert(start < end <= draws);
        theorem_the_waves_cover_each_draw_once(draws, concurrency, end);
    }
}

/// `next_policy_revision` as a spec.
pub open spec fn spec_next_policy_revision(current: u64) -> Option<u64> {
    if current < u64::MAX {
        Some((current + 1) as u64)
    } else {
        None
    }
}

// mirror: autumn-harvest/src/agent_policy_log.rs next_policy_revision
pub fn next_policy_revision(current: u64) -> (r: Option<u64>)
    ensures
        r == spec_next_policy_revision(current),
        r.is_some() ==> r.unwrap() == current + 1,
        r.is_some() ==> r.unwrap() >= 1,
        r.is_none() ==> current == u64::MAX,
{
    if current < u64::MAX {
        Some(current + 1)
    } else {
        None
    }
}

} // verus!
