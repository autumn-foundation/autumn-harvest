//! A sweep never opens more draws than the policy cap.
//!
//! `sweep_plan` splits the concurrency cap of a policy between the items
//! of a sweep and the draws of each item. The copy below is the kernel in
//! `sampling/admit.rs` of the agent crate.
//!
//! The proofs show:
//!
//! - An ask over the cap is refused, and an ask at or under it runs.
//! - The items and the draws at the same time are each at least 1.
//! - `items * draws` is never above the cap, so the open draws of the
//!   sweep never pass the cap of the policy.
//! - An ask of `n` items runs `n` items at the same time.
//! - The draws of one item are never above the draws that it can use, when
//!   it can use one or more.

use vstd::arithmetic::div_mod::*;
use vstd::prelude::*;

verus! {

/// The bound on concurrent children in the agent crate.
pub const MAX_PARALLEL_CHILDREN: u32 = 16;

/// The concurrency of a sweep.
pub enum SweepPlan {
    Run { items: u32, draws: u32 },
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

/// The base share of `items` over `cap` fits: `items * (cap / items)` is at
/// most `cap`, and it is at least 1 when `items <= cap`.
proof fn lemma_the_share_fits(cap: int, items: int)
    requires
        1 <= items <= cap,
    ensures
        1 <= cap / items,
        items * (cap / items) <= cap,
{
    lemma_fundamental_div_mod(cap, items);
    lemma_mod_pos_bound(cap, items);
    let q = cap / items;
    assert(q >= 1) by (nonlinear_arith)
        requires
            cap == items * q + cap % items,
            0 <= cap % items < items,
            items <= cap,
    ;
}

/// A smaller share still fits.
proof fn lemma_a_smaller_share_fits(cap: int, items: int, draws: int)
    requires
        1 <= items <= cap,
        1 <= draws <= cap / items,
    ensures
        items * draws <= cap,
{
    lemma_the_share_fits(cap, items);
    let q = cap / items;
    assert(items * draws <= items * q) by (nonlinear_arith)
        requires
            1 <= items,
            draws <= q,
    ;
}

// mirror: autumn-harvest-agent/src/sampling/admit.rs sweep_plan
pub fn sweep_plan(max_concurrent: u32, asked: u32, wide: u32) -> (r: SweepPlan)
    ensures
        r is TooManyConcurrent <==> asked > spec_cap(max_concurrent),
        r is Run ==> 1 <= r->Run_items,
        r is Run ==> 1 <= r->Run_draws,
        r is Run ==> r->Run_items <= spec_cap(max_concurrent),
        r is Run ==> r->Run_items * r->Run_draws <= spec_cap(max_concurrent),
        r is Run && asked > 0 ==> r->Run_items == asked,
        r is Run && wide > 0 ==> r->Run_draws <= wide,
{
    let mut cap = max_concurrent;
    if cap > MAX_PARALLEL_CHILDREN {
        cap = MAX_PARALLEL_CHILDREN;
    }
    if cap == 0 {
        cap = 1;
    }
    if asked > cap {
        return SweepPlan::TooManyConcurrent;
    }
    let items = if asked == 0 { cap } else { asked };
    let mut draws = cap / items;
    if draws > wide {
        draws = wide;
    }
    if draws == 0 {
        draws = 1;
    }
    proof {
        lemma_the_share_fits(cap as int, items as int);
        lemma_a_smaller_share_fits(cap as int, items as int, draws as int);
    }
    SweepPlan::Run { items, draws }
}

} // verus!
