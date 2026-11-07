//! A mandate never widens the draw cap, and the draws of a strategy run
//! fit the fan-out of the tree.
//!
//! `meet_cap` meets the draw cap of a parent mandate with the cap of a
//! peer, or of an inbound mandate. `draw_verdict` admits the draws of one
//! strategy run. Each draw counts as one delegation. The copies below are
//! the kernels in `mesh.rs` of the agent crate.
//!
//! The proofs show:
//!
//! - The meet is never above a cap of either side. It has no cap only when
//!   neither side has one. So a chain of meets only narrows.
//! - An admitted run passes no cap, and the draws and the delegations left
//!   sum to the room of the tree. So the draws never pass the fan-out.
//! - A refusal names the first bound that fails: the cap, then the room.

use vstd::prelude::*;

verus! {

/// Whether the cap `child` is no wider than the cap `parent`.
pub open spec fn within(child: Option<u32>, parent: Option<u32>) -> bool {
    match parent {
        None => true,
        Some(p) => match child {
            None => false,
            Some(c) => c <= p,
        },
    }
}

/// Whether `draws` fits the cap `max_draws`.
pub open spec fn fits(max_draws: Option<u32>, draws: u32) -> bool {
    match max_draws {
        None => true,
        Some(max) => draws <= max,
    }
}

// mirror: autumn-harvest-agent/src/mesh.rs meet_cap
pub fn meet_cap(left: Option<u32>, right: Option<u32>) -> (r: Option<u32>)
    ensures
        within(r, left),
        within(r, right),
        r is None <==> (left is None && right is None),
        r is Some && left is Some && right is Some ==> (r->0 == left->0 || r->0 == right->0),
{
    match (left, right) {
        (Some(a), Some(b)) => {
            if a < b {
                Some(a)
            } else {
                Some(b)
            }
        }
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// The verdict of `draw_verdict`.
pub enum DrawVerdict {
    Run { left: u32 },
    DrawsExceeded,
    FanOutExhausted,
}

// mirror: autumn-harvest-agent/src/mesh.rs draw_verdict
pub fn draw_verdict(max_draws: Option<u32>, room: u32, draws: u32) -> (r: DrawVerdict)
    ensures
        r is DrawsExceeded <==> !fits(max_draws, draws),
        r is FanOutExhausted <==> (fits(max_draws, draws) && draws > room),
        r is Run <==> (fits(max_draws, draws) && draws <= room),
        r is Run ==> r->Run_left + draws == room,
{
    let over = match max_draws {
        Some(max) => draws > max,
        None => false,
    };
    if over {
        return DrawVerdict::DrawsExceeded;
    }
    if draws > room {
        return DrawVerdict::FanOutExhausted;
    }
    DrawVerdict::Run { left: room - draws }
}

/// A cap within a cap within `top` is within `top`. So a chain of meets
/// down a delegation tree never widens the cap of the root.
pub proof fn theorem_within_is_transitive(a: Option<u32>, b: Option<u32>, top: Option<u32>)
    requires
        within(a, b),
        within(b, top),
    ensures
        within(a, top),
{
}

/// A count that fits a cap fits each wider cap. So a run that the child
/// mandate admits also fits the cap of each mandate above it.
pub proof fn theorem_a_fit_holds_under_a_wider_cap(child: Option<u32>, parent: Option<u32>, draws: u32)
    requires
        within(child, parent),
        fits(child, draws),
    ensures
        fits(parent, draws),
{
}

} // verus!
