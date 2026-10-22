//! The owner fence passes no commit at an epoch older than the row's
//! (issue #2251).
//!
//! The engine computes the next epoch with `next_epoch` and checks a commit
//! with `fence_passes`. These copies prove both for every input. The spec
//! below models the epoch of one owner row under any sequence of lease
//! operations. Only an acquisition moves it. So after a takeover, the fence
//! refuses every commit of the old tenure.

use vstd::prelude::*;

verus! {

// mirror: autumn-harvest/src/decision_owner.rs next_epoch
pub fn next_epoch(epoch: i64) -> (r: Option<i64>)
    ensures
        epoch < i64::MAX ==> r == Some((epoch + 1) as i64),
        epoch == i64::MAX ==> r == None::<i64>,
{
    if epoch < i64::MAX {
        Some(epoch + 1)
    } else {
        None
    }
}

// mirror: autumn-harvest/src/decision_owner.rs fence_passes
pub fn fence_passes(pinned: i64, row: i64, holder_matches: bool) -> (r: bool)
    ensures
        r == (holder_matches && pinned == row),
        pinned < row ==> !r,
        pinned > row ==> !r,
{
    holder_matches && pinned == row
}

/// One statement on an owner row.
pub enum LeaseOp {
    /// `acquire`: the epoch grows by 1.
    Acquire,
    /// `renew_all`: the lease end moves. The epoch stays.
    Renew,
    /// `release_all`: the row is free. The epoch stays.
    Release,
    /// The clock, or the DR bump: the lease ends. The epoch stays.
    Expire,
}

/// The epoch after one statement.
pub open spec fn step(epoch: int, op: LeaseOp) -> int {
    match op {
        LeaseOp::Acquire => epoch + 1,
        _ => epoch,
    }
}

/// The epoch after a sequence of statements.
pub open spec fn run(epoch: int, ops: Seq<LeaseOp>) -> int
    decreases ops.len(),
{
    if ops.len() == 0 {
        epoch
    } else {
        run(step(epoch, ops[0]), ops.drop_first())
    }
}

/// The number of acquisitions in a sequence.
pub open spec fn acquisitions(ops: Seq<LeaseOp>) -> int
    decreases ops.len(),
{
    if ops.len() == 0 {
        0
    } else {
        let head = match ops[0] {
            LeaseOp::Acquire => 1int,
            _ => 0int,
        };
        head + acquisitions(ops.drop_first())
    }
}

/// The epoch grows by exactly the number of acquisitions.
proof fn lemma_run_counts_acquisitions(epoch: int, ops: Seq<LeaseOp>)
    ensures
        run(epoch, ops) == epoch + acquisitions(ops),
        acquisitions(ops) >= 0,
    decreases ops.len(),
{
    if ops.len() > 0 {
        lemma_run_counts_acquisitions(step(epoch, ops[0]), ops.drop_first());
    }
}

/// The epoch never falls.
pub proof fn theorem_the_epoch_never_falls(epoch: int, ops: Seq<LeaseOp>)
    ensures
        run(epoch, ops) >= epoch,
{
    lemma_run_counts_acquisitions(epoch, ops);
}

/// Renewals, releases and expiries never move the epoch. So an owner never
/// fences itself.
pub proof fn theorem_only_an_acquisition_moves_the_epoch(epoch: int, ops: Seq<LeaseOp>)
    requires
        acquisitions(ops) == 0,
    ensures
        run(epoch, ops) == epoch,
{
    lemma_run_counts_acquisitions(epoch, ops);
}

/// A tenure pinned `pinned` at its acquisition. After any later sequence
/// with one acquisition or more, the row epoch is newer. The fence then
/// refuses every commit of that tenure, whatever the holder.
pub proof fn theorem_a_takeover_fences_the_old_tenure(
    pinned: int,
    ops: Seq<LeaseOp>,
    holder_matches: bool,
)
    requires
        acquisitions(ops) > 0,
    ensures
        run(pinned, ops) > pinned,
        !(holder_matches && pinned == run(pinned, ops)),
{
    lemma_run_counts_acquisitions(pinned, ops);
}

} // verus!
