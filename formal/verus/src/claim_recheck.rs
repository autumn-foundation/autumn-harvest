//! The pipelined claim rechecks keep a row only when the sequential
//! rechecks keep it.
//!
//! A claim takes the queue lock, then runs its rechecks. The sequential form
//! stops at the first failure. The pipelined form sends every statement, so
//! a recheck can run after the lock failed or after an earlier recheck gave
//! the row back. A release matches only a `RUNNING` row, so a later release
//! changes nothing.
//!
//! The copy below proves the kernel in `autumn-harvest/src/queue.rs`. The
//! theorem proves that both forms end with the same outcome for every input.

use vstd::prelude::*;

verus! {

/// What the claim does after its rechecks.
pub enum RecheckVerdict {
    /// The queue lock holds and no recheck gave the row back.
    Keep,
    /// A recheck gave the row back.
    Released,
    /// The lock failed and the row is still claimed. The claim gives it back.
    Release,
}

/// `recheck_verdict` as a spec.
pub open spec fn spec_recheck_verdict(acquired: bool, released: bool) -> RecheckVerdict {
    if released {
        RecheckVerdict::Released
    } else if acquired {
        RecheckVerdict::Keep
    } else {
        RecheckVerdict::Release
    }
}

// mirror: autumn-harvest/src/queue.rs recheck_verdict
pub fn recheck_verdict(acquired: bool, released: bool) -> (r: RecheckVerdict)
    ensures
        r == spec_recheck_verdict(acquired, released),
        r is Keep <==> acquired && !released,
        r is Release <==> !acquired && !released,
        r is Released <==> released,
{
    if released {
        RecheckVerdict::Released
    } else if acquired {
        RecheckVerdict::Keep
    } else {
        RecheckVerdict::Release
    }
}

/// The sequential rechecks: the lock, then each recheck in turn. Each one
/// stops the claim when it fails. True means that the claim keeps its row.
pub open spec fn sequential_keeps(acquired: bool, paused: Seq<bool>) -> bool {
    acquired && forall|i: int| 0 <= i < paused.len() ==> !paused[i]
}

/// The rechecks in a pipeline. A recheck after a release matches no row,
/// so it reports no release of its own.
pub open spec fn pipelined_releases(paused: Seq<bool>) -> Seq<bool>
    decreases paused.len(),
{
    if paused.len() == 0 {
        Seq::empty()
    } else {
        let head = paused[0];
        let rest = pipelined_releases(paused.drop_first());
        if head {
            seq![true].add(Seq::new(rest.len(), |i: int| false))
        } else {
            seq![false].add(rest)
        }
    }
}

/// Some recheck of the pipeline gave the row back.
pub open spec fn any_release(released: Seq<bool>) -> bool {
    exists|i: int| 0 <= i < released.len() && released[i]
}

/// A pipeline releases exactly when some recheck finds a hold.
proof fn lemma_pipeline_releases_iff_a_hold(paused: Seq<bool>)
    ensures
        pipelined_releases(paused).len() == paused.len(),
        any_release(pipelined_releases(paused)) <==> any_release(paused),
    decreases paused.len(),
{
    if paused.len() > 0 {
        let rest = pipelined_releases(paused.drop_first());
        lemma_pipeline_releases_iff_a_hold(paused.drop_first());
        let out = pipelined_releases(paused);
        if paused[0] {
            assert(out[0]);
        } else {
            assert(out =~= seq![false].add(rest));
            if any_release(paused) {
                let i = choose|i: int| 0 <= i < paused.len() && paused[i];
                assert(paused.drop_first()[i - 1]);
                let j = choose|j: int| 0 <= j < rest.len() && rest[j];
                assert(out[j + 1]);
            }
            if any_release(out) {
                let i = choose|i: int| 0 <= i < out.len() && out[i];
                assert(rest[i - 1]);
                let j = choose|j: int|
                    0 <= j < paused.drop_first().len() && paused.drop_first()[j];
                assert(paused[j + 1]);
            }
        }
    }
}

/// The pipelined claim keeps its row exactly when the sequential claim
/// keeps it. Every verdict but `Keep` ends with the row released: `Released`
/// by a recheck, `Release` by the claim.
pub proof fn theorem_the_pipeline_keeps_what_the_sequence_keeps(
    acquired: bool,
    paused: Seq<bool>,
)
    ensures
        (spec_recheck_verdict(acquired, any_release(pipelined_releases(paused))) is Keep)
            <==> sequential_keeps(acquired, paused),
{
    lemma_pipeline_releases_iff_a_hold(paused);
    if sequential_keeps(acquired, paused) {
        assert(!any_release(paused));
    }
    if !any_release(paused) {
        assert forall|i: int| 0 <= i < paused.len() implies !paused[i] by {
            if paused[i] {
                assert(any_release(paused));
            }
        }
    }
}

} // verus!
