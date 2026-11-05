//! The fixed budget of a refine run (issue #2380).
//!
//! `refine/ledger.rs` checks the counts of a run with `identity_holds`. The
//! copy below is that kernel. The spec `count` runs one plan over any
//! sequence of stage outcomes. Both modes plan 3k stages for each source.
//! The theorems state three properties. Each plan meets the identity. A
//! missing or an extra probe breaks it. A missing stage breaks it. The
//! proptest `each_plan_meets_the_identity` checks the same counts on a
//! model of the plan.

use vstd::prelude::*;

verus! {

// mirror: autumn-harvest-agent/src/refine/ledger.rs identity_holds
pub fn identity_holds(planned: u64, sessions: u64, probes: u64, skipped: u64) -> (r: bool)
    ensures
        r <==> (sessions == planned && skipped <= sessions && probes == sessions - skipped),
{
    sessions == planned && skipped <= sessions && probes == sessions - skipped
}

/// The stages of one source in a mode. Parallel runs 2k proposals and k
/// compositions. Sequential runs k steps of three stages.
pub open spec fn stages_of(parallel: bool, k: int) -> int {
    if parallel {
        2 * k + k
    } else {
        3 * k
    }
}

/// The counts after `n` stages over `outcomes`. `true` is a candidate,
/// which adds a probe. Any other stage adds a skip. Returns the sessions,
/// the probes and the skips.
pub open spec fn count(outcomes: Seq<bool>, n: int) -> (int, int, int)
    decreases n,
{
    if n <= 0 {
        (0, 0, 0)
    } else {
        let before = count(outcomes, n - 1);
        let candidate = if n - 1 < outcomes.len() {
            outcomes[n - 1]
        } else {
            true
        };
        if candidate {
            (before.0 + 1, before.1 + 1, before.2)
        } else {
            (before.0 + 1, before.1, before.2 + 1)
        }
    }
}

/// After `n` stages, the sessions are `n` and each session is a probe or
/// a skip.
proof fn lemma_count(outcomes: Seq<bool>, n: int)
    requires
        0 <= n,
    ensures
        count(outcomes, n).0 == n,
        count(outcomes, n).1 + count(outcomes, n).2 == n,
        0 <= count(outcomes, n).1,
        0 <= count(outcomes, n).2,
    decreases n,
{
    if n > 0 {
        lemma_count(outcomes, n - 1);
    }
}

/// Both modes plan 3k sessions for each source.
pub proof fn theorem_both_modes_plan_three_k(k: int)
    requires
        0 <= k,
    ensures
        stages_of(true, k) == 3 * k,
        stages_of(false, k) == 3 * k,
{
}

/// Each plan over each sequence of outcomes meets the identity. The run
/// plays one plan for each of its `sources` source runs in a row, so its
/// stages are `sources` plans long.
pub proof fn theorem_each_plan_meets_the_identity(
    outcomes: Seq<bool>,
    parallel: bool,
    k: u64,
    sources: u64,
)
    requires
        k <= 8,
        sources <= 16,
    ensures
        ({
            let c = count(outcomes, sources * stages_of(parallel, k as int));
            c.0 == 3 * k * sources && c.2 <= c.0 && c.1 == c.0 - c.2
        }),
{
    assert(sources * stages_of(parallel, k as int) == 3 * k * sources) by (nonlinear_arith)
        requires
            stages_of(parallel, k as int) == 3 * k,
    ;
    assert(sources * stages_of(parallel, k as int) >= 0) by (nonlinear_arith)
        requires
            stages_of(parallel, k as int) == 3 * k,
            0 <= k,
            0 <= sources,
    ;
    lemma_count(outcomes, sources * stages_of(parallel, k as int));
}

/// The identity over integers.
pub open spec fn holds(planned: int, sessions: int, probes: int, skipped: int) -> bool {
    sessions == planned && skipped <= sessions && probes == sessions - skipped
}

/// The kernel accepts exactly the counts of `holds`.
pub proof fn theorem_the_kernel_meets_holds(planned: u64, sessions: u64, probes: u64, skipped: u64)
    ensures
        holds(planned as int, sessions as int, probes as int, skipped as int) == (sessions
            == planned && skipped <= sessions && probes as int == sessions as int - skipped as int),
{
}

/// One probe fewer or one more than the plan breaks the identity.
pub proof fn theorem_a_wrong_probe_count_breaks_it(planned: int, skipped: int)
    requires
        0 <= skipped <= planned,
    ensures
        !holds(planned, planned, planned - skipped - 1, skipped),
        !holds(planned, planned, planned - skipped + 1, skipped),
{
}

/// A run that stops one session short breaks the identity, whatever its
/// probes.
pub proof fn theorem_a_short_run_breaks_it(planned: int, probes: int, skipped: int)
    ensures
        !holds(planned, planned - 1, probes, skipped),
{
}

} // verus!
