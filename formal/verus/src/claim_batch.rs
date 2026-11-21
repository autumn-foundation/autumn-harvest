//! The size of one batch claim, and the rows that a batch keeps.
//!
//! Each claim loop holds its share of the free permits of each allowed kind
//! across its claim. So one loop never holds the permits of every peer. A
//! batch keeps its rows in claim order up to the second row with a key. So a
//! concurrency key or a rate-limit bucket meets at most one row of a batch.

use vstd::prelude::*;

verus! {

pub open spec fn effective_loops(loops: int) -> int {
    if loops == 0 {
        1
    } else {
        loops
    }
}

/// The permits of one loop when `free` permits are free: `free / loops`,
/// rounded up.
pub open spec fn share(free: int, loops: int) -> int {
    (free - 1) / loops + 1
}

pub open spec fn spec_claim_batch_share(workflow: int, activity: int, loops: int, max: int) -> int {
    let l = effective_loops(loops);
    if workflow == 0 && activity == 0 {
        1
    } else {
        let rows = if workflow == 0 {
            share(activity, l)
        } else if activity == 0 {
            share(workflow, l)
        } else if share(workflow, l) < share(activity, l) {
            share(workflow, l)
        } else {
            share(activity, l)
        };
        if rows <= max {
            rows
        } else if max == 0 {
            1
        } else {
            max
        }
    }
}

proof fn lemma_a_share_fits_its_pool(free: int, loops: int)
    requires
        free >= 1,
        loops >= 1,
    ensures
        1 <= share(free, loops) <= free,
        (share(free, loops) - 1) * loops < free,
{
    assert((free - 1) / loops <= free - 1) by (nonlinear_arith)
        requires
            free >= 1,
            loops >= 1,
    ;
    assert(((free - 1) / loops) * loops <= free - 1) by (nonlinear_arith)
        requires
            free >= 1,
            loops >= 1,
    ;
}

proof fn lemma_a_smaller_batch_fits_too(rows: int, bound: int, loops: int, free: int)
    requires
        1 <= rows <= bound,
        loops >= 1,
        (bound - 1) * loops < free,
    ensures
        (rows - 1) * loops < free,
{
    assert((rows - 1) * loops <= (bound - 1) * loops) by (nonlinear_arith)
        requires
            1 <= rows <= bound,
            loops >= 1,
    ;
}

// mirror: autumn-harvest/src/worker.rs claim_batch_share
pub fn claim_batch_share(workflow: usize, activity: usize, loops: usize, max: usize) -> (r: usize)
    ensures
        r as int == spec_claim_batch_share(workflow as int, activity as int, loops as int, max as int),
{
    let loops = if loops == 0 {
        1
    } else {
        loops
    };
    if workflow == 0 && activity == 0 {
        return 1;
    }
    proof {
        if workflow > 0 {
            lemma_a_share_fits_its_pool(workflow as int, loops as int);
        }
        if activity > 0 {
            lemma_a_share_fits_its_pool(activity as int, loops as int);
        }
    }
    let workflow_share = if workflow == 0 {
        usize::MAX
    } else {
        (workflow - 1) / loops + 1
    };
    let activity_share = if activity == 0 {
        usize::MAX
    } else {
        (activity - 1) / loops + 1
    };
    let rows = if workflow_share < activity_share {
        workflow_share
    } else {
        activity_share
    };
    if rows <= max {
        rows
    } else if max == 0 {
        1
    } else {
        max
    }
}

/// A batch is never empty and never over its cap. Each allowed kind has a
/// permit for every row. The loops that take the same batch together take
/// less than one batch past the free permits.
pub proof fn theorem_a_batch_fits_each_allowed_kind(
    workflow: int,
    activity: int,
    loops: int,
    max: int,
)
    requires
        workflow >= 0,
        activity >= 0,
        loops >= 0,
        max >= 0,
    ensures
        ({
            let r = spec_claim_batch_share(workflow, activity, loops, max);
            let l = effective_loops(loops);
            &&& r >= 1
            &&& max >= 1 ==> r <= max
            &&& workflow > 0 ==> (r - 1) * l < workflow
            &&& activity > 0 ==> (r - 1) * l < activity
        }),
{
    let r = spec_claim_batch_share(workflow, activity, loops, max);
    let l = effective_loops(loops);
    if workflow > 0 {
        lemma_a_share_fits_its_pool(workflow, l);
    }
    if activity > 0 {
        lemma_a_share_fits_its_pool(activity, l);
    }
    if workflow > 0 {
        lemma_a_smaller_batch_fits_too(r, share(workflow, l), l, workflow);
    }
    if activity > 0 {
        lemma_a_smaller_batch_fits_too(r, share(activity, l), l, activity);
    }
}

/// The rows from `i` on that come before the next keyed row.
pub open spec fn first_keyed_from(keyed: Seq<bool>, i: int) -> int
    decreases keyed.len() - i,
{
    if i >= keyed.len() {
        keyed.len() as int
    } else if keyed[i] {
        i
    } else {
        first_keyed_from(keyed, i + 1)
    }
}

/// The rows that `candidate` keeps of the ranked rows. `keyed[i]` holds when
/// row `i` carries a concurrency key or a rate-limit key.
pub open spec fn kept(keyed: Seq<bool>) -> int {
    if keyed.len() == 0 {
        0
    } else {
        first_keyed_from(keyed, 1)
    }
}

proof fn lemma_the_scan_stops_at_the_first_key(keyed: Seq<bool>, i: int)
    requires
        0 <= i <= keyed.len(),
    ensures
        i <= first_keyed_from(keyed, i) <= keyed.len(),
        forall|j: int| i <= j < first_keyed_from(keyed, i) ==> !keyed[j],
        first_keyed_from(keyed, i) < keyed.len() ==> keyed[first_keyed_from(keyed, i)],
    decreases keyed.len() - i,
{
    if i < keyed.len() && !keyed[i] {
        lemma_the_scan_stops_at_the_first_key(keyed, i + 1);
    }
}

/// A batch keeps its first row and the longest run after it with no key.
/// So only the first kept row may carry a key.
pub proof fn theorem_only_the_first_kept_row_carries_a_key(keyed: Seq<bool>)
    ensures
        0 <= kept(keyed) <= keyed.len(),
        keyed.len() > 0 ==> kept(keyed) >= 1,
        forall|j: int| 1 <= j < kept(keyed) ==> !keyed[j],
        kept(keyed) < keyed.len() ==> keyed[kept(keyed)],
{
    if keyed.len() > 0 {
        lemma_the_scan_stops_at_the_first_key(keyed, 1);
    }
}

} // verus!
