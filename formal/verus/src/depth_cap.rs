//! The depth gauge caps the sum of the shards (issue #2356).
//!
//! The sampler reads one bounded depth per shard. A bounded depth is the
//! eligible count, or the cap when the shard holds more rows. The sampler
//! adds the shards with `add_bounded_depth`. The copy below proves that each
//! call is one `step` and stays at or below the cap.
//! `theorem_the_gauge_is_the_capped_total` proves that a fold of `step` over
//! any number of shards gives the capped sum of the true counts. So the gauge
//! is exact below the cap and reads the cap at or above it.

use vstd::prelude::*;

verus! {

/// `n`, or `cap` when `n` is larger.
pub open spec fn capped(n: int, cap: int) -> int {
    if n > cap {
        cap
    } else {
        n
    }
}

/// One call of `add_bounded_depth`: the capped sum of the capped total and
/// the capped shard depth.
pub open spec fn step(total: int, shard_depth: int, cap: int) -> int {
    capped(capped(total, cap) + capped(shard_depth, cap), cap)
}

// mirror: autumn-harvest/src/queue.rs add_bounded_depth
pub fn add_bounded_depth(total: u64, shard_depth: u64, cap: u32) -> (r: u64)
    ensures
        r == step(total as int, shard_depth as int, cap as int),
        r <= cap,
{
    let cap = u64::from(cap);
    let total = if total > cap {
        cap
    } else {
        total
    };
    let part = if shard_depth > cap {
        cap
    } else {
        shard_depth
    };
    let sum = total + part;
    if sum > cap {
        cap
    } else {
        sum
    }
}

/// The sum of the true counts of the shards.
pub open spec fn total(counts: Seq<nat>) -> nat
    decreases counts.len(),
{
    if counts.len() == 0 {
        0
    } else {
        total(counts.drop_last()) + counts.last()
    }
}

/// The gauge after the sampler adds each shard, from 0. The SQL returns the
/// bounded depth of a shard as `capped(count, cap)`.
pub open spec fn fold(counts: Seq<nat>, cap: int) -> int
    decreases counts.len(),
{
    if counts.len() == 0 {
        0
    } else {
        step(fold(counts.drop_last(), cap), capped(counts.last() as int, cap), cap)
    }
}

/// A cap before the add gives the same result as a cap after it.
proof fn lemma_a_cap_before_the_add_changes_nothing(sum: int, count: int, cap: int)
    requires
        sum >= 0,
        count >= 0,
        cap >= 0,
    ensures
        capped(capped(sum, cap) + capped(count, cap), cap) == capped(sum + count, cap),
{
}

/// The fold over the bounded depths is the capped total of the counts.
pub proof fn theorem_the_gauge_is_the_capped_total(counts: Seq<nat>, cap: nat)
    ensures
        fold(counts, cap as int) == capped(total(counts) as int, cap as int),
    decreases counts.len(),
{
    if counts.len() > 0 {
        let prefix = counts.drop_last();
        theorem_the_gauge_is_the_capped_total(prefix, cap);
        lemma_a_cap_before_the_add_changes_nothing(
            total(prefix) as int,
            counts.last() as int,
            cap as int,
        );
    }
}

/// A call on the fold so far and the bounded depth of one more shard gives
/// the fold with that shard.
pub proof fn theorem_a_call_extends_the_fold(prefix: Seq<nat>, count: nat, cap: u32)
    ensures
        step(fold(prefix, cap as int), capped(count as int, cap as int), cap as int) == fold(
            prefix.push(count),
            cap as int,
        ),
{
    assert(prefix.push(count).drop_last() =~= prefix);
}

} // verus!
