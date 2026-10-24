//! Concurrent children split the remaining tokens of a parent exactly
//! (issue #2270).
//!
//! `sibling_cap` gives child `index` of `siblings` its token share. The
//! copy below proves that no step overflows, and that each share is the
//! base share or one more. The spec sums the shares. They sum to the
//! remainder, so the siblings never pass the cap of the parent, and they
//! leave no token unused.

use vstd::prelude::*;
use vstd::arithmetic::div_mod::*;

verus! {

/// The share of child `index`: the base share, and one more for each of
/// the first `remaining % siblings` children.
pub open spec fn share(remaining: int, siblings: int, index: int) -> int {
    if index < remaining % siblings {
        remaining / siblings + 1
    } else {
        remaining / siblings
    }
}

/// The sum of the shares of children `0..n`.
pub open spec fn total(remaining: int, siblings: int, n: int) -> int
    decreases n,
{
    if n <= 0 {
        0
    } else {
        total(remaining, siblings, n - 1) + share(remaining, siblings, n - 1)
    }
}

/// A share of one more than the base still fits in `remaining`. One more
/// goes only to a child under `remaining % siblings`, so that remainder is
/// at least 1.
proof fn lemma_a_share_fits(remaining: int, siblings: int, index: int)
    requires
        0 <= remaining,
        0 < siblings,
        0 <= index,
    ensures
        0 <= remaining / siblings <= remaining,
        0 <= remaining % siblings < siblings,
        index < remaining % siblings ==> remaining / siblings + 1 <= remaining,
{
    lemma_fundamental_div_mod(remaining, siblings);
    lemma_mod_pos_bound(remaining, siblings);
    lemma_div_pos_is_pos(remaining, siblings);
    let q = remaining / siblings;
    assert(siblings * q >= q) by (nonlinear_arith)
        requires
            siblings >= 1,
            q >= 0,
    ;
}

// mirror: autumn-harvest-agent/src/bounds.rs sibling_cap
pub fn sibling_cap(remaining: u32, siblings: u32, index: u32) -> (r: u32)
    ensures
        siblings == 0 ==> r == 0,
        siblings > 0 ==> r == share(remaining as int, siblings as int, index as int),
        siblings > 0 ==> (r == remaining / siblings || r == remaining / siblings + 1),
        r <= remaining,
{
    if siblings == 0 {
        return 0;
    }
    let base = remaining / siblings;
    proof {
        lemma_a_share_fits(remaining as int, siblings as int, index as int);
    }
    if index < remaining % siblings {
        base + 1
    } else {
        base
    }
}

/// How many of the first `n` children get one more than the base share.
pub open spec fn extras(remaining: int, siblings: int, n: int) -> int {
    if n < remaining % siblings {
        n
    } else {
        remaining % siblings
    }
}

/// The first `n` shares sum to `n` base shares and their extras.
proof fn lemma_total_of_the_first(remaining: int, siblings: int, n: int)
    requires
        0 <= remaining,
        0 < siblings,
        0 <= n,
    ensures
        total(remaining, siblings, n) == n * (remaining / siblings) + extras(remaining, siblings, n),
    decreases n,
{
    lemma_mod_pos_bound(remaining, siblings);
    let q = remaining / siblings;
    let m = remaining % siblings;
    if n > 0 {
        lemma_total_of_the_first(remaining, siblings, n - 1);
        assert(n * q == (n - 1) * q + q) by (nonlinear_arith);
        if n - 1 < m {
            assert(total(remaining, siblings, n) == n * q + n);
        } else {
            assert(total(remaining, siblings, n) == n * q + m);
        }
    } else {
        assert(0 * q == 0);
    }
}

/// The shares of all siblings sum to the remainder. The siblings never pass
/// the cap of the parent, and they leave no token unused.
pub proof fn theorem_the_shares_sum_to_the_remainder(remaining: int, siblings: int)
    requires
        0 <= remaining,
        0 < siblings,
    ensures
        total(remaining, siblings, siblings) == remaining,
{
    lemma_total_of_the_first(remaining, siblings, siblings);
    lemma_a_share_fits(remaining, siblings, 0);
    lemma_fundamental_div_mod(remaining, siblings);
}

/// The children that started first never pass the cap. The first `n`
/// shares sum to `remaining` or less, for each `n` up to `siblings`.
pub proof fn theorem_no_first_children_pass_the_cap(remaining: int, siblings: int, n: int)
    requires
        0 <= remaining,
        0 < siblings,
        0 <= n <= siblings,
    ensures
        total(remaining, siblings, n) <= remaining,
{
    lemma_total_of_the_first(remaining, siblings, n);
    lemma_a_share_fits(remaining, siblings, 0);
    lemma_fundamental_div_mod(remaining, siblings);
    let q = remaining / siblings;
    assert(n * q <= siblings * q) by (nonlinear_arith)
        requires
            n <= siblings,
            q >= 0,
    ;
}

/// Each share is the base share or one more, and an earlier child never
/// gets less than a later one. So the split is as even as it can be.
pub proof fn theorem_each_share_is_the_base_or_one_more(
    remaining: int,
    siblings: int,
    i: int,
    j: int,
)
    requires
        0 <= remaining,
        0 < siblings,
    ensures
        share(remaining, siblings, i) == remaining / siblings || share(remaining, siblings, i)
            == remaining / siblings + 1,
        i <= j ==> share(remaining, siblings, i) >= share(remaining, siblings, j),
{
}

} // verus!
