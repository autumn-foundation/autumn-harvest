//! The id width of an anonymised pool (issue #2376).
//!
//! `eval::anon::assign` gives each candidate the id `c-<prefix>`. The
//! prefix is the first `width` hex digits of the sort key of the candidate.
//! The copy `shared_digits` counts the leading digits that two keys share.
//! The copy `id_width` picks the width from the shared count of every pair.
//! `theorem_the_width_is_the_least_safe_width` shows that the width is above
//! each shared count and is the least such width from the floor.
//! `theorem_a_width_above_the_shared_count_splits_a_pair` shows that two
//! prefixes of that width then differ. So no id repeats.

use vstd::prelude::*;

verus! {

/// The number of leading digits that `a` and `b` share.
pub open spec fn common(a: Seq<u8>, b: Seq<u8>) -> nat
    decreases a.len(),
{
    if a.len() == 0 || b.len() == 0 || a[0] != b[0] {
        0
    } else {
        1 + common(a.drop_first(), b.drop_first())
    }
}

/// `common` is a length that both sequences hold. Each digit below it is
/// equal. The digit at it differs, when both sequences hold it.
proof fn lemma_common(a: Seq<u8>, b: Seq<u8>)
    ensures
        common(a, b) <= a.len(),
        common(a, b) <= b.len(),
        forall|i: int| 0 <= i < common(a, b) ==> a[i] == b[i],
        common(a, b) < a.len() && common(a, b) < b.len() ==> a[common(a, b) as int] != b[common(
            a,
            b,
        ) as int],
    decreases a.len(),
{
    if a.len() > 0 && b.len() > 0 && a[0] == b[0] {
        lemma_common(a.drop_first(), b.drop_first());
        assert forall|i: int| 0 <= i < common(a, b) implies a[i] == b[i] by {
            if i > 0 {
                assert(a.drop_first()[i - 1] == b.drop_first()[i - 1]);
            }
        }
    }
}

// mirror: autumn-harvest-agent/src/eval/anon.rs shared_digits
pub fn shared_digits(a: &[u8], b: &[u8]) -> (r: usize)
    ensures
        r == common(a@, b@),
{
    proof {
        assert(a@.subrange(0, a@.len() as int) =~= a@);
        assert(b@.subrange(0, b@.len() as int) =~= b@);
    }
    let mut i = 0;
    while i < a.len() && i < b.len() && a[i] == b[i]
        invariant
            i <= a.len(),
            i <= b.len(),
            forall|j: int| 0 <= j < i ==> a@[j] == b@[j],
            common(a@, b@) == i + common(a@.subrange(i as int, a@.len() as int), b@.subrange(
                i as int,
                b@.len() as int,
            )),
        decreases a.len() - i,
    {
        proof {
            let ra = a@.subrange(i as int, a@.len() as int);
            let rb = b@.subrange(i as int, b@.len() as int);
            assert(ra.drop_first() =~= a@.subrange(i + 1, a@.len() as int));
            assert(rb.drop_first() =~= b@.subrange(i + 1, b@.len() as int));
        }
        i += 1;
    }
    proof {
        let ra = a@.subrange(i as int, a@.len() as int);
        let rb = b@.subrange(i as int, b@.len() as int);
        assert(ra.len() == 0 || rb.len() == 0 || ra[0] != rb[0]);
    }
    i
}

/// `r` is above every shared count below `cap`.
pub open spec fn safe(shared: Seq<usize>, cap: int, r: int) -> bool {
    forall|i: int| 0 <= i < shared.len() && shared[i] < cap ==> shared[i] < r
}

// mirror: autumn-harvest-agent/src/eval/anon.rs id_width
pub fn id_width(shared: &[usize], floor: usize, cap: usize) -> (r: usize)
    requires
        floor <= cap,
    ensures
        floor <= r <= cap,
        safe(shared@, cap as int, r as int),
        r == floor || exists|i: int|
            0 <= i < shared@.len() && #[trigger] shared@[i] < cap && r == shared@[i] + 1,
{
    let mut width = floor;
    let mut i = 0;
    while i < shared.len()
        invariant
            i <= shared.len(),
            floor <= width <= cap,
            safe(shared@.subrange(0, i as int), cap as int, width as int),
            width == floor || exists|j: int|
                0 <= j < i && #[trigger] shared@[j] < cap && width == shared@[j] + 1,
        decreases shared.len() - i,
    {
        if shared[i] >= width && shared[i] < cap {
            width = shared[i] + 1;
        }
        proof {
            assert forall|j: int|
                0 <= j < i + 1 && #[trigger] shared@.subrange(0, i + 1)[j] < cap implies shared@.subrange(
                0,
                i + 1,
            )[j] < width by {
                if j < i {
                    assert(shared@.subrange(0, i + 1)[j] == shared@.subrange(0, i as int)[j]);
                }
            }
        }
        i += 1;
    }
    proof {
        assert(shared@.subrange(0, shared.len() as int) =~= shared@);
    }
    width
}

/// The width is above each shared count below the cap. No smaller width
/// from the floor is: the width is the floor, or one shared count plus one.
pub proof fn theorem_the_width_is_the_least_safe_width(shared: Seq<usize>, floor: int, cap: int, r: int)
    requires
        floor <= r <= cap,
        safe(shared, cap, r),
        r == floor || exists|i: int| 0 <= i < shared.len() && #[trigger] shared[i] < cap && r == shared[i] + 1,
    ensures
        forall|w: int| floor <= w < r ==> !safe(shared, cap, w),
{
    assert forall|w: int| floor <= w < r implies !safe(shared, cap, w) by {
        let i = choose|i: int| 0 <= i < shared.len() && #[trigger] shared[i] < cap && r == shared[i] + 1;
        assert(shared[i] >= w);
    }
}

/// Two keys of at least `width` digits that share fewer than `width`
/// leading digits have different prefixes of that width.
pub proof fn theorem_a_width_above_the_shared_count_splits_a_pair(a: Seq<u8>, b: Seq<u8>, width: int)
    requires
        common(a, b) < width,
        width <= a.len(),
        width <= b.len(),
    ensures
        a.subrange(0, width) != b.subrange(0, width),
{
    lemma_common(a, b);
    let c = common(a, b) as int;
    assert(a.subrange(0, width)[c] == a[c]);
    assert(b.subrange(0, width)[c] == b[c]);
}

} // verus!
