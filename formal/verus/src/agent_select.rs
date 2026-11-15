//! The selection kernel picks the best scored draw (issue #2441).
//!
//! `score::select` in the agent crate maps each verdict to an entry. An
//! entry is a draw id and an order key. A reject has no key. The copy
//! `key_of_bits` makes the key, and a theorem shows that the key keeps the
//! order of the bits. The copies below are `pick` and `best_draw`. The spec
//! `chosen` folds `pick` over a sequence. The theorems state three
//! properties. The winner has the top key. A reject never wins while a key
//! exists. The result depends only on the set of entries, so no order of the
//! input changes it. The proptest `select_does_not_depend_on_the_order`
//! checks the engine with floats.

use vstd::prelude::*;

verus! {

/// The order key of the bits of an `f64`.
pub open spec fn key(bits: i64) -> i64 {
    bits ^ ((((bits >> 63i64) as u64) >> 1u64) as i64)
}

/// The sign-magnitude order of the bits of two `f64` values. IEEE 754
/// orders finite values in this order, except `-0.0`, whose bits are
/// `i64::MIN`.
pub open spec fn bits_below(a: i64, b: i64) -> bool {
    if a < 0 && b >= 0 {
        true
    } else if a >= 0 && b >= 0 {
        a < b
    } else if a < 0 && b < 0 {
        (a & 0x7fff_ffff_ffff_ffffi64) > (b & 0x7fff_ffff_ffff_ffffi64)
    } else {
        false
    }
}

// mirror: autumn-harvest-agent/src/score.rs key_of_bits
pub fn key_of_bits(bits: i64) -> (r: i64)
    ensures
        r == key(bits),
{
    bits ^ ((((bits >> 63i64) as u64) >> 1u64) as i64)
}

/// The key keeps the sign-magnitude order of the bits, and two bit
/// patterns share a key only when they are equal. `FiniteF64` never holds
/// `-0.0`, so its bits are never `i64::MIN`.
pub proof fn theorem_the_order_key_keeps_the_bit_order(a: i64, b: i64)
    by (bit_vector)
    requires
        a != i64::MIN,
        b != i64::MIN,
    ensures
        bits_below(a, b) <==> key(a) < key(b),
        a == b <==> key(a) == key(b),
{
}

/// `pick` as a spec.
pub open spec fn spec_pick(best: Option<(u32, i64)>, next: (u32, Option<i64>)) -> Option<(u32, i64)> {
    match next.1 {
        None => best,
        Some(key) => match best {
            None => Some((next.0, key)),
            Some(b) => if key > b.1 || (key == b.1 && next.0 < b.0) {
                Some((next.0, key))
            } else {
                best
            },
        },
    }
}

/// The kernel result over a whole sequence.
pub open spec fn chosen(s: Seq<(u32, Option<i64>)>) -> Option<(u32, i64)>
    decreases s.len(),
{
    if s.len() == 0 {
        None
    } else {
        spec_pick(chosen(s.drop_last()), s.last())
    }
}

/// `w` is at least as good as the entry `e`. A reject is below every key.
pub open spec fn covers(w: (u32, i64), e: (u32, Option<i64>)) -> bool {
    match e.1 {
        None => true,
        Some(k) => k < w.1 || (k == w.1 && w.0 <= e.0),
    }
}

// mirror: autumn-harvest-agent/src/score.rs pick
pub fn pick(best: Option<(u32, i64)>, next: (u32, Option<i64>)) -> (r: Option<(u32, i64)>)
    ensures
        r == spec_pick(best, next),
{
    match next.1 {
        None => best,
        Some(key) => match best {
            None => Some((next.0, key)),
            Some(b) => {
                if key > b.1 || (key == b.1 && next.0 < b.0) {
                    Some((next.0, key))
                } else {
                    best
                }
            }
        },
    }
}

// mirror: autumn-harvest-agent/src/score.rs best_draw
pub fn best_draw(draws: &[(u32, Option<i64>)]) -> (r: Option<(u32, i64)>)
    ensures
        r == chosen(draws@),
{
    let mut best = None;
    let mut i = 0;
    while i < draws.len()
        invariant
            i <= draws.len(),
            best == chosen(draws@.subrange(0, i as int)),
        decreases draws.len() - i,
    {
        proof {
            assert(draws@.subrange(0, i + 1).drop_last() =~= draws@.subrange(0, i as int));
        }
        best = pick(best, draws[i]);
        i += 1;
    }
    proof {
        assert(draws@.subrange(0, draws.len() as int) =~= draws@);
    }
    best
}

/// The fold result is `None` exactly when no entry has a key. A result is an
/// entry of the sequence, and it covers every entry.
proof fn lemma_chosen(s: Seq<(u32, Option<i64>)>)
    ensures
        chosen(s) is None <==> forall|i: int| 0 <= i < s.len() ==> (#[trigger] s[i]).1 is None,
        chosen(s) is Some ==> s.contains((chosen(s).unwrap().0, Some(chosen(s).unwrap().1))),
        chosen(s) is Some ==> forall|i: int|
            0 <= i < s.len() ==> covers(chosen(s).unwrap(), #[trigger] s[i]),
    decreases s.len(),
{
    if s.len() > 0 {
        let p = s.drop_last();
        lemma_chosen(p);
        let last = s.last();
        assert forall|i: int| 0 <= i < p.len() implies #[trigger] p[i] == s[i] by {}
        let r = chosen(s);
        if r is Some {
            let w = r.unwrap();
            if chosen(p) is Some && r == chosen(p) {
                let j = choose|j: int| 0 <= j < p.len() && p[j] == (w.0, Some(w.1));
                assert(s[j] == (w.0, Some(w.1)));
            } else {
                assert(s[s.len() - 1] == (w.0, Some(w.1)));
            }
            assert forall|i: int| 0 <= i < s.len() implies covers(w, #[trigger] s[i]) by {
                if i < s.len() - 1 {
                    assert(s[i] == p[i]);
                    if chosen(p) is Some {
                        assert(covers(chosen(p).unwrap(), p[i]));
                    }
                }
            }
        }
        if r is None {
            assert(chosen(p) is None && last.1 is None);
            assert forall|i: int| 0 <= i < s.len() implies (#[trigger] s[i]).1 is None by {
                if i < s.len() - 1 {
                    assert(s[i] == p[i]);
                }
            }
        }
        if forall|i: int| 0 <= i < s.len() ==> (#[trigger] s[i]).1 is None {
            assert(last.1 is None);
            assert forall|i: int| 0 <= i < p.len() implies (#[trigger] p[i]).1 is None by {
                assert(s[i] == p[i]);
            }
        }
    }
}

/// The winner has a key at least as high as each other key. On a tie, the
/// winner has the lowest draw id. The winner is an entry of the input.
pub proof fn theorem_the_winner_has_the_top_score(s: Seq<(u32, Option<i64>)>)
    ensures
        chosen(s) is Some ==> s.contains((chosen(s).unwrap().0, Some(chosen(s).unwrap().1))),
        chosen(s) is Some ==> forall|i: int|
            0 <= i < s.len() && (#[trigger] s[i]).1 is Some ==> s[i].1.unwrap() <= chosen(
                s,
            ).unwrap().1,
        chosen(s) is Some ==> forall|i: int|
            0 <= i < s.len() && (#[trigger] s[i]).1 == Some(chosen(s).unwrap().1) ==> chosen(
                s,
            ).unwrap().0 <= s[i].0,
{
    lemma_chosen(s);
    if chosen(s) is Some {
        let w = chosen(s).unwrap();
        assert forall|i: int| 0 <= i < s.len() && (#[trigger] s[i]).1 is Some implies s[i].1.unwrap()
            <= w.1 by {
            assert(covers(w, s[i]));
        }
        assert forall|i: int| 0 <= i < s.len() && (#[trigger] s[i]).1 == Some(w.1) implies w.0
            <= s[i].0 by {
            assert(covers(w, s[i]));
        }
    }
}

/// A reject never wins while a key exists. The result is `None` exactly
/// when every entry is a reject, an empty input included.
pub proof fn theorem_a_reject_never_wins_while_a_score_exists(s: Seq<(u32, Option<i64>)>)
    ensures
        chosen(s) is None <==> forall|i: int| 0 <= i < s.len() ==> (#[trigger] s[i]).1 is None,
        (exists|i: int| 0 <= i < s.len() && (#[trigger] s[i]).1 is Some) ==> chosen(s) is Some,
        chosen(s) is Some ==> s.contains((chosen(s).unwrap().0, Some(chosen(s).unwrap().1))),
        s.len() == 0 ==> chosen(s) is None,
{
    lemma_chosen(s);
}

/// Two inputs with the same entries give the same result. The order and the
/// count of each entry do not matter.
pub proof fn theorem_the_result_does_not_depend_on_the_order(
    s1: Seq<(u32, Option<i64>)>,
    s2: Seq<(u32, Option<i64>)>,
)
    requires
        forall|e: (u32, Option<i64>)| s1.contains(e) <==> s2.contains(e),
    ensures
        chosen(s1) == chosen(s2),
{
    lemma_chosen(s1);
    lemma_chosen(s2);
    if chosen(s1) is Some {
        let w1 = chosen(s1).unwrap();
        assert(s2.contains((w1.0, Some(w1.1))));
        let j = choose|j: int| 0 <= j < s2.len() && s2[j] == (w1.0, Some(w1.1));
        assert(s2[j].1 is Some);
        let w2 = chosen(s2).unwrap();
        assert(covers(w2, s2[j]));
        assert(s1.contains((w2.0, Some(w2.1))));
        let k = choose|k: int| 0 <= k < s1.len() && s1[k] == (w2.0, Some(w2.1));
        assert(covers(w1, s1[k]));
    } else if chosen(s2) is Some {
        let w2 = chosen(s2).unwrap();
        assert(s1.contains((w2.0, Some(w2.1))));
        let k = choose|k: int| 0 <= k < s1.len() && s1[k] == (w2.0, Some(w2.1));
        assert(s1[k].1 is Some);
    }
}

/// A permutation of the input gives the same result.
pub proof fn theorem_a_permutation_gives_the_same_result(
    s1: Seq<(u32, Option<i64>)>,
    s2: Seq<(u32, Option<i64>)>,
)
    requires
        s1.to_multiset() =~= s2.to_multiset(),
    ensures
        chosen(s1) == chosen(s2),
{
    s1.to_multiset_ensures();
    s2.to_multiset_ensures();
    assert forall|e: (u32, Option<i64>)| s1.contains(e) <==> s2.contains(e) by {
        assert(s1.to_multiset().count(e) == s2.to_multiset().count(e));
    }
    theorem_the_result_does_not_depend_on_the_order(s1, s2);
}

} // verus!
