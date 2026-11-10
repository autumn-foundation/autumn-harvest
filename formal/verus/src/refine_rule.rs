//! The rule pick of a refine run.
//!
//! `refine/rule.rs` in the agent crate orders the probed candidates of a
//! round. The copies below are `rule_key`, `rule_step` and `rule_best`. A
//! live failure has no key. The spec `ruled` folds `rule_step` over a
//! sequence. The theorems state four properties. A completed end comes
//! before an end that did not complete, and fewer stubbed calls come
//! first. A live failure never wins. The result is `None` exactly when
//! each entry is a live failure. The winner has the lowest key, and the
//! lowest index on a tie. The proptest `the_rule_picks_the_lowest_key`
//! checks the engine against a model.

use vstd::prelude::*;

verus! {

/// The key of one candidate. `None` keeps it out of the pick.
pub open spec fn spec_key(live_failed: bool, completed: bool, stubbed: u32) -> Option<u64> {
    if live_failed {
        None
    } else if completed {
        Some(stubbed as u64)
    } else {
        Some((0x1_0000_0000u64 + stubbed as u64) as u64)
    }
}

// mirror: autumn-harvest-agent/src/refine/rule.rs rule_key
pub fn rule_key(live_failed: bool, completed: bool, stubbed: u32) -> (r: Option<u64>)
    ensures
        r == spec_key(live_failed, completed, stubbed),
{
    if live_failed {
        None
    } else if completed {
        Some(stubbed as u64)
    } else {
        Some(0x1_0000_0000 + stubbed as u64)
    }
}

/// A completed end comes before each end that did not complete, for any
/// stubbed counts. With the same end, fewer stubbed calls come first. A
/// live failure has no key.
pub proof fn theorem_a_completed_end_comes_first(a: u32, b: u32, end: bool)
    ensures
        spec_key(false, true, a).unwrap() < spec_key(false, false, b).unwrap(),
        a < b ==> spec_key(false, end, a).unwrap() < spec_key(false, end, b).unwrap(),
        spec_key(false, end, a) is Some,
        spec_key(true, end, a) is None,
{
}

/// `rule_step` as a spec.
pub open spec fn spec_step(best: Option<(u32, u64)>, next: (u32, Option<u64>)) -> Option<(u32, u64)> {
    match next.1 {
        None => best,
        Some(key) => match best {
            None => Some((next.0, key)),
            Some(b) => if key < b.1 || (key == b.1 && next.0 < b.0) {
                Some((next.0, key))
            } else {
                best
            },
        },
    }
}

/// The rule result over a whole sequence.
pub open spec fn ruled(s: Seq<(u32, Option<u64>)>) -> Option<(u32, u64)>
    decreases s.len(),
{
    if s.len() == 0 {
        None
    } else {
        spec_step(ruled(s.drop_last()), s.last())
    }
}

/// `w` is at least as good as the entry `e`. A live failure is below every
/// key.
pub open spec fn beats(w: (u32, u64), e: (u32, Option<u64>)) -> bool {
    match e.1 {
        None => true,
        Some(k) => w.1 < k || (k == w.1 && w.0 <= e.0),
    }
}

// mirror: autumn-harvest-agent/src/refine/rule.rs rule_step
pub fn rule_step(best: Option<(u32, u64)>, next: (u32, Option<u64>)) -> (r: Option<(u32, u64)>)
    ensures
        r == spec_step(best, next),
{
    match next.1 {
        None => best,
        Some(key) => match best {
            None => Some((next.0, key)),
            Some(b) => {
                if key < b.1 || (key == b.1 && next.0 < b.0) {
                    Some((next.0, key))
                } else {
                    best
                }
            }
        },
    }
}

// mirror: autumn-harvest-agent/src/refine/rule.rs rule_best
pub fn rule_best(entries: &[(u32, Option<u64>)]) -> (r: Option<(u32, u64)>)
    ensures
        r == ruled(entries@),
{
    let mut best = None;
    let mut i = 0;
    while i < entries.len()
        invariant
            i <= entries.len(),
            best == ruled(entries@.subrange(0, i as int)),
        decreases entries.len() - i,
    {
        proof {
            assert(entries@.subrange(0, i + 1).drop_last() =~= entries@.subrange(0, i as int));
        }
        best = rule_step(best, entries[i]);
        i += 1;
    }
    proof {
        assert(entries@.subrange(0, entries.len() as int) =~= entries@);
    }
    best
}

/// The fold result is `None` exactly when no entry has a key. A result is
/// an entry of the sequence, and it beats every entry.
proof fn lemma_ruled(s: Seq<(u32, Option<u64>)>)
    ensures
        ruled(s) is None <==> forall|i: int| 0 <= i < s.len() ==> (#[trigger] s[i]).1 is None,
        ruled(s) is Some ==> s.contains((ruled(s).unwrap().0, Some(ruled(s).unwrap().1))),
        ruled(s) is Some ==> forall|i: int|
            0 <= i < s.len() ==> beats(ruled(s).unwrap(), #[trigger] s[i]),
    decreases s.len(),
{
    if s.len() > 0 {
        let p = s.drop_last();
        lemma_ruled(p);
        let last = s.last();
        assert forall|i: int| 0 <= i < p.len() implies #[trigger] p[i] == s[i] by {}
        let r = ruled(s);
        if r is Some {
            let w = r.unwrap();
            if ruled(p) is Some && r == ruled(p) {
                let j = choose|j: int| 0 <= j < p.len() && p[j] == (w.0, Some(w.1));
                assert(s[j] == (w.0, Some(w.1)));
            } else {
                assert(s[s.len() - 1] == (w.0, Some(w.1)));
            }
            assert forall|i: int| 0 <= i < s.len() implies beats(w, #[trigger] s[i]) by {
                if i < s.len() - 1 {
                    assert(s[i] == p[i]);
                    if ruled(p) is Some {
                        assert(beats(ruled(p).unwrap(), p[i]));
                    }
                }
            }
        }
        if r is None {
            assert(ruled(p) is None && last.1 is None);
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

/// A live failure never wins. The result is `None` exactly when each entry
/// is a live failure, an empty input included. A key in the input gives a
/// result, and the result is an entry with a key.
pub proof fn theorem_a_live_failure_never_wins(s: Seq<(u32, Option<u64>)>)
    ensures
        ruled(s) is None <==> forall|i: int| 0 <= i < s.len() ==> (#[trigger] s[i]).1 is None,
        (exists|i: int| 0 <= i < s.len() && (#[trigger] s[i]).1 is Some) ==> ruled(s) is Some,
        ruled(s) is Some ==> s.contains((ruled(s).unwrap().0, Some(ruled(s).unwrap().1))),
        s.len() == 0 ==> ruled(s) is None,
{
    lemma_ruled(s);
}

/// The winner has a key at most each other key. On a tie, the winner has
/// the lowest index.
pub proof fn theorem_the_winner_has_the_lowest_key(s: Seq<(u32, Option<u64>)>)
    ensures
        ruled(s) is Some ==> forall|i: int|
            0 <= i < s.len() && (#[trigger] s[i]).1 is Some ==> ruled(s).unwrap().1 <= s[i].1.unwrap(),
        ruled(s) is Some ==> forall|i: int|
            0 <= i < s.len() && (#[trigger] s[i]).1 == Some(ruled(s).unwrap().1) ==> ruled(
                s,
            ).unwrap().0 <= s[i].0,
{
    lemma_ruled(s);
    if ruled(s) is Some {
        let w = ruled(s).unwrap();
        assert forall|i: int| 0 <= i < s.len() && (#[trigger] s[i]).1 is Some implies w.1
            <= s[i].1.unwrap() by {
            assert(beats(w, s[i]));
        }
        assert forall|i: int| 0 <= i < s.len() && (#[trigger] s[i]).1 == Some(w.1) implies w.0
            <= s[i].0 by {
            assert(beats(w, s[i]));
        }
    }
}

} // verus!
