//! The vote kernel of `agent_sample` picks the most common answer key.
//!
//! The engine maps each key text to the index of its first appearance. A
//! vote is then a draw id and an optional key index. A draw with no key
//! casts no vote. The copies below are the kernels in `sampling/vote.rs`
//! of the agent crate.
//!
//! The spec `votes_of` gives the votes of a key, and `first_of` gives the
//! lowest draw id among them. `winner_upto` folds `spec_pick_key` over the
//! keys. The proofs show:
//!
//! - The winner has the most votes. On a tie, it has the lowest first
//!   draw. With no vote, there is no winner.
//! - Let `lead` be the votes of the winner and `second` a bound on the
//!   votes of each other key. When `lead > second + left`, each extension
//!   by at most `left` votes keeps the winner. So an early stop gives the
//!   winner of the full run, for each way the left draws vote. When the
//!   left draws have higher draw ids, the selected first draw stays too.
//! - Two vote sequences with the same multiset give the same winner.
//! - A cast order changes the key numbering too. With one vote per draw, a
//!   renamed and reordered sequence gives the renamed winner, with the
//!   same votes and the same first draw. So the order in which draws end
//!   does not change the result.
//!
//! The engine gives the facts that the theorems require. Its key indices
//! are below the key count. Interning only appends, so a later vote keeps
//! each earlier index. `left` is the planned draws minus the draws that
//! ran, and each later draw adds at most one vote, with a higher draw id.
//! `Tally::cast` drops a second vote of the same draw.
//!
//! The proptests `an_early_stop_never_changes_the_winner` and
//! `the_order_of_the_votes_does_not_change_the_tally` check the engine
//! with key texts.

use vstd::prelude::*;

verus! {

/// The votes of the key `key` in `s`.
pub open spec fn votes_of(s: Seq<(u32, Option<u32>)>, key: u32) -> nat
    decreases s.len(),
{
    if s.len() == 0 {
        0
    } else {
        votes_of(s.drop_last(), key) + if s.last().1 == Some(key) {
            1nat
        } else {
            0nat
        }
    }
}

/// One step of `first`: the lower of `f` and the draw of `e`, when `e`
/// votes for `key`.
pub open spec fn first_step(f: u32, e: (u32, Option<u32>), key: u32) -> u32 {
    if e.1 == Some(key) && e.0 < f {
        e.0
    } else {
        f
    }
}

/// The lowest draw id that votes for `key` in `s`, or `u32::MAX` when no
/// draw votes for it.
pub open spec fn first_of(s: Seq<(u32, Option<u32>)>, key: u32) -> u32
    decreases s.len(),
{
    if s.len() == 0 {
        u32::MAX
    } else {
        first_step(first_of(s.drop_last(), key), s.last(), key)
    }
}

/// `pick_key` as a spec. An entry is a key, its votes and its first draw.
pub open spec fn spec_pick_key(best: Option<(u32, u32, u32)>, next: (u32, u32, u32)) -> Option<
    (u32, u32, u32),
> {
    if next.1 == 0 {
        best
    } else {
        match best {
            None => Some(next),
            Some(b) => if next.1 > b.1 || (next.1 == b.1 && next.2 < b.2) {
                Some(next)
            } else {
                best
            },
        }
    }
}

/// The winner among the keys `0..n`.
pub open spec fn winner_upto(s: Seq<(u32, Option<u32>)>, n: u32) -> Option<(u32, u32, u32)>
    decreases n,
{
    if n == 0 {
        None
    } else {
        let k = (n - 1) as u32;
        spec_pick_key(winner_upto(s, k), (k, votes_of(s, k) as u32, first_of(s, k)))
    }
}

/// Each key in `s` is below `keys`.
pub open spec fn keyed_below(s: Seq<(u32, Option<u32>)>, keys: u32) -> bool {
    forall|i: int|
        0 <= i < s.len() ==> match (#[trigger] s[i]).1 {
            Some(k) => k < keys,
            None => true,
        }
}

/// `w` is at least as good as the key `j`.
pub open spec fn covers(s: Seq<(u32, Option<u32>)>, w: (u32, u32, u32), j: u32) -> bool {
    votes_of(s, j) < w.1 as nat || (votes_of(s, j) == w.1 as nat && w.2 <= first_of(s, j))
}

proof fn lemma_count_le_len(s: Seq<(u32, Option<u32>)>, key: u32)
    ensures
        votes_of(s, key) <= s.len(),
    decreases s.len(),
{
    if s.len() > 0 {
        lemma_count_le_len(s.drop_last(), key);
    }
}

proof fn lemma_count_outside(s: Seq<(u32, Option<u32>)>, keys: u32, key: u32)
    requires
        keyed_below(s, keys),
        key >= keys,
    ensures
        votes_of(s, key) == 0,
    decreases s.len(),
{
    if s.len() > 0 {
        let p = s.drop_last();
        assert(keyed_below(p, keys)) by {
            assert forall|i: int| 0 <= i < p.len() implies match (#[trigger] p[i]).1 {
                Some(k) => k < keys,
                None => true,
            } by {
                assert(p[i] == s[i]);
            }
        }
        lemma_count_outside(p, keys, key);
        assert(s[s.len() - 1] == s.last());
    }
}

proof fn lemma_count_append(s: Seq<(u32, Option<u32>)>, e: Seq<(u32, Option<u32>)>, key: u32)
    ensures
        votes_of(s + e, key) == votes_of(s, key) + votes_of(e, key),
    decreases e.len(),
{
    if e.len() == 0 {
        assert(s + e =~= s);
    } else {
        assert((s + e).drop_last() =~= s + e.drop_last());
        assert((s + e).last() == e.last());
        lemma_count_append(s, e.drop_last(), key);
    }
}

/// The fold is `None` exactly when no key below `n` has a vote. A winner
/// is a key below `n` with its votes and its first draw, and it covers
/// each key below `n`.
proof fn lemma_winner_upto(s: Seq<(u32, Option<u32>)>, n: u32)
    requires
        s.len() <= u32::MAX,
    ensures
        winner_upto(s, n) is None <==> forall|j: u32| j < n ==> #[trigger] votes_of(s, j) == 0,
        winner_upto(s, n) matches Some(w) ==> w.0 < n && w.1 > 0 && w.1 as nat == votes_of(s, w.0)
            && w.2 == first_of(s, w.0),
        winner_upto(s, n) matches Some(w) ==> forall|j: u32| j < n ==> #[trigger] covers(s, w, j),
    decreases n,
{
    if n > 0 {
        let k = (n - 1) as u32;
        lemma_winner_upto(s, k);
        lemma_count_le_len(s, k);
        let p = winner_upto(s, k);
        let r = winner_upto(s, n);
        if r is None {
            assert forall|j: u32| j < n implies #[trigger] votes_of(s, j) == 0 by {
                if j == k {
                    assert(votes_of(s, k) as u32 == 0);
                }
            }
        }
        if forall|j: u32| j < n ==> #[trigger] votes_of(s, j) == 0 {
            assert(votes_of(s, k) == 0);
            assert forall|j: u32| j < k implies #[trigger] votes_of(s, j) == 0 by {
                assert(j < n);
            }
        }
        if r is Some {
            let w = r.unwrap();
            assert forall|j: u32| j < n implies #[trigger] covers(s, w, j) by {
                if j < k {
                    if p is Some {
                        assert(covers(s, p.unwrap(), j));
                    } else {
                        assert(votes_of(s, j) == 0);
                    }
                }
            }
        }
    }
}

/// The winner has the most votes, and the lowest first draw on a tie.
/// There is no winner exactly when no key below `keys` has a vote.
pub proof fn theorem_the_winner_has_the_most_votes(s: Seq<(u32, Option<u32>)>, keys: u32)
    requires
        s.len() <= u32::MAX,
    ensures
        winner_upto(s, keys) is None <==> forall|j: u32| j < keys ==> #[trigger] votes_of(s, j) == 0,
        winner_upto(s, keys) matches Some(w) ==> w.0 < keys && w.1 > 0 && w.1 as nat == votes_of(
            s,
            w.0,
        ) && w.2 == first_of(s, w.0),
        winner_upto(s, keys) matches Some(w) ==> forall|j: u32|
            j < keys ==> votes_of(s, j) <= w.1 as nat && (#[trigger] votes_of(s, j) == w.1 as nat ==> w.2
                <= first_of(s, j)),
{
    lemma_winner_upto(s, keys);
    if winner_upto(s, keys) is Some {
        let w = winner_upto(s, keys).unwrap();
        assert forall|j: u32| j < keys implies votes_of(s, j) <= w.1 as nat && (#[trigger] votes_of(s, j)
            == w.1 as nat ==> w.2 <= first_of(s, j)) by {
            assert(covers(s, w, j));
        }
    }
}

/// An early stop keeps the winner.
///
/// `s` holds the votes so far, and `w` is its winner. Each other key has
/// at most `second` votes, and `w` has more than `second + left`. Then
/// each extension `e` by at most `left` votes keeps `w` as the winner,
/// whatever keys `e` holds, new keys included.
pub proof fn theorem_an_early_stop_keeps_the_winner(
    s: Seq<(u32, Option<u32>)>,
    e: Seq<(u32, Option<u32>)>,
    keys: u32,
    all_keys: u32,
    second: u32,
    left: u32,
)
    requires
        s.len() + e.len() <= u32::MAX,
        keyed_below(s, keys),
        keys <= all_keys,
        winner_upto(s, keys) is Some,
        forall|j: u32|
            j < keys && j != winner_upto(s, keys).unwrap().0 ==> #[trigger] votes_of(s, j) <= second,
        winner_upto(s, keys).unwrap().1 as int > second as int + left as int,
        e.len() <= left,
    ensures
        winner_upto(s + e, all_keys) is Some,
        winner_upto(s + e, all_keys).unwrap().0 == winner_upto(s, keys).unwrap().0,
{
    let w = winner_upto(s, keys).unwrap();
    let t = s + e;
    lemma_winner_upto(s, keys);
    lemma_winner_upto(t, all_keys);
    lemma_count_append(s, e, w.0);
    assert(votes_of(t, w.0) >= w.1 as nat);
    assert(votes_of(t, w.0) > 0);
    assert(w.0 < all_keys);
    let w2 = winner_upto(t, all_keys).unwrap();
    assert(covers(t, w2, w.0));
    if w2.0 != w.0 {
        let j = w2.0;
        lemma_count_append(s, e, j);
        lemma_count_le_len(e, j);
        if j >= keys {
            lemma_count_outside(s, keys, j);
        }
        assert(votes_of(s, j) <= second as nat);
        assert(votes_of(t, j) < votes_of(t, w.0));
    }
}

/// `first_step` keeps the lower draw, so two steps commute.
proof fn lemma_first_steps_commute(f: u32, a: (u32, Option<u32>), b: (u32, Option<u32>), key: u32)
    ensures
        first_step(first_step(f, a, key), b, key) == first_step(first_step(f, b, key), a, key),
{
}

/// Removing the entry `i` takes its vote out of `count` and `first`.
proof fn lemma_remove(s: Seq<(u32, Option<u32>)>, i: int, key: u32)
    requires
        0 <= i < s.len(),
    ensures
        votes_of(s, key) == votes_of(s.remove(i), key) + if s[i].1 == Some(key) {
            1nat
        } else {
            0nat
        },
        first_of(s, key) == first_step(first_of(s.remove(i), key), s[i], key),
    decreases s.len(),
{
    if i == s.len() - 1 {
        assert(s.remove(i) =~= s.drop_last());
    } else {
        let t = s.drop_last();
        lemma_remove(t, i, key);
        assert(s.remove(i).drop_last() =~= t.remove(i));
        assert(s.remove(i).last() == s.last());
        assert(t[i] == s[i]);
        lemma_first_steps_commute(first_of(t.remove(i), key), s[i], s.last(), key);
    }
}

/// Two sequences with the same multiset give the same `count` and the
/// same `first` for each key.
proof fn lemma_permutation(s1: Seq<(u32, Option<u32>)>, s2: Seq<(u32, Option<u32>)>)
    requires
        s1.to_multiset() =~= s2.to_multiset(),
    ensures
        forall|key: u32| #[trigger] votes_of(s1, key) == votes_of(s2, key) && first_of(s1, key) == first_of(
            s2,
            key,
        ),
    decreases s1.len(),
{
    s1.to_multiset_ensures();
    s2.to_multiset_ensures();
    if s1.len() == 0 {
        assert(s2.len() == 0);
    } else {
        let a = s1.last();
        let t1 = s1.drop_last();
        assert(s1 =~= t1.push(a));
        t1.to_multiset_ensures();
        assert(s1.contains(a)) by {
            assert(s1[s1.len() - 1] == a);
        }
        assert(s1.to_multiset().count(a) > 0);
        assert(s2.to_multiset().count(a) > 0);
        assert(s2.contains(a));
        let j = choose|j: int| 0 <= j < s2.len() && s2[j] == a;
        let t2 = s2.remove(j);
        assert(t1.to_multiset() =~= t2.to_multiset());
        lemma_permutation(t1, t2);
        assert forall|key: u32| #[trigger] votes_of(s1, key) == votes_of(s2, key) && first_of(s1, key)
            == first_of(s2, key) by {
            lemma_remove(s2, j, key);
            assert(votes_of(t1, key) == votes_of(t2, key));
            assert(first_of(t1, key) == first_of(t2, key));
            assert(s1.drop_last() == t1);
            assert(s1.last() == a);
        }
    }
}

proof fn lemma_same_tally_same_winner(s1: Seq<(u32, Option<u32>)>, s2: Seq<(u32, Option<u32>)>, n: u32)
    requires
        forall|key: u32| #[trigger] votes_of(s1, key) == votes_of(s2, key) && first_of(s1, key) == first_of(
            s2,
            key,
        ),
    ensures
        winner_upto(s1, n) == winner_upto(s2, n),
    decreases n,
{
    if n > 0 {
        let k = (n - 1) as u32;
        lemma_same_tally_same_winner(s1, s2, k);
        assert(votes_of(s1, k) == votes_of(s2, k));
    }
}

/// Two vote sequences with the same multiset give the same winner. So the
/// order in which draws end does not change the result.
pub proof fn theorem_the_winner_does_not_depend_on_the_order(
    s1: Seq<(u32, Option<u32>)>,
    s2: Seq<(u32, Option<u32>)>,
    keys: u32,
)
    requires
        s1.to_multiset() =~= s2.to_multiset(),
    ensures
        winner_upto(s1, keys) == winner_upto(s2, keys),
{
    lemma_permutation(s1, s2);
    lemma_same_tally_same_winner(s1, s2, keys);
}

/// A key with no vote has no first draw.
proof fn lemma_no_vote_no_first(s: Seq<(u32, Option<u32>)>, key: u32)
    requires
        votes_of(s, key) == 0,
    ensures
        first_of(s, key) == u32::MAX,
    decreases s.len(),
{
    if s.len() > 0 {
        lemma_no_vote_no_first(s.drop_last(), key);
    }
}

/// The first draw of a key with a vote is the draw of an entry with that
/// key.
proof fn lemma_first_is_a_draw(s: Seq<(u32, Option<u32>)>, key: u32)
    requires
        votes_of(s, key) > 0,
    ensures
        exists|i: int| 0 <= i < s.len() && s[i] == (first_of(s, key), Some(key)),
    decreases s.len(),
{
    let p = s.drop_last();
    let last = s.last();
    assert(s[s.len() - 1] == last);
    if last.1 == Some(key) && last.0 < first_of(p, key) {
        assert(s[s.len() - 1] == (first_of(s, key), Some(key)));
    } else if votes_of(p, key) > 0 {
        lemma_first_is_a_draw(p, key);
        let i = choose|i: int| 0 <= i < p.len() && p[i] == (first_of(p, key), Some(key));
        assert(s[i] == p[i]);
    } else {
        lemma_no_vote_no_first(p, key);
        assert(last.1 == Some(key));
        assert(last.0 == u32::MAX);
        assert(s[s.len() - 1] == (first_of(s, key), Some(key)));
    }
}

/// Each draw of `e` comes after each draw of `s`.
pub open spec fn later(s: Seq<(u32, Option<u32>)>, e: Seq<(u32, Option<u32>)>) -> bool {
    forall|i: int, j: int| 0 <= i < s.len() && 0 <= j < e.len() ==> #[trigger] s[i].0 < #[trigger] e[j].0
}

/// Later draws do not move the first draw of a key that has a vote.
proof fn lemma_first_keeps_under_later_draws(
    s: Seq<(u32, Option<u32>)>,
    e: Seq<(u32, Option<u32>)>,
    key: u32,
)
    requires
        votes_of(s, key) > 0,
        later(s, e),
    ensures
        first_of(s + e, key) == first_of(s, key),
    decreases e.len(),
{
    if e.len() == 0 {
        assert(s + e =~= s);
    } else {
        let d = e.drop_last();
        assert(later(s, d)) by {
            assert forall|i: int, j: int| 0 <= i < s.len() && 0 <= j < d.len() implies #[trigger] s[i].0
                < #[trigger] d[j].0 by {
                assert(d[j] == e[j]);
            }
        }
        lemma_first_keeps_under_later_draws(s, d, key);
        assert((s + e).drop_last() =~= s + d);
        assert((s + e).last() == e.last());
        lemma_first_is_a_draw(s, key);
        let i = choose|i: int| 0 <= i < s.len() && s[i] == (first_of(s, key), Some(key));
        assert(e[e.len() - 1] == e.last());
        assert(s[i].0 < e[e.len() - 1].0);
    }
}

/// An early stop also keeps the selected draw: the first draw of the
/// winning key. The draws after the stop have higher draw ids, so none of
/// them is first.
pub proof fn theorem_an_early_stop_keeps_the_selected_draw(
    s: Seq<(u32, Option<u32>)>,
    e: Seq<(u32, Option<u32>)>,
    keys: u32,
    all_keys: u32,
    second: u32,
    left: u32,
)
    requires
        s.len() + e.len() <= u32::MAX,
        keyed_below(s, keys),
        keys <= all_keys,
        winner_upto(s, keys) is Some,
        forall|j: u32|
            j < keys && j != winner_upto(s, keys).unwrap().0 ==> #[trigger] votes_of(s, j) <= second,
        winner_upto(s, keys).unwrap().1 as int > second as int + left as int,
        e.len() <= left,
        later(s, e),
    ensures
        winner_upto(s + e, all_keys) is Some,
        winner_upto(s + e, all_keys).unwrap().0 == winner_upto(s, keys).unwrap().0,
        winner_upto(s + e, all_keys).unwrap().2 == winner_upto(s, keys).unwrap().2,
{
    theorem_an_early_stop_keeps_the_winner(s, e, keys, all_keys, second, left);
    let w = winner_upto(s, keys).unwrap();
    lemma_winner_upto(s, keys);
    lemma_winner_upto(s + e, all_keys);
    lemma_first_keeps_under_later_draws(s, e, w.0);
}

/// No two entries of `s` share a draw id. Each draw votes once.
pub open spec fn distinct_draws(s: Seq<(u32, Option<u32>)>) -> bool {
    forall|i: int, j: int|
        0 <= i < s.len() && 0 <= j < s.len() && i != j ==> #[trigger] s[i].0 != #[trigger] s[j].0
}

/// `s` with each key index `k` renamed to `f(k)`.
pub open spec fn relabel(s: Seq<(u32, Option<u32>)>, f: spec_fn(u32) -> u32) -> Seq<
    (u32, Option<u32>),
> {
    s.map_values(
        |e: (u32, Option<u32>)|
            (
                e.0,
                match e.1 {
                    Some(k) => Some(f(k)),
                    None => None,
                },
            ),
    )
}

/// `f` maps the keys below `keys` to distinct keys below `all_keys`.
pub open spec fn renames(f: spec_fn(u32) -> u32, keys: u32, all_keys: u32) -> bool {
    &&& forall|a: u32| a < keys ==> #[trigger] f(a) < all_keys
    &&& forall|a: u32, b: u32| a < keys && b < keys && a != b ==> #[trigger] f(a) != #[trigger] f(b)
}

/// A rename keeps the votes and the first draw of each key, and gives no
/// vote to a key outside its image.
proof fn lemma_relabel(s: Seq<(u32, Option<u32>)>, f: spec_fn(u32) -> u32, keys: u32, all_keys: u32)
    requires
        keyed_below(s, keys),
        renames(f, keys, all_keys),
    ensures
        forall|a: u32|
            a < keys ==> #[trigger] votes_of(relabel(s, f), f(a)) == votes_of(s, a) && first_of(
                relabel(s, f),
                f(a),
            ) == first_of(s, a),
        forall|k: u32|
            (forall|a: u32| a < keys ==> #[trigger] f(a) != k) ==> #[trigger] votes_of(
                relabel(s, f),
                k,
            ) == 0,
    decreases s.len(),
{
    if s.len() == 0 {
        assert(relabel(s, f).len() == 0);
    } else {
        let p = s.drop_last();
        assert(keyed_below(p, keys)) by {
            assert forall|i: int| 0 <= i < p.len() implies match (#[trigger] p[i]).1 {
                Some(k) => k < keys,
                None => true,
            } by {
                assert(p[i] == s[i]);
            }
        }
        lemma_relabel(p, f, keys, all_keys);
        let r = relabel(s, f);
        let last = s.last();
        assert(r.drop_last() =~= relabel(p, f));
        assert(s[s.len() - 1] == last);
        assert(r.last() == r[s.len() - 1]);
        assert(r.last() == (
            last.0,
            match last.1 {
                Some(k) => Some(f(k)),
                None => None,
            },
        ));
        assert forall|a: u32| a < keys implies #[trigger] votes_of(r, f(a)) == votes_of(s, a)
            && first_of(r, f(a)) == first_of(s, a) by {
            assert(votes_of(relabel(p, f), f(a)) == votes_of(p, a));
            assert(first_of(relabel(p, f), f(a)) == first_of(p, a));
            assert(votes_of(r, f(a)) == votes_of(r.drop_last(), f(a)) + if r.last().1 == Some(f(a)) {
                1nat
            } else {
                0nat
            });
            assert(first_of(r, f(a)) == first_step(first_of(r.drop_last(), f(a)), r.last(), f(a)));
            match last.1 {
                Some(b) => {
                    assert(b < keys);
                    if b != a {
                        assert(f(b) != f(a));
                    }
                },
                None => {},
            }
        }
        assert forall|k: u32| (forall|a: u32| a < keys ==> #[trigger] f(a) != k) implies #[trigger] votes_of(
            r,
            k,
        ) == 0 by {
            assert(votes_of(relabel(p, f), k) == 0);
            match last.1 {
                Some(b) => {
                    assert(b < keys);
                    assert(f(b) != k);
                },
                None => {},
            }
        }
    }
}

/// Two orders of casting give the same winning key.
///
/// The engine numbers each key by its first appearance. So a different
/// cast order gives another numbering and another order of the same
/// votes. Here `t` holds the votes of `s` renamed by `f`, in any order.
/// With one vote per draw, the winner of `t` is the rename of the winner
/// of `s`, with the same votes and the same first draw.
pub proof fn theorem_a_cast_order_does_not_change_the_winner(
    s: Seq<(u32, Option<u32>)>,
    t: Seq<(u32, Option<u32>)>,
    f: spec_fn(u32) -> u32,
    keys: u32,
    all_keys: u32,
)
    requires
        s.len() <= u32::MAX,
        distinct_draws(s),
        keyed_below(s, keys),
        renames(f, keys, all_keys),
        t.to_multiset() =~= relabel(s, f).to_multiset(),
    ensures
        winner_upto(t, all_keys) is Some <==> winner_upto(s, keys) is Some,
        winner_upto(s, keys) is Some ==> winner_upto(t, all_keys) == Some(
            (
                f(winner_upto(s, keys).unwrap().0),
                winner_upto(s, keys).unwrap().1,
                winner_upto(s, keys).unwrap().2,
            ),
        ),
{
    let r = relabel(s, f);
    assert(r.len() == s.len());
    theorem_the_winner_does_not_depend_on_the_order(t, r, all_keys);
    lemma_relabel(s, f, keys, all_keys);
    lemma_winner_upto(s, keys);
    lemma_winner_upto(r, all_keys);
    if winner_upto(s, keys) is Some {
        let w = winner_upto(s, keys).unwrap();
        assert(votes_of(r, f(w.0)) == votes_of(s, w.0));
        assert(f(w.0) < all_keys);
        let w2 = winner_upto(r, all_keys).unwrap();
        let k2 = w2.0;
        if forall|a: u32| a < keys ==> #[trigger] f(a) != k2 {
            assert(votes_of(r, k2) == 0);
        }
        let b = choose|b: u32| b < keys && #[trigger] f(b) == k2;
        assert(votes_of(r, f(b)) == votes_of(s, b));
        assert(first_of(r, f(b)) == first_of(s, b));
        assert(w2.1 as nat == votes_of(s, b));
        assert(w2.2 == first_of(s, b));
        assert(covers(s, w, b));
        assert(covers(r, w2, f(w.0)));
        assert(first_of(r, f(w.0)) == first_of(s, w.0));
        if b != w.0 {
            assert(votes_of(s, b) == votes_of(s, w.0));
            assert(first_of(s, b) == first_of(s, w.0));
            lemma_first_is_a_draw(s, b);
            lemma_first_is_a_draw(s, w.0);
            let i = choose|i: int| 0 <= i < s.len() && s[i] == (first_of(s, b), Some(b));
            let j = choose|j: int| 0 <= j < s.len() && s[j] == (first_of(s, w.0), Some(w.0));
            assert(i != j);
            assert(s[i].0 == s[j].0);
        }
        assert(b == w.0);
        assert(k2 == f(w.0));
        assert(w2 == (f(w.0), w.1, w.2));
    } else {
        assert forall|k: u32| k < all_keys implies #[trigger] votes_of(r, k) == 0 by {
            if exists|a: u32| a < keys && #[trigger] f(a) == k {
                let a = choose|a: u32| a < keys && #[trigger] f(a) == k;
                assert(votes_of(s, a) == 0);
            }
        }
    }
    if winner_upto(r, all_keys) is Some && winner_upto(s, keys) is None {
        let w2 = winner_upto(r, all_keys).unwrap();
        assert(votes_of(r, w2.0) == 0);
    }
}

// mirror: autumn-harvest-agent/src/sampling/vote.rs key_votes
pub fn key_votes(votes: &[(u32, Option<u32>)], key: u32) -> (r: (u32, u32))
    requires
        votes@.len() <= u32::MAX,
    ensures
        r.0 as nat == votes_of(votes@, key),
        r.1 == first_of(votes@, key),
{
    let mut count = 0;
    let mut first = u32::MAX;
    let mut i = 0;
    while i < votes.len()
        invariant
            i <= votes.len(),
            votes@.len() <= u32::MAX,
            count as nat == votes_of(votes@.subrange(0, i as int), key),
            count <= i,
            first == first_of(votes@.subrange(0, i as int), key),
        decreases votes.len() - i,
    {
        proof {
            assert(votes@.subrange(0, i + 1).drop_last() =~= votes@.subrange(0, i as int));
            assert(votes@.subrange(0, i + 1).last() == votes@[i as int]);
        }
        let hit = if let Some(k) = votes[i].1 { k == key } else { false };
        if hit {
            count += 1;
            if votes[i].0 < first {
                first = votes[i].0;
            }
        }
        i += 1;
    }
    proof {
        assert(votes@.subrange(0, votes.len() as int) =~= votes@);
    }
    (count, first)
}

// mirror: autumn-harvest-agent/src/sampling/vote.rs pick_key
pub fn pick_key(best: Option<(u32, u32, u32)>, next: (u32, u32, u32)) -> (r: Option<(u32, u32, u32)>)
    ensures
        r == spec_pick_key(best, next),
{
    if next.1 == 0 {
        return best;
    }
    match best {
        None => Some(next),
        Some(b) => {
            if next.1 > b.1 || (next.1 == b.1 && next.2 < b.2) {
                Some(next)
            } else {
                best
            }
        }
    }
}

// mirror: autumn-harvest-agent/src/sampling/vote.rs vote_winner
pub fn vote_winner(votes: &[(u32, Option<u32>)], keys: u32) -> (r: Option<(u32, u32, u32)>)
    requires
        votes@.len() <= u32::MAX,
    ensures
        r == winner_upto(votes@, keys),
{
    let mut best = None;
    let mut key = 0;
    while key < keys
        invariant
            key <= keys,
            votes@.len() <= u32::MAX,
            best == winner_upto(votes@, key),
        decreases keys - key,
    {
        let (count, first) = key_votes(votes, key);
        best = pick_key(best, (key, count, first));
        key += 1;
    }
    best
}

// mirror: autumn-harvest-agent/src/sampling/vote.rs runner_up
pub fn runner_up(votes: &[(u32, Option<u32>)], keys: u32, winner: u32) -> (r: u32)
    requires
        votes@.len() <= u32::MAX,
    ensures
        forall|j: u32| j < keys && j != winner ==> #[trigger] votes_of(votes@, j) <= r as nat,
        r == 0 || exists|j: u32| j < keys && j != winner && #[trigger] votes_of(votes@, j) == r as nat,
{
    let mut second = 0;
    let mut key = 0;
    while key < keys
        invariant
            key <= keys,
            votes@.len() <= u32::MAX,
            forall|j: u32| j < key && j != winner ==> #[trigger] votes_of(votes@, j) <= second as nat,
            second == 0 || exists|j: u32|
                j < key && j != winner && #[trigger] votes_of(votes@, j) == second as nat,
        decreases keys - key,
    {
        if key != winner {
            let (count, _first) = key_votes(votes, key);
            if count > second {
                second = count;
            }
        }
        key += 1;
    }
    second
}

// mirror: autumn-harvest-agent/src/sampling/vote.rs vote_settled
pub fn vote_settled(lead: u32, second: u32, left: u32) -> (r: bool)
    ensures
        r == (lead as int > second as int + left as int),
{
    lead > second && lead - second > left
}

} // verus!
