//! The skip links of the activity scan index find the first live entry
//! (issue #2330).
//!
//! The activity terminal scan asks for the first unconsumed position in a
//! sorted list. `SkipList::first_unconsumed` answers through `first_live`.
//! Each entry holds a link to itself or to a later entry. Every entry that
//! a link jumps over is dead. A dead entry is a consumed event, and it never comes back
//! to life.
//!
//! The copy below proves that `first_live` returns the first live entry at
//! or after its start, or the end. It also proves that each call keeps the
//! links sound. The theorems prove that a new list is sound, that an added
//! entry keeps it sound, and that more dead entries keep it sound. So the
//! links stay sound for the life of the index.
//!
//! The query calls `is_dead` only on a real entry, never on the end slot. So
//! the engine closure never reads past its list.

use vstd::prelude::*;

verus! {

/// The link of entry `k` jumps over dead entries only.
pub open spec fn skips(next: Seq<usize>, dead: Set<int>, k: int) -> bool {
    forall|m: int| k <= m < next[k] ==> #[trigger] dead.contains(m)
}

/// The links are sound for `dead`. The last slot is the end, and it links
/// to itself. Each link points at its own entry or a later one.
pub open spec fn links(next: Seq<usize>, dead: Set<int>) -> bool {
    &&& next.len() > 0
    &&& next[next.len() - 1] == next.len() - 1
    &&& forall|k: int| 0 <= k < next.len() ==> k <= #[trigger] next[k] < next.len()
    &&& forall|k: int| 0 <= k < next.len() ==> #[trigger] skips(next, dead, k)
}

/// A path halving step keeps the links sound. The new link of `i` jumps
/// over two old links, and both of them jump over dead entries only.
proof fn lemma_a_halving_keeps_the_links(next: Seq<usize>, dead: Set<int>, i: int)
    requires
        links(next, dead),
        0 <= i < next.len(),
    ensures
        links(next.update(i, next[next[i] as int]), dead),
        next[next[i] as int] >= next[i] >= i,
        forall|m: int| i <= m < next[next[i] as int] ==> #[trigger] dead.contains(m),
{
    let j = next[i] as int;
    let after = next.update(i, next[j]);
    assert(skips(next, dead, i));
    assert(skips(next, dead, j));
    assert forall|k: int| 0 <= k < after.len() implies k <= #[trigger] after[k] < after.len() by {
        if k == i {
            assert(j <= next[j]);
        } else {
            assert(k <= next[k]);
        }
    }
    assert forall|m: int| i <= m < next[j] implies #[trigger] dead.contains(m) by {
        if m < j {
            assert(skips(next, dead, i));
        } else {
            assert(skips(next, dead, j));
        }
    }
    assert forall|k: int| 0 <= k < after.len() implies #[trigger] skips(after, dead, k) by {
        if k != i {
            assert(skips(next, dead, k));
        }
    }
    assert(after[after.len() - 1] == after.len() - 1) by {
        if i == next.len() - 1 {
            assert(next[i] == i);
        }
    }
}

/// A link from a dead entry to the next entry keeps the links sound.
proof fn lemma_a_dead_link_keeps_the_links(next: Seq<usize>, dead: Set<int>, i: int)
    requires
        links(next, dead),
        0 <= i < next.len() - 1,
        dead.contains(i),
    ensures
        links(next.update(i, (i + 1) as usize), dead),
{
    let after = next.update(i, (i + 1) as usize);
    assert forall|k: int| 0 <= k < after.len() implies k <= #[trigger] after[k] < after.len() by {
        if k != i {
            assert(k <= next[k]);
        }
    }
    assert forall|k: int| 0 <= k < after.len() implies #[trigger] skips(after, dead, k) by {
        if k != i {
            assert(skips(next, dead, k));
        }
    }
}

// mirror: autumn-harvest/src/activity_scan_index.rs first_live
pub fn first_live(
    next: &mut [usize],
    from: usize,
    is_dead: impl Fn(usize) -> bool,
    Ghost(dead): Ghost<Set<int>>,
) -> (r: usize)
    requires
        links(old(next)@, dead),
        from < old(next).len(),
        forall|j: usize| j + 1 < old(next).len() ==> is_dead.requires((j,)),
        forall|j: usize, b: bool| is_dead.ensures((j,), b) ==> b == dead.contains(j as int),
    ensures
        links(final(next)@, dead),
        final(next).len() == old(next).len(),
        from <= r < final(next).len(),
        r == final(next).len() - 1 || !dead.contains(r as int),
        forall|m: int| from <= m < r ==> #[trigger] dead.contains(m),
{
    let mut i = from;
    loop
        invariant
            links(next@, dead),
            next.len() == old(next).len(),
            from <= i < next.len(),
            forall|m: int| from <= m < i ==> #[trigger] dead.contains(m),
            forall|j: usize| j + 1 < next.len() ==> is_dead.requires((j,)),
            forall|j: usize, b: bool| is_dead.ensures((j,), b) ==> b == dead.contains(j as int),
        decreases next.len() - i,
    {
        let up = next[i];
        if up != i {
            proof {
                lemma_a_halving_keeps_the_links(next@, dead, i as int);
            }
            next[i] = next[up];
            i = next[i];
        } else if i + 1 == next.len() || !is_dead(i) {
            return i;
        } else {
            proof {
                lemma_a_dead_link_keeps_the_links(next@, dead, i as int);
            }
            next[i] = i + 1;
            i += 1;
        }
    }
}

/// A new list links each slot to itself. Its links are sound for any dead
/// set.
pub proof fn theorem_a_new_list_is_sound(next: Seq<usize>, dead: Set<int>)
    requires
        next.len() > 0,
        forall|k: int| 0 <= k < next.len() ==> #[trigger] next[k] == k,
    ensures
        links(next, dead),
{
    assert forall|k: int| 0 <= k < next.len() implies #[trigger] skips(next, dead, k) by {
        assert(next[k] == k);
    }
}

/// `SkipList::push` adds an entry at the end. The old end slot becomes the
/// new entry, and a new end slot links to itself. The links stay sound.
pub proof fn theorem_an_added_entry_keeps_the_links(next: Seq<usize>, dead: Set<int>)
    requires
        links(next, dead),
        next.len() < usize::MAX,
    ensures
        links(next.push(next.len() as usize), dead),
{
    let after = next.push(next.len() as usize);
    assert forall|k: int| 0 <= k < after.len() implies k <= #[trigger] after[k] < after.len() by {
        if k < next.len() {
            assert(after[k] == next[k]);
            assert(k <= next[k]);
        } else {
            assert(after[k] == next.len());
        }
    }
    assert forall|k: int| 0 <= k < after.len() implies #[trigger] skips(after, dead, k) by {
        if k < next.len() {
            assert(after[k] == next[k]);
            assert(skips(next, dead, k));
        }
    }
}

/// The consumed sets only grow. Links that are sound for a dead set stay
/// sound for any larger one.
pub proof fn theorem_more_dead_entries_keep_the_links(
    next: Seq<usize>,
    dead: Set<int>,
    more: Set<int>,
)
    requires
        links(next, dead),
        dead.subset_of(more),
    ensures
        links(next, more),
{
    assert forall|k: int| 0 <= k < next.len() implies #[trigger] skips(next, more, k) by {
        assert(skips(next, dead, k));
    }
}

} // verus!
