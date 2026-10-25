//! The applied set of a peer receiver drops no new message (issue #2269).
//!
//! The receiver keeps the numbers that it applied as a floor and a set
//! above it. `Applied::apply` reads the pair through `is_new`, and folds the
//! floor up through `next_floor`. The copies below prove both kernels for
//! every input.
//!
//! The spec models the pair as a set of numbers. It proves that an apply
//! and its fold keep the pair equal to the plain set with the new number
//! inserted. So the pair never takes a new message for a duplicate. The
//! TLA+ model `AgentMesh.tla` found that loss with a high-water mark,
//! because the outbox can deliver message 2 before message 1.
//!
//! The bound on the set above forgets its lowest number. The spec proves
//! that this forgets only an applied number, so it makes no false
//! duplicate either.

use vstd::prelude::*;

verus! {

/// `is_new` as a spec.
pub open spec fn spec_is_new(floor: int, seq: int, in_above: bool) -> bool {
    seq > floor && !in_above
}

/// `next_floor` as a spec.
pub open spec fn spec_next_floor(floor: int, next_in_above: bool) -> int {
    if next_in_above && floor < u64::MAX {
        floor + 1
    } else {
        floor
    }
}

// mirror: autumn-harvest-agent/src/agent/dedupe.rs is_new
pub fn is_new(floor: u64, seq: u64, in_above: bool) -> (r: bool)
    ensures
        r == spec_is_new(floor as int, seq as int, in_above),
        seq <= floor ==> !r,
        in_above ==> !r,
{
    seq > floor && !in_above
}

// mirror: autumn-harvest-agent/src/agent/dedupe.rs next_floor
pub fn next_floor(floor: u64, next_in_above: bool) -> (r: u64)
    ensures
        r == spec_next_floor(floor as int, next_in_above),
        floor <= r <= floor + 1,
        r == floor + 1 <==> (next_in_above && floor < u64::MAX),
{
    if next_in_above && floor < u64::MAX {
        floor + 1
    } else {
        floor
    }
}

/// Whether the pair `(floor, above)` holds `s`. `Applied::contains` reads
/// the pair this way.
pub open spec fn contains(floor: int, above: Set<int>, s: int) -> bool {
    !spec_is_new(floor, s, above.contains(s))
}

/// The pair holds exactly the numbers of `applied`. Senders number their
/// messages from 1.
pub open spec fn encodes(floor: int, above: Set<int>, applied: Set<int>) -> bool {
    forall|s: int| 1 <= s ==> (#[trigger] applied.contains(s) == contains(floor, above, s))
}

/// The pair holds no number outside `applied`. So it takes no new message
/// for a duplicate.
pub open spec fn sound(floor: int, above: Set<int>, applied: Set<int>) -> bool {
    forall|s: int| 1 <= s && #[trigger] contains(floor, above, s) ==> applied.contains(s)
}

/// The pair after the loop in `Applied::apply`. The floor moves up while
/// the set above holds the next number, and the loop removes that number.
pub open spec fn fold(floor: int, above: Set<int>) -> (int, Set<int>)
    decreases u64::MAX - floor,
{
    let next = spec_next_floor(floor, above.contains(floor + 1));
    if next == floor {
        (floor, above)
    } else {
        fold(next, above.remove(next))
    }
}

/// The pair after `Applied::apply(seq)`, without the bound on the set above.
pub open spec fn apply(floor: int, above: Set<int>, seq: int) -> (int, Set<int>) {
    if spec_is_new(floor, seq, above.contains(seq)) {
        fold(floor, above.insert(seq))
    } else {
        (floor, above)
    }
}

/// The pair after a sequence of applies, first number first.
pub open spec fn apply_all(floor: int, above: Set<int>, seqs: Seq<int>) -> (int, Set<int>)
    decreases seqs.len(),
{
    if seqs.len() == 0 {
        (floor, above)
    } else {
        let next = apply(floor, above, seqs[0]);
        apply_all(next.0, next.1, seqs.drop_first())
    }
}

/// The fold holds the same numbers as the pair before it. It only moves a
/// number from the set above into the floor.
pub proof fn theorem_a_fold_keeps_the_applied_set(floor: int, above: Set<int>, s: int)
    ensures
        contains(fold(floor, above).0, fold(floor, above).1, s) == contains(floor, above, s),
    decreases u64::MAX - floor,
{
    let next = spec_next_floor(floor, above.contains(floor + 1));
    if next != floor {
        theorem_a_fold_keeps_the_applied_set(next, above.remove(next), s);
    }
}

/// The floor never moves over a gap. Each number that the fold puts under
/// the floor was in the set above.
pub proof fn theorem_the_floor_never_moves_over_a_gap(floor: int, above: Set<int>, s: int)
    ensures
        floor <= fold(floor, above).0,
        floor < s <= fold(floor, above).0 ==> above.contains(s),
    decreases u64::MAX - floor,
{
    let next = spec_next_floor(floor, above.contains(floor + 1));
    if next != floor {
        theorem_the_floor_never_moves_over_a_gap(next, above.remove(next), s);
    }
}

/// An apply is the plain insert. The kernel calls a number new exactly when
/// the set does not hold it. After the apply and its fold, the pair holds
/// exactly the set with the number inserted.
pub proof fn theorem_an_apply_is_the_plain_insert(
    floor: int,
    above: Set<int>,
    applied: Set<int>,
    seq: int,
)
    requires
        encodes(floor, above, applied),
        1 <= seq,
    ensures
        spec_is_new(floor, seq, above.contains(seq)) == !applied.contains(seq),
        encodes(apply(floor, above, seq).0, apply(floor, above, seq).1, applied.insert(seq)),
{
    assert(applied.contains(seq) == contains(floor, above, seq));
    let after = apply(floor, above, seq);
    let inserted = applied.insert(seq);
    assert forall|s: int| 1 <= s implies #[trigger] inserted.contains(s) == contains(
        after.0,
        after.1,
        s,
    ) by {
        assert(applied.contains(s) == contains(floor, above, s));
        if spec_is_new(floor, seq, above.contains(seq)) {
            theorem_a_fold_keeps_the_applied_set(floor, above.insert(seq), s);
        }
    }
}

/// Any order of numbers, with any repeats, leaves the pair equal to the set
/// of the numbers that arrived.
proof fn lemma_apply_all_is_the_union(
    floor: int,
    above: Set<int>,
    applied: Set<int>,
    seqs: Seq<int>,
)
    requires
        encodes(floor, above, applied),
        forall|i: int| 0 <= i < seqs.len() ==> 1 <= #[trigger] seqs[i],
    ensures
        encodes(
            apply_all(floor, above, seqs).0,
            apply_all(floor, above, seqs).1,
            applied.union(seqs.to_set()),
        ),
    decreases seqs.len(),
{
    if seqs.len() == 0 {
        assert(applied.union(seqs.to_set()) =~= applied);
    } else {
        let next = apply(floor, above, seqs[0]);
        let rest = seqs.drop_first();
        assert(1 <= seqs[0]);
        theorem_an_apply_is_the_plain_insert(floor, above, applied, seqs[0]);
        assert forall|i: int| 0 <= i < rest.len() implies 1 <= #[trigger] rest[i] by {
            assert(rest[i] == seqs[i + 1]);
        }
        lemma_apply_all_is_the_union(next.0, next.1, applied.insert(seqs[0]), rest);
        assert forall|x: int| seqs.contains(x) <==> (x == seqs[0] || rest.contains(x)) by {
            if seqs.contains(x) {
                let i = choose|i: int| 0 <= i < seqs.len() && seqs[i] == x;
                if i > 0 {
                    assert(rest[i - 1] == x);
                }
            }
            if rest.contains(x) {
                let i = choose|i: int| 0 <= i < rest.len() && rest[i] == x;
                assert(seqs[i + 1] == x);
            }
            if x == seqs[0] {
                assert(seqs[0] == x);
            }
        }
        assert(applied.insert(seqs[0]).union(rest.to_set()) =~= applied.union(seqs.to_set()));
    }
}

/// Each number applies once. In any order and with any repeats, the first
/// arrival of a number is new, and each repeat is a duplicate. The
/// proptest `each_number_applies_once` samples this property.
pub proof fn theorem_each_number_applies_once(seqs: Seq<int>, i: int)
    requires
        forall|j: int| 0 <= j < seqs.len() ==> 1 <= #[trigger] seqs[j],
        0 <= i < seqs.len(),
    ensures
        ({
            let before = apply_all(0, Set::empty(), seqs.take(i));
            spec_is_new(before.0, seqs[i], before.1.contains(seqs[i])) == !seqs.take(i).contains(
                seqs[i],
            )
        }),
        encodes(
            apply_all(0, Set::empty(), seqs).0,
            apply_all(0, Set::empty(), seqs).1,
            seqs.to_set(),
        ),
{
    let empty = Set::<int>::empty();
    assert(encodes(0, empty, empty)) by {
        assert forall|s: int| 1 <= s implies #[trigger] empty.contains(s) == contains(
            0,
            empty,
            s,
        ) by {}
    }
    let prefix = seqs.take(i);
    assert forall|j: int| 0 <= j < prefix.len() implies 1 <= #[trigger] prefix[j] by {
        assert(prefix[j] == seqs[j]);
    }
    lemma_apply_all_is_the_union(0, empty, empty, prefix);
    assert(empty.union(prefix.to_set()) =~= prefix.to_set());
    let before = apply_all(0, empty, prefix);
    theorem_an_apply_is_the_plain_insert(before.0, before.1, prefix.to_set(), seqs[i]);
    lemma_apply_all_is_the_union(0, empty, empty, seqs);
    assert(empty.union(seqs.to_set()) =~= seqs.to_set());
}

/// Each step keeps the pair sound, the bound on the set above included. So
/// the receiver never takes a new message for a duplicate. A number that
/// the bound forgot can apply again, and the engine key dedupe catches it.
pub proof fn theorem_the_bound_makes_no_false_duplicate(
    floor: int,
    above: Set<int>,
    applied: Set<int>,
    seq: int,
    forgot: int,
)
    requires
        sound(floor, above, applied),
        1 <= seq,
    ensures
        !spec_is_new(floor, seq, above.contains(seq)) ==> applied.contains(seq),
        sound(apply(floor, above, seq).0, apply(floor, above, seq).1, applied.insert(seq)),
        sound(floor, above.remove(forgot), applied),
{
    let after = apply(floor, above, seq);
    let inserted = applied.insert(seq);
    assert forall|s: int| 1 <= s && #[trigger] contains(after.0, after.1, s) implies inserted.contains(
        s,
    ) by {
        if spec_is_new(floor, seq, above.contains(seq)) {
            theorem_a_fold_keeps_the_applied_set(floor, above.insert(seq), s);
            if s != seq {
                assert(contains(floor, above, s));
            }
        } else {
            assert(contains(floor, above, s));
        }
    }
    assert(contains(floor, above, seq) ==> applied.contains(seq));
    let smaller = above.remove(forgot);
    assert forall|s: int| 1 <= s && #[trigger] contains(floor, smaller, s) implies applied.contains(
        s,
    ) by {
        assert(contains(floor, above, s));
    }
}

} // verus!
