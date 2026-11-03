//! The parts of an evidence file tile the text (issue #2377).
//!
//! `split_parts` in `eval/evidence.rs` of the agent crate calls
//! `part_end` once per part. The copy below is that kernel. The spec
//! `tile` runs the split over any choice of cuts. The theorems state that
//! the parts tile the text. Each part is not empty and holds at most the
//! cap. Each part starts where the last one ended. The last part ends at
//! the end of the text. So the split never cuts a byte. The proptest
//! `the_parts_join_to_the_text_and_fit_the_cap` checks the engine split.

use vstd::prelude::*;

verus! {

/// `part_end` as a spec.
pub open spec fn spec_part_end(start: int, len: int, cap: int, cut: int) -> int {
    if len - start <= cap {
        len
    } else if start < cut && cut <= start + cap {
        cut
    } else {
        start + cap
    }
}

// mirror: autumn-harvest-agent/src/eval/evidence.rs part_end
pub fn part_end(start: usize, len: usize, cap: usize, cut: usize) -> (r: usize)
    requires
        start < len,
        0 < cap,
    ensures
        r == spec_part_end(start as int, len as int, cap as int, cut as int),
        start < r <= len,
        r - start <= cap,
        len - start <= cap ==> r == len,
{
    if len - start <= cap {
        len
    } else if start < cut && cut <= start + cap {
        cut
    } else {
        start + cap
    }
}

/// The end of each part of a text of `len` bytes, from `start`. `cut`
/// gives the best cut of the part that starts at a given byte.
pub open spec fn tile(start: int, len: int, cap: int, cut: spec_fn(int) -> int) -> Seq<int>
    decreases len - start,
{
    if start < 0 || start >= len || cap <= 0 {
        Seq::empty()
    } else {
        let end = spec_part_end(start, len, cap, cut(start));
        seq![end] + tile(end, len, cap, cut)
    }
}

/// The start of part `i`: `start` for the first part, else the end of the
/// part before it.
pub open spec fn part_start(ends: Seq<int>, start: int, i: int) -> int {
    if i == 0 {
        start
    } else {
        ends[i - 1]
    }
}

/// The parts from `start` tile the rest of the text.
proof fn lemma_tile(start: int, len: int, cap: int, cut: spec_fn(int) -> int)
    requires
        0 <= start <= len,
        0 < cap,
    ensures
        (tile(start, len, cap, cut).len() == 0) == (start == len),
        tile(start, len, cap, cut).len() > 0 ==> tile(start, len, cap, cut).last() == len,
        forall|i: int|
            0 <= i < tile(start, len, cap, cut).len() ==> part_start(
                tile(start, len, cap, cut),
                start,
                i,
            ) < #[trigger] tile(start, len, cap, cut)[i] <= len,
        forall|i: int|
            0 <= i < tile(start, len, cap, cut).len() ==> #[trigger] tile(start, len, cap, cut)[i]
                - part_start(tile(start, len, cap, cut), start, i) <= cap,
    decreases len - start,
{
    if start < len {
        let end = spec_part_end(start, len, cap, cut(start));
        lemma_tile(end, len, cap, cut);
        let rest = tile(end, len, cap, cut);
        let all = tile(start, len, cap, cut);
        assert(all == seq![end] + rest);
        assert forall|i: int| 0 <= i < all.len() implies part_start(all, start, i) < #[trigger] all[i]
            <= len && all[i] - part_start(all, start, i) <= cap by {
            if i > 0 {
                assert(all[i] == rest[i - 1]);
                assert(part_start(all, start, i) == part_start(rest, end, i - 1));
            }
        }
        if rest.len() > 0 {
            assert(all.last() == rest.last());
        }
    }
}

/// The parts of a text tile it. There is no part of an empty text. Each
/// part is not empty and holds at most `cap` bytes. Each part starts where
/// the last one ended, and the last part ends at `len`. So the parts joined
/// in order are the whole text, and nothing is cut.
pub proof fn theorem_the_parts_tile_the_text(len: int, cap: int, cut: spec_fn(int) -> int)
    requires
        0 <= len,
        0 < cap,
    ensures
        (tile(0, len, cap, cut).len() == 0) == (len == 0),
        tile(0, len, cap, cut).len() > 0 ==> tile(0, len, cap, cut).last() == len,
        forall|i: int|
            0 <= i < tile(0, len, cap, cut).len() ==> part_start(tile(0, len, cap, cut), 0, i)
                < #[trigger] tile(0, len, cap, cut)[i],
        forall|i: int|
            0 <= i < tile(0, len, cap, cut).len() ==> #[trigger] tile(0, len, cap, cut)[i]
                - part_start(tile(0, len, cap, cut), 0, i) <= cap,
{
    lemma_tile(0, len, cap, cut);
}

/// A text that fits the cap is one part.
pub proof fn theorem_a_text_that_fits_is_one_part(len: int, cap: int, cut: spec_fn(int) -> int)
    requires
        0 < len <= cap,
    ensures
        tile(0, len, cap, cut) == seq![len],
{
    assert(tile(len, len, cap, cut) == Seq::<int>::empty());
    assert(seq![len] + Seq::<int>::empty() == seq![len]);
}

} // verus!
