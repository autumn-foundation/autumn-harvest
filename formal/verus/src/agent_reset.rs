//! The context reset of an agent run fires at its threshold (issue #2453).
//!
//! `reaches_percent` asks whether a request of `len` bytes reaches
//! `percent` of the request cap. `reset_due` adds the reset budget. The
//! copies below prove that no step overflows and that each one equals its
//! spec. The theorems show that a request over the cap always reaches the
//! threshold. With a reset left, the loop therefore resets before it ends
//! the run as `transcript_full`. A zero cap never resets, and no reset
//! passes `max_resets`.

use vstd::prelude::*;

verus! {

/// The percent that the loop uses: 0 counts as 1, and over 100 counts as
/// 100.
pub open spec fn clamped(percent: int) -> int {
    if percent == 0 {
        1
    } else if percent > 100 {
        100
    } else {
        percent
    }
}

/// A request of `len` bytes reaches `percent` of `cap`. A zero cap is no
/// cap, so no request reaches it.
pub open spec fn reaches(len: int, cap: int, percent: int) -> bool {
    cap > 0 && len * 100 >= cap * clamped(percent)
}

// mirror: autumn-harvest-agent/src/bounds.rs reaches_percent
pub fn reaches_percent(len: u64, cap: u64, percent: u8) -> (r: bool)
    ensures
        r == reaches(len as int, cap as int, percent as int),
{
    if cap == 0 {
        return false;
    }
    let percent = if percent == 0 {
        1
    } else if percent > 100 {
        100
    } else {
        percent
    };
    proof {
        assert((cap as int) * (percent as int) <= (cap as int) * 100) by (nonlinear_arith)
            requires
                percent <= 100,
                cap >= 0,
        ;
    }
    (len as u128) * 100 >= (cap as u128) * (percent as u128)
}

/// A reset is due when a reset is left and the request reaches the
/// threshold.
pub open spec fn due(resets: int, max_resets: int, len: int, cap: int, percent: int) -> bool {
    resets < max_resets && reaches(len, cap, percent)
}

// mirror: autumn-harvest-agent/src/bounds.rs reset_due
pub fn reset_due(resets: u32, max_resets: u32, len: u64, cap: u64, percent: u8) -> (r: bool)
    ensures
        r == due(resets as int, max_resets as int, len as int, cap as int, percent as int),
{
    resets < max_resets && reaches_percent(len, cap, percent)
}

/// A request over the cap reaches the threshold for every percent. So a
/// run with a reset left resets before the cap ends it.
pub proof fn theorem_a_request_over_the_cap_reaches_the_threshold(len: int, cap: int, percent: int)
    requires
        0 < cap < len,
        0 <= percent,
    ensures
        reaches(len, cap, percent),
{
    let p = clamped(percent);
    assert(cap * p <= cap * 100) by (nonlinear_arith)
        requires
            p <= 100,
            cap > 0,
    ;
}

/// With a reset left, a request over the cap makes a reset due.
pub proof fn theorem_a_reset_comes_before_the_cap(
    resets: int,
    max_resets: int,
    len: int,
    cap: int,
    percent: int,
)
    requires
        resets < max_resets,
        0 < cap < len,
        0 <= percent,
    ensures
        due(resets, max_resets, len, cap, percent),
{
    theorem_a_request_over_the_cap_reaches_the_threshold(len, cap, percent);
}

/// A larger request reaches the threshold when a smaller one does.
pub proof fn theorem_the_threshold_is_monotone(small: int, large: int, cap: int, percent: int)
    requires
        0 <= small <= large,
        reaches(small, cap, percent),
    ensures
        reaches(large, cap, percent),
{
}

/// A request under the threshold fits the cap, for a percent up to 100.
pub proof fn theorem_a_request_under_the_threshold_fits(len: int, cap: int, percent: int)
    requires
        0 <= len,
        0 < cap,
        0 <= percent,
        !reaches(len, cap, percent),
    ensures
        len < cap,
{
    let p = clamped(percent);
    assert(cap * p <= cap * 100) by (nonlinear_arith)
        requires
            p <= 100,
            cap > 0,
    ;
}

/// A zero cap is no cap, so it never makes a reset due.
pub proof fn theorem_a_zero_cap_never_resets(resets: int, max_resets: int, len: int, percent: int)
    ensures
        !due(resets, max_resets, len, 0, percent),
{
}

/// The reset count after one request: one more when a reset is due.
pub open spec fn after(resets: int, max_resets: int, len: int, cap: int, percent: int) -> int {
    if due(resets, max_resets, len, cap, percent) {
        resets + 1
    } else {
        resets
    }
}

/// The reset count after a sequence of request sizes.
pub open spec fn count(resets: int, max_resets: int, lens: Seq<int>, cap: int, percent: int) -> int
    decreases lens.len(),
{
    if lens.len() == 0 {
        resets
    } else {
        count(
            after(resets, max_resets, lens[0], cap, percent),
            max_resets,
            lens.subrange(1, lens.len() as int),
            cap,
            percent,
        )
    }
}

/// No sequence of requests takes the reset count past `max_resets`, and the
/// count never goes down.
pub proof fn theorem_resets_never_pass_the_cap(
    resets: int,
    max_resets: int,
    lens: Seq<int>,
    cap: int,
    percent: int,
)
    requires
        0 <= resets <= max_resets,
    ensures
        resets <= count(resets, max_resets, lens, cap, percent) <= max_resets,
    decreases lens.len(),
{
    if lens.len() > 0 {
        let next = after(resets, max_resets, lens[0], cap, percent);
        theorem_resets_never_pass_the_cap(
            next,
            max_resets,
            lens.subrange(1, lens.len() as int),
            cap,
            percent,
        );
    }
}

} // verus!
