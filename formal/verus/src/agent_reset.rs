//! The context reset of an agent run fires at its threshold (issue #2453).
//!
//! `reaches_percent` asks whether a request of `len` bytes reaches
//! `percent` of the request cap. `reset_due` adds the reset budget. The
//! copies below prove that no step overflows and that each one equals its
//! spec. The theorems show that a request over the cap always reaches the
//! threshold. So the check before a request finds a reset due while one is
//! left. A zero cap never resets, and no reset passes `max_resets`.
//!
//! A tool round can pass the cap before that check (issue #2461).
//! `full_round_resets` then decides if the round resets. The theorems show
//! that such a reset leaves a window that fits the cap. They also show that
//! no mix of both kinds of reset passes `max_resets`.

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

/// A request over the cap reaches the threshold for every percent.
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


/// A round over the cap resets when a reset is left and the window after the
/// reset falls under the threshold (issue #2461). A zero cap never resets.
pub open spec fn full_round(resets: int, max_resets: int, after_len: int, cap: int, percent: int) -> bool {
    resets < max_resets && cap > 0 && !reaches(after_len, cap, percent)
}

// mirror: autumn-harvest-agent/src/bounds.rs full_round_resets
pub fn full_round_resets(resets: u32, max_resets: u32, after_len: u64, cap: u64, percent: u8) -> (r: bool)
    ensures
        r == full_round(resets as int, max_resets as int, after_len as int, cap as int, percent as int),
{
    resets < max_resets && cap > 0 && !reaches_percent(after_len, cap, percent)
}

/// The window after a full-round reset fits the cap, and a reset is left
/// for it.
pub proof fn theorem_a_full_round_reset_fits_the_cap(
    resets: int,
    max_resets: int,
    after_len: int,
    cap: int,
    percent: int,
)
    requires
        0 <= after_len,
        0 <= percent,
        full_round(resets, max_resets, after_len, cap, percent),
    ensures
        after_len < cap,
        resets + 1 <= max_resets,
{
    theorem_a_request_under_the_threshold_fits(after_len, cap, percent);
}

/// A zero cap never resets a round.
pub proof fn theorem_a_zero_cap_never_resets_a_round(
    resets: int,
    max_resets: int,
    after_len: int,
    percent: int,
)
    ensures
        !full_round(resets, max_resets, after_len, 0, percent),
{
}

/// The reset count after one event. An event is a request of `len` bytes,
/// or a round over the cap whose reset window is `len` bytes.
pub open spec fn after_event(
    resets: int,
    max_resets: int,
    event: (bool, int),
    cap: int,
    percent: int,
) -> int {
    if event.0 {
        if full_round(resets, max_resets, event.1, cap, percent) {
            resets + 1
        } else {
            resets
        }
    } else {
        after(resets, max_resets, event.1, cap, percent)
    }
}

/// The reset count after a sequence of events.
pub open spec fn count_events(
    resets: int,
    max_resets: int,
    events: Seq<(bool, int)>,
    cap: int,
    percent: int,
) -> int
    decreases events.len(),
{
    if events.len() == 0 {
        resets
    } else {
        count_events(
            after_event(resets, max_resets, events[0], cap, percent),
            max_resets,
            events.subrange(1, events.len() as int),
            cap,
            percent,
        )
    }
}

/// No mix of requests and full rounds takes the reset count past
/// `max_resets`, and the count never goes down.
pub proof fn theorem_mixed_resets_never_pass_the_cap(
    resets: int,
    max_resets: int,
    events: Seq<(bool, int)>,
    cap: int,
    percent: int,
)
    requires
        0 <= resets <= max_resets,
    ensures
        resets <= count_events(resets, max_resets, events, cap, percent) <= max_resets,
    decreases events.len(),
{
    if events.len() > 0 {
        let next = after_event(resets, max_resets, events[0], cap, percent);
        theorem_mixed_resets_never_pass_the_cap(
            next,
            max_resets,
            events.subrange(1, events.len() as int),
            cap,
            percent,
        );
    }
}

} // verus!
