//! Size checks that keep a run inside the engine payload caps.
//!
//! The engine refuses an activity input over
//! [`DEFAULT_MAX_ACTIVITY_INPUT_BYTES`] with `PayloadTooLarge`. That error is
//! not retryable, so the run would FAIL after the model call was paid for. The
//! loop measures the request first and stops under its own reason instead.
//!
//! These checks run inside the workflow, so they must answer the same way on
//! replay. They read no clock and no state outside their arguments. The cap
//! comes from recorded input or from a constant of the build.
//!
//! The engine cap is configurable. A worker with a cap other than the default
//! must pass it in, for example as `AgentTask::max_request_bytes`. A cap of
//! zero means no cap, as in the engine.

use autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES;
use serde::Serialize;

/// The serialised size of `value`, in bytes.
///
/// A value that cannot be serialised measures as zero. No check refuses work
/// over a size that it failed to read.
#[must_use]
pub fn json_len<T: Serialize + ?Sized>(value: &T) -> u64 {
    serde_json::to_vec(value).map_or(0, |json| json.len() as u64)
}

/// Is `value` larger than `cap` bytes once serialised?
///
/// A `cap` of zero is no cap, as it is in the engine.
#[must_use]
pub fn exceeds_bytes<T: Serialize + ?Sized>(value: &T, cap: u64) -> bool {
    cap > 0 && json_len(value) > cap
}

/// The room that one tool round has in the next request.
///
/// The round answers every call in one tool message. That message joins the
/// transcript, so the whole next request must fit the cap.
///
/// The budget starts from the size of that request with one placeholder
/// answer for each call. A result replaces its placeholder, so it adds only
/// the difference in length. The sum is exact for the result text. It does
/// not cover state that a call changes while it runs, such as the labels of a
/// delegated child. The loop checks the next request again before it sends
/// it, and ends the run if that request exceeds the cap.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RoundBudget {
    cap: u64,
    projected: u64,
    placeholder: u64,
}

impl RoundBudget {
    /// A budget for a next request of `floor` bytes under `cap`. The floor
    /// holds one placeholder of `placeholder` bytes for each call. A `cap` of
    /// zero is no cap.
    pub(crate) const fn new(cap: u64, floor: u64, placeholder: u64) -> Self {
        Self {
            cap,
            projected: floor,
            placeholder,
        }
    }

    /// A budget that measures the results alone, as the first version did.
    /// A task recorded before the whole-request budget uses it, so a run in
    /// flight replays as it ran.
    pub(crate) const fn results_only(cap: u64) -> Self {
        Self::new(cap, 0, 0)
    }

    /// Can the round run a call? Only when a placeholder for each call fits
    /// the next request. A call that runs when no answer fits has a result
    /// that the model can never read.
    pub(crate) const fn can_run(&self) -> bool {
        self.cap == 0 || self.projected <= self.cap
    }

    /// Add a result of `size` bytes in place of its placeholder. Returns
    /// `false`, and adds nothing, when the next request would exceed the cap.
    pub(crate) const fn admit(&mut self, size: u64) -> bool {
        let next = self
            .projected
            .saturating_add(size)
            .saturating_sub(self.placeholder);
        if self.cap != 0 && next > self.cap {
            return false;
        }
        self.projected = next;
        true
    }
}

/// Is `value` too large to send as one activity input under the default cap?
///
/// It refuses only what the engine is certain to refuse, and never a value
/// that fits.
#[must_use]
pub fn exceeds_activity_input<T: Serialize + ?Sized>(value: &T) -> bool {
    exceeds_bytes(value, DEFAULT_MAX_ACTIVITY_INPUT_BYTES)
}

/// The token share of child `index` of `siblings` that run at the same
/// time, from the `remaining` tokens of the parent (issue #2270).
///
/// Each child gets `remaining / siblings`, and the first `remaining %
/// siblings` children get one more. The shares sum to `remaining`, so the
/// siblings together never pass the cap of the parent.
/// `formal/verus/src/agent_siblings.rs` proves it for every input.
#[must_use]
pub const fn sibling_cap(remaining: u32, siblings: u32, index: u32) -> u32 {
    if siblings == 0 {
        return 0;
    }
    let base = remaining / siblings;
    if index < remaining % siblings {
        base + 1
    } else {
        base
    }
}

/// Does a request of `len` bytes reach `percent` of `cap` (issue #2453)?
///
/// A `percent` of 0 counts as 1, and a value over 100 counts as 100. A
/// `cap` of zero is no cap, so no request reaches it. The product runs in
/// `u128`, so it never overflows.
/// `formal/verus/src/agent_reset.rs` proves it for every input.
#[must_use]
pub const fn reaches_percent(len: u64, cap: u64, percent: u8) -> bool {
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
    (len as u128) * 100 >= (cap as u128) * (percent as u128)
}

/// Is a context reset due before a request of `len` bytes (issue #2453)?
///
/// It is due when the run used fewer than `max_resets` resets and the
/// request reaches `percent` of `cap`. A request over the cap always
/// reaches it. A tool round can pass the cap first. [`full_round_resets`]
/// decides that case.
/// `formal/verus/src/agent_reset.rs` proves it for every input.
#[must_use]
pub const fn reset_due(resets: u32, max_resets: u32, len: u64, cap: u64, percent: u8) -> bool {
    resets < max_resets && reaches_percent(len, cap, percent)
}

/// Does a tool round over the cap reset, and not end the run (issue
/// #2461)?
///
/// `after_len` is the size of the request after the reset, with the round
/// kept and the notes of that time. The round resets when a reset is left
/// and that request falls under `percent` of `cap`. So that request fits the
/// cap. A `cap` of zero is no cap, so no round passes it.
/// `formal/verus/src/agent_reset.rs` proves it for every input.
#[must_use]
pub const fn full_round_resets(
    resets: u32,
    max_resets: u32,
    after_len: u64,
    cap: u64,
    percent: u8,
) -> bool {
    resets < max_resets && cap > 0 && !reaches_percent(after_len, cap, percent)
}

/// The episodes that one append evicts from a log of `len` episodes
/// with the bound `max` (issue #2454).
///
/// A bound of 0 counts as 1, so the new episode always fits. After the
/// append, the log holds at most the bound. An append to a log within the
/// bound evicts at most one episode.
/// `formal/verus/src/agent_episodes.rs` proves it for every input.
#[must_use]
pub const fn episode_evictions(len: u64, max: u64) -> u64 {
    let max = if max == 0 { 1 } else { max };
    if len < max { 0 } else { len - max + 1 }
}

/// Is a consolidation pass due after `appended` episodes since the last
/// pass, with the trigger `every` (issue #2454)?
///
/// The count is of appends, not of pending episodes. A full log keeps its
/// pending count, so only an append count moves the trigger. A trigger of
/// 0 never fires.
/// `formal/verus/src/agent_episodes.rs` proves it for every input.
#[must_use]
pub const fn consolidation_due(appended: u64, every: u32) -> bool {
    every > 0 && appended >= every as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_append_keeps_the_log_in_its_bound() {
        assert_eq!(episode_evictions(0, 2), 0);
        assert_eq!(episode_evictions(1, 2), 0);
        assert_eq!(episode_evictions(2, 2), 1);
        // A bound of 0 counts as 1.
        assert_eq!(episode_evictions(0, 0), 0);
        assert_eq!(episode_evictions(1, 0), 1);
        // A log over the bound shrinks to it.
        assert_eq!(episode_evictions(5, 2), 4);
    }

    #[test]
    fn a_pass_is_due_at_the_trigger() {
        assert!(!consolidation_due(0, 3));
        assert!(!consolidation_due(2, 3));
        assert!(consolidation_due(3, 3));
        assert!(consolidation_due(4, 3));
        assert!(!consolidation_due(u64::MAX, 0));
    }

    proptest::proptest! {
        #[test]
        fn no_append_passes_the_bound(len in 0_u64..10_000, max in 0_u64..10_000) {
            let evicted = episode_evictions(len, max);
            proptest::prop_assert!(evicted <= len);
            proptest::prop_assert!(len - evicted < max.max(1));
        }
    }

    #[test]
    fn the_threshold_is_a_percent_of_the_cap() {
        assert!(!reaches_percent(49, 100, 50));
        assert!(reaches_percent(50, 100, 50));
        assert!(reaches_percent(u64::MAX, u64::MAX, 100));
        assert!(!reaches_percent(u64::MAX - 1, u64::MAX, 100));
    }

    #[test]
    fn a_zero_cap_never_reaches_the_threshold() {
        assert!(!reaches_percent(u64::MAX, 0, 1));
        assert!(!reset_due(0, 9, u64::MAX, 0, 50));
    }

    #[test]
    fn an_out_of_range_percent_is_clamped() {
        // 0 counts as 1.
        assert!(!reaches_percent(0, 100, 0));
        assert!(reaches_percent(1, 100, 0));
        // Over 100 counts as 100.
        assert!(!reaches_percent(99, 100, 255));
        assert!(reaches_percent(100, 100, 255));
    }

    #[test]
    fn a_reset_is_due_only_with_one_left() {
        assert!(reset_due(0, 1, 60, 100, 50));
        assert!(!reset_due(1, 1, 60, 100, 50));
        assert!(!reset_due(0, 0, 60, 100, 50));
        assert!(!reset_due(0, 1, 40, 100, 50));
    }

    #[test]
    fn a_full_round_resets_only_with_room_and_a_reset_left() {
        assert!(full_round_resets(0, 1, 40, 100, 50));
        assert!(!full_round_resets(0, 1, 50, 100, 50), "at the threshold");
        assert!(!full_round_resets(1, 1, 40, 100, 50), "no reset left");
        assert!(!full_round_resets(0, 0, 0, 100, 50));
        assert!(!full_round_resets(0, 9, 0, 0, 50), "a zero cap is no cap");
        // 0 counts as 1, so only an empty window fits.
        assert!(full_round_resets(0, 1, 0, 100, 0));
        assert!(!full_round_resets(0, 1, 1, 100, 0));
        // Half of 3 bytes is 1.5, so 1 byte is under it and 2 bytes are not.
        assert!(full_round_resets(0, 1, 1, 3, 50));
        assert!(!full_round_resets(0, 1, 2, 3, 50));
    }

    proptest::proptest! {
        #[test]
        fn a_full_round_reset_leaves_a_window_that_fits(
            resets in 0_u32..8,
            max_resets in 0_u32..8,
            after_len in 0_u64..u64::MAX,
            cap in 0_u64..u64::MAX,
            percent in 0_u8..=255,
        ) {
            let clamped = u128::from(percent.clamp(1, 100));
            let under = u128::from(after_len) * 100 < u128::from(cap) * clamped;
            proptest::prop_assert_eq!(
                full_round_resets(resets, max_resets, after_len, cap, percent),
                resets < max_resets && cap > 0 && under
            );
            if full_round_resets(resets, max_resets, after_len, cap, percent) {
                proptest::prop_assert!(after_len < cap);
            }
        }

        #[test]
        fn a_request_over_the_cap_always_reaches_the_threshold(
            cap in 1_u64..u64::MAX,
            extra in 1_u64..1_000,
            percent in 0_u8..=255,
        ) {
            let len = cap.saturating_add(extra);
            proptest::prop_assume!(len > cap);
            proptest::prop_assert!(reaches_percent(len, cap, percent));
        }

        #[test]
        fn the_threshold_is_monotone_in_the_size(
            a in 0_u64..u64::MAX,
            b in 0_u64..u64::MAX,
            cap in 0_u64..u64::MAX,
            percent in 0_u8..=255,
        ) {
            let (small, large) = if a <= b { (a, b) } else { (b, a) };
            if reaches_percent(small, cap, percent) {
                proptest::prop_assert!(reaches_percent(large, cap, percent));
            }
        }
    }

    /// A JSON string of exactly `bytes` serialised bytes.
    fn string_of(bytes: u64) -> String {
        "x".repeat(usize::try_from(bytes - 2).unwrap())
    }

    #[test]
    fn the_cap_itself_fits_and_one_byte_more_does_not() {
        let at_cap = string_of(DEFAULT_MAX_ACTIVITY_INPUT_BYTES);
        assert_eq!(json_len(&at_cap), DEFAULT_MAX_ACTIVITY_INPUT_BYTES);
        assert!(!exceeds_activity_input(&at_cap));
        let over = string_of(DEFAULT_MAX_ACTIVITY_INPUT_BYTES + 1);
        assert!(exceeds_activity_input(&over));
        assert!(exceeds_bytes(&"abc", 4));
        assert!(!exceeds_bytes(&"abc", 5));
    }

    #[test]
    fn a_small_value_fits() {
        assert_eq!(json_len(&serde_json::json!({"a": 1})), 7);
        assert!(!exceeds_activity_input(&"small"));
    }

    #[test]
    fn a_zero_cap_is_no_cap() {
        // The engine reads a cap of zero as no cap.
        assert!(!exceeds_bytes(&"abc", 0));
        assert!(!exceeds_bytes(
            &string_of(DEFAULT_MAX_ACTIVITY_INPUT_BYTES * 2),
            0
        ));
    }

    #[test]
    fn a_round_that_cannot_hold_minimal_answers_cannot_run() {
        assert!(RoundBudget::new(100, 100, 10).can_run());
        assert!(!RoundBudget::new(100, 101, 10).can_run());
    }

    #[test]
    fn a_result_is_charged_over_its_placeholder() {
        // The floor already holds 10 bytes for this call.
        let mut budget = RoundBudget::new(100, 90, 10);
        assert!(budget.admit(20), "90 + (20 - 10) = 100 fits exactly");
        assert!(budget.can_run());
        assert!(
            budget.admit(10),
            "a result as long as the placeholder is free"
        );
        assert!(!budget.admit(11), "one byte over");
        assert!(budget.admit(10), "a refused result charged nothing");
    }

    #[test]
    fn a_short_result_frees_room_for_the_next() {
        let mut budget = RoundBudget::new(100, 100, 10);
        assert!(budget.admit(4), "6 bytes freed");
        assert!(budget.admit(16), "uses the 6 freed bytes");
        assert!(!budget.admit(11));
    }

    #[test]
    fn a_results_only_budget_adds_each_result_in_full() {
        let mut budget = RoundBudget::results_only(100);
        assert!(budget.can_run());
        assert!(budget.admit(60));
        assert!(!budget.admit(41), "60 + 41 exceeds the cap");
        assert!(budget.admit(40));
        let mut open = RoundBudget::results_only(0);
        assert!(open.admit(u64::MAX), "a zero cap is no cap");
    }

    proptest::proptest! {
        #[test]
        fn a_budget_that_starts_inside_the_cap_stays_inside_it(
            cap in 1_u64..20_000,
            floor in 0_u64..20_000,
            placeholder in 0_u64..300,
            sizes in proptest::collection::vec(0_u64..5_000, 0..24),
        ) {
            let mut budget = RoundBudget::new(cap, floor, placeholder);
            proptest::prop_assert_eq!(budget.can_run(), floor <= cap);
            let mut projected = floor;
            for size in sizes {
                let next = (projected + size).saturating_sub(placeholder);
                let admitted = budget.admit(size);
                proptest::prop_assert_eq!(admitted, next <= cap);
                if admitted {
                    projected = next;
                }
                proptest::prop_assert_eq!(budget.can_run(), projected <= cap);
                if floor <= cap {
                    proptest::prop_assert!(projected <= cap, "an admitted result stays inside the cap");
                }
            }
        }
    }

    #[test]
    fn a_zero_budget_cap_admits_everything() {
        let mut budget = RoundBudget::new(0, u64::MAX, 10);
        assert!(budget.can_run());
        assert!(budget.admit(u64::MAX));
    }

    #[test]
    fn sibling_shares_sum_to_the_remainder() {
        for remaining in [0_u32, 1, 7, 100, 1_000_003] {
            for siblings in 1_u32..=9 {
                let sum: u64 = (0..siblings)
                    .map(|index| u64::from(sibling_cap(remaining, siblings, index)))
                    .sum();
                assert_eq!(sum, u64::from(remaining), "{remaining} over {siblings}");
            }
        }
        assert_eq!(sibling_cap(10, 0, 0), 0);
        assert_eq!(sibling_cap(10, 3, 0), 4);
        assert_eq!(sibling_cap(10, 3, 2), 3);
    }
}
