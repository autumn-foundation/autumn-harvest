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
/// transcript, so the whole next request must fit the cap. The budget starts
/// from the size of that request with a placeholder for each call. A result
/// replaces its placeholder, so it adds only the difference. A JSON string is
/// measured on its own, so the sum is exact.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RoundBudget {
    cap: u64,
    projected: u64,
    minimal: u64,
}

impl RoundBudget {
    /// A budget for a next request of `floor` bytes under `cap`. Each call
    /// holds a placeholder of `minimal` bytes in `floor`. A `cap` of zero is
    /// no cap.
    pub(crate) const fn new(cap: u64, floor: u64, minimal: u64) -> Self {
        Self {
            cap,
            projected: floor,
            minimal,
        }
    }

    /// Can the round run a call? Only when every call can still get its
    /// minimal answer. A call that runs has a result that no request can
    /// carry otherwise.
    pub(crate) const fn can_run(&self) -> bool {
        self.cap == 0 || self.projected <= self.cap
    }

    /// Charge a result of `size` bytes. Returns `false`, and charges
    /// nothing, when the next request would pass the cap.
    pub(crate) const fn admit(&mut self, size: u64) -> bool {
        let next = self
            .projected
            .saturating_add(size)
            .saturating_sub(self.minimal);
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_zero_budget_cap_admits_everything() {
        let mut budget = RoundBudget::new(0, u64::MAX, 10);
        assert!(budget.can_run());
        assert!(budget.admit(u64::MAX));
    }
}
