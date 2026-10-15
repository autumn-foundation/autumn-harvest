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
//! must pass it in, for example as `AgentTask::max_request_bytes`.

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
#[must_use]
pub fn exceeds_bytes<T: Serialize + ?Sized>(value: &T, cap: u64) -> bool {
    json_len(value) > cap
}

/// The bytes that one tool round may still add to the next request.
///
/// SKELETON: it admits everything until the budget logic lands.
#[derive(Debug, Clone, Copy)]
pub struct RoundBudget;

impl RoundBudget {
    /// A budget for a request of `floor` bytes under `cap`, where each open
    /// call holds a placeholder of `minimal` bytes. A `cap` of zero is no cap.
    #[must_use]
    pub const fn new(_cap: u64, _floor: u64, _minimal: u64) -> Self {
        Self
    }

    /// Can the round run a call? Only when every call can still get at least
    /// a minimal answer.
    #[must_use]
    pub const fn can_run(&self) -> bool {
        true
    }

    /// Charge a result of `size` bytes. Returns `false`, and charges
    /// nothing, when the result does not fit.
    pub const fn admit(&mut self, _size: u64) -> bool {
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
