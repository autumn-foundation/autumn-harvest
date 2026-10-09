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
}
