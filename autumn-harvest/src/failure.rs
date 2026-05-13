//! Typed activity failure surface for structured error classification.
//!
//! `ActivityFailure` lets activity authors signal whether a failure is
//! retryable and carry a stable error-type name that the engine and operators
//! can filter on without parsing human-readable message strings.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use autumn_harvest::failure::ActivityFailure;
//!
//! fn validate_order(total: i64) -> Result<(), ActivityFailure> {
//!     if total < 0 {
//!         // Permanent failure — skip retries immediately.
//!         return Err(ActivityFailure::non_retryable("InvalidInput", "order total is negative"));
//!     }
//!     Ok(())
//! }
//!
//! fn call_gateway(url: &str) -> Result<(), ActivityFailure> {
//!     // Transient failure — honour the activity's retry policy.
//!     Err(ActivityFailure::retryable("UpstreamTimeout", "payment gateway timed out"))
//! }
//! ```
//!
//! Activities that still return `Err(String)` continue to work unchanged;
//! the engine maps them to `error_type = "Error"` and `non_retryable = false`.

use serde::{Deserialize, Serialize};

/// Typed failure carrier for activity handlers.
///
/// ## Backward compatibility
///
/// `From<String>` produces a retryable `ActivityFailure` with
/// `error_type = "Error"`, so every activity that today returns
/// `Err(String)` continues to compile and behave identically.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivityFailure {
    /// A stable, low-cardinality error-type name used for metrics and policy
    /// matching (e.g. `"InvalidInput"`, `"RateLimitExceeded"`).
    pub error_type: String,
    /// Human-readable description of the failure.
    pub message: String,
    /// Optional structured details serialised alongside the failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    /// When `true` the worker skips all remaining retry attempts and routes
    /// the task directly to the DLQ.
    pub non_retryable: bool,
}

impl ActivityFailure {
    /// Transient failure: the activity's retry policy applies normally.
    pub fn retryable(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
            details: None,
            non_retryable: false,
        }
    }

    /// Permanent failure: skip all retry attempts and route to DLQ immediately.
    pub fn non_retryable(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
            details: None,
            non_retryable: true,
        }
    }

    /// Attach optional structured details to this failure.
    #[must_use]
    pub fn with_details(mut self, value: serde_json::Value) -> Self {
        self.details = Some(value);
        self
    }
}

impl std::fmt::Display for ActivityFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error_type, self.message)
    }
}

impl From<String> for ActivityFailure {
    fn from(message: String) -> Self {
        Self::retryable("Error", message)
    }
}

impl From<&str> for ActivityFailure {
    fn from(message: &str) -> Self {
        Self::retryable("Error", message)
    }
}

// ---------------------------------------------------------------------------
// Dispatch bridge
// ---------------------------------------------------------------------------

/// Trait that lets the macro-generated dispatch shim serialise both `String`
/// and `ActivityFailure` errors into the engine's wire format without runtime
/// type-checking.
///
/// `String` passes through unchanged (legacy path).
/// `ActivityFailure` is serialised to JSON so the engine can recover
/// `error_type` and `non_retryable` when writing the `ActivityFailed` event.
pub trait IntoActivityErrorString {
    /// Convert this error into the string payload carried on the engine's
    /// internal `Result<serde_json::Value, String>` boundary.
    fn into_error_payload(self) -> String;
}

impl IntoActivityErrorString for String {
    fn into_error_payload(self) -> String {
        self
    }
}

impl IntoActivityErrorString for ActivityFailure {
    fn into_error_payload(self) -> String {
        serde_json::to_string(&self).unwrap_or_else(|_| self.to_string())
    }
}

/// Parse an error payload string, returning `(error_type, non_retryable,
/// human_readable_message)`.
///
/// If the payload is valid `ActivityFailure` JSON it is decoded; otherwise the
/// legacy fallback of `error_type = "Error"` and `non_retryable = false` is
/// used, and the payload is returned as-is for the human-readable field.
#[must_use]
pub fn parse_error_payload(payload: &str) -> (String, bool, String) {
    if let Ok(failure) = serde_json::from_str::<ActivityFailure>(payload) {
        (failure.error_type, failure.non_retryable, failure.message)
    } else {
        ("Error".to_string(), false, payload.to_string())
    }
}

// ---------------------------------------------------------------------------
// Tests (red phase: written before implementation compiles)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_sets_non_retryable_false() {
        let f = ActivityFailure::retryable("UpstreamTimeout", "gateway timed out");
        assert_eq!(f.error_type, "UpstreamTimeout");
        assert_eq!(f.message, "gateway timed out");
        assert!(!f.non_retryable);
        assert!(f.details.is_none());
    }

    #[test]
    fn non_retryable_sets_flag_true() {
        let f = ActivityFailure::non_retryable("InvalidInput", "order total is negative");
        assert_eq!(f.error_type, "InvalidInput");
        assert!(f.non_retryable);
    }

    #[test]
    fn with_details_attaches_payload() {
        let f = ActivityFailure::non_retryable("X", "msg")
            .with_details(serde_json::json!({"code": 404}));
        assert_eq!(f.details, Some(serde_json::json!({"code": 404})));
    }

    #[test]
    fn display_returns_type_colon_message() {
        let f = ActivityFailure::non_retryable("InvalidInput", "bad value");
        assert_eq!(f.to_string(), "InvalidInput: bad value");
    }

    #[test]
    fn from_string_produces_retryable_error_type() {
        let f = ActivityFailure::from("network error".to_string());
        assert_eq!(f.error_type, "Error");
        assert_eq!(f.message, "network error");
        assert!(!f.non_retryable);
    }

    #[test]
    fn from_str_produces_retryable_error_type() {
        let f = ActivityFailure::from("network error");
        assert_eq!(f.error_type, "Error");
        assert!(!f.non_retryable);
    }

    #[test]
    fn serde_round_trip() {
        let original = ActivityFailure::non_retryable("PermanentValidation", "bad data")
            .with_details(serde_json::json!({"field": "amount"}));
        let json = serde_json::to_string(&original).unwrap();
        let back: ActivityFailure = serde_json::from_str(&json).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn into_error_payload_for_string_is_passthrough() {
        let s = "simple error".to_string();
        assert_eq!(s.into_error_payload(), "simple error");
    }

    #[test]
    fn into_error_payload_for_activity_failure_is_json() {
        let f = ActivityFailure::non_retryable("X", "y");
        let payload = f.into_error_payload();
        // Should round-trip back to ActivityFailure
        let back: ActivityFailure = serde_json::from_str(&payload).unwrap();
        assert_eq!(back.error_type, "X");
        assert!(back.non_retryable);
    }

    #[test]
    fn parse_error_payload_decodes_activity_failure_json() {
        let payload =
            ActivityFailure::non_retryable("RateLimit", "too many requests").into_error_payload();
        let (error_type, non_retryable, message) = parse_error_payload(&payload);
        assert_eq!(error_type, "RateLimit");
        assert!(non_retryable);
        assert_eq!(message, "too many requests");
    }

    #[test]
    fn parse_error_payload_legacy_string_gives_error_type() {
        let (error_type, non_retryable, message) = parse_error_payload("connection refused");
        assert_eq!(error_type, "Error");
        assert!(!non_retryable);
        assert_eq!(message, "connection refused");
    }

    #[test]
    fn parse_error_payload_retryable_failure_has_non_retryable_false() {
        let payload = ActivityFailure::retryable("Transient", "retry me").into_error_payload();
        let (_, non_retryable, _) = parse_error_payload(&payload);
        assert!(!non_retryable);
    }
}
