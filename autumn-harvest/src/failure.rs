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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        // ActivityFailure has no maps with non-string keys, so to_string never
        // fails — but if it ever does, fall back to the Display string of the
        // inner failure rather than panicking on the worker hot path.
        let fallback = self.to_string();
        serde_json::to_string(&WirePayload::ActivityFailureV1(self)).unwrap_or(fallback)
    }
}

/// Wire-format envelope for `ActivityFailure` payloads.
///
/// The explicit `harvest_activity_failure_v1` discriminator (`#[serde(tag)]`
/// via an enum variant name) prevents collision with legacy activities that
/// happen to return JSON-shaped error strings. Only payloads emitted by
/// `IntoActivityErrorString` are routed through the typed path; every other
/// string — even one that looks like an `ActivityFailure` JSON object —
/// stays on the legacy `error_type = "Error"`, `non_retryable = false`
/// fallback.
///
/// `v1` leaves room to add a `v2` variant without breaking stored events.
#[derive(serde::Serialize, serde::Deserialize)]
enum WirePayload {
    #[serde(rename = "harvest_activity_failure_v1")]
    ActivityFailureV1(ActivityFailure),
}

/// Parse an error payload string, returning `(error_type, non_retryable,
/// human_readable_message)`.
///
/// If the payload is a well-formed wire envelope produced by
/// [`IntoActivityErrorString`], decode it. Any other string — including
/// JSON that happens to share `ActivityFailure`'s field shape — is treated
/// as a legacy payload: `error_type = "Error"`, `non_retryable = false`,
/// and the human-readable message is the payload verbatim.
#[must_use]
pub fn parse_error_payload(payload: &str) -> (String, bool, String) {
    let failure = parse_error_payload_full(payload);
    (failure.error_type, failure.non_retryable, failure.message)
}

/// Returns `Some(ActivityFailure)` only for typed-wire-format payloads.
///
/// Returns `None` for legacy plain-string payloads (no `harvest_activity_failure_v1`
/// envelope).
///
/// Use this for **retry-policy decisions**: callers must not consult the
/// synthetic `error_type = "Error"` that `parse_error_payload_full` returns
/// for legacy payloads, because a pre-existing
/// `RetryPolicy::non_retryable_errors` entry of `"Error"` would otherwise
/// silently halt retries on every legacy `Err(String)` failure, breaking
/// the back-compat guarantee promised in issue #227.
#[must_use]
pub fn parse_typed_payload(payload: &str) -> Option<ActivityFailure> {
    match serde_json::from_str::<WirePayload>(payload) {
        Ok(WirePayload::ActivityFailureV1(failure)) => Some(failure),
        Err(_) => None,
    }
}

/// Like [`parse_error_payload`] but returns the full [`ActivityFailure`] so
/// callers can also recover the structured `details` value.
///
/// Use this when persisting a failure into an event whose schema carries
/// `details` (e.g. `WorkflowEvent::ActivityFailed`). Legacy payloads fall
/// back to `error_type = "Error"`, `non_retryable = false`, `details = None`.
#[must_use]
pub fn parse_error_payload_full(payload: &str) -> ActivityFailure {
    if let Ok(WirePayload::ActivityFailureV1(failure)) =
        serde_json::from_str::<WirePayload>(payload)
    {
        failure
    } else {
        ActivityFailure {
            error_type: "Error".to_string(),
            message: payload.to_string(),
            details: None,
            non_retryable: false,
        }
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
    fn into_error_payload_for_activity_failure_is_versioned_envelope() {
        let f = ActivityFailure::non_retryable("X", "y");
        let payload = f.into_error_payload();
        // The wire format carries an explicit discriminator so legacy
        // JSON-shaped error strings can never be misread as a typed failure.
        assert!(payload.contains("harvest_activity_failure_v1"));
        // And it round-trips through `parse_error_payload`.
        let (error_type, non_retryable, _) = parse_error_payload(&payload);
        assert_eq!(error_type, "X");
        assert!(non_retryable);
    }

    #[test]
    fn parse_error_payload_rejects_legacy_activity_failure_shaped_json() {
        // A pre-#227 activity could return a JSON string that happens to share
        // the `ActivityFailure` field shape. Without the wire-format
        // discriminator we would silently treat it as a typed failure and
        // (possibly) skip retries; with the discriminator it stays on the
        // legacy fallback.
        let look_alike = r#"{"error_type":"InvalidInput","message":"x","non_retryable":true}"#;
        let (error_type, non_retryable, message) = parse_error_payload(look_alike);
        assert_eq!(error_type, "Error");
        assert!(!non_retryable);
        assert_eq!(message, look_alike);
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
