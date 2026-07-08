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

use crate::policy::RetryPolicy;

/// Stable error-type name for circuit-breaker short-circuit failures (issue #369).
///
/// Synthesised when an activity's circuit breaker is open. Workflow authors
/// match on this in their `Err` arm to compensate, branch, or fail; operators
/// filter metrics by it. A circuit-open failure is always non-retryable for the
/// in-flight attempt.
pub const ERROR_TYPE_CIRCUIT_OPEN: &str = "CircuitOpen";

/// Stable error-type name for worker-session breakage (issue #606).
///
/// Synthesised by the broken-session scanner for every PENDING/RUNNING
/// member-activity task belonging to a session whose host worker died or
/// drained (or whose lease expired). Always non-retryable — a hard-pinned
/// session activity can never fail over to a different worker, so retrying
/// in place would loop forever against a dead host.
pub const ERROR_TYPE_SESSION_BROKEN: &str = "SessionBroken";

/// Stable error-type name for operator-forced activity failures (issue #765).
///
/// Synthesised by [`crate::timeout::force_fail_activity`] when an operator
/// force-fails a hung in-flight activity via the management API. Workflow
/// authors match on this in their `Err` arm (or via
/// [`HarvestError::is_operator_force_failed`](crate::error::HarvestError::is_operator_force_failed))
/// to tell an operator intervention apart from a genuine activity error.
/// Always non-retryable: the override skips every remaining retry attempt
/// regardless of retry policy.
///
/// This error type is engine-reserved for the operator force-fail endpoint:
/// activity code that fabricates it (returns an `ActivityFailure` carrying
/// this type itself) will make a later fail-now call misreport
/// `already_forced: true` instead of the documented `409` — harmless at the
/// state-machine level (the idempotent branch performs zero writes) but
/// misleading in the response.
pub const ERROR_TYPE_OPERATOR_FORCE_FAILED: &str = "OperatorForceFailed";

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

    /// Construct the non-retryable failure used when an activity's circuit
    /// breaker is open (issue #369).
    ///
    /// The `error_type` is always [`ERROR_TYPE_CIRCUIT_OPEN`] and the failure is
    /// non-retryable so the in-flight attempt terminates immediately instead of
    /// burning the retry curve against a downstream that is known to be down.
    /// `opened_at` is the wall-clock instant the breaker tripped (if known) and
    /// `retry_after` is how long until a half-open probe is admitted; both are
    /// carried in `details` so workflow code and operators can read them.
    ///
    /// `retry_after` is `None` when the breaker is operator-forced open: no
    /// probe is admitted on any timer, so callers must not derive a retry delay
    /// from this failure — recovery requires an explicit force-close. In that
    /// case `details.forced` is `true` and `retry_after_secs` is omitted.
    #[must_use]
    pub fn circuit_open(
        activity_name: &str,
        opened_at: Option<chrono::DateTime<chrono::Utc>>,
        retry_after: Option<std::time::Duration>,
    ) -> Self {
        let mut details = serde_json::json!({ "activity_name": activity_name });
        if let Some(opened) = opened_at {
            details["opened_at"] = serde_json::json!(opened.to_rfc3339());
        }
        let message = if let Some(after) = retry_after {
            let secs = after.as_secs_f64();
            details["retry_after_secs"] = serde_json::json!(secs);
            format!(
                "circuit breaker open for activity '{activity_name}'; \
                 retry after {secs:.1}s"
            )
        } else {
            // Operator-forced open: indefinite until force-close.
            details["forced"] = serde_json::json!(true);
            format!(
                "circuit breaker forced open for activity '{activity_name}'; \
                 no automatic probe — awaiting operator force-close"
            )
        };
        Self::non_retryable(ERROR_TYPE_CIRCUIT_OPEN, message).with_details(details)
    }

    /// Construct the non-retryable failure used when an operator force-fails
    /// a hung in-flight activity (issue #765).
    ///
    /// The `error_type` is always [`ERROR_TYPE_OPERATOR_FORCE_FAILED`] and the
    /// failure is non-retryable so every remaining retry attempt is skipped —
    /// the whole point of the override is to stop the retry curve dead and
    /// hand the outcome to the workflow's own failure/compensation path.
    /// The operator-supplied `reason` (if any) is appended to the message and
    /// carried in `details.reason` so workflow code and audit trails can read
    /// it back.
    ///
    /// [`ERROR_TYPE_OPERATOR_FORCE_FAILED`] is engine-reserved for the
    /// force-fail endpoint: activity code that fabricates this failure itself
    /// will make a later fail-now call misreport `already_forced: true`
    /// instead of the documented `409` (harmless — zero writes — but
    /// misleading).
    #[must_use]
    pub fn operator_force_failed(reason: Option<&str>) -> Self {
        let mut details = serde_json::json!({ "forced_by_operator": true });
        let message = reason.map_or_else(
            || "activity force-failed by operator".to_string(),
            |reason| {
                details["reason"] = serde_json::json!(reason);
                format!("activity force-failed by operator: {reason}")
            },
        );
        Self::non_retryable(ERROR_TYPE_OPERATOR_FORCE_FAILED, message).with_details(details)
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

/// Whether `error` is non-retryable under the shared retry-termination rule
/// (issue #227): the typed payload's own `non_retryable` flag, or the
/// resolved policy's `non_retryable_errors` list (which also matches legacy
/// `Err(String)` values the typed flag never sees). Used by the `db`-gated
/// live worker's retry/circuit-breaker classification (`worker.rs`); the
/// DB-less `WorkflowSimulator` test harness (`simulator.rs`) calls
/// [`classify_activity_error`] directly instead, so the two callers share
/// one rule without pulling `worker.rs` into a `--no-default-features` build.
#[cfg(feature = "db")]
#[must_use]
pub(crate) fn failure_is_non_retryable(error: &str, retry_policy: Option<&RetryPolicy>) -> bool {
    classify_activity_error(error, retry_policy).1
}

/// Parse `error` once and return both the recovered [`ActivityFailure`]
/// (equivalent to [`parse_error_payload_full`]) and whether it is
/// non-retryable (equivalent to [`failure_is_non_retryable`]), without
/// deserializing the wire payload twice.
#[must_use]
pub(crate) fn classify_activity_error(
    error: &str,
    retry_policy: Option<&RetryPolicy>,
) -> (ActivityFailure, bool) {
    let typed = parse_typed_payload(error);
    let non_retryable = typed.as_ref().is_some_and(|f| f.non_retryable)
        || retry_policy.is_some_and(|policy| {
            let typed_error_type = typed.as_ref().map(|f| f.error_type.as_str());
            policy.is_non_retryable(typed_error_type, error)
        });
    let failure = typed.unwrap_or_else(|| ActivityFailure {
        error_type: "Error".to_string(),
        message: error.to_string(),
        details: None,
        non_retryable: false,
    });
    (failure, non_retryable)
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

    #[test]
    fn circuit_open_is_non_retryable_typed_failure() {
        let f = ActivityFailure::circuit_open(
            "send_email",
            Some(chrono::Utc::now()),
            Some(std::time::Duration::from_secs(42)),
        );
        assert_eq!(f.error_type, ERROR_TYPE_CIRCUIT_OPEN);
        assert_eq!(f.error_type, "CircuitOpen");
        assert!(f.non_retryable, "circuit-open must skip retries");
        let details = f.details.expect("details carry the breaker context");
        assert_eq!(details["activity_name"], "send_email");
        assert!((details["retry_after_secs"].as_f64().unwrap() - 42.0).abs() < 0.001);
        assert!(details.get("opened_at").is_some());
        assert!(details.get("forced").is_none());
    }

    #[test]
    fn circuit_open_forced_omits_retry_after_and_flags_forced() {
        // An operator-forced-open breaker admits no probe on any timer, so the
        // failure must not advertise a retry-after callers could wait on.
        let f = ActivityFailure::circuit_open("send_email", None, None);
        assert!(f.non_retryable);
        let details = f.details.expect("details carry the breaker context");
        assert_eq!(details["forced"], true);
        assert!(
            details.get("retry_after_secs").is_none(),
            "forced-open must not advertise a retry-after"
        );
    }

    #[test]
    fn circuit_open_round_trips_through_wire_format() {
        // Replay safety: the synthesised failure must survive the same wire
        // envelope every other typed failure uses, so the recorded
        // ActivityFailed event reproduces the CircuitOpen outcome on replay.
        let payload = ActivityFailure::circuit_open(
            "charge_card",
            None,
            Some(std::time::Duration::from_secs(5)),
        )
        .into_error_payload();
        let (error_type, non_retryable, _) = parse_error_payload(&payload);
        assert_eq!(error_type, "CircuitOpen");
        assert!(non_retryable);
    }

    #[test]
    fn classify_activity_error_matches_parse_error_payload_full_for_legacy() {
        let (failure, non_retryable) = classify_activity_error("connection refused", None);
        assert_eq!(failure, parse_error_payload_full("connection refused"));
        assert!(!non_retryable);
    }

    #[test]
    fn classify_activity_error_matches_parse_error_payload_full_for_typed() {
        let payload =
            ActivityFailure::retryable("UpstreamTimeout", "gateway timed out").into_error_payload();
        let (failure, non_retryable) = classify_activity_error(&payload, None);
        assert_eq!(failure, parse_error_payload_full(&payload));
        assert!(!non_retryable);
    }

    #[test]
    fn classify_activity_error_honours_typed_non_retryable_flag() {
        let payload =
            ActivityFailure::non_retryable("InvalidInput", "bad request").into_error_payload();
        let (failure, non_retryable) = classify_activity_error(&payload, None);
        assert!(non_retryable);
        assert_eq!(failure.error_type, "InvalidInput");
    }

    #[test]
    fn classify_activity_error_honours_policy_non_retryable_errors_list() {
        let policy = crate::policy::RetryPolicy {
            max_attempts: 5,
            initial_interval: std::time::Duration::from_secs(1),
            backoff_coefficient: 1.0,
            max_interval: std::time::Duration::from_secs(1),
            non_retryable_errors: vec!["bad input".to_string()],
            jitter: crate::policy::JitterPolicy::None,
        };
        let (_, non_retryable) = classify_activity_error("bad input", Some(&policy));
        assert!(non_retryable);
    }

    #[test]
    fn classify_activity_error_legacy_never_matches_policy_error_synthetic_type() {
        // Regression guard for issue #227: a policy listing "Error" (the
        // synthetic legacy error_type) must not halt retries on every
        // untyped `Err(String)` failure.
        let policy = crate::policy::RetryPolicy {
            max_attempts: 5,
            initial_interval: std::time::Duration::from_secs(1),
            backoff_coefficient: 1.0,
            max_interval: std::time::Duration::from_secs(1),
            non_retryable_errors: vec!["Error".to_string()],
            jitter: crate::policy::JitterPolicy::None,
        };
        let (_, non_retryable) = classify_activity_error("some transient failure", Some(&policy));
        assert!(!non_retryable);
    }

    #[test]
    #[cfg(feature = "db")]
    fn failure_is_non_retryable_delegates_to_classify_activity_error() {
        let payload = ActivityFailure::non_retryable("X", "y").into_error_payload();
        assert!(failure_is_non_retryable(&payload, None));
        assert!(!failure_is_non_retryable("plain string", None));
    }

    // ── operator force-fail (issue #765) ──────────────────────────────────

    #[test]
    fn operator_force_failed_is_non_retryable_typed_failure() {
        let f = ActivityFailure::operator_force_failed(None);
        assert_eq!(f.error_type, ERROR_TYPE_OPERATOR_FORCE_FAILED);
        assert_eq!(f.error_type, "OperatorForceFailed");
        assert!(
            f.non_retryable,
            "operator force-fail must skip all remaining retries"
        );
        assert_eq!(f.message, "activity force-failed by operator");
    }

    #[test]
    fn operator_force_failed_appends_reason_to_message_and_details() {
        let f = ActivityFailure::operator_force_failed(Some("stuck on dead downstream"));
        assert_eq!(
            f.message,
            "activity force-failed by operator: stuck on dead downstream"
        );
        let details = f.details.expect("details carry the operator context");
        assert_eq!(details["forced_by_operator"], true);
        assert_eq!(details["reason"], "stuck on dead downstream");
    }

    #[test]
    fn operator_force_failed_without_reason_omits_reason_detail() {
        let f = ActivityFailure::operator_force_failed(None);
        let details = f.details.expect("details carry the operator context");
        assert_eq!(details["forced_by_operator"], true);
        assert!(
            details.get("reason").is_none(),
            "no reason given → no reason detail"
        );
    }

    #[test]
    fn operator_force_failed_round_trips_through_wire_format() {
        // Replay safety: the synthesised failure must survive the same wire
        // envelope every other typed failure uses, so the recorded
        // ActivityFailed event reproduces the OperatorForceFailed outcome on
        // replay and `parse_error_payload_full` (the exact decoder
        // `finalize_activity_failure` uses) recovers it losslessly.
        let payload =
            ActivityFailure::operator_force_failed(Some("incident INC-42")).into_error_payload();
        assert!(payload.contains("harvest_activity_failure_v1"));
        let full = parse_error_payload_full(&payload);
        assert_eq!(full.error_type, ERROR_TYPE_OPERATOR_FORCE_FAILED);
        assert!(full.non_retryable);
        assert_eq!(
            full.message,
            "activity force-failed by operator: incident INC-42"
        );
        assert_eq!(
            full.details.expect("details survive the envelope")["reason"],
            "incident INC-42"
        );
    }

    #[test]
    #[cfg(feature = "db")]
    fn operator_force_failed_is_non_retryable_regardless_of_retry_policy() {
        // The override stops retrying regardless of retry policy (issue #765
        // AC): even a generous policy that lists nothing as non-retryable must
        // classify the forced failure as terminal.
        let policy = crate::policy::RetryPolicy {
            max_attempts: 100,
            initial_interval: std::time::Duration::from_secs(1),
            backoff_coefficient: 2.0,
            max_interval: std::time::Duration::from_secs(60),
            non_retryable_errors: vec![],
            jitter: crate::policy::JitterPolicy::None,
        };
        let payload = ActivityFailure::operator_force_failed(None).into_error_payload();
        assert!(failure_is_non_retryable(&payload, Some(&policy)));
        assert!(failure_is_non_retryable(&payload, None));
    }
}
