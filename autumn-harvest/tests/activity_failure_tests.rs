//! Tests for the typed activity failure surface (issue #227).
//!
//! These tests exercise:
//! - `ActivityFailure` constructors, `Display`, `From<String>`, serde
//! - `IntoActivityErrorString` dispatch bridge
//! - `ActivityFailed` event backward-compat deserialization (old JSON with no
//!   `error_type` / `non_retryable` fields)
//! - `parse_error_payload` helper used by worker + scheduler retry logic
//! - Replay of a pre-#227 `ActivityFailed` history via `WorkflowReplayer`
//! - `MetricsRecorder::record_activity_failed` new counter

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString, parse_error_payload};
use autumn_harvest::types::ActivityExecId;
use serde_json::json;

// ---------------------------------------------------------------------------
// ActivityFailure unit tests
// ---------------------------------------------------------------------------

#[test]
fn retryable_failure_has_false_flag() {
    let f = ActivityFailure::retryable("Transient", "gateway timeout");
    assert_eq!(f.error_type, "Transient");
    assert!(!f.non_retryable);
}

#[test]
fn non_retryable_failure_has_true_flag() {
    let f = ActivityFailure::non_retryable("PermanentValidation", "amount is negative");
    assert_eq!(f.error_type, "PermanentValidation");
    assert!(f.non_retryable);
}

#[test]
fn with_details_stores_payload() {
    let f = ActivityFailure::non_retryable("X", "y")
        .with_details(json!({"field": "amount", "got": -1}));
    assert_eq!(f.details.unwrap()["field"], "amount");
}

#[test]
fn display_is_type_colon_message() {
    let f = ActivityFailure::non_retryable("InvalidInput", "bad value");
    assert_eq!(format!("{f}"), "InvalidInput: bad value");
}

#[test]
fn from_string_backwards_compat() {
    let f = ActivityFailure::from("network error".to_string());
    assert_eq!(f.error_type, "Error");
    assert!(!f.non_retryable);
    assert_eq!(f.message, "network error");
}

#[test]
fn serde_round_trip_with_details() {
    let original = ActivityFailure::non_retryable("PaymentDeclined", "card expired")
        .with_details(json!({"code": "card_expired"}));
    let json = serde_json::to_string(&original).unwrap();
    let back: ActivityFailure = serde_json::from_str(&json).unwrap();
    assert_eq!(back, original);
}

#[test]
fn serde_round_trip_without_details_omits_field() {
    let f = ActivityFailure::retryable("Retry", "try again");
    let json = serde_json::to_string(&f).unwrap();
    // details field should be absent (skip_serializing_if)
    assert!(!json.contains("details"));
}

// ---------------------------------------------------------------------------
// IntoActivityErrorString dispatch bridge
// ---------------------------------------------------------------------------

#[test]
fn string_passthrough() {
    let s = "plain error string".to_string();
    assert_eq!(s.into_error_payload(), "plain error string");
}

#[test]
fn activity_failure_payload_carries_versioned_discriminator() {
    let f = ActivityFailure::non_retryable("RateLimit", "too many requests");
    let payload = f.into_error_payload();
    // Wire format is wrapped in `harvest_activity_failure_v1` so it can
    // never collide with a legacy activity that happens to return a JSON-
    // shaped error string. Use `parse_error_payload` to recover the fields.
    assert!(payload.contains("harvest_activity_failure_v1"));
    let (error_type, non_retryable, _) = parse_error_payload(&payload);
    assert_eq!(error_type, "RateLimit");
    assert!(non_retryable);
}

// ---------------------------------------------------------------------------
// parse_error_payload helper
// ---------------------------------------------------------------------------

#[test]
fn parse_activity_failure_json_payload() {
    let payload = ActivityFailure::non_retryable("Auth", "forbidden").into_error_payload();
    let (error_type, non_retryable, message) = parse_error_payload(&payload);
    assert_eq!(error_type, "Auth");
    assert!(non_retryable);
    assert_eq!(message, "forbidden");
}

#[test]
fn parse_legacy_string_payload() {
    let (error_type, non_retryable, message) = parse_error_payload("connection refused");
    assert_eq!(error_type, "Error");
    assert!(!non_retryable);
    assert_eq!(message, "connection refused");
}

#[test]
fn parse_retryable_activity_failure() {
    let payload = ActivityFailure::retryable("Timeout", "upstream slow").into_error_payload();
    let (_, non_retryable, _) = parse_error_payload(&payload);
    assert!(!non_retryable);
}

// ---------------------------------------------------------------------------
// ActivityFailed event: new fields and backward compat
// ---------------------------------------------------------------------------

#[test]
fn activity_failed_event_new_fields_round_trip() {
    let id = ActivityExecId::new();
    let event = WorkflowEvent::ActivityFailed {
        activity_id: id,
        error: "InvalidInput: bad amount".into(),
        attempt: 1,
        error_type: "InvalidInput".into(),
        non_retryable: true,
        details: None,
    };
    let json = serde_json::to_string(&event).unwrap();
    let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
    match back {
        WorkflowEvent::ActivityFailed {
            error_type,
            non_retryable,
            attempt,
            ..
        } => {
            assert_eq!(error_type, "InvalidInput");
            assert!(non_retryable);
            assert_eq!(attempt, 1);
        }
        _ => panic!("wrong variant"),
    }
}

/// `ActivityFailed` now carries the structured `details` payload so consumers
/// reading workflow history (replay diagnostics, history exports, etc.) can
/// recover what `ActivityFailure::with_details(...)` attached at the call site.
#[test]
fn activity_failed_event_preserves_details_round_trip() {
    use autumn_harvest::failure::parse_error_payload_full;

    let failure = ActivityFailure::non_retryable("PaymentDeclined", "card expired")
        .with_details(json!({"code": "card_expired", "field": "amount"}));
    let payload = failure.into_error_payload();
    let parsed = parse_error_payload_full(&payload);
    assert_eq!(parsed.error_type, "PaymentDeclined");
    assert_eq!(
        parsed.details,
        Some(json!({"code": "card_expired", "field": "amount"})),
    );

    let event = WorkflowEvent::ActivityFailed {
        activity_id: ActivityExecId::new(),
        error: parsed.message,
        attempt: 1,
        error_type: parsed.error_type,
        non_retryable: parsed.non_retryable,
        details: parsed.details,
    };
    let json = serde_json::to_string(&event).unwrap();
    let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
    match back {
        WorkflowEvent::ActivityFailed { details, .. } => {
            assert_eq!(
                details,
                Some(json!({"code": "card_expired", "field": "amount"}))
            );
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn activity_failed_event_old_json_gets_defaults() {
    // Simulates a pre-#227 event stored in harvest_events.event_data.
    let old_json = r#"{"type":"ActivityFailed","data":{"activity_id":"00000000-0000-0000-0000-000000000001","error":"timeout","attempt":3}}"#;
    let back: WorkflowEvent = serde_json::from_str(old_json).expect("must deserialize old format");
    match back {
        WorkflowEvent::ActivityFailed {
            error,
            attempt,
            error_type,
            non_retryable,
            details,
            ..
        } => {
            assert_eq!(error, "timeout");
            assert_eq!(attempt, 3);
            assert_eq!(error_type, "Error");
            assert!(!non_retryable);
            // New `details` field also serde-defaults to None for pre-#227 events.
            assert!(details.is_none());
        }
        _ => panic!("wrong variant"),
    }
}

// ---------------------------------------------------------------------------
// Replay harness: old-format ActivityFailed history replays successfully
// (requires "testing" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "testing")]
mod replay_tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;

    use autumn_harvest::context::WorkflowContext;
    use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};
    use autumn_harvest::types::ExecutionId;
    use chrono::Utc;
    use serde_json::Value;

    fn failing_workflow<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.execute_activity_raw("do_work", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
            unreachable!("activity always fails in these tests")
        })
    }

    /// Verify that an `ActivityFailed` event with the new `error_type` /
    /// `non_retryable` fields (both at their serde defaults) replays through the
    /// `WorkflowReplayer` without panicking.  The history ends with
    /// `ActivityFailed` and no terminal event, so the report is `WorkflowFailed`
    /// — that is the correct, expected outcome.
    #[tokio::test]
    async fn old_activity_failed_history_replays_without_panic() {
        let exec_id = ExecutionId::new();
        let act_id = ActivityExecId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: act_id,
                name: "do_work".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            // Fields introduced in #227; both carry their serde-default values,
            // matching what legacy stored events produce after deserialization.
            WorkflowEvent::ActivityFailed {
                activity_id: act_id,
                error: "connection refused".into(),
                attempt: 1,
                error_type: "Error".into(),
                non_retryable: false,
                details: None,
            },
        ];

        let snapshot = HistorySnapshot {
            workflow_name: "test_wf".into(),
            execution_id: exec_id,
            events,
        };

        let report = WorkflowReplayer::new()
            .register_fn("test_wf", failing_workflow)
            .replay_from_snapshot(snapshot)
            .await;

        // The activity failed and the workflow propagated the error — WorkflowFailed
        // is the correct outcome.  The important invariant is that the replayer
        // returned a structured report rather than panicking.
        assert!(
            matches!(report.status, ReplayStatus::WorkflowFailed { .. }),
            "expected WorkflowFailed (activity failed in history), got: {report:?}"
        );
    }

    /// Verify that pre-#227 JSON (no `error_type` / `non_retryable` fields on
    /// `ActivityFailed`) deserialises via serde defaults and the `WorkflowReplayer`
    /// processes it without panicking.
    #[tokio::test]
    async fn old_activity_failed_json_fixture_replays_cleanly() {
        // Old JSON without error_type / non_retryable — serde fills in defaults.
        let fixture_json = r#"{
            "workflow_name": "test_wf",
            "execution_id": "00000000-0000-0000-0000-000000000002",
            "events": [
                {"type":"WorkflowStarted","data":{"input":null,"timestamp":"2026-01-01T00:00:00Z"}},
                {"type":"ActivityScheduled","data":{"activity_id":"00000000-0000-0000-0000-000000000003","name":"do_work","input":null,"queue":"default"}},
                {"type":"ActivityFailed","data":{"activity_id":"00000000-0000-0000-0000-000000000003","error":"timeout","attempt":1}}
            ]
        }"#;

        let report = WorkflowReplayer::new()
            .register_fn("test_wf", failing_workflow)
            .replay_from_json(fixture_json)
            .await
            .expect("pre-#227 JSON fixture must deserialise via serde defaults");

        // Old JSON parses and the replayer returns a structured WorkflowFailed
        // report — not a panic, not a deserialization error.
        assert!(
            matches!(report.status, ReplayStatus::WorkflowFailed { .. }),
            "pre-#227 JSON fixture replay must not panic: {report:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Metrics: record_activity_failed counter
// ---------------------------------------------------------------------------

use autumn_harvest::telemetry::MetricsRecorder;
use std::sync::{Arc, Mutex};

type FailureCall = (String, String, String, bool);

/// Test recorder that counts `record_activity_failed` invocations.
#[derive(Default, Clone)]
struct FailureCounter {
    calls: Arc<Mutex<Vec<FailureCall>>>,
}

impl MetricsRecorder for FailureCounter {
    fn record_activity_failed(
        &self,
        activity_name: &str,
        workflow_type: &str,
        error_type: &str,
        non_retryable: bool,
    ) {
        self.calls.lock().unwrap().push((
            activity_name.to_string(),
            workflow_type.to_string(),
            error_type.to_string(),
            non_retryable,
        ));
    }
}

#[test]
fn record_activity_failed_receives_error_type() {
    let recorder = FailureCounter::default();
    recorder.record_activity_failed("charge_card", "billing_workflow", "PaymentDeclined", true);
    let snapshot: Vec<FailureCall> = recorder.calls.lock().unwrap().clone();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].0, "charge_card");
    assert_eq!(snapshot[0].1, "billing_workflow");
    assert_eq!(snapshot[0].2, "PaymentDeclined");
    assert!(snapshot[0].3);
}

#[test]
fn default_no_op_recorder_accepts_activity_failed() {
    use autumn_harvest::telemetry::NoOpMetrics;
    // Should not panic; default impl is a no-op.
    NoOpMetrics.record_activity_failed("my_activity", "my_workflow", "Error", false);
}
