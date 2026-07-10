//! Property tests for [`autumn_harvest::WorkflowEvent`] serde (Tier B targets 8
//! & 9).
//!
//! `WorkflowEvent` is `#[serde(tag = "type", content = "data")]` and does *not*
//! derive `PartialEq`, so round-trip fidelity is asserted by comparing the JSON
//! `Value` of the original against the JSON `Value` of the re-parsed event.
//!
//! Contract under test:
//! - every generated variant round-trips (`to_json(ev) == to_json(from_str(to_string(ev)))`);
//! - the adjacently-tagged `{"type", "data"}` envelope is always present;
//! - **back-compat**: pre-#521 `ExternalSignalRequested` JSON (no
//!   `idempotency_key`) and pre-#488 `WorkflowStarted` JSON (no
//!   `last_completion_result` / `last_error` / `scheduled_time`) deserialize with
//!   those additive fields defaulting to `None`.

use autumn_harvest::WorkflowEvent;
use autumn_harvest::event::SideEffectKind;
use autumn_harvest::types::{ActivityExecId, ExecutionId, ExternalSignalId, TimerId};
use chrono::{DateTime, Utc};
use proptest::prelude::*;
use serde_json::Value;
use uuid::Uuid;

use super::prop_config::config;

fn uuid() -> impl Strategy<Value = Uuid> {
    proptest::array::uniform16(any::<u8>()).prop_map(Uuid::from_bytes)
}

fn text() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_.:-]{0,16}"
}

fn json_leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        text().prop_map(Value::from),
    ]
}

fn json_value() -> impl Strategy<Value = Value> {
    json_leaf().prop_recursive(3, 12, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..3).prop_map(Value::from),
            proptest::collection::hash_map(text(), inner, 0..3)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

/// A JSON value that is never top-level `Null`.
///
/// Required for the `Option<serde_json::Value>` fields carrying
/// `#[serde(skip_serializing_if = "Option::is_none")]` (`last_completion_result`,
/// `details`): `Some(Value::Null)` serializes to `null`, which serde's `Option`
/// deserializer collapses back to `None` — an inherent `Option<Value>` idiom, not
/// a round-trip defect — so re-serialization would omit the field. Excluding
/// top-level `Null` keeps the generator to shapes the round-trip contract
/// actually covers. (Nested nulls inside an array/object round-trip fine and are
/// still generated.)
///
/// Note: the engine CAN legitimately produce `Some(Value::Null)` for
/// `last_completion_result` (issue #488 — when a prior scheduled run's output is
/// JSON `null`), which then benignly collapses to `None` on reload. That is a
/// real but harmless serde-`Option` quirk this property intentionally cannot
/// cover, by construction of this generator.
fn json_value_non_null() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        text().prop_map(Value::from),
        proptest::collection::vec(json_value(), 0..3).prop_map(Value::from),
        proptest::collection::hash_map(text(), json_value(), 0..3)
            .prop_map(|m| Value::Object(m.into_iter().collect())),
    ]
}

fn timestamp() -> impl Strategy<Value = DateTime<Utc>> {
    (0i64..4_000_000_000, 0u32..1_000_000_000)
        .prop_map(|(s, n)| DateTime::from_timestamp(s, n).expect("valid timestamp"))
}

fn side_effect_kind() -> impl Strategy<Value = SideEffectKind> {
    prop_oneof![
        Just(SideEffectKind::Now),
        Just(SideEffectKind::Uuid),
        Just(SideEffectKind::Random),
        Just(SideEffectKind::Custom),
    ]
}

/// A representative cross-section of `WorkflowEvent` variants with real newtype
/// fields, additive optional fields exercised both present and absent.
fn workflow_event() -> impl Strategy<Value = WorkflowEvent> {
    prop_oneof![
        (
            json_value(),
            timestamp(),
            proptest::option::of(json_value_non_null()),
            proptest::option::of(text()),
            proptest::option::of(timestamp()),
        )
            .prop_map(|(input, timestamp, lcr, lerr, sched)| {
                WorkflowEvent::WorkflowStarted {
                    input,
                    timestamp,
                    last_completion_result: lcr,
                    last_error: lerr,
                    scheduled_time: sched,
                }
            }),
        (uuid(), text(), json_value(), text()).prop_map(|(id, name, input, queue)| {
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::from_uuid(id),
                name,
                input,
                queue,
            }
        }),
        (uuid(), json_value()).prop_map(|(id, output)| WorkflowEvent::ActivityCompleted {
            activity_id: ActivityExecId::from_uuid(id),
            output,
        }),
        (
            uuid(),
            text(),
            any::<u32>(),
            text(),
            any::<bool>(),
            proptest::option::of(json_value_non_null()),
        )
            .prop_map(|(id, error, attempt, error_type, non_retryable, details)| {
                WorkflowEvent::ActivityFailed {
                    activity_id: ActivityExecId::from_uuid(id),
                    error,
                    attempt,
                    error_type,
                    non_retryable,
                    details,
                }
            }),
        (text(), any::<u64>()).prop_map(|(tid, secs)| WorkflowEvent::TimerStarted {
            timer_id: TimerId::new(tid),
            duration_secs: secs,
        }),
        text().prop_map(|tid| WorkflowEvent::TimerFired {
            timer_id: TimerId::new(tid),
        }),
        (text(), json_value()).prop_map(|(signal_name, payload)| {
            WorkflowEvent::SignalReceived {
                signal_name,
                payload,
            }
        }),
        (text(), json_value())
            .prop_map(|(name, details)| WorkflowEvent::MarkerRecorded { name, details }),
        (
            side_effect_kind(),
            proptest::option::of(text()),
            json_value()
        )
            .prop_map(|(kind, name, value)| WorkflowEvent::SideEffectRecorded {
                kind,
                name,
                value
            }),
        (
            uuid(),
            uuid(),
            text(),
            json_value(),
            proptest::option::of(text()),
        )
            .prop_map(|(sid, target, signal_name, payload, idem)| {
                WorkflowEvent::ExternalSignalRequested {
                    signal_id: ExternalSignalId::from_uuid(sid),
                    target: ExecutionId::from_uuid(target),
                    signal_name,
                    payload,
                    idempotency_key: idem,
                }
            }),
    ]
}

proptest! {
    #![proptest_config(config())]

    /// Every generated variant round-trips through JSON with a stable value, and
    /// carries the adjacently-tagged `{"type", "data"}` envelope.
    #[test]
    fn workflow_event_round_trips(ev in workflow_event()) {
        let s = serde_json::to_string(&ev).expect("serialize");
        let back: WorkflowEvent = serde_json::from_str(&s).expect("deserialize");

        let original = serde_json::to_value(&ev).expect("to_value");
        let reparsed = serde_json::to_value(&back).expect("to_value");
        prop_assert_eq!(&original, &reparsed, "round-trip changed the JSON value");

        prop_assert!(original.get("type").and_then(Value::as_str).is_some(), "missing type tag");
        prop_assert!(original.get("data").is_some(), "missing data envelope");
    }

    /// Back-compat: a pre-#521 `ExternalSignalRequested` (data without an
    /// `idempotency_key`) deserializes with `idempotency_key == None`.
    #[test]
    fn external_signal_requested_backcompat(
        sid in uuid(),
        target in uuid(),
        signal_name in text(),
        payload in json_value(),
    ) {
        let ev = WorkflowEvent::ExternalSignalRequested {
            signal_id: ExternalSignalId::from_uuid(sid),
            target: ExecutionId::from_uuid(target),
            signal_name,
            payload,
            idempotency_key: Some("present".to_string()),
        };
        let mut json = serde_json::to_value(&ev).expect("to_value");
        // Strip the additive field to simulate a pre-#521 stored event.
        json.get_mut("data")
            .and_then(Value::as_object_mut)
            .expect("data object")
            .remove("idempotency_key");

        let back: WorkflowEvent = serde_json::from_value(json).expect("deserialize legacy");
        prop_assert!(
            matches!(
                back,
                WorkflowEvent::ExternalSignalRequested { idempotency_key: None, .. }
            ),
            "legacy ExternalSignalRequested did not default idempotency_key to None: {back:?}"
        );
    }

    /// Back-compat: a pre-#488 `WorkflowStarted` (data without
    /// `last_completion_result` / `last_error` / `scheduled_time`) deserializes
    /// with all three defaulting to `None`.
    #[test]
    fn workflow_started_backcompat(input in json_value(), ts in timestamp()) {
        let ev = WorkflowEvent::WorkflowStarted {
            input,
            timestamp: ts,
            last_completion_result: Some(serde_json::json!({"prior": 1})),
            last_error: Some("boom".to_string()),
            scheduled_time: Some(ts),
        };
        let mut json = serde_json::to_value(&ev).expect("to_value");
        let data = json
            .get_mut("data")
            .and_then(Value::as_object_mut)
            .expect("data object");
        data.remove("last_completion_result");
        data.remove("last_error");
        data.remove("scheduled_time");

        let back: WorkflowEvent = serde_json::from_value(json).expect("deserialize legacy");
        prop_assert!(
            matches!(
                back,
                WorkflowEvent::WorkflowStarted {
                    last_completion_result: None,
                    last_error: None,
                    scheduled_time: None,
                    ..
                }
            ),
            "legacy WorkflowStarted did not default the additive fields to None: {back:?}"
        );
    }
}
