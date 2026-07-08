//! Tests for the `metrics-rs` adapter that bridges `MetricsRecorder` to the
//! `metrics` crate registry.
//!
//! Requires `features = ["metrics-rs"]`.

#![cfg(feature = "metrics-rs")]

use autumn_harvest::error::PayloadKind;
use autumn_harvest::metrics_rs_adapter::MetricsRsRecorder;
use autumn_harvest::telemetry::{
    ActivityStatus, MetricsRecorder, TelemetryConfig, WebhookOutcome, WorkflowStatus,
};
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use std::sync::{Arc, Mutex};

#[test]
fn metrics_rs_recorder_implements_metrics_recorder_trait() {
    // MetricsRsRecorder must implement MetricsRecorder and be object-safe.
    let recorder: Arc<dyn MetricsRecorder> = Arc::new(MetricsRsRecorder);
    // Calling all methods must not panic — the metrics crate routes to the
    // global recorder, which is a no-op sink unless explicitly installed.
    recorder.record_workflow_started("onboarding", "default");
    recorder.record_workflow_completed("onboarding", "default", 1.5, WorkflowStatus::Completed);
    recorder.record_activity_completed("send_email", "default", 0.3, ActivityStatus::Completed);
    recorder.record_timer_started(30.0);
    recorder.record_queue_depth("default", 5);
    recorder.record_dlq_entries(0, 2);
    recorder.record_schedule_run("workflow", "nightly");
    recorder.record_schedule_skipped("workflow", "nightly", "paused");
    recorder.record_retention_tick(0, 100, 50, 0.01);
    recorder.record_concurrency_key_in_flight("email-cap", 3);
    recorder.record_concurrency_key_deferred("email-cap", 1);
    recorder.record_schedule_to_start("default", 1.5);
    recorder.record_queue_oldest_pending_age("default", 30.0);
    // AC5 (issue #528): new activity outcome counters must be bridged.
    recorder.record_activity_attempt("charge_card", "billing", ActivityStatus::Completed);
    recorder.record_activity_attempt("charge_card", "billing", ActivityStatus::Failed);
    recorder.record_activity_retried("charge_card", "billing");
    // Inbound webhook receiver counters (issue #344) must be bridged.
    recorder.record_webhook_received("/hooks/orders", WebhookOutcome::Accepted);
    recorder.record_webhook_rejected("/hooks/orders", WebhookOutcome::VerifyFailed);
}

#[test]
fn metrics_rs_recorder_wires_into_telemetry_config() {
    // Verify that MetricsRsRecorder can be injected via the TelemetryConfig
    // builder — the same path an application would use.
    let telemetry = TelemetryConfig::builder()
        .metrics(Arc::new(MetricsRsRecorder))
        .build();

    // All record_* calls must not panic.
    telemetry
        .metrics
        .record_workflow_started("checkout", "priority");
    telemetry.metrics.record_workflow_completed(
        "checkout",
        "priority",
        0.8,
        WorkflowStatus::Failed,
    );
    telemetry.metrics.record_activity_completed(
        "charge_card",
        "priority",
        0.4,
        ActivityStatus::Failed,
    );
    telemetry.metrics.record_dlq_entries(0, 1);
}

// ---------------------------------------------------------------------------
// Bridge-coverage tests (issue #754 adapter bug fix).
//
// The adapter's module doc promises "All `METRIC_*` names from `telemetry.rs`
// are forwarded verbatim", but `record_workflow_timeout`,
// `record_payload_observed`, and `record_payload_rejected` previously fell
// through to the trait's no-op defaults, making the catalogued
// `harvest.workflow.timeout`, `harvest.payload.bytes`, and
// `harvest.payload.rejected` series unreachable from a Prometheus scrape.
// These tests install a capturing local recorder so a missing bridge fails
// (nothing registered), not just "does not panic".
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CapturingRecorder {
    keys: Mutex<Vec<Key>>,
}

impl CapturingRecorder {
    fn capture(&self, key: &Key) {
        self.keys
            .lock()
            .expect("capturing recorder mutex poisoned")
            .push(key.clone());
    }
}

impl Recorder for CapturingRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        self.capture(key);
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        self.capture(key);
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.capture(key);
        Histogram::noop()
    }
}

/// Runs `f` against a capturing local recorder and returns every metric key
/// the adapter registered during the call.
fn captured_keys(f: impl FnOnce()) -> Vec<Key> {
    let recorder = CapturingRecorder::default();
    metrics::with_local_recorder(&recorder, f);
    recorder
        .keys
        .into_inner()
        .expect("capturing recorder mutex poisoned")
}

fn labels_of(key: &Key) -> Vec<(&str, &str)> {
    key.labels().map(|l| (l.key(), l.value())).collect()
}

fn find_key<'a>(keys: &'a [Key], name: &str) -> &'a Key {
    keys.iter().find(|k| k.name() == name).unwrap_or_else(|| {
        panic!(
            "expected the adapter to register `{name}` (issue #754 bridge fix); registered keys: {:?}",
            keys.iter().map(|k| k.name().to_owned()).collect::<Vec<_>>()
        )
    })
}

#[test]
fn record_workflow_timeout_bridges_to_metrics_registry() {
    let keys = captured_keys(|| {
        MetricsRsRecorder.record_workflow_timeout("onboarding", "default");
    });
    let key = find_key(&keys, "harvest.workflow.timeout");
    let labels = labels_of(key);
    assert!(
        labels.contains(&("workflow", "onboarding")),
        "harvest.workflow.timeout must carry the workflow label, got {labels:?}"
    );
    assert!(
        labels.contains(&("queue", "default")),
        "harvest.workflow.timeout must carry the queue label, got {labels:?}"
    );
}

#[test]
fn record_payload_observed_bridges_histogram_with_activity_name() {
    let keys = captured_keys(|| {
        MetricsRsRecorder.record_payload_observed(
            &PayloadKind::ActivityInput,
            "onboarding",
            Some("send_email"),
            4096,
        );
    });
    let key = find_key(&keys, "harvest.payload.bytes");
    let labels = labels_of(key);
    assert!(
        labels.contains(&("payload.kind", "ActivityInput")),
        "harvest.payload.bytes must carry the payload.kind label, got {labels:?}"
    );
    assert!(
        labels.contains(&("workflow.type", "onboarding")),
        "harvest.payload.bytes must carry the workflow.type label, got {labels:?}"
    );
    assert!(
        labels.contains(&("activity.name", "send_email")),
        "harvest.payload.bytes must carry activity.name when present, got {labels:?}"
    );
}

#[test]
fn record_payload_observed_bridges_histogram_without_activity_name() {
    // Mirrors the record_external_signal_sent optional-label pattern: a None
    // activity omits the label entirely rather than emitting an empty value.
    let keys = captured_keys(|| {
        MetricsRsRecorder.record_payload_observed(
            &PayloadKind::WorkflowInput,
            "onboarding",
            None,
            1024,
        );
    });
    let key = find_key(&keys, "harvest.payload.bytes");
    let labels = labels_of(key);
    assert!(
        labels.contains(&("payload.kind", "WorkflowInput")),
        "harvest.payload.bytes must carry the payload.kind label, got {labels:?}"
    );
    assert!(
        labels.contains(&("workflow.type", "onboarding")),
        "harvest.payload.bytes must carry the workflow.type label, got {labels:?}"
    );
    assert!(
        !labels.iter().any(|(k, _)| *k == "activity.name"),
        "harvest.payload.bytes must omit activity.name when not applicable, got {labels:?}"
    );
}

#[test]
fn record_payload_rejected_bridges_counter() {
    let keys = captured_keys(|| {
        MetricsRsRecorder.record_payload_rejected(&PayloadKind::ActivityResult, "onboarding");
    });
    let key = find_key(&keys, "harvest.payload.rejected");
    let labels = labels_of(key);
    assert!(
        labels.contains(&("payload.kind", "ActivityResult")),
        "harvest.payload.rejected must carry the payload.kind label, got {labels:?}"
    );
    assert!(
        labels.contains(&("workflow.type", "onboarding")),
        "harvest.payload.rejected must carry the workflow.type label, got {labels:?}"
    );
}
