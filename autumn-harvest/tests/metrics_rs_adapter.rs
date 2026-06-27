//! Tests for the `metrics-rs` adapter that bridges `MetricsRecorder` to the
//! `metrics` crate registry.
//!
//! Requires `features = ["metrics-rs"]`.

#![cfg(feature = "metrics-rs")]

use autumn_harvest::metrics_rs_adapter::MetricsRsRecorder;
use autumn_harvest::telemetry::{ActivityStatus, MetricsRecorder, TelemetryConfig, WorkflowStatus};
use std::sync::Arc;

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
