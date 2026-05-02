//! [`metrics`](https://docs.rs/metrics) crate adapter for [`MetricsRecorder`].
//!
//! Bridges the harvest engine's [`MetricsRecorder`] trait to the `metrics`
//! crate facade. Applications that already use `metrics-exporter-prometheus`
//! or another `metrics`-compatible backend can wire this in with two lines:
//!
//! ```rust,ignore
//! use autumn_harvest::metrics_rs_adapter::MetricsRsRecorder;
//! use autumn_harvest::telemetry::TelemetryConfig;
//! use std::sync::Arc;
//!
//! let telemetry = TelemetryConfig::builder()
//!     .metrics(Arc::new(MetricsRsRecorder))
//!     .build();
//! ```
//!
//! # Prometheus / OTLP recipe
//!
//! ```rust,ignore
//! // 1. Install your preferred exporter (e.g. prometheus).
//! metrics_exporter_prometheus::PrometheusBuilder::new()
//!     .install()
//!     .expect("failed to install Prometheus exporter");
//!
//! // 2. Wrap the harvest builder with MetricsRsRecorder.
//! let telemetry = TelemetryConfig::builder()
//!     .metrics(Arc::new(MetricsRsRecorder))
//!     .build();
//!
//! // 3. Pass it to HarvestBuilder.
//! let harvest = HarvestBuilder::new(pool)
//!     .telemetry(telemetry)
//!     // ... other config
//!     .build();
//! ```
//!
//! All `METRIC_*` names from `telemetry.rs` are forwarded verbatim so
//! Prometheus metric names follow the standard dot-to-underscore conversion
//! applied by `metrics-exporter-prometheus` (e.g.
//! `harvest_workflow_started_total`).
//!
//! Enabled by the `metrics-rs` cargo feature.

use metrics::{counter, gauge, histogram};

use crate::telemetry::{
    ActivityStatus, METRIC_ACTIVITY_DURATION, METRIC_DLQ_ENTRIES, METRIC_LABEL_ACTIVITY,
    METRIC_LABEL_KEY, METRIC_LABEL_KIND, METRIC_LABEL_NAME, METRIC_LABEL_QUEUE, METRIC_LABEL_REASON,
    METRIC_LABEL_SHARD, METRIC_LABEL_STATUS, METRIC_LABEL_WORKFLOW, METRIC_QUEUE_DEPTH,
    METRIC_RETENTION_DELETED, METRIC_SCHEDULE_RUNS, METRIC_SCHEDULE_SKIPPED, METRIC_TIMER_DURATION,
    METRIC_TIMER_STARTED, METRIC_WORKFLOW_DURATION, METRIC_WORKFLOW_STARTED, MetricsRecorder,
    WorkflowStatus,
};

/// [`MetricsRecorder`] implementation that forwards every sample to the
/// global [`metrics`] registry.
///
/// Install a compatible exporter (e.g. `metrics-exporter-prometheus`) before
/// any harvest worker starts; the recorder itself is stateless.
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsRsRecorder;

impl MetricsRecorder for MetricsRsRecorder {
    fn record_workflow_started(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_STARTED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_completed(
        &self,
        workflow_name: &str,
        queue: &str,
        duration_secs: f64,
        status: WorkflowStatus,
    ) {
        histogram!(
            METRIC_WORKFLOW_DURATION,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_STATUS => status.as_str(),
        )
        .record(duration_secs);
    }

    fn record_activity_completed(
        &self,
        activity_name: &str,
        queue: &str,
        duration_secs: f64,
        status: ActivityStatus,
    ) {
        histogram!(
            METRIC_ACTIVITY_DURATION,
            METRIC_LABEL_ACTIVITY => activity_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_STATUS => status.as_str(),
        )
        .record(duration_secs);
    }

    fn record_timer_started(&self, duration_secs: f64) {
        counter!(METRIC_TIMER_STARTED).increment(1);
        histogram!(METRIC_TIMER_DURATION).record(duration_secs);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_queue_depth(&self, queue_name: &str, depth: u64) {
        gauge!(
            METRIC_QUEUE_DEPTH,
            METRIC_LABEL_QUEUE => queue_name.to_owned(),
        )
        .set(depth as f64);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_dlq_entries(&self, shard: u16, depth: u64) {
        gauge!(
            METRIC_DLQ_ENTRIES,
            METRIC_LABEL_SHARD => shard.to_string(),
        )
        .set(depth as f64);
    }

    fn record_schedule_run(&self, kind: &str, name: &str) {
        counter!(
            METRIC_SCHEDULE_RUNS,
            METRIC_LABEL_KIND => kind.to_owned(),
            METRIC_LABEL_NAME => name.to_owned(),
        )
        .increment(1);
    }

    fn record_schedule_skipped(&self, kind: &str, name: &str, reason: &str) {
        counter!(
            METRIC_SCHEDULE_SKIPPED,
            METRIC_LABEL_KIND => kind.to_owned(),
            METRIC_LABEL_NAME => name.to_owned(),
            METRIC_LABEL_REASON => reason.to_owned(),
        )
        .increment(1);
    }

    fn record_retention_tick(
        &self,
        shard: u16,
        _candidate_count: u64,
        deleted_count: u64,
        _duration_secs: f64,
    ) {
        counter!(
            METRIC_RETENTION_DELETED,
            METRIC_LABEL_SHARD => shard.to_string(),
        )
        .increment(deleted_count);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_concurrency_key_in_flight(&self, key: &str, in_flight: u64) {
        gauge!(
            "harvest.concurrency.in_flight",
            METRIC_LABEL_KEY => key.to_owned(),
        )
        .set(in_flight as f64);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_concurrency_key_deferred(&self, key: &str, deferred: u64) {
        gauge!(
            "harvest.concurrency.deferred",
            METRIC_LABEL_KEY => key.to_owned(),
        )
        .set(deferred as f64);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::TelemetryConfig;
    use std::sync::Arc;

    #[test]
    fn metrics_rs_recorder_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MetricsRsRecorder>();
    }

    #[test]
    fn metrics_rs_recorder_is_default() {
        let _r = MetricsRsRecorder::default();
    }

    #[test]
    fn metrics_rs_recorder_can_be_arc_boxed_as_metrics_recorder() {
        let _: Arc<dyn MetricsRecorder> = Arc::new(MetricsRsRecorder);
    }

    #[test]
    fn all_record_methods_do_not_panic_with_no_global_recorder() {
        // metrics 0.24 routes to a no-op sink when no global recorder is
        // installed — these calls must complete without panicking.
        let rec = MetricsRsRecorder;
        rec.record_workflow_started("wf", "q");
        rec.record_workflow_completed("wf", "q", 1.0, WorkflowStatus::Completed);
        rec.record_activity_completed("act", "q", 0.5, ActivityStatus::Completed);
        rec.record_timer_started(30.0);
        rec.record_queue_depth("q", 5);
        rec.record_dlq_entries(0, 2);
        rec.record_schedule_run("workflow", "nightly");
        rec.record_schedule_skipped("workflow", "nightly", "paused");
        rec.record_retention_tick(0, 100, 50, 0.01);
        rec.record_concurrency_key_in_flight("cap", 3);
        rec.record_concurrency_key_deferred("cap", 1);
    }

    #[test]
    fn metrics_rs_wires_into_telemetry_config_builder() {
        let telemetry = TelemetryConfig::builder()
            .metrics(Arc::new(MetricsRsRecorder))
            .build();
        // Smoke test — must not panic.
        telemetry
            .metrics
            .record_workflow_started("onboarding", "default");
        telemetry.metrics.record_dlq_entries(0, 0);
    }
}
