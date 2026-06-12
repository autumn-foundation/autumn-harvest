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
    ActivityStatus, METRIC_ACTIVITY_DURATION, METRIC_ACTIVITY_FAILED, METRIC_ADMISSION_BLOCKED,
    METRIC_ADMISSION_GATES_ACTIVE, METRIC_CIRCUIT_CLOSED, METRIC_CIRCUIT_TRIPPED,
    METRIC_COMPLETION_TRIGGER_FIRED, METRIC_DLQ_ENTRIES, METRIC_EXTERNAL_SIGNAL_SENT,
    METRIC_LABEL_ACTIVITY, METRIC_LABEL_ACTIVITY_NAME, METRIC_LABEL_BUILD_ID,
    METRIC_LABEL_ERROR_TYPE, METRIC_LABEL_KEY, METRIC_LABEL_KIND, METRIC_LABEL_NAME,
    METRIC_LABEL_NON_RETRYABLE, METRIC_LABEL_OUTCOME, METRIC_LABEL_QUERY, METRIC_LABEL_QUEUE,
    METRIC_LABEL_REASON, METRIC_LABEL_REASON_CODE, METRIC_LABEL_SCOPE, METRIC_LABEL_SHARD,
    METRIC_LABEL_STATUS, METRIC_LABEL_TRIGGER, METRIC_LABEL_WORKFLOW, METRIC_LABEL_WORKFLOW_TYPE,
    METRIC_QUERY_DURATION, METRIC_QUEUE_DEPTH, METRIC_RATE_LIMIT_REFILL_RATE,
    METRIC_RATE_LIMIT_THROTTLED, METRIC_RATE_LIMIT_TOKENS_AVAILABLE, METRIC_RETENTION_DELETED,
    METRIC_SCHEDULE_AUTO_PAUSED, METRIC_SCHEDULE_DECISION_WRITE_FAILED,
    METRIC_SCHEDULE_FIRE_ATTEMPTS, METRIC_SCHEDULE_MANUAL_TRIGGER, METRIC_SCHEDULE_RUNS,
    METRIC_SCHEDULE_SKIPPED, METRIC_TASK_QUARANTINED, METRIC_TIMER_DURATION, METRIC_TIMER_STARTED,
    METRIC_WORKFLOW_CACHE_HIT, METRIC_WORKFLOW_CACHE_MISS, METRIC_WORKFLOW_CONTINUE_AS_NEW,
    METRIC_WORKFLOW_DURATION, METRIC_WORKFLOW_HISTORY_SIZE, METRIC_WORKFLOW_NON_DETERMINISM,
    METRIC_WORKFLOW_PAUSE_DURATION, METRIC_WORKFLOW_PAUSED, METRIC_WORKFLOW_STARTED,
    METRIC_WORKFLOW_TERMINAL, MetricsRecorder, WorkflowStatus,
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

    fn record_workflow_terminal(&self, workflow_name: &str, queue: &str, outcome: WorkflowStatus) {
        counter!(
            METRIC_WORKFLOW_TERMINAL,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_OUTCOME => outcome.as_str(),
        )
        .increment(1);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_workflow_history_size(&self, workflow_name: &str, event_count: u64) {
        histogram!(
            METRIC_WORKFLOW_HISTORY_SIZE,
            METRIC_LABEL_WORKFLOW_TYPE => workflow_name.to_owned(),
        )
        .record(event_count as f64);
    }

    fn record_workflow_continue_as_new(&self, workflow_name: &str) {
        counter!(
            METRIC_WORKFLOW_CONTINUE_AS_NEW,
            METRIC_LABEL_WORKFLOW_TYPE => workflow_name.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_non_determinism(&self, workflow_name: &str, build_id: &str) {
        counter!(
            METRIC_WORKFLOW_NON_DETERMINISM,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_BUILD_ID => build_id.to_owned(),
        )
        .increment(1);
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

    fn record_activity_completed_with_error_type(
        &self,
        activity_name: &str,
        queue: &str,
        duration_secs: f64,
        status: ActivityStatus,
        error_type: Option<&str>,
    ) {
        // Failed records carry `error.type` so operators can slice the
        // duration histogram by failure class (ADR-0001 §7).
        if let Some(error_type) = error_type {
            histogram!(
                METRIC_ACTIVITY_DURATION,
                METRIC_LABEL_ACTIVITY => activity_name.to_owned(),
                METRIC_LABEL_QUEUE => queue.to_owned(),
                METRIC_LABEL_STATUS => status.as_str(),
                METRIC_LABEL_ERROR_TYPE => error_type.to_owned(),
            )
            .record(duration_secs);
        } else {
            self.record_activity_completed(activity_name, queue, duration_secs, status);
        }
    }

    fn record_activity_failed(
        &self,
        activity_name: &str,
        workflow_type: &str,
        error_type: &str,
        non_retryable: bool,
    ) {
        counter!(
            METRIC_ACTIVITY_FAILED,
            METRIC_LABEL_ACTIVITY => activity_name.to_owned(),
            METRIC_LABEL_WORKFLOW_TYPE => workflow_type.to_owned(),
            METRIC_LABEL_ERROR_TYPE => error_type.to_owned(),
            METRIC_LABEL_NON_RETRYABLE => non_retryable.to_string(),
        )
        .increment(1);
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

    fn record_schedule_decision_write_failed(&self) {
        counter!(METRIC_SCHEDULE_DECISION_WRITE_FAILED).increment(1);
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

    fn record_query_completed(&self, query_name: &str, duration_secs: f64, success: bool) {
        histogram!(
            METRIC_QUERY_DURATION,
            METRIC_LABEL_QUERY => query_name.to_owned(),
            METRIC_LABEL_STATUS => if success { "ok" } else { "error" },
        )
        .record(duration_secs);
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

    fn record_workflow_cache_hit(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_CACHE_HIT,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_cache_miss(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_CACHE_MISS,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_external_signal_sent(&self, outcome: &str, reason_code: Option<&str>) {
        if let Some(reason) = reason_code {
            counter!(
                METRIC_EXTERNAL_SIGNAL_SENT,
                METRIC_LABEL_OUTCOME => outcome.to_owned(),
                METRIC_LABEL_REASON_CODE => reason.to_owned(),
            )
            .increment(1);
        } else {
            counter!(
                METRIC_EXTERNAL_SIGNAL_SENT,
                METRIC_LABEL_OUTCOME => outcome.to_owned(),
            )
            .increment(1);
        }
    }

    fn record_rate_limit_tokens_available(&self, key: &str, tokens: f64) {
        gauge!(
            METRIC_RATE_LIMIT_TOKENS_AVAILABLE,
            METRIC_LABEL_KEY => key.to_owned(),
        )
        .set(tokens);
    }

    fn record_rate_limit_refill_rate(&self, key: &str, refill_rate: f64) {
        gauge!(
            METRIC_RATE_LIMIT_REFILL_RATE,
            METRIC_LABEL_KEY => key.to_owned(),
        )
        .set(refill_rate);
    }

    fn record_rate_limit_throttled(&self, key: &str) {
        counter!(
            METRIC_RATE_LIMIT_THROTTLED,
            METRIC_LABEL_KEY => key.to_owned(),
        )
        .increment(1);
    }

    fn record_schedule_manual_trigger(&self, schedule_name: &str, outcome: &str) {
        counter!(
            METRIC_SCHEDULE_MANUAL_TRIGGER,
            METRIC_LABEL_NAME => schedule_name.to_owned(),
            METRIC_LABEL_OUTCOME => outcome.to_owned(),
        )
        .increment(1);
    }

    fn record_schedule_fire_attempt(&self, schedule_name: &str, outcome: &str) {
        counter!(
            METRIC_SCHEDULE_FIRE_ATTEMPTS,
            METRIC_LABEL_NAME => schedule_name.to_owned(),
            METRIC_LABEL_OUTCOME => outcome.to_owned(),
        )
        .increment(1);
    }

    fn record_schedule_auto_paused(&self, schedule_name: &str) {
        counter!(
            METRIC_SCHEDULE_AUTO_PAUSED,
            METRIC_LABEL_NAME => schedule_name.to_owned(),
        )
        .increment(1);
    }

    fn record_task_quarantined(&self, queue: &str, reason: &str) {
        counter!(
            METRIC_TASK_QUARANTINED,
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_REASON => reason.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_paused(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_PAUSED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_pause_duration(&self, workflow_name: &str, queue: &str, duration_secs: f64) {
        histogram!(
            METRIC_WORKFLOW_PAUSE_DURATION,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .record(duration_secs);
    }

    fn record_circuit_tripped(&self, activity_name: &str) {
        counter!(
            METRIC_CIRCUIT_TRIPPED,
            METRIC_LABEL_ACTIVITY_NAME => activity_name.to_owned(),
        )
        .increment(1);
    }

    fn record_circuit_closed(&self, activity_name: &str) {
        counter!(
            METRIC_CIRCUIT_CLOSED,
            METRIC_LABEL_ACTIVITY_NAME => activity_name.to_owned(),
        )
        .increment(1);
    }

    fn record_completion_trigger_fired(&self, trigger_id: &str, outcome: &str) {
        counter!(
            METRIC_COMPLETION_TRIGGER_FIRED,
            METRIC_LABEL_TRIGGER => trigger_id.to_owned(),
            METRIC_LABEL_OUTCOME => outcome.to_owned(),
        )
        .increment(1);
    }

    fn record_admission_blocked(&self, scope_kind: &str, reason_hash: &str) {
        // Bound cardinality: use the first 8 hex chars of a FNV-1a hash of the
        // reason string rather than the raw free-text label (ADR-0001 §7).
        let mut h: u64 = 14_695_981_039_346_656_037;
        for b in reason_hash.as_bytes() {
            h ^= u64::from(*b);
            h = h.wrapping_mul(1_099_511_628_211);
        }
        let hashed = format!("{:08x}", h & 0xFFFF_FFFF);
        counter!(
            METRIC_ADMISSION_BLOCKED,
            METRIC_LABEL_SCOPE => scope_kind.to_owned(),
            METRIC_LABEL_REASON => hashed,
        )
        .increment(1);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_admission_gates_active(&self, count: i64) {
        gauge!(METRIC_ADMISSION_GATES_ACTIVE).set(count as f64);
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
        let _ = MetricsRsRecorder;
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
        rec.record_workflow_history_size("wf", 2);
        rec.record_workflow_continue_as_new("wf");
        rec.record_activity_completed("act", "q", 0.5, ActivityStatus::Completed);
        rec.record_timer_started(30.0);
        rec.record_queue_depth("q", 5);
        rec.record_dlq_entries(0, 2);
        rec.record_schedule_run("workflow", "nightly");
        rec.record_schedule_skipped("workflow", "nightly", "paused");
        rec.record_schedule_decision_write_failed();
        rec.record_retention_tick(0, 100, 50, 0.01);
        rec.record_concurrency_key_in_flight("cap", 3);
        rec.record_concurrency_key_deferred("cap", 1);
        rec.record_workflow_cache_hit("wf", "q");
        rec.record_workflow_cache_miss("wf", "q");
        rec.record_rate_limit_tokens_available("rl", 10.0);
        rec.record_rate_limit_refill_rate("rl", 2.0);
        rec.record_rate_limit_throttled("rl");
        rec.record_workflow_non_determinism("wf", "build-123");
    }

    // -----------------------------------------------------------------------
    // RED-phase tests: record_workflow_terminal bridge (issue #519)
    // These tests fail until MetricsRsRecorder implements record_workflow_terminal.
    // -----------------------------------------------------------------------

    #[test]
    fn record_workflow_terminal_does_not_panic_for_all_outcomes() {
        use crate::telemetry::WorkflowStatus;
        let rec = MetricsRsRecorder;
        // Must not panic with no global recorder installed (metrics 0.24
        // routes to a no-op sink).
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Completed);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Failed);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Cancelled);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::TimedOut);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Terminated);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::ContinuedAsNew);
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
