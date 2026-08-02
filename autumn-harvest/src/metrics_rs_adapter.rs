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

use metrics::{Key, Label, counter, gauge, histogram};

use crate::telemetry::{
    ActivityStatus, METRIC_ACTIVITY_ATTEMPTS, METRIC_ACTIVITY_DURATION, METRIC_ACTIVITY_FAILED,
    METRIC_ACTIVITY_PANIC, METRIC_ACTIVITY_RETRIES, METRIC_ADMISSION_BLOCKED,
    METRIC_ADMISSION_BYPASSED, METRIC_ADMISSION_GATES_ACTIVE, METRIC_CANARY_FAILURE,
    METRIC_CANARY_ROUNDTRIP, METRIC_CANARY_SUCCESS, METRIC_CIRCUIT_CLOSED, METRIC_CIRCUIT_TRIPPED,
    METRIC_COMPLETION_TRIGGER_FIRED, METRIC_COMPLETION_TRIGGER_SKIPPED, METRIC_DEBOUNCE_FIRED,
    METRIC_DLQ_ENTRIES, METRIC_DLQ_REDRIVEN, METRIC_EXTERNAL_SIGNAL_SENT, METRIC_LABEL_ACTIVITY,
    METRIC_LABEL_ACTIVITY_NAME, METRIC_LABEL_BUILD_ID, METRIC_LABEL_DECISION,
    METRIC_LABEL_ERROR_TYPE, METRIC_LABEL_KEY, METRIC_LABEL_KIND, METRIC_LABEL_NAME,
    METRIC_LABEL_NON_RETRYABLE, METRIC_LABEL_OUTCOME, METRIC_LABEL_PATH, METRIC_LABEL_PRODUCER,
    METRIC_LABEL_QUERY, METRIC_LABEL_QUEUE, METRIC_LABEL_REASON, METRIC_LABEL_REASON_CODE,
    METRIC_LABEL_SCOPE, METRIC_LABEL_SHARD, METRIC_LABEL_SLOT_TYPE, METRIC_LABEL_STATE,
    METRIC_LABEL_STATUS, METRIC_LABEL_TRIGGER, METRIC_LABEL_WORKFLOW, METRIC_LABEL_WORKFLOW_TYPE,
    METRIC_MUTEX_CONTENTION, METRIC_MUTEX_HELD, METRIC_MUTEX_WAIT, METRIC_PAYLOAD_BYTES,
    METRIC_PAYLOAD_OFFLOAD_FETCH_DURATION, METRIC_PAYLOAD_OFFLOADED, METRIC_PAYLOAD_REJECTED,
    METRIC_QUERY_DURATION, METRIC_QUEUE_DEPTH, METRIC_QUEUE_DISPATCHED,
    METRIC_QUEUE_OLDEST_PENDING_AGE, METRIC_QUEUE_PAUSED, METRIC_QUEUE_SCHEDULE_TO_START,
    METRIC_RATE_LIMIT_REFILL_RATE, METRIC_RATE_LIMIT_THROTTLED, METRIC_RATE_LIMIT_TOKENS_AVAILABLE,
    METRIC_RETENTION_DELETED, METRIC_SAGA_COMPENSATED, METRIC_SAGA_COMPENSATION_FAILED,
    METRIC_SCHEDULE_AUTO_PAUSED, METRIC_SCHEDULE_DECISION_WRITE_FAILED,
    METRIC_SCHEDULE_FIRE_ATTEMPTS, METRIC_SCHEDULE_MANUAL_TRIGGER, METRIC_SCHEDULE_OVERDUE,
    METRIC_SCHEDULE_RUNS, METRIC_SCHEDULE_SKIPPED, METRIC_SESSION_ACQUISITION,
    METRIC_SIGNAL_RECEIVED, METRIC_SIGNAL_UNHANDLED, METRIC_SUMMARY_DELETED,
    METRIC_TASK_QUARANTINED, METRIC_TIMER_DURATION, METRIC_TIMER_STARTED, METRIC_UPDATE_ADMITTED,
    METRIC_UPDATE_COMPLETED, METRIC_UPDATE_DURATION, METRIC_UPDATE_FAILED, METRIC_UPDATE_REJECTED,
    METRIC_WEBHOOK_RECEIVED, METRIC_WEBHOOK_REJECTED, METRIC_WORKER_SLOT_TARGET,
    METRIC_WORKER_SLOTS_AVAILABLE, METRIC_WORKER_SLOTS_IN_USE, METRIC_WORKER_TUNER_DECISIONS,
    METRIC_WORKFLOW_ACTIVE, METRIC_WORKFLOW_CACHE_HIT, METRIC_WORKFLOW_CACHE_MISS,
    METRIC_WORKFLOW_CHAIN_TIMEOUT, METRIC_WORKFLOW_CONTINUE_AS_NEW, METRIC_WORKFLOW_DEBOUNCED,
    METRIC_WORKFLOW_DURATION, METRIC_WORKFLOW_HISTORY_BLOAT, METRIC_WORKFLOW_HISTORY_OVERSIZED,
    METRIC_WORKFLOW_HISTORY_SIZE, METRIC_WORKFLOW_ND_BLOCKED, METRIC_WORKFLOW_NON_DETERMINISM,
    METRIC_WORKFLOW_PANIC, METRIC_WORKFLOW_PAUSE_DURATION, METRIC_WORKFLOW_PAUSED,
    METRIC_WORKFLOW_RETRIES, METRIC_WORKFLOW_SLA_BREACHED, METRIC_WORKFLOW_START_THROTTLED,
    METRIC_WORKFLOW_STARTED, METRIC_WORKFLOW_TASK_TIMEOUT, METRIC_WORKFLOW_TERMINAL,
    METRIC_WORKFLOW_TIMEOUT, METRIC_WORKFLOW_UNFINISHED_HANDLERS, MetricsRecorder,
    SessionAcquisitionOutcome, SlotType, TunerDecision, WebhookOutcome, WorkflowStatus,
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

    fn record_workflow_unfinished_handlers(&self, workflow_name: &str, kind: &str, count: u64) {
        counter!(
            METRIC_WORKFLOW_UNFINISHED_HANDLERS,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_KIND => kind.to_owned(),
        )
        .increment(count);
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

    fn record_workflow_nondeterministic_block(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_ND_BLOCKED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
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

    fn record_activity_attempt(&self, activity_name: &str, queue: &str, outcome: ActivityStatus) {
        counter!(
            METRIC_ACTIVITY_ATTEMPTS,
            METRIC_LABEL_ACTIVITY => activity_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_OUTCOME => outcome.as_str(),
        )
        .increment(1);
    }

    fn record_activity_retried(&self, activity_name: &str, queue: &str) {
        counter!(
            METRIC_ACTIVITY_RETRIES,
            METRIC_LABEL_ACTIVITY => activity_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_activity_panic(&self, activity_name: &str, queue: &str) {
        counter!(
            METRIC_ACTIVITY_PANIC,
            METRIC_LABEL_ACTIVITY => activity_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_panic(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_PANIC,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
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

    fn record_task_dispatched(&self, queue_name: &str) {
        counter!(
            METRIC_QUEUE_DISPATCHED,
            METRIC_LABEL_QUEUE => queue_name.to_owned(),
        )
        .increment(1);
    }

    fn record_schedule_to_start(&self, queue_name: &str, wait_secs: f64) {
        histogram!(
            METRIC_QUEUE_SCHEDULE_TO_START,
            METRIC_LABEL_QUEUE => queue_name.to_owned(),
        )
        .record(wait_secs);
    }

    fn record_queue_oldest_pending_age(&self, queue_name: &str, age_secs: f64) {
        gauge!(
            METRIC_QUEUE_OLDEST_PENDING_AGE,
            METRIC_LABEL_QUEUE => queue_name.to_owned(),
        )
        .set(age_secs);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_dlq_entries(&self, shard: u16, depth: u64) {
        gauge!(
            METRIC_DLQ_ENTRIES,
            METRIC_LABEL_SHARD => shard.to_string(),
        )
        .set(depth as f64);
    }

    fn record_queue_paused(&self, queue: &str, paused: bool) {
        gauge!(
            METRIC_QUEUE_PAUSED,
            METRIC_LABEL_QUEUE => queue.to_string(),
        )
        .set(if paused { 1.0 } else { 0.0 });
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_worker_slots(&self, slot_type: SlotType, in_use: u64, available: u64) {
        gauge!(
            METRIC_WORKER_SLOTS_IN_USE,
            METRIC_LABEL_SLOT_TYPE => slot_type.as_str(),
        )
        .set(in_use as f64);
        gauge!(
            METRIC_WORKER_SLOTS_AVAILABLE,
            METRIC_LABEL_SLOT_TYPE => slot_type.as_str(),
        )
        .set(available as f64);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_worker_slot_target(&self, slot_type: SlotType, target: u64) {
        gauge!(
            METRIC_WORKER_SLOT_TARGET,
            METRIC_LABEL_SLOT_TYPE => slot_type.as_str(),
        )
        .set(target as f64);
    }

    fn record_tuner_decision(&self, slot_type: SlotType, decision: TunerDecision) {
        counter!(
            METRIC_WORKER_TUNER_DECISIONS,
            METRIC_LABEL_SLOT_TYPE => slot_type.as_str(),
            METRIC_LABEL_DECISION => decision.as_str(),
        )
        .increment(1);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_shard_stranded_pending(&self, shard: u16, count: u64) {
        gauge!(
            crate::telemetry::METRIC_SHARD_STRANDED_PENDING,
            METRIC_LABEL_SHARD => shard.to_string(),
        )
        .set(count as f64);
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

    fn record_schedule_skipped_n(&self, kind: &str, name: &str, reason: &str, count: u64) {
        // Exact, single batched increment — no per-slot loop, so a large
        // bounded-catchup recovery is counted exactly without stalling the tick.
        counter!(
            METRIC_SCHEDULE_SKIPPED,
            METRIC_LABEL_KIND => kind.to_owned(),
            METRIC_LABEL_NAME => name.to_owned(),
            METRIC_LABEL_REASON => reason.to_owned(),
        )
        .increment(count);
    }

    fn record_schedule_decision_write_failed(&self) {
        counter!(METRIC_SCHEDULE_DECISION_WRITE_FAILED).increment(1);
    }

    fn record_retention_tick(
        &self,
        shard: u16,
        candidate_count: u64,
        deleted_count: u64,
        duration_secs: f64,
    ) {
        // The `harvest.retention.deleted` counter is now owned by
        // `record_retention_deleted`, which carries a per-workflow-type label
        // (issue #737). Emitting it here too would double-count. Candidate
        // count and duration have no metric today; kept as a no-op so the
        // per-shard tick observability point remains available.
        let _ = (shard, candidate_count, deleted_count, duration_secs);
    }

    fn record_retention_deleted(&self, workflow: &str, count: u64) {
        counter!(
            METRIC_RETENTION_DELETED,
            METRIC_LABEL_WORKFLOW => workflow.to_owned(),
        )
        .increment(count);
    }

    fn record_summary_deleted(&self, workflow: &str, count: u64) {
        counter!(
            METRIC_SUMMARY_DELETED,
            METRIC_LABEL_WORKFLOW => workflow.to_owned(),
        )
        .increment(count);
    }

    fn record_payload_offloaded(&self, field: &str, store_id: &str, byte_len: u64) {
        counter!(
            METRIC_PAYLOAD_OFFLOADED,
            "payload.field" => field.to_owned(),
            "store.id" => store_id.to_owned(),
        )
        .increment(byte_len);
    }

    fn record_payload_offload_fetch(&self, store_id: &str, duration_secs: f64) {
        histogram!(
            METRIC_PAYLOAD_OFFLOAD_FETCH_DURATION,
            "store.id" => store_id.to_owned(),
        )
        .record(duration_secs);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_payload_observed(
        &self,
        kind: &crate::error::PayloadKind,
        workflow_type: &str,
        activity_name: Option<&str>,
        observed_bytes: u64,
    ) {
        // Two-arm optional-label pattern (mirrors record_external_signal_sent):
        // non-activity payloads omit `activity.name` rather than emitting an
        // empty label value.
        if let Some(activity_name) = activity_name {
            histogram!(
                METRIC_PAYLOAD_BYTES,
                "payload.kind" => kind.to_string(),
                METRIC_LABEL_WORKFLOW_TYPE => workflow_type.to_owned(),
                METRIC_LABEL_ACTIVITY_NAME => activity_name.to_owned(),
            )
            .record(observed_bytes as f64);
        } else {
            histogram!(
                METRIC_PAYLOAD_BYTES,
                "payload.kind" => kind.to_string(),
                METRIC_LABEL_WORKFLOW_TYPE => workflow_type.to_owned(),
            )
            .record(observed_bytes as f64);
        }
    }

    fn record_payload_rejected(&self, kind: &crate::error::PayloadKind, workflow_type: &str) {
        counter!(
            METRIC_PAYLOAD_REJECTED,
            "payload.kind" => kind.to_string(),
            METRIC_LABEL_WORKFLOW_TYPE => workflow_type.to_owned(),
        )
        .increment(1);
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

    fn record_rate_limit_throttled(&self, activity: &str) {
        // Labelled by the bounded activity name, never the raw bucket key:
        // a dynamic per-key key (`dyn-rate:{expr}:{tenant}`, issue #699) embeds
        // unbounded tenant input and must never become a metric label
        // (ADR-0001 §7).
        counter!(
            METRIC_RATE_LIMIT_THROTTLED,
            METRIC_LABEL_ACTIVITY => activity.to_owned(),
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

    fn record_schedule_overdue(&self, kind: &str, name: &str, overdue: bool) {
        gauge!(
            METRIC_SCHEDULE_OVERDUE,
            METRIC_LABEL_KIND => kind.to_owned(),
            METRIC_LABEL_NAME => name.to_owned(),
        )
        .set(if overdue { 1.0 } else { 0.0 });
    }

    fn record_task_quarantined(&self, queue: &str, reason: &str) {
        counter!(
            METRIC_TASK_QUARANTINED,
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_REASON => reason.to_owned(),
        )
        .increment(1);
    }

    fn record_dlq_redriven(&self, queue: &str, outcome: &str) {
        counter!(
            METRIC_DLQ_REDRIVEN,
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_OUTCOME => outcome.to_owned(),
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

    fn record_workflow_sla_breach(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_SLA_BREACHED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_history_bloat(&self, workflow_name: &str) {
        counter!(
            METRIC_WORKFLOW_HISTORY_BLOAT,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_retry(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_RETRIES,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_task_timeout(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_TASK_TIMEOUT,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_timeout(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_TIMEOUT,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_workflow_chain_timeout(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_WORKFLOW_CHAIN_TIMEOUT,
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

    fn record_completion_trigger_skipped(&self, trigger_id: &str, reason: &str) {
        counter!(
            METRIC_COMPLETION_TRIGGER_SKIPPED,
            METRIC_LABEL_TRIGGER => trigger_id.to_owned(),
            METRIC_LABEL_REASON => reason.to_owned(),
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

    fn record_admission_bypassed(&self, producer: &str) {
        counter!(
            METRIC_ADMISSION_BYPASSED,
            METRIC_LABEL_PRODUCER => producer.to_owned(),
        )
        .increment(1);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_workflow_history_oversized(&self, workflow_name: &str, count: u64) {
        gauge!(
            METRIC_WORKFLOW_HISTORY_OVERSIZED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
        )
        .set(count as f64);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_workflow_active(&self, workflow: &str, state: &str, count: u64) {
        gauge!(
            METRIC_WORKFLOW_ACTIVE,
            METRIC_LABEL_WORKFLOW => workflow.to_owned(),
            METRIC_LABEL_STATE => state.to_owned(),
        )
        .set(count as f64);
    }

    fn record_workflow_debounced(&self, workflow_name: &str) {
        // `debounce_key` is intentionally not a label (unbounded cardinality —
        // ADR-0001 §7); it lives in logs and the /admin/debounce endpoint.
        counter!(
            METRIC_WORKFLOW_DEBOUNCED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
        )
        .increment(1);
    }

    fn record_debounce_fired(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_DEBOUNCE_FIRED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_start_throttled(&self, workflow_name: &str) {
        // The resolved throttle key is intentionally not a label (unbounded
        // cardinality — ADR-0001 §7); per-key backlog is exposed via the
        // /admin/start-throttle endpoint.
        counter!(
            METRIC_WORKFLOW_START_THROTTLED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
        )
        .increment(1);
    }

    fn record_webhook_received(&self, path: &str, outcome: WebhookOutcome) {
        counter!(
            METRIC_WEBHOOK_RECEIVED,
            METRIC_LABEL_PATH => path.to_owned(),
            METRIC_LABEL_OUTCOME => outcome.as_str(),
        )
        .increment(1);
    }

    fn record_webhook_rejected(&self, path: &str, outcome: WebhookOutcome) {
        counter!(
            METRIC_WEBHOOK_REJECTED,
            METRIC_LABEL_PATH => path.to_owned(),
            METRIC_LABEL_OUTCOME => outcome.as_str(),
        )
        .increment(1);
    }

    fn record_session_acquisition(&self, queue: &str, outcome: SessionAcquisitionOutcome) {
        counter!(
            METRIC_SESSION_ACQUISITION,
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_OUTCOME => outcome.as_str(),
        )
        .increment(1);
    }

    fn record_saga_compensated(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_SAGA_COMPENSATED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_saga_compensation_failed(&self, workflow_name: &str, queue: &str) {
        counter!(
            METRIC_SAGA_COMPENSATION_FAILED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_mutex_wait(&self, workflow: &str, seconds: f64) {
        histogram!(
            METRIC_MUTEX_WAIT,
            METRIC_LABEL_WORKFLOW => workflow.to_owned(),
        )
        .record(seconds);
    }

    fn record_mutex_held(&self, workflow: &str, seconds: f64) {
        histogram!(
            METRIC_MUTEX_HELD,
            METRIC_LABEL_WORKFLOW => workflow.to_owned(),
        )
        .record(seconds);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_mutex_contention(&self, workflow: &str, depth: u64) {
        gauge!(
            METRIC_MUTEX_CONTENTION,
            METRIC_LABEL_WORKFLOW => workflow.to_owned(),
        )
        .set(depth as f64);
    }

    fn record_canary_success(&self, queue: &str, shard: u16) {
        counter!(
            METRIC_CANARY_SUCCESS,
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_SHARD => shard.to_string(),
        )
        .increment(1);
    }

    fn record_canary_failure(&self, queue: &str, shard: u16) {
        counter!(
            METRIC_CANARY_FAILURE,
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_SHARD => shard.to_string(),
        )
        .increment(1);
    }

    fn record_canary_roundtrip(&self, queue: &str, shard: u16, duration_secs: f64) {
        histogram!(
            METRIC_CANARY_ROUNDTRIP,
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_SHARD => shard.to_string(),
        )
        .record(duration_secs);
    }

    fn record_signal_received(&self, workflow_name: &str, queue: &str) {
        // Issue #684 (Codex P2): no `name` label — signal names come from the
        // free-form send route and have no declared registry to bound them.
        counter!(
            METRIC_SIGNAL_RECEIVED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_signal_unhandled(&self, workflow_name: &str, queue: &str) {
        // Issue #684 (Codex P2): no `name` label — see record_signal_received.
        counter!(
            METRIC_SIGNAL_UNHANDLED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_update_admitted(&self, workflow_name: &str, queue: &str) {
        // Issue #684 (Codex P2): no `name` label — admission is at the free-form
        // update route boundary where the name is not yet resolved against a
        // registered handler (declarative OR imperative), so it cannot be bounded
        // by construction. Per-name visibility lives on completed/failed/rejected.
        counter!(
            METRIC_UPDATE_ADMITTED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_update_rejected(&self, workflow_name: &str, update_name: &str) {
        counter!(
            METRIC_UPDATE_REJECTED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_NAME => update_name.to_owned(),
        )
        .increment(1);
    }

    fn record_update_completed(&self, workflow_name: &str, update_name: &str, queue: &str) {
        counter!(
            METRIC_UPDATE_COMPLETED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_NAME => update_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_update_failed(&self, workflow_name: &str, update_name: &str, queue: &str) {
        counter!(
            METRIC_UPDATE_FAILED,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_NAME => update_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
        )
        .increment(1);
    }

    fn record_update_duration(
        &self,
        workflow_name: &str,
        update_name: &str,
        queue: &str,
        outcome: &str,
        duration_secs: f64,
    ) {
        histogram!(
            METRIC_UPDATE_DURATION,
            METRIC_LABEL_WORKFLOW => workflow_name.to_owned(),
            METRIC_LABEL_NAME => update_name.to_owned(),
            METRIC_LABEL_QUEUE => queue.to_owned(),
            METRIC_LABEL_OUTCOME => outcome.to_owned(),
        )
        .record(duration_secs);
    }

    fn record_user_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        let ls: Vec<Label> = labels
            .iter()
            .map(|&(k, v)| Label::new(k.to_string(), v.to_string()))
            .collect();
        let key = Key::from_parts(name.to_string(), ls);
        metrics::with_recorder(|recorder| {
            recorder
                .register_counter(
                    &key,
                    &metrics::Metadata::new(module_path!(), metrics::Level::INFO, None),
                )
                .increment(value);
        });
    }

    fn record_user_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let ls: Vec<Label> = labels
            .iter()
            .map(|&(k, v)| Label::new(k.to_string(), v.to_string()))
            .collect();
        let key = Key::from_parts(name.to_string(), ls);
        metrics::with_recorder(|recorder| {
            recorder
                .register_gauge(
                    &key,
                    &metrics::Metadata::new(module_path!(), metrics::Level::INFO, None),
                )
                .set(value);
        });
    }

    fn record_user_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let ls: Vec<Label> = labels
            .iter()
            .map(|&(k, v)| Label::new(k.to_string(), v.to_string()))
            .collect();
        let key = Key::from_parts(name.to_string(), ls);
        metrics::with_recorder(|recorder| {
            recorder
                .register_histogram(
                    &key,
                    &metrics::Metadata::new(module_path!(), metrics::Level::INFO, None),
                )
                .record(value);
        });
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
        rec.record_workflow_history_bloat("wf");
        rec.record_workflow_continue_as_new("wf");
        rec.record_activity_completed("act", "q", 0.5, ActivityStatus::Completed);
        rec.record_timer_started(30.0);
        rec.record_queue_depth("q", 5);
        rec.record_dlq_entries(0, 2);
        rec.record_schedule_run("workflow", "nightly");
        rec.record_schedule_skipped("workflow", "nightly", "paused");
        rec.record_schedule_decision_write_failed();
        rec.record_retention_tick(0, 100, 50, 0.01);
        rec.record_retention_deleted("wf", 50);
        rec.record_concurrency_key_in_flight("cap", 3);
        rec.record_concurrency_key_deferred("cap", 1);
        rec.record_workflow_cache_hit("wf", "q");
        rec.record_workflow_cache_miss("wf", "q");
        rec.record_rate_limit_tokens_available("rl", 10.0);
        rec.record_rate_limit_refill_rate("rl", 2.0);
        rec.record_rate_limit_throttled("rl");
        rec.record_workflow_non_determinism("wf", "build-123");
        rec.record_workflow_nondeterministic_block("wf", "q");
        rec.record_schedule_to_start("q", 1.5);
        rec.record_queue_oldest_pending_age("q", 30.0);
        rec.record_completion_trigger_fired("trigger-uuid", "started");
        rec.record_completion_trigger_skipped("trigger-uuid", "condition_unmet");
        rec.record_mutex_wait("wf", 0.25);
        rec.record_mutex_held("wf", 1.5);
        rec.record_mutex_contention("wf", 3);
        // Issue #617: chain-timeout counter bridge.
        rec.record_workflow_timeout("wf", "q");
        rec.record_workflow_chain_timeout("wf", "q");
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
    fn record_handler_panic_bridges_do_not_panic() {
        // Contained-handler-panic counter bridges (issue #782). Must not panic
        // with no global recorder installed (metrics 0.24 routes to a no-op sink).
        let rec = MetricsRsRecorder;
        rec.record_activity_panic("send_email", "default");
        rec.record_workflow_panic("onboarding", "default");
    }

    #[test]
    fn record_retention_deleted_does_not_panic() {
        // Per-workflow-type retention deletion counter bridge (issue #737).
        // Must not panic with no global recorder installed.
        let rec = MetricsRsRecorder;
        rec.record_retention_deleted("onboarding", 5);
        rec.record_retention_deleted("nightly_report", 0);
    }

    #[test]
    fn record_summary_deleted_does_not_panic() {
        // Summary-retention GC deletion counter bridge (issue #752). Must not
        // panic with no global recorder installed.
        let rec = MetricsRsRecorder;
        rec.record_summary_deleted("onboarding", 5);
        rec.record_summary_deleted("nightly_report", 0);
    }

    #[test]
    fn record_canary_metrics_do_not_panic() {
        // Synthetic liveness-canary bridges (issue #796). Two counters + one
        // histogram, labeled `queue`,`shard`. Must not panic with no global
        // recorder installed. Distinct from the #512 replay canary.
        let rec = MetricsRsRecorder;
        rec.record_canary_success("default", 0);
        rec.record_canary_failure("email", 2);
        rec.record_canary_roundtrip("default", 0, 0.42);
    }

    #[test]
    fn record_completion_trigger_skipped_does_not_panic_for_all_reasons() {
        // Output-guard skip counter bridge (issue #810). Must not panic with
        // no global recorder installed; both bounded reason values covered.
        let rec = MetricsRsRecorder;
        rec.record_completion_trigger_skipped("trigger-uuid", "condition_unmet");
        rec.record_completion_trigger_skipped("trigger-uuid", "condition_invalid");
    }

    #[test]
    fn record_session_acquisition_does_not_panic_for_all_outcomes() {
        use crate::telemetry::SessionAcquisitionOutcome;
        let rec = MetricsRsRecorder;
        rec.record_session_acquisition("gpu-workers", SessionAcquisitionOutcome::Acquired);
        rec.record_session_acquisition("gpu-workers", SessionAcquisitionOutcome::TimedOut);
        rec.record_session_acquisition("gpu-workers", SessionAcquisitionOutcome::Broken);
    }

    #[test]
    fn record_admission_bypassed_does_not_panic() {
        // issue #618: exempt-producer bypass counter bridge. Must not panic
        // with no global recorder installed.
        let rec = MetricsRsRecorder;
        rec.record_admission_bypassed("outbox");
    }

    // -----------------------------------------------------------------------
    // Saga compensation observability bridges (issue #801)
    // -----------------------------------------------------------------------

    #[test]
    fn bridges_saga_counters_with_workflow_and_queue_labels() {
        // Real label-content assertion (issue #801 post-review): a local
        // `metrics::Recorder` captures the registered counter keys, so a
        // swapped or dropped label value in the bridge is caught here —
        // unlike the file's older no-panic smoke idiom, which predates
        // `metrics::with_local_recorder` (0.24).
        type CounterKey = (String, Vec<(String, String)>);

        #[derive(Default)]
        struct CapturingRecorder {
            counters: std::sync::Mutex<Vec<CounterKey>>,
        }

        impl metrics::Recorder for &CapturingRecorder {
            fn describe_counter(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_gauge(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_histogram(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn register_counter(
                &self,
                key: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Counter {
                self.counters.lock().unwrap().push((
                    key.name().to_owned(),
                    key.labels()
                        .map(|l| (l.key().to_owned(), l.value().to_owned()))
                        .collect(),
                ));
                metrics::Counter::noop()
            }
            fn register_gauge(
                &self,
                _: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Gauge {
                metrics::Gauge::noop()
            }
            fn register_histogram(
                &self,
                _: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Histogram {
                metrics::Histogram::noop()
            }
        }

        let capture = CapturingRecorder::default();
        metrics::with_local_recorder(&&capture, || {
            let rec = MetricsRsRecorder;
            rec.record_saga_compensated("book_trip", "payments");
            rec.record_saga_compensation_failed("book_trip", "payments");
        });

        let counters = capture.counters.lock().unwrap().clone();
        let expected_labels = vec![
            (METRIC_LABEL_WORKFLOW.to_owned(), "book_trip".to_owned()),
            (METRIC_LABEL_QUEUE.to_owned(), "payments".to_owned()),
        ];
        assert_eq!(
            counters.as_slice(),
            &[
                (METRIC_SAGA_COMPENSATED.to_owned(), expected_labels.clone()),
                (METRIC_SAGA_COMPENSATION_FAILED.to_owned(), expected_labels),
            ],
            "the bridge must register both saga counters with exactly the \
             workflow + queue label constants, values un-swapped"
        );
    }

    // -----------------------------------------------------------------------
    // Operator early-warning for workflow history bloat bridge (issue #704)
    // -----------------------------------------------------------------------

    #[test]
    fn bridges_workflow_history_bloat_with_workflow_label_only() {
        // Real label-content assertion: a local `metrics::Recorder` captures
        // the registered counter key, so a swapped label, a dropped label, or
        // an accidentally-added `queue` label (unlike its sibling
        // `sla_breached`/`nd_blocked` counters, this one is deliberately
        // `workflow`-only, per the constant's doc comment) is caught here.
        type CounterKey = (String, Vec<(String, String)>);

        #[derive(Default)]
        struct CapturingRecorder {
            counters: std::sync::Mutex<Vec<CounterKey>>,
        }

        impl metrics::Recorder for &CapturingRecorder {
            fn describe_counter(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_gauge(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_histogram(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn register_counter(
                &self,
                key: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Counter {
                self.counters.lock().unwrap().push((
                    key.name().to_owned(),
                    key.labels()
                        .map(|l| (l.key().to_owned(), l.value().to_owned()))
                        .collect(),
                ));
                metrics::Counter::noop()
            }
            fn register_gauge(
                &self,
                _: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Gauge {
                metrics::Gauge::noop()
            }
            fn register_histogram(
                &self,
                _: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Histogram {
                metrics::Histogram::noop()
            }
        }

        let capture = CapturingRecorder::default();
        metrics::with_local_recorder(&&capture, || {
            let rec = MetricsRsRecorder;
            rec.record_workflow_history_bloat("onboarding");
        });

        let counters = capture.counters.lock().unwrap().clone();
        assert_eq!(
            counters.as_slice(),
            &[(
                METRIC_WORKFLOW_HISTORY_BLOAT.to_owned(),
                vec![(METRIC_LABEL_WORKFLOW.to_owned(), "onboarding".to_owned())],
            )],
            "the bridge must register exactly the workflow label constant, \
             with no queue label and no value swap"
        );
    }

    // -----------------------------------------------------------------------
    // Active-workflow gauge bridge (issue #770)
    // -----------------------------------------------------------------------

    #[test]
    fn bridges_workflow_active_gauge_with_workflow_and_state_labels_and_value() {
        // Real gauge-VALUE assertion: a local `metrics::Recorder` returns a
        // custom `Gauge` whose `set(f64)` calls are captured alongside the
        // registered key name + labels, so a swapped label or dropped value in
        // the bridge is caught here (the #754/#801 hardened bar).
        type GaugeSample = (String, Vec<(String, String)>, f64);

        struct RecordingGauge {
            name: String,
            labels: Vec<(String, String)>,
            sink: std::sync::Arc<std::sync::Mutex<Vec<GaugeSample>>>,
        }
        impl metrics::GaugeFn for RecordingGauge {
            fn increment(&self, _: f64) {}
            fn decrement(&self, _: f64) {}
            fn set(&self, value: f64) {
                self.sink
                    .lock()
                    .unwrap()
                    .push((self.name.clone(), self.labels.clone(), value));
            }
        }

        #[derive(Default)]
        struct CapturingRecorder {
            gauges: std::sync::Arc<std::sync::Mutex<Vec<GaugeSample>>>,
        }
        impl metrics::Recorder for &CapturingRecorder {
            fn describe_counter(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_gauge(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_histogram(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn register_counter(
                &self,
                _: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Counter {
                metrics::Counter::noop()
            }
            fn register_gauge(
                &self,
                key: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Gauge {
                metrics::Gauge::from_arc(std::sync::Arc::new(RecordingGauge {
                    name: key.name().to_owned(),
                    labels: key
                        .labels()
                        .map(|l| (l.key().to_owned(), l.value().to_owned()))
                        .collect(),
                    sink: std::sync::Arc::clone(&self.gauges),
                }))
            }
            fn register_histogram(
                &self,
                _: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Histogram {
                metrics::Histogram::noop()
            }
        }

        let capture = CapturingRecorder::default();
        metrics::with_local_recorder(&&capture, || {
            let rec = MetricsRsRecorder;
            rec.record_workflow_active("checkout", "running", 5);
        });

        let gauges = capture.gauges.lock().unwrap().clone();
        assert_eq!(
            gauges.as_slice(),
            &[(
                METRIC_WORKFLOW_ACTIVE.to_owned(),
                vec![
                    (METRIC_LABEL_WORKFLOW.to_owned(), "checkout".to_owned()),
                    (METRIC_LABEL_STATE.to_owned(), "running".to_owned()),
                ],
                5.0,
            )],
            "the active-workflow gauge bridge must set harvest.workflow.active \
             with exactly the workflow+state label constants and value 5.0"
        );
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

    #[test]
    #[allow(clippy::too_many_lines)] // inline CapturingRecorder boilerplate
    fn bridges_signal_update_lifecycle_counters_with_bounded_labels() {
        // Issue #684: a local `metrics::Recorder` captures the registered
        // counter keys, so a swapped or dropped label value in any of the six
        // signal/update lifecycle bridges is caught here (not just no-panic).
        type CounterKey = (String, Vec<(String, String)>);

        #[derive(Default)]
        struct CapturingRecorder {
            counters: std::sync::Mutex<Vec<CounterKey>>,
        }

        impl metrics::Recorder for &CapturingRecorder {
            fn describe_counter(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_gauge(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_histogram(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn register_counter(
                &self,
                key: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Counter {
                self.counters.lock().unwrap().push((
                    key.name().to_owned(),
                    key.labels()
                        .map(|l| (l.key().to_owned(), l.value().to_owned()))
                        .collect(),
                ));
                metrics::Counter::noop()
            }
            fn register_gauge(
                &self,
                _: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Gauge {
                metrics::Gauge::noop()
            }
            fn register_histogram(
                &self,
                _: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Histogram {
                metrics::Histogram::noop()
            }
        }

        let capture = CapturingRecorder::default();
        metrics::with_local_recorder(&&capture, || {
            let rec = MetricsRsRecorder;
            rec.record_signal_received("wf", "q");
            rec.record_signal_unhandled("wf", "q");
            rec.record_update_admitted("wf", "q");
            rec.record_update_rejected("wf", "set_priority");
            rec.record_update_completed("wf", "set_priority", "q");
            rec.record_update_failed("wf", "set_priority", "q");
        });

        // Signals carry no `name` label (issue #684, Codex P2): free-form send
        // route, no declared registry to bound it.
        let wf_q = |q: &str| {
            vec![
                (METRIC_LABEL_WORKFLOW.to_owned(), "wf".to_owned()),
                (METRIC_LABEL_QUEUE.to_owned(), q.to_owned()),
            ]
        };
        let wf_name_q = |name: &str, q: &str| {
            vec![
                (METRIC_LABEL_WORKFLOW.to_owned(), "wf".to_owned()),
                (METRIC_LABEL_NAME.to_owned(), name.to_owned()),
                (METRIC_LABEL_QUEUE.to_owned(), q.to_owned()),
            ]
        };
        let wf_name = |name: &str| {
            vec![
                (METRIC_LABEL_WORKFLOW.to_owned(), "wf".to_owned()),
                (METRIC_LABEL_NAME.to_owned(), name.to_owned()),
            ]
        };

        let counters = capture.counters.lock().unwrap().clone();
        assert_eq!(
            counters.as_slice(),
            &[
                (METRIC_SIGNAL_RECEIVED.to_owned(), wf_q("q")),
                (METRIC_SIGNAL_UNHANDLED.to_owned(), wf_q("q")),
                (METRIC_UPDATE_ADMITTED.to_owned(), wf_q("q")),
                (METRIC_UPDATE_REJECTED.to_owned(), wf_name("set_priority")),
                (
                    METRIC_UPDATE_COMPLETED.to_owned(),
                    wf_name_q("set_priority", "q")
                ),
                (
                    METRIC_UPDATE_FAILED.to_owned(),
                    wf_name_q("set_priority", "q")
                ),
            ],
            "signal + update.admitted bridges register with (workflow, queue) only \
             and the completed/failed/rejected update bridges with the documented \
             workflow/name[/queue] constants, values un-swapped"
        );
    }

    #[test]
    fn bridges_update_duration_histogram_with_bounded_labels() {
        // Issue #781: a local `metrics::Recorder` captures the registered
        // histogram key, so a swapped or dropped label value in the
        // admit→terminal latency bridge is caught here (not just no-panic). The
        // histogram must register `harvest.update.duration` with exactly the
        // workflow/name/queue/outcome constants, values un-swapped.
        type HistKey = (String, Vec<(String, String)>);

        #[derive(Default)]
        struct CapturingRecorder {
            histograms: std::sync::Mutex<Vec<HistKey>>,
        }

        impl metrics::Recorder for &CapturingRecorder {
            fn describe_counter(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_gauge(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_histogram(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn register_counter(
                &self,
                _: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Counter {
                metrics::Counter::noop()
            }
            fn register_gauge(
                &self,
                _: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Gauge {
                metrics::Gauge::noop()
            }
            fn register_histogram(
                &self,
                key: &metrics::Key,
                _: &metrics::Metadata<'_>,
            ) -> metrics::Histogram {
                self.histograms.lock().unwrap().push((
                    key.name().to_owned(),
                    key.labels()
                        .map(|l| (l.key().to_owned(), l.value().to_owned()))
                        .collect(),
                ));
                metrics::Histogram::noop()
            }
        }

        let capture = CapturingRecorder::default();
        metrics::with_local_recorder(&&capture, || {
            let rec = MetricsRsRecorder;
            rec.record_update_duration("wf", "set_priority", "q", "completed", 0.42);
            rec.record_update_duration("wf", "cancel", "q", "failed", 1.5);
        });

        let histograms = capture.histograms.lock().unwrap().clone();
        let labels = |name: &str, outcome: &str| {
            vec![
                (METRIC_LABEL_WORKFLOW.to_owned(), "wf".to_owned()),
                (METRIC_LABEL_NAME.to_owned(), name.to_owned()),
                (METRIC_LABEL_QUEUE.to_owned(), "q".to_owned()),
                (METRIC_LABEL_OUTCOME.to_owned(), outcome.to_owned()),
            ]
        };
        assert_eq!(
            histograms.as_slice(),
            &[
                (
                    METRIC_UPDATE_DURATION.to_owned(),
                    labels("set_priority", "completed")
                ),
                (
                    METRIC_UPDATE_DURATION.to_owned(),
                    labels("cancel", "failed")
                ),
            ],
            "the update-duration histogram bridge must register with exactly the \
             workflow/name/queue/outcome label constants, values un-swapped"
        );
    }
}
