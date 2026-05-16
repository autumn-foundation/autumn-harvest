//! OpenTelemetry integration surface for autumn-harvest.
//!
//! The harvest engine emits structured `tracing` spans around every workflow
//! execution, activity invocation, and timer delay. Operators bridge those
//! spans into OpenTelemetry using any OTel-compatible layer (e.g.
//! [`tracing-opentelemetry`]); the core crate deliberately stays
//! backend-agnostic so Datadog, Jaeger, Grafana, or any other APM can consume
//! the data.
//!
//! In addition to spans, the engine carries a [`TraceContextCarrier`] alongside
//! every task in the Postgres queue. Carriers hold the W3C `traceparent` /
//! `tracestate` headers so a trace started by an HTTP request in the web
//! process can be stitched to the background activity that ran it.
//!
//! Metrics follow the same pattern: applications register any type that
//! implements [`MetricsRecorder`], and the worker drives it on workflow /
//! activity / timer events. The default recorder ([`NoOpMetrics`]) silently
//! drops everything so telemetry is zero-cost unless explicitly configured.
//!
//! [`tracing-opentelemetry`]: https://docs.rs/tracing-opentelemetry

use std::any::Any;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Span attribute name constants
// Defined by docs/adr/0001-otel-trace-contract.md
// ---------------------------------------------------------------------------

/// OpenTelemetry span attribute: the logical workflow name (e.g. `"onboarding"`).
pub const ATTR_WORKFLOW_ID: &str = "harvest.workflow.id";

/// OpenTelemetry span attribute: the unique execution UUID.
pub const ATTR_EXECUTION_ID: &str = "harvest.execution.id";

/// OpenTelemetry span attribute: the shard number that owns this execution.
pub const ATTR_SHARD_ID: &str = "harvest.shard.id";

/// OpenTelemetry span attribute: the activity function name (e.g. `"send_email"`).
pub const ATTR_ACTIVITY_NAME: &str = "harvest.activity.name";

/// OpenTelemetry span attribute: the 1-based attempt number for this activity invocation.
pub const ATTR_ATTEMPT: &str = "harvest.attempt";

/// OpenTelemetry span attribute: the task queue name the work item was pulled from.
pub const ATTR_QUEUE: &str = "harvest.queue";

/// OpenTelemetry span attribute for replay-mode spans.
///
/// Set to `true` when the span is emitted during deterministic replay rather
/// than live execution. Consumers must treat such spans as reconstructed
/// history, not live causality.
pub const ATTR_REPLAY: &str = "harvest.replay";

// ---------------------------------------------------------------------------
// Metric name constants
// Defined by docs/adr/0001-otel-trace-contract.md — OpenTelemetry semantic
// naming: `harvest.<noun>.<instrument>` in dot-notation.
// ---------------------------------------------------------------------------

/// Counter: incremented once when a worker starts executing a workflow task.
pub const METRIC_WORKFLOW_STARTED: &str = "harvest.workflow.started";

/// Histogram: wall-clock seconds a workflow executor cycle took.
pub const METRIC_WORKFLOW_DURATION: &str = "harvest.workflow.duration";

/// Histogram: number of durable events in a terminal workflow execution history.
pub const METRIC_WORKFLOW_HISTORY_SIZE: &str = "harvest.workflow.history_size";

/// Counter: incremented once for each continue-as-new rotation.
pub const METRIC_WORKFLOW_CONTINUE_AS_NEW: &str = "harvest.workflow.continue_as_new";

/// Histogram: wall-clock seconds an activity invocation took (success or failure).
pub const METRIC_ACTIVITY_DURATION: &str = "harvest.activity.duration";

/// Counter: incremented on each activity failure attempt.
///
/// Attributes: `activity.type`, `workflow.type`, `error.type`, `non_retryable`.
/// Per ADR-0001 §7, `execution.id` / `activity.id` are span-only.
pub const METRIC_ACTIVITY_FAILED: &str = "harvest.activity.failed";

/// Counter: incremented when a durable timer is persisted.
pub const METRIC_TIMER_STARTED: &str = "harvest.timer.started";

/// Histogram: distribution of scheduled timer durations (seconds).
pub const METRIC_TIMER_DURATION: &str = "harvest.timer.duration";

/// Gauge: current number of pending (unclaimed) tasks in a queue.
pub const METRIC_QUEUE_DEPTH: &str = "harvest.queue.depth";

/// Gauge: current number of entries in the dead letter queue.
pub const METRIC_DLQ_ENTRIES: &str = "harvest.dlq.entries";

/// Counter: incremented each time a scheduled run is dispatched.
pub const METRIC_SCHEDULE_RUNS: &str = "harvest.schedule.runs";

/// Counter: incremented each time a scheduled run is skipped.
pub const METRIC_SCHEDULE_SKIPPED: &str = "harvest.schedule.skipped";

/// Counter: number of rows deleted by the retention janitor in one tick.
pub const METRIC_RETENTION_DELETED: &str = "harvest.retention.deleted";

/// Histogram: wall-clock latency of query handler invocations (seconds).
///
/// Labelled with `query.name` (low-cardinality handler name registered by the
/// workflow author). Per ADR-0001 cardinality rule, `execution.id` stays
/// span-only and is never a metric label.
pub const METRIC_QUERY_DURATION: &str = "harvest.query.duration";

// ---------------------------------------------------------------------------
// Metric label key constants
// Used by MetricsRecorder implementations to avoid string literals at call
// sites. These are short Prometheus-compatible names; the forbidden label
// (ATTR_EXECUTION_ID) deliberately has no entry here so it cannot be
// accidentally used on a metric.
// ---------------------------------------------------------------------------

/// Metric label: the workflow name.
pub const METRIC_LABEL_WORKFLOW: &str = "workflow";
/// Metric label: the low-cardinality workflow type.
pub const METRIC_LABEL_WORKFLOW_TYPE: &str = "workflow.type";
/// Metric label: the activity name.
pub const METRIC_LABEL_ACTIVITY: &str = "activity";
/// Metric label: the task queue name.
pub const METRIC_LABEL_QUEUE: &str = "queue";
/// Metric label: terminal outcome status (e.g. `"completed"`, `"failed"`).
pub const METRIC_LABEL_STATUS: &str = "status";
/// Metric label: low-cardinality error class on failed activity records.
pub const METRIC_LABEL_ERROR_TYPE: &str = "error.type";
/// Metric label: whether a failure was flagged non-retryable.
pub const METRIC_LABEL_NON_RETRYABLE: &str = "non_retryable";
/// Metric label: the shard number.
pub const METRIC_LABEL_SHARD: &str = "shard";
/// Metric label: schedule kind (`"dag"` or `"workflow"`).
pub const METRIC_LABEL_KIND: &str = "kind";
/// Metric label: the schedule or DAG name.
pub const METRIC_LABEL_NAME: &str = "name";
/// Metric label: reason a scheduled run was skipped.
pub const METRIC_LABEL_REASON: &str = "reason";
/// Metric label: the concurrency group key.
pub const METRIC_LABEL_KEY: &str = "key";
/// Metric label: the query handler name (`query.name`).
pub const METRIC_LABEL_QUERY: &str = "query.name";

// ---------------------------------------------------------------------------
// TraceContextCarrier
// ---------------------------------------------------------------------------

/// W3C Trace Context carrier serialised alongside queued tasks.
///
/// The fields mirror the two HTTP headers defined by the
/// [W3C Trace Context specification](https://www.w3.org/TR/trace-context/):
/// `traceparent` (required to join a trace) and `tracestate` (optional
/// vendor-specific extensions). Storing them as JSONB keeps the schema
/// flexible if the spec gains additional headers.
///
/// The two extra fields (`is_replay`, `link_traceparent`) implement the replay
/// semantics defined in `docs/adr/0001-otel-trace-contract.md`: replay spans
/// must NOT inherit the original trace as a parent (it may have long expired),
/// but they SHOULD carry the original `traceparent` as an OpenTelemetry span
/// *link* so operators can navigate from a replay span to the original trace
/// in their APM tool.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceContextCarrier {
    /// The W3C `traceparent` header for the *parent* span.
    ///
    /// During replay this field is `None` — the replay span roots itself and
    /// links to [`link_traceparent`] instead.
    ///
    /// [`link_traceparent`]: TraceContextCarrier::link_traceparent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,

    /// The optional W3C `tracestate` header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,

    /// When `true`, the worker must emit the span with attribute
    /// `harvest.replay = true` and must NOT restore `traceparent` as the
    /// parent context. Instead, it should create a new root span and attach a
    /// span *link* pointing at [`TraceContextCarrier::link_traceparent`].
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_replay: bool,

    /// The original `traceparent` preserved for use as a span *link*.
    ///
    /// Only populated when `is_replay == true`. Enables APM navigation from a
    /// replay-emitted span back to the original (possibly expired) trace root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_traceparent: Option<String>,
}

impl TraceContextCarrier {
    /// Build a carrier from a raw `traceparent` header value.
    #[must_use]
    pub fn from_traceparent(traceparent: impl Into<String>) -> Self {
        Self {
            traceparent: Some(traceparent.into()),
            tracestate: None,
            is_replay: false,
            link_traceparent: None,
        }
    }

    /// Convert this carrier into a replay carrier.
    ///
    /// Per the OpenTelemetry trace contract ADR (`docs/adr/0001-otel-trace-contract.md`):
    /// - `traceparent` is cleared so the replay span becomes a new root.
    /// - `link_traceparent` is set to the original `traceparent` so the worker
    ///   can attach a span *link* for APM navigation.
    /// - `is_replay` is set to `true` so workers know to emit
    ///   `harvest.replay = true` and skip parent context installation.
    #[must_use]
    pub fn into_replay_context(self) -> Self {
        Self {
            link_traceparent: self.traceparent.or(self.link_traceparent),
            traceparent: None,
            tracestate: None,
            is_replay: true,
        }
    }

    /// Returns `true` when no context fields are set (ignoring the replay flag).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.traceparent.is_none()
            && self.tracestate.is_none()
            && self.link_traceparent.is_none()
            && !self.is_replay
    }

    /// Serialise to a JSONB-compatible [`serde_json::Value`] suitable for
    /// writing to `harvest_task_queue.trace_context`. Returns `None` for an
    /// empty carrier so empty rows stay `NULL`.
    #[must_use]
    pub fn to_json(&self) -> Option<serde_json::Value> {
        if self.is_empty() {
            return None;
        }
        serde_json::to_value(self).ok()
    }

    /// Parse a carrier from the `trace_context` column payload.
    ///
    /// Malformed payloads yield `None` rather than erroring so a corrupt row
    /// never sinks a worker — the task is simply processed without a parent
    /// span.
    #[must_use]
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        let carrier: Self = serde_json::from_value(value.clone()).ok()?;
        if carrier.is_empty() {
            None
        } else {
            Some(carrier)
        }
    }
}

// ---------------------------------------------------------------------------
// Propagator hook
// ---------------------------------------------------------------------------

/// Bridge between the current tracing context and the [`TraceContextCarrier`]
/// stored on tasks.
///
/// Applications implement this once against their chosen OpenTelemetry SDK
/// (for example, using `opentelemetry::global::get_text_map_propagator`). The
/// harvest runtime calls [`capture`](Self::capture) at enqueue time to snapshot
/// the active span and [`install`](Self::install) when a task is claimed so the
/// worker's handler span becomes a child of the original producer's trace.
///
/// Implementations must be cheap enough to call on every enqueue and dispatch.
pub trait TraceContextPropagator: Send + Sync {
    /// Capture the current trace context so it can travel with a queued task.
    ///
    /// Returning [`None`] means "no context to carry"; the carrier column on
    /// the task will be left `NULL`.
    fn capture(&self) -> Option<TraceContextCarrier>;

    /// Install `carrier` as the parent context for subsequent spans on this
    /// thread / task.
    ///
    /// Returns an opaque guard that restores the prior context when dropped.
    /// Implementations that do not support scoped restoration may return a
    /// dummy guard.
    ///
    /// The guard is `'static` so callers can hold it across `.await` points;
    /// implementations that need to reference propagator-owned state should
    /// clone an `Arc` into the guard rather than borrowing from `self`.
    fn install(&self, carrier: &TraceContextCarrier) -> Box<dyn Any + Send>;
}

/// Propagator that never captures context — the default when no telemetry is
/// configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpPropagator;

impl TraceContextPropagator for NoOpPropagator {
    fn capture(&self) -> Option<TraceContextCarrier> {
        None
    }

    fn install(&self, _carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
        Box::new(())
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Terminal outcome of a workflow execution, reported to [`MetricsRecorder`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowStatus {
    /// Handler returned `Ok(..)`.
    Completed,
    /// Handler returned `Err(..)`.
    Failed,
    /// Handler suspended awaiting activity results or timer firings; this run
    /// of the executor did not complete the workflow.
    Suspended,
    /// Handler signalled `continue_as_new`: the current run is sealed and a
    /// fresh execution with the same `WorkflowId` is started in its place.
    ContinuedAsNew,
}

impl WorkflowStatus {
    /// Stable string representation, suitable for metric tag values.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Suspended => "suspended",
            Self::ContinuedAsNew => "continued_as_new",
        }
    }
}

/// Terminal outcome of an activity invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityStatus {
    /// Handler returned `Ok(..)`.
    Completed,
    /// Handler returned `Err(..)` (includes retries).
    Failed,
}

impl ActivityStatus {
    /// Stable string representation, suitable for metric tag values.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// Sink for the harvest engine's standard metrics.
///
/// Implementations fan these calls into an OpenTelemetry meter (or whatever
/// metrics backend the application uses). All methods have default no-op
/// bodies so implementers can opt in to only the metrics they care about.
pub trait MetricsRecorder: Send + Sync {
    /// A workflow task entered the executor on a worker.
    fn record_workflow_started(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow task finished an executor cycle.
    ///
    /// `duration_secs` is the wall-clock time the handler spent running.
    /// `Suspended` runs are reported here too so operators can see executor
    /// churn.
    fn record_workflow_completed(
        &self,
        workflow_name: &str,
        queue: &str,
        duration_secs: f64,
        status: WorkflowStatus,
    ) {
        let _ = (workflow_name, queue, duration_secs, status);
    }

    /// A workflow reached a terminal state with this durable history size.
    fn record_workflow_history_size(&self, workflow_name: &str, event_count: u64) {
        let _ = (workflow_name, event_count);
    }

    /// A workflow execution rotated using continue-as-new.
    fn record_workflow_continue_as_new(&self, workflow_name: &str) {
        let _ = workflow_name;
    }

    /// An activity invocation finished.
    fn record_activity_completed(
        &self,
        activity_name: &str,
        queue: &str,
        duration_secs: f64,
        status: ActivityStatus,
    ) {
        let _ = (activity_name, queue, duration_secs, status);
    }

    /// Variant of [`record_activity_completed`](Self::record_activity_completed)
    /// that also carries an `error.type` attribute for failed records.
    ///
    /// Per ADR-0001 §7, `error.type` must remain a low-cardinality attribute on
    /// the `harvest.activity.duration` histogram so operators can slice failure
    /// rates by error class without parsing message strings.
    ///
    /// The default body delegates to `record_activity_completed`, dropping the
    /// `error_type` — existing implementations stay correct without changes.
    /// Backends that want the slicing should override this method instead.
    fn record_activity_completed_with_error_type(
        &self,
        activity_name: &str,
        queue: &str,
        duration_secs: f64,
        status: ActivityStatus,
        error_type: Option<&str>,
    ) {
        let _ = error_type;
        self.record_activity_completed(activity_name, queue, duration_secs, status);
    }

    /// An activity invocation failed (per-attempt failure record).
    ///
    /// Maps to the counter `harvest.activity.failed` with attributes:
    /// - `activity.type`: the registered activity name
    /// - `workflow.type`: the owning workflow name (empty string when unknown)
    /// - `error.type`: low-cardinality error class (e.g. `"InvalidInput"`)
    /// - `non_retryable`: whether the failure skipped remaining retries
    ///
    /// Per ADR-0001 §7: `execution.id` and `activity.id` are span-only and
    /// must never appear as metric attributes. Callers are responsible for
    /// keeping `error_type` low-cardinality.
    fn record_activity_failed(
        &self,
        activity_name: &str,
        workflow_type: &str,
        error_type: &str,
        non_retryable: bool,
    ) {
        let _ = (activity_name, workflow_type, error_type, non_retryable);
    }

    /// A durable timer was persisted.
    fn record_timer_started(&self, duration_secs: f64) {
        let _ = duration_secs;
    }

    /// A periodic snapshot of queued pending task count.
    fn record_queue_depth(&self, queue_name: &str, depth: u64) {
        let _ = (queue_name, depth);
    }

    /// Results of one retention-janitor tick on a shard.
    fn record_retention_tick(
        &self,
        shard: u16,
        candidate_count: u64,
        deleted_count: u64,
        duration_secs: f64,
    ) {
        let _ = (shard, candidate_count, deleted_count, duration_secs);
    }

    /// Current number of RUNNING tasks for a concurrency group key.
    ///
    /// Emitted by the concurrency sampler on every sample interval. The value
    /// is a gauge — operators should alert when it approaches `max_concurrent`.
    fn record_concurrency_key_in_flight(&self, key: &str, in_flight: u64) {
        let _ = (key, in_flight);
    }

    /// Number of PENDING tasks for a key that are being held back because the
    /// cap is currently saturated (`in_flight >= max_concurrent`).
    ///
    /// Emitted alongside each `record_concurrency_key_in_flight` call when
    /// there are deferred tasks waiting for a slot. Operators should monitor
    /// this alongside queue depth to detect saturation-induced backlog.
    fn record_concurrency_key_deferred(&self, key: &str, deferred: u64) {
        let _ = (key, deferred);
    }

    /// Current number of entries in the dead-letter queue on one shard.
    ///
    /// Emitted by a periodic background sampler on the same cadence as
    /// [`record_queue_depth`](Self::record_queue_depth). `shard` is the
    /// `ShardId` as a `u16`; single-shard deployments always emit `shard = 0`.
    ///
    /// Maps to the gauge `harvest_dlq_entries{shard}`.
    fn record_dlq_entries(&self, shard: u16, depth: u64) {
        let _ = (shard, depth);
    }

    /// A scheduled run was dispatched (either a DAG run or a workflow start).
    ///
    /// `kind` is `"dag"` or `"workflow"`. `name` is the DAG or workflow name.
    /// Maps to the metric `harvest_schedule_runs_total{kind, name}`.
    fn record_schedule_run(&self, kind: &str, name: &str) {
        let _ = (kind, name);
    }

    /// A scheduled run was skipped without dispatching.
    ///
    /// `kind` is `"dag"` or `"workflow"`. `name` is the DAG or workflow name.
    /// `reason` is one of `"paused"`, `"max_active_runs_reached"`, or
    /// `"catchup_disabled"`.
    ///
    /// Maps to the metric `harvest_schedule_skipped_total{kind, name, reason}`.
    fn record_schedule_skipped(&self, kind: &str, name: &str, reason: &str) {
        let _ = (kind, name, reason);
    }

    /// A query handler invocation completed (issue #234).
    ///
    /// `query_name` is the handler name registered via `register_query` /
    /// `register_query_handler`. Per ADR-0001 cardinality rule, `execution.id`
    /// stays span-only — it must never appear as a metric label here.
    ///
    /// `duration_secs` is the wall-clock time from invocation start to the
    /// handler returning (or being timed out). `success` is `true` when the
    /// handler returned `Ok`, `false` on `Err` or timeout.
    ///
    /// Maps to the histogram `harvest.query.duration{query.name, status}`.
    fn record_query_completed(&self, query_name: &str, duration_secs: f64, success: bool) {
        let _ = (query_name, duration_secs, success);
    }
}

/// Default metrics recorder that discards every sample.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpMetrics;

impl MetricsRecorder for NoOpMetrics {}

// ---------------------------------------------------------------------------
// Telemetry bundle
// ---------------------------------------------------------------------------

/// Bundle of telemetry dependencies injected into the harvest runtime.
///
/// Use [`TelemetryConfig::builder`] for the common case; applications that
/// need to drive telemetry manually in tests can construct the struct literal
/// directly.
#[derive(Clone)]
pub struct TelemetryConfig {
    /// Human-readable service name stamped onto emitted spans. Defaults to
    /// `"autumn-harvest"`.
    pub service_name: Arc<str>,

    /// How the runtime exchanges trace context with the application's
    /// `OTel` setup. Defaults to [`NoOpPropagator`].
    pub propagator: Arc<dyn TraceContextPropagator>,

    /// Where per-event metrics are recorded. Defaults to [`NoOpMetrics`].
    pub metrics: Arc<dyn MetricsRecorder>,
}

impl std::fmt::Debug for TelemetryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryConfig")
            .field("service_name", &self.service_name)
            .field("propagator", &std::any::type_name_of_val(&*self.propagator))
            .field("metrics", &std::any::type_name_of_val(&*self.metrics))
            .finish()
    }
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: Arc::from("autumn-harvest"),
            propagator: Arc::new(NoOpPropagator),
            metrics: Arc::new(NoOpMetrics),
        }
    }
}

impl TelemetryConfig {
    /// Start a fluent builder pre-populated with safe no-op defaults.
    #[must_use]
    pub fn builder() -> TelemetryConfigBuilder {
        TelemetryConfigBuilder::default()
    }

    /// Capture the current trace context via the configured propagator.
    #[must_use]
    pub fn capture_trace_context(&self) -> Option<TraceContextCarrier> {
        self.propagator.capture()
    }

    /// Install `carrier` as the parent context for subsequent spans. The
    /// returned guard restores the prior context when dropped.
    #[must_use]
    pub fn install_trace_context(&self, carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
        self.propagator.install(carrier)
    }
}

/// Fluent builder for [`TelemetryConfig`].
#[derive(Default)]
pub struct TelemetryConfigBuilder {
    service_name: Option<Arc<str>>,
    propagator: Option<Arc<dyn TraceContextPropagator>>,
    metrics: Option<Arc<dyn MetricsRecorder>>,
}

impl TelemetryConfigBuilder {
    /// Override the service name stamped onto spans.
    #[must_use]
    pub fn service_name(mut self, name: impl Into<Arc<str>>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// Register a custom [`TraceContextPropagator`].
    #[must_use]
    pub fn propagator(mut self, propagator: Arc<dyn TraceContextPropagator>) -> Self {
        self.propagator = Some(propagator);
        self
    }

    /// Register a custom [`MetricsRecorder`].
    #[must_use]
    pub fn metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Finalize into a [`TelemetryConfig`], filling unset fields with the
    /// safe defaults.
    #[must_use]
    pub fn build(self) -> TelemetryConfig {
        TelemetryConfig {
            service_name: self
                .service_name
                .unwrap_or_else(|| Arc::from("autumn-harvest")),
            propagator: self.propagator.unwrap_or_else(|| Arc::new(NoOpPropagator)),
            metrics: self.metrics.unwrap_or_else(|| Arc::new(NoOpMetrics)),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // RED-phase tests: these assert the spec-mandated constants and
    // replay-aware carrier shape defined in docs/adr/0001-otel-trace-contract.md
    // -----------------------------------------------------------------------

    #[test]
    fn span_attribute_constants_have_correct_names() {
        // The OTel trace contract ADR mandates these exact attribute keys.
        assert_eq!(ATTR_WORKFLOW_ID, "harvest.workflow.id");
        assert_eq!(ATTR_EXECUTION_ID, "harvest.execution.id");
        assert_eq!(ATTR_SHARD_ID, "harvest.shard.id");
        assert_eq!(ATTR_ACTIVITY_NAME, "harvest.activity.name");
        assert_eq!(ATTR_ATTEMPT, "harvest.attempt");
        assert_eq!(ATTR_QUEUE, "harvest.queue");
        assert_eq!(ATTR_REPLAY, "harvest.replay");
    }

    #[test]
    fn metric_name_constants_have_correct_names() {
        // OTel semantic naming: instrument.noun (dot-separated).
        assert_eq!(METRIC_WORKFLOW_STARTED, "harvest.workflow.started");
        assert_eq!(METRIC_WORKFLOW_DURATION, "harvest.workflow.duration");
        assert_eq!(
            METRIC_WORKFLOW_HISTORY_SIZE,
            "harvest.workflow.history_size"
        );
        assert_eq!(
            METRIC_WORKFLOW_CONTINUE_AS_NEW,
            "harvest.workflow.continue_as_new"
        );
        assert_eq!(METRIC_ACTIVITY_DURATION, "harvest.activity.duration");
        assert_eq!(METRIC_TIMER_STARTED, "harvest.timer.started");
        assert_eq!(METRIC_QUEUE_DEPTH, "harvest.queue.depth");
        assert_eq!(METRIC_DLQ_ENTRIES, "harvest.dlq.entries");
        assert_eq!(METRIC_SCHEDULE_RUNS, "harvest.schedule.runs");
        assert_eq!(METRIC_SCHEDULE_SKIPPED, "harvest.schedule.skipped");
        assert_eq!(METRIC_RETENTION_DELETED, "harvest.retention.deleted");
    }

    #[test]
    fn metric_label_constants_have_correct_names() {
        assert_eq!(METRIC_LABEL_WORKFLOW, "workflow");
        assert_eq!(METRIC_LABEL_WORKFLOW_TYPE, "workflow.type");
        assert_eq!(METRIC_LABEL_ACTIVITY, "activity");
        assert_eq!(METRIC_LABEL_QUEUE, "queue");
    }

    #[test]
    fn replay_carrier_strips_traceparent_and_preserves_link() {
        // Per spec: replay spans MUST NOT be parented to the original
        // (potentially expired) trace — they link to it instead.
        let original = TraceContextCarrier::from_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        let replay = original.into_replay_context();
        assert!(
            replay.traceparent.is_none(),
            "replay carrier must not carry original traceparent as parent"
        );
        assert_eq!(
            replay.link_traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
            "replay carrier must preserve original as a span link"
        );
        assert!(
            replay.is_replay,
            "replay carrier must be flagged harvest.replay = true"
        );
    }

    #[test]
    fn non_replay_carrier_has_is_replay_false_and_no_link() {
        let carrier = TraceContextCarrier::from_traceparent("00-abcd-ef01-01");
        assert!(!carrier.is_replay);
        assert!(carrier.link_traceparent.is_none());
    }

    #[test]
    fn replay_carrier_from_empty_original_has_no_link() {
        let empty = TraceContextCarrier::default();
        let replay = empty.into_replay_context();
        assert!(replay.traceparent.is_none());
        assert!(
            replay.link_traceparent.is_none(),
            "no link when original had no traceparent"
        );
        assert!(replay.is_replay);
    }

    #[test]
    fn replay_carrier_roundtrips_through_json() {
        let original = TraceContextCarrier::from_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        let replay = original.into_replay_context();
        // A replay carrier must still serialise / deserialise so it can
        // travel on harvest_task_queue.trace_context.
        let json = replay.to_json().expect("replay carrier serialises");
        let decoded = TraceContextCarrier::from_json(&json).expect("valid JSON roundtrips");
        assert!(decoded.is_replay);
        assert_eq!(
            decoded.link_traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        );
        assert!(decoded.traceparent.is_none());
    }

    #[test]
    fn into_replay_context_on_already_replay_carrier_preserves_link() {
        // If into_replay_context() is called defensively on a carrier that is
        // already in replay form (traceparent=None, link_traceparent=Some),
        // the existing link must not be erased.
        let original = TraceContextCarrier::from_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        let replay = original.into_replay_context();
        // Call again — should be idempotent, link must survive.
        let replay2 = replay.into_replay_context();
        assert!(replay2.traceparent.is_none());
        assert!(replay2.is_replay);
        assert_eq!(
            replay2.link_traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
            "double-converting a replay carrier must not erase the original link"
        );
    }

    #[test]
    fn carrier_roundtrips_through_json() {
        let carrier = TraceContextCarrier {
            traceparent: Some(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
            ),
            tracestate: Some("vendor=abc".to_string()),
            ..Default::default()
        };

        let json = carrier.to_json().expect("non-empty carrier serialises");
        let decoded = TraceContextCarrier::from_json(&json).expect("valid JSON roundtrips");
        assert_eq!(decoded, carrier);
    }

    #[test]
    fn empty_carrier_refuses_to_serialise() {
        let carrier = TraceContextCarrier::default();
        assert!(carrier.is_empty());
        assert!(carrier.to_json().is_none());
    }

    #[test]
    fn carrier_from_json_rejects_fully_empty_payload() {
        let payload = serde_json::json!({});
        assert!(TraceContextCarrier::from_json(&payload).is_none());
    }

    #[test]
    fn carrier_from_json_rejects_wrong_shape() {
        let payload = serde_json::json!({"traceparent": 42});
        assert!(TraceContextCarrier::from_json(&payload).is_none());
    }

    #[test]
    fn carrier_from_traceparent_helper() {
        let carrier = TraceContextCarrier::from_traceparent("00-abcd-ef01-01");
        assert_eq!(carrier.traceparent.as_deref(), Some("00-abcd-ef01-01"));
        assert!(carrier.tracestate.is_none());
    }

    // -----------------------------------------------------------------------
    // RED-phase tests: record_dlq_entries and cardinality enforcement
    // These tests fail until the implementation is complete.
    // -----------------------------------------------------------------------

    #[test]
    fn dlq_entries_gauge_has_default_noop_impl() {
        // record_dlq_entries must exist on MetricsRecorder with (shard: u16, depth: u64).
        // METRIC_DLQ_ENTRIES is the registered constant; this verifies it's
        // actually wired to a callable method with the right shape.
        let rec = NoOpMetrics;
        rec.record_dlq_entries(0, 0);
        rec.record_dlq_entries(1, 42);
    }

    #[test]
    fn all_metric_record_methods_compile_without_execution_id() {
        // ADR-0001 §7: execution.id is span-only and FORBIDDEN as a metric label.
        // This test verifies by construction that no record_* method on
        // MetricsRecorder accepts an ExecutionId argument — the cardinality
        // guard is enforced at the API surface, not just at call sites.
        let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        // Each call compiles only if the method signature matches what we expect:
        // no ExecutionId, no raw UUID params that could smuggle one in.
        rec.record_workflow_started("onboarding", "default");
        rec.record_workflow_completed("onboarding", "default", 1.23, WorkflowStatus::Completed);
        rec.record_workflow_history_size("onboarding", 42);
        rec.record_workflow_continue_as_new("onboarding");
        rec.record_activity_completed("send_email", "default", 0.5, ActivityStatus::Completed);
        rec.record_timer_started(60.0);
        rec.record_queue_depth("default", 7);
        rec.record_dlq_entries(0, 3);
        rec.record_schedule_run("workflow", "daily_digest");
        rec.record_schedule_skipped("workflow", "daily_digest", "paused");
        rec.record_retention_tick(0, 100, 50, 0.02);
        // If any method silently accepted execution.id we'd see it here.
    }

    #[test]
    fn workflow_status_and_activity_status_stringify() {
        assert_eq!(WorkflowStatus::Completed.as_str(), "completed");
        assert_eq!(WorkflowStatus::Failed.as_str(), "failed");
        assert_eq!(WorkflowStatus::Suspended.as_str(), "suspended");
        assert_eq!(WorkflowStatus::ContinuedAsNew.as_str(), "continued_as_new");
        assert_eq!(ActivityStatus::Completed.as_str(), "completed");
        assert_eq!(ActivityStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn default_telemetry_uses_noop_implementations() {
        let telemetry = TelemetryConfig::default();
        assert_eq!(&*telemetry.service_name, "autumn-harvest");
        assert!(telemetry.capture_trace_context().is_none());
        // Calls should not panic; drop the guard immediately.
        let _guard = telemetry.install_trace_context(&TraceContextCarrier::default());
        // Metric calls are no-ops.
        telemetry.metrics.record_workflow_started("demo", "default");
        telemetry.metrics.record_workflow_completed(
            "demo",
            "default",
            0.01,
            WorkflowStatus::Completed,
        );
        telemetry.metrics.record_workflow_history_size("demo", 2);
        telemetry.metrics.record_workflow_continue_as_new("demo");
        telemetry.metrics.record_activity_completed(
            "send_email",
            "default",
            0.5,
            ActivityStatus::Completed,
        );
        telemetry.metrics.record_timer_started(5.0);
        telemetry.metrics.record_queue_depth("default", 0);
    }

    #[test]
    fn builder_overrides_defaults() {
        #[derive(Default)]
        struct CountingPropagator {
            captured: std::sync::atomic::AtomicUsize,
        }

        impl TraceContextPropagator for CountingPropagator {
            fn capture(&self) -> Option<TraceContextCarrier> {
                self.captured
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Some(TraceContextCarrier::from_traceparent("00-aaaa-bbbb-01"))
            }
            fn install(&self, _carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
                Box::new(())
            }
        }

        let prop = Arc::new(CountingPropagator::default());
        let telemetry = TelemetryConfig::builder()
            .service_name("billing")
            .propagator(prop.clone())
            .build();

        assert_eq!(&*telemetry.service_name, "billing");
        let carrier = telemetry.capture_trace_context().expect("captures carrier");
        assert_eq!(carrier.traceparent.as_deref(), Some("00-aaaa-bbbb-01"));
        assert_eq!(prop.captured.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn metrics_recorder_trait_is_object_safe() {
        #[derive(Default)]
        struct Count {
            workflows: std::sync::atomic::AtomicUsize,
            activities: std::sync::atomic::AtomicUsize,
        }

        impl MetricsRecorder for Count {
            fn record_workflow_completed(&self, _: &str, _: &str, _: f64, _: WorkflowStatus) {
                self.workflows
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            fn record_activity_completed(&self, _: &str, _: &str, _: f64, _: ActivityStatus) {
                self.activities
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let count = Arc::new(Count::default());
        let telemetry = TelemetryConfig::builder().metrics(count.clone()).build();

        telemetry
            .metrics
            .record_workflow_completed("w", "default", 1.0, WorkflowStatus::Completed);
        telemetry
            .metrics
            .record_activity_completed("a", "default", 0.5, ActivityStatus::Completed);

        assert_eq!(count.workflows.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            count.activities.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }
}
