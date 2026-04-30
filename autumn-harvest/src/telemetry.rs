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
// TraceContextCarrier
// ---------------------------------------------------------------------------

/// W3C Trace Context carrier serialised alongside queued tasks.
///
/// The fields mirror the two HTTP headers defined by the
/// [W3C Trace Context specification](https://www.w3.org/TR/trace-context/):
/// `traceparent` (required to join a trace) and `tracestate` (optional
/// vendor-specific extensions). Storing them as JSONB keeps the schema
/// flexible if the spec gains additional headers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceContextCarrier {
    /// The W3C `traceparent` header, e.g.
    /// `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,

    /// The optional W3C `tracestate` header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
}

impl TraceContextCarrier {
    /// Build a carrier from a raw `traceparent` header value.
    #[must_use]
    pub fn from_traceparent(traceparent: impl Into<String>) -> Self {
        Self {
            traceparent: Some(traceparent.into()),
            tracestate: None,
        }
    }

    /// Returns `true` when neither `traceparent` nor `tracestate` is set.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.traceparent.is_none() && self.tracestate.is_none()
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

    #[test]
    fn carrier_roundtrips_through_json() {
        let carrier = TraceContextCarrier {
            traceparent: Some(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
            ),
            tracestate: Some("vendor=abc".to_string()),
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
