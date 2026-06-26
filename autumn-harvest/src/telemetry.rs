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

/// Counter: incremented **exactly once** when a workflow execution reaches a
/// terminal state.
///
/// Unlike `harvest.workflow.duration` (which fires on every executor cycle,
/// including suspended cycles), this counter fires at the final
/// terminal-state transition only — so `completed / (completed + failed +
/// cancelled + timed_out)` gives a reliable per-scrape-interval success rate
/// without any normalisation.
///
/// Labels (all low-cardinality):
/// - `workflow` — the workflow type name.
/// - `queue`    — the task-queue the execution was dispatched on.
/// - `outcome`  — one of the six bounded values below.
///
/// Bounded `outcome` values:
/// - `"completed"` — handler returned `Ok(…)`.
/// - `"failed"`    — handler returned `Err(…)` or exhausted retries.
/// - `"cancelled"` — gracefully cancelled via `cancel_workflow_execution`.
/// - `"timed_out"` — execution deadline (`deadline_at`) elapsed.
/// - `"terminated"`— force-killed via `terminate_workflow_execution`.
/// - `"continued_as_new"` — execution rotated; **excluded from the
///   success-rate denominator** (`completed + failed + cancelled + timed_out`).
///
/// Per ADR-0001 §7, `execution.id` is **span-only** and must never appear
/// as a label on this counter.
pub const METRIC_WORKFLOW_TERMINAL: &str = "harvest.workflow.terminal";

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

/// Histogram: wall-clock seconds a task waited between becoming eligible
/// (`scheduled_at`) and being claimed by a worker (`started_at`).
///
/// Recorded at claim time on the existing `claim_task` path. Labels:
///   - `"queue"` — the task queue name (bounded cardinality).
///
/// Use this as the canonical "do I need more workers?" SLI: page when the
/// p99 exceeds your per-queue SLA (e.g. `histogram_quantile(0.99, …) > 30`).
/// Per ADR-0001 §7, `execution.id` / `activity.id` are span-only and MUST NOT
/// appear as metric labels here.
pub const METRIC_QUEUE_SCHEDULE_TO_START: &str = "harvest.queue.schedule_to_start";

/// Gauge: age in seconds of the oldest currently-unclaimed eligible task in a
/// queue, sampled on the same cadence as `harvest.queue.depth`.
///
/// "Eligible" means `state = 'PENDING' AND scheduled_at <= NOW()` — the same
/// slice that `claim_task` competes over. Labels:
///   - `"queue"` — the task queue name (bounded cardinality).
///
/// Reports `0` when no eligible tasks are queued, preventing stale gauge values
/// from lingering after a queue drains.
pub const METRIC_QUEUE_OLDEST_PENDING_AGE: &str = "harvest.queue.oldest_pending_age";

/// Counter: incremented once each time a task is dispatched from a queue.
///
/// This lets operators confirm that the live per-queue dispatch split matches
/// the configured `queue_weights` (issue #515). The counter is recorded in
/// `dispatch_task` for both weighted and default paths, so it is always
/// observable regardless of whether weights are configured.
///
/// Labels:
///   - `"queue"` — the task queue name (bounded cardinality, ADR-0001 §7).
///
/// `execution.id` / `activity.id` are span-only and MUST NOT appear as labels.
pub const METRIC_QUEUE_DISPATCHED: &str = "harvest.queue.dispatched";

/// Gauge: current number of claimable pending tasks on a shard that has no
/// covering live worker (issue #522).
///
/// Emitted per shard by the stranded-work sampler. Labelled `{shard}`.
/// A healthy steady state is `0` on every shard.
pub const METRIC_SHARD_STRANDED_PENDING: &str = "harvest.shard.stranded_pending";

/// Gauge: current number of entries in the dead letter queue.
pub const METRIC_DLQ_ENTRIES: &str = "harvest.dlq.entries";

/// Counter: incremented once per dead-letter entry processed by an operator
/// redrive (issue #510). Labelled `{queue, outcome}` where `outcome` is one of
/// `redriven` / `skipped` / `failed`.
pub const METRIC_DLQ_REDRIVEN: &str = "harvest.dlq.redriven";

/// Counter: incremented each time a scheduled run is dispatched.
pub const METRIC_SCHEDULE_RUNS: &str = "harvest.schedule.runs";

/// Counter: incremented each time a scheduled run is skipped.
pub const METRIC_SCHEDULE_SKIPPED: &str = "harvest.schedule.skipped";

/// Counter: incremented when a completion trigger fires (issue #517).
///
/// Attributes: `trigger`, `outcome` (`started` | `skipped` | `deduped` |
/// `validation_failed` | `payload_too_large`).
pub const METRIC_COMPLETION_TRIGGER_FIRED: &str = "harvest.completion_trigger.fires";

/// Counter: incremented each time `POST /admin/schedules/{id}/trigger` fires a
/// one-off run (issue #343).
///
/// Labels: `schedule.name` (low-cardinality), `outcome` (`"fired"` or
/// `"skipped_overlap"` or `"rejected_paused"`).
pub const METRIC_SCHEDULE_MANUAL_TRIGGER: &str = "harvest.schedule.manual_trigger";

/// Counter: incremented when a schedule decision write fails due to a database error.
pub const METRIC_SCHEDULE_DECISION_WRITE_FAILED: &str = "harvest.schedule.decision_write_failed";

/// Counter: number of rows deleted by the retention janitor in one tick.
pub const METRIC_RETENTION_DELETED: &str = "harvest.retention.deleted";

/// Histogram: wall-clock latency of query handler invocations (seconds).
///
/// Labelled with `query.name` (low-cardinality handler name registered by the
/// workflow author). Per ADR-0001 cardinality rule, `execution.id` stays
/// span-only and is never a metric label.
pub const METRIC_QUERY_DURATION: &str = "harvest.query.duration";

/// Counter: incremented when a workflow task is picked up by the worker that
/// already holds that execution's replay state in its in-process LRU cache.
///
/// Labeled by `workflow` (workflow name). `execution.id` stays span-only per
/// the existing cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_CACHE_HIT: &str = "harvest.workflow.cache_hit";

/// Counter: incremented when a workflow task is picked up by a worker that
/// does NOT hold that execution's replay state in its in-process LRU cache,
/// causing a full event-history reload from Postgres.
///
/// Labeled by `workflow` (workflow name). `execution.id` stays span-only per
/// the existing cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_CACHE_MISS: &str = "harvest.workflow.cache_miss";

/// Counter: incremented once per `signal_external_workflow` call after the
/// terminal outcome is recorded in `harvest_events`.
///
/// Labels: `outcome` (`"delivered"` or `"failed"`), `reason_code` (only set
/// when `outcome == "failed"`; values: `"target_terminal"`, `"target_unknown"`).
///
/// Per ADR-0001 §7, `harvest.target.execution.id` and `harvest.signal.id` are
/// **span-only** and must never appear as metric labels.
pub const METRIC_EXTERNAL_SIGNAL_SENT: &str = "harvest.workflow.external_signal.sent";

/// OpenTelemetry span attribute: the signal name for `signal_external_workflow` spans.
///
/// Used in `harvest.signal.send` child spans. Low-cardinality (equals the
/// string literal passed to `ctx.signal_external_workflow`).
pub const ATTR_SIGNAL_NAME: &str = "harvest.signal.name";

/// OpenTelemetry span attribute: the target execution ID for cross-workflow signal spans.
///
/// Per ADR-0001 §7 cardinality rule this attribute is **span-only** and must
/// never be used as a metric label.
pub const ATTR_TARGET_EXECUTION_ID: &str = "harvest.target.execution.id";

/// OpenTelemetry span attribute: the signal ID for external signal spans.
pub const ATTR_SIGNAL_ID: &str = "harvest.signal.id";

/// Counter: incremented when a workflow execution is terminated because its
/// `deadline_at` elapsed before the workflow completed.
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_TIMEOUT: &str = "harvest.workflow.timeout";

/// Counter: incremented each time a workflow-task dispatch is abandoned because
/// it did not complete or suspend within `WorkerConfig::workflow_task_timeout`
/// (issue #494).
///
/// Each increment signals one reclamation of a worker concurrency slot from a
/// hung or blocking workflow body. After `poison_pill_threshold` consecutive
/// increments for the same execution the task is quarantined to the DLQ with
/// [`DeadLetterReason::WorkflowTaskTimeout`](crate::dlq::DeadLetterReason).
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_TASK_TIMEOUT: &str = "harvest.workflow.task_timeout";

/// Counter: incremented exactly once per run when a workflow execution exceeds its
/// declared soft SLA budget (`sla_deadline_at`) while still RUNNING/SUSPENDED.
///
/// This is a **soft, non-fatal signal** (issue #487): the run is never terminated.
/// A breaching run that later completes still reaches COMPLETED with its normal result.
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_SLA_BREACHED: &str = "harvest.workflow.sla_breached";

/// Counter: incremented each time a failed workflow execution is automatically
/// rescheduled for a retry run (issue #523).
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_RETRIES: &str = "harvest.workflow.retries";

/// Counter: incremented when a replay non-determinism (divergence) failure occurs.
///
/// Labeled by `workflow` (workflow name) and `build_id`.
pub const METRIC_WORKFLOW_NON_DETERMINISM: &str = "harvest.workflow.non_determinism";

/// Counter: incremented each time a workflow execution is paused by an operator
/// or the bounded-pause auto-resume scanner (issue #383).
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_PAUSED: &str = "harvest.workflow.paused";

/// Histogram: wall-clock seconds an execution spent in the `PAUSED` state,
/// recorded once on resume (issue #383).
///
/// Labeled by `workflow` (workflow name) and `queue` (task queue name).
/// `execution.id` stays span-only per the cardinality rule (ADR-0001 §7).
pub const METRIC_WORKFLOW_PAUSE_DURATION: &str = "harvest.workflow.pause_duration";

/// Counter: incremented each time a poison-pill task is quarantined to the
/// dead-letter queue after crashing `poison_pill_threshold` workers in a row
/// (issue #367).
///
/// Labeled by `queue` (task queue name) and `reason`
/// (`"poison_pill"`). `execution.id` stays span-only per ADR-0001 §7.
pub const METRIC_TASK_QUARANTINED: &str = "harvest.task.quarantined";

/// Counter: incremented each time an activity's circuit breaker trips open
/// (closed → open) or re-opens after a failed half-open probe (issue #369).
///
/// Labeled by `activity.name`. `execution.id` stays span-only per ADR-0001 §7.
pub const METRIC_CIRCUIT_TRIPPED: &str = "harvest.activity.circuit.tripped";

/// Counter: incremented each time an activity's circuit breaker recovers to the
/// closed state after a successful half-open probe (issue #369).
///
/// Labeled by `activity.name`. `execution.id` stays span-only per ADR-0001 §7.
pub const METRIC_CIRCUIT_CLOSED: &str = "harvest.activity.circuit.closed";

/// Counter: incremented each time a start request is admitted to a debounce
/// pending record — i.e. the burst is absorbed without starting a run (issue #499).
///
/// Labeled by `workflow` (workflow type name) and `debounce_key` (resolved key).
/// Per ADR-0001 §7, `execution.id` is span-only and must never appear here.
pub const METRIC_WORKFLOW_DEBOUNCED: &str = "harvest.workflow.debounced";

/// Counter: incremented each time the debounce scanner fires a pending record
/// and starts exactly one workflow execution (issue #499).
///
/// Labeled by `workflow` (workflow type name) and `queue` (task queue name).
/// Per ADR-0001 §7, `execution.id` is span-only and must never appear here.
pub const METRIC_DEBOUNCE_FIRED: &str = "harvest.workflow.debounce_fired";

/// Histogram: observed payload size in bytes at each write boundary (issue #252).
///
/// Emitted for every payload written to `harvest_events`, regardless of whether
/// it was accepted or rejected. Labeled with:
/// - `payload.kind`: the [`PayloadKind`] variant (e.g. `"ActivityInput"`)
/// - `workflow.type`: the workflow type name
/// - `activity.name`: the activity name (empty string when not applicable)
///
/// Per ADR-0001 §7, `execution.id` is span-only and must never appear here.
///
/// [`PayloadKind`]: crate::error::PayloadKind
pub const METRIC_PAYLOAD_BYTES: &str = "harvest.payload.bytes";

/// Counter: incremented each time a payload is rejected because it exceeds the
/// configured size cap (issue #252).
///
/// Labeled with `payload.kind` and `workflow.type`. Incrementing once per
/// rejection event (not per byte) so operators can alert on rejection rate.
///
/// Per ADR-0001 §7, `execution.id` is span-only.
pub const METRIC_PAYLOAD_REJECTED: &str = "harvest.payload.rejected";

/// Counter + bytes measure: a payload-bearing field was offloaded to an external
/// [`PayloadStore`](crate::payload_store::PayloadStore) via claim-check (issue #524).
///
/// Incremented once per offloaded field; the increment value is the **byte
/// length** of the offloaded payload, so operators get both an offload rate
/// (count of events) and total bytes offloaded.
///
/// Labels: `payload.field` (the event field name, e.g. `"output"`), `store.id`.
/// Per ADR-0001 §7, `execution.id` is span-only and must never appear here.
pub const METRIC_PAYLOAD_OFFLOADED: &str = "harvest.payload.offloaded";

/// Histogram: wall-clock seconds to fetch an offloaded payload back from the
/// external store on read/replay (issue #524).
///
/// Labels: `store.id`. Per ADR-0001 §7, `execution.id` is span-only.
pub const METRIC_PAYLOAD_OFFLOAD_FETCH_DURATION: &str = "harvest.payload.offload_fetch_duration";

/// Counter: incremented each time a new workflow start is blocked by an
/// active admission gate (issue #377).
///
/// Labels:
///   - `"scope_kind"` — the gate's scope kind (`"fleet"`, `"workflow_name"`,
///     `"queue"`, `"shard_id"`, or `"owner"`).
///   - `"reason_hash"` — first 8 chars of a stable SHA-256 of the reason
///     string to bound cardinality while preserving debuggability.
///
/// Per ADR-0001 §7, `execution.id` and `gate_id` are never metric labels.
pub const METRIC_ADMISSION_BLOCKED: &str = "harvest.admission.blocked";

/// Gauge: current number of active admission gates.
pub const METRIC_ADMISSION_GATES_ACTIVE: &str = "harvest.admission.gates_active";

/// Gauge: current available tokens in a rate limit bucket.
pub const METRIC_RATE_LIMIT_TOKENS_AVAILABLE: &str = "harvest.rate_limit.tokens_available";

/// Gauge: refill rate (tokens per second) for a rate limit bucket.
pub const METRIC_RATE_LIMIT_REFILL_RATE: &str = "harvest.rate_limit.refill_rate";

/// Counter: incremented when a task claim is throttled/skipped due to rate limiting.
pub const METRIC_RATE_LIMIT_THROTTLED: &str = "harvest.rate_limit.throttled";

/// Counter: incremented on each scheduler tick-loop fire attempt for a due schedule slot.
///
/// Labels:
/// - `schedule` — the workflow or DAG name (low-cardinality).
/// - `outcome` — one of:
///   - `"claimed"` — this replica atomically claimed the slot and will fire it.
///   - `"lost_race"` — another replica already holds a live claim for this
///     slot; this replica skips it without firing.
///
/// Use this metric to verify in Grafana / your alert stack that contention is
/// happening, claims are exclusive, and no replica is silently dominating the
/// fire path. See `docs/runbooks/ha-deployment.md` for thresholds.
pub const METRIC_SCHEDULE_FIRE_ATTEMPTS: &str = "harvest.schedule.fire_attempts";

/// Counter emitted once each time a schedule is automatically paused after
/// `consecutive_failure_limit` consecutive execution failures (issue #360).
///
/// Labels:
///   - `"schedule"` — the workflow name bound to the schedule.
///
/// Alert threshold: `harvest_schedule_auto_paused_total > 0` over any 5-minute
/// window. Each auto-pause event means operator action is required to resume.
pub const METRIC_SCHEDULE_AUTO_PAUSED: &str = "harvest.schedule.auto_paused";

/// Gauge: count of *in-flight* (RUNNING/SUSPENDED) executions whose durable
/// event history has grown past the configured soft
/// `continue_as_new_threshold` (issue #493).
///
/// Labeled by `workflow` (workflow type name). `execution.id` MUST NOT appear
/// here (ADR-0001 §7 cardinality rule).
///
/// Sampled on the same cadence as `harvest.queue.depth` (the
/// `spawn_queue_depth_sampler` interval). A non-zero value means at least one
/// in-flight workflow is accumulating history faster than its author drains it
/// via `continue_as_new`. Alert on sustained non-zero values so the operator
/// can nudge the author or enable `max_workflow_history_events`.
pub const METRIC_WORKFLOW_HISTORY_OVERSIZED: &str = "harvest.workflow.history_oversized";

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
/// Metric label: the activity name, dotted form used by circuit-breaker
/// counters (issue #369) to match the ADR-0001 `activity.name` attribute.
pub const METRIC_LABEL_ACTIVITY_NAME: &str = "activity.name";
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
/// Metric label: terminal outcome (e.g. `"delivered"`, `"failed"`).
pub const METRIC_LABEL_OUTCOME: &str = "outcome";
/// Metric label: reason code for external signal failure.
pub const METRIC_LABEL_REASON_CODE: &str = "reason_code";
/// Metric label: the completion trigger ID (issue #517).
pub const METRIC_LABEL_TRIGGER: &str = "trigger";
/// Metric label: admission gate scope kind (issue #377).
pub const METRIC_LABEL_SCOPE: &str = "scope";
/// Metric label: the build ID of the worker.
pub const METRIC_LABEL_BUILD_ID: &str = "build_id";

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
    ///
    /// **Not a terminal state.** Used only for the per-cycle
    /// `harvest.workflow.duration` histogram. Must never be passed to
    /// [`MetricsRecorder::record_workflow_terminal`].
    Suspended,
    /// Handler signalled `continue_as_new`: the current run is sealed and a
    /// fresh execution with the same `WorkflowId` is started in its place.
    ///
    /// Counted by `harvest.workflow.terminal{outcome="continued_as_new"}` but
    /// **excluded from the success-rate denominator**
    /// (`completed + failed + cancelled + timed_out`) because the logical
    /// workflow continues in a new execution.
    ContinuedAsNew,
    /// Gracefully cancelled via `cancel_workflow_execution`.
    Cancelled,
    /// Execution deadline (`deadline_at`) elapsed before the workflow completed.
    TimedOut,
    /// Force-killed via `terminate_workflow_execution` (operator override).
    Terminated,
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
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Terminated => "terminated",
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
    /// A completion trigger was fired/evaluated (issue #517).
    ///
    /// `outcome` is one of: `"started"`, `"skipped"`, `"deduped"`,
    /// `"validation_failed"`, `"payload_too_large"`.
    fn record_completion_trigger_fired(&self, trigger_id: &str, outcome: &str) {
        let _ = (trigger_id, outcome);
    }

    /// A new workflow start was blocked by an active admission gate (issue #377).
    ///
    /// `scope_kind` is one of: `"fleet"`, `"workflow_name"`, `"queue"`,
    /// `"shard_id"`, `"owner"`. `reason_hash` is the first 8 chars of a
    /// stable SHA-256 of the reason string for bounded cardinality.
    fn record_admission_blocked(&self, scope_kind: &str, reason_hash: &str) {
        let _ = (scope_kind, reason_hash);
    }

    /// The active gate count changed (useful for alerting on nonzero gates).
    fn record_admission_gates_active(&self, count: i64) {
        let _ = count;
    }

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

    /// A workflow execution reached a terminal state.
    ///
    /// Fires **exactly once** per execution at the terminal-state transition.
    /// Unlike [`record_workflow_completed`](Self::record_workflow_completed),
    /// this counter never fires for suspended executor cycles — a workflow
    /// that suspends N times and then completes produces exactly one
    /// `Completed` increment.
    ///
    /// `outcome` must be one of the six terminal variants of [`WorkflowStatus`]:
    /// `Completed`, `Failed`, `Cancelled`, `TimedOut`, `Terminated`, or
    /// `ContinuedAsNew`. Callers must **never** pass `Suspended`.
    ///
    /// Maps to the counter [`METRIC_WORKFLOW_TERMINAL`] with labels
    /// `workflow`, `queue`, and `outcome`.
    /// Per ADR-0001 §7, `execution.id` must never be a label here.
    fn record_workflow_terminal(&self, workflow_name: &str, queue: &str, outcome: WorkflowStatus) {
        let _ = (workflow_name, queue, outcome);
    }

    /// A workflow execution rotated using continue-as-new.
    fn record_workflow_continue_as_new(&self, workflow_name: &str) {
        let _ = workflow_name;
    }

    /// A workflow replay non-determinism (divergence) failure was detected.
    fn record_workflow_non_determinism(&self, workflow_name: &str, build_id: &str) {
        let _ = (workflow_name, build_id);
    }

    /// Gauge snapshot: `count` in-flight executions for `workflow_name` whose
    /// durable event count exceeds the configured soft continue-as-new
    /// threshold (issue #493).
    ///
    /// Callers emit one call per distinct workflow name per sampler tick so
    /// the gauge resets cleanly when oversized executions drain. A `count` of
    /// `0` should be emitted for known workflow names once the oversized count
    /// drops to zero so the gauge falls back to zero rather than going stale.
    ///
    /// Maps to [`METRIC_WORKFLOW_HISTORY_OVERSIZED`].
    /// Per ADR-0001 §7, `execution.id` must never be a label here.
    fn record_workflow_history_oversized(&self, workflow_name: &str, count: u64) {
        let _ = (workflow_name, count);
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

    /// A task was dispatched from the named queue (issue #515).
    ///
    /// Recorded once per `dispatch_task` call. Lets operators confirm the live
    /// per-queue dispatch split matches `WorkerConfig::queue_weights`.
    /// Maps to the counter [`METRIC_QUEUE_DISPATCHED`].
    fn record_task_dispatched(&self, queue_name: &str) {
        let _ = queue_name;
    }

    /// Wall-clock seconds a task waited between becoming eligible
    /// (`scheduled_at`) and being claimed by a worker (`started_at`).
    ///
    /// Recorded at claim time. Only `queue_name` is used as a label;
    /// `execution.id` / `activity.id` are span-only per ADR-0001 §7.
    fn record_schedule_to_start(&self, queue_name: &str, wait_secs: f64) {
        let _ = (queue_name, wait_secs);
    }

    /// Age in seconds of the oldest currently-unclaimed eligible task in a
    /// queue. Sampled alongside `record_queue_depth`. Reports `0` when the
    /// queue is drained so stale gauge values do not linger.
    fn record_queue_oldest_pending_age(&self, queue_name: &str, age_secs: f64) {
        let _ = (queue_name, age_secs);
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

    /// Record the current available tokens for a rate limit bucket key.
    ///
    /// Maps to the gauge `harvest.rate_limit.tokens_available{key}`.
    fn record_rate_limit_tokens_available(&self, key: &str, tokens: f64) {
        let _ = (key, tokens);
    }

    /// Record the refill rate (tokens per second) for a rate limit bucket key.
    ///
    /// Maps to the gauge `harvest.rate_limit.refill_rate{key}`.
    fn record_rate_limit_refill_rate(&self, key: &str, refill_rate: f64) {
        let _ = (key, refill_rate);
    }

    /// Record that a task claim was throttled/skipped due to rate limiting.
    ///
    /// Maps to the counter `harvest.rate_limit.throttled{key}`.
    fn record_rate_limit_throttled(&self, key: &str) {
        let _ = key;
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

    /// Current number of claimable pending tasks on a shard that has no
    /// covering live worker (issue #522).
    ///
    /// Emitted per shard by the stranded-work sampler. When a shard is
    /// covered by at least one live worker the value is emitted as `0`.
    /// A non-zero value means work is stranded: workflows rendezvous-hashed
    /// onto this shard will never reach `RUNNING` until a covering worker is
    /// started.
    ///
    /// Maps to the gauge `METRIC_SHARD_STRANDED_PENDING{shard}`.
    fn record_shard_stranded_pending(&self, shard: u16, count: u64) {
        let _ = (shard, count);
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
    /// `reason` is one of `"paused"`, `"max_active_runs_reached"`,
    /// `"catchup_disabled"`, or `"catchup_window_exceeded"`.
    ///
    /// Maps to the metric `harvest_schedule_skipped_total{kind, name, reason}`.
    fn record_schedule_skipped(&self, kind: &str, name: &str, reason: &str) {
        let _ = (kind, name, reason);
    }

    /// Record `count` skips of the same `(kind, name, reason)` at once.
    ///
    /// Used when a single bounded-catchup recovery drops many missed slots: the
    /// counter contract is "one increment per dropped slot", but looping that
    /// many synchronous calls inside the scheduler tick would stall the thread on
    /// a large recovery. Recorders that back a real counter should override this
    /// with a single batched increment (the metrics-rs adapter does). The default
    /// delegates to [`Self::record_schedule_skipped`], bounded so a custom
    /// recorder cannot be forced into an unbounded loop by a huge backlog.
    ///
    /// Maps to the metric `harvest_schedule_skipped_total{kind, name, reason}`
    /// incremented by `count`.
    fn record_schedule_skipped_n(&self, kind: &str, name: &str, reason: &str, count: u64) {
        for _ in 0..count.min(10_000) {
            self.record_schedule_skipped(kind, name, reason);
        }
    }

    /// A scheduler decision write failed due to a database error.
    ///
    /// Maps to the counter `harvest.schedule.decision_write_failed`.
    fn record_schedule_decision_write_failed(&self) {}

    /// A manual `POST /admin/schedules/{id}/trigger` call completed (issue #343).
    ///
    /// `schedule_name` is the workflow or DAG name the schedule targets
    /// (low-cardinality, same cardinality as `record_schedule_run`).
    /// `outcome` is `"fired"`, `"skipped_overlap"`, or `"rejected_paused"`.
    ///
    /// Maps to the counter `harvest.schedule.manual_trigger{schedule.name, outcome}`.
    fn record_schedule_manual_trigger(&self, schedule_name: &str, outcome: &str) {
        let _ = (schedule_name, outcome);
    }

    /// A tick-loop fire attempt for a due schedule slot (issue #350).
    ///
    /// `schedule_name` is the workflow or DAG name (low-cardinality).
    /// `outcome` is one of:
    /// - `"claimed"` — this replica won the atomic claim race and will fire.
    /// - `"lost_race"` — another replica already holds a live claim for this
    ///   slot; this replica skips it without firing.
    ///
    /// Maps to the counter [`METRIC_SCHEDULE_FIRE_ATTEMPTS`].
    fn record_schedule_fire_attempt(&self, schedule_name: &str, outcome: &str) {
        let _ = (schedule_name, outcome);
    }

    /// A schedule was automatically paused after reaching `consecutive_failure_limit`
    /// consecutive execution failures (issue #360).
    ///
    /// `schedule_name` is the workflow name bound to the schedule (low-cardinality).
    /// Emitted once per auto-pause event. Operators should alert on
    /// `harvest_schedule_auto_paused_total > 0` and resume the schedule once
    /// the underlying issue is resolved.
    ///
    /// Maps to the counter [`METRIC_SCHEDULE_AUTO_PAUSED`].
    fn record_schedule_auto_paused(&self, schedule_name: &str) {
        let _ = schedule_name;
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

    /// A workflow task was served from the in-process LRU cache (warm path).
    ///
    /// The worker already holds this execution's event history in its local
    /// `WorkflowCache`, so only delta events (new timer firings / signals) need
    /// to be fetched from Postgres rather than the full history.
    ///
    /// Maps to the counter `harvest.workflow.cache_hit{workflow}`.
    fn record_workflow_cache_hit(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow task required a full event-history reload from Postgres (cold path).
    ///
    /// The worker's LRU cache did not contain an entry for this execution —
    /// either because this is the first task for this execution on this worker,
    /// the entry was evicted by LRU pressure, or sticky routing is disabled.
    ///
    /// Maps to the counter `harvest.workflow.cache_miss{workflow}`.
    fn record_workflow_cache_miss(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow execution was terminated because its `deadline_at` elapsed.
    ///
    /// Maps to the counter `harvest.workflow.timeout{workflow, queue}`.
    fn record_workflow_timeout(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow-task dispatch was abandoned because it did not complete or
    /// suspend within `WorkerConfig::workflow_task_timeout` (issue #494).
    ///
    /// Each call represents one reclaimed worker concurrency slot. After
    /// `poison_pill_threshold` consecutive calls for the same execution the
    /// task is escalated to the DLQ.
    ///
    /// Maps to the counter `harvest.workflow.task_timeout{workflow, queue}`.
    fn record_workflow_task_timeout(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A workflow execution has exceeded its declared soft SLA budget while
    /// still RUNNING/SUSPENDED (issue #487).
    ///
    /// Emitted **exactly once per run** by the SLA breach scanner.  The run is
    /// never terminated; a breaching run that later completes still reaches
    /// COMPLETED with its normal result.
    ///
    /// Maps to the counter `harvest.workflow.sla_breached{workflow, queue}`.
    fn record_workflow_sla_breach(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A failed workflow execution was automatically rescheduled for a retry run (issue #523).
    ///
    /// Maps to the counter `harvest.workflow.retries{workflow, queue}`.
    fn record_workflow_retry(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A poison-pill task was quarantined to the dead-letter queue after
    /// crashing the configured number of workers in a row (issue #367).
    ///
    /// Maps to the counter `harvest.task.quarantined{queue, reason}`.
    fn record_task_quarantined(&self, queue: &str, reason: &str) {
        let _ = (queue, reason);
    }

    /// An operator redrive processed one dead-letter entry (issue #510).
    ///
    /// Maps to the counter `harvest.dlq.redriven{queue, outcome}` where
    /// `outcome` is `redriven` / `skipped` / `failed`. Per the ADR-0001
    /// cardinality rule, `execution.id` is never a metric label.
    fn record_dlq_redriven(&self, queue: &str, outcome: &str) {
        let _ = (queue, outcome);
    }

    /// A workflow execution was paused by an operator or the auto-resume
    /// scanner (issue #383).
    ///
    /// Maps to the counter `harvest.workflow.paused{workflow, queue}`.
    fn record_workflow_paused(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// A paused workflow execution was resumed; `duration_secs` is the
    /// wall-clock time it spent in the `PAUSED` state (issue #383).
    ///
    /// Maps to the histogram `harvest.workflow.pause_duration{workflow, queue}`.
    fn record_workflow_pause_duration(&self, workflow_name: &str, queue: &str, duration_secs: f64) {
        let _ = (workflow_name, queue, duration_secs);
    }

    /// An activity's circuit breaker tripped open or re-opened after a failed
    /// half-open probe (issue #369).
    ///
    /// Maps to the counter `harvest.activity.circuit.tripped{activity.name}`.
    fn record_circuit_tripped(&self, activity_name: &str) {
        let _ = activity_name;
    }

    /// An activity's circuit breaker recovered to closed after a successful
    /// half-open probe (issue #369).
    ///
    /// Maps to the counter `harvest.activity.circuit.closed{activity.name}`.
    fn record_circuit_closed(&self, activity_name: &str) {
        let _ = activity_name;
    }

    /// A payload was observed at a write boundary (issue #252).
    ///
    /// Called for every payload written (accepted or rejected) to
    /// `harvest_events`. Maps to the histogram `harvest.payload.bytes`
    /// with labels `payload.kind`, `workflow.type`, and `activity.name`.
    ///
    /// `activity_name` is `None` for non-activity payloads (signal, side-effect,
    /// workflow-input).
    fn record_payload_observed(
        &self,
        kind: &crate::error::PayloadKind,
        workflow_type: &str,
        activity_name: Option<&str>,
        observed_bytes: u64,
    ) {
        let _ = (kind, workflow_type, activity_name, observed_bytes);
    }

    /// A payload was rejected because it exceeded the configured cap (issue #252).
    ///
    /// Maps to the counter `harvest.payload.rejected` with labels
    /// `payload.kind` and `workflow.type`.
    fn record_payload_rejected(&self, kind: &crate::error::PayloadKind, workflow_type: &str) {
        let _ = (kind, workflow_type);
    }

    /// A payload-bearing field was offloaded to an external store (issue #524).
    ///
    /// Maps to the [`METRIC_PAYLOAD_OFFLOADED`] counter, incremented by
    /// `byte_len`. `field` is the event field name (`"input"`, `"output"`, …);
    /// `store_id` identifies the configured store.
    fn record_payload_offloaded(&self, field: &str, store_id: &str, byte_len: u64) {
        let _ = (field, store_id, byte_len);
    }

    /// An offloaded payload was fetched back from the external store on
    /// read/replay (issue #524).
    ///
    /// Maps to the [`METRIC_PAYLOAD_OFFLOAD_FETCH_DURATION`] histogram.
    fn record_payload_offload_fetch(&self, store_id: &str, duration_secs: f64) {
        let _ = (store_id, duration_secs);
    }

    /// A cross-workflow external signal was sent.
    ///
    /// Maps to the counter `harvest.workflow.external_signal.sent` with attributes:
    /// - `outcome`: `"delivered"` or `"failed"`
    /// - `reason_code`: `"target_terminal"` or `"target_unknown"` (optional)
    fn record_external_signal_sent(&self, outcome: &str, reason_code: Option<&str>) {
        let _ = (outcome, reason_code);
    }

    /// Record one external cancel dispatch outcome (`outcome`: `"delivered"` / `"failed"`).
    fn record_external_cancel_sent(&self, outcome: &str, reason_code: Option<&str>) {
        let _ = (outcome, reason_code);
    }

    /// A start request was absorbed by a debounce pending record (issue #499).
    ///
    /// Maps to the counter [`METRIC_WORKFLOW_DEBOUNCED`] with label `workflow`.
    /// The debounce key is deliberately **not** a label: it is resolved from
    /// user/tenant input and would create unbounded metric cardinality
    /// (ADR-0001 §7). The raw key remains available in logs and the
    /// `GET /admin/debounce` endpoint.
    fn record_workflow_debounced(&self, workflow_name: &str) {
        let _ = workflow_name;
    }

    /// The debounce scanner fired a pending record and started one execution (issue #499).
    ///
    /// Maps to the counter [`METRIC_DEBOUNCE_FIRED`] with labels
    /// `workflow` and `queue`.
    fn record_debounce_fired(&self, workflow_name: &str, queue: &str) {
        let _ = (workflow_name, queue);
    }

    /// Whether this recorder actually forwards samples anywhere.
    ///
    /// Defaults to `true` for every real recorder. [`NoOpMetrics`] overrides it to
    /// `false` so callers can skip *expensive sample-production work* (e.g. the
    /// per-poll-interval `oldest_pending_ages` / `queue_depths` sampler SQL) when
    /// no recorder is configured — the per-event `record_*` calls are already
    /// zero-cost, but the queries that feed gauges are not. Treat this as a hint
    /// for guarding observation work, never for changing engine behavior.
    fn is_enabled(&self) -> bool {
        true
    }
}

/// Default metrics recorder that discards every sample.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpMetrics;

impl MetricsRecorder for NoOpMetrics {
    fn is_enabled(&self) -> bool {
        false
    }
}

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
        assert_eq!(
            METRIC_SCHEDULE_DECISION_WRITE_FAILED,
            "harvest.schedule.decision_write_failed"
        );
        assert_eq!(METRIC_RETENTION_DELETED, "harvest.retention.deleted");
        assert_eq!(METRIC_WORKFLOW_TIMEOUT, "harvest.workflow.timeout");
        assert_eq!(METRIC_TASK_QUARANTINED, "harvest.task.quarantined");
        assert_eq!(
            METRIC_WORKFLOW_NON_DETERMINISM,
            "harvest.workflow.non_determinism"
        );
        assert_eq!(
            METRIC_WORKFLOW_TASK_TIMEOUT,
            "harvest.workflow.task_timeout"
        );
        assert_eq!(
            METRIC_QUEUE_SCHEDULE_TO_START,
            "harvest.queue.schedule_to_start"
        );
        assert_eq!(
            METRIC_QUEUE_OLDEST_PENDING_AGE,
            "harvest.queue.oldest_pending_age"
        );
    }

    #[test]
    fn metric_label_constants_have_correct_names() {
        assert_eq!(METRIC_LABEL_WORKFLOW, "workflow");
        assert_eq!(METRIC_LABEL_WORKFLOW_TYPE, "workflow.type");
        assert_eq!(METRIC_LABEL_ACTIVITY, "activity");
        assert_eq!(METRIC_LABEL_QUEUE, "queue");
        assert_eq!(METRIC_LABEL_BUILD_ID, "build_id");
    }

    // -----------------------------------------------------------------------
    // RED-phase tests for harvest.workflow.terminal counter (issue #519)
    // These tests fail until the implementation is complete.
    // -----------------------------------------------------------------------

    #[test]
    fn metric_workflow_terminal_constant_has_correct_name() {
        // The terminal counter name is specified by ADR-0001 §7 naming convention.
        assert_eq!(METRIC_WORKFLOW_TERMINAL, "harvest.workflow.terminal");
    }

    #[test]
    fn workflow_status_terminal_variants_stringify_correctly() {
        // Outcome label values for the terminal counter are
        // bounded: completed | failed | cancelled | timed_out | terminated | continued_as_new
        assert_eq!(WorkflowStatus::Cancelled.as_str(), "cancelled");
        assert_eq!(WorkflowStatus::TimedOut.as_str(), "timed_out");
        assert_eq!(WorkflowStatus::Terminated.as_str(), "terminated");
    }

    #[test]
    fn workflow_status_suspended_is_not_a_terminal_counter_outcome() {
        // Suspended executor cycles must NOT increment the terminal counter.
        // Verify that "suspended" is not in the bounded terminal outcome set.
        let terminal_outcomes = [
            WorkflowStatus::Completed.as_str(),
            WorkflowStatus::Failed.as_str(),
            WorkflowStatus::Cancelled.as_str(),
            WorkflowStatus::TimedOut.as_str(),
            WorkflowStatus::Terminated.as_str(),
            WorkflowStatus::ContinuedAsNew.as_str(),
        ];
        assert!(
            !terminal_outcomes.contains(&WorkflowStatus::Suspended.as_str()),
            "Suspended must not appear in the terminal outcome label set; \
             a suspended executor cycle does not mark a workflow as terminal"
        );
    }

    #[test]
    fn record_workflow_terminal_has_noop_default() {
        // MetricsRecorder::record_workflow_terminal must exist with a no-op
        // default body so existing MetricsRecorder implementations compile
        // without changes.
        let rec = NoOpMetrics;
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Completed);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Failed);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Cancelled);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::TimedOut);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::Terminated);
        rec.record_workflow_terminal("onboarding", "default", WorkflowStatus::ContinuedAsNew);
        // Calling for Suspended is allowed by the type system but callers
        // must never do so in practice (enforced by worker.rs call sites).
    }

    #[test]
    fn record_workflow_terminal_fires_once_per_distinct_outcome() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct TerminalCounter(AtomicUsize);

        impl MetricsRecorder for TerminalCounter {
            fn record_workflow_terminal(&self, _wf: &str, _q: &str, outcome: WorkflowStatus) {
                // The counter should fire exactly once per terminal outcome
                // and never for Suspended.
                assert!(
                    !matches!(outcome, WorkflowStatus::Suspended),
                    "record_workflow_terminal must not be called with Suspended"
                );
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let counter = Arc::new(TerminalCounter::default());
        for outcome in [
            WorkflowStatus::Completed,
            WorkflowStatus::Failed,
            WorkflowStatus::Cancelled,
            WorkflowStatus::TimedOut,
            WorkflowStatus::Terminated,
            WorkflowStatus::ContinuedAsNew,
        ] {
            counter.record_workflow_terminal("wf", "q", outcome);
        }
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            6,
            "should emit once for each of the 6 terminal outcomes"
        );
    }

    #[test]
    fn execution_id_is_not_a_parameter_of_record_workflow_terminal() {
        // ADR-0001 §7 cardinality rule: execution.id is span-only.
        // This test verifies by construction that record_workflow_terminal
        // accepts only (workflow_name, queue, outcome) — no execution id.
        let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        rec.record_workflow_terminal("billing", "default", WorkflowStatus::Completed);
        // If execution.id were a parameter the call above would require an
        // extra UUID argument and this test would fail to compile.
    }

    // ── Workflow-task timeout metric tests (issue #494) ───────────────────

    #[test]
    fn record_workflow_task_timeout_has_noop_default() {
        // MetricsRecorder::record_workflow_task_timeout must exist with a
        // no-op default so existing implementations compile without changes.
        let rec = NoOpMetrics;
        rec.record_workflow_task_timeout("onboarding", "default");
    }

    #[test]
    fn execution_id_is_not_a_parameter_of_record_workflow_task_timeout() {
        // ADR-0001 §7 cardinality rule: execution.id is span-only.
        let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        rec.record_workflow_task_timeout("billing", "default");
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
    fn shard_stranded_pending_gauge_has_default_noop_impl() {
        // record_shard_stranded_pending must exist on MetricsRecorder with (shard: u16, count: u64).
        // METRIC_SHARD_STRANDED_PENDING is the constant; this verifies shape + no-op (issue #522).
        let rec = NoOpMetrics;
        rec.record_shard_stranded_pending(0, 0);
        rec.record_shard_stranded_pending(1, 42);
        // verify the constant is exported
        assert_eq!(
            METRIC_SHARD_STRANDED_PENDING,
            "harvest.shard.stranded_pending"
        );
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
        rec.record_workflow_non_determinism("onboarding", "v1.0.0");
        rec.record_schedule_to_start("default", 1.5);
        rec.record_queue_oldest_pending_age("default", 30.0);
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
    fn queue_latency_metrics_have_noop_defaults() {
        let rec = NoOpMetrics;
        // Both new queue latency methods must compile with only queue_name and
        // the value (no execution.id) and must not panic on the no-op recorder.
        rec.record_schedule_to_start("default", 0.0);
        rec.record_schedule_to_start("priority", 1.234);
        rec.record_queue_oldest_pending_age("default", 0.0);
        rec.record_queue_oldest_pending_age("priority", 42.5);
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
