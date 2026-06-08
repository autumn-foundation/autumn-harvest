//! Event types for the workflow event-sourcing engine.
//!
//! Every state change in a workflow execution is represented as an event
//! appended to `harvest_events`. Replay re-executes the workflow function
//! from the beginning, feeding recorded results back instead of re-executing
//! activities.
//!
//! **Append-only invariant:** Never remove or reorder variants. Stored JSON
//! must deserialize into the same variants after deployment.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::TimeoutType;
use crate::types::{
    ActivityExecId, ExecutionId, ExternalActivityToken, ExternalSignalId, TimerId, UpdateId,
    WorkerId,
};

fn default_error_type() -> String {
    "Error".to_string()
}

/// Which deterministic built-in produced a [`WorkflowEvent::SideEffectRecorded`]
/// event (issue #384).
///
/// This is a **bounded** enum — it has a fixed, small set of variants and is
/// safe to use as a low-cardinality `OTel` attribute value (ADR-0001 §7). Each of
/// the `WorkflowContext` deterministic primitives lowers onto a single
/// `SideEffectRecorded` event and stamps the originating helper here so replay
/// diagnostics and metrics can distinguish a clock read from a UUID mint without
/// inspecting the recorded value.
///
/// **Append-only invariant:** never remove or rename a variant. The serialised
/// form is the bare variant name (`"Now"`, `"Uuid"`, …); stored events depend on
/// it. New helper kinds are added at the end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SideEffectKind {
    /// `ctx.system_now()` / `ctx.system_time_now()` — a captured wall-clock instant.
    Now,
    /// `ctx.new_uuid()` — a captured `UUIDv7`.
    Uuid,
    /// `ctx.random_u64()` / `ctx.random_f64()` / `ctx.random_range(..)` — a captured draw.
    Random,
    /// `ctx.side_effect(name, f)` — an author-named one-shot value capture.
    Custom,
}

impl SideEffectKind {
    /// Stable, low-cardinality string label for metrics / diagnostics.
    ///
    /// These values are bounded (one per variant) and safe to use as an `OTel`
    /// attribute value per ADR-0001 §7.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Now => "now",
            Self::Uuid => "uuid",
            Self::Random => "random",
            Self::Custom => "custom",
        }
    }
}

/// All possible events in a workflow's history.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum WorkflowEvent {
    // ── Lifecycle ──────────────────────────────────────────────────
    /// A new workflow execution has started.
    WorkflowStarted {
        /// The JSON payload used to start the workflow.
        input: serde_json::Value,
        /// Time when the workflow was initiated.
        timestamp: DateTime<Utc>,
    },
    /// The workflow ran to completion without an error.
    WorkflowCompleted {
        /// The JSON result returned by the workflow function.
        output: serde_json::Value,
    },
    /// The workflow panicked or returned a non-recoverable error.
    WorkflowFailed {
        /// String representation of the failure.
        error: String,
    },
    /// The workflow was intentionally cancelled (e.g., via API or parent workflow).
    WorkflowCancelled {
        /// The reason given for cancellation.
        reason: String,
    },

    // ── Activities ────────────────────────────────────────────────
    /// An activity was requested by the workflow.
    ActivityScheduled {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// The name of the registered activity handler.
        name: String,
        /// JSON input for the activity.
        input: serde_json::Value,
        /// Target worker queue.
        queue: String,
    },
    /// A worker picked up the activity and began executing it.
    ActivityStarted {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// The worker instance running the activity.
        worker_id: WorkerId,
    },
    /// The activity finished executing successfully.
    ActivityCompleted {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// The JSON result returned by the activity.
        output: serde_json::Value,
    },
    /// The activity returned an error or panicked.
    ///
    /// ## Backward-compatibility note (issue #227)
    ///
    /// `error_type` and `non_retryable` were added after the initial release.
    /// Old events stored without these fields deserialise cleanly via
    /// `#[serde(default)]`: `error_type` falls back to `"Error"` and
    /// `non_retryable` falls back to `false`. The append-only invariant is
    /// preserved — no variants removed, no renames.
    ActivityFailed {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Human-readable string representation of the failure.
        error: String,
        /// How many times the activity has failed so far.
        attempt: u32,
        /// Low-cardinality error-type name for metrics and policy matching.
        ///
        /// Defaults to `"Error"` for events stored before issue #227.
        #[serde(default = "default_error_type")]
        error_type: String,
        /// When `true`, the worker skipped retry and routed to DLQ immediately.
        ///
        /// Defaults to `false` for events stored before issue #227.
        #[serde(default)]
        non_retryable: bool,
        /// Optional structured details preserved from
        /// [`ActivityFailure::with_details`](crate::failure::ActivityFailure::with_details).
        ///
        /// Defaults to `None` for events stored before issue #227 or for
        /// failures returned via the legacy `Err(String)` path. Omitted from
        /// the serialised form when `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<serde_json::Value>,
    },
    /// The activity exceeded its allocated `start_to_close` or `heartbeat` timeout.
    ActivityTimedOut {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Which timeout triggered the failure.
        timeout_type: TimeoutType,
    },
    /// The activity successfully sent a heartbeat.
    ActivityHeartbeat {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// JSON payload attached to the heartbeat, used to resume progress after failures.
        details: serde_json::Value,
    },

    // ── Timers ────────────────────────────────────────────────────
    /// The workflow requested a durable sleep/timer.
    TimerStarted {
        /// The ID used to wake the workflow later.
        timer_id: TimerId,
        /// Duration in seconds (Duration is not JSON-serializable natively).
        duration_secs: u64,
    },
    /// The requested timer elapsed and the workflow can wake up.
    TimerFired {
        /// The ID that just finished waiting.
        timer_id: TimerId,
    },

    // ── Signals ───────────────────────────────────────────────────
    /// An external signal arrived while the workflow was waiting.
    SignalReceived {
        /// Name of the signal channel.
        signal_name: String,
        /// JSON payload delivered by the signal.
        payload: serde_json::Value,
    },

    // ── Child workflows ───────────────────────────────────────────
    /// A sub-workflow was scheduled by a parent workflow.
    ChildWorkflowStarted {
        /// The ID of the spawned execution.
        child_id: ExecutionId,
        /// The target child workflow handler.
        workflow_name: String,
        /// The input passed to the child workflow.
        input: serde_json::Value,
    },
    /// The spawned sub-workflow completed successfully.
    ChildWorkflowCompleted {
        /// The ID of the spawned execution.
        child_id: ExecutionId,
        /// Result of the completed workflow.
        output: serde_json::Value,
    },
    /// The spawned sub-workflow encountered a fatal error.
    ChildWorkflowFailed {
        /// The ID of the spawned execution.
        child_id: ExecutionId,
        /// The reason the child failed.
        error: String,
    },

    // ── Markers ───────────────────────────────────────────────────
    /// An explicit version change or side-effect marker was recorded.
    MarkerRecorded {
        /// The name of the recorded side effect (e.g. version block name).
        name: String,
        /// Arbitrary data recorded with the marker.
        details: serde_json::Value,
    },

    // ── Continue-as-new ───────────────────────────────────────────
    /// The workflow signalled `continue_as_new`. This is the terminal event
    /// for the current execution; a fresh execution sharing the same logical
    /// `WorkflowId` is started in its place with the recorded `input`.
    WorkflowContinuedAsNew {
        /// The execution ID of the freshly started run that succeeds this one.
        new_exec_id: ExecutionId,
        /// The JSON payload passed to the next iteration of the workflow.
        input: serde_json::Value,
    },

    // ── Local activities (issue #98) ─────────────────────────────────────
    /// A local activity was dispatched for inline execution on the workflow
    /// worker. Unlike regular activities, local activities never write a row to
    /// `harvest_task_queue`; the worker runs the handler in the same Tokio task
    /// that drives the workflow loop.
    LocalActivityScheduled {
        /// Unique ID for this local activity attempt sequence.
        activity_id: ActivityExecId,
        /// The name of the registered activity handler.
        name: String,
        /// JSON input for the activity.
        input: serde_json::Value,
    },
    /// A local activity finished executing successfully.
    LocalActivityCompleted {
        /// Unique ID matching the corresponding `LocalActivityScheduled`.
        activity_id: ActivityExecId,
        /// The JSON result returned by the activity handler.
        output: serde_json::Value,
    },
    /// A local activity attempt returned an error or the handler panicked.
    ///
    /// Multiple `LocalActivityFailed` events may appear in sequence (one per
    /// attempt) before a terminal `LocalActivityCompleted` (retry eventually
    /// succeeded) or before the retry budget is exhausted (followed by a
    /// `LocalActivityExhausted` event that marks the terminal state).
    LocalActivityFailed {
        /// Unique ID matching the corresponding `LocalActivityScheduled`.
        activity_id: ActivityExecId,
        /// String representation of the failure.
        error: String,
        /// How many attempts have been made so far (1-based).
        attempt: u32,
    },

    // ── External activity completion (issue #92) ──────────────────────
    /// An external activity was scheduled and is awaiting completion via the
    /// management API task-token endpoint. The `token` is a single-use handle
    /// that external systems round-trip through `/activities/external/{token}/complete`.
    ActivityAwaitingExternal {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Opaque token that external systems use to complete or fail the activity.
        token: ExternalActivityToken,
        /// The name of the registered activity handler.
        name: String,
        /// JSON input for the activity.
        input: serde_json::Value,
        /// Target worker queue (informational; external activities don't occupy a slot).
        queue: String,
        /// Maximum seconds the external system has to deliver a result.
        schedule_to_close_secs: u64,
    },
    /// An external system delivered a successful result via the management API.
    ActivityCompletedExternally {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Token that was used to complete the activity.
        token: ExternalActivityToken,
        /// The JSON result returned by the external system.
        output: serde_json::Value,
    },
    /// An external system reported a failure via the management API.
    ActivityFailedExternally {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Token that was used to fail the activity.
        token: ExternalActivityToken,
        /// String representation of the failure.
        error: String,
        /// Whether the failure is retryable per the activity's `RetryPolicy`.
        retryable: bool,
    },
    /// The schedule-to-close deadline for an external activity was extended via
    /// the management API heartbeat endpoint.
    ActivityExternalDeadlineExtended {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// Token of the activity whose deadline was extended.
        token: ExternalActivityToken,
    },

    // ── Updates (issue #140) ──────────────────────────────────────────────
    /// An update request passed its validator and was durably admitted into
    /// the workflow's event history. The `update_id` correlates with the
    /// paired `UpdateCompleted` or `UpdateFailed` event.
    ///
    /// Validator failures leave **no trace** in history — only admitted updates
    /// produce this event.
    UpdateAdmitted {
        /// Unique ID for this update invocation. Stable across worker restarts.
        update_id: UpdateId,
        /// The name of the registered update handler.
        name: String,
        /// JSON payload delivered with the update request.
        input: serde_json::Value,
        /// Time when the update was admitted.
        timestamp: DateTime<Utc>,
    },
    /// The update handler ran to completion and returned a value.
    UpdateCompleted {
        /// Unique ID matching the corresponding `UpdateAdmitted`.
        update_id: UpdateId,
        /// The JSON result returned by the update handler.
        output: serde_json::Value,
    },
    /// The update handler returned an error.
    UpdateFailed {
        /// Unique ID matching the corresponding `UpdateAdmitted`.
        update_id: UpdateId,
        /// String representation of the handler error.
        error: String,
    },

    // ── Workflow reset (issue #148) ─────────────────────────────────────────
    /// Marker appended to the forked execution after the carried-over history.
    ///
    /// Replay treats this as informational: it records why the fork exists
    /// without corresponding to a workflow command.
    WorkflowResetFork {
        /// The source execution that was reset.
        reset_from_exec_id: ExecutionId,
        /// Last source event copied into this fork.
        reset_to_event_id: i64,
        /// Operator-supplied recovery reason.
        reason: String,
        /// Operator identity for audit.
        operator_id: String,
    },
    /// Marker appended to the source execution when a reset fork supersedes it.
    WorkflowResetTerminated {
        /// The forked execution that should continue forward.
        reset_to_exec_id: ExecutionId,
        /// Operator-supplied recovery reason.
        reason: String,
        /// Operator identity for audit.
        operator_id: String,
    },
    /// All retry attempts for a local activity were exhausted. Appended
    /// immediately after the final `LocalActivityFailed` event so replay can
    /// identify the terminal state without knowing the current retry policy.
    ///
    /// This makes the terminal-vs-in-progress distinction policy-invariant:
    /// if this event is present the activity is unambiguously done; if only
    /// `LocalActivityFailed` events are present (without a following
    /// `LocalActivityExhausted`) the worker crashed between retries and must
    /// continue from the next attempt.
    LocalActivityExhausted {
        /// Unique ID matching the corresponding `LocalActivityScheduled`.
        activity_id: ActivityExecId,
        /// Error from the final attempt.
        error: String,
        /// Total attempts that were made (equals `max_attempts`).
        attempt: u32,
    },

    // ── External workflow signals (issue #330) ────────────────────────────
    /// A workflow requested delivery of a named signal to another running
    /// workflow by `ExecutionId`.
    ///
    /// The `signal_id` correlates this request with its terminal outcome event
    /// (`ExternalSignalDelivered` or `ExternalSignalFailed`). On replay the
    /// caller's context returns the recorded outcome without re-issuing the
    /// side effect.
    ExternalSignalRequested {
        /// Correlation ID linking this event to its terminal outcome.
        signal_id: ExternalSignalId,
        /// The execution ID of the workflow that should receive the signal.
        target: ExecutionId,
        /// Name of the signal channel on the receiving workflow.
        signal_name: String,
        /// JSON payload to deliver to the receiving workflow.
        payload: serde_json::Value,
    },
    /// The signal was successfully inserted into the target workflow's signal
    /// queue (or durably queued via the outbox for cross-shard delivery).
    ExternalSignalDelivered {
        /// Correlation ID matching the corresponding `ExternalSignalRequested`.
        signal_id: ExternalSignalId,
    },
    /// The signal could not be delivered. The `reason_code` is one of:
    /// - `"target_terminal"` — the target workflow is already in a terminal state.
    /// - `"target_unknown"` — no execution with the given ID was found after
    ///   the configured grace window.
    ExternalSignalFailed {
        /// Correlation ID matching the corresponding `ExternalSignalRequested`.
        signal_id: ExternalSignalId,
        /// Machine-readable reason code (`"target_terminal"` or `"target_unknown"`).
        reason_code: String,
    },

    // ── Detached child workflow spawn (issue #347) ───────────────────────────
    /// A child workflow was spawned in **detached** mode: the parent does not
    /// suspend awaiting the child's terminal result. The `parent_close_policy`
    /// determines what happens to this child when the parent reaches a terminal
    /// state.
    ///
    /// This is an **append-only** variant — old histories that contain only
    /// `ChildWorkflowStarted` rows will still deserialize correctly because
    /// this variant is new and independent.
    ChildWorkflowSpawnedDetached {
        /// The execution ID of the spawned child.
        child_id: ExecutionId,
        /// The name of the child workflow handler.
        workflow_name: String,
        /// The input passed to the child workflow.
        input: serde_json::Value,
        /// Policy applied to this child when the parent reaches a terminal state.
        parent_close_policy: crate::types::ParentClosePolicy,
    },

    /// The executor applied a parent-close cascade policy to a detached child.
    ///
    /// Recorded once per child immediately after the cascade action is taken so
    /// that replay is deterministic — re-running the history never re-fires the
    /// cascade.
    ///
    /// `action` is one of `"request_cancel"` or `"terminate"` (never `"abandon"`,
    /// which is a no-op).
    ChildWorkflowCascadeApplied {
        /// The execution ID of the child to which cascade was applied.
        child_id: ExecutionId,
        /// The policy that triggered this cascade.
        policy: crate::types::ParentClosePolicy,
        /// Machine-readable action taken: `"request_cancel"` or `"terminate"`.
        action: String,
    },

    // ── Workflow execution timeout (issue #243) ───────────────────────────────
    /// The workflow execution exceeded its configured `execution_timeout` wall-clock
    /// deadline and was forcibly terminated by the timeout scanner.
    ///
    /// This is a **terminal** lifecycle event: once appended, the execution row
    /// transitions to `TIMED_OUT` and no further events are written.
    ///
    /// ## Replay determinism
    ///
    /// The `deadline` field is the absolute UTC instant computed at start time
    /// (`started_at + execution_timeout`). Re-running history always produces the
    /// same `WorkflowExecutionTimedOut` event without consulting the live clock —
    /// the timeout decision is derivable from the stored `deadline` alone.
    WorkflowExecutionTimedOut {
        /// The absolute deadline that was exceeded (`started_at + execution_timeout`).
        deadline: DateTime<Utc>,
        /// The actual wall-clock time when the scanner detected and enforced the timeout.
        timed_out_at: DateTime<Utc>,
    },

    // ── Deterministic side-effect primitives (issue #384) ─────────────────────
    /// A deterministic value was captured during live execution and frozen into
    /// history so subsequent replays return the identical value.
    ///
    /// All of the `WorkflowContext` deterministic primitives — `system_now()`,
    /// `system_time_now()`, `new_uuid()`, `random_u64()`, `random_f64()`,
    /// `random_range()`, and `side_effect()` — lower onto this single variant so
    /// the event-schema cost of the feature is paid exactly once. The `kind`
    /// discriminator (a bounded enum) records which helper produced the value;
    /// `name` is `Some` only for `side_effect()` (the author-supplied dedup key)
    /// and `None` for the built-in unnamed helpers; `value` is the recorded JSON
    /// result returned on every replay.
    ///
    /// This is an **append-only** variant added at the end of the enum. The
    /// `harvest_events` table stores it as opaque JSON, so no migration is
    /// required. `name` is omitted from the serialised form when `None`.
    SideEffectRecorded {
        /// Which built-in helper produced this value.
        kind: SideEffectKind,
        /// Author-supplied dedup key for `side_effect()`; `None` for built-ins.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// The recorded JSON value, replayed verbatim on every subsequent pass.
        value: serde_json::Value,
    },

    // ── Pause / Resume (issue #383) ───────────────────────────────────────────
    /// An operator paused this execution. While paused the executor refuses to
    /// dispatch new commands (activities, timers, child workflows); in-flight
    /// activities continue to completion and their results are recorded
    /// normally. This is a **non-terminal** lifecycle event — the execution
    /// resumes from exactly this point on
    /// [`WorkflowExecutionResumed`](Self::WorkflowExecutionResumed).
    ///
    /// ## Replay determinism
    ///
    /// Pause/resume events are no-ops for command dispatch during replay: the
    /// [`crate::replay::HistoryMatcher`] skips them transparently, so a recorded
    /// pause/resume pair never alters the command sequence reconstructed from
    /// history.
    WorkflowExecutionPaused {
        /// Wall-clock time the pause was applied.
        paused_at: DateTime<Utc>,
        /// Optional operator-supplied reason (max 500 chars, enforced at the API
        /// boundary). `None` when no reason was given.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        /// Identity of the operator (or `"auto-resume(timeout)"` peer) that
        /// requested the pause, captured from the audit trail.
        actor: String,
    },
    /// An operator (or the bounded-pause auto-resume scanner) resumed a paused
    /// execution. The executor re-arms and the workflow advances on its next
    /// decision attempt; timers whose fire time elapsed while paused fire
    /// immediately in their original order, and signals queued during the pause
    /// are delivered in order.
    WorkflowExecutionResumed {
        /// Wall-clock time the resume was applied.
        resumed_at: DateTime<Utc>,
        /// Identity that requested the resume. `"auto-resume(timeout)"` when the
        /// bounded-pause scanner resumed an over-long pause.
        actor: String,
    },
}

impl WorkflowEvent {
    /// Stable string identifier for this event variant, stored in
    /// `harvest_events.event_type`.
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::WorkflowStarted { .. } => "WorkflowStarted",
            Self::WorkflowCompleted { .. } => "WorkflowCompleted",
            Self::WorkflowFailed { .. } => "WorkflowFailed",
            Self::WorkflowCancelled { .. } => "WorkflowCancelled",
            Self::ActivityScheduled { .. } => "ActivityScheduled",
            Self::ActivityStarted { .. } => "ActivityStarted",
            Self::ActivityCompleted { .. } => "ActivityCompleted",
            Self::ActivityFailed { .. } => "ActivityFailed",
            Self::ActivityTimedOut { .. } => "ActivityTimedOut",
            Self::ActivityHeartbeat { .. } => "ActivityHeartbeat",
            Self::TimerStarted { .. } => "TimerStarted",
            Self::TimerFired { .. } => "TimerFired",
            Self::SignalReceived { .. } => "SignalReceived",
            Self::ChildWorkflowStarted { .. } => "ChildWorkflowStarted",
            Self::ChildWorkflowCompleted { .. } => "ChildWorkflowCompleted",
            Self::ChildWorkflowFailed { .. } => "ChildWorkflowFailed",
            Self::MarkerRecorded { .. } => "MarkerRecorded",
            Self::WorkflowContinuedAsNew { .. } => "WorkflowContinuedAsNew",
            Self::LocalActivityScheduled { .. } => "LocalActivityScheduled",
            Self::LocalActivityCompleted { .. } => "LocalActivityCompleted",
            Self::LocalActivityFailed { .. } => "LocalActivityFailed",
            Self::ActivityAwaitingExternal { .. } => "ActivityAwaitingExternal",
            Self::ActivityCompletedExternally { .. } => "ActivityCompletedExternally",
            Self::ActivityFailedExternally { .. } => "ActivityFailedExternally",
            Self::ActivityExternalDeadlineExtended { .. } => "ActivityExternalDeadlineExtended",
            Self::UpdateAdmitted { .. } => "UpdateAdmitted",
            Self::UpdateCompleted { .. } => "UpdateCompleted",
            Self::UpdateFailed { .. } => "UpdateFailed",
            Self::WorkflowResetFork { .. } => "WorkflowResetFork",
            Self::WorkflowResetTerminated { .. } => "WorkflowResetTerminated",
            Self::LocalActivityExhausted { .. } => "LocalActivityExhausted",
            Self::ExternalSignalRequested { .. } => "ExternalSignalRequested",
            Self::ExternalSignalDelivered { .. } => "ExternalSignalDelivered",
            Self::ExternalSignalFailed { .. } => "ExternalSignalFailed",
            Self::WorkflowExecutionTimedOut { .. } => "WorkflowExecutionTimedOut",
            Self::ChildWorkflowSpawnedDetached { .. } => "ChildWorkflowSpawnedDetached",
            Self::ChildWorkflowCascadeApplied { .. } => "ChildWorkflowCascadeApplied",
            Self::SideEffectRecorded { .. } => "SideEffectRecorded",
            Self::WorkflowExecutionPaused { .. } => "WorkflowExecutionPaused",
            Self::WorkflowExecutionResumed { .. } => "WorkflowExecutionResumed",
        }
    }

    /// Returns `true` for terminal lifecycle events that are appended by the
    /// executor after a workflow finishes and are never consumed by workflow
    /// commands during replay.
    #[must_use]
    pub const fn is_terminal_lifecycle(&self) -> bool {
        matches!(
            self,
            Self::WorkflowCompleted { .. }
                | Self::WorkflowFailed { .. }
                | Self::WorkflowCancelled { .. }
                | Self::WorkflowContinuedAsNew { .. }
                | Self::WorkflowResetTerminated { .. }
                | Self::WorkflowExecutionTimedOut { .. }
                // Written to the parent history after the parent's own terminal event;
                // never consumed by the workflow function itself, so must be skipped
                // during unconsumed-event checks to avoid false non-determinism reports.
                | Self::ChildWorkflowCascadeApplied { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ActivityExecId;
    use chrono::Utc;

    // ── ActivityFailed typed-failure tests (issue #227) ──────────────────────

    #[test]
    fn activity_failed_has_error_type_and_non_retryable_fields() {
        let id = ActivityExecId::new();
        let event = WorkflowEvent::ActivityFailed {
            activity_id: id,
            error: "InvalidInput: bad value".into(),
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

    #[test]
    fn activity_failed_old_format_deserializes_with_defaults() {
        // Old events stored without error_type / non_retryable must deserialize
        // cleanly via serde(default).
        let old_json = r#"{"type":"ActivityFailed","data":{"activity_id":"00000000-0000-0000-0000-000000000001","error":"connection refused","attempt":2}}"#;
        let back: WorkflowEvent = serde_json::from_str(old_json).unwrap();
        match back {
            WorkflowEvent::ActivityFailed {
                error,
                attempt,
                error_type,
                non_retryable,
                ..
            } => {
                assert_eq!(error, "connection refused");
                assert_eq!(attempt, 2);
                assert_eq!(error_type, "Error", "default error_type must be 'Error'");
                assert!(!non_retryable, "default non_retryable must be false");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn activity_failed_retryable_round_trips() {
        let id = ActivityExecId::new();
        let event = WorkflowEvent::ActivityFailed {
            activity_id: id,
            error: "Transient: timeout".into(),
            attempt: 1,
            error_type: "Transient".into(),
            non_retryable: false,
            details: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            back,
            WorkflowEvent::ActivityFailed {
                non_retryable: false,
                ..
            }
        ));
    }

    #[test]
    fn workflow_started_round_trips_serde() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"user_id": 42}),
            timestamp: Utc::now(),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(back, WorkflowEvent::WorkflowStarted { .. }));
        Ok(())
    }

    #[test]
    fn activity_scheduled_round_trips() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "send_email".into(),
            input: serde_json::Value::Null,
            queue: "default".into(),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(back, WorkflowEvent::ActivityScheduled { .. }));
        Ok(())
    }

    #[test]
    fn local_activity_scheduled_round_trips() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::LocalActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "format_data".into(),
            input: serde_json::json!({"x": 1}),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(back, WorkflowEvent::LocalActivityScheduled { .. }));
        Ok(())
    }

    #[test]
    fn local_activity_completed_round_trips() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::LocalActivityCompleted {
            activity_id: ActivityExecId::new(),
            output: serde_json::json!({"result": 42}),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(back, WorkflowEvent::LocalActivityCompleted { .. }));
        Ok(())
    }

    #[test]
    fn local_activity_failed_round_trips() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::LocalActivityFailed {
            activity_id: ActivityExecId::new(),
            error: "db connection refused".into(),
            attempt: 3,
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::LocalActivityFailed { attempt: 3, .. }
        ));
        Ok(())
    }

    #[test]
    fn local_activity_exhausted_round_trips() -> Result<(), serde_json::Error> {
        let id = ActivityExecId::new();
        let event = WorkflowEvent::LocalActivityExhausted {
            activity_id: id,
            error: "always fails".into(),
            attempt: 3,
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::LocalActivityExhausted { attempt: 3, .. }
        ));
        assert_eq!(event.type_name(), "LocalActivityExhausted");
        Ok(())
    }

    #[test]
    fn event_type_name_is_stable() {
        let e = WorkflowEvent::WorkflowCompleted {
            output: serde_json::Value::Null,
        };
        assert_eq!(e.type_name(), "WorkflowCompleted");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn all_type_names_are_unique() {
        use crate::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};
        use std::collections::HashSet;

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::Value::Null,
            },
            WorkflowEvent::WorkflowFailed { error: "x".into() },
            WorkflowEvent::WorkflowCancelled { reason: "x".into() },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "a".into(),
                input: serde_json::Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityStarted {
                activity_id: ActivityExecId::new(),
                worker_id: WorkerId::new("w"),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: ActivityExecId::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::ActivityFailed {
                activity_id: ActivityExecId::new(),
                error: "x".into(),
                attempt: 1,
                error_type: "Error".into(),
                non_retryable: false,
                details: None,
            },
            WorkflowEvent::ActivityTimedOut {
                activity_id: ActivityExecId::new(),
                timeout_type: crate::error::TimeoutType::StartToClose,
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: ActivityExecId::new(),
                details: serde_json::Value::Null,
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("t"),
                duration_secs: 10,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("t"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "s".into(),
                payload: serde_json::Value::Null,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: ExecutionId::new(),
                workflow_name: "w".into(),
                input: serde_json::Value::Null,
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: ExecutionId::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id: ExecutionId::new(),
                error: "x".into(),
            },
            WorkflowEvent::MarkerRecorded {
                name: "m".into(),
                details: serde_json::Value::Null,
            },
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id: ExecutionId::new(),
                input: serde_json::Value::Null,
            },
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id: ActivityExecId::new(),
                token: crate::types::ExternalActivityToken::new(),
                name: "x".into(),
                input: serde_json::Value::Null,
                queue: "default".into(),
                schedule_to_close_secs: 0,
            },
            WorkflowEvent::ActivityCompletedExternally {
                activity_id: ActivityExecId::new(),
                token: crate::types::ExternalActivityToken::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::ActivityFailedExternally {
                activity_id: ActivityExecId::new(),
                token: crate::types::ExternalActivityToken::new(),
                error: "x".into(),
                retryable: false,
            },
            WorkflowEvent::ActivityExternalDeadlineExtended {
                activity_id: ActivityExecId::new(),
                token: crate::types::ExternalActivityToken::new(),
            },
            WorkflowEvent::LocalActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "format_data".into(),
                input: serde_json::Value::Null,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: ActivityExecId::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: ActivityExecId::new(),
                error: "transient".into(),
                attempt: 1,
            },
            WorkflowEvent::UpdateAdmitted {
                update_id: crate::types::UpdateId::new(),
                name: "approve".into(),
                input: serde_json::Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::UpdateCompleted {
                update_id: crate::types::UpdateId::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::UpdateFailed {
                update_id: crate::types::UpdateId::new(),
                error: "x".into(),
            },
            WorkflowEvent::WorkflowResetFork {
                reset_from_exec_id: ExecutionId::new(),
                reset_to_event_id: 1,
                reason: "bad deploy".into(),
                operator_id: "ops".into(),
            },
            WorkflowEvent::WorkflowResetTerminated {
                reset_to_exec_id: ExecutionId::new(),
                reason: "bad deploy".into(),
                operator_id: "ops".into(),
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id: crate::types::ExternalSignalId::new(),
                target: ExecutionId::new(),
                signal_name: "cancel".into(),
                payload: serde_json::Value::Null,
            },
            WorkflowEvent::ExternalSignalDelivered {
                signal_id: crate::types::ExternalSignalId::new(),
            },
            WorkflowEvent::ExternalSignalFailed {
                signal_id: crate::types::ExternalSignalId::new(),
                reason_code: "target_terminal".into(),
            },
            WorkflowEvent::WorkflowExecutionTimedOut {
                deadline: Utc::now(),
                timed_out_at: Utc::now(),
            },
            WorkflowEvent::SideEffectRecorded {
                kind: crate::event::SideEffectKind::Now,
                name: None,
                value: serde_json::Value::Null,
            },
            WorkflowEvent::WorkflowExecutionPaused {
                paused_at: Utc::now(),
                reason: Some("incident".into()),
                actor: "oncall".into(),
            },
            WorkflowEvent::WorkflowExecutionResumed {
                resumed_at: Utc::now(),
                actor: "oncall".into(),
            },
        ];

        assert_eq!(events.len(), 37);
        let names: HashSet<_> = events.iter().map(WorkflowEvent::type_name).collect();
        assert_eq!(names.len(), 37, "duplicate type names detected");
    }

    // ── SideEffectRecorded tests (issue #384) ─────────────────────────────────

    #[test]
    fn side_effect_recorded_round_trips_without_name() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Now,
            name: None,
            value: serde_json::json!(1_700_000_000_u64),
        };
        assert_eq!(event.type_name(), "SideEffectRecorded");
        let json = serde_json::to_string(&event)?;
        // `name: None` must be omitted from the serialised form.
        assert!(
            !json.contains("\"name\""),
            "name should be skipped when None"
        );
        assert!(
            json.contains("\"kind\":\"Now\""),
            "kind tag must be present"
        );
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::SideEffectRecorded { kind, name, value } => {
                assert_eq!(kind, SideEffectKind::Now);
                assert_eq!(name, None);
                assert_eq!(value, serde_json::json!(1_700_000_000_u64));
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn side_effect_recorded_round_trips_with_custom_name() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Custom,
            name: Some("env_lookup".into()),
            value: serde_json::json!({"region": "us-east-1"}),
        };
        let json = serde_json::to_string(&event)?;
        assert!(json.contains("\"name\":\"env_lookup\""));
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Custom,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn side_effect_recorded_is_not_terminal_lifecycle() {
        let event = WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Uuid,
            name: None,
            value: serde_json::Value::Null,
        };
        assert!(!event.is_terminal_lifecycle());
    }

    #[test]
    fn side_effect_kind_labels_are_bounded() {
        assert_eq!(SideEffectKind::Now.as_str(), "now");
        assert_eq!(SideEffectKind::Uuid.as_str(), "uuid");
        assert_eq!(SideEffectKind::Random.as_str(), "random");
        assert_eq!(SideEffectKind::Custom.as_str(), "custom");
    }

    #[test]
    fn workflow_reset_events_round_trip_and_type_names_are_stable() -> Result<(), serde_json::Error>
    {
        let source = ExecutionId::new();
        let fork = ExecutionId::new();
        let fork_event = WorkflowEvent::WorkflowResetFork {
            reset_from_exec_id: source,
            reset_to_event_id: 42,
            reason: "rolled back bad signal".into(),
            operator_id: "oncall".into(),
        };
        let terminated_event = WorkflowEvent::WorkflowResetTerminated {
            reset_to_exec_id: fork,
            reason: "rolled back bad signal".into(),
            operator_id: "oncall".into(),
        };

        assert_eq!(fork_event.type_name(), "WorkflowResetFork");
        assert_eq!(terminated_event.type_name(), "WorkflowResetTerminated");

        let json = serde_json::to_string(&fork_event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::WorkflowResetFork {
                reset_from_exec_id,
                reset_to_event_id: 42,
                ..
            } if reset_from_exec_id == source
        ));

        Ok(())
    }

    // ── ExternalSignal event tests (issue #330) ───────────────────────────

    #[test]
    fn external_signal_requested_round_trips() -> Result<(), serde_json::Error> {
        let signal_id = crate::types::ExternalSignalId::new();
        let target = ExecutionId::new();
        let event = WorkflowEvent::ExternalSignalRequested {
            signal_id,
            target,
            signal_name: "tenant_cancel".into(),
            payload: serde_json::json!({"reason": "billing_lapse"}),
        };
        assert_eq!(event.type_name(), "ExternalSignalRequested");
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::ExternalSignalRequested {
                signal_id: sid,
                target: t,
                signal_name,
                payload,
            } => {
                assert_eq!(sid, signal_id);
                assert_eq!(t, target);
                assert_eq!(signal_name, "tenant_cancel");
                assert_eq!(payload["reason"], "billing_lapse");
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_signal_delivered_round_trips() -> Result<(), serde_json::Error> {
        let signal_id = crate::types::ExternalSignalId::new();
        let event = WorkflowEvent::ExternalSignalDelivered { signal_id };
        assert_eq!(event.type_name(), "ExternalSignalDelivered");
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::ExternalSignalDelivered { signal_id: sid } if sid == signal_id
        ));
        Ok(())
    }

    #[test]
    fn external_signal_failed_round_trips() -> Result<(), serde_json::Error> {
        let signal_id = crate::types::ExternalSignalId::new();
        let event = WorkflowEvent::ExternalSignalFailed {
            signal_id,
            reason_code: "target_terminal".into(),
        };
        assert_eq!(event.type_name(), "ExternalSignalFailed");
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::ExternalSignalFailed {
                signal_id: sid,
                reason_code,
            } => {
                assert_eq!(sid, signal_id);
                assert_eq!(reason_code, "target_terminal");
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_signal_failed_unknown_target_reason_code() -> Result<(), serde_json::Error> {
        let signal_id = crate::types::ExternalSignalId::new();
        let event = WorkflowEvent::ExternalSignalFailed {
            signal_id,
            reason_code: "target_unknown".into(),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::ExternalSignalFailed { reason_code, .. } => {
                assert_eq!(reason_code, "target_unknown");
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn external_signal_events_are_not_terminal_lifecycle() {
        let signal_id = crate::types::ExternalSignalId::new();
        let target = ExecutionId::new();
        assert!(
            !WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "x".into(),
                payload: serde_json::Value::Null,
            }
            .is_terminal_lifecycle()
        );
        assert!(!WorkflowEvent::ExternalSignalDelivered { signal_id }.is_terminal_lifecycle());
        assert!(
            !WorkflowEvent::ExternalSignalFailed {
                signal_id,
                reason_code: "target_terminal".into(),
            }
            .is_terminal_lifecycle()
        );
    }

    // ── WorkflowExecutionTimedOut tests (issue #243) ──────────────────────────

    #[test]
    fn workflow_execution_timed_out_round_trips() -> Result<(), serde_json::Error> {
        let deadline = Utc::now();
        let timed_out_at = Utc::now();
        let event = WorkflowEvent::WorkflowExecutionTimedOut {
            deadline,
            timed_out_at,
        };

        assert_eq!(event.type_name(), "WorkflowExecutionTimedOut");
        assert!(event.is_terminal_lifecycle(), "timed-out must be terminal");

        let json = serde_json::to_string(&event)?;
        assert!(
            json.contains("WorkflowExecutionTimedOut"),
            "type tag must appear in JSON"
        );
        assert!(
            json.contains("deadline"),
            "deadline field must be serialised"
        );
        assert!(
            json.contains("timed_out_at"),
            "timed_out_at field must be serialised"
        );

        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(
            matches!(back, WorkflowEvent::WorkflowExecutionTimedOut { .. }),
            "must deserialise to the correct variant"
        );
        Ok(())
    }

    #[test]
    fn workflow_execution_timed_out_type_name_is_stable() {
        let e = WorkflowEvent::WorkflowExecutionTimedOut {
            deadline: Utc::now(),
            timed_out_at: Utc::now(),
        };
        assert_eq!(e.type_name(), "WorkflowExecutionTimedOut");
    }

    // ── Pause/Resume tests (issue #383) ───────────────────────────────────────

    #[test]
    fn workflow_execution_paused_round_trips() -> Result<(), serde_json::Error> {
        let paused_at = Utc::now();
        let event = WorkflowEvent::WorkflowExecutionPaused {
            paused_at,
            reason: Some("investigating runaway dispatch".into()),
            actor: "oncall@example.com".into(),
        };

        assert_eq!(event.type_name(), "WorkflowExecutionPaused");
        // Pause is NOT terminal — a paused workflow resumes and keeps running.
        assert!(
            !event.is_terminal_lifecycle(),
            "pause must not be a terminal lifecycle event"
        );

        let json = serde_json::to_string(&event)?;
        assert!(json.contains("WorkflowExecutionPaused"));
        assert!(json.contains("paused_at"));
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::WorkflowExecutionPaused { reason, actor, .. } => {
                assert_eq!(reason.as_deref(), Some("investigating runaway dispatch"));
                assert_eq!(actor, "oncall@example.com");
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }

    #[test]
    fn workflow_execution_paused_round_trips_without_reason() -> Result<(), serde_json::Error> {
        let event = WorkflowEvent::WorkflowExecutionPaused {
            paused_at: Utc::now(),
            reason: None,
            actor: "auto".into(),
        };
        let json = serde_json::to_string(&event)?;
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            WorkflowEvent::WorkflowExecutionPaused { reason: None, .. }
        ));
        Ok(())
    }

    #[test]
    fn workflow_execution_resumed_round_trips() -> Result<(), serde_json::Error> {
        let resumed_at = Utc::now();
        let event = WorkflowEvent::WorkflowExecutionResumed {
            resumed_at,
            actor: "auto-resume(timeout)".into(),
        };

        assert_eq!(event.type_name(), "WorkflowExecutionResumed");
        assert!(
            !event.is_terminal_lifecycle(),
            "resume must not be a terminal lifecycle event"
        );

        let json = serde_json::to_string(&event)?;
        assert!(json.contains("WorkflowExecutionResumed"));
        assert!(json.contains("resumed_at"));
        let back: WorkflowEvent = serde_json::from_str(&json)?;
        match back {
            WorkflowEvent::WorkflowExecutionResumed { actor, .. } => {
                assert_eq!(actor, "auto-resume(timeout)");
            }
            _ => panic!("wrong variant"),
        }
        Ok(())
    }
}
