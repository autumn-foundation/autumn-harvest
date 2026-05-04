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
    ActivityExecId, ExecutionId, ExternalActivityToken, TimerId, UpdateId, WorkerId,
};

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
    ActivityFailed {
        /// Unique ID for this specific activity attempt.
        activity_id: ActivityExecId,
        /// String representation of the failure.
        error: String,
        /// How many times the activity has failed so far.
        attempt: u32,
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
    /// succeeded) or before the retry budget is exhausted (the last
    /// `LocalActivityFailed` with no following `LocalActivityCompleted`).
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
                | Self::WorkflowResetTerminated { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ActivityExecId;
    use chrono::Utc;

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
        ];

        assert_eq!(events.len(), 30);
        let names: HashSet<_> = events.iter().map(WorkflowEvent::type_name).collect();
        assert_eq!(names.len(), 30, "duplicate type names detected");
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
}
