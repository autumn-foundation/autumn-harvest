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
use crate::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};

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
        }
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
    fn event_type_name_is_stable() {
        let e = WorkflowEvent::WorkflowCompleted {
            output: serde_json::Value::Null,
        };
        assert_eq!(e.type_name(), "WorkflowCompleted");
    }

    #[test]
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
        ];

        assert_eq!(events.len(), 17);
        let names: HashSet<_> = events.iter().map(WorkflowEvent::type_name).collect();
        assert_eq!(names.len(), 17, "duplicate type names detected");
    }
}
