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
    WorkflowStarted {
        input: serde_json::Value,
        timestamp: DateTime<Utc>,
    },
    WorkflowCompleted {
        output: serde_json::Value,
    },
    WorkflowFailed {
        error: String,
    },
    WorkflowCancelled {
        reason: String,
    },

    // ── Activities ────────────────────────────────────────────────
    ActivityScheduled {
        activity_id: ActivityExecId,
        name: String,
        input: serde_json::Value,
        queue: String,
    },
    ActivityStarted {
        activity_id: ActivityExecId,
        worker_id: WorkerId,
    },
    ActivityCompleted {
        activity_id: ActivityExecId,
        output: serde_json::Value,
    },
    ActivityFailed {
        activity_id: ActivityExecId,
        error: String,
        attempt: u32,
    },
    ActivityTimedOut {
        activity_id: ActivityExecId,
        timeout_type: TimeoutType,
    },
    ActivityHeartbeat {
        activity_id: ActivityExecId,
        details: serde_json::Value,
    },

    // ── Timers ────────────────────────────────────────────────────
    TimerStarted {
        timer_id: TimerId,
        /// Duration in seconds (Duration is not JSON-serializable natively).
        duration_secs: u64,
    },
    TimerFired {
        timer_id: TimerId,
    },

    // ── Signals ───────────────────────────────────────────────────
    SignalReceived {
        signal_name: String,
        payload: serde_json::Value,
    },

    // ── Child workflows ───────────────────────────────────────────
    ChildWorkflowStarted {
        child_id: ExecutionId,
        workflow_name: String,
        input: serde_json::Value,
    },
    ChildWorkflowCompleted {
        child_id: ExecutionId,
        output: serde_json::Value,
    },
    ChildWorkflowFailed {
        child_id: ExecutionId,
        error: String,
    },

    // ── Markers ───────────────────────────────────────────────────
    MarkerRecorded {
        name: String,
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
    fn workflow_started_round_trips_serde() {
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"user_id": 42}),
            timestamp: Utc::now(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, WorkflowEvent::WorkflowStarted { .. }));
    }

    #[test]
    fn activity_scheduled_round_trips() {
        let event = WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "send_email".into(),
            input: serde_json::Value::Null,
            queue: "default".into(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, WorkflowEvent::ActivityScheduled { .. }));
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
