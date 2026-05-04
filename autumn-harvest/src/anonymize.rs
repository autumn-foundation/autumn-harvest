//! Event history anonymizer.
//!
//! Provides utilities to redact sensitive data from workflow event histories,
//! making them safe to export, share, or use in local replay tests.

use crate::event::WorkflowEvent;
use serde_json::Value;

/// Redacts all sensitive JSON payloads and string messages from a workflow history.
///
/// This function iterates through a slice of workflow events and replaces any
/// `input`, `output`, `details`, `error`, or `reason` fields with safe, static
/// values (e.g., `Value::String("[REDACTED]".to_string())` or `"[REDACTED]"`).
/// Structural identifiers like IDs, names, and timestamps are preserved to
/// ensure the history remains structurally valid for analysis or replay.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::event::WorkflowEvent;
/// use autumn_harvest::anonymize::anonymize_history;
/// use chrono::Utc;
///
/// let history = vec![
///     WorkflowEvent::WorkflowStarted {
///         input: serde_json::json!({"user": "john_doe", "ssn": "123"}),
///         timestamp: Utc::now(),
///     }
/// ];
///
/// let anonymized = anonymize_history(&history);
///
/// if let WorkflowEvent::WorkflowStarted { input, .. } = &anonymized[0] {
///     assert_eq!(input.as_str(), Some("[REDACTED]"));
/// }
/// ```
#[must_use]
pub fn anonymize_history(events: &[WorkflowEvent]) -> Vec<WorkflowEvent> {
    events.iter().map(anonymize_event).collect()
}

fn redacted_value() -> Value {
    Value::String("[REDACTED]".to_string())
}

fn redacted_string() -> String {
    "[REDACTED]".to_string()
}

fn anonymize_event(event: &WorkflowEvent) -> WorkflowEvent {
    match event {
        // Lifecycle
        WorkflowEvent::WorkflowStarted { input: _, timestamp } => WorkflowEvent::WorkflowStarted {
            input: redacted_value(),
            timestamp: *timestamp,
        },
        WorkflowEvent::WorkflowCompleted { output: _ } => WorkflowEvent::WorkflowCompleted {
            output: redacted_value(),
        },
        WorkflowEvent::WorkflowFailed { error: _ } => WorkflowEvent::WorkflowFailed {
            error: redacted_string(),
        },
        WorkflowEvent::WorkflowCancelled { reason: _ } => WorkflowEvent::WorkflowCancelled {
            reason: redacted_string(),
        },

        // Activities
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name,
            input: _,
            queue,
        } => WorkflowEvent::ActivityScheduled {
            activity_id: activity_id.clone(),
            name: name.clone(),
            input: redacted_value(),
            queue: queue.clone(),
        },
        WorkflowEvent::ActivityStarted {
            activity_id,
            worker_id,
        } => WorkflowEvent::ActivityStarted {
            activity_id: activity_id.clone(),
            worker_id: worker_id.clone(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: _,
        } => WorkflowEvent::ActivityCompleted {
            activity_id: activity_id.clone(),
            output: redacted_value(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id,
            error: _,
            attempt,
        } => WorkflowEvent::ActivityFailed {
            activity_id: activity_id.clone(),
            error: redacted_string(),
            attempt: *attempt,
        },
        WorkflowEvent::ActivityTimedOut {
            activity_id,
            timeout_type,
        } => WorkflowEvent::ActivityTimedOut {
            activity_id: activity_id.clone(),
            timeout_type: timeout_type.clone(),
        },
        WorkflowEvent::ActivityHeartbeat {
            activity_id,
            details: _,
        } => WorkflowEvent::ActivityHeartbeat {
            activity_id: activity_id.clone(),
            details: redacted_value(),
        },

        // Timers
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs,
        } => WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: *duration_secs,
        },
        WorkflowEvent::TimerFired { timer_id } => WorkflowEvent::TimerFired {
            timer_id: timer_id.clone(),
        },

        // Signals
        WorkflowEvent::SignalReceived {
            signal_name,
            payload: _,
        } => WorkflowEvent::SignalReceived {
            signal_name: signal_name.clone(),
            payload: redacted_value(),
        },

        // Child Workflows
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name,
            input: _,
        } => WorkflowEvent::ChildWorkflowStarted {
            child_id: child_id.clone(),
            workflow_name: workflow_name.clone(),
            input: redacted_value(),
        },
        WorkflowEvent::ChildWorkflowCompleted { child_id, output: _ } => {
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_id.clone(),
                output: redacted_value(),
            }
        }
        WorkflowEvent::ChildWorkflowFailed { child_id, error: _ } => {
            WorkflowEvent::ChildWorkflowFailed {
                child_id: child_id.clone(),
                error: redacted_string(),
            }
        }

        // Misc
        WorkflowEvent::MarkerRecorded { name, details: _ } => WorkflowEvent::MarkerRecorded {
            name: name.clone(),
            details: redacted_value(),
        },
        WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id,
            input: _,
        } => WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: new_exec_id.clone(),
            input: redacted_value(),
        },

        // Local Activities
        WorkflowEvent::LocalActivityScheduled {
            activity_id,
            name,
            input: _,
        } => WorkflowEvent::LocalActivityScheduled {
            activity_id: activity_id.clone(),
            name: name.clone(),
            input: redacted_value(),
        },
        WorkflowEvent::LocalActivityCompleted {
            activity_id,
            output: _,
        } => WorkflowEvent::LocalActivityCompleted {
            activity_id: activity_id.clone(),
            output: redacted_value(),
        },
        WorkflowEvent::LocalActivityFailed {
            activity_id,
            error: _,
            attempt,
        } => WorkflowEvent::LocalActivityFailed {
            activity_id: activity_id.clone(),
            error: redacted_string(),
            attempt: *attempt,
        },

        // External Activities
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            name,
            token,
            input: _,
            queue,
            schedule_to_close_secs,
        } => WorkflowEvent::ActivityAwaitingExternal {
            activity_id: activity_id.clone(),
            name: name.clone(),
            token: token.clone(),
            input: redacted_value(),
            queue: queue.clone(),
            schedule_to_close_secs: *schedule_to_close_secs,
        },
        WorkflowEvent::ActivityCompletedExternally { activity_id, token, output: _ } => {
            WorkflowEvent::ActivityCompletedExternally {
                activity_id: activity_id.clone(),
                token: token.clone(),
                output: redacted_value(),
            }
        }
        WorkflowEvent::ActivityFailedExternally { activity_id, token, error: _, retryable } => {
            WorkflowEvent::ActivityFailedExternally {
                activity_id: activity_id.clone(),
                token: token.clone(),
                error: redacted_string(),
                retryable: *retryable,
            }
        }
        WorkflowEvent::ActivityExternalDeadlineExtended {
            activity_id,
            token,
        } => WorkflowEvent::ActivityExternalDeadlineExtended {
            activity_id: activity_id.clone(),
            token: token.clone(),
        },

        // Updates
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name,
            input: _,
            timestamp,
        } => WorkflowEvent::UpdateAdmitted {
            update_id: update_id.clone(),
            name: name.clone(),
            input: redacted_value(),
            timestamp: *timestamp,
        },
        WorkflowEvent::UpdateCompleted {
            update_id,
            output: _,
        } => WorkflowEvent::UpdateCompleted {
            update_id: update_id.clone(),
            output: redacted_value(),
        },
        WorkflowEvent::UpdateFailed {
            update_id,
            error: _,
        } => WorkflowEvent::UpdateFailed {
            update_id: update_id.clone(),
            error: redacted_string(),
        },

        // Reset
        WorkflowEvent::WorkflowResetFork {
            reset_from_exec_id,
            reset_to_event_id,
            reason: _,
            operator_id,
        } => WorkflowEvent::WorkflowResetFork {
            reset_from_exec_id: reset_from_exec_id.clone(),
            reset_to_event_id: *reset_to_event_id,
            reason: redacted_string(),
            operator_id: operator_id.clone(),
        },
        WorkflowEvent::WorkflowResetTerminated {
            reset_to_exec_id,
            reason: _,
            operator_id,
        } => WorkflowEvent::WorkflowResetTerminated {
            reset_to_exec_id: reset_to_exec_id.clone(),
            reason: redacted_string(),
            operator_id: operator_id.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ActivityExecId;
    use chrono::Utc;

    #[test]
    fn should_redact_workflow_started_input() {
        let events = vec![WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"secret": "password123"}),
            timestamp: Utc::now(),
        }];

        let result = anonymize_history(&events);

        match &result[0] {
            WorkflowEvent::WorkflowStarted { input, .. } => {
                assert_eq!(input.as_str(), Some("[REDACTED]"));
            }
            _ => panic!("Expected WorkflowStarted"),
        }
    }

    #[test]
    fn should_redact_activity_error() {
        let id = ActivityExecId::new();
        let events = vec![WorkflowEvent::ActivityFailed {
            activity_id: id.clone(),
            error: "Connection refused to db at 192.168.1.5".to_string(),
            attempt: 1,
        }];

        let result = anonymize_history(&events);

        match &result[0] {
            WorkflowEvent::ActivityFailed { activity_id, error, attempt } => {
                assert_eq!(activity_id, &id);
                assert_eq!(error, "[REDACTED]");
                assert_eq!(*attempt, 1);
            }
            _ => panic!("Expected ActivityFailed"),
        }
    }

    #[test]
    fn should_preserve_timer_duration() {
        let events = vec![WorkflowEvent::TimerStarted {
            timer_id: crate::types::TimerId::new("t1"),
            duration_secs: 60,
        }];

        let result = anonymize_history(&events);

        match &result[0] {
            WorkflowEvent::TimerStarted { timer_id, duration_secs } => {
                assert_eq!(timer_id.as_str(), "t1");
                assert_eq!(*duration_secs, 60);
            }
            _ => panic!("Expected TimerStarted"),
        }
    }
}
