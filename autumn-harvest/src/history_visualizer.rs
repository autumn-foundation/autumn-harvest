//! Visualization exporters for workflow event history.
//!
//! Provides utilities to export workflow history (a series of `WorkflowEvent`s) into
//! a human-readable Mermaid.js sequence diagram (`sequenceDiagram`). This is useful
//! for understanding how activities, child workflows, signals, and timers interact
//! over time within a single workflow execution.

use crate::event::WorkflowEvent;
use std::fmt::Write;

/// Exports a slice of workflow events into a Mermaid.js sequence diagram.
///
/// Converts the history into a series of messages between "Workflow", "Activities",
/// "`ChildWorkflows`", "Timers", and "Signals" actors.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::event::WorkflowEvent;
/// use autumn_harvest::history_visualizer::export_mermaid_sequence;
/// use chrono::Utc;
/// use autumn_harvest::types::ActivityExecId;
///
/// let events = vec![
///     WorkflowEvent::WorkflowStarted {
///         input: serde_json::Value::Null,
///         timestamp: Utc::now(),
///         last_completion_result: None,
///         last_error: None,
///         scheduled_time: None,
///     },
///     WorkflowEvent::ActivityScheduled {
///         activity_id: ActivityExecId::new(),
///         name: "charge_card".to_string(),
///         input: serde_json::Value::Null,
///         queue: "default".to_string(),
///     },
/// ];
///
/// let mermaid = export_mermaid_sequence(&events).unwrap();
/// assert!(mermaid.contains("sequenceDiagram"));
/// assert!(mermaid.contains("Workflow->>Activity"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_mermaid_sequence(events: &[WorkflowEvent]) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "sequenceDiagram")?;
    writeln!(out, "    participant W as Workflow")?;
    writeln!(out, "    participant A as Activity")?;
    writeln!(out, "    participant T as Timer")?;
    writeln!(out, "    participant C as ChildWorkflow")?;
    writeln!(out, "    participant S as Signal")?;

    for event in events {
        match event {
            WorkflowEvent::WorkflowStarted { .. } => {
                writeln!(out, "    Note over W: Workflow Started")?;
            }
            WorkflowEvent::WorkflowCompleted { .. } => {
                writeln!(out, "    Note over W: Workflow Completed")?;
            }
            WorkflowEvent::WorkflowFailed { error } => {
                writeln!(out, "    Note over W: Workflow Failed: {}", error.replace('\n', "<br/>").replace('\"', "\\\""))?;
            }
            WorkflowEvent::WorkflowCancelled { reason } => {
                writeln!(out, "    Note over W: Workflow Cancelled: {}", reason.replace('\n', "<br/>").replace('\"', "\\\""))?;
            }
            WorkflowEvent::ActivityScheduled { name, .. } => {
                writeln!(out, "    W->>A: Schedule Activity ({name})")?;
            }
            WorkflowEvent::ActivityStarted { .. } => {
                // Optionally show when it starts, usually omitted for brevity or shown as note
                writeln!(out, "    Note over A: Activity Started")?;
            }
            WorkflowEvent::ActivityCompleted { .. } => {
                writeln!(out, "    A-->>W: Activity Completed")?;
            }
            WorkflowEvent::ActivityFailed { .. } => {
                writeln!(out, "    A-->>W: Activity Failed")?;
            }
            WorkflowEvent::ActivityTimedOut { .. } => {
                writeln!(out, "    A-->>W: Activity Timed Out")?;
            }
            WorkflowEvent::TimerStarted { duration_secs, .. } => {
                writeln!(out, "    W->>T: Start Timer ({duration_secs}s)")?;
            }
            WorkflowEvent::TimerFired { .. } => {
                writeln!(out, "    T-->>W: Timer Fired")?;
            }
            WorkflowEvent::SignalReceived { signal_name, .. } => {
                writeln!(out, "    S-->>W: Signal Received ({signal_name})")?;
            }
            WorkflowEvent::ChildWorkflowStarted { workflow_name, .. } => {
                writeln!(out, "    W->>C: Start Child Workflow ({workflow_name})")?;
            }
            WorkflowEvent::ChildWorkflowCompleted { .. } => {
                writeln!(out, "    C-->>W: Child Workflow Completed")?;
            }
            WorkflowEvent::ChildWorkflowFailed { .. } => {
                writeln!(out, "    C-->>W: Child Workflow Failed")?;
            }
            _ => {
                // Ignore other events for diagram simplicity
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ActivityExecId, ExecutionId, TimerId};
    use chrono::Utc;

    #[test]
    fn test_export_mermaid_sequence() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "charge_card".to_string(),
                input: serde_json::Value::Null,
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: ActivityExecId::new(),
                output: serde_json::Value::Null,
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("t1"),
                duration_secs: 10,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("t1"),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: ExecutionId::new(),
                workflow_name: "send_email".to_string(),
                input: serde_json::Value::Null,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel_order".to_string(),
                payload: serde_json::Value::Null,
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::Value::Null,
            },
        ];

        let mermaid = export_mermaid_sequence(&events).unwrap();

        assert!(mermaid.contains("sequenceDiagram"));
        assert!(mermaid.contains("Note over W: Workflow Started"));
        assert!(mermaid.contains("W->>A: Schedule Activity (charge_card)"));
        assert!(mermaid.contains("A-->>W: Activity Completed"));
        assert!(mermaid.contains("W->>T: Start Timer (10s)"));
        assert!(mermaid.contains("T-->>W: Timer Fired"));
        assert!(mermaid.contains("W->>C: Start Child Workflow (send_email)"));
        assert!(mermaid.contains("S-->>W: Signal Received (cancel_order)"));
        assert!(mermaid.contains("Note over W: Workflow Completed"));
    }

    #[test]
    fn test_export_empty_sequence() {
        let events = vec![];
        let mermaid = export_mermaid_sequence(&events).unwrap();
        assert!(mermaid.contains("sequenceDiagram"));
        assert!(mermaid.contains("participant W as Workflow"));
        assert!(!mermaid.contains("Note over W"));
    }
}
