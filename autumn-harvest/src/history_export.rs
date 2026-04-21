//! Export workflow event histories to Mermaid sequence diagrams.

use crate::event::WorkflowEvent;
use std::fmt::Write;

/// Export a slice of workflow events into a Mermaid.js sequence diagram.
///
/// This provides an intuitive visual representation of the workflow's execution
/// lifecycle, making it easier to debug timing issues, parallel activities, and
/// system interactions.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[allow(clippy::too_many_lines)]
pub fn export_mermaid_sequence(events: &[WorkflowEvent]) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "sequenceDiagram")?;
    writeln!(out, "    autonumber")?;
    writeln!(out, "    participant WF as Workflow")?;

    // We'll keep track of dynamic participants to avoid re-declaring them.
    let mut participants = std::collections::HashSet::new();
    participants.insert("WF".to_string());

    for event in events {
        match event {
            WorkflowEvent::WorkflowStarted { .. } => {
                writeln!(out, "    Note over WF: Workflow Started")?;
            }
            WorkflowEvent::WorkflowCompleted { .. } => {
                writeln!(out, "    Note over WF: Workflow Completed")?;
            }
            WorkflowEvent::WorkflowFailed { error } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(out, "    Note over WF: Workflow Failed: {safe_error}")?;
            }
            WorkflowEvent::WorkflowCancelled { reason } => {
                let safe_reason = reason.replace('\n', " ").replace('"', "'");
                writeln!(out, "    Note over WF: Workflow Cancelled: {safe_reason}")?;
            }
            WorkflowEvent::ActivityScheduled {
                name, activity_id, ..
            } => {
                let participant = format!("Activity_{name}");
                if participants.insert(participant.clone()) {
                    writeln!(out, "    participant {participant} as Activity: {name}")?;
                }
                writeln!(out, "    WF->>+{participant}: Schedule (ID: {activity_id})")?;
            }
            WorkflowEvent::ActivityStarted {
                worker_id,
                activity_id,
                ..
            } => {
                // Without mapping activity_id to name, we use a generic Worker.
                // In a perfect world, we'd track activity_id -> name, but let's keep it simple.
                writeln!(
                    out,
                    "    Note right of WF: Activity Started (ID: {activity_id}) on {worker_id}"
                )
                ?;
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. } => {
                // Note: since we lack the activity name here, we'll draw it back to WF generally
                // or just use a note. To do an arrow, we would need to map activity_id -> participant.
                writeln!(
                    out,
                    "    Note right of WF: Activity Completed (ID: {activity_id})"
                )
                ?;
            }
            WorkflowEvent::ActivityFailed {
                activity_id,
                error,
                attempt,
            } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(
                    out,
                    "    Note right of WF: Activity Failed (ID: {activity_id}, Attempt: {attempt}): {safe_error}"
                )
                ?;
            }
            WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type,
            } => {
                writeln!(
                    out,
                    "    Note right of WF: Activity Timed Out (ID: {activity_id}, Type: {timeout_type:?})"
                )
                ?;
            }
            WorkflowEvent::ActivityHeartbeat { activity_id, .. } => {
                writeln!(
                    out,
                    "    Note right of WF: Activity Heartbeat (ID: {activity_id})"
                )
                ?;
            }
            WorkflowEvent::TimerStarted {
                timer_id,
                duration_secs,
            } => {
                let participant = "Timer";
                if participants.insert(participant.to_string()) {
                    writeln!(out, "    participant {participant} as Timer")?;
                }
                writeln!(
                    out,
                    "    WF->>+{participant}: Start Timer {timer_id} ({duration_secs}s)"
                )
                ?;
            }
            WorkflowEvent::TimerFired { timer_id } => {
                let participant = "Timer";
                writeln!(out, "    {participant}-->>-WF: Timer {timer_id} Fired")?;
            }
            WorkflowEvent::SignalReceived { signal_name, .. } => {
                let participant = "External";
                if participants.insert(participant.to_string()) {
                    writeln!(out, "    participant {participant} as External")?;
                }
                writeln!(
                    out,
                    "    {participant}->>WF: Signal Received: {signal_name}"
                )
                ?;
            }
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name,
                ..
            } => {
                let participant = format!("Child_{workflow_name}");
                if participants.insert(participant.clone()) {
                    writeln!(
                        out,
                        "    participant {participant} as Child: {workflow_name}"
                    )
                    ?;
                }
                writeln!(out, "    WF->>+{participant}: Start Child (ID: {child_id})")?;
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. } => {
                writeln!(
                    out,
                    "    Note right of WF: Child Workflow Completed (ID: {child_id})"
                )
                ?;
            }
            WorkflowEvent::ChildWorkflowFailed { child_id, error } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(
                    out,
                    "    Note right of WF: Child Workflow Failed (ID: {child_id}): {safe_error}"
                )
                ?;
            }
            WorkflowEvent::MarkerRecorded { name, .. } => {
                writeln!(out, "    Note over WF: Marker: {name}")?;
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ActivityExecId;
    use chrono::Utc;

    #[test]
    fn test_export_mermaid_sequence_empty() {
        let events = vec![];
        let diagram = export_mermaid_sequence(&events).expect("export should succeed");
        assert!(diagram.contains("sequenceDiagram"));
        assert!(diagram.contains("participant WF as Workflow"));
    }

    #[test]
    fn test_export_mermaid_sequence_basic_workflow() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "download_file".to_string(),
                input: serde_json::json!({}),
                queue: "default".to_string(),
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::json!({}),
            },
        ];

        let diagram = export_mermaid_sequence(&events).unwrap();
        assert!(diagram.contains("Note over WF: Workflow Started"));
        assert!(diagram.contains("participant Activity_download_file as Activity: download_file"));
        assert!(diagram.contains("WF->>+Activity_download_file: Schedule (ID: "));
        assert!(diagram.contains("Note over WF: Workflow Completed"));
    }
}
