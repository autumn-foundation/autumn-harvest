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
pub fn export_mermaid_sequence(events: &[WorkflowEvent]) -> Result<String, std::fmt::Error> {
    let mut exporter = MermaidExporter::new();
    exporter.export(events)?;
    Ok(exporter.out)
}

struct MermaidExporter {
    out: String,
    participants: std::collections::HashSet<String>,
}

impl MermaidExporter {
    fn new() -> Self {
        Self {
            out: String::new(),
            participants: std::collections::HashSet::new(),
        }
    }

    fn export(&mut self, events: &[WorkflowEvent]) -> Result<(), std::fmt::Error> {
        writeln!(self.out, "sequenceDiagram")?;
        writeln!(self.out, "    autonumber")?;
        writeln!(self.out, "    participant WF as Workflow")?;

        // We'll keep track of dynamic participants to avoid re-declaring them.
        self.participants.insert("WF".to_string());

        for event in events {
            match event {
                WorkflowEvent::WorkflowStarted { .. }
                | WorkflowEvent::WorkflowCompleted { .. }
                | WorkflowEvent::WorkflowFailed { .. }
                | WorkflowEvent::WorkflowCancelled { .. }
                | WorkflowEvent::WorkflowContinuedAsNew { .. } => {
                    self.handle_workflow_event(event)?;
                }
                WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::ActivityStarted { .. }
                | WorkflowEvent::ActivityCompleted { .. }
                | WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. }
                | WorkflowEvent::ActivityHeartbeat { .. } => {
                    self.handle_activity_event(event)?;
                }
                WorkflowEvent::TimerStarted { .. } | WorkflowEvent::TimerFired { .. } => {
                    self.handle_timer_event(event)?;
                }
                WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. } => {
                    self.handle_child_workflow_event(event)?;
                }
                WorkflowEvent::SignalReceived { .. } | WorkflowEvent::MarkerRecorded { .. } => {
                    self.handle_misc_event(event)?;
                }
            }
        }
        Ok(())
    }

    fn handle_workflow_event(&mut self, event: &WorkflowEvent) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::WorkflowStarted { .. } => {
                writeln!(self.out, "    Note over WF: Workflow Started")?;
            }
            WorkflowEvent::WorkflowCompleted { .. } => {
                writeln!(self.out, "    Note over WF: Workflow Completed")?;
            }
            WorkflowEvent::WorkflowFailed { error } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(self.out, "    Note over WF: Workflow Failed: {safe_error}")?;
            }
            WorkflowEvent::WorkflowCancelled { reason } => {
                let safe_reason = reason.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note over WF: Workflow Cancelled: {safe_reason}"
                )?;
            }
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => {
                writeln!(
                    self.out,
                    "    Note over WF: Continued As New (next: {new_exec_id})"
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_activity_event(&mut self, event: &WorkflowEvent) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::ActivityScheduled {
                name, activity_id, ..
            } => {
                let participant = format!("Activity_{name}");
                if self.participants.insert(participant.clone()) {
                    writeln!(
                        self.out,
                        "    participant {participant} as Activity: {name}"
                    )?;
                }
                writeln!(
                    self.out,
                    "    WF->>+{participant}: Schedule (ID: {activity_id})"
                )?;
            }
            WorkflowEvent::ActivityStarted {
                worker_id,
                activity_id,
                ..
            } => {
                // Without mapping activity_id to name, we use a generic Worker.
                // In a perfect world, we'd track activity_id -> name, but let's keep it simple.
                writeln!(
                    self.out,
                    "    Note right of WF: Activity Started (ID: {activity_id}) on {worker_id}"
                )?;
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. } => {
                // Note: since we lack the activity name here, we'll draw it back to WF generally
                // or just use a note. To do an arrow, we would need to map activity_id -> participant.
                writeln!(
                    self.out,
                    "    Note right of WF: Activity Completed (ID: {activity_id})"
                )?;
            }
            WorkflowEvent::ActivityFailed {
                activity_id,
                error,
                attempt,
            } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note right of WF: Activity Failed (ID: {activity_id}, Attempt: {attempt}): {safe_error}"
                )?;
            }
            WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type,
            } => {
                writeln!(
                    self.out,
                    "    Note right of WF: Activity Timed Out (ID: {activity_id}, Type: {timeout_type:?})"
                )?;
            }
            WorkflowEvent::ActivityHeartbeat { activity_id, .. } => {
                writeln!(
                    self.out,
                    "    Note right of WF: Activity Heartbeat (ID: {activity_id})"
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_timer_event(&mut self, event: &WorkflowEvent) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::TimerStarted {
                timer_id,
                duration_secs,
            } => {
                let participant = "Timer";
                if self.participants.insert(participant.to_string()) {
                    writeln!(self.out, "    participant {participant} as Timer")?;
                }
                writeln!(
                    self.out,
                    "    WF->>+{participant}: Start Timer {timer_id} ({duration_secs}s)"
                )?;
            }
            WorkflowEvent::TimerFired { timer_id } => {
                let participant = "Timer";
                writeln!(self.out, "    {participant}-->>-WF: Timer {timer_id} Fired")?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_child_workflow_event(
        &mut self,
        event: &WorkflowEvent,
    ) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name,
                ..
            } => {
                let participant = format!("Child_{workflow_name}");
                if self.participants.insert(participant.clone()) {
                    writeln!(
                        self.out,
                        "    participant {participant} as Child: {workflow_name}"
                    )?;
                }
                writeln!(
                    self.out,
                    "    WF->>+{participant}: Start Child (ID: {child_id})"
                )?;
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. } => {
                writeln!(
                    self.out,
                    "    Note right of WF: Child Workflow Completed (ID: {child_id})"
                )?;
            }
            WorkflowEvent::ChildWorkflowFailed { child_id, error } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note right of WF: Child Workflow Failed (ID: {child_id}): {safe_error}"
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_misc_event(&mut self, event: &WorkflowEvent) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::SignalReceived { signal_name, .. } => {
                let participant = "External";
                if self.participants.insert(participant.to_string()) {
                    writeln!(self.out, "    participant {participant} as External")?;
                }
                writeln!(
                    self.out,
                    "    {participant}->>WF: Signal Received: {signal_name}"
                )?;
            }
            WorkflowEvent::MarkerRecorded { name, .. } => {
                writeln!(self.out, "    Note over WF: Marker: {name}")?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
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

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_workflow_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        // WorkflowCompleted is not an unreachable arm, but ActivityScheduled is (for workflow event handler)
        let event = WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "test".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        };
        let _ = exporter.handle_workflow_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_activity_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_activity_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_timer_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_timer_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_child_workflow_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_child_workflow_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_misc_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_misc_event(&event);
    }
}
