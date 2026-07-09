//! Generates runnable Rust integration tests from production event histories.
//!
//! This tool takes a recorded workflow execution history (e.g., from a failing
//! production run) and generates a self-contained `#[tokio::test]` that uses
//! `WorkflowSimulator` to exactly reproduce the execution locally, mocking out
//! all activities to match their recorded production outcomes.
//!
//! Expects a **production-shaped** history: every `ActivityCompleted`/
//! `ActivityFailed` event's `activity_id` has a matching earlier
//! `ActivityScheduled` (true of every history read from `harvest_events`,
//! since a real execution appends exactly one terminal event per scheduled
//! activity). A history produced by `WorkflowSimulator`'s own retry loop
//! (`simulator.rs`) is not safe input here: its non-terminal retry-attempt
//! `ActivityFailed` events intentionally use a synthetic id with no matching
//! `ActivityScheduled`, and the id-keyed lookups below silently drop any
//! event whose id they don't recognize.

use crate::event::WorkflowEvent;
use crate::types::ActivityExecId;
use std::collections::HashMap;
use std::fmt::Write;

/// Generates a Rust test harness from a workflow event history.
pub struct TestHarnessGenerator {
    test_name: String,
    handler_name: String,
}

impl Default for TestHarnessGenerator {
    fn default() -> Self {
        Self {
            test_name: "generated_history_test".to_string(),
            handler_name: "TODO_WORKFLOW_HANDLER".to_string(),
        }
    }
}

impl TestHarnessGenerator {
    /// Create a new generator with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the generated test function name.
    #[must_use]
    pub fn with_test_name(mut self, name: impl Into<String>) -> Self {
        self.test_name = name.into();
        self
    }

    /// Override the workflow handler function name to use in `WorkflowSimulator::new`.
    #[must_use]
    pub fn with_handler_name(mut self, name: impl Into<String>) -> Self {
        self.handler_name = name.into();
        self
    }

    /// Generate the Rust test code string from a given event history.
    ///
    /// # Errors
    /// Returns `std::fmt::Error` if string formatting fails.
    #[allow(clippy::too_many_lines)]
    pub fn generate(&self, history: &[WorkflowEvent]) -> Result<String, std::fmt::Error> {
        let mut out = String::new();
        let mut activity_names: HashMap<ActivityExecId, String> = HashMap::new();
        let mut mocks = Vec::new();
        let mut workflow_input = serde_json::Value::Null;
        let mut final_result: Option<Result<serde_json::Value, String>> = None;

        for event in history {
            match event {
                WorkflowEvent::WorkflowStarted { input, .. } => {
                    workflow_input = input.clone();
                }
                WorkflowEvent::ActivityScheduled {
                    activity_id, name, ..
                } => {
                    activity_names.insert(*activity_id, name.clone());
                }
                WorkflowEvent::ActivityCompleted {
                    activity_id,
                    output,
                } => {
                    if let Some(name) = activity_names.get(activity_id) {
                        mocks.push((name.clone(), Ok(output.clone())));
                    }
                }
                WorkflowEvent::ActivityFailed {
                    activity_id, error, ..
                } => {
                    if let Some(name) = activity_names.get(activity_id) {
                        // For simplicity, we just capture the failure outcome for the mock.
                        mocks.push((name.clone(), Err(error.clone())));
                    }
                }
                WorkflowEvent::WorkflowCompleted { output } => {
                    final_result = Some(Ok(output.clone()));
                }
                WorkflowEvent::WorkflowFailed { error, .. } => {
                    final_result = Some(Err(error.clone()));
                }
                _ => {}
            }
        }

        writeln!(out, "#[tokio::test]")?;
        writeln!(out, "async fn {}() {{", self.test_name)?;
        writeln!(out, "    use autumn_harvest::simulator::WorkflowSimulator;")?;
        writeln!(out)?;
        writeln!(
            out,
            "    let sim = WorkflowSimulator::new({})",
            self.handler_name
        )?;

        // De-duplicate mocks by name (take the first occurrence for simplicity).
        let mut mocked_names = std::collections::HashSet::new();

        for (name, outcome) in mocks {
            if mocked_names.insert(name.clone()) {
                match outcome {
                    Ok(output) => {
                        let json_str =
                            serde_json::to_string(&output).unwrap_or_else(|_| "null".into());
                        writeln!(out, "        .mock_activity(\"{name}\", |_input| {{")?;
                        writeln!(out, "            Ok(serde_json::json!({json_str}))")?;
                        writeln!(out, "        }})")?;
                    }
                    Err(error) => {
                        writeln!(out, "        .mock_activity(\"{name}\", |_input| {{")?;
                        writeln!(out, "            Err({error:?}.to_string())")?;
                        writeln!(out, "        }})")?;
                    }
                }
            }
        }
        writeln!(out, "        ;")?;
        writeln!(out)?;

        let input_str = serde_json::to_string(&workflow_input).unwrap_or_else(|_| "null".into());
        writeln!(out, "    let input = serde_json::json!({input_str});")?;
        writeln!(out, "    let result = sim.run(input).await;")?;
        writeln!(out)?;

        match final_result {
            Some(Ok(output)) => {
                let output_str = serde_json::to_string(&output).unwrap_or_else(|_| "null".into());
                writeln!(out, "    assert!(result.final_output.is_ok());")?;
                writeln!(out, "    assert_eq!(")?;
                writeln!(out, "        result.final_output.unwrap(),")?;
                writeln!(out, "        serde_json::json!({output_str})")?;
                writeln!(out, "    );")?;
            }
            Some(Err(error)) => {
                writeln!(out, "    assert!(result.final_output.is_err());")?;
                writeln!(out, "    assert_eq!(")?;
                writeln!(out, "        result.final_output.unwrap_err(),")?;
                writeln!(out, "        {error:?}")?;
                writeln!(out, "    );")?;
            }
            None => {
                writeln!(out, "    // No terminal workflow event found in history.")?;
            }
        }

        writeln!(out, "}}")?;

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_generator_successful_workflow() {
        let act_id = ActivityExecId::new();
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({"user": "alice"}),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: act_id,
                name: "send_email".into(),
                input: serde_json::Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: act_id,
                output: serde_json::json!({"sent": true}),
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::json!({"status": "ok"}),
            },
        ];

        let generator = TestHarnessGenerator::new().with_handler_name("my_handler");
        let code = generator
            .generate(&history)
            .expect("generate should succeed");

        assert!(code.contains("#[tokio::test]"));
        assert!(code.contains("let sim = WorkflowSimulator::new(my_handler)"));
        assert!(code.contains(".mock_activity(\"send_email\""));
        assert!(code.contains("Ok(serde_json::json!({\"sent\":true}))"));
        assert!(code.contains("let input = serde_json::json!({\"user\":\"alice\"});"));
        assert!(code.contains("assert!(result.final_output.is_ok());"));
        assert!(code.contains("serde_json::json!({\"status\":\"ok\"})"));
    }

    #[test]
    fn test_generator_failed_workflow_and_activity() {
        let act_id = ActivityExecId::new();
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: act_id,
                name: "charge_card".into(),
                input: serde_json::Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id: act_id,
                error: "insufficient funds".into(),
                attempt: 1,
                error_type: "Error".into(),
                non_retryable: false,
                details: None,
            },
            WorkflowEvent::workflow_failed("workflow failed: charge_card failed"),
        ];

        let generator = TestHarnessGenerator::new();
        let code = generator
            .generate(&history)
            .expect("generate should succeed");

        assert!(code.contains(".mock_activity(\"charge_card\""));
        assert!(code.contains("Err(\"insufficient funds\".to_string())"));
        assert!(code.contains("assert!(result.final_output.is_err());"));
        assert!(code.contains("\"workflow failed: charge_card failed\""));
    }
}
