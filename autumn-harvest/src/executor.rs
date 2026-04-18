//! Workflow executor -- runs a single workflow function through replay + live execution.
//!
//! The executor builds a [`WorkflowContext`] from the event history, runs the
//! handler with a short timeout, and classifies the outcome:
//!
//! - **Completed**: handler returned `Ok(output)`.
//! - **Failed**: handler returned `Err(error)`.
//! - **Suspended**: handler blocked on a oneshot (waiting for activity/timer resolution).
//!
//! This module is pure async logic and does NOT require the `db` feature.

use std::time::Duration;

use serde_json::Value;

use crate::context::{SharedState, WorkflowCommand, WorkflowContext, empty_shared_state};
use crate::event::WorkflowEvent;
use crate::info::WorkflowHandlerFn;
use crate::types::ExecutionId;

/// The outcome of running a workflow function through the executor.
#[derive(Debug)]
pub enum WorkflowOutcome {
    /// The workflow ran to completion and returned a value.
    Completed { output: Value },
    /// The workflow function returned an error.
    Failed { error: String },
    /// The workflow suspended awaiting activity results or timer firings.
    /// The accumulated commands describe what the worker needs to schedule.
    Suspended { commands: Vec<WorkflowCommand> },
}

/// Default timeout for detecting suspension -- if the workflow hasn't completed
/// within this window, it's blocked on a oneshot channel (suspended).
const SUSPENSION_TIMEOUT: Duration = Duration::from_millis(100);

/// Run a workflow function through replay and live execution.
///
/// Builds a [`WorkflowContext`] from the provided event history, invokes the
/// handler, and returns the outcome. If the handler completes within the
/// timeout, the result is `Completed` or `Failed`. If it blocks (suspended on
/// a oneshot waiting for activity/timer resolution), the accumulated commands
/// are returned as `Suspended`.
///
/// # Arguments
///
/// * `exec_id` - The execution ID for this workflow run.
/// * `history` - The event history to replay (must start with `WorkflowStarted`).
/// * `handler` - The type-erased workflow handler function.
/// * `input` - The serialized input to pass to the workflow.
pub async fn run_workflow(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
) -> WorkflowOutcome {
    run_workflow_with_state(exec_id, history, handler, input, empty_shared_state()).await
}

/// Run a workflow function through replay and live execution with shared state.
pub async fn run_workflow_with_state(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
) -> WorkflowOutcome {
    let ctx = WorkflowContext::for_replay_with_state(exec_id, history, state);

    // Run the handler with a timeout. If it completes, we get the result.
    // If it blocks on a oneshot (suspended), the timeout fires and we drain
    // the accumulated commands.
    let timeout_result = tokio::time::timeout(SUSPENSION_TIMEOUT, handler(&ctx, input)).await;

    match timeout_result {
        // Handler completed within the timeout window.
        Ok(Ok(output)) => WorkflowOutcome::Completed { output },
        Ok(Err(error)) => WorkflowOutcome::Failed { error },

        // Timeout elapsed -- the handler is suspended on a oneshot channel.
        // Drain the commands it emitted before suspending.
        Err(_elapsed) => {
            let commands = ctx.drain_commands();
            WorkflowOutcome::Suspended { commands }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::WorkflowEvent;
    use crate::types::{ActivityExecId, ExecutionId};
    use chrono::Utc;
    use std::pin::Pin;

    /// A trivial workflow that just returns its input.
    fn echo_workflow<'a>(
        _ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Ok(input) })
    }

    /// A workflow that always fails.
    fn failing_workflow<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Err("something went wrong".to_string()) })
    }

    /// A workflow that calls an activity (will suspend if not in history).
    fn activity_workflow<'a>(
        ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let result = ctx
                .execute_activity_raw("send_email", input, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(result)
        })
    }

    #[tokio::test]
    async fn executor_replays_completed_workflow() {
        let exec_id = ExecutionId::new();
        let input = serde_json::json!({"greeting": "hello"});

        // Full history: workflow started and the echo handler completes immediately.
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
        }];

        let outcome = run_workflow(exec_id, history, echo_workflow, input.clone()).await;

        match outcome {
            WorkflowOutcome::Completed { output } => {
                assert_eq!(output, input);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_returns_failed_for_erroring_workflow() {
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        }];

        let outcome = run_workflow(exec_id, history, failing_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Failed { error } => {
                assert!(error.contains("something went wrong"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_suspends_on_new_activity() {
        let exec_id = ExecutionId::new();
        let input = serde_json::json!({"to": "alice@example.com"});

        // History has only WorkflowStarted -- no activity events.
        // The workflow will call execute_activity_raw which will emit a
        // ScheduleActivity command and block on the oneshot.
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
        }];

        let outcome = run_workflow(exec_id, history, activity_workflow, input).await;

        match outcome {
            WorkflowOutcome::Suspended { commands } => {
                assert_eq!(commands.len(), 1, "expected exactly one command");
                assert!(
                    matches!(&commands[0], WorkflowCommand::ScheduleActivity { name, .. } if name == "send_email"),
                    "expected ScheduleActivity command for send_email"
                );
            }
            other => panic!("expected Suspended, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_replays_activity_from_history() {
        let exec_id = ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let input = serde_json::json!({"to": "alice@example.com"});
        let activity_output = serde_json::json!({"email_id": "msg-001"});

        // Full history with completed activity -- replay should complete.
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: input.clone(),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: activity_output.clone(),
            },
        ];

        let outcome = run_workflow(exec_id, history, activity_workflow, input).await;

        match outcome {
            WorkflowOutcome::Completed { output } => {
                assert_eq!(output, activity_output);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }
}
