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
use tracing::Instrument;

use crate::context::{SharedState, WorkflowCommand, WorkflowContext, empty_shared_state};
use crate::event::WorkflowEvent;
use crate::info::WorkflowHandlerFn;
use crate::types::ExecutionId;

/// The outcome of running a workflow function through the executor.
#[derive(Debug)]
pub enum WorkflowOutcome {
    /// The workflow ran to completion and returned a value.
    Completed {
        /// The final result serialized as JSON.
        output: Value,
    },
    /// The workflow function returned an error.
    Failed {
        /// The string description of the error encountered.
        error: String,
    },
    /// The workflow suspended awaiting activity results or timer firings.
    /// The accumulated commands describe what the worker needs to schedule.
    Suspended {
        /// A list of commands representing the side effects (e.g. activities) requested.
        commands: Vec<WorkflowCommand>,
    },
    /// The workflow signalled `continue_as_new`. The current execution is
    /// terminal and the worker should atomically start a fresh execution
    /// with the same logical `WorkflowId` but a new `ExecutionId`, passing
    /// `input` as the initial payload.
    ContinuedAsNew {
        /// JSON payload to pass to the next iteration of the workflow.
        input: Value,
    },
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

/// Like [`run_workflow`] but runs in strict replay mode.
///
/// Uses [`WorkflowContext::for_replay_strict`] so that activity and local-activity
/// dispatch additionally compare input payloads against the recorded history,
/// returning a non-determinism error on any mismatch.  This is used by
/// [`WorkflowReplayer`](crate::testing::WorkflowReplayer) to catch
/// input-changing code changes before deployment.
pub async fn run_workflow_strict(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
) -> WorkflowOutcome {
    let ctx = WorkflowContext::for_replay_strict_with_state(exec_id, history, state);

    let span = tracing::info_span!(
        "harvest.workflow.run_strict",
        "otel.kind" = "internal",
        workflow.execution_id = %exec_id,
    );

    async {
        let timeout_result = tokio::time::timeout(SUSPENSION_TIMEOUT, handler(&ctx, input)).await;
        match timeout_result {
            Ok(Ok(output)) => WorkflowOutcome::Completed { output },
            Ok(Err(error)) => WorkflowOutcome::Failed { error },
            Err(_elapsed) => {
                let mut commands = ctx.drain_commands();
                if let Some(idx) = commands
                    .iter()
                    .rposition(|cmd| matches!(cmd, WorkflowCommand::ContinueAsNew { .. }))
                    && let WorkflowCommand::ContinueAsNew { input } = commands.swap_remove(idx)
                {
                    return WorkflowOutcome::ContinuedAsNew { input };
                }
                WorkflowOutcome::Suspended { commands }
            }
        }
    }
    .instrument(span)
    .await
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

    // Emit a span around every executor cycle so operators can correlate the
    // handler invocation with timers, activity dispatches, and log events that
    // happen inside. Applications bridge `tracing` to OpenTelemetry via
    // `tracing-opentelemetry` or an equivalent layer — the harvest engine
    // itself stays backend-agnostic.
    let span = tracing::info_span!(
        "harvest.workflow.run",
        "otel.kind" = "internal",
        workflow.execution_id = %exec_id,
    );

    async {
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
                let mut commands = ctx.drain_commands();
                // ContinueAsNew is terminal: when the workflow body parks on
                // the dedicated suspension future, the latest command in the
                // drain is the ContinueAsNew the user requested. Any commands
                // earlier in the drain (e.g. RecordMarker for `version()` or
                // `side_effect`) are intentionally discarded — they describe
                // bookkeeping for an execution that is about to be sealed.
                if let Some(idx) = commands
                    .iter()
                    .rposition(|cmd| matches!(cmd, WorkflowCommand::ContinueAsNew { .. }))
                    && let WorkflowCommand::ContinueAsNew { input } = commands.swap_remove(idx)
                {
                    return WorkflowOutcome::ContinuedAsNew { input };
                }
                WorkflowOutcome::Suspended { commands }
            }
        }
    }
    .instrument(span)
    .await
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

    /// A workflow that triggers `continue_as_new` mid-flight.
    fn continue_as_new_workflow<'a>(
        ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            // The future returned by continue_as_new never resolves on its
            // own; the executor's suspension timeout drains the command and
            // surfaces it as ContinuedAsNew.
            let _ = ctx
                .continue_as_new(serde_json::json!({"prev": input}))
                .await;
            unreachable!("continue_as_new must not resolve");
        })
    }

    #[tokio::test]
    async fn executor_returns_continued_as_new_when_command_drained() {
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        }];

        let outcome = run_workflow(
            exec_id,
            history,
            continue_as_new_workflow,
            serde_json::json!("v1"),
        )
        .await;

        match outcome {
            WorkflowOutcome::ContinuedAsNew { input } => {
                assert_eq!(input, serde_json::json!({"prev": "v1"}));
            }
            other => panic!("expected ContinuedAsNew, got {other:?}"),
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
