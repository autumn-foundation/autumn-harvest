//! Replay engine correctness tests — pure unit tests, no database required.
//!
//! These tests exercise the executor's replay logic by constructing synthetic
//! event histories and verifying the `WorkflowOutcome` produced by
//! `executor::run_workflow()`.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Test workflow handler functions (must be `fn` pointers, not closures)
// ---------------------------------------------------------------------------

/// Workflow that executes two sequential activities and combines their results.
fn two_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let r1 = ctx
            .execute_activity_raw("step_1", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;

        let r2 = ctx
            .execute_activity_raw("step_2", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;

        Ok(serde_json::json!({
            "first": r1,
            "second": r2,
        }))
    })
}

/// Workflow that calls an activity named `wrong_name` -- used to test
/// non-determinism detection when history has a different activity name.
fn wrong_name_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = ctx
            .execute_activity_raw("wrong_name", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    })
}

/// Workflow that uses `ctx.version()` to gate code paths.
fn versioned_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let v = ctx.version("billing_v2", 1, 3);
        Ok(serde_json::json!({"version": v}))
    })
}

/// Workflow that calls two activities — suspends if only the first is in history.
fn two_step_suspend_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let r1 = ctx
            .execute_activity_raw("step_1", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;

        // This second call will suspend if not in history
        let r2 = ctx
            .execute_activity_raw("step_2", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;

        Ok(serde_json::json!({"r1": r1, "r2": r2}))
    })
}

/// Workflow that calls a single activity and propagates any error.
fn activity_error_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = ctx
            .execute_activity_raw("flaky_step", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Replay with 2 completed activity pairs. Workflow reads both and returns
/// a combined result.
#[tokio::test]
async fn replay_two_sequential_activities() {
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let id2 = ActivityExecId::new();
    let output1 = serde_json::json!("result_1");
    let output2 = serde_json::json!("result_2");

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_1".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: output1.clone(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id2,
            name: "step_2".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id2,
            output: output2.clone(),
        },
    ];

    let outcome = run_workflow(exec_id, history, two_activity_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output } => {
            assert_eq!(
                output,
                serde_json::json!({"first": "result_1", "second": "result_2"})
            );
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// History has `step_1` but workflow calls `wrong_name` at that position.
/// The replay engine should detect the non-determinism and the workflow
/// should fail with an error message mentioning the mismatch.
#[tokio::test]
async fn replay_detects_non_determinism() {
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_1".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: serde_json::json!("ok"),
        },
    ];

    let outcome = run_workflow(exec_id, history, wrong_name_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Failed { error } => {
            assert!(
                error.contains("wrong_name") || error.contains("step_1"),
                "error should mention activity name mismatch, got: {error}"
            );
        }
        other => panic!("expected Failed due to non-determinism, got {other:?}"),
    }
}

/// Version gate routes code paths:
/// - With a recorded marker in history, returns the recorded version.
/// - With empty history (past end), returns `max_version`.
#[tokio::test]
async fn version_gate_routes_code_paths_with_marker() {
    let exec_id = ExecutionId::new();

    // History with a version marker recording version 2
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:billing_v2".into(),
            details: serde_json::json!(2),
        },
    ];

    let outcome = run_workflow(exec_id, history, versioned_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output } => {
            assert_eq!(output, serde_json::json!({"version": 2}));
        }
        other => panic!("expected Completed with version 2, got {other:?}"),
    }
}

/// Version gate with empty history (new code path) returns `max_version`.
#[tokio::test]
async fn version_gate_new_execution_returns_max() {
    let exec_id = ExecutionId::new();

    // Only WorkflowStarted, no marker — past end of history
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
    }];

    let outcome = run_workflow(exec_id, history, versioned_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output } => {
            // max_version = 3 for our versioned_workflow
            assert_eq!(output, serde_json::json!({"version": 3}));
        }
        other => panic!("expected Completed with version 3, got {other:?}"),
    }
}

/// History has 1 completed activity but workflow calls 2. The second call
/// should suspend (no history to replay from).
#[tokio::test]
async fn workflow_suspends_mid_execution() {
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();

    // History only has the first activity completed
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_1".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: serde_json::json!("first_done"),
        },
    ];

    let outcome = run_workflow(exec_id, history, two_step_suspend_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Suspended { commands } => {
            // The second activity call should have emitted a ScheduleActivity command
            assert_eq!(commands.len(), 1, "expected exactly 1 pending command");
            assert!(
                matches!(
                    &commands[0],
                    autumn_harvest::context::WorkflowCommand::ScheduleActivity { name, .. }
                    if name == "step_2"
                ),
                "expected ScheduleActivity for step_2, got {:?}",
                commands[0]
            );
        }
        other => panic!("expected Suspended, got {other:?}"),
    }
}

/// History has `ActivityFailed` for the activity -- workflow should get the
/// error and propagate it as a Failed outcome.
#[tokio::test]
async fn replay_handles_failed_activity() {
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "flaky_step".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: id1,
            error: "SMTP connection refused".into(),
            attempt: 3,
        },
    ];

    let outcome = run_workflow(exec_id, history, activity_error_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Failed { error } => {
            assert!(
                error.contains("flaky_step"),
                "error should mention activity name, got: {error}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}
