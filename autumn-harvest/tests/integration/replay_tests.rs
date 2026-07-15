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
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId};
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
        WorkflowOutcome::Completed { output, .. } => {
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
        WorkflowOutcome::Failed { error, .. } => {
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:billing_v2".into(),
            details: serde_json::json!(2),
        },
    ];

    let outcome = run_workflow(exec_id, history, versioned_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
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
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    let outcome = run_workflow(exec_id, history, versioned_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
    ];

    let outcome = run_workflow(exec_id, history, activity_error_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(
                error.contains("flaky_step"),
                "error should mention activity name, got: {error}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Local activity replay tests
// ---------------------------------------------------------------------------

/// Workflow that uses one local activity (format) followed by one regular activity.
fn mixed_local_regular_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let formatted = ctx
            .execute_local_activity_raw("format_data", Value::Null, None, None)
            .await
            .map_err(|e| e.to_string())?;

        let sent = ctx
            .execute_activity_raw("send_email", formatted.clone(), "default")
            .await
            .map_err(|e| e.to_string())?;

        Ok(serde_json::json!({"formatted": formatted, "sent": sent}))
    })
}

/// Workflow that uses only local activities (no regular queue activities).
fn all_local_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let r1 = ctx
            .execute_local_activity_raw("step_1", Value::Null, None, None)
            .await
            .map_err(|e| e.to_string())?;
        let r2 = ctx
            .execute_local_activity_raw("step_2", r1.clone(), None, None)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"first": r1, "second": r2}))
    })
}

#[tokio::test]
async fn local_activity_completes_from_full_history() {
    let local_id = ActivityExecId::new();
    let regular_id = ActivityExecId::new();
    let local_out = serde_json::json!("formatted-text");
    let regular_out = serde_json::json!({"email_id": "msg-999"});
    let exec_id = ExecutionId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::LocalActivityScheduled {
            activity_id: local_id,
            name: "format_data".into(),
            input: Value::Null,
            retry_policy: None,
        },
        WorkflowEvent::LocalActivityCompleted {
            activity_id: local_id,
            output: local_out.clone(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: regular_id,
            name: "send_email".into(),
            input: local_out.clone(),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: regular_id,
            output: regular_out.clone(),
        },
    ];

    let outcome = run_workflow(exec_id, history, mixed_local_regular_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            assert_eq!(output["formatted"], local_out);
            assert_eq!(output["sent"], regular_out);
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

#[tokio::test]
async fn local_activity_suspends_when_not_in_history() {
    let exec_id = ExecutionId::new();

    // History has only WorkflowStarted — local activity is new.
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    let outcome = run_workflow(exec_id, history, mixed_local_regular_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Suspended { commands } => {
            assert_eq!(commands.len(), 1);
            assert!(
                matches!(
                    &commands[0],
                    autumn_harvest::context::WorkflowCommand::RunLocalActivity { name, .. }
                    if name == "format_data"
                ),
                "expected RunLocalActivity for format_data, got {commands:?}"
            );
        }
        other => panic!("expected Suspended, got {other:?}"),
    }
}

#[tokio::test]
async fn local_activity_replays_correctly_across_simulated_worker_restart() {
    let id1 = ActivityExecId::new();
    let id2 = ActivityExecId::new();
    let out1 = serde_json::json!("step1-result");
    let out2 = serde_json::json!("step2-result");
    let exec_id = ExecutionId::new();

    // Full history: both local activities completed.
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::LocalActivityScheduled {
            activity_id: id1,
            name: "step_1".into(),
            input: Value::Null,
            retry_policy: None,
        },
        WorkflowEvent::LocalActivityCompleted {
            activity_id: id1,
            output: out1.clone(),
        },
        WorkflowEvent::LocalActivityScheduled {
            activity_id: id2,
            name: "step_2".into(),
            input: out1.clone(),
            retry_policy: None,
        },
        WorkflowEvent::LocalActivityCompleted {
            activity_id: id2,
            output: out2.clone(),
        },
    ];

    let outcome = run_workflow(exec_id, history, all_local_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            assert_eq!(output["first"], out1);
            assert_eq!(output["second"], out2);
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

#[tokio::test]
async fn local_activity_with_retry_in_history_replays_final_success() {
    let id1 = ActivityExecId::new();
    let id2 = ActivityExecId::new();
    let final_out = serde_json::json!("ok");
    let exec_id = ExecutionId::new();

    // History shows two failed attempts for step_1, then success, then step_2.
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::LocalActivityScheduled {
            activity_id: id1,
            name: "step_1".into(),
            input: Value::Null,
            retry_policy: None,
        },
        WorkflowEvent::LocalActivityFailed {
            activity_id: id1,
            error: "transient".into(),
            attempt: 1,
        },
        WorkflowEvent::LocalActivityFailed {
            activity_id: id1,
            error: "still transient".into(),
            attempt: 2,
        },
        WorkflowEvent::LocalActivityCompleted {
            activity_id: id1,
            output: final_out.clone(),
        },
        WorkflowEvent::LocalActivityScheduled {
            activity_id: id2,
            name: "step_2".into(),
            input: final_out.clone(),
            retry_policy: None,
        },
        WorkflowEvent::LocalActivityCompleted {
            activity_id: id2,
            output: serde_json::json!("done"),
        },
    ];

    let outcome = run_workflow(exec_id, history, all_local_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            assert_eq!(output["first"], final_out);
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

#[tokio::test]
async fn local_activity_exhausted_retries_fails_the_workflow() {
    let id = ActivityExecId::new();
    let exec_id = ExecutionId::new();

    // History shows one failed attempt with no completion (retries exhausted).
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::LocalActivityScheduled {
            activity_id: id,
            name: "step_1".into(),
            input: Value::Null,
            retry_policy: None,
        },
        WorkflowEvent::LocalActivityFailed {
            activity_id: id,
            error: "permanent failure".into(),
            attempt: 1,
        },
    ];

    let outcome = run_workflow(exec_id, history, all_local_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(
                error.contains("permanent failure") || error.contains("step_1"),
                "error should mention failure, got: {error}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Await condition workflow handlers and integration tests for TDD RED phase
// ---------------------------------------------------------------------------

fn await_condition_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mut approvals = 0;
        if approvals < 2 {
            let _ = ctx
                .wait_for_signal("approved")
                .await
                .map_err(|e| e.to_string())?;
            approvals += 1;
        }
        if approvals < 2 {
            let _ = ctx
                .wait_for_signal("approved")
                .await
                .map_err(|e| e.to_string())?;
            approvals += 1;
        }
        ctx.await_condition(move || approvals >= 2)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"done": true}))
    })
}

fn await_condition_timeout_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let approvals = 0;
        let met = ctx
            .await_condition_timeout("my-timer", 60, move || approvals >= 2)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"met": met}))
    })
}

fn await_condition_timeout_happy_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mut approvals = 0;
        if approvals < 2 {
            let _ = ctx
                .wait_for_signal("approved")
                .await
                .map_err(|e| e.to_string())?;
            approvals += 1;
        }
        if approvals < 2 {
            let _ = ctx
                .wait_for_signal("approved")
                .await
                .map_err(|e| e.to_string())?;
            approvals += 1;
        }
        let met = ctx
            .await_condition_timeout("my-timer", 60, move || approvals >= 2)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"met": met}))
    })
}

#[tokio::test]
async fn replay_await_condition_happy_path_completes_when_predicate_met() {
    let exec_id = ExecutionId::new();

    // History contains 2 "approved" signals. Predicate requires >= 2.
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "approved".into(),
            payload: Value::Null,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "approved".into(),
            payload: Value::Null,
        },
    ];

    let outcome = run_workflow(exec_id, history, await_condition_workflow, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            assert_eq!(output, serde_json::json!({"done": true}));
        }
        other => panic!("expected Completed when predicate is met, got {other:?}"),
    }
}

#[tokio::test]
async fn replay_await_condition_happy_path_suspends_when_predicate_not_met() {
    let exec_id = ExecutionId::new();

    // History has only 1 signal. Predicate requires >= 2, so it should suspend.
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "approved".into(),
            payload: Value::Null,
        },
    ];

    let outcome = run_workflow(exec_id, history, await_condition_workflow, Value::Null).await;

    assert!(
        matches!(outcome, WorkflowOutcome::Suspended { .. }),
        "expected Suspended because approvals < 2, got {outcome:?}"
    );
}

#[tokio::test]
async fn replay_await_condition_timeout_resolves_true_if_condition_met() {
    let exec_id = ExecutionId::new();

    // History has 2 signals. Condition met before timeout.
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "approved".into(),
            payload: Value::Null,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "approved".into(),
            payload: Value::Null,
        },
    ];

    let outcome = run_workflow(
        exec_id,
        history,
        await_condition_timeout_happy_workflow,
        Value::Null,
    )
    .await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            assert_eq!(output, serde_json::json!({"met": true}));
        }
        other => panic!("expected Completed(true), got {other:?}"),
    }
}

#[tokio::test]
async fn replay_await_condition_timeout_resolves_false_if_timer_fires_first() {
    let exec_id = ExecutionId::new();

    // History shows timer fired. approvals = 0, so condition met = false.
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("my-timer"),
            duration_secs: 60,
        },
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("my-timer"),
        },
    ];

    let outcome = run_workflow(
        exec_id,
        history,
        await_condition_timeout_workflow,
        Value::Null,
    )
    .await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            assert_eq!(output, serde_json::json!({"met": false}));
        }
        other => panic!("expected Completed(false), got {other:?}"),
    }
}

#[cfg(feature = "testing")]
fn non_deterministic_await_condition_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.await_condition(|| {
            // Predicate evaluates to false on replay, causing early suspension,
            // but the history has a timer event that expects us to have proceeded.
            false
        })
        .await
        .map_err(|e| e.to_string())?;

        ctx.timer("subsequent-timer", 5)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"done": true}))
    })
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn replay_await_condition_non_deterministic_divergence_fails() {
    use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};

    let exec_id = ExecutionId::new();

    // History claims we completed the condition and started a timer.
    // But on replay, the condition returns false, so the workflow suspends.
    // The history matcher should catch this divergence and fail the replay.
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("subsequent-timer"),
            duration_secs: 5,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "non_deterministic",
            non_deterministic_await_condition_workflow,
        )
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "non_deterministic".to_string(),
            execution_id: exec_id,
            events: history,
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
        })
        .await;

    match report.status {
        ReplayStatus::NonDeterminismDetected { .. } => {
            // Success! The replayer detected that the workflow suspended early / diverged.
        }
        other => panic!("expected ReplayStatus::NonDeterminismDetected, got {other:?}"),
    }
}
