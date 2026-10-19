//! Depth cap for workflow-side payloads (issue #2224).
//!
//! A value that workflow code hands to the engine can nest past the
//! `serde_json` read limit. The context refuses it at the call site, so the
//! workflow can handle the error. A workflow result has no call site. A deep
//! result fails the run. No path stores a value that a later drive cannot
//! read.

#![allow(clippy::used_underscore_binding)]
#![allow(clippy::unused_async)]

use std::time::Duration;

use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{
    ActivitySpec, MAX_PAYLOAD_DEPTH, PAYLOAD_TOO_DEEP_ERROR_TYPE, RunState, SqliteError,
    SqliteRuntime, WorkflowEvent,
};
use serde_json::{Value, json};

/// The depth of the issue #2224 probe. `serde_json` reads at most 128 levels.
/// The event wrapper adds two, so a stored value of this depth fails to read.
const PROBE_DEPTH: usize = 126;

fn nested(depth: usize) -> Value {
    let mut value = json!(1);
    for _ in 0..depth {
        value = Value::Array(vec![value]);
    }
    value
}

/// A pass-through remote activity.
#[activity]
async fn echo_remote(_ctx: &ActivityContext, v: Value) -> Result<Value, String> {
    Ok(v)
}

/// A pass-through local activity.
#[activity(local = true)]
async fn echo_local(_ctx: &ActivityContext, v: Value) -> Result<Value, String> {
    Ok(v)
}

/// Do the call that `path` names with a value of `depth` levels.
///
/// The input is `{ "path": .., "depth": .. }`. Any other input is the input
/// of a continued run, and the run returns `"done"`.
#[workflow]
async fn probe(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let (Some(path), Some(depth)) = (
        input.get("path").and_then(Value::as_str),
        input.get("depth").and_then(Value::as_u64),
    ) else {
        return Ok(json!("done"));
    };
    let value = nested(usize::try_from(depth).map_err(|e| e.to_string())?);
    let outcome = match path {
        "activity" => ctx
            .execute_activity_raw("echo_remote", value, "default")
            .await
            .map(|_| ()),
        "local_activity" => ctx
            .execute_local_activity_raw("echo_local", value, None, None)
            .await
            .map(|_| ()),
        "child" => ctx
            .spawn_child_workflow_raw("probe", value)
            .await
            .map(|_| ()),
        "continue_as_new" => ctx.continue_as_new(value).await,
        "side_effect" => ctx.side_effect("s", || value).map(|_| ()),
        "result" => return Ok(value),
        other => return Err(format!("unknown path {other}")),
    };
    if path == "side_effect" && outcome.is_ok() {
        // The timer ends the cycle, so the stored event is read back.
        ctx.timer("t", 1).await.map_err(|e| e.to_string())?;
    }
    match outcome {
        Ok(()) => Ok(json!({ "ok": true })),
        Err(e) => Ok(json!({ "failed": e.to_string() })),
    }
}

fn runtime() -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&probe_info());
    rt.register_activity(&echo_local_info());
    rt.register_activity_raw("echo_remote", ActivitySpec::new(1, |v| Ok(v)));
    rt
}

/// Drive a probe to rest. Return the state and whether the history reads back.
async fn drive(path: &str, depth: usize) -> (RunState, Vec<WorkflowEvent>) {
    let mut rt = runtime();
    let exec = rt
        .start_workflow("probe", json!({ "path": path, "depth": depth }))
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    rt.run_until_idle().await.unwrap();
    let history = rt.load_history(exec).unwrap();
    (state, history)
}

fn workflow_output(state: &RunState) -> &Value {
    match state {
        RunState::Completed(output) => output,
        other => panic!("expected a completed run, got {other:?}"),
    }
}

/// The call paths that return a handled error to the workflow.
const CALL_PATHS: [&str; 5] = [
    "activity",
    "local_activity",
    "child",
    "continue_as_new",
    "side_effect",
];

#[tokio::test]
async fn a_value_over_the_cap_is_refused_to_the_workflow() {
    for path in CALL_PATHS {
        for depth in [MAX_PAYLOAD_DEPTH + 1, PROBE_DEPTH] {
            let (state, history) = drive(path, depth).await;
            let output = workflow_output(&state);
            let failed = output
                .get("failed")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("{path}@{depth}: expected a handled error: {output}"));
            assert!(
                failed.contains("too deep"),
                "{path}@{depth}: the error names the depth: {failed}"
            );
            assert!(
                !history.is_empty(),
                "{path}@{depth}: the history reads back"
            );
        }
    }
}

#[tokio::test]
async fn a_value_at_the_cap_is_accepted() {
    for path in CALL_PATHS {
        let (state, history) = drive(path, MAX_PAYLOAD_DEPTH).await;
        match path {
            // A continued run ends the first run, so it reports no output.
            "continue_as_new" => assert!(
                history
                    .iter()
                    .any(|e| matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. })),
                "the run continued as new"
            ),
            // The timer is still armed. The value is stored and read back.
            "side_effect" => assert!(matches!(state, RunState::WaitingTimer), "got {state:?}"),
            _ => {
                let output = workflow_output(&state);
                assert!(output.get("failed").is_none(), "{path}: refused: {output}");
            }
        }
        assert!(!history.is_empty(), "{path}: the history reads back");
    }
}

#[tokio::test]
async fn a_result_at_the_cap_completes() {
    let (state, history) = drive("result", MAX_PAYLOAD_DEPTH).await;
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
    assert!(!history.is_empty());
}

#[tokio::test]
async fn a_result_over_the_cap_fails_the_run_and_the_history_reads_back() {
    for depth in [MAX_PAYLOAD_DEPTH + 1, PROBE_DEPTH] {
        let (state, history) = drive("result", depth).await;
        let RunState::Failed(error) = &state else {
            panic!("@{depth}: expected a failed run, got {state:?}");
        };
        assert!(error.contains("workflow result nests more than"), "{error}");
        let failed = history
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::WorkflowFailed { error_type, .. } => Some(error_type.clone()),
                _ => None,
            })
            .expect("a WorkflowFailed event");
        assert_eq!(failed.as_deref(), Some(PAYLOAD_TOO_DEEP_ERROR_TYPE));
        assert!(
            !history
                .iter()
                .any(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. })),
            "@{depth}: no deep WorkflowCompleted is stored"
        );
    }
}

/// Returns a deep result on every attempt. The policy allows five attempts.
#[workflow(retry = RetryPolicy {
    max_attempts: 5,
    initial_interval: Duration::ZERO,
    backoff_coefficient: 1.0,
    max_interval: Duration::ZERO,
    non_retryable_errors: Vec::new(),
    jitter: JitterPolicy::None,
})]
async fn deep_with_retry(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let _ = ctx;
    Ok(nested(PROBE_DEPTH))
}

#[tokio::test]
async fn a_deep_result_is_not_retried() {
    let mut rt = runtime();
    rt.register_workflow(&deep_with_retry_info());
    let exec = rt.start_workflow("deep_with_retry", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Failed(_)), "got {state:?}");
    rt.run_until_idle().await.unwrap();
    let history = rt.load_history(exec).unwrap();
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. })),
        "the same result would recur, so no retry is scheduled"
    );
}

/// Serve an `update` handler whose result nests `depth` levels.
#[workflow]
async fn deep_update(ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    ctx.register_update_handler_no_validator("deep", |depth: Value| async move {
        let depth = usize::try_from(depth.as_u64().unwrap_or_default()).unwrap_or_default();
        Ok(nested(depth))
    });
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

#[tokio::test]
async fn an_update_result_over_the_cap_fails_the_update_and_the_run_stays_live() {
    let mut rt = runtime();
    rt.register_workflow(&deep_update_info());
    let exec = rt.start_workflow("deep_update", json!(0)).unwrap();
    rt.run_until_idle().await.unwrap();

    let at_cap = rt.update(exec, "deep", json!(MAX_PAYLOAD_DEPTH)).await;
    assert_eq!(at_cap.unwrap(), nested(MAX_PAYLOAD_DEPTH));

    for depth in [MAX_PAYLOAD_DEPTH + 1, PROBE_DEPTH] {
        let err = rt.update(exec, "deep", json!(depth)).await.unwrap_err();
        assert!(
            matches!(&err, SqliteError::UpdateFailed { error, .. } if error.contains("too deep")),
            "@{depth}: got {err:?}"
        );
    }
    // The history, with the stored updates, reads back and the run drives on.
    rt.run_until_idle().await.unwrap();
    assert!(rt.load_history(exec).is_ok());
    rt.send_signal(exec, "go", json!("ok")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v == "ok"));
}

/// Catch the refusal of a deep call, then wait on a timer.
#[workflow]
async fn catch_then_wait(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let path = input.as_str().unwrap_or_default();
    let value = nested(MAX_PAYLOAD_DEPTH + 1);
    let refused = match path {
        "activity" => ctx
            .execute_activity_raw("echo_remote", value, "default")
            .await
            .is_err(),
        "local_activity" => ctx
            .execute_local_activity_raw("echo_local", value, None, None)
            .await
            .is_err(),
        "child" => ctx.spawn_child_workflow_raw("probe", value).await.is_err(),
        _ => return Err(format!("unknown path {path}")),
    };
    ctx.timer("t", 1).await.map_err(|e| e.to_string())?;
    Ok(json!({ "refused": refused }))
}

#[tokio::test]
async fn a_caught_refusal_replays_after_a_later_command() {
    for path in ["activity", "local_activity", "child"] {
        let mut rt = runtime();
        rt.register_workflow(&catch_then_wait_info());
        let exec = rt.start_workflow("catch_then_wait", json!(path)).unwrap();
        // The first drive arms the timer. The second drive replays the refusal.
        let first = rt.run_until_blocked(exec).await.unwrap();
        assert!(matches!(first, RunState::WaitingTimer), "{path}: {first:?}");
        let again = rt.run_until_blocked(exec).await;
        assert!(
            matches!(again, Ok(RunState::WaitingTimer)),
            "{path}: replay diverged: {again:?}"
        );
    }
}
