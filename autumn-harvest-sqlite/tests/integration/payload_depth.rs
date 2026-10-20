//! Payload-depth cap at the ingress boundaries (issue #2157).
//!
//! `serde_json` writes any depth but refuses to read past 128 levels.
//! A payload nested too deep used to be stored, and then every drive failed
//! with "corrupt stored value". These tests drive the public API and check
//! that ingress refuses such a payload before it stores anything.
//!
//! An activity result is one more boundary (issue #2216). A result nested too
//! deep becomes a non-retryable `PayloadTooDeep` failure. The workflow sees
//! the failure, and the run can continue.

#![allow(clippy::used_underscore_binding)]
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{
    ActivitySpec, MAX_PAYLOAD_DEPTH, PAYLOAD_TOO_DEEP_ERROR_TYPE, RunState, SignalWithStart,
    SqliteError, SqliteRuntime, StartOptions, WorkflowEvent,
};
use serde_json::{Value, json};

/// A value of `depth` nested one-element arrays around the number 1.
fn nested(depth: usize) -> Value {
    let mut value = json!(1);
    for _ in 0..depth {
        value = Value::Array(vec![value]);
    }
    value
}

/// A value of `depth` nested one-key objects around the number 1.
fn nested_object(depth: usize) -> Value {
    let mut value = json!(1);
    for _ in 0..depth {
        value = json!({ "k": value });
    }
    value
}

/// Echo the input.
#[workflow]
async fn echo_input(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let _ = ctx;
    Ok(input)
}

/// Wait for the `go` signal, then echo its payload. Also serves an `echo`
/// update.
#[workflow]
async fn echo_signal(ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    ctx.register_update_handler_no_validator("echo", |input: Value| async move { Ok(input) });
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

fn runtime() -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&echo_input_info());
    rt.register_workflow(&echo_signal_info());
    rt
}

fn assert_too_deep(err: &SqliteError, expected_kind: &str) {
    match err {
        SqliteError::PayloadTooDeep {
            kind, cap_depth, ..
        } => {
            assert_eq!(*kind, expected_kind);
            assert_eq!(*cap_depth, MAX_PAYLOAD_DEPTH);
        }
        other => panic!("expected PayloadTooDeep, got: {other:?}"),
    }
}

#[tokio::test]
async fn start_input_over_the_cap_is_rejected_and_the_driver_stays_healthy() {
    let mut rt = runtime();
    let err = rt
        .start_workflow("echo_input", nested(MAX_PAYLOAD_DEPTH + 1))
        .unwrap_err();
    assert_too_deep(&err, "workflow_input");
    rt.run_until_idle().await.unwrap();
}

#[tokio::test]
async fn start_input_at_the_cap_round_trips() {
    let mut rt = runtime();
    let input = nested(MAX_PAYLOAD_DEPTH);
    let exec = rt.start_workflow("echo_input", input.clone()).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if *v == input));
}

#[tokio::test]
async fn signal_over_the_cap_is_rejected_and_the_run_stays_live() {
    let mut rt = runtime();
    let exec = rt.start_workflow("echo_signal", json!(0)).unwrap();
    rt.run_until_idle().await.unwrap();

    let err = rt
        .send_signal(exec, "go", nested(MAX_PAYLOAD_DEPTH + 1))
        .unwrap_err();
    assert_too_deep(&err, "signal_payload");

    // Nothing staged: the driver is healthy and a valid signal still lands.
    rt.run_until_idle().await.unwrap();
    rt.send_signal(exec, "go", json!("ok")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v == "ok"));
}

#[tokio::test]
async fn signal_at_the_cap_round_trips() {
    let mut rt = runtime();
    let exec = rt.start_workflow("echo_signal", json!(0)).unwrap();
    rt.run_until_idle().await.unwrap();
    let payload = nested(MAX_PAYLOAD_DEPTH);
    rt.send_signal(exec, "go", payload.clone()).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if *v == payload));
}

#[tokio::test]
async fn memo_over_the_cap_is_rejected() {
    let mut rt = runtime();
    let mut options = StartOptions::new();
    options.memo = Some(nested(MAX_PAYLOAD_DEPTH + 1));
    let err = rt
        .start_workflow_with_options("echo_input", json!(0), options)
        .unwrap_err();
    assert_too_deep(&err, "memo");
    rt.run_until_idle().await.unwrap();
}

#[tokio::test]
async fn signal_with_start_rejects_both_payloads_and_stores_nothing() {
    let mut rt = runtime();
    let request = |input: Value, payload: Value| SignalWithStart {
        workflow_name: "echo_signal",
        workflow_id: "wf-deep",
        input,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        signal_name: "go",
        signal_payload: payload,
        idempotency_key: None,
    };
    let deep = nested(MAX_PAYLOAD_DEPTH + 1);

    let err = rt
        .signal_with_start(request(deep.clone(), json!("ok")))
        .unwrap_err();
    assert_too_deep(&err, "workflow_input");
    let err = rt.signal_with_start(request(json!(0), deep)).unwrap_err();
    assert_too_deep(&err, "signal_payload");

    // The rejected calls rolled back: no run exists for the key.
    let outcome = rt
        .signal_with_start(request(json!(0), json!("ok")))
        .unwrap();
    assert!(outcome.started_fresh);
    rt.run_until_idle().await.unwrap();
}

/// Far past the cap, yet shallow enough that the caller can drop the value.
/// Dropping a `Value` recurses, so a much deeper one overflows on macOS.
const FAR_PAST_THE_CAP: usize = 1_000;

#[test]
fn a_value_far_past_the_cap_is_rejected() {
    let mut rt = runtime();
    let deep = nested(FAR_PAST_THE_CAP);
    let err = rt.start_workflow("echo_input", deep).unwrap_err();
    assert_too_deep(&err, "workflow_input");
}

#[tokio::test]
async fn a_deep_promise_signal_is_rejected_as_too_deep() {
    let mut rt = runtime();
    let exec = rt.start_workflow("echo_signal", json!(0)).unwrap();
    rt.run_until_idle().await.unwrap();
    // Promise signals are decoded before the generic payload checks run. A
    // wrong order reports an invalid settlement and not a depth error.
    let name = format!(
        "{}x",
        autumn_harvest::durable_promise::PROMISE_SIGNAL_PREFIX
    );
    let err = rt
        .send_signal(exec, &name, nested(FAR_PAST_THE_CAP))
        .unwrap_err();
    assert_too_deep(&err, "signal_payload");
}

#[tokio::test]
async fn nested_objects_and_the_by_id_signal_path_obey_the_cap() {
    let mut rt = runtime();
    let exec = rt
        .start_workflow_with_id("echo_signal", "wf-obj", json!(0))
        .unwrap();
    rt.run_until_idle().await.unwrap();

    let err = rt
        .send_signal_by_workflow_id(
            "echo_signal",
            "wf-obj",
            "go",
            nested_object(MAX_PAYLOAD_DEPTH + 1),
            None,
        )
        .unwrap_err();
    assert_too_deep(&err, "signal_payload");

    let payload = nested_object(MAX_PAYLOAD_DEPTH);
    rt.send_signal_by_workflow_id("echo_signal", "wf-obj", "go", payload.clone(), None)
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if *v == payload));
}

#[tokio::test]
async fn memo_at_the_cap_is_accepted() {
    let mut rt = runtime();
    let mut options = StartOptions::new();
    options.memo = Some(nested(MAX_PAYLOAD_DEPTH));
    let outcome = rt
        .start_workflow_with_options("echo_input", json!(0), options)
        .unwrap();
    let state = rt.run_until_blocked(outcome.exec_id).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)));
}

#[tokio::test]
async fn update_input_obeys_the_cap_and_the_run_stays_live() {
    let mut rt = runtime();
    let exec = rt.start_workflow("echo_signal", json!(0)).unwrap();
    rt.run_until_idle().await.unwrap();

    let err = rt
        .update(exec, "echo", nested(MAX_PAYLOAD_DEPTH + 1))
        .await
        .unwrap_err();
    assert_too_deep(&err, "update_input");
    rt.run_until_idle().await.unwrap();

    let input = nested(MAX_PAYLOAD_DEPTH);
    let result = rt.update(exec, "echo", input.clone()).await.unwrap();
    assert_eq!(result, input);
    // The history, with the stored update, reads back.
    rt.run_until_idle().await.unwrap();
    rt.send_signal(exec, "go", json!("ok")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v == "ok"));
}

// ── Activity results (issue #2216) ──────────────────────────────────────────

/// Run the `deep` activity. Report its output, or the error it failed with.
#[workflow]
async fn run_deep(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    match ctx
        .execute_activity_raw("deep", json!(null), "default")
        .await
    {
        Ok(output) => Ok(output),
        Err(e) => Ok(json!({ "failed": e.to_string() })),
    }
}

/// The depth of the issue #2216 repro. With the event wrapper, a stored result
/// of this depth passes the 128-level read limit of `serde_json`.
const ISSUE_2216_DEPTH: usize = 126;

/// A runtime whose `deep` activity returns `result`. It allows three attempts
/// and counts the bodies that ran.
fn deep_activity_runtime(result: Value) -> (SqliteRuntime, Arc<AtomicU32>) {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&run_deep_info());
    rt.register_workflow(&echo_input_info());
    let runs = Arc::new(AtomicU32::new(0));
    let counter = Arc::clone(&runs);
    rt.register_activity_raw(
        "deep",
        ActivitySpec::new(3, move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(result.clone())
        }),
    );
    (rt, runs)
}

/// The typed fields of the one `ActivityFailed` event in `history`.
fn activity_failure(history: &[WorkflowEvent]) -> (String, bool, String) {
    let mut failures = history.iter().filter_map(|event| match event {
        WorkflowEvent::ActivityFailed {
            error_type,
            non_retryable,
            error,
            ..
        } => Some((error_type.clone(), *non_retryable, error.clone())),
        _ => None,
    });
    let failure = failures.next().expect("an ActivityFailed event");
    assert!(
        failures.next().is_none(),
        "exactly one ActivityFailed event"
    );
    failure
}

#[tokio::test]
async fn a_deep_activity_result_fails_the_activity_and_the_run_completes() {
    // The repro of issue #2216: this result made every drive fail.
    let (mut rt, runs) = deep_activity_runtime(nested(ISSUE_2216_DEPTH));
    let exec = rt.start_workflow("run_deep", json!(0)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Completed(output) = state else {
        panic!("the workflow handles the failure and completes, got {state:?}");
    };
    assert!(
        output.get("failed").is_some(),
        "the workflow sees a failure: {output}"
    );
    rt.run_until_idle().await.unwrap();

    let history = rt.load_history(exec).unwrap();
    let (error_type, non_retryable, error) = activity_failure(&history);
    assert_eq!(error_type, PAYLOAD_TOO_DEEP_ERROR_TYPE);
    assert!(non_retryable, "a deep result is not retried");
    assert!(
        error.contains("'deep'"),
        "the message names the activity: {error}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "no deep ActivityCompleted is stored"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1, "the body runs once");
    // The audit row holds the failure, so it reads back too.
    let attempts = rt.activity_attempts(exec, "deep").unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(attempts[0].result.is_err());

    // The history replays, and another run on the runtime still completes.
    let report = WorkflowReplayer::new()
        .register_fn("run_deep", run_deep_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the history must replay:\n{report}"
    );
    let other = rt.start_workflow("echo_input", json!(7)).unwrap();
    let state = rt.run_until_blocked(other).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v == 7));
}

#[tokio::test]
async fn an_activity_result_one_level_over_the_cap_fails() {
    for result in [
        nested(MAX_PAYLOAD_DEPTH + 1),
        nested_object(MAX_PAYLOAD_DEPTH + 1),
    ] {
        let (mut rt, _) = deep_activity_runtime(result);
        let exec = rt.start_workflow("run_deep", json!(0)).unwrap();
        rt.run_until_blocked(exec).await.unwrap();
        let (error_type, ..) = activity_failure(&rt.load_history(exec).unwrap());
        assert_eq!(error_type, PAYLOAD_TOO_DEEP_ERROR_TYPE);
    }
}

#[tokio::test]
async fn an_activity_result_at_the_cap_round_trips() {
    let (mut rt, _) = deep_activity_runtime(nested(MAX_PAYLOAD_DEPTH));
    let exec = rt.start_workflow("run_deep", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if *v == nested(MAX_PAYLOAD_DEPTH)),
        "got {state:?}"
    );
    rt.run_until_idle().await.unwrap();
}

#[tokio::test]
async fn an_activity_result_far_past_the_cap_fails() {
    // The depth walk must not recurse on a value it refuses.
    let (mut rt, _) = deep_activity_runtime(nested(FAR_PAST_THE_CAP));
    let exec = rt.start_workflow("run_deep", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let (error_type, ..) = activity_failure(&rt.load_history(exec).unwrap());
    assert_eq!(error_type, PAYLOAD_TOO_DEEP_ERROR_TYPE);
}

/// Counts the bodies of `deep_local` that ran. Only one test runs it.
static DEEP_LOCAL_RUNS: AtomicU32 = AtomicU32::new(0);

/// A local activity that returns a value one level over the cap.
#[activity(local = true, retry = RetryPolicy::fixed(3, Duration::ZERO))]
async fn deep_local(_ctx: &ActivityContext, _n: i64) -> Result<Value, String> {
    DEEP_LOCAL_RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(nested(MAX_PAYLOAD_DEPTH + 1))
}

/// A local activity that returns a value at the cap.
#[activity(local = true)]
async fn capped_local(_ctx: &ActivityContext, _n: i64) -> Result<Value, String> {
    Ok(nested(MAX_PAYLOAD_DEPTH))
}

/// Run the local activity that `name` selects. Report its output, or the
/// error it failed with.
#[workflow]
async fn run_local(ctx: &WorkflowContext, name: String) -> Result<Value, String> {
    let info = if name == "deep" {
        deep_local_info()
    } else {
        capped_local_info()
    };
    match ctx.execute_local_activity::<i64, Value>(&info, 0).await {
        Ok(output) => Ok(output),
        Err(e) => Ok(json!({ "failed": e.to_string() })),
    }
}

fn local_runtime() -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&run_local_info());
    rt.register_activity(&deep_local_info());
    rt.register_activity(&capped_local_info());
    rt
}

#[tokio::test]
async fn a_deep_local_activity_result_fails_without_a_retry() {
    let mut rt = local_runtime();
    let exec = rt.start_workflow("run_local", json!("deep")).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Completed(output) = state else {
        panic!("the workflow handles the failure and completes, got {state:?}");
    };
    let failed = output["failed"]
        .as_str()
        .expect("the workflow sees a failure");
    assert!(
        failed.contains("activity 'deep_local' result nests more than 100 levels"),
        "got {failed}"
    );
    rt.run_until_idle().await.unwrap();

    let history = rt.load_history(exec).unwrap();
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityCompleted { .. })),
        "no deep LocalActivityCompleted is stored"
    );
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityExhausted { attempt: 1, .. })),
        "the first attempt ends the activity"
    );
    assert_eq!(
        DEEP_LOCAL_RUNS.load(Ordering::SeqCst),
        1,
        "the body runs once"
    );
}

#[tokio::test]
async fn a_local_activity_result_at_the_cap_round_trips() {
    let mut rt = local_runtime();
    let exec = rt.start_workflow("run_local", json!("capped")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if *v == nested(MAX_PAYLOAD_DEPTH)),
        "got {state:?}"
    );
    rt.run_until_idle().await.unwrap();
}

// ── Heartbeat details (issue #2216) ─────────────────────────────────────────

/// The first attempt saves a good checkpoint, then one past the read limit of
/// `serde_json`, and fails. The retry returns the checkpoint it resumes from.
/// The good checkpoint is two levels under the cap, so the result is at the cap.
#[activity(retry = RetryPolicy::fixed(2, Duration::ZERO))]
async fn deep_heartbeat(ctx: &ActivityContext, _n: i64) -> Result<Value, String> {
    let resumed: Option<Value> = ctx.heartbeat_details().map_err(|e| e.to_string())?;
    if ctx.attempt() == 1 {
        ctx.heartbeat(json!({ "good": nested(MAX_PAYLOAD_DEPTH - 2) }))
            .await
            .map_err(|e| e.to_string())?;
        ctx.heartbeat(nested(FAR_PAST_THE_CAP))
            .await
            .map_err(|e| e.to_string())?;
        return Err("first attempt".to_string());
    }
    Ok(json!({ "resumed": resumed }))
}

#[workflow]
async fn run_deep_heartbeat(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity(&deep_heartbeat_info(), n)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn a_deep_heartbeat_is_dropped_and_the_last_good_checkpoint_survives() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&run_deep_heartbeat_info());
    rt.register_activity(&deep_heartbeat_info());
    let exec = rt.start_workflow("run_deep_heartbeat", json!(0)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(
            state,
            RunState::Completed(ref v)
                if v["resumed"] == json!({ "good": nested(MAX_PAYLOAD_DEPTH - 2) })
        ),
        "the retry resumes from the last good checkpoint, got {state:?}"
    );
    rt.run_until_idle().await.unwrap();
}
