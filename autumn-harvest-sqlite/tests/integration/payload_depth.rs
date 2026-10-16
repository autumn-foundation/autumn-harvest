//! Payload-depth cap at the ingress boundaries (issue #2157).
//!
//! `serde_json` writes any depth but refuses to read past 128 levels.
//! A payload nested too deep used to be stored, and then every drive failed
//! with "corrupt stored value". These tests drive the public API and check
//! that ingress refuses such a payload before it stores anything.

#![allow(clippy::used_underscore_binding)]
#![allow(clippy::unused_async)]

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{
    MAX_PAYLOAD_DEPTH, RunState, SignalWithStart, SqliteError, SqliteRuntime, StartOptions,
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

#[test]
fn a_very_deep_value_is_rejected_without_overflowing_the_stack() {
    let mut rt = runtime();
    // Deep enough that a recursive walk would overflow the call stack.
    let deep = nested(10_000);
    let err = rt.start_workflow("echo_input", deep).unwrap_err();
    assert_too_deep(&err, "workflow_input");
}

#[tokio::test]
async fn a_very_deep_promise_signal_is_rejected_without_overflowing_the_stack() {
    let mut rt = runtime();
    let exec = rt.start_workflow("echo_signal", json!(0)).unwrap();
    rt.run_until_idle().await.unwrap();
    // Promise signals are decoded before the generic payload checks run.
    let name = format!(
        "{}x",
        autumn_harvest::durable_promise::PROMISE_SIGNAL_PREFIX
    );
    let err = rt.send_signal(exec, &name, nested(10_000)).unwrap_err();
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
