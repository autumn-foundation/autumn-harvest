//! Payload-size cap enforcement at the ingress boundaries (issue #252, Codex
//! #1069 P2: `runtime.rs:264` workflow input, `runtime.rs:346` signal payload,
//! `worker.rs:340` activity result).
//!
//! The Postgres path enforces payload caps at every boundary; this backend must
//! mirror it so it never stores an unbounded history row or accepts a payload
//! other backends reject. These tests drive the PUBLIC API:
//!
//! - **workflow input (START)** — `start_workflow` rejects an input over
//!   `DEFAULT_MAX_WORKFLOW_INPUT_BYTES` (2 MiB) with `PayloadTooLarge` BEFORE any
//!   execution row / `WorkflowStarted` event is written.
//! - **signal payload (ingress)** — `send_signal` rejects a payload over
//!   `DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES` (256 KiB) at ingress, AFTER the
//!   not-found / terminal-target checks, so no oversized `SignalReceived` is
//!   recorded.
//! - **activity result** — an oversized SUCCESSFUL activity result is normalized
//!   into a non-retryable `PayloadTooLarge` `ActivityFailed` (the workflow
//!   observes the failure) rather than stored as an unbounded `ActivityCompleted`.

// The `#[workflow]` macro references the workflow's input parameter, so a
// deliberately-unused param reads as a "used underscore binding".
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::unused_async)]

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteError, SqliteRuntime, WorkflowEvent};
use serde_json::json;

// Serialized-JSON byte lengths matching the core caps (`serde_json::to_string`).
const OVER_WORKFLOW_INPUT: usize = 2 * 1024 * 1024 + 1; // > 2 MiB
const OVER_SIGNAL_PAYLOAD: usize = 256 * 1024 + 1; // > 256 KiB

// ── Test workflows ─────────────────────────────────────────────────────────────

/// Echo the integer input straight through (no activities/signals).
#[workflow]
async fn echo_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let _ = ctx;
    Ok(n)
}

/// Wait for the `go` signal, then echo its payload.
#[workflow]
async fn wait_then_echo(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Run one activity ("blob") and return its output.
#[workflow]
async fn one_activity(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    ctx.execute_activity_raw("blob", json!(null), "default")
        .await
        .map_err(|e| e.to_string())
}

// ── FIX A: workflow input cap on start ─────────────────────────────────────────

#[tokio::test]
async fn oversized_workflow_input_rejected_before_persisting() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&echo_wf_info());

    let big = json!("x".repeat(OVER_WORKFLOW_INPUT));
    let err = rt.start_workflow("echo_wf", big).unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::PayloadTooLarge {
                kind: "workflow_input",
                ..
            }
        ),
        "oversized start input must be rejected with PayloadTooLarge, got: {err:?}"
    );

    // The runtime is not wedged and nothing was persisted: a VALID start still
    // works and drives to completion. (The rejection returns BEFORE the insert
    // transaction is even opened, so no execution row / WorkflowStarted exists —
    // the in-crate `runtime.rs` unit test asserts the row count is 0 directly.)
    let exec = rt.start_workflow("echo_wf", json!(7)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(7)),
        "a valid start after a rejected one still completes: {state:?}"
    );
}

#[tokio::test]
async fn under_cap_workflow_input_starts_normally() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&echo_wf_info());
    // A comfortably-under-cap input starts and runs unchanged.
    let exec = rt.start_workflow("echo_wf", json!(42)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(42)));
}

// ── FIX C: signal payload cap at ingress ───────────────────────────────────────

#[tokio::test]
async fn oversized_signal_payload_rejected_at_ingress() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&wait_then_echo_info());
    let exec = rt.start_workflow("wait_then_echo", json!(0)).unwrap();
    // Park it on the signal wait.
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(ref s) if s == "go"));

    let big = json!({ "blob": "y".repeat(OVER_SIGNAL_PAYLOAD) });
    let err = rt.send_signal(exec, "go", big).unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::PayloadTooLarge {
                kind: "signal_payload",
                ..
            }
        ),
        "oversized signal payload must be rejected with PayloadTooLarge, got: {err:?}"
    );

    // The oversized signal did NOT stage (still waiting), and a valid signal is
    // delivered normally — proving no oversized SignalReceived was recorded.
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "go"),
        "still parked on the wait — the rejected signal never staged: {state:?}"
    );
    rt.send_signal(exec, "go", json!({ "ok": true })).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({ "ok": true })),
        "a valid signal after a rejected one is delivered: {state:?}"
    );
}

#[tokio::test]
async fn signal_cap_yields_to_not_found_and_terminal_checks() {
    // The prompt's chosen order: the target's existence/state is validated FIRST,
    // so an oversized payload to an unknown or terminal target reports THAT (the
    // actionable error) rather than a size complaint.
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&echo_wf_info());

    let big = json!("z".repeat(OVER_SIGNAL_PAYLOAD));

    // Unknown id → ExecutionNotFound (not PayloadTooLarge).
    let unknown = ExecutionId::new();
    let err = rt.send_signal(unknown, "go", big.clone()).unwrap_err();
    assert!(
        matches!(err, SqliteError::ExecutionNotFound(_)),
        "unknown target beats the size check: {err:?}"
    );

    // Terminal target → WorkflowNotRunning (not PayloadTooLarge).
    let exec = rt.start_workflow("echo_wf", json!(1)).unwrap();
    let _ = rt.run_until_blocked(exec).await.unwrap(); // drives to COMPLETED
    let err = rt.send_signal(exec, "go", big).unwrap_err();
    assert!(
        matches!(err, SqliteError::WorkflowNotRunning { .. }),
        "terminal target beats the size check: {err:?}"
    );
}

#[tokio::test]
async fn under_cap_signal_delivered_normally() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&wait_then_echo_info());
    let exec = rt.start_workflow("wait_then_echo", json!(0)).unwrap();
    let _ = rt.run_until_blocked(exec).await.unwrap();
    rt.send_signal(exec, "go", json!({ "small": true }))
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v == &json!({ "small": true })));
}

// ── FIX B: activity result cap (end-to-end via a real workflow) ────────────────

#[tokio::test]
async fn oversized_activity_result_normalized_to_payload_too_large_failure() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&one_activity_info());
    // The activity body returns a value whose serialized length exceeds the 2 MiB
    // result cap.
    rt.register_activity_raw(
        "blob",
        ActivitySpec::new(1, |_input| Ok(json!("q".repeat(2 * 1024 * 1024 + 1)))),
    );

    let exec = rt.start_workflow("one_activity", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    // The workflow observes the activity FAILURE (not a success), so it fails.
    assert!(
        matches!(state, RunState::Failed(_)),
        "an oversized activity result surfaces as an activity failure: {state:?}"
    );

    // The recorded history carries a NON-RETRYABLE PayloadTooLarge ActivityFailed —
    // NOT an (unbounded) ActivityCompleted — and no wire envelope leaks.
    let history = rt.load_history(exec).unwrap();
    let failed = history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::ActivityFailed {
                error_type,
                non_retryable,
                error,
                ..
            } => Some((error_type.clone(), *non_retryable, error.clone())),
            _ => None,
        })
        .expect("an ActivityFailed event must be recorded");
    assert_eq!(failed.0, "PayloadTooLarge");
    assert!(failed.1, "the result-cap failure is non-retryable");
    assert!(
        failed.2.contains("result exceeds cap"),
        "human message names the boundary: {}",
        failed.2
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "no unbounded ActivityCompleted must be stored"
    );
}

#[tokio::test]
async fn under_cap_activity_result_completes_normally() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&one_activity_info());
    rt.register_activity_raw(
        "blob",
        ActivitySpec::new(1, |_input| Ok(json!({ "small": "result" }))),
    );
    let exec = rt.start_workflow("one_activity", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({ "small": "result" })),
        "an under-cap activity result completes normally: {state:?}"
    );
}
