//! Regression tests for issue #1530: one execution reaching a dynamically
//! unsupported command must not head-of-line-block unrelated executions in
//! `poll_once`/`run_until_idle`.
//!
//! `store::running_executions` drives executions in ascending `ExecutionId`
//! string order. Before the fix, `poll_once`'s loop propagated the first
//! per-execution error with a bare `?`. Every execution ordered after the
//! broken one in that pass was never driven. Not once, forever, even across
//! a restart. The fix keeps driving the rest of the fleet. It still reports
//! the first error, matching the existing single-error `poll_once` signature.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]
// `healthy_wf` holds no `.await` — it completes in one synchronous cycle by
// design, so it lands past the broken execution in the very same pass.
#![allow(clippy::unused_async)]

use std::sync::atomic::{AtomicU32, Ordering};

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ExecutionOutcome, SqliteError, SqliteRuntime};
use chrono::Utc;
use serde_json::json;

// Reaches `WorkflowCommand::SignalExternalWorkflow` through ordinary dynamic
// control flow — not a statically-declared `#[workflow(...)]` feature, so
// `register_workflow`'s static audit cannot catch it up front.
#[workflow]
async fn broken_wf(ctx: &WorkflowContext, target: ExecutionId) -> Result<(), String> {
    ctx.signal_external_workflow(target, "x", ())
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn healthy_wf(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n * 2)
}

/// A second, dedicated broken workflow with its own attempt counter. A
/// distinct type keeps the counter test isolated from `broken_wf`'s use in
/// the other tests in this file, which run concurrently in the same binary.
static COUNTING_BROKEN_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

#[workflow]
async fn counting_broken_wf(ctx: &WorkflowContext, target: ExecutionId) -> Result<(), String> {
    COUNTING_BROKEN_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    ctx.signal_external_workflow(target, "x", ())
        .await
        .map_err(|e| e.to_string())
}

/// Starts executions of `workflow_name` until one sorts, as an
/// `ExecutionId` string, after `after`. It then lands later in
/// `running_executions`'s ascending scan — the ordering the issue's repro
/// forces (~1-2 tries on average).
fn start_after(
    rt: &mut SqliteRuntime,
    workflow_name: &str,
    input: &serde_json::Value,
    after: ExecutionId,
) -> ExecutionId {
    for _ in 0..50i64 {
        let id = rt.start_workflow(workflow_name, input.clone()).unwrap();
        if id.to_string() > after.to_string() {
            return id;
        }
    }
    panic!("did not land an execution after the target one in 50 tries");
}

fn start_healthy_after(rt: &mut SqliteRuntime, broken: ExecutionId) -> ExecutionId {
    start_after(rt, "healthy_wf", &json!(0), broken)
}

#[tokio::test]
async fn poll_once_drives_the_rest_of_the_fleet_past_an_unsupported_command() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&broken_wf_info());
    rt.register_workflow(&healthy_wf_info());

    let broken = rt.start_workflow("broken_wf", json!(ExecutionId::new())).unwrap();
    let healthy_after = start_healthy_after(&mut rt, broken);

    let err = rt
        .poll_once()
        .await
        .expect_err("the broken execution's command must still be rejected loudly");
    match err {
        SqliteError::Unsupported(msg) => {
            assert!(
                msg.contains("SignalExternalWorkflow"),
                "the surfaced error must name the offending command; got: {msg}"
            );
        }
        other => panic!("expected SqliteError::Unsupported, got {other:?}"),
    }

    // The unrelated, later-sorting execution must have been driven in the
    // SAME poll_once pass, not silently skipped.
    assert!(
        matches!(
            rt.outcome(healthy_after).unwrap(),
            ExecutionOutcome::Completed(_)
        ),
        "an unrelated execution must not be head-of-line-blocked by another \
         execution's unsupported command"
    );
}

#[tokio::test]
async fn run_until_idle_does_not_permanently_stall_executions_after_a_broken_one() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&broken_wf_info());
    rt.register_workflow(&healthy_wf_info());

    let broken = rt.start_workflow("broken_wf", json!(ExecutionId::new())).unwrap();
    let healthy_after = start_healthy_after(&mut rt, broken);

    // Mirrors the issue's repro: an application driving the fleet in a loop,
    // tolerating (and re-surfacing) the broken execution's own error.
    for _ in 1..=3 {
        let _ = rt.run_until_idle().await;
    }

    assert!(
        matches!(
            rt.outcome(healthy_after).unwrap(),
            ExecutionOutcome::Completed(_)
        ),
        "a restart-durable stall on an unrelated execution must not survive the fix"
    );
}

// `poll_once_as_of` is `#[doc(hidden)]`. It is a separate function, not
// shared code, with its own copy of the fleet loop. It needs its own
// coverage of the same fix.
#[tokio::test]
async fn poll_once_as_of_drives_the_rest_of_the_fleet_past_an_unsupported_command() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&broken_wf_info());
    rt.register_workflow(&healthy_wf_info());

    let broken = rt.start_workflow("broken_wf", json!(ExecutionId::new())).unwrap();
    let healthy_after = start_healthy_after(&mut rt, broken);

    let err = rt
        .poll_once_as_of(Utc::now())
        .await
        .expect_err("the broken execution's command must still be rejected loudly");
    assert!(
        matches!(err, SqliteError::Unsupported(_)),
        "expected SqliteError::Unsupported, got {err:?}"
    );
    assert!(
        matches!(
            rt.outcome(healthy_after).unwrap(),
            ExecutionOutcome::Completed(_)
        ),
        "poll_once_as_of must also drive the rest of the fleet past a broken execution"
    );
}

/// Two broken executions in one pass, not just one. `poll_once` (issue
/// #1530) must attempt every execution and report only the first error.
/// The attempt counter proves the second broken execution still ran, even
/// though its own error never surfaces.
#[tokio::test]
async fn poll_once_still_attempts_every_broken_execution_in_one_pass() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&counting_broken_wf_info());
    let attempts_before = COUNTING_BROKEN_ATTEMPTS.load(Ordering::SeqCst);

    let first = rt
        .start_workflow("counting_broken_wf", json!(ExecutionId::new()))
        .unwrap();
    let second = start_after(
        &mut rt,
        "counting_broken_wf",
        &json!(ExecutionId::new()),
        first,
    );

    let err = rt
        .poll_once()
        .await
        .expect_err("the first-sorting broken execution must still be rejected");
    assert!(
        matches!(err, SqliteError::Unsupported(_)),
        "expected SqliteError::Unsupported, got {err:?}"
    );

    // `start_after` may discard a few ordering-probe starts before `second`
    // lands past `first`. Each discarded start is itself a real, persisted
    // `counting_broken_wf` execution. The true count is >= 2, not == 2.
    assert!(
        COUNTING_BROKEN_ATTEMPTS.load(Ordering::SeqCst) - attempts_before >= 2,
        "both broken executions must be driven in the same pass, not just the first"
    );
    assert!(matches!(
        rt.outcome(second).unwrap(),
        ExecutionOutcome::Running
    ));
}
