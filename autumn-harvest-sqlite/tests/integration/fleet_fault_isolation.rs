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
//!
//! Also covers the residual issue #1555 gap. `run_until_idle`'s outer loop
//! propagated `poll_once`'s error with a bare `?` too. It stopped after ONE
//! internal pass whenever a broken execution was present, not the documented
//! "repeat until quiescent". A multi-cycle execution then needed several
//! external `run_until_idle()` calls to finish, instead of one.
//!
//! A follow-up review of the #1555 fix (Codex P1) found that a naive
//! "keep looping" fix re-drives a broken execution on EVERY internal pass.
//! For a WORKFLOW-handler panic, each re-drive strikes the bounded panic
//! budget (`WORKFLOW_PANIC_MAX_ATTEMPTS`), so a persistently-panicking
//! execution could exhaust its budget and get sealed `FAILED` within ONE
//! external call, instead of one strike per call. The fix skips a
//! newly-erroring execution for the rest of that `run_until_idle` call.

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

/// Needs TWO sequential decision cycles, unlike `healthy_wf`'s one. One
/// `run_until_idle()` pass is indistinguishable from full convergence for a
/// one-cycle workflow. That is why #1536's own regression test missed the
/// #1555 gap.
#[workflow]
async fn two_step_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let a = ctx
        .execute_activity_raw("increment", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    let b = ctx
        .execute_activity_raw("increment", a, "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(b.as_i64().unwrap_or_default())
}

fn increment_activity() -> autumn_harvest_sqlite::ActivitySpec {
    autumn_harvest_sqlite::ActivitySpec::new(1, |input: serde_json::Value| {
        Ok(json!(input.as_i64().unwrap_or_default() + 1))
    })
}

/// Errors on a LATER pass, not the first. Cycle 1 schedules a real activity
/// (progress, no error). Only once that activity resolves does cycle 2 reach
/// `RequestCancelExternalWorkflow`, a DIFFERENT unsupported command than
/// `broken_wf`'s `SignalExternalWorkflow`.
#[workflow]
async fn broken_after_one_cycle_wf(
    ctx: &WorkflowContext,
    target: ExecutionId,
) -> Result<(), String> {
    let _ = ctx
        .execute_activity_raw("increment", json!(0), "default")
        .await
        .map_err(|e| e.to_string())?;
    ctx.request_cancel_external_workflow(target)
        .await
        .map_err(|e| e.to_string())
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

    let broken = rt
        .start_workflow("broken_wf", json!(ExecutionId::new()))
        .unwrap();
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

    let broken = rt
        .start_workflow("broken_wf", json!(ExecutionId::new()))
        .unwrap();
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

    let broken = rt
        .start_workflow("broken_wf", json!(ExecutionId::new()))
        .unwrap();
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

/// Issue #1555's repro. A lone persistently-broken execution must not cap
/// `run_until_idle`'s convergence at one decision cycle per call. A single
/// call must drive a multi-cycle, unrelated execution all the way to
/// completion. It must still report the broken execution's error.
#[tokio::test]
async fn run_until_idle_converges_a_multi_cycle_execution_in_one_call_past_a_broken_one() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&broken_wf_info());
    rt.register_workflow(&two_step_wf_info());
    rt.register_activity_raw("increment", increment_activity());

    let broken = rt
        .start_workflow("broken_wf", json!(ExecutionId::new()))
        .unwrap();
    let healthy_after = start_after(&mut rt, "two_step_wf", &json!(10), broken);

    let err = rt
        .run_until_idle()
        .await
        .expect_err("the broken execution's error must still surface, not be swallowed");
    assert!(
        matches!(err, SqliteError::Unsupported(_)),
        "expected SqliteError::Unsupported, got {err:?}"
    );

    assert!(
        matches!(
            rt.outcome(healthy_after).unwrap(),
            ExecutionOutcome::Completed(ref v) if v.as_i64() == Some(12)
        ),
        "a single run_until_idle() call must drive a multi-cycle unrelated \
         execution to completion, not just one decision cycle"
    );
}

/// `run_until_idle`'s outer loop keeps only the FIRST error seen across ALL
/// passes (`first_error.get_or_insert`), not the last pass's error. Pass 1
/// has only `broken_wf`'s error. Pass 2 adds a SECOND, differently-named
/// error once `broken_after_one_cycle_wf`'s activity resolves. The returned
/// error must still be pass 1's.
#[tokio::test]
async fn run_until_idle_keeps_the_first_error_seen_across_passes_not_a_later_one() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&broken_wf_info());
    rt.register_workflow(&broken_after_one_cycle_wf_info());
    rt.register_activity_raw("increment", increment_activity());

    rt.start_workflow("broken_wf", json!(ExecutionId::new()))
        .unwrap();
    rt.start_workflow("broken_after_one_cycle_wf", json!(ExecutionId::new()))
        .unwrap();

    let err = rt
        .run_until_idle()
        .await
        .expect_err("a fleet with no progressable work left must still report an error");
    match err {
        SqliteError::Unsupported(msg) => {
            assert!(
                msg.contains("SignalExternalWorkflow"),
                "run_until_idle must keep pass 1's error (broken_wf's), not \
                 pass 2's different error (RequestCancelExternalWorkflow); \
                 got: {msg}"
            );
        }
        other => panic!("expected SqliteError::Unsupported, got {other:?}"),
    }
}

/// Panics on EVERY decision cycle. Distinct from `broken_wf`: a panic is
/// contained under a bounded budget (`WORKFLOW_PANIC_MAX_ATTEMPTS`) rather
/// than rejected outright, so re-driving it repeatedly has an observable
/// side effect an unsupported command does not.
#[workflow]
async fn always_panics_wf(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _ = ctx;
    panic!("workflow boom");
}

/// Needs FOUR sequential decision cycles, more than the default panic
/// budget of three. It keeps `run_until_idle`'s internal loop running long
/// enough to re-strike a co-located panicking execution, if the fix did not
/// skip it after its first strike.
#[workflow]
async fn four_step_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let a = ctx
        .execute_activity_raw("increment", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    let b = ctx
        .execute_activity_raw("increment", a, "default")
        .await
        .map_err(|e| e.to_string())?;
    let c = ctx
        .execute_activity_raw("increment", b, "default")
        .await
        .map_err(|e| e.to_string())?;
    let d = ctx
        .execute_activity_raw("increment", c, "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(d.as_i64().unwrap_or_default())
}

/// Codex P1 follow-up on the #1555 fix. A single `run_until_idle()` call
/// must strike a persistently-panicking execution AT MOST ONCE, even while
/// an unrelated execution needs many more internal passes to converge.
/// Re-striking it every pass would exhaust `WORKFLOW_PANIC_MAX_ATTEMPTS`
/// and seal it `FAILED` within one call, denying the caller the chance to
/// react to the first `WorkflowPanicked` error between calls.
#[tokio::test]
async fn run_until_idle_strikes_a_panicking_execution_at_most_once_per_call() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&always_panics_wf_info());
    rt.register_workflow(&four_step_wf_info());
    rt.register_activity_raw("increment", increment_activity());

    let panicking = rt.start_workflow("always_panics_wf", json!(0)).unwrap();
    let long_running = rt.start_workflow("four_step_wf", json!(0)).unwrap();

    let err = rt
        .run_until_idle()
        .await
        .expect_err("a persistently-panicking execution must still surface an error");
    assert!(
        matches!(err, SqliteError::WorkflowPanicked { .. }),
        "expected SqliteError::WorkflowPanicked, got {err:?}"
    );

    assert!(
        matches!(rt.outcome(panicking).unwrap(), ExecutionOutcome::Running),
        "one run_until_idle() call must strike a panicking execution AT MOST \
         ONCE and leave it RUNNING, not exhaust its whole panic budget while \
         unrelated work keeps the call's internal loop going"
    );
    assert!(
        matches!(
            rt.outcome(long_running).unwrap(),
            ExecutionOutcome::Completed(ref v) if v.as_i64() == Some(4)
        ),
        "the unrelated multi-cycle execution must still converge fully in \
         the same call"
    );
}
