//! Registration-time honor/rejection of unsupported `ActivityInfo`-level features —
//! the THIRD author-declared-feature ingress surface (Codex #1069 P2, `runtime.rs:39`).
//!
//! After the `ScheduleActivity` **command** fields (`retry_policy_override` /
//! `start_to_close_override`, honored) and the **`WorkflowInfo`** fields (rejected at
//! registration), a `#[activity(...)]` can still declare *dispatch-time defaults the
//! Postgres worker enforces from `ActivityInfo` but which are NOT copied into the
//! `ScheduleActivity` command* — most notably `default_schedule_to_close` (the total,
//! cross-retry deadline, issue #378). A name-only body registration never saw them, so
//! they were neither honored nor rejected: an activity with a total deadline and a
//! retry policy could keep backing off/retrying PAST the declared deadline and
//! eventually complete instead of recording `ActivityTimedOut`.
//!
//! The audited, `ActivityInfo`-driven
//! [`SqliteRuntime::register_activity`](autumn_harvest_sqlite::SqliteRuntime::register_activity)
//! closes that gap: it HONORS `default_schedule_to_close` (persisted + enforced) and
//! REJECTS the dispatch-admission / cross-worker fields LOUDLY at registration (a
//! setup-time panic naming the feature). These tests drive the PUBLIC API; the
//! `unsupported_activity_feature` audit fn has exhaustive in-crate unit coverage.

// The `#[workflow]`/`#[activity]` macros reference their input params, so a
// deliberately-unused param reads as a "used underscore binding" through expansion.
#![allow(clippy::used_underscore_binding)]
// The `#[activity]` fixtures have trivial await-free bodies — the crate runs a
// registered SYNCHRONOUS closure, not the macro's async body.
#![allow(clippy::unused_async)]

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{HarvestError, TimeoutType};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use serde_json::json;

// ── HONORED: default_schedule_to_close (the finding's headline, issue #378) ────────
//
// The total deadline is 1s, but the retry back-off is 30s — so on the FIRST failure
// the next attempt would land 30s out, well past the 1s total deadline. The worker
// therefore seals the activity terminal `ActivityTimedOut { ScheduleToClose }` instead
// of requeuing — the run converges in one `run_until_blocked` call with no clock
// manipulation. `default_schedule_to_close` is NOT carried on the ScheduleActivity
// command; it is resolved by name from the registry at dispatch (exactly as the
// Postgres worker resolves ActivityInfo from its HandlerRegistry).
#[activity(schedule_to_close = "1s", retry = RetryPolicy::fixed(5, Duration::from_secs(30)))]
async fn always_fails(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn schedule_to_close_wf(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx
        .execute_activity::<_, i64>(&always_fails_info(), n)
        .await
    {
        Ok(_) => Ok("completed".to_string()),
        // A schedule-to-close timeout surfaces (like start-to-close) as
        // HarvestError::Timeout — the workflow drives its own timeout branch.
        Err(HarvestError::Timeout { .. }) => Ok("sched_timed_out".to_string()),
        Err(other) => Err(other.to_string()),
    }
}

// RED pre-fix: `default_schedule_to_close` was never seen by the backend, so the body
// kept being retried (`RetryPolicy::fixed(5, ..)`) and eventually recorded
// `ActivityFailed` after 5 attempts — never a terminal `ActivityTimedOut`, and a
// slow-but-eventually-succeeding body would have recorded `ActivityCompleted` past the
// declared total deadline.
#[tokio::test]
async fn schedule_to_close_deadline_records_activity_timed_out_not_retry_or_complete() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&schedule_to_close_wf_info());
    // The audited, info-based registration — HONORS the declared schedule_to_close.
    rt.register_activity(&always_fails_info(), |_input| Err("boom".to_string()));
    let exec = rt.start_workflow("schedule_to_close_wf", json!(7)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    // The workflow drove its timeout branch (proves it observed HarvestError::Timeout).
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "sched_timed_out"),
        "the workflow must observe the schedule-to-close timeout, got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::ScheduleToClose,
                ..
            }
        )),
        "history must record a terminal ActivityTimedOut {{ ScheduleToClose }}:\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "an activity past its total deadline must NOT complete (RED pre-fix):\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "a schedule-to-close timeout is a TimedOut terminal, not a retry-exhaustion \
         ActivityFailed (RED pre-fix would retry to exhaustion):\n{history:?}"
    );

    // The timed-out history replays byte-identically on the core engine — the handler
    // re-observes the recorded ActivityTimedOut and drives the same timeout branch.
    let report = WorkflowReplayer::new()
        .register_fn("schedule_to_close_wf", schedule_to_close_wf_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a ScheduleToClose history must replay cleanly on the core:\n{report}"
    );
}

// ── Control: a generous schedule_to_close does NOT false-positive ──────────────────
#[activity(schedule_to_close = "300s", retry = RetryPolicy::fixed(3, Duration::from_secs(1)))]
async fn under_deadline(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn under_deadline_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&under_deadline_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

#[tokio::test]
async fn a_successful_body_under_a_generous_deadline_completes_normally() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&under_deadline_wf_info());
    rt.register_activity(&under_deadline_info(), Ok);
    let exec = rt.start_workflow("under_deadline_wf", json!(9)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(9)),
        "a body well under its 300s total deadline must complete normally, got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "an in-deadline body records ActivityCompleted:\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. })),
        "an in-deadline body must NOT be falsely timed out:\n{history:?}"
    );
}

// ── REJECTED at registration (setup-time panic, naming the feature) ────────────────

#[activity(heartbeat_timeout = "10s")]
async fn heartbeating(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[test]
#[should_panic(expected = "heartbeat_timeout")]
fn registering_an_activity_with_heartbeat_timeout_panics_naming_the_feature() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    // No heartbeating in the synchronous inline drain — reject at setup rather than
    // silently running a body that can never observe the declared heartbeat timeout.
    rt.register_activity(&heartbeating_info(), Ok);
}

#[activity(local = true)]
async fn local_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[test]
#[should_panic(expected = "local = true")]
fn registering_a_local_activity_panics_naming_the_feature() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    // Local activities dispatch via RunLocalActivity, a command-layer non-goal here.
    rt.register_activity(&local_act_info(), Ok);
}

// ── No regression: a plain activity (only body + honored start_to_close/retry) ─────

#[activity(start_to_close = "30s", retry = RetryPolicy::fixed(3, Duration::from_secs(1)))]
async fn plain_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn plain_act_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&plain_act_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

#[tokio::test]
async fn a_plain_activity_registers_and_runs_unchanged() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&plain_act_wf_info());
    // Only honored fields (start_to_close via command, retry): registers + runs.
    rt.register_activity(&plain_act_info(), Ok);
    let exec = rt.start_workflow("plain_act_wf", json!(21)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(21)),
        "a plain, only-honored-defaults activity must run to completion, got {state:?}"
    );
}
