//! FIX 2 (Codex #1069 P2, `runtime.rs:715`): the `SQLite` backend HONORS an activity's
//! `start_to_close` timeout, and closes the dropped-`ScheduleActivity`-field class.
//!
//! The core `ScheduleActivity` command carries `start_to_close_override` (from both
//! `execute_activity_with_opts` and `ActivityInfo::default_start_to_close`). Pre-fix
//! this backend dropped it, so a slow registered body ran to completion and recorded
//! `ActivityCompleted` instead of `ActivityTimedOut`. The backend now persists the
//! budget on the task row and the worker enforces it as a **post-execution outcome**:
//! a synchronous body cannot be cancelled mid-flight in a single-writer runtime, but a
//! body whose real wall-clock runtime exceeds its budget records a terminal
//! `ActivityTimedOut { StartToClose }` — byte-equivalent to the durable event the
//! Postgres timeout scanner (`enforce_activity_timeout`) records. Terminal, no retry
//! (the workflow observes `HarvestError::Timeout` and drives its own timeout branch).
//!
//! The session fields (`session_id`/`session_worker_id`, issue #606) and the
//! session-acquire-only `schedule_to_start_override` are OUTSIDE the single-writer
//! subset and are now REJECTED LOUDLY rather than silently dropped — so a workflow
//! using `ctx.create_session(...)` fails with a clear `Unsupported` error instead of
//! running its activities un-pinned.

// The `#[workflow]`/`#[activity]` macros reference their input params, so a
// deliberately-unused param reads as a "used underscore binding" through expansion.
#![allow(clippy::used_underscore_binding)]
// The `work` activity fixture (and the terminal-only workflows) have trivial,
// await-free bodies — the crate runs a registered `ActivitySpec`, not the `#[activity]`
// body, and a terminal-failure workflow returns immediately.
#![allow(clippy::unused_async)]

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{HarvestError, SessionOptions};
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteError, SqliteRuntime};
use serde_json::json;

// A remote activity carrying a GENEROUS default start-to-close (30s). Its `_info()`
// companion supplies the `ActivityInfo` (name + `default_start_to_close`); the actual
// body is the registered `ActivitySpec` (the crate's execution model). The macro fn
// body itself is never run by this backend.
#[activity(start_to_close = "30s")]
async fn work(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

// Times out: a per-call 1ms start-to-close override against a registered body that
// sleeps ~40ms. Catches the timeout so the run COMPLETES down the timeout branch,
// making the outcome unambiguous.
#[workflow]
async fn timed_activity_wf(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx
        .execute_activity_with_opts::<_, i64>(
            &work_info(),
            n,
            None,
            None,
            Some(Duration::from_millis(1)),
        )
        .await
    {
        Ok(_) => Ok("completed".to_string()),
        Err(HarvestError::Timeout { .. }) => Ok("timed_out".to_string()),
        Err(other) => Err(other.to_string()),
    }
}

// Control: the GENEROUS default start-to-close (30s, via `ActivityInfo`) flows through
// and a fast body is NOT falsely timed out — it records `ActivityCompleted`.
#[workflow]
async fn default_timeout_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&work_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

// A slow body (~40ms) — exceeds a 1ms budget. Uses a real (blocking) sleep because
// the crate's activity body is a synchronous closure.
fn slow_body() -> ActivitySpec {
    ActivitySpec::new(1, |input: serde_json::Value| {
        std::thread::sleep(Duration::from_millis(40));
        Ok(json!(input.as_i64().unwrap_or_default()))
    })
}

// A fast body — completes well under any budget.
fn fast_body() -> ActivitySpec {
    ActivitySpec::new(1, |input: serde_json::Value| {
        Ok(json!(input.as_i64().unwrap_or_default()))
    })
}

// ── A body exceeding its per-call start-to-close records ActivityTimedOut ───────
//
// RED pre-fix: the budget was dropped, the ~40ms body ran to completion, and history
// recorded `ActivityCompleted` (never `ActivityTimedOut`).
#[tokio::test]
async fn slow_activity_records_activity_timed_out_not_completed() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&timed_activity_wf_info());
    rt.register_activity_raw("work", slow_body());
    let exec = rt.start_workflow("timed_activity_wf", json!(7)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    // The workflow drove its timeout branch (proves it observed HarvestError::Timeout).
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "timed_out"),
        "the workflow must observe the activity timeout, got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: autumn_harvest::TimeoutType::StartToClose,
                ..
            }
        )),
        "history must record a terminal ActivityTimedOut {{ StartToClose }}:\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "an over-budget body must NOT record ActivityCompleted (RED pre-fix):\n{history:?}"
    );

    // AC5: the timed-out history replays byte-identically on the core engine — the
    // handler re-observes the recorded ActivityTimedOut and drives the same branch.
    let report = WorkflowReplayer::new()
        .register_fn("timed_activity_wf", timed_activity_wf_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "an ActivityTimedOut history must replay cleanly on the core:\n{report}"
    );
}

// ── The ActivityInfo default start-to-close is honored (persisted) AND a fast body
//    under a generous budget is not falsely timed out ─────────────────────────────
#[tokio::test]
async fn fast_activity_under_generous_default_completes_normally() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&default_timeout_wf_info());
    rt.register_activity_raw("work", fast_body());
    let exec = rt.start_workflow("default_timeout_wf", json!(9)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(9)),
        "a fast body under a 30s default budget must complete normally, got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "a fast, in-budget body records ActivityCompleted:\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. })),
        "a fast, in-budget body must NOT be falsely timed out:\n{history:?}"
    );
}

// ── Dropped-field class: worker-session commands are rejected LOUDLY ─────────────
//
// A workflow that calls `ctx.create_session(...)` dispatches the internal
// session-acquire activity, whose `ScheduleActivity` carries the session-only
// `schedule_to_start_override` (the acquisition timeout). That field is outside the
// single-writer subset; the backend now rejects it with `Unsupported` rather than
// silently dropping it and running the session activity un-pinned.
#[workflow]
async fn uses_session(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    let session = ctx
        .create_session(SessionOptions::new("gpu"))
        .await
        .map_err(|e| e.to_string())?;
    session
        .execute_activity_raw("work", json!(1), "gpu")
        .await
        .map_err(|e| e.to_string())?;
    session.complete().await.map_err(|e| e.to_string())?;
    Ok(())
}

#[tokio::test]
async fn worker_session_command_is_rejected_loudly_not_silently_dropped() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&uses_session_info());
    rt.register_activity_raw("work", fast_body());
    let exec = rt.start_workflow("uses_session", json!(0)).unwrap();

    let err = rt
        .run_until_blocked(exec)
        .await
        .expect_err("a worker-session command must be rejected, not silently dropped");
    match err {
        SqliteError::Unsupported(msg) => {
            assert!(
                msg.contains("session"),
                "the rejection must name the unsupported session field, got: {msg}"
            );
        }
        other => panic!("expected SqliteError::Unsupported, got {other:?}"),
    }
}
