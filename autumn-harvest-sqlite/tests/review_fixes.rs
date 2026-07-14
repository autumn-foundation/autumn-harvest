//! Regression tests for the code-review findings (issue #1068).
//!
//! - **B1** terminal-cycle bookkeeping (`RecordSideEffect`/`RecordMarker` emitted
//!   in the cycle a workflow completes) is persisted, so the history replays on
//!   the core `WorkflowReplayer`.
//! - **B2** a benign `SetCurrentDetails` does not wedge an in-subset workflow.
//! - **B3** retry-exhaustion / terminal-failure paths.
//! - **B4** an unsupported command mid-batch rolls the whole cycle back.
//! - **timer-id reuse** (Codex round-5): a re-armed timer id fires again.
//! - **m7** multi-execution `run_until_idle`.

// The `#[workflow]` macro references the workflow's input parameter, so a
// deliberately-unused param reads as a "used underscore binding" through the
// expansion.
#![allow(clippy::used_underscore_binding)]
// Some regression workflows deliberately have no suspension (`just_uuid`,
// `direct_fail`) to exercise the terminal-only / direct-error paths — they are
// `async` because `#[workflow]` requires it, but hold no `.await`.
#![allow(clippy::unused_async)]

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{ActivitySpec, ExecutionOutcome, RunState, SqliteError, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

fn temp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

fn echo_activity() -> ActivitySpec {
    ActivitySpec::new(1, |input: serde_json::Value| {
        Ok(json!(input.as_i64().unwrap_or_default()))
    })
}

// ── B1(a): a side effect in a no-suspension terminal cycle is persisted ────────

/// Mints a UUID and returns immediately — there is NO suspension at all, so the
/// `RecordSideEffect` command lives only in the terminal (Completed) drain. Before
/// the fix it was dropped and the history failed the core replayer with
/// `SideEffectDrift`.
#[workflow]
async fn just_uuid(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    let id = ctx.new_uuid().to_string();
    Ok(json!({ "id": id }))
}

#[tokio::test]
async fn terminal_only_side_effect_persists_and_replays() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&just_uuid_info());
    let exec = rt.start_workflow("just_uuid", json!(0)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");

    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. })),
        "the terminal-cycle side effect must be persisted, not dropped:\n{history:?}"
    );

    let report = WorkflowReplayer::new()
        .register_fn("just_uuid", just_uuid_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a terminal-only side effect must replay byte-identically:\n{report}"
    );
}

// ── B1(b): a side effect AFTER the last suspension (terminal cycle) persists ───

/// Runs an activity, then mints a UUID and returns in that same terminal cycle.
/// The side effect is emitted *after* the last suspension point.
#[workflow]
async fn work_then_uuid(ctx: &WorkflowContext, n: i64) -> Result<serde_json::Value, String> {
    let out = ctx
        .execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    let id = ctx.new_uuid().to_string();
    Ok(json!({ "id": id, "doubled": out.as_i64().unwrap_or_default() * 2 }))
}

#[tokio::test]
async fn side_effect_after_last_suspension_persists_and_replays() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&work_then_uuid_info());
    rt.register_activity("work", echo_activity());
    let exec = rt.start_workflow("work_then_uuid", json!(4)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");

    let history = rt.load_history(exec).unwrap();
    // The SideEffectRecorded must sit AFTER the ActivityCompleted and BEFORE the
    // terminal WorkflowCompleted (command order preserved).
    let side_effect_idx = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. }))
        .expect("terminal-cycle side effect must be persisted");
    let completed_idx = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. }))
        .expect("terminal event present");
    assert!(
        side_effect_idx < completed_idx,
        "the side effect must be appended before the terminal event:\n{history:?}"
    );

    let report = WorkflowReplayer::new()
        .register_fn("work_then_uuid", work_then_uuid_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a side effect after the last suspension must replay:\n{report}"
    );
}

// ── B2: SetCurrentDetails does not wedge an in-subset workflow ─────────────────

#[workflow]
async fn status_workflow(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.set_current_details("step 1: starting");
    let out = ctx
        .execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    // Overwrite (last-write-wins) — and this one rides the terminal drain.
    ctx.set_current_details("step 2: done");
    Ok(out.as_i64().unwrap_or_default() * 2)
}

#[tokio::test]
async fn set_current_details_does_not_wedge_completion() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&status_workflow_info());
    rt.register_activity("work", echo_activity());
    let exec = rt.start_workflow("status_workflow", json!(9)).unwrap();

    // Must NOT return Err(Unsupported("SetCurrentDetails")).
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(18)),
        "a workflow calling set_current_details must complete, got {state:?}"
    );
    // It appends no WorkflowEvent — the history is exactly the activity lifecycle.
    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .all(|e| !matches!(e, WorkflowEvent::SideEffectRecorded { .. })),
        "set_current_details must leave no event footprint"
    );
}

// ── B3: retry exhaustion → terminal ActivityFailed → WorkflowFailed ────────────

#[workflow]
async fn calls_boom(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("boom", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap_or_default())
}

#[tokio::test]
async fn activity_exhausts_retries_and_workflow_fails() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&calls_boom_info());
    rt.register_activity(
        "boom",
        ActivitySpec::new(3, |_input: serde_json::Value| {
            Err("permanent failure".to_string())
        }),
    );
    let exec = rt.start_workflow("calls_boom", json!(1)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Failed(err) = state else {
        panic!("expected RunState::Failed, got {state:?}");
    };
    assert!(!err.is_empty(), "the terminal failure must carry an error");

    // Exactly max_attempts (3) attempts, all failing.
    let attempts = rt.activity_attempts(exec, "boom").unwrap();
    assert_eq!(attempts.len(), 3, "all 3 attempts consumed");
    assert!(attempts.iter().all(|a| a.result.is_err()));

    let history = rt.load_history(exec).unwrap();
    let activity_failed = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(activity_failed, 1, "exactly one terminal ActivityFailed");
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a failing workflow records a WorkflowFailed event"
    );
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Failed(_)
    ));
}

/// A workflow that returns `Err` directly (no activity) exercises the
/// `WorkflowOutcome::Failed` arm without retries.
#[workflow]
async fn direct_fail(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _ = ctx;
    Err("boom".to_string())
}

#[tokio::test]
async fn direct_error_return_fails_terminally() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&direct_fail_info());
    let exec = rt.start_workflow("direct_fail", json!(0)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(ref e) if e == "boom"),
        "got {state:?}"
    );
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Failed(ref e) if e == "boom"
    ));
    assert!(
        rt.load_history(exec)
            .unwrap()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. }))
    );
}

// ── B4: an unsupported command mid-batch rolls the WHOLE cycle back ────────────

/// Records a side effect (supported) and then spawns a child workflow
/// (unsupported) in the SAME suspending batch. The whole `apply_commands`
/// transaction must roll back — the side-effect event must NOT persist.
#[workflow]
async fn side_effect_then_child(
    ctx: &WorkflowContext,
    _n: i64,
) -> Result<serde_json::Value, String> {
    let _id = ctx.new_uuid().to_string();
    let r = ctx
        .spawn_child_workflow_raw("child", json!({}))
        .await
        .map_err(|e| e.to_string())?;
    Ok(r)
}

#[tokio::test]
async fn unsupported_command_rolls_the_whole_cycle_back() {
    let (_dir, path) = temp_db();
    let exec;
    {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&side_effect_then_child_info());
        exec = rt
            .start_workflow("side_effect_then_child", json!(0))
            .unwrap();

        let err = rt.run_until_blocked(exec).await.unwrap_err();
        assert!(
            matches!(err, SqliteError::Unsupported(ref c) if c == "StartChildWorkflow"),
            "expected Unsupported(StartChildWorkflow), got {err}"
        );
    } // "crash".

    // Reopen: the side-effect event must NOT have persisted (the cycle's tx,
    // which appended SideEffectRecorded before hitting the unsupported command,
    // rolled back as a whole).
    let rt2 = SqliteRuntime::open(&path).unwrap();
    let history = rt2.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .all(|e| !matches!(e, WorkflowEvent::SideEffectRecorded { .. })),
        "no partial side-effect write may survive a rolled-back cycle:\n{history:?}"
    );
    // Only WorkflowStarted was committed (by start_workflow, a separate tx).
    assert_eq!(history.len(), 1, "only the committed start event remains");
    assert!(matches!(history[0], WorkflowEvent::WorkflowStarted { .. }));
}

// ── Timer-id reuse: a re-armed timer id fires again (Codex round-5) ────────────

/// Arms the SAME timer id twice in sequence (the poll-loop idiom). The core
/// supports this via cursor-based per-id FIFO pairing; the backend must create a
/// fresh unfired row for the second arm rather than wedging on the first arm's
/// spent row.
#[workflow]
async fn double_timer_same_id(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    ctx.timer("tick", 5).await.map_err(|e| e.to_string())?;
    ctx.timer("tick", 5).await.map_err(|e| e.to_string())?;
    Ok("done".to_string())
}

#[tokio::test]
async fn reused_timer_id_fires_again_after_a_prior_fire() {
    let (_dir, path) = temp_db();
    let t0: DateTime<Utc> = DateTime::from_timestamp(3_000_000, 0).unwrap();

    let exec;
    {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&double_timer_same_id_info());
        exec = rt.start_workflow("double_timer_same_id", json!(0)).unwrap();

        // First arm blocks (deadline t0+5 not due at t0).
        let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
        assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");

        // Fire the first timer; the SECOND arm of "tick" is created (deadline
        // t0+10+5 = t0+15, not yet due at t0+10) and the run blocks again.
        let state = rt
            .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(10))
            .await
            .unwrap();
        assert!(
            matches!(state, RunState::WaitingTimer),
            "the re-armed timer must still be pending, got {state:?}"
        );
    } // drop == crash before the second timer fires.

    // Reopen and fire the SECOND arm past its deadline.
    let mut rt2 = SqliteRuntime::open(&path).unwrap();
    rt2.register_workflow(&double_timer_same_id_info());
    let state = rt2
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(20))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "done"),
        "the re-armed timer must fire and the workflow complete, got {state:?}"
    );

    // History carries TWO arms and TWO fires of the same id.
    let history = rt2.load_history(exec).unwrap();
    let started = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::TimerStarted { timer_id, .. } if timer_id.as_str() == "tick"))
        .count();
    let fired = history
        .iter()
        .filter(
            |e| matches!(e, WorkflowEvent::TimerFired { timer_id } if timer_id.as_str() == "tick"),
        )
        .count();
    assert_eq!(started, 2, "both arms of the reused id must be recorded");
    assert_eq!(fired, 2, "both fires of the reused id must be recorded");
}

// ── m7: multi-execution run_until_idle drives every running workflow ───────────

#[workflow]
async fn double_it(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap_or_default() * 2)
}

#[tokio::test]
async fn run_until_idle_drives_multiple_executions() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&double_it_info());
    rt.register_activity("work", echo_activity());

    let e1 = rt.start_workflow("double_it", json!(3)).unwrap();
    let e2 = rt.start_workflow("double_it", json!(10)).unwrap();
    let e3 = rt.start_workflow("double_it", json!(50)).unwrap();

    rt.run_until_idle().await.unwrap();

    assert!(
        matches!(rt.outcome(e1).unwrap(), ExecutionOutcome::Completed(ref v) if v.as_i64() == Some(6))
    );
    assert!(
        matches!(rt.outcome(e2).unwrap(), ExecutionOutcome::Completed(ref v) if v.as_i64() == Some(20))
    );
    assert!(
        matches!(rt.outcome(e3).unwrap(), ExecutionOutcome::Completed(ref v) if v.as_i64() == Some(100))
    );
}
