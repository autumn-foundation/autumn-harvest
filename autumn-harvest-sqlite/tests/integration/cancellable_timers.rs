//! Cancellable durable timers on the `SQLite` backend (issue #2029, #768).
//!
//! `ctx.start_timer` and `TimerHandle` used to seal the run `FAILED` with
//! `UnsupportedFeature`. Each test runs such a workflow to completion, then
//! replays its history on the core `WorkflowReplayer`.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use serde_json::json;

/// Arms a timer and waits for it to fire.
#[workflow]
async fn waits_for_timer(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let handle = ctx.start_timer("gate", 10);
    let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
    Ok(format!("{outcome:?}"))
}

/// Arms a timer, cancels it, then awaits it.
#[workflow]
async fn cancels_timer(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let handle = ctx.start_timer("gate", 60);
    handle.cancel().map_err(|e| e.to_string())?;
    let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
    Ok(format!("{outcome:?}"))
}

/// Arms a long timer, resets it to a short one, then awaits it.
#[workflow]
async fn resets_timer(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let mut handle = ctx.start_timer("gate", 600);
    handle.reset(5).map_err(|e| e.to_string())?;
    let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
    Ok(format!("{outcome:?}"))
}

/// Arms a deadline, waits for a signal, then cancels the deadline.
#[workflow]
async fn cancels_after_signal(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let handle = ctx.start_timer("deadline", 60);
    let _: serde_json::Value = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    handle.cancel().map_err(|e| e.to_string())?;
    let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
    Ok(format!("{outcome:?}"))
}

/// Fires a cancellable timer, cancels it, then reuses its id for `ctx.timer`.
#[workflow]
async fn fires_cancels_then_reuses(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let handle = ctx.start_timer("x", 5);
    handle.await_fire().await.map_err(|e| e.to_string())?;
    handle.cancel().map_err(|e| e.to_string())?;
    ctx.timer("x", 5).await.map_err(|e| e.to_string())?;
    Ok("done".to_string())
}

/// Arms a timer but never awaits it, then waits for a signal.
#[workflow]
async fn arms_without_await(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let _handle = ctx.start_timer("unawaited", 5);
    let _: serde_json::Value = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok("signalled".to_string())
}

/// Cancels a cancellable timer, then reuses its id for `ctx.timer`.
#[workflow]
async fn reuses_cancelled_id(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let handle = ctx.start_timer("x", 60);
    handle.cancel().map_err(|e| e.to_string())?;
    ctx.timer("x", 1).await.map_err(|e| e.to_string())?;
    Ok("classic fired".to_string())
}

fn t(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_000_000 + secs, 0).unwrap()
}

fn runtime(info: &WorkflowInfo) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(info);
    rt
}

async fn assert_replays(name: &str, handler: WorkflowHandlerFn, history: Vec<WorkflowEvent>) {
    let report = WorkflowReplayer::new()
        .register_fn(name, handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the history must replay:\n{report}"
    );
}

fn count(history: &[WorkflowEvent], kind: &str) -> usize {
    history.iter().filter(|e| e.type_name() == kind).count()
}

#[tokio::test]
async fn an_awaited_timer_fires_and_the_run_completes() {
    let info = waits_for_timer_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t(0)).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    let state = rt.run_until_blocked_as_of(exec, t(10)).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "Fired"),
        "got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert_eq!(count(&history, "TimerStarted"), 1);
    assert_eq!(count(&history, "TimerFired"), 1);
    assert_replays(info.name, info.handler, history).await;
}

// H1: each replay re-emits the await arm. The row keeps its first deadline.
#[tokio::test]
async fn a_repeated_await_keeps_the_first_deadline() {
    let info = waits_for_timer_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(0)).unwrap();

    for secs in [0, 4, 9] {
        let state = rt.run_until_blocked_as_of(exec, t(secs)).await.unwrap();
        assert!(
            matches!(state, RunState::WaitingTimer),
            "at {secs}s: {state:?}"
        );
    }
    let state = rt.run_until_blocked_as_of(exec, t(10)).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
}

#[tokio::test]
async fn a_cancelled_timer_never_fires() {
    let info = cancels_timer_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t(0)).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "Cancelled"),
        "got {state:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert_eq!(count(&history, "TimerCancelled"), 1);
    assert_eq!(count(&history, "TimerFired"), 0);
    assert_replays(info.name, info.handler, history).await;
}

#[tokio::test]
async fn a_reset_timer_fires_at_its_new_duration() {
    let info = resets_timer_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t(0)).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    let state = rt.run_until_blocked_as_of(exec, t(5)).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "Fired"),
        "got {state:?}"
    );
    assert_replays(info.name, info.handler, rt.load_history(exec).unwrap()).await;
}

#[tokio::test]
async fn a_timer_cancelled_after_a_signal_completes_the_run() {
    let info = cancels_after_signal_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t(0)).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "go"),
        "got {state:?}"
    );
    rt.send_signal_as_of(exec, "go", json!(1), t(1)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t(2)).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "Cancelled"),
        "got {state:?}"
    );
    // The armed timer never had a row, and a later clock fires nothing.
    let state = rt.run_until_blocked_as_of(exec, t(120)).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
    assert_replays(info.name, info.handler, rt.load_history(exec).unwrap()).await;
}

// H2: a cancelled arm must not block a classic timer on the same id.
#[tokio::test]
async fn a_cancelled_id_can_arm_a_classic_timer() {
    let info = reuses_cancelled_id_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t(0)).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    let state = rt.run_until_blocked_as_of(exec, t(1)).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
    assert_replays(info.name, info.handler, rt.load_history(exec).unwrap()).await;
}

#[tokio::test]
async fn an_armed_timer_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("timers.db");
    let info = waits_for_timer_info();
    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&info);
        let exec = rt.start_workflow(info.name, json!(0)).unwrap();
        let state = rt.run_until_blocked_as_of(exec, t(0)).await.unwrap();
        assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
        exec
    };
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&info);
    let state = rt
        .run_until_blocked_as_of(exec, t(0) + ChronoDuration::seconds(10))
        .await
        .unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
}

// A cancel after a fire ends no arm. The classic timer on the same id then
// arms once, and does not arm again on each replay.
#[tokio::test]
async fn a_fired_then_cancelled_id_arms_one_classic_timer() {
    let info = fires_cancels_then_reuses_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(0)).unwrap();

    for secs in [0, 5] {
        let state = rt.run_until_blocked_as_of(exec, t(secs)).await.unwrap();
        assert!(
            matches!(state, RunState::WaitingTimer),
            "at {secs}s: {state:?}"
        );
    }
    let state = rt.run_until_blocked_as_of(exec, t(10)).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
    let history = rt.load_history(exec).unwrap();
    assert_eq!(count(&history, "TimerStarted"), 2);
    assert_eq!(count(&history, "TimerFired"), 2);
    assert_replays(info.name, info.handler, history).await;
}

// T2: an armed timer that the body never awaits has no row, so it never
// fires while the body waits on something else.
#[tokio::test]
async fn an_unawaited_timer_never_fires() {
    let info = arms_without_await_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(0)).unwrap();

    rt.run_until_blocked_as_of(exec, t(0)).await.unwrap();
    let state = rt.run_until_blocked_as_of(exec, t(60)).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "got {state:?}");
    rt.send_signal_as_of(exec, "go", json!(1), t(61)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t(62)).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
    let history = rt.load_history(exec).unwrap();
    assert_eq!(count(&history, "TimerFired"), 0);
    assert_replays(info.name, info.handler, history).await;
}
