//! Workflow `execution_timeout` on the `SQLite` backend (issue #2029, #243).
//!
//! `#[workflow(execution_timeout = ..)]` used to panic at registration. A run
//! now stores its deadline at start, and the drive ends it `TIMED_OUT` once
//! the deadline passes. Each history replays on the core `WorkflowReplayer`.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest_sqlite::{ExecutionOutcome, RunState, SqliteError, SqliteRuntime};
use chrono::{TimeZone, Utc};
use serde_json::json;

/// Reports whether the context carries a deadline.
#[workflow(execution_timeout = "30s")]
async fn quick(ctx: &WorkflowContext, n: i64) -> Result<bool, String> {
    let _ = n;
    Ok(ctx.deadline().is_some())
}

/// Waits for a signal that never comes.
#[workflow(execution_timeout = "30s")]
async fn waits_forever(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _: serde_json::Value = ctx
        .wait_for_signal("never")
        .await
        .map_err(|e| e.to_string())?;
    Ok(0)
}

/// Arms a timer that outlives the deadline.
#[workflow(execution_timeout = "30s")]
async fn sleeps_past_deadline(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    ctx.timer("long", 120).await.map_err(|e| e.to_string())?;
    Ok(0)
}

/// A settable test clock in epoch seconds.
#[derive(Clone)]
struct Clock(Arc<AtomicI64>);

impl Clock {
    fn set(&self, secs: i64) {
        self.0.store(1_000_000 + secs, Ordering::SeqCst);
    }
}

fn runtime(info: &WorkflowInfo) -> (SqliteRuntime, Clock) {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(info);
    let clock = Clock(Arc::new(AtomicI64::new(0)));
    clock.set(0);
    let read = clock.clone();
    rt.set_clock(move || Utc.timestamp_opt(read.0.load(Ordering::SeqCst), 0).unwrap());
    (rt, clock)
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

#[tokio::test]
async fn a_run_within_its_deadline_completes() {
    let info = quick_info();
    let (mut rt, _clock) = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(1)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == true),
        "ctx.deadline() must report the deadline: {state:?}"
    );
    assert_replays(info.name, info.handler, rt.load_history(exec).unwrap()).await;
}

#[tokio::test]
async fn a_blocked_run_past_its_deadline_ends_timed_out() {
    let info = waits_forever_info();
    let (mut rt, clock) = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(1)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "got {state:?}");
    clock.set(29);
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "got {state:?}");

    clock.set(30);
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::TimedOut(ref e) if e == "timeout: WorkflowExecution for waits_forever"),
        "got {state:?}"
    );
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Terminated(ref s) if s == "TIMED_OUT"
    ));
    let history = rt.load_history(exec).unwrap();
    let last = history.last().unwrap();
    assert!(
        matches!(last, WorkflowEvent::WorkflowExecutionTimedOut { deadline, .. }
            if deadline.timestamp() == 1_000_030),
        "got {last:?}"
    );
    assert_replays(info.name, info.handler, history).await;

    // The run is terminal. A signal is refused, and a drive does not rerun it.
    assert!(matches!(
        rt.send_signal(exec, "never", json!(1)),
        Err(SqliteError::WorkflowNotRunning { .. })
    ));
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::TimedOut(_)), "got {state:?}");
}

// H8: the seal removes the run's unfired timer.
#[tokio::test]
async fn the_fleet_poll_times_out_a_sleeping_run_and_drops_its_timer() {
    let info = sleeps_past_deadline_info();
    let (mut rt, clock) = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(1)).unwrap();
    rt.run_until_idle().await.unwrap();

    clock.set(31);
    rt.run_until_idle().await.unwrap();
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Terminated(ref s) if s == "TIMED_OUT"
    ));
    // A later clock past the timer fires nothing on the sealed run.
    clock.set(200);
    rt.run_until_idle().await.unwrap();
    let history = rt.load_history(exec).unwrap();
    assert!(
        !history.iter().any(|e| e.type_name() == "TimerFired"),
        "the sealed run must not fire its timer"
    );
    assert_replays(info.name, info.handler, history).await;
}
