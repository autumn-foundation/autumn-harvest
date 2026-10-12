//! Cycle-start signal ingest (issue #2020).
//!
//! Each decision cycle appends every staged signal to history before replay.
//! The non-blocking reads and the push handlers then see the signals, as on
//! Postgres.

// The `#[workflow]` macro reads the input parameter, so an unused one is a
// used underscore binding.
#![allow(clippy::used_underscore_binding)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::WorkflowHandlerFn;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{RunState, SqliteError, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

const fn t0() -> DateTime<Utc> {
    DateTime::from_timestamp(4_000_000, 0).unwrap()
}

fn at(secs: i64) -> DateTime<Utc> {
    t0() + chrono::Duration::seconds(secs)
}

fn completed(state: &RunState) -> Value {
    match state {
        RunState::Completed(v) => v.clone(),
        other => panic!("expected Completed, got {other:?}"),
    }
}

async fn assert_replays(name: &str, handler: WorkflowHandlerFn, history: Vec<WorkflowEvent>) {
    let report = WorkflowReplayer::new()
        .register_fn(name, handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the history must replay on the core engine:\n{report}"
    );
}

/// The probe loop from the issue. Each pass drains `x`, then sleeps.
#[workflow]
async fn drain_loop(ctx: &WorkflowContext, _n: i64) -> Result<usize, String> {
    let mut seen = 0;
    for _ in 0..3 {
        seen += ctx.drain_signals_raw("x").map_err(|e| e.to_string())?.len();
        ctx.timer("tick", 1).await.map_err(|e| e.to_string())?;
    }
    Ok(seen)
}

#[tokio::test]
async fn drain_loop_keeps_every_signal() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&drain_loop_info());
    let exec = rt.start_workflow("drain_loop", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
    rt.send_signal_as_of(exec, "x", json!("a"), at(0)).unwrap();
    rt.send_signal_as_of(exec, "x", json!("b"), at(0)).unwrap();

    let mut state = state;
    for secs in 2..10 {
        state = rt.run_until_blocked_as_of(exec, at(secs)).await.unwrap();
        if matches!(state, RunState::Completed(_)) {
            break;
        }
    }
    assert_eq!(completed(&state), json!(2), "both signals count");

    let history = rt.load_history(exec).unwrap();
    assert_replays("drain_loop", drain_loop_info().handler, history).await;
}

/// A push handler stores the payload. The body waits for it.
///
/// A handler runs at the next history call after it registers. The timer is
/// that call, as the core docs prescribe.
#[workflow]
async fn handler_gate(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let slot: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&slot);
    ctx.register_signal_handler_raw("go", move |payload| {
        *sink.lock().unwrap() = Some(payload);
    });
    ctx.timer("settle", 1).await.map_err(|e| e.to_string())?;
    let probe = Arc::clone(&slot);
    ctx.await_condition(move || probe.lock().unwrap().is_some())
        .await
        .map_err(|e| e.to_string())?;
    let out = slot.lock().unwrap().take().unwrap_or(Value::Null);
    Ok(out)
}

#[tokio::test]
async fn signal_handler_and_await_condition_complete_with_the_payload() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&handler_gate_info());
    let exec = rt.start_workflow("handler_gate", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
    rt.send_signal_as_of(exec, "go", json!("ship"), t0())
        .unwrap();

    let state = rt.run_until_blocked_as_of(exec, at(2)).await.unwrap();
    assert_eq!(completed(&state), json!("ship"));

    let history = rt.load_history(exec).unwrap();
    assert_replays("handler_gate", handler_gate_info().handler, history).await;
}

/// One non-blocking read. No suspension point comes before it.
// A workflow body is async by contract, and this one never awaits.
#[allow(clippy::unused_async)]
#[workflow]
async fn try_once(ctx: &WorkflowContext, _n: i64) -> Result<Option<String>, String> {
    ctx.try_receive_signal::<String>("x")
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn try_receive_signal_returns_a_staged_signal_without_blocking() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&try_once_info());
    let exec = rt.start_workflow("try_once", json!(0)).unwrap();
    rt.send_signal_as_of(exec, "x", json!("hello"), t0())
        .unwrap();

    let state = rt.run_until_blocked_as_of(exec, at(1)).await.unwrap();
    assert_eq!(completed(&state), json!("hello"));

    let history = rt.load_history(exec).unwrap();
    assert_replays("try_once", try_once_info().handler, history).await;
}

#[tokio::test]
async fn try_receive_signal_returns_none_when_nothing_is_staged() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&try_once_info());
    let exec = rt.start_workflow("try_once", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, at(1)).await.unwrap();
    assert_eq!(completed(&state), Value::Null);
}

/// Waits for `b`, then drains `a`. The `a` signals arrive first.
#[workflow]
async fn wait_then_drain(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let b = ctx.wait_for_signal("b").await.map_err(|e| e.to_string())?;
    let a = ctx.drain_signals_raw("a").map_err(|e| e.to_string())?;
    Ok(json!({ "b": b, "a": a }))
}

#[tokio::test]
async fn other_names_are_ingested_in_arrival_order() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&wait_then_drain_info());
    let exec = rt.start_workflow("wait_then_drain", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "b"),
        "{state:?}"
    );

    // Stage out of arrival order. Ingest must sort by arrival time.
    rt.send_signal_as_of(exec, "a", json!(2), at(3)).unwrap();
    rt.send_signal_as_of(exec, "a", json!(1), at(1)).unwrap();
    rt.send_signal_as_of(exec, "b", json!("go"), at(2)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, at(4)).await.unwrap();
    assert_eq!(completed(&state), json!({ "b": "go", "a": [1, 2] }));

    let history = rt.load_history(exec).unwrap();
    let order: Vec<Value> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::SignalReceived { payload, .. } => Some(payload.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(order, vec![json!(1), json!("go"), json!(2)]);
    assert_replays("wait_then_drain", wait_then_drain_info().handler, history).await;
}

/// The approval race from `signal_timeout.rs`, with a one-minute deadline.
///
/// The drain reads each later approval. Strict replay rejects a history with
/// a signal that the run never reads.
#[workflow]
async fn short_gate(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let decision = ctx
        .wait_for_signal_timeout("approval", Duration::from_secs(60))
        .await
        .map_err(|e| e.to_string())?;
    ctx.drain_signals_raw("approval")
        .map_err(|e| e.to_string())?;
    Ok(decision.map_or_else(
        || "auto_rejected".to_string(),
        |p| format!("approved:{}", p.as_str().unwrap_or_default()),
    ))
}

#[tokio::test]
async fn an_on_time_signal_wins_when_a_late_one_is_also_staged() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&short_gate_info());
    let exec = rt.start_workflow("short_gate", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "approval"),
        "{state:?}"
    );

    // One signal before the deadline, one after. One drive past both.
    rt.send_signal_as_of(exec, "approval", json!("alice"), at(30))
        .unwrap();
    rt.send_signal_as_of(exec, "approval", json!("bob"), at(90))
        .unwrap();

    let state = rt.run_until_blocked_as_of(exec, at(120)).await.unwrap();
    assert_eq!(completed(&state), json!("approved:alice"));

    let history = rt.load_history(exec).unwrap();
    assert_replays("short_gate", short_gate_info().handler, history).await;
}

#[tokio::test]
async fn a_late_signal_loses_to_the_deadline_under_eager_ingest() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&short_gate_info());
    let exec = rt.start_workflow("short_gate", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "approval"),
        "{state:?}"
    );
    rt.send_signal_as_of(exec, "approval", json!("late"), at(90))
        .unwrap();

    let state = rt.run_until_blocked_as_of(exec, at(120)).await.unwrap();
    assert_eq!(completed(&state), json!("auto_rejected"));

    let history = rt.load_history(exec).unwrap();
    assert_replays("short_gate", short_gate_info().handler, history).await;
}

/// A run parked on `await_condition` emits no command. A drive before the
/// signal therefore reports `Stuck`. The next drive after the signal resumes
/// the run.
#[tokio::test]
async fn a_parked_condition_resumes_on_the_drive_after_the_signal() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&handler_gate_info());
    let exec = rt.start_workflow("handler_gate", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
    let parked = rt.run_until_blocked_as_of(exec, at(2)).await;
    assert!(matches!(parked, Err(SqliteError::Stuck(_))), "{parked:?}");

    rt.send_signal_as_of(exec, "go", json!("late"), at(3))
        .unwrap();
    let state = rt.run_until_blocked_as_of(exec, at(4)).await.unwrap();
    assert_eq!(completed(&state), json!("late"));

    let history = rt.load_history(exec).unwrap();
    assert_replays("handler_gate", handler_gate_info().handler, history).await;
}
