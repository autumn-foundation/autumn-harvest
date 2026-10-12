//! Cycle-start signal ingest (issue #2020).
//!
//! Each decision cycle appends every staged signal to history before replay.
//! The non-blocking reads and the push handlers then see the signals, as on
//! Postgres.

// The `#[workflow]` macro reads the input parameter, so an unused one is a
// used underscore binding.
#![allow(clippy::used_underscore_binding)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::WorkflowHandlerFn;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{NonDeterminismKind, ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteError, SqliteRuntime};
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

/// Two races on one signal name, with different deadlines.
#[workflow]
async fn twin_gates(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let (a, b) = tokio::join!(
        ctx.wait_for_signal_timeout("go", Duration::from_secs(60)),
        ctx.wait_for_signal_timeout("go", Duration::from_secs(120)),
    );
    let a = a.map_err(|e| e.to_string())?;
    let b = b.map_err(|e| e.to_string())?;
    Ok(json!({ "a": a, "b": b }))
}

/// Each staged signal is checked against every due deadline of its name. A
/// signal after the second deadline cannot win the second race (issue #476).
#[tokio::test]
async fn a_late_signal_loses_the_second_of_two_same_name_races() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&twin_gates_info());
    let exec = rt.start_workflow("twin_gates", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "{state:?}");
    rt.send_signal_as_of(exec, "go", json!("on_time"), at(30))
        .unwrap();
    rt.send_signal_as_of(exec, "go", json!("late"), at(150))
        .unwrap();

    let state = rt.run_until_blocked_as_of(exec, at(200)).await.unwrap();
    assert_eq!(completed(&state), json!({ "a": "on_time", "b": null }));

    let history = rt.load_history(exec).unwrap();
    assert_replays("twin_gates", twin_gates_info().handler, history).await;
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

/// `1` arms a durable timer. `2` schedules an activity in its place, which
/// diverges from the phase-1 history. Only one test reads it.
static DRIFT_PHASE: AtomicU32 = AtomicU32::new(1);

#[workflow]
async fn drifty_drain(ctx: &WorkflowContext, _n: i64) -> Result<usize, String> {
    if DRIFT_PHASE.load(Ordering::SeqCst) == 1 {
        ctx.timer("t", 3600).await.map_err(|e| e.to_string())?;
    } else {
        ctx.execute_activity_raw("work", json!(1), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(ctx.drain_signals_raw("x").map_err(|e| e.to_string())?.len())
}

/// A non-deterministic cycle is discarded, so it consumes no signal.
#[tokio::test]
async fn a_non_deterministic_cycle_consumes_no_signal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&drifty_drain_info());
    rt.register_activity_raw("work", ActivitySpec::new(1, Ok));

    DRIFT_PHASE.store(1, Ordering::SeqCst);
    let exec = rt.start_workflow("drifty_drain", json!(0)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
    let before = rt.load_history(exec).unwrap();
    rt.send_signal_as_of(exec, "x", json!(1), t0()).unwrap();

    DRIFT_PHASE.store(2, Ordering::SeqCst);
    let err = rt.run_until_blocked_as_of(exec, at(1)).await.unwrap_err();
    assert!(matches!(err, SqliteError::NonDeterministic { .. }), "{err}");
    assert_eq!(rt.load_history(exec).unwrap().len(), before.len());
    assert_eq!(undelivered(&path, exec), 1, "the signal stays staged");

    // The fixed build ingests the signal on its next drive.
    DRIFT_PHASE.store(1, Ordering::SeqCst);
    let state = rt.run_until_blocked_as_of(exec, at(2)).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
    assert_eq!(undelivered(&path, exec), 0);
}

/// A fleet pass reports a parked run as `Stuck` and still drives the others.
#[tokio::test]
async fn a_fleet_pass_still_drives_other_runs_past_a_parked_condition() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&handler_gate_info());
    rt.register_workflow(&try_once_info());
    let parked = rt.start_workflow("handler_gate", json!(0)).unwrap();
    let state = rt.run_until_blocked_as_of(parked, t0()).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");

    let other = rt.start_workflow("try_once", json!(0)).unwrap();
    rt.send_signal_as_of(other, "x", json!("hi"), t0()).unwrap();

    // The first pass fires the parked run's timer. The second finds it parked.
    assert!(
        rt.poll_once_as_of(at(2)).await.unwrap(),
        "the first pass progresses"
    );
    let err = rt.poll_once_as_of(at(3)).await.unwrap_err();
    assert!(matches!(err, SqliteError::Stuck(e) if e == parked), "{err}");
    let state = rt.run_until_blocked_as_of(other, at(2)).await.unwrap();
    assert_eq!(completed(&state), json!("hi"));
}

/// A corrupt staged row fails the drive. It changes no history and stays
/// staged.
#[tokio::test]
async fn a_corrupt_staged_row_fails_the_drive_and_stays_staged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&try_once_info());
    let exec = rt.start_workflow("try_once", json!(0)).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "INSERT INTO harvest_signals (exec_id, name, payload_json, delivered, received_at) \
             VALUES (?1, 'junk', 'not json', 0, 0)",
            [exec.to_string()],
        )
        .unwrap();

    let err = rt.run_until_blocked_as_of(exec, at(1)).await.unwrap_err();
    assert!(matches!(err, SqliteError::Json(_)), "{err}");
    assert_eq!(rt.load_history(exec).unwrap().len(), 1, "only the start");
    assert_eq!(undelivered(&path, exec), 1);
}

/// An unread signal reaches history. Strict replay reports it, as for a
/// Postgres history.
#[tokio::test]
async fn an_unread_signal_fails_strict_replay() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&try_once_info());
    let exec = rt.start_workflow("try_once", json!(0)).unwrap();
    rt.send_signal_as_of(exec, "x", json!("read"), t0())
        .unwrap();
    rt.send_signal_as_of(exec, "y", json!("unread"), t0())
        .unwrap();

    let state = rt.run_until_blocked_as_of(exec, at(1)).await.unwrap();
    assert_eq!(completed(&state), json!("read"));

    let report = WorkflowReplayer::new()
        .register_fn("try_once", try_once_info().handler)
        .replay_from_events(rt.load_history(exec).unwrap())
        .await;
    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::EarlyCompletion,
                ..
            }
        ),
        "{report}"
    );
}

/// Each pass runs an activity, drains two names, then sleeps.
#[workflow]
async fn activity_drains(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let (mut x, mut y) = (0, 0);
    for i in 0..2 {
        ctx.execute_activity_raw("work", json!(i), "default")
            .await
            .map_err(|e| e.to_string())?;
        x += ctx.drain_signals_raw("x").map_err(|e| e.to_string())?.len();
        y += ctx.drain_signals_raw("y").map_err(|e| e.to_string())?.len();
        ctx.timer("tick", 1).await.map_err(|e| e.to_string())?;
    }
    Ok(json!([x, y]))
}

#[tokio::test]
async fn drains_interleave_with_activities() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&activity_drains_info());
    rt.register_activity_raw("work", ActivitySpec::new(1, Ok));
    let exec = rt.start_workflow("activity_drains", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
    rt.send_signal_as_of(exec, "x", json!(1), t0()).unwrap();
    rt.send_signal_as_of(exec, "y", json!(2), t0()).unwrap();
    rt.send_signal_as_of(exec, "x", json!(3), t0()).unwrap();

    let mut state = state;
    for secs in 2..10 {
        state = rt.run_until_blocked_as_of(exec, at(secs)).await.unwrap();
        if matches!(state, RunState::Completed(_)) {
            break;
        }
    }
    assert_eq!(completed(&state), json!([2, 1]));
    let history = rt.load_history(exec).unwrap();
    assert_replays("activity_drains", activity_drains_info().handler, history).await;
}

/// Panics when it reads a `boom` signal.
#[allow(clippy::unused_async)]
#[workflow]
async fn boom_on_signal(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let booms = ctx.drain_signals_raw("boom").map_err(|e| e.to_string())?;
    assert!(booms.is_empty(), "boom signal");
    Ok(0)
}

fn undelivered(path: &std::path::Path, exec: ExecutionId) -> i64 {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT COUNT(*) FROM harvest_signals WHERE exec_id = ?1 AND delivered = 0",
        [exec.to_string()],
        |r| r.get(0),
    )
    .unwrap()
}

/// A discarded panic strike leaves the signal staged. The terminal seal writes
/// the signal that replay saw, ahead of `WorkflowFailed`.
#[tokio::test]
async fn a_panic_seal_records_the_signal_that_replay_saw() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&boom_on_signal_info());
    let exec = rt.start_workflow("boom_on_signal", json!(0)).unwrap();
    rt.send_signal_as_of(exec, "boom", json!(1), t0()).unwrap();

    let mut state = None;
    for secs in 1..10 {
        match rt.run_until_blocked_as_of(exec, at(secs)).await {
            Err(SqliteError::WorkflowPanicked { .. }) => {
                assert_eq!(undelivered(&path, exec), 1, "a discarded strike");
                assert_eq!(rt.load_history(exec).unwrap().len(), 1, "only the start");
            }
            other => {
                state = Some(other.unwrap());
                break;
            }
        }
    }
    assert!(matches!(state, Some(RunState::Failed(_))), "{state:?}");
    assert_eq!(undelivered(&path, exec), 0, "the seal consumes the signal");

    let history = rt.load_history(exec).unwrap();
    let n = history.len();
    assert!(
        matches!(history[n - 2], WorkflowEvent::SignalReceived { .. })
            && matches!(history[n - 1], WorkflowEvent::WorkflowFailed { .. }),
        "{history:?}"
    );
}
