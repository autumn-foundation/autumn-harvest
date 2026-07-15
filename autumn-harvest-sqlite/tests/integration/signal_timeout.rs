//! Regression tests for the signal-or-deadline wait (`wait_for_signal_timeout` /
//! `receive_signal_timeout`, issue #476) on the single-writer backend — Codex
//! #1069 P2.
//!
//! When such a race's **signal wins**, the determinism core pushes a
//! `WorkflowCommand::CancelRaceLosers { timers: [<deadline timer>], .. }` to tear
//! down the still-armed deadline timer. Before the fix, both the suspending-batch
//! path (`apply_commands`) and the terminal drain
//! (`persist_terminal_pending_commands`) treated that command as
//! `Unsupported("Unknown")`, so after `SignalReceived` had already been committed
//! the next cycle errored and the execution could never complete — wedging every
//! normal pull-style signal workflow that adds a timeout.
//!
//! These tests drive the runtime with the injected `*_as_of` clock seam so the
//! signal-wins and deadline-wins branches are exercised deterministically without
//! sleeping.

// The `#[workflow]` macro references the workflow's input parameter, so a
// deliberately-unused param reads as a "used underscore binding" through the
// expansion.
#![allow(clippy::used_underscore_binding)]

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

fn temp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

/// Count still-pending (`fired = 0`) durable timer rows for `exec`, read from a
/// raw connection to the same file (the established inspection pattern in
/// `reopen_consistency.rs`/`atomicity.rs`).
fn pending_timer_rows(path: &std::path::Path, exec: ExecutionId) -> i64 {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT COUNT(*) FROM harvest_timers WHERE exec_id = ?1 AND fired = 0",
        [exec.to_string()],
        |r| r.get(0),
    )
    .unwrap()
}

// A one-hour deadline: far past every `_as_of` instant these tests use, so the
// branch is decided purely by whether the signal is delivered first.
const ONE_HOUR: Duration = Duration::from_secs(3600);

/// Await an approval signal with a deadline. Completes with the approver on a
/// signal, or `auto_rejected` if the deadline fires first (the classic
/// human-in-the-loop shape). The trailing `Ok(..)` puts the `CancelRaceLosers`
/// teardown in the **terminal** drain when the signal wins.
#[workflow]
async fn approval_gate(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let decision = ctx
        .wait_for_signal_timeout("approval", ONE_HOUR)
        .await
        .map_err(|e| e.to_string())?;
    let out = decision.map_or_else(
        || "auto_rejected".to_string(),
        |payload| format!("approved:{}", payload.as_str().unwrap_or_default()),
    );
    Ok(out)
}

// ── Signal wins → the workflow COMPLETES (the fix; FAILS pre-fix) ──────────────

#[tokio::test]
async fn wait_for_signal_timeout_signal_wins_completes_and_deletes_deadline_timer() {
    let (_dir, path) = temp_db();
    let t0: DateTime<Utc> = DateTime::from_timestamp(3_000_000, 0).unwrap();

    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&approval_gate_info());
    let exec = rt.start_workflow("approval_gate", json!(0)).unwrap();

    // First drive (well before the deadline): the race arms the deadline timer
    // and blocks awaiting the signal.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "approval"),
        "must block on the signal before the deadline, got {state:?}"
    );
    assert_eq!(
        pending_timer_rows(&path, exec),
        1,
        "the deadline timer is armed while the race is open"
    );

    // Deliver the signal BEFORE the deadline.
    rt.send_signal(exec, "approval", json!("alice")).unwrap();

    // Drive again, still before the deadline. The signal wins; the core pushes
    // `CancelRaceLosers` to delete the losing deadline timer and the workflow
    // COMPLETES. Pre-fix this `.unwrap()` panicked on
    // `Err(Unsupported("Unknown"))` from the terminal drain.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "approved:alice"),
        "signal-win must complete with the payload, got {state:?}"
    );

    // The losing deadline timer row was actually deleted — no orphaned pending
    // timer left to fire later or to pin the terminal run.
    assert_eq!(
        pending_timer_rows(&path, exec),
        0,
        "the losing deadline timer must be deleted on signal-win"
    );

    // And it was deleted, not fired: no stray TimerFired in history.
    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .all(|e| !matches!(e, WorkflowEvent::TimerFired { .. })),
        "the deadline timer must be torn down, never fired, on signal-win:\n{history:?}"
    );
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SignalReceived { .. })),
        "the winning signal must be recorded"
    );

    // Cross-backend guarantee: the signal-win history replays byte-identically on
    // the core engine (the re-pushed, event-less `CancelRaceLosers` is fine).
    let report = WorkflowReplayer::new()
        .register_fn("approval_gate", approval_gate_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a signal-win-with-timeout history must replay on the core:\n{report}"
    );
}

// ── Deadline wins → returns None and the workflow completes ────────────────────

#[tokio::test]
async fn wait_for_signal_timeout_deadline_wins_returns_none() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&approval_gate_info());
    let t0: DateTime<Utc> = DateTime::from_timestamp(3_000_000, 0).unwrap();
    let exec = rt.start_workflow("approval_gate", json!(0)).unwrap();

    // Block on the signal (timer not yet due).
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "approval"),
        "got {state:?}"
    );

    // NO signal delivered. Drive past the deadline: the timer fires, the race
    // resolves to the timeout branch (None), and the workflow completes.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(3601))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "auto_rejected"),
        "deadline-win must complete with the timeout branch, got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "the deadline timer must have fired on the timeout branch:\n{history:?}"
    );
    assert!(
        history
            .iter()
            .all(|e| !matches!(e, WorkflowEvent::SignalReceived { .. })),
        "no signal was delivered on the timeout branch"
    );
}

// ── Arrival-time ordering: a LATE signal must NOT beat an expired deadline ─────
//
// Codex #1069 P1 (runtime.rs:562). When a workflow is parked in
// `wait_for_signal_timeout`, the decision cycle used to consume any staged signal
// BEFORE the due `TimerFired` was appended, so a signal delivered AFTER the
// deadline (while the runtime was idle) recorded `SignalReceived` first and
// wrongly won the race as an approval. The fix interleaves the signal against the
// due deadline timer by ARRIVAL time (mirroring the core `merge_wake_events`): a
// deadline that expired at or before the signal arrived fires FIRST, so the
// timeout wins and the (still-recorded) signal stays observable to a later plain
// wait.

fn event_pos(history: &[WorkflowEvent], pred: impl Fn(&WorkflowEvent) -> bool) -> Option<usize> {
    history.iter().position(pred)
}

#[tokio::test]
async fn late_signal_does_not_beat_expired_timeout() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&approval_gate_info());
    let t0: DateTime<Utc> = DateTime::from_timestamp(3_000_000, 0).unwrap();
    let exec = rt.start_workflow("approval_gate", json!(0)).unwrap();

    // Block on the signal; the deadline is armed at t0 + 3600.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "approval"),
        "got {state:?}"
    );

    // Deliver the signal AFTER the deadline (received_at = t0 + 3601 > t0 + 3600),
    // as if it arrived while the idle runtime had already blown past the deadline.
    rt.send_signal_as_of(
        exec,
        "approval",
        json!("late"),
        t0 + chrono::Duration::seconds(3601),
    )
    .unwrap();

    // Drive past the deadline. The deadline expired at or before the signal
    // arrived, so the TIMEOUT wins — NOT the late signal.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(3602))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "auto_rejected"),
        "a signal that arrived after the deadline must NOT win — the timeout wins, got {state:?}"
    );

    // History records the fire BEFORE the late signal, and the signal is still
    // recorded (observable to a later plain wait — never silently dropped).
    let history = rt.load_history(exec).unwrap();
    let timer_pos = event_pos(&history, |e| matches!(e, WorkflowEvent::TimerFired { .. }))
        .expect("the expired deadline must fire");
    let signal_pos = event_pos(&history, |e| {
        matches!(e, WorkflowEvent::SignalReceived { .. })
    })
    .expect("the late signal must still be recorded");
    assert!(
        timer_pos < signal_pos,
        "TimerFired must precede the late SignalReceived (arrival-time ordering):\n{history:?}"
    );

    // The winning-timeout history replays byte-identically on the core engine.
    let report = WorkflowReplayer::new()
        .register_fn("approval_gate", approval_gate_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a late-signal-vs-expired-timeout history must replay on the core:\n{report}"
    );
}

#[tokio::test]
async fn on_time_signal_still_wins() {
    // The OTHER branch: a signal that arrived BEFORE the deadline still wins, even
    // when the run is only driven PAST the deadline (the fix orders by arrival
    // time, not by consumption/drive order).
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&approval_gate_info());
    let t0: DateTime<Utc> = DateTime::from_timestamp(3_000_000, 0).unwrap();
    let exec = rt.start_workflow("approval_gate", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "approval"),
        "got {state:?}"
    );

    // Signal arrives BEFORE the deadline (received_at = t0 + 10 < t0 + 3600) …
    rt.send_signal_as_of(
        exec,
        "approval",
        json!("alice"),
        t0 + chrono::Duration::seconds(10),
    )
    .unwrap();

    // … but the run is only driven PAST the deadline. The on-time signal must
    // still win the approval.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(3700))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "approved:alice"),
        "an on-time signal must win even when driven past the deadline, got {state:?}"
    );

    // The signal is ordered ahead of the (later-fired) deadline timer in history.
    let history = rt.load_history(exec).unwrap();
    let signal_pos = event_pos(&history, |e| {
        matches!(e, WorkflowEvent::SignalReceived { .. })
    })
    .expect("the winning signal must be recorded");
    if let Some(timer_pos) = event_pos(&history, |e| matches!(e, WorkflowEvent::TimerFired { .. }))
    {
        assert!(
            signal_pos < timer_pos,
            "the on-time SignalReceived must precede any TimerFired:\n{history:?}"
        );
    }

    let report = WorkflowReplayer::new()
        .register_fn("approval_gate", approval_gate_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the on-time-signal-win history must replay on the core:\n{report}"
    );
}

// ── Typed sibling `receive_signal_timeout` completes on signal-win too ─────────

#[derive(serde::Deserialize, serde::Serialize)]
struct Approval {
    approver: String,
}

/// Typed variant: also does real work (an activity) AFTER the resolved race, so
/// the `CancelRaceLosers` teardown rides a **suspending** batch (`apply_commands`)
/// rather than the terminal drain — covering the second call site of the fix.
#[workflow]
async fn typed_approval_then_work(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let decision: Option<Approval> = ctx
        .receive_signal_timeout("approval", ONE_HOUR)
        .await
        .map_err(|e| e.to_string())?;
    let base = i64::from(decision.is_some());
    // A suspension AFTER the race: the teardown command lands in the next
    // suspending batch alongside this activity's ScheduleActivity.
    let out = ctx
        .execute_activity_raw("double", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap_or_default() * 2 + base)
}

#[tokio::test]
async fn receive_signal_timeout_signal_wins_with_trailing_activity_completes() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&typed_approval_then_work_info());
    rt.register_activity_raw(
        "double",
        autumn_harvest_sqlite::ActivitySpec::new(1, |input: serde_json::Value| {
            Ok(json!(input.as_i64().unwrap_or_default()))
        }),
    );
    let t0: DateTime<Utc> = DateTime::from_timestamp(3_000_000, 0).unwrap();
    let exec = rt
        .start_workflow("typed_approval_then_work", json!(5))
        .unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "approval"),
        "got {state:?}"
    );

    rt.send_signal(exec, "approval", json!({ "approver": "bob" }))
        .unwrap();

    // Signal wins; the teardown rides the SAME batch as the trailing activity's
    // dispatch. Pre-fix `apply_commands` rejected it as Unsupported.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(11)),
        "signal-win + trailing activity must complete (5*2 + 1), got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .all(|e| !matches!(e, WorkflowEvent::TimerFired { .. })),
        "the deadline timer must be torn down, never fired:\n{history:?}"
    );
    let report = WorkflowReplayer::new()
        .register_fn(
            "typed_approval_then_work",
            typed_approval_then_work_info().handler,
        )
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the signal-win-plus-activity history must replay on the core:\n{report}"
    );
}
