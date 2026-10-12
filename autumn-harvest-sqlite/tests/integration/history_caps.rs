//! Issue #2023: the #1804 history caps on the `SQLite` backend.
//!
//! The defaults match the Postgres worker: 50,000 events and 50 MiB per run.
//! A run that reaches a cap fails with a typed reason, unless it continues as
//! new. These tests lower the caps so that a run reaches them fast.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;
use autumn_harvest::{DEFAULT_HISTORY_BYTE_HARD_CAP, DEFAULT_HISTORY_EVENT_HARD_CAP};
use autumn_harvest_sqlite::{
    ActivitySpec, ExecutionOutcome, HistoryCapReason, RunState, SqliteRuntime,
};
use serde_json::{Value, json};

/// Runs `step` forever. Each turn adds two events.
#[workflow]
async fn endless_loop(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    loop {
        ctx.execute_activity_raw("step", json!(null), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
}

/// Phase 0 runs four steps and continues. Phase 1 completes.
#[workflow]
async fn four_steps_then_continue(ctx: &WorkflowContext, phase: i64) -> Result<i64, String> {
    if phase == 0 {
        for _ in 0..4 {
            ctx.execute_activity_raw("step", json!(null), "default")
                .await
                .map_err(|e| e.to_string())?;
        }
        ctx.continue_as_new(json!(1))
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(phase)
}

/// Runs four steps, then completes.
#[workflow]
async fn four_steps_then_complete(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    for _ in 0..4 {
        ctx.execute_activity_raw("step", json!(null), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(4)
}

/// A step result of about 500 bytes, so the byte cap is reached fast.
fn runtime() -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&endless_loop_info());
    rt.register_workflow(&four_steps_then_continue_info());
    rt.register_workflow(&four_steps_then_complete_info());
    rt.register_activity_raw(
        "step",
        ActivitySpec::new(1, |_: Value| Ok(json!("x".repeat(500)))),
    );
    rt
}

fn failure_reason(state: &RunState) -> HistoryCapReason {
    match state {
        RunState::Failed(error) => serde_json::from_str(error)
            .unwrap_or_else(|e| panic!("the error must be a typed reason ({e}): {error}")),
        other => panic!("expected a FAILED run, got {other:?}"),
    }
}

/// The last event is the `WorkflowFailed` the Postgres worker writes.
fn assert_failed_event(rt: &SqliteRuntime, exec: ExecutionId, reason: &HistoryCapReason) {
    let history = rt.load_history(exec).unwrap();
    let stored = serde_json::to_string(history.last().unwrap()).unwrap();
    let postgres =
        serde_json::to_string(&WorkflowEvent::workflow_failed(reason.to_string())).unwrap();
    assert_eq!(
        stored, postgres,
        "the event must match the Postgres cap failure byte for byte"
    );
    assert!(
        matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Failed(ref e) if e == &reason.to_string())
    );
}

#[test]
fn the_default_caps_match_the_postgres_worker() {
    let rt = SqliteRuntime::open_in_memory().unwrap();

    assert_eq!(
        rt.history_event_hard_cap(),
        Some(DEFAULT_HISTORY_EVENT_HARD_CAP)
    );
    assert_eq!(
        rt.history_byte_hard_cap(),
        Some(DEFAULT_HISTORY_BYTE_HARD_CAP)
    );
}

// ── AC3: a run past the event cap fails with `HistoryCapExceeded` ─────────

#[tokio::test]
async fn a_run_past_the_event_cap_fails_with_history_cap_exceeded() {
    let mut rt = runtime();
    rt.set_history_event_hard_cap(Some(10));
    let exec = rt.start_workflow("endless_loop", json!(0)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();

    let reason = failure_reason(&state);
    match &reason {
        HistoryCapReason::HistoryCapExceeded {
            count,
            cap,
            workflow_type,
        } => {
            assert_eq!(*cap, 10);
            assert_eq!(*count, 10, "the decision that adds event 10 breaches");
            assert_eq!(workflow_type, "endless_loop");
        }
        other @ HistoryCapReason::HistoryBytesCapExceeded { .. } => {
            panic!("expected HistoryCapExceeded, got {other:?}")
        }
    }
    assert_failed_event(&rt, exec, &reason);
    // The breaching `ActivityScheduled` rolled back: 9 events, then the failure.
    let history = rt.load_history(exec).unwrap();
    assert_eq!(history.len(), 10);
    assert!(matches!(
        history[8],
        WorkflowEvent::ActivityCompleted { .. }
    ));
    // A later drive returns the stored failure and runs nothing.
    let again = rt.run_until_blocked(exec).await.unwrap();
    assert_eq!(failure_reason(&again), reason);
}

#[tokio::test]
async fn a_run_past_the_byte_cap_fails_with_history_bytes_cap_exceeded() {
    let mut rt = runtime();
    rt.set_history_byte_hard_cap(Some(2_000));
    let exec = rt.start_workflow("endless_loop", json!(0)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();

    let reason = failure_reason(&state);
    match &reason {
        HistoryCapReason::HistoryBytesCapExceeded {
            bytes,
            cap,
            workflow_type,
        } => {
            assert_eq!(*cap, 2_000);
            assert!(*bytes >= 2_000, "bytes {bytes} must reach the cap");
            assert_eq!(workflow_type, "endless_loop");
        }
        other @ HistoryCapReason::HistoryCapExceeded { .. } => {
            panic!("expected HistoryBytesCapExceeded, got {other:?}")
        }
    }
    assert_failed_event(&rt, exec, &reason);
}

#[tokio::test]
async fn a_continuing_run_is_exempt_from_the_cap() {
    let mut rt = runtime();
    // Phase 0 holds 9 events when it continues: one start and four pairs.
    rt.set_history_event_hard_cap(Some(9));
    let first = rt
        .start_workflow("four_steps_then_continue", json!(0))
        .unwrap();

    let state = rt.run_until_blocked(first).await.unwrap();

    let RunState::ContinuedAsNew(second) = state else {
        panic!("a run at the cap may still continue as new, got {state:?}");
    };
    let done = rt.run_until_blocked(second).await.unwrap();
    assert!(
        matches!(done, RunState::Completed(ref v) if v == &json!(1)),
        "{done:?}"
    );
}

#[tokio::test]
async fn a_continuing_run_is_exempt_from_the_byte_cap() {
    // Measure the bytes stored when the hand-off decision starts.
    let mut probe = runtime();
    let first = probe
        .start_workflow("four_steps_then_continue", json!(0))
        .unwrap();
    probe.run_until_blocked(first).await.unwrap();
    let history = probe.load_history(first).unwrap();
    let before_hand_off: usize = history[..history.len() - 1]
        .iter()
        .map(|e| serde_json::to_string(e).unwrap().len())
        .sum();
    // One step stores about 600 bytes, so only the hand-off decision is at the
    // cap. The slack absorbs a timestamp of another length.
    let cap = u64::try_from(before_hand_off).unwrap() - 50;

    let mut rt = runtime();
    rt.set_history_byte_hard_cap(Some(cap));
    let first = rt
        .start_workflow("four_steps_then_continue", json!(0))
        .unwrap();
    let state = rt.run_until_blocked(first).await.unwrap();

    assert!(matches!(state, RunState::ContinuedAsNew(_)), "{state:?}");
}

#[tokio::test]
async fn a_completing_run_at_the_cap_fails_and_its_rows_go() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("caps.db");
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&four_steps_then_complete_info());
    rt.register_activity_raw("step", ActivitySpec::new(1, |_: Value| Ok(json!(null))));
    // The completing decision holds 10 events before its terminal event. One
    // of them is the staged signal, which the first decision appends (#2020).
    rt.set_history_event_hard_cap(Some(10));
    let exec = rt
        .start_workflow("four_steps_then_complete", json!(0))
        .unwrap();
    rt.send_signal(exec, "never_awaited", json!(null)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();

    let reason = failure_reason(&state);
    assert_eq!(
        reason,
        HistoryCapReason::HistoryCapExceeded {
            count: 10,
            cap: 10,
            workflow_type: "four_steps_then_complete".into(),
        }
    );
    assert_failed_event(&rt, exec, &reason);
    assert_eq!(rt.load_history(exec).unwrap().len(), 11);
    let raw = rusqlite::Connection::open(&path).unwrap();
    let staged: i64 = raw
        .query_row(
            "SELECT COUNT(*) FROM harvest_signals WHERE exec_id = ?1 AND delivered = 0",
            rusqlite::params![exec.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        staged, 0,
        "the signal is in history, and no row stays staged"
    );

    // Without the signal, the same cap leaves room for the run to complete.
    let mut rt = runtime();
    rt.set_history_event_hard_cap(Some(10));
    let exec = rt
        .start_workflow("four_steps_then_complete", json!(0))
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(4)),
        "{state:?}"
    );
}

#[tokio::test]
async fn an_unlimited_cap_never_fails_the_run() {
    let mut rt = runtime();
    // A cap of 5 would fail phase 0. `None` removes it.
    rt.set_history_event_hard_cap(Some(5));
    rt.set_history_event_hard_cap(None);
    rt.set_history_byte_hard_cap(Some(5));
    rt.set_history_byte_hard_cap(None);
    let exec = rt
        .start_workflow("four_steps_then_continue", json!(0))
        .unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(state, RunState::ContinuedAsNew(_)), "{state:?}");
}

// ── The reason is byte-identical to the Postgres `DeadLetterReason` ───────

#[test]
fn the_reason_json_matches_the_postgres_dead_letter_reason() {
    let events = HistoryCapReason::HistoryCapExceeded {
        count: 12_001,
        cap: 12_000,
        workflow_type: "billing_poll".into(),
    };
    let bytes = HistoryCapReason::HistoryBytesCapExceeded {
        bytes: 52_428_801,
        cap: 52_428_800,
        workflow_type: "billing_poll".into(),
    };

    assert_eq!(
        events.to_string(),
        r#"{"type":"HistoryCapExceeded","count":12001,"cap":12000,"workflow_type":"billing_poll"}"#
    );
    assert_eq!(
        bytes.to_string(),
        r#"{"type":"HistoryBytesCapExceeded","bytes":52428801,"cap":52428800,"workflow_type":"billing_poll"}"#
    );
    assert_eq!(events.type_tag(), "HistoryCapExceeded");
    assert_eq!(bytes.type_tag(), "HistoryBytesCapExceeded");
}
