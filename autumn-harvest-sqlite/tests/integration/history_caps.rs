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

/// A step result of about 500 bytes, so the byte cap is reached fast.
fn runtime() -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&endless_loop_info());
    rt.register_workflow(&four_steps_then_continue_info());
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

/// The last event is a typed `WorkflowFailed` that carries `reason`.
fn assert_failed_event(rt: &SqliteRuntime, exec: ExecutionId, reason: &HistoryCapReason) {
    match rt.load_history(exec).unwrap().last() {
        Some(WorkflowEvent::WorkflowFailed {
            error,
            error_type,
            non_retryable,
            ..
        }) => {
            assert_eq!(error, &reason.to_string());
            assert_eq!(error_type.as_deref(), Some(reason.type_tag()));
            assert_eq!(*non_retryable, Some(true));
        }
        other => panic!("expected a typed WorkflowFailed, got {other:?}"),
    }
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
            assert!(*count >= 10, "count {count} must reach the cap");
            assert_eq!(workflow_type, "endless_loop");
        }
        other @ HistoryCapReason::HistoryBytesCapExceeded { .. } => {
            panic!("expected HistoryCapExceeded, got {other:?}")
        }
    }
    assert_failed_event(&rt, exec, &reason);
    // The breaching decision rolled back, so the history stops near the cap.
    assert!(rt.load_history(exec).unwrap().len() <= 11);
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
