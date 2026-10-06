//! Issue #1834: an execution that reaches an unsupported feature ends `FAILED`.
//!
//! Before the fix, the backend rejected the command and left the run `RUNNING`.
//! Every later drive re-ran the handler and failed again, forever. Now the
//! drive that meets the feature seals the run `FAILED`. The terminal
//! `WorkflowFailed` event carries a typed reason:
//!
//! - `error_type`: [`UNSUPPORTED_FEATURE_ERROR_TYPE`].
//! - `details.feature`: the rejected command or field.
//! - `non_retryable`: `true`. A retry meets the same feature again.
//!
//! The sealing drive still returns `SqliteError::Unsupported`, so the caller
//! sees the reason at once. A later drive returns `RunState::Failed`.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]
// `upserts_then_completes` and `healthy` hold no `.await`; each ends in one cycle.
#![allow(clippy::unused_async)]

use std::sync::atomic::{AtomicU32, Ordering};

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{
    ActivitySpec, ExecutionOutcome, RunState, SqliteError, SqliteRuntime,
    UNSUPPORTED_FEATURE_ERROR_TYPE,
};
use serde_json::json;

/// Counts runs of `spawns_child_counted` only. Other tests run in parallel in
/// this binary, so they use `spawns_child` and never touch this counter.
static COUNTED_RUNS: AtomicU32 = AtomicU32::new(0);

#[workflow]
async fn spawns_child_counted(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    COUNTED_RUNS.fetch_add(1, Ordering::SeqCst);
    ctx.spawn_child_workflow_raw("child", json!({}))
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn spawns_child(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    // A side effect in the same cycle. The rejected cycle must not keep it.
    let _id = ctx.new_uuid();
    ctx.spawn_child_workflow_raw("child", json!({}))
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn continues_as_new(ctx: &WorkflowContext, n: i64) -> Result<(), String> {
    ctx.continue_as_new(json!(n + 1))
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn uses_a_session(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    let session = ctx
        .create_session(SessionOptions::new("gpu"))
        .await
        .map_err(|e| e.to_string())?;
    session.complete().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Completes, but its last cycle also emits a search-attribute upsert. The
/// terminal drain rejects that command.
#[workflow]
async fn upserts_then_completes(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.upsert_search_attrs([("stage".to_string(), Some(json!("done")))])
        .map_err(|e| e.to_string())?;
    Ok(n)
}

#[workflow]
async fn calls_unregistered_activity(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let v = ctx
        .execute_activity_raw("not_registered_yet", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(v.as_i64().unwrap_or_default())
}

#[workflow]
async fn healthy(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n * 2)
}

/// Assert the stored terminal state and the typed `WorkflowFailed` event.
fn assert_sealed_unsupported(rt: &SqliteRuntime, exec: ExecutionId, feature: &str) {
    match rt.outcome(exec).unwrap() {
        ExecutionOutcome::Failed(message) => assert!(
            message.contains(feature),
            "the stored error must name `{feature}`: {message}"
        ),
        other => panic!("expected a FAILED execution, got {other:?}"),
    }
    let history = rt.load_history(exec).unwrap();
    match history.last() {
        Some(WorkflowEvent::WorkflowFailed {
            error,
            error_type,
            details,
            non_retryable,
        }) => {
            assert!(error.contains(feature), "event error: {error}");
            assert_eq!(error_type.as_deref(), Some(UNSUPPORTED_FEATURE_ERROR_TYPE));
            let named = details
                .as_ref()
                .and_then(|d| d.get("feature"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            assert_eq!(named, feature, "details.feature is a stable token");
            assert_eq!(*non_retryable, Some(true));
        }
        other => panic!("the last event must be a typed WorkflowFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn a_child_workflow_ends_failed_with_a_typed_reason() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&spawns_child_info());
    let exec = rt.start_workflow("spawns_child", json!(0)).unwrap();

    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::Unsupported(ref c) if c == "StartChildWorkflow"),
        "the sealing drive still reports the reason: {err}"
    );
    assert_sealed_unsupported(&rt, exec, "StartChildWorkflow");

    // The rejected cycle rolled back as a whole: no side effect survives.
    let history = rt.load_history(exec).unwrap();
    assert_eq!(history.len(), 2, "{history:?}");
    assert!(matches!(history[0], WorkflowEvent::WorkflowStarted { .. }));
}

#[tokio::test]
async fn a_sealed_execution_is_not_driven_again() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&spawns_child_counted_info());
    let exec = rt.start_workflow("spawns_child_counted", json!(0)).unwrap();
    let _ = rt.run_until_blocked(exec).await.unwrap_err();
    let runs = COUNTED_RUNS.load(Ordering::SeqCst);

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(ref m) if m.contains("StartChildWorkflow")),
        "a later drive reports the terminal state: {state:?}"
    );
    assert_eq!(
        COUNTED_RUNS.load(Ordering::SeqCst),
        runs,
        "a FAILED run must not re-run its handler"
    );
}

#[tokio::test]
async fn continue_as_new_ends_failed_with_a_typed_reason() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&continues_as_new_info());
    let exec = rt.start_workflow("continues_as_new", json!(0)).unwrap();

    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(matches!(err, SqliteError::Unsupported(_)), "{err}");
    assert_sealed_unsupported(&rt, exec, "ContinueAsNew");
}

#[tokio::test]
async fn a_worker_session_ends_failed_with_a_typed_reason() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&uses_a_session_info());
    let exec = rt.start_workflow("uses_a_session", json!(0)).unwrap();

    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(matches!(err, SqliteError::Unsupported(_)), "{err}");
    assert_sealed_unsupported(&rt, exec, "ScheduleActivity.schedule_to_start_override");
}

#[tokio::test]
async fn an_unsupported_command_in_the_terminal_cycle_ends_failed() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&upserts_then_completes_info());
    let exec = rt
        .start_workflow("upserts_then_completes", json!(7))
        .unwrap();

    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(matches!(err, SqliteError::Unsupported(_)), "{err}");
    assert_sealed_unsupported(&rt, exec, "UpsertSearchAttributes");
    assert!(
        !rt.load_history(exec)
            .unwrap()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. })),
        "the rolled-back terminal cycle must not leave a WorkflowCompleted"
    );
}

#[tokio::test]
async fn the_fleet_stops_reporting_a_sealed_execution() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&spawns_child_info());
    rt.register_workflow(&healthy_info());
    let broken = rt.start_workflow("spawns_child", json!(0)).unwrap();
    let ok = rt.start_workflow("healthy", json!(4)).unwrap();

    let err = rt.poll_once().await.unwrap_err();
    assert!(matches!(err, SqliteError::Unsupported(_)), "{err}");
    assert_sealed_unsupported(&rt, broken, "StartChildWorkflow");
    assert!(matches!(
        rt.outcome(ok).unwrap(),
        ExecutionOutcome::Completed(ref v) if v.as_i64() == Some(8)
    ));

    rt.run_until_idle()
        .await
        .expect("a sealed execution is terminal, so the fleet is clean");
}

#[tokio::test]
async fn the_sealed_state_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&spawns_child_info());
        let exec = rt.start_workflow("spawns_child", json!(0)).unwrap();
        let _ = rt.run_until_blocked(exec).await.unwrap_err();
        exec
    };

    let rt = SqliteRuntime::open(&path).unwrap();
    assert_sealed_unsupported(&rt, exec, "StartChildWorkflow");
}

/// Only an unsupported feature seals. A missing registration is fixable in
/// the same runtime, so it must leave the run `RUNNING`.
#[tokio::test]
async fn an_unregistered_activity_does_not_seal_the_run() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&calls_unregistered_activity_info());
    let exec = rt
        .start_workflow("calls_unregistered_activity", json!(1))
        .unwrap();

    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(matches!(err, SqliteError::UnregisteredActivity(_)), "{err}");
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Running
    ));

    rt.register_activity_raw(
        "not_registered_yet",
        ActivitySpec::new(1, |v| Ok(json!(v.as_i64().unwrap_or_default() + 1))),
    );
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(2)));
}

/// Races a quick activity against a long timer. The activity wins, and the
/// same cycle meets an unsupported child. The cycle's loser cancellation rolls
/// back with it, so only the seal can remove the timer row.
#[workflow]
async fn races_then_spawns_child(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    ctx.race()
        .activity_raw("quick", json!(1), "default")
        .timer(std::time::Duration::from_secs(3600))
        .run()
        .await
        .map_err(|e| e.to_string())?;
    ctx.spawn_child_workflow_raw("child", json!({}))
        .await
        .map(drop)
        .map_err(|e| e.to_string())
}

/// A sealed run is terminal, and this backend has no retention pass. The seal
/// therefore removes the run's PENDING tasks, unfired timers and staged
/// signals, as the `TerminateIfRunning` seal does. Nothing can ever use them.
#[tokio::test]
async fn sealing_removes_the_runs_outstanding_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&races_then_spawns_child_info());
    rt.register_activity_raw("quick", ActivitySpec::new(1, Ok));
    let exec = rt
        .start_workflow("races_then_spawns_child", json!(0))
        .unwrap();

    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(matches!(err, SqliteError::Unsupported(_)), "{err}");
    assert_sealed_unsupported(&rt, exec, "StartChildWorkflow");

    let inspector = rusqlite::Connection::open(&path).unwrap();
    let count = |sql: &str| -> i64 {
        inspector
            .query_row(sql, [exec.to_string()], |row| row.get(0))
            .unwrap()
    };
    assert_eq!(
        count("SELECT COUNT(*) FROM harvest_timers WHERE exec_id = ?1 AND fired = 0"),
        0,
        "the lost race's timer must not outlive the sealed run"
    );
    assert_eq!(
        count("SELECT COUNT(*) FROM harvest_tasks WHERE exec_id = ?1 AND state = 'PENDING'"),
        0
    );
}
