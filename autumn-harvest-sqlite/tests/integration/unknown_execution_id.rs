//! Issue #1735: every public accessor that takes an `ExecutionId` reports an
//! unknown id as `SqliteError::ExecutionNotFound(exec)`.
//!
//! Before the fix, only `outcome` and `send_signal` did. `run_until_blocked`
//! leaked `Sqlite(QueryReturnedNoRows)`, which names no id. `load_history`
//! and `activity_attempts` returned `Ok(vec![])`, which a caller cannot tell
//! apart from a real run with no rows.

// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{RunState, SqliteError, SqliteResult, SqliteRuntime};
use chrono::Utc;
use serde_json::json;

/// Blocks on a signal, so the run is live but schedules no activity.
#[workflow]
async fn parked(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Asserts that `result` is `ExecutionNotFound` for `bogus`, and that its
/// message names the id.
fn assert_not_found<T: std::fmt::Debug>(what: &str, result: SqliteResult<T>, bogus: ExecutionId) {
    let err = result.expect_err(&format!("{what} must reject an unknown id"));
    assert!(
        matches!(err, SqliteError::ExecutionNotFound(id) if id == bogus),
        "{what} must return ExecutionNotFound({bogus}), got: {err:?}"
    );
    assert!(
        err.to_string().contains(&bogus.to_string()),
        "{what} error must name the id, got: {err}"
    );
}

#[tokio::test]
async fn run_until_blocked_on_unknown_id_is_execution_not_found() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let bogus = ExecutionId::new();

    let result = rt.run_until_blocked(bogus).await;
    assert_not_found("run_until_blocked", result, bogus);
}

#[tokio::test]
async fn run_until_blocked_as_of_on_unknown_id_is_execution_not_found() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let bogus = ExecutionId::new();

    let result = rt.run_until_blocked_as_of(bogus, Utc::now()).await;
    assert_not_found("run_until_blocked_as_of", result, bogus);
}

#[test]
fn load_history_on_unknown_id_is_execution_not_found() {
    let rt = SqliteRuntime::open_in_memory().unwrap();
    let bogus = ExecutionId::new();

    let result = rt.load_history(bogus);
    assert_not_found("load_history", result, bogus);
}

#[test]
fn activity_attempts_on_unknown_id_is_execution_not_found() {
    let rt = SqliteRuntime::open_in_memory().unwrap();
    let bogus = ExecutionId::new();

    let result = rt.activity_attempts(bogus, "whatever");
    assert_not_found("activity_attempts", result, bogus);
}

/// `outcome` and `send_signal` already had the right shape. This test keeps
/// them on it.
#[tokio::test]
async fn outcome_and_send_signal_on_unknown_id_stay_execution_not_found() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let bogus = ExecutionId::new();

    let result = rt.outcome(bogus);
    assert_not_found("outcome", result, bogus);
    let result = rt.send_signal(bogus, "go", json!(true));
    assert_not_found("send_signal", result, bogus);
}

/// A known run with no attempts for a name is a legitimate empty result. The
/// fix must not turn it into an error.
#[tokio::test]
async fn known_id_reads_are_unchanged() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&parked_info());
    let exec = rt.start_workflow("parked", json!(0)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref name) if name == "go"),
        "got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        matches!(history.first(), Some(WorkflowEvent::WorkflowStarted { .. })),
        "a started run has WorkflowStarted first, got: {history:?}"
    );

    let attempts = rt.activity_attempts(exec, "never_scheduled").unwrap();
    assert!(attempts.is_empty(), "got: {attempts:?}");
}
