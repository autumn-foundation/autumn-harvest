//! Workflow updates on the single-writer backend (issue #2025).
//!
//! `SqliteRuntime::update` replays the run to rebuild its handlers. It runs
//! the validator, then the handler. It writes `UpdateAdmitted` and the result
//! in one transaction. A rejection writes no event.

// The `#[workflow]` and `#[update]` macros reference their input parameters,
// and need an `async fn` with a `Result` return.
#![allow(
    clippy::used_underscore_binding,
    clippy::unused_async,
    clippy::unnecessary_wraps
)]

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};
use autumn_harvest::{UpdateId, WorkflowHandlerFn};
use autumn_harvest_sqlite::{
    ExecutionOutcome, RunState, SqliteError, SqliteRuntime, UNSUPPORTED_FEATURE_ERROR_TYPE,
};
use serde_json::{Value, json};

// ─────────────────────────────── fixtures ────────────────────────────────────

/// Registers `set_limit`, then waits for `go`. The validator accepts a
/// positive number only. A limit of 13 makes the handler fail.
#[workflow]
async fn limit_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_update_handler(
        "set_limit",
        |input: &Value| match input.as_i64() {
            Some(n) if n > 0 => Ok(()),
            _ => Err("limit must be a positive number".to_string()),
        },
        |input: Value| async move {
            if input == json!(13) {
                return Err("limit 13 is unlucky".to_string());
            }
            Ok(json!({ "limit": input }))
        },
    );
    ctx.register_update_handler_no_validator("hang", |_input: Value| async move {
        std::future::pending::<Result<Value, String>>().await
    });
    ctx.register_update_handler_no_validator("boom", |_input: Value| async move {
        panic!("boom handler")
    });
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Waits for `go`. Declarative handlers serve its updates.
#[workflow]
async fn declarative_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Owns the foreign handler that the workflow-name filter must skip.
#[workflow]
async fn other_wf(_ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    Ok(Value::Null)
}

fn validate_rename(input: &Value) -> Result<(), String> {
    if input.as_str().is_some_and(|s| !s.is_empty()) {
        Ok(())
    } else {
        Err("name must not be empty".to_string())
    }
}

#[update(workflow = "declarative_wf", validator = validate_rename)]
async fn rename(_ctx: &WorkflowContext, name: String) -> Result<String, String> {
    Ok(format!("renamed to {name}"))
}

#[update(workflow = "other_wf")]
async fn foreign_rename(_ctx: &WorkflowContext, name: String) -> Result<String, String> {
    Ok(name)
}

/// A fixed id, so the body-driven update replays deterministically.
const BODY_UPDATE_ID: uuid::Uuid = uuid::Uuid::from_u128(0x2025_0000_0000_7000_8000_0000_0000_0001);

/// Runs a body-driven update, then waits for `go`. The `RecordUpdateResult`
/// rides on a suspending batch.
#[workflow]
async fn body_update_then_wait_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_update_handler_no_validator("double", |input: Value| async move {
        Ok(json!(input.as_i64().unwrap_or(0) * 2))
    });
    let doubled = ctx
        .execute_admitted_update(UpdateId::from_uuid(BODY_UPDATE_ID), "double", json!(21))
        .await?;
    let go = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(json!({ "doubled": doubled, "go": go }))
}

/// Runs a body-driven update, then returns. The `RecordUpdateResult` rides
/// on the terminal drain.
#[workflow]
async fn body_update_then_return_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_update_handler_no_validator("double", |input: Value| async move {
        Ok(json!(input.as_i64().unwrap_or(0) * 2))
    });
    ctx.execute_admitted_update(UpdateId::from_uuid(BODY_UPDATE_ID), "double", json!(4))
        .await
}

/// Registers `set_limit`, waits for `go`, then panics.
#[workflow]
async fn panic_after_go_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_update_handler_no_validator("set_limit", |input: Value| async move { Ok(input) });
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    panic!("panic after go");
}

/// A runtime with `limit_wf` started and blocked on `go`.
async fn blocked_limit_run() -> (SqliteRuntime, ExecutionId) {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&limit_wf_info());
    let exec = rt.start_workflow("limit_wf", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(ref s) if s == "go"));
    (rt, exec)
}

fn update_events(history: &[WorkflowEvent]) -> Vec<&WorkflowEvent> {
    history
        .iter()
        .filter(|e| {
            matches!(
                e,
                WorkflowEvent::UpdateAdmitted { .. }
                    | WorkflowEvent::UpdateCompleted { .. }
                    | WorkflowEvent::UpdateFailed { .. }
            )
        })
        .collect()
}

/// Replay a terminal history in strict mode.
async fn assert_replays(name: &str, handler: WorkflowHandlerFn, history: Vec<WorkflowEvent>) {
    let report = WorkflowReplayer::new()
        .register_fn(name, handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the history must replay on the core:\n{report}"
    );
}

/// Replay an open history in canary mode. Strict mode reports every
/// in-flight run as an early completion.
async fn assert_open_history_replays(
    name: &str,
    handler: WorkflowHandlerFn,
    exec: ExecutionId,
    history: Vec<WorkflowEvent>,
) {
    let report = WorkflowReplayer::new()
        .register_fn(name, handler)
        .replay_canary_snapshot(HistorySnapshot {
            workflow_name: name.to_string(),
            execution_id: exec,
            events: history,
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
            queue_name: None,
        })
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the open history must replay on the core:\n{report}"
    );
}

// ────────────────────────── acceptance criteria ──────────────────────────────

/// AC 1: an accepted update returns its result and is in history.
#[tokio::test]
async fn accepted_update_returns_its_result_and_is_in_history() {
    let (mut rt, exec) = blocked_limit_run().await;

    let result = rt.update(exec, "set_limit", json!(5)).await.unwrap();
    assert_eq!(result, json!({ "limit": 5 }));

    let history = rt.load_history(exec).unwrap();
    let updates = update_events(&history);
    assert_eq!(updates.len(), 2, "admitted and completed:\n{history:?}");
    let WorkflowEvent::UpdateAdmitted {
        update_id: admitted,
        name,
        input,
        ..
    } = updates[0]
    else {
        panic!(
            "first update event must be UpdateAdmitted: {:?}",
            updates[0]
        );
    };
    assert_eq!(name, "set_limit");
    assert_eq!(input, &json!(5));
    let WorkflowEvent::UpdateCompleted { update_id, output } = updates[1] else {
        panic!(
            "second update event must be UpdateCompleted: {:?}",
            updates[1]
        );
    };
    assert_eq!(update_id, admitted, "the result pairs with the admission");
    assert_eq!(output, &json!({ "limit": 5 }));

    // The update does not drive the run. The run continues as before.
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Running
    ));
    rt.send_signal(exec, "go", json!("ok")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v == &json!("ok")));
}

/// AC 2: a rejected update (validator fails) writes no event.
#[tokio::test]
async fn rejected_update_writes_no_event() {
    let (mut rt, exec) = blocked_limit_run().await;
    let before = rt.load_history(exec).unwrap();

    let err = rt.update(exec, "set_limit", json!(-1)).await.unwrap_err();
    match err {
        SqliteError::UpdateRejected { name, reason, .. } => {
            assert_eq!(name, "set_limit");
            assert_eq!(reason, "limit must be a positive number");
        }
        other => panic!("expected UpdateRejected, got {other:?}"),
    }

    let after = rt.load_history(exec).unwrap();
    assert_eq!(
        serde_json::to_value(&before).unwrap(),
        serde_json::to_value(&after).unwrap(),
        "a rejected update must not change history"
    );
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Running
    ));
}

/// AC 3: the history replays on the core `WorkflowReplayer`.
#[tokio::test]
async fn update_history_replays_on_the_core_replayer() {
    let (mut rt, exec) = blocked_limit_run().await;
    rt.update(exec, "set_limit", json!(5)).await.unwrap();
    rt.update(exec, "set_limit", json!(13)).await.unwrap_err();

    // Mid-run: the open history replays.
    assert_open_history_replays(
        "limit_wf",
        limit_wf_info().handler,
        exec,
        rt.load_history(exec).unwrap(),
    )
    .await;

    // Terminal: the completed history replays too.
    rt.send_signal(exec, "go", json!("ok")).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let history = rt.load_history(exec).unwrap();
    assert_eq!(update_events(&history).len(), 4, "{history:?}");
    assert_replays("limit_wf", limit_wf_info().handler, history).await;
}

// ───────────────────────────── handler outcomes ──────────────────────────────

/// A handler error records `UpdateFailed` and returns `UpdateFailed`.
#[tokio::test]
async fn handler_error_records_update_failed() {
    let (mut rt, exec) = blocked_limit_run().await;

    let err = rt.update(exec, "set_limit", json!(13)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UpdateFailed { ref error, .. } if error == "limit 13 is unlucky"),
        "{err:?}"
    );

    let history = rt.load_history(exec).unwrap();
    let updates = update_events(&history);
    assert_eq!(updates.len(), 2, "{history:?}");
    assert!(matches!(
        updates[1],
        WorkflowEvent::UpdateFailed { error, .. } if error == "limit 13 is unlucky"
    ));
}

/// A handler panic is contained and recorded as `UpdateFailed`.
#[tokio::test]
async fn handler_panic_records_update_failed() {
    let (mut rt, exec) = blocked_limit_run().await;

    let err = rt.update(exec, "boom", json!(null)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UpdateFailed { ref error, .. } if error.contains("boom handler")),
        "{err:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert!(matches!(
        update_events(&history)[..],
        [
            WorkflowEvent::UpdateAdmitted { .. },
            WorkflowEvent::UpdateFailed { .. }
        ]
    ));
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Running
    ));
}

/// A handler that never finishes returns `UpdateBlocked` and writes no event.
#[tokio::test]
async fn blocked_handler_writes_no_event() {
    let (mut rt, exec) = blocked_limit_run().await;
    let before = rt.load_history(exec).unwrap().len();

    let err = rt.update(exec, "hang", json!(null)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UpdateBlocked { ref name, .. } if name == "hang"),
        "{err:?}"
    );
    assert_eq!(rt.load_history(exec).unwrap().len(), before);
}

// ───────────────────────────── ingress checks ────────────────────────────────

/// An unknown update name returns `UpdateHandlerNotFound` and writes no event.
#[tokio::test]
async fn unknown_update_name_writes_no_event() {
    let (mut rt, exec) = blocked_limit_run().await;
    let before = rt.load_history(exec).unwrap().len();

    let err = rt.update(exec, "missing", json!(1)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UpdateHandlerNotFound { ref name, .. } if name == "missing"),
        "{err:?}"
    );
    assert_eq!(rt.load_history(exec).unwrap().len(), before);
}

/// An unknown id returns `ExecutionNotFound`.
#[tokio::test]
async fn update_to_unknown_execution_is_not_found() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&limit_wf_info());
    let ghost = ExecutionId::new();
    let err = rt.update(ghost, "set_limit", json!(1)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::ExecutionNotFound(id) if id == ghost),
        "{err:?}"
    );
}

/// A terminal run returns `WorkflowNotRunning` and writes no event.
#[tokio::test]
async fn update_to_terminal_run_is_rejected() {
    let (mut rt, exec) = blocked_limit_run().await;
    rt.send_signal(exec, "go", json!("ok")).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let before = rt.load_history(exec).unwrap().len();

    let err = rt.update(exec, "set_limit", json!(1)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::WorkflowNotRunning { ref state, .. } if state == "COMPLETED"),
        "{err:?}"
    );
    assert_eq!(rt.load_history(exec).unwrap().len(), before);
}

/// A workflow panic during the state rebuild is contained and writes no
/// event.
#[tokio::test]
async fn rebuild_panic_is_contained() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&panic_after_go_wf_info());
    let exec = rt.start_workflow("panic_after_go_wf", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    rt.send_signal(exec, "go", json!(1)).unwrap();
    let drive = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(
        matches!(drive, SqliteError::WorkflowPanicked { .. }),
        "{drive:?}"
    );
    let before = rt.load_history(exec).unwrap().len();

    let err = rt.update(exec, "set_limit", json!(1)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::WorkflowPanicked { .. }),
        "{err:?}"
    );
    assert_eq!(rt.load_history(exec).unwrap().len(), before);
}

// ──────────────────────────── declarative handlers ───────────────────────────

/// A declarative `#[update]` runs its validator and its handler.
#[tokio::test]
async fn declarative_update_runs_validator_and_handler() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&declarative_wf_info());
    rt.register_workflow(&other_wf_info());
    rt.register_update(&rename_info());
    rt.register_update(&foreign_rename_info());
    let exec = rt.start_workflow("declarative_wf", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let ok = rt.update(exec, "rename", json!("bob")).await.unwrap();
    assert_eq!(ok, json!("renamed to bob"));

    let err = rt.update(exec, "rename", json!("")).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UpdateRejected { ref reason, .. } if reason == "name must not be empty"),
        "{err:?}"
    );

    // A handler of another workflow type does not answer.
    let err = rt
        .update(exec, "foreign_rename", json!("x"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, SqliteError::UpdateHandlerNotFound { .. }),
        "{err:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert_eq!(update_events(&history).len(), 2, "{history:?}");
    rt.send_signal(exec, "go", json!("done")).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    assert_replays(
        "declarative_wf",
        declarative_wf_info().handler,
        rt.load_history(exec).unwrap(),
    )
    .await;
}

// ──────────────────────────────── durability ─────────────────────────────────

/// An update result survives a reopen, and the run then completes.
#[tokio::test]
async fn update_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&limit_wf_info());
        let exec = rt.start_workflow("limit_wf", json!(0)).unwrap();
        rt.run_until_blocked(exec).await.unwrap();
        assert_eq!(
            rt.update(exec, "set_limit", json!(7)).await.unwrap(),
            json!({ "limit": 7 })
        );
        exec
    };

    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&limit_wf_info());
    assert_eq!(update_events(&rt.load_history(exec).unwrap()).len(), 2);
    rt.send_signal(exec, "go", json!("ok")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
}

// ───────────────────── RecordUpdateResult in the decision cycle ──────────────

/// A body-driven update on a suspending batch is persisted. It no longer
/// seals the run as `UnsupportedFeature`.
#[tokio::test]
async fn record_update_result_on_suspend_path_is_persisted() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&body_update_then_wait_wf_info());
    let exec = rt
        .start_workflow("body_update_then_wait_wf", json!(0))
        .unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "go"),
        "{state:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::UpdateCompleted { update_id, output }
                if *update_id == UpdateId::from_uuid(BODY_UPDATE_ID) && output == &json!(42)
        )),
        "{history:?}"
    );

    rt.send_signal(exec, "go", json!("ok")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({ "doubled": 42, "go": "ok" })),
        "{state:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        update_events(&history).len(),
        1,
        "recorded once:\n{history:?}"
    );
    assert_replays(
        "body_update_then_wait_wf",
        body_update_then_wait_wf_info().handler,
        history,
    )
    .await;
}

/// A body-driven update on the terminal drain is persisted before the
/// terminal event.
#[tokio::test]
async fn record_update_result_on_terminal_drain_is_persisted() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&body_update_then_return_wf_info());
    let exec = rt
        .start_workflow("body_update_then_return_wf", json!(0))
        .unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(8)),
        "{state:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert!(
        !history.iter().any(|e| matches!(
            e,
            WorkflowEvent::WorkflowFailed { error_type: Some(t), .. }
                if t == UNSUPPORTED_FEATURE_ERROR_TYPE
        )),
        "{history:?}"
    );
    let n = history.len();
    assert!(matches!(
        history[n - 1],
        WorkflowEvent::WorkflowCompleted { .. }
    ));
    assert!(
        matches!(history[n - 2], WorkflowEvent::UpdateCompleted { ref output, .. } if output == &json!(8)),
        "{history:?}"
    );
    assert_replays(
        "body_update_then_return_wf",
        body_update_then_return_wf_info().handler,
        history,
    )
    .await;
}
