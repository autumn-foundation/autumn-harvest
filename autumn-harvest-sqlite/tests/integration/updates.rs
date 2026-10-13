//! Workflow updates on the single-writer backend (issue #2025).
//!
//! `SqliteRuntime::update` replays the run to rebuild its handlers. It runs
//! the validator, then the handler. It writes `UpdateAdmitted` and the result
//! in one transaction. A rejection writes no event.
//!
//! No Docker: `SQLite` is embedded (`rusqlite` `bundled`).

// The `#[workflow]` and `#[update]` macros reference their input parameters,
// and need an `async fn` with a `Result` return.
#![allow(
    clippy::used_underscore_binding,
    clippy::unused_async,
    clippy::unnecessary_wraps
)]

use autumn_harvest::prelude::*;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;

use autumn_harvest::testing::{
    HistorySnapshot, ReplayStatus, TestRunStatus, WorkflowReplayer, WorkflowTestEnv,
};
use autumn_harvest::{UpdateId, WorkflowHandlerFn, WorkflowIdReusePolicy};
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
    ctx.register_update_handler_no_validator("later", |_input: Value| woken_later());
    ctx.register_update_handler_no_validator("yielder", |_input: Value| yield_then_finish());
    ctx.register_update_handler(
        "fragile",
        |_input: &Value| panic!("validator boom"),
        |input: Value| async move { Ok(input) },
    );
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Registers `status`, which returns the cancel reason that the body saw.
#[workflow]
async fn cancel_aware_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let reason = ctx.cancellation_reason().map(str::to_string);
    ctx.register_update_handler_no_validator("status", move |_input: Value| {
        let reason = reason.clone();
        async move { Ok(json!(reason)) }
    });
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// A handler body that a separate thread wakes after 20 ms. It stands for a
/// timer, I/O or an async lock.
fn woken_later() -> impl Future<Output = Result<Value, String>> + Send {
    let mut started = false;
    let done = Arc::new(AtomicBool::new(false));
    std::future::poll_fn(move |cx| {
        if done.load(Ordering::Acquire) {
            return Poll::Ready(Ok(json!("woken")));
        }
        if !started {
            started = true;
            let done = Arc::clone(&done);
            let waker = cx.waker().clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                done.store(true, Ordering::Release);
                waker.wake();
            });
        }
        Poll::Pending
    })
}

/// Arms another timer than `timer_wf`. Registered under the name
/// `timer_wf`, it stands for a changed workflow build.
#[workflow]
async fn drifted_timer_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_update_handler_no_validator("set_limit", |input: Value| async move { Ok(input) });
    ctx.timer("other", 60).await.map_err(|e| e.to_string())?;
    Ok(json!("fired"))
}

/// A handler body that yields to the tokio scheduler once. Tokio defers that
/// wake, so the handler is `Pending` with no wake at first.
async fn yield_then_finish() -> Result<Value, String> {
    tokio::task::yield_now().await;
    Ok(json!("yielded"))
}

/// Mints a new update id on each cycle, then waits for `go`. The id is not
/// deterministic, so each replay runs the handler again.
#[workflow]
async fn fresh_update_id_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_update_handler_no_validator("echo", |input: Value| async move { Ok(input) });
    ctx.execute_admitted_update(UpdateId::new(), "echo", json!(1))
        .await?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Registers `set_limit`, then waits on a 60 second timer.
#[workflow]
async fn timer_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_update_handler_no_validator("set_limit", |input: Value| async move { Ok(input) });
    ctx.timer("t", 60).await.map_err(|e| e.to_string())?;
    Ok(json!("fired"))
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

/// Awaits a durable timer, which an update handler cannot do here.
#[update(workflow = "declarative_wf")]
async fn slow_rename(ctx: &WorkflowContext, name: String) -> Result<String, String> {
    ctx.timer("ht", 5).await.map_err(|e| e.to_string())?;
    Ok(name)
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

/// Runs a body-driven update whose handler fails, then waits for `go`.
#[workflow]
async fn body_update_err_then_wait_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_update_handler_no_validator("bad", |_input: Value| async move {
        Err::<Value, String>("nope".to_string())
    });
    let failed = ctx
        .execute_admitted_update(UpdateId::from_uuid(BODY_UPDATE_ID), "bad", json!(1))
        .await
        .unwrap_err();
    let go = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(json!({ "failed": failed, "go": go }))
}

/// Registers `set_limit`, waits on a 60 second timer, then panics. The
/// fired timer is in history, so every replay past it panics.
#[workflow]
async fn panic_after_timer_wf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_update_handler_no_validator("set_limit", |input: Value| async move { Ok(input) });
    ctx.timer("t", 60).await.map_err(|e| e.to_string())?;
    panic!("panic after the timer");
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

/// The serialized history, for a whole-history comparison.
fn snapshot(rt: &SqliteRuntime, exec: ExecutionId) -> Value {
    serde_json::to_value(rt.load_history(exec).unwrap()).unwrap()
}

/// Each update event as `(type, name, payload)`, with ids replaced by their
/// order of first use. It compares histories across runs and backends.
fn update_shape(history: &[WorkflowEvent]) -> Vec<(String, usize, Value)> {
    let mut ids: Vec<UpdateId> = Vec::new();
    let mut index = |id: &UpdateId| {
        ids.iter().position(|seen| seen == id).unwrap_or_else(|| {
            ids.push(*id);
            ids.len() - 1
        })
    };
    history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::UpdateAdmitted {
                update_id,
                name,
                input,
                ..
            } => Some((format!("Admitted:{name}"), index(update_id), input.clone())),
            WorkflowEvent::UpdateCompleted { update_id, output } => {
                Some(("Completed".to_string(), index(update_id), output.clone()))
            }
            WorkflowEvent::UpdateFailed { update_id, error } => {
                Some(("Failed".to_string(), index(update_id), json!(error)))
            }
            _ => None,
        })
        .collect()
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
    let before = snapshot(&rt, exec);

    let result = rt.update(exec, "set_limit", json!(5)).await.unwrap();
    assert_eq!(result, json!({ "limit": 5 }));

    let history = rt.load_history(exec).unwrap();
    let after = serde_json::to_value(&history).unwrap();
    let prefix = before.as_array().unwrap();
    assert_eq!(
        &after.as_array().unwrap()[..prefix.len()],
        &prefix[..],
        "the update only appends"
    );
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
    // Replay skips update events, so check their shape here: each result
    // pairs with its own admission, and the two ids differ.
    assert_eq!(
        update_shape(&history),
        vec![
            ("Admitted:set_limit".to_string(), 0, json!(5)),
            ("Completed".to_string(), 0, json!({ "limit": 5 })),
            ("Admitted:set_limit".to_string(), 1, json!(13)),
            ("Failed".to_string(), 1, json!("limit 13 is unlucky")),
        ],
        "{history:?}"
    );
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
    assert_eq!(
        update_shape(&history),
        vec![
            ("Admitted:set_limit".to_string(), 0, json!(13)),
            ("Failed".to_string(), 0, json!("limit 13 is unlucky")),
        ]
    );
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

/// A handler that never finishes returns `UpdateTimedOut` and writes no
/// event.
#[tokio::test]
async fn handler_that_never_finishes_times_out_and_writes_no_event() {
    let (mut rt, exec) = blocked_limit_run().await;
    rt.set_update_timeout(Duration::from_millis(100));
    let before = rt.load_history(exec).unwrap().len();

    let err = rt.update(exec, "hang", json!(null)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UpdateTimedOut { ref name, .. } if name == "hang"),
        "{err:?}"
    );
    assert_eq!(rt.load_history(exec).unwrap().len(), before);
}

/// A handler that a timer, I/O or an async lock wakes later still finishes.
#[tokio::test]
async fn handler_woken_asynchronously_finishes() {
    let (mut rt, exec) = blocked_limit_run().await;
    let result = rt.update(exec, "later", json!(null)).await.unwrap();
    assert_eq!(result, json!("woken"));
    assert_eq!(update_events(&rt.load_history(exec).unwrap()).len(), 2);
}

/// A handler that yields to the scheduler finishes, on the default
/// current-thread runtime too.
#[tokio::test]
async fn handler_that_yields_finishes() {
    let (mut rt, exec) = blocked_limit_run().await;
    assert_eq!(
        rt.update(exec, "yielder", json!(null)).await.unwrap(),
        json!("yielded")
    );
}

/// A declarative handler that awaits a durable timer times out. The timer of
/// the handler does not leak into history.
#[tokio::test]
async fn handler_awaiting_a_durable_command_times_out() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&declarative_wf_info());
    rt.register_update(&slow_rename_info());
    rt.set_update_timeout(Duration::from_millis(100));
    let exec = rt.start_workflow("declarative_wf", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let before = snapshot(&rt, exec);

    let err = rt
        .update(exec, "slow_rename", json!("x"))
        .await
        .unwrap_err();
    assert!(matches!(err, SqliteError::UpdateTimedOut { .. }), "{err:?}");
    assert_eq!(snapshot(&rt, exec), before, "no event, no leaked timer");
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
    rt.register_workflow(&panic_after_timer_wf_info());
    let exec = rt.start_workflow("panic_after_timer_wf", json!(0)).unwrap();
    let t0 = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    let drive = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(61))
        .await
        .unwrap_err();
    assert!(
        matches!(drive, SqliteError::WorkflowPanicked { .. }),
        "{drive:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "the panic point must be in stored history:\n{history:?}"
    );
    let before = history.len();

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

    let before = snapshot(&rt, exec);
    let err = rt.update(exec, "rename", json!("")).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UpdateRejected { ref reason, .. } if reason == "name must not be empty"),
        "{err:?}"
    );

    assert_eq!(snapshot(&rt, exec), before, "a rejection writes no event");

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
    assert_eq!(
        update_shape(&rt.load_history(exec).unwrap()),
        vec![
            ("Admitted:set_limit".to_string(), 0, json!(7)),
            ("Completed".to_string(), 0, json!({ "limit": 7 })),
        ]
    );
    rt.send_signal(exec, "go", json!("ok")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    assert_replays(
        "limit_wf",
        limit_wf_info().handler,
        rt.load_history(exec).unwrap(),
    )
    .await;
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

/// A body-driven update whose handler fails records `UpdateFailed` on the
/// suspend path.
#[tokio::test]
async fn record_update_result_error_on_suspend_path_is_persisted() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&body_update_err_then_wait_wf_info());
    let exec = rt
        .start_workflow("body_update_err_then_wait_wf", json!(0))
        .unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "{state:?}");
    assert_eq!(
        update_shape(&rt.load_history(exec).unwrap()),
        vec![("Failed".to_string(), 0, json!("nope"))]
    );

    rt.send_signal(exec, "go", json!("ok")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({ "failed": "nope", "go": "ok" })),
        "{state:?}"
    );
    assert_replays(
        "body_update_err_then_wait_wf",
        body_update_err_then_wait_wf_info().handler,
        rt.load_history(exec).unwrap(),
    )
    .await;
}

// ──────────────────────────────── atomicity ──────────────────────────────────

/// A failed result write rolls back the admission too (design risk R2).
#[tokio::test]
async fn admission_and_result_commit_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&limit_wf_info());
    let exec = rt.start_workflow("limit_wf", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let before = snapshot(&rt, exec);

    // Make the second write of the update fail.
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch(
        "CREATE TRIGGER fail_result BEFORE INSERT ON harvest_events \
         WHEN NEW.event_json LIKE '%UpdateCompleted%' \
         BEGIN SELECT RAISE(ABORT, 'injected'); END;",
    )
    .unwrap();

    let err = rt.update(exec, "set_limit", json!(5)).await.unwrap_err();
    assert!(matches!(err, SqliteError::Sqlite(_)), "{err:?}");
    assert_eq!(
        snapshot(&rt, exec),
        before,
        "no admitted update without a result"
    );

    raw.execute_batch("DROP TRIGGER fail_result;").unwrap();
    assert_eq!(
        rt.update(exec, "set_limit", json!(5)).await.unwrap(),
        json!({ "limit": 5 })
    );
}

// ──────────────────────────── more ingress checks ────────────────────────────

/// A declarative arg schema rejects a bad input and writes no event.
#[tokio::test]
async fn declarative_arg_schema_rejects_and_writes_no_event() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&declarative_wf_info());
    rt.register_update(&rename_info().with_arg_schema_fn(|| json!({ "type": "string" })));
    let exec = rt.start_workflow("declarative_wf", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let before = snapshot(&rt, exec);

    let err = rt.update(exec, "rename", json!(7)).await.unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::UpdateInputInvalid { ref violations, .. }
                if violations.len() == 1 && violations[0].message.contains("string")
        ),
        "{err:?}"
    );
    assert_eq!(snapshot(&rt, exec), before);
    assert_eq!(
        rt.update(exec, "rename", json!("a")).await.unwrap(),
        json!("renamed to a")
    );
}

/// An update to a sealed `CONTINUED_AS_NEW` run is rejected.
#[tokio::test]
async fn update_to_sealed_run_is_rejected() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&limit_wf_info());
    let prior = rt
        .start_workflow_with_id("limit_wf", "k", json!(0))
        .unwrap();
    rt.run_until_blocked(prior).await.unwrap();
    rt.start_workflow_with_reuse_policy(
        "limit_wf",
        "k",
        json!(0),
        WorkflowIdReusePolicy::TerminateIfRunning,
    )
    .unwrap();
    let before = snapshot(&rt, prior);

    let err = rt.update(prior, "set_limit", json!(1)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::WorkflowNotRunning { ref state, .. } if state == "CONTINUED_AS_NEW"),
        "{err:?}"
    );
    assert_eq!(snapshot(&rt, prior), before);
}

/// An update to a run whose workflow is not registered is rejected.
#[tokio::test]
async fn update_to_unregistered_workflow_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&limit_wf_info());
        rt.start_workflow("limit_wf", json!(0)).unwrap()
    };
    let mut rt = SqliteRuntime::open(&path).unwrap();
    let before = snapshot(&rt, exec);

    let err = rt.update(exec, "set_limit", json!(1)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UnknownWorkflow(ref w) if w == "limit_wf"),
        "{err:?}"
    );
    assert_eq!(snapshot(&rt, exec), before);
}

// ──────────────────────────── run states ─────────────────────────────────────

/// An update before the first drive works. The run then completes.
#[tokio::test]
async fn update_before_the_first_drive() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&limit_wf_info());
    let exec = rt.start_workflow("limit_wf", json!(0)).unwrap();

    assert_eq!(
        rt.update(exec, "set_limit", json!(2)).await.unwrap(),
        json!({ "limit": 2 })
    );
    rt.send_signal(exec, "go", json!("ok")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    assert_replays(
        "limit_wf",
        limit_wf_info().handler,
        rt.load_history(exec).unwrap(),
    )
    .await;
}

/// An update while a durable timer is pending. The timer still fires and the
/// run completes.
#[tokio::test]
async fn update_while_a_timer_is_pending() {
    let t0 = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&timer_wf_info());
    let exec = rt.start_workflow("timer_wf", json!(0)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");

    assert_eq!(
        rt.update(exec, "set_limit", json!(3)).await.unwrap(),
        json!(3)
    );
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(61))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("fired")),
        "{state:?}"
    );
    assert_replays(
        "timer_wf",
        timer_wf_info().handler,
        rt.load_history(exec).unwrap(),
    )
    .await;
}

// ─────────────────────────────── parity ──────────────────────────────────────

/// The update events match those of the core `WorkflowTestEnv` for the same
/// steps. Ids and timestamps are left out.
#[tokio::test]
async fn update_events_match_the_core_test_env() {
    let (mut rt, exec) = blocked_limit_run().await;
    rt.update(exec, "set_limit", json!(5)).await.unwrap();
    rt.update(exec, "set_limit", json!(13)).await.unwrap_err();
    rt.update(exec, "set_limit", json!(-1)).await.unwrap_err();
    rt.send_signal(exec, "go", json!("ok")).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let sqlite = rt.load_history(exec).unwrap();

    let env = WorkflowTestEnv::new();
    let mut run = env.start(limit_wf_info().handler, json!(0));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);
    run.update("set_limit", json!(5)).await.unwrap();
    run.update("set_limit", json!(13)).await.unwrap_err();
    run.update("set_limit", json!(-1)).await.unwrap_err();
    run.signal("go", json!("ok")).unwrap();
    let core = run.finish().await;

    let types = |h: &[WorkflowEvent]| h.iter().map(WorkflowEvent::type_name).collect::<Vec<_>>();
    assert_eq!(types(&sqlite), types(core.events()));
    assert_eq!(update_shape(&sqlite), update_shape(core.events()));
}

/// A validator panic is contained. It is a rejection and writes no event.
#[tokio::test]
async fn validator_panic_is_a_rejection() {
    let (mut rt, exec) = blocked_limit_run().await;
    let before = snapshot(&rt, exec);

    let err = rt.update(exec, "fragile", json!(1)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UpdateRejected { ref reason, .. } if reason.contains("validator boom")),
        "{err:?}"
    );
    assert_eq!(snapshot(&rt, exec), before);
    assert_eq!(
        rt.update(exec, "set_limit", json!(1)).await.unwrap(),
        json!({ "limit": 1 })
    );
}

/// A changed build that diverges from history cannot run an update. The
/// update writes no event.
#[tokio::test]
async fn update_on_a_drifted_build_is_non_deterministic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&timer_wf_info());
        let exec = rt.start_workflow("timer_wf", json!(0)).unwrap();
        rt.run_until_blocked(exec).await.unwrap();
        exec
    };

    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&WorkflowInfo {
        name: "timer_wf",
        ..drifted_timer_wf_info()
    });
    let before = snapshot(&rt, exec);
    let err = rt.update(exec, "set_limit", json!(1)).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::NonDeterministic { .. }),
        "{err:?}"
    );
    assert_eq!(snapshot(&rt, exec), before);
}

/// A body-driven update with a new id on each cycle appends one result per
/// drive. It does not spin to the iteration bound.
#[tokio::test]
async fn record_update_result_does_not_spin_the_drive() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&fresh_update_id_wf_info());
    let exec = rt.start_workflow("fresh_update_id_wf", json!(0)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "go"),
        "{state:?}"
    );
    assert_eq!(update_events(&rt.load_history(exec).unwrap()).len(), 1);
}

// ─────────────────────────────── history caps ────────────────────────────────

/// An update that would reach the event cap writes no update event. The run
/// fails at the cap, as a drive fails it (issue #2023).
#[tokio::test]
async fn update_at_the_event_cap_fails_the_run() {
    let (mut rt, exec) = blocked_limit_run().await;
    let len = u64::try_from(rt.load_history(exec).unwrap().len()).unwrap();
    // Room for one more event, not for the two that an update writes.
    rt.set_history_event_hard_cap(Some(len + 2));

    let err = rt.update(exec, "set_limit", json!(5)).await.unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::UpdateHistoryCapReached { ref reason, .. }
                if reason.type_tag() == "HistoryCapExceeded"
        ),
        "{err:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert!(update_events(&history).is_empty(), "{history:?}");
    assert!(
        matches!(history.last(), Some(WorkflowEvent::WorkflowFailed { error, .. }) if error.contains("HistoryCapExceeded")),
        "{history:?}"
    );
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Failed(_)
    ));
}

/// An update whose events would reach the byte cap fails the run the same way.
#[tokio::test]
async fn update_at_the_byte_cap_fails_the_run() {
    let (mut rt, exec) = blocked_limit_run().await;
    rt.set_history_byte_hard_cap(Some(64 * 1024));

    let big = json!("x".repeat(64 * 1024));
    let err = rt.update(exec, "later", big).await.unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::UpdateHistoryCapReached { ref reason, .. }
                if reason.type_tag() == "HistoryBytesCapExceeded"
        ),
        "{err:?}"
    );
    assert!(update_events(&rt.load_history(exec).unwrap()).is_empty());
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Failed(_)
    ));
}

/// An update that reaches a cap under a pending cancel ends the run
/// `CANCELLED`, not `FAILED` (issue #2021).
#[tokio::test]
async fn update_at_the_event_cap_under_a_pending_cancel_ends_cancelled() {
    let (mut rt, exec) = blocked_limit_run().await;
    let len = u64::try_from(rt.load_history(exec).unwrap().len()).unwrap();
    rt.set_history_event_hard_cap(Some(len + 2));
    rt.cancel(exec, "stop").unwrap();

    let err = rt.update(exec, "set_limit", json!(5)).await.unwrap_err();

    assert!(
        matches!(err, SqliteError::UpdateHistoryCapReached { .. }),
        "{err:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert!(update_events(&history).is_empty(), "{history:?}");
    assert!(
        matches!(history.last(), Some(WorkflowEvent::WorkflowCancelled { reason }) if reason == "stop"),
        "{history:?}"
    );
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Terminated(state) if state == "CANCELLED"
    ));
}

/// The update replay sees a pending cancel, as the next drive does (issue
/// #2021).
#[tokio::test]
async fn an_update_sees_a_pending_cancel() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&cancel_aware_wf_info());
    let exec = rt.start_workflow("cancel_aware_wf", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    assert_eq!(
        rt.update(exec, "status", json!(null)).await.unwrap(),
        json!(null)
    );

    rt.cancel(exec, "stop").unwrap();

    assert_eq!(
        rt.update(exec, "status", json!(null)).await.unwrap(),
        json!("stop")
    );
}

/// An update below the caps is unchanged.
#[tokio::test]
async fn update_below_the_caps_succeeds() {
    let (mut rt, exec) = blocked_limit_run().await;
    let len = u64::try_from(rt.load_history(exec).unwrap().len()).unwrap();
    rt.set_history_event_hard_cap(Some(len + 3));

    assert_eq!(
        rt.update(exec, "set_limit", json!(5)).await.unwrap(),
        json!({ "limit": 5 })
    );
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Running
    ));
}
