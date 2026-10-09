//! Mid-run signal, update and query injection for `WorkflowTestEnv` (issue #1991).
//!
//! A test starts a run, drives it until it blocks, injects an input, and drives
//! it again. No database is involved.
//!
//! Run with:
//!
//! ```text
//! cargo test -p autumn-harvest --no-default-features --features testing \
//!   --test integration workflow_test_env_mid_run_tests
//! ```

// The `#[workflow]`, `#[query]` and `#[update]` macros need an `async fn` or
// a `Result` return, even when the body neither awaits nor fails.
#![allow(clippy::unused_async, clippy::unnecessary_wraps)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::{queries, query, update, updates, workflow};
use autumn_harvest::testing::{ReplayStatus, TestRunStatus, WorkflowTestEnv};
use serde_json::{Value, json};

// ─────────────────────────────── workflows ───────────────────────────────────

/// Runs an activity, waits for `approve`, sleeps one hour, then waits for
/// `confirm`. The `seen` query returns each value the body has received.
///
/// The `set_limit` update accepts a positive number only. A limit of 13
/// makes the handler fail after admission.
fn approval_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
        let query_view = Arc::clone(&seen);
        ctx.register_query("seen", move || json!(*query_view.lock().unwrap()));
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

        let prepared = ctx
            .execute_activity_raw("prepare", json!(null), "default")
            .await
            .map_err(|e| e.to_string())?;
        seen.lock().unwrap().push(prepared);

        let approve = ctx
            .wait_for_signal("approve")
            .await
            .map_err(|e| e.to_string())?;
        seen.lock().unwrap().push(approve.clone());

        ctx.timer("cool_off", 3600)
            .await
            .map_err(|e| e.to_string())?;

        let confirm = ctx
            .wait_for_signal("confirm")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "approve": approve, "confirm": confirm }))
    })
}

/// Waits for one `go` signal. Declarative handlers serve its query and update.
///
/// The `#[workflow]` declaration gives the handlers below their typed stub.
#[workflow]
async fn declarative_workflow(ctx: &WorkflowContext) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Owns the foreign handlers that the workflow-name filter must skip.
#[workflow]
async fn other_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[query(workflow = "declarative_workflow")]
fn phase(_ctx: &WorkflowContext) -> Result<String, String> {
    Ok("waiting".to_string())
}

fn validate_rename(input: &Value) -> Result<(), String> {
    if input.as_str().is_some_and(|s| !s.is_empty()) {
        Ok(())
    } else {
        Err("name must not be empty".to_string())
    }
}

#[update(workflow = "declarative_workflow", validator = validate_rename)]
async fn rename(_ctx: &WorkflowContext, name: String) -> Result<String, String> {
    Ok(format!("renamed to {name}"))
}

#[query(workflow = "other_workflow")]
fn foreign_phase(_ctx: &WorkflowContext) -> Result<String, String> {
    Ok("foreign".to_string())
}

#[update(workflow = "other_workflow")]
async fn foreign_rename(_ctx: &WorkflowContext, name: String) -> Result<String, String> {
    Ok(name)
}

/// Registers an update that never finishes, an update that panics and a
/// query that panics, then waits for `go`.
fn faulty_update_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.register_update_handler_no_validator("hang", |_input: Value| async move {
            std::future::pending::<Result<Value, String>>().await
        });
        ctx.register_update_handler_no_validator("boom", |_input: Value| async move {
            panic!("boom handler")
        });
        ctx.register_query("explode", || panic!("query boom"));
        ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
    })
}

/// A child wins its deadline race, then the workflow waits for `go`.
///
/// Every later cycle replays the win and emits `CancelRaceLosers` for the
/// deadline timer. That command appends no event.
fn child_then_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child = ctx
            .spawn_child_workflow_timeout(
                "child_processing",
                json!(null),
                std::time::Duration::from_secs(60),
            )
            .await
            .map_err(|e| e.to_string())?;
        let go = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
        Ok(json!({ "child": child, "go": go }))
    })
}

/// Picks an activity by whether the `phase` query is registered. Replay
/// must see the same handlers as the live run, or it takes the other branch.
fn branch_on_handlers_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity = if ctx.list_query_names().iter().any(|n| n == "phase") {
            "with_phase"
        } else {
            "without_phase"
        };
        ctx.execute_activity_raw(activity, json!(null), "default")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Sleeps one hour, then waits for `go`. Its `clock` update reports the
/// handler's view of `ctx.now()`.
#[workflow]
async fn timed_workflow(ctx: &WorkflowContext) -> Result<Value, String> {
    ctx.timer("nap", 3600).await.map_err(|e| e.to_string())?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

#[update(workflow = "timed_workflow")]
async fn clock(ctx: &WorkflowContext) -> Result<String, String> {
    Ok(ctx.now().to_rfc3339())
}

fn string_schema() -> Value {
    json!({ "type": "string" })
}

// ─────────────────────────────── helpers ─────────────────────────────────────

fn approval_env() -> WorkflowTestEnv {
    WorkflowTestEnv::new().mock_activity("prepare", |_| Ok(json!("prepared")))
}

fn count(events: &[WorkflowEvent], type_name: &str) -> usize {
    events.iter().filter(|e| e.type_name() == type_name).count()
}

fn position(events: &[WorkflowEvent], type_name: &str) -> usize {
    events
        .iter()
        .position(|e| e.type_name() == type_name)
        .unwrap_or_else(|| panic!("no {type_name} event in history"))
}

// ─────────────────────────────── signals ─────────────────────────────────────

/// AC 1: a signal sent after the workflow blocks unblocks it. The test sees
/// the effect in the query result and in the event order.
#[tokio::test]
async fn signal_sent_after_block_unblocks_the_workflow() {
    let env = approval_env();
    let mut run = env.start(approval_workflow, json!(null));

    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);
    assert_eq!(count(run.events(), "SignalReceived"), 0);
    assert_eq!(run.now(), env.now(), "no timer has fired yet");
    assert_eq!(
        run.query("seen", Value::Null).await.expect("query"),
        json!(["prepared"])
    );

    run.signal("approve", json!({ "by": "alice" }))
        .expect("a blocked run accepts a signal");
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);
    assert_eq!(
        run.query("seen", Value::Null).await.expect("query"),
        json!(["prepared", { "by": "alice" }])
    );

    // The signal lands after the activity, and the timer starts after it.
    let events = run.events();
    assert!(position(events, "ActivityCompleted") < position(events, "SignalReceived"));
    assert!(position(events, "SignalReceived") < position(events, "TimerStarted"));

    run.signal("confirm", json!("yes")).expect("signal");
    let outcome = run.finish().await;
    assert_eq!(
        outcome.result,
        Ok(json!({ "approve": { "by": "alice" }, "confirm": "yes" }))
    );
    assert_eq!(outcome.elapsed().num_seconds(), 3600);
    let report = outcome.replay_check(approval_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{report}"
    );
}

/// A pre-queued signal and a mid-run signal work in one run.
#[tokio::test]
async fn queued_signal_and_mid_run_signal_combine() {
    let env = approval_env().queue_signal("approve", json!("early"));
    let mut run = env.start(approval_workflow, json!(null));

    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);
    run.signal("confirm", json!("late")).expect("signal");
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Finished);

    let outcome = run.finish().await;
    assert_eq!(
        outcome.result,
        Ok(json!({ "approve": "early", "confirm": "late" }))
    );
}

/// `start(..).finish()` with no injection matches `run()`, including the
/// error for a run that stays blocked.
#[tokio::test]
async fn finish_on_a_blocked_run_matches_run() {
    let env = approval_env();
    let via_run = env.run(approval_workflow, json!(null)).await;
    let via_start = env.start(approval_workflow, json!(null)).finish().await;

    assert_eq!(via_start.result, via_run.result);
    assert!(
        via_start
            .result
            .as_ref()
            .unwrap_err()
            .contains("no resolvable commands")
    );
    let types = |o: &autumn_harvest::testing::TestRunOutcome| {
        o.events()
            .iter()
            .map(WorkflowEvent::type_name)
            .collect::<Vec<_>>()
    };
    assert_eq!(types(&via_start), types(&via_run));
}

// ─────────────────────────────── updates ─────────────────────────────────────

/// AC 2: an update against a blocked workflow runs its handler and records
/// `UpdateAdmitted` and `UpdateCompleted`.
#[tokio::test]
async fn update_against_a_blocked_workflow_runs_the_handler() {
    let env = approval_env();
    let mut run = env.start(approval_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);

    let result = run.update("set_limit", json!(5)).await;
    assert_eq!(result.expect("update succeeds"), json!({ "limit": 5 }));

    let events = run.events();
    assert_eq!(count(events, "UpdateAdmitted"), 1);
    assert_eq!(count(events, "UpdateCompleted"), 1);
    match &events[position(events, "UpdateAdmitted")] {
        WorkflowEvent::UpdateAdmitted {
            name,
            input,
            timestamp,
            ..
        } => {
            assert_eq!(name, "set_limit");
            assert_eq!(input, &json!(5));
            assert_eq!(*timestamp, env.now());
        }
        other => panic!("unexpected event {other:?}"),
    }

    // The update does not unblock the run, and history still replays.
    run.signal("approve", json!(1)).expect("signal");
    run.signal("confirm", json!(2)).expect("signal");
    let outcome = run.finish().await;
    assert!(outcome.result.is_ok(), "{:?}", outcome.result);
    let report = outcome.replay_check(approval_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{report}"
    );
}

/// AC 2: a validator rejection returns `UpdateRejected` and writes no event.
#[tokio::test]
async fn update_validator_rejection_writes_no_event() {
    let env = approval_env();
    let mut run = env.start(approval_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);
    let before = run.events().len();

    let err = run
        .update("set_limit", json!(-1))
        .await
        .expect_err("validator rejects");
    match err {
        HarvestError::UpdateRejected { reason } => {
            assert_eq!(reason, "limit must be a positive number");
        }
        other => panic!("expected UpdateRejected, got {other:?}"),
    }
    assert_eq!(run.events().len(), before, "a rejection writes no event");
}

/// A handler error records `UpdateFailed` and returns the error.
#[tokio::test]
async fn update_handler_error_records_update_failed() {
    let env = approval_env();
    let mut run = env.start(approval_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);

    let err = run
        .update("set_limit", json!(13))
        .await
        .expect_err("handler fails");
    assert!(err.to_string().contains("limit 13 is unlucky"), "{err}");
    assert_eq!(count(run.events(), "UpdateAdmitted"), 1);
    assert_eq!(count(run.events(), "UpdateFailed"), 1);
}

/// An unknown update name returns `UpdateHandlerNotFound` and writes no event.
#[tokio::test]
async fn unknown_update_returns_not_found() {
    let env = approval_env();
    let mut run = env.start(approval_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);
    let before = run.events().len();

    let err = run.update("missing", json!(1)).await.expect_err("unknown");
    assert!(
        matches!(err, HarvestError::UpdateHandlerNotFound(ref n) if n == "missing"),
        "{err:?}"
    );
    assert_eq!(run.events().len(), before);
}

/// The update timestamp follows the virtual clock across a time skip.
#[tokio::test]
async fn update_timestamp_follows_the_virtual_clock() {
    let env = approval_env();
    let mut run = env.start(approval_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);
    run.signal("approve", json!(1)).expect("signal");
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);

    assert_eq!(run.now(), env.now() + chrono::Duration::hours(1));
    run.update("set_limit", json!(2)).await.expect("update");
    let stamp = run.events().iter().find_map(|e| match e {
        WorkflowEvent::UpdateAdmitted { timestamp, .. } => Some(*timestamp),
        _ => None,
    });
    assert_eq!(stamp, Some(env.now() + chrono::Duration::hours(1)));
}

/// Declarative `#[query]` and `#[update]` handlers work when the env
/// registers them, and the declarative validator runs.
#[tokio::test]
async fn declarative_handlers_serve_query_and_update() {
    let env = WorkflowTestEnv::new()
        .queries(queries![phase])
        .updates(updates![rename]);
    let mut run = env.start(declarative_workflow_info().handler, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);

    assert_eq!(
        run.query("phase", Value::Null).await.expect("query"),
        json!("waiting")
    );
    assert_eq!(
        run.update("rename", json!("bob")).await.expect("update"),
        json!("renamed to bob")
    );
    let err = run.update("rename", json!("")).await.expect_err("rejected");
    assert!(
        matches!(err, HarvestError::UpdateRejected { .. }),
        "{err:?}"
    );
}

/// With a workflow name set, the env registers only that workflow's
/// declarative handlers, as the worker does.
#[tokio::test]
async fn declarative_handlers_of_another_workflow_are_not_registered() {
    let env = WorkflowTestEnv::new()
        .with_workflow_name("declarative_workflow")
        .queries(queries![phase, foreign_phase])
        .updates(updates![rename, foreign_rename]);
    let mut run = env.start(declarative_workflow_info().handler, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);

    assert_eq!(
        run.query("phase", Value::Null).await.expect("own query"),
        json!("waiting")
    );
    let err = run
        .query("foreign_phase", Value::Null)
        .await
        .expect_err("foreign query");
    assert!(
        matches!(err, HarvestError::QueryHandlerNotFound(_)),
        "{err:?}"
    );
    let err = run
        .update("foreign_rename", json!("x"))
        .await
        .expect_err("foreign update");
    assert!(
        matches!(err, HarvestError::UpdateHandlerNotFound(_)),
        "{err:?}"
    );
}

/// An update handler that never finishes returns a timeout. The admitted
/// update stays in history with no result, as in production.
#[tokio::test(start_paused = true)]
async fn update_handler_that_never_finishes_times_out() {
    let env = WorkflowTestEnv::new();
    let mut run = env.start(faulty_update_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);

    let err = run.update("hang", json!(null)).await.expect_err("timeout");
    assert!(matches!(err, HarvestError::Timeout { .. }), "{err:?}");
    assert_eq!(count(run.events(), "UpdateAdmitted"), 1);
    assert_eq!(count(run.events(), "UpdateCompleted"), 0);
    assert_eq!(count(run.events(), "UpdateFailed"), 0);

    // The run still works after the timeout.
    run.signal("go", json!(1)).expect("signal");
    assert_eq!(run.finish().await.result, Ok(json!(1)));
}

/// A panic in an update handler records `UpdateFailed`. The test does not
/// unwind.
#[tokio::test]
async fn update_handler_panic_records_update_failed() {
    let env = WorkflowTestEnv::new();
    let mut run = env.start(faulty_update_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);

    let err = run.update("boom", json!(null)).await.expect_err("panic");
    assert!(err.to_string().contains("boom handler"), "{err}");
    assert_eq!(count(run.events(), "UpdateFailed"), 1);
}

/// The `arg_schema` of a declarative update rejects a bad input before the
/// validator runs, and writes no event (issue #610).
#[tokio::test]
async fn update_arg_schema_rejects_a_bad_input() {
    let with_schema = updates![rename]
        .into_iter()
        .map(|info| info.with_arg_schema_fn(string_schema))
        .collect();
    let env = WorkflowTestEnv::new().updates(with_schema);
    let mut run = env.start(declarative_workflow_info().handler, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);
    let before = run.events().len();

    let err = run.update("rename", json!(7)).await.expect_err("schema");
    assert!(
        matches!(err, HarvestError::InputValidationFailed { .. }),
        "{err:?}"
    );
    assert_eq!(run.events().len(), before);
}

/// A replayed child-timeout win emits `CancelRaceLosers` with no event. The
/// run still reports `Blocked` on the next wait.
#[tokio::test]
async fn child_timeout_win_then_wait_reports_blocked() {
    let env = WorkflowTestEnv::new().mock_child_workflow("child_processing", |_| Ok(json!("done")));
    let mut run = env.start(child_then_signal_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);

    run.signal("go", json!(2)).expect("signal");
    assert_eq!(
        run.finish().await.result,
        Ok(json!({ "child": "done", "go": 2 }))
    );
}

/// `replay_check` registers the same declarative handlers as the live run,
/// with and without a workflow name.
#[tokio::test]
async fn replay_check_sees_the_declarative_handlers() {
    for env in [
        WorkflowTestEnv::new(),
        WorkflowTestEnv::new().with_workflow_name("declarative_workflow"),
    ] {
        let env = env
            .queries(queries![phase, foreign_phase])
            .mock_activity("with_phase", |_| Ok(json!("with")))
            .mock_activity("without_phase", |_| Ok(json!("without")));
        let outcome = env.run(branch_on_handlers_workflow, json!(null)).await;
        assert_eq!(outcome.result, Ok(json!("with")));
        let report = outcome.replay_check(branch_on_handlers_workflow).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );
    }
}

/// A declarative update handler sees the virtual time, as the body does.
#[tokio::test]
async fn declarative_update_handler_sees_the_virtual_clock() {
    let env = WorkflowTestEnv::new().updates(updates![clock]);
    let mut run = env.start(timed_workflow_info().handler, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);

    let seen = run.update("clock", json!(null)).await.expect("update");
    assert_eq!(seen, json!(run.now().to_rfc3339()));
    assert_eq!(run.now(), env.now() + chrono::Duration::hours(1));
}

// ─────────────────────────────── queries ─────────────────────────────────────

/// AC 2: a query reads state and writes no event.
#[tokio::test]
async fn query_against_a_blocked_workflow_writes_no_event() {
    let env = approval_env();
    let mut run = env.start(approval_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);
    let before = run.events().len();

    assert_eq!(
        run.query("seen", Value::Null).await.expect("query"),
        json!(["prepared"])
    );
    assert_eq!(run.events().len(), before, "a query writes no event");

    let err = run
        .query("missing", Value::Null)
        .await
        .expect_err("unknown");
    assert!(
        matches!(err, HarvestError::QueryHandlerNotFound(ref n) if n == "missing"),
        "{err:?}"
    );
}

/// A query before the first drive replays the start event only.
#[tokio::test]
async fn query_before_the_first_drive_sees_the_initial_state() {
    let env = approval_env();
    let run = env.start(approval_workflow, json!(null));
    assert_eq!(
        run.query("seen", Value::Null).await.expect("query"),
        json!([])
    );
}

/// A panic in an imperative query handler returns `QueryHandlerPanicked`.
/// The test does not unwind.
#[tokio::test]
async fn query_handler_panic_is_contained() {
    let env = WorkflowTestEnv::new();
    let mut run = env.start(faulty_update_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);

    let err = run.query("explode", Value::Null).await.expect_err("panic");
    assert!(
        matches!(err, HarvestError::QueryHandlerPanicked(ref m) if m.contains("query boom")),
        "{err:?}"
    );
}

// ─────────────────────────────── finished runs ───────────────────────────────

/// After the run finishes, a signal returns `WorkflowNotRunning` and an
/// update returns `UpdateRejected`, as the update API does. A query still
/// reads the final state.
#[tokio::test]
async fn finished_run_rejects_signal_and_update_but_serves_query() {
    let env = approval_env()
        .queue_signal("approve", json!("a"))
        .queue_signal("confirm", json!("c"));
    let mut run = env.start(approval_workflow, json!(null));
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Finished);
    let exec_id = run.exec_id();

    let err = run.signal("approve", json!("late")).expect_err("finished");
    assert!(matches!(err, HarvestError::WorkflowNotRunning(id) if id == exec_id));
    let err = run
        .update("set_limit", json!(1))
        .await
        .expect_err("finished");
    assert!(
        matches!(err, HarvestError::UpdateRejected { ref reason } if reason.contains("not RUNNING")),
        "{err:?}"
    );
    assert_eq!(
        run.query("seen", Value::Null)
            .await
            .expect("terminal query"),
        json!(["prepared", "a"])
    );

    // Driving a finished run again is a no-op.
    assert_eq!(run.run_until_blocked().await, TestRunStatus::Finished);
    let outcome = run.finish().await;
    assert_eq!(
        outcome.result,
        Ok(json!({ "approve": "a", "confirm": "c" }))
    );
}
