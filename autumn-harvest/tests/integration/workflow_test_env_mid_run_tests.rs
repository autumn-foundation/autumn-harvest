//! Mid-run signal, update and query injection for `WorkflowTestEnv` (issue #1991).
//!
//! A test starts a run, drives it until it blocks, injects an input, and drives
//! it again. No database is involved.
//!
//! Run with:
//!   cargo test -p autumn-harvest --no-default-features --features testing \
//!     --test integration `workflow_test_env_mid_run_tests`

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::{queries, query, update, updates};
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
fn declarative_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { ctx.wait_for_signal("go").await.map_err(|e| e.to_string()) })
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
    let mut run = env.start(declarative_workflow, json!(null));
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

// ─────────────────────────────── finished runs ───────────────────────────────

/// After the run finishes, a signal or update returns `WorkflowNotRunning`.
/// A query still reads the final state.
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
    assert!(matches!(err, HarvestError::WorkflowNotRunning(id) if id == exec_id));
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
