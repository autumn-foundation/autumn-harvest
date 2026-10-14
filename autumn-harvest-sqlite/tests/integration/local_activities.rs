//! Local activities on the `SQLite` backend (issue #2029).
//!
//! `#[activity(local = true)]` used to panic at registration, and
//! `RunLocalActivity` used to seal the run `FAILED`. Each test runs a local
//! activity to completion, then replays the history on the core
//! `WorkflowReplayer`.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]
// Fixture bodies return at once, with no `.await`.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest_sqlite::{RunState, SqliteError, SqliteRuntime};
use serde_json::json;

#[activity(local = true)]
async fn double(ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    if !ctx.is_local() {
        return Err("expected a local context".to_string());
    }
    Ok(n * 2)
}

#[activity(local = true, retry = RetryPolicy::fixed(3, Duration::ZERO))]
async fn flaky(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[activity(local = true, retry = RetryPolicy::fixed(2, Duration::ZERO))]
async fn broken(_ctx: &ActivityContext, _n: i64) -> Result<i64, String> {
    Err("always broken".to_string())
}

#[activity]
async fn remote(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn calls_double(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_local_activity(&double_info(), n)
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn calls_flaky(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_local_activity(&flaky_info(), n)
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn calls_broken(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx
        .execute_local_activity::<_, i64>(&broken_info(), n)
        .await
    {
        Ok(_) => Ok("unexpected success".to_string()),
        Err(e) => Ok(format!("caught: {e}")),
    }
}

/// Runs a local activity, then a regular one, then a local one again.
#[workflow]
async fn mixes_local_and_remote(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let a: i64 = ctx
        .execute_local_activity(&double_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    let b: i64 = ctx
        .execute_activity(&remote_info(), a)
        .await
        .map_err(|e| e.to_string())?;
    ctx.execute_local_activity(&double_info(), b)
        .await
        .map_err(|e| e.to_string())
}

/// Joins a local activity with a timer, which issue #950 forbids.
#[workflow]
async fn joins_local_with_timer(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let info = double_info();
    let (out, _) = tokio::join!(
        ctx.execute_local_activity::<_, i64>(&info, n),
        ctx.timer("t", 5),
    );
    out.map_err(|e| e.to_string())
}

fn runtime(info: &WorkflowInfo) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(info);
    rt.register_activity(&double_info());
    rt.register_activity(&remote_info());
    rt.register_activity(&broken_info());
    rt
}

async fn assert_replays(name: &str, handler: WorkflowHandlerFn, history: Vec<WorkflowEvent>) {
    let report = WorkflowReplayer::new()
        .register_fn(name, handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the history must replay:\n{report}"
    );
}

fn kinds(history: &[WorkflowEvent]) -> Vec<&'static str> {
    history.iter().map(WorkflowEvent::type_name).collect()
}

fn attempt_rows(rt: &SqliteRuntime, exec: ExecutionId) -> usize {
    rt.activity_attempts(exec, "double").unwrap().len()
}

#[tokio::test]
async fn a_local_activity_runs_inline_and_completes() {
    let info = calls_double_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(21)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == 42),
        "got {state:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        kinds(&history),
        [
            "WorkflowStarted",
            "LocalActivityScheduled",
            "LocalActivityCompleted",
            "WorkflowCompleted"
        ]
    );
    assert_eq!(attempt_rows(&rt, exec), 0, "a local activity uses no task");
    assert_replays(info.name, info.handler, history).await;
}

#[tokio::test]
async fn a_failing_local_activity_retries_then_succeeds() {
    let info = calls_flaky_info();
    let mut rt = runtime(&info);
    let calls = Arc::new(AtomicU32::new(0));
    let seen = calls.clone();
    rt.register_activity_with_body(&flaky_info(), move |input| {
        if seen.fetch_add(1, Ordering::SeqCst) < 2 {
            Err("not yet".to_string())
        } else {
            Ok(input)
        }
    });
    let exec = rt.start_workflow(info.name, json!(5)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == 5),
        "got {state:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        kinds(&history),
        [
            "WorkflowStarted",
            "LocalActivityScheduled",
            "LocalActivityFailed",
            "LocalActivityFailed",
            "LocalActivityCompleted",
            "WorkflowCompleted"
        ]
    );
    assert_replays(info.name, info.handler, history).await;
}

#[tokio::test]
async fn an_exhausted_local_activity_fails_the_call() {
    let info = calls_broken_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(1)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_str().is_some_and(|s| s.contains("always broken"))),
        "got {state:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        kinds(&history),
        [
            "WorkflowStarted",
            "LocalActivityScheduled",
            "LocalActivityFailed",
            "LocalActivityFailed",
            "LocalActivityExhausted",
            "WorkflowCompleted"
        ]
    );
    assert_replays(info.name, info.handler, history).await;
}

#[tokio::test]
async fn local_and_regular_activities_compose_in_sequence() {
    let info = mixes_local_and_remote_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(3)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == 12),
        "got {state:?}"
    );
    assert_replays(info.name, info.handler, rt.load_history(exec).unwrap()).await;
}

// Issue #950: a local activity cannot share a batch with a durable wait.
#[tokio::test]
async fn a_local_activity_beside_a_timer_ends_failed() {
    let info = joins_local_with_timer_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(1)).unwrap();

    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::Unsupported(ref m) if m.starts_with("RunLocalActivity") && m.contains("StartTimer")),
        "got {err:?}"
    );
}

// The macro rejects `schedule_to_close` on a local activity. A hand-built
// `ActivityInfo` meets the same rule at registration.
#[test]
#[should_panic(expected = "schedule_to_close")]
fn a_local_activity_with_schedule_to_close_panics_at_registration() {
    let mut info = double_info();
    info.default_schedule_to_close = Some(Duration::from_secs(10));
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_activity(&info);
}
