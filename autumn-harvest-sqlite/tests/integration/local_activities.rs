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
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest_sqlite::{RunState, SqliteError, SqliteRuntime};
use chrono::{TimeZone, Utc};
use serde_json::json;

/// A policy with a fixed backoff and no jitter.
const fn paced(max_attempts: u32, secs: u64) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        initial_interval: Duration::from_secs(secs),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_secs(secs),
        non_retryable_errors: Vec::new(),
        jitter: JitterPolicy::None,
    }
}

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

#[activity(local = true, retry = paced(2, 10))]
async fn slow_retry(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[activity(local = true, retry = paced(2, 0))]
async fn hinted(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
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
async fn calls_slow_retry(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_local_activity(&slow_retry_info(), n)
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn calls_hinted(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_local_activity(&hinted_info(), n)
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

/// Joins two local activities, which this backend rejects.
#[workflow]
async fn joins_two_locals(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let info = double_info();
    let (a, b) = tokio::join!(
        ctx.execute_local_activity::<_, i64>(&info, n),
        ctx.execute_local_activity::<_, i64>(&info, n),
    );
    Ok(a.map_err(|e| e.to_string())? + b.map_err(|e| e.to_string())?)
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

/// A settable clock in epoch seconds, installed on `rt`.
fn install_clock(rt: &mut SqliteRuntime) -> Arc<AtomicI64> {
    let clock = Arc::new(AtomicI64::new(1_000_000));
    let read = clock.clone();
    rt.set_clock(move || Utc.timestamp_opt(read.load(Ordering::SeqCst), 0).unwrap());
    clock
}

/// Registers `info` with a body that fails once with `error`, then returns
/// its input. Returns the call counter.
fn fail_once(
    rt: &mut SqliteRuntime,
    info: &autumn_harvest::ActivityInfo,
    error: String,
) -> Arc<AtomicU32> {
    let calls = Arc::new(AtomicU32::new(0));
    let seen = calls.clone();
    rt.register_activity_with_body(info, move |input| {
        if seen.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(error.clone())
        } else {
            Ok(input)
        }
    });
    calls
}

// A retry backoff must not block the drive. The run waits until the
// backoff ends, and only then runs the next attempt.
#[tokio::test]
async fn a_local_retry_backoff_parks_the_run_without_sleeping() {
    let info = calls_slow_retry_info();
    let mut rt = runtime(&info);
    let clock = install_clock(&mut rt);
    let calls = fail_once(&mut rt, &slow_retry_info(), "not yet".to_string());
    let exec = rt.start_workflow(info.name, json!(4)).unwrap();

    let started = Instant::now();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the drive slept"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    clock.store(1_000_009, Ordering::SeqCst);
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "no attempt before the backoff"
    );

    clock.store(1_000_010, Ordering::SeqCst);
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == 4),
        "got {state:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_replays(info.name, info.handler, rt.load_history(exec).unwrap()).await;
}

// A typed `retry_after` hint wins over a zero policy backoff, as on Postgres.
#[tokio::test]
async fn a_retry_after_hint_sets_the_backoff() {
    let info = calls_hinted_info();
    let mut rt = runtime(&info);
    let clock = install_clock(&mut rt);
    let error = ActivityFailure::retryable("Http429", "busy")
        .with_retry_after(Duration::from_secs(30))
        .into_error_payload();
    let calls = fail_once(&mut rt, &hinted_info(), error);
    let exec = rt.start_workflow(info.name, json!(6)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    clock.store(1_000_029, Ordering::SeqCst);
    rt.run_until_blocked(exec).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the hint holds the retry");

    clock.store(1_000_030, Ordering::SeqCst);
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == 6),
        "got {state:?}"
    );
    // The stored error is the typed message, as on Postgres.
    let history = rt.load_history(exec).unwrap();
    assert!(history.iter().any(|e| matches!(
        e,
        WorkflowEvent::LocalActivityFailed { error, .. } if error == "busy"
    )));
}

// This backend runs one local activity per batch. It rejects a second one,
// and the run ends `FAILED` with no local activity recorded.
#[tokio::test]
async fn two_local_activities_in_one_batch_end_failed() {
    let info = joins_two_locals_info();
    let mut rt = runtime(&info);
    let exec = rt.start_workflow(info.name, json!(1)).unwrap();
    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::Unsupported(ref m) if m.starts_with("RunLocalActivity")),
        "got {err:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert!(!kinds(&history).contains(&"LocalActivityScheduled"));
    assert!(matches!(
        history.last(),
        Some(WorkflowEvent::WorkflowFailed { error_type: Some(t), .. })
            if t == autumn_harvest_sqlite::UNSUPPORTED_FEATURE_ERROR_TYPE
    ));
}

// H6: a reopen during a local retry backoff resumes at the next attempt. It
// does not record a second `LocalActivityScheduled`.
#[tokio::test]
async fn a_local_retry_resumes_after_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("local.db");
    let info = calls_slow_retry_info();
    let open = |path: &std::path::Path| {
        let mut rt = SqliteRuntime::open(path).unwrap();
        rt.register_workflow(&info);
        let clock = install_clock(&mut rt);
        (rt, clock)
    };
    let exec = {
        let (mut rt, _clock) = open(&path);
        let calls = fail_once(&mut rt, &slow_retry_info(), "not yet".to_string());
        let exec = rt.start_workflow(info.name, json!(8)).unwrap();
        let state = rt.run_until_blocked(exec).await.unwrap();
        assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        exec
    };
    let (mut rt, clock) = open(&path);
    let calls = Arc::new(AtomicU32::new(0));
    let seen = calls.clone();
    rt.register_activity_with_body(&slow_retry_info(), move |input| {
        seen.fetch_add(1, Ordering::SeqCst);
        Ok(input)
    });
    clock.store(1_000_010, Ordering::SeqCst);
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == 8),
        "got {state:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "only attempt 2 ran again");
    let history = rt.load_history(exec).unwrap();
    let scheduled = kinds(&history)
        .iter()
        .filter(|k| **k == "LocalActivityScheduled")
        .count();
    assert_eq!(scheduled, 1);
    assert_replays(info.name, info.handler, history).await;
}

// At a history cap the attempt's result stays, as on Postgres. The run then
// fails at the cap, and the cap never retries.
#[tokio::test]
async fn a_history_cap_during_a_local_attempt_keeps_its_result() {
    let info = calls_broken_info();
    let mut rt = runtime(&info);
    rt.set_history_event_hard_cap(Some(3));
    let exec = rt.start_workflow(info.name, json!(1)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(ref e) if e.contains("HistoryCapExceeded")),
        "got {state:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        kinds(&history),
        [
            "WorkflowStarted",
            "LocalActivityScheduled",
            "LocalActivityFailed",
            "WorkflowFailed"
        ]
    );
}
