//! Workflow-level `retry_policy` on the `SQLite` backend (issue #2029, #523).
//!
//! `#[workflow(retry = ..)]` used to panic at registration. A failed run now
//! starts a retry successor, as on Postgres. Each history replays on the core
//! `WorkflowReplayer`.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]
// Fixture bodies return at once, with no `.await`.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::Duration;

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest_sqlite::{ActivitySpec, ExecutionOutcome, RunState, SqliteRuntime};
use chrono::{TimeZone, Utc};
use serde_json::json;

const fn no_jitter(max_attempts: u32, secs: u64) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        initial_interval: Duration::from_secs(secs),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_secs(secs),
        non_retryable_errors: Vec::new(),
        jitter: JitterPolicy::None,
    }
}

/// Fails while the `attempt` activity reports a value below 2.
#[workflow(retry = no_jitter(3, 0))]
async fn flaky_run(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let attempt = ctx
        .execute_activity_raw("attempt", json!(null), "default")
        .await
        .map_err(|e| e.to_string())?
        .as_i64()
        .unwrap_or_default();
    if attempt < 2 {
        return Err(format!("attempt {attempt} failed"));
    }
    Ok(attempt)
}

/// Waits ten seconds between attempts.
#[workflow(retry = no_jitter(2, 10))]
async fn slow_retry(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let attempt = ctx
        .execute_activity_raw("attempt", json!(null), "default")
        .await
        .map_err(|e| e.to_string())?
        .as_i64()
        .unwrap_or_default();
    if attempt < 2 {
        return Err("first attempt failed".to_string());
    }
    Ok(attempt)
}

/// Always fails.
#[workflow(retry = no_jitter(2, 0))]
async fn always_fails(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _ = ctx;
    Err("broken".to_string())
}

/// Fails with an error that the policy marks non-retryable.
#[workflow(retry = RetryPolicy {
    non_retryable_errors: vec!["fatal".to_string()],
    ..no_jitter(3, 0)
})]
async fn fails_fatally(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _ = ctx;
    Err("fatal".to_string())
}

/// Waits for `go`, then fails on the first attempt.
#[workflow(retry = no_jitter(2, 0))]
async fn waits_then_fails(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let go = ctx
        .wait_for_signal("go")
        .await
        .map_err(|e| e.to_string())?
        .as_i64()
        .unwrap_or_default();
    let attempt = ctx
        .execute_activity_raw("attempt", json!(null), "default")
        .await
        .map_err(|e| e.to_string())?
        .as_i64()
        .unwrap_or_default();
    if attempt < 2 {
        return Err("first attempt failed".to_string());
    }
    Ok(go)
}

/// Fails on attempt 1, then waits for a signal that never comes.
#[workflow(execution_timeout = "30s", retry = no_jitter(2, 10))]
async fn fails_then_waits(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let attempt = ctx
        .execute_activity_raw("attempt", json!(null), "default")
        .await
        .map_err(|e| e.to_string())?
        .as_i64()
        .unwrap_or_default();
    if attempt < 2 {
        return Err("first attempt failed".to_string());
    }
    let _: serde_json::Value = ctx
        .wait_for_signal("never")
        .await
        .map_err(|e| e.to_string())?;
    Ok(attempt)
}

fn runtime(info: &WorkflowInfo) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(info);
    let calls = Arc::new(AtomicU32::new(0));
    rt.register_activity_raw(
        "attempt",
        ActivitySpec::new(1, move |_| {
            Ok(json!(calls.fetch_add(1, Ordering::SeqCst) + 1))
        }),
    );
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

fn retry_of(history: &[WorkflowEvent]) -> Option<(ExecutionId, u32)> {
    match history.last()? {
        WorkflowEvent::WorkflowRetryScheduled {
            retry_exec_id,
            attempt,
            ..
        } => Some((*retry_exec_id, *attempt)),
        _ => None,
    }
}

#[tokio::test]
async fn a_failed_run_retries_and_the_retry_completes() {
    let info = flaky_run_info();
    let mut rt = runtime(&info);
    let first = rt.start_workflow(info.name, json!(0)).unwrap();

    let state = rt.run_until_blocked(first).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(ref e) if e == "attempt 1 failed"),
        "got {state:?}"
    );
    let history = rt.load_history(first).unwrap();
    let (second, attempt) = retry_of(&history).expect("a WorkflowRetryScheduled event");
    assert_eq!(attempt, 2);
    assert_eq!(rt.successor(first).unwrap(), Some(second));
    assert_replays(info.name, info.handler, history).await;

    let state = rt.run_until_blocked(second).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == 2),
        "got {state:?}"
    );
    assert_eq!(rt.successor(second).unwrap(), None);
    assert_replays(info.name, info.handler, rt.load_history(second).unwrap()).await;
}

// H10: the successor does not run before its backoff.
#[tokio::test]
async fn the_retry_waits_for_its_backoff() {
    let info = slow_retry_info();
    let mut rt = runtime(&info);
    let clock = Arc::new(AtomicI64::new(1_000_000));
    let read = clock.clone();
    rt.set_clock(move || Utc.timestamp_opt(read.load(Ordering::SeqCst), 0).unwrap());
    let first = rt.start_workflow(info.name, json!(0)).unwrap();

    rt.run_until_blocked(first).await.unwrap();
    let second = rt.successor(first).unwrap().expect("a retry");
    clock.store(1_000_009, Ordering::SeqCst);
    let state = rt.run_until_blocked(second).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    assert!(
        rt.load_history(second).unwrap().len() == 1,
        "nothing ran yet"
    );

    clock.store(1_000_010, Ordering::SeqCst);
    let state = rt.run_until_blocked(second).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
}

// H9: retries stop at `max_attempts`.
#[tokio::test]
async fn retries_stop_at_max_attempts() {
    let info = always_fails_info();
    let mut rt = runtime(&info);
    let first = rt.start_workflow(info.name, json!(0)).unwrap();
    rt.run_until_idle().await.unwrap();

    let second = rt.successor(first).unwrap().expect("one retry");
    assert!(matches!(
        rt.outcome(second).unwrap(),
        ExecutionOutcome::Failed(ref e) if e == "broken"
    ));
    assert_eq!(rt.successor(second).unwrap(), None, "attempt 2 is the last");
    assert_replays(info.name, info.handler, rt.load_history(second).unwrap()).await;
}

#[tokio::test]
async fn a_non_retryable_error_does_not_retry() {
    let info = fails_fatally_info();
    let mut rt = runtime(&info);
    let first = rt.start_workflow(info.name, json!(0)).unwrap();
    let state = rt.run_until_blocked(first).await.unwrap();
    assert!(matches!(state, RunState::Failed(_)), "got {state:?}");
    assert_eq!(rt.successor(first).unwrap(), None);
}

// The retry gets every signal of the failed run again, as on Postgres.
#[tokio::test]
async fn the_retry_receives_the_signals_again() {
    let info = waits_then_fails_info();
    let mut rt = runtime(&info);
    let first = rt.start_workflow(info.name, json!(0)).unwrap();
    rt.send_signal(first, "go", json!(9)).unwrap();
    rt.run_until_blocked(first).await.unwrap();

    let second = rt.successor(first).unwrap().expect("a retry");
    let state = rt.run_until_blocked(second).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == 9),
        "got {state:?}"
    );
    assert_replays(info.name, info.handler, rt.load_history(second).unwrap()).await;
}

/// Installs a settable clock in epoch seconds on `rt`.
fn install_clock(rt: &mut SqliteRuntime) -> Arc<AtomicI64> {
    let clock = Arc::new(AtomicI64::new(1_000_000));
    let read = clock.clone();
    rt.set_clock(move || Utc.timestamp_opt(read.load(Ordering::SeqCst), 0).unwrap());
    clock
}

// A cancel ends a retry that still waits for its backoff, at once.
#[tokio::test]
async fn a_cancel_ends_a_retry_during_its_backoff() {
    let info = slow_retry_info();
    let mut rt = runtime(&info);
    let clock = install_clock(&mut rt);
    let first = rt.start_workflow(info.name, json!(0)).unwrap();
    rt.run_until_blocked(first).await.unwrap();
    let second = rt.successor(first).unwrap().expect("a retry");

    clock.store(1_000_001, Ordering::SeqCst);
    rt.cancel(second, "stop").unwrap();
    let state = rt.run_until_blocked(second).await.unwrap();
    assert!(
        matches!(state, RunState::Cancelled(ref r) if r == "stop"),
        "got {state:?}"
    );
}

// The retry keeps the timeout, and its deadline counts from its own start.
#[tokio::test]
async fn the_retry_counts_its_deadline_from_its_start() {
    let info = fails_then_waits_info();
    let mut rt = runtime(&info);
    let clock = install_clock(&mut rt);
    let first = rt.start_workflow(info.name, json!(0)).unwrap();
    rt.run_until_blocked(first).await.unwrap();
    let second = rt.successor(first).unwrap().expect("a retry");

    // The retry starts at 10 s, so its deadline is 40 s.
    clock.store(1_000_039, Ordering::SeqCst);
    let state = rt.run_until_blocked(second).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "got {state:?}");
    clock.store(1_000_040, Ordering::SeqCst);
    let state = rt.run_until_blocked(second).await.unwrap();
    assert!(matches!(state, RunState::TimedOut(_)), "got {state:?}");
    assert_eq!(
        rt.successor(second).unwrap(),
        None,
        "a timeout never retries"
    );
}

// A reuse-policy seal of a failed run is not a continue-as-new, even when
// the failed run started a retry.
#[tokio::test]
async fn a_reuse_seal_of_a_retried_run_reports_no_successor() {
    let info = always_fails_info();
    let mut rt = runtime(&info);
    let first = rt
        .start_workflow_with_id(info.name, "key", json!(0))
        .unwrap();
    rt.run_until_blocked(first).await.unwrap();
    assert!(rt.successor(first).unwrap().is_some());

    rt.start_workflow_with_reuse_policy(
        info.name,
        "key",
        json!(0),
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
    )
    .unwrap();
    assert_eq!(rt.successor(first).unwrap(), None);
    let state = rt.run_until_blocked(first).await.unwrap();
    assert!(
        !matches!(state, RunState::ContinuedAsNew(_)),
        "got {state:?}"
    );
}

// `ctx.deadline()` of a retry counts from its start, as `WorkflowStarted`
// carries the start instant.
#[tokio::test]
async fn the_retry_starts_at_its_backoff() {
    let info = slow_retry_info();
    let mut rt = runtime(&info);
    let _clock = install_clock(&mut rt);
    let first = rt.start_workflow(info.name, json!(0)).unwrap();
    rt.run_until_blocked(first).await.unwrap();
    let second = rt.successor(first).unwrap().expect("a retry");
    let started = rt.load_history(second).unwrap();
    assert!(
        matches!(&started[0], WorkflowEvent::WorkflowStarted { timestamp, .. }
            if timestamp.timestamp() == 1_000_010),
        "got {:?}",
        started[0]
    );
}

/// The clock of `slow_failure`. Its body moves it, as a long compute would.
static SLOW_CLOCK: AtomicI64 = AtomicI64::new(2_000_000);

/// Spends 30 clock seconds, then fails. The retry waits 10 seconds.
#[workflow(retry = no_jitter(2, 10))]
async fn slow_failure(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _ = ctx;
    SLOW_CLOCK.fetch_add(30, Ordering::SeqCst);
    Err("failed late".to_string())
}

// The retry delay counts from the failure, not from the start of the cycle.
#[tokio::test]
async fn the_retry_delay_counts_from_the_failure() {
    let info = slow_failure_info();
    let mut rt = runtime(&info);
    rt.set_clock(|| {
        Utc.timestamp_opt(SLOW_CLOCK.load(Ordering::SeqCst), 0)
            .unwrap()
    });
    let first = rt.start_workflow(info.name, json!(0)).unwrap();
    let start = SLOW_CLOCK.load(Ordering::SeqCst);
    rt.run_until_blocked(first).await.unwrap();

    let fire_at = rt
        .load_history(first)
        .unwrap()
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowRetryScheduled { fire_at, .. } => Some(fire_at.timestamp()),
            _ => None,
        })
        .expect("a retry");
    assert_eq!(fire_at, start + 40, "the delay must start at the failure");
}
