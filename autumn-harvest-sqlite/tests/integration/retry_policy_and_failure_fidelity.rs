//! Cross-backend fidelity regressions (issue #1069).
//!
//! - **Typed failure decode.** A terminal `ActivityFailed` decodes the failure
//!   envelope with `parse_error_payload_full`. It carries the same typed
//!   metadata (`error_type`, `details`, `non_retryable` and the human
//!   `message`) as `finalize_activity_failure` of the Postgres worker. It does
//!   not hard-code `error_type = "Error"` or store the raw
//!   `harvest_activity_failure_v1` envelope as the error.
//! - **Whole retry policy.** The worker honors the WHOLE persisted
//!   `RetryPolicy`, not only `max_attempts`. A retryable failure requeues at
//!   `now + backoff_delay` (`compute_retry_delay`), not at `now`. A
//!   non-retryable failure (the typed flag or a `non_retryable_errors` match)
//!   fails at once and does not repeat the body.
//! - **Signal arrival order.** Pending signals are consumed in ARRIVAL order
//!   (`ORDER BY received_at, signal_seq`). So a `wait_for_signal_timeout` race
//!   compares the right arrival time against the deadline. An on-time signal
//!   staged AFTER a late one still wins.
//!
//! No Docker: `SQLite` is embedded (`rusqlite` `bundled`). The injected
//! `*_as_of` clock drives backoff timing and the signal race, so nothing sleeps.

// The `#[workflow]`/`#[activity]` macros reference their input params, so an
// unused param reads as a "used underscore binding" through expansion.
#![allow(clippy::used_underscore_binding)]
// The `#[activity]` fixture bodies are trivial and await-free. These tests
// register closures in their place.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

// ── Shared helpers ───────────────────────────────────────────────────────────

fn temp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

fn fixed_now() -> DateTime<Utc> {
    "2026-07-15T00:00:00Z".parse().unwrap()
}

/// The `run_at` (epoch-ms) of the single `PENDING` task for `exec`, or `None`.
/// It reads through a raw connection to the same file.
fn pending_task_run_at(path: &std::path::Path, exec: ExecutionId) -> Option<i64> {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT run_at FROM harvest_tasks WHERE exec_id = ?1 AND state = 'PENDING'",
        [exec.to_string()],
        |r| r.get(0),
    )
    .ok()
}

/// The single stored `ActivityFailed` event for `exec`. Panics on 0 or more than 1.
fn only_activity_failed(rt: &SqliteRuntime, exec: ExecutionId) -> WorkflowEvent {
    let mut it = rt
        .load_history(exec)
        .unwrap()
        .into_iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }));
    let ev = it.next().expect("expected one ActivityFailed event");
    assert!(it.next().is_none(), "expected exactly one ActivityFailed");
    ev
}

// ── Typed failure decode: the envelope is decoded into the terminal event ─────

// The raw path carries no policy, so `max_attempts` is 1 from the
// `ActivitySpec`, and the first typed failure is terminal. The test checks
// the decode, apart from the retry gate.
#[workflow]
async fn typed_fail_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("charge", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap_or_default())
}

// The bug: the terminal `ActivityFailed` stored the raw envelope as `error`
// and hard-coded `error_type = "Error"`, `non_retryable = false` and
// `details = None`. A typed failure lost its metadata and did not match the
// Postgres history.
#[tokio::test]
async fn terminal_activity_failure_decodes_typed_envelope_and_replays() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&typed_fail_wf_info());
    rt.register_activity_raw(
        "charge",
        ActivitySpec::new(1, |_input: serde_json::Value| {
            // A retryable typed failure with its own error_type and details.
            // `max_attempts = 1`, so the first failure is terminal.
            Err(
                ActivityFailure::retryable("PaymentDeclined", "card was declined")
                    .with_details(json!({"code": "insufficient_funds", "amount": 4200}))
                    .into_error_payload(),
            )
        }),
    );

    let exec = rt.start_workflow("typed_fail_wf", json!(7)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(_)),
        "the workflow propagates the activity failure, got {state:?}"
    );

    match only_activity_failed(&rt, exec) {
        WorkflowEvent::ActivityFailed {
            error,
            error_type,
            non_retryable,
            details,
            ..
        } => {
            assert_eq!(error_type, "PaymentDeclined", "decoded error_type");
            assert_eq!(
                error, "card was declined",
                "human message, not the envelope"
            );
            assert!(
                !error.contains("harvest_activity_failure_v1"),
                "no raw envelope"
            );
            assert!(
                !non_retryable,
                "retryable typed failure decodes non_retryable=false"
            );
            assert_eq!(
                details,
                Some(json!({"code": "insufficient_funds", "amount": 4200})),
                "structured details survive the decode"
            );
        }
        other => panic!("expected ActivityFailed, got {other:?}"),
    }

    // Cross-backend byte-identity (AC5): the decoded history replays on the
    // core with NO non-determinism drift. The workflow fails, so replay
    // reproduces the failure. The decoded human message flows through as is.
    let history = rt.load_history(exec).unwrap();
    let report = WorkflowReplayer::new()
        .register_fn("typed_fail_wf", typed_fail_wf_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        !matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "a decoded typed-activity-failure history must replay without drift:\n{report}"
    );
    // Issue #952: a reproduced failure on a terminal-failure history reports
    // `ReplaySucceeded` and carries its error in `failure_message()`.
    let error = report
        .failure_message()
        .unwrap_or_else(|| panic!("expected a deterministic reproduced failure:\n{report}"));
    assert!(
        error.contains("card was declined"),
        "the decoded human message flows through replay identically, got {error:?}"
    );
}

// ── Whole retry policy (a): the backoff DELAYS a retry ────────────────────────

// The fixed 60s policy on `_info()` makes `execute_activity` resolve
// `retry_policy_override = Some(RetryPolicy::fixed(3, 60s))`.
#[activity(retry = RetryPolicy::fixed(3, Duration::from_secs(60)))]
async fn flaky(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn flaky_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&flaky_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out * 2)
}

// The bug: only `max_attempts` was stored, so the retry requeued at `now`.
// It re-ran at once in the SAME drain pass, and `run_at` was never in the
// future.
#[tokio::test]
async fn retry_policy_backoff_delays_the_requeue() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();

    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&flaky_wf_info());
    rt.register_activity_raw(
        "flaky",
        ActivitySpec::new(3, move |input: serde_json::Value| {
            if body_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("transient".to_string())
            } else {
                Ok(json!(input.as_i64().unwrap()))
            }
        }),
    );

    let t0 = fixed_now();
    let exec = rt.start_workflow("flaky_wf", json!(21)).unwrap();

    // At t0, attempt 1 fails and requeues 60s out. The run blocks on the
    // backoff, and the body ran ONCE.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "a backing-off retry must block (WaitingTimer), not complete or wedge; got {state:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a poll before the backoff deadline must NOT re-run the body"
    );

    let t0_ms = t0.timestamp_millis();
    let run_at = pending_task_run_at(&path, exec).expect("a pending backing-off task");
    assert_eq!(
        run_at,
        t0_ms + 60_000,
        "the fixed(3, 60s) policy must delay the requeue by 60s, not requeue at now"
    );

    let before = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(59))
        .await
        .unwrap();
    assert!(
        matches!(before, RunState::WaitingTimer),
        "still backing off at t0+59s"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "body still ran only once before the deadline"
    );

    let done = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(61))
        .await
        .unwrap();
    assert!(
        matches!(done, RunState::Completed(ref v) if v.as_i64() == Some(42)),
        "past the backoff deadline the retry runs and the run completes; got {done:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "body ran exactly twice (fail, ok)"
    );
}

// ── The backoff anchors to the failure time ───────────────────────────────────

// Wall-clock companion to `retry_policy_backoff_delays_the_requeue`. That test
// uses a fixed `_as_of` clock, so `failure_now` equals the cycle-start `now`.
// This test installs an ADVANCING clock with `set_clock`. The body seems to
// run 45s before it fails. The 60s backoff must anchor to the failure
// (t0+45s), not to the cycle start (t0). Postgres also computes the delay
// after it handles the result.
//
// The clock returns t0 on the FIRST read and t0+45s on each later read.
//
// The bug (#1069): `run_at = cycle_start_now + delay`, so the retry anchored to
// t0 + 60s, however long the body ran.
#[tokio::test]
async fn retry_policy_backoff_anchors_to_failure_time() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();

    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&flaky_wf_info());
    rt.register_activity_raw(
        "flaky",
        ActivitySpec::new(3, move |input: serde_json::Value| {
            if body_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("transient".to_string())
            } else {
                Ok(json!(input.as_i64().unwrap()))
            }
        }),
    );

    let t0 = fixed_now();
    let t_fail = t0 + chrono::Duration::seconds(45);
    let reads = Arc::new(AtomicUsize::new(0));
    let reads_clock = reads.clone();
    rt.set_clock(move || {
        // The body "runs" 45s after the first read, so `failure_now` is t0+45s.
        if reads_clock.fetch_add(1, Ordering::SeqCst) == 0 {
            t0
        } else {
            t_fail
        }
    });

    let exec = rt.start_workflow("flaky_wf", json!(21)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "a backing-off retry must block (WaitingTimer); got {state:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the body ran once (the failing attempt); the backoff blocks the retry"
    );

    let run_at = pending_task_run_at(&path, exec).expect("a pending backing-off task");
    let t_fail_ms = t_fail.timestamp_millis();
    assert_eq!(
        run_at,
        t_fail_ms + 60_000,
        "the retry must anchor its 60s backoff to the FAILURE instant (t0+45s), not the cycle start"
    );
    assert_ne!(
        run_at,
        t0.timestamp_millis() + 60_000,
        "pre-fix: run_at was anchored to the cycle-start t0"
    );
    assert!(
        run_at > t_fail_ms,
        "the retry must be in the future relative to the failure instant, not immediately ready"
    );
}

// ── Whole retry policy (b): a non-retryable failure is NOT retried ────────────

// An exponential(3) policy would retry, but a typed
// `ActivityFailure { non_retryable: true }` is terminal on the first attempt.
#[activity(retry = RetryPolicy::exponential(3, Duration::from_millis(1)))]
async fn nonret_typed(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn nonret_typed_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&nonret_typed_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

// The bug: the worker read only `max_attempts` and ignored the typed
// `non_retryable` flag, so the body ran up to 3 times.
#[tokio::test]
async fn typed_non_retryable_failure_is_not_retried() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();

    rt.register_workflow(&nonret_typed_wf_info());
    rt.register_activity_raw(
        "nonret_typed",
        ActivitySpec::new(3, move |_input: serde_json::Value| {
            body_calls.fetch_add(1, Ordering::SeqCst);
            Err(ActivityFailure::non_retryable("Fatal", "unrecoverable").into_error_payload())
        }),
    );

    let exec = rt.start_workflow("nonret_typed_wf", json!(1)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(state, RunState::Failed(_)),
        "run fails terminally; got {state:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a typed non_retryable failure must run the body exactly ONCE (no retries)"
    );
    match only_activity_failed(&rt, exec) {
        WorkflowEvent::ActivityFailed {
            non_retryable,
            error_type,
            attempt,
            ..
        } => {
            assert!(
                non_retryable,
                "the terminal event records non_retryable=true"
            );
            assert_eq!(error_type, "Fatal");
            assert_eq!(attempt, 1, "terminal after the first attempt");
        }
        other => panic!("expected ActivityFailed, got {other:?}"),
    }
}

// Policy `non_retryable_errors` on a legacy `Err(String)`.
fn fatal_string_policy() -> RetryPolicy {
    RetryPolicy {
        non_retryable_errors: vec!["fatal".to_string()],
        ..RetryPolicy::exponential(3, Duration::from_millis(1))
    }
}

#[activity(retry = fatal_string_policy())]
async fn nonret_policy(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn nonret_policy_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&nonret_policy_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

// The bug: the policy's `non_retryable_errors` list was dropped, so a legacy
// `Err("fatal")` was retried.
#[tokio::test]
async fn policy_non_retryable_error_class_is_not_retried() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();

    rt.register_workflow(&nonret_policy_wf_info());
    rt.register_activity_raw(
        "nonret_policy",
        ActivitySpec::new(3, move |_input: serde_json::Value| {
            body_calls.fetch_add(1, Ordering::SeqCst);
            Err("fatal".to_string())
        }),
    );

    let exec = rt.start_workflow("nonret_policy_wf", json!(1)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(state, RunState::Failed(_)),
        "run fails terminally; got {state:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "an error in the policy's non_retryable_errors list must run the body exactly ONCE"
    );
}

// ── The RAW path falls back to the registered `default_retry_policy` ──────────
//
// The sections above cover the TYPED `execute_activity` path. It carries the
// resolved `RetryPolicy` on the command as `retry_policy_override`. Here a
// workflow schedules an info-registered activity BY NAME with
// `execute_activity_raw`. That command has no override
// (`task.retry_policy == None`). Registration used to keep only
// `max_attempts` of `default_retry_policy`. So the raw path lost the backoff
// and the `non_retryable_errors` list. Postgres falls back to the FULL
// registered policy when the command has no override. This backend does the
// same at CLAIM time from the `ActivitySpec`, as for `schedule_to_close` and
// `start_to_close`.

// A fixed 60s policy on `_info()`. The RAW path below carries no override, so
// the backoff comes from the claim-time fallback to the registered spec.
#[activity(retry = RetryPolicy::fixed(3, Duration::from_secs(60)))]
async fn raw_backoff_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

// Uses the RAW dispatch path BY NAME, with no `retry_policy_override`.
#[workflow]
async fn raw_backoff_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("raw_backoff_act", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap() * 2)
}

// The bug: the spec kept only `max_attempts`, and the raw command had no
// override. So `task.retry_policy == None`, and the worker requeued at `now`.
// The run converged in one drain pass, and the body ran twice with no wait.
// It should block on the registered 60s backoff after one failure.
#[tokio::test]
async fn raw_path_falls_back_to_registered_default_retry_policy_backoff() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();

    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&raw_backoff_wf_info());
    // An INFO-based registration carries the FULL `default_retry_policy`. A
    // raw dispatch falls back to it at claim time.
    rt.register_activity_with_body(&raw_backoff_act_info(), move |input: serde_json::Value| {
        // Fail attempt 1, succeed attempt 2, as in `flaky_wf`.
        if body_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err("transient".to_string())
        } else {
            Ok(input)
        }
    });

    let t0 = fixed_now();
    let exec = rt.start_workflow("raw_backoff_wf", json!(21)).unwrap();

    // At t0, attempt 1 fails and MUST requeue 60s out, per the registered
    // policy. The run blocks on the backoff, and the body ran ONCE.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "a raw-path call to an info-registered activity must honor the registered \
         default_retry_policy backoff and block (WaitingTimer); got {state:?} \
         (RED pre-fix: requeued immediately and the run converged)"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the body ran once, then backed off (RED pre-fix ran it repeatedly with no backoff)"
    );

    let run_at = pending_task_run_at(&path, exec).expect("a pending backing-off task");
    assert_eq!(
        run_at,
        t0.timestamp_millis() + 60_000,
        "the registered fixed(3, 60s) policy must delay the raw-path requeue by 60s, \
         not requeue at now"
    );

    let done = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(61))
        .await
        .unwrap();
    assert!(
        matches!(done, RunState::Completed(ref v) if v.as_i64() == Some(42)),
        "past the backoff deadline the retry runs and the run completes; got {done:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "body ran exactly twice (fail, ok)"
    );
}

// Policy `non_retryable_errors` on a legacy `Err(String)`, scheduled via the RAW path.
fn raw_fatal_policy() -> RetryPolicy {
    RetryPolicy {
        non_retryable_errors: vec!["fatal".to_string()],
        ..RetryPolicy::fixed(5, Duration::from_secs(1))
    }
}

#[activity(retry = raw_fatal_policy())]
async fn raw_nonret_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn raw_nonret_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    match ctx
        .execute_activity_raw("raw_nonret_act", json!(n), "default")
        .await
    {
        Ok(_) => Ok(0),
        Err(_) => Err("failed".to_string()),
    }
}

// The bug: the raw path had no override, so `task.retry_policy == None`. The
// registered `non_retryable_errors` list was ignored, and the body ran up to
// `max_attempts` (5) times.
#[tokio::test]
async fn raw_path_honors_registered_policy_non_retryable_errors() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();

    rt.register_workflow(&raw_nonret_wf_info());
    // An INFO-based registration carries the full policy.
    rt.register_activity_with_body(&raw_nonret_act_info(), move |_input: serde_json::Value| {
        body_calls.fetch_add(1, Ordering::SeqCst);
        Err("fatal".to_string())
    });

    let exec = rt.start_workflow("raw_nonret_wf", json!(1)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(state, RunState::Failed(_)),
        "run fails terminally; got {state:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a raw-path call to an info-registered activity must honor the registered policy's \
         non_retryable_errors and run the body exactly ONCE (RED pre-fix retried to max_attempts)"
    );
    match only_activity_failed(&rt, exec) {
        WorkflowEvent::ActivityFailed { attempt, .. } => {
            assert_eq!(attempt, 1, "terminal after the first attempt");
        }
        other => panic!("expected ActivityFailed, got {other:?}"),
    }
}

// ── Signal arrival order: consume by ARRIVAL, not by insertion ────────────────

const ONE_HOUR: Duration = Duration::from_secs(3600);

// Await an approval with a 1h deadline: `approved:<who>` on a signal, else
// `auto_rejected` if the deadline fires first (the human-in-the-loop shape).
#[workflow]
async fn approval_gate(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let decision = ctx
        .wait_for_signal_timeout("approval", ONE_HOUR)
        .await
        .map_err(|e| e.to_string())?;
    // Read each later approval. Every staged signal reaches history (issue
    // #2020), and strict replay rejects a signal that the run never reads.
    ctx.drain_signals_raw("approval")
        .map_err(|e| e.to_string())?;
    Ok(decision.map_or_else(
        || "auto_rejected".to_string(),
        |payload| format!("approved:{}", payload.as_str().unwrap_or_default()),
    ))
}

// The bug before issue #1069: the signal read ordered by `signal_seq`.
// The LATE signal (staged first, lowest seq) was consumed first. Its late
// `received_at` let the deadline timer fire first. The race resolved to
// `auto_rejected`, and the on-time signal stayed queued. Under the cycle-start
// ingest (issue #2020), that order bug gives `approved:late`. With
// `ORDER BY received_at, signal_seq` the on-time signal comes first and wins.
#[tokio::test]
async fn pending_signals_are_consumed_in_arrival_order_not_insertion_order() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&approval_gate_info());

    let t0 = fixed_now();
    let exec = rt.start_workflow("approval_gate", json!(0)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(ref s) if s == "approval"));

    // Stage two "approval" signals OUT of arrival order:
    //   • FIRST (lowest signal_seq): the LATE one, arriving t0 + 3700s (past the
    //     1h deadline).
    //   • SECOND (higher signal_seq): the ON-TIME one, arriving t0 + 10s (before
    //     the deadline).
    rt.send_signal_as_of(
        exec,
        "approval",
        json!("late"),
        t0 + chrono::Duration::seconds(3700),
    )
    .unwrap();
    rt.send_signal_as_of(
        exec,
        "approval",
        json!("ontime"),
        t0 + chrono::Duration::seconds(10),
    )
    .unwrap();

    // Drive after both arrivals but BEFORE the 1h deadline (t0 + 20s).
    // `store::pending_signals` orders by `received_at` and ignores `now`. So
    // the on-time signal (10s < 3600s) comes first and wins. The deadline
    // timer is not due, so no stray `TimerFired` appears. The `signal_seq`
    // order put the LATE signal (3700s) first. The deadline timer then fired
    // first, and the result was `auto_rejected`. The late signal is ingested
    // too, although its `received_at` is after `now`. The ingest ignores `now`,
    // as the old wait path did.
    let done = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(20))
        .await
        .unwrap();
    assert!(
        matches!(done, RunState::Completed(ref v) if v.as_str() == Some("approved:ontime")),
        "the on-time signal (earliest received_at) must win the race, not the timeout; got {done:?}"
    );

    // The winning signal is recorded, and the timeout branch did NOT fire.
    let history = rt.load_history(exec).unwrap();
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::SignalReceived { payload, .. } if payload.as_str() == Some("ontime")
        )),
        "the on-time signal must be the one consumed:\n{history:?}"
    );
    let arrivals: Vec<&str> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::SignalReceived { payload, .. } => payload.as_str(),
            _ => None,
        })
        .collect();
    assert_eq!(
        arrivals,
        ["ontime", "late"],
        "history follows arrival order"
    );
    assert!(
        history
            .iter()
            .all(|e| !matches!(e, WorkflowEvent::TimerFired { .. })),
        "the deadline timer must not fire when an on-time signal wins:\n{history:?}"
    );

    // Cross-backend: the signal-win history replays on the core.
    let report = WorkflowReplayer::new()
        .register_fn("approval_gate", approval_gate_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the arrival-order signal-win history must replay on the core:\n{report}"
    );
}
