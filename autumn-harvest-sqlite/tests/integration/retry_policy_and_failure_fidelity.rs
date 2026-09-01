//! Cross-backend fidelity regressions (issue #1069 P2, Codex review of `9e480b8`):
//!
//! - **FIX 1 (`worker.rs:299`)** — a terminal `ActivityFailed` DECODES the failure
//!   envelope (`parse_error_payload_full`) so it carries the same typed metadata
//!   (`error_type` / `details` / `non_retryable` + the human `message`) the
//!   Postgres worker's `finalize_activity_failure` records, instead of hard-coding
//!   `error_type = "Error"`, `non_retryable = false`, `details = None` and storing
//!   the raw `harvest_activity_failure_v1` envelope as the error.
//! - **FIX 2 (`runtime.rs:985`)** — the worker honors the WHOLE persisted
//!   `RetryPolicy`, not just `max_attempts`: a retryable failure is requeued at
//!   `now + backoff_delay` (`compute_retry_delay`), NOT immediately at `now`; and a
//!   non-retryable failure (the typed `non_retryable` flag OR a
//!   `non_retryable_errors` match) fails terminally without consuming further
//!   attempts or repeating the body.
//! - **FIX 3 (`store.rs:327`)** — pending signals are consumed in ARRIVAL order
//!   (`ORDER BY received_at, signal_seq`), so a `wait_for_signal_timeout` race
//!   compares the correct arrival time against the deadline: an on-time signal
//!   staged AFTER a late one still wins.
//!
//! No Docker: `SQLite` is embedded (`rusqlite` `bundled`); backoff timing and the
//! signal race are driven by the injected `*_as_of` clock so nothing sleeps.

// The `#[workflow]`/`#[activity]` macros reference their input params, so a
// deliberately-unused param reads as a "used underscore binding" through expansion.
#![allow(clippy::used_underscore_binding)]
// The `#[activity]` fixture bodies are trivial and await-free — the crate runs a
// registered `ActivitySpec`, not the macro body.
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

/// The `run_at` (epoch-ms) of the single `PENDING` task for `exec`, read from a
/// raw connection to the same file (the inspection pattern from
/// `signal_timeout.rs` / `reopen_consistency.rs`). `None` if no pending task.
fn pending_task_run_at(path: &std::path::Path, exec: ExecutionId) -> Option<i64> {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT run_at FROM harvest_tasks WHERE exec_id = ?1 AND state = 'PENDING'",
        [exec.to_string()],
        |r| r.get(0),
    )
    .ok()
}

/// The single stored `ActivityFailed` event for `exec` (panics if 0 or >1).
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

// ── FIX 1: typed activity-failure envelope is decoded into the terminal event ──

// Registered body only — scheduled via the raw path (no policy). `max_attempts`
// therefore comes from the `ActivitySpec` (1), so the very first typed failure is
// terminal and the decode is what's under test (independent of the retry gate).
#[workflow]
async fn typed_fail_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("charge", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap_or_default())
}

// RED pre-fix (`worker.rs:299`): the terminal `ActivityFailed` stored the raw
// envelope as `error` and hard-coded `error_type = "Error"`, `non_retryable =
// false`, `details = None`, so a typed activity failure lost all its metadata and
// was not byte-equivalent to the Postgres worker's history.
#[tokio::test]
async fn terminal_activity_failure_decodes_typed_envelope_and_replays() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&typed_fail_wf_info());
    rt.register_activity_raw(
        "charge",
        ActivitySpec::new(1, |_input: serde_json::Value| {
            // A typed (retryable) ActivityFailure with a distinct error_type +
            // structured details. max_attempts = 1 → terminal on the first failure.
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

    // The terminal ActivityFailed must carry the DECODED typed metadata, not the
    // hard-coded `"Error"`/false/None and not the raw envelope.
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

    // Cross-backend byte-identity (AC5): the decoded history replays DETERMINISTICALLY
    // on the core — this workflow legitimately fails (the activity failed), so the
    // replay reproduces the failure (`WorkflowFailed`) rather than `ReplaySucceeded`;
    // the important guarantee is NO non-determinism drift, and that the decoded human
    // message flows through identically.
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

// ── FIX 2(a): a retry is DELAYED by the policy's backoff, not immediate ────────

// Carries a fixed 60s retry policy on its `_info()`, so `execute_activity`
// resolves `retry_policy_override = Some(RetryPolicy::fixed(3, 60s))`.
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

// RED pre-fix (`runtime.rs:985`): only `max_attempts` was persisted, so the retry
// was requeued at `now` and re-ran immediately in the SAME drain pass — the run
// completed at `t0` and `run_at` was never in the future.
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
            // Fail attempt 1, succeed attempt 2.
            if body_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("transient".to_string())
            } else {
                Ok(json!(input.as_i64().unwrap()))
            }
        }),
    );

    let t0 = fixed_now();
    let exec = rt.start_workflow("flaky_wf", json!(21)).unwrap();

    // Drive at t0: attempt 1 fails and is requeued 60s out. The run does NOT
    // complete — it blocks on the backing-off activity, and the body ran ONCE.
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

    // The requeued task's run_at is ≈ t0 + 60s (60_000 ms), not t0.
    let t0_ms = t0.timestamp_millis();
    let run_at = pending_task_run_at(&path, exec).expect("a pending backing-off task");
    assert_eq!(
        run_at,
        t0_ms + 60_000,
        "the fixed(3, 60s) policy must delay the requeue by 60s, not requeue at now"
    );

    // A poll one second BEFORE the deadline still must not re-run it.
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

    // Advance past the deadline: the retry now runs and the workflow completes.
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

// ── FIX A (worker.rs:328): the backoff anchors to the FAILURE time ─────────────

// Wall-clock companion to `retry_policy_backoff_delays_the_requeue` above. That
// test drives via `_as_of` (a fixed clock), so `failure_now == cycle-start now`
// and the failure-time anchor is indistinguishable from the cycle-start one. This
// installs an ADVANCING clock via `set_clock` so the body appears to consume real
// time (45s) before failing: the retry's 60s backoff must anchor to the FAILURE
// instant (t0+45s), NOT the cycle start (t0) — matching Postgres, which computes
// the delay AFTER handling the result.
//
// The clock returns t0 on the FIRST read (cycle-1 start) and t0+45s on every
// subsequent read (the finalize-time `failure_now`, and later cycle starts).
//
// RED pre-fix (Codex #1069 P2, worker.rs:328): `run_at = cycle_start_now + delay`,
// so the retry was anchored to t0 + 60s regardless of how long the body ran.
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
            // Attempt 1 fails (retryable); the run then blocks on the backoff.
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
        // First read = cycle-1 start (t0); the body then "runs" 45s and fails, so
        // the finalize-time `failure_now` (and every later read) is t0+45s.
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

// ── FIX 2(b): a non-retryable failure is NOT retried ───────────────────────────

// Typed non-retryable: an exponential(3) policy would otherwise retry, but a typed
// `ActivityFailure { non_retryable: true }` must go terminal on the first attempt.
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

// RED pre-fix: the worker decided retries solely from `max_attempts` and ignored
// the typed `non_retryable` flag, so the body was re-run up to 3 times.
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

// RED pre-fix: the policy's `non_retryable_errors` list was dropped entirely, so a
// legacy `Err("fatal")` was retried despite the policy saying to stop.
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

// ── FINDING B (`runtime.rs:331`): the RAW path falls back to the registered
//    ActivityInfo default_retry_policy (backoff + non_retryable_errors) ────────────
//
// The tests in the `FIX 2` sections above prove the TYPED `execute_activity` path (which
// carries the resolved `RetryPolicy` on the `ScheduleActivity` command as
// `retry_policy_override`). Finding B is the RAW path: a workflow that schedules an
// info-registered activity BY NAME via `execute_activity_raw` — so the command carries
// NO override (`task.retry_policy == None`). The registration path used to collapse the
// declared `default_retry_policy` down to only `max_attempts`, so the raw path lost BOTH
// the backoff timing AND the `non_retryable_errors` list. The Postgres dispatch path
// falls back to the registered activity's FULL `default_retry_policy` when the command
// carries no override; this backend must do the same, resolved at CLAIM time from the
// registered `ActivitySpec` (mirroring `schedule_to_close`/`start_to_close`).

// Carries a fixed 60s retry policy on its `_info()`. Scheduled below via the RAW path
// (`execute_activity_raw`), so the command carries no override and the backoff must come
// from the CLAIM-time fallback to the registered spec's `default_retry_policy`.
#[activity(retry = RetryPolicy::fixed(3, Duration::from_secs(60)))]
async fn raw_backoff_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

// Calls the RAW dispatch path BY NAME — no `retry_policy_override` on the command.
#[workflow]
async fn raw_backoff_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("raw_backoff_act", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap() * 2)
}

// RED pre-fix (`runtime.rs:331`): the registered spec collapsed `default_retry_policy`
// to only `max_attempts`, and the raw command carried no override, so `task.retry_policy
// == None` → the worker requeued IMMEDIATELY at `now` (no backoff). The run converged in
// one drain pass (Completed) and the body ran twice with no wait, instead of blocking on
// the registered 60s backoff after one failure.
#[tokio::test]
async fn raw_path_falls_back_to_registered_default_retry_policy_backoff() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();

    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&raw_backoff_wf_info());
    // INFO-based registration: the spec must carry the FULL `default_retry_policy`, so a
    // raw (no-override) dispatch can fall back to it at claim time.
    rt.register_activity(&raw_backoff_act_info(), move |input: serde_json::Value| {
        // Fail attempt 1, succeed attempt 2 (like `flaky_wf`).
        if body_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err("transient".to_string())
        } else {
            Ok(input)
        }
    });

    let t0 = fixed_now();
    let exec = rt.start_workflow("raw_backoff_wf", json!(21)).unwrap();

    // Drive at t0: attempt 1 fails and MUST be requeued 60s out (the registered policy's
    // backoff), so the run blocks on the backing-off activity — it does NOT converge, and
    // the body ran exactly ONCE.
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

    // The requeued task's run_at is ≈ t0 + 60s (60_000 ms), not t0.
    let run_at = pending_task_run_at(&path, exec).expect("a pending backing-off task");
    assert_eq!(
        run_at,
        t0.timestamp_millis() + 60_000,
        "the registered fixed(3, 60s) policy must delay the raw-path requeue by 60s, \
         not requeue at now"
    );

    // Past the backoff deadline the retry runs and the run completes (2 body calls).
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

// RED pre-fix (`runtime.rs:331`): the raw path carried no override, so `task.retry_policy
// == None` and the registered policy's `non_retryable_errors` list was ignored → the body
// was retried up to `max_attempts` (5) instead of failing terminally on the first attempt.
#[tokio::test]
async fn raw_path_honors_registered_policy_non_retryable_errors() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();

    rt.register_workflow(&raw_nonret_wf_info());
    // INFO-based registration carries the full policy (incl. `non_retryable_errors`).
    rt.register_activity(&raw_nonret_act_info(), move |_input: serde_json::Value| {
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

// ── FIX 3: pending signals consumed in ARRIVAL order, not insertion order ──────

const ONE_HOUR: Duration = Duration::from_secs(3600);

// Await an approval with a 1h deadline: `approved:<who>` on a signal, else
// `auto_rejected` if the deadline fires first (the human-in-the-loop shape).
#[workflow]
async fn approval_gate(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let decision = ctx
        .wait_for_signal_timeout("approval", ONE_HOUR)
        .await
        .map_err(|e| e.to_string())?;
    Ok(decision.map_or_else(
        || "auto_rejected".to_string(),
        |payload| format!("approved:{}", payload.as_str().unwrap_or_default()),
    ))
}

// RED pre-fix (`store.rs:327`): `peek_pending_signal` ordered by `signal_seq`, so
// the LATE signal (staged first, lowest seq) was consumed; its late `received_at`
// let the deadline timer fire first → the race resolved to `auto_rejected` while
// the on-time signal stayed queued. With `ORDER BY received_at, signal_seq` the
// on-time signal (earliest arrival) is consumed and wins.
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

    // Drive AFTER both arrivals but BEFORE the 1h deadline (t0 + 20s). Arrival-order
    // consumption (`peek_pending_signal` orders by `received_at`, unfiltered by
    // `now`) picks the on-time signal (received_at = 10 < deadline 3600), so the
    // signal WINS the race cleanly — the deadline timer is not yet due, so there is
    // no stray later `TimerFired` to reason about. With the pre-fix `signal_seq`
    // ordering the LATE signal (received_at 3700 > deadline) would be consumed and
    // the deadline timer would fire first → `auto_rejected`.
    let done = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(20))
        .await
        .unwrap();
    assert!(
        matches!(done, RunState::Completed(ref v) if v.as_str() == Some("approved:ontime")),
        "the on-time signal (earliest received_at) must win the race, not the timeout; got {done:?}"
    );

    // The winning signal is recorded, and the timeout branch did NOT fire (no
    // TimerFired ahead of the signal).
    let history = rt.load_history(exec).unwrap();
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::SignalReceived { payload, .. } if payload.as_str() == Some("ontime")
        )),
        "the on-time signal must be the one consumed:\n{history:?}"
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
