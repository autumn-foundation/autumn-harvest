//! Regression tests for two #1069 fixes on commit `48d54b2`.
//!
//! - **Timer fire order** (`queue.rs`): an `arm_seq` tie-breaker makes timers
//!   with the same `fire_at` (e.g. `tokio::join!(ctx.timer("a", 1),
//!   ctx.timer("b", 1))`) FIRE in `TimerStarted`-append order. The core
//!   `HistoryMatcher::match_timer` scans for its own `TimerFired` but STOPS at an
//!   unconsumed sibling `TimerFired`, so a reversed fire order wedges replay.
//! - **Declared retry policy** (`runtime.rs`): the typed
//!   `ctx.execute_activity(&info, ...)` and `execute_activity_with_opts` path
//!   carries the declared or per-call `RetryPolicy` in
//!   `ScheduleActivity::retry_policy_override`. The backend stores its
//!   `max_attempts` on the task row, and the worker honors it over the
//!   registered `ActivitySpec` default.

// The `#[workflow]`/`#[activity]` macros reference the fn's input parameter, so
// an unused param reads as a "used underscore binding" through the expansion.
#![allow(clippy::used_underscore_binding)]
// The retry-policy activities are `async` because `#[activity]` requires it, but
// hold no `.await`.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_harvest::TimerId;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

// ══════════════════════════════════════════════════════════════════════════════
// Timer fire order — equal-deadline timers fire in arm (TimerStarted) order.
// ══════════════════════════════════════════════════════════════════════════════

#[workflow]
async fn two_timers(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let (a, b) = tokio::join!(ctx.timer("a", 1), ctx.timer("b", 1));
    a.map_err(|e| e.to_string())?;
    b.map_err(|e| e.to_string())?;
    Ok(1)
}

/// A SQLite-driven `join!` of two same-deadline timers fires them in
/// `TimerStarted` order, and the history replays cleanly on the core
/// `WorkflowReplayer`. Before the `arm_seq` tie-break, `SQLite` left the fire
/// order of equal-`fire_at` rows unspecified. A reversed order wedged replay.
#[tokio::test]
async fn equal_deadline_timers_fire_in_arm_order_and_replay_on_core() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&two_timers_info());
    let exec = rt.start_workflow("two_timers", json!(0)).unwrap();

    let t0: DateTime<Utc> = DateTime::from_timestamp(4_000_000, 0).unwrap();
    // First pass arms both timers (deadline t0+1) and blocks.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    // Second pass fires both past their deadline and completes.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(5))
        .await
        .unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");

    let history = rt.load_history(exec).unwrap();

    // The two TimerStarted events are in arm order (a, b) …
    let started: Vec<&str> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::TimerStarted { timer_id, .. } => Some(timer_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(started, vec!["a", "b"], "TimerStarted order");
    // … and the TimerFired events fire in that SAME order — the guarantee.
    let fired: Vec<&str> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::TimerFired { timer_id } => Some(timer_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        fired,
        vec!["a", "b"],
        "equal-deadline timers must fire in TimerStarted order"
    );

    // And the whole history replays on the core engine.
    let report = WorkflowReplayer::new()
        .register_fn("two_timers", two_timers_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a same-deadline two-timer history must replay cleanly:\n{report}"
    );
}

/// Companion to the test above, updated for issue #1071. It replays a
/// history equal to the passing one EXCEPT that the two `TimerFired` events
/// are reversed.
///
/// Before #1071, `HistoryMatcher::match_timer` STOPPED at an unconsumed
/// sibling `TimerFired`, so that history WEDGED core replay
/// (`NonDeterminismDetected`). Issue #1071 made the forward-scan matcher
/// tolerate interleaved sibling terminals. `match_timer_strict` now crosses a
/// FOREIGN `TimerFired` without consuming it and still finds the own fire. So
/// the reversed order replays CLEANLY. The core test
/// `interleaved_sibling_multi_timer_reversed_fire_order_replays_succeeded` in
/// `replayer_tests` pins the same behavior. The `arm_seq` tie-break still
/// gives a STABLE arm-order fire sequence, as the test above asserts. It is no
/// longer needed for replay correctness. This test catches a core regression
/// that wedges reversed sibling fire order again.
#[tokio::test]
async fn reversed_timer_fire_order_replays_on_core_after_1071() {
    let reversed = vec![
        WorkflowEvent::workflow_started(json!(0), Utc::now()),
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("a"),
            duration_secs: 1,
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("b"),
            duration_secs: 1,
        },
        // Fired b BEFORE a — the reverse of TimerStarted order.
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("b"),
        },
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("a"),
        },
        WorkflowEvent::WorkflowCompleted { output: json!(1) },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("two_timers", two_timers_info().handler)
        .replay_from_events(reversed)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "after issue #1071 the core timer matcher tolerates a reversed equal-deadline \
         sibling fire order, so this history must replay cleanly:\n{report}"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// Declared retry policy — the declared or per-call policy wins over the
// registered ActivitySpec default.
// ══════════════════════════════════════════════════════════════════════════════

// The macro declares a policy of 4 attempts. The runtime registers its BODY
// with a DIFFERENT (1-attempt) ActivitySpec, so the two give different
// results.
#[activity(retry = RetryPolicy::fixed(4, Duration::from_millis(0)))]
async fn declared_retry_up(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn calls_declared_up(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&declared_retry_up_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

/// The registered spec allows 1 attempt, the DECLARED policy 4. The body fails
/// its first 3 attempts, then succeeds. The workflow completes ONLY if the
/// declared policy wins. With the registered spec, the activity fails after
/// one attempt, and so does the workflow.
#[tokio::test]
async fn declared_retry_policy_raises_attempts_over_registered_spec() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&calls_declared_up_info());
    let counter = Arc::new(AtomicU32::new(0));
    let c = counter.clone();
    // Registered with max_attempts = 1 (no retry) — deliberately BELOW the
    // declared policy so the two diverge.
    rt.register_activity_raw(
        "declared_retry_up",
        ActivitySpec::new(1, move |input: serde_json::Value| {
            let prior = c.fetch_add(1, Ordering::SeqCst);
            if prior < 3 {
                Err(format!("transient failure #{prior}"))
            } else {
                Ok(input)
            }
        }),
    );

    let exec = rt.start_workflow("calls_declared_up", json!(7)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(7)),
        "the declared 4-attempt policy must be honored so the flaky activity \
         completes (registered spec is 1); got {state:?}"
    );

    // Exactly 4 attempts were made (3 failures + 1 success) — proving the declared
    // cap of 4 governed, not the registered cap of 1.
    let attempts = rt.activity_attempts(exec, "declared_retry_up").unwrap();
    assert_eq!(
        attempts.len(),
        4,
        "declared max_attempts=4 governed retries"
    );
    assert_eq!(counter.load(Ordering::SeqCst), 4, "body ran 4 times");
}

// Declares a policy of 1 attempt (no retries). The runtime registers its body
// with a 5-attempt ActivitySpec.
#[activity(retry = RetryPolicy::fixed(1, Duration::from_millis(0)))]
async fn declared_retry_down(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn calls_declared_down(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&declared_retry_down_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

/// The registered spec allows 5 attempts, the DECLARED policy 1. The body
/// always fails. The workflow must fail after EXACTLY ONE attempt. So a call
/// that disabled retries cannot retry up to the registered cap. The bug ran 5
/// attempts.
#[tokio::test]
async fn declared_retry_policy_disables_retries_below_registered_spec() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&calls_declared_down_info());
    let counter = Arc::new(AtomicU32::new(0));
    let c = counter.clone();
    // Registered with max_attempts = 5 — deliberately ABOVE the declared policy.
    rt.register_activity_raw(
        "declared_retry_down",
        ActivitySpec::new(5, move |_input: serde_json::Value| {
            c.fetch_add(1, Ordering::SeqCst);
            Err("permanent failure".to_string())
        }),
    );

    let exec = rt.start_workflow("calls_declared_down", json!(1)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(_)),
        "the always-failing activity must fail the workflow; got {state:?}"
    );

    let attempts = rt.activity_attempts(exec, "declared_retry_down").unwrap();
    assert_eq!(
        attempts.len(),
        1,
        "declared max_attempts=1 must cap retries at one, NOT the registered 5"
    );
    assert_eq!(counter.load(Ordering::SeqCst), 1, "body ran exactly once");
}

// ── Regression guard: the raw `execute_activity_raw` path (no override) still
//    falls back to the registered ActivitySpec default.
// ─────────────────────────────────────────────────────────────────────────────

#[workflow]
async fn calls_raw(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("raw_flaky", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap_or_default())
}

/// The raw path carries `retry_policy_override: None`, so the worker still uses
/// the registered `ActivitySpec` cap (3 here).
#[tokio::test]
async fn raw_path_without_override_uses_registered_spec_default() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&calls_raw_info());
    let counter = Arc::new(AtomicU32::new(0));
    let c = counter.clone();
    rt.register_activity_raw(
        "raw_flaky",
        ActivitySpec::new(3, move |input: serde_json::Value| {
            let prior = c.fetch_add(1, Ordering::SeqCst);
            if prior < 2 {
                Err("transient".to_string())
            } else {
                Ok(input)
            }
        }),
    );

    let exec = rt.start_workflow("calls_raw", json!(9)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(9)),
        "the raw path must fall back to the registered 3-attempt spec; got {state:?}"
    );
    assert_eq!(
        rt.activity_attempts(exec, "raw_flaky").unwrap().len(),
        3,
        "registered spec default (3) governs the raw path"
    );
}
