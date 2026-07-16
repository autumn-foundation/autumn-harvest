//! Fairness regression for `ctx.race()` over **activity** branches whose losing
//! branch uses a ZERO-DELAY retry (issue #1068, hardening item 3).
//!
//! # The bug
//!
//! `drain_ready` claims the oldest-by-`seq` ready task (`ORDER BY seq LIMIT 1`).
//! A retryable failure requeues the task via [`queue::requeue_task`], which — pre
//! fix — kept the task's ORIGINAL `seq`. When the retry delay is zero (a
//! `RetryPolicy::fixed(_, 0ms)` policy, or the raw/no-policy immediate-retry
//! default) the requeue also lands at `run_at = now`, so the just-failed branch is
//! *immediately re-claimable in the SAME drain pass* AND still holds the lowest
//! `seq` — so `claim_next_ready_task_tx` re-selects it before any ready SIBLING branch
//! gets a turn. Under a `ctx.race()`, a low-index branch that fails-and-retries can
//! therefore monopolize the drain, exhaust all its attempts, terminal-out (even by
//! FAILING), and *settle the race first* — cancelling a ready sibling that would
//! have finished (e.g. an instant success) without ever running it. That biases the
//! winner toward the lowest-index branch and violates "first branch to finish
//! wins".
//!
//! # The fix
//!
//! On an IMMEDIATE (zero-delay) retry requeue — `run_at <= now`, so the task would
//! be re-claimable THIS pass — the task is moved to the BACK of the ready queue
//! (assigned a fresh, highest `seq`), so `claim_next_ready_task_tx(ORDER BY seq)`
//! yields to every other ready sibling before returning to the just-failed branch
//! (round-robin fairness). A NON-immediate retry (a real positive backoff,
//! `run_at > now`) already yields naturally — its future `run_at` makes it
//! un-claimable this pass — and is left unchanged.
//!
//! No wall-clock sleeping: driven with the deterministic `_as_of` clock seam.

// The `#[workflow]`/`#[activity]` macros reference their input params, so a
// deliberately-unused param reads as a "used underscore binding" through expansion.
#![allow(clippy::used_underscore_binding)]
// The `#[activity]` fixture bodies are trivial and await-free — the crate runs a
// registered `ActivitySpec`, not the macro body.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

const fn t0() -> DateTime<Utc> {
    match DateTime::from_timestamp(4_000_000, 0) {
        Some(t) => t,
        None => unreachable!(),
    }
}

/// The maximum attempts of the always-failing, zero-delay-retry branch. Large
/// enough that "A ran ~1 time" (fair yield) is unmistakably distinct from "A ran
/// all N times" (monopoly).
const A_MAX_ATTEMPTS: u32 = 5;

// Branch A: fixed(N, 0ms) — a genuine ZERO-DELAY retry policy on the info path.
// A raw (`activity_raw`) race dispatch carries no override, so the worker falls
// back to this registered `default_retry_policy` at claim time (the
// `raw_path_falls_back_to_registered_default_retry_policy_backoff` contract), and
// `compute_retry_delay(fixed(_, 0ms)) == 0` → an IMMEDIATE requeue at `now`.
#[activity(retry = RetryPolicy::fixed(A_MAX_ATTEMPTS, Duration::ZERO))]
async fn slow_branch_a(_ctx: &ActivityContext, _n: i64) -> Result<String, String> {
    Ok("never".to_string())
}

// The race: branch A (added FIRST → lower task `seq`) always fails and retries
// with zero delay; branch B (added SECOND → higher `seq`) succeeds on the first
// attempt. "First branch to finish wins" ⇒ B must win.
#[workflow]
async fn fairness_race(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let winner = ctx
        .race()
        .activity_raw("slow_branch_a", json!({}), "default")
        .activity_raw("fast_branch_b", json!({}), "default")
        .run()
        .await
        .map_err(|e| e.to_string())?;
    let out: String = winner.decode().map_err(|e| e.to_string())?;
    Ok(out)
}

/// Headline fairness test — a zero-delay-retry branch must YIELD to a faster
/// sibling between immediate retries instead of monopolizing the drain.
///
/// RED pre-fix: branch A (lowest `seq`, zero-delay retry) re-claims itself every
/// immediate retry, exhausts all `A_MAX_ATTEMPTS`, terminal-outs by FAILING, and
/// settles the race first — so the winner is A's failure (the workflow `Failed`),
/// NOT B, and A's body ran `A_MAX_ATTEMPTS` times while B never ran.
#[tokio::test]
async fn zero_delay_retry_branch_yields_to_a_faster_sibling() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();

    let a_calls = Arc::new(AtomicUsize::new(0));
    let b_calls = Arc::new(AtomicUsize::new(0));

    rt.register_workflow(&fairness_race_info());
    {
        let a_calls = a_calls.clone();
        // Info-based registration so the raw race dispatch falls back to the
        // `fixed(N, 0ms)` `default_retry_policy` at claim time.
        rt.register_activity(&slow_branch_a_info(), move |_input: serde_json::Value| {
            a_calls.fetch_add(1, Ordering::SeqCst);
            Err("branch A always fails".to_string())
        });
    }
    {
        let b_calls = b_calls.clone();
        rt.register_activity_raw(
            "fast_branch_b",
            ActivitySpec::new(1, move |_input: serde_json::Value| {
                b_calls.fetch_add(1, Ordering::SeqCst);
                Ok(json!("B_WINS"))
            }),
        );
    }

    let exec = rt.start_workflow("fairness_race", json!(0)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();

    // (1) The faster sibling B wins — "first branch to finish wins".
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "B_WINS"),
        "the faster sibling (branch B, instant success) must WIN the race; got \
         {state:?} (RED pre-fix: branch A monopolizes the drain and settles the \
         race by failing first)"
    );

    // (2) Branch A yielded after its FIRST immediate retry rather than
    // monopolizing the drain for all `A_MAX_ATTEMPTS` attempts. It ran exactly
    // once: attempt 1 fails → requeue to the BACK of the ready queue → B is now
    // the lowest-`seq` ready task → B runs → terminal → race settles on B → A is
    // cancelled while PENDING.
    assert_eq!(
        a_calls.load(Ordering::SeqCst),
        1,
        "branch A must run its body exactly ONCE then yield to the ready sibling; \
         got {} (RED pre-fix: A monopolizes and runs all {A_MAX_ATTEMPTS} attempts)",
        a_calls.load(Ordering::SeqCst),
    );
    assert!(
        a_calls.load(Ordering::SeqCst) < A_MAX_ATTEMPTS as usize,
        "branch A must NOT exhaust all its attempts before the sibling gets a turn"
    );
    // The winning sibling ran exactly once.
    assert_eq!(
        b_calls.load(Ordering::SeqCst),
        1,
        "the winning sibling (branch B) runs exactly once"
    );

    // History: exactly one branch completes (B). A never reaches a terminal in
    // history — it is cancelled while PENDING via the synthetic 'lost race'
    // `ActivityFailed` (its retries are audit-only, not in the event log).
    let history = rt.load_history(exec).unwrap();
    let completed = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
        .count();
    assert_eq!(completed, 1, "exactly one branch completes (the winner B)");
    let lost_race = history
        .iter()
        .filter(|e| {
            matches!(
                e,
                WorkflowEvent::ActivityFailed { error, .. }
                    if error == "lost race to a sibling branch"
            )
        })
        .count();
    assert_eq!(
        lost_race, 1,
        "the losing branch A is cancelled with the synthetic 'lost race' failure"
    );
}

// ── The seq-bump must not break single-activity convergence ───────────────────

#[activity(retry = RetryPolicy::fixed(3, Duration::ZERO))]
async fn flaky_zero_delay(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn flaky_zero_delay_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("flaky_zero_delay", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap() * 2)
}

/// A single (non-race) activity with a zero-delay `fixed(3, 0ms)` policy that
/// fails twice then succeeds must still converge in one drive — the fair-yield
/// seq-bump (bumping a single task with no siblings) is harmless: with nothing
/// else ready, the bumped task is simply re-claimed immediately.
#[tokio::test]
async fn zero_delay_retry_still_converges_without_a_race() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();

    rt.register_workflow(&flaky_zero_delay_wf_info());
    rt.register_activity(&flaky_zero_delay_info(), move |input: serde_json::Value| {
        // Fail attempts 1 and 2, succeed on attempt 3.
        if body_calls.fetch_add(1, Ordering::SeqCst) < 2 {
            Err("transient".to_string())
        } else {
            Ok(input)
        }
    });

    let exec = rt.start_workflow("flaky_zero_delay_wf", json!(21)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();

    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(42)),
        "a zero-delay-retry single activity (fail, fail, succeed) must still \
         converge in one drive; got {state:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "the body runs three times (two failures + one success)"
    );
}

// ── Replay determinism: the fair-yield winner is pinned in history ────────────

/// The fairness fix only changes CLAIM ORDER within a live drain — the winner is
/// still recorded via the core `race_winner:{seq}` marker, so the produced history
/// (a) replays deterministically on the core engine and (b) pins branch B (index
/// 1) as the winner. This proves the fair-yield outcome is replay-deterministic by
/// construction.
#[tokio::test]
async fn race_winner_is_replay_deterministic_after_fair_yield() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();

    rt.register_workflow(&fairness_race_info());
    rt.register_activity(&slow_branch_a_info(), |_input: serde_json::Value| {
        Err("branch A always fails".to_string())
    });
    rt.register_activity_raw(
        "fast_branch_b",
        ActivitySpec::new(1, |_input: serde_json::Value| Ok(json!("B_WINS"))),
    );

    let exec = rt.start_workflow("fairness_race", json!(0)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "B_WINS"),
        "must complete with B as the winner; got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();

    // The winner is pinned in history: a `race_winner:*` marker resolving to
    // branch index 1 (B). Deterministic on replay by construction.
    let winner_marker = history.iter().find_map(|e| match e {
        WorkflowEvent::MarkerRecorded { name, details } if name.starts_with("race_winner:") => {
            Some(details.clone())
        }
        _ => None,
    });
    assert_eq!(
        winner_marker,
        Some(json!(1)),
        "the recorded race_winner marker must pin branch index 1 (the faster \
         sibling B) as the winner, making the outcome deterministic on replay"
    );

    // And the recorded history replays cleanly on the core engine, reproducing B.
    let report = WorkflowReplayer::new()
        .register_fn("fairness_race", fairness_race_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the fair-yield race history must replay deterministically on the core \
         engine:\n{report}"
    );
}
