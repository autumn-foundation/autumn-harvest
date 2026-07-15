//! Regression tests for `ctx.race()` over **activity** branches (issue #600) on
//! the single-writer backend — Codex #1069 P1 (`worker.rs:76`).
//!
//! The `ctx.race()` contract: only the WINNING branch runs to completion; every
//! still-pending losing branch is durably cancelled (a synthetic
//! `ActivityFailed { "lost race to a sibling branch" }`), and the loser's body
//! never runs. For a hedged-call / provider-race pattern this is what prevents a
//! double-charge / double-send.
//!
//! Pre-fix the `SQLite` worker's `drain_ready` drained EVERY ready branch to a
//! terminal event before the workflow was re-polled, so by the time `settle_race`
//! ran, every loser was already `DONE` (its body already ran) and there was
//! nothing left for `CancelRaceLosers` to cancel — BOTH providers always ran, a
//! real user-visible divergence from the Postgres backend.
//!
//! These tests drive the runtime with the deterministic `_as_of` clock seam so no
//! wall-clock sleeping is involved.

// The `#[workflow]` macro references the workflow's input parameter, so a
// deliberately-unused param reads as a "used underscore binding" through the
// expansion.
#![allow(clippy::used_underscore_binding)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Hedge two providers against each other and take the first to finish. Branch 0
/// (`provider_a`) is enqueued first (lowest task `seq`), so it wins the race
/// deterministically on this single-writer backend; `provider_b` is the loser.
#[workflow]
async fn hedged_call(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let winner = ctx
        .race()
        .activity_raw("provider_a", json!({}), "default")
        .activity_raw("provider_b", json!({}), "default")
        .run()
        .await
        .map_err(|e| e.to_string())?;
    let out: String = winner.decode().map_err(|e| e.to_string())?;
    Ok(out)
}

// ── The headline P1: only the winner's body runs; the loser is cancelled ──────

#[tokio::test]
async fn race_two_activities_only_winner_runs_loser_is_cancelled() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();

    // Per-provider call counters — the observable side effect of each branch's
    // body actually running.
    let a_calls = Arc::new(AtomicUsize::new(0));
    let b_calls = Arc::new(AtomicUsize::new(0));
    // The shared "charges" counter: a double-charge is `charges == 2`.
    let charges = Arc::new(AtomicUsize::new(0));

    rt.register_workflow(&hedged_call_info());
    {
        let a_calls = a_calls.clone();
        let charges = charges.clone();
        rt.register_activity_raw(
            "provider_a",
            ActivitySpec::new(1, move |_input: serde_json::Value| {
                a_calls.fetch_add(1, Ordering::SeqCst);
                charges.fetch_add(1, Ordering::SeqCst);
                Ok(json!("charged_by_a"))
            }),
        );
    }
    {
        let b_calls = b_calls.clone();
        let charges = charges.clone();
        rt.register_activity_raw(
            "provider_b",
            ActivitySpec::new(1, move |_input: serde_json::Value| {
                b_calls.fetch_add(1, Ordering::SeqCst);
                charges.fetch_add(1, Ordering::SeqCst);
                Ok(json!("charged_by_b"))
            }),
        );
    }

    let exec = rt.start_workflow("hedged_call", json!(0)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();

    assert!(
        matches!(state, RunState::Completed(ref v) if v == "charged_by_a"),
        "branch 0 (provider_a) must win deterministically, got {state:?}"
    );

    // The loser's body NEVER runs (the whole point of the race cancellation).
    assert_eq!(
        b_calls.load(Ordering::SeqCst),
        0,
        "the LOSING branch (provider_b) body must NEVER run"
    );
    assert_eq!(
        a_calls.load(Ordering::SeqCst),
        1,
        "the winning branch (provider_a) body runs exactly once"
    );
    // No double-charge.
    assert_eq!(
        charges.load(Ordering::SeqCst),
        1,
        "exactly ONE provider may charge — a double-charge (2) is the P1 bug"
    );

    // History: the winner has an ActivityCompleted; the loser has the synthetic
    // "lost race" ActivityFailed (never ran, cancelled while pending).
    let history = rt.load_history(exec).unwrap();
    let completed = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
        .count();
    assert_eq!(completed, 1, "exactly one branch completes (the winner)");
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
        "exactly one loser is cancelled with the synthetic 'lost race' failure"
    );
}

// ── Fan-out regression: wait-ALL still runs every branch ──────────────────────

/// A fan-out (wait-ALL) over N inputs: every branch body must run and every
/// result must be collected. This proves the race drain-one fix did NOT regress
/// fan-out, which schedules N activities in one batch but resolves only when all
/// N complete and emits NO `CancelRaceLosers`.
#[workflow]
async fn fan_out_all(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let inputs: Vec<(String, serde_json::Value, String)> = (0..n)
        .map(|i| ("fan_worker".to_string(), json!(i), "default".to_string()))
        .collect();
    let results: Vec<serde_json::Value> = ctx
        .execute_activity_fan_out_raw(inputs)
        .await
        .map_err(|e| e.to_string())?;
    let sum: i64 = results.iter().filter_map(serde_json::Value::as_i64).sum();
    Ok(sum)
}

#[tokio::test]
async fn fan_out_still_runs_all_branches() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));

    rt.register_workflow(&fan_out_all_info());
    {
        let calls = calls.clone();
        rt.register_activity_raw(
            "fan_worker",
            ActivitySpec::new(1, move |input: serde_json::Value| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(input) // echo the index back
            }),
        );
    }

    let exec = rt.start_workflow("fan_out_all", json!(5)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();

    // 0+1+2+3+4 = 10
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(10)),
        "fan-out must collect ALL branch results, got {state:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        5,
        "every one of the 5 fan-out branch bodies must run"
    );
}

// ── Replay stability: the race history replays cleanly on the core engine ─────

#[tokio::test]
async fn race_history_replays_on_core_engine() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();

    rt.register_workflow(&hedged_call_info());
    rt.register_activity_raw(
        "provider_a",
        ActivitySpec::new(1, |_input: serde_json::Value| Ok(json!("charged_by_a"))),
    );
    rt.register_activity_raw(
        "provider_b",
        ActivitySpec::new(1, |_input: serde_json::Value| Ok(json!("charged_by_b"))),
    );

    let exec = rt.start_workflow("hedged_call", json!(0)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "must complete");

    let history = rt.load_history(exec).unwrap();
    let report = WorkflowReplayer::new()
        .register_fn("hedged_call", hedged_call_info().handler)
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the race-produced history (winner completed, loser 'lost race') must \
         replay deterministically on the core engine:\n{report}"
    );
}
