//! A race whose signal arrives before the race opens.
//!
//! The signal lands while an earlier activity backs off on a retry. The
//! race then opens and takes the buffered signal at once. Every later pass
//! must replay that outcome.

#![allow(clippy::used_underscore_binding)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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

#[activity(retry = RetryPolicy::fixed(3, Duration::from_secs(10)))]
async fn notice(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn notice_then_race(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let _: i64 = ctx
        .execute_activity(&notice_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    let first = ctx
        .race()
        .signal("decision")
        .signal("interrupt")
        .timer(Duration::from_secs(600))
        .run()
        .await
        .map_err(|e| e.to_string())?;
    // A second wait after the race, as the agent loop has.
    ctx.timer("after", 1).await.map_err(|e| e.to_string())?;
    Ok(i64::try_from(first.index).unwrap_or(-1))
}

async fn run_case(signal: &str, expected_index: i64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("race.db");
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&notice_then_race_info());
    rt.register_activity_raw(
        "notice",
        ActivitySpec::new(3, move |input: serde_json::Value| {
            if body_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("transient".to_string())
            } else {
                Ok(input)
            }
        }),
    );
    let exec = rt.start_workflow("notice_then_race", json!(1)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");

    // The signal lands while the activity backs off, before the race opens.
    rt.send_signal(exec, signal, json!({"ok": true})).unwrap();
    let mut at = t0();
    let mut state = RunState::WaitingTimer;
    for _ in 0..5 {
        at += chrono::Duration::seconds(15);
        state = rt.run_until_blocked_as_of(exec, at).await.unwrap();
        if !matches!(state, RunState::WaitingTimer) {
            break;
        }
    }
    let RunState::Completed(output) = state else {
        panic!("the run completes, got {state:?}");
    };
    assert_eq!(output, json!(expected_index));

    let events = rt.load_history(exec).unwrap();
    let replay = WorkflowReplayer::new()
        .register(vec![notice_then_race_info()])
        .with_execution_id(exec)
        .replay_from_events(events)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

#[tokio::test]
async fn a_first_branch_signal_that_arrived_before_the_race_replays() {
    run_case("decision", 0).await;
}

#[tokio::test]
async fn a_later_branch_signal_that_arrived_before_the_race_replays() {
    run_case("interrupt", 1).await;
}

#[workflow]
async fn notice_then_race_activity(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let _: i64 = ctx
        .execute_activity(&notice_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    let first = ctx
        .race()
        .activity_raw("slow", json!({}), "default")
        .signal("decision")
        .run()
        .await
        .map_err(|e| e.to_string())?;
    ctx.timer("after", 1).await.map_err(|e| e.to_string())?;
    Ok(i64::try_from(first.index).unwrap_or(-1))
}

#[tokio::test]
async fn an_activity_branch_beside_a_buffered_signal_replays() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("race.db");
    let calls = Arc::new(AtomicUsize::new(0));
    let slow_calls = Arc::new(AtomicUsize::new(0));
    let (body_calls, slow_body) = (calls.clone(), slow_calls.clone());
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&notice_then_race_activity_info());
    rt.register_activity_raw(
        "notice",
        ActivitySpec::new(3, move |input: serde_json::Value| {
            if body_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("transient".to_string())
            } else {
                Ok(input)
            }
        }),
    );
    rt.register_activity_raw(
        "slow",
        ActivitySpec::new(1, move |_input: serde_json::Value| {
            slow_body.fetch_add(1, Ordering::SeqCst);
            Ok(json!("slow"))
        }),
    );
    let exec = rt
        .start_workflow("notice_then_race_activity", json!(1))
        .unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0()).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
    rt.send_signal(exec, "decision", json!({"ok": true}))
        .unwrap();
    let mut at = t0();
    let mut state = RunState::WaitingTimer;
    for _ in 0..5 {
        at += chrono::Duration::seconds(15);
        state = rt.run_until_blocked_as_of(exec, at).await.unwrap();
        if !matches!(state, RunState::WaitingTimer) {
            break;
        }
    }
    let RunState::Completed(output) = state else {
        panic!("the run completes, got {state:?}");
    };
    assert_eq!(output, json!(1), "the buffered signal wins");
    assert_eq!(
        slow_calls.load(Ordering::SeqCst),
        0,
        "the losing activity never ran"
    );

    let events = rt.load_history(exec).unwrap();
    let replay = WorkflowReplayer::new()
        .register(vec![notice_then_race_activity_info()])
        .with_execution_id(exec)
        .replay_from_events(events)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}
