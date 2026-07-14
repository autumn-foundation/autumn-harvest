//! Integration smoke tests for the `SQLite` feasibility spike (issue #966).
//!
//! Throwaway R&D — feature-gated behind `sqlite-spike`, no Docker required
//! (`SQLite` is embedded via `rusqlite`'s `bundled` feature). These tests
//! exercise the four durability scenarios the spike must prove plus the
//! cross-backend replay guarantee (AC4): a history written by the `SQLite`
//! prototype replays cleanly on the engine's own (Postgres-oriented)
//! `WorkflowReplayer` path, because both backends serialize the *same*
//! `WorkflowEvent` via `serde_json`.
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::prelude::*;
use autumn_harvest::sqlite_spike::{ActivitySpec, RunState, SqliteRuntime};
use serde_json::json;

// ── Test workflows (real `#[workflow]` fns — the reused determinism core) ──

/// One activity ("work"), then double its integer output. Used by the retry,
/// crash-replay, and cross-backend scenarios.
#[workflow]
async fn single_activity(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().ok_or("bad activity output")? * 2)
}

/// Arm a durable timer, then return a constant. Used by the timer-across-restart
/// scenario.
#[workflow]
async fn timer_then_done(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    ctx.timer("sleep", 5).await.map_err(|e| e.to_string())?;
    Ok("woke_up".to_string())
}

/// Wait for a signal, then echo its payload. Used by the signal-delivery scenario.
#[workflow]
async fn wait_then_echo(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    let payload = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(payload)
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn temp_db() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("spike.sqlite3")
        .to_str()
        .unwrap()
        .to_string();
    (dir, path)
}

fn count_events(events: &[WorkflowEvent], pred: impl Fn(&WorkflowEvent) -> bool) -> usize {
    events.iter().filter(|e| pred(e)).count()
}

// ── Scenario 1: activity retry (fail once, then succeed) ─────────────────────

#[tokio::test]
async fn scenario_1_activity_retry_then_success() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&single_activity_info());

    // "work" fails on attempt 1, succeeds on attempt 2 (returns n * 10).
    let attempts = Arc::new(AtomicUsize::new(0));
    let a2 = attempts.clone();
    rt.register_activity(
        "work",
        ActivitySpec::new(3, move |input: serde_json::Value| {
            let n = a2.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                Err("transient boom".to_string())
            } else {
                Ok(json!(input.as_i64().unwrap() * 10))
            }
        }),
    );

    let exec = rt.start_workflow("single_activity", json!(5)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    // (5 * 10) * 2 == 100.
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(100)),
        "state = {state:?}"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "activity should run exactly twice"
    );

    // The durable per-attempt audit log records the retry: attempt 1 failed,
    // attempt 2 succeeded.
    let log = rt.activity_attempts(exec, "work").unwrap();
    assert_eq!(log.len(), 2, "two attempts recorded: {log:?}");
    assert_eq!(log[0].attempt, 1);
    assert!(log[0].result.is_err(), "attempt 1 failed");
    assert_eq!(log[1].attempt, 2);
    assert!(log[1].result.is_ok(), "attempt 2 succeeded");

    // The *replayable* workflow event log holds the terminal outcome only
    // (ActivityScheduled -> ActivityCompleted), mirroring the PG engine, whose
    // retryable failures live on the task-queue row, not `harvest_events`.
    let events = rt.load_history(exec).unwrap();
    assert_eq!(
        count_events(&events, |e| matches!(
            e,
            WorkflowEvent::ActivityScheduled { .. }
        )),
        1
    );
    assert_eq!(
        count_events(&events, |e| matches!(
            e,
            WorkflowEvent::ActivityCompleted { .. }
        )),
        1
    );
    assert_eq!(
        count_events(&events, |e| matches!(
            e,
            WorkflowEvent::ActivityFailed { .. }
        )),
        0,
        "retryable failures are not in the replayable log"
    );
}

// ── Scenario 2: durable timer fires across a process restart ─────────────────

#[tokio::test]
async fn scenario_2_timer_fires_across_restart() {
    let (_dir, path) = temp_db();

    // Runtime #1: arm the timer, then simulate process exit WITHOUT firing it.
    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&timer_then_done_info());
        let exec = rt.start_workflow("timer_then_done", json!(0)).unwrap();
        let state = rt.run_until_blocked(exec).await.unwrap();
        assert!(matches!(state, RunState::WaitingTimer), "state = {state:?}");
        // The timer is durable; drop the runtime (== process exit).
        exec
    };

    // Runtime #2: fresh process on the SAME file. Advance time past the
    // deadline; the durable timer must fire and the workflow must complete.
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&timer_then_done_info());
    rt.advance_time(5);
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("woke_up")),
        "state = {state:?}"
    );
    let events = rt.load_history(exec).unwrap();
    assert_eq!(
        count_events(&events, |e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        1
    );
    assert_eq!(
        count_events(&events, |e| matches!(e, WorkflowEvent::TimerFired { .. })),
        1
    );
}

// ── Scenario 3: signal delivery unblocks a waiting workflow ──────────────────

#[tokio::test]
async fn scenario_3_signal_delivery() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&wait_then_echo_info());

    let exec = rt.start_workflow("wait_then_echo", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref n) if n == "go"),
        "state = {state:?}"
    );

    rt.deliver_signal(exec, "go", json!({"decision": "approved"}))
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({"decision": "approved"})),
        "state = {state:?}"
    );
    let events = rt.load_history(exec).unwrap();
    assert_eq!(
        count_events(&events, |e| matches!(
            e,
            WorkflowEvent::SignalReceived { .. }
        )),
        1
    );
}

// ── Scenario 4: deterministic replay after a simulated crash ─────────────────

#[tokio::test]
async fn scenario_4_deterministic_replay_after_crash() {
    let (_dir, path) = temp_db();

    // Shared invocation counter survives the "crash" (only the runtime + its
    // SQLite connection are dropped; the event log is durable).
    let invocations = Arc::new(AtomicUsize::new(0));

    // Runtime #1: drive exactly one cycle — the activity COMPLETES and is
    // persisted, but the workflow is NOT resumed past it.
    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&single_activity_info());
        let inv = invocations.clone();
        rt.register_activity(
            "work",
            ActivitySpec::new(1, move |input: serde_json::Value| {
                inv.fetch_add(1, Ordering::SeqCst);
                Ok(json!(input.as_i64().unwrap() * 10))
            }),
        );
        let exec = rt.start_workflow("single_activity", json!(7)).unwrap();
        let state = rt.drive_one_cycle(exec).await.unwrap();
        assert!(matches!(state, RunState::InProgress), "state = {state:?}");

        // The activity ran and its completion is durable.
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        let events = rt.load_history(exec).unwrap();
        assert_eq!(
            count_events(&events, |e| matches!(
                e,
                WorkflowEvent::ActivityCompleted { .. }
            )),
            1
        );
        assert_eq!(
            count_events(&events, |e| matches!(
                e,
                WorkflowEvent::WorkflowCompleted { .. }
            )),
            0
        );
        exec
    };

    // Runtime #2: fresh process on the SAME file. The workflow resumes purely
    // by replaying recorded history — the activity is NOT re-executed.
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&single_activity_info());
    let inv = invocations.clone();
    rt.register_activity(
        "work",
        ActivitySpec::new(1, move |input: serde_json::Value| {
            inv.fetch_add(1, Ordering::SeqCst);
            Ok(json!(input.as_i64().unwrap() * 10))
        }),
    );
    let state = rt.run_until_blocked(exec).await.unwrap();

    // (7 * 10) * 2 == 140, produced without re-running the activity.
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(140)),
        "state = {state:?}"
    );
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "activity must NOT be re-executed on replay"
    );

    let events = rt.load_history(exec).unwrap();
    assert_eq!(
        count_events(&events, |e| matches!(
            e,
            WorkflowEvent::ActivityScheduled { .. }
        )),
        1
    );
    assert_eq!(
        count_events(&events, |e| matches!(
            e,
            WorkflowEvent::ActivityCompleted { .. }
        )),
        1
    );
    assert!(matches!(
        events.last(),
        Some(WorkflowEvent::WorkflowCompleted { .. })
    ));
}

// ── AC4: cross-backend replay (SQLite history replays on the engine path) ────

#[cfg(feature = "testing")]
#[tokio::test]
async fn scenario_cross_backend_replay() {
    use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};

    let (_dir, path) = temp_db();

    // 1) Produce a real history with the SQLite prototype.
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&single_activity_info());
    rt.register_activity(
        "work",
        ActivitySpec::new(1, |input: serde_json::Value| {
            Ok(json!(input.as_i64().unwrap() * 10))
        }),
    );
    let exec = rt.start_workflow("single_activity", json!(3)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(60)),
        "state = {state:?}"
    );
    let events = rt.load_history(exec).unwrap();

    // 2) Feed the SQLite-written history to the engine's own replay path.
    let snapshot = HistorySnapshot {
        workflow_name: "single_activity".to_string(),
        execution_id: exec,
        events: events.clone(),
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
    };
    let jsn = serde_json::to_string(&snapshot).unwrap();
    let report = WorkflowReplayer::new()
        .register_fn("single_activity", single_activity_info().handler)
        .replay_from_json(&jsn)
        .await
        .expect("snapshot must parse");
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "SQLite history must replay on the engine path:\n{report}"
    );

    // 3) Symmetric direction: strip the trailing terminal event (a PG-shaped
    //    in-flight history) and drive the prototype's OWN reload path from it —
    //    identical final outcome, no duplicate activity execution.
    let mut in_flight = events.clone();
    assert!(matches!(
        in_flight.pop(),
        Some(WorkflowEvent::WorkflowCompleted { .. })
    ));

    let (_dir2, path2) = temp_db();
    let mut rt2 = SqliteRuntime::open(&path2).unwrap();
    rt2.register_workflow(&single_activity_info());
    let seen = Arc::new(AtomicUsize::new(0));
    let s2 = seen.clone();
    rt2.register_activity(
        "work",
        ActivitySpec::new(1, move |input: serde_json::Value| {
            s2.fetch_add(1, Ordering::SeqCst);
            Ok(json!(input.as_i64().unwrap() * 10))
        }),
    );
    let exec2 = rt2
        .import_execution("single_activity", json!(3), in_flight)
        .unwrap();
    let state = rt2.run_until_blocked(exec2).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(60)),
        "state = {state:?}"
    );
    assert_eq!(
        seen.load(Ordering::SeqCst),
        0,
        "importing a completed-activity history must not re-run the activity"
    );
}
