//! The persist-site **class invariant** (corrections 3 & 4).
//!
//! Every persist site in the backend appends its event(s) and mutates its durable
//! rows inside ONE `conn.transaction()`, so a crash (modelled by dropping the
//! runtime and re-opening the file) can never leave the event history and the
//! durable rows observably out of sync. Rather than only per-site tests, this
//! sweep drives a single workflow through EVERY site type — start, deterministic
//! side effect, activity dispatch + drain, timer arm + fire, signal delivery, and
//! terminal — reopening the database at each stage and asserting history ⟷ rows
//! consistency after each reopen. It also proves the activity body runs exactly
//! once across all the reopens (deterministic replay).
//!
//! **What "crash" means here:** each stage boundary is a clean `drop(rt)` of a
//! *quiesced* runtime, so this sweep proves **logical atomicity** — the committed
//! state is internally consistent (history ⟷ rows) at every reopen. It does NOT
//! model a mid-transaction interruption or fsync/power-loss durability: true
//! crash-between-append-and-mutate is covered by the inline `*_within_tx`
//! drop-uncommitted unit tests in `src/{runtime,worker}.rs`, and a genuine
//! stranded-`RUNNING` reopen is covered by
//! `durability::orphaned_running_task_is_reclaimed_and_body_reruns`.

// The `#[workflow]` macro references the workflow's input parameter, so a
// deliberately-unused param reads as a "used underscore binding" through the
// expansion.
#![allow(clippy::used_underscore_binding)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ActivitySpec, ExecutionId, ExecutionOutcome, RunState, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

/// Exercises every persist site: a frozen side effect, then an activity, then a
/// durable timer, then a signal wait, then a terminal return.
#[workflow]
async fn full_lifecycle(ctx: &WorkflowContext, n: i64) -> Result<serde_json::Value, String> {
    let id = ctx.new_uuid().to_string();
    let doubled = ctx
        .execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?
        .as_i64()
        .unwrap_or_default()
        * 2;
    ctx.timer("gate", 5).await.map_err(|e| e.to_string())?;
    let sig = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(json!({ "id": id, "doubled": doubled, "signal": sig }))
}

fn build_rt(path: &Path, calls: &Arc<AtomicUsize>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).unwrap();
    rt.register_workflow(&full_lifecycle_info());
    let body_calls = calls.clone();
    rt.register_activity_raw(
        "work",
        ActivitySpec::new(1, move |input: serde_json::Value| {
            body_calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!(input.as_i64().unwrap()))
        }),
    );
    rt
}

fn count_i64(events: &[WorkflowEvent], pred: impl Fn(&WorkflowEvent) -> bool) -> i64 {
    i64::try_from(events.iter().filter(|e| pred(e)).count()).unwrap()
}

fn scalar(raw: &rusqlite::Connection, sql: &str, exec: ExecutionId) -> i64 {
    raw.query_row(sql, [exec.to_string()], |r| r.get(0))
        .unwrap()
}

/// The class invariant, checked against a freshly-reopened runtime: at NO stage
/// can the durable rows and the event history be observably out of sync.
fn assert_reopen_consistent(rt: &SqliteRuntime, path: &Path, exec: ExecutionId) {
    let history = rt.load_history(exec).unwrap();
    let raw = rusqlite::Connection::open(path).unwrap();

    // Dispatch pair: every ActivityScheduled event ⟺ exactly one task row (no
    // scheduled event without its row, no row without its event).
    let scheduled = count_i64(&history, |e| {
        matches!(e, WorkflowEvent::ActivityScheduled { .. })
    });
    let task_rows = scalar(
        &raw,
        "SELECT COUNT(*) FROM harvest_tasks WHERE exec_id = ?1",
        exec,
    );
    assert_eq!(scheduled, task_rows, "ActivityScheduled ⟺ task row");

    // After a reopen the orphan reclaim has run, so no task may be left RUNNING.
    let running = scalar(
        &raw,
        "SELECT COUNT(*) FROM harvest_tasks WHERE exec_id = ?1 AND state = 'RUNNING'",
        exec,
    );
    assert_eq!(running, 0, "no task may be RUNNING after a reopen");

    // Drain pair: every terminal activity event ⟺ a DONE task row (no completed
    // event with a still-pending row).
    let terminal_activity = count_i64(&history, |e| {
        matches!(
            e,
            WorkflowEvent::ActivityCompleted { .. } | WorkflowEvent::ActivityFailed { .. }
        )
    });
    let done = scalar(
        &raw,
        "SELECT COUNT(*) FROM harvest_tasks WHERE exec_id = ?1 AND state = 'DONE'",
        exec,
    );
    assert_eq!(
        terminal_activity, done,
        "terminal activity event ⟺ DONE task"
    );

    // Timer pairs: every TimerStarted ⟺ a timer row; every TimerFired ⟺ that row
    // marked fired (no fired timer left unmarked).
    let timer_started = count_i64(&history, |e| {
        matches!(e, WorkflowEvent::TimerStarted { .. })
    });
    let timer_rows = scalar(
        &raw,
        "SELECT COUNT(*) FROM harvest_timers WHERE exec_id = ?1",
        exec,
    );
    assert_eq!(timer_started, timer_rows, "TimerStarted ⟺ timer row");

    let timer_fired = count_i64(&history, |e| matches!(e, WorkflowEvent::TimerFired { .. }));
    let fired_rows = scalar(
        &raw,
        "SELECT COUNT(*) FROM harvest_timers WHERE exec_id = ?1 AND fired = 1",
        exec,
    );
    assert_eq!(
        timer_fired, fired_rows,
        "TimerFired ⟺ timer row marked fired"
    );

    // Terminal: execution state ⟺ terminal event in history.
    let has_completed = history
        .iter()
        .any(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. }));
    let has_failed = history
        .iter()
        .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. }));
    match rt.outcome(exec).unwrap() {
        ExecutionOutcome::Completed(_) => {
            assert!(
                has_completed,
                "COMPLETED state must have a WorkflowCompleted event"
            );
        }
        ExecutionOutcome::Failed(_) => {
            assert!(has_failed, "FAILED state must have a WorkflowFailed event");
        }
        ExecutionOutcome::Running => {
            assert!(
                !has_completed && !has_failed,
                "a RUNNING execution must carry no terminal event"
            );
        }
    }
}

#[tokio::test]
async fn reopen_consistency_across_every_persist_site() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let t0: DateTime<Utc> = DateTime::from_timestamp(2_000_000, 0).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));

    // Stage 1 — start (execution row + WorkflowStarted, one transaction).
    let exec = {
        let mut rt = build_rt(&path, &calls);
        rt.start_workflow("full_lifecycle", json!(21)).unwrap()
    };
    assert_reopen_consistent(&build_rt(&path, &calls), &path, exec);

    // Stage 2 — drive through the side effect + activity dispatch/drain; the timer
    // arms and the run blocks WaitingTimer (deadline t0+5 is not due at t0).
    {
        let mut rt = build_rt(&path, &calls);
        let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
        assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    }
    let rt = build_rt(&path, &calls);
    assert_reopen_consistent(&rt, &path, exec);
    // The side effect persisted (rode the activity's suspending batch).
    assert!(
        rt.load_history(exec)
            .unwrap()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. })),
        "the frozen side effect must be durable"
    );
    drop(rt);

    // Stage 3 — fire the timer (as-of past the deadline), then block WaitingSignal.
    {
        let mut rt = build_rt(&path, &calls);
        let state = rt
            .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(10))
            .await
            .unwrap();
        assert!(
            matches!(state, RunState::WaitingSignal(ref s) if s == "go"),
            "got {state:?}"
        );
    }
    assert_reopen_consistent(&build_rt(&path, &calls), &path, exec);

    // Stage 4 — deliver the signal and drive to terminal completion.
    {
        let mut rt = build_rt(&path, &calls);
        rt.send_signal(exec, "go", json!("approved")).unwrap();
        let state = rt
            .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(20))
            .await
            .unwrap();
        assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
    }
    let rt = build_rt(&path, &calls);
    assert_reopen_consistent(&rt, &path, exec);
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Completed(_)
    ));

    // The activity body ran exactly once across all four reopens — deterministic
    // replay never re-executed a completed activity.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the activity must run exactly once despite the reopens"
    );
}
