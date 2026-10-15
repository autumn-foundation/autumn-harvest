//! The durable workflow mutex on the `SQLite` backend (issue #2029, #691).
//!
//! `ctx.mutex(key).acquire()` used to seal the run `FAILED` with
//! `UnsupportedFeature`. Each test runs a mutex workflow to completion, then
//! replays its history on the core `WorkflowReplayer`.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest_sqlite::{
    ActivitySpec, ExecutionOutcome, RunState, SqliteError, SqliteRuntime,
    UNSUPPORTED_FEATURE_ERROR_TYPE,
};
use serde_json::json;

/// Takes the lock, runs one activity under it, and releases it.
#[workflow]
async fn locks_once(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let guard = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
    let out = ctx
        .execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    guard.release();
    Ok(out.as_i64().unwrap_or_default())
}

/// Takes the lock and holds it until the signal `release` arrives.
#[workflow]
async fn holds_until_signal(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let guard = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
    let _: serde_json::Value = ctx
        .wait_for_signal("release")
        .await
        .map_err(|e| e.to_string())?;
    guard.release();
    Ok(0)
}

/// Waits for the lock and a signal in one batch.
#[workflow]
async fn acquires_beside_a_signal(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let (guard, _signal) = tokio::join!(ctx.mutex("k").acquire(), ctx.wait_for_signal("go"),);
    drop(guard);
    Ok(0)
}

/// Takes the lock, releases it, then waits for a signal that never comes.
#[workflow]
async fn locks_then_waits(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let guard = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
    guard.release();
    let _: serde_json::Value = ctx
        .wait_for_signal("never")
        .await
        .map_err(|e| e.to_string())?;
    Ok(0)
}

/// Waits for the lock and a cancellable timer in one batch.
#[workflow]
async fn locks_beside_a_timer(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let timer = ctx.start_timer("pace", 5);
    let (guard, fired) = tokio::join!(ctx.mutex("k").acquire(), timer.await_fire());
    let guard = guard.map_err(|e| e.to_string())?;
    let fired = fired.map_err(|e| e.to_string())?;
    guard.release();
    Ok(format!("{fired:?}"))
}

/// Takes the lock, upserts a search attribute, then holds the lock until the
/// signal `release` arrives (issue #2027).
#[workflow]
async fn upserts_under_the_lock(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let guard = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
    ctx.upsert_search_attrs([("stage".to_string(), Some(json!("locked")))])
        .map_err(|e| e.to_string())?;
    let _: serde_json::Value = ctx
        .wait_for_signal("release")
        .await
        .map_err(|e| e.to_string())?;
    guard.release();
    Ok(0)
}

/// Takes the lock and fails before it releases the lock.
#[workflow]
async fn fails_under_the_lock(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _guard = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
    Err("broken".to_string())
}

fn runtime(infos: &[&WorkflowInfo]) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    for info in infos {
        rt.register_workflow(info);
    }
    rt.register_activity_raw("work", ActivitySpec::new(1, Ok));
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

/// The `lock_seq` of the first `MutexGranted` in `history`.
fn granted_seq(history: &[WorkflowEvent]) -> i64 {
    history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::MutexGranted { lock_seq, .. } => Some(*lock_seq),
            _ => None,
        })
        .expect("a MutexGranted event")
}

#[tokio::test]
async fn a_free_mutex_is_granted_and_the_run_completes() {
    let info = locks_once_info();
    let mut rt = runtime(&[&info]);
    let exec = rt.start_workflow(info.name, json!(7)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == 7),
        "got {state:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert!(granted_seq(&history) > 0);
    assert_replays(info.name, info.handler, history).await;
}

#[tokio::test]
async fn a_contended_mutex_grants_waiters_in_fifo_order() {
    let holder = holds_until_signal_info();
    let waiter = locks_once_info();
    let mut rt = runtime(&[&holder, &waiter]);

    let h = rt.start_workflow(holder.name, json!(0)).unwrap();
    let state = rt.run_until_blocked(h).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "got {state:?}");

    let a = rt.start_workflow(waiter.name, json!(1)).unwrap();
    let b = rt.start_workflow(waiter.name, json!(2)).unwrap();
    for exec in [a, b] {
        let state = rt.run_until_blocked(exec).await.unwrap();
        assert!(
            matches!(state, RunState::WaitingMutex(ref k) if k == "k"),
            "got {state:?}"
        );
    }

    rt.send_signal(h, "release", json!(null)).unwrap();
    rt.run_until_idle().await.unwrap();

    for exec in [h, a, b] {
        assert!(
            matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Completed(_)),
            "{exec} must complete"
        );
    }
    let (ha, hb) = (rt.load_history(a).unwrap(), rt.load_history(b).unwrap());
    let hh = rt.load_history(h).unwrap();
    assert!(granted_seq(&hh) < granted_seq(&ha));
    assert!(granted_seq(&ha) < granted_seq(&hb), "a waited first");
    assert_replays(holder.name, holder.handler, hh).await;
    assert_replays(waiter.name, waiter.handler, ha).await;
    assert_replays(waiter.name, waiter.handler, hb).await;
}

// H4: a holder that ends without a release frees its lock.
#[tokio::test]
async fn a_terminated_holder_frees_its_lock() {
    let holder = holds_until_signal_info();
    let waiter = locks_once_info();
    let mut rt = runtime(&[&holder, &waiter]);

    let h = rt.start_workflow(holder.name, json!(0)).unwrap();
    rt.run_until_blocked(h).await.unwrap();
    let w = rt.start_workflow(waiter.name, json!(3)).unwrap();
    let state = rt.run_until_blocked(w).await.unwrap();
    assert!(matches!(state, RunState::WaitingMutex(_)), "got {state:?}");

    rt.terminate(h, "stop").unwrap();
    let state = rt.run_until_blocked(w).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
}

// An upsert is not a release. The holder keeps the lock after it upserts
// (issue #2027).
#[tokio::test]
async fn an_upsert_under_the_lock_keeps_the_lock() {
    let holder = upserts_under_the_lock_info();
    let waiter = locks_once_info();
    let mut rt = runtime(&[&holder, &waiter]);

    let h = rt.start_workflow(holder.name, json!(0)).unwrap();
    let state = rt.run_until_blocked(h).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "got {state:?}");
    let w = rt.start_workflow(waiter.name, json!(4)).unwrap();
    let state = rt.run_until_blocked(w).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingMutex(ref k) if k == "k"),
        "got {state:?}"
    );
}

// A holder that fails frees its lock, as a terminated holder does.
#[tokio::test]
async fn a_failed_holder_frees_its_lock() {
    let holder = fails_under_the_lock_info();
    let waiter = locks_once_info();
    let mut rt = runtime(&[&holder, &waiter]);

    let h = rt.start_workflow(holder.name, json!(0)).unwrap();
    let state = rt.run_until_blocked(h).await.unwrap();
    assert!(matches!(state, RunState::Failed(_)), "got {state:?}");
    let w = rt.start_workflow(waiter.name, json!(5)).unwrap();
    let state = rt.run_until_blocked(w).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == 5),
        "got {state:?}"
    );
}

// A held lock is durable. The holder keeps it across a reopen.
#[tokio::test]
async fn a_held_lock_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mutex.db");
    let holder = holds_until_signal_info();
    let waiter = locks_once_info();
    let (h, w) = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&holder);
        rt.register_workflow(&waiter);
        let h = rt.start_workflow(holder.name, json!(0)).unwrap();
        rt.run_until_blocked(h).await.unwrap();
        let w = rt.start_workflow(waiter.name, json!(4)).unwrap();
        rt.run_until_blocked(w).await.unwrap();
        (h, w)
    };
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&holder);
    rt.register_workflow(&waiter);
    rt.register_activity_raw("work", ActivitySpec::new(1, Ok));
    let state = rt.run_until_blocked(w).await.unwrap();
    assert!(matches!(state, RunState::WaitingMutex(_)), "got {state:?}");
    rt.send_signal(h, "release", json!(null)).unwrap();
    rt.run_until_idle().await.unwrap();
    assert!(matches!(
        rt.outcome(w).unwrap(),
        ExecutionOutcome::Completed(_)
    ));
}

// The acquire must be the only awaitable in its batch, as on Postgres.
#[tokio::test]
async fn an_acquire_beside_another_awaitable_ends_failed() {
    let info = acquires_beside_a_signal_info();
    let mut rt = runtime(&[&info]);
    let exec = rt.start_workflow(info.name, json!(0)).unwrap();

    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::Unsupported(ref m) if m.starts_with("AcquireMutex")),
        "got {err:?}"
    );
    let history = rt.load_history(exec).unwrap();
    assert!(history.iter().any(|e| matches!(
        e,
        WorkflowEvent::WorkflowFailed { error_type: Some(t), .. }
            if t == UNSUPPORTED_FEATURE_ERROR_TYPE
    )));
}

// A waiter that is cancelled leaves the queue, so the next waiter gets the
// lock.
#[tokio::test]
async fn a_cancelled_waiter_leaves_the_queue() {
    let holder = holds_until_signal_info();
    let waiter = locks_once_info();
    let mut rt = runtime(&[&holder, &waiter]);
    let h = rt.start_workflow(holder.name, json!(0)).unwrap();
    rt.run_until_blocked(h).await.unwrap();
    let first = rt.start_workflow(waiter.name, json!(1)).unwrap();
    let second = rt.start_workflow(waiter.name, json!(2)).unwrap();
    rt.run_until_blocked(first).await.unwrap();
    rt.run_until_blocked(second).await.unwrap();

    rt.cancel(first, "stop").unwrap();
    let state = rt.run_until_blocked(first).await.unwrap();
    assert!(matches!(state, RunState::Cancelled(_)), "got {state:?}");
    rt.send_signal(h, "release", json!(null)).unwrap();
    rt.run_until_idle().await.unwrap();
    assert!(matches!(
        rt.outcome(second).unwrap(),
        ExecutionOutcome::Completed(_)
    ));
}

// H5: a release counts as progress, also when the holder then blocks. The
// fleet poll then makes one more pass, so a waiter can run.
#[tokio::test]
async fn a_release_is_progress_when_the_holder_then_blocks() {
    let info = locks_then_waits_info();
    let mut rt = runtime(&[&info]);
    rt.start_workflow(info.name, json!(0)).unwrap();
    let passes = [
        rt.poll_once().await.unwrap(),
        rt.poll_once().await.unwrap(),
        rt.poll_once().await.unwrap(),
    ];
    assert_eq!(passes, [true, true, false], "grant, release, then blocked");
}

// Postgres accepts a timer await beside an acquire, but the shape does not
// replay. A timer that fires first puts `TimerFired` where the positional
// grant matcher expects `MutexGranted`. This backend rejects it.
#[tokio::test]
async fn an_acquire_beside_a_timer_await_ends_failed() {
    let info = locks_beside_a_timer_info();
    let mut rt = runtime(&[&info]);
    let exec = rt.start_workflow(info.name, json!(0)).unwrap();
    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::Unsupported(ref m) if m.starts_with("AcquireMutex") && m.contains("ArmTimer")),
        "got {err:?}"
    );
}
