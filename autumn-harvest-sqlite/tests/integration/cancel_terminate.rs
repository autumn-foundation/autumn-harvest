//! Cancel and terminate a run (issue #2021).
//!
//! `cancel` is cooperative. The body sees `cancellation_reason()` on the next
//! drive and can run cleanup activities. `terminate` seals the run at once.
//! Both paths record the core `WorkflowCancelled` event as the last event.
//! A history replays on the core `WorkflowReplayer` when the body checks for
//! the cancel before its next new command.

// The `#[workflow]` macro reads its input through an underscore binding.
#![allow(clippy::used_underscore_binding)]
// The `#[activity]` fixture only supplies `flaky_step_info()`. Its body never runs.
#![allow(clippy::unused_async)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{
    ActivitySpec, ExecutionId, ExecutionOutcome, RunState, SqliteError, SqliteRuntime,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

// ── Fixtures ─────────────────────────────────────────────────────────────────

/// Runs `step`. When cancelled, it runs `cleanup` with the reason and returns.
/// Otherwise it waits for the `go` signal.
#[workflow]
async fn cleans_up(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    if let Some(reason) = ctx.cancellation_reason() {
        let reason = reason.to_string();
        ctx.execute_activity_raw("cleanup", json!(reason), "default")
            .await
            .map_err(|e| e.to_string())?;
        return Ok(json!({ "cleaned": reason }));
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Runs `step`, then waits for `go`. It never checks for a cancel.
#[workflow]
async fn parks(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Runs `step`. When cancelled, it returns `Ok` with no new command.
#[workflow]
async fn stops(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    if ctx.is_cancelled() {
        return Ok(json!("stopped"));
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Runs `step`, then fails with `Cancelled` when cancelled.
#[workflow]
async fn bails(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    ctx.check_cancellation().map_err(|e| e.to_string())?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Sleeps on a one-hour durable timer.
#[workflow]
async fn naps(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.timer("nap", 3600).await.map_err(|e| e.to_string())?;
    Ok(json!("woke"))
}

/// A typed activity with a fixed 60 s retry backoff.
#[activity(retry = RetryPolicy::fixed(3, Duration::from_secs(60)))]
async fn flaky_step(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

/// Runs `flaky_step`. When cancelled, it returns `Ok`.
#[workflow]
async fn retries(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    let out: i64 = ctx
        .execute_activity(&flaky_step_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    if ctx.is_cancelled() {
        return Ok(json!({ "stopped_after": out }));
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Checks for a cancel at the top of each loop turn, before recorded steps.
#[workflow]
async fn loops(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    for i in 0..2 {
        if ctx.is_cancelled() {
            ctx.execute_activity_raw("cleanup", json!(i), "default")
                .await
                .map_err(|e| e.to_string())?;
            return Ok(json!("cleaned"));
        }
        ctx.execute_activity_raw("step", json!(i), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Runs `step`, checks for a cancel, then sleeps on a one-hour timer.
#[workflow]
async fn checks_then_naps(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    if ctx.is_cancelled() {
        ctx.execute_activity_raw("cleanup", json!(n), "default")
            .await
            .map_err(|e| e.to_string())?;
        return Ok(json!("cleaned"));
    }
    ctx.timer("nap", 3600).await.map_err(|e| e.to_string())?;
    Ok(json!("woke"))
}

/// Runs `step`. When cancelled, it waits on a grace timer before cleanup.
#[workflow]
async fn grace(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    if ctx.is_cancelled() {
        ctx.timer("grace", 3600).await.map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("cleanup", json!(n), "default")
            .await
            .map_err(|e| e.to_string())?;
        return Ok(json!("cleaned"));
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// When cancelled, it runs `cleanup`, mints a UUID, and returns. The UUID is a
/// side effect of the cycle that returns.
#[workflow]
async fn mints_on_cancel(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    if ctx.is_cancelled() {
        ctx.execute_activity_raw("cleanup", json!(n), "default")
            .await
            .map_err(|e| e.to_string())?;
        return Ok(json!(ctx.new_uuid().to_string()));
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// When cancelled, it waits on a future that never wakes, so the cycle
/// deadlocks.
#[workflow]
async fn hangs_on_cancel(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    if ctx.is_cancelled() {
        std::future::pending::<()>().await;
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// When cancelled, the body panics.
#[workflow]
async fn panics_on_cancel(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    assert!(!ctx.is_cancelled(), "cleanup bug");
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Runs `step`, waits for `go`, then continues as new.
#[workflow]
async fn continues(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    ctx.continue_as_new(json!(n + 1))
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!("unreachable"))
}

/// Runs `step` forever and never checks for a cancel.
#[workflow]
async fn spins(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let mut i = 0_i64;
    loop {
        ctx.execute_activity_raw("step", json!(i), "default")
            .await
            .map_err(|e| e.to_string())?;
        i += 1;
        if i == 2 && !ctx.is_cancelled() {
            ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
        }
    }
}

/// When cancelled, mints 1,100 UUIDs in one cycle, then returns.
#[workflow]
async fn floods_on_cancel(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    if ctx.is_cancelled() {
        for _ in 0..1_100 {
            let _ = ctx.new_uuid();
        }
        return Ok(json!("flooded"));
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// When cancelled, mints 1,100 UUIDs, then waits for a signal that never
/// comes.
#[workflow]
async fn floods_then_waits(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    if ctx.is_cancelled() {
        for _ in 0..1_100 {
            let _ = ctx.new_uuid();
        }
        return ctx
            .wait_for_signal("never")
            .await
            .map_err(|e| e.to_string());
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// When cancelled, it calls an activity that no runtime registers.
#[workflow]
async fn calls_missing_on_cancel(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("step", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    if ctx.is_cancelled() {
        ctx.execute_activity_raw("missing_cleanup", json!(n), "default")
            .await
            .map_err(|e| e.to_string())?;
        return Ok(json!("cleaned"));
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Counts the calls of each activity and records the cleanup input.
#[derive(Default)]
struct Calls {
    step: AtomicUsize,
    cleanup: AtomicUsize,
    flaky: AtomicUsize,
    cleanup_input: Mutex<Option<Value>>,
}

fn temp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

fn fixed_now() -> DateTime<Utc> {
    "2026-07-15T00:00:00Z".parse().unwrap()
}

/// Registers every fixture on `rt`. `flaky_step` fails its first call.
fn register(rt: &mut SqliteRuntime, calls: &Arc<Calls>) {
    rt.register_workflow(&cleans_up_info());
    rt.register_workflow(&parks_info());
    rt.register_workflow(&stops_info());
    rt.register_workflow(&bails_info());
    rt.register_workflow(&naps_info());
    rt.register_workflow(&retries_info());
    rt.register_workflow(&loops_info());
    rt.register_workflow(&checks_then_naps_info());
    rt.register_workflow(&grace_info());
    rt.register_workflow(&mints_on_cancel_info());
    rt.register_workflow(&hangs_on_cancel_info());
    rt.register_workflow(&panics_on_cancel_info());
    rt.register_workflow(&continues_info());
    rt.register_workflow(&spins_info());
    rt.register_workflow(&floods_on_cancel_info());
    rt.register_workflow(&floods_then_waits_info());
    rt.register_workflow(&calls_missing_on_cancel_info());
    let c = Arc::clone(calls);
    rt.register_activity_raw(
        "step",
        ActivitySpec::new(1, move |input| {
            c.step.fetch_add(1, Ordering::SeqCst);
            Ok(input)
        }),
    );
    let c = Arc::clone(calls);
    rt.register_activity_raw(
        "cleanup",
        ActivitySpec::new(1, move |input| {
            c.cleanup.fetch_add(1, Ordering::SeqCst);
            *c.cleanup_input.lock().unwrap() = Some(input.clone());
            Ok(input)
        }),
    );
    let c = Arc::clone(calls);
    rt.register_activity_raw(
        "flaky_step",
        ActivitySpec::new(3, move |input| {
            if c.flaky.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("transient".to_string())
            } else {
                Ok(input)
            }
        }),
    );
}

fn runtime(path: &std::path::Path) -> (SqliteRuntime, Arc<Calls>) {
    let calls = Arc::new(Calls::default());
    let mut rt = SqliteRuntime::open(path).unwrap();
    register(&mut rt, &calls);
    (rt, calls)
}

/// Counts the rows that a sealed run must not keep, read from a raw
/// connection: `PENDING` tasks, unfired timers and undelivered signals.
fn leftover_rows(path: &std::path::Path, exec: ExecutionId) -> (i64, i64, i64) {
    let raw = rusqlite::Connection::open(path).unwrap();
    let count = |sql: &str| -> i64 {
        raw.query_row(sql, [exec.to_string()], |r| r.get(0))
            .unwrap()
    };
    (
        count("SELECT COUNT(*) FROM harvest_tasks WHERE exec_id = ?1 AND state = 'PENDING'"),
        count("SELECT COUNT(*) FROM harvest_timers WHERE exec_id = ?1 AND fired = 0"),
        count("SELECT COUNT(*) FROM harvest_signals WHERE exec_id = ?1 AND delivered = 0"),
    )
}

fn last_event(rt: &SqliteRuntime, exec: ExecutionId) -> WorkflowEvent {
    rt.load_history(exec).unwrap().pop().unwrap()
}

fn cancelled_events(rt: &SqliteRuntime, exec: ExecutionId) -> usize {
    rt.load_history(exec)
        .unwrap()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::WorkflowCancelled { .. }))
        .count()
}

async fn replay(
    history: Vec<WorkflowEvent>,
    name: &str,
    handler: autumn_harvest::WorkflowHandlerFn,
) -> autumn_harvest::testing::ReplayReport {
    WorkflowReplayer::new()
        .register_fn(name, handler)
        .replay_from_events(history)
        .await
}

/// Starts `workflow` and drives it until it waits for the `go` signal.
async fn start_parked(rt: &mut SqliteRuntime, workflow: &str) -> ExecutionId {
    let exec = rt.start_workflow(workflow, json!(7)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(&state, RunState::WaitingSignal(name) if name == "go"),
        "the run must park on `go`, got {state:?}"
    );
    exec
}

// ── AC1: a cancelled run observes the reason and runs cleanup ───────────────

#[tokio::test]
async fn cancel_lets_the_body_see_the_reason_and_run_cleanup() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "cleans_up").await;

    rt.cancel(exec, "user stop").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(&state, RunState::Cancelled(reason) if reason == "user stop"),
        "the drive must seal the run cancelled, got {state:?}"
    );
    assert_eq!(calls.cleanup.load(Ordering::SeqCst), 1, "cleanup runs once");
    assert_eq!(
        *calls.cleanup_input.lock().unwrap(),
        Some(json!("user stop")),
        "the body read the reason through `cancellation_reason()`"
    );
    assert!(
        matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Terminated(s) if s == "CANCELLED")
    );
    assert!(
        matches!(last_event(&rt, exec), WorkflowEvent::WorkflowCancelled { reason } if reason == "user stop"),
        "`WorkflowCancelled` is the terminal event"
    );
    assert_eq!(cancelled_events(&rt, exec), 1);
    assert_eq!(leftover_rows(&path, exec), (0, 0, 0));
}

#[tokio::test]
async fn cancel_appends_no_event_until_the_seal() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = start_parked(&mut rt, "cleans_up").await;
    let before = rt.load_history(exec).unwrap().len();

    rt.cancel(exec, "user stop").unwrap();

    assert_eq!(rt.load_history(exec).unwrap().len(), before);
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Running
    ));
}

#[tokio::test]
async fn a_cancel_request_survives_a_reopen() {
    let (_dir, path) = temp_db();
    let exec = {
        let (mut rt, _calls) = runtime(&path);
        let exec = start_parked(&mut rt, "cleans_up").await;
        rt.cancel(exec, "before the crash").unwrap();
        exec
    };

    let (mut rt, calls) = runtime(&path);
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(&state, RunState::Cancelled(reason) if reason == "before the crash"),
        "the stored request survives the reopen, got {state:?}"
    );
    assert_eq!(calls.cleanup.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_seals_a_body_parked_on_a_signal() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "parks").await;
    rt.send_signal(exec, "stray", json!(1)).unwrap();

    rt.cancel(exec, "stuck").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(&state, RunState::Cancelled(reason) if reason == "stuck"),
        "a parked body cannot see the cancel, so the run seals, got {state:?}"
    );
    assert_eq!(
        calls.step.load(Ordering::SeqCst),
        1,
        "step does not run again"
    );
    assert_eq!(leftover_rows(&path, exec), (0, 0, 0));
}

#[tokio::test]
async fn cancel_seals_a_body_parked_on_a_timer() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = rt.start_workflow("naps", json!(0)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::WaitingTimer
    ));

    rt.cancel(exec, "no nap").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(&state, RunState::Cancelled(reason) if reason == "no nap"));
    assert_eq!(leftover_rows(&path, exec), (0, 0, 0));
}

#[tokio::test]
async fn cancel_waits_for_a_pending_activity_retry() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let t0 = fixed_now();
    let exec = rt.start_workflow("retries", json!(5)).unwrap();
    assert!(matches!(
        rt.run_until_blocked_as_of(exec, t0).await.unwrap(),
        RunState::WaitingTimer
    ));

    rt.cancel(exec, "enough").unwrap();
    let waiting = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(waiting, RunState::WaitingTimer),
        "a pending retry can still resolve the await, got {waiting:?}"
    );

    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(61))
        .await
        .unwrap();
    assert!(
        matches!(&state, RunState::Cancelled(reason) if reason == "enough"),
        "the retry runs, then the body sees the cancel, got {state:?}"
    );
    assert_eq!(calls.flaky.load(Ordering::SeqCst), 2);
}

/// Asserts that `exec` ended `CANCELLED` with `reason`, that no cleanup ran,
/// and that the history is the old prefix plus one `WorkflowCancelled`.
fn assert_sealed_without_cleanup(
    rt: &SqliteRuntime,
    calls: &Calls,
    exec: ExecutionId,
    prefix: &[WorkflowEvent],
    reason: &str,
) {
    assert_eq!(calls.cleanup.load(Ordering::SeqCst), 0, "no cleanup ran");
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        history.len(),
        prefix.len() + 1,
        "only the seal event is new"
    );
    assert!(
        matches!(history.last(), Some(WorkflowEvent::WorkflowCancelled { reason: r }) if r == reason)
    );
    assert!(
        matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Terminated(s) if s == "CANCELLED")
    );
}

#[tokio::test]
async fn a_check_before_recorded_steps_still_ends_the_run() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "loops").await;
    let prefix = rt.load_history(exec).unwrap();

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await;

    assert!(
        matches!(&state, Ok(RunState::Cancelled(reason)) if reason == "stop"),
        "a check that replays before recorded steps must not wedge the run, got {state:?}"
    );
    assert_sealed_without_cleanup(&rt, &calls, exec, &prefix, "stop");
    let report = replay(
        rt.load_history(exec).unwrap(),
        "loops",
        loops_info().handler,
    )
    .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "the documented limit: this history does not replay:\n{report}"
    );
}

#[tokio::test]
async fn a_check_before_a_recorded_timer_still_ends_the_run() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = rt.start_workflow("checks_then_naps", json!(3)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::WaitingTimer
    ));
    let prefix = rt.load_history(exec).unwrap();

    rt.cancel(exec, "no nap").unwrap();
    let state = rt.run_until_blocked(exec).await;

    assert!(matches!(&state, Ok(RunState::Cancelled(reason)) if reason == "no nap"));
    assert_sealed_without_cleanup(&rt, &calls, exec, &prefix, "no nap");
    assert_eq!(leftover_rows(&path, exec), (0, 0, 0));
}

#[tokio::test]
async fn a_deadlock_under_a_cancel_request_ends_the_run() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "hangs_on_cancel").await;
    let prefix = rt.load_history(exec).unwrap();

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await;

    assert!(
        matches!(&state, Ok(RunState::Cancelled(reason)) if reason == "stop"),
        "a deadlocked cycle must not keep a cancelled run alive, got {state:?}"
    );
    assert_sealed_without_cleanup(&rt, &calls, exec, &prefix, "stop");
    assert_eq!(leftover_rows(&path, exec), (0, 0, 0));
}

#[tokio::test]
async fn a_panic_under_a_cancel_request_keeps_the_panic_budget() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = start_parked(&mut rt, "panics_on_cancel").await;
    rt.cancel(exec, "stop").unwrap();

    for strike in 1..3 {
        let state = rt.run_until_blocked(exec).await;
        assert!(
            matches!(state, Err(SqliteError::WorkflowPanicked { .. })),
            "strike {strike} stays non-terminal, got {state:?}"
        );
        assert!(matches!(
            rt.outcome(exec).unwrap(),
            ExecutionOutcome::Running
        ));
        assert_eq!(cancelled_events(&rt, exec), 0);
    }

    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(state, RunState::Failed(_)),
        "the used-up budget ends the run FAILED, as without a cancel, got {state:?}"
    );
    assert_eq!(cancelled_events(&rt, exec), 0);
}

#[tokio::test]
async fn a_side_effect_of_the_returning_cycle_persists_before_the_seal() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "mints_on_cancel").await;

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(&state, RunState::Cancelled(reason) if reason == "stop"));
    assert_eq!(calls.cleanup.load(Ordering::SeqCst), 1);
    let history = rt.load_history(exec).unwrap();
    assert!(
        matches!(
            &history[history.len() - 2..],
            [
                WorkflowEvent::SideEffectRecorded { .. },
                WorkflowEvent::WorkflowCancelled { .. }
            ]
        ),
        "the side effect lands just before the seal event: {history:?}"
    );
    let report = replay(history, "mints_on_cancel", mints_on_cancel_info().handler).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{report}"
    );
}

#[tokio::test]
async fn a_cancelled_run_is_not_driven_again() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "cleans_up").await;
    rt.cancel(exec, "user stop").unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let history = rt.load_history(exec).unwrap().len();

    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(&state, RunState::Cancelled(reason) if reason == "user stop"));
    assert_eq!(calls.step.load(Ordering::SeqCst), 1);
    assert_eq!(calls.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(rt.load_history(exec).unwrap().len(), history);
}

#[tokio::test]
async fn a_reason_is_trimmed_as_on_postgres() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let cancelled = start_parked(&mut rt, "parks").await;
    let terminated = start_parked(&mut rt, "parks").await;

    rt.cancel(cancelled, "  stop  ").unwrap();
    rt.terminate(terminated, " kill ").unwrap();

    assert!(matches!(
        rt.run_until_blocked(cancelled).await.unwrap(),
        RunState::Cancelled(reason) if reason == "stop"
    ));
    assert!(matches!(
        rt.run_until_blocked(terminated).await.unwrap(),
        RunState::Terminated(reason) if reason == "kill"
    ));
    for (exec, reason) in [(cancelled, "stop"), (terminated, "kill")] {
        assert!(matches!(
            last_event(&rt, exec),
            WorkflowEvent::WorkflowCancelled { reason: r } if r == reason
        ));
    }
}

#[tokio::test]
async fn a_cleanup_that_waits_on_a_timer_is_cut_short() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "grace").await;

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(&state, RunState::Cancelled(reason) if reason == "stop"));
    assert_eq!(
        calls.cleanup.load(Ordering::SeqCst),
        0,
        "the documented limit: a cleanup wait on a timer seals the run"
    );
    assert_eq!(leftover_rows(&path, exec), (0, 0, 0));
}

#[tokio::test]
async fn a_body_that_completes_after_a_cancel_request_seals_cancelled() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = start_parked(&mut rt, "parks").await;

    rt.cancel(exec, "stop").unwrap();
    rt.send_signal(exec, "go", json!({ "answer": 42 })).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(&state, RunState::Cancelled(reason) if reason == "stop"),
        "the request wins over a late completion, got {state:?}"
    );
    assert_eq!(cancelled_events(&rt, exec), 1);
    let history = rt.load_history(exec).unwrap();
    assert!(
        matches!(
            &history[history.len() - 2..],
            [
                WorkflowEvent::SignalReceived { signal_name, .. },
                WorkflowEvent::WorkflowCancelled { .. }
            ] if signal_name == "go"
        ),
        "the consumed signal stays in history before the seal: {history:?}"
    );
    let report = replay(history, "parks", parks_info().handler).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{report}"
    );
}

#[tokio::test]
async fn a_cancel_wins_over_continue_as_new() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = start_parked(&mut rt, "continues").await;

    rt.cancel(exec, "stop").unwrap();
    rt.send_signal(exec, "go", json!(1)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(&state, RunState::Cancelled(reason) if reason == "stop"),
        "a cancelled run starts no successor, got {state:?}"
    );
    assert_eq!(rt.successor(exec).unwrap(), None);
    assert!(
        !rt.load_history(exec)
            .unwrap()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. })),
        "the cancel event is the only terminal event"
    );
    rt.run_until_idle().await.unwrap();
    assert!(
        matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Terminated(s) if s == "CANCELLED")
    );
}

#[tokio::test]
async fn a_body_that_ignores_the_cancel_and_keeps_working_still_ends() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "spins").await;
    let before = rt.load_history(exec).unwrap().len();

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await;

    assert!(
        matches!(&state, Ok(RunState::Cancelled(reason)) if reason == "stop"),
        "a busy body must not outrun the cancel, got {state:?}"
    );
    let added = rt.load_history(exec).unwrap().len() - before;
    assert!(
        added <= 1_002,
        "the cleanup budget bounds the events after the request, got {added}"
    );
    assert!(calls.step.load(Ordering::SeqCst) < 600);
}

#[tokio::test]
async fn a_cleanup_that_reaches_the_history_cap_still_ends_cancelled() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "cleans_up").await;
    let prefix = rt.load_history(exec).unwrap();
    // The cleanup's `ActivityScheduled` would be the event at the cap.
    rt.set_history_event_hard_cap(Some(u64::try_from(prefix.len() + 1).unwrap()));

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(&state, RunState::Cancelled(reason) if reason == "stop"),
        "a pending cancel wins over a cap that its cleanup reaches, got {state:?}"
    );
    assert_sealed_without_cleanup(&rt, &calls, exec, &prefix, "stop");
}

#[tokio::test]
async fn a_final_batch_past_the_cleanup_budget_is_discarded() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "floods_on_cancel").await;
    let prefix = rt.load_history(exec).unwrap();

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(&state, RunState::Cancelled(reason) if reason == "stop"));
    assert_sealed_without_cleanup(&rt, &calls, exec, &prefix, "stop");
}

#[tokio::test]
async fn a_suspending_batch_past_the_cleanup_budget_is_discarded() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "floods_then_waits").await;
    let prefix = rt.load_history(exec).unwrap();

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(&state, RunState::Cancelled(reason) if reason == "stop"));
    assert_sealed_without_cleanup(&rt, &calls, exec, &prefix, "stop");
}

#[tokio::test]
async fn a_final_batch_past_the_history_cap_is_discarded() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "mints_on_cancel").await;
    let prefix = rt.load_history(exec).unwrap();
    // The cleanup activity fits. Its UUID side effect reaches the cap.
    rt.set_history_event_hard_cap(Some(u64::try_from(prefix.len() + 3).unwrap()));

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(&state, RunState::Cancelled(reason) if reason == "stop"));
    let history = rt.load_history(exec).unwrap();
    assert!(
        history.len() <= prefix.len() + 3,
        "the seal must not grow history past the cap: {} events",
        history.len()
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. })),
        "the batch that reached the cap is discarded"
    );
    assert_eq!(calls.cleanup.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_unregistered_cleanup_activity_still_ends_the_run() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = start_parked(&mut rt, "calls_missing_on_cancel").await;

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await;

    assert!(
        matches!(&state, Ok(RunState::Cancelled(reason)) if reason == "stop"),
        "a cleanup that cannot dispatch must not keep the run alive, got {state:?}"
    );
    assert_eq!(leftover_rows(&path, exec), (0, 0, 0));
    assert_eq!(cancelled_events(&rt, exec), 1);
}

#[tokio::test]
async fn an_unregistered_workflow_still_ends_the_run() {
    let (_dir, path) = temp_db();
    let exec = {
        let (mut rt, _calls) = runtime(&path);
        start_parked(&mut rt, "parks").await
    };
    // A reopen that registers no workflow cannot replay the body.
    let mut rt = SqliteRuntime::open(&path).unwrap();

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await;

    assert!(
        matches!(&state, Ok(RunState::Cancelled(reason)) if reason == "stop"),
        "got {state:?}"
    );
    assert_eq!(cancelled_events(&rt, exec), 1);
}

#[tokio::test]
async fn a_repeat_cancel_keeps_the_first_reason() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = start_parked(&mut rt, "cleans_up").await;

    rt.cancel(exec, "first").unwrap();
    rt.cancel(exec, "second").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(&state, RunState::Cancelled(reason) if reason == "first"));
    assert_eq!(cancelled_events(&rt, exec), 1);
}

#[tokio::test]
async fn a_blank_reason_uses_the_default_text() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let cancelled = start_parked(&mut rt, "parks").await;
    let terminated = start_parked(&mut rt, "parks").await;

    rt.cancel(cancelled, "  ").unwrap();
    rt.terminate(terminated, "").unwrap();

    assert!(matches!(
        rt.run_until_blocked(cancelled).await.unwrap(),
        RunState::Cancelled(reason) if reason == "workflow cancellation requested"
    ));
    assert!(matches!(
        rt.run_until_blocked(terminated).await.unwrap(),
        RunState::Terminated(reason) if reason == "workflow termination requested"
    ));
}

#[tokio::test]
async fn a_body_that_fails_on_cancel_still_seals_cancelled() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = start_parked(&mut rt, "bails").await;

    rt.cancel(exec, "halt").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(&state, RunState::Cancelled(reason) if reason == "halt"));
    assert!(
        matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Terminated(s) if s == "CANCELLED")
    );
    assert_eq!(cancelled_events(&rt, exec), 1);
    assert!(
        !rt.load_history(exec)
            .unwrap()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a cancelled run records no `WorkflowFailed`"
    );
}

#[tokio::test]
async fn run_until_idle_seals_a_cancelled_run() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "cleans_up").await;

    rt.cancel(exec, "fleet").unwrap();
    rt.run_until_idle().await.unwrap();

    assert!(
        matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Terminated(s) if s == "CANCELLED")
    );
    assert_eq!(calls.cleanup.load(Ordering::SeqCst), 1);
}

// ── AC2: a terminated run ends TERMINATED with no pending rows ──────────────

#[tokio::test]
async fn terminate_seals_at_once_and_drops_timers_and_signals() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = rt.start_workflow("naps", json!(0)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::WaitingTimer
    ));
    rt.send_signal(exec, "stray", json!(1)).unwrap();
    assert_eq!(
        leftover_rows(&path, exec),
        (0, 1, 1),
        "setup: a timer and a signal"
    );

    rt.terminate(exec, "operator kill").unwrap();

    assert!(
        matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Terminated(s) if s == "TERMINATED")
    );
    assert!(
        matches!(last_event(&rt, exec), WorkflowEvent::WorkflowCancelled { reason } if reason == "operator kill")
    );
    assert_eq!(leftover_rows(&path, exec), (0, 0, 0));
}

#[tokio::test]
async fn terminate_drops_a_pending_activity_task() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let t0 = fixed_now();
    let exec = rt.start_workflow("retries", json!(5)).unwrap();
    rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert_eq!(leftover_rows(&path, exec).0, 1, "setup: a retry is pending");

    rt.terminate(exec, "kill").unwrap();

    assert_eq!(leftover_rows(&path, exec), (0, 0, 0));
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(61))
        .await
        .unwrap();
    assert!(matches!(&state, RunState::Terminated(reason) if reason == "kill"));
    assert_eq!(
        calls.flaky.load(Ordering::SeqCst),
        1,
        "the retry never runs"
    );
}

#[tokio::test]
async fn a_terminated_run_never_runs_its_body_again() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "cleans_up").await;

    rt.terminate(exec, "kill").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    rt.run_until_idle().await.unwrap();

    assert!(matches!(&state, RunState::Terminated(reason) if reason == "kill"));
    assert_eq!(calls.cleanup.load(Ordering::SeqCst), 0, "no cleanup runs");
    assert_eq!(calls.step.load(Ordering::SeqCst), 1);
    assert_eq!(cancelled_events(&rt, exec), 1);
}

#[tokio::test]
async fn terminate_overrides_a_pending_cancel() {
    let (_dir, path) = temp_db();
    let (mut rt, calls) = runtime(&path);
    let exec = start_parked(&mut rt, "cleans_up").await;

    rt.cancel(exec, "soft").unwrap();
    rt.terminate(exec, "hard").unwrap();

    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Terminated(reason) if reason == "hard"
    ));
    assert_eq!(calls.cleanup.load(Ordering::SeqCst), 0);
    assert_eq!(cancelled_events(&rt, exec), 1);
}

// ── AC3: unknown and terminal targets ───────────────────────────────────────

#[tokio::test]
async fn an_unknown_id_returns_execution_not_found() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let ghost = ExecutionId::new();

    assert!(matches!(
        rt.cancel(ghost, "x"),
        Err(SqliteError::ExecutionNotFound(id)) if id == ghost
    ));
    assert!(matches!(
        rt.terminate(ghost, "x"),
        Err(SqliteError::ExecutionNotFound(id)) if id == ghost
    ));
}

#[tokio::test]
async fn a_terminal_target_returns_workflow_not_running() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);

    let completed = start_parked(&mut rt, "parks").await;
    rt.send_signal(completed, "go", json!("done")).unwrap();
    assert!(matches!(
        rt.run_until_blocked(completed).await.unwrap(),
        RunState::Completed(_)
    ));
    let cancelled = start_parked(&mut rt, "parks").await;
    rt.cancel(cancelled, "c").unwrap();
    assert!(matches!(
        rt.run_until_blocked(cancelled).await.unwrap(),
        RunState::Cancelled(_)
    ));
    let terminated = start_parked(&mut rt, "parks").await;
    rt.terminate(terminated, "t").unwrap();

    for (exec, state) in [
        (completed, "COMPLETED"),
        (cancelled, "CANCELLED"),
        (terminated, "TERMINATED"),
    ] {
        let history = rt.load_history(exec).unwrap().len();
        for result in [rt.cancel(exec, "again"), rt.terminate(exec, "again")] {
            assert!(
                matches!(
                    &result,
                    Err(SqliteError::WorkflowNotRunning { execution_id, state: s })
                        if *execution_id == exec && s == state
                ),
                "a {state} target must be refused, got {result:?}"
            );
        }
        assert_eq!(
            rt.load_history(exec).unwrap().len(),
            history,
            "a refused call appends nothing"
        );
    }
}

// ── AC4: both histories replay on the core replayer ─────────────────────────

#[tokio::test]
async fn a_cancelled_history_replays_on_the_core_replayer() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = start_parked(&mut rt, "cleans_up").await;
    rt.cancel(exec, "user stop").unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let report = replay(
        rt.load_history(exec).unwrap(),
        "cleans_up",
        cleans_up_info().handler,
    )
    .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the cancel history must replay with its cleanup:\n{report}"
    );
}

#[tokio::test]
async fn a_cancel_sealed_while_parked_does_not_replay() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = start_parked(&mut rt, "parks").await;
    rt.cancel(exec, "stuck").unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let report = replay(
        rt.load_history(exec).unwrap(),
        "parks",
        parks_info().handler,
    )
    .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "the documented limit: a body parked without a check does not replay, as on Postgres:\n{report}"
    );
}

#[tokio::test]
async fn a_terminated_history_replays_on_the_core_replayer() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let exec = start_parked(&mut rt, "stops").await;
    rt.terminate(exec, "kill").unwrap();

    let report = replay(
        rt.load_history(exec).unwrap(),
        "stops",
        stops_info().handler,
    )
    .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the terminate history must replay:\n{report}"
    );
}

// ── Reuse matrix ────────────────────────────────────────────────────────────

#[tokio::test]
async fn allow_duplicate_failed_only_replaces_a_cancelled_prior() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let prior = cancelled_prior(&mut rt, "session-1").await;

    let next = rt
        .start_workflow_with_reuse_policy(
            "parks",
            "session-1",
            json!(2),
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        )
        .unwrap();

    assert!(
        next.created,
        "a cancelled prior is replaced like a failed one"
    );
    assert_ne!(next.exec_id, prior);
    assert!(
        matches!(rt.outcome(prior).unwrap(), ExecutionOutcome::Terminated(s) if s == "CONTINUED_AS_NEW")
    );
    assert_eq!(cancelled_events(&rt, prior), 1, "the replace adds no event");
}

/// Starts `parks` under `workflow_id`, cancels it, and drives it to the end.
async fn cancelled_prior(rt: &mut SqliteRuntime, workflow_id: &str) -> ExecutionId {
    let prior = rt
        .start_workflow_with_id("parks", workflow_id, json!(1))
        .unwrap();
    rt.run_until_blocked(prior).await.unwrap();
    rt.cancel(prior, "c").unwrap();
    assert!(matches!(
        rt.run_until_blocked(prior).await.unwrap(),
        RunState::Cancelled(_)
    ));
    prior
}

#[tokio::test]
async fn the_other_policies_treat_a_cancelled_prior_as_terminal() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let prior = cancelled_prior(&mut rt, "session-3").await;
    let start = |rt: &mut SqliteRuntime, policy| {
        rt.start_workflow_with_reuse_policy("parks", "session-3", json!(2), policy)
    };

    let attached = start(&mut rt, WorkflowIdReusePolicy::AllowDuplicate).unwrap();
    assert!(!attached.created);
    assert_eq!(attached.exec_id, prior);

    let refused = start(&mut rt, WorkflowIdReusePolicy::RejectDuplicate);
    assert!(matches!(
        &refused,
        Err(SqliteError::AlreadyExists { existing_exec_id, existing_state })
            if *existing_exec_id == prior && existing_state == "CANCELLED"
    ));

    let fresh = start(&mut rt, WorkflowIdReusePolicy::TerminateIfRunning).unwrap();
    assert!(fresh.created);
    assert_eq!(cancelled_events(&rt, prior), 1, "no second cancel event");
}

#[tokio::test]
async fn a_terminated_prior_frees_its_reuse_key() {
    let (_dir, path) = temp_db();
    let (mut rt, _calls) = runtime(&path);
    let prior = rt
        .start_workflow_with_id("parks", "session-2", json!(1))
        .unwrap();
    rt.terminate(prior, "t").unwrap();

    let next = rt
        .start_workflow_with_reuse_policy(
            "parks",
            "session-2",
            json!(2),
            WorkflowIdReusePolicy::RejectDuplicate,
        )
        .unwrap();

    assert!(next.created);
    assert_ne!(next.exec_id, prior);
}
