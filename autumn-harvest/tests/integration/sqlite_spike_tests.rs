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
use autumn_harvest::sqlite_spike::{ActivitySpec, RunState, SpikeError, SqliteRuntime};
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

/// Arm a durable timer, wait for it, then arm a SECOND durable timer (at the
/// advanced logical time) and wait for that too. Used by the durable-clock
/// regression: the second timer's absolute `fire_at` is relative to the clock
/// value at *its* arm time, so a restart must restore the clock rather than reset
/// it to 0 or the second timer never becomes due.
#[workflow]
async fn two_timers(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    ctx.timer("first", 5).await.map_err(|e| e.to_string())?;
    ctx.timer("second", 5).await.map_err(|e| e.to_string())?;
    Ok("both_fired".to_string())
}

/// Wait for a signal, then echo its payload. Used by the signal-delivery scenario.
#[workflow]
async fn wait_then_echo(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    let payload = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(payload)
}

/// Concurrently schedule an activity AND spawn a child workflow. The pure
/// `run_workflow` executor collects BOTH commands into one suspension batch
/// (`[ScheduleActivity, StartChildWorkflow]`, in `join!` poll order). The spike
/// supports `ScheduleActivity` but not `StartChildWorkflow`, so `apply_commands`
/// appends the activity's event + task row, then hits the unsupported child
/// command and rolls the whole transaction back. Used by the atomicity-rollback
/// regression test.
#[workflow]
async fn activity_then_child(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let (a, _c) = tokio::join!(
        ctx.execute_activity_raw("work", json!(n), "default"),
        ctx.spawn_child_workflow_raw("child", json!(n)),
    );
    let out = a.map_err(|e| e.to_string())?;
    Ok(out.as_i64().ok_or("bad activity output")? * 2)
}

/// Mint a deterministic UUID (issue #384 side effect) BEFORE running an activity,
/// then return both. Used by the side-effect-persistence regression: the minted
/// value is emitted as a `RecordSideEffect` command in the same batch as the
/// activity schedule, so it must be frozen into history (`SideEffectRecorded`)
/// and recovered verbatim on every replay — including on a FRESH runtime after a
/// restart. Dropping it (the pre-fix silent no-op) diverges replay.
#[workflow]
async fn uuid_then_activity(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    let id = ctx.new_uuid();
    let out = ctx
        .execute_activity_raw("work", json!(1), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "uuid": id.to_string(), "work": out }))
}

/// Mint a deterministic UUID (issue #384 side effect) and return it IMMEDIATELY —
/// no suspending activity/timer/signal. This is the terminal-cycle bookkeeping
/// case: the workflow completes in ONE cycle, emitting a `RecordSideEffect` in the
/// SAME command batch it returns from. `run_workflow` drops that terminal-cycle
/// command vector, so the prototype must use `run_workflow_with_state` and persist
/// the pending `SideEffectRecorded` BEFORE the `WorkflowCompleted` seal. Dropping
/// it (the pre-fix path) leaves history with no `SideEffectRecorded`, so a replay
/// re-mints a different value / diverges.
#[workflow]
async fn uuid_then_done(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    let id = ctx.new_uuid();
    Ok(json!({ "uuid": id.to_string() }))
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

/// Cross-table invariant: every `TimerStarted` event in the durable history has
/// a matching armed (unfired) `spike_timers` row — the atomic append+arm never
/// commits one without the other.
fn assert_timers_match_events(rt: &SqliteRuntime, exec: ExecutionId) {
    let events = rt.load_history(exec).unwrap();
    let armed: std::collections::HashSet<String> =
        rt.armed_timer_ids(exec).unwrap().into_iter().collect();
    let started = count_events(&events, |e| matches!(e, WorkflowEvent::TimerStarted { .. }));
    for e in &events {
        if let WorkflowEvent::TimerStarted { timer_id, .. } = e {
            assert!(
                armed.contains(&timer_id.to_string()),
                "TimerStarted {timer_id:?} has no matching armed spike_timers row"
            );
        }
    }
    assert_eq!(
        armed.len(),
        started,
        "every armed timer row corresponds to a TimerStarted event and vice versa"
    );
}

/// Cross-table invariant (timer-fire atomicity): every `TimerFired` event in the
/// durable history has a matching `fired = 1` row in `spike_timers` — the atomic
/// `TimerFired` append + `fired = 1` flag never commits one without the other, so
/// a reload can never re-fire an already-fired timer (which would append a stray
/// duplicate `TimerFired` to the replayable log).
fn assert_fired_timers_match_events(rt: &SqliteRuntime, exec: ExecutionId) {
    let events = rt.load_history(exec).unwrap();
    let fired: std::collections::HashSet<String> =
        rt.fired_timer_ids(exec).unwrap().into_iter().collect();
    let fired_events = count_events(&events, |e| matches!(e, WorkflowEvent::TimerFired { .. }));
    for e in &events {
        if let WorkflowEvent::TimerFired { timer_id } = e {
            assert!(
                fired.contains(&timer_id.to_string()),
                "TimerFired {timer_id:?} has no matching fired=1 spike_timers row"
            );
        }
    }
    assert_eq!(
        fired.len(),
        fired_events,
        "every fired timer row corresponds to a TimerFired event and vice versa"
    );
}

/// Cross-table invariant: every `ActivityScheduled` event in the durable history
/// has a matching `spike_tasks` row — the atomic append+enqueue never commits
/// one without the other.
fn assert_activities_match_events(rt: &SqliteRuntime, exec: ExecutionId) {
    let events = rt.load_history(exec).unwrap();
    let queued: std::collections::HashSet<String> =
        rt.queued_activity_ids(exec).unwrap().into_iter().collect();
    for e in &events {
        if let WorkflowEvent::ActivityScheduled { activity_id, .. } = e {
            assert!(
                queued.contains(&activity_id.to_string()),
                "ActivityScheduled {activity_id:?} has no matching spike_tasks row"
            );
        }
    }
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
    rt.advance_time(5).unwrap();
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
    // Timer-fire atomicity: the fired timer's event and its `fired = 1` row agree.
    assert_fired_timers_match_events(&rt, exec);
}

// ── FIX 1 regression: timer-fire is atomic (TimerFired event + fired=1 flag) ──
//
// Before the fix, `drain_ready` appended the `TimerFired` event and set the
// `fired = 1` flag in SEPARATE autocommits. A crash between them would leave the
// event durable with `fired = 0`, so a reload's `due_timers` scan re-fired the
// timer and appended a stray SECOND `TimerFired` — corrupting the replayable log
// with a non-lifecycle event the workflow consumed only once. The append + flag
// now commit in one transaction, so the invariant "a durable `TimerFired` always
// has its `fired = 1` row" holds — including across a reopen, where `due_timers`
// then finds nothing to re-fire.
#[tokio::test]
async fn scenario_timer_fire_is_atomic_no_double_fire_across_reload() {
    let (_dir, path) = temp_db();
    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&timer_then_done_info());
        let exec = rt.start_workflow("timer_then_done", json!(0)).unwrap();
        assert!(matches!(
            rt.run_until_blocked(exec).await.unwrap(),
            RunState::WaitingTimer
        ));
        rt.advance_time(5).unwrap();
        assert!(matches!(
            rt.run_until_blocked(exec).await.unwrap(),
            RunState::Completed(_)
        ));
        // The fired timer's event and its `fired = 1` row agree.
        assert_fired_timers_match_events(&rt, exec);
        exec
    };

    // Reopen (== restart): exactly one `TimerFired`, and the flag survived with
    // it, so `due_timers` finds nothing to re-fire — no stray duplicate.
    let rt = SqliteRuntime::open(&path).unwrap();
    let events = rt.load_history(exec).unwrap();
    assert_eq!(
        count_events(&events, |e| matches!(e, WorkflowEvent::TimerFired { .. })),
        1,
        "exactly one TimerFired — never re-fired across reload"
    );
    assert_fired_timers_match_events(&rt, exec);
}

// ── FIX 2 regression: the virtual clock is durably restored across a restart ──
//
// A workflow arms+fires a FIRST timer (advancing the virtual clock), then arms a
// SECOND timer whose absolute deadline (`fire_at = 10`) is relative to that
// advanced clock. Crucially the runtime then advances the clock *part-way* toward
// the second deadline (to 8 < 10) — progress captured ONLY by the durable clock,
// NOT by the belt-and-braces fired-timer floor (still 5) — before restarting.
// Before the fix, `open` reset the clock to 0, so advancing 2 more only reached
// logical time 2 (≪ 10) and the second timer never fired; the run wedged at
// `WaitingTimer` forever. With the durable clock the restart restores logical
// time 8, and +2 reaches 10, firing the second timer and completing the run. (The
// part-way advance is what makes this isolate the *persisted-clock* fix rather
// than being masked by the fired-timer floor.)
#[tokio::test]
async fn scenario_two_timers_second_fires_across_restart_via_durable_clock() {
    let (_dir, path) = temp_db();

    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&two_timers_info());
        let exec = rt.start_workflow("two_timers", json!(0)).unwrap();

        // Block on the first timer (fire_at = 0 + 5 = 5).
        assert!(matches!(
            rt.run_until_blocked(exec).await.unwrap(),
            RunState::WaitingTimer
        ));
        // Fire the first timer; the workflow arms the second at the advanced clock
        // (fire_at = 5 + 5 = 10) and blocks on it.
        rt.advance_time(5).unwrap();
        assert!(matches!(
            rt.run_until_blocked(exec).await.unwrap(),
            RunState::WaitingTimer
        ));
        // Advance PART-WAY toward the second deadline (clock = 8, still < 10). This
        // progress lives only in the durable clock — the fired-timer floor is 5.
        rt.advance_time(3).unwrap();
        assert!(matches!(
            rt.run_until_blocked(exec).await.unwrap(),
            RunState::WaitingTimer
        ));
        // First timer fired; second armed and unfired.
        assert_fired_timers_match_events(&rt, exec);
        exec
    };

    // Runtime #2: fresh process on the SAME file. The durable clock restores
    // logical time 8 (NOT 0, and NOT merely the fired-timer floor of 5).
    // Advancing 2 more reaches 10 — the second timer's absolute deadline.
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&two_timers_info());
    rt.advance_time(2).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("both_fired")),
        "state = {state:?}"
    );
    let events = rt.load_history(exec).unwrap();
    assert_eq!(
        count_events(&events, |e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        2
    );
    assert_eq!(
        count_events(&events, |e| matches!(e, WorkflowEvent::TimerFired { .. })),
        2
    );
    // Timer-fire atomicity holds across the reload for BOTH fired timers.
    assert_fired_timers_match_events(&rt, exec);
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

// ── AC4: a RETRY-bearing SQLite history replays on the engine's strict path ──
//
// Closes the §5.3 coverage gap: the original cross-backend test exercised only a
// success-path, single-attempt history. Here the history is produced by driving
// the *retry* scenario (fail once, then succeed) through the prototype, then
// round-tripped through the engine's own `WorkflowReplayer`. The retryable
// failure lives in the audit table, not the replayable log, so the resulting
// event stream is terminal-clean (`ActivityScheduled → ActivityCompleted`) — and
// this proves that stream replays cleanly on the strict engine path.
#[cfg(feature = "testing")]
#[tokio::test]
async fn scenario_cross_backend_replay_retry_history() {
    use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};

    let (_dir, path) = temp_db();

    // Drive the retry scenario: "work" fails on attempt 1, succeeds on attempt 2.
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&single_activity_info());
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
    let exec = rt.start_workflow("single_activity", json!(4)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    // (4 * 10) * 2 == 80, after exactly two activity attempts.
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(80)),
        "state = {state:?}"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let events = rt.load_history(exec).unwrap();

    // The retry-produced history round-trips through the engine's strict replay.
    let snapshot = HistorySnapshot {
        workflow_name: "single_activity".to_string(),
        execution_id: exec,
        events,
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
        "retry-bearing SQLite history must replay on the engine path:\n{report}"
    );
}

// ── AC4 (vice versa): a genuinely PG-SHAPED history drives the SQLite runtime ──
//
// The other direction of the cross-backend guarantee, closing the §5.3 gap that
// the executed test never pushed an `ActivityStarted`-bearing history through the
// prototype. Hand-build a `Vec<WorkflowEvent>` in the exact shape the *Postgres*
// engine writes — including the `ActivityStarted` event the spike itself never
// emits (`WorkflowStarted → ActivityScheduled → ActivityStarted →
// ActivityCompleted`) — import it into a fresh SQLite runtime, and drive the
// prototype's OWN reload path. It must resume deterministically to the identical
// output with the already-completed activity NOT re-executed.
#[cfg(feature = "testing")]
#[tokio::test]
async fn scenario_pg_shaped_history_replays_on_sqlite() {
    let activity_id = ActivityExecId::new();

    // A PG-shaped, replay-equivalent-not-byte-identical history: the `Scheduled →
    // Started → Completed` triple a Postgres claim produces, terminating before
    // any suspension (the `single_activity` workflow completes on replay).
    let pg_history = vec![
        WorkflowEvent::workflow_started(json!(3), chrono::Utc::now()),
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "work".to_string(),
            input: json!(3),
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityStarted {
            activity_id,
            worker_id: WorkerId::new("pg-worker-1"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: json!(30),
        },
    ];

    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&single_activity_info());
    let ran = Arc::new(AtomicUsize::new(0));
    let r2 = ran.clone();
    rt.register_activity(
        "work",
        ActivitySpec::new(1, move |input: serde_json::Value| {
            r2.fetch_add(1, Ordering::SeqCst);
            Ok(json!(input.as_i64().unwrap() * 10))
        }),
    );

    let exec = rt
        .import_execution("single_activity", json!(3), pg_history)
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    // Resumes purely by replaying the PG-shaped history: (3 * 10) * 2 == 60,
    // with the activity body NEVER invoked (the matcher skips `ActivityStarted`
    // and reads the recorded `ActivityCompleted`).
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(60)),
        "state = {state:?}"
    );
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "a PG-shaped completed-activity history must not re-run the activity"
    );

    // And the prototype's persisted stream now carries the imported
    // `ActivityStarted` (proof it drove a genuinely PG-shaped history), plus its
    // own appended terminal `WorkflowCompleted`.
    let events = rt.load_history(exec).unwrap();
    assert_eq!(
        count_events(&events, |e| matches!(
            e,
            WorkflowEvent::ActivityStarted { .. }
        )),
        1
    );
    assert!(matches!(
        events.last(),
        Some(WorkflowEvent::WorkflowCompleted { .. })
    ));
}

// ── Codex P1 regression: per-cycle persistence is atomic (event + queue/timer row) ──
//
// The prototype persists a decision cycle's derived event AND its paired task /
// durable-timer row in ONE `SQLite` transaction (`apply_commands`), and a drained
// activity's terminal event + task-state flip in ONE transaction (`drain_ready`).
// Before the fix these were separate autocommits: a crash between the event
// append and the row insert left history saying an activity/timer was scheduled
// while no queue/timer row existed, wedging the reload path at `SpikeError::Stuck`.

/// Invariant guard (deterministic): across a reload, every scheduled-work event
/// is observed together with its queue/timer row — never one without the other.
#[tokio::test]
async fn scenario_atomic_persist_event_and_row_never_out_of_sync() {
    // Timer: after arming (blocked on the timer), the `TimerStarted` event and
    // its `spike_timers` row are BOTH present, and survive a reload together.
    let (_dir, path) = temp_db();
    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&timer_then_done_info());
        let exec = rt.start_workflow("timer_then_done", json!(0)).unwrap();
        assert!(matches!(
            rt.run_until_blocked(exec).await.unwrap(),
            RunState::WaitingTimer
        ));
        assert_timers_match_events(&rt, exec);
        exec
    };
    // Reopen on the same file (== process restart): event and row still agree.
    let rt = SqliteRuntime::open(&path).unwrap();
    assert_timers_match_events(&rt, exec);

    // Activity: a completed activity's `ActivityScheduled` event keeps its
    // `spike_tasks` row across a reload — the append+enqueue committed together.
    let (_dir2, path2) = temp_db();
    let exec2 = {
        let mut rt = SqliteRuntime::open(&path2).unwrap();
        rt.register_workflow(&single_activity_info());
        rt.register_activity(
            "work",
            ActivitySpec::new(1, |input: serde_json::Value| {
                Ok(json!(input.as_i64().unwrap() * 10))
            }),
        );
        let exec2 = rt.start_workflow("single_activity", json!(3)).unwrap();
        assert!(matches!(
            rt.run_until_blocked(exec2).await.unwrap(),
            RunState::Completed(_)
        ));
        assert_activities_match_events(&rt, exec2);
        exec2
    };
    let rt2 = SqliteRuntime::open(&path2).unwrap();
    assert_activities_match_events(&rt2, exec2);
}

/// Both-or-neither proof (fails on the pre-fix per-command-autocommit code): a
/// batch of `[ScheduleActivity, StartChildWorkflow]` appends+enqueues the
/// activity, then hits the unsupported child command and returns `Err`, dropping
/// the uncommitted transaction. The activity's event AND its task row are rolled
/// back with the failed batch — NEITHER persists. (Under separate autocommits the
/// `ActivityScheduled` event + task row would have committed before the
/// unsupported command was reached, so this assertion would fail.)
#[tokio::test]
async fn scenario_atomic_persist_rolls_back_the_whole_batch_on_unsupported_command() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&activity_then_child_info());
    rt.register_activity(
        "work",
        ActivitySpec::new(1, |input: serde_json::Value| {
            Ok(json!(input.as_i64().unwrap() * 10))
        }),
    );

    let exec = rt.start_workflow("activity_then_child", json!(5)).unwrap();
    let err = rt.drive_one_cycle(exec).await.unwrap_err();
    assert!(
        matches!(err, SpikeError::Unsupported(ref c) if c == "StartChildWorkflow"),
        "err = {err:?}"
    );

    // Atomicity: the `ScheduleActivity` appended+enqueued earlier in the batch was
    // rolled back with the failed transaction — neither the event nor the row.
    let events = rt.load_history(exec).unwrap();
    assert_eq!(
        count_events(&events, |e| matches!(
            e,
            WorkflowEvent::ActivityScheduled { .. }
        )),
        0,
        "the scheduled-activity event must roll back with the failed batch"
    );
    assert!(
        rt.queued_activity_ids(exec).unwrap().is_empty(),
        "the task-queue row must roll back with the failed batch"
    );
    // Only the pre-cycle `WorkflowStarted` (its own earlier autocommit) survives.
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], WorkflowEvent::WorkflowStarted { .. }));
}

// ── Codex P1 regression (round 4): importing an IN-FLIGHT open activity ──
//
// `import_execution` seeds a cross-backend history. When that history carries an
// *open* (un-terminated) `ActivityScheduled` — the source backend stopped with
// the activity still in flight (`Scheduled → Started`, no `Completed`) — the
// importer must materialize the companion PENDING `spike_tasks` row, or replay
// re-derives a `WaitForActivity` whose wait branch enqueues nothing and the run
// wedges at `SpikeError::Stuck` (the normal command path was made atomic in round
// 3, but the import path re-created the same "event without companion row" state).
// With the fix the worker claims the materialized task, runs the body once, and
// the imported execution drains to `Completed`. Fails on the pre-fix code
// (`assert_activities_match_events` finds an empty task set / `run_until_blocked`
// returns `Err(Stuck)`).
#[tokio::test]
async fn scenario_import_in_flight_activity_materializes_task_and_drains() {
    let activity_id = ActivityExecId::new();
    // In-flight PG-shaped history: Scheduled + Started, but NO terminal event.
    let in_flight = vec![
        WorkflowEvent::workflow_started(json!(3), chrono::Utc::now()),
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "work".to_string(),
            input: json!(3),
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityStarted {
            activity_id,
            worker_id: WorkerId::new("pg-worker-1"),
        },
    ];

    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&single_activity_info());
    let ran = Arc::new(AtomicUsize::new(0));
    let r2 = ran.clone();
    rt.register_activity(
        "work",
        ActivitySpec::new(1, move |input: serde_json::Value| {
            r2.fetch_add(1, Ordering::SeqCst);
            Ok(json!(input.as_i64().unwrap() * 10))
        }),
    );

    let exec = rt
        .import_execution("single_activity", json!(3), in_flight)
        .unwrap();
    // The importer materialized the derived PENDING task for the open activity:
    // every `ActivityScheduled` event now has its `spike_tasks` row (pre-fix this
    // set is empty and this assertion fails).
    assert_activities_match_events(&rt, exec);
    assert_eq!(
        rt.queued_activity_ids(exec).unwrap(),
        vec![activity_id.to_string()],
        "the open imported activity has a materialized task row"
    );

    let state = rt.run_until_blocked(exec).await.unwrap();
    // (3 * 10) * 2 == 60 — the in-flight activity runs to completion on handoff.
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(60)),
        "state = {state:?}"
    );
    assert_eq!(
        ran.load(Ordering::SeqCst),
        1,
        "the open imported activity runs exactly once on handoff (at-least-once)"
    );
}

// ── Codex P2 regression (round 4): deterministic side-effect commands persist ──
//
// A workflow that calls a deterministic primitive (`ctx.new_uuid()`) BEFORE a
// suspending activity emits a `RecordSideEffect` in the same command batch. If
// `apply_commands` drops it (the pre-fix silent no-op), no `SideEffectRecorded`
// reaches history; on the next replay the matcher finds `ActivityScheduled` where
// it expects the side effect and either diverges (`SideEffectDrift → WorkflowFailed`)
// or re-mints a different value. With the fix the value is frozen into history
// (before the activity schedule, in command order) and recovered VERBATIM across a
// restart onto a fresh runtime. Fails on the pre-fix code: no `SideEffectRecorded`
// is persisted, so the `.expect(...)` below panics (and the run cannot even
// complete deterministically).
#[tokio::test]
async fn scenario_side_effect_command_persists_and_replays_across_restart() {
    let (_dir, path) = temp_db();

    // Runtime #1: drive ONE cycle so the side effect + activity are persisted but
    // the workflow is NOT resumed past the activity (mirrors scenario_4's crash).
    let (exec, recorded_uuid) = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&uuid_then_activity_info());
        let ran = Arc::new(AtomicUsize::new(0));
        let r2 = ran.clone();
        rt.register_activity(
            "work",
            ActivitySpec::new(1, move |_input: serde_json::Value| {
                r2.fetch_add(1, Ordering::SeqCst);
                Ok(json!("done"))
            }),
        );
        let exec = rt.start_workflow("uuid_then_activity", json!(0)).unwrap();
        let state = rt.drive_one_cycle(exec).await.unwrap();
        assert!(matches!(state, RunState::InProgress), "state = {state:?}");
        assert_eq!(ran.load(Ordering::SeqCst), 1);

        // The side effect is frozen into the replayable history, BEFORE the
        // scheduled activity (command emission order).
        let events = rt.load_history(exec).unwrap();
        let recorded = events.iter().find_map(|e| match e {
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Uuid,
                value,
                ..
            } => Some(value.as_str().unwrap().to_string()),
            _ => None,
        });
        let recorded_uuid =
            recorded.expect("SideEffectRecorded(Uuid) must be persisted to history");
        let side_effect_pos = events
            .iter()
            .position(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. }))
            .unwrap();
        let scheduled_pos = events
            .iter()
            .position(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
            .unwrap();
        assert!(
            side_effect_pos < scheduled_pos,
            "the side effect is recorded before the activity schedule (command order)"
        );
        (exec, recorded_uuid)
    };

    // Runtime #2: fresh process on the SAME file. The workflow re-runs from the top
    // and MUST recover the minted UUID from `SideEffectRecorded` (not re-mint / not
    // diverge).
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&uuid_then_activity_info());
    let ran2 = Arc::new(AtomicUsize::new(0));
    let r2 = ran2.clone();
    rt.register_activity(
        "work",
        ActivitySpec::new(1, move |_input: serde_json::Value| {
            r2.fetch_add(1, Ordering::SeqCst);
            Ok(json!("done"))
        }),
    );
    let state = rt.run_until_blocked(exec).await.unwrap();

    match state {
        RunState::Completed(v) => assert_eq!(
            v.get("uuid").and_then(serde_json::Value::as_str),
            Some(recorded_uuid.as_str()),
            "the deterministic UUID is recovered verbatim across the restart"
        ),
        other => panic!("expected Completed, got {other:?} — the side effect was not replayed"),
    }
    assert_eq!(
        ran2.load(Ordering::SeqCst),
        0,
        "the already-completed activity must not re-run on replay"
    );
}

// ── Codex P2 regression (round 5): terminal-cycle bookkeeping persists before seal ──
//
// A workflow that mints a deterministic value (`ctx.new_uuid()`) and then RETURNS
// in the SAME cycle — with no suspending activity/timer/signal — emits a
// `RecordSideEffect` in the terminal command batch. `run_workflow` drops that
// vector; the prototype must instead use `run_workflow_with_state` and persist the
// pending `SideEffectRecorded` BEFORE the `WorkflowCompleted` seal (mirroring the
// engine's `worker::persist_terminal_outcome_commands`). Fails on the pre-fix code:
// no `SideEffectRecorded` reaches history, so the `.expect(...)` below panics; and
// the genuine-restart leg re-mints a DIFFERENT uuid on the imported (side-effect-
// less) history rather than recovering the recorded one.
#[tokio::test]
async fn scenario_terminal_cycle_side_effect_persists_before_seal() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&uuid_then_done_info());

    let exec = rt.start_workflow("uuid_then_done", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    // Completed in ONE terminal cycle (no suspension): the minted UUID is the output.
    let output_uuid = match state {
        RunState::Completed(ref v) => v
            .get("uuid")
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .to_string(),
        other => panic!("expected Completed, got {other:?}"),
    };

    // The side effect is frozen into the replayable history BEFORE the terminal
    // seal — dropping it (pre-fix) leaves history with no `SideEffectRecorded`, so
    // `.expect` panics.
    let events = rt.load_history(exec).unwrap();
    let recorded = events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Uuid,
                value,
                ..
            } => Some(value.as_str().unwrap().to_string()),
            _ => None,
        })
        .expect("SideEffectRecorded(Uuid) must be persisted before the terminal seal");
    assert_eq!(
        recorded, output_uuid,
        "the recorded side effect is the minted UUID returned as the output"
    );
    let se_pos = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. }))
        .unwrap();
    let done_pos = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. }))
        .unwrap();
    assert!(
        se_pos < done_pos,
        "the side effect precedes the terminal WorkflowCompleted"
    );

    // Genuine-restart proof: strip the terminal seal (a PG-shaped in-flight
    // history) and drive the reload path on a FRESH runtime — the workflow recovers
    // the SAME UUID from `SideEffectRecorded` rather than re-minting. Pre-fix, the
    // stripped history is `[WorkflowStarted]` (no side effect), so a fresh UUID
    // ≠ output is minted and this assertion fails.
    let mut in_flight = events.clone();
    assert!(matches!(
        in_flight.pop(),
        Some(WorkflowEvent::WorkflowCompleted { .. })
    ));
    let (_dir2, path2) = temp_db();
    let mut rt2 = SqliteRuntime::open(&path2).unwrap();
    rt2.register_workflow(&uuid_then_done_info());
    let exec2 = rt2
        .import_execution("uuid_then_done", json!(0), in_flight)
        .unwrap();
    match rt2.run_until_blocked(exec2).await.unwrap() {
        RunState::Completed(v) => assert_eq!(
            v.get("uuid").and_then(serde_json::Value::as_str),
            Some(output_uuid.as_str()),
            "the terminal-cycle side effect is recovered verbatim on restart"
        ),
        other => panic!("expected Completed, got {other:?}"),
    }
}

// ── Codex P2 regression (round 5): a SEALED imported history derives terminal state ──
//
// `import_execution` seeds a cross-backend history. When that history is already
// SEALED — its tail lifecycle event is a terminal `WorkflowCompleted` (a source
// backend handing off an *already-finished* execution) — the importer must
// initialize the execution row directly in that terminal state with the imported
// output, NOT create it RUNNING. Otherwise the next `run_until_blocked` replays the
// already-sealed history and appends a SECOND `WorkflowCompleted` before flipping
// the row, corrupting the imported log. With the fix: the imported run reads as
// terminal with the imported output, and driving it appends NO second terminal
// event (event count unchanged) and re-runs nothing. Fails on the pre-fix code
// (the row is RUNNING, the workflow is re-driven, and a duplicate terminal is
// appended).
#[tokio::test]
async fn scenario_import_sealed_history_derives_terminal_state() {
    let activity_id = ActivityExecId::new();
    // A fully SEALED PG-shaped history: Scheduled → Started → Completed →
    // WorkflowCompleted{output}. The imported output ((3 * 10) * 2 == 60) is what
    // the run reports without any re-execution.
    let sealed = vec![
        WorkflowEvent::workflow_started(json!(3), chrono::Utc::now()),
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "work".to_string(),
            input: json!(3),
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityStarted {
            activity_id,
            worker_id: WorkerId::new("pg-worker-1"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: json!(30),
        },
        WorkflowEvent::WorkflowCompleted { output: json!(60) },
    ];
    let imported_len = sealed.len();

    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&single_activity_info());
    let ran = Arc::new(AtomicUsize::new(0));
    let r2 = ran.clone();
    rt.register_activity(
        "work",
        ActivitySpec::new(1, move |input: serde_json::Value| {
            r2.fetch_add(1, Ordering::SeqCst);
            Ok(json!(input.as_i64().unwrap() * 10))
        }),
    );

    let exec = rt
        .import_execution("single_activity", json!(3), sealed)
        .unwrap();

    // The sealed import is immediately terminal with the imported output — a
    // `run_until_blocked` short-circuits at the "already terminal?" check.
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(60)),
        "state = {state:?}"
    );
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "a sealed imported history must not re-run any activity"
    );

    // No SECOND terminal event was appended: the durable log is exactly the
    // imported history, unchanged (pre-fix, a duplicate `WorkflowCompleted` is
    // appended, corrupting the log).
    let events = rt.load_history(exec).unwrap();
    assert_eq!(
        events.len(),
        imported_len,
        "the imported sealed history is not grown by a second terminal event"
    );
    assert_eq!(
        count_events(&events, |e| matches!(
            e,
            WorkflowEvent::WorkflowCompleted { .. }
        )),
        1,
        "exactly one WorkflowCompleted — never a duplicate terminal"
    );

    // A sealed FAILED import is derived the same way (its imported error surfaces).
    let sealed_failed = vec![
        WorkflowEvent::workflow_started(json!(1), chrono::Utc::now()),
        WorkflowEvent::workflow_failed("boom from source backend".to_string()),
    ];
    let (_dir2, path2) = temp_db();
    let mut rt2 = SqliteRuntime::open(&path2).unwrap();
    rt2.register_workflow(&single_activity_info());
    let exec2 = rt2
        .import_execution("single_activity", json!(1), sealed_failed)
        .unwrap();
    let state2 = rt2.run_until_blocked(exec2).await.unwrap();
    assert!(
        matches!(state2, RunState::Failed(ref e) if e == "boom from source backend"),
        "state = {state2:?}"
    );
    assert_eq!(
        rt2.load_history(exec2).unwrap().len(),
        2,
        "the sealed FAILED import is not grown by a second terminal event"
    );
}

// ── Codex P2 regression (round 6): an unregistered activity requeues, not wedges ──
//
// A workflow schedules an activity whose handler is NOT registered. The claim
// commits the task RUNNING in its own transaction (the body runs between the claim
// and the post-body persistence, so the two can't share one), and only THEN does
// the handler lookup fail. Before the fix that error propagated with the task left
// stranded RUNNING — and later claims select only PENDING, so registering the
// handler and re-running could NEVER drain it: the run wedged at
// `SpikeError::Stuck` forever. With the fix the failed claim requeues the task to
// PENDING (attempt unchanged) via `mark_pending` and surfaces the original
// `UnregisteredActivity` error, so a subsequent run — after the handler is
// registered — drains the same task and the workflow completes. This is the
// recoverable-application-condition half of `drain_ready`'s
// requeue-on-any-post-claim-error invariant (the other half — a transient
// post-body persistence error — requeues by the same path).
#[tokio::test]
async fn scenario_unregistered_activity_requeues_and_drains_after_registration() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&single_activity_info());
    // NB: the "work" activity is deliberately NOT registered for the first run.

    let exec = rt.start_workflow("single_activity", json!(6)).unwrap();

    // First run: the activity is scheduled and claimed RUNNING, then the missing
    // handler surfaces `UnregisteredActivity` — but the task is requeued to
    // PENDING, NOT left stranded RUNNING.
    let first = rt.run_until_blocked(exec).await;
    assert!(
        matches!(first, Err(SpikeError::UnregisteredActivity(ref n)) if n == "work"),
        "first run should surface the unregistered activity: {first:?}"
    );
    // The claimed task was returned to PENDING (re-drainable), not stranded
    // RUNNING. Pre-fix this reads `["RUNNING"]` and the assertion fails.
    assert_eq!(
        rt.task_states(exec).unwrap(),
        vec!["PENDING".to_string()],
        "the claimed task is requeued to PENDING, not stranded RUNNING"
    );

    // Register the missing handler and re-run: the PENDING task now drains and the
    // workflow completes. (Pre-fix, the task was stuck RUNNING —
    // `claim_next_ready_task` selects only PENDING — so this re-run returns
    // `Err(Stuck)` and the `.unwrap()` panics.)
    let ran = Arc::new(AtomicUsize::new(0));
    let r2 = ran.clone();
    rt.register_activity(
        "work",
        ActivitySpec::new(1, move |input: serde_json::Value| {
            r2.fetch_add(1, Ordering::SeqCst);
            Ok(json!(input.as_i64().unwrap() * 10))
        }),
    );
    let state = rt.run_until_blocked(exec).await.unwrap();

    // (6 * 10) * 2 == 120, drained after the handler was registered.
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(120)),
        "state = {state:?}"
    );
    assert_eq!(
        ran.load(Ordering::SeqCst),
        1,
        "the activity runs exactly once, on the run after it was registered"
    );
}
