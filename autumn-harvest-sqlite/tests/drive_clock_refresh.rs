//! Regression tests for the public drivers re-reading the wall clock **once per
//! decision cycle** (`run_until_blocked` / `poll_once`, Codex #1069 P2,
//! runtime.rs:~361).
//!
//! Before the fix the public `run_until_blocked` captured `Utc::now()` a single
//! time and reused that timestamp for every internal `drive_one_cycle`. When a
//! cycle consumed real wall-clock time — e.g. a 2-second activity body ran before
//! the workflow armed a timer in a later cycle — a timer scheduled after the
//! activity was persisted with an absolute deadline relative to the START of the
//! outer call rather than its true arm instant. A `timer(1s)` following a 2s
//! activity therefore stored `fire_at = call_start + 1s`, already ~1s in the past
//! by the time it was armed, so it fired immediately on the next poll instead of
//! waiting ~1s from when it was armed. The public drivers now re-read the clock at
//! the start of each cycle; the `_as_of` seams keep a caller-fixed timestamp for
//! deterministic simulation.
//!
//! These tests are fully deterministic (no sleeping): one drives cycle-by-cycle
//! via the `_as_of` seam with manually advanced timestamps, the other injects an
//! advancing clock (`set_clock`) to prove the *public* `run_until_blocked` path
//! refreshes per cycle.

// The `#[workflow]` macro references the workflow's input parameter, so a
// deliberately-unused param reads as a "used underscore binding" through the
// expansion.
#![allow(clippy::used_underscore_binding)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

fn temp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

/// Raw `fire_at` of the single armed timer, read from a second connection to the
/// same file (the established inspection pattern in `subsecond_timer.rs`).
fn timer_fire_at(path: &std::path::Path, exec: ExecutionId) -> i64 {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT fire_at FROM harvest_timers WHERE exec_id = ?1",
        [exec.to_string()],
        |r| r.get(0),
    )
    .unwrap()
}

/// Run one activity ("work"), THEN arm a 1-second durable timer, then complete.
/// The activity precedes the timer specifically so that — in a real drive — the
/// timer is armed in a *later* cycle than the one the outer call started in.
#[workflow]
async fn activity_then_timer(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    ctx.execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    ctx.timer("t", 1).await.map_err(|e| e.to_string())?;
    Ok("done".to_string())
}

fn register(rt: &mut SqliteRuntime) {
    rt.register_workflow(&activity_then_timer_info());
    // Instant, always-succeeding activity body — the point is the *cycle
    // boundary* it introduces, not the work itself.
    rt.register_activity_raw(
        "work",
        ActivitySpec::new(1, |v: Value| -> Result<Value, String> { Ok(v) }),
    );
}

/// Concept lock via the deterministic `_as_of` seam (no clock injection): driving
/// cycle-by-cycle with manually advanced timestamps — simulating ~2s elapsing
/// during the activity — the timer armed in the LATER cycle anchors its absolute
/// deadline to that cycle's `now` (arm instant), never to the earlier call-start
/// instant. This locks the underlying `drive_one_cycle` arm behavior the public
/// per-cycle refresh depends on.
#[tokio::test]
async fn timer_armed_after_a_slow_activity_anchors_to_its_arm_cycle_not_call_start() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register(&mut rt);
    let exec = rt.start_workflow("activity_then_timer", json!(7)).unwrap();

    // t0 = the instant the drive starts.
    let t0: DateTime<Utc> = DateTime::from_timestamp(5_000_000, 0).unwrap();

    // Cycle 1 at t0: schedules + runs the activity (durable progress).
    assert!(
        rt.poll_once_as_of(t0).await.unwrap(),
        "cycle 1 (activity) must make progress"
    );

    // ~2s of real time elapsed while the activity ran; cycle 2 at t0+2s arms the
    // timer. (Each `poll_once_as_of` drives exactly one cycle for the one run.)
    let t_arm = t0 + chrono::Duration::seconds(2);
    assert!(
        rt.poll_once_as_of(t_arm).await.unwrap(),
        "cycle 2 (arm timer) must make progress"
    );

    // The stored deadline is anchored to the ARM cycle: (t0+2s) + 1s = t0+3s. If
    // it were anchored to the call-start t0 it would read t0+1s — already ~1s
    // overdue by the time it was actually armed.
    assert_eq!(
        timer_fire_at(&path, exec),
        t_arm.timestamp_millis() + 1000,
        "timer must anchor to its true arm instant (t0+2s + 1s), not the drive's start"
    );

    // At the arm instant (t0+2s) the true deadline (t0+3s) has NOT passed, so the
    // run stays blocked on the timer — it must not fire ~1s early.
    let state = rt.run_until_blocked_as_of(exec, t_arm).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "timer must still be pending ~1s after arming, got {state:?}"
    );

    // At the true deadline (t0+3s) it fires and the run completes.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(3))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "done"),
        "timer fires at (and only at) its true deadline, got {state:?}"
    );
}

/// Red→green proof for the actual finding: the PUBLIC `run_until_blocked` re-reads
/// the wall clock per cycle. An injected clock returns t0 on its first read and
/// t0+2s on every read after (simulating a 2s activity between cycle 1 and cycle
/// 2). With the per-cycle refresh, the timer armed in cycle 2 anchors to t0+2s
/// (deadline t0+3s); with the pre-fix single read it would anchor to t0 (deadline
/// t0+1s) and fire immediately on the very next poll.
#[tokio::test]
async fn public_run_until_blocked_refreshes_the_clock_per_cycle() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register(&mut rt);
    let exec = rt.start_workflow("activity_then_timer", json!(7)).unwrap();

    // Advancing clock: read #1 (the activity cycle) returns t0; every later read
    // (the arm cycle onward) returns t0 + 2s. A once-captured `Utc::now()` (the
    // pre-fix behavior) would collapse this to a single t0 for the whole call.
    let t0: DateTime<Utc> = DateTime::from_timestamp(6_000_000, 0).unwrap();
    let two_s_later = t0 + chrono::Duration::seconds(2);
    let reads = Arc::new(AtomicUsize::new(0));
    let reads_for_clock = Arc::clone(&reads);
    rt.set_clock(move || {
        if reads_for_clock.fetch_add(1, Ordering::SeqCst) == 0 {
            t0
        } else {
            two_s_later
        }
    });

    // One public drive to the timer block. Internally: cycle 1 (now=t0) runs the
    // activity; cycle 2 (now=t0+2s) arms the timer; cycle 3 (now=t0+2s) finds it
    // not yet due and blocks.
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "must block on the armed timer, got {state:?}"
    );
    assert!(
        reads.load(Ordering::SeqCst) >= 2,
        "the public driver must read the clock more than once (per cycle), read it {} time(s)",
        reads.load(Ordering::SeqCst)
    );

    // THE assertion that distinguishes fixed from buggy: the timer is anchored to
    // its arm-cycle clock (t0+2s + 1s = t0+3s), NOT the call-start clock (which
    // pre-fix would give t0+1s).
    assert_eq!(
        timer_fire_at(&path, exec),
        two_s_later.timestamp_millis() + 1000,
        "the timer's deadline must reflect the per-cycle (arm-instant) clock, not the call start"
    );

    // Direct manifestation of the bug: a poll at the arm instant (t0+2s) must find
    // the timer still pending. Pre-fix, `fire_at` would be t0+1s — already overdue
    // at t0+2s — so this poll would fire it immediately and complete the run ~1s
    // early. (The injected clock is anchored to the same t0 epoch as this
    // `_as_of` instant, so the comparison is coherent.)
    let state = rt.run_until_blocked_as_of(exec, two_s_later).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "timer must NOT be overdue at its own arm instant — pre-fix it would fire immediately, \
         got {state:?}"
    );

    // And it still fires at its true deadline (t0+3s).
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(3))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "done"),
        "timer fires at its true deadline, got {state:?}"
    );
}
