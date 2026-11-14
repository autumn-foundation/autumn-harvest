//! Regression tests for the public drivers re-reading the wall clock **once per
//! decision cycle** (`run_until_blocked` / `poll_once`, issue #1069).
//!
//! The public `run_until_blocked` once captured `Utc::now()` a single time. It
//! reused that timestamp for every internal `drive_one_cycle`. A cycle can use real
//! time, for example a 2-second activity body before a timer arms in a later cycle.
//! That timer's absolute deadline then counted from the START of the outer call, not
//! from its arm instant. A `timer(1s)` after a 2s activity stored
//! `fire_at = call_start + 1s`. That was already ~1s in the past when it armed. So
//! it fired on the next poll instead of ~1s after it armed. The public drivers now
//! re-read the clock at the start of each cycle. The `_as_of` seams keep a
//! caller-fixed timestamp for deterministic simulation.
//!
//! These tests are fully deterministic and never sleep. One drives cycle by cycle
//! through the `_as_of` seam with manually advanced timestamps. The other injects an
//! advancing clock (`set_clock`). It proves that the *public* `run_until_blocked`
//! path refreshes per cycle.

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
/// The activity comes first, so in a real drive the timer arms in a *later* cycle
/// than the one the outer call started in.
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

/// Concept lock through the deterministic `_as_of` seam, with no clock injection.
/// The test drives cycle by cycle and advances the timestamp ~2s during the
/// activity. The timer armed in the LATER cycle anchors its deadline to that
/// cycle's `now`, never to the call-start instant. This locks the
/// `drive_one_cycle` arm behavior that the public per-cycle refresh depends on.
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

    // At the arm instant (t0+2s) the true deadline (t0+3s) has NOT passed. The run
    // stays blocked on the timer and must not fire ~1s early.
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

/// Proof that the PUBLIC `run_until_blocked` re-reads the wall clock per cycle.
/// An injected clock returns t0 on its first read and t0+2s on every later read.
/// That simulates a 2s activity between cycle 1 and cycle 2. With the per-cycle
/// refresh, the timer armed in cycle 2 anchors to t0+2s (deadline t0+3s). A single
/// read would anchor it to t0 (deadline t0+1s), and it would fire on the next poll.
#[tokio::test]
async fn public_run_until_blocked_refreshes_the_clock_per_cycle() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register(&mut rt);
    let exec = rt.start_workflow("activity_then_timer", json!(7)).unwrap();

    // Advancing clock: read #1 (the activity cycle) returns t0. Every later read
    // returns t0 + 2s. A once-captured `Utc::now()` would see t0 for the whole call.
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

    // THE assertion that tells fixed from buggy. The timer anchors to its arm-cycle
    // clock (t0+2s + 1s = t0+3s), NOT the call-start clock (t0+1s).
    assert_eq!(
        timer_fire_at(&path, exec),
        two_s_later.timestamp_millis() + 1000,
        "the timer's deadline must reflect the per-cycle (arm-instant) clock, not the call start"
    );

    // A poll at the arm instant (t0+2s) must find the timer still pending. With the
    // bug, `fire_at` is t0+1s, already overdue at t0+2s. This poll would fire it and
    // complete the run ~1s early. The injected clock and this `_as_of` instant share
    // the t0 epoch, so the comparison is coherent.
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

/// Read the clock, then wait one second on a timer, then read it again.
#[workflow]
async fn two_clock_reads(ctx: &WorkflowContext, _n: i64) -> Result<Vec<i64>, String> {
    let first = ctx.system_now().timestamp_millis();
    ctx.timer("pause", 1).await.map_err(|e| e.to_string())?;
    Ok(vec![first, ctx.system_now().timestamp_millis()])
}

// A clock read takes the clock of its cycle, as the timers do (issue #2276).
// An `_as_of` drive therefore keeps one clock. Before the fix, `system_now`
// read the wall clock. A durable agent then compared a deadline with the wall
// clock and saw it pass before the drive clock reached it.
#[tokio::test]
async fn a_clock_read_takes_the_clock_of_its_cycle() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&two_clock_reads_info());
    let exec = rt.start_workflow("two_clock_reads", json!(0)).unwrap();
    let t0: DateTime<Utc> = "2031-02-03T04:05:06Z".parse().unwrap();
    let t1 = t0 + chrono::Duration::seconds(5);
    assert!(matches!(
        rt.run_until_blocked_as_of(exec, t0).await.unwrap(),
        RunState::WaitingTimer
    ));
    let state = rt.run_until_blocked_as_of(exec, t1).await.unwrap();
    let RunState::Completed(output) = state else {
        panic!("the run completes after its timer: {state:?}");
    };
    assert_eq!(
        output,
        json!([t0.timestamp_millis(), t1.timestamp_millis()]),
        "each read returns the clock of the cycle that made it"
    );
}
