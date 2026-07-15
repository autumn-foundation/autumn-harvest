//! FIX 2 (Codex #1069 P2, runtime.rs:686): durable timer deadlines are stored at
//! MILLISECOND precision, so a timer never fires before its true sub-second
//! deadline.
//!
//! Before the fix the drivers passed `now.timestamp()` (whole seconds) into the
//! arm calculation, flooring the arm instant before adding `duration_secs`. A
//! timer armed at `…000.900` with duration `1s` stored `fire_at = …001` (whole
//! second), so a poll at `…001.001` fired it after only ~100 ms — ~900 ms EARLY —
//! violating "a timer never resolves before its deadline". Milliseconds
//! throughout (`now.timestamp_millis()` at the seam + `duration_secs * 1000` in
//! the arm calc) preserves the sub-second deadline; every internal epoch value
//! (`fire_at`, `received_at`, `run_at`) is a relative epoch-millisecond, so the
//! comparison stays self-consistent with no DDL change.

#![allow(clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

fn temp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

/// Raw `fire_at` of the single armed timer, read from a second connection to the
/// same file (the established inspection pattern).
fn timer_fire_at(path: &std::path::Path, exec: ExecutionId) -> i64 {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT fire_at FROM harvest_timers WHERE exec_id = ?1",
        [exec.to_string()],
        |r| r.get(0),
    )
    .unwrap()
}

/// Arm a 1-second durable timer, then complete.
#[workflow]
async fn one_second_timer(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    ctx.timer("t", 1).await.map_err(|e| e.to_string())?;
    Ok("fired".to_string())
}

#[tokio::test]
async fn timer_armed_at_fractional_second_does_not_fire_early() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&one_second_timer_info());

    // Arm at t0 = 3_000_000.900 s. The TRUE 1-second deadline is 3_000_001.900 s.
    let t0: DateTime<Utc> = DateTime::from_timestamp(3_000_000, 900_000_000).unwrap();
    let exec = rt.start_workflow("one_second_timer", json!(0)).unwrap();

    // Drive 1 (at t0): arms the timer and blocks.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "must block on the armed timer, got {state:?}"
    );

    // The stored deadline reflects the SUB-SECOND arm instant: t0 + 1000 ms, NOT
    // the floored `floor(t0) + 1` whole second (pre-fix that stored 3_000_001).
    assert_eq!(
        timer_fire_at(&path, exec),
        t0.timestamp_millis() + 1000,
        "fire_at must be the true sub-second deadline (now_ms + 1000), not a floored second"
    );

    // Drive 2 at t0 + 100 ms (= 3_000_001.000 s). This is BEFORE the true deadline
    // (3_000_001.900 s), so the timer must NOT fire — the run stays WaitingTimer.
    // Pre-fix (whole-second flooring) the deadline was 3_000_001 s and this poll —
    // at 3_000_001 s — fired it ~900 ms early, completing the run.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::milliseconds(100))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "the timer must NOT fire ~100 ms after a fractional-second arm — it is ~900 ms \
         before the true 1s deadline, got {state:?}"
    );

    // Drive 3 at t0 + 1 s (= 3_000_001.900 s) — exactly the true deadline. Now it
    // fires and the run completes.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "fired"),
        "the timer fires at (and only at) its true sub-second deadline, got {state:?}"
    );
}

// The precision change is uniform, so a whole-second arm (the common case, used
// by every other timer test) still fires exactly at its whole-second deadline —
// no regression for integer durations.
#[tokio::test]
async fn whole_second_timer_still_fires_at_its_deadline() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&one_second_timer_info());

    let t0: DateTime<Utc> = DateTime::from_timestamp(4_000_000, 0).unwrap();
    let exec = rt.start_workflow("one_second_timer", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    assert_eq!(
        timer_fire_at(&path, exec),
        t0.timestamp_millis() + 1000,
        "a whole-second arm stores a whole-second-millis deadline"
    );

    // Just before the deadline: not fired.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::milliseconds(999))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "must not fire 1 ms before the deadline, got {state:?}"
    );

    // At the deadline: fires.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "fired"),
        "got {state:?}"
    );
}
