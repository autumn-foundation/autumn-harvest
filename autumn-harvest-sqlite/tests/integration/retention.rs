//! Issue #2031: `purge_terminal` removes old terminal runs.
//!
//! A terminal write stores `closed_at` from the real clock. These tests move
//! the runtime clock forward to age a run past the window.

// A `#[workflow]` must be `async`, even when its body has no `.await`.
#![allow(clippy::unused_async)]

use std::time::Duration;

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteError, SqliteRuntime};
use chrono::Utc;
use rusqlite::Connection;
use serde_json::json;

/// One activity, then wait for `go`, so a run has rows in every table.
#[workflow]
async fn busy(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    ctx.timer("nap", 0).await.map_err(|e| e.to_string())?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(n)
}

#[workflow]
async fn echo(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

const HOUR: Duration = Duration::from_secs(3600);

/// The tables that hold rows of a run.
const RUN_TABLES: [&str; 6] = [
    "harvest_executions",
    "harvest_events",
    "harvest_tasks",
    "harvest_timers",
    "harvest_signals",
    "harvest_activity_attempts",
];

fn runtime(path: &std::path::Path) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).unwrap();
    rt.register_workflow(&busy_info());
    rt.register_workflow(&echo_info());
    rt.register_activity_raw("work", ActivitySpec::new(1, Ok));
    rt
}

/// Rows of `exec` in each run table.
fn rows_of(path: &std::path::Path, exec: &str) -> Vec<(&'static str, i64)> {
    let conn = Connection::open(path).unwrap();
    RUN_TABLES
        .iter()
        .map(|table| {
            let n = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE exec_id = ?1"),
                    [exec],
                    |r| r.get(0),
                )
                .unwrap();
            (*table, n)
        })
        .collect()
}

/// Purge with the window counted from two hours on, so each run closed now
/// is past a one-hour window.
fn purge_two_hours_on(rt: &mut SqliteRuntime) -> u64 {
    rt.purge_terminal_as_of(Utc::now() + chrono::Duration::hours(2), HOUR)
        .unwrap()
}

// AC2: purge removes a terminal run past the window, with every row of it,
// and keeps a live run.
#[tokio::test]
async fn purge_removes_terminal_runs_past_the_window_and_keeps_live_ones() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("purge.sqlite3");
    let mut rt = runtime(&path);

    let done = rt.start_workflow("busy", json!(1)).unwrap();
    rt.run_until_blocked(done).await.unwrap();
    rt.send_signal(done, "go", json!(null)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(done).await.unwrap(),
        RunState::Completed(_)
    ));
    let live = rt.start_workflow("busy", json!(2)).unwrap();
    rt.run_until_blocked(live).await.unwrap();
    assert!(
        rows_of(&path, &done.to_string())
            .iter()
            .all(|(_, n)| *n > 0),
        "the finished run has rows in every table first: {:?}",
        rows_of(&path, &done.to_string())
    );

    assert_eq!(purge_two_hours_on(&mut rt), 1);

    for (table, n) in rows_of(&path, &done.to_string()) {
        assert_eq!(n, 0, "{table} keeps no row of the purged run");
    }
    assert!(matches!(
        rt.describe(done),
        Err(SqliteError::ExecutionNotFound(_))
    ));

    rt.send_signal(live, "go", json!(null)).unwrap();
    assert!(
        matches!(
            rt.run_until_blocked(live).await.unwrap(),
            RunState::Completed(_)
        ),
        "the live run still finishes"
    );
    assert_eq!(rt.purge_terminal(HOUR).unwrap(), 0, "it closed just now");
}

// A run closed inside the window stays. The window counts on the real clock,
// as `closed_at` does, so the runtime clock does not move it.
#[tokio::test]
async fn purge_keeps_terminal_runs_inside_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("window.sqlite3");
    let mut rt = runtime(&path);
    let exec = rt.start_workflow("echo", json!(1)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    assert_eq!(rt.purge_terminal(HOUR).unwrap(), 0);
    rt.set_clock(|| Utc::now() + chrono::Duration::days(30));
    assert_eq!(
        rt.purge_terminal(HOUR).unwrap(),
        0,
        "the runtime clock is not used"
    );
    rt.describe(exec).expect("the run stays");
    assert_eq!(rt.purge_terminal(Duration::MAX).unwrap(), 0);
}

// A RUNNING row is never a candidate, even with a close time.
#[tokio::test]
async fn purge_never_removes_a_running_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("running.sqlite3");
    let mut rt = runtime(&path);
    let live = rt.start_workflow("busy", json!(1)).unwrap();
    rt.run_until_blocked(live).await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute("UPDATE harvest_executions SET closed_at = 1", [])
        .unwrap();

    assert_eq!(purge_two_hours_on(&mut rt), 0);
    rt.describe(live).expect("the running row stays");
}

// Only the close time counts, as on Postgres. The old run of a chain goes,
// even while the newest run of the chain still runs.
#[tokio::test]
async fn purge_removes_an_old_run_of_a_live_chain() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prior.sqlite3");
    let mut rt = runtime(&path);
    let old = rt.start_workflow_with_id("busy", "k", json!(1)).unwrap();
    rt.run_until_blocked(old).await.unwrap();
    let new = rt
        .start_workflow_with_reuse_policy(
            "busy",
            "k",
            json!(2),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap();
    rt.run_until_blocked(new.exec_id).await.unwrap();

    assert_eq!(purge_two_hours_on(&mut rt), 1);
    assert!(matches!(
        rt.describe(old),
        Err(SqliteError::ExecutionNotFound(_))
    ));
    rt.send_signal(new.exec_id, "go", json!(null)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(new.exec_id).await.unwrap(),
        RunState::Completed(_)
    ));
}

// A row closed before issue #2027 has no `closed_at`. Its age is unknown, so
// it stays.
#[tokio::test]
async fn purge_keeps_a_row_with_no_close_time() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite3");
    let mut rt = runtime(&path);
    let exec = rt.start_workflow("echo", json!(1)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute("UPDATE harvest_executions SET closed_at = NULL", [])
        .unwrap();

    assert_eq!(purge_two_hours_on(&mut rt), 0);
}

// A damaged id cannot stall retention. The purge skips it and removes the
// rest, and a second purge ends at once.
#[tokio::test]
async fn purge_skips_a_damaged_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("damaged.sqlite3");
    let mut rt = runtime(&path);
    let exec = rt.start_workflow("echo", json!(1)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute(
            "INSERT INTO harvest_executions (exec_id, workflow_name, input_json, state, closed_at) \
             VALUES (x'00ff', 'echo', '1', 'COMPLETED', 1)",
            [],
        )
        .unwrap();

    assert_eq!(purge_two_hours_on(&mut rt), 1);
    assert_eq!(purge_two_hours_on(&mut rt), 0);
}

// More runs than one batch all go, and the count is exact.
#[tokio::test]
async fn purge_removes_more_runs_than_one_batch() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&echo_info());
    for n in 0..600 {
        rt.start_workflow("echo", json!(n)).unwrap();
    }
    rt.run_until_idle().await.unwrap();

    assert_eq!(purge_two_hours_on(&mut rt), 600);
    assert_eq!(purge_two_hours_on(&mut rt), 0);
}
