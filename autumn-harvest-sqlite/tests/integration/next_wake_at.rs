//! `SqliteRuntime::next_wake_at`: the earliest time at which a drive has due
//! work.
//!
//! A fleet host opens a closed tenant at this time. The tests use a settable
//! clock, so no test sleeps.

// The `#[workflow]` and `#[activity]` macros read the unused input through
// their expansion.
#![allow(clippy::used_underscore_binding)]
// A `#[workflow]` must be `async`, even when its body has no `.await`.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use autumn_harvest::policy::{RetryPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::SqliteRuntime;
use chrono::DateTime;
use serde_json::json;

/// 2026-01-01T00:00:00Z in epoch milliseconds.
const T0: i64 = 1_767_225_600_000;
const SECOND: i64 = 1_000;
const MINUTE: i64 = 60 * SECOND;

/// Sleeps on a durable timer of `secs` seconds.
#[workflow]
async fn napper(ctx: &WorkflowContext, secs: u64) -> Result<(), String> {
    ctx.timer("nap", secs).await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Waits for `go`, so the run stays active with no deadline.
#[workflow]
async fn waiter(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Each attempt fails. A retry waits one minute.
#[activity(retry = RetryPolicy::fixed(3, Duration::from_secs(60)))]
async fn always_fails(_ctx: &ActivityContext, _n: i64) -> Result<i64, String> {
    Err("transient".to_owned())
}

#[workflow]
async fn retrier(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_activity(&always_fails_info(), n)
        .await
        .map_err(|e| e.to_string())
}

/// Waits for `go` with a 30-second execution timeout.
#[workflow(execution_timeout = "30s")]
async fn bounded(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(())
}

/// A runtime whose clock reads `clock`, with each test workflow registered.
fn runtime_at(path: &std::path::Path, clock: &Arc<AtomicI64>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).unwrap();
    let clock = clock.clone();
    rt.set_clock(move || DateTime::from_timestamp_millis(clock.load(Ordering::SeqCst)).unwrap());
    rt.register_workflow(&napper_info());
    rt.register_workflow(&waiter_info());
    rt.register_workflow(&retrier_info());
    rt.register_workflow(&bounded_info());
    rt.register_activity(&always_fails_info());
    rt
}

fn fixture() -> (tempfile::TempDir, Arc<AtomicI64>, SqliteRuntime) {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let rt = runtime_at(&dir.path().join("h.db"), &clock);
    (dir, clock, rt)
}

fn every_minute(workflow: &str) -> WorkflowSchedule {
    WorkflowSchedule::new(workflow, Schedule::Interval(Duration::from_secs(60)))
}

/// A file with no run and no schedule has no deadline.
#[tokio::test]
async fn an_empty_file_has_no_deadline() {
    let (_dir, _clock, rt) = fixture();
    assert_eq!(rt.next_wake_at().unwrap(), None);
}

/// A run that waits only for a signal has no deadline.
#[tokio::test]
async fn a_signal_wait_has_no_deadline() {
    let (_dir, _clock, mut rt) = fixture();
    rt.start_workflow("waiter", json!(0)).unwrap();
    rt.poll_once().await.unwrap();
    assert_eq!(rt.next_wake_at().unwrap(), None);
}

/// An unfired timer gives its `fire_at`. The earliest of two wins.
#[tokio::test]
async fn an_unfired_timer_gives_its_deadline() {
    let (_dir, clock, mut rt) = fixture();
    rt.start_workflow("napper", json!(5)).unwrap();
    rt.start_workflow("napper", json!(90)).unwrap();
    rt.poll_once().await.unwrap();
    assert_eq!(rt.next_wake_at().unwrap(), Some(T0 + 5 * SECOND));

    // The first timer fires. The second is next.
    clock.store(T0 + 5 * SECOND, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(rt.next_wake_at().unwrap(), Some(T0 + 90 * SECOND));

    // Both fired. No deadline is left.
    clock.store(T0 + 90 * SECOND, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(rt.next_wake_at().unwrap(), None);
}

/// A timer of a run that is no longer running is not a deadline.
#[tokio::test]
async fn a_timer_of_a_terminated_run_is_not_a_deadline() {
    let (_dir, _clock, mut rt) = fixture();
    let exec = rt.start_workflow("napper", json!(5)).unwrap();
    rt.poll_once().await.unwrap();
    rt.terminate(exec, "operator").unwrap();
    assert_eq!(rt.next_wake_at().unwrap(), None);
}

/// An unpaused schedule gives its next slot.
#[tokio::test]
async fn a_schedule_gives_its_next_slot() {
    let (_dir, _clock, mut rt) = fixture();
    rt.upsert_schedule(&every_minute("waiter")).unwrap();
    let slot = rt.list_schedules().unwrap()[0]
        .next_run_at
        .expect("a slot")
        .timestamp_millis();
    assert!(slot > T0 && slot <= T0 + MINUTE, "{slot}");
    assert_eq!(rt.next_wake_at().unwrap(), Some(slot));
}

/// A paused schedule gives no deadline. A resume gives it back.
#[tokio::test]
async fn a_paused_schedule_gives_no_deadline() {
    let (_dir, _clock, mut rt) = fixture();
    rt.upsert_schedule(&every_minute("waiter")).unwrap();
    rt.pause_schedule("waiter").unwrap();
    assert_eq!(rt.next_wake_at().unwrap(), None);
    rt.resume_schedule("waiter").unwrap();
    assert!(rt.next_wake_at().unwrap().is_some());
}

/// A disabled scheduler fires no slot, so a slot is no deadline.
#[tokio::test]
async fn a_disabled_scheduler_gives_no_schedule_deadline() {
    let (_dir, _clock, mut rt) = fixture();
    rt.upsert_schedule(&every_minute("waiter")).unwrap();
    rt.set_schedules_enabled(false);
    assert_eq!(rt.next_wake_at().unwrap(), None);
}

/// The earliest of a timer and a schedule slot wins.
#[tokio::test]
async fn the_earliest_source_wins() {
    let (_dir, _clock, mut rt) = fixture();
    rt.upsert_schedule(&every_minute("waiter")).unwrap();
    rt.start_workflow("napper", json!(5)).unwrap();
    rt.poll_once().await.unwrap();
    assert_eq!(rt.next_wake_at().unwrap(), Some(T0 + 5 * SECOND));
}

/// An activity retry backoff is a deadline.
#[tokio::test]
async fn an_activity_retry_backoff_is_a_deadline() {
    let (_dir, _clock, mut rt) = fixture();
    rt.start_workflow("retrier", json!(1)).unwrap();
    rt.poll_once().await.unwrap();
    assert_eq!(rt.next_wake_at().unwrap(), Some(T0 + MINUTE));
}

/// A run deadline is a deadline: the drive at that time ends the run.
#[tokio::test]
async fn an_execution_timeout_is_a_deadline() {
    let (_dir, clock, mut rt) = fixture();
    rt.start_workflow("bounded", json!(0)).unwrap();
    rt.poll_once().await.unwrap();
    assert_eq!(rt.next_wake_at().unwrap(), Some(T0 + 30 * SECOND));

    clock.store(T0 + 30 * SECOND, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(rt.next_wake_at().unwrap(), None, "the run timed out");
}
