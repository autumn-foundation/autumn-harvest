//! Issue #2471: the schedule controls that the SQLite plugin serves.
//!
//! The plugin turns the scheduler off for `harvest.scheduler_enabled =
//! false`. Its admin routes trigger a schedule now and read schedules from
//! the read-only connection. The tests use a settable clock, so no test
//! sleeps.

// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]
// A `#[workflow]` must be `async`, even when its body has no `.await`.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use autumn_harvest::policy::{OverlapPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{
    ExecutionFilter, ExecutionSummary, ScheduleTrigger, SqliteError, SqliteReader, SqliteRuntime,
    Stored,
};
use chrono::DateTime;
use serde_json::json;

#[workflow]
async fn tick(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

/// Waits for `go`, so the run stays active.
#[workflow]
async fn hold(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(())
}

/// 2026-01-01T00:00:00Z in epoch milliseconds.
const T0: i64 = 1_767_225_600_000;
const MINUTE: i64 = 60_000;

fn runtime_at(path: &std::path::Path, clock: &Arc<AtomicI64>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).unwrap();
    let clock = clock.clone();
    rt.set_clock(move || DateTime::from_timestamp_millis(clock.load(Ordering::SeqCst)).unwrap());
    rt.register_workflow(&tick_info());
    rt.register_workflow(&hold_info());
    rt
}

fn every_minute(workflow: &str) -> WorkflowSchedule {
    WorkflowSchedule::new(workflow, Schedule::Interval(Duration::from_secs(60)))
        .with_input(json!(5))
}

fn runs(rt: &SqliteRuntime, workflow: &str) -> Vec<ExecutionSummary> {
    rt.list_executions(&ExecutionFilter::new().workflow_name(workflow))
        .unwrap()
        .executions
}

fn workflow_id(row: &ExecutionSummary) -> String {
    match &row.workflow_id {
        Stored::Value(id) => id.clone(),
        other => panic!("no workflow id: {other:?}"),
    }
}

fn next_run_at(rt: &SqliteRuntime, workflow: &str) -> Option<i64> {
    rt.list_schedules()
        .unwrap()
        .into_iter()
        .find(|s| s.workflow_name == workflow)
        .expect("the schedule exists")
        .next_run_at
        .map(|at| at.timestamp_millis())
}

/// `harvest.scheduler_enabled = false` stops every fire. The rows stay, and
/// the slot stays due, so a later enable fires it.
#[tokio::test]
async fn a_disabled_scheduler_fires_no_slot() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("off.db"), &clock);
    rt.set_schedules_enabled(false);
    rt.upsert_schedule(&every_minute("tick")).unwrap();

    clock.store(T0 + MINUTE, Ordering::SeqCst);
    assert!(!rt.poll_once().await.unwrap());
    rt.run_until_idle().await.unwrap();
    assert_eq!(runs(&rt, "tick").len(), 0, "no slot fires");
    assert_eq!(
        next_run_at(&rt, "tick"),
        Some(T0 + MINUTE),
        "the slot stays"
    );

    rt.set_schedules_enabled(true);
    assert!(rt.poll_once().await.unwrap());
    assert_eq!(runs(&rt, "tick").len(), 1);
}

/// A trigger starts one run now. The slot does not move.
#[tokio::test]
async fn a_trigger_starts_a_run_now_and_keeps_the_slot() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("trigger.db"), &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();

    clock.store(T0 + 10_000, Ordering::SeqCst);
    let ScheduleTrigger::Fired {
        exec_id,
        workflow_id: id,
    } = rt.trigger_schedule("tick", false, None).unwrap()
    else {
        panic!("the trigger fires");
    };
    assert!(id.starts_with("manual-tick-"), "{id}");
    let rows = runs(&rt, "tick");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].execution_id(), Some(exec_id));
    assert_eq!(workflow_id(&rows[0]), id);
    assert_eq!(
        next_run_at(&rt, "tick"),
        Some(T0 + MINUTE),
        "the slot stays"
    );

    // Two triggers in one millisecond start two runs. The first run ends
    // first, so `skip` does not count it.
    rt.run_until_idle().await.unwrap();
    let again = rt.trigger_schedule("tick", false, None).unwrap();
    assert!(matches!(again, ScheduleTrigger::Fired { .. }));
    assert_eq!(runs(&rt, "tick").len(), 2);
}

/// A paused schedule triggers only with `force`.
#[tokio::test]
async fn a_paused_schedule_triggers_only_with_force() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("paused.db"), &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    rt.pause_schedule("tick").unwrap();

    assert!(matches!(
        rt.trigger_schedule("tick", false, None).unwrap(),
        ScheduleTrigger::Paused
    ));
    assert_eq!(runs(&rt, "tick").len(), 0);
    assert!(matches!(
        rt.trigger_schedule("tick", true, None).unwrap(),
        ScheduleTrigger::Fired { .. }
    ));
    assert_eq!(runs(&rt, "tick").len(), 1);
    assert!(
        rt.list_schedules().unwrap()[0].paused,
        "a force keeps the pause"
    );
}

/// Under `skip` a trigger counts the active runs. A run the trigger starts
/// counts too, so a slot then drops.
#[tokio::test]
async fn a_trigger_under_skip_honours_the_run_limit() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("skip.db"), &clock);
    rt.upsert_schedule(&every_minute("hold")).unwrap();

    assert!(matches!(
        rt.trigger_schedule("hold", false, None).unwrap(),
        ScheduleTrigger::Fired { .. }
    ));
    rt.run_until_idle().await.unwrap();
    assert!(matches!(
        rt.trigger_schedule("hold", false, None).unwrap(),
        ScheduleTrigger::SkippedOverlap { .. }
    ));
    // A request override of the policy wins.
    assert!(matches!(
        rt.trigger_schedule("hold", false, Some(OverlapPolicy::AllowAll))
            .unwrap(),
        ScheduleTrigger::Fired { .. }
    ));
    assert_eq!(runs(&rt, "hold").len(), 2);

    // The triggered runs count against the next slot.
    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(runs(&rt, "hold").len(), 2, "the slot drops");
}

/// An unknown name and an unsupported policy are refused, and write nothing.
#[tokio::test]
async fn a_trigger_refuses_an_unknown_schedule_and_an_unsupported_policy() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("refuse.db"), &clock);
    assert!(matches!(
        rt.trigger_schedule("tick", false, None),
        Err(SqliteError::ScheduleNotFound(_))
    ));
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    let refused = rt.trigger_schedule("tick", false, Some(OverlapPolicy::BufferOne));
    assert!(
        matches!(&refused, Err(SqliteError::Unsupported(message)) if message.contains("overlap_policy")),
        "{refused:?}"
    );
    assert_eq!(runs(&rt, "tick").len(), 0);
}

/// The read-only connection lists the schedules, with their input and their
/// overlap policy, as the runtime does.
#[tokio::test]
async fn the_reader_lists_schedules_with_input_and_policy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reader.db");
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&path, &clock);
    let mut all = every_minute("hold");
    all.overlap_policy = OverlapPolicy::AllowAll;
    all.max_active_runs = 3;
    rt.upsert_schedule(&all).unwrap();
    rt.upsert_schedule(&every_minute("tick")).unwrap();

    let reader = SqliteReader::open(&path).unwrap();
    let listed = reader.list_schedules().unwrap();
    let names: Vec<&str> = listed.iter().map(|s| s.workflow_name.as_str()).collect();
    assert_eq!(names, ["hold", "tick"]);
    assert_eq!(listed[0].input, json!(5));
    assert_eq!(listed[0].overlap_policy, OverlapPolicy::AllowAll);
    assert_eq!(listed[0].max_active_runs, 3);
    assert_eq!(listed[1].overlap_policy, OverlapPolicy::Skip);
    assert_eq!(
        listed[1].next_run_at.map(|at| at.timestamp_millis()),
        Some(T0 + MINUTE)
    );
    assert!(reader.schedule_override("tick").unwrap().is_none());
}

/// A pause keeps its reason until the resume, as on Postgres.
#[tokio::test]
async fn a_pause_keeps_its_reason_until_the_resume() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("reason.db"), &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    rt.pause_schedule_with_reason("tick", Some("maintenance"))
        .unwrap();
    let paused = &rt.list_schedules().unwrap()[0];
    assert!(paused.paused);
    assert_eq!(paused.pause_reason.as_deref(), Some("maintenance"));

    rt.resume_schedule("tick").unwrap();
    let resumed = &rt.list_schedules().unwrap()[0];
    assert!(!resumed.paused);
    assert_eq!(resumed.pause_reason, None);
}
