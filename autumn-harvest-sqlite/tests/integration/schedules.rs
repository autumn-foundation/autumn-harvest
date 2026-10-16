//! Issue #2031: cron and interval schedules on the `SQLite` backend.
//!
//! A schedule is a row in `harvest_schedules`. Each `poll_once` pass fires
//! the due schedules, then drives the fleet. The tests use a settable clock,
//! so no test sleeps.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use autumn_harvest::policy::{CatchupPolicy, OverlapPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{
    ExecutionFilter, ExecutionSummary, SqliteError, SqliteRuntime, Stored,
};
use chrono::{DateTime, Utc};
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

/// Phase 0 continues as new at once. Phase 1 waits for `go`.
#[workflow]
async fn hand_off(ctx: &WorkflowContext, phase: i64) -> Result<(), String> {
    if phase == 0 {
        ctx.continue_as_new(json!(1))
            .await
            .map_err(|e| e.to_string())?;
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(())
}

/// 2026-01-01T00:00:00Z in epoch milliseconds.
const T0: i64 = 1_767_225_600_000;
const MINUTE: i64 = 60_000;

/// A runtime whose clock reads `clock`, with every test workflow registered.
fn runtime_at(path: &std::path::Path, clock: &Arc<AtomicI64>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).unwrap();
    let clock = clock.clone();
    rt.set_clock(move || DateTime::from_timestamp_millis(clock.load(Ordering::SeqCst)).unwrap());
    rt.register_workflow(&tick_info());
    rt.register_workflow(&hold_info());
    rt.register_workflow(&hand_off_info());
    rt
}

fn every_minute(workflow: &str) -> WorkflowSchedule {
    WorkflowSchedule::new(workflow, Schedule::Interval(Duration::from_secs(60)))
        .with_input(json!(5))
}

/// The runs of `workflow`, oldest first.
fn runs(rt: &SqliteRuntime, workflow: &str) -> Vec<ExecutionSummary> {
    let mut rows = rt
        .list_executions(&ExecutionFilter::new().workflow_name(workflow))
        .unwrap()
        .executions;
    rows.reverse();
    rows
}

fn started_at(row: &ExecutionSummary) -> i64 {
    match &row.started_at {
        Stored::Value(at) => at.timestamp_millis(),
        other => panic!("no start time: {other:?}"),
    }
}

fn state(row: &ExecutionSummary) -> String {
    match &row.state {
        Stored::Value(state) => state.clone(),
        other => panic!("no state: {other:?}"),
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

// AC1: an interval schedule starts runs on time across a reopen. The app
// upserts its schedule on every open, so the upsert must keep the slot.
#[tokio::test]
async fn an_interval_schedule_starts_runs_on_time_across_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sched.sqlite3");
    let clock = Arc::new(AtomicI64::new(T0));

    let mut rt = runtime_at(&path, &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + MINUTE));
    drop(rt);

    // Reopen just before the slot. Nothing fires.
    clock.store(T0 + MINUTE - 1, Ordering::SeqCst);
    let mut rt = runtime_at(&path, &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + MINUTE), "kept");
    assert!(!rt.poll_once().await.unwrap());
    assert!(runs(&rt, "tick").is_empty());

    // At the slot, one run starts and completes in the same pass.
    clock.store(T0 + MINUTE, Ordering::SeqCst);
    assert!(rt.poll_once().await.unwrap());
    let first = runs(&rt, "tick");
    assert_eq!(first.len(), 1);
    assert_eq!(started_at(&first[0]), T0 + MINUTE, "on time");
    assert_eq!(state(&first[0]), "COMPLETED");
    let Stored::Value(id) = &first[0].workflow_id else {
        panic!("no workflow id")
    };
    assert_eq!(id, &format!("sched:tick:{}", (T0 + MINUTE) / 1000));
    drop(rt);

    // Reopen at the next slot. The second run starts on time.
    clock.store(T0 + 2 * MINUTE, Ordering::SeqCst);
    let mut rt = runtime_at(&path, &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    rt.poll_once().await.unwrap();
    let both = runs(&rt, "tick");
    assert_eq!(both.len(), 2);
    assert_eq!(started_at(&both[1]), T0 + 2 * MINUTE);
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + 3 * MINUTE));

    // A second pass in the same slot starts nothing.
    clock.store(T0 + 2 * MINUTE + 59_000, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(runs(&rt, "tick").len(), 2);
}

// A changed spec takes effect at once. Its next slot counts from now.
#[tokio::test]
async fn a_changed_spec_resets_the_next_slot() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("s.db"), &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    clock.store(T0 + 10_000, Ordering::SeqCst);
    rt.upsert_schedule(&WorkflowSchedule::new(
        "tick",
        Schedule::Interval(Duration::from_secs(300)),
    ))
    .unwrap();
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + 10_000 + 5 * MINUTE));
}

// A cron schedule fires at its cron slot.
#[tokio::test]
async fn a_cron_schedule_fires_at_its_slot() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0 + 1_000));
    let mut rt = runtime_at(&dir.path().join("cron.db"), &clock);
    rt.upsert_schedule(&WorkflowSchedule::new(
        "tick",
        Schedule::Cron("*/5 * * * *".to_string()),
    ))
    .unwrap();
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + 5 * MINUTE));

    clock.store(T0 + 5 * MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(runs(&rt, "tick").len(), 1);
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + 10 * MINUTE));
}

// After a long gap, catch-up off fires one run, then realigns to now.
#[tokio::test]
async fn a_missed_gap_fires_once_then_realigns() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("gap.db"), &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();

    clock.store(T0 + 10 * MINUTE + 30_000, Ordering::SeqCst);
    rt.run_until_idle().await.unwrap();
    assert_eq!(runs(&rt, "tick").len(), 1, "one run, not ten");
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + 11 * MINUTE + 30_000));
}

// `Skip` drops a slot while the last run is active. The slot still moves on.
#[tokio::test]
async fn skip_drops_a_slot_while_a_run_is_active() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("skip.db"), &clock);
    rt.upsert_schedule(&every_minute("hold")).unwrap();

    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    clock.store(T0 + 2 * MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    let active = runs(&rt, "hold");
    assert_eq!(active.len(), 1, "the second slot is dropped");
    assert_eq!(next_run_at(&rt, "hold"), Some(T0 + 3 * MINUTE));

    rt.send_signal(active[0].execution_id().unwrap(), "go", json!(null))
        .unwrap();
    rt.poll_once().await.unwrap();
    clock.store(T0 + 3 * MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(runs(&rt, "hold").len(), 2, "a slot after the end fires");
}

// A continue-as-new successor is still the schedule's run, as on Postgres.
#[tokio::test]
async fn skip_counts_a_continue_as_new_successor() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("can.db"), &clock);
    rt.upsert_schedule(&every_minute("hand_off").with_input(json!(0)))
        .unwrap();

    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.run_until_idle().await.unwrap();
    clock.store(T0 + 2 * MINUTE, Ordering::SeqCst);
    rt.run_until_idle().await.unwrap();
    let states: Vec<String> = runs(&rt, "hand_off").iter().map(state).collect();
    assert_eq!(
        states,
        vec!["CONTINUED_AS_NEW".to_string(), "RUNNING".to_string()],
        "the successor blocks the next slot"
    );
}

// `AllowAll` starts a run for each slot.
#[tokio::test]
async fn allow_all_starts_a_run_per_slot() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("all.db"), &clock);
    rt.upsert_schedule(&every_minute("hold").with_overlap_policy(OverlapPolicy::AllowAll))
        .unwrap();
    for slot in 1..=3 {
        clock.store(T0 + slot * MINUTE, Ordering::SeqCst);
        rt.poll_once().await.unwrap();
    }
    assert_eq!(runs(&rt, "hold").len(), 3);
}

// A paused schedule does not fire. A resume counts its next slot from now.
#[tokio::test]
async fn a_paused_schedule_does_not_fire() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pause.db");
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&path, &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    rt.pause_schedule("tick").unwrap();
    drop(rt);

    clock.store(T0 + 5 * MINUTE, Ordering::SeqCst);
    let mut rt = runtime_at(&path, &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    rt.poll_once().await.unwrap();
    assert!(runs(&rt, "tick").is_empty(), "a re-upsert keeps the pause");
    assert!(rt.list_schedules().unwrap()[0].paused);

    rt.resume_schedule("tick").unwrap();
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + 6 * MINUTE));
    rt.poll_once().await.unwrap();
    assert!(runs(&rt, "tick").is_empty(), "no burst on resume");
}

// A deleted schedule stops firing. Its run history stays.
#[tokio::test]
async fn delete_stops_a_schedule() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("del.db"), &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();

    rt.delete_schedule("tick").unwrap();
    assert!(rt.list_schedules().unwrap().is_empty());
    clock.store(T0 + 2 * MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(runs(&rt, "tick").len(), 1);
    assert!(matches!(
        rt.delete_schedule("tick"),
        Err(SqliteError::ScheduleNotFound(_))
    ));
    assert!(matches!(
        rt.pause_schedule("tick"),
        Err(SqliteError::ScheduleNotFound(_))
    ));
}

// A slot whose workflow is not registered stays due, and the pass says why.
#[tokio::test]
async fn an_unregistered_workflow_keeps_its_slot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unreg.db");
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&path, &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    drop(rt);

    clock.store(T0 + MINUTE, Ordering::SeqCst);
    let mut bare = SqliteRuntime::open(&path).unwrap();
    let clock2 = clock.clone();
    bare.set_clock(move || DateTime::from_timestamp_millis(clock2.load(Ordering::SeqCst)).unwrap());
    assert!(matches!(
        bare.poll_once().await,
        Err(SqliteError::UnknownWorkflow(name)) if name == "tick"
    ));
    drop(bare);

    let mut rt = runtime_at(&path, &clock);
    rt.poll_once().await.unwrap();
    assert_eq!(runs(&rt, "tick").len(), 1, "the slot fires once registered");
}

// Each field this backend cannot honor is rejected by name. A bad spec and
// an unknown workflow are rejected too.
#[tokio::test]
async fn unsupported_fields_are_rejected_by_name() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&tick_info());
    let base = || every_minute("tick");
    let mut cases: Vec<(&str, WorkflowSchedule)> = vec![
        ("catchup", {
            let mut s = base();
            s.catchup = true;
            s
        }),
        (
            "catchup_policy",
            base().with_catchup_policy(CatchupPolicy::MostRecent),
        ),
        (
            "overlap_policy",
            base().with_overlap_policy(OverlapPolicy::BufferOne),
        ),
        ("calendar", {
            let mut s = base();
            s.calendar = Some("us-federal-holidays".to_string());
            s
        }),
        ("end_at", {
            let mut s = base();
            s.end_at = Some(Utc::now());
            s
        }),
        ("max_runs", {
            let mut s = base();
            s.max_runs = Some(3);
            s
        }),
        ("queue_name", base().with_queue_name("other")),
    ];
    for (field, schedule) in cases.drain(..) {
        let err = rt.upsert_schedule(&schedule).unwrap_err();
        assert!(
            matches!(&err, SqliteError::Unsupported(m) if m.starts_with(&format!("WorkflowSchedule.{field}"))),
            "{field}: {err}"
        );
    }
    assert!(rt.list_schedules().unwrap().is_empty(), "nothing is stored");

    let ok = base().with_catchup_policy(CatchupPolicy::SkipAll);
    rt.upsert_schedule(&ok).expect("catch-up off is supported");

    let bad = WorkflowSchedule::new("tick", Schedule::Cron("not a cron".to_string()));
    assert!(matches!(
        rt.upsert_schedule(&bad),
        Err(SqliteError::InvalidSchedule(_))
    ));
    assert!(matches!(
        rt.upsert_schedule(&every_minute("nope")),
        Err(SqliteError::UnknownWorkflow(_))
    ));
}

// A manual schedule never fires on its own.
#[tokio::test]
async fn a_manual_schedule_never_fires() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("manual.db"), &clock);
    rt.upsert_schedule(&WorkflowSchedule::new("tick", Schedule::Manual))
        .unwrap();
    assert_eq!(next_run_at(&rt, "tick"), None);
    clock.store(T0 + 1_000 * MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert!(runs(&rt, "tick").is_empty());
}

// The `_as_of` pass fires at its own time.
#[tokio::test]
async fn poll_once_as_of_fires_due_schedules() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("asof.db"), &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    let at = DateTime::<Utc>::from_timestamp_millis(T0 + MINUTE).unwrap();
    assert!(rt.poll_once_as_of(at).await.unwrap());
    assert_eq!(runs(&rt, "tick").len(), 1);
}
