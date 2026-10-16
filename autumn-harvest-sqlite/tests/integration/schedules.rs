//! Issue #2031: cron and interval schedules on the `SQLite` backend.
//!
//! A schedule is a row in `harvest_schedules`. Each `poll_once` pass fires
//! the due schedules, then drives the fleet. The tests use a settable clock,
//! so no test sleeps.

// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]
// A `#[workflow]` must be `async`, even when its body has no `.await`.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use autumn_harvest::policy::{
    CatchupPolicy, JitterPolicy, OverlapPolicy, RetryPolicy, Schedule, WorkflowSchedule,
};
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

/// Fails at once. Its retry waits ten minutes, so it stays `RUNNING`.
#[workflow(retry = RetryPolicy {
    max_attempts: 2,
    initial_interval: Duration::from_secs(600),
    backoff_coefficient: 1.0,
    max_interval: Duration::from_secs(600),
    non_retryable_errors: Vec::new(),
    jitter: JitterPolicy::None,
})]
async fn fail_then_retry(_ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    Err("boom".to_string())
}

/// Waits for `go` with a 30-second execution timeout.
#[workflow(execution_timeout = "30s")]
async fn short_lived(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(())
}

/// 2026-01-01T00:00:00Z in epoch milliseconds.
const T0: i64 = 1_767_225_600_000;
const MINUTE: i64 = 60_000;

/// Make the clock of `rt` read `clock`, in epoch milliseconds.
fn follow(rt: &mut SqliteRuntime, clock: &Arc<AtomicI64>) {
    let clock = clock.clone();
    rt.set_clock(move || DateTime::from_timestamp_millis(clock.load(Ordering::SeqCst)).unwrap());
}

/// A runtime whose clock reads `clock`, with every test workflow registered.
fn runtime_at(path: &std::path::Path, clock: &Arc<AtomicI64>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).unwrap();
    follow(&mut rt, clock);
    rt.register_workflow(&tick_info());
    rt.register_workflow(&hold_info());
    rt.register_workflow(&hand_off_info());
    rt.register_workflow(&fail_then_retry_info());
    rt.register_workflow(&short_lived_info());
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
    assert_eq!(runs(&rt, "tick").len(), 0);

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
    assert_eq!(runs(&rt, "tick").len(), 0, "a re-upsert keeps the pause");
    assert!(rt.list_schedules().unwrap()[0].paused);

    rt.resume_schedule("tick").unwrap();
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + 6 * MINUTE));
    clock.store(T0 + 6 * MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(
        runs(&rt, "tick").len(),
        1,
        "one run, not one per missed slot"
    );
}

// A new schedule upserted paused stays paused.
#[tokio::test]
async fn a_new_schedule_can_start_paused() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("paused.db"), &clock);
    let mut paused = every_minute("tick");
    paused.paused = true;
    rt.upsert_schedule(&paused).unwrap();
    assert!(rt.list_schedules().unwrap()[0].paused);
    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(runs(&rt, "tick").len(), 0);
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

// A slot whose workflow is not registered stays due. It logs a warning and
// does not fail the pass, so the other schedules still fire.
#[tokio::test]
async fn an_unregistered_workflow_keeps_its_slot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unreg.db");
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&path, &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    rt.upsert_schedule(&every_minute("hold")).unwrap();
    drop(rt);

    clock.store(T0 + MINUTE, Ordering::SeqCst);
    let mut bare = SqliteRuntime::open(&path).unwrap();
    follow(&mut bare, &clock);
    bare.register_workflow(&hold_info());
    assert!(bare.poll_once().await.unwrap(), "the hold slot fires");
    assert_eq!(runs(&bare, "hold").len(), 1);
    assert_eq!(next_run_at(&bare, "tick"), Some(T0 + MINUTE), "still due");
    drop(bare);

    let mut rt = runtime_at(&path, &clock);
    rt.poll_once().await.unwrap();
    assert_eq!(runs(&rt, "tick").len(), 1, "the slot fires once registered");
}

// A damaged row does not stop the other schedules. `list_schedules` reports
// it.
#[tokio::test]
async fn a_damaged_schedule_row_does_not_stop_the_others() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("damaged.db");
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&path, &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    rt.upsert_schedule(&every_minute("hold")).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE harvest_schedules SET schedule_json = 'nope' WHERE workflow_name = 'tick'",
            [],
        )
        .unwrap();

    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(runs(&rt, "hold").len(), 1);
    assert_eq!(runs(&rt, "tick").len(), 0);
    assert!(matches!(rt.list_schedules(), Err(SqliteError::Corrupt(_))));
}

// Each field this backend cannot honor is rejected by name. A bad spec and
// an unknown workflow are rejected too.
#[tokio::test]
async fn unsupported_fields_are_rejected_by_name() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&tick_info());
    let base = || every_minute("tick");
    let cases: Vec<(&str, WorkflowSchedule)> = vec![
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
        ("dag_name", {
            let mut s = base();
            s.dag_name = Some("dag".to_string());
            s
        }),
        ("consecutive_failure_limit", {
            let mut s = base();
            s.consecutive_failure_limit = Some(3);
            s
        }),
        ("retry_policy", {
            let mut s = base();
            s.retry_policy = Some(RetryPolicy::default());
            s
        }),
        ("execution_timeout", {
            let mut s = base();
            s.execution_timeout = Some(Duration::from_secs(5));
            s
        }),
        ("chain_execution_timeout", {
            let mut s = base();
            s.chain_execution_timeout = Some(Duration::from_secs(5));
            s
        }),
        ("all_writable_shards", {
            let mut s = base();
            s.all_writable_shards = true;
            s
        }),
    ];
    for (field, schedule) in cases {
        let err = rt.upsert_schedule(&schedule).unwrap_err();
        assert!(
            matches!(&err, SqliteError::Unsupported(m) if m.starts_with(&format!("WorkflowSchedule.{field} —"))),
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
    assert_eq!(runs(&rt, "tick").len(), 0);
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

fn last_run_at(rt: &SqliteRuntime, workflow: &str) -> Option<i64> {
    rt.list_schedules()
        .unwrap()
        .into_iter()
        .find(|s| s.workflow_name == workflow)
        .expect("the schedule exists")
        .last_run_at
        .map(|at| at.timestamp_millis())
}

// A late fire keeps the cadence: the next slot counts from the slot, not
// from the fire. The fired slot becomes `last_run_at`.
#[tokio::test]
async fn a_late_fire_keeps_the_cadence() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("late.db"), &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();

    clock.store(T0 + MINUTE + 10_000, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    let fired = runs(&rt, "tick");
    assert_eq!(
        started_at(&fired[0]),
        T0 + MINUTE + 10_000,
        "starts at the fire"
    );
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + 2 * MINUTE));
    assert_eq!(last_run_at(&rt, "tick"), Some(T0 + MINUTE));
}

// A dropped slot moves on but keeps the last run's slot.
#[tokio::test]
async fn a_dropped_slot_keeps_the_last_run_slot() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("drop.db"), &clock);
    rt.upsert_schedule(&every_minute("hold")).unwrap();
    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    clock.store(T0 + 2 * MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    assert_eq!(last_run_at(&rt, "hold"), Some(T0 + MINUTE));
    assert_eq!(next_run_at(&rt, "hold"), Some(T0 + 3 * MINUTE));
}

// `Skip` honors a `max_active_runs` above one.
#[tokio::test]
async fn skip_honors_max_active_runs() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("max.db"), &clock);
    let mut two = every_minute("hold");
    two.max_active_runs = 2;
    rt.upsert_schedule(&two).unwrap();
    for slot in 1..=3 {
        clock.store(T0 + slot * MINUTE, Ordering::SeqCst);
        rt.poll_once().await.unwrap();
    }
    assert_eq!(runs(&rt, "hold").len(), 2, "the third slot is dropped");
}

// A slot that already has its run starts no second run. It still counts as
// dispatched, so it becomes `last_run_at`.
#[tokio::test]
async fn a_slot_with_a_run_starts_no_second_run() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dup.db");
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&path, &clock);
    let mut many = every_minute("tick").with_overlap_policy(OverlapPolicy::AllowAll);
    many.max_active_runs = 10;
    rt.upsert_schedule(&many).unwrap();
    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE harvest_schedules SET next_run_at = ?1, last_run_at = NULL",
        [T0 + MINUTE],
    )
    .unwrap();

    rt.poll_once().await.unwrap();
    assert_eq!(runs(&rt, "tick").len(), 1, "no second run of the slot");
    assert_eq!(last_run_at(&rt, "tick"), Some(T0 + MINUTE));
    assert_eq!(next_run_at(&rt, "tick"), Some(T0 + 2 * MINUTE));
}

// A workflow retry is the same logical run, so it blocks the next slot.
#[tokio::test]
async fn skip_counts_a_workflow_retry() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("retry.db"), &clock);
    rt.upsert_schedule(&every_minute("fail_then_retry"))
        .unwrap();
    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    clock.store(T0 + 2 * MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    let states: Vec<String> = runs(&rt, "fail_then_retry").iter().map(state).collect();
    assert_eq!(
        states,
        vec!["FAILED".to_string(), "RUNNING".to_string()],
        "the retry waits for its backoff and blocks the slot"
    );
}

// A run past its deadline does not count. Postgres has already ended it.
#[tokio::test]
async fn skip_does_not_count_an_overdue_run() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("overdue.db"), &clock);
    rt.upsert_schedule(&every_minute("short_lived")).unwrap();
    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    clock.store(T0 + 2 * MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    let states: Vec<String> = runs(&rt, "short_lived").iter().map(state).collect();
    assert_eq!(states, vec!["TIMED_OUT".to_string(), "RUNNING".to_string()]);
}

// A run with a pending cancel does not count. Postgres has already ended it.
#[tokio::test]
async fn skip_does_not_count_a_cancelled_run() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("cancel.db"), &clock);
    rt.upsert_schedule(&every_minute("hold")).unwrap();
    clock.store(T0 + MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    let first = runs(&rt, "hold")[0].execution_id().unwrap();
    rt.cancel(first, "stop").unwrap();
    clock.store(T0 + 2 * MINUTE, Ordering::SeqCst);
    rt.poll_once().await.unwrap();
    let states: Vec<String> = runs(&rt, "hold").iter().map(state).collect();
    assert_eq!(states, vec!["CANCELLED".to_string(), "RUNNING".to_string()]);
}

// `run_until_idle` fires on its first pass only. A clock that passes a slot
// on each pass cannot keep it busy.
#[tokio::test]
async fn run_until_idle_quiesces_on_a_fast_clock() {
    let dir = tempfile::tempdir().unwrap();
    let mut rt = runtime_at(&dir.path().join("fast.db"), &Arc::new(AtomicI64::new(T0)));
    rt.upsert_schedule(&WorkflowSchedule::new(
        "tick",
        Schedule::Interval(Duration::from_secs(1)),
    ))
    .unwrap();
    let clock = Arc::new(AtomicI64::new(T0 + 1_000));
    let ticks = clock.clone();
    rt.set_clock(move || {
        DateTime::from_timestamp_millis(ticks.fetch_add(1_000, Ordering::SeqCst)).unwrap()
    });
    rt.run_until_idle().await.expect("the call quiesces");
    assert_eq!(runs(&rt, "tick").len(), 1);
}

// A cron in a time zone round-trips through the row. Its slot is local.
#[tokio::test]
async fn a_cron_in_a_time_zone_fires_at_its_local_slot() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&dir.path().join("tz.db"), &clock);
    rt.upsert_schedule(&WorkflowSchedule::new(
        "tick",
        Schedule::CronInTimezone {
            expr: "0 9 * * *".to_string(),
            tz: "America/New_York".to_string(),
        },
    ))
    .unwrap();
    let nine_est = T0 + 14 * 60 * MINUTE;
    assert_eq!(
        next_run_at(&rt, "tick"),
        Some(nine_est),
        "09:00 EST is 14:00Z"
    );
    assert!(matches!(
        &rt.list_schedules().unwrap()[0].schedule,
        Schedule::CronInTimezone { tz, .. } if tz == "America/New_York"
    ));
}

// A failed write is not a stale schedule. The pass still fires the other
// schedules, then returns the error, so the caller sees the lost fire.
#[tokio::test]
async fn a_failed_fire_write_is_returned() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("write.db");
    let clock = Arc::new(AtomicI64::new(T0));
    let mut rt = runtime_at(&path, &clock);
    rt.upsert_schedule(&every_minute("tick")).unwrap();
    rt.upsert_schedule(&every_minute("hold")).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail_tick BEFORE INSERT ON harvest_executions \
             WHEN NEW.workflow_name = 'tick' \
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();

    clock.store(T0 + MINUTE, Ordering::SeqCst);
    let err = rt.poll_once().await.expect_err("the failed write returns");
    assert!(matches!(err, SqliteError::Sqlite(_)), "{err}");
    assert_eq!(runs(&rt, "hold").len(), 1, "the other schedule still fires");
    assert_eq!(
        next_run_at(&rt, "tick"),
        Some(T0 + MINUTE),
        "the slot stays due"
    );
}
