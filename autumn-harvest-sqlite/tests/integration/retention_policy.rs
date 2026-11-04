//! Issue #2471: a retention pass with per-type ages and a dry run.
//!
//! The SQLite plugin maps `RetentionConfig` to a [`PurgePolicy`]. The rule
//! is the Postgres one: a type override wins over the global age, and a type
//! with neither stays.

// A `#[workflow]` must be `async`, even when its body has no `.await`.
#![allow(clippy::unused_async)]
// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{PurgePolicy, RunState, SqliteError, SqliteRuntime};
use chrono::Utc;
use serde_json::json;

#[workflow]
async fn short(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn long(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

/// Waits for `go`, so the run stays `RUNNING`.
#[workflow]
async fn live(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(())
}

const HOUR: Duration = Duration::from_secs(3600);

fn runtime(path: &std::path::Path) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).unwrap();
    rt.register_workflow(&short_info());
    rt.register_workflow(&long_info());
    rt.register_workflow(&live_info());
    rt
}

/// Start `workflow` and drive it to its end.
async fn finish(rt: &mut SqliteRuntime, workflow: &str) -> autumn_harvest::ExecutionId {
    let exec = rt.start_workflow(workflow, json!(1)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
    exec
}

/// Two hours from now, so a run closed now is two hours old.
fn two_hours_on() -> chrono::DateTime<Utc> {
    Utc::now() + chrono::Duration::hours(2)
}

fn gone(rt: &SqliteRuntime, exec: autumn_harvest::ExecutionId) -> bool {
    matches!(rt.describe(exec), Err(SqliteError::ExecutionNotFound(_)))
}

/// A type override keeps its own age. The global age covers the other types.
#[tokio::test]
async fn a_type_override_wins_over_the_global_age() {
    let dir = tempfile::tempdir().unwrap();
    let mut rt = runtime(&dir.path().join("override.db"));
    let short_run = finish(&mut rt, "short").await;
    let long_run = finish(&mut rt, "long").await;
    let live_run = rt.start_workflow("live", json!(1)).unwrap();
    rt.run_until_blocked(live_run).await.unwrap();

    let policy = PurgePolicy::new()
        .max_age(HOUR)
        .override_age("long", 3 * HOUR);
    let report = rt.purge_with_as_of(two_hours_on(), &policy).unwrap();

    assert_eq!(report.removed, 1);
    assert_eq!(report.by_workflow.get("short"), Some(&1));
    assert!(!report.by_workflow.contains_key("long"));
    assert!(!report.dry_run);
    assert!(gone(&rt, short_run));
    assert!(
        !gone(&rt, long_run),
        "its override keeps it for three hours"
    );
    assert!(!gone(&rt, live_run), "a running run never goes");

    // The override also purges on its own, with no global age.
    let only = PurgePolicy::new().override_age("long", HOUR);
    let report = rt.purge_with_as_of(two_hours_on(), &only).unwrap();
    assert_eq!(report.removed, 1);
    assert!(gone(&rt, long_run));
}

/// A type with no override and no global age stays.
#[tokio::test]
async fn a_type_with_no_age_stays() {
    let dir = tempfile::tempdir().unwrap();
    let mut rt = runtime(&dir.path().join("none.db"));
    let short_run = finish(&mut rt, "short").await;
    let long_run = finish(&mut rt, "long").await;

    let policy = PurgePolicy::new().override_age("long", HOUR);
    let report = rt.purge_with_as_of(two_hours_on(), &policy).unwrap();
    assert_eq!(report.removed, 1);
    assert!(!gone(&rt, short_run), "no age covers `short`");
    assert!(gone(&rt, long_run));

    let empty = rt
        .purge_with_as_of(two_hours_on(), &PurgePolicy::new())
        .unwrap();
    assert_eq!(empty.removed, 0);
    assert!(!gone(&rt, short_run));
}

/// A dry run counts the runs it would remove, by type, and removes none.
#[tokio::test]
async fn a_dry_run_counts_and_removes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut rt = runtime(&dir.path().join("dry.db"));
    let first = finish(&mut rt, "short").await;
    finish(&mut rt, "short").await;
    let long_run = finish(&mut rt, "long").await;

    let policy = PurgePolicy::new().max_age(HOUR).dry_run(true);
    let report = rt.purge_with_as_of(two_hours_on(), &policy).unwrap();
    assert!(report.dry_run);
    assert_eq!(report.removed, 3);
    assert_eq!(report.by_workflow.get("short"), Some(&2));
    assert_eq!(report.by_workflow.get("long"), Some(&1));
    assert!(!gone(&rt, first) && !gone(&rt, long_run), "nothing goes");
}

/// The window counts on the real clock: a run closed now is inside it.
#[tokio::test]
async fn a_run_inside_the_window_stays() {
    let dir = tempfile::tempdir().unwrap();
    let mut rt = runtime(&dir.path().join("window.db"));
    let run = finish(&mut rt, "short").await;
    let report = rt.purge_with(&PurgePolicy::new().max_age(HOUR)).unwrap();
    assert_eq!(report.removed, 0);
    assert!(!gone(&rt, run));
}
