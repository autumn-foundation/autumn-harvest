//! Decision-cycle persistence atomicity at the integration level (AC6).
//!
//! The transactional guarantee itself (event append + queue/timer row insert
//! commit together or roll back together) is proven directly by the inline unit
//! tests in `src/runtime.rs` and `src/worker.rs`, and the whole *class* of persist
//! sites is pinned by the reopen-consistency sweep in
//! `tests/reopen_consistency.rs`. This test asserts the observable consequence
//! end-to-end: after a real run every `ActivityScheduled` event has a matching
//! task row (the both-or-neither invariant that the atomic dispatch pair
//! guarantees can never diverge).

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteRuntime};
use serde_json::json;

#[workflow]
async fn single_activity(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().ok_or("bad activity output")? * 2)
}

// ── AC6: every ActivityScheduled event has a matching task row ────────────────

#[tokio::test]
async fn completed_run_has_matching_scheduled_events_and_task_rows() {
    let (_dir, path) = {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("harvest.sqlite3");
        (dir, p)
    };

    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&single_activity_info());
    rt.register_activity_raw(
        "work",
        ActivitySpec::new(1, |input: serde_json::Value| {
            Ok(json!(input.as_i64().unwrap()))
        }),
    );
    let exec = rt.start_workflow("single_activity", json!(9)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)));

    let scheduled_events = i64::try_from(
        rt.load_history(exec)
            .unwrap()
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
            .count(),
    )
    .unwrap();
    assert_eq!(scheduled_events, 1);

    // Every scheduled event landed with its task-queue row — the dispatch pair is
    // atomic, so the counts can never diverge.
    let raw = rusqlite::Connection::open(&path).unwrap();
    let task_rows: i64 = raw
        .query_row(
            "SELECT COUNT(*) FROM harvest_tasks WHERE exec_id = ?1",
            [exec.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        task_rows, scheduled_events,
        "each ActivityScheduled event must have exactly one task row"
    );
}
