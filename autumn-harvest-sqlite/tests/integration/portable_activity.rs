//! The shared `#[activity]` fixture runs unchanged on SQLite (issue #2024).
//!
//! The core runs the same fixture on Postgres and asserts the same report.

use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use serde_json::json;

use super::portable_fixture::{
    PORTABLE_WORKFLOW, expected_report, portable_flow_info, portable_probe_info,
};

#[tokio::test]
async fn the_portable_activity_runs_unchanged_on_sqlite() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&portable_flow_info());
    rt.register_activity(&portable_probe_info());
    let exec = rt
        .start_workflow_with_id(PORTABLE_WORKFLOW, "order-42", json!("sqlite"))
        .unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();

    let RunState::Completed(report) = state else {
        panic!("the run must complete, got {state:?}");
    };
    assert_eq!(report, expected_report("sqlite", exec, "order-42"));
}

#[tokio::test]
async fn the_portable_history_replays_on_the_core_replayer() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&portable_flow_info());
    rt.register_activity(&portable_probe_info());
    let exec = rt
        .start_workflow(PORTABLE_WORKFLOW, json!("replay"))
        .unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let report = WorkflowReplayer::new()
        .register_fn(PORTABLE_WORKFLOW, portable_flow_info().handler)
        .replay_from_events(rt.load_history(exec).unwrap())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the SQLite history must replay on the core:\n{report}"
    );
}
