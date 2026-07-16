//! Integration tests for `ctx.publish_progress` (issue #791).
//!
//! Progress chunks are an EPHEMERAL, best-effort live-output side channel:
//! replay-neutral, **zero `harvest_events` footprint, NO new `WorkflowEvent`
//! variant, NO migration**. Chunk content MAY be non-deterministic precisely
//! because it is never recorded or replayed.
//!
//! These tests pin the falsifiable AC2 bar — 0 bytes to `harvest_events`, 0
//! replay divergence — using the in-process harnesses (no DB required).
//!
//! Run with:
//!   cargo test -p autumn-harvest --test integration --features testing \
//!     --no-default-features -- publish_progress

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Test workflow handlers
// ---------------------------------------------------------------------------

/// Two sequential activities with a `publish_progress` chunk emitted before,
/// between, and after them. Because progress leaves **zero** footprint in
/// `harvest_events`, this workflow must replay identically against a history
/// containing ONLY the two activity pairs — the same history a workflow that
/// never published progress would produce.
fn publish_progress_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.publish_progress(json!({"phase": "starting", "pct": 0}))
            .map_err(|e| e.to_string())?;
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.publish_progress(json!({"phase": "mid", "pct": 50}))
            .map_err(|e| e.to_string())?;
        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.publish_progress(json!({"phase": "done", "pct": 100}))
            .map_err(|e| e.to_string())?;
        Ok(json!({"first": r1, "second": r2}))
    })
}

/// Identical shape but with NO `publish_progress` calls. Its produced event
/// history must be byte-for-byte identical (by event-type sequence) to the
/// progress-publishing sibling above.
fn no_progress_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"first": r1, "second": r2}))
    })
}

// ---------------------------------------------------------------------------
// History fixtures (activity-only — progress records nothing)
// ---------------------------------------------------------------------------

fn two_activity_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let id2 = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: json!("result_one"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id2,
            name: "step_two".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id2,
            output: json!("result_two"),
        },
    ];
    (exec_id, events)
}

// ---------------------------------------------------------------------------
// AC2 (a): zero `harvest_events` footprint — a progress-publishing workflow
//          produces byte-for-byte the same event history as one that does not.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_progress_leaves_zero_event_footprint() {
    let progress_outcome = WorkflowTestEnv::new()
        .mock_activity("step_one", |_| Ok(json!("result_one")))
        .mock_activity("step_two", |_| Ok(json!("result_two")))
        .run(publish_progress_workflow, json!(null))
        .await;

    let plain_outcome = WorkflowTestEnv::new()
        .mock_activity("step_one", |_| Ok(json!("result_one")))
        .mock_activity("step_two", |_| Ok(json!("result_two")))
        .run(no_progress_workflow, json!(null))
        .await;

    // The workflow completes normally — publish_progress never fails it.
    assert_eq!(
        progress_outcome.result,
        Ok(json!({"first": "result_one", "second": "result_two"})),
        "publish_progress must not alter the workflow result"
    );

    let progress_types: Vec<&'static str> = progress_outcome
        .events()
        .iter()
        .map(WorkflowEvent::type_name)
        .collect();
    let plain_types: Vec<&'static str> = plain_outcome
        .events()
        .iter()
        .map(WorkflowEvent::type_name)
        .collect();

    assert_eq!(
        progress_types, plain_types,
        "publish_progress must leave ZERO footprint in harvest_events: the \
         event-type sequence must equal the no-progress sibling's, got \
         {progress_types:?} vs {plain_types:?}"
    );

    // Belt-and-braces: no event type name mentions "progress" — there is no
    // progress WorkflowEvent variant at all.
    assert!(
        progress_outcome
            .events()
            .iter()
            .all(|e| !e.type_name().to_ascii_lowercase().contains("progress")),
        "no progress-related WorkflowEvent may ever be recorded"
    );
}

// ---------------------------------------------------------------------------
// AC2 (b): replay-neutrality — a workflow that publishes progress replays
//          cleanly (ReplaySucceeded) against a history with zero corresponding
//          events, exactly like the `set_current_details` (#593) precedent.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_progress_replays_with_zero_divergence() {
    let (exec_id, events) = two_activity_history();
    let replayer =
        WorkflowReplayer::new().register_fn("publish_progress_workflow", publish_progress_workflow);

    let report = replayer
        .replay_from_snapshot(autumn_harvest::testing::HistorySnapshot {
            workflow_name: "publish_progress_workflow".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
        })
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a workflow calling publish_progress must replay against a history with \
         zero corresponding events (progress is replay-neutral), got: {report}"
    );
    assert!(
        report.events_replayed > 0,
        "events_replayed must be positive"
    );
}
