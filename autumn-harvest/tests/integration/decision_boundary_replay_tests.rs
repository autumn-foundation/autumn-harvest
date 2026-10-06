//! Replay ignores decision boundaries (issue #1833).
//!
//! A history written before #1833 has no `DecisionCommitted` events. It must
//! replay unchanged. A history with boundaries must replay the same way.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{HistorySnapshot, ReplayReport, ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::{BuildId, WorkerId};
use serde_json::{Value, json};

/// A history recorded before issue #1833, kept on disk as a fixture.
const PRE_1833_HISTORY: &str = include_str!("../fixtures/pre_1833_order_history.json");

/// The workflow that wrote `PRE_1833_HISTORY`.
fn order_flow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let reserved = ctx
            .execute_activity_raw("reserve", input.clone(), "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cool-off", 5).await.map_err(|e| e.to_string())?;
        ctx.wait_for_signal("approve")
            .await
            .map_err(|e| e.to_string())?;
        let charged = ctx
            .execute_activity_raw("charge", input, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(format!(
            "{}+{}",
            reserved.as_str().unwrap_or_default(),
            charged.as_str().unwrap_or_default()
        )))
    })
}

fn boundary(build: &str) -> WorkflowEvent {
    WorkflowEvent::DecisionCommitted {
        build_id: BuildId::new(build),
        worker_id: WorkerId::new("worker-eu-1"),
    }
}

fn fixture() -> HistorySnapshot {
    serde_json::from_str(PRE_1833_HISTORY).expect("fixture parses")
}

/// Inserts a boundary after each event index in `after`, as a worker would.
fn with_boundaries_after(mut snapshot: HistorySnapshot, after: &[usize]) -> HistorySnapshot {
    for (offset, index) in after.iter().enumerate() {
        snapshot
            .events
            .insert(index + 1 + offset, boundary(&format!("build-{offset}")));
    }
    snapshot
}

fn replayer() -> WorkflowReplayer {
    WorkflowReplayer::new().register_fn("order_flow", order_flow)
}

async fn replay(snapshot: &HistorySnapshot) -> ReplayReport {
    replayer().replay_from_snapshot(snapshot.clone()).await
}

fn is_success(report: &ReplayReport) -> bool {
    matches!(report.status, ReplayStatus::ReplaySucceeded)
}

#[tokio::test]
async fn a_pre_1833_history_replays_unchanged() {
    let snapshot = fixture();
    assert!(
        !snapshot
            .events
            .iter()
            .any(WorkflowEvent::is_decision_boundary),
        "the fixture must predate decision boundaries"
    );
    let report = replayer()
        .replay_from_json(PRE_1833_HISTORY)
        .await
        .expect("fixture parses");
    assert!(is_success(&report), "{report}");
    assert_eq!(report.events_replayed, snapshot.events.len());
}

#[tokio::test]
async fn the_same_history_with_boundaries_replays_the_same_way() {
    // The decision ends of `order_flow`: after the first schedule (1), the
    // timer start (4), the second schedule (7) and the completion (10).
    let plain = replay(&fixture()).await;
    let snapshot = with_boundaries_after(fixture(), &[1, 4, 7, 10]);
    let report = replay(&snapshot).await;
    assert!(is_success(&report), "{report}");
    assert_eq!(report.events_replayed, snapshot.events.len());
    assert_eq!(report.reproduced_failure, plain.reproduced_failure);
}

#[tokio::test]
async fn a_boundary_after_every_event_is_still_ignored() {
    let every: Vec<usize> = (0..fixture().events.len()).collect();
    let report = replay(&with_boundaries_after(fixture(), &every)).await;
    assert!(is_success(&report), "{report}");
}

#[tokio::test]
async fn a_boundary_does_not_hide_a_real_divergence() {
    // Swap the two activity names. The boundary must not mask the change.
    let mut snapshot = with_boundaries_after(fixture(), &[1, 4, 7, 10]);
    for event in &mut snapshot.events {
        if let WorkflowEvent::ActivityScheduled { name, .. } = event {
            *name = if name == "reserve" {
                "charge"
            } else {
                "reserve"
            }
            .to_string();
        }
    }
    let report = replay(&snapshot).await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "{report}"
    );
}

/// A run that failed, then had its last decision recorded.
fn failing<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("reserve", json!({}), "default")
            .await
            .map_err(|e| e.to_string())?;
        Err("out of stock".to_string())
    })
}

#[tokio::test]
async fn a_failed_history_with_a_trailing_boundary_reproduces_the_failure() {
    let activity_id = autumn_harvest::types::ActivityExecId::new();
    let events = vec![
        WorkflowEvent::workflow_started(json!({}), chrono::Utc::now()),
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "reserve".into(),
            input: json!({}),
            queue: "default".into(),
        },
        boundary("build-a"),
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: json!("reserved"),
        },
        WorkflowEvent::workflow_failed("out of stock"),
        boundary("build-a"),
    ];
    let report = WorkflowReplayer::new()
        .register_fn("failing", failing)
        .replay_from_events(events)
        .await;
    assert!(is_success(&report), "{report}");
    assert_eq!(report.reproduced_failure.as_deref(), Some("out of stock"));
}
