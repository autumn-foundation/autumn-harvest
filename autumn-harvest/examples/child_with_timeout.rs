#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Deadline-bounded child-workflow await (issue #779).
//!
//! Demonstrates `ctx.execute_child_workflow_timeout`: spawn a child workflow
//! and await its result, but give up after a deadline and run a fallback —
//! in ≤ 3 lines, with no hand-rolled `tokio::join!` over a child + a timer
//! (which a replay engine cannot make deterministic on its own).
//!
//! Determinism contract: the race composes the existing
//! `ChildWorkflowStarted`/`ChildWorkflowCompleted`/`ChildWorkflowFailed` and
//! `TimerStarted`/`TimerFired` events — no new event variant. Whichever
//! resolution event was recorded first in history wins on every replay,
//! regardless of wall-clock timing on the replaying worker. If the deadline
//! wins, the still-running child is durably request-cancelled (it never leaks).
//!
//! Run with:
//!   cargo run --example child_with_timeout

use std::time::Duration;

use autumn_harvest::prelude::*;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct EnrichResult {
    pub score: u32,
}

/// A child workflow that computes an enrichment score. In production it might
/// call a slow downstream service via its own activities.
#[workflow]
async fn enrich_order(_ctx: &WorkflowContext, order_id: String) -> Result<EnrichResult, String> {
    println!("enriching order {order_id}");
    Ok(EnrichResult { score: 87 })
}

/// Await the enrichment child for up to 5 seconds; if it does not finish in
/// time, fall back to a default score and move on. The two lines that matter
/// are the `execute_child_workflow_timeout` call and the `match` on its
/// `Option` result.
#[workflow]
async fn place_order(ctx: &WorkflowContext, order_id: String) -> Result<u32, String> {
    let enriched: Option<EnrichResult> = ctx
        .execute_child_workflow_timeout(
            &enrich_order_info(),
            order_id.clone(),
            Duration::from_secs(5),
        )
        .await
        .map_err(|e| e.to_string())?;

    let score = match enriched {
        // The child finished before the deadline.
        Some(result) => result.score,
        // The deadline fired first — the child was request-cancelled; use a
        // conservative default and continue.
        None => 50,
    };

    Ok(score)
}

fn main() {
    println!("child_with_timeout example loaded successfully!");
    println!();
    println!("Deadline-bounded child await in two lines:");
    println!("  let enriched: Option<EnrichResult> = ctx");
    println!(
        "      .execute_child_workflow_timeout(&enrich_order_info(), order_id, Duration::from_secs(5))"
    );
    println!("      .await?;");
    println!();
    println!("  Some(result) — the child completed before the deadline");
    println!("  None         — the deadline fired first; the child was cancelled (plan B)");
    println!();
    println!("Register on a HarvestBuilder:");
    println!("  .workflows(workflows![place_order, enrich_order])");
}

// Embedded WorkflowTestEnv tests exercise both branches without any real
// sleeping. Gated on the `testing` feature so the example itself compiles for
// external consumers without it.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use serde_json::json;

    #[tokio::test]
    async fn child_completes_before_deadline_uses_child_score() {
        let outcome = WorkflowTestEnv::new()
            .mock_child_workflow("enrich_order", |_input| Ok(json!({"score": 87})))
            .run(place_order_info().handler, json!("order-1"))
            .await;

        assert_eq!(outcome.result, Ok(json!(87)));
        assert!(
            outcome
                .events()
                .iter()
                .any(|e| matches!(e, WorkflowEvent::ChildWorkflowCompleted { .. })),
            "expected ChildWorkflowCompleted on the child-win branch"
        );

        let report = outcome.replay_check(place_order_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "child branch must replay:\n{report}"
        );
    }

    #[tokio::test]
    async fn deadline_fires_first_uses_fallback_score() {
        // No child mock — the stubbed deadline timer fires immediately.
        let outcome = WorkflowTestEnv::new()
            .run(place_order_info().handler, json!("order-2"))
            .await;

        assert_eq!(outcome.result, Ok(json!(50)));
        assert!(
            outcome
                .events()
                .iter()
                .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
            "expected the deadline TimerFired"
        );

        let report = outcome.replay_check(place_order_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "deadline branch must replay:\n{report}"
        );
    }
}
