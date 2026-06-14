#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Deterministic external workflow cancellation (issue #492).
//!
//! Demonstrates `ctx.request_cancel_external_workflow(target)`: a supervisor
//! workflow aborts an in-flight fulfillment workflow after a fraud review
//! verdict arrives — one deterministic call, exactly-once cancel dispatch,
//! clean replay on every worker.
//!
//! # Key semantics
//!
//! * **Already-terminal target → no-op success** (`ExternalCancelDelivered`):
//!   the goal (target not running) is already met.
//! * **Unknown target after the grace window → `ExternalCancelFailed`**
//!   (`reason_code = "target_unknown"`).
//! * **Self-cancel → immediate `ExternalCancelFailed`** (`reason_code = "self_cancel"`).
//! * No payload — only `target: ExecutionId` is needed.
//!
//! # Three correlated events (append-only)
//!
//! 1. `ExternalCancelRequested { cancel_id, target }` — recorded on first live call.
//! 2. `ExternalCancelDelivered { cancel_id }` — target was cancelled (or already terminal).
//! 3. `ExternalCancelFailed { cancel_id, reason_code }` — target was unknown after grace window.
//!
//! The `cancel_id` correlation key lets `HistoryMatcher::match_external_cancel` find the
//! terminal event in history and replay the identical `Ok(())` / `Err(...)` deterministically.

use std::time::Duration;

use autumn_harvest::prelude::*;

/// Input shared by both workflows.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct OrderId {
    pub order_id: String,
    pub fulfillment_exec_id: String,
}

/// Fraud review decides to cancel an in-flight fulfillment order.
///
/// In production the `fulfillment_exec_id` is passed in from the orchestrating
/// system (e.g. the HTTP trigger that starts this workflow).  We call
/// `request_cancel_external_workflow` exactly once — on crash + replay the
/// cancel is NOT re-dispatched; the recorded `ExternalCancelDelivered` event
/// is replayed as `Ok(())`.
#[workflow]
async fn fraud_review_supervisor(ctx: &WorkflowContext, input: OrderId) -> Result<String, String> {
    // Parse the fulfillment execution ID that was started earlier.
    let fulfillment_id = uuid::Uuid::parse_str(&input.fulfillment_exec_id)
        .map_err(|e| format!("invalid exec id: {e}"))?;
    let target = ExecutionId::from_uuid(fulfillment_id);

    // Deterministic, replay-safe cancel.  Returns Ok(()) on delivery or if
    // the target is already terminal (no-op success), Err on unknown target.
    match ctx.request_cancel_external_workflow(target).await {
        Ok(()) => Ok(format!(
            "fulfillment {} cancelled (or already done)",
            input.order_id
        )),
        Err(HarvestError::ExternalCancelFailed { reason_code, .. }) => {
            // "target_unknown" after the grace window — log and continue.
            Ok(format!(
                "cancel of {} failed: {reason_code} (order already cleared?)",
                input.order_id
            ))
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Long-running fulfillment workflow — parks until cancelled or completes.
#[workflow]
async fn fulfillment_workflow(ctx: &WorkflowContext, input: OrderId) -> Result<String, String> {
    // In real code: execute multiple activities, wait for shipping confirmation, etc.
    // Here we park on a long timer so fraud_review_supervisor can cancel us.
    ctx.timer("fulfillment_deadline", Duration::from_secs(3600).as_secs())
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("fulfillment {} completed normally", input.order_id))
}

fn main() {
    // This example is compiled to verify it type-checks.
    // In production wire both workflows into HarvestBuilder:
    //
    //   HarvestBuilder::new()
    //       .workflows(workflows![fraud_review_supervisor, fulfillment_workflow])
    //       .worker(WorkerConfig::default())
    //       .build();
    //
    // Trigger the supervisor via the management API:
    //   POST /api/harvest/workflows/fraud_review_supervisor/start
    //   { "input": { "order_id": "ord-123", "fulfillment_exec_id": "<uuid>" } }
    println!("cancel_external_workflow example — see source for usage.");
}
