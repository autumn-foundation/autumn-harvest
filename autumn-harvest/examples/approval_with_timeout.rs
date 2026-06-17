#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Signal-or-deadline approval gate (issue #476).
//!
//! Demonstrates `ctx.receive_signal_timeout`: await a human approval signal,
//! else auto-reject after a deadline — in two lines, with zero shared mutable
//! state and no hand-rolled `Arc<Mutex<...>>` + `await_condition_timeout`
//! workaround.
//!
//! Determinism contract: the race composes the existing `TimerStarted` /
//! `TimerFired` and `SignalReceived` events. Whichever event was recorded
//! first in history wins on every replay, regardless of wall-clock timing on
//! the replaying worker. If the deadline wins, the signal payload is NOT
//! consumed — a late approval is still observable by a later
//! `receive_signal*` call.
//!
//! Deliver the approval over the existing management route:
//!   POST /api/harvest/workflows/{exec_id}/signal
//!   { "signal_name": "approval", "payload": { "approved": true, "approver": "alice" } }

use std::time::Duration;

use autumn_harvest::prelude::*;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Decision {
    pub approved: bool,
    pub approver: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ReviewOutcome {
    pub state: String,
    pub approver: Option<String>,
}

#[activity(start_to_close = "30s")]
async fn notify_requester(_ctx: &ActivityContext, outcome: ReviewOutcome) -> Result<(), String> {
    println!("notifying requester: {outcome:?}");
    Ok(())
}

/// Wait up to 24 hours for a human decision; auto-reject when the deadline
/// fires first. The two lines that matter are the `receive_signal_timeout`
/// call and the `match` on its `Option` result.
#[workflow]
async fn document_review(
    ctx: &WorkflowContext,
    _document_id: String,
) -> Result<ReviewOutcome, String> {
    let decision: Option<Decision> = ctx
        .receive_signal_timeout("approval", Duration::from_secs(24 * 60 * 60))
        .await
        .map_err(|e| e.to_string())?;

    let outcome = match decision {
        Some(decision) if decision.approved => ReviewOutcome {
            state: "approved".to_string(),
            approver: Some(decision.approver),
        },
        Some(decision) => ReviewOutcome {
            state: "rejected".to_string(),
            approver: Some(decision.approver),
        },
        // Deadline fired first — escalate to the default path.
        None => ReviewOutcome {
            state: "auto_rejected".to_string(),
            approver: None,
        },
    };

    let _: () = ctx
        .execute_activity(&notify_requester_info(), outcome.clone())
        .await
        .map_err(|e| e.to_string())?;

    Ok(outcome)
}

fn main() {
    println!("approval_with_timeout example loaded successfully!");
    println!();
    println!("Signal-or-deadline in two lines:");
    println!("  let decision: Option<Decision> = ctx");
    println!("      .receive_signal_timeout(\"approval\", Duration::from_secs(86_400))");
    println!("      .await?;");
    println!();
    println!("  Some(payload) — the approval signal arrived before the deadline");
    println!("  None          — the durable timer fired first (auto-reject path)");
    println!();
    println!("Register on a HarvestBuilder:");
    println!("  .workflows(workflows![document_review])");
    println!("  .activities(activities![notify_requester])");
}
