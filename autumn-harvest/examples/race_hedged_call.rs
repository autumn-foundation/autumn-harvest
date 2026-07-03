#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Deterministic race/select primitive: hedged provider calls and an
//! approval-or-timeout gate expressed via `ctx.race()` (issue #600).
//!
//! Harvest already sanctions `futures::join!` for wait-*all* concurrency
//! (see `docs/workflow-determinism-guide.md`, HVG005). Until this primitive,
//! there was no sanctioned way to wait for the *first* of several concurrent
//! operations and durably cancel the rest — `tokio::select!`/`futures::select!`
//! over ctx awaitables is now a compile-time HardBlocker (HVG010) because it
//! is a double footgun in a replay engine: the winner depends on
//! non-deterministic poll order, and dropped loser futures do not durably
//! cancel the underlying work (a scheduled activity keeps running; a durable
//! timer row stays live).
//!
//! # Determinism contract
//!
//! The winning branch is recorded via the existing `MarkerRecorded` event —
//! mirroring `execute_activity_fan_out`'s count marker, no new
//! `WorkflowEvent` variant. Every subsequent replay of the same history
//! *verifies* — rather than re-derives — the previously recorded winner, so a
//! code change that would flip the outcome is rejected as
//! `HarvestError::NonDeterministic` instead of silently diverging.
//!
//! # Cancellation
//!
//! Losing branches are durably cancelled in the *same* transaction that
//! records the winner: a still-open losing activity's task row is
//! transitioned out of `PENDING`/`RUNNING` and a synthetic `ActivityFailed`
//! terminal is recorded (reusing the existing event variant) so no future
//! replay ever observes it stuck `ActivityInProgress`. A losing child
//! workflow is cancelled via the same primitive
//! `ctx.request_cancel_external_workflow` uses (issue #492).
//!
//! # Supported shapes (this slice)
//!
//! - A homogeneous race of **activity** branches (this example).
//! - A homogeneous race of **child-workflow** branches.
//! - Exactly one **timer** branch paired with exactly one **signal** branch —
//!   a thin wrapper over the already-shipped `receive_signal_timeout`
//!   (issue #476).
//!
//! Mixing branch kinds (e.g. an activity racing a timer in the same call) is
//! out of scope for this slice — see the `WorkflowContext::race` rustdoc.

use std::time::Duration;

use autumn_harvest::prelude::*;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Quote {
    pub provider: String,
    pub price_cents: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct QuoteRequest {
    pub sku: String,
}

#[activity(start_to_close = "10s")]
async fn fetch_quote_primary(_ctx: &ActivityContext, req: QuoteRequest) -> Result<Quote, String> {
    Ok(Quote {
        provider: "primary".to_string(),
        price_cents: 4200 + u64::from(req.sku.is_empty()),
    })
}

#[activity(start_to_close = "10s")]
async fn fetch_quote_fallback(_ctx: &ActivityContext, req: QuoteRequest) -> Result<Quote, String> {
    Ok(Quote {
        provider: "fallback".to_string(),
        price_cents: 4500 + u64::from(req.sku.is_empty()),
    })
}

/// Hedge two providers, take whichever answers first, cancel the loser — the
/// ≤5-line DX target measured from `ctx.race()` to the decoded result.
#[workflow]
async fn hedged_quote(ctx: &WorkflowContext, req: QuoteRequest) -> Result<Quote, String> {
    let winner = ctx
        .race()
        .activity(&fetch_quote_primary_info(), req.clone())
        .activity(&fetch_quote_fallback_info(), req)
        .run()
        .await
        .map_err(|e| e.to_string())?;
    winner.decode().map_err(|e| e.to_string())
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Decision {
    pub approved: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ReviewOutcome {
    pub state: String,
}

/// Approval-signal-or-timeout expressed via the same `ctx.race()` builder
/// (the other headline example from the issue's User Story) — internally a
/// thin wrapper over `receive_signal_timeout` (issue #476).
#[workflow]
async fn review_with_deadline(
    ctx: &WorkflowContext,
    _document_id: String,
) -> Result<ReviewOutcome, String> {
    let winner = ctx
        .race()
        .signal("approval")
        .timer(Duration::from_secs(24 * 60 * 60))
        .run()
        .await
        .map_err(|e| e.to_string())?;

    let outcome = if winner.value.is_null() {
        ReviewOutcome {
            state: "auto_rejected".to_string(),
        }
    } else {
        let decision: Decision = winner.decode().map_err(|e| e.to_string())?;
        ReviewOutcome {
            state: if decision.approved {
                "approved"
            } else {
                "rejected"
            }
            .to_string(),
        }
    };
    Ok(outcome)
}

fn main() {
    println!("race_hedged_call example loaded successfully!");
    println!();
    println!("Race two activities, take the winner, cancel the loser — five lines:");
    println!("  let winner = ctx.race()");
    println!("      .activity(&fetch_quote_primary_info(), req.clone())");
    println!("      .activity(&fetch_quote_fallback_info(), req)");
    println!("      .run().await?;");
    println!("  let quote: Quote = winner.decode()?;");
    println!();
    println!("Approval-or-timeout, via the same builder:");
    println!(
        "  let winner = ctx.race().signal(\"approval\").timer(Duration::from_secs(86_400)).run().await?;"
    );
    println!();
    println!("Register on a HarvestBuilder:");
    println!("  .workflows(workflows![hedged_quote, review_with_deadline])");
    println!("  .activities(activities![fetch_quote_primary, fetch_quote_fallback])");
}
