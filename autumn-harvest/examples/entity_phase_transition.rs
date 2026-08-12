#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Multi-phase entity workflows via cross-type continue-as-new — issue #803.
//!
//! ## The problem
//!
//! A long-lived entity (a subscription, a device, an account) does genuinely
//! *different* work in each lifecycle phase. A trial subscription counts down
//! to conversion; a paid one bills monthly; a churned one just answers
//! reactivation. `ctx.continue_as_new(input)` resets history — which is what
//! you want at a phase boundary — but always resurrects the run as the **same**
//! workflow type. So you get two bad options:
//!
//! 1. **One monolithic handler** that branches on a `phase` field forever. Every
//!    phase's code is loaded, versioned and replayed by every other phase, and
//!    the `ctx.patched()` gates never retire.
//! 2. **Terminate and start a new type.** You get a clean handler per phase, but
//!    the entity's stable `workflow_id` is broken across the seam, and any
//!    `signal_with_start` (#244) / `update_with_start` (#479) racing the
//!    transition lands on the wrong run.
//!
//! ## The solution: `ctx.continue_as_new_as_type(...)`
//!
//! Continue the *same logical entity* as a *different registered type*:
//!
//! ```rust,ignore
//! #[workflow]
//! async fn trial_subscription(ctx: &WorkflowContext, sub: Subscription) -> Result<Value, String> {
//!     // ... trial-specific logic ...
//!     ctx.continue_as_new_as_type("paid_subscription", json!(sub)).await?;
//!     unreachable!("continue_as_new_as_type never resolves");
//! }
//! ```
//!
//! or, avoiding the magic string, with the target's companion `WorkflowInfo`:
//!
//! ```rust,ignore
//! ctx.continue_as_new_as::<Subscription>(&paid_subscription_info(), sub).await?;
//! ```
//!
//! The successor keeps the same `workflow_id`, shard and queue; its history
//! starts clean; and its lifecycle defaults come from the **new** type.
//!
//! ## What carries, what re-resolves
//!
//! | Property | Behaviour |
//! |---|---|
//! | `workflow_id`, shard, queue | **Kept** — the entity's stable identity survives every phase. |
//! | Event history | **Reset** — that is the point of continue-as-new. |
//! | `execution_timeout` (#243), `sla` (#487) | **Re-resolved from the new type** — a billing phase's budget is not a trial phase's. |
//! | Concurrency key/limit (#247) | **Re-resolved from the new type's policy** against the new input. |
//! | `owner` / `runbook_url` / `severity` (#372) | **Re-resolved from the new type** — alerts page the team that owns *this* phase. |
//! | Workflow-level retry policy (#523) | **Re-resolved from the new type.** |
//! | Chain lifetime cap (#617) | **Carried verbatim** — changing type is *not* an escape hatch from a runaway-loop budget. |
//! | Unconsumed signals, `last_completion_result` (#488), schedule lineage (#534) | **Carried** — the carryover code is type-agnostic. |
//!
//! A same-type `ctx.continue_as_new(input)` is completely unchanged: it carries
//! every lifecycle column verbatim, exactly as before this feature existed.
//!
//! ## Addressing consequence — read this before adopting
//!
//! Harvest's active-run identity is the **pair** `(workflow_name, workflow_id)`,
//! not `workflow_id` alone. After the entity transitions from
//! `trial_subscription` to `paid_subscription`, an external caller must name the
//! **current phase type**:
//!
//! ```bash
//! # ✅ attaches to the live successor
//! curl -X POST .../workflows/paid_subscription/signal-with-start \
//!   -d '{"workflow_id": "sub-42", "signal_name": "upgrade", ...}'
//!
//! # ❌ starts a SEPARATE run of the old type that coexists with the successor
//! curl -X POST .../workflows/trial_subscription/signal-with-start \
//!   -d '{"workflow_id": "sub-42", "signal_name": "upgrade", ...}'
//! ```
//!
//! The old type's uniqueness slot is released by the transition, so naming it
//! does not error — it silently starts a fresh run. If external callers cannot
//! track the current phase, keep the entity on one type and branch internally.
//!
//! ## Rollout ordering
//!
//! The target type must be registered on the worker that runs the transition.
//! Deploy the new phase's handler **fleet-wide first**, then deploy the code
//! that continues into it. Continuing into an unregistered type fails the run
//! terminally with a clear operator message rather than creating a successor
//! that can never be dispatched.
//!
//! Run with:
//!   cargo run --example entity_phase_transition

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Subscription {
    account_id: String,
    plan: String,
}

// ---------------------------------------------------------------------------
// Phase 1 — trial. Converts to `paid_subscription` or lapses to `churned`.
// ---------------------------------------------------------------------------

/// A trial run: wait for the conversion decision, then hand the entity to the
/// phase that actually knows how to handle the outcome.
///
/// Note `execution_timeout` / `owner` / `severity` here are the **trial**
/// phase's — the successor does not inherit them.
#[workflow(
    execution_timeout = "30d",
    description = "Trial phase of a subscription entity"
)]
async fn trial_subscription(
    ctx: &WorkflowContext,
    sub: Subscription,
) -> Result<serde_json::Value, String> {
    ctx.set_current_details(format!("trial: awaiting conversion for {}", sub.account_id));

    // A real trial would race a durable timer against the signal; kept to one
    // await here so the example stays about the transition itself.
    let converted: bool = ctx
        .receive_signal("conversion_decision")
        .await
        .map_err(|e| e.to_string())?;

    if converted {
        // Graduate into the billing phase. Same account, same workflow_id,
        // fresh history, and the *billing* phase's timeout/owner/SLA.
        ctx.continue_as_new_as::<Subscription>(&paid_subscription_info(), sub)
            .await
            .map_err(|e| e.to_string())?;
    } else {
        // The untyped form, for when the target is chosen dynamically.
        ctx.continue_as_new_as_type("churned", json!(sub))
            .await
            .map_err(|e| e.to_string())?;
    }
    unreachable!("continue_as_new_as* suspends the run and never resolves");
}

// ---------------------------------------------------------------------------
// Phase 2 — paid. Bills monthly; loops on ITSELF with plain continue_as_new.
// ---------------------------------------------------------------------------

/// The billing phase. Note the two continuation styles side by side:
///
/// * `ctx.continue_as_new(...)` — the *same* type, for the monthly billing loop
///   (unchanged behaviour: every lifecycle column is carried verbatim).
/// * `ctx.continue_as_new_as_type("churned", ...)` — a *different* type, when
///   the entity leaves this phase.
#[workflow(
    execution_timeout = "45d",
    description = "Billing phase of a subscription entity"
)]
async fn paid_subscription(
    ctx: &WorkflowContext,
    sub: Subscription,
) -> Result<serde_json::Value, String> {
    ctx.set_current_details(format!("paid: billing {} on {}", sub.account_id, sub.plan));

    let _: () = ctx
        .execute_activity(&charge_monthly_info(), sub.clone())
        .await
        .map_err(|e| e.to_string())?;

    let cancelled: bool = ctx
        .receive_signal("cancel_decision")
        .await
        .map_err(|e| e.to_string())?;

    if cancelled {
        ctx.continue_as_new_as_type("churned", json!(sub))
            .await
            .map_err(|e| e.to_string())?;
    } else {
        // Same-type loop — the classic continue-as-new, untouched by #803.
        ctx.continue_as_new(json!(sub))
            .await
            .map_err(|e| e.to_string())?;
    }
    unreachable!("continue_as_new* suspends the run and never resolves");
}

// ---------------------------------------------------------------------------
// Phase 3 — churned. Terminal phase: just answers reactivation.
// ---------------------------------------------------------------------------

#[workflow(description = "Dormant phase of a subscription entity")]
async fn churned(ctx: &WorkflowContext, sub: Subscription) -> Result<serde_json::Value, String> {
    ctx.set_current_details(format!("churned: {} is dormant", sub.account_id));
    Ok(json!({ "phase": "churned", "account_id": sub.account_id }))
}

#[activity(start_to_close = "30s")]
async fn charge_monthly(_ctx: &ActivityContext, _sub: Subscription) -> Result<(), String> {
    Ok(())
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    // All three phases must be registered on the same fleet: a transition can
    // only reach a type the worker running it knows about.
    let _wfs = workflows![trial_subscription, paid_subscription, churned];
    let _acts = activities![charge_monthly];

    println!("entity_phase_transition example compiled successfully");
    println!();
    println!("One entity, three registered types, one stable workflow_id:");
    println!("  trial_subscription --continue_as_new_as--> paid_subscription");
    println!("  paid_subscription  --continue_as_new-----> paid_subscription  (monthly loop)");
    println!("  paid_subscription  --continue_as_new_as--> churned");
    println!();
    println!("Address the entity by its CURRENT phase type:");
    println!(
        "  curl -X POST .../workflows/paid_subscription/signal-with-start \\\n     -d '{{\"workflow_id\": \"sub-42\", \"signal_name\": \"cancel_decision\", ...}}'"
    );
}

// Gated on the `testing` feature as well as `test`: the example itself must
// keep building under `--no-default-features` (which CI exercises), while
// `autumn_harvest::testing` only exists for external consumers when the
// `testing` feature is enabled.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};

    fn sub() -> serde_json::Value {
        json!({"account_id": "acct-42", "plan": "pro"})
    }

    /// The recorded transition names the target type, so an operator (and the
    /// replayer) can see exactly which phase the entity graduated into.
    #[tokio::test]
    async fn converting_a_trial_records_the_target_phase_type() {
        let outcome = WorkflowTestEnv::new()
            .queue_signal("conversion_decision", json!(true))
            .run(trial_subscription_info().handler, sub())
            .await;

        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
        let recorded = outcome
            .events()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::WorkflowContinuedAsNew {
                    new_workflow_type, ..
                } => Some(new_workflow_type.clone()),
                _ => None,
            })
            .expect("a transition must be recorded");
        assert_eq!(
            recorded.as_deref(),
            Some("paid_subscription"),
            "the graduation must name the billing phase"
        );
    }

    /// The dynamic (`_as_type`) form reaches a different phase from the same
    /// handler — the branch is part of recorded history, so it replays.
    #[tokio::test]
    async fn a_lapsed_trial_transitions_to_the_churned_phase() {
        let outcome = WorkflowTestEnv::new()
            .queue_signal("conversion_decision", json!(false))
            .run(trial_subscription_info().handler, sub())
            .await;

        let recorded = outcome
            .events()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::WorkflowContinuedAsNew {
                    new_workflow_type, ..
                } => Some(new_workflow_type.clone()),
                _ => None,
            })
            .expect("a transition must be recorded");
        assert_eq!(recorded.as_deref(), Some("churned"));
    }

    /// The billing phase's same-type monthly loop records **no** target type —
    /// `None` is exactly what every pre-#803 history carries, so the plain
    /// `continue_as_new` path is provably unchanged.
    #[tokio::test]
    async fn the_monthly_billing_loop_is_a_same_type_continuation() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("charge_monthly", |_| Ok(json!(null)))
            .queue_signal("cancel_decision", json!(false))
            .run(paid_subscription_info().handler, sub())
            .await;

        let recorded = outcome
            .events()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::WorkflowContinuedAsNew {
                    new_workflow_type, ..
                } => Some(new_workflow_type.clone()),
                _ => None,
            })
            .expect("a continuation must be recorded");
        assert!(
            recorded.is_none(),
            "a same-type loop must record no target type, got {recorded:?}"
        );
    }

    /// The falsifiable determinism bar from issue #803: a history whose
    /// `WorkflowContinuedAsNew` carries `new_workflow_type` must replay to the
    /// same successor type, every run.
    #[tokio::test]
    async fn a_type_changing_history_replays_deterministically() {
        let outcome = WorkflowTestEnv::new()
            .queue_signal("conversion_decision", json!(true))
            .run(trial_subscription_info().handler, sub())
            .await;

        let report = outcome
            .replay_check(trial_subscription_info().handler)
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "a cross-type transition must replay cleanly:\n{report}"
        );
    }
}
