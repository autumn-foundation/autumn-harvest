#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Boolean code-evolution gates via `ctx.patched` / `ctx.deprecate_patch` — issue #687.
//!
//! ## The problem
//!
//! Changing an in-flight workflow's logic is the classic durable-execution
//! footgun: executions that recorded history under the old code must keep
//! replaying the old branch, while new executions should take the new one.
//! `ctx.version()` solves this with integer bookkeeping (`min`/`max`,
//! `>= 2` comparisons) that is overkill for the overwhelmingly common case:
//! a change with exactly **two** states — before and after.
//!
//! ## The solution: a three-deploy lifecycle
//!
//! **Deploy 1 — introduce.** Fence the change behind `ctx.patched(id)`:
//!
//! ```rust,ignore
//! if ctx.patched("discount-v2") {
//!     ctx.execute_activity(&compute_price_v2_info(), input).await?  // new
//! } else {
//!     ctx.execute_activity(&compute_price_v1_info(), input).await?  // old
//! }
//! ```
//!
//! New executions record a `patch:discount-v2` marker (the same
//! `MarkerRecorded` event `ctx.version()` uses — no new event variant, no
//! migration) and take the new branch. Pre-patch executions replay the old
//! branch deterministically.
//!
//! **Deploy 2 — deprecate.** Once every *pre-patch* run has drained (see the
//! "Patched gates" section of `docs/runbooks/version-gate-retirement.md` —
//! the `version-usage`/retirement-check CLI tooling does NOT see `patch:`
//! markers; use that section's raw SQL drain queries), drop the old branch
//! and make the recorded markers transparent:
//!
//! ```rust,ignore
//! ctx.deprecate_patch("discount-v2");
//! ctx.execute_activity(&compute_price_v2_info(), input).await?  // unconditional
//! ```
//!
//! **Deploy 3 — remove.** Once every *marker-bearing* run has drained,
//! delete the `deprecate_patch` call. Nothing remains of the gate.
//!
//! ## Semantics
//!
//! | Property | Behaviour |
//! |---|---|
//! | **No new event variant** | `patch:{id}` rides the existing `MarkerRecorded` event; the append-only event-JSON contract is untouched. |
//! | **`version()` interop** | A history that recorded `version:{id}` under the old API is observed as patched — you can migrate a two-version `ctx.version(id, 1, 2)` gate to `ctx.patched(id)` in place. |
//! | **Deprecation memo** | After `deprecate_patch(id)`, a residual `patched(id)` call stays deterministic (`true` for marker-bearing histories, `false` for pre-patch ones)… |
//! | **…but is a footgun** | …a residual `patched(id)` also returns `false` for NEW executions started post-deprecation, and records nothing. Delete the residual call when you deprecate. |
//! | **Multi-version escape hatch** | `ctx.version()` remains for gates with more than two concurrent versions. |
//!
//! A too-early deploy 3 (deleting the `patched()` call while marker-bearing
//! runs are still in flight) is surfaced by `WorkflowReplayer` as
//! `NonDeterminismKind::PatchMarkerMismatch`.
//!
//! Run with:
//!   cargo run --example patched_workflow

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    total_cents: u64,
}

// ---------------------------------------------------------------------------
// Deploy 1: the gated workflow. In real life `apply_discount_v2` below is the
// SAME function after its next edit — two functions here only so one example
// binary can show both phases side by side.
// ---------------------------------------------------------------------------

/// Phase-1 workflow: pre-patch runs replay `compute_price_v1`; new runs
/// record the `patch:discount-v2` marker and run `compute_price_v2`.
#[workflow]
async fn apply_discount(ctx: &WorkflowContext, order: Order) -> Result<u64, String> {
    let price: u64 = if ctx.patched("discount-v2") {
        ctx.execute_activity(&compute_price_v2_info(), order)
            .await
            .map_err(|e| e.to_string())?
    } else {
        ctx.execute_activity(&compute_price_v1_info(), order)
            .await
            .map_err(|e| e.to_string())?
    };
    Ok(price)
}

/// Phase-2 workflow (deploy 2): every pre-patch run has drained, so the old
/// branch is gone. `deprecate_patch` makes the recorded markers transparent
/// wherever they sit, so phase-1 histories still replay cleanly.
#[workflow]
async fn apply_discount_v2(ctx: &WorkflowContext, order: Order) -> Result<u64, String> {
    ctx.deprecate_patch("discount-v2");
    let price: u64 = ctx
        .execute_activity(&compute_price_v2_info(), order)
        .await
        .map_err(|e| e.to_string())?;
    Ok(price)
}

#[activity(start_to_close = "10s")]
async fn compute_price_v1(_ctx: &ActivityContext, order: Order) -> Result<u64, String> {
    // Old pricing: flat 5% discount.
    Ok(order.total_cents * 95 / 100)
}

#[activity(start_to_close = "10s")]
async fn compute_price_v2(_ctx: &ActivityContext, order: Order) -> Result<u64, String> {
    // New pricing: 10% discount over 100_00 cents.
    if order.total_cents > 100_00 {
        Ok(order.total_cents * 90 / 100)
    } else {
        Ok(order.total_cents)
    }
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _wfs = workflows![apply_discount, apply_discount_v2];
    let _acts = activities![compute_price_v1, compute_price_v2];
    println!("patched_workflow example compiled successfully");
    println!();
    println!("Lifecycle:");
    println!("  deploy 1: if ctx.patched(\"discount-v2\") {{ new }} else {{ old }}");
    println!("  deploy 2: ctx.deprecate_patch(\"discount-v2\"); + unconditional new code");
    println!("  deploy 3: delete the call entirely");
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use serde_json::json;

    fn order() -> serde_json::Value {
        json!({"order_id": "ord_1", "total_cents": 200_00})
    }

    /// Deploy 1, new execution: the patch marker is recorded and the new
    /// branch (compute_price_v2) runs.
    #[tokio::test]
    async fn phase1_new_run_records_marker_and_takes_new_branch() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("compute_price_v2", |_| Ok(json!(180_00)))
            .run(apply_discount_info().handler, order())
            .await;

        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
        assert_eq!(outcome.result.as_ref().unwrap(), &json!(180_00));

        let markers: Vec<_> = outcome
            .events()
            .iter()
            .filter(|e| {
                matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name == "patch:discount-v2")
            })
            .collect();
        assert_eq!(
            markers.len(),
            1,
            "exactly one patch marker must be recorded"
        );
    }

    /// Deploy 1's own history replays cleanly against deploy 1's code — the
    /// falsifiable replay-safety bar every code-evolution primitive must meet.
    #[tokio::test]
    async fn phase1_history_replays_cleanly_against_phase1_code() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("compute_price_v2", |_| Ok(json!(180_00)))
            .run(apply_discount_info().handler, order())
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let report = outcome.replay_check(apply_discount_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "patched() must be replay-safe:\n{report}"
        );
    }

    /// Deploy 2: the phase-2 code (deprecate_patch + unconditional new
    /// branch) replays a phase-1 (marker-bearing) history cleanly, even
    /// though the marker no longer has a positional `patched()` call.
    #[tokio::test]
    async fn phase2_deprecated_handler_replays_phase1_history() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("compute_price_v2", |_| Ok(json!(180_00)))
            .run(apply_discount_info().handler, order())
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        // Replay the phase-1 history against the phase-2 handler.
        let report = outcome.replay_check(apply_discount_v2_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "deprecate_patch must keep phase-1 histories replayable:\n{report}"
        );
    }

    /// Deploy 2 emits nothing durable: a new execution of the phase-2 code
    /// records no marker at all.
    #[tokio::test]
    async fn phase2_new_run_records_no_marker() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("compute_price_v2", |_| Ok(json!(180_00)))
            .run(apply_discount_v2_info().handler, order())
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        assert!(
            !outcome
                .events()
                .iter()
                .any(|e| matches!(e, WorkflowEvent::MarkerRecorded { .. })),
            "deprecate_patch must record nothing on a fresh execution"
        );
    }
}
