#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Durable await of external workflows by `ExecutionId` (issue #757).
//!
//! Demonstrates `ctx.await_external_workflow::<T>(target)` — the third verb of
//! the external-workflow family (signal #244 / cancel #492 / **await #757**).
//! A fan-in coordinator awaits the terminal outcome of three **independently
//! started** sibling workflows (they are NOT children of the coordinator — no
//! parent/child linkage is created) and aggregates their typed results.
//!
//! Each await is a single line:
//!
//! ```ignore
//! let a: LegResult = ctx.await_external_workflow(leg_a).await?;
//! let b: LegResult = ctx.await_external_workflow(leg_b).await?;
//! let c: LegResult = ctx.await_external_workflow(leg_c).await?;
//! ```
//!
//! # Semantics (vs cancel #492)
//!
//! * Target `COMPLETED` → resolves with the **deserialized output**.
//! * Target `FAILED`/`TIMED_OUT`/`CANCELLED`/`TERMINATED` → returns a typed
//!   `Err` carrying the target's terminal cause (an OUTCOME — distinct from
//!   cancel, where an already-terminal target is a no-op success). Branch on it
//!   with `err.workflow_error_type()` / `err.workflow_details()`.
//! * Target `RUNNING`/`PAUSED` → the coordinator parks durably and resolves
//!   within one outbox poll after the target reaches any terminal state.
//! * Self-await → immediate `HarvestError::ExternalAwaitFailed { reason_code:
//!   "self_await" }`.
//! * Unknown target after the grace window → `ExternalAwaitFailed { reason_code:
//!   "target_unknown" }`.
//! * **Observe-only**: awaiting never cancels, signals, or otherwise affects the
//!   target's lifecycle.
//!
//! # Three correlated events (append-only, appended to the AWAITER's history)
//!
//! 1. `ExternalAwaitRequested { await_id, target }` — recorded on first live call.
//! 2. `ExternalAwaitResolved { await_id, output }` — target COMPLETED.
//! 3. `ExternalAwaitFailed { await_id, reason_code, .. }` — target terminal-not-completed / unknown.

use autumn_harvest::prelude::*;

/// Input: the three leg execution ids the coordinator fans in on.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct FanInInput {
    pub leg_a: String,
    pub leg_b: String,
    pub leg_c: String,
}

/// A leg's typed output.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct LegResult {
    pub region: String,
    pub total: u64,
}

fn parse_target(s: &str) -> HarvestResult<ExecutionId> {
    uuid::Uuid::parse_str(s)
        .map(ExecutionId::from_uuid)
        .map_err(|e| HarvestError::Config(format!("invalid exec id: {e}")))
}

/// Fan-in coordinator: await three independently-started legs by id and
/// aggregate their typed totals. Each straightforward await is a single line
/// (`?`), because the coordinator returns `Result<_, HarvestError>` — so the
/// typed error from `await_external_workflow` propagates directly, with no
/// `map_err(|e| e.to_string())` boilerplate. The whole aggregation is
/// deterministic and replay-safe (each leg's output is frozen into the
/// coordinator's own history).
#[workflow]
async fn fan_in_coordinator(ctx: &WorkflowContext, input: FanInInput) -> Result<u64, HarvestError> {
    let leg_a = parse_target(&input.leg_a)?;
    let leg_b = parse_target(&input.leg_b)?;
    let leg_c = parse_target(&input.leg_c)?;

    // Two one-line awaits by ExecutionId. Await OBSERVES the legs — it never
    // makes them children of the coordinator.
    let a: LegResult = ctx.await_external_workflow(leg_a).await?;
    let b: LegResult = ctx.await_external_workflow(leg_b).await?;

    // Branch on a leg's typed terminal cause: a `RegionUnavailable` failure is
    // tolerated (contributes 0), any other failure aborts the coordinator (the
    // typed error propagates unchanged).
    let c_total = match ctx.await_external_workflow::<LegResult>(leg_c).await {
        Ok(c) => c.total,
        Err(e) if e.workflow_error_type() == Some("RegionUnavailable") => 0,
        Err(e) => return Err(e),
    };

    Ok(a.total + b.total + c_total)
}

/// A leg workflow (started independently; awaited by the coordinator).
#[workflow]
async fn region_rollup(_ctx: &WorkflowContext, region: String) -> Result<LegResult, String> {
    Ok(LegResult { region, total: 100 })
}

fn main() {
    // Compiled to verify it type-checks. In production wire both workflows in:
    //
    //   HarvestBuilder::new()
    //       .workflows(workflows![fan_in_coordinator, region_rollup])
    //       .worker(WorkerConfig::default())
    //       .build();
    //
    // Start the three legs (any workflow, any shard), then start the coordinator
    // passing their execution ids:
    //   POST /api/harvest/workflows/fan_in_coordinator/start
    //   { "input": { "leg_a": "<uuid>", "leg_b": "<uuid>", "leg_c": "<uuid>" } }
    println!("await_external_workflow example — see source for usage.");
}
