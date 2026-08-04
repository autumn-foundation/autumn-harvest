#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Signal/cancel a long-lived, continue-as-new-ing child addressed *only* by
//! `workflow_id` (issue #751).
//!
//! Every external-workflow primitive that existed before this issue —
//! `signal_external_workflow` (#244), `request_cancel_external_workflow`
//! (#492), `await_external_workflow` (#757) — targets an `ExecutionId`. That
//! is exactly wrong for a long-lived *entity* workflow that periodically
//! `continue_as_new`s to keep its history bounded (a device session, a
//! per-tenant job queue, a subscription): the entity's `ExecutionId` changes
//! on every continue-as-new, but its business `workflow_id` never does. A
//! supervisor that stored the entity's `ExecutionId` at the moment it first
//! started would silently signal a **sealed, `CONTINUED_AS_NEW` predecessor**
//! after the very first rotation — the signal is durably recorded as
//! delivered, and the running successor never sees it.
//!
//! This example demonstrates the fix: `ctx.signal_external_workflow_by_id`
//! and `ctx.request_cancel_external_workflow_by_id` resolve to *the current
//! run* — the latest non-terminal execution of `(workflow_name, workflow_id)`
//! **at delivery time**, not at request time — so the supervisor only ever
//! needs to remember the entity's stable business key.
//!
//! # Key semantics
//!
//! * **Continue-as-new race closed by construction** (AC3): if the entity
//!   continues-as-new between the supervisor's call and delivery, resolution
//!   re-runs and finds the live successor. Zero misdeliveries across any
//!   interleaving — see the randomized tests in
//!   `tests/integration/workflow_id_targeted_tests.rs`.
//! * **Signal to a `workflow_id` with no live run** → typed failure after the
//!   existing outbox grace window (`reason_code = "not_running"` when the
//!   *current* run is terminal, `"target_unknown"` when no run has ever
//!   existed) — never silently dropped.
//! * **Cancel of an already-terminal `workflow_id`** → no-op success (the
//!   goal — "not running" — is already met).
//! * **Self-targeting** (own `workflow_name`/`workflow_id`) → immediate typed
//!   failure (`"self_signal"` / `"self_cancel"`), no DB round trip.
//! * Cross-shard: resolution routes to the owning shard via the same
//!   `(workflow_name, workflow_id)` rendezvous hash a fresh start would use —
//!   same-shard delivers inline, cross-shard uses the existing outbox
//!   (reusing the #492 same-shard/outbox split).
//!
//! # Two correlated events per call (append-only, appended to the CALLER's
//! history — unchanged from the `ExecutionId`-targeted primitives)
//!
//! 1. `ExternalSignalRequested` / `ExternalCancelRequested` — recorded on
//!    first live call. The `target` field is the widened `ExternalTarget`
//!    enum (`ExecutionId` | `WorkflowId { workflow_name, workflow_id }`), so
//!    stored pre-#751 history — which only ever recorded `ExecutionId`
//!    targets — deserializes and replays unchanged.
//! 2. `ExternalSignalDelivered`/`Failed` or `ExternalCancelDelivered`/`Failed`
//!    — the terminal outcome, replayed verbatim on every later pass.

use autumn_harvest::prelude::*;

/// A job handed to the pool.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Job {
    pub job_id: String,
}

/// `job_pool` is a long-lived entity workflow: it accumulates jobs delivered
/// by signal, and once it has processed `ROTATE_AFTER` of them it
/// `continue_as_new`s to keep its event history bounded. Its `workflow_id`
/// (e.g. `"tenant-42"`) is stable forever; its `ExecutionId` is a fresh UUID
/// after every rotation.
const ROTATE_AFTER: u32 = 3;

#[workflow]
async fn job_pool(ctx: &WorkflowContext, processed_so_far: u32) -> Result<(), String> {
    let mut processed = processed_so_far;
    loop {
        // Block until the supervisor delivers a job — addressed by
        // `workflow_id`, so it doesn't matter which rotation of this entity
        // is currently live.
        let job: Job = ctx
            .receive_signal("add_job")
            .await
            .map_err(|e| e.to_string())?;
        let _ = job; // ... process the job ...
        processed += 1;

        if processed >= ROTATE_AFTER {
            // Reset history, carrying the running count forward. The
            // successor gets a brand-new ExecutionId; its workflow_id is
            // unchanged. `continue_as_new` never returns.
            ctx.continue_as_new(serde_json::json!(0))
                .await
                .map_err(|e| e.to_string())?;
            unreachable!("continue_as_new suspends forever on a live run");
        }
    }
}

/// `pool_supervisor` never learns, stores, or reasons about `job_pool`'s
/// `ExecutionId` — only its stable `workflow_id`. It can dispatch jobs and,
/// independently, decide to decommission the pool, at any point in the
/// pool's lifetime, across any number of continue-as-new rotations.
#[workflow]
async fn pool_supervisor(ctx: &WorkflowContext, pool_id: String) -> Result<String, String> {
    // Dispatch a job to whichever run of `job_pool` is currently live.
    match ctx
        .signal_external_workflow_by_id(
            "job_pool",
            &pool_id,
            "add_job",
            Job {
                job_id: "job-1".to_string(),
            },
        )
        .await
    {
        Ok(()) => {}
        Err(HarvestError::ExternalSignalFailed { reason_code, .. }) => {
            // "not_running" (the pool decommissioned) or "target_unknown"
            // (no pool with this id was ever started) — either way this is a
            // durable, typed, never-silently-dropped answer.
            return Ok(format!(
                "pool {pool_id} did not accept the job: {reason_code}"
            ));
        }
        Err(e) => return Err(e.to_string()),
    }

    // ... later, independently, decommission the pool — addressed by the
    // SAME `workflow_id`, regardless of how many times it has rotated in
    // between. An already-terminal pool is a no-op success (goal already
    // met); a genuinely unknown id fails after the grace window.
    match ctx
        .request_cancel_external_workflow_by_id("job_pool", &pool_id)
        .await
    {
        Ok(()) => Ok(format!(
            "pool {pool_id}: job dispatched and pool decommissioned"
        )),
        Err(HarvestError::ExternalCancelFailed { reason_code, .. }) => Ok(format!(
            "pool {pool_id}: job dispatched; decommission failed: {reason_code}"
        )),
        Err(e) => Err(e.to_string()),
    }
}

fn main() {
    // Compiled to verify it type-checks. In production wire both workflows in:
    //
    //   HarvestBuilder::new()
    //       .workflows(workflows![job_pool, pool_supervisor])
    //       .worker(WorkerConfig::default())
    //       .build();
    //
    // Start the long-lived entity once, keyed by its stable business id:
    //   POST /api/harvest/workflows/job_pool/start
    //   { "workflow_id": "tenant-42", "input": 0 }
    //
    // The supervisor addresses it purely by that same `workflow_id`, no
    // matter how many times it has continued-as-new since:
    //   POST /api/harvest/workflows/pool_supervisor/start
    //   { "input": "tenant-42" }
    println!("workflow_id_addressed_supervisor example — see source for usage.");
}
