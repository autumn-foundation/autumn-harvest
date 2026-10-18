//! **C18** — a `Saga` step with a compensation.
//!
//! `Saga::step` is an `async fn` of the engine, so its future has no body
//! here. MIR prints its type trimmed, as `{async fn body of Saga<'_>::step()}`.
//! The analyzer once reported polling it as an external body. A `let`
//! declaration names the type in full, as `autumn_harvest::Saga<'_>`, a
//! trusted crate. Both closures are analyzed where they are passed. Verdict:
//! `proven-deterministic`.

use autumn_harvest::Saga;
use autumn_harvest::prelude::*;

/// Reserves stock, and releases it when a later step fails.
#[workflow]
pub async fn wf_saga_step(ctx: &WorkflowContext, sku: String) -> Result<u64, String> {
    let mut saga = Saga::new(ctx);
    let reserved = saga
        .step::<u64, serde_json::Value, _, _, _, _>(
            || async {
                ctx.execute_activity_raw("reserve", serde_json::json!({ "sku": sku }), "corpus")
                    .await
                    .map(|_| 1)
            },
            |count| async move {
                ctx.execute_activity_raw("release", serde_json::json!({ "count": count }), "corpus")
                    .await
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(reserved)
}
