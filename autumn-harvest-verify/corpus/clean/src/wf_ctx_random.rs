//! **C02** — `ctx.random_u64()` and `ctx.random_uuid(label)`.
//!
//! Both lower onto a recorded side-effect event, so the drawn value is captured
//! once and replayed. Sanctioned sources whose returns are clean. Verdict:
//! `proven-deterministic`.

use autumn_harvest::prelude::*;

/// Assigns a durable sampling bucket and idempotency key.
#[workflow]
pub async fn wf_ctx_random(ctx: &WorkflowContext, job: String) -> Result<String, String> {
    let bucket = ctx.random_u64() % 100;
    let key = ctx.random_uuid("submit_key").map_err(|e| e.to_string())?;
    ctx.execute_activity_raw(
        "submit",
        serde_json::json!({ "job": job, "bucket": bucket, "key": key.to_string() }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(key.to_string())
}
