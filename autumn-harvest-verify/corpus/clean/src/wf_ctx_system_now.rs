//! **C01** — `ctx.system_now()` feeding an activity argument.
//!
//! The wall clock is read through the sanctioned primitive, which records the
//! instant in the event history on the live run and replays it verbatim. The
//! value is history-derived, so it is a **clean root**, not a source. Verdict:
//! `proven-deterministic`.

use autumn_harvest::prelude::*;

/// Stamps the receipt with the recorded execution time.
#[workflow]
pub async fn wf_ctx_system_now(ctx: &WorkflowContext, order: String) -> Result<i64, String> {
    let now = ctx.system_now();
    let ts = now.timestamp();
    ctx.execute_activity_raw(
        "receipt",
        serde_json::json!({ "order": order, "ts": ts }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(ts)
}
