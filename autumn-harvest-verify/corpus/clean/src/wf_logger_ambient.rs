//! **C09** — an ambient value formatted into a **replay-aware log line**.
//!
//! `ctx.logger()` is suppressed during replay and pushes no durable command, so
//! it is a non-sink exactly like `ctx.metrics()`. Interpolating
//! `helpers::next_seq()` (a `static AtomicU64` read) into the message changes
//! nothing about the recorded history.
//!
//! Note the contrast with DET009/HVG009, which flag a *bare* `tracing::info!`
//! in a workflow body precisely because it is **not** replay-aware. Using the
//! blessed logger is the fix, and the fix must not be flagged. Verdict:
//! `proven-deterministic`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Logs progress with an ambient sequence number, then does durable work.
#[workflow]
pub async fn wf_logger_ambient(ctx: &WorkflowContext, job: String) -> Result<u8, String> {
    let seq = helpers::next_seq();
    ctx.logger().info(&format!("starting {job} (seq {seq})"));
    ctx.execute_activity_raw("run", serde_json::json!({ "job": job }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(1)
}
