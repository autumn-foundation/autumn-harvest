//! **S24** — a forbidden *effect*, not a taint flow: `tokio::time::sleep` in a
//! helper.
//!
//! **Mechanism.** Forbidden effect. `helpers::pace()` awaits
//! `tokio::time::sleep`. Nothing about the sleep flows into any command
//! argument or branch condition, so taint analysis alone reports **nothing** —
//! this is the case that proves the analyzer needs a reachability-only
//! `[forbidden]` table beside its source table. The sleep is non-durable: it
//! re-executes on every replay instead of being skipped after the first
//! completion, and it is invisible to the replay engine.
//!
//! **Launder.** HVG004 lists `tokio::time::sleep` and DET006 lists
//! `tokio::time::sleep(` — again both know the call exactly, and again both are
//! body-only / same-file-same-module, so one crate boundary defeats them.
//!
//! **Expected trace hops.** `wf_tokio_sleep_in_helper` ⇒
//! `harvest_verify_corpus_helpers::pace` ⇒ `tokio::time::sleep` (the forbidden
//! effect). The trace is a pure call chain — there is no source→sink flow to
//! report.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Throttles between two stages by pausing the task.
#[workflow]
pub async fn wf_tokio_sleep_in_helper(ctx: &WorkflowContext, job: String) -> Result<u8, String> {
    ctx.execute_activity_raw("stage_one", serde_json::json!({ "job": job }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    helpers::pace().await;
    ctx.execute_activity_raw("stage_two", serde_json::json!({ "job": job }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(2)
}
