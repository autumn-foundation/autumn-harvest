//! **S34** — a command emitted inside a recursive cycle.
//!
//! **Mechanism.** `Value` into a sink that only a recursive call reaches with
//! taint. `relay` emits an activity with its `stamp` argument, then calls
//! itself with `fine_stamp()` as the next stamp. The first call passes a clean
//! `0`, so the sink is clean in the outer activation. Only the recursive
//! activation carries the clock. The analyzer once cut the cycle there and
//! reported no sink inside it, so this was `unknown`. The fixpoint reruns
//! `relay` with the joined argument taint, and the sink is then tainted.
//!
//! **Launder.** `fine_stamp` is two crates away, past det_check's one hop, and
//! HVG never leaves the annotated body. The recursion is the boxed `async fn`
//! shape that a delegating agent loop uses.
//!
//! **Expected trace hops.** `relay` ⇒ `fine_stamp` ⇒ `SystemTime::now` ⇒ the
//! recursive call ⇒ `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers_deep as deep;

/// Emits one hop, then relays the next stamp `hops` more times.
async fn relay(ctx: &WorkflowContext, hops: u32, stamp: u64) -> Result<(), String> {
    ctx.execute_activity_raw("relay", serde_json::json!({ "stamp": stamp }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    if hops > 0 {
        Box::pin(relay(ctx, hops.saturating_sub(1), deep::fine_stamp())).await?;
    }
    Ok(())
}

/// Relays a stamp along `hops` activities.
#[workflow]
pub async fn wf_recursive_relay(ctx: &WorkflowContext, hops: u32) -> Result<(), String> {
    relay(ctx, hops, 0).await
}
