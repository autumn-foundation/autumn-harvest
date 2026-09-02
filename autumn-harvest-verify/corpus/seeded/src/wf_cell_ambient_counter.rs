//! **S06** — a `Cell` thread-local bumped in a helper.
//!
//! **Mechanism.** `Value`. `helpers::bump_hits()` reads and writes
//! `thread_local! { static HITS: Cell<u32> }` through `LocalKey::with`. The hit
//! count depends on how much other work this worker thread already did, and it
//! is written into the activity's input.
//!
//! **Launder.** `Cell` and `LocalKey::with` appear in no HVG rule and no DET
//! pattern table — the ambient-state surface both layers model is exactly one
//! rule (HVG007, `.lock()` on an uppercase receiver in the body). The read is
//! also one crate away, which independently defeats det_check's same-module
//! one-hop resolver and the body-only HVG visitor.
//!
//! **Expected trace hops.** `wf_cell_ambient_counter` ⇒
//! `harvest_verify_corpus_helpers::bump_hits` ⇒ `LocalKey::with` on `HITS` ⇒
//! `bump_hits::{closure#0}` ⇒ `Cell::<u32>::get` (the source) ⇒
//! `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Records a per-thread hit count alongside the payload.
#[workflow]
pub async fn wf_cell_ambient_counter(
    ctx: &WorkflowContext,
    payload: String,
) -> Result<u32, String> {
    let hits = helpers::bump_hits();
    ctx.execute_activity_raw(
        "record",
        serde_json::json!({ "payload": payload, "hits": hits }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(hits)
}
