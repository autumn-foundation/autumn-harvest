//! **S26** — a three-hop call chain with the source at the leaf.
//!
//! **Mechanism.** `Value` at depth three. `wf_three_hop_chain` calls
//! `helpers::a`, which calls `helpers::b`, which calls
//! `helpers_deep::fine_stamp` — and only that leaf reads the clock. `a` and `b`
//! are pure arithmetic pass-throughs, so the analyzer's bottom-up summaries have
//! to *compose*: `fine_stamp` is a source, `b`'s return is `FromSource`, `a`'s
//! return is `FromParam`-plus-`FromSource`, and the workflow's activity argument
//! is tainted only if all three summaries chain correctly.
//!
//! **Launder.** AC3 asks for "at least one helper-function layer"; this is
//! three, across two crates. det_check resolves exactly one hop and only
//! same-file/same-module, so even the *first* link is out of reach; HVG never
//! leaves the annotated body. The trace must render every intermediate hop, not
//! just source and sink.
//!
//! **Expected trace hops.** `wf_three_hop_chain` ⇒
//! `harvest_verify_corpus_helpers::a` ⇒ `harvest_verify_corpus_helpers::b` ⇒
//! `harvest_verify_corpus_helpers_deep::fine_stamp` ⇒ `SystemTime::now` (the
//! source) ⇒ back up ⇒ `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Derives a "nonce" for the request three calls away from the clock.
#[workflow]
pub async fn wf_three_hop_chain(ctx: &WorkflowContext, base: u64) -> Result<u64, String> {
    let nonce = helpers::a(base);
    ctx.execute_activity_raw("submit", serde_json::json!({ "nonce": nonce }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(nonce)
}
