//! **S33** — a first-party helper named like a std reduction.
//!
//! **Mechanism.** `Value` from a helper whose name shadows a model row.
//! `helpers::count` reads the clock through `fine_stamp`. The model has a bare
//! `[[reduction]] path = "count"` row for `Iterator::count`. That row once
//! applied to any callee named `count`, so the helper's body was never read
//! and the clock was lost. A bare row now applies only where the callee has no
//! body here, as a bare source row already did.
//!
//! **Launder.** `fine_stamp` is two crates away, where det_check does not
//! follow, and HVG never leaves the annotated body.
//!
//! **Expected trace hops.** `wf_helper_named_count` ⇒
//! `harvest_verify_corpus_helpers::count` ⇒ `fine_stamp` ⇒ `SystemTime::now` ⇒
//! `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Submits the size of the batch.
#[workflow]
pub async fn wf_helper_named_count(ctx: &WorkflowContext, batch: Vec<u64>) -> Result<u64, String> {
    let size = helpers::count(&batch);
    ctx.execute_activity_raw("submit", serde_json::json!({ "size": size }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(size)
}
