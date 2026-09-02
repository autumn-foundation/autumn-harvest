//! **S04 / AC3-mandatory #4** — `SystemTime::now()` two crates deep.
//!
//! **Mechanism.** `Value`. `helpers::batch_label()` formats the result of
//! `helpers_deep::stamp()`, which is
//! `SystemTime::now().duration_since(UNIX_EPOCH)…as_secs()`. The label becomes
//! the child workflow's input, so the recorded `StartChildWorkflow` command
//! carries a wall-clock value and diverges on every replay.
//!
//! **Launder.** HVG001 knows `SystemTime::now` exactly — and only inside the
//! annotated body, which contains no such token. DET001 knows the same spelling
//! and resolves one hop, but only to a helper declared in the **same file and
//! same module path** (`resolve_helper`); `batch_label` is one crate away and
//! `stamp` is two. Neither layer follows a call across a crate boundary, so the
//! best-known non-determinism source in the catalog sails straight through.
//!
//! **Expected trace hops.** `wf_systemtime_two_crates_deep` ⇒
//! `harvest_verify_corpus_helpers::batch_label` ⇒
//! `harvest_verify_corpus_helpers_deep::stamp` ⇒ `SystemTime::now` (the source)
//! ⇒ return ⇒ `WorkflowContext::spawn_child_workflow_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Starts a child batch whose id embeds a wall-clock label.
#[workflow]
pub async fn wf_systemtime_two_crates_deep(
    ctx: &WorkflowContext,
    tenant: String,
) -> Result<String, String> {
    let label = helpers::batch_label();
    ctx.spawn_child_workflow_raw(
        "corpus_child",
        serde_json::json!({ "tenant": tenant, "label": label }),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(label)
}
