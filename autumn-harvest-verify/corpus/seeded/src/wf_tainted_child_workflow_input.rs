//! **S16** — an ambient value wrapped in a serde struct and sent to a child.
//!
//! **Mechanism.** `Value`. `helpers::seed()` is `SEQ.load(Relaxed)` on a
//! `static AtomicU64`. The value is stored in a field of `ChildRequest`, the
//! struct is serialized, and the JSON becomes the `StartChildWorkflow`
//! command's input — so taint must survive a struct aggregate assignment and a
//! by-value move, not just a scalar rename.
//!
//! **Launder.** As in S02: HVG007 (the only ambient rule) needs a literal
//! `.lock()` on an uppercase receiver **in the body**, `SEQ.load` is neither, and
//! it is one crate away past det_check's same-module one-hop resolver.
//!
//! **Expected trace hops.** `wf_tainted_child_workflow_input` ⇒
//! `harvest_verify_corpus_helpers::seed` ⇒ `AtomicU64::load` on `static SEQ`
//! (the source) ⇒ `ChildRequest` aggregate ⇒ `serde_json::to_value` ⇒
//! `WorkflowContext::spawn_child_workflow_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;
use serde::{Deserialize, Serialize};

/// Input handed to the child workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildRequest {
    /// The tenant to process.
    pub tenant: String,
    /// An ambient sequence value, believed by the author to be a harmless tag.
    pub seed: u64,
}

/// Delegates a tenant to a child workflow with an ambient seed attached.
#[workflow]
pub async fn wf_tainted_child_workflow_input(
    ctx: &WorkflowContext,
    tenant: String,
) -> Result<u64, String> {
    let request = ChildRequest {
        tenant,
        seed: helpers::seed(),
    };
    let seed = request.seed;
    let input = serde_json::to_value(request).map_err(|e| e.to_string())?;
    ctx.spawn_child_workflow_raw("corpus_child", input)
        .await
        .map_err(|e| e.to_string())?;
    Ok(seed)
}
