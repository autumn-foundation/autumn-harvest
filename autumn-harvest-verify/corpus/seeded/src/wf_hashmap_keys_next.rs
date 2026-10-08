//! **S12** — positional selection over hash order (`keys().next()`).
//!
//! **Mechanism.** `Order` collapsing into `Value`. `helpers::any_key` returns
//! `map.keys().next().cloned()` — "any key" in the author's head, "whichever key
//! this process's `RandomState` put first" in reality. That key becomes the
//! child workflow's input, so the `StartChildWorkflow` command differs between
//! the live run and a replay in a different process.
//!
//! **Launder.** The map arrives at `any_key` as a **function parameter**, which
//! HVG011's `local_binding_target` documents as never tracked, and DET010's
//! line scanner likewise only tracks `let` bindings. Neither rule looks at
//! `keys()` outside a `for … in` header anyway. And `any_key` is one crate away
//! from the workflow, past det_check's same-file/same-module one-hop resolver.
//!
//! **Expected trace hops.** `wf_hashmap_keys_next` ⇒
//! `harvest_verify_corpus_helpers::any_key` ⇒ `HashMap::<String, u32>::keys`
//! (the Order source) ⇒ `Iterator::next` (Order → Value by positional
//! selection) ⇒ return ⇒ `WorkflowContext::spawn_child_workflow_raw`.

use std::collections::HashMap;

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Delegates the first available tenant to a child workflow.
#[workflow]
pub async fn wf_hashmap_keys_next(
    ctx: &WorkflowContext,
    quotas: HashMap<String, u32>,
) -> Result<String, String> {
    let Some(tenant) = helpers::any_key(&quotas) else {
        return Ok(String::new());
    };
    ctx.spawn_child_workflow_raw("corpus_child", serde_json::json!({ "tenant": tenant }))
        .await
        .map_err(|e| e.to_string())?;
    Ok(tenant)
}
