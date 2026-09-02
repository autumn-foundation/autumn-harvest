//! **S20** — an environment read spelled `var_os`, in a helper.
//!
//! **Mechanism.** `Value`. `helpers::region()` calls
//! `helpers_deep::env_region("us")`, i.e. `std::env::var_os("…")`. The region
//! selects the child workflow's name, so a worker deployed with a different
//! environment records a different `StartChildWorkflow` command.
//!
//! **Launder (a pattern-table gap, not just a hop).** DET004's substring list is
//! `std::env::var(`, `env::var(`, `std::env::args(`, `env::args(`,
//! `std::env::vars(`, `env::vars(` — there is **no** `var_os` entry, so this
//! call would pass det_check even from a same-module helper. HVG003 *does* list
//! `env::var_os`, but the visitor only ever runs over the annotated body, and
//! this body has no env token. Both layers are defeated, for two different
//! reasons.
//!
//! **Expected trace hops.** `wf_env_var_os_in_helper` ⇒
//! `harvest_verify_corpus_helpers::region` ⇒
//! `harvest_verify_corpus_helpers_deep::env_region` ⇒ `std::env::var_os` (the
//! source) ⇒ return ⇒ `WorkflowContext::spawn_child_workflow_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Routes the order to the regional child workflow.
#[workflow]
pub async fn wf_env_var_os_in_helper(
    ctx: &WorkflowContext,
    order: String,
) -> Result<String, String> {
    let region = helpers::region();
    let child = format!("corpus_child_{region}");
    ctx.spawn_child_workflow_raw(&child, serde_json::json!({ "order": order }))
        .await
        .map_err(|e| e.to_string())?;
    Ok(child)
}
