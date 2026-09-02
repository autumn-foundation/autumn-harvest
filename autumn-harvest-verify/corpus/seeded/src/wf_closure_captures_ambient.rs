//! **S19** — a closure whose *body* reads ambient state, invoked in another
//! crate.
//!
//! **Mechanism.** `Value`. The workflow builds `|n| n * helpers::factor()` and
//! hands it to `helpers::apply_all`, which maps it over the items. The
//! multiplier comes from `static FACTOR: AtomicU32`, so every dispatched
//! activity argument is ambient.
//!
//! **Launder.** The closure body is a **separate MIR item**
//! (`wf_closure_captures_ambient::{closure#0}`) reached only through
//! `<{closure@…} as Fn<(u32,)>>::call` from a *different crate*. No syntactic
//! walker can follow that edge: HVG only ever visits the annotated item, and
//! det_check's one-hop resolver matches a bare call to a same-module free
//! function by name — a closure has no name to match. `FACTOR.load` is
//! unmodeled by both layers regardless.
//!
//! **Expected trace hops.** `wf_closure_captures_ambient` ⇒
//! `harvest_verify_corpus_helpers::apply_all` ⇒
//! `wf_closure_captures_ambient::{closure#0}` (closure-body resolution) ⇒
//! `harvest_verify_corpus_helpers::factor` ⇒ `AtomicU32::load` on
//! `static FACTOR` (the source) ⇒ mapped element ⇒
//! `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Scales every quantity by an ambient factor, then dispatches.
#[workflow]
pub async fn wf_closure_captures_ambient(
    ctx: &WorkflowContext,
    items: Vec<u32>,
) -> Result<usize, String> {
    let scale = |n: u32| n.wrapping_mul(helpers::factor());
    let scaled = helpers::apply_all(items, scale);
    for value in &scaled {
        ctx.execute_activity_raw("scaled", serde_json::json!({ "value": value }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(scaled.len())
}
