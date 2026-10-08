//! **S10** — `HashSet` iteration hidden behind a *trait method*.
//!
//! **Mechanism.** `Order`. `impl Plan for HashSet<String>` in the helpers crate
//! implements `steps()` as `self.iter().cloned().collect()`. The workflow calls
//! `set.steps()` and dispatches one activity per step, so the recorded command
//! order is hash-seeded.
//!
//! **Launder.** The call site in MIR is `<HashSet<String> as Plan>::steps` — a
//! *trait* call, not a free function — so resolving it needs the `<impl at
//! file:l:c>` body header mapped back through syn to `(self_ty, trait,
//! method)`. Syntactically: HVG011 and DET010 only flag a `for … in` over a
//! locally-bound hash ident, and the loop here iterates a `Vec<String>`
//! returned by a method; the `iter()` that actually causes the trouble is in
//! another crate inside a trait impl. No DET substring table mentions
//! `HashSet`.
//!
//! **Expected trace hops.** `wf_hashset_via_trait_method` ⇒
//! `<HashSet<String> as harvest_verify_corpus_helpers::Plan>::steps` ⇒
//! `<&HashSet<String> as IntoIterator>::into_iter` (the Order source) ⇒ return
//! ⇒ loop element ⇒ `WorkflowContext::execute_activity_raw`.

use std::collections::HashSet;

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers::Plan as _;

/// Runs each step of a plan that arrived as an unordered set.
#[workflow]
pub async fn wf_hashset_via_trait_method(
    ctx: &WorkflowContext,
    names: Vec<String>,
) -> Result<usize, String> {
    let set: HashSet<String> = names.into_iter().collect();
    let steps = set.steps();
    for step in &steps {
        ctx.execute_activity_raw("run_step", serde_json::json!({ "step": step }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(steps.len())
}
