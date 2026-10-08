//! **U02** — an indirect call through a function pointer.
//!
//! **Boundary.** `indirect-call`.
//!
//! `helpers::picker(drift)` returns a `fn(&[u32]) -> u32`. At the call site MIR
//! emits `_0 = move _1(move _2)` — the callee operand is a *local*, not a path,
//! so there is no callee name to resolve and no body to summarize. One of the
//! two possible targets reads the wall clock; the other does not.
//!
//! Unlike `dyn` dispatch there is not even a trait to enumerate implementors of,
//! so this boundary is strictly harder than U01. `unknown` is the only honest
//! verdict.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Picks a representative sample with a pluggable policy.
#[workflow]
pub async fn wf_fn_pointer(ctx: &WorkflowContext, samples: Vec<u32>) -> Result<u32, String> {
    let pick = helpers::picker(samples.len() % 2 == 0);
    let chosen = pick(&samples);
    ctx.execute_activity_raw("inspect", serde_json::json!({ "chosen": chosen }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(chosen)
}
