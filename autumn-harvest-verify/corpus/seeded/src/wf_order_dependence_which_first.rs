//! **S18** — ordering dependence: an ambient bool decides *which activity runs
//! first*.
//!
//! **Mechanism.** `Control`. `helpers::hot_path()` is
//! `ROUND.load(SeqCst) % 2 == 0` on a `static AtomicUsize`. Both branches emit
//! the same two commands with byte-identical arguments; only the **sequence**
//! differs. That alone diverges replay, because history is an ordered log —
//! which is exactly why the verdict has to be about the command *sequence*, not
//! about argument values.
//!
//! **Launder.** No argument anywhere is tainted, so a value-only taint model
//! reports nothing. Syntactically: HVG007 needs `.lock()` on an uppercase
//! receiver in the body (this is `.load()`, in another crate); DET has no
//! static/atomic rule at all.
//!
//! **Expected trace hops.** `wf_order_dependence_which_first` ⇒
//! `harvest_verify_corpus_helpers::hot_path` ⇒ `AtomicUsize::load` on
//! `static ROUND` (the source) ⇒ `switchInt` ⇒ both
//! `WorkflowContext::execute_activity_raw` sinks, in their branch regions.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Runs the two stages, hot stage first when the worker is warm.
#[workflow]
pub async fn wf_order_dependence_which_first(
    ctx: &WorkflowContext,
    _job: String,
) -> Result<bool, String> {
    let hot = helpers::hot_path();
    if hot {
        ctx.execute_activity_raw("alpha", serde_json::json!({ "n": 1 }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("beta", serde_json::json!({ "n": 2 }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
    } else {
        ctx.execute_activity_raw("beta", serde_json::json!({ "n": 2 }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("alpha", serde_json::json!({ "n": 1 }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(hot)
}
