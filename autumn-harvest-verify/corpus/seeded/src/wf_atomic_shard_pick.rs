//! **S09** — an atomic round-robin cursor selects *which* activity is emitted.
//!
//! **Mechanism.** `Control`. `helpers::shard_of(3)` is
//! `ROUND.fetch_add(1, SeqCst) % 3` on a `static AtomicUsize`. Every argument
//! at every sink in this workflow is a **constant**, so a value-only taint
//! model reports nothing; only control-dependence — a `switchInt` on a tainted
//! operand, with sinks in branch regions that do not post-dominate the branch —
//! finds it.
//!
//! **Launder.** Nothing in this body reads anything ambient; the `match`
//! scrutinee is a plain `usize` local. HVG has no static/atomic rule (HVG007
//! covers only `.lock()` on an uppercase receiver in the body) and DET has
//! none; and the read is one crate away regardless.
//!
//! **Expected trace hops.** `wf_atomic_shard_pick` ⇒
//! `harvest_verify_corpus_helpers::shard_of` ⇒ `AtomicUsize::fetch_add` on
//! `static ROUND` (the source) ⇒ return ⇒ `switchInt` on the tainted shard ⇒
//! `WorkflowContext::execute_activity_raw` in a non-post-dominating branch.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Routes the job to one of three shards.
#[workflow]
pub async fn wf_atomic_shard_pick(ctx: &WorkflowContext, _job: String) -> Result<u8, String> {
    let shard = helpers::shard_of(3);
    match shard {
        0 => {
            ctx.execute_activity_raw("shard_a", serde_json::json!({ "n": 1 }), "corpus")
                .await
                .map_err(|e| e.to_string())?;
            Ok(0)
        }
        1 => {
            ctx.execute_activity_raw("shard_b", serde_json::json!({ "n": 1 }), "corpus")
                .await
                .map_err(|e| e.to_string())?;
            Ok(1)
        }
        _ => {
            ctx.execute_activity_raw("shard_c", serde_json::json!({ "n": 1 }), "corpus")
                .await
                .map_err(|e| e.to_string())?;
            Ok(2)
        }
    }
}
