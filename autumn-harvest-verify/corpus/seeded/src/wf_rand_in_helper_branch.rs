//! **S25** — `rand::random` in a helper decides whether a command is emitted at
//! all.
//!
//! **Mechanism.** `Value` → `Control`. `helpers::coin()` is
//! `rand::random::<bool>()`. When it comes up heads a `sample` activity is
//! scheduled; when it comes up tails no command is emitted at all. The two runs
//! record histories of different *lengths*.
//!
//! **Launder.** HVG002 and DET002 both list `rand::random`; both are body-only
//! / same-file-same-module, and `coin` is one crate away. Note also that the
//! tainted value never reaches an argument — only a branch — so even a
//! value-only taint model that *did* cross the crate boundary would report
//! nothing here.
//!
//! **Expected trace hops.** `wf_rand_in_helper_branch` ⇒
//! `harvest_verify_corpus_helpers::coin` ⇒ `rand::random` (the source) ⇒
//! return ⇒ `switchInt` ⇒ `WorkflowContext::execute_activity_raw` in the
//! non-post-dominating branch.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Samples a fraction of jobs for deep inspection.
#[workflow]
pub async fn wf_rand_in_helper_branch(ctx: &WorkflowContext, job: String) -> Result<bool, String> {
    let sampled = helpers::coin();
    if sampled {
        ctx.execute_activity_raw("sample", serde_json::json!({ "job": job }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(sampled)
}
