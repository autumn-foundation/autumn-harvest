//! **S17** — control dependence: a command that is *sometimes not emitted*.
//!
//! **Mechanism.** `Control`. `helpers::flag("dual_write")` consults a
//! `static OnceLock<Vec<String>>` populated from the environment on first read.
//! When the flag is on, an extra `mirror` activity is scheduled; when it is off
//! it is not. The `primary` activity is emitted unconditionally.
//!
//! **Launder.** DET004's table has `env::var(`, `env::vars(`, `env::args(` — but
//! **no** `var_os` spelling, so the underlying read escapes det_check even in a
//! same-module helper; HVG003 does cover `env::var_os`, but only inside the
//! annotated body, and this body has no env token. The memoizing `OnceLock` is
//! unmodeled by both layers. Two crate boundaries seal it.
//!
//! **Scope assertion (the point of this case).** The expected trace must name
//! **exactly one** sink: `mirror`. `primary` post-dominates the branch join and
//! is therefore *not* control-dependent on the tainted condition. An
//! implementation that flags "every sink after the branch" fails here — which is
//! precisely the regression test that forces proper post-dominance.
//!
//! **Expected trace hops.** `wf_conditional_command_emission` ⇒
//! `harvest_verify_corpus_helpers::flag` ⇒ `OnceLock::get_or_init` on
//! `static FLAGS` ⇒ `harvest_verify_corpus_helpers_deep::env_region` ⇒
//! `std::env::var_os` (the source) ⇒ `switchInt` ⇒
//! `WorkflowContext::execute_activity_raw` (`mirror` only).

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Writes to the primary store, and to the mirror when dual-write is enabled.
#[workflow]
pub async fn wf_conditional_command_emission(
    ctx: &WorkflowContext,
    record: String,
) -> Result<bool, String> {
    let dual = helpers::flag("dual_write");
    if dual {
        ctx.execute_activity_raw("mirror", serde_json::json!({ "record": record }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
    }
    ctx.execute_activity_raw("primary", serde_json::json!({ "record": record }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(dual)
}
