//! **C13** — a helper that takes `&WorkflowContext` and **emits a command
//! itself**, with clean arguments.
//!
//! Sinks are not confined to the workflow body: real code factors dispatch into
//! helpers. `helpers_ctx::dispatch_stage` lives in another module of this crate,
//! takes the context, and calls `execute_activity_raw` with arguments derived
//! only from its parameters. Reaching a sink through a helper is perfectly fine
//! — what matters is whether anything *tainted* reaches it.
//!
//! This is the case that stops the analyzer from over-approximating "a helper
//! that touches `ctx` is unanalyzable". Verdict: `proven-deterministic`.

use autumn_harvest::prelude::*;

use crate::helpers_ctx;

/// Runs both stages through a shared dispatch helper.
#[workflow]
pub async fn wf_helper_emits_clean_activity(
    ctx: &WorkflowContext,
    job: String,
) -> Result<u8, String> {
    helpers_ctx::dispatch_stage(ctx, "stage_one", &job).await?;
    helpers_ctx::dispatch_stage(ctx, "stage_two", &job).await?;
    Ok(2)
}
