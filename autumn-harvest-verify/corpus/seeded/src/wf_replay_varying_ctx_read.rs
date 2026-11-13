//! **S27** — a *replay-varying* `WorkflowContext` read, inside a helper that
//! takes `&WorkflowContext`.
//!
//! **Mechanism.** `Value`. `crate::helpers_ctx::replay_phase(ctx)` returns
//! `ctx.history_event_count()`. That is a sanctioned API. But its value is *by
//! definition* different during the live run (short history) and during a
//! replay (long history). In an activity argument, it makes the recorded
//! command depend on which cycle produced it. This case proves that the
//! `WorkflowContext` model is not "everything on `ctx` is safe". Some `ctx`
//! reads are sanctioned sources and some are non-sinks. A few are *sources* in
//! their own right: `is_replaying`, `history_event_count`, `replay_position`.
//!
//! **Launder.** Neither layer models `WorkflowContext` semantics at all. The
//! only `ctx`-aware logic in HVG is the HVG011 severity downgrade and the
//! `side_effect` closure exemption. DET has no `ctx` rules. Nothing here matches any
//! pattern in either table, so the code is invisible even written inline. The
//! helper additionally lives in a different **module and file** of this crate,
//! which is exactly where det_check's `resolve_helper` stops.
//!
//! **Expected trace hops.** `wf_replay_varying_ctx_read` ⇒
//! `harvest_verify_corpus_seeded::helpers_ctx::replay_phase` ⇒
//! `WorkflowContext::history_event_count` (the source) ⇒
//! `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;

use crate::helpers_ctx;

/// Tags the checkpoint with "where we are" in the history.
#[workflow]
pub async fn wf_replay_varying_ctx_read(
    ctx: &WorkflowContext,
    label: String,
) -> Result<u64, String> {
    let phase = helpers_ctx::replay_phase(ctx);
    ctx.execute_activity_raw(
        "checkpoint",
        serde_json::json!({ "label": label, "phase": phase }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(phase)
}
