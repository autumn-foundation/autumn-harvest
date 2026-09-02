//! **S27** — a *replay-varying* `WorkflowContext` read, inside a helper that
//! takes `&WorkflowContext`.
//!
//! **Mechanism.** `Value`. `crate::helpers_ctx::replay_phase(ctx)` returns
//! `ctx.history_event_count()`. That is a perfectly sanctioned API — but its
//! value is *by definition* different during the live run (history is short)
//! and during a replay (history is long). Feeding it into an activity argument
//! makes the recorded command depend on which cycle produced it. This is the
//! case that proves the `WorkflowContext` model is not simply "everything on
//! `ctx` is safe": some `ctx` reads are sanctioned sources, some are non-sinks,
//! and a few — `is_replaying`, `history_event_count`, `replay_position` — are
//! *sources* in their own right.
//!
//! **Launder.** Neither layer models `WorkflowContext` semantics at all: HVG's
//! only `ctx`-aware logic is HVG011's severity downgrade and the `side_effect`
//! closure exemption, and DET has no `ctx` rules. Nothing here matches any
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
