//! **C03** — `ctx.side_effect` whose **closure reads the wall clock**.
//!
//! The sharpest AC4 test. `helpers_deep::stamp()` is `SystemTime::now()` — a
//! genuine source — but it runs *inside* a `side_effect` closure, so the value
//! is recorded on the first execution and replayed verbatim afterwards. The
//! analyzer must therefore **stop at the `side_effect` boundary**: its return is
//! clean and its closure body is not descended into. An analyzer that walks
//! into every closure argument reports a false positive here, and a false
//! positive on `side_effect` would make the tool unusable on real code.
//!
//! Verdict: `proven-deterministic`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers_deep as deep;

/// Captures the ingest time once, then bills against it.
#[workflow]
pub async fn wf_ctx_side_effect(ctx: &WorkflowContext, order: String) -> Result<u64, String> {
    let captured: u64 = ctx
        .side_effect("ingest_epoch", || deep::stamp())
        .map_err(|e| e.to_string())?;
    ctx.execute_activity_raw(
        "bill",
        serde_json::json!({ "order": order, "at": captured }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(captured)
}
