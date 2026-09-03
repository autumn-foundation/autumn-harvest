//! **S22** — the process id in an activity payload.
//!
//! **Mechanism.** `Value`. `helpers::origin_tag()` returns
//! `format!("w{}", std::process::id())` from `helpers_deep`. The tag is written
//! into the `ScheduleActivity` input, so a replay on another worker process
//! records a different command.
//!
//! **Launder.** DET005 *does* know `process::id(` — but it is classified
//! `DetSeverity::Warning`, so it never counts as a hard blocker even when it
//! fires, and it cannot fire here anyway: it resolves one hop, same file and
//! same module, and `origin_tag` is one crate away with the actual call a crate
//! beyond that. There is **no HVG twin at all** — HVG001–HVG011 contains no
//! process-id rule — so the compile-time layer is silent by construction.
//!
//! **Expected trace hops.** `wf_process_id_payload` ⇒
//! `harvest_verify_corpus_helpers::origin_tag` ⇒
//! `harvest_verify_corpus_helpers_deep::origin_tag` ⇒ `std::process::id` (the
//! source) ⇒ `format!` ⇒ `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Audits an event, tagging it with the emitting worker.
#[workflow]
pub async fn wf_process_id_payload(ctx: &WorkflowContext, event: String) -> Result<String, String> {
    let origin = helpers::origin_tag();
    ctx.execute_activity_raw(
        "audit",
        serde_json::json!({ "event": event, "origin": origin }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(origin)
}
