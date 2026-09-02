//! **S08** — a `static RwLock` read in a helper sizes the fan-out window.
//!
//! **Mechanism.** `Value`. `helpers::window_size()` does `WINDOW.read()` on a
//! `static RwLock<usize>` that a control plane can retune at runtime. The value
//! becomes `max_in_flight` for `execute_activity_fan_out_raw_windowed`, so the
//! dispatch shape recorded in history depends on ambient configuration.
//!
//! **Launder.** `.read()` is not `.lock()`, so HVG007 — the single ambient-state
//! rule — misses this *even when written inline in a workflow body*; the crate
//! boundary is belt-and-braces. DET has no lock or `RwLock` pattern at all.
//!
//! **Expected trace hops.** `wf_rwlock_config_read` ⇒
//! `harvest_verify_corpus_helpers::window_size` ⇒ `RwLock::<usize>::read` on
//! `static WINDOW` (the source) ⇒ return ⇒
//! `WorkflowContext::execute_activity_fan_out_raw_windowed`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Fans work out with an ambient concurrency window.
#[workflow]
pub async fn wf_rwlock_config_read(
    ctx: &WorkflowContext,
    items: Vec<String>,
) -> Result<usize, String> {
    let window = helpers::window_size();
    let batch: Vec<(String, serde_json::Value, String)> = items
        .iter()
        .map(|item| {
            (
                "process".to_string(),
                serde_json::json!({ "item": item }),
                "corpus".to_string(),
            )
        })
        .collect();
    let out = ctx
        .execute_activity_fan_out_raw_windowed(batch, window)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.len())
}
