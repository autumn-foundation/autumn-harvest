//! **S14** — `Instant::elapsed()` behind a helper.
//!
//! **Mechanism.** `Value`. `helpers::remaining_secs(budget)` subtracts
//! `helpers_deep::elapsed_secs(start)` — i.e. `Instant::elapsed()` against a
//! lazily-captured process-start `Instant` — from the budget. The remainder is
//! written into the activity input, so the recorded command carries a
//! monotonic-clock reading.
//!
//! **Launder.** HVG001's path list and DET001's substring table both contain
//! only the *constructors* `Instant::now` / `SystemTime::now` — neither has an
//! `.elapsed()` pattern, and neither models a field or static of type
//! `Instant`. So this escapes the syntactic layer **even written inline in a
//! workflow body**; the two crate boundaries are belt-and-braces.
//!
//! **Expected trace hops.** `wf_instant_elapsed_field` ⇒
//! `harvest_verify_corpus_helpers::remaining_secs` ⇒
//! `harvest_verify_corpus_helpers_deep::elapsed_secs` ⇒ `Instant::elapsed` (the
//! source) ⇒ `Duration::as_secs` ⇒ `u64::saturating_sub` ⇒
//! `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Reports how much of the SLA budget is left.
#[workflow]
pub async fn wf_instant_elapsed_field(ctx: &WorkflowContext, budget: u64) -> Result<u64, String> {
    let left = helpers::remaining_secs(budget);
    ctx.execute_activity_raw("report_sla", serde_json::json!({ "left": left }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(left)
}
