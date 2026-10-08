//! **S15** — a wall-clock value used as a **timer duration**.
//!
//! **Mechanism.** `Value` into a `StartTimer` command. `helpers::backoff_secs()`
//! is `helpers_deep::stamp() % 30`, i.e. `SystemTime::now()` two crates deep.
//! No activity argument changes; what diverges is the *duration* recorded in
//! the timer command, which is enough to diverge the history.
//!
//! **Launder.** Identical to S04: HVG001 is body-only and this body has no
//! clock token; DET001 resolves one hop, same file and same module, and
//! `backoff_secs` is a crate away with `stamp` a crate beyond that.
//!
//! **Expected trace hops.** `wf_time_dependent_timer_duration` ⇒
//! `harvest_verify_corpus_helpers::backoff_secs` ⇒
//! `harvest_verify_corpus_helpers_deep::stamp` ⇒ `SystemTime::now` (the source)
//! ⇒ return ⇒ `WorkflowContext::timer` duration argument.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Waits a "smoothed" backoff before the next attempt.
#[workflow]
pub async fn wf_time_dependent_timer_duration(
    ctx: &WorkflowContext,
    attempt: u32,
) -> Result<u32, String> {
    ctx.timer("smoothed_backoff", helpers::backoff_secs())
        .await
        .map_err(|e| e.to_string())?;
    ctx.execute_activity_raw("retry", serde_json::json!({ "attempt": attempt }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(attempt + 1)
}
