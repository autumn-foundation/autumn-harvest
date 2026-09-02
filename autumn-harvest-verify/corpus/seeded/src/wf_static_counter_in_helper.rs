//! **S02 / AC3-mandatory #2** — a process-global counter read in a helper.
//!
//! **Mechanism.** `Value`. `helpers::next_seq()` performs
//! `SEQ.fetch_add(1, Relaxed)` on a `static AtomicU64`. The returned ticket
//! depends on how many other executions ran first in this process, so it
//! differs between the original run and any replay; it is written verbatim into
//! the activity's idempotency field, i.e. into the recorded command.
//!
//! **Launder.** HVG007 is the *only* ambient-state rule in either layer, and it
//! matches exclusively a literal `.lock()` method call whose receiver is an
//! all-uppercase path expression **in the workflow body**. `fetch_add` is not
//! `lock`, `SEQ` is not in this body, and the read is one crate away. DET005 is
//! the nearest DET rule (process-global state) and its patterns are only
//! `std::process::id(` / `process::id(` — and it is a `Warning`, which never
//! blocks a build. No other HVG or DET rule models statics or atomics at all.
//!
//! **Expected trace hops.** `wf_static_counter_in_helper` ⇒
//! `harvest_verify_corpus_helpers::next_seq` ⇒ `AtomicU64::fetch_add` on
//! `static SEQ` ⇒ `format!` ⇒ `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Charges a customer with an idempotency key built from an ambient counter.
#[workflow]
pub async fn wf_static_counter_in_helper(
    ctx: &WorkflowContext,
    customer: String,
) -> Result<String, String> {
    let ticket = helpers::next_seq();
    let key = format!("charge-{customer}-{ticket}");
    ctx.execute_activity_raw(
        "charge",
        serde_json::json!({ "idempotency": key }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(key)
}
