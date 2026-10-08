//! **S07** — a `static Mutex<Vec<_>>` drained in a helper.
//!
//! **Mechanism.** `Value` on the elements and `Order` on the sequence.
//! `helpers::drain_pending()` locks `static PENDING: Mutex<Vec<String>>` and
//! `mem::take`s it. Both *which* items come back and *how many* depend on what
//! other executions parked on this worker, and the workflow emits one
//! `ScheduleActivity` per item.
//!
//! **Launder.** HVG007 is the only rule in either layer that mentions locking,
//! and it requires the literal method name `lock` on an ALL-CAPS **path
//! expression receiver inside the workflow body** — the receiver here is
//! `PENDING` in another crate, so the visitor never sees it. DET has no lock
//! rule whatsoever. The `Vec` the workflow loops over is a perfectly ordinary
//! local, so DET010/HVG011 have nothing to say either.
//!
//! **Expected trace hops.** `wf_static_mutex_queue` ⇒
//! `harvest_verify_corpus_helpers::drain_pending` ⇒ `Mutex::<Vec<String>>::lock`
//! on `static PENDING` (the source) ⇒ `mem::take` ⇒ return ⇒ loop element ⇒
//! `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Flushes whatever this worker had parked.
#[workflow]
pub async fn wf_static_mutex_queue(ctx: &WorkflowContext, _job: String) -> Result<usize, String> {
    let parked = helpers::drain_pending();
    for item in &parked {
        ctx.execute_activity_raw("flush", serde_json::json!({ "item": item }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(parked.len())
}
