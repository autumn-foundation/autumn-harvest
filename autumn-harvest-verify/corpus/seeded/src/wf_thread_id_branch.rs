//! **S21** — the OS thread id decides which activity is emitted.
//!
//! **Mechanism.** `Value` → `Control`. `helpers::worker_slot_parity()` hashes
//! `std::thread::current().id()` (in `helpers_deep::worker_slot`) and takes it
//! modulo two. A replay scheduled onto another worker thread takes the other
//! branch and records a different command.
//!
//! **Launder.** No rule in either layer mentions `thread::current`, `ThreadId`,
//! or `DefaultHasher`: HVG005/DET007 cover `thread::spawn`, which is a
//! different thing entirely, and DET005 (process id, a `Warning`) is the closest
//! relative. Two crate boundaries on top.
//!
//! **Expected trace hops.** `wf_thread_id_branch` ⇒
//! `harvest_verify_corpus_helpers::worker_slot_parity` ⇒
//! `harvest_verify_corpus_helpers_deep::worker_slot` ⇒ `std::thread::current`
//! (the source) ⇒ `ThreadId::hash` ⇒ `switchInt` ⇒ the two
//! `WorkflowContext::execute_activity_raw` sinks.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Picks the fast or slow lane depending on which worker thread is running.
#[workflow]
pub async fn wf_thread_id_branch(ctx: &WorkflowContext, _job: String) -> Result<bool, String> {
    let fast_lane = helpers::worker_slot_parity() == 0;
    if fast_lane {
        ctx.execute_activity_raw("fast_lane", serde_json::json!({ "n": 1 }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
    } else {
        ctx.execute_activity_raw("slow_lane", serde_json::json!({ "n": 1 }), "corpus")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(fast_lane)
}
