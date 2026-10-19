//! **S35** — a clock value passed to an `async fn` helper.
//!
//! **Mechanism.** `Value` through the arguments of an `async fn`. The
//! workflow calls `emit(ctx, n, fine_stamp())`, and `emit` passes `stamp` to
//! an activity. A call to an `async fn` resolves to its coroutine, whose
//! parameters are the pinned future and the context. The analyzer once seeded
//! the call's arguments onto those parameters by position, so `stamp`, the
//! third argument, was dropped and this came back `proven-deterministic`. The
//! union of the arguments now seeds the future, which holds them all.
//!
//! **Launder.** `fine_stamp` is two crates away, past det_check's one hop, and
//! HVG never leaves the annotated body.
//!
//! **Expected trace hops.** `wf_async_helper_argument` ⇒ `fine_stamp` ⇒
//! `SystemTime::now` ⇒ `emit` ⇒ `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers_deep as deep;

/// Records one event with its stamp.
async fn emit(ctx: &WorkflowContext, n: u32, stamp: u64) -> Result<(), String> {
    ctx.execute_activity_raw(
        "emit",
        serde_json::json!({ "n": n, "stamp": stamp }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// Records the `n`th event, stamped by the wall clock.
#[workflow]
pub async fn wf_async_helper_argument(ctx: &WorkflowContext, n: u32) -> Result<(), String> {
    emit(ctx, n, deep::fine_stamp()).await
}
