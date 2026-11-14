//! **S03 / AC3-mandatory #3** — interior mutability in ambient state, mutated
//! by a closure the helper crate invokes.
//!
//! **Mechanism.** `Value`. `helpers::with_page_cursor` hands a reference to an
//! ambient `thread_local! { static PAGE_CURSOR: RefCell<u32> }` to a closure
//! defined *here*. The closure `borrow_mut()`s it and increments it. So the
//! page number in the `ScheduleActivity` command depends on how many
//! executions previously ran on this worker thread.
//!
//! **Launder.** Neither layer models interior mutability at all. No HVG001–HVG011
//! or DET001–DET011 pattern names `RefCell`, `Cell`, `borrow`, `borrow_mut` or
//! `LocalKey`. So the mechanism is invisible *even inline in the body*. Also,
//! the state lives in another crate, and the mutation happens inside a closure
//! body. That body is a separate MIR item, reached only through
//! `<{closure@…} as FnOnce>::call_once`. No syntactic call-graph walker can
//! follow that edge.
//!
//! **Expected trace hops.** `wf_refcell_captured_state` ⇒
//! `harvest_verify_corpus_helpers::with_page_cursor` ⇒ `LocalKey::with` on
//! `PAGE_CURSOR` ⇒ `wf_refcell_captured_state::{closure#0}` ⇒
//! `RefCell::<u32>::borrow_mut` (the source) ⇒ closure return ⇒
//! `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Fetches "the next page", where *next* is ambient per-thread state.
#[workflow]
pub async fn wf_refcell_captured_state(ctx: &WorkflowContext, feed: String) -> Result<u32, String> {
    let page = helpers::with_page_cursor(|cursor| {
        let mut at = cursor.borrow_mut();
        *at += 1;
        *at
    });
    ctx.execute_activity_raw(
        "fetch_page",
        serde_json::json!({ "feed": feed, "page": page }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(page)
}
