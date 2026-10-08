//! **S28** — a `thread_local!` `RefCell` sequence read entirely inside a helper.
//!
//! **Mechanism.** `Value`. `helpers::next_tl_seq()` is
//! `TL_SEQ.with(|c| { *c.borrow_mut() += 1; *c.borrow() })` over
//! `thread_local! { static TL_SEQ: RefCell<u64> }`. In MIR this is
//! `LocalKey::<RefCell<u64>>::with::<{closure@…}, u64>` plus the
//! `promoted[0]` indirection that thread-local access lowers to — so the
//! analyzer must follow a `LocalKey::with` closure argument *and* recognize the
//! promoted-const path back to the thread-local's name.
//!
//! **Launder.** Sibling of S03 with the mutation on the *helper's* side rather
//! than the caller's, so it exercises the plain `LocalKey::with` shape with no
//! caller closure involved. No HVG or DET rule mentions `thread_local`,
//! `LocalKey`, `RefCell` or `borrow_mut`; the crate boundary is on top of that.
//!
//! **Expected trace hops.** `wf_threadlocal_refcell_seq` ⇒
//! `harvest_verify_corpus_helpers::next_tl_seq` ⇒ `LocalKey::with` on
//! `TL_SEQ` ⇒ `next_tl_seq::{closure#0}` ⇒ `RefCell::<u64>::borrow_mut` (the
//! source) ⇒ return ⇒ `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Emits a per-thread monotonic ticket with the document.
#[workflow]
pub async fn wf_threadlocal_refcell_seq(
    ctx: &WorkflowContext,
    document: String,
) -> Result<u64, String> {
    let ticket = helpers::next_tl_seq();
    ctx.execute_activity_raw(
        "index",
        serde_json::json!({ "document": document, "ticket": ticket }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(ticket)
}
