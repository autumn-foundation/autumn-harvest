//! **C11** — interior mutability that is **locally constructed and never
//! escapes**.
//!
//! A `RefCell` created inside the body, mutated in a loop over deterministic
//! input, and read back is just a mutable local with extra steps: its final
//! value is a pure function of the workflow input. The mirror image of
//! `harvest_verify_corpus_seeded::wf_refcell_captured_state`, where the same
//! `RefCell` machinery is applied to *ambient* state and must be found.
//!
//! What separates them is the **root** of the receiver place, not the type: a
//! locally-allocated cell is a clean root; a `static`/thread-local one is an
//! ambient source. An analyzer that keys on `RefCell` as a syntactic token
//! flags this. Verdict: `proven-deterministic`.

use std::cell::RefCell;

use autumn_harvest::prelude::*;

/// Totals the amounts through a local cell, then submits the total.
#[workflow]
pub async fn wf_local_refcell_no_escape(
    ctx: &WorkflowContext,
    amounts: Vec<u32>,
) -> Result<u64, String> {
    let running = RefCell::new(0u64);
    for amount in &amounts {
        *running.borrow_mut() += u64::from(*amount);
    }
    let total = *running.borrow();
    ctx.execute_activity_raw("settle", serde_json::json!({ "total": total }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(total)
}
