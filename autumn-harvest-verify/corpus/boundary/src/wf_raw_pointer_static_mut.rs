//! **U04** — a raw-pointer read of a `static mut`.
//!
//! **Boundary.** `unsafe-raw-pointer`.
//!
//! `helpers::tick()` reads `static mut TICK: u64` via
//! `std::ptr::read(&raw const TICK)`. In MIR this is a raw-pointer
//! dereference: the place's root is a pointer local, not a named static, so the
//! `allocN (static: NAME)` footer that resolves ordinary static reads does not
//! apply and the value's provenance is outside the analyzer's memory model.
//!
//! **A deliberate choice, not an oversight.** `static mut` specifically *is*
//! knowable — a points-to pass could recover it and classify it as an ambient
//! `Value` source, which would make this `nondeterminism-found`. But the rule
//! that fires here has to be the general one ("an arbitrary raw-pointer read is
//! outside the model"), and the general rule cannot be sound in either
//! direction. The corpus pins the conservative answer, and the feasibility
//! report argues the case; if a later revision adds a points-to pass, this
//! expectation moves to `nondeterminism-found` **deliberately**, with the
//! reasoning recorded here rather than silently.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Tags the heartbeat with the process-global tick.
#[workflow]
pub async fn wf_raw_pointer_static_mut(ctx: &WorkflowContext, node: String) -> Result<u64, String> {
    let tick = helpers::tick();
    ctx.execute_activity_raw(
        "heartbeat",
        serde_json::json!({ "node": node, "tick": tick }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(tick)
}
