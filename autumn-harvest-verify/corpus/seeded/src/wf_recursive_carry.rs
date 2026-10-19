//! **S30** — a clock value carried into a recursive call.
//!
//! **Mechanism.** `Value` through a recursive cycle. `carry` is first called
//! with a clean accumulator. It passes `acc ^ fine_stamp()` to its own
//! recursive call and returns what that call returns. So the clock reaches
//! the activity argument only through the argument of the recursive call.
//! The first pass over `carry` sees a clean accumulator. Only the fixpoint
//! round that runs `carry` again with the joined argument taint finds it.
//!
//! **Launder.** `fine_stamp` is in another crate, where det_check does not
//! follow, and HVG never leaves the annotated body. Before the fixpoint, the
//! analyzer cut the cycle with a `recursion` boundary, so this was `unknown`.
//!
//! **Expected trace hops.** `wf_recursive_carry` ⇒ `carry` ⇒ `fine_stamp` ⇒
//! `SystemTime::now` ⇒ `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers_deep as deep;

/// Folds `rounds` clock readings into `acc`.
fn carry(rounds: u32, acc: u64) -> u64 {
    if rounds == 0 {
        acc
    } else {
        carry(rounds.saturating_sub(1), acc ^ deep::fine_stamp())
    }
}

/// Derives a token from carried clock readings.
#[workflow]
pub async fn wf_recursive_carry(ctx: &WorkflowContext, rounds: u32) -> Result<u64, String> {
    let token = carry(rounds, 0);
    ctx.execute_activity_raw("submit", serde_json::json!({ "token": token }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(token)
}
