//! **C12** — `?` on a clean `Result`.
//!
//! `str::parse` desugars into `Try::branch` plus a `switchInt` on the
//! `ControlFlow` discriminant, and the sinks after it are control-dependent on
//! that switch in the raw CFG sense. But the scrutinee is derived only from
//! workflow input, so it carries no taint and the branch is clean by
//! construction.
//!
//! This pins the rule that Control taint follows the **taint of the switch
//! operand**, not the mere presence of a branch — otherwise every `?` in every
//! workflow (which is to say, all of them) becomes a finding. Verdict:
//! `proven-deterministic`.

use autumn_harvest::prelude::*;

/// Parses the quantity, then reserves it.
#[workflow]
pub async fn wf_try_on_clean_result(
    ctx: &WorkflowContext,
    quantity: String,
) -> Result<u32, String> {
    let parsed: u32 = quantity.parse().map_err(|_| "not a number".to_string())?;
    ctx.execute_activity_raw(
        "reserve",
        serde_json::json!({ "quantity": parsed }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(parsed)
}
