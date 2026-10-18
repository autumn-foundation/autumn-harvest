//! **C17** — `!=` and `<` on derived comparisons.
//!
//! `#[derive(PartialEq, PartialOrd)]` generates `eq` and `partial_cmp`. A call
//! to `ne` or `lt` runs core's default body, which calls them. The analyzer
//! once had no body for `ne` and reported the call as an external body. Now
//! it follows the derived method that the default calls. Verdict:
//! `proven-deterministic`.

use autumn_harvest::prelude::*;

/// What one experiment did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize)]
pub enum Status {
    /// Kept the change.
    Keep,
    /// Reverted the change.
    Discard,
}

/// Records each experiment that did not keep its change.
#[workflow]
pub async fn wf_derived_comparison(ctx: &WorkflowContext, status: Status) -> Result<bool, String> {
    let reverted = status != Status::Keep && Status::Keep < status;
    if reverted {
        ctx.execute_activity_raw("revert", serde_json::json!({}), "corpus")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(reverted)
}
