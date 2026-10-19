//! **C14** — a first-party `const` passed as an argument to std methods.
//!
//! MIR passes a `const` item as an operand, as `const wf_const_argument::KEEP`.
//! A fn item has the same shape, as `const Uuid::new_v4`. The analyzer once
//! read the constant as a callback into an external crate named after the
//! module, so the workflow came back `unknown`. A constant is fixed at compile
//! time. Verdict: `proven-deterministic`.

use autumn_harvest::prelude::*;

/// How many rows the window keeps.
const KEEP: usize = 3;
/// The marker of a skipped row.
const SKIP: &str = "skip";

/// Keeps the newest rows that are not skipped.
#[workflow]
pub async fn wf_const_argument(ctx: &WorkflowContext, rows: Vec<String>) -> Result<usize, String> {
    let start = rows.len().saturating_sub(KEEP);
    let kept = rows
        .iter()
        .skip(start)
        .filter(|row| !row.contains(SKIP))
        .count();
    ctx.execute_activity_raw("keep", serde_json::json!({ "kept": kept }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(kept)
}
