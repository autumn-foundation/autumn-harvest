//! **C19** — a recursive helper over workflow input.
//!
//! The analyzer once cut recursion: it did not enter a body already on the
//! stack, and reported a `recursion` boundary. Now it solves the cycle as a
//! fixpoint. The recursive call returns the cycle's current summary, and the
//! outer call runs again with the joined argument taint until no fact is
//! added. Nothing here reads ambient state. Verdict: `proven-deterministic`.

use autumn_harvest::prelude::*;

/// The depth of a nested list, written as brackets.
fn depth(text: &str) -> u32 {
    match text
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        Some(inner) => depth(inner).saturating_add(1),
        None => 0,
    }
}

/// Reports how deeply the input nests.
#[workflow]
pub async fn wf_recursive_helper(ctx: &WorkflowContext, text: String) -> Result<u32, String> {
    let levels = depth(&text);
    ctx.execute_activity_raw("depth", serde_json::json!({ "levels": levels }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(levels)
}
