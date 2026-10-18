//! **C15** — std methods on a helper's parameters.
//!
//! A signature prints its parameter types trimmed, as
//! `fn remember(_1: &mut Vec<u32>, ..)`. A `let` declaration prints the same
//! type in full, as `std::vec::Vec<u32>`. With no root at the site, the
//! analyzer once reported `Vec::push` on the parameter as an external body.
//! rustc trims only a name that is unique in the crate graph. So `Vec` is the
//! one item its rooted spellings name. Slice methods on a `&[u32]` parameter
//! and `Vec::new` passed as a fn item are the same case. Verdict:
//! `proven-deterministic`.

use autumn_harvest::prelude::*;

/// Appends `row` and keeps the newest three.
fn remember(recent: &mut Vec<u32>, row: u32) {
    recent.push(row);
    let extra = recent.len().saturating_sub(3);
    recent.drain(..extra);
}

/// Renders the rows as one line.
fn table(rows: &[u32]) -> String {
    let mut out = String::new();
    for row in rows {
        out.push_str(&row.to_string());
    }
    out
}

/// Records each row and reports the window.
#[workflow]
pub async fn wf_trimmed_param_receiver(
    ctx: &WorkflowContext,
    rows: Option<Vec<u32>>,
) -> Result<String, String> {
    let mut recent = Vec::new();
    for row in rows.clone().map_or_else(Vec::new, |rows| rows) {
        remember(&mut recent, row);
    }
    let line = table(&recent);
    ctx.execute_activity_raw("report", serde_json::json!({ "line": line }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(line)
}
