//! **C06** — `HashMap` keys collected and **sorted** before iteration.
//!
//! This is verbatim the `alternative` text DET010/HVG011 print as the
//! recommended fix ("collect the keys, sort them, then iterate"). `sort` is a
//! sanitizer: it kills Order taint outright. If the analyzer flagged its own
//! layer's recommended fix, adoption would die on contact. Verdict:
//! `proven-deterministic`.

use std::collections::HashMap;

use autumn_harvest::prelude::*;

/// Applies every limit in sorted key order.
#[workflow]
pub async fn wf_sorted_keys(
    ctx: &WorkflowContext,
    limits: HashMap<String, u32>,
) -> Result<usize, String> {
    let mut keys: Vec<String> = limits.keys().cloned().collect();
    keys.sort();
    for key in &keys {
        let value = limits.get(key).copied().unwrap_or(0);
        ctx.execute_activity_raw(
            "apply_limit",
            serde_json::json!({ "key": key, "value": value }),
            "corpus",
        )
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(keys.len())
}
