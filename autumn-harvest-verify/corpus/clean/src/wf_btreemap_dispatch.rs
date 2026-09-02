//! **C05** — `BTreeMap` iteration driving activity dispatch.
//!
//! `BTreeMap` iterates in key order, which is a total order fixed by the data,
//! not by a per-process hash seed. Dispatching one activity per entry is
//! deterministic. The analyzer's Order sources must be restricted to the
//! hash-backed containers; flagging every map iteration would make the
//! recommended fix for HVG011 look like a bug. Verdict:
//! `proven-deterministic`.

use std::collections::BTreeMap;

use autumn_harvest::prelude::*;

/// Applies every limit in key order.
#[workflow]
pub async fn wf_btreemap_dispatch(
    ctx: &WorkflowContext,
    limits: BTreeMap<String, u32>,
) -> Result<usize, String> {
    for (key, value) in &limits {
        ctx.execute_activity_raw(
            "apply_limit",
            serde_json::json!({ "key": key, "value": value }),
            "corpus",
        )
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(limits.len())
}
