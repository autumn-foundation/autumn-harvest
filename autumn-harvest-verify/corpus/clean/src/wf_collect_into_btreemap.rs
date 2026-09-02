//! **C07** — hash-ordered data sanitized by collecting into a `BTreeMap`.
//!
//! The sanitizer here is the **collect target type**, not an explicit `sort`:
//! `collect::<BTreeMap<_, _>>()` re-establishes a total key order, so the
//! subsequent iteration is deterministic. The mirror image of
//! `harvest_verify_corpus_seeded::wf_collect_into_hashmap_then_reiterate`, which
//! collects into a `HashMap` and must be found. Verdict:
//! `proven-deterministic`.

use std::collections::{BTreeMap, HashMap};

use autumn_harvest::prelude::*;

/// Orders the incoming quotas, then charges each one.
#[workflow]
pub async fn wf_collect_into_btreemap(
    ctx: &WorkflowContext,
    quotas: HashMap<String, u32>,
) -> Result<usize, String> {
    let ordered: BTreeMap<String, u32> = quotas.into_iter().collect();
    for (key, value) in &ordered {
        ctx.execute_activity_raw(
            "charge",
            serde_json::json!({ "key": key, "value": value }),
            "corpus",
        )
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(ordered.len())
}
