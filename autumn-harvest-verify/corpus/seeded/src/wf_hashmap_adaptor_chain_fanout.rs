//! **S13** — a four-adaptor chain over a `HashMap` **struct field**, entirely
//! in the workflow body.
//!
//! **Mechanism.** `Order`. `cfg.limits` is a `HashMap`; the chain
//! `.iter().filter(..).map(..).take(3)` selects three keys by hash position and
//! fans out one activity per key. Which three, and in what order, is
//! hash-seeded.
//!
//! **Launder (no cross-module hiding at all — this is the persuasive case).**
//! HVG011 fires only from `visit_expr_for_loop`, and there is no `for` loop
//! here. Even rewritten as one it would miss twice over: `local_binding_target`
//! never tracks **struct fields**, and `iterated_hash_local` accepts only a
//! bare ident, `&ident`, or a *single* argument-free method from
//! `HASH_ITER_METHODS` — a four-adaptor chain is rejected by design. DET010
//! mirrors both limitations. No DET substring table contains `HashMap`.
//!
//! **Expected trace hops.** `wf_hashmap_adaptor_chain_fanout` ⇒
//! `<&HashMap<String, u32> as IntoIterator>::into_iter` on `cfg.limits` (the
//! Order source, in this body) ⇒ `Filter`/`Map`/`Take` adaptors ⇒ `collect` ⇒
//! `WorkflowContext::execute_activity_fan_out_raw`.

use std::collections::HashMap;

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

/// Tenant configuration carrying per-key limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cfg {
    /// Per-key limits; iteration order is hash-seeded.
    pub limits: HashMap<String, u32>,
}

/// Processes the three "top" limits.
#[workflow]
pub async fn wf_hashmap_adaptor_chain_fanout(
    ctx: &WorkflowContext,
    cfg: Cfg,
) -> Result<usize, String> {
    let top: Vec<String> = cfg
        .limits
        .iter()
        .filter(|(_, v)| **v > 0)
        .map(|(k, _)| k.clone())
        .take(3)
        .collect();
    let batch: Vec<(String, serde_json::Value, String)> = top
        .iter()
        .map(|k| {
            (
                "top_limit".to_string(),
                serde_json::json!({ "key": k }),
                "corpus".to_string(),
            )
        })
        .collect();
    let out = ctx
        .execute_activity_fan_out_raw(batch)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.len())
}
