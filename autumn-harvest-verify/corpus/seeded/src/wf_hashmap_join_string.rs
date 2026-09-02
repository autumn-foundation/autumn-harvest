//! **S29** — hash order collapsed into a value by a non-commutative fold.
//!
//! **Mechanism.** `Order` → `Value`. `helpers::values_joined` builds
//! `map.values().map(u32::to_string).collect::<Vec<_>>().join(",")`. The
//! *multiset* of values is stable across runs; the joined **string** is not,
//! because `join` is not commutative. That string is the activity's argument.
//!
//! **Purpose.** This pins the model's order-killing reduction list to a narrow,
//! auditable set (`len`, `count`, integer `sum`, `max`, `min`, `all`, `any`,
//! `is_empty`). If `fold`/`join`/float `sum` ever leak into that list, this case
//! flips to `proven-deterministic` and the corpus catches it.
//!
//! **Launder.** The workflow body has no `HashMap` token, so every DET
//! substring table and every HVG rule is inert. Even inline it would escape
//! HVG011/DET010: those flag only a `for … in` over a locally-bound hash ident,
//! and there is no `for` loop here at all — the iteration is an adaptor chain in
//! another crate over a **function parameter**, which `local_binding_target`
//! documents as never tracked.
//!
//! **Expected trace hops.** `wf_hashmap_join_string` ⇒
//! `harvest_verify_corpus_helpers::values_joined` ⇒
//! `HashMap::<String, u32>::values` (the Order source) ⇒ `Iterator::collect` ⇒
//! `[String]::join` (non-commutative fold: Order → Value) ⇒
//! `WorkflowContext::execute_activity_raw`.

use std::collections::HashMap;

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Emits a digest of the observed counters.
#[workflow]
pub async fn wf_hashmap_join_string(
    ctx: &WorkflowContext,
    counters: HashMap<String, u32>,
) -> Result<String, String> {
    let digest = helpers::values_joined(&counters);
    ctx.execute_activity_raw("digest", serde_json::json!({ "digest": digest }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(digest)
}
