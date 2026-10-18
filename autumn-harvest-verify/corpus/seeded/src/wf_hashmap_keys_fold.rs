//! **S32** — hash order collapsed into a value by `fold`.
//!
//! **Mechanism.** `Order` → `Value`. `helpers::keys_folded` builds
//! `map.keys().fold(String::new(), |acc, key| acc + key)`. Concatenation is not
//! commutative, so the string follows the hash order. That string is the
//! activity's argument.
//!
//! **Purpose.** The model once carried a `[[reduction]] path = "fold"` row,
//! marked in prose as a "documented non-rule". With no receiver, it matched
//! every `fold` call and cleared the order taint, so this case came back
//! `proven-deterministic`. The row is gone. This case keeps it gone.
//!
//! **Launder.** As S29: no `HashMap` token in the body, and the iteration is an
//! adaptor chain in another crate over a function parameter.
//!
//! **Expected trace hops.** `wf_hashmap_keys_fold` ⇒
//! `harvest_verify_corpus_helpers::keys_folded` ⇒ `HashMap::keys` (the Order
//! source) ⇒ `Iterator::fold` ⇒ `WorkflowContext::execute_activity_raw`.

use std::collections::HashMap;

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Emits a digest of the observed counter names.
#[workflow]
pub async fn wf_hashmap_keys_fold(
    ctx: &WorkflowContext,
    counters: HashMap<String, u32>,
) -> Result<String, String> {
    let digest = helpers::keys_folded(&counters);
    ctx.execute_activity_raw("digest", serde_json::json!({ "digest": digest }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(digest)
}
