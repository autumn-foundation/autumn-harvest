//! **S11** — order destroyed *inside* a helper, from deterministic input.
//!
//! **Mechanism.** `Order`. `helpers::normalize` takes a
//! `Vec<(String, u32)>` — perfectly ordered data — collects it into a
//! `HashMap` to de-duplicate, then immediately re-iterates the map back into a
//! `Vec`. The de-duplication is legitimate; the re-iteration is where
//! determinism dies, and it dies on data the workflow author believes is clean.
//!
//! **Launder.** This workflow body contains no `HashMap` token anywhere, so
//! every DET substring rule and every HVG rule is inert by construction. Even
//! written inline it would escape HVG011/DET010: those track a `for … in` over
//! a bound hash ident, and the re-iteration here is a two-adaptor chain
//! (`.into_iter().collect()`) on an unnamed temporary, which
//! `iterated_hash_local` rejects by design ("longer chains are never flagged").
//!
//! **Expected trace hops.** `wf_collect_into_hashmap_then_reiterate` ⇒
//! `harvest_verify_corpus_helpers::normalize` ⇒ `collect` into
//! `HashMap<String, u32>` ⇒ `<HashMap<String, u32> as IntoIterator>::into_iter`
//! (the Order source, same body) ⇒ return ⇒ loop element ⇒
//! `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// De-duplicates the rows, then charges each one.
#[workflow]
pub async fn wf_collect_into_hashmap_then_reiterate(
    ctx: &WorkflowContext,
    rows: Vec<(String, u32)>,
) -> Result<usize, String> {
    let unique = helpers::normalize(rows);
    for (id, amount) in &unique {
        ctx.execute_activity_raw(
            "bill",
            serde_json::json!({ "id": id, "amount": amount }),
            "corpus",
        )
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(unique.len())
}
