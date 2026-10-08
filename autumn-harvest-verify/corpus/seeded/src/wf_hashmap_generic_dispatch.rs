//! **S01 / AC3-mandatory #1** — HashMap iteration order laundered through a
//! *generic* helper.
//!
//! **Mechanism.** `Order`. The workflow input is a `HashMap<String, u32>`.
//! `helpers::pairs` flattens it with `<T as IntoIterator>::into_iter` into a
//! `Vec`, freezing whatever order `RandomState` chose for this process. The
//! workflow then dispatches one `ScheduleActivity` command per element, so the
//! recorded command sequence is hash-seeded and a replay in another process
//! records a different order.
//!
//! **Launder (three independent reasons the syntactic layer misses it).**
//! 1. HVG011 fires only on a `for … in` over a *locally bound* ident whose
//!    `let` initializer or type annotation is a `HashMap`/`HashSet`
//!    (`iterated_hash_local` + `local_binding_target`); here the map arrives as
//!    a **function parameter**, which is documented as never tracked, and the
//!    `for` loop iterates a `Vec`.
//! 2. DET010 is the same rule re-implemented as a line scan over locally-bound
//!    hash idents and misses it for the same reason; the remaining DET001–DET011
//!    are substring tables that contain no `HashMap` spelling at all.
//! 3. The iteration itself happens in another **crate**, inside a *generic*
//!    function whose body contains no `HashMap` token — only the call site's
//!    substitution reveals `T`. det_check resolves one hop, same file and same
//!    module, so it never reaches `pairs`; and HVG is body-only.
//!
//! **Expected trace hops.** `wf_hashmap_generic_dispatch` ⇒
//! `harvest_verify_corpus_helpers::pairs` with substitution
//! `[T := HashMap<String, u32>]` ⇒ `<T as IntoIterator>::into_iter` (the Order
//! source) ⇒ back through the returned `Vec` ⇒
//! `WorkflowContext::execute_activity_raw` (the sink).

use std::collections::HashMap;

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Dispatches one activity per limit, in the order the generic helper produced.
#[workflow]
pub async fn wf_hashmap_generic_dispatch(
    ctx: &WorkflowContext,
    limits: HashMap<String, u32>,
) -> Result<usize, String> {
    let items = helpers::pairs(limits);
    let mut dispatched = 0usize;
    for (id, n) in items {
        ctx.execute_activity_raw(
            "apply_limit",
            serde_json::json!({ "id": id, "n": n }),
            "corpus",
        )
        .await
        .map_err(|e| e.to_string())?;
        dispatched += 1;
    }
    Ok(dispatched)
}
