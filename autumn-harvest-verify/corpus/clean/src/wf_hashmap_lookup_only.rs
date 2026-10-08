//! **C10** — a `HashMap` used only for **lookup, length and an integer sum**.
//!
//! None of `get`, `len` or an integer `values().sum()` exposes iteration order:
//! `get` is keyed, `len` is a count, and integer addition is commutative and
//! associative. These are the *order-killing reductions*, and the model's list
//! of them (`len`, `count`, integer `sum`, `max`, `min`, `all`, `any`,
//! `is_empty`) is deliberately short and auditable.
//!
//! Its deliberate counterexample is
//! `harvest_verify_corpus_seeded::wf_hashmap_join_string`, where `join` — a
//! *non-commutative* fold over the same iterator — must be found. Verdict:
//! `proven-deterministic`.

use std::collections::HashMap;

use autumn_harvest::prelude::*;

/// Summarises the counters without ever depending on their order.
#[workflow]
pub async fn wf_hashmap_lookup_only(
    ctx: &WorkflowContext,
    counters: HashMap<String, u32>,
) -> Result<u64, String> {
    let distinct = counters.len() as u64;
    let total: u64 = counters.values().map(|v| u64::from(*v)).sum();
    let primary = counters.get("primary").copied().unwrap_or(0);
    ctx.execute_activity_raw(
        "summarise",
        serde_json::json!({ "distinct": distinct, "total": total, "primary": primary }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(total)
}
