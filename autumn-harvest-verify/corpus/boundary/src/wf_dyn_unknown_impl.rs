//! **U01** — a trait object the analyzer cannot devirtualize.
//!
//! **Boundary.** `dyn-dispatch`.
//!
//! Two concrete types are unsized into `dyn Fetcher` inside the analyzed set:
//! `Fixed` (deterministic) and `Drifting` (`SystemTime::now`). A runtime flag
//! picks the one that reaches `.get()`. So RTA-lite over unsizing coercions
//! has **two** candidates for `<dyn Fetcher as Fetcher>::get` and must not
//! pick one.
//!
//! Both wrong answers are unacceptable. `proven-deterministic` is unsound,
//! because the `Drifting` impl reads the clock. `nondeterminism-found` is a
//! false positive on the `Fixed` path. The honest verdict is `unknown` with
//! the callee named.
//!
//! Its deliberate contrast is
//! `harvest_verify_corpus_seeded::wf_rand_behind_dyn`, where exactly **one**
//! type is unsized into the trait, RTA resolves it, and the verdict must be
//! `nondeterminism-found`. Conflating the two would let the detection metric be
//! gamed by guessing.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Reads a value through a fetcher chosen at runtime.
#[workflow]
pub async fn wf_dyn_unknown_impl(ctx: &WorkflowContext, live: bool) -> Result<u64, String> {
    let value = {
        let fetcher = if live {
            helpers::drifting_fetcher()
        } else {
            helpers::fixed_fetcher(7)
        };
        fetcher.get()
    };
    ctx.execute_activity_raw("store", serde_json::json!({ "value": value }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(value)
}
