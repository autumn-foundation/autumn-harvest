//! **S05 / AC3-mandatory #5** — randomness behind a trait object.
//!
//! **Mechanism.** `Value`. `helpers::default_jitter()` returns
//! `Box<dyn Jitter>`; the single implementation, `Live`, computes
//! `rand::random::<u64>() % 500`. The result is the *duration* of a
//! `StartTimer` command, so the recorded history differs run to run.
//!
//! **Launder.** There is no `rand` token in this body, in any same-module
//! helper, or even at the call site: MIR shows `<dyn Jitter as Jitter>::ms`.
//! HVG002 and DET002 both know `rand::random`, and both are body-only /
//! same-module-one-hop, so neither can see through the crate boundary — and
//! neither has any notion of virtual dispatch to see through in the first place.
//!
//! **Devirtualization (deliberate).** `Live` is the *only* type unsized into
//! `dyn Jitter` anywhere in the analyzed set, and the coercion
//! (`Box::new(Live)`) happens inside `default_jitter`, which is analyzed. An
//! RTA-lite pass over unsizing coercions therefore has exactly one candidate,
//! so the honest verdict here is `nondeterminism-found` naming `Live` — **not**
//! `unknown`. The unresolvable sibling is
//! `harvest_verify_corpus_boundary::wf_dyn_unknown_impl`, which must be
//! `unknown`; conflating the two would inflate the detection metric.
//!
//! **Expected trace hops.** `wf_rand_behind_dyn` ⇒
//! `harvest_verify_corpus_helpers::default_jitter` (construction site) ⇒
//! devirtualized `<dyn Jitter as Jitter>::ms` → `Live` ⇒ `rand::random` (the
//! source) ⇒ `WorkflowContext::timer`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Waits a jittered backoff before retrying.
#[workflow]
pub async fn wf_rand_behind_dyn(ctx: &WorkflowContext, attempt: u32) -> Result<u32, String> {
    // The trait object is dropped at the end of this statement so the workflow
    // future stays `Send`; the dyn call itself is what matters here.
    let wait = helpers::default_jitter().ms();
    ctx.timer("backoff", wait)
        .await
        .map_err(|e| e.to_string())?;
    Ok(attempt + 1)
}
