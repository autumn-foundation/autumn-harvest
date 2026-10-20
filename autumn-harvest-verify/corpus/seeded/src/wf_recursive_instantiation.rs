//! **S36** — a recursive call that changes the generic instantiation.
//!
//! **Mechanism.** `Value` into a sink, read through a trait impl that only the
//! recursive call selects. `chain::<Fixed>` calls `chain::<Clock>`. Both are
//! one body with two substitutions. `Clock::stamp` reads the clock through
//! `fine_stamp()`. A cycle keyed on the body path alone takes the inner call
//! for a re-entry of the outer one. It then solves only the `Fixed`
//! substitution, and `Clock::stamp` is never analyzed.
//!
//! **Launder.** `fine_stamp` is two crates away, past det_check's one hop. The
//! clock read is behind a static trait call on a type parameter. Only the
//! substitution of the recursive call names the impl that reads it.
//!
//! **Expected trace hops.** `chain` ⇒ `Clock::stamp` ⇒ `fine_stamp` ⇒
//! `SystemTime::now` ⇒ `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers_deep as deep;

/// A source of stamps.
trait Stamp {
    fn stamp() -> u64;
}

/// A fixed stamp.
struct Fixed;

impl Stamp for Fixed {
    fn stamp() -> u64 {
        7
    }
}

/// A stamp read from the clock.
struct Clock;

impl Stamp for Clock {
    fn stamp() -> u64 {
        deep::fine_stamp()
    }
}

/// The stamp at the end of `hops` links. Every link after the first reads
/// the clock.
fn chain<S: Stamp>(hops: u32) -> u64 {
    if hops == 0 {
        S::stamp()
    } else {
        chain::<Clock>(hops.saturating_sub(1))
    }
}

/// Records the stamp at the end of the chain.
#[workflow]
pub async fn wf_recursive_instantiation(ctx: &WorkflowContext, hops: u32) -> Result<(), String> {
    let stamp = chain::<Fixed>(hops);
    ctx.execute_activity_raw("record", serde_json::json!({ "stamp": stamp }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}
