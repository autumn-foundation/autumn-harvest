//! **U03** — a call into a foreign function.
//!
//! **Boundary.** `ffi`.
//!
//! `helpers::native_abs` calls `abs` from an `unsafe extern "C"` block. There is
//! no MIR for a foreign function — the compiler never produces a body — so the
//! analyzer has nothing to summarize and no way to know whether the callee
//! consults the clock, a global, or nothing at all.
//!
//! `abs` happens to be pure, which is exactly why this is the right shape for
//! the test: the analyzer must report `unknown` because it **cannot know**, not
//! because the call is suspicious. Assuming purity for unmodeled FFI is the
//! unsoundness AC2 exists to forbid; assuming impurity would flag every
//! workflow that touches a C library.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Normalizes a signed adjustment through a native routine.
#[workflow]
pub async fn wf_extern_c(ctx: &WorkflowContext, adjustment: i32) -> Result<i32, String> {
    let magnitude = helpers::native_abs(adjustment);
    ctx.execute_activity_raw(
        "adjust",
        serde_json::json!({ "magnitude": magnitude }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(magnitude)
}
