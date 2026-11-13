//! **U03** — a call into a foreign function.
//!
//! **Boundary.** `ffi`.
//!
//! `helpers::native_abs` calls `abs` from an `unsafe extern "C"` block. A
//! foreign function has no MIR body, so the analyzer has nothing to summarize.
//! It cannot know whether the callee reads the clock, a global, or nothing.
//!
//! `abs` is pure, which is why this shape suits the test. The analyzer must
//! report `unknown` because it **cannot know**, not because the call is
//! suspicious. Assuming purity for unmodeled FFI is the
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
