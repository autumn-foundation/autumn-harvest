//! **C16** — inherent float methods.
//!
//! MIR prints `f64::<impl f64>::round` and `f64::<impl f64>::mul_add` with a
//! primitive as the first segment. The resolver once took `f64` for the name
//! of an untrusted crate. Inherent methods on a primitive live in core and
//! std. Verdict: `proven-deterministic`.

use autumn_harvest::prelude::*;

/// Rounds a size in MiB to tenths of a GiB.
fn gib(mib: f64) -> f64 {
    (mib / 1024.0 * 10.0).round() / 10.0
}

/// Reports a weighted score.
#[workflow]
pub async fn wf_float_inherent(ctx: &WorkflowContext, mib: f64) -> Result<f64, String> {
    let score = gib(mib).mul_add(2.0, 1.0);
    ctx.execute_activity_raw("score", serde_json::json!({ "score": score }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(score)
}
