//! **C04** — `match ctx.version(..)` emitting a different command sequence per
//! branch.
//!
//! `version` is a *deterministic* gate: on the live run it returns `max` and
//! records a marker; on replay it returns the recorded value. The branch it
//! controls is therefore **control-clean**, and the two arms may legitimately
//! emit different sequences.
//!
//! This is the single most likely false positive in real code: a naive
//! "switchInt on anything that came from a call ⇒ Control taint" rule flags
//! every versioned workflow in the repository. Verdict:
//! `proven-deterministic`.

use autumn_harvest::prelude::*;

/// Runs the v1 or v2 pipeline depending on the recorded version gate.
#[workflow]
pub async fn wf_ctx_version_branch(ctx: &WorkflowContext, payload: String) -> Result<u32, String> {
    let version = ctx.version("pipeline", 1, 2);
    if version >= 2 {
        ctx.execute_activity_raw(
            "validate",
            serde_json::json!({ "payload": payload }),
            "corpus",
        )
        .await
        .map_err(|e| e.to_string())?;
    }
    ctx.execute_activity_raw(
        "persist",
        serde_json::json!({ "payload": payload }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(version)
}
