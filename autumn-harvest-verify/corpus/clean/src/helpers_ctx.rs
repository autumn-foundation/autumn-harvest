//! Corpus-local helpers that take a `&WorkflowContext` and emit commands.
//!
//! A helper containing a sink is ordinary, good code. The question the analyzer
//! must answer is whether anything **tainted** reaches that sink — not whether
//! the sink is inside the annotated body.

use autumn_harvest::prelude::*;

/// Schedules one stage of a job. Every argument is derived from parameters.
///
/// # Errors
/// Propagates the activity failure as a string.
pub async fn dispatch_stage(ctx: &WorkflowContext, stage: &str, job: &str) -> Result<(), String> {
    ctx.execute_activity_raw(stage, serde_json::json!({ "job": job }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}
