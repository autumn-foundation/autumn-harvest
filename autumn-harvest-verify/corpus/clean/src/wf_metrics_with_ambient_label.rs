//! **C08** — a **tainted value reaching a non-sink**: an ambient metric label.
//!
//! `helpers::origin_tag()` is genuinely non-deterministic (it embeds
//! `std::process::id()`), and it is passed straight into
//! `ctx.metrics().counter(..)`. That is *fine*: `UserMetrics` is
//! replay-suppressed (issue #532) and emits **no command**, so nothing about
//! the recorded history depends on the label. The analyzer's model must
//! classify `metrics()` as a **non-sink**, and taint reaching a non-sink is not
//! a finding.
//!
//! The second-sharpest AC4 test after `side_effect`: an analyzer that treats
//! every `ctx` method as a sink flags this, and observability calls are
//! everywhere in real workflows. Verdict: `proven-deterministic`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Counts the run, labelled by the emitting worker.
#[workflow]
pub async fn wf_metrics_with_ambient_label(
    ctx: &WorkflowContext,
    job: String,
) -> Result<u8, String> {
    let origin = helpers::origin_tag();
    ctx.metrics()
        .counter("corpus_runs", 1, &[("origin", origin.as_str())]);
    ctx.execute_activity_raw("run", serde_json::json!({ "job": job }), "corpus")
        .await
        .map_err(|e| e.to_string())?;
    Ok(1)
}
