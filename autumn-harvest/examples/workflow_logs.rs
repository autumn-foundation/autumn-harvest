#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Durable per-execution workflow logs — issue #790.
//!
//! ## The problem
//!
//! `ctx.log_info` / `log_warn` / `log_error` (issue #379) are replay-safe and
//! already the blessed way to log from workflow code. But they are
//! fire-and-forget to the host app's `tracing` subscriber: reading them back
//! means leaving Vantage for Loki/Elastic/OTel and correlating by
//! `execution_id` by hand. That context switch dominates MTTR for the first
//! question an operator asks — *what did this run actually say?*
//!
//! ## The solution: opt in to the durable sink
//!
//! One builder call. The **workflow code does not change at all**:
//!
//! ```rust,ignore
//! use autumn_harvest::WorkflowLogPolicy;
//!
//! HarvestPlugin::new()
//!     .workflows(workflows![import_batch])
//!     .activities(activities![import_chunk])
//!     // Defaults: 1,000 lines per execution, 4 KiB per message.
//!     .workflow_log_persistence(WorkflowLogPolicy::default())
//! ```
//!
//! Now the same `ctx.log_*` calls are ALSO persisted per execution, readable in
//! one call with zero external log-aggregation correlation:
//!
//! ```bash
//! curl -s .../api/harvest/workflows/{exec_id}/logs \
//!   | jq -r '.lines[] | "\(.level)\t\(.message)"'
//! # info    starting import of 3 chunks
//! # warn    chunk 1 was empty; skipping
//! # info    imported 2 chunks
//!
//! # Or from the CLI:
//! harvest workflow logs {exec_id} --level warn
//! ```
//!
//! Vantage's execution-detail page gains a **Logs** panel over the same rows.
//!
//! ## The contract: observational only
//!
//! | Property | Behaviour |
//! |---|---|
//! | **Not event history** | No `WorkflowEvent` variant; `harvest_events` untouched. |
//! | **No determinism guarantee** | Never replayed, so nothing depends on a line. |
//! | **Never read back** | There is no `ctx.read_logs()`. Need to *act* on it? Make it state. |
//! | **At most once** | Replay suppression + a call-ordinal `seq` + `ON CONFLICT DO NOTHING`. Never duplicated, never reordered; best-effort, so a line can be stored zero times. |
//! | **Bounded** | Per-message byte cap; per-execution line cap with a visible truncation marker. |
//! | **Retention-tied** | `ON DELETE CASCADE` — logs can never outlive their execution. |
//! | **Opt-in** | With the sink off, `ctx.logger()` is byte-for-byte today's behaviour. |
//!
//! The workflow below is written the way you would write it anyway. Everything
//! the feature adds happens at the builder, not in the workflow body — which is
//! the whole point.
//!
//! See `docs/workflow-logs.md` for the full contract.

use autumn_harvest::prelude::*;

/// A batch importer that narrates its own progress.
///
/// Note what is NOT here: no logging sink to configure, no logger to thread
/// through, no `execution_id` to stamp onto each line. `ctx.log_*` is the same
/// call whether or not the durable sink is enabled.
#[workflow]
async fn import_batch(ctx: &WorkflowContext, chunks: Vec<String>) -> Result<u32, String> {
    ctx.log_info(&format!("starting import of {} chunks", chunks.len()));

    let mut imported = 0u32;
    for (i, chunk) in chunks.iter().enumerate() {
        // A log line may embed non-deterministic content — it is never
        // replayed, so nothing depends on it. (Do NOT do this with an activity
        // input, a timer id, or anything that drives control flow.)
        if chunk.is_empty() {
            ctx.log_warn(&format!("chunk {i} was empty; skipping"));
            continue;
        }

        match ctx
            .execute_activity(&import_chunk_info(), chunk.clone())
            .await
        {
            Ok(()) => imported += 1,
            Err(e) => {
                // The error is logged for the operator AND returned, so the
                // run's outcome does not depend on the log line existing.
                ctx.log_error(&format!("chunk {i} failed: {e}"));
                return Err(format!("import failed at chunk {i}"));
            }
        }
    }

    ctx.log_info(&format!("imported {imported} chunks"));
    Ok(imported)
}

#[activity(start_to_close = "30s")]
async fn import_chunk(_ctx: &ActivityContext, chunk: String) -> Result<(), String> {
    let _ = chunk;
    Ok(())
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _wfs = workflows![import_batch];
    let _acts = activities![import_chunk];
    println!("workflow_logs example compiled successfully");
    println!();
    println!("Enable the durable sink on the builder:");
    println!("  .workflow_log_persistence(WorkflowLogPolicy::default())");
    println!();
    println!("Then read a run's lines back in ONE call:");
    println!("  curl .../api/harvest/workflows/{{exec_id}}/logs | jq -r '.lines[].message'");
    println!("  harvest workflow logs {{exec_id}} --level error");
}

// Gated on the `testing` feature as well as `test`: the example itself must
// keep building under `--no-default-features` (which CI exercises), while
// `autumn_harvest::testing` only exists for external consumers when the
// `testing` feature is enabled.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use autumn_harvest::{WorkflowLogLevel, WorkflowLogPolicy};

    /// With the sink ENABLED, the author's lines are recorded durably, in
    /// emission order, with their severities intact.
    #[tokio::test]
    async fn logs_are_recorded_when_the_sink_is_enabled() {
        let outcome = WorkflowTestEnv::new()
            .with_log_policy(Some(WorkflowLogPolicy::default()))
            .mock_activity("import_chunk", |_| Ok(serde_json::json!(null)))
            .run(
                import_batch_info().handler,
                serde_json::json!(["a", "", "b"]),
            )
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let lines: Vec<(WorkflowLogLevel, &str)> = outcome
            .recorded_logs()
            .iter()
            .map(|l| (l.level, l.message.as_str()))
            .collect();
        assert_eq!(
            lines,
            vec![
                (WorkflowLogLevel::Info, "starting import of 3 chunks"),
                (WorkflowLogLevel::Warn, "chunk 1 was empty; skipping"),
                (WorkflowLogLevel::Info, "imported 2 chunks"),
            ],
        );

        // `seq` is a logical-position identity, not a 0-based counter — assert
        // the property that matters (strictly increasing = emission order).
        assert!(
            outcome
                .recorded_logs()
                .windows(2)
                .all(|w| w[0].seq < w[1].seq),
            "seq must be strictly increasing: {:?}",
            outcome.recorded_logs(),
        );
    }

    /// AC6: with the sink DISABLED (the default), `ctx.log_*` still works —
    /// it simply records nothing durable, and the run is identical.
    #[tokio::test]
    async fn nothing_is_recorded_when_the_sink_is_disabled() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("import_chunk", |_| Ok(serde_json::json!(null)))
            .run(import_batch_info().handler, serde_json::json!(["a", "b"]))
            .await;

        assert!(
            outcome.recorded_logs().is_empty(),
            "the default (disabled) sink must record nothing"
        );
        // ...and the workflow itself is completely unaffected.
        assert_eq!(outcome.result, Ok(serde_json::json!(2)));
    }

    /// AC7 + AC2: logging leaves ZERO footprint in the event history, and the
    /// recorded history replays deterministically.
    #[tokio::test]
    async fn logging_leaves_no_event_footprint_and_replays_clean() {
        let outcome = WorkflowTestEnv::new()
            .with_log_policy(Some(WorkflowLogPolicy::default()))
            .mock_activity("import_chunk", |_| Ok(serde_json::json!(null)))
            .run(import_batch_info().handler, serde_json::json!(["a", "b"]))
            .await;

        let dumped = format!("{:?}", outcome.events());
        assert!(
            !dumped.contains("starting import"),
            "a log line must never reach harvest_events: {dumped}"
        );

        let report = outcome.replay_check(import_batch_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay regression:\n{report}"
        );
    }
}
