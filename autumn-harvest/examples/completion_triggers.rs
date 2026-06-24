//! Declarative completion triggers — two-stage ETL → reporting (issue #517).
//!
//! Run workflow **B** *because* workflow **A** finished — without A knowing B
//! exists. This decouples multi-stage pipelines: no clock-offset races, and no
//! hard-coding the downstream stage as a child spawn inside the upstream code.
//!
//! ## The pipeline
//!
//! - **Stage 1 — `etl_ingest`**: runs on whatever cadence you like and returns an
//!   output such as `{ "summary": { "rows": 12000, "date": "2026-06-24" }, ... }`.
//! - **Stage 2 — `daily_report`**: must run the moment `etl_ingest` *completes*,
//!   receiving just the `summary` sub-object as its input.
//!
//! Wiring this dependency touches neither workflow's body — it is a single
//! declarative `CompletionTrigger` registration:
//!
//! ```rust
//! use autumn_harvest::prelude::*;
//! use autumn_harvest::completion_trigger::{CompletionTrigger, InputMapping, TerminalState};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Summary { rows: u64, date: String }
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct IngestOutput { summary: Summary }
//!
//! // Stage 1: the upstream ingest. It knows nothing about reporting.
//! #[workflow]
//! async fn etl_ingest(_ctx: &WorkflowContext, _input: ()) -> Result<IngestOutput, String> {
//!     // ... pull, transform, load ...
//!     Ok(IngestOutput { summary: Summary { rows: 12_000, date: "2026-06-24".into() } })
//! }
//!
//! // Stage 2: the downstream report. It takes the projected `summary` as its input.
//! #[workflow]
//! async fn daily_report(_ctx: &WorkflowContext, summary: Summary) -> Result<(), String> {
//!     // ... render and publish the report for `summary.date` ...
//!     Ok(())
//! }
//!
//! // The single declarative edge between them:
//! let pipeline = CompletionTrigger::new("etl_ingest", "daily_report")
//!     .with_terminal_states(vec![TerminalState::Completed]) // `Completed` is also the default
//!     .with_input_mapping(InputMapping::Projection("summary".to_string()));
//!
//! let harvest = HarvestBuilder::new()
//!     .workflows(workflows![etl_ingest, daily_report])
//!     .completion_trigger(pipeline);
//! ```
//!
//! ## Contract
//!
//! - **Exactly-once-after-dedupe**: when an `etl_ingest` run with execution id `E`
//!   completes, Harvest starts `daily_report` with the deterministic id
//!   `completion-trigger-{trigger_id}-E`. A re-delivered or replayed terminal
//!   transition collapses onto that same id (via the `harvest_completion_trigger_fires`
//!   ledger + start-or-load reuse policy) — never a second `daily_report`.
//! - **State matching**: if `etl_ingest` instead FAILS, nothing starts — the trigger
//!   only matches `Completed`. Register additional `TerminalState`s (or a `Failed`
//!   trigger to a different target) if you want an on-failure branch.
//! - **No new event variant**: the target's start emits an ordinary `WorkflowStarted`;
//!   the source emits nothing extra.
//!
//! See `docs/completion-triggers.md` for the full reference, including the
//! cross-shard at-least-once delivery contract.

use autumn_harvest::completion_trigger::{
    CompletionTrigger, InputMapping, TerminalState, project_json_path,
};
use serde_json::json;

fn main() {
    // The declarative edge: `etl_ingest` COMPLETED -> start `daily_report` with the
    // source output's `summary` field as input.
    let trigger = CompletionTrigger::new("etl_ingest", "daily_report")
        .with_terminal_states(vec![TerminalState::Completed])
        .with_input_mapping(InputMapping::Projection("summary".to_string()));

    println!("Completion trigger:");
    println!("  source : {}", trigger.source_workflow_name);
    println!("  target : {}", trigger.target_workflow_name);
    println!(
        "  fires on: {:?}",
        trigger
            .terminal_states
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
    );

    // Simulate a completed `etl_ingest` run and its output.
    let source_output = json!({
        "summary": { "rows": 12_000, "date": "2026-06-24" },
        "debug": { "duration_ms": 8421 }
    });

    // Apply the input mapping exactly as the engine does at fire time.
    let target_input = match &trigger.input_mapping {
        InputMapping::Passthrough => source_output,
        InputMapping::Static(v) => v.clone(),
        InputMapping::Projection(path) => project_json_path(&source_output, path),
    };
    println!("\nProjected `daily_report` input: {target_input}");
    assert_eq!(
        target_input,
        json!({ "rows": 12_000, "date": "2026-06-24" })
    );

    // The deterministic target workflow id makes duplicate fires collapse.
    let source_exec_id = "0000abcd-0000-0000-0000-000000000001";
    let target_workflow_id = format!("completion-trigger-{}-{}", trigger.id, source_exec_id);
    println!("Deterministic target workflow_id: {target_workflow_id}");

    // A FAILED source would NOT fire this `Completed`-only trigger.
    let fires_on_failed = trigger.terminal_states.contains(&TerminalState::Failed);
    println!("\nWould fire on a FAILED source? {fires_on_failed}");
    assert!(!fires_on_failed);

    println!("\nWired `etl_ingest` -> `daily_report` with a single registration.");
}
