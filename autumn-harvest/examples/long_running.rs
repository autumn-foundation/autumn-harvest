#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Example: long-running polling workflow with continue-as-new guardrails.
//!
//! This example is intentionally compile-friendly: it demonstrates the workflow
//! code and registration shape without requiring a database connection in
//! `main`. Check it with:
//!
//! ```bash
//! cargo check -p autumn-harvest --example long_running --no-default-features
//! cargo check -p autumn-harvest --example long_running
//! ```

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

// Two independent lifetime bounds (issue #617):
//
//   * `execution_timeout = "1h"` bounds a SINGLE run. It is re-anchored on every
//     `continue_as_new`, so a healthy poller that checkpoints and continues never
//     trips it — it only kills a single hung/runaway *attempt*.
//
//   * `chain_execution_timeout = "7d"` bounds the WHOLE continue-as-new chain. It
//     is anchored at the first run's start and carried verbatim across every
//     `continue_as_new`, so the deadline is the same absolute instant for run #1
//     and run #500. This is the runaway-loop protection a per-run cap cannot give:
//     a bug that keeps continuing-as-new without making progress is force-timed-out
//     (`TIMED_OUT`) once the chain outlives 7 days, and the metric
//     `harvest.workflow.chain_timeout` distinguishes it from a per-run timeout.
//
// Fleet-wide alternative: `HarvestBuilder::max_workflow_chain_timeout(d)` caps AND
// defaults every chain, so you can bound total lifetime fleet-wide even for
// workflows that declare no `chain_execution_timeout`.
#[workflow(execution_timeout = "1h", chain_execution_timeout = "7d")]
#[allow(clippy::missing_errors_doc)]
pub async fn poll_customer_exports(ctx: &WorkflowContext, state: Value) -> HarvestResult<Value> {
    let mut state = state;
    loop {
        let cycle = state.get("cycle").and_then(Value::as_u64).unwrap_or(0);
        let cursor = state.get("cursor").cloned().unwrap_or(Value::Null);

        let page: Value = ctx
            .execute_activity(
                &poll_customer_export_page_info(),
                json!({
                    "cycle": cycle,
                    "cursor": cursor,
                }),
            )
            .await?;

        if page.get("ready").and_then(Value::as_bool).unwrap_or(false) {
            return Ok(json!({
                "status": "ready",
                "page": page,
                "history_events": ctx.history_event_count(),
            }));
        }

        let next_state = json!({
            "cycle": cycle + 1,
            "cursor": page.get("next_cursor").cloned().unwrap_or(Value::Null),
        });

        if ctx.should_continue_as_new() {
            ctx.continue_as_new(next_state).await?;
            unreachable!("continue_as_new does not resolve while the execution is active");
        }

        let timer_id = format!("poll-delay-{cycle}");
        ctx.timer(&timer_id, 60).await?;
        state = next_state;
    }
}

#[activity(start_to_close = "30s", queue = "pollers")]
#[allow(clippy::missing_errors_doc, clippy::unused_async)]
pub async fn poll_customer_export_page(
    _ctx: &ActivityContext,
    input: Value,
) -> Result<Value, String> {
    let cycle = input.get("cycle").and_then(Value::as_u64).unwrap_or(0);

    Ok(json!({
        "ready": cycle >= 5,
        "next_cursor": format!("page-{cycle}"),
    }))
}

fn main() {
    let _registration = HarvestBuilder::new()
        .workflows(workflows![poll_customer_exports])
        .activities(activities![poll_customer_export_page])
        .history_continue_as_new_threshold(10_000)
        .history_event_hard_cap(20_000)
        .try_build()
        .expect("example registration should be valid");

    println!("long_running example: see module docs for compile checks");
}
