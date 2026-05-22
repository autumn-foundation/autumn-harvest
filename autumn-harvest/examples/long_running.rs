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

#[workflow]
#[allow(clippy::missing_errors_doc)]
pub async fn poll_customer_exports(
    ctx: &WorkflowContext,
    mut state: Value,
) -> HarvestResult<Value> {
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
