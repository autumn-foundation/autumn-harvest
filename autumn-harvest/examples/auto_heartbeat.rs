#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Example: auto-heartbeat guard for a long-running activity (issue #682).
//!
//! A long-running activity that makes steady progress but does not want to
//! sprinkle `ctx.heartbeat(..)` calls through its body can install a single
//! auto-heartbeat guard. The guard spawns a background ticker that re-sends the
//! activity's last heartbeat payload (or a `null` liveness ping) every
//! `heartbeat_timeout / 3`, so the heartbeat-timeout scanner never spuriously
//! reclaims a healthy-but-quiet activity — no hand-rolled `tokio::select!` +
//! ticker in every activity.
//!
//! This example is intentionally compile-friendly: it demonstrates the activity
//! code and registration shape without requiring a database connection in
//! `main`. Check it with:
//!
//! ```bash
//! cargo check -p autumn-harvest --example auto_heartbeat --no-default-features
//! cargo check -p autumn-harvest --example auto_heartbeat
//! ```

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

/// A long-running data-crunching activity. It installs the auto-heartbeat guard
/// on the first line, then does its work without any manual heartbeat calls.
///
/// `heartbeat_timeout = "30s"` is what makes `start_auto_heartbeat_default()`
/// valid — the guard derives a `~10s` tick interval from it. Pair it with a
/// `start_to_close` when you need a hard upper bound on runtime: auto-heartbeat
/// keeps a *progressing* activity alive, but the independent `start_to_close`
/// ceiling still fires for a genuinely wedged one.
#[activity(heartbeat_timeout = "30s", start_to_close = "10m")]
#[allow(clippy::missing_errors_doc)]
pub async fn crunch_dataset(ctx: &ActivityContext, job: Value) -> HarvestResult<Value> {
    // One line: liveness pings now happen automatically for the whole activity.
    // Bind to a NAMED local — `let _ = ...` would drop the guard immediately and
    // stop the auto-heartbeat.
    let _guard = ctx.start_auto_heartbeat_default()?;

    let rows = job.get("rows").and_then(Value::as_u64).unwrap_or(0);
    let mut processed = 0u64;
    while processed < rows {
        // ... do a chunk of real CPU/IO work here ...
        processed += 1;

        // Optional: still send a real checkpoint occasionally. The guard's
        // liveness pings re-send whatever you last passed to `heartbeat`, so a
        // later retry can resume from it via `ctx.heartbeat_details()`.
        if processed % 1000 == 0 {
            ctx.heartbeat(json!({"processed": processed})).await?;
        }

        // Cooperatively honour cancellation on long loops — the guard never
        // swallows it (its token is a child of the activity's own token).
        ctx.check_cancellation().await?;
    }

    Ok(json!({"processed": processed}))
}

/// A workflow that dispatches the long-running activity.
#[workflow]
#[allow(clippy::missing_errors_doc)]
pub async fn crunch_workflow(ctx: &WorkflowContext, input: Value) -> HarvestResult<Value> {
    let result: Value = ctx.execute_activity(&crunch_dataset_info(), input).await?;
    Ok(result)
}

fn main() {
    // Registration shape (no DB needed to illustrate it):
    let _workflows = workflows![crunch_workflow];
    let _activities = activities![crunch_dataset];
    println!(
        "auto_heartbeat example: register these with HarvestPlugin.\n\
         The activity installs `let _guard = ctx.start_auto_heartbeat_default()?;`\n\
         and never manually heartbeats — the guard keeps it alive."
    );
}
