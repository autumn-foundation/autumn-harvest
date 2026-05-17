//! Example: Read-only Query handlers for live workflow state inspection (issue #234).
//!
//! Demonstrates `WorkflowContext::register_query_handler` and the `#[query]`
//! macro for exposing progress metrics from a long-running workflow without
//! writing any event to `harvest_events`.
//!
//! A hypothetical batch-processing workflow lets operators call
//! `POST /api/harvest/workflows/{exec_id}/query/progress` to find out how
//! many records have been processed so far, while the workflow is still running.
//!
//! Run with:
//!   cargo run --example `progress_query`

use std::sync::{Arc, Mutex};

use autumn_harvest::prelude::*;
use serde_json::json;

// ---------------------------------------------------------------------------
// Query request / response types
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct ProgressQuery {
    /// When true, include a human-readable summary string in the response.
    include_summary: bool,
}

#[derive(serde::Serialize)]
struct ProgressResponse {
    processed: u64,
    total: u64,
    percent: f32,
    summary: Option<String>,
}

// ---------------------------------------------------------------------------
// Workflow definition
// ---------------------------------------------------------------------------

#[workflow]
async fn batch_processor(ctx: &WorkflowContext, input: ()) -> Result<(), String> {
    let () = input;
    let total: u64 = 1_000;
    let processed = Arc::new(Mutex::new(0u64));

    // Register a typed query handler. The closure captures `processed` and
    // `total` from the surrounding scope. No events are ever written to
    // `harvest_events` — the registry lives only in-memory.
    let query_state = processed.clone();
    #[allow(clippy::cast_precision_loss)]
    ctx.register_query_handler("progress", move |req: &ProgressQuery| {
        let n = *query_state.lock().unwrap();
        let pct = if total > 0 {
            (n as f32 / total as f32) * 100.0
        } else {
            0.0
        };
        Ok(ProgressResponse {
            processed: n,
            total,
            percent: pct,
            summary: if req.include_summary {
                Some(format!("{n}/{total} records processed ({pct:.1}%)"))
            } else {
                None
            },
        })
    });

    // Register a simple no-arg query alongside the typed one.
    ctx.register_query("status", || json!("running"));

    // Simulate batch work (in a real workflow these would be activities).
    for i in 0..total {
        *processed.lock().unwrap() = i + 1;
        // ctx.execute_activity_raw("process_batch_chunk", ...).await?;
    }

    std::future::ready(()).await;
    Ok(())
}

fn main() {
    println!("progress_query example — see source for usage");
    println!();
    println!("After starting the workflow, query its state:");
    println!();
    println!("# Typed query with args:");
    println!(
        r"  curl -X POST http://localhost:8080/api/harvest/workflows/{{exec_id}}/query/progress \"
    );
    println!(r"       -H 'Content-Type: application/json' \");
    println!(r#"       -d '{{"args": {{"include_summary": true}}}}'"#);
    println!();
    println!("# Simple no-arg query (GET or POST):");
    println!("  curl http://localhost:8080/api/harvest/workflows/{{exec_id}}/query/status");
    println!();
    println!("# List all registered query names:");
    println!("  curl http://localhost:8080/api/harvest/workflows/{{exec_id}}/queries");
}
