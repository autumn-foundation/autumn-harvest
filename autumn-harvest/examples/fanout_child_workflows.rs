#![allow(clippy::doc_markdown, clippy::missing_errors_doc, clippy::unused_async)]
//! Fan-out / parallel child-workflow batch example (issue #601).
//!
//! Sibling to `examples/fanout_batch.rs` (issue #359, activity fan-out) —
//! demonstrates the same three shapes, but fanning out **child workflows**
//! instead of activities. Reach for a child workflow instead of an activity
//! when each unit of work needs its own durable history, timers, signals, or
//! retry/compensation logic — see the "Fan-out / Parallel Child Workflows"
//! section of CLAUDE.md for the full activity-vs-child decision guidance.
//!
//! Demonstrates:
//! 1. Fail-fast fan-out: N child workflows run in parallel; returns on first failure.
//! 2. Collect-all fan-out: N children run in parallel; per-slot success/failure.
//! 3. Dynamic fan-out derived from a prior activity's output.
//!
//! The input collection MUST be derived from already-recorded state (workflow
//! input, prior activity outputs, signals) — never from non-deterministic
//! sources such as the wall clock or a random number generator.
//!
//! Run with:
//!   cargo run --example fanout_child_workflows

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

// ── Domain types ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BatchInput {
    pub items: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ItemResult {
    pub item: String,
    pub processed: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BatchResult {
    pub succeeded: Vec<String>,
    pub failed: Vec<String>,
}

// ── Activities ───────────────────────────────────────────────────────────────

/// Mock "list items" activity that returns a list from some external source.
#[activity(start_to_close = "5s")]
pub async fn fetch_items(_ctx: &ActivityContext, source: String) -> Result<BatchInput, String> {
    println!("[Activity] fetch_items from source: {source}");
    Ok(BatchInput {
        items: vec![
            format!("{source}_item_1"),
            format!("{source}_item_2"),
            format!("{source}_item_3"),
        ],
    })
}

// ── Child workflow ───────────────────────────────────────────────────────────

/// Sub-orchestration that processes a single item.
///
/// Modeled as a child workflow (rather than an activity) because a real
/// per-item pipeline would typically need its own retries, timers, or
/// signals — this example keeps the body trivial to focus on the fan-out
/// primitive. Fails for items starting with "bad_" so the collect-all
/// example can show partial failure.
#[workflow]
pub async fn process_item_child(
    _ctx: &WorkflowContext,
    item: String,
) -> Result<ItemResult, String> {
    if item.starts_with("bad_") {
        return Err(format!("rejected: {item}"));
    }
    Ok(ItemResult {
        processed: true,
        item,
    })
}

// ── Parent workflows ─────────────────────────────────────────────────────────

/// Fail-fast fan-out: spawn one child workflow per item in parallel; stop on
/// first failure.
///
/// ≤ 3 lines of orchestration code for the parallel step:
/// ```rust,ignore
/// let results = ctx.spawn_child_workflow_fan_out(&process_item_child_info(), input.items)
///     .await.map_err(|e| e.to_string())?;
/// ```
#[workflow]
pub async fn fail_fast_child_batch(
    ctx: &WorkflowContext,
    input: BatchInput,
) -> Result<Vec<ItemResult>, String> {
    // Fan-out in one line. All children scheduled concurrently; first Err aborts.
    let results = ctx
        .spawn_child_workflow_fan_out(&process_item_child_info(), input.items)
        .await
        .map_err(|e| e.to_string())?;
    println!(
        "[Workflow] fail_fast_child_batch: all {} children succeeded",
        results.len()
    );
    Ok(results)
}

/// Collect-all fan-out: spawn all children; gather per-slot success / failure.
#[workflow]
pub async fn collect_all_child_batch(
    ctx: &WorkflowContext,
    input: BatchInput,
) -> Result<BatchResult, String> {
    // Fan-out collect-all in one line. Returns Vec<Result<ItemResult, String>>.
    let per_slot: Vec<Result<ItemResult, String>> = ctx
        .spawn_child_workflow_fan_out_collect(&process_item_child_info(), input.items)
        .await
        .map_err(|e| e.to_string())?;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    for slot in per_slot {
        match slot {
            Ok(r) => succeeded.push(r.item),
            Err(e) => failed.push(e),
        }
    }

    println!(
        "[Workflow] collect_all_child_batch: {} succeeded, {} failed",
        succeeded.len(),
        failed.len()
    );
    Ok(BatchResult { succeeded, failed })
}

/// Dynamic fan-out: N is derived from a prior activity output.
///
/// The collection is always derived from durable recorded state (the output of
/// `fetch_items`), never from the wall clock or a random number — this is what
/// keeps fan-out deterministic across replays.
#[workflow]
pub async fn dynamic_child_fan_out(
    ctx: &WorkflowContext,
    source: String,
) -> Result<BatchResult, String> {
    // Step 1: fetch the list from an external source.
    let batch: BatchInput = ctx
        .execute_activity(&fetch_items_info(), source)
        .await
        .map_err(|e| e.to_string())?;

    // Step 2: fan-out one child workflow per fetched item.
    // N is derived from `batch.items` — durable, recorded state.
    let per_slot: Vec<Result<ItemResult, String>> = ctx
        .spawn_child_workflow_fan_out_collect(&process_item_child_info(), batch.items)
        .await
        .map_err(|e| e.to_string())?;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    for slot in per_slot {
        match slot {
            Ok(r) => succeeded.push(r.item),
            Err(e) => failed.push(e),
        }
    }
    Ok(BatchResult { succeeded, failed })
}

fn main() {
    println!("fanout_child_workflows example loaded successfully!");
    println!();
    println!("Workflows exported:");
    println!("  fail_fast_child_batch   — parallel child fan-out, stops on first failure");
    println!("  collect_all_child_batch — parallel child fan-out, collects per-slot results");
    println!("  dynamic_child_fan_out   — N derived from a prior activity's output");
    println!("  process_item_child      — the fanned-out child workflow");
    println!();
    println!(
        "Register on a HarvestBuilder:\n  .workflows(workflows![fail_fast_child_batch, collect_all_child_batch, dynamic_child_fan_out, process_item_child])"
    );
    println!("  .activities(activities![fetch_items])");
}
