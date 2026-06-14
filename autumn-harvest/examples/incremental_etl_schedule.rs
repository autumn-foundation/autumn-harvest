#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Cursor-based incremental ETL schedule (issue #488).
//!
//! Demonstrates the `ctx.last_completion_result()` / `ctx.last_error()` accessors
//! for scheduled workflows that need to process only rows added since the previous
//! successful run — without an external high-water-mark table.
//!
//! ## How it works
//!
//! When a `WorkflowSchedule` fires a new run, Harvest:
//! 1. Queries the **same shard** for the most recent prior COMPLETED run of the
//!    same schedule.
//! 2. Resolves `last_completion_result` (its JSON output) and `last_error` (the
//!    error of the most recent terminal run if it was FAILED/TIMED_OUT).
//! 3. Freezes both values into the immutable `WorkflowStarted` event before the
//!    workflow function is ever invoked.
//!
//! From that point on, replay on any worker returns the identical values from the
//! frozen event — no re-query, no non-determinism.
//!
//! ## Semantics
//!
//! | Accessor | Returns |
//! |---|---|
//! | `ctx.last_completion_result::<T>()` | Output of the most recent *COMPLETED* run of the same schedule, or `None` on the first run or if no prior run succeeded |
//! | `ctx.last_error()` | Error of the most recent *terminal* run if it was FAILED/TIMED_OUT; `None` when the most recent terminal run COMPLETED (i.e. recovered) |
//!
//! **Manual starts** (not triggered by a schedule) always see `None` for both.
//!
//! **Skipped fires** (`OverlapPolicy::Skip`) do not advance the cursor — the
//! skipped slot never calls `start_or_load_workflow_execution`, so the next run's
//! `last_completion_result` still refers to the last non-skipped COMPLETED run.
//!
//! ## Example
//!
//! ```rust
//! use autumn_harvest::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct Cursor {
//!     last_processed_id: i64,
//!     rows_processed: u64,
//! }
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct EtlResult {
//!     cursor: Cursor,
//!     batch_rows: u64,
//! }
//!
//! #[workflow]
//! async fn incremental_etl(
//!     ctx: &WorkflowContext,
//!     _input: (),
//! ) -> Result<EtlResult, String> {
//!     // Retrieve the cursor from the last successful run (None on first run).
//!     let prior_cursor: Option<Cursor> = ctx
//!         .last_completion_result::<EtlResult>()
//!         .map_err(|e| e.to_string())?
//!         .map(|r| r.cursor);
//!
//!     // Log when recovering from a previous failure.
//!     if let Some(err) = ctx.last_error() {
//!         // last_error() is Some only when the most recent terminal run was FAILED/TIMED_OUT.
//!         ctx.logger().warn(&format!("Recovering after previous failure: {err}"));
//!     }
//!
//!     let since_id = prior_cursor
//!         .as_ref()
//!         .map(|c| c.last_processed_id)
//!         .unwrap_or(0);
//!     let total_processed = prior_cursor
//!         .as_ref()
//!         .map(|c| c.rows_processed)
//!         .unwrap_or(0);
//!
//!     // Activity would fetch rows WHERE id > since_id.
//!     // let batch = ctx.execute_activity(&fetch_batch_info(), since_id).await?;
//!     let batch_rows: u64 = 0; // placeholder
//!     let last_processed_id = since_id; // placeholder
//!
//!     Ok(EtlResult {
//!         cursor: Cursor {
//!             last_processed_id,
//!             rows_processed: total_processed + batch_rows,
//!         },
//!         batch_rows,
//!     })
//! }
//!
//! // Register with an hourly schedule:
//! //
//! // use autumn_harvest::policy::{Schedule, WorkflowSchedule};
//! //
//! // let schedule = WorkflowSchedule::new(
//! //     "incremental_etl",
//! //     Schedule::Interval(std::time::Duration::from_secs(3600)),
//! // );
//! ```

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cursor {
    last_processed_id: i64,
    rows_processed: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct EtlResult {
    cursor: Cursor,
    batch_rows: u64,
}

fn main() {
    println!("incremental_etl_schedule example — compile-time demonstration.");
    println!("See the module-level docs above for a full usage walkthrough.");

    let first_run_cursor: Option<Cursor> = None;
    let since_id = first_run_cursor
        .as_ref()
        .map(|c| c.last_processed_id)
        .unwrap_or(0);

    println!("First run: processing rows with id > {since_id}");

    let second_run_cursor = Some(Cursor {
        last_processed_id: 42,
        rows_processed: 100,
    });
    let since_id = second_run_cursor
        .as_ref()
        .map(|c| c.last_processed_id)
        .unwrap_or(0);

    println!("Second run: processing rows with id > {since_id}");
    println!(
        "Total rows processed across all runs: {}",
        second_run_cursor
            .as_ref()
            .map(|c| c.rows_processed)
            .unwrap_or(0)
    );
}
