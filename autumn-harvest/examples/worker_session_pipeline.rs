#![allow(clippy::doc_markdown, clippy::missing_errors_doc, clippy::unused_async)]
//! Worker session pipeline example (issue #606).
//!
//! Demonstrates co-locating a `download -> transcode -> upload` pipeline on a
//! single physical worker so the three activities can share machine-local
//! state (here: a scratch-directory path) instead of round-tripping the
//! artifact through external storage between every hop.
//!
//! Adoption cost, per the issue's success metric, is exactly:
//! 1. One builder call: `WorkerConfig::with_max_concurrent_sessions(n)`.
//! 2. Wrapping the pipeline in a session scope (`ctx.create_session(...)`)
//!    and dispatching through `session.execute_activity(...)` instead of
//!    `ctx.execute_activity(...)`.
//!
//! No change to the activity signatures themselves — `download_chunk`,
//! `transcode_chunk`, and `upload_chunk` below are ordinary `#[activity]`
//! functions; only the workflow's dispatch calls change.
//!
//! See the "Worker Sessions" decision-matrix section of `CLAUDE.md` for when
//! to reach for a session instead of a local activity, a plain activity, or
//! claim-check payload offloading (issue #524).
//!
//! Run with:
//!   cargo run --example worker_session_pipeline

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

// ── Domain types ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MediaJob {
    pub source_url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MediaResult {
    pub uploaded_url: String,
    pub bytes: u64,
}

// ── Activities ───────────────────────────────────────────────────────────────
//
// Each activity operates on a *local filesystem path* on the host that ran
// the previous step — that locality guarantee is exactly what a worker
// session provides. Without a session, `transcode_chunk` could land on a
// different worker than `download_chunk` and would find no such file.

#[activity(start_to_close = "5m", queue = "gpu-workers")]
pub async fn download_chunk(_ctx: &ActivityContext, source_url: String) -> Result<String, String> {
    println!("[Activity] download_chunk({source_url}) -> /scratch/input.bin");
    // Real implementation: stream the ~2GB source artifact to local scratch
    // disk and return its path.
    Ok("/scratch/input.bin".to_string())
}

#[activity(start_to_close = "10m", queue = "gpu-workers")]
pub async fn transcode_chunk(_ctx: &ActivityContext, local_path: String) -> Result<String, String> {
    println!("[Activity] transcode_chunk({local_path}) -> /scratch/output.bin");
    // Real implementation: run the transcode in-place against the file
    // `download_chunk` just wrote — no re-fetch needed.
    Ok("/scratch/output.bin".to_string())
}

#[activity(start_to_close = "5m", queue = "gpu-workers")]
pub async fn upload_chunk(
    _ctx: &ActivityContext,
    local_path: String,
) -> Result<MediaResult, String> {
    println!("[Activity] upload_chunk({local_path})");
    // Real implementation: stream the transcoded file to durable storage.
    Ok(MediaResult {
        uploaded_url: "s3://media-bucket/output.bin".to_string(),
        bytes: 2_000_000_000,
    })
}

// ── Workflow ─────────────────────────────────────────────────────────────────

/// The entire session-scoped pipeline. Three activities, one worker, one
/// local artifact — never re-fetched or round-tripped between steps.
#[workflow]
pub async fn transcode_pipeline(
    ctx: &WorkflowContext,
    job: MediaJob,
) -> Result<MediaResult, String> {
    // Open the session: the acquiring worker hosts every activity dispatched
    // through `session` below for the session's entire lifetime. Bounded by
    // `SessionOptions::acquisition_timeout` (default 30s) — if no worker
    // currently advertises free session capacity, this fails fast with a
    // typed `HarvestError::SessionAcquireTimeout` rather than hanging.
    let session = ctx
        .create_session(SessionOptions::new("gpu-workers"))
        .await
        .map_err(|e| e.to_string())?;

    // `session.execute_activity` — same call shape as `ctx.execute_activity`,
    // hard-pinned to the session's host worker.
    let local_input: String = session
        .execute_activity(&download_chunk_info(), job.source_url)
        .await
        .map_err(|e| e.to_string())?;

    let local_output: String = session
        .execute_activity(&transcode_chunk_info(), local_input)
        .await
        .map_err(|e| e.to_string())?;

    let result: MediaResult = session
        .execute_activity(&upload_chunk_info(), local_output)
        .await
        .map_err(|e| e.to_string())?;

    // Release the session's slot on the host worker. If the host died or
    // drained mid-pipeline, this (like the activity calls above) returns
    // `HarvestError::SessionBroken` instead of silently retrying elsewhere —
    // the caller re-establishes a fresh session rather than trusting stale
    // local state on an unreachable host.
    session.complete().await.map_err(|e| e.to_string())?;

    Ok(result)
}

fn main() {
    println!("worker_session_pipeline example loaded successfully!");
    println!();
    println!("Workflow exported:");
    println!("  transcode_pipeline — download -> transcode -> upload, one worker session");
    println!();
    println!("Register on a HarvestBuilder:");
    println!("  .workflows(workflows![transcode_pipeline])");
    println!("  .activities(activities![download_chunk, transcode_chunk, upload_chunk])");
    println!("  .worker(WorkerConfig::default().with_max_concurrent_sessions(2))");
}
