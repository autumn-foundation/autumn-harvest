//! Registration-time rejection of unsupported `WorkflowInfo`-level features
//! (Codex #1069 P2, `runtime.rs:602` `execution_timeout` / `:686` `retry_policy`).
//!
//! [`SqliteRuntime::register_workflow`] retains ONLY `name` + `handler`, so every
//! other execution- or admission-affecting `WorkflowInfo` field would otherwise be
//! silently dropped — diverging from the declared `#[workflow(...)]` contract. This
//! backend rejects such a workflow LOUDLY at registration (a setup-time panic,
//! mirroring the Postgres `HarvestPlugin::build()` panic on misconfiguration),
//! naming the specific feature. Pure metadata / observability-only fields
//! (`description`, `sla`, schemas, owner/runbook/severity, `mcp`) are accepted and
//! inert. These tests drive the PUBLIC API (`register_workflow`) rather than the
//! private audit fn, which has its own in-crate unit coverage.

// The `#[workflow]` macro references the workflow's input parameter, so a
// deliberately-unused param reads as a "used underscore binding" through the
// expansion.
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::unused_async)]

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use serde_json::json;

// ── Rejected: execution_timeout (Codex #1069 P2, runtime.rs:602) ───────────────

#[workflow(execution_timeout = "30s")]
async fn timed_wf(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _ = ctx;
    Ok(0)
}

#[test]
#[should_panic(expected = "execution_timeout")]
fn registering_a_workflow_with_execution_timeout_panics_naming_the_feature() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    // Would silently never enforce the declared hard deadline — reject at setup.
    rt.register_workflow(&timed_wf_info());
}

// ── Rejected: workflow-level retry_policy (Codex #1069 P2, runtime.rs:686) ──────

#[workflow(retry = RetryPolicy::fixed(3, std::time::Duration::from_secs(1)))]
async fn retried_wf(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _ = ctx;
    Ok(0)
}

#[test]
#[should_panic(expected = "retry_policy")]
fn registering_a_workflow_with_workflow_level_retry_panics_naming_the_feature() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    // Would silently ignore the declared workflow-level auto-retry — reject at setup.
    rt.register_workflow(&retried_wf_info());
}

// ── Accepted (inert): metadata / observability-only fields ─────────────────────

#[workflow(description = "an ordinary workflow with only descriptive metadata")]
async fn documented_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let _ = ctx;
    Ok(n + 1)
}

#[tokio::test]
async fn a_metadata_only_workflow_registers_and_runs() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    // `description` is pure metadata (surfaced by GET /workflows/registered on the
    // Postgres HTTP edge, which this backend has no analog for): accepted, inert,
    // never blocks registration or execution.
    rt.register_workflow(&documented_wf_info());
    let exec = rt.start_workflow("documented_wf", json!(41)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(42)),
        "a metadata-only workflow must run to completion, got {state:?}",
    );
}

// ── Accepted: a plain workflow (no attributes at all) is unaffected ────────────

#[workflow]
async fn plain_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let _ = ctx;
    Ok(n * 2)
}

#[tokio::test]
async fn a_plain_workflow_registers_and_runs_unchanged() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&plain_wf_info());
    let exec = rt.start_workflow("plain_wf", json!(21)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(42)),
        "the common no-attribute case must be unaffected, got {state:?}",
    );
}
