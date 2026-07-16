//! Published interaction schema + `/interface` discovery (issue #610).
//!
//! Extends the #373 workflow input/output/error schema story to the
//! *interaction* surface — a workflow's signals, queries, and updates — so an
//! operator or non-Rust caller can discover the exact JSON shape of every way
//! to talk to a running workflow, and so malformed payloads are rejected at the
//! HTTP boundary before durable enqueue.
//!
//! Demonstrates:
//! 1. Declaring a `#[signal]`, `#[query]`, and `#[update]` with a
//!    `description = "…"`, and attaching a JSON Schema at registration via
//!    `with_schemas::<…>()` (auto-derived from `schemars::JsonSchema` types).
//! 2. Building the `WorkflowInterfaceRecord` that
//!    `GET /workflows/registered/{name}/interface` returns.
//! 3. `validate_arg` rejecting a malformed signal payload with a structured
//!    RFC-6901 `field_path` error — the exact check the signal, signal-with-start,
//!    and update HTTP routes run before a payload is enqueued.
//!
//! Run with:
//!   cargo run --example interface_schema_workflow --features schema

#![allow(
    clippy::unused_async,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::used_underscore_binding,
    clippy::needless_pass_by_value
)]

use autumn_harvest::info::{InterfaceHandlerRecord, WorkflowInterfaceRecord};
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

// ── Interaction payload types ───────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CancelRequest {
    /// Human-readable reason for the cancellation.
    pub reason: String,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StatusRequest {
    /// Include a verbose per-item breakdown in the response.
    pub verbose: bool,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StatusResponse {
    pub state: String,
    pub processed: u64,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SetPriority {
    /// New priority for the run (higher = sooner).
    pub priority: i64,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PriorityAck {
    pub applied: bool,
}

// ── Workflow + its interaction handlers ─────────────────────────────────────

#[workflow(description = "Fulfils an order; reacts to cancel signals and priority updates")]
pub async fn order_workflow(_ctx: &WorkflowContext, _input: ()) -> Result<(), String> {
    // A real implementation would register these handlers on `ctx` and
    // orchestrate activities. Here we only demonstrate the published schema.
    Ok(())
}

/// Signal: cancel the in-flight order.
///
/// The `#[signal]` macro derives `cancel_order_info()` from this signature; the
/// function body itself is never invoked (signals have no handler fn — they are
/// dispatched via `register_signal_handler`), so it is intentionally unused here.
#[allow(dead_code)]
#[signal(
    workflow = "order_workflow",
    description = "Cancel the in-flight order"
)]
fn cancel_order(_ctx: &WorkflowContext, _req: CancelRequest) {}

/// Query: read the order's current progress.
#[query(
    workflow = "order_workflow",
    description = "Read the order's current progress"
)]
fn order_status(_ctx: &WorkflowContext, req: StatusRequest) -> Result<StatusResponse, String> {
    Ok(StatusResponse {
        state: if req.verbose {
            "RUNNING (verbose)".into()
        } else {
            "RUNNING".into()
        },
        processed: 0,
    })
}

/// Update: change the run's priority.
#[update(workflow = "order_workflow", description = "Change the run's priority")]
async fn set_priority(_ctx: &WorkflowContext, _req: SetPriority) -> Result<PriorityAck, String> {
    Ok(PriorityAck { applied: true })
}

// ── Interface record assembly (mirrors the /interface endpoint) ─────────────

/// Build the `WorkflowInterfaceRecord` for `order_workflow`, attaching the
/// auto-derived JSON Schema to each handler. This is exactly what
/// `GET /workflows/registered/order_workflow/interface` serialises.
fn order_interface() -> WorkflowInterfaceRecord {
    WorkflowInterfaceRecord {
        signals: vec![InterfaceHandlerRecord::from_signal(
            &cancel_order_info().with_schemas::<CancelRequest>(),
        )],
        queries: vec![InterfaceHandlerRecord::from_query(
            &order_status_info().with_schemas::<StatusRequest, StatusResponse>(),
        )],
        updates: vec![InterfaceHandlerRecord::from_update(
            &set_priority_info().with_schemas::<SetPriority, PriorityAck>(),
        )],
    }
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() {
    println!("=== Harvest workflow interaction schema demo (issue #610) ===\n");

    // GET /workflows/registered/order_workflow/interface
    let record = order_interface();
    println!("GET /workflows/registered/order_workflow/interface:");
    println!("{}\n", serde_json::to_string_pretty(&record).unwrap());

    // Server-side signal payload validation (the check the HTTP routes run
    // before enqueue).
    println!("--- Boundary validation of a signal payload ---");
    let cancel_info = cancel_order_info().with_schemas::<CancelRequest>();

    let valid = serde_json::json!({ "reason": "customer changed their mind" });
    match cancel_info.validate_arg(&valid) {
        Ok(()) => println!("✓ Valid `cancel` payload accepted"),
        Err(v) => println!("✗ Unexpected rejection: {v:?}"),
    }

    // Missing the required `reason` field → 400 with a field-level pointer.
    let malformed = serde_json::json!({});
    match cancel_info.validate_arg(&malformed) {
        Ok(()) => println!("✗ Should have been rejected"),
        Err(violations) => {
            println!("✓ Malformed `cancel` payload rejected (HTTP 400 would be returned):");
            for v in &violations {
                println!(
                    "   field_path={}, message={}",
                    v.field_path.as_deref().unwrap_or("(root)"),
                    v.message
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interface_record_carries_schemas_and_descriptions_for_all_three_kinds() {
        let record = order_interface();

        // Signal: arg_schema + description, never a response_schema.
        let sig = &record.signals[0];
        assert_eq!(sig.name, "cancel_order");
        assert_eq!(
            sig.description.as_deref(),
            Some("Cancel the in-flight order")
        );
        assert!(sig.arg_schema.is_some(), "signal must carry an arg_schema");
        assert!(
            sig.response_schema.is_none(),
            "signals have no response_schema"
        );

        // Query: arg + response schema + description.
        let q = &record.queries[0];
        assert_eq!(q.name, "order_status");
        assert_eq!(
            q.description.as_deref(),
            Some("Read the order's current progress")
        );
        assert!(q.arg_schema.is_some());
        assert!(q.response_schema.is_some());

        // Update: arg + response schema + description.
        let u = &record.updates[0];
        assert_eq!(u.name, "set_priority");
        assert_eq!(u.description.as_deref(), Some("Change the run's priority"));
        assert!(u.arg_schema.is_some());
        assert!(u.response_schema.is_some());
    }

    #[test]
    fn malformed_signal_payload_is_rejected_with_field_path() {
        let info = cancel_order_info().with_schemas::<CancelRequest>();
        let violations = info
            .validate_arg(&serde_json::json!({}))
            .expect_err("missing required `reason` must be rejected");
        assert!(
            violations
                .iter()
                .any(|v| v.field_path.as_deref() == Some("/reason")),
            "expected a violation at /reason, got: {violations:?}"
        );
        assert!(violations.iter().all(|v| !v.message.is_empty()));
    }

    #[test]
    fn valid_signal_payload_passes() {
        let info = cancel_order_info().with_schemas::<CancelRequest>();
        assert!(
            info.validate_arg(&serde_json::json!({ "reason": "ok" }))
                .is_ok()
        );
    }
}
