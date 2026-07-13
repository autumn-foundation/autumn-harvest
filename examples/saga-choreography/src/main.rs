#![allow(clippy::items_after_test_module, clippy::type_complexity)]
//! Saga choreography example: tenant-cancel fan-out.
//!
//! A `tenant_cancel` workflow discovers all in-flight onboarding workflows for
//! a tenant and pushes a `cancel_onboarding` signal to each one using
//! `ctx.signal_external_workflow`. The onboarding workflows wait on that
//! signal and abort gracefully when it arrives.
//!
//! Run against the standalone runner:
//!
//! ```
//! cargo run -p standalone-runner
//! ```
//!
//! The integration tests in this file exercise the cross-workflow signal path
//! under replay using `WorkflowReplayer`.

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Onboarding workflow — waits for provisioning then listens for cancellation
// ---------------------------------------------------------------------------

/// Per-tenant onboarding: provisions resources then waits for either a
/// completion signal or a `cancel_onboarding` cancellation signal.
#[workflow(
    owner = "platform",
    runbook = "https://wiki.acme.com/onboarding-runbook",
    severity = "sev3"
)]
pub async fn onboarding(ctx: &WorkflowContext, input: Value) -> HarvestResult<Value> {
    let tenant_id = input["tenant_id"].as_str().unwrap_or("unknown").to_owned();

    // Simulate a slow provisioning activity.
    ctx.execute_activity_raw(
        "provision_tenant_resources",
        json!({ "tenant_id": tenant_id }),
        "default",
    )
    .await?;

    // Wait for either a completion or a cancellation signal.
    let signal = ctx.wait_for_signal("onboarding_outcome").await?;
    let cancelled = signal["cancelled"].as_bool().unwrap_or(false);

    if cancelled {
        ctx.execute_activity_raw(
            "cleanup_tenant_resources",
            json!({ "tenant_id": tenant_id }),
            "default",
        )
        .await?;
        return Ok(json!({ "tenant_id": tenant_id, "result": "cancelled" }));
    }

    Ok(json!({ "tenant_id": tenant_id, "result": "completed" }))
}

// ---------------------------------------------------------------------------
// Tenant-cancel workflow — signals all in-flight onboardings
// ---------------------------------------------------------------------------

/// Fan-out cancellation: given a list of onboarding execution IDs, signal
/// each one with `cancel_onboarding` so they abort gracefully.
///
/// Uses `ctx.signal_external_workflow` for deterministic, replay-safe delivery.
/// A `target_terminal` or `target_unknown` result means the onboarding already
/// finished — treated as a no-op (not an error) for the cancellation saga.
#[workflow(
    owner = "platform",
    runbook = "https://wiki.acme.com/cancel-runbook",
    severity = "sev2"
)]
pub async fn tenant_cancel(ctx: &WorkflowContext, input: Value) -> HarvestResult<Value> {
    let tenant_id = input["tenant_id"].as_str().unwrap_or("unknown").to_owned();
    let exec_ids: Vec<String> = input["onboarding_exec_ids"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();

    let mut signalled = 0u32;
    let mut already_done = 0u32;

    for raw_id in &exec_ids {
        let target: ExecutionId = match raw_id.parse() {
            Ok(id) => id,
            Err(_) => continue,
        };

        match ctx
            .signal_external_workflow(target, "onboarding_outcome", json!({ "cancelled": true }))
            .await
        {
            Ok(()) => signalled += 1,
            // Target already finished — safe to ignore for a cancellation fan-out.
            Err(HarvestError::ExternalSignalFailed { reason_code, .. })
                if reason_code == "target_terminal" || reason_code == "target_unknown" =>
            {
                already_done += 1;
            }
            Err(e) => return Err(e),
        }
    }

    Ok(json!({
        "tenant_id": tenant_id,
        "signalled": signalled,
        "already_done": already_done,
    }))
}

// ---------------------------------------------------------------------------
// Replay tests — exercise the cross-workflow signal path under WorkflowReplayer
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;

    use autumn_harvest::context::WorkflowContext;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};
    use autumn_harvest::types::{ActivityExecId, ExecutionId, ExternalSignalId};
    use chrono::Utc;
    use serde_json::{Value, json};

    fn tenant_cancel_handler<'a>(
        ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            super::tenant_cancel(ctx, input)
                .await
                .map_err(|e| e.to_string())
        })
    }

    fn onboarding_handler<'a>(
        ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            super::onboarding(ctx, input)
                .await
                .map_err(|e| e.to_string())
        })
    }

    fn tenant_cancel_delivered_history(target_id: ExecutionId) -> Vec<WorkflowEvent> {
        let signal_id = ExternalSignalId::new();
        vec![
            WorkflowEvent::WorkflowStarted {
                input: json!({
                    "tenant_id": "acme",
                    "onboarding_exec_ids": [target_id.to_string()],
                }),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target: target_id,
                signal_name: "onboarding_outcome".to_string(),
                payload: json!({ "cancelled": true }),
                idempotency_key: None,
            },
            WorkflowEvent::ExternalSignalDelivered { signal_id },
            WorkflowEvent::WorkflowCompleted {
                output: json!({ "tenant_id": "acme", "signalled": 1, "already_done": 0 }),
            },
        ]
    }

    fn tenant_cancel_terminal_history(target_id: ExecutionId) -> Vec<WorkflowEvent> {
        let signal_id = ExternalSignalId::new();
        vec![
            WorkflowEvent::WorkflowStarted {
                input: json!({
                    "tenant_id": "acme",
                    "onboarding_exec_ids": [target_id.to_string()],
                }),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target: target_id,
                signal_name: "onboarding_outcome".to_string(),
                payload: json!({ "cancelled": true }),
                idempotency_key: None,
            },
            WorkflowEvent::ExternalSignalFailed {
                signal_id,
                reason_code: "target_terminal".to_string(),
            },
            WorkflowEvent::WorkflowCompleted {
                output: json!({ "tenant_id": "acme", "signalled": 0, "already_done": 1 }),
            },
        ]
    }

    fn onboarding_cancelled_history(tenant_id: &str) -> Vec<WorkflowEvent> {
        let act1 = ActivityExecId::new();
        let act2 = ActivityExecId::new();
        vec![
            WorkflowEvent::WorkflowStarted {
                input: json!({ "tenant_id": tenant_id }),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: act1,
                name: "provision_tenant_resources".to_string(),
                input: json!({ "tenant_id": tenant_id }),
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: act1,
                output: json!(null),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "onboarding_outcome".to_string(),
                payload: json!({ "cancelled": true }),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: act2,
                name: "cleanup_tenant_resources".to_string(),
                input: json!({ "tenant_id": tenant_id }),
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: act2,
                output: json!(null),
            },
            WorkflowEvent::WorkflowCompleted {
                output: json!({ "tenant_id": tenant_id, "result": "cancelled" }),
            },
        ]
    }

    async fn run_replay(
        workflow_name: &str,
        handler: fn(
            &WorkflowContext,
            Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>>,
        events: Vec<WorkflowEvent>,
    ) -> ReplayReport {
        let exec_id = ExecutionId::new();
        let snapshot = HistorySnapshot {
            workflow_name: workflow_name.to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
        };
        let json = serde_json::to_string(&snapshot).unwrap();
        WorkflowReplayer::new()
            .register_fn(workflow_name, handler)
            .replay_from_json(&json)
            .await
            .expect("snapshot must parse")
    }

    use autumn_harvest::testing::ReplayReport;

    #[tokio::test]
    async fn replays_signal_delivered_to_onboarding() {
        let target_id = ExecutionId::new();
        let report = run_replay(
            "tenant_cancel",
            tenant_cancel_handler,
            tenant_cancel_delivered_history(target_id),
        )
        .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay regression: {report}"
        );
    }

    #[tokio::test]
    async fn replays_signal_target_already_terminal() {
        let target_id = ExecutionId::new();
        let report = run_replay(
            "tenant_cancel",
            tenant_cancel_handler,
            tenant_cancel_terminal_history(target_id),
        )
        .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay regression: {report}"
        );
    }

    #[tokio::test]
    async fn onboarding_replays_signal_received_and_cancels() {
        let report = run_replay(
            "onboarding",
            onboarding_handler,
            onboarding_cancelled_history("acme"),
        )
        .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay regression: {report}"
        );
    }
}

fn main() {
    println!("Saga-choreography example — run with `cargo test -p saga-choreography`");
    println!("For an end-to-end demo against a live Postgres instance,");
    println!("wire these workflow handlers into the standalone-runner.");
}
