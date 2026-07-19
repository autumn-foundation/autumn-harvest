//! Built-in synthetic liveness canary registration (issue #796).
//!
//! Registers a throwaway workflow (one per configured probe queue) plus its
//! reserved activity, and schedules the workflow on an aggressive interval, so
//! the *live* `start → dispatch → activity → durable-timer → complete`
//! execution path is exercised continuously. A wedged pipeline (workers polling
//! but never completing, a stalled scheduler tick, a write-blocked shard) then
//! surfaces within one probe interval — before any customer workflow misses its
//! SLA — with zero operator-authored workflow code.
//!
//! **Distinct from the #512 replay canary.** The replay canary validates *code
//! changes* by replaying in-flight histories against new workflow code. This
//! synthetic liveness canary validates *the running pipeline* by actively
//! executing a real throwaway workflow end to end. The two share only the word
//! "canary".
//!
//! The reserved names and predicates live in the core crate
//! ([`autumn_harvest::canary`]); this plugin module owns the *registration*
//! (workflow/activity `Info` construction, per-writable-shard schedule,
//! aggressive self-cleaning retention) and the opt-in [`CanaryConfig`].

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use autumn_harvest::prelude::*;

/// Handler for the built-in synthetic liveness canary workflow (issue #796).
///
/// Exercises the full live execution path: dispatch one **non-local** activity
/// on the probe's target queue (proving the claim/dispatch/complete path), then
/// wait on a short **durable** timer (proving the scheduler/timer path), then
/// complete. Matches [`autumn_harvest::WorkflowHandlerFn`] exactly so it can be
/// stored directly in a hand-built [`WorkflowInfo`] whose `name` is the
/// per-queue probe name (which the `#[workflow]` macro cannot express, since it
/// derives the name from the function identifier).
fn canary_workflow_handler(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + '_>> {
    Box::pin(async move {
        let queue = input
            .get("queue")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("default")
            .to_string();

        // AC2: a real DISPATCHED (non-local) activity on the probe's queue.
        ctx.execute_activity_raw(
            autumn_harvest::canary::CANARY_ACTIVITY_NAME,
            serde_json::json!({}),
            &queue,
        )
        .await
        .map_err(|e| e.to_string())?;

        // AC2: a short DURABLE timer (whole-second granularity).
        ctx.timer("probe", 1).await.map_err(|e| e.to_string())?;

        Ok(serde_json::Value::Null)
    })
}

/// Handler for the built-in synthetic liveness canary activity (issue #796).
///
/// Trivial by design — its only job is to prove the dispatch path works end to
/// end. Unlike the reserved worker-session internal activities (issue #606),
/// whose handlers are never invoked, this handler is *actually executed* on
/// every probe. Matches [`autumn_harvest::ActivityHandlerFn`] exactly.
fn canary_activity_handler(
    _ctx: &ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + '_>> {
    Box::pin(async move { Ok(serde_json::json!({ "ok": true })) })
}

/// Build the [`WorkflowInfo`] for a per-queue synthetic liveness canary
/// workflow (issue #796).
///
/// The name is dynamic (`{PREFIX}__{queue}`), so — unlike a `#[workflow]`
/// macro — the `Info` is hand-built and the runtime `String` name is
/// `Box::leak`ed into the required `&'static str` field. The number of canary
/// workflows is bounded (one per configured probe queue, registered once at
/// startup), so this bounded leak mirrors the MCP tool-route precedent.
///
/// `per_probe_timeout` is stamped as `execution_timeout` (AC6) so a wedged
/// probe times out rather than blocking the next tick.
#[must_use]
pub fn canary_workflow_info(workflow_name: String, per_probe_timeout: Duration) -> WorkflowInfo {
    let name: &'static str = Box::leak(workflow_name.into_boxed_str());
    WorkflowInfo {
        name,
        module: "autumn_harvest_plugin::canary",
        handler: canary_workflow_handler,
        execution_timeout: Some(per_probe_timeout),
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: Some("Built-in synthetic liveness canary probe (issue #796)."),
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
        mcp: false,
    }
}

/// Build the [`ActivityInfo`] for the built-in synthetic liveness canary
/// activity (issue #796).
///
/// `is_local = false` is load-bearing: AC2 requires a genuinely **dispatched**
/// activity so the claim/dispatch/complete path is exercised, not an inline
/// local activity.
#[must_use]
pub fn canary_activity_info() -> ActivityInfo {
    ActivityInfo {
        name: autumn_harvest::canary::CANARY_ACTIVITY_NAME,
        module: "autumn_harvest_plugin::canary",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(10)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        default_schedule_to_close: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: canary_activity_handler,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canary_workflow_info_uses_the_given_name_and_timeout() {
        let info = canary_workflow_info(
            "__harvest_canary_probe__default".to_string(),
            Duration::from_secs(15),
        );
        assert_eq!(info.name, "__harvest_canary_probe__default");
        assert!(autumn_harvest::canary::is_canary_workflow(info.name));
        assert_eq!(info.execution_timeout, Some(Duration::from_secs(15)));
        assert!(!info.mcp);
        assert!(info.concurrency.is_none());
    }

    #[test]
    fn canary_activity_info_is_dispatched_not_local() {
        let info = canary_activity_info();
        assert_eq!(info.name, autumn_harvest::canary::CANARY_ACTIVITY_NAME);
        // AC2: must be a dispatched activity, never a local (inline) one.
        assert!(!info.is_local);
        assert_eq!(info.default_start_to_close, Some(Duration::from_secs(10)));
    }
}
