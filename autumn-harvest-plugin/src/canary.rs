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

/// Upper bound on any derived (or user-supplied) retention age, kept safely
/// below the core `RetentionConfig` validator's 10-year ceiling so a
/// pathologically large probe interval can never fail `try_build()`
/// (issue #796, AC9). Realistic canary intervals are seconds-to-minutes; this
/// clamp is purely defensive.
const MAX_DERIVED_RETENTION: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 10);

/// One hour — the floor for the aggressive self-cleaning retention window, and
/// the fallback when the derived staleness-based value would be smaller
/// (issue #796, AC9). The floor must exceed the staleness window so a
/// `GET /admin/canary` read can always see the last recorded success.
const RETENTION_FLOOR: Duration = Duration::from_secs(60 * 60);

/// Opt-in configuration for the built-in synthetic liveness canary (issue #796).
///
/// Passed to [`crate::plugin::HarvestPlugin::synthetic_canary`]. Absent (the
/// plugin default), the canary is entirely off and the runtime is byte-for-byte
/// identical (AC1).
#[derive(Clone, Debug)]
pub struct CanaryConfig {
    /// How often each queue's probe fires. Should be comfortably larger than
    /// the probe's own work (one dispatched activity + a 1s durable timer) —
    /// a few seconds at minimum, tens of seconds recommended.
    interval: Duration,
    /// Probe queues. Defaults to `["default"]`; one canary workflow +
    /// schedule is registered per queue so a single wedged worker pool is
    /// distinguishable from "some queue is draining" (AC3).
    queues: Vec<String>,
    /// Per-probe execution timeout (`execution_timeout` → `deadline_at`). A
    /// wedged probe times out before the next tick, so probes never block
    /// (AC6). Defaults to a fraction of `interval`, strictly below it where
    /// the granularity allows.
    per_probe_timeout: Duration,
    /// Window after which a missing success is considered stale (AC7). `None`
    /// resolves to `2 × interval` via [`Self::effective_staleness_window`].
    staleness_window: Option<Duration>,
    /// How long a completed canary run's history is retained before the
    /// janitor self-cleans it (AC9). `None` resolves via
    /// [`Self::effective_retention`] to a value that always exceeds the
    /// staleness window.
    retention: Option<Duration>,
}

impl CanaryConfig {
    /// Create a canary config firing every `interval`, probing the `"default"`
    /// queue, with a derived per-probe timeout strictly below `interval`
    /// (clamped to at least 1s), and `None` (auto) staleness/retention.
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            queues: vec!["default".to_string()],
            per_probe_timeout: Self::derive_per_probe_timeout(interval),
            staleness_window: None,
            retention: None,
        }
    }

    /// Derive a per-probe timeout strictly below `interval` where the
    /// whole-second granularity allows, clamped to at least 1s (AC6).
    fn derive_per_probe_timeout(interval: Duration) -> Duration {
        (interval / 2)
            .min(interval.saturating_sub(Duration::from_secs(1)))
            .max(Duration::from_secs(1))
    }

    /// Replace the probe queue set (AC3).
    #[must_use]
    pub fn with_queues(mut self, queues: Vec<String>) -> Self {
        self.queues = queues;
        self
    }

    /// Add a single probe queue.
    #[must_use]
    pub fn with_queue(mut self, queue: String) -> Self {
        self.queues.push(queue);
        self
    }

    /// Override the per-probe execution timeout (AC6).
    #[must_use]
    pub const fn with_per_probe_timeout(mut self, timeout: Duration) -> Self {
        self.per_probe_timeout = timeout;
        self
    }

    /// Override the staleness window (AC7). Default: `2 × interval`.
    #[must_use]
    pub const fn with_staleness_window(mut self, window: Duration) -> Self {
        self.staleness_window = Some(window);
        self
    }

    /// Override the canary-history retention window (AC9). Clamped to a sane
    /// range at read time so it can never fail `try_build()`.
    #[must_use]
    pub const fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = Some(retention);
        self
    }

    /// The configured probe interval.
    #[must_use]
    pub const fn interval(&self) -> Duration {
        self.interval
    }

    /// The configured probe queues.
    #[must_use]
    pub fn queues(&self) -> &[String] {
        &self.queues
    }

    /// The configured per-probe execution timeout.
    #[must_use]
    pub const fn per_probe_timeout(&self) -> Duration {
        self.per_probe_timeout
    }

    /// The effective staleness window: the override if set, else `2 × interval`
    /// (AC7).
    #[must_use]
    pub fn effective_staleness_window(&self) -> Duration {
        self.staleness_window
            .unwrap_or_else(|| self.interval.saturating_mul(2))
    }

    /// The effective canary-history retention window (AC9).
    ///
    /// The override if set, else `max(2 × staleness_window, 1h)` — always
    /// exceeding the staleness window so a `GET /admin/canary` read can still
    /// see the last recorded success. The result is clamped to
    /// `[1s, MAX_DERIVED_RETENTION]` so it is always within the core
    /// `RetentionConfig` validator's bounds and can never fail `try_build()`.
    #[must_use]
    pub fn effective_retention(&self) -> Duration {
        let raw = self.retention.unwrap_or_else(|| {
            self.effective_staleness_window()
                .saturating_mul(2)
                .max(RETENTION_FLOOR)
        });
        raw.clamp(Duration::from_secs(1), MAX_DERIVED_RETENTION)
    }
}

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

    #[test]
    fn config_defaults() {
        let cfg = CanaryConfig::new(Duration::from_secs(30));
        // AC3: default probes the "default" queue.
        assert_eq!(cfg.queues(), &["default".to_string()]);
        assert_eq!(cfg.interval(), Duration::from_secs(30));
        // AC6: per-probe timeout strictly below interval.
        assert!(cfg.per_probe_timeout() < cfg.interval());
        assert!(cfg.per_probe_timeout() >= Duration::from_secs(1));
        // AC7: default staleness window is 2 × interval.
        assert_eq!(cfg.effective_staleness_window(), Duration::from_secs(60));
        // AC9: retention floor must EXCEED the staleness window.
        assert!(cfg.effective_retention() > cfg.effective_staleness_window());
    }

    #[test]
    fn per_probe_timeout_is_at_least_one_second_for_tiny_intervals() {
        // A 1s interval cannot yield a strictly-smaller whole-second timeout;
        // the floor is 1s.
        let cfg = CanaryConfig::new(Duration::from_secs(1));
        assert_eq!(cfg.per_probe_timeout(), Duration::from_secs(1));
        // A 2s interval yields a strictly-smaller 1s timeout.
        let cfg = CanaryConfig::new(Duration::from_secs(2));
        assert_eq!(cfg.per_probe_timeout(), Duration::from_secs(1));
        assert!(cfg.per_probe_timeout() < cfg.interval());
    }

    #[test]
    fn config_builders_override_defaults() {
        let cfg = CanaryConfig::new(Duration::from_secs(10))
            .with_queues(vec!["email".to_string(), "sms".to_string()])
            .with_queue("priority".to_string())
            .with_per_probe_timeout(Duration::from_secs(4))
            .with_staleness_window(Duration::from_secs(120))
            .with_retention(Duration::from_secs(7200));
        assert_eq!(cfg.queues(), &["email", "sms", "priority"]);
        assert_eq!(cfg.per_probe_timeout(), Duration::from_secs(4));
        assert_eq!(cfg.effective_staleness_window(), Duration::from_secs(120));
        assert_eq!(cfg.effective_retention(), Duration::from_secs(7200));
    }

    #[test]
    fn effective_retention_is_clamped_within_validator_bounds() {
        // A huge interval would derive a retention above the 10-year ceiling;
        // it must clamp so try_build() cannot fail (AC9).
        let cfg = CanaryConfig::new(MAX_DERIVED_RETENTION);
        assert!(cfg.effective_retention() <= MAX_DERIVED_RETENTION);
        assert!(cfg.effective_retention() >= Duration::from_secs(1));
        // An explicit zero retention is floored to a valid (>= 1s) value.
        let cfg = CanaryConfig::new(Duration::from_secs(30)).with_retention(Duration::ZERO);
        assert!(cfg.effective_retention() >= Duration::from_secs(1));
    }
}
