//! Time-based retention janitor for completed workflow history.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(feature = "db")]
use std::{collections::HashMap, time::Instant};

use chrono::{DateTime, Utc};
#[cfg(feature = "db")]
use diesel::prelude::*;
#[cfg(feature = "db")]
use diesel::sql_types::{Array, BigInt, Nullable, Text, Timestamptz, Uuid as SqlUuid};
#[cfg(feature = "db")]
use diesel_async::{AsyncConnection, RunQueryDsl};
#[cfg(feature = "db")]
use futures::future::join_all;
use serde::Serialize;
#[cfg(feature = "db")]
use tokio::sync::mpsc;
#[cfg(feature = "db")]
use tokio::task::JoinHandle;
#[cfg(feature = "db")]
use tokio_util::sync::CancellationToken;

#[cfg(feature = "db")]
use crate::error::{HarvestError, HarvestResult, database_error};
#[cfg(feature = "db")]
use crate::models::NewExecutionSummary;
#[cfg(feature = "db")]
use crate::schema::harvest_workflow_executions;
#[cfg(feature = "db")]
use crate::schema::{
    harvest_completion_deliveries, harvest_dead_letters, harvest_execution_summaries,
    harvest_signals, harvest_task_queue, harvest_timers,
};
#[cfg(feature = "db")]
use crate::shard::ShardedDbPool;
#[cfg(feature = "db")]
use crate::telemetry::MetricsRecorder;
use crate::types::ShardId;

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(60 * 60);
const DEFAULT_BATCH_SIZE: usize = 1_000;
const MIN_MAX_AGE: Duration = Duration::from_secs(1);
const MAX_MAX_AGE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 10);
const DEFAULT_ARCHIVAL_TIMEOUT_SECS: u64 = 30;

/// Default byte cap for an opt-in captured summary payload (issue #752).
///
/// A `result`/`error` value larger than this is replaced with a typed
/// `_harvest_omitted` marker rather than stored verbatim, keeping each summary
/// row small (~< 1 KiB target for the common case).
pub const DEFAULT_SUMMARY_PAYLOAD_CAP: usize = 4096;

/// JSON key inserted into a summary `result` when the real payload was omitted
/// (offloaded or over the byte cap) — issue #752.
///
/// Distinct from the erasure tombstone (`_harvest_erased`) and the offload
/// envelope (`_harvest_offload_envelope`) so the three are never confused.
pub const OMITTED_MARKER_KEY: &str = "_harvest_omitted";

// ── Summary (tiered) retention config (issue #752) ─────────────────────────────

/// How long summarized (demoted) execution rows are retained (issue #752).
///
/// Decoupled from the history retention horizon: a deployment may keep
/// summaries far longer than full histories, or forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryRetention {
    /// Retain summaries for the given duration (`completed_at`-based), then GC.
    For(Duration),
    /// Never GC summaries — keep them forever.
    Unbounded,
}

/// Tiered/summary retention policy (issue #752).
///
/// When set on [`RetentionConfig`], the history-retention janitor demotes each
/// hard-deleted terminal execution into a compact `harvest_execution_summaries`
/// row (written in the same transaction as the delete) instead of losing it
/// entirely. Captured `result`/`error` payloads are opt-in and byte-bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SummaryPolicy {
    /// Summary retention horizon (own GC pass; `Unbounded` = keep forever).
    pub retention: SummaryRetention,
    /// Whether to capture the run's `result`/`error` payload into the summary.
    /// Defaults to `false` (identity + timing + search-attrs only).
    pub capture_payload: bool,
    /// Byte cap for a captured payload; oversized values become a typed
    /// `_harvest_omitted` marker. Defaults to [`DEFAULT_SUMMARY_PAYLOAD_CAP`].
    pub max_payload_bytes: usize,
}

impl SummaryPolicy {
    /// Retain summaries for `days` days (payload capture off by default).
    #[must_use]
    pub const fn for_days(days: u64) -> Self {
        Self {
            retention: SummaryRetention::For(Duration::from_secs(days * 86_400)),
            capture_payload: false,
            max_payload_bytes: DEFAULT_SUMMARY_PAYLOAD_CAP,
        }
    }

    /// Retain summaries for the given duration (payload capture off by default).
    #[must_use]
    pub const fn for_duration(retention: Duration) -> Self {
        Self {
            retention: SummaryRetention::For(retention),
            capture_payload: false,
            max_payload_bytes: DEFAULT_SUMMARY_PAYLOAD_CAP,
        }
    }

    /// Keep summaries forever (payload capture off by default).
    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            retention: SummaryRetention::Unbounded,
            capture_payload: false,
            max_payload_bytes: DEFAULT_SUMMARY_PAYLOAD_CAP,
        }
    }

    /// Opt into capturing the run's `result`/`error` payload (byte-bounded).
    #[must_use]
    pub const fn with_payload_capture(mut self) -> Self {
        self.capture_payload = true;
        self
    }

    /// Override the captured-payload byte cap.
    #[must_use]
    pub const fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes;
        self
    }

    /// The summary GC horizon as a [`Duration`], or `None` for `Unbounded`.
    #[must_use]
    pub const fn retention_age(&self) -> Option<Duration> {
        match self.retention {
            SummaryRetention::For(age) => Some(age),
            SummaryRetention::Unbounded => None,
        }
    }

    /// Whether captured payloads are enabled for this policy.
    #[must_use]
    pub const fn capture_payload(&self) -> bool {
        self.capture_payload
    }

    /// The captured-payload byte cap.
    #[must_use]
    pub const fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }
}

/// Returns the typed "omitted" marker value for a summary `result` field
/// (issue #752): `{"_harvest_omitted": true, "reason": "...", "bytes": N?}`.
///
/// Used instead of silently truncating an oversized payload or storing an
/// offload reference envelope (whose blob may be GC'd, #524).
#[must_use]
pub fn omitted_marker(reason: &str, bytes: Option<usize>) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        OMITTED_MARKER_KEY.to_string(),
        serde_json::Value::Bool(true),
    );
    obj.insert(
        "reason".to_string(),
        serde_json::Value::String(reason.to_string()),
    );
    if let Some(bytes) = bytes {
        obj.insert("bytes".to_string(), serde_json::Value::Number(bytes.into()));
    }
    serde_json::Value::Object(obj)
}

/// Compute the `result` column value for a summary from a run's `output`
/// (issue #752).
///
/// - `None` output → `None` (nothing captured).
/// - An offload reference envelope (issue #524) → `omitted_marker("offloaded")`
///   — **never** store the envelope, since its blob may be GC'd out from under
///   the summary.
/// - An `output` whose serialized bytes exceed `cap` →
///   `omitted_marker("too_large", Some(len))` (valid JSON, never a silent
///   truncation).
/// - Otherwise the value **verbatim** (codec-encoded form preserved, matching
///   the history-export Full policy — the summary is not decoded/redacted here).
///
/// The `capture_payload` opt-in is applied at the call site (a policy with
/// capture disabled passes `None`, so the column stays NULL).
#[must_use]
pub fn cap_result_payload(
    output: Option<&serde_json::Value>,
    cap: usize,
) -> Option<serde_json::Value> {
    let output = output?;
    // Never store an offload reference envelope — the blob may be GC'd (#524).
    if crate::payload_store::extract_offload_ref(output).is_some() {
        return Some(omitted_marker("offloaded", None));
    }
    let len = serde_json::to_vec(output).map_or(0, |v| v.len());
    if len > cap {
        return Some(omitted_marker("too_large", Some(len)));
    }
    Some(output.clone())
}

/// Compute the `error` column value for a summary from a run's `error` text
/// (issue #752).
///
/// Oversized text becomes a typed marker string rather than a silent
/// UTF-8-boundary truncation. `None` in → `None` out. Capture opt-in is applied
/// at the call site.
#[must_use]
pub fn cap_error_text(error: Option<&str>, cap: usize) -> Option<String> {
    let error = error?;
    if error.len() > cap {
        return Some(format!("[omitted: too_large, {} bytes]", error.len()));
    }
    Some(error.to_string())
}

/// Future type returned by [`HistoryArchiver::archive`].
pub type ArchiverFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send
            + 'a,
    >,
>;

/// Trait for pre-retention workflow history cold storage archivers.
///
/// Implementations of this trait are invoked by the retention janitor to ship
/// a completed workflow execution's event history to cold storage *before* it
/// is permanently deleted from the database.
pub trait HistoryArchiver: Send + Sync + 'static {
    /// Ship the history export document to cold storage.
    ///
    /// If this returns `Err`, the retention janitor skips deleting the
    /// workflow execution and its associated events on this tick, retrying
    /// on the next tick to prevent data loss.
    fn archive(&self, doc: &crate::history_export::HistoryExportDocument) -> ArchiverFuture<'_>;
}

/// Configuration for the background retention job.
///
/// **Why does this exist?**
/// Workflow histories and audit logs can grow unbounded. This configuration allows operators
/// to define constraints for automatically pruning old, closed workflows and stale audit events
/// to prevent storage exhaustion.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::retention::RetentionConfig;
/// use std::time::Duration;
///
/// let config = RetentionConfig::with_max_age(Duration::from_secs(86400))
///     .with_audit_retention_days(30);
///
/// assert!(config.enabled());
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct RetentionConfig {
    /// Maximum age in seconds for closed workflows before they are eligible for deletion.
    /// If `None`, workflow history retention is disabled.
    pub max_age_secs: Option<u64>,
    /// Per-workflow-type retention overrides keyed by registered workflow name,
    /// each mapping to its own max-age in seconds (matching `max_age_secs`).
    ///
    /// A completed execution whose `workflow_name` has an override is retained
    /// for that type's age instead of the global `max_age_secs`. A type with no
    /// override falls back to the global `max_age_secs`; if neither is set, that
    /// type is never deleted. Uses [`BTreeMap`] for deterministic ordering and
    /// serialization. Issue #737.
    pub overrides: BTreeMap<String, u64>,
    /// How often the background retention job wakes up to scan for expired data.
    pub tick_interval_secs: u64,
    /// The maximum number of records to process in a single transaction/batch.
    pub batch_size: usize,
    /// If `true`, the retention job simulates deletions and logs what would have been deleted
    /// without actually modifying the database.
    pub dry_run: bool,
    /// Audit log retention in days, independent of workflow-history retention.
    /// Defaults to 90 days (3 months). Set to 0 to disable audit purging.
    pub audit_retention_days: i64,
    /// Schedule decisions retention in days.
    /// Defaults to 7 days. Set to 0 to disable schedule decision purging.
    pub schedule_decision_retention_days: i64,
    /// The timeout in seconds for executing the pre-retention archival hook.
    /// Defaults to 30 seconds.
    pub archival_timeout_secs: u64,
    /// Tiered/summary retention policy (issue #752).
    ///
    /// When `Some`, a terminal execution hard-deleted by the history janitor is
    /// first demoted into a compact `harvest_execution_summaries` row (written
    /// in the same transaction as the delete). `None` (the default) is
    /// byte-for-byte identical to pre-#752 behavior: hard delete, no summary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<SummaryPolicy>,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_age_secs: None,
            overrides: BTreeMap::new(),
            tick_interval_secs: DEFAULT_TICK_INTERVAL.as_secs(),
            batch_size: DEFAULT_BATCH_SIZE,
            dry_run: false,
            audit_retention_days: 90,
            schedule_decision_retention_days: 7,
            archival_timeout_secs: DEFAULT_ARCHIVAL_TIMEOUT_SECS,
            summary: None,
        }
    }
}

impl RetentionConfig {
    /// Bootstraps a fresh configuration template that explicitly opts-in to the workflow retention features.
    #[must_use]
    pub fn with_max_age(max_age: Duration) -> Self {
        Self {
            max_age_secs: Some(max_age.as_secs()),
            ..Self::default()
        }
    }

    /// Register a per-workflow-type retention override (issue #737).
    ///
    /// The named workflow type is retained for `max_age` instead of the global
    /// `max_age`. Overrides are validated against the same
    /// `MIN_MAX_AGE..=MAX_MAX_AGE` bounds as the global `max_age` at build time.
    #[must_use]
    pub fn with_workflow_override(
        mut self,
        workflow_name: impl Into<String>,
        max_age: Duration,
    ) -> Self {
        self.overrides
            .insert(workflow_name.into(), max_age.as_secs());
        self
    }

    /// Bulk-register per-workflow-type retention overrides (issue #737).
    #[must_use]
    pub fn with_workflow_overrides<S: Into<String>>(
        mut self,
        iter: impl IntoIterator<Item = (S, Duration)>,
    ) -> Self {
        for (name, max_age) in iter {
            self.overrides.insert(name.into(), max_age.as_secs());
        }
        self
    }

    /// Override the audit log retention window.
    #[must_use]
    pub const fn with_audit_retention_days(mut self, days: i64) -> Self {
        self.audit_retention_days = days;
        self
    }

    /// Override the schedule decision retention window.
    #[must_use]
    pub const fn with_schedule_decision_retention_days(mut self, days: i64) -> Self {
        self.schedule_decision_retention_days = days;
        self
    }

    /// Override the archival hook execution timeout.
    #[must_use]
    pub const fn with_archival_timeout_secs(mut self, secs: u64) -> Self {
        self.archival_timeout_secs = secs;
        self
    }

    /// Enable tiered/summary retention with an explicit policy (issue #752).
    #[must_use]
    pub const fn with_summary_retention(mut self, policy: SummaryPolicy) -> Self {
        self.summary = Some(policy);
        self
    }

    /// Enable tiered/summary retention with a `days`-day summary horizon
    /// (payload capture off). Shorthand for
    /// `with_summary_retention(SummaryPolicy::for_days(days))`.
    #[must_use]
    pub const fn with_summary_retention_days(mut self, days: u64) -> Self {
        self.summary = Some(SummaryPolicy::for_days(days));
        self
    }

    /// Enable tiered/summary retention keeping summaries forever (payload
    /// capture off).
    #[must_use]
    pub const fn with_summary_retention_unbounded(mut self) -> Self {
        self.summary = Some(SummaryPolicy::unbounded());
        self
    }

    /// The summary GC horizon as a [`Duration`], or `None` when summaries are
    /// disabled OR configured [`SummaryRetention::Unbounded`] (issue #752).
    #[must_use]
    pub fn summary_age(&self) -> Option<Duration> {
        self.summary.and_then(|p| p.retention_age())
    }

    /// Whether summary (tiered) retention is enabled at all (issue #752).
    #[must_use]
    pub const fn summary_enabled(&self) -> bool {
        self.summary.is_some()
    }

    /// Whether the summary GC pass should run this tick: a summary policy with a
    /// bounded (`For(_)`) horizon is set (issue #752). `Unbounded` summaries are
    /// never GC'd, so this is `false` for them.
    #[must_use]
    pub fn summary_gc_active(&self) -> bool {
        self.summary_age().is_some()
    }

    /// Read access to the tiered/summary retention policy (issue #752).
    #[must_use]
    pub const fn summary_policy(&self) -> Option<SummaryPolicy> {
        self.summary
    }

    /// Safely unpacks the raw configuration integer into a standard rust [`Duration`], gracefully
    /// handling systems where the feature is entirely turned off.
    #[must_use]
    pub fn max_age(&self) -> Option<Duration> {
        self.max_age_secs.map(Duration::from_secs)
    }

    /// Resolves the effective history max-age for a given workflow type (issue #737).
    ///
    /// Returns the per-type override if one is registered for `workflow_name`,
    /// otherwise the global `max_age`. Returns `None` when neither is set — in
    /// which case that type's history is never deleted.
    #[must_use]
    pub fn effective_max_age(&self, workflow_name: &str) -> Option<Duration> {
        self.overrides
            .get(workflow_name)
            .copied()
            .map(Duration::from_secs)
            .or_else(|| self.max_age())
    }

    /// The smallest effective retention age across the global `max_age` and all
    /// per-type overrides (issue #737).
    ///
    /// This is the SQL candidate pre-filter age: the smallest age yields the
    /// cutoff closest to "now", which is a correct *superset* of every
    /// deletable row (any row deletable under a type-specific age `age(T)`
    /// satisfies `completed_at < now - age(T) <= now - min_age`). The scanner
    /// then applies the exact per-type age to each candidate in Rust.
    ///
    /// Returns `None` iff the global `max_age` is unset *and* there are no
    /// overrides.
    #[must_use]
    pub fn loosest_cutoff_age(&self) -> Option<Duration> {
        self.max_age()
            .into_iter()
            .chain(self.overrides.values().copied().map(Duration::from_secs))
            .min()
    }

    /// Returns `true` if workflow-history retention should run this tick, i.e.
    /// either the global `max_age` or at least one per-type override is set
    /// (issue #737).
    #[must_use]
    pub fn history_retention_active(&self) -> bool {
        self.loosest_cutoff_age().is_some()
    }

    /// Read access to the per-workflow-type retention overrides (issue #737).
    #[must_use]
    pub const fn workflow_overrides(&self) -> &BTreeMap<String, u64> {
        &self.overrides
    }

    /// Translates the raw numeric tick value into a standard [`Duration`] for the scheduler loop.
    #[must_use]
    pub const fn tick_interval(&self) -> Duration {
        Duration::from_secs(self.tick_interval_secs)
    }

    /// Translates the raw archival timeout value into a standard [`Duration`] for timeout enforcement.
    #[must_use]
    pub const fn archival_timeout(&self) -> Duration {
        Duration::from_secs(self.archival_timeout_secs)
    }

    /// # Errors
    ///
    /// Returns an error string if `tick_interval_secs` is 0, `batch_size` is 0,
    /// or `max_age` is outside the allowed range.
    pub fn validate(&self) -> Result<(), String> {
        if self.tick_interval_secs == 0 {
            return Err("tick_interval must be >= 1s".to_string());
        }
        if self.batch_size == 0 {
            return Err("batch_size must be >= 1".to_string());
        }
        if self.archival_timeout_secs == 0 {
            return Err("archival_timeout_secs must be >= 1s".to_string());
        }
        if let Some(max_age) = self.max_age()
            && !(MIN_MAX_AGE..=MAX_MAX_AGE).contains(&max_age)
        {
            return Err(format!(
                "max_age must be between {}s and {}s",
                MIN_MAX_AGE.as_secs(),
                MAX_MAX_AGE.as_secs()
            ));
        }
        // Each per-type override is validated against the same bounds as the
        // global max_age; an out-of-range override fails the build rather than
        // silently clamping (issue #737, AC5).
        for (name, secs) in &self.overrides {
            let age = Duration::from_secs(*secs);
            if !(MIN_MAX_AGE..=MAX_MAX_AGE).contains(&age) {
                return Err(format!(
                    "retention override for '{name}' must be between {}s and {}s",
                    MIN_MAX_AGE.as_secs(),
                    MAX_MAX_AGE.as_secs()
                ));
            }
        }
        // The summary GC horizon is bounded by the same range as `max_age`
        // (issue #752). `Unbounded` needs no bound. An out-of-range horizon
        // fails the build rather than silently clamping.
        if let Some(age) = self.summary_age()
            && !(MIN_MAX_AGE..=MAX_MAX_AGE).contains(&age)
        {
            return Err(format!(
                "summary retention must be between {}s and {}s",
                MIN_MAX_AGE.as_secs(),
                MAX_MAX_AGE.as_secs()
            ));
        }
        Ok(())
    }

    /// Returns `true` if any retention features (workflow history, per-type
    /// history overrides, audit log, or schedule decision purging) are enabled.
    ///
    /// Per-workflow-type overrides count as enabling workflow-history retention
    /// even when the global `max_age` is unset (issue #737), so an
    /// overrides-only configuration still spawns the janitor.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.max_age_secs.is_some()
            || !self.overrides.is_empty()
            || self.audit_retention_days > 0
            || self.schedule_decision_retention_days > 0
            // A bounded summary policy spawns the janitor so its GC pass runs
            // even if the history horizon was later removed (issue #752).
            || self.summary_gc_active()
    }
}

/// The result of a single execution tick of the retention job on a specific shard.
///
/// **Why does this exist?**
/// Provides observability into the retention job's performance and impact. It captures
/// how many records were evaluated, how many were deleted, and any errors encountered,
/// allowing operators to monitor the health of the background cleanup process.
#[derive(Debug, Clone, Serialize, Default)]
pub struct RetentionTickResult {
    /// The ID of the shard this retention tick operated on.
    pub shard: u16,
    /// The timestamp when this retention tick started.
    pub ran_at: Option<DateTime<Utc>>,
    /// The number of expired candidate records identified during the tick.
    pub candidate_count: usize,
    /// The actual number of records successfully deleted during the tick.
    pub deleted_count: usize,
    /// The age (in seconds) of the oldest closed workflow that was skipped (not yet expired).
    /// Used for tuning the `max_age_secs` configuration.
    pub oldest_age_secs_skipped: Option<u64>,
    /// The duration of the retention tick in milliseconds.
    pub duration_ms: u128,
    /// The last error encountered during the tick, if any.
    pub last_error: Option<String>,
    /// Per-workflow-type deletion counts for this tick (issue #737).
    ///
    /// In dry-run mode these are the counts the janitor *would* delete under
    /// each type's resolved retention age. In a real run they are the counts
    /// actually deleted. Surfaced via `GET /admin/retention` for per-type
    /// reporting.
    pub deleted_by_workflow: BTreeMap<String, u64>,
    /// Number of execution summaries created (demoted) during this tick (issue
    /// #752). Surfaced via `GET /admin/retention` for creation observability.
    /// Real deletes only — a `dry_run` tick creates no summaries.
    pub summarized_count: usize,
}

/// The current overall status of the retention subsystem.
///
/// **Why does this exist?**
/// Aggregates the static configuration and the dynamic runtime state (per-shard results)
/// to provide a comprehensive snapshot of the retention process for diagnostic APIs.
#[derive(Debug, Clone, Serialize, Default)]
pub struct RetentionStatus {
    /// The active retention configuration.
    pub config: RetentionConfig,
    /// The latest execution results for each active shard.
    pub per_shard: Vec<RetentionTickResult>,
}

/// A thread-safe monitor for observing the background retention process.
///
/// **Why does this exist?**
/// Enables the background retention job to asynchronously report its progress and results,
/// while allowing external components (like administrative APIs or telemetry systems)
/// to safely query the latest status without blocking or tearing.
#[derive(Debug, Clone)]
pub struct RetentionMonitor {
    inner: Arc<Mutex<RetentionStatus>>,
}

impl RetentionMonitor {
    /// Boots up a clean monitoring tracker that acts as the initial blank canvas before shards report results.
    #[must_use]
    pub fn new(config: RetentionConfig, shards: impl Iterator<Item = ShardId>) -> Self {
        let per_shard = shards
            .map(|shard| RetentionTickResult {
                shard: u16::try_from(shard.as_i32()).unwrap_or(0),
                ..RetentionTickResult::default()
            })
            .collect();
        Self {
            inner: Arc::new(Mutex::new(RetentionStatus { config, per_shard })),
        }
    }

    /// # Panics
    ///
    /// Panics if the internal mutex has been poisoned.
    #[must_use]
    pub fn snapshot(&self) -> RetentionStatus {
        self.inner
            .lock()
            .expect("retention monitor lock poisoned")
            .clone()
    }

    #[cfg(feature = "db")]
    fn update(&self, shard: ShardId, result: RetentionTickResult) {
        let mut guard = self.inner.lock().expect("retention monitor lock poisoned");
        if let Some(existing) = guard
            .per_shard
            .iter_mut()
            .find(|x| x.shard == u16::try_from(shard.as_i32()).unwrap_or(0))
        {
            *existing = result;
        }
    }
}

/// Represents the running background task that processes retention policies.
///
/// **Why does this exist?**
/// Provides a handle to control and monitor the active background retention job.
/// It encapsulates the background tokio task, the cancellation token for graceful shutdown,
/// and the channel used to force immediate retention sweeps.
#[cfg(feature = "db")]
pub struct RetentionRuntime {
    shutdown: CancellationToken,
    trigger_tx: mpsc::Sender<()>,
    handle: JoinHandle<()>,
    monitor: RetentionMonitor,
}

#[cfg(feature = "db")]
impl RetentionRuntime {
    /// # Panics
    ///
    /// Panics inside the spawned task if the enabled config is missing `max_age`
    /// (which cannot happen when `config.enabled()` is `true`).
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn spawn(
        pools: ShardedDbPool,
        config: RetentionConfig,
        metrics: Arc<dyn MetricsRecorder>,
        archiver: Option<Arc<dyn HistoryArchiver>>,
        offloader: Option<Arc<crate::payload_store::PayloadOffloader>>,
    ) -> Option<Self> {
        if !config.enabled() {
            return None;
        }
        let monitor = RetentionMonitor::new(config.clone(), pools.shard_ids().into_iter());
        let shutdown = CancellationToken::new();
        let shutdown_task = shutdown.clone();
        let monitor_task = monitor.clone();
        let (trigger_tx, mut trigger_rx) = mpsc::channel(1);
        let handle = tokio::spawn(async move {
            let mut scan_cursors: HashMap<ShardId, Option<RetentionScanCursor>> = HashMap::new();
            loop {
                tokio::select! {
                    () = shutdown_task.cancelled() => break,
                    () = tokio::time::sleep(config.tick_interval()) => {},
                    Some(()) = trigger_rx.recv() => {
                        while trigger_rx.try_recv().is_ok() {}
                    }
                }

                // Workflow-history retention: runs when the global max_age OR
                // any per-workflow-type override is configured (issue #737).
                // Resolve the loosest cutoff age up front. `None` here means
                // either no retention age is configured (the common case) OR —
                // fail-safe — the configured loosest age is unrepresentable as a
                // `chrono::Duration` (unreachable for validated ages, which are
                // bounded by MAX_MAX_AGE ≪ chrono's ~292M-year range). In the
                // latter case we skip the workflow-history retention phase this
                // tick and retain everything, rather than falling back to a zero
                // cutoff that would make `loose_cutoff == now` and over-select
                // nearly every completed row. The audit/schedule purges below
                // still run.
                if config
                    .loosest_cutoff_age()
                    .and_then(|age| chrono::Duration::from_std(age).ok())
                    .is_some()
                {
                    // Phase-active gate (issue #737): the `and_then(from_std)`
                    // above is the fail-safe guard from commit 34ddb62 — if the
                    // loosest configured age is unrepresentable as a
                    // `chrono::Duration` (unreachable for validated ages) we skip
                    // the whole workflow-history phase this tick rather than
                    // over-selecting. Compute `now` once so every shard's SQL
                    // predicate and per-candidate resolution use one consistent
                    // clock. The exact per-type cutoffs are pushed into the SELECT
                    // inside `run_shard_tick` (PR #990 review) — there is no
                    // single "loose cutoff" SQL bind any more.
                    let now = Utc::now();
                    let tick_futures = pools.iter_shards().map(|(shard, pool)| {
                        let pool = pool.clone();
                        let config = config.clone();
                        let metrics = Arc::clone(&metrics);
                        let archiver = archiver.clone();
                        let offloader = offloader.clone();
                        let cursor = scan_cursors.get(&shard).copied().flatten();
                        async move {
                            let started = Instant::now();
                            let tick = run_shard_tick(
                                pool,
                                shard,
                                now,
                                &config,
                                archiver,
                                cursor,
                                Arc::clone(&metrics),
                                offloader,
                            )
                            .await;
                            (shard, started, tick)
                        }
                    });

                    for (shard, started, tick) in join_all(tick_futures).await {
                        let mut result = RetentionTickResult {
                            shard: u16::try_from(shard.as_i32()).unwrap_or(0),
                            ran_at: Some(Utc::now()),
                            duration_ms: started.elapsed().as_millis(),
                            ..RetentionTickResult::default()
                        };
                        match tick {
                            Ok(ok) => {
                                scan_cursors.insert(shard, ok.next_cursor);
                                result.candidate_count = ok.candidate_count;
                                result.deleted_count = ok.deleted_count;
                                result.oldest_age_secs_skipped = ok.oldest_age_secs_skipped;
                                result.deleted_by_workflow = ok.deleted_by_workflow.clone();
                                result.summarized_count = ok.summarized_count;
                                tracing::info!(
                                    shard = %shard,
                                    candidates = ok.candidate_count,
                                    deleted = ok.deleted_count,
                                    oldest_age_secs_skipped = ok.oldest_age_secs_skipped,
                                    duration_ms = result.duration_ms,
                                    dry_run = config.dry_run,
                                    "harvest retention tick completed"
                                );
                                #[allow(clippy::cast_precision_loss)]
                                metrics.record_retention_tick(
                                    u16::try_from(shard.as_i32()).unwrap_or(0),
                                    ok.candidate_count as u64,
                                    ok.deleted_count as u64,
                                    result.duration_ms as f64 / 1000.0,
                                );
                                // Per-workflow-type deletion counter (issue
                                // #737, AC8). Real deletes only — the metric
                                // confirms ACTUAL deletion (it reads 0 for a
                                // long-retained type until its own age), so a
                                // dry-run's would-delete counts are excluded.
                                if !config.dry_run {
                                    for (name, count) in &ok.deleted_by_workflow {
                                        metrics.record_retention_deleted(name, *count);
                                    }
                                }
                            }
                            Err(error) => {
                                result.last_error = Some(error.to_string());
                                scan_cursors.insert(shard, None);
                                tracing::warn!(shard = %shard, error = %error, "harvest retention tick failed");
                            }
                        }
                        monitor_task.update(shard, result);
                    }
                }

                // Purge old audit records once per tick, best-effort.
                // Audit rows may live on any shard (workflow starts use shard-aware
                // inserts), so iterate every shard to honour the retention window.
                if config.audit_retention_days > 0 && !config.dry_run {
                    for (_, pool) in pools.iter_shards() {
                        if let Ok(mut conn) = pool.get().await
                            && let Err(err) = crate::audit::purge_old_audit_records(
                                &mut conn,
                                config.audit_retention_days,
                            )
                            .await
                        {
                            tracing::warn!(error = %err, "harvest audit log purge failed");
                        }
                    }
                }

                // Purge old schedule decisions once per tick, best-effort.
                if config.schedule_decision_retention_days > 0 && !config.dry_run {
                    for (_, pool) in pools.iter_shards() {
                        if let Ok(mut conn) = pool.get().await
                            && let Err(err) =
                                crate::schedule_decision::purge_old_schedule_decisions(
                                    &mut conn,
                                    config.schedule_decision_retention_days,
                                )
                                .await
                        {
                            tracing::warn!(error = %err, "harvest schedule decisions purge failed");
                        }
                    }
                }

                // Summary GC pass (issue #752): garbage-collect execution
                // summaries older than the summary horizon, once per tick,
                // best-effort. Only runs for a bounded (`For(_)`) horizon —
                // `Unbounded` keeps summaries forever. Shard-local (summaries
                // live on the demoted execution's own shard). The
                // `harvest.retention.summary_deleted` counter is emitted for
                // real GC deletes only.
                if config.summary_gc_active()
                    && !config.dry_run
                    && let Some(summary_age) = config.summary_age()
                {
                    let now = Utc::now();
                    for (shard, pool) in pools.iter_shards() {
                        if let Ok(mut conn) = pool.get().await {
                            match purge_expired_summaries(
                                &mut conn,
                                u16::try_from(shard.as_i32()).unwrap_or(0),
                                summary_age,
                                config.batch_size,
                                false,
                                now,
                            )
                            .await
                            {
                                Ok(counts) => {
                                    for (name, count) in counts {
                                        if count > 0 {
                                            metrics.record_summary_deleted(&name, count);
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!(shard = %shard, error = %err, "harvest execution-summary GC failed");
                                }
                            }
                        }
                    }
                }
            }
        });

        Some(Self {
            shutdown,
            trigger_tx,
            handle,
            monitor,
        })
    }

    /// Shares a snapshot interface allowing telemetry dashboards to safely peek at the process.
    #[must_use]
    pub fn monitor(&self) -> RetentionMonitor {
        self.monitor.clone()
    }

    /// Forces the background worker to wake up and aggressively prune immediately without waiting
    /// for the next interval loop.
    pub fn run_now(&self) {
        let _ = self.trigger_tx.try_send(());
    }

    /// Exposes a direct channel to bypass scheduling and command the worker to act right now.
    #[must_use]
    pub fn trigger_sender(&self) -> mpsc::Sender<()> {
        self.trigger_tx.clone()
    }

    /// Triggers the emergency stop sequence to abort any running operations gracefully.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// # Errors
    ///
    /// Returns a [`tokio::task::JoinError`] if the spawned retention task panicked
    /// or was aborted.
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.handle.await
    }
}

#[cfg(feature = "db")]
#[derive(Debug, Default)]
struct ShardTickOutcome {
    candidate_count: usize,
    deleted_count: usize,
    oldest_age_secs_skipped: Option<u64>,
    next_cursor: Option<RetentionScanCursor>,
    /// Per-workflow-type deletion counts (real deletes and dry-run
    /// would-deletes). Issue #737.
    deleted_by_workflow: BTreeMap<String, u64>,
    /// Number of execution summaries created (demoted) this tick (issue #752).
    /// Real deletes only — dry-run creates no summaries.
    summarized_count: usize,
}

#[cfg(feature = "db")]
#[derive(Debug, QueryableByName)]
struct CandidateExecution {
    #[diesel(sql_type = SqlUuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = Text)]
    workflow_name: String,
    #[diesel(sql_type = Text)]
    workflow_id: String,
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    context_headers: Option<serde_json::Value>,
    #[diesel(sql_type = Nullable<Timestamptz>) ]
    completed_at: Option<DateTime<Utc>>,
    /// Legal-hold columns (issue #747), read only for the per-candidate skip
    /// gate. The SELECT's WHERE clause is intentionally NOT changed — the gate
    /// is evaluated in Rust so the two-variant bind numbering stays stable.
    #[diesel(sql_type = Nullable<Timestamptz>) ]
    legal_hold_set_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Timestamptz>) ]
    legal_hold_until: Option<DateTime<Utc>>,
}

#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq)]
struct RetentionScanCursor {
    completed_at: DateTime<Utc>,
    id: uuid::Uuid,
}

#[cfg(feature = "db")]
struct RetentionLeaseGuard {
    pool: crate::worker::DbPool,
    lease_id: String,
    active_ids: Arc<Mutex<Vec<uuid::Uuid>>>,
    active: bool,
}

#[cfg(feature = "db")]
impl Drop for RetentionLeaseGuard {
    fn drop(&mut self) {
        if self.active {
            let pool = self.pool.clone();
            let lease_id = self.lease_id.clone();
            let ids = {
                let guard = self.active_ids.lock().expect("lease guard lock poisoned");
                guard.clone()
            };
            if !ids.is_empty() {
                tokio::spawn(async move {
                    if let Ok(mut conn) = pool.get().await {
                        let _ = diesel::update(
                            harvest_workflow_executions::table
                                .filter(harvest_workflow_executions::id.eq_any(ids))
                                .filter(
                                    harvest_workflow_executions::sticky_worker_id
                                        .eq(Some(lease_id)),
                                ),
                        )
                        .set(
                            harvest_workflow_executions::sticky_worker_id
                                .eq::<Option<String>>(None),
                        )
                        .execute(&mut conn)
                        .await;
                    }
                });
            }
        }
    }
}

#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
async fn run_shard_tick(
    pool: crate::worker::DbPool,
    shard: ShardId,
    now: DateTime<Utc>,
    config: &RetentionConfig,
    archiver: Option<Arc<dyn HistoryArchiver>>,
    start_cursor: Option<RetentionScanCursor>,
    _metrics: Arc<dyn MetricsRecorder>,
    offloader: Option<Arc<crate::payload_store::PayloadOffloader>>,
) -> HarvestResult<ShardTickOutcome> {
    let mut outcome = ShardTickOutcome {
        next_cursor: start_cursor,
        ..ShardTickOutcome::default()
    };
    let mut cursor = start_cursor;
    let mut wrapped = false;
    let mut remaining = config.batch_size;
    let mut has_failed = false;

    // Per-type effective cutoff pushed into the candidate SELECT (issue #737,
    // PR #990 review). Instead of a single loose cutoff (`now - min(all ages)`)
    // followed by a Rust per-candidate skip — which selects, claims, and re-skips
    // a long-retained type's not-yet-eligible rows every tick, consuming batch
    // budget and starving newer already-expired rows of a shorter policy — we
    // build one (name, cutoff) pair per override and let the SQL COALESCE resolve
    // each row's own effective cutoff. Only genuinely-eligible rows are ever
    // selected, so mixed-policy deployments never starve.
    //
    // `override_cut_names[i]` ↔ `override_cuts[i]` are kept in lockstep (unnest
    // requires equal-length arrays). A non-overridden type falls through COALESCE
    // to `global_fallback` (the global `max_age` cutoff) or, when no global age is
    // set, to `'-infinity'` — meaning it is never selected.
    let mut override_cut_names: Vec<String> = Vec::new();
    let mut override_cuts: Vec<DateTime<Utc>> = Vec::new();
    for (name, secs) in config.workflow_overrides() {
        // Fail safe: an unrepresentable override age (unreachable for validated
        // ages, bounded by MAX_MAX_AGE ≪ chrono's ~292M-year range) is omitted so
        // its rows are never selected under a too-aggressive cutoff. With a global
        // `max_age` set they then fall back to it; without one they fall back to
        // `'-infinity'` (never selected). Retaining is the correct failure mode.
        if let Ok(delta) = chrono::Duration::from_std(Duration::from_secs(*secs)) {
            override_cut_names.push(name.clone());
            override_cuts.push(now - delta);
        }
    }
    // The global fallback cutoff for un-overridden types. `None` means "no global
    // age" (or an unrepresentable one — same fail-safe): the SQL uses an
    // `'-infinity'` literal so those types are never selected.
    let global_fallback: Option<DateTime<Utc>> = config
        .max_age()
        .and_then(|age| chrono::Duration::from_std(age).ok())
        .map(|delta| now - delta);

    let lease_id = format!("retention-lease-{}", uuid::Uuid::new_v4());
    let guard = RetentionLeaseGuard {
        pool: pool.clone(),
        lease_id: lease_id.clone(),
        active_ids: Arc::new(Mutex::new(Vec::new())),
        active: true,
    };

    // Reclaim `harvest_completion_deliveries` rows that resolved to
    // `DELIVERED` *after* their owning execution was already collected
    // (issue #921 review, Codex P2, follow-up). A PENDING/INFLIGHT/FAILED
    // row is deliberately kept when its owner is collected below (the
    // delivery may still need to retry or await redrive), but if that same
    // row *later* succeeds, nothing else ever revisits it -- the candidate
    // loop only ever iterates over still-live executions, so an orphaned
    // row (whose `workflow_exec_id` no longer names an existing execution)
    // would otherwise carry its frozen result/error PII with no retention
    // bound at all. Scoped to `DELIVERED` only, matching the per-candidate
    // delete's existing "not finished yet" rule for PENDING/INFLIGHT/FAILED
    // rows. Runs once per shard tick (not per candidate) since it is a
    // table-wide reclaim, not scoped to this tick's candidate batch.
    {
        let mut conn = pool
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;
        let reclaimed = diesel::sql_query(
            "DELETE FROM harvest_completion_deliveries
             WHERE state = 'DELIVERED'
               AND NOT EXISTS (
                   SELECT 1 FROM harvest_workflow_executions
                   WHERE harvest_workflow_executions.id = harvest_completion_deliveries.workflow_exec_id
               )",
        )
        .execute(&mut conn)
        .await
        .map_err(database_error)?;
        outcome.deleted_count += reclaimed;
    }

    while remaining > 0 {
        // Check out a short-lived connection just to load and claim this batch of candidates in a single transaction
        let mut conn = pool
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;

        let lease_id_inner = lease_id.clone();
        let names_inner = override_cut_names.clone();
        let cuts_inner = override_cuts.clone();
        let candidates = conn.transaction::<Vec<CandidateExecution>, HarvestError, _>(|conn| {
            Box::pin(async move {
                // Push each row's exact per-type effective cutoff into the
                // predicate (issue #737, PR #990 review): the correlated
                // `unnest` subquery resolves the override cutoff for this row's
                // workflow_name, falling through COALESCE to the global cutoff
                // ($3) or, when there is no global age, to `'-infinity'` — so a
                // non-overridden never-delete type is never selected. Only
                // genuinely-eligible rows are returned, so a long-retained
                // type's not-yet-eligible backlog can neither consume the batch
                // budget nor starve newer expired rows of a shorter policy.
                //
                // Two query-string variants keep the bind numbering unambiguous:
                // with a global age the fallback is bound as $3; without one it
                // is the `'-infinity'` literal and the cursor/limit binds shift
                // down by one.
                let sql = if global_fallback.is_some() {
                    "SELECT id, workflow_name, workflow_id, state, completed_at, context_headers, legal_hold_set_at, legal_hold_until
                     FROM harvest_workflow_executions
                     WHERE state IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW','TERMINATED')
                       AND completed_at IS NOT NULL
                       AND sticky_worker_id IS NULL
                       AND completed_at < COALESCE(
                           (SELECT ov.cut
                              FROM unnest($1::text[], $2::timestamptz[]) AS ov(nm, cut)
                             WHERE ov.nm = harvest_workflow_executions.workflow_name),
                           $3)
                       AND (
                           $4 IS NULL
                           OR completed_at > $4
                           OR (completed_at = $4 AND id > $5)
                       )
                     ORDER BY completed_at ASC, id ASC
                     LIMIT $6
                     FOR UPDATE SKIP LOCKED"
                } else {
                    "SELECT id, workflow_name, workflow_id, state, completed_at, context_headers, legal_hold_set_at, legal_hold_until
                     FROM harvest_workflow_executions
                     WHERE state IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW','TERMINATED')
                       AND completed_at IS NOT NULL
                       AND sticky_worker_id IS NULL
                       AND completed_at < COALESCE(
                           (SELECT ov.cut
                              FROM unnest($1::text[], $2::timestamptz[]) AS ov(nm, cut)
                             WHERE ov.nm = harvest_workflow_executions.workflow_name),
                           '-infinity'::timestamptz)
                       AND (
                           $3 IS NULL
                           OR completed_at > $3
                           OR (completed_at = $3 AND id > $4)
                       )
                     ORDER BY completed_at ASC, id ASC
                     LIMIT $5
                     FOR UPDATE SKIP LOCKED"
                };
                // Bind order maps to $1..$N regardless of textual position. The
                // override arrays ($1/$2) are always bound; $3 is the global
                // fallback only in the global-age variant.
                let query = diesel::sql_query(sql)
                    .bind::<Array<Text>, _>(names_inner)
                    .bind::<Array<Timestamptz>, _>(cuts_inner);
                let rows = if let Some(fallback) = global_fallback {
                    query
                        .bind::<Timestamptz, _>(fallback)
                        .bind::<Nullable<Timestamptz>, _>(cursor.map(|it| it.completed_at))
                        .bind::<Nullable<SqlUuid>, _>(cursor.map(|it| it.id))
                        .bind::<BigInt, _>(i64::try_from(remaining).unwrap_or(i64::MAX))
                        .load::<CandidateExecution>(conn)
                        .await
                } else {
                    query
                        .bind::<Nullable<Timestamptz>, _>(cursor.map(|it| it.completed_at))
                        .bind::<Nullable<SqlUuid>, _>(cursor.map(|it| it.id))
                        .bind::<BigInt, _>(i64::try_from(remaining).unwrap_or(i64::MAX))
                        .load::<CandidateExecution>(conn)
                        .await
                }
                .map_err(database_error)?;

                if !rows.is_empty() {
                    let ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.id).collect();
                    diesel::update(
                        harvest_workflow_executions::table
                            .filter(harvest_workflow_executions::id.eq_any(ids)),
                    )
                    .set(harvest_workflow_executions::sticky_worker_id.eq(Some(lease_id_inner)))
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                }

                Ok(rows)
            })
        })
        .await?;

        // Release the checked-out connection immediately back to the pool
        drop(conn);

        if !candidates.is_empty() {
            let ids: Vec<uuid::Uuid> = candidates.iter().map(|r| r.id).collect();
            guard
                .active_ids
                .lock()
                .expect("lease guard lock poisoned")
                .extend(ids);
        }

        if candidates.is_empty() {
            // Prevent same-tick rescanning/wrapping if we have encountered any failures
            if cursor.is_some() && !wrapped && !has_failed {
                cursor = None;
                wrapped = true;
                continue;
            }
            outcome.next_cursor = cursor;
            break;
        }

        let mut batch_failed = false;
        for candidate in candidates {
            let completed_at = candidate
                .completed_at
                .expect("retention candidate query enforces completed_at IS NOT NULL");
            let candidate_cursor = RetentionScanCursor {
                completed_at,
                id: candidate.id,
            };
            cursor = Some(candidate_cursor);
            outcome.candidate_count += 1;
            remaining = remaining.saturating_sub(1);

            // Checkout a connection to run candidate dependency validations
            let mut conn = pool
                .get()
                .await
                .map_err(|error| HarvestError::Database(error.to_string()))?;

            // --- Per-candidate retention decision (issue #737) ------------
            // This is the single seam where a candidate's fate is decided.
            // Resolve the effective max-age for THIS workflow type (override
            // or global fallback), then gate on it before any archive/delete.
            // Future policies (#747 legal hold, #752 tiered summary) hook in
            // here.
            //
            // Legal hold (issue #747) is the FIRST gate — it precedes archival
            // (#345 hook below) AND delete, and precedes the metric emission, so
            // `harvest.retention.deleted` never increments for a held id. An
            // active hold (`legal_hold_active`) exempts this execution's history
            // from retention entirely, until the hold is released or expires.
            // Evaluated against the tick's `now` (the same clock the SELECT cutoff
            // uses) so the decision is consistent with candidate selection.
            if legal_hold_active(candidate.legal_hold_set_at, candidate.legal_hold_until, now) {
                routine_skip_candidate(
                    &mut conn,
                    candidate.id,
                    candidate_cursor,
                    has_failed,
                    &mut outcome,
                    &guard.active_ids,
                )
                .await?;
                continue;
            }
            //
            // NB (PR #990 review): the candidate SELECT now pushes each row's
            // exact per-type cutoff into SQL, so the two skip branches below
            // (`effective_max_age == None` and `completed_at >= resolved_cutoff`)
            // are effectively unreachable — no not-yet-eligible or never-delete
            // row is ever selected. They are kept as cheap defense-in-depth. The
            // `should_skip_candidate` chain-link check below still needs the
            // RESOLVED per-type cutoff, which is derived here.
            let Some(age) = config.effective_max_age(&candidate.workflow_name) else {
                // Neither an override nor a global max-age applies to this
                // type: never delete it. Routine skip.
                routine_skip_candidate(
                    &mut conn,
                    candidate.id,
                    candidate_cursor,
                    has_failed,
                    &mut outcome,
                    &guard.active_ids,
                )
                .await?;
                continue;
            };
            let Ok(chrono_age) = chrono::Duration::from_std(age) else {
                // Unrepresentable duration (unreachable for validated ages, which
                // are bounded by MAX_MAX_AGE ≪ chrono's ~292M-year range); fail
                // safe toward RETAINING the row rather than deleting it. A zero
                // fallback would make `resolved_cutoff = now`, deleting nearly
                // every completed row — the wrong failure mode for a retention
                // janitor. Routine skip.
                routine_skip_candidate(
                    &mut conn,
                    candidate.id,
                    candidate_cursor,
                    has_failed,
                    &mut outcome,
                    &guard.active_ids,
                )
                .await?;
                continue;
            };
            let resolved_cutoff = now - chrono_age;
            if completed_at >= resolved_cutoff {
                // The loosest-cutoff SQL pre-filter is a superset; this type is
                // not old enough under its own effective age. Routine skip, and
                // record its age for tuning observability (as the existing
                // should_skip branch does for not-yet-collectable candidates).
                let skipped_age = now
                    .signed_duration_since(completed_at)
                    .num_seconds()
                    .max(0)
                    .cast_unsigned();
                outcome.oldest_age_secs_skipped = Some(
                    outcome
                        .oldest_age_secs_skipped
                        .map_or(skipped_age, |existing| existing.max(skipped_age)),
                );
                routine_skip_candidate(
                    &mut conn,
                    candidate.id,
                    candidate_cursor,
                    has_failed,
                    &mut outcome,
                    &guard.active_ids,
                )
                .await?;
                continue;
            }

            // NOTE: pass the per-type `resolved_cutoff` (not the loose cutoff)
            // to should_skip_candidate — its continue-as-new chain-link check
            // compares `completed_at >= $cutoff` and must see this type's own
            // cutoff to be correct.
            if should_skip_candidate(&mut conn, &candidate, resolved_cutoff).await? {
                let skipped_age = now
                    .signed_duration_since(completed_at)
                    .num_seconds()
                    .max(0)
                    .cast_unsigned();
                outcome.oldest_age_secs_skipped = Some(
                    outcome
                        .oldest_age_secs_skipped
                        .map_or(skipped_age, |existing| existing.max(skipped_age)),
                );
                routine_skip_candidate(
                    &mut conn,
                    candidate.id,
                    candidate_cursor,
                    has_failed,
                    &mut outcome,
                    &guard.active_ids,
                )
                .await?;
                continue;
            }

            // Best-effort pre-archival legal-hold re-read (issue #747 BLOCKER 1):
            // a hold placed between candidate selection and now must not be
            // archived to cold storage. This NARROWS (does not fully close) the
            // archival window — a hold landing DURING the multi-second archival
            // network call below may still be archived, then skipped at
            // delete-time by the authoritative FOR UPDATE re-check in
            // `delete_candidate_execution`. That residual is acceptable: the row
            // is preserved in place (never deleted), so archiving an
            // about-to-be-held copy leaks a cold-storage copy but loses no data.
            // We deliberately do NOT hold a row lock across the archival network
            // call. Runs before dry-run too, so a held row is never counted.
            match read_candidate_hold(&mut conn, candidate.id).await {
                Ok((set_at, until)) if legal_hold_active(set_at, until, now) => {
                    routine_skip_candidate(
                        &mut conn,
                        candidate.id,
                        candidate_cursor,
                        has_failed,
                        &mut outcome,
                        &guard.active_ids,
                    )
                    .await?;
                    continue;
                }
                Ok(_) => {}
                Err(err) => {
                    // Fail safe toward RETAINING on uncertainty (the retention
                    // janitor's correct failure mode): do not archive/delete.
                    has_failed = true;
                    batch_failed = true;
                    tracing::error!(candidate_id = %candidate.id, error = %err, "failed to re-read legal hold before archival; skipping deletion");
                    break;
                }
            }

            let mut doc = None;
            if !config.dry_run && archiver.is_some() {
                let exec_id = crate::types::ExecutionId::from_uuid(candidate.id);
                // Inflate offloaded envelopes before archiving so the archived
                // document contains real payloads, not blob references that will
                // be deleted moments later. Issue #524.
                let load_result = if offloader.is_some() {
                    crate::store::load_history_inflated(
                        &mut conn,
                        exec_id,
                        &crate::payload_codec::PayloadCodecs::default(),
                        offloader.as_deref(),
                    )
                    .await
                } else {
                    crate::store::load_history(&mut conn, exec_id).await
                };
                match load_result {
                    Ok(history) => {
                        let req = crate::history_export::HistoryExportRequest {
                            workflow_name: candidate.workflow_name.clone(),
                            execution_id: exec_id,
                            shard_id: shard.as_i32(),
                            state: candidate.state.clone(),
                            events: history.events,
                            exported_at: chrono::Utc::now(),
                            payload_policy: crate::history_export::HistoryPayloadPolicy::Full,
                            max_bytes: Some(usize::MAX),
                            context_headers: candidate
                                .context_headers
                                .as_ref()
                                .and_then(|v| serde_json::from_value(v.clone()).ok()),
                        };
                        match crate::history_export::export_history(req) {
                            Ok(document) => {
                                doc = Some((exec_id, document));
                            }
                            Err(error) => {
                                tracing::error!(
                                    execution_id = %exec_id,
                                    error = %error,
                                    "failed to serialize history export; skipping deletion"
                                );
                                has_failed = true;
                                batch_failed = true;
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            execution_id = %exec_id,
                            error = %error,
                            "failed to load history events for retention candidate; skipping deletion"
                        );
                        has_failed = true;
                        batch_failed = true;
                        break;
                    }
                }
            }

            // Drop/release the DB connection back to the pool before executing the slow network/filesystem archival await!
            drop(conn);

            let mut archive_success = true;
            if let Some((exec_id, document)) = doc
                && let Some(archiver) = &archiver
            {
                let timeout_dur = config.archival_timeout();
                match tokio::time::timeout(timeout_dur, archiver.archive(&document)).await {
                    Ok(Ok(())) => {
                        tracing::debug!(
                            execution_id = %exec_id,
                            "pre-retention archival hook completed successfully"
                        );
                    }
                    Ok(Err(error)) => {
                        tracing::error!(
                            execution_id = %exec_id,
                            error = %error,
                            "pre-retention archival hook failed; skipping deletion"
                        );
                        has_failed = true;
                        archive_success = false;
                        batch_failed = true;
                    }
                    Err(_) => {
                        tracing::error!(
                            execution_id = %exec_id,
                            timeout_secs = timeout_dur.as_secs(),
                            "pre-retention archival hook timed out; skipping deletion"
                        );
                        has_failed = true;
                        archive_success = false;
                        batch_failed = true;
                    }
                }
            }

            if !archive_success {
                break;
            }

            if config.dry_run {
                outcome.deleted_count += 1;
                // Per-type would-delete count (issue #737, AC7).
                *outcome
                    .deleted_by_workflow
                    .entry(candidate.workflow_name.clone())
                    .or_insert(0) += 1;
                if !has_failed {
                    outcome.next_cursor = Some(candidate_cursor);
                }
                continue;
            }

            // Check out a short-lived connection exclusively to execute the candidate deletion transaction
            let mut conn = pool
                .get()
                .await
                .map_err(|error| HarvestError::Database(error.to_string()))?;

            // Collect the candidate's offloaded blob references BEFORE deletion
            // (the rows cascade-delete with the execution). Issue #524.
            let candidate_exec_id = crate::types::ExecutionId::from_uuid(candidate.id);
            let candidate_blob_refs = if offloader.is_some() {
                match crate::store::load_payload_refs(&mut conn, candidate_exec_id).await {
                    Ok(refs) => refs,
                    Err(err) => {
                        has_failed = true;
                        batch_failed = true;
                        tracing::error!(candidate_id = %candidate.id, error = %err, "failed to load payload refs for blob GC; skipping deletion");
                        break;
                    }
                }
            } else {
                Vec::new()
            };

            match delete_candidate_execution(&mut conn, candidate.id, now, config.summary.as_ref())
                .await
            {
                Err(err) => {
                    has_failed = true;
                    batch_failed = true;
                    tracing::error!(candidate_id = %candidate.id, error = %err, "failed to delete candidate execution");
                    break;
                }
                Ok(CandidateDeleteOutcome::SkippedHeld) => {
                    // A legal hold landed after selection (issue #747 BLOCKER 1):
                    // the delete-tx FOR UPDATE re-check found it active and
                    // aborted the delete. Treat exactly like a routine skip —
                    // no delete, no blob GC, no metric, no `has_failed`. The
                    // previously-loaded `candidate_blob_refs` are simply
                    // discarded; the rows still exist (nothing cascaded).
                    routine_skip_candidate(
                        &mut conn,
                        candidate.id,
                        candidate_cursor,
                        has_failed,
                        &mut outcome,
                        &guard.active_ids,
                    )
                    .await?;
                    continue;
                }
                Ok(CandidateDeleteOutcome::Deleted { summarized }) => {
                    // A summary row was written in the delete tx (issue #752).
                    if summarized {
                        outcome.summarized_count += 1;
                    }
                }
            }
            outcome.deleted_count += 1;
            // Per-type real-delete count (issue #737, AC7/AC8).
            *outcome
                .deleted_by_workflow
                .entry(candidate.workflow_name.clone())
                .or_insert(0) += 1;

            // After the execution row (and its refs) are durably gone, delete any
            // blob no longer referenced by a surviving execution. A blob still
            // referenced by e.g. a continue-as-new successor is left intact.
            // Issue #524.
            if let Some(offloader) = &offloader
                && !candidate_blob_refs.is_empty()
            {
                let keys: Vec<String> = candidate_blob_refs
                    .iter()
                    .map(|b| b.blob_key.clone())
                    .collect();
                match crate::store::batch_blob_keys_still_referenced(&mut conn, &keys).await {
                    Ok(still_referenced) => {
                        for blob in &candidate_blob_refs {
                            if !still_referenced.contains(&blob.blob_key)
                                && let Err(err) = offloader.store().delete(&blob.blob_key).await
                            {
                                // Row is already gone; a failed blob delete only leaks
                                // storage (never a dangling reference). Log and continue.
                                tracing::warn!(blob_key = %blob.blob_key, error = %err.0, "failed to delete offloaded blob during retention; leaving for a later sweep");
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to batch-check residual blob references; leaving all blobs intact");
                    }
                }
            }

            {
                let mut active_guard = guard.active_ids.lock().expect("lease guard lock poisoned");
                if let Some(pos) = active_guard.iter().position(|&x| x == candidate.id) {
                    active_guard.swap_remove(pos);
                }
            }

            if !has_failed {
                outcome.next_cursor = Some(candidate_cursor);
            }
        }

        if batch_failed {
            break;
        }
    }

    tracing::debug!(shard = %shard, candidates = outcome.candidate_count, deleted = outcome.deleted_count, "retention shard tick");
    Ok(outcome)
}

/// Outcome of a candidate-deletion attempt (issue #747 BLOCKER 1). A hold that
/// commits AFTER candidate selection but before this delete transaction must be
/// honored, so the delete tx re-checks the legal-hold columns under a row lock
/// as its first statement and aborts the delete when the hold is active.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateDeleteOutcome {
    /// The execution (and its dependent rows) were deleted. `summarized` is
    /// `true` when a `harvest_execution_summaries` row was written in the same
    /// transaction (issue #752); `false` when summary retention is disabled or
    /// the row was already summarized (idempotent ON CONFLICT no-op).
    Deleted { summarized: bool },
    /// A legal hold was found active under the delete-tx row lock; the delete
    /// was aborted and NOTHING was touched (no `payload_refs`, no blobs, no
    /// execution row, no summary). The caller treats this exactly like a
    /// routine skip.
    SkippedHeld,
}

/// The two legal-hold timestamp columns `(legal_hold_set_at, legal_hold_until)`.
#[cfg(feature = "db")]
type HoldTimestamps = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

/// Best-effort, non-locking read of a candidate's legal-hold columns, used for
/// the pre-archival re-read gate (issue #747 BLOCKER 1). A concurrently-deleted
/// row (`None`) reports "not held" — the subsequent delete of a missing row is a
/// harmless no-op.
#[cfg(feature = "db")]
async fn read_candidate_hold(
    conn: &mut diesel_async::AsyncPgConnection,
    candidate_id: uuid::Uuid,
) -> HarvestResult<HoldTimestamps> {
    harvest_workflow_executions::table
        .find(candidate_id)
        .select((
            harvest_workflow_executions::legal_hold_set_at,
            harvest_workflow_executions::legal_hold_until,
        ))
        .first::<HoldTimestamps>(conn)
        .await
        .optional()
        .map_err(database_error)
        .map(|opt| opt.unwrap_or((None, None)))
}

#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
async fn delete_candidate_execution(
    conn: &mut diesel_async::AsyncPgConnection,
    candidate_id: uuid::Uuid,
    now: DateTime<Utc>,
    summary: Option<&SummaryPolicy>,
) -> HarvestResult<CandidateDeleteOutcome> {
    // Copy the policy into the transaction closure (it is `Copy`).
    let summary = summary.copied();
    conn.transaction::<_, HarvestError, _>(|conn| {
        Box::pin(async move {
            let mut summarized = false;
            // ── Authoritative legal-hold re-check under a row lock (issue #747
            // BLOCKER 1) ─────────────────────────────────────────────────────
            // The candidate SELECT read the hold columns then committed and
            // released its lock BEFORE archival + delete, so a hold placed via
            // `set_legal_hold` in that window would otherwise be missed and the
            // held execution deleted. Reading the hold columns FOR UPDATE here,
            // as the FIRST statement of the delete transaction, closes that
            // window absolutely: it serializes against `set_legal_hold`'s own
            // locking read/update, so a hold committed before this SELECT is
            // seen (→ abort the delete), and a hold committing after must wait
            // for this delete tx to finish — by which point the row is gone and
            // the operator's `set_legal_hold` observes a missing row (404). If
            // the hold is active, abort the delete entirely: do NOT touch
            // payload_refs or blobs.
            //
            // Tiered/summary retention (issue #752): when a summary policy is
            // set, the SAME FOR UPDATE row lock loads the demotion source
            // columns (identity, timing, shard, search-attrs, and — opt-in —
            // result/error payload) so the summary INSERT is atomic with the
            // legal-hold re-check and the delete. There is never a window where
            // both the execution and its summary are absent, and a rollback
            // (e.g. a delete error below) discards the summary too, so no
            // orphan can result. The summary INSERT happens AFTER the hold
            // re-check (a held row is never summarized) and BEFORE the deletes.
            if let Some(policy) = summary {
                let row: Option<SummarySourceRow> = harvest_workflow_executions::table
                    .find(candidate_id)
                    .select((
                        harvest_workflow_executions::legal_hold_set_at,
                        harvest_workflow_executions::legal_hold_until,
                        harvest_workflow_executions::workflow_name,
                        harvest_workflow_executions::workflow_id,
                        harvest_workflow_executions::state,
                        harvest_workflow_executions::started_at,
                        harvest_workflow_executions::completed_at,
                        harvest_workflow_executions::shard_id,
                        harvest_workflow_executions::output,
                        harvest_workflow_executions::error,
                        harvest_workflow_executions::search_attrs,
                    ))
                    .for_update()
                    .first::<SummarySourceRow>(conn)
                    .await
                    .optional()
                    .map_err(database_error)?;

                // `None` = row concurrently deleted: nothing to summarize; the
                // deletes below are harmless no-ops (matches the hold-only
                // path's missing-row behavior).
                if let Some((
                    set_at,
                    until,
                    workflow_name,
                    workflow_id,
                    state,
                    started_at,
                    completed_at,
                    shard_id,
                    output,
                    error,
                    search_attrs,
                )) = row
                {
                    if legal_hold_active(set_at, until, now) {
                        return Ok(CandidateDeleteOutcome::SkippedHeld);
                    }
                    // completed_at is NOT NULL by the candidate query's WHERE
                    // clause; fall back to started_at defensively so the NOT
                    // NULL summary column always has a value.
                    let completed = completed_at.unwrap_or(started_at);
                    let duration_ms = Some((completed - started_at).num_milliseconds());
                    // Payload capture is opt-in (AC3): a policy with capture
                    // disabled leaves result/error NULL.
                    let (result, error_out) = if policy.capture_payload {
                        (
                            cap_result_payload(output.as_ref(), policy.max_payload_bytes),
                            cap_error_text(error.as_deref(), policy.max_payload_bytes),
                        )
                    } else {
                        (None, None)
                    };
                    let new_summary = NewExecutionSummary {
                        execution_id: candidate_id,
                        workflow_name,
                        workflow_id,
                        state,
                        started_at,
                        completed_at: completed,
                        duration_ms,
                        shard_id,
                        search_attrs,
                        result,
                        error: error_out,
                    };
                    // ON CONFLICT DO NOTHING makes the demotion idempotent
                    // across a retried delete tx.
                    let inserted = diesel::insert_into(harvest_execution_summaries::table)
                        .values(&new_summary)
                        .on_conflict(harvest_execution_summaries::execution_id)
                        .do_nothing()
                        .execute(conn)
                        .await
                        .map_err(database_error)?;
                    summarized = inserted > 0;
                }
            } else {
                // Summary retention disabled: byte-for-byte the pre-#752
                // hold-only re-check.
                let hold: Option<HoldTimestamps> = harvest_workflow_executions::table
                    .find(candidate_id)
                    .select((
                        harvest_workflow_executions::legal_hold_set_at,
                        harvest_workflow_executions::legal_hold_until,
                    ))
                    .for_update()
                    .first::<HoldTimestamps>(conn)
                    .await
                    .optional()
                    .map_err(database_error)?;
                if let Some((set_at, until)) = hold
                    && legal_hold_active(set_at, until, now)
                {
                    return Ok(CandidateDeleteOutcome::SkippedHeld);
                }
            }

            diesel::update(
                harvest_workflow_executions::table
                    .filter(harvest_workflow_executions::parent_id.eq(Some(candidate_id)))
                    .filter(harvest_workflow_executions::state.eq_any([
                        "COMPLETED",
                        "FAILED",
                        "CANCELLED",
                        "TIMED_OUT",
                        "CONTINUED_AS_NEW",
                        "TERMINATED",
                    ])),
            )
            .set(harvest_workflow_executions::parent_id.eq::<Option<uuid::Uuid>>(None))
            .execute(conn)
            .await
            .map_err(database_error)?;

            // `task_type = 'CALLBACK'` dead letters (issue #605) are
            // excluded here (issue #921 review, Codex P2): a CALLBACK
            // dead-letter row only ever exists for a delivery that reached
            // `FAILED`, and the completion-deliveries delete just below
            // deliberately keeps every non-`DELIVERED` (i.e. `FAILED`)
            // delivery row around for redrive -- deleting its DLQ entry
            // here would drop it from the `GET /dead-letters` / aggregate
            // discovery surface while the delivery row itself (and its
            // redrive path) still exists, breaking the advertised "find
            // failures via DLQ" operator workflow for any callback failure
            // that outlives the owning workflow's retention window. A
            // redrive already deletes its own DLQ row on success, so this
            // exclusion cannot leak an entry whose delivery was resolved.
            diesel::delete(
                harvest_dead_letters::table
                    .filter(harvest_dead_letters::workflow_exec_id.eq(Some(candidate_id)))
                    .filter(harvest_dead_letters::task_type.ne("CALLBACK")),
            )
            .execute(conn)
            .await
            .map_err(database_error)?;

            // `harvest_completion_deliveries.workflow_exec_id` has no `ON
            // DELETE CASCADE` (issue #605 code review). Only `DELIVERED`
            // rows are deleted here — a fully successful delivery has
            // nothing left to do, so it is safe cleanup exactly like
            // `harvest_dead_letters` above. A `PENDING`/`INFLIGHT`/`FAILED`
            // row is deliberately left alone (PR #921 review, Codex): its
            // `payload` is frozen precisely so delivery does not depend on
            // the execution row surviving, and this execution reaching its
            // retention age has no bearing on whether its callback still
            // needs to be retried or is awaiting an operator's redrive.
            // Known limitation: once its owning execution is collected, a
            // surviving non-`DELIVERED` row references a `workflow_exec_id`
            // that no longer exists (there is no FK to violate, so this is
            // safe) and this retention pass will never revisit that exact
            // execution again — so even if the delivery later resolves to
            // `DELIVERED`, nothing currently deletes it. A future dedicated
            // delivery-retention policy, scoped to this table's own age/
            // state rather than its owning execution's, would be needed to
            // reclaim those rows; out of scope here, where the goal is only
            // to stop retention from destroying a delivery that hasn't
            // finished yet.
            diesel::delete(
                harvest_completion_deliveries::table
                    .filter(harvest_completion_deliveries::workflow_exec_id.eq(candidate_id))
                    .filter(harvest_completion_deliveries::state.eq("DELIVERED")),
            )
            .execute(conn)
            .await
            .map_err(database_error)?;

            diesel::delete(
                harvest_workflow_executions::table
                    .filter(harvest_workflow_executions::id.eq(candidate_id)),
            )
            .execute(conn)
            .await
            .map_err(database_error)?;
            Ok(CandidateDeleteOutcome::Deleted { summarized })
        })
    })
    .await
}

/// Columns loaded FOR UPDATE to build a summary alongside the delete (issue
/// #752): the two legal-hold columns plus the demotion source columns.
#[cfg(feature = "db")]
type SummarySourceRow = (
    Option<DateTime<Utc>>,     // legal_hold_set_at
    Option<DateTime<Utc>>,     // legal_hold_until
    String,                    // workflow_name
    String,                    // workflow_id
    String,                    // state
    DateTime<Utc>,             // started_at
    Option<DateTime<Utc>>,     // completed_at
    i32,                       // shard_id
    Option<serde_json::Value>, // output
    Option<String>,            // error
    Option<serde_json::Value>, // search_attrs
);

/// Garbage-collect execution summaries older than the summary horizon (issue
/// #752), returning per-workflow-type deleted counts.
///
/// Selection is by `completed_at < now - summary_age` (deterministic;
/// *not* `summarized_at`), so the summary tier's own horizon anchors on the
/// original run window exactly like history retention. Batched to bound the
/// transaction size; deletes only (a `dry_run` tick simulates by counting).
/// Shard-local: `conn` is already routed to the summary's own shard.
#[cfg(feature = "db")]
pub(crate) async fn purge_expired_summaries(
    conn: &mut diesel_async::AsyncPgConnection,
    _shard: u16,
    summary_age: Duration,
    batch_size: usize,
    dry_run: bool,
    now: DateTime<Utc>,
) -> HarvestResult<BTreeMap<String, u64>> {
    #[derive(QueryableByName)]
    struct NameRow {
        #[diesel(sql_type = Text)]
        workflow_name: String,
    }

    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let Ok(chrono_age) = chrono::Duration::from_std(summary_age) else {
        // Unrepresentable age (unreachable for validated horizons, bounded by
        // MAX_MAX_AGE ≪ chrono's range): fail safe toward RETAINING — GC
        // nothing this tick rather than deleting everything under a zero cutoff.
        return Ok(counts);
    };
    let cutoff = now - chrono_age;
    let batch = i64::try_from(batch_size).unwrap_or(i64::MAX).max(1);

    if dry_run {
        let rows = diesel::sql_query(
            "SELECT workflow_name FROM harvest_execution_summaries WHERE completed_at < $1",
        )
        .bind::<Timestamptz, _>(cutoff)
        .load::<NameRow>(conn)
        .await
        .map_err(database_error)?;
        for r in rows {
            *counts.entry(r.workflow_name).or_insert(0) += 1;
        }
        return Ok(counts);
    }

    loop {
        let rows = diesel::sql_query(
            "DELETE FROM harvest_execution_summaries
             WHERE execution_id IN (
                 SELECT execution_id FROM harvest_execution_summaries
                 WHERE completed_at < $1
                 ORDER BY completed_at ASC, execution_id ASC
                 LIMIT $2
             )
             RETURNING workflow_name",
        )
        .bind::<Timestamptz, _>(cutoff)
        .bind::<BigInt, _>(batch)
        .load::<NameRow>(conn)
        .await
        .map_err(database_error)?;
        let n = rows.len();
        for r in rows {
            *counts.entry(r.workflow_name).or_insert(0) += 1;
        }
        if n < batch_size {
            break;
        }
    }
    Ok(counts)
}

/// Common bookkeeping for a "routine skip" of a retention candidate (issue
/// #737): release the candidate's retention lease so a later tick can revisit
/// it, advance the scan cursor when the tick has not already failed, and drop
/// the id from the lease guard's active set. Used by every non-deleting,
/// non-failing per-candidate decision (never-delete type, not-old-enough for
/// its type, and the pre-existing dependency-based `should_skip_candidate`).
/// It must NOT freeze the cursor and must NOT set `has_failed`.
#[cfg(feature = "db")]
async fn routine_skip_candidate(
    conn: &mut diesel_async::AsyncPgConnection,
    candidate_id: uuid::Uuid,
    candidate_cursor: RetentionScanCursor,
    has_failed: bool,
    outcome: &mut ShardTickOutcome,
    active_ids: &Arc<Mutex<Vec<uuid::Uuid>>>,
) -> HarvestResult<()> {
    // Release its lease immediately so it can be picked up on subsequent ticks.
    diesel::update(
        harvest_workflow_executions::table.filter(harvest_workflow_executions::id.eq(candidate_id)),
    )
    .set(harvest_workflow_executions::sticky_worker_id.eq::<Option<String>>(None))
    .execute(conn)
    .await
    .map_err(database_error)?;

    // Advance cursor for routine skips.
    if !has_failed {
        outcome.next_cursor = Some(candidate_cursor);
    }

    {
        let mut active_guard = active_ids.lock().expect("lease guard lock poisoned");
        if let Some(pos) = active_guard.iter().position(|&x| x == candidate_id) {
            active_guard.swap_remove(pos);
        }
    }
    Ok(())
}

#[cfg(feature = "db")]
async fn should_skip_candidate(
    conn: &mut diesel_async::AsyncPgConnection,
    candidate: &CandidateExecution,
    cutoff: DateTime<Utc>,
) -> HarvestResult<bool> {
    let active_parent_ref_count = diesel::sql_query(
        "SELECT COUNT(*) AS count
         FROM harvest_workflow_executions
         WHERE parent_id = $1
           AND state NOT IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW','TERMINATED')",
    )
    .bind::<SqlUuid, _>(candidate.id)
    .get_result::<CountRow>(conn)
    .await
    .map_err(database_error)?
    .count;

    if active_parent_ref_count > 0 {
        return Ok(true);
    }

    let inflight_task_count = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(candidate.id)))
        .filter(harvest_task_queue::state.eq_any(["PENDING", "RUNNING"]))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    if inflight_task_count > 0 {
        return Ok(true);
    }

    let pending_signal_count = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(candidate.id))
        .filter(harvest_signals::consumed.eq(false))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    if pending_signal_count > 0 {
        return Ok(true);
    }

    let pending_timer_count = harvest_timers::table
        .filter(harvest_timers::workflow_exec_id.eq(candidate.id))
        .filter(harvest_timers::fired.eq(false))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    if pending_timer_count > 0 {
        return Ok(true);
    }

    let chain_link_count = diesel::sql_query(
        "SELECT COUNT(*) AS count
         FROM harvest_workflow_executions
         WHERE workflow_name = $1
           AND workflow_id = $2
           AND id <> $3
           AND (
                state NOT IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW','TERMINATED')
               OR completed_at IS NULL
               OR completed_at >= $4
           )",
    )
    .bind::<Text, _>(&candidate.workflow_name)
    .bind::<Text, _>(&candidate.workflow_id)
    .bind::<SqlUuid, _>(candidate.id)
    .bind::<Timestamptz, _>(cutoff)
    .get_result::<CountRow>(conn)
    .await
    .map_err(database_error)?
    .count;

    Ok(chain_link_count > 0)
}

#[cfg(feature = "db")]
#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

// ── Per-execution legal hold (issue #747) ─────────────────────────────────────

/// Returns `true` when a per-execution legal hold is currently ACTIVE.
///
/// This is the **single source of truth** for the active-hold predicate, shared
/// by the retention-skip gate, the PII-erasure gate (issue #495), the
/// describe/list surfaces, and the set/release core functions. A hold is active
/// when it was placed (`set_at IS NOT NULL`) and has not auto-expired
/// (`until IS NULL`, i.e. indefinite, or `until > now`).
///
/// An expired hold (`until <= now`) is treated as inactive: the execution
/// becomes eligible for retention and erasure again without any explicit
/// release.
#[must_use]
pub fn legal_hold_active(
    set_at: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> bool {
    set_at.is_some() && until.is_none_or(|deadline| deadline > now)
}

/// Result of a [`set_legal_hold`] or [`release_legal_hold`] call.
///
/// Modeled on the idempotency shape of `terminate_workflow_execution`: the
/// operation is idempotent, and `newly_held` / `released` report whether this
/// call actually changed state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LegalHoldOutcome {
    /// The execution the hold operation targeted.
    pub execution_id: String,
    /// Whether a legal hold is ACTIVE after this operation completed.
    pub held: bool,
    /// The active hold's reason, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legal_hold_reason: Option<String>,
    /// The active hold's actor (the principal who placed it), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legal_hold_actor: Option<String>,
    /// When the active hold was placed, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legal_hold_set_at: Option<DateTime<Utc>>,
    /// The active hold's auto-expiry, if any (`None` = indefinite).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legal_hold_until: Option<DateTime<Utc>>,
    /// `true` when this `set` call actually placed a fresh hold; `false` when a
    /// hold was already active (idempotent no-op, provenance preserved).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub newly_held: bool,
    /// `true` when this `release` call actually cleared a hold; `false` when no
    /// hold was set (idempotent no-op).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub released: bool,
}

#[cfg(feature = "db")]
mod legal_hold_db {
    use chrono::{DateTime, Utc};
    use diesel::prelude::*;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    use crate::error::{HarvestError, HarvestResult, database_error};
    use crate::schema::harvest_workflow_executions;
    use crate::types::ExecutionId;

    use super::{LegalHoldOutcome, legal_hold_active};

    /// The four legal-hold columns loaded from a locked execution row.
    type HoldColumns = (
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        Option<String>,
        Option<String>,
    );

    async fn load_hold_for_update(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<HoldColumns> {
        harvest_workflow_executions::table
            .find(exec_id.as_uuid())
            .select((
                harvest_workflow_executions::legal_hold_set_at,
                harvest_workflow_executions::legal_hold_until,
                harvest_workflow_executions::legal_hold_reason,
                harvest_workflow_executions::legal_hold_actor,
            ))
            .for_update()
            .first::<HoldColumns>(conn)
            .await
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
    }

    /// Place (or refresh) a per-execution legal hold (issue #747). Shard-local:
    /// the caller routes `conn` to the execution's own shard.
    ///
    /// Idempotent: if an ACTIVE hold already exists, its provenance is left
    /// unchanged and `newly_held = false` is returned (no write). If there is no
    /// hold, or a previously-set hold has expired, the four columns are set and
    /// `newly_held = true` is returned.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NotFound`] when the execution does not exist (→ 404).
    /// - [`HarvestError::Database`] on any persistence failure.
    pub async fn set_legal_hold(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        reason: &str,
        hold_until: Option<DateTime<Utc>>,
        actor: &str,
        now: DateTime<Utc>,
    ) -> HarvestResult<LegalHoldOutcome> {
        // Empty reason normalizes to NULL (issue #747 MINOR 4) so a blank hold
        // stores `NULL` rather than `Some("")`, keeping the erase-rejection
        // message and describe field clean.
        let stored_reason = (!reason.is_empty()).then_some(reason);

        // The read + update run in one transaction (issue #747 MINOR 1) so the
        // `FOR UPDATE` lock in `load_hold_for_update` actually serializes the
        // read-modify-write against a concurrent set/release (and against the
        // retention delete-tx re-check), rather than releasing at statement end
        // in autocommit mode.
        conn.transaction::<LegalHoldOutcome, HarvestError, _>(|conn| {
            Box::pin(async move {
                let (set_at, until, cur_reason, cur_actor) =
                    load_hold_for_update(conn, exec_id).await?;

                if legal_hold_active(set_at, until, now) {
                    // Idempotent: an active hold already exists. Do NOT overwrite
                    // provenance — return the existing hold unchanged.
                    return Ok(LegalHoldOutcome {
                        execution_id: exec_id.to_string(),
                        held: true,
                        legal_hold_reason: cur_reason,
                        legal_hold_actor: cur_actor,
                        legal_hold_set_at: set_at,
                        legal_hold_until: until,
                        newly_held: false,
                        released: false,
                    });
                }

                diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                    .set((
                        harvest_workflow_executions::legal_hold_set_at.eq(Some(now)),
                        harvest_workflow_executions::legal_hold_until.eq(hold_until),
                        harvest_workflow_executions::legal_hold_reason.eq(stored_reason),
                        harvest_workflow_executions::legal_hold_actor.eq(Some(actor)),
                    ))
                    .execute(conn)
                    .await
                    .map_err(database_error)?;

                // Report held/newly_held ACCURATELY (issue #747 MAJOR): a
                // `hold_until` already in the past writes the columns but places
                // no ACTIVE hold, so a direct core/CLI caller never gets a false
                // `held: true`. (The HTTP handler rejects a past `hold_until`
                // with 400 before reaching here — this is the belt-and-braces
                // core-layer guard.)
                let active = legal_hold_active(Some(now), hold_until, now);

                Ok(LegalHoldOutcome {
                    execution_id: exec_id.to_string(),
                    held: active,
                    legal_hold_reason: stored_reason.map(str::to_string),
                    legal_hold_actor: Some(actor.to_string()),
                    legal_hold_set_at: Some(now),
                    legal_hold_until: hold_until,
                    newly_held: active,
                    released: false,
                })
            })
        })
        .await
    }

    /// Release a per-execution legal hold (issue #747). Shard-local.
    ///
    /// Idempotent: NULLs all four columns. `released = true` when a hold was
    /// previously set (regardless of whether it had already expired),
    /// `released = false` when the execution carried no hold at all (no-op).
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NotFound`] when the execution does not exist (→ 404).
    /// - [`HarvestError::Database`] on any persistence failure.
    pub async fn release_legal_hold(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        _now: DateTime<Utc>,
    ) -> HarvestResult<LegalHoldOutcome> {
        // Read + update in one transaction (issue #747 MINOR 1) so the
        // `FOR UPDATE` lock holds across the clear, serializing against a
        // concurrent set/release.
        conn.transaction::<LegalHoldOutcome, HarvestError, _>(|conn| {
            Box::pin(async move {
                let (set_at, _until, _reason, _actor) = load_hold_for_update(conn, exec_id).await?;

                let was_set = set_at.is_some();
                if was_set {
                    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                        .set((
                            harvest_workflow_executions::legal_hold_set_at
                                .eq::<Option<DateTime<Utc>>>(None),
                            harvest_workflow_executions::legal_hold_until
                                .eq::<Option<DateTime<Utc>>>(None),
                            harvest_workflow_executions::legal_hold_reason
                                .eq::<Option<String>>(None),
                            harvest_workflow_executions::legal_hold_actor
                                .eq::<Option<String>>(None),
                        ))
                        .execute(conn)
                        .await
                        .map_err(database_error)?;
                }

                Ok(LegalHoldOutcome {
                    execution_id: exec_id.to_string(),
                    held: false,
                    legal_hold_reason: None,
                    legal_hold_actor: None,
                    legal_hold_set_at: None,
                    legal_hold_until: None,
                    newly_held: false,
                    released: was_set,
                })
            })
        })
        .await
    }
}

#[cfg(feature = "db")]
pub use legal_hold_db::{release_legal_hold, set_legal_hold};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ShardId;
    use std::time::Duration;

    #[test]
    fn test_retention_config_validation() {
        let config = RetentionConfig::default();
        assert!(config.validate().is_ok());

        // Test tick_interval = 0 is invalid
        let config = RetentionConfig {
            tick_interval_secs: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test batch_size = 0 is invalid
        let config = RetentionConfig {
            batch_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test max_age validation bounds
        let mut config = RetentionConfig {
            max_age_secs: Some(0), // under MIN_MAX_AGE (1s)
            ..Default::default()
        };
        assert!(config.validate().is_err());

        config.max_age_secs = Some(60 * 60 * 24 * 365 * 20); // over MAX_MAX_AGE (10 years)
        assert!(config.validate().is_err());

        config.max_age_secs = Some(3600); // valid
        assert!(config.validate().is_ok());

        // Test archival_timeout_secs = 0 is invalid
        let config = RetentionConfig {
            archival_timeout_secs: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // --- Issue #737: per-workflow-type history retention overrides ---

    #[test]
    fn test_effective_max_age_resolution() {
        // override present -> override wins over global
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("slow_wf", Duration::from_secs(7200));
        assert_eq!(
            config.effective_max_age("slow_wf"),
            Some(Duration::from_secs(7200))
        );
        // no override for this type -> falls back to global
        assert_eq!(
            config.effective_max_age("other_wf"),
            Some(Duration::from_secs(3600))
        );

        // no global, override present -> override for that type, None for others
        // (Default already leaves max_age_secs = None.)
        let config =
            RetentionConfig::default().with_workflow_override("only_wf", Duration::from_secs(500));
        assert_eq!(
            config.effective_max_age("only_wf"),
            Some(Duration::from_secs(500))
        );
        assert_eq!(config.effective_max_age("other_wf"), None);

        // neither -> None (never delete)
        let config = RetentionConfig::default();
        assert_eq!(config.effective_max_age("anything"), None);
    }

    #[test]
    fn test_loosest_cutoff_age() {
        // global only
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600));
        assert_eq!(config.loosest_cutoff_age(), Some(Duration::from_secs(3600)));

        // global + overrides -> min of all
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("longer", Duration::from_secs(7200))
            .with_workflow_override("shorter", Duration::from_secs(600));
        assert_eq!(config.loosest_cutoff_age(), Some(Duration::from_secs(600)));

        // no global, overrides only -> min override
        let config = RetentionConfig::default()
            .with_workflow_override("a", Duration::from_secs(900))
            .with_workflow_override("b", Duration::from_secs(300));
        assert_eq!(config.loosest_cutoff_age(), Some(Duration::from_secs(300)));

        // global present but larger than an override -> override wins as loosest
        let config = RetentionConfig::with_max_age(Duration::from_secs(5000))
            .with_workflow_override("tiny", Duration::from_secs(100));
        assert_eq!(config.loosest_cutoff_age(), Some(Duration::from_secs(100)));

        // neither -> None
        let config = RetentionConfig::default();
        assert_eq!(config.loosest_cutoff_age(), None);
    }

    #[test]
    fn test_history_retention_active() {
        // both unset
        let config = RetentionConfig::default();
        assert!(!config.history_retention_active());

        // global set
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600));
        assert!(config.history_retention_active());

        // only overrides set
        let config =
            RetentionConfig::default().with_workflow_override("wf", Duration::from_secs(60));
        assert!(config.history_retention_active());
    }

    #[test]
    fn test_validate_overrides_bounds() {
        // below MIN_MAX_AGE
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("wf", Duration::from_secs(0));
        assert!(config.validate().is_err());

        // above MAX_MAX_AGE
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("wf", Duration::from_secs(60 * 60 * 24 * 365 * 20));
        assert!(config.validate().is_err());

        // in range
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("wf", Duration::from_secs(7200));
        assert!(config.validate().is_ok());

        // exactly MIN_MAX_AGE (1s) -> Ok (inclusive lower bound; guards against
        // an off-by-one flip of ..= to ..)
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("wf", Duration::from_secs(1));
        assert!(config.validate().is_ok());

        // exactly MAX_MAX_AGE (10 years) -> Ok (inclusive upper bound)
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("wf", Duration::from_secs(60 * 60 * 24 * 365 * 10));
        assert!(config.validate().is_ok());

        // empty overrides + valid global -> Ok
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_overrides_backward_compat_empty() {
        // With no overrides, effective_max_age(any) == max_age() for all names.
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600));
        assert!(config.workflow_overrides().is_empty());
        assert_eq!(config.effective_max_age("any_wf"), config.max_age());
        assert_eq!(config.effective_max_age("another"), config.max_age());
    }

    #[test]
    fn test_with_workflow_overrides_bulk() {
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_overrides([
                ("a".to_string(), Duration::from_secs(100)),
                ("b".to_string(), Duration::from_secs(200)),
            ]);
        assert_eq!(config.workflow_overrides().len(), 2);
        assert_eq!(
            config.effective_max_age("a"),
            Some(Duration::from_secs(100))
        );
        assert_eq!(
            config.effective_max_age("b"),
            Some(Duration::from_secs(200))
        );
    }

    #[test]
    fn test_retention_config_enabled() {
        let config = RetentionConfig {
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            ..Default::default()
        };
        // default with no purging is not enabled
        assert!(!config.enabled());

        let config = RetentionConfig {
            max_age_secs: Some(3600),
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            ..Default::default()
        };
        assert!(config.enabled());

        let config = RetentionConfig {
            audit_retention_days: 30,
            ..Default::default()
        };
        assert!(config.enabled());

        let config = RetentionConfig {
            schedule_decision_retention_days: 7,
            ..Default::default()
        };
        assert!(config.enabled());

        // Overrides-only (no global max_age, no audit/schedule purging) still
        // enables the janitor (issue #737).
        let config = RetentionConfig {
            max_age_secs: None,
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            ..Default::default()
        }
        .with_workflow_override("wf", Duration::from_secs(3600));
        assert!(config.enabled());
    }

    // --- Issue #752: tiered / summary retention ---

    #[test]
    fn summary_policy_builders() {
        let p = SummaryPolicy::for_days(7);
        assert_eq!(p.retention_age(), Some(Duration::from_secs(7 * 86_400)));
        assert!(!p.capture_payload());
        assert_eq!(p.max_payload_bytes(), DEFAULT_SUMMARY_PAYLOAD_CAP);

        let p = SummaryPolicy::for_duration(Duration::from_secs(500));
        assert_eq!(p.retention_age(), Some(Duration::from_secs(500)));

        let p = SummaryPolicy::unbounded();
        assert_eq!(p.retention_age(), None);
        assert_eq!(p.retention, SummaryRetention::Unbounded);

        let p = SummaryPolicy::for_days(1)
            .with_payload_capture()
            .with_max_payload_bytes(2048);
        assert!(p.capture_payload());
        assert_eq!(p.max_payload_bytes(), 2048);
    }

    #[test]
    fn summary_config_builders_and_predicates() {
        // Default: summary disabled -> byte-for-byte today's behavior.
        let config = RetentionConfig::default();
        assert!(!config.summary_enabled());
        assert!(!config.summary_gc_active());
        assert_eq!(config.summary_age(), None);
        assert!(config.summary_policy().is_none());

        // Bounded summary -> gc active, spawns janitor even without a history
        // horizon (issue #752).
        let config = RetentionConfig {
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            ..RetentionConfig::default()
        }
        .with_summary_retention_days(30);
        assert!(config.summary_enabled());
        assert!(config.summary_gc_active());
        assert_eq!(config.summary_age(), Some(Duration::from_secs(30 * 86_400)));
        assert!(config.enabled(), "bounded summary must enable the janitor");

        // Unbounded summary -> enabled but never GC'd; does NOT by itself spawn
        // the janitor (no deletes => no summaries to create, nothing to GC).
        let config = RetentionConfig {
            audit_retention_days: 0,
            schedule_decision_retention_days: 0,
            ..RetentionConfig::default()
        }
        .with_summary_retention(SummaryPolicy::unbounded());
        assert!(config.summary_enabled());
        assert!(!config.summary_gc_active());
        assert_eq!(config.summary_age(), None);
        assert!(
            !config.enabled(),
            "an unbounded-summary-only config with no history/audit horizon is not enabled"
        );

        // But a history horizon + unbounded summary IS enabled (via history).
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_summary_retention(SummaryPolicy::unbounded());
        assert!(config.enabled());
    }

    #[test]
    fn summary_validate_bounds_the_horizon() {
        // below MIN_MAX_AGE
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_summary_retention(SummaryPolicy::for_duration(Duration::from_secs(0)));
        assert!(config.validate().is_err());

        // above MAX_MAX_AGE
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_summary_retention(SummaryPolicy::for_duration(Duration::from_secs(
                60 * 60 * 24 * 365 * 20,
            )));
        assert!(config.validate().is_err());

        // in range
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_summary_retention_days(90);
        assert!(config.validate().is_ok());

        // Unbounded needs no bound.
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_summary_retention(SummaryPolicy::unbounded());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn omitted_marker_shape() {
        let m = omitted_marker("too_large", Some(5000));
        assert_eq!(m[OMITTED_MARKER_KEY], true);
        assert_eq!(m["reason"], "too_large");
        assert_eq!(m["bytes"], 5000);

        let m = omitted_marker("offloaded", None);
        assert_eq!(m[OMITTED_MARKER_KEY], true);
        assert_eq!(m["reason"], "offloaded");
        assert!(m.get("bytes").is_none(), "no bytes for offloaded marker");
    }

    #[test]
    fn cap_result_payload_none_and_small_and_over_cap() {
        // None -> None
        assert!(cap_result_payload(None, 4096).is_none());

        // small inline -> stored verbatim
        let small = serde_json::json!({"ok": true, "n": 1});
        assert_eq!(cap_result_payload(Some(&small), 4096), Some(small.clone()));

        // over-cap -> too_large marker carrying the observed byte length
        let big_str = "x".repeat(100);
        let big = serde_json::json!({"blob": big_str});
        let len = serde_json::to_vec(&big).unwrap().len();
        let capped = cap_result_payload(Some(&big), 32).expect("some");
        assert_eq!(capped[OMITTED_MARKER_KEY], true);
        assert_eq!(capped["reason"], "too_large");
        assert_eq!(capped["bytes"], len);
    }

    #[test]
    fn cap_result_payload_never_stores_an_offload_envelope() {
        // A synthetic offload reference envelope (issue #524): must become the
        // "offloaded" marker, NOT the envelope itself (the blob may be GC'd).
        let envelope = serde_json::json!({
            "_harvest_offload_envelope": 1,
            "store_id": "s3",
            "key": "blob/abc123",
            "len": 2_000_000,
            "checksum": "deadbeef",
        });
        let capped = cap_result_payload(Some(&envelope), 4096).expect("some");
        assert_eq!(capped[OMITTED_MARKER_KEY], true);
        assert_eq!(capped["reason"], "offloaded");
        assert!(
            capped.get("key").is_none(),
            "the blob key/reference must NOT leak into the summary"
        );
    }

    #[test]
    fn cap_error_text_none_small_and_over_cap() {
        assert!(cap_error_text(None, 4096).is_none());
        assert_eq!(cap_error_text(Some("boom"), 4096), Some("boom".to_string()));
        let big = "e".repeat(200);
        let capped = cap_error_text(Some(&big), 32).expect("some");
        assert_eq!(capped, "[omitted: too_large, 200 bytes]");
    }

    #[test]
    fn test_retention_monitor() {
        let config = RetentionConfig::with_max_age(Duration::from_secs(3600));
        let shards = vec![ShardId::new(0), ShardId::new(1)].into_iter();
        let monitor = RetentionMonitor::new(config, shards);

        let snapshot = monitor.snapshot();
        assert_eq!(snapshot.per_shard.len(), 2);
        assert_eq!(snapshot.per_shard[0].shard, 0);
        assert_eq!(snapshot.per_shard[1].shard, 1);
    }

    #[tokio::test]
    #[cfg(feature = "db")]
    async fn test_run_shard_tick_cursor_frozen_on_skip() {
        // Build mock candidates
        let candidate_ok = CandidateExecution {
            id: uuid::Uuid::new_v4(),
            workflow_name: "test".to_string(),
            workflow_id: "ok".to_string(),
            state: "COMPLETED".to_string(),
            completed_at: Some(Utc::now() - chrono::Duration::days(10)),
            context_headers: None,
            legal_hold_set_at: None,
            legal_hold_until: None,
        };
        let candidate_skip = CandidateExecution {
            id: uuid::Uuid::new_v4(),
            workflow_name: "test".to_string(),
            workflow_id: "skip".to_string(),
            state: "COMPLETED".to_string(),
            completed_at: Some(Utc::now() - chrono::Duration::days(9)),
            context_headers: None,
            legal_hold_set_at: None,
            legal_hold_until: None,
        };

        // When evaluating outcome next_cursor logic, if the first candidate completes,
        // outcome.next_cursor should advance to it. If the second candidate is skipped,
        // outcome.next_cursor must freeze on the first candidate's cursor to ensure retries on subsequent ticks.
        let mut outcome = ShardTickOutcome::default();
        let mut has_skipped = false;

        // candidate 1 (success)
        let cursor1 = RetentionScanCursor {
            completed_at: candidate_ok.completed_at.unwrap(),
            id: candidate_ok.id,
        };
        if !has_skipped {
            outcome.next_cursor = Some(cursor1);
        }

        // candidate 2 (skipped)
        let cursor2 = RetentionScanCursor {
            completed_at: candidate_skip.completed_at.unwrap(),
            id: candidate_skip.id,
        };
        has_skipped = true;
        if !has_skipped {
            outcome.next_cursor = Some(cursor2);
        }

        assert_eq!(outcome.next_cursor, Some(cursor1));
    }

    // ── Legal hold (issue #747) ───────────────────────────────────────────────

    #[test]
    fn legal_hold_inactive_when_never_set() {
        let now = Utc::now();
        assert!(!legal_hold_active(None, None, now));
        // A stray `until` with no `set_at` is still not a hold.
        assert!(!legal_hold_active(
            None,
            Some(now + chrono::Duration::days(1)),
            now
        ));
    }

    #[test]
    fn legal_hold_active_when_set_and_indefinite() {
        let now = Utc::now();
        assert!(legal_hold_active(
            Some(now - chrono::Duration::hours(1)),
            None,
            now
        ));
    }

    #[test]
    fn legal_hold_active_when_until_in_future() {
        let now = Utc::now();
        assert!(legal_hold_active(
            Some(now - chrono::Duration::hours(1)),
            Some(now + chrono::Duration::hours(1)),
            now
        ));
    }

    #[test]
    fn legal_hold_inactive_when_expired() {
        let now = Utc::now();
        // until == now is expired (strict `>`): a hold whose deadline has been
        // reached is no longer active.
        assert!(!legal_hold_active(
            Some(now - chrono::Duration::hours(2)),
            Some(now),
            now
        ));
        assert!(!legal_hold_active(
            Some(now - chrono::Duration::hours(2)),
            Some(now - chrono::Duration::hours(1)),
            now
        ));
    }

    #[test]
    fn legal_hold_outcome_omits_optional_none_fields() {
        let released = LegalHoldOutcome {
            execution_id: "exec-1".into(),
            held: false,
            legal_hold_reason: None,
            legal_hold_actor: None,
            legal_hold_set_at: None,
            legal_hold_until: None,
            newly_held: false,
            released: true,
        };
        let v = serde_json::to_value(&released).unwrap();
        assert_eq!(v["held"], false);
        assert_eq!(v["released"], true);
        assert!(v.get("newly_held").is_none(), "false flag is omitted");
        assert!(v.get("legal_hold_reason").is_none());
        assert!(v.get("legal_hold_until").is_none());

        let held = LegalHoldOutcome {
            execution_id: "exec-2".into(),
            held: true,
            legal_hold_reason: Some("subpoena".into()),
            legal_hold_actor: Some("legal@corp".into()),
            legal_hold_set_at: Some(Utc::now()),
            legal_hold_until: None,
            newly_held: true,
            released: false,
        };
        let v = serde_json::to_value(&held).unwrap();
        assert_eq!(v["held"], true);
        assert_eq!(v["newly_held"], true);
        assert_eq!(v["legal_hold_reason"], "subpoena");
        assert!(v.get("released").is_none(), "false flag is omitted");
    }
}
