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

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use diesel::sql_types::{Nullable, Text, Timestamptz};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::future::join_all;
use serde::Serialize;

use crate::api::HarvestApiState;
use crate::shard_fanout::{self, FanoutStatus, ShardObservation, UnavailableShard};

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

/// Register the built-in synthetic liveness canary onto a [`HarvestBuilder`]
/// (issue #796).
///
/// Pure and unit-testable — it mutates and returns the builder without starting
/// a runtime. For each configured probe queue it:
///
/// - registers the reserved canary activity (once, for all queues);
/// - registers a per-queue probe workflow named `{PREFIX}__{queue}`;
/// - ensures a worker drains that queue;
/// - schedules the probe on `cfg.interval()` across **every writable shard**
///   (AC4), with [`OverlapPolicy::Skip`] so probes never pile up (AC6) and a
///   per-run `execution_timeout` so a wedged probe times out (AC6); and
/// - registers an aggressive self-cleaning retention override for the probe
///   workflow (AC9), which — since an overrides-only [`RetentionConfig`] still
///   runs the janitor and resolves the override age via `effective_max_age` —
///   causes the canary's own history to be purged shortly after each run even
///   when no global retention `max_age` is configured.
#[must_use]
pub fn register_canary(mut builder: HarvestBuilder, cfg: &CanaryConfig) -> HarvestBuilder {
    use autumn_harvest::canary::CANARY_WORKFLOW_NAME_PREFIX;

    // Register the reserved canary activity once (shared by every queue's probe).
    builder = builder.activities(vec![canary_activity_info()]);

    let per_probe_timeout = cfg.per_probe_timeout();
    let retention_age = cfg.effective_retention();

    // Dedupe queues (a user may double-add via with_queue) while preserving
    // order, so we never register two probes for the same queue.
    let mut seen: Vec<String> = Vec::new();
    let mut wf_names: Vec<String> = Vec::new();
    for queue in cfg.queues() {
        if seen.iter().any(|q| q == queue) {
            continue;
        }
        seen.push(queue.clone());

        let wf_name = format!("{CANARY_WORKFLOW_NAME_PREFIX}__{queue}");

        // Register the per-queue probe workflow.
        builder = builder.workflows(vec![canary_workflow_info(
            wf_name.clone(),
            per_probe_timeout,
        )]);

        // Ensure the probe's queue is drained by a worker.
        if !builder
            .worker_config_mut()
            .queues
            .iter()
            .any(|q| q == queue)
        {
            builder.worker_config_mut().queues.push(queue.clone());
        }

        // Schedule the probe across every writable shard (AC4), never piling
        // up (AC6), with a per-run deadline (AC6). The input carries the queue
        // so the probe workflow dispatches its activity there.
        let ws = WorkflowSchedule::new(wf_name.clone(), Schedule::Interval(cfg.interval()))
            .with_queue_name(queue.clone())
            .with_overlap_policy(OverlapPolicy::Skip)
            .with_execution_timeout(per_probe_timeout)
            .with_all_writable_shards()
            .with_input(serde_json::json!({ "queue": queue }));
        builder = builder.workflow_schedule(ws);

        wf_names.push(wf_name);
    }

    // Aggressive self-cleaning retention override per canary workflow (AC9).
    let mut retention = builder.retention_config().clone();
    for wf_name in wf_names {
        retention = retention.with_workflow_override(wf_name, retention_age);
    }
    builder = builder.retention(retention);

    builder
}

// ── `GET /admin/canary` read model (issue #796) ─────────────────────────────
//
// This is the OBSERVE side of the synthetic liveness canary: it reconstructs,
// per `(queue, shard)`, the freshness of the built-in probe by reading the
// canary executions the scheduler has been firing. It never starts a probe and
// never mutates anything — the schedule + workflow (registered above) do the
// probing; this endpoint only reports the last-success age so an operator (or a
// staleness alert) can tell "the pipeline is live" from "queue X on shard Y has
// not completed a probe in far too long".
//
// Distinct from the #512 replay canary, whose `/admin/workflows/replay-canary`
// route validates *code changes* by replaying histories. This endpoint reports
// the *running pipeline*'s liveness.

/// Cross-shard completeness of a `GET /admin/canary` snapshot.
///
/// A type alias onto the shared [`FanoutStatus`] so the
/// complete/partial/unavailable derivation rule lives in exactly one place,
/// alongside `workflow_count`'s and `schedule_runs`' reports.
pub type CanaryReportStatus = FanoutStatus;

/// Terminal states counted as a canary *failure* (a completed-but-not-succeeded
/// probe). `RUNNING`/`SUSPENDED`/`PAUSED` are in-flight and counted as neither.
const CANARY_FAILURE_STATES: &[&str] = &["FAILED", "TIMED_OUT", "CANCELLED", "TERMINATED"];

/// One raw canary execution row read from a single shard.
///
/// Deliberately minimal — only the columns the freshness merge needs. Payloads
/// are never read (a canary probe carries none of interest).
#[derive(Debug, Clone)]
pub struct CanaryRow {
    /// The probe's target queue (`harvest_workflow_executions.queue_name`).
    pub queue_name: String,
    /// Terminal or in-flight state string.
    pub state: String,
    /// When the probe run started.
    pub started_at: DateTime<Utc>,
    /// When the probe run reached a terminal state (`NULL` while in flight).
    pub completed_at: Option<DateTime<Utc>>,
}

/// Freshness of one `(queue, shard)` synthetic-liveness probe (issue #796).
#[derive(Debug, Clone, Serialize)]
pub struct CanaryProbeStatus {
    /// The probe's target queue.
    pub queue: String,
    /// The shard this probe ran on.
    pub shard: i32,
    /// `completed_at` of the most recent COMPLETED probe run, or `None` if this
    /// `(queue, shard)` has never had a probe complete (AC4/AC7 — an empty or
    /// write-blocked shard reports `None` here and `stale: true`).
    pub last_success_at: Option<DateTime<Utc>>,
    /// Seconds since [`Self::last_success_at`] as of the snapshot, or `None`
    /// when there has been no success.
    pub age_of_last_success_secs: Option<i64>,
    /// Wall-clock `completed_at - started_at` of the most recent COMPLETED probe
    /// (its round-trip latency), or `None` when there has been no success.
    pub last_roundtrip_ms: Option<i64>,
    /// Count of COMPLETED probe runs in the retained (rolling) window.
    pub success_count: i64,
    /// Count of terminal non-completed probe runs (FAILED/TIMED_OUT/CANCELLED/
    /// TERMINATED) in the retained window.
    pub failure_count: i64,
    /// `true` when the last success is older than the staleness window (or there
    /// has been no success at all) — the alertable liveness signal (AC7).
    pub stale: bool,
}

/// The full `GET /admin/canary` freshness report (issue #796).
#[derive(Debug, Clone, Serialize)]
pub struct CanaryReport {
    /// Cross-shard completeness of this snapshot.
    pub status: CanaryReportStatus,
    /// When the snapshot was assembled.
    pub as_of: DateTime<Utc>,
    /// The staleness window (seconds) each probe's freshness was judged against.
    pub staleness_window_secs: i64,
    /// Per-`(queue, shard)` probe freshness, sorted by `(queue, shard)`.
    pub probes: Vec<CanaryProbeStatus>,
    /// Shards that could not be queried, named with a reason. Never silently
    /// dropped; drives `status = partial`/`unavailable`.
    pub unavailable_shards: Vec<UnavailableShard>,
}

impl CanaryReport {
    /// An empty, complete report — used when the canary is disabled (no config
    /// mirrored onto the runtime). The endpoint still answers `200`, it just
    /// reports nothing (AC1: disabled ⇒ no probes).
    fn empty(as_of: DateTime<Utc>) -> Self {
        Self {
            status: FanoutStatus::Complete,
            as_of,
            staleness_window_secs: 0,
            probes: Vec::new(),
            unavailable_shards: Vec::new(),
        }
    }
}

/// Merge per-shard canary observations into the freshness report (pure, no DB).
///
/// The probe grid is `expected` (the configured queues × the router's writable
/// shards) **unioned** with any `(queue, shard)` pairs actually found in the
/// data — so a queue recently removed from config still surfaces its stale
/// leftover rows, and a configured `(queue, shard)` with zero rows still
/// surfaces as a probe with `last_success_at: None, stale: true` (AC4).
///
/// For a reachable shard, each probe's freshness is derived from that shard's
/// rows filtered to the probe's queue: `last_success` is the newest COMPLETED
/// row (`last_success_at`/`last_roundtrip_ms`/`age`), `success_count` counts
/// COMPLETED, `failure_count` counts terminal-non-completed. `stale` is true
/// when there has been no success or the last success is older than
/// `staleness_window`. Because canary history is retention-bounded (AC9),
/// counting all present rows *is* the rolling window.
///
/// For an unreachable shard, every expected probe on it is reported stale with
/// `last_success_at: None`, and the shard is named in `unavailable_shards`, so
/// a write-blocked shard is never mistaken for healthy (AC7) and `status` drops
/// to `partial`/`unavailable` — the call never fails wholesale.
#[must_use]
pub fn build_canary_report(
    expected: &[(String, i32)],
    observations: &[ShardObservation<CanaryRow>],
    staleness_window: Duration,
    now: DateTime<Utc>,
) -> CanaryReport {
    let (inspected, unavailable_shards) = shard_fanout::summarize_shard_errors(observations);
    let unreachable: BTreeSet<i32> = unavailable_shards.iter().map(|u| u.shard_id).collect();

    // Rows from every reachable shard (a shard with `error: None` was inspected,
    // even if it returned zero rows).
    let mut rows_by_shard: BTreeMap<i32, &Vec<CanaryRow>> = BTreeMap::new();
    for observation in observations {
        if observation.error.is_none() {
            rows_by_shard.insert(observation.shard_id, &observation.rows);
        }
    }

    // Grid = expected ∪ observed (surface stale leftovers on shards/queues that
    // are no longer configured but still have canary rows).
    let mut grid: BTreeSet<(String, i32)> = expected.iter().cloned().collect();
    for (shard, rows) in &rows_by_shard {
        for row in rows.iter() {
            grid.insert((row.queue_name.clone(), *shard));
        }
    }

    let window_secs = i64::try_from(staleness_window.as_secs()).unwrap_or(i64::MAX);

    let mut probes: Vec<CanaryProbeStatus> = grid
        .into_iter()
        .map(|(queue, shard)| {
            // Unreachable, or a shard that was never inspected at all (e.g. a
            // writable shard with no pool wired up yet): no data ⇒ stale.
            let Some(rows) = rows_by_shard.get(&shard) else {
                return stale_probe(queue, shard);
            };
            if unreachable.contains(&shard) {
                return stale_probe(queue, shard);
            }

            let mut success_count = 0i64;
            let mut failure_count = 0i64;
            let mut last_success: Option<&CanaryRow> = None;
            for row in rows.iter().filter(|r| r.queue_name == queue) {
                if row.state == "COMPLETED" {
                    success_count += 1;
                    if let Some(completed) = row.completed_at {
                        let newer = last_success
                            .and_then(|prev| prev.completed_at)
                            .is_none_or(|prev_completed| completed > prev_completed);
                        if newer {
                            last_success = Some(row);
                        }
                    }
                } else if CANARY_FAILURE_STATES.contains(&row.state.as_str()) {
                    failure_count += 1;
                }
            }

            let (last_success_at, age, latency) = match last_success.and_then(|r| {
                r.completed_at.map(|completed| (completed, r.started_at))
            }) {
                Some((completed, started)) => {
                    let age = shard_fanout::age_secs(now, completed);
                    let latency = completed
                        .signed_duration_since(started)
                        .num_milliseconds()
                        .max(0);
                    (Some(completed), Some(age), Some(latency))
                }
                None => (None, None, None),
            };

            let stale = age.is_none_or(|a| a > window_secs);
            CanaryProbeStatus {
                queue,
                shard,
                last_success_at,
                age_of_last_success_secs: age,
                last_roundtrip_ms: latency,
                success_count,
                failure_count,
                stale,
            }
        })
        .collect();

    probes.sort_by(|a, b| a.queue.cmp(&b.queue).then_with(|| a.shard.cmp(&b.shard)));

    CanaryReport {
        status: FanoutStatus::from_counts(inspected, unavailable_shards.len()),
        as_of: now,
        staleness_window_secs: window_secs,
        probes,
        unavailable_shards,
    }
}

/// A probe with no observed success — used for an unreachable or un-inspected
/// `(queue, shard)`.
fn stale_probe(queue: String, shard: i32) -> CanaryProbeStatus {
    CanaryProbeStatus {
        queue,
        shard,
        last_success_at: None,
        age_of_last_success_secs: None,
        last_roundtrip_ms: None,
        success_count: 0,
        failure_count: 0,
        stale: true,
    }
}

/// Per-shard canary rows read from the DB (`QueryableByName`).
#[derive(Debug, diesel::QueryableByName)]
struct CanaryRowDb {
    #[diesel(sql_type = Text)]
    queue_name: String,
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Timestamptz)]
    started_at: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    completed_at: Option<DateTime<Utc>>,
}

/// Bounded upper limit on canary rows read per shard.
///
/// Canary history is aggressively retention-bounded (AC9), so a healthy shard
/// holds only a handful of recent probe runs; this cap is a defensive ceiling
/// so a mis-configured (never-cleaned) shard can never return an unbounded row
/// set to the freshness merge. Ordered newest-first so the most recent success
/// is always within the window.
const CANARY_ROW_CAP: i64 = 500;

/// Load this shard's canary execution rows (issue #796).
///
/// Selects only rows whose `workflow_name` carries the reserved canary prefix
/// (`starts_with`, matching the fleet-count exclusion seam byte-for-byte — a
/// prefix predicate, not a `LIKE` whose `_` wildcards would over-match the
/// underscore-laden prefix).
async fn load_canary_rows(conn: &mut AsyncPgConnection) -> Result<Vec<CanaryRow>, String> {
    let rows: Vec<CanaryRowDb> = diesel::sql_query(
        "SELECT queue_name, state, started_at, completed_at \
         FROM harvest_workflow_executions \
         WHERE starts_with(workflow_name, $1) \
         ORDER BY completed_at DESC NULLS LAST \
         LIMIT $2",
    )
    .bind::<Text, _>(autumn_harvest::canary::CANARY_WORKFLOW_NAME_PREFIX)
    .bind::<diesel::sql_types::BigInt, _>(CANARY_ROW_CAP)
    .load(conn)
    .await
    .map_err(|e| e.to_string())?;

    Ok(rows
        .into_iter()
        .map(|r| CanaryRow {
            queue_name: r.queue_name,
            state: r.state,
            started_at: r.started_at,
            completed_at: r.completed_at,
        })
        .collect())
}

/// Query one shard for its canary rows (issue #796).
///
/// Returns a [`ShardObservation`] rather than propagating errors so a single
/// unreachable shard never fails the whole fan-out — the merge folds it into
/// `unavailable_shards` and a `partial`/`unavailable` status instead.
async fn observe_canary_shard(shard_id: i32, pool: Option<DbPool>) -> ShardObservation<CanaryRow> {
    let Some(pool) = pool else {
        return ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(format!("shard {shard_id} has no configured storage pool")),
        };
    };
    let Ok(mut conn) = pool.get().await else {
        return ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(format!(
                "database connection for shard {shard_id} could not be acquired"
            )),
        };
    };
    match load_canary_rows(&mut conn).await {
        Ok(rows) => ShardObservation {
            shard_id,
            rows,
            error: None,
        },
        Err(e) => ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(e),
        },
    }
}

/// Build the synthetic-liveness-canary freshness report for `GET /admin/canary`
/// (issue #796).
///
/// The probe grid is the configured queues × the router's **writable** shards
/// (AC4): every writable shard must run its own probe loop, so a write-blocked
/// shard with zero completed rows surfaces as a stale probe rather than being
/// silently absent. The fan-out set unions that writable-shard grid with
/// [`shard_fanout::expected_shards`] (pools + readable + default) so a shard
/// mid-rollout, or one holding stale leftover probes for a de-configured queue,
/// is still inspected. All shards are queried concurrently (`join_all`).
///
/// Never fails outright: an unreachable shard surfaces as `status:
/// partial`/`unavailable` with a named `unavailable_shards` entry. When the
/// canary is disabled (no config mirrored onto the runtime) an empty `complete`
/// report is returned — the endpoint still works, it just reports nothing (AC1).
pub async fn build_canary_report_from_shards(api_state: &HarvestApiState) -> CanaryReport {
    let now = Utc::now();

    let Some(cfg) = api_state.canary_config() else {
        return CanaryReport::empty(now);
    };
    let staleness_window = cfg.effective_staleness_window();

    // The router's writable shards drive the AC4 grid. A single-shard (or
    // not-yet-started) router falls back to its default shard.
    let writable: Vec<i32> = api_state.runtime().ok().map_or_else(
        || vec![0],
        |runtime| {
            let router = runtime.router();
            let shards: Vec<i32> = router.writable_shards().iter().map(|s| s.as_i32()).collect();
            if shards.is_empty() {
                vec![router.default_shard().as_i32()]
            } else {
                shards
            }
        },
    );

    // Expected grid = queues × writable shards.
    let mut expected: Vec<(String, i32)> = Vec::new();
    for queue in cfg.queues() {
        for shard in &writable {
            expected.push((queue.clone(), *shard));
        }
    }

    // Fan-out set = expected_shards (pools + readable + default) ∪ writable, so
    // every writable shard is inspected even when it has no pool wired up yet
    // (reported unavailable), and any leftover-probe shard is included too.
    let pools = shard_fanout::pools_by_shard(api_state);
    let mut fanout: BTreeSet<i32> = shard_fanout::expected_shards(api_state, &pools);
    fanout.extend(writable.iter().copied());

    let observations = fanout
        .iter()
        .map(|shard_id| observe_canary_shard(*shard_id, pools.get(shard_id).cloned()))
        .collect::<Vec<_>>();
    let observations = join_all(observations).await;

    build_canary_report(&expected, &observations, staleness_window, now)
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

    fn canary_schedule<'a>(
        builder: &'a HarvestBuilder,
        wf_name: &str,
    ) -> &'a autumn_harvest::WorkflowSchedule {
        builder
            .workflow_schedules()
            .iter()
            .find(|s| s.workflow_name == wf_name)
            .unwrap_or_else(|| panic!("expected a schedule for {wf_name}"))
    }

    #[test]
    fn register_canary_single_queue() {
        let cfg = CanaryConfig::new(Duration::from_secs(30));
        let builder = register_canary(HarvestBuilder::default(), &cfg);

        // Exactly one canary workflow, named for the default queue.
        let canary_wfs: Vec<&str> = builder
            .workflow_infos()
            .iter()
            .map(|w| w.name)
            .filter(|n| autumn_harvest::canary::is_canary_workflow(n))
            .collect();
        assert_eq!(canary_wfs, vec!["__harvest_canary_probe__default"]);

        // The reserved activity is registered exactly once.
        let activity_count = builder
            .activity_infos()
            .iter()
            .filter(|a| a.name == autumn_harvest::canary::CANARY_ACTIVITY_NAME)
            .count();
        assert_eq!(activity_count, 1);

        // Exactly one schedule, per-writable-shard, Skip overlap, timeout,
        // queue routed.
        let canary_sched_count = builder
            .workflow_schedules()
            .iter()
            .filter(|s| autumn_harvest::canary::is_canary_workflow(&s.workflow_name))
            .count();
        assert_eq!(canary_sched_count, 1);
        let s = canary_schedule(&builder, "__harvest_canary_probe__default");
        assert!(s.all_writable_shards, "AC4: per-writable-shard coverage");
        assert!(matches!(s.overlap_policy, OverlapPolicy::Skip), "AC6");
        assert_eq!(s.execution_timeout, Some(cfg.per_probe_timeout()));
        assert_eq!(s.queue_name, "default");

        // The probe queue is present in the worker's queue set.
        assert!(
            builder
                .worker_config()
                .queues
                .iter()
                .any(|q| q == "default")
        );

        // A retention override exists for the canary workflow (AC9).
        assert!(
            builder
                .retention_config()
                .workflow_overrides()
                .contains_key("__harvest_canary_probe__default")
        );
    }

    #[test]
    fn register_canary_multi_queue() {
        let cfg = CanaryConfig::new(Duration::from_secs(30))
            .with_queues(vec!["default".to_string(), "email".to_string()]);
        let builder = register_canary(HarvestBuilder::default(), &cfg);

        let mut canary_wfs: Vec<&str> = builder
            .workflow_infos()
            .iter()
            .map(|w| w.name)
            .filter(|n| autumn_harvest::canary::is_canary_workflow(n))
            .collect();
        canary_wfs.sort_unstable();
        assert_eq!(
            canary_wfs,
            vec![
                "__harvest_canary_probe__default",
                "__harvest_canary_probe__email"
            ]
        );

        // Two schedules, one per queue, each per-writable-shard.
        let canary_scheds: Vec<&autumn_harvest::WorkflowSchedule> = builder
            .workflow_schedules()
            .iter()
            .filter(|s| autumn_harvest::canary::is_canary_workflow(&s.workflow_name))
            .collect();
        assert_eq!(canary_scheds.len(), 2);
        assert!(canary_scheds.iter().all(|s| s.all_writable_shards));

        // Both queues in the worker queue set.
        assert!(
            builder
                .worker_config()
                .queues
                .iter()
                .any(|q| q == "default")
        );
        assert!(builder.worker_config().queues.iter().any(|q| q == "email"));

        // Retention overrides for both.
        let overrides = builder.retention_config().workflow_overrides();
        assert!(overrides.contains_key("__harvest_canary_probe__default"));
        assert!(overrides.contains_key("__harvest_canary_probe__email"));

        // The activity is still registered exactly once across queues.
        let activity_count = builder
            .activity_infos()
            .iter()
            .filter(|a| a.name == autumn_harvest::canary::CANARY_ACTIVITY_NAME)
            .count();
        assert_eq!(activity_count, 1);
    }

    #[test]
    fn register_canary_dedupes_repeated_queues() {
        // A user double-adding the same queue must not register two probes.
        let cfg = CanaryConfig::new(Duration::from_secs(30))
            .with_queues(vec!["default".to_string()])
            .with_queue("default".to_string());
        let builder = register_canary(HarvestBuilder::default(), &cfg);
        let canary_scheds = builder
            .workflow_schedules()
            .iter()
            .filter(|s| autumn_harvest::canary::is_canary_workflow(&s.workflow_name))
            .count();
        assert_eq!(canary_scheds, 1);
    }

    #[test]
    fn register_canary_output_passes_try_build() {
        // The registered canary must produce a buildable configuration —
        // schedule references a registered workflow, retention override names a
        // registered workflow, and the retention age is within validator
        // bounds (AC9 self-clean mechanism confirmed end to end).
        let cfg = CanaryConfig::new(Duration::from_secs(30))
            .with_queues(vec!["default".to_string(), "email".to_string()]);
        let builder = register_canary(HarvestBuilder::default(), &cfg);
        let built = builder.try_build();
        assert!(
            built.is_ok(),
            "register_canary output must build: {built:?}"
        );
    }

    // ── build_canary_report (pure read-model, no DB) ─────────────────────────

    use chrono::TimeZone as _;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

    fn row(
        queue: &str,
        state: &str,
        started: i64,
        completed: Option<i64>,
    ) -> CanaryRow {
        CanaryRow {
            queue_name: queue.to_string(),
            state: state.to_string(),
            started_at: at(started),
            completed_at: completed.map(at),
        }
    }

    fn obs(shard_id: i32, rows: Vec<CanaryRow>, error: Option<&str>) -> ShardObservation<CanaryRow> {
        ShardObservation {
            shard_id,
            rows,
            error: error.map(ToOwned::to_owned),
        }
    }

    fn probe<'a>(report: &'a CanaryReport, queue: &str, shard: i32) -> &'a CanaryProbeStatus {
        report
            .probes
            .iter()
            .find(|p| p.queue == queue && p.shard == shard)
            .unwrap_or_else(|| panic!("expected a probe for ({queue}, {shard})"))
    }

    #[test]
    fn healthy_probe_is_not_stale_with_latency_and_counts() {
        // Newest COMPLETED probe finished at t=1000 (started 999 → 1000ms
        // roundtrip); observed at t=1010 with a 60s window ⇒ fresh.
        let now = at(1010);
        let rows = vec![
            row("default", "COMPLETED", 990, Some(991)),
            row("default", "COMPLETED", 999, Some(1000)),
        ];
        let report = build_canary_report(
            &[("default".to_string(), 0)],
            &[obs(0, rows, None)],
            Duration::from_secs(60),
            now,
        );
        assert_eq!(report.status, CanaryReportStatus::Complete);
        assert_eq!(report.staleness_window_secs, 60);
        assert_eq!(report.probes.len(), 1);
        let p = probe(&report, "default", 0);
        assert!(!p.stale);
        assert_eq!(p.last_success_at, Some(at(1000)));
        assert_eq!(p.age_of_last_success_secs, Some(10));
        assert_eq!(p.last_roundtrip_ms, Some(1000));
        assert_eq!(p.success_count, 2);
        assert_eq!(p.failure_count, 0);
    }

    #[test]
    fn old_success_is_stale() {
        // Last success at t=100; observed at t=1000 with a 60s window ⇒ stale.
        let now = at(1000);
        let rows = vec![row("default", "COMPLETED", 99, Some(100))];
        let report = build_canary_report(
            &[("default".to_string(), 0)],
            &[obs(0, rows, None)],
            Duration::from_secs(60),
            now,
        );
        let p = probe(&report, "default", 0);
        assert!(p.stale);
        assert_eq!(p.age_of_last_success_secs, Some(900));
        assert_eq!(p.success_count, 1);
    }

    #[test]
    fn never_succeeded_probe_is_stale_with_no_success_and_failure_count() {
        // Only FAILED/TIMED_OUT rows ⇒ stale, no last success, failure_count > 0.
        let now = at(1000);
        let rows = vec![
            row("default", "FAILED", 10, Some(11)),
            row("default", "TIMED_OUT", 20, Some(21)),
        ];
        let report = build_canary_report(
            &[("default".to_string(), 0)],
            &[obs(0, rows, None)],
            Duration::from_secs(60),
            now,
        );
        let p = probe(&report, "default", 0);
        assert!(p.stale);
        assert!(p.last_success_at.is_none());
        assert!(p.age_of_last_success_secs.is_none());
        assert!(p.last_roundtrip_ms.is_none());
        assert_eq!(p.success_count, 0);
        assert_eq!(p.failure_count, 2);
    }

    #[test]
    fn expected_shard_with_zero_rows_is_present_and_stale() {
        // AC4: a configured (queue, shard) with NO rows at all (e.g. a
        // write-blocked shard that never completed a probe) must still appear as
        // a stale probe, not vanish.
        let now = at(1000);
        let report = build_canary_report(
            &[("default".to_string(), 0)],
            &[obs(0, Vec::new(), None)],
            Duration::from_secs(60),
            now,
        );
        assert_eq!(report.status, CanaryReportStatus::Complete);
        assert_eq!(report.probes.len(), 1);
        let p = probe(&report, "default", 0);
        assert!(p.stale, "AC4: empty shard probe must be stale");
        assert!(p.last_success_at.is_none());
        assert_eq!(p.success_count, 0);
        assert_eq!(p.failure_count, 0);
    }

    #[test]
    fn unreachable_shard_probes_are_stale_and_shard_is_named_partial() {
        // AC7: an unreachable shard's expected probes are stale AND the shard is
        // named in unavailable_shards AND status is partial (one shard OK).
        let now = at(1000);
        let report = build_canary_report(
            &[("default".to_string(), 0), ("default".to_string(), 1)],
            &[
                obs(0, vec![row("default", "COMPLETED", 999, Some(1000))], None),
                obs(1, Vec::new(), Some("connection refused")),
            ],
            Duration::from_secs(60),
            now,
        );
        assert_eq!(report.status, CanaryReportStatus::Partial);
        // Shard 0 is fresh, shard 1 (unreachable) is stale.
        assert!(!probe(&report, "default", 0).stale);
        let down = probe(&report, "default", 1);
        assert!(down.stale);
        assert!(down.last_success_at.is_none());
        // Shard 1 is named, never silently dropped.
        assert_eq!(report.unavailable_shards.len(), 1);
        assert_eq!(report.unavailable_shards[0].shard_id, 1);
        assert_eq!(report.unavailable_shards[0].reason, "connection refused");
    }

    #[test]
    fn all_shards_unavailable_is_unavailable_not_a_hard_failure() {
        let now = at(1000);
        let report = build_canary_report(
            &[("default".to_string(), 0)],
            &[obs(0, Vec::new(), Some("pool missing"))],
            Duration::from_secs(60),
            now,
        );
        assert_eq!(report.status, CanaryReportStatus::Unavailable);
        assert_eq!(report.probes.len(), 1);
        assert!(probe(&report, "default", 0).stale);
        assert_eq!(report.unavailable_shards.len(), 1);
    }

    #[test]
    fn mixed_queues_and_shards_grid_with_leftover_row() {
        // Grid = expected (default×{0,1}, email×{0,1}) ∪ observed. Shard 0 has a
        // fresh `default` probe plus a leftover `legacy` queue row (de-configured
        // but still present); shard 1 has a stale `email` probe. All appear.
        let now = at(1000);
        let report = build_canary_report(
            &[
                ("default".to_string(), 0),
                ("default".to_string(), 1),
                ("email".to_string(), 0),
                ("email".to_string(), 1),
            ],
            &[
                obs(
                    0,
                    vec![
                        row("default", "COMPLETED", 999, Some(1000)),
                        row("legacy", "COMPLETED", 500, Some(501)),
                    ],
                    None,
                ),
                obs(1, vec![row("email", "COMPLETED", 100, Some(101))], None),
            ],
            Duration::from_secs(60),
            now,
        );
        assert_eq!(report.status, CanaryReportStatus::Complete);
        // default/0 fresh; default/1 empty→stale; email/0 empty→stale; email/1 old→stale.
        assert!(!probe(&report, "default", 0).stale);
        assert!(probe(&report, "default", 1).stale);
        assert!(probe(&report, "email", 0).stale);
        assert!(probe(&report, "email", 1).stale);
        // The de-configured `legacy` leftover surfaces (observed ∪ expected).
        let legacy = probe(&report, "legacy", 0);
        assert!(legacy.stale, "leftover is old (age 499 > 60s window)");
        assert_eq!(legacy.success_count, 1);
        // Probes are sorted by (queue, shard).
        let order: Vec<(&str, i32)> = report
            .probes
            .iter()
            .map(|p| (p.queue.as_str(), p.shard))
            .collect();
        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(order, sorted);
    }

    #[test]
    fn report_status_serializes_snake_case() {
        let report = CanaryReport::empty(at(0));
        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(value["status"], serde_json::json!("complete"));
        assert!(value["probes"].as_array().unwrap().is_empty());
        assert_eq!(value["staleness_window_secs"], serde_json::json!(0));
    }
}
