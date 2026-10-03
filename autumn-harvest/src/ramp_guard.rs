//! Metric-gated automatic abort of a build ramp (issue #1814).
//!
//! A ramp (issue #604) sends a share of new starts to a target build. The
//! guard compares the target build with the base build during the current
//! ramp step. When the target build fails or ND-blocks too many more runs, the
//! guard clears the ramp and writes an audit row.
//!
//! The counts come from `harvest_workflow_executions`, not from a metrics
//! backend, so every replica sees the same fleet-wide data.
//! [`evaluate`] is the pure verdict. [`guard_once`] is one pass over the
//! database. [`run_ramp_guard`] is the loop.
//!
//! `docs/operations/build-ramp-guard.md` is the specification.

use std::time::Duration;

/// The default time between two guard passes.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);
/// The shortest accepted time between two guard passes.
pub const MIN_INTERVAL: Duration = Duration::from_secs(1);
/// The longest accepted time between two guard passes.
pub const MAX_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// The default minimum number of target-build runs for a verdict.
pub const DEFAULT_MIN_SAMPLES: u64 = 20;
/// The default maximum increase of the failure rate over the base build.
pub const DEFAULT_MAX_FAILURE_RATE_INCREASE: f64 = 0.05;
/// The default maximum increase of the ND-block rate over the base build.
pub const DEFAULT_MAX_ND_BLOCK_RATE_INCREASE: f64 = 0.05;

/// The settings of the ramp guard.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RampGuardConfig {
    enabled: bool,
    interval: Duration,
    min_samples: u64,
    max_failure_rate_increase: f64,
    max_nd_block_rate_increase: f64,
}

impl Default for RampGuardConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: DEFAULT_INTERVAL,
            min_samples: DEFAULT_MIN_SAMPLES,
            max_failure_rate_increase: DEFAULT_MAX_FAILURE_RATE_INCREASE,
            max_nd_block_rate_increase: DEFAULT_MAX_ND_BLOCK_RATE_INCREASE,
        }
    }
}

impl RampGuardConfig {
    /// An enabled guard with the default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }

    /// Set the time between two passes.
    ///
    /// The value is clamped to the range from [`MIN_INTERVAL`] to
    /// [`MAX_INTERVAL`]. The loop adds the interval to the clock, so the bound
    /// keeps every deadline finite.
    #[must_use]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval.clamp(MIN_INTERVAL, MAX_INTERVAL);
        self
    }

    /// Set the minimum number of target-build runs for a verdict.
    ///
    /// A value of 0 becomes 1.
    #[must_use]
    pub fn with_min_samples(mut self, min_samples: u64) -> Self {
        self.min_samples = min_samples.max(1);
        self
    }

    /// Set the maximum increase of the failure rate over the base build.
    ///
    /// The value is clamped to `0.0..=1.0`. `NaN` keeps the current value.
    #[must_use]
    pub fn with_max_failure_rate_increase(mut self, increase: f64) -> Self {
        self.max_failure_rate_increase = clamp_rate(increase, self.max_failure_rate_increase);
        self
    }

    /// Set the maximum increase of the ND-block rate over the base build.
    ///
    /// The value is clamped to `0.0..=1.0`. `NaN` keeps the current value.
    #[must_use]
    pub fn with_max_nd_block_rate_increase(mut self, increase: f64) -> Self {
        self.max_nd_block_rate_increase = clamp_rate(increase, self.max_nd_block_rate_increase);
        self
    }

    /// `true` when the guard runs.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// The time between two passes.
    #[must_use]
    pub const fn interval(&self) -> Duration {
        self.interval
    }

    /// The minimum number of target-build runs for a verdict.
    #[must_use]
    pub const fn min_samples(&self) -> u64 {
        self.min_samples
    }

    /// The maximum increase of the failure rate over the base build.
    #[must_use]
    pub const fn max_failure_rate_increase(&self) -> f64 {
        self.max_failure_rate_increase
    }

    /// The maximum increase of the ND-block rate over the base build.
    #[must_use]
    pub const fn max_nd_block_rate_increase(&self) -> f64 {
        self.max_nd_block_rate_increase
    }
}

/// `value` clamped to `0.0..=1.0`, or `current` when `value` is `NaN`.
fn clamp_rate(value: f64, current: f64) -> f64 {
    if value.is_nan() {
        current
    } else {
        value.clamp(0.0, 1.0)
    }
}

/// The outcome counts of one build on one queue during one ramp step.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BuildOutcomeStats {
    /// Runs that started during the step.
    pub started: u64,
    /// Runs that completed.
    pub completed: u64,
    /// Runs that failed or timed out.
    pub failed: u64,
    /// Runs that are blocked on replay non-determinism now.
    pub nd_blocked: u64,
}

impl BuildOutcomeStats {
    /// Runs with a success or failure outcome.
    #[must_use]
    pub const fn settled(&self) -> u64 {
        self.completed.saturating_add(self.failed)
    }

    /// `failed / settled`, or 0 when no run settled.
    #[must_use]
    pub fn failure_rate(&self) -> f64 {
        ratio(self.failed, self.settled())
    }

    /// `nd_blocked / started`, or 0 when no run started.
    #[must_use]
    pub fn nd_block_rate(&self) -> f64 {
        ratio(self.nd_blocked, self.started)
    }

    /// Add the counts of `other`, for the merge of shard pools.
    #[must_use]
    pub const fn plus(self, other: Self) -> Self {
        Self {
            started: self.started.saturating_add(other.started),
            completed: self.completed.saturating_add(other.completed),
            failed: self.failed.saturating_add(other.failed),
            nd_blocked: self.nd_blocked.saturating_add(other.nd_blocked),
        }
    }
}

/// `numerator / denominator`, or 0 when `denominator` is 0.
#[allow(clippy::cast_precision_loss)]
fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        // The counts are run counts, far below 2^52, so the cast is exact.
        numerator as f64 / denominator as f64
    }
}

/// Why the guard aborted a ramp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RampAbortReason {
    /// The target build fails too many runs.
    FailureRate,
    /// The target build blocks too many runs on replay non-determinism.
    NdBlockRate,
}

impl RampAbortReason {
    /// The stable label and audit value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FailureRate => "failure_rate",
            Self::NdBlockRate => "nd_block_rate",
        }
    }
}

/// The verdict of one ramp check.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RampVerdict {
    /// The target build has too few runs for a verdict.
    InsufficientData,
    /// The target build is within the thresholds.
    Healthy,
    /// The target build exceeds a threshold.
    Abort {
        /// The rate that exceeds its threshold.
        reason: RampAbortReason,
        /// That rate on the base build.
        base_rate: f64,
        /// That rate on the target build.
        target_rate: f64,
    },
}

/// Compare the target build with the base build.
///
/// The failure rate counts only settled runs. The ND-block rate counts all
/// started runs, because a blocked run never settles. Each check needs
/// `min_samples` target runs in its own denominator. A rate aborts only when
/// it is more than its threshold above the same rate on the base build. The
/// failure check runs first.
#[must_use]
pub fn evaluate(
    base: &BuildOutcomeStats,
    target: &BuildOutcomeStats,
    config: &RampGuardConfig,
) -> RampVerdict {
    let failure_judged = target.settled() >= config.min_samples;
    let nd_judged = target.started >= config.min_samples;
    if failure_judged {
        let (base_rate, target_rate) = (base.failure_rate(), target.failure_rate());
        if target_rate - base_rate > config.max_failure_rate_increase {
            return RampVerdict::Abort {
                reason: RampAbortReason::FailureRate,
                base_rate,
                target_rate,
            };
        }
    }
    if nd_judged {
        let (base_rate, target_rate) = (base.nd_block_rate(), target.nd_block_rate());
        if target_rate - base_rate > config.max_nd_block_rate_increase {
            return RampVerdict::Abort {
                reason: RampAbortReason::NdBlockRate,
                base_rate,
                target_rate,
            };
        }
    }
    if failure_judged || nd_judged {
        RampVerdict::Healthy
    } else {
        RampVerdict::InsufficientData
    }
}

/// One ramp that a guard pass aborted.
#[derive(Debug, Clone, PartialEq)]
pub struct RampAbort {
    /// The queue of the ramp.
    pub queue: String,
    /// The base build that keeps all new starts.
    pub base_build_id: String,
    /// The target build that lost its share.
    pub target_build_id: String,
    /// The ramp percentage before the abort.
    pub ramp_percent: i32,
    /// Why the guard aborted the ramp.
    pub reason: RampAbortReason,
    /// The compared rate on the base build.
    pub base_rate: f64,
    /// The compared rate on the target build.
    pub target_rate: f64,
    /// The outcome counts of the target build.
    pub target: BuildOutcomeStats,
}

// ── Database pass ─────────────────────────────────────────────────────────────

/// SQL for the outcome counts of the two builds of one ramp step.
///
/// The binds are the queue, the base build, the target build, the step
/// start and the canary-probe name prefix. Exposed for shape tests.
#[must_use]
pub const fn ramp_outcome_stats_query() -> &'static str {
    "SELECT assigned_build_id AS build_id, \
         COUNT(*) AS started, \
         COUNT(*) FILTER (WHERE state = 'COMPLETED') AS completed, \
         COUNT(*) FILTER (WHERE state IN ('FAILED', 'TIMED_OUT')) AS failed, \
         COUNT(*) FILTER ( \
             WHERE state = 'RUNNING' AND nd_blocked_at IS NOT NULL \
         ) AS nd_blocked \
     FROM harvest_workflow_executions \
     WHERE queue_name = $1 \
       AND assigned_build_id IN ($2, $3) \
       AND created_at >= $4 \
       AND NOT starts_with(workflow_name, $5) \
     GROUP BY assigned_build_id"
}

/// SQL for the compare-and-swap clear of one ramp. Exposed for shape tests.
#[must_use]
pub const fn abort_ramp_query() -> &'static str {
    "UPDATE harvest_build_policies \
     SET target_build_id = NULL, ramp_percent = NULL, updated_at = NOW() \
     WHERE queue_name = $1 AND build_id = $2 AND target_build_id = $3"
}

/// The audit actor of an abort row.
pub const AUDIT_ACTOR: &str = "system";

/// The audit route of an abort row.
pub const AUDIT_ROUTE: &str = "background.ramp_guard";

/// Clear the ramp of `queue` when it still ramps `base` to `target`.
///
/// The clear is a compare-and-swap. A ramp that an operator moved to another
/// target, or a queue with another base build, stays. Returns `true` when
/// this call cleared the ramp.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn abort_ramp(
    conn: &mut diesel_async::AsyncPgConnection,
    queue: &str,
    base: &str,
    target: &str,
) -> crate::error::HarvestResult<bool> {
    use diesel::sql_types::Text;
    use diesel_async::RunQueryDsl;

    let changed = diesel::sql_query(abort_ramp_query())
        .bind::<Text, _>(queue)
        .bind::<Text, _>(base)
        .bind::<Text, _>(target)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(changed > 0)
}

/// The identity of one ramp: queue, base build and target build.
#[cfg(feature = "db")]
type RampKey = (String, String, String);

/// One ramp as one guard pass sees it, merged over the shard pools.
#[cfg(feature = "db")]
#[derive(Debug, Default)]
struct ObservedRamp {
    ramp_percent: i32,
    base: BuildOutcomeStats,
    target: BuildOutcomeStats,
}

/// Read the outcome counts of the two builds of one ramp step on one pool.
#[cfg(feature = "db")]
async fn read_step_stats(
    conn: &mut diesel_async::AsyncPgConnection,
    policy: &crate::build_routing::BuildPolicy,
    target: &str,
) -> crate::error::HarvestResult<(BuildOutcomeStats, BuildOutcomeStats)> {
    use diesel::sql_types::{BigInt, Text, Timestamptz};
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        build_id: String,
        #[diesel(sql_type = BigInt)]
        started: i64,
        #[diesel(sql_type = BigInt)]
        completed: i64,
        #[diesel(sql_type = BigInt)]
        failed: i64,
        #[diesel(sql_type = BigInt)]
        nd_blocked: i64,
    }

    let rows: Vec<Row> = diesel::sql_query(ramp_outcome_stats_query())
        .bind::<Text, _>(&policy.queue_name)
        .bind::<Text, _>(&policy.build_id)
        .bind::<Text, _>(target)
        .bind::<Timestamptz, _>(policy.updated_at)
        .bind::<Text, _>(crate::canary::CANARY_WORKFLOW_NAME_PREFIX)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let count = |n: i64| u64::try_from(n).unwrap_or(0);
    let (mut base, mut target_stats) = (BuildOutcomeStats::default(), BuildOutcomeStats::default());
    for row in rows {
        let stats = BuildOutcomeStats {
            started: count(row.started),
            completed: count(row.completed),
            failed: count(row.failed),
            nd_blocked: count(row.nd_blocked),
        };
        if row.build_id == target {
            target_stats = stats;
        } else {
            base = stats;
        }
    }
    Ok((base, target_stats))
}

/// Read every active ramp and its step counts on one pool.
#[cfg(feature = "db")]
async fn read_pool_ramps(
    pool: &crate::worker::DbPool,
) -> crate::error::HarvestResult<Vec<(RampKey, ObservedRamp)>> {
    let mut conn = pool.get().await.map_err(crate::error::database_error)?;
    let policies = crate::build_routing::list_build_policies(&mut conn).await?;
    let mut ramps = Vec::new();
    for policy in policies {
        let (Some(target), Some(percent)) = (policy.target_build_id.clone(), policy.ramp_percent)
        else {
            continue;
        };
        if percent <= 0 {
            continue;
        }
        let (base, target_stats) = read_step_stats(&mut conn, &policy, &target).await?;
        ramps.push((
            (policy.queue_name.clone(), policy.build_id.clone(), target),
            ObservedRamp {
                ramp_percent: percent,
                base,
                target: target_stats,
            },
        ));
    }
    Ok(ramps)
}

/// Read every active ramp on every pool and merge the counts per ramp.
///
/// The pools are read at the same time. Returns `None` when any read fails,
/// so a pass never decides on part of the fleet.
#[cfg(feature = "db")]
async fn read_ramps(
    pools: &[crate::worker::DbPool],
) -> Option<std::collections::BTreeMap<RampKey, ObservedRamp>> {
    let reads = pools.iter().map(read_pool_ramps);
    let mut merged: std::collections::BTreeMap<RampKey, ObservedRamp> =
        std::collections::BTreeMap::new();
    for result in futures::future::join_all(reads).await {
        let ramps = match result {
            Ok(ramps) => ramps,
            Err(error) => {
                tracing::warn!(error = %error, "ramp guard read failed; no verdict this pass");
                return None;
            }
        };
        for (key, ramp) in ramps {
            let slot = merged.entry(key).or_default();
            slot.ramp_percent = slot.ramp_percent.max(ramp.ramp_percent);
            slot.base = slot.base.plus(ramp.base);
            slot.target = slot.target.plus(ramp.target);
        }
    }
    Some(merged)
}

/// Clear one ramp on every pool. Returns the cleared row count and the errors.
#[cfg(feature = "db")]
async fn clear_on_pools(pools: &[crate::worker::DbPool], key: &RampKey) -> (usize, Vec<String>) {
    let (queue, base, target) = key;
    let mut cleared = 0;
    let mut errors = Vec::new();
    for pool in pools {
        let result = match pool.get().await {
            Ok(mut conn) => abort_ramp(&mut conn, queue, base, target)
                .await
                .map_err(|e| e.to_string()),
            Err(error) => Err(error.to_string()),
        };
        match result {
            Ok(true) => cleared += 1,
            Ok(false) => {}
            Err(error) => errors.push(error),
        }
    }
    (cleared, errors)
}

/// Write the audit row of one abort.
///
/// The write is best effort. A failed write logs a warning and does not undo
/// the abort.
#[cfg(feature = "db")]
async fn record_abort(audit_pool: &crate::worker::DbPool, abort: &RampAbort, errors: &[String]) {
    let mut summary = format!(
        "reason={} target_build={} base_build={} ramp_percent={} \
         target_rate={:.4} base_rate={:.4} target_started={} target_settled={}",
        abort.reason.as_str(),
        abort.target_build_id,
        abort.base_build_id,
        abort.ramp_percent,
        abort.target_rate,
        abort.base_rate,
        abort.target.started,
        abort.target.settled(),
    );
    if !errors.is_empty() {
        summary.push_str("; clear errors: ");
        summary.push_str(&errors.join("; "));
    }
    let status = if errors.is_empty() {
        crate::audit::STATUS_SUCCEEDED
    } else {
        crate::audit::STATUS_FAILED
    };
    let record = crate::models::NewAuditRecord {
        actor: AUDIT_ACTOR,
        operation: crate::audit::OP_BUILD_RAMP_AUTO_ABORT,
        target_type: crate::audit::TARGET_BUILD_ROUTING,
        target_id: Some(&abort.queue),
        route_or_command: AUDIT_ROUTE,
        request_id: None,
        idempotency_key: None,
        status,
        error_summary: Some(&summary),
        shard_id: None,
        // The table accepts only `api`, `cli` and `ui`. The guard runs in the
        // API process, and the actor and route mark the row as automatic.
        source: crate::audit::SOURCE_API,
    };
    let result = match audit_pool.get().await {
        Ok(mut conn) => crate::audit::insert_audit(&mut conn, &record)
            .await
            .map_err(|e| e.to_string()),
        Err(error) => Err(error.to_string()),
    };
    if let Err(error) = result {
        tracing::warn!(queue = %abort.queue, error = %error, "ramp guard audit write failed");
    }
}

/// Run one guard pass over every ramp in `pools`.
///
/// `pools` holds one pool per physical database. The pass reads every ramp
/// and its step counts, then aborts each ramp with an abort verdict. The reads
/// share one bound of one interval. A failed or slow read aborts nothing.
///
/// An abort counts only when this pass cleared the ramp on one pool or more.
/// For each such abort, the pass writes one audit row to `audit_pool`. It also
/// counts the abort on `metrics` and logs a warning. Returns those aborts.
#[cfg(feature = "db")]
pub async fn guard_once(
    pools: &[crate::worker::DbPool],
    audit_pool: &crate::worker::DbPool,
    config: &RampGuardConfig,
    metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
) -> Vec<RampAbort> {
    let Ok(read) = tokio::time::timeout(config.interval(), read_ramps(pools)).await else {
        tracing::warn!("ramp guard read timed out; no verdict this pass");
        return Vec::new();
    };
    let Some(ramps) = read else {
        return Vec::new();
    };

    let mut aborts = Vec::new();
    for (key, ramp) in ramps {
        let RampVerdict::Abort {
            reason,
            base_rate,
            target_rate,
        } = evaluate(&ramp.base, &ramp.target, config)
        else {
            continue;
        };
        let (cleared, errors) = clear_on_pools(pools, &key).await;
        if cleared == 0 {
            // Another replica or an operator changed the ramp first.
            for error in &errors {
                tracing::warn!(queue = %key.0, error = %error, "ramp guard clear failed");
            }
            continue;
        }
        let (queue, base_build_id, target_build_id) = key;
        let abort = RampAbort {
            queue,
            base_build_id,
            target_build_id,
            ramp_percent: ramp.ramp_percent,
            reason,
            base_rate,
            target_rate,
            target: ramp.target,
        };
        tracing::warn!(
            queue = %abort.queue,
            base_build = %abort.base_build_id,
            target_build = %abort.target_build_id,
            reason = abort.reason.as_str(),
            target_rate = abort.target_rate,
            base_rate = abort.base_rate,
            "ramp guard aborted a build ramp"
        );
        if let Some(m) = metrics {
            m.record_build_ramp_aborted(&abort.queue, abort.reason.as_str());
        }
        record_abort(audit_pool, &abort, &errors).await;
        aborts.push(abort);
    }
    aborts
}

/// Run guard passes every `config.interval()` until `cancel` fires.
///
/// Returns at once when the config is disabled. A pass is not cancelled
/// halfway, so an abort and its audit row are not split. An overdue tick
/// fires at once, and the next tick comes one interval after it.
#[cfg(feature = "db")]
pub async fn run_ramp_guard(
    pools: Vec<crate::worker::DbPool>,
    audit_pool: crate::worker::DbPool,
    config: RampGuardConfig,
    metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
    cancel: tokio_util::sync::CancellationToken,
) {
    if !config.is_enabled() {
        return;
    }
    let mut ticks = tokio::time::interval(config.interval());
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = ticks.tick() => {}
        }
        guard_once(&pools, &audit_pool, &config, Some(metrics.as_ref())).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(started: u64, completed: u64, failed: u64, nd_blocked: u64) -> BuildOutcomeStats {
        BuildOutcomeStats {
            started,
            completed,
            failed,
            nd_blocked,
        }
    }

    fn config() -> RampGuardConfig {
        RampGuardConfig::new().with_min_samples(10)
    }

    #[test]
    fn default_config_is_disabled_and_new_is_enabled() {
        assert!(!RampGuardConfig::default().is_enabled());
        let c = RampGuardConfig::new();
        assert!(c.is_enabled());
        assert_eq!(c.interval(), DEFAULT_INTERVAL);
        assert_eq!(c.min_samples(), DEFAULT_MIN_SAMPLES);
        assert!((c.max_failure_rate_increase() - 0.05).abs() < f64::EPSILON);
        assert!((c.max_nd_block_rate_increase() - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn config_clamps_out_of_range_values() {
        let c = RampGuardConfig::new()
            .with_interval(Duration::ZERO)
            .with_min_samples(0)
            .with_max_failure_rate_increase(-1.0)
            .with_max_nd_block_rate_increase(2.0);
        assert_eq!(c.interval(), MIN_INTERVAL);
        assert_eq!(c.min_samples(), 1);
        assert!(c.max_failure_rate_increase().abs() < f64::EPSILON);
        assert!((c.max_nd_block_rate_increase() - 1.0).abs() < f64::EPSILON);

        let c = RampGuardConfig::new()
            .with_interval(Duration::MAX)
            .with_max_failure_rate_increase(f64::NAN)
            .with_max_nd_block_rate_increase(f64::INFINITY);
        assert_eq!(c.interval(), MAX_INTERVAL);
        assert!(
            (c.max_failure_rate_increase() - DEFAULT_MAX_FAILURE_RATE_INCREASE).abs()
                < f64::EPSILON,
            "NaN keeps the default"
        );
        assert!((c.max_nd_block_rate_increase() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn stats_rates_handle_zero_denominators() {
        let empty = BuildOutcomeStats::default();
        assert_eq!(empty.settled(), 0);
        assert!(empty.failure_rate().abs() < f64::EPSILON);
        assert!(empty.nd_block_rate().abs() < f64::EPSILON);

        let s = stats(10, 6, 2, 1);
        assert_eq!(s.settled(), 8);
        assert!((s.failure_rate() - 0.25).abs() < f64::EPSILON);
        assert!((s.nd_block_rate() - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn target_failing_every_run_aborts_on_failure_rate() {
        let base = stats(90, 90, 0, 0);
        let target = stats(10, 0, 10, 0);
        assert_eq!(
            evaluate(&base, &target, &config()),
            RampVerdict::Abort {
                reason: RampAbortReason::FailureRate,
                base_rate: 0.0,
                target_rate: 1.0,
            }
        );
    }

    #[test]
    fn target_nd_blocking_aborts_on_nd_block_rate() {
        let base = stats(90, 90, 0, 0);
        let target = stats(10, 7, 0, 3);
        assert_eq!(
            evaluate(&base, &target, &config()),
            RampVerdict::Abort {
                reason: RampAbortReason::NdBlockRate,
                base_rate: 0.0,
                target_rate: 0.3,
            }
        );
    }

    #[test]
    fn the_threshold_is_an_increase_over_the_base_build() {
        // Both builds fail 30 %: a shared outage is not the new build's fault.
        let base = stats(100, 70, 30, 0);
        let target = stats(20, 14, 6, 0);
        assert_eq!(evaluate(&base, &target, &config()), RampVerdict::Healthy);
        // The increase must exceed the threshold, so equality holds. The
        // rates here are exact in binary floating point.
        let base = stats(100, 100, 0, 0);
        let c = config().with_max_failure_rate_increase(0.25);
        let target = stats(20, 15, 5, 0);
        assert_eq!(evaluate(&base, &target, &c), RampVerdict::Healthy);
        let target = stats(20, 14, 6, 0);
        assert!(matches!(
            evaluate(&base, &target, &c),
            RampVerdict::Abort {
                reason: RampAbortReason::FailureRate,
                ..
            }
        ));
    }

    #[test]
    fn too_few_target_runs_give_no_verdict() {
        let base = stats(90, 90, 0, 0);
        let target = stats(9, 0, 9, 0);
        assert_eq!(
            evaluate(&base, &target, &config()),
            RampVerdict::InsufficientData
        );
    }

    #[test]
    fn nd_block_needs_only_started_runs() {
        // Blocked runs never settle, so the ND check counts started runs.
        let base = stats(90, 90, 0, 0);
        let target = stats(10, 0, 0, 10);
        assert!(matches!(
            evaluate(&base, &target, &config()),
            RampVerdict::Abort {
                reason: RampAbortReason::NdBlockRate,
                ..
            }
        ));
    }

    #[test]
    fn failure_rate_wins_when_both_rates_exceed() {
        let base = stats(90, 90, 0, 0);
        let target = stats(20, 0, 10, 10);
        assert!(matches!(
            evaluate(&base, &target, &config()),
            RampVerdict::Abort {
                reason: RampAbortReason::FailureRate,
                ..
            }
        ));
    }

    #[test]
    fn abort_reason_labels_are_stable() {
        assert_eq!(RampAbortReason::FailureRate.as_str(), "failure_rate");
        assert_eq!(RampAbortReason::NdBlockRate.as_str(), "nd_block_rate");
    }
}
