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
/// The default minimum number of runs of each build for a verdict.
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
    /// The setter clamps the value to the range from [`MIN_INTERVAL`] to
    /// [`MAX_INTERVAL`]. The loop adds the interval to the clock, so the bound
    /// keeps every deadline finite.
    #[must_use]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval.clamp(MIN_INTERVAL, MAX_INTERVAL);
        self
    }

    /// Set the minimum number of runs of each build for a verdict.
    ///
    /// A value of 0 becomes 1.
    #[must_use]
    pub fn with_min_samples(mut self, min_samples: u64) -> Self {
        self.min_samples = min_samples.max(1);
        self
    }

    /// Set the maximum increase of the failure rate over the base build.
    ///
    /// The setter clamps the value to `0.0..=1.0`. `NaN` keeps the current
    /// value. A value of 1 turns the failure check off.
    #[must_use]
    pub const fn with_max_failure_rate_increase(mut self, increase: f64) -> Self {
        self.max_failure_rate_increase = clamp_rate(increase, self.max_failure_rate_increase);
        self
    }

    /// Set the maximum increase of the ND-block rate over the base build.
    ///
    /// The setter clamps the value to `0.0..=1.0`. `NaN` keeps the current
    /// value. A value of 1 turns the ND-block check off.
    #[must_use]
    pub const fn with_max_nd_block_rate_increase(mut self, increase: f64) -> Self {
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

    /// The minimum number of runs of each build for a verdict.
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
const fn clamp_rate(value: f64, current: f64) -> f64 {
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
    /// One of the two builds has too few runs for a verdict.
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
        /// The 95 % Wilson lower bound of that rate on the target build.
        target_lower_bound: f64,
    },
}

/// The z value of a two-sided 95 % confidence interval.
const WILSON_Z: f64 = 1.96;

/// The 95 % Wilson score lower bound of `hits / n`, or 0 when `n` is 0.
///
/// The bound is low for a small `n`. A few unlucky runs therefore cannot
/// abort a healthy ramp.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn wilson_lower_bound(hits: u64, n: u64) -> f64 {
    if n == 0 {
        return 0.0;
    }
    // The counts are run counts, far below 2^52, so the casts are exact.
    let n = n as f64;
    let p = hits as f64 / n;
    let z2 = WILSON_Z * WILSON_Z;
    let centre = p + z2 / (2.0 * n);
    let spread = WILSON_Z * (p * (1.0 - p) / n + z2 / (4.0 * n * n)).sqrt();
    ((centre - spread) / (1.0 + z2 / n)).max(0.0)
}

/// Compare the target build with the base build.
///
/// The failure rate counts only settled runs. The ND-block rate counts all
/// started runs, because a blocked run never settles. Each check needs
/// `min_samples` runs of each build in its own denominator. A thin base
/// sample is noise, so it gives no verdict.
///
/// A check aborts when the Wilson lower bound of the target rate is more than
/// its threshold above the base rate. The lower bound, not the point rate,
/// keeps a small unlucky target sample from aborting a healthy ramp. The
/// failure check runs first.
#[must_use]
pub fn evaluate(
    base: &BuildOutcomeStats,
    target: &BuildOutcomeStats,
    config: &RampGuardConfig,
) -> RampVerdict {
    let min = config.min_samples;
    let failure_judged = target.settled() >= min && base.settled() >= min;
    let nd_judged = target.started >= min && base.started >= min;
    if failure_judged {
        let base_rate = base.failure_rate();
        let lower = wilson_lower_bound(target.failed, target.settled());
        if lower - base_rate > config.max_failure_rate_increase {
            return RampVerdict::Abort {
                reason: RampAbortReason::FailureRate,
                base_rate,
                target_rate: target.failure_rate(),
                target_lower_bound: lower,
            };
        }
    }
    if nd_judged {
        let base_rate = base.nd_block_rate();
        let lower = wilson_lower_bound(target.nd_blocked, target.started);
        if lower - base_rate > config.max_nd_block_rate_increase {
            return RampVerdict::Abort {
                reason: RampAbortReason::NdBlockRate,
                base_rate,
                target_rate: target.nd_block_rate(),
                target_lower_bound: lower,
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
    /// The 95 % Wilson lower bound of the target rate.
    pub target_lower_bound: f64,
    /// The outcome counts of the base build.
    pub base: BuildOutcomeStats,
    /// The outcome counts of the target build.
    pub target: BuildOutcomeStats,
    /// `true` when a pool did not clear and the guard retries it.
    pub incomplete: bool,
}

// ── Database pass ─────────────────────────────────────────────────────────────

/// The longest time that the reads of one pass may take.
///
/// The read bound is the pass interval or this value, whichever is less. A
/// long interval therefore cannot hold a connection for an hour.
pub const MAX_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// The audit actor of an abort row.
pub const AUDIT_ACTOR: &str = "system";

/// The audit route of an abort row.
pub const AUDIT_ROUTE: &str = "background.ramp_guard";

/// SQL for the outcome counts of the two builds of one ramp step.
///
/// The binds are the queue, the base build, the target build, the step
/// start and the canary-probe name prefix. `idx_harvest_we_ramp_guard_outcome`
/// serves the read. A blocked run is `RUNNING`, or `PAUSED` when an operator
/// paused it.
#[must_use]
pub const fn ramp_outcome_stats_query() -> &'static str {
    "SELECT assigned_build_id AS build_id, \
         COUNT(*) AS started, \
         COUNT(*) FILTER (WHERE state = 'COMPLETED') AS completed, \
         COUNT(*) FILTER (WHERE state IN ('FAILED', 'TIMED_OUT')) AS failed, \
         COUNT(*) FILTER ( \
             WHERE state IN ('RUNNING', 'PAUSED') AND nd_blocked_at IS NOT NULL \
         ) AS nd_blocked \
     FROM harvest_workflow_executions \
     WHERE queue_name = $1 \
       AND assigned_build_id IN ($2, $3) \
       AND created_at >= $4 \
       AND NOT starts_with(workflow_name, $5) \
     GROUP BY assigned_build_id"
}

/// SQL for the compare-and-swap clear of one ramp step.
///
/// The binds are the queue, the base build, the target build and the step
/// start. The row must still hold the same step, so a verdict about an old
/// step cannot clear a new one.
#[must_use]
pub const fn abort_ramp_query() -> &'static str {
    "UPDATE harvest_build_policies \
     SET target_build_id = NULL, ramp_percent = NULL, updated_at = NOW() \
     WHERE queue_name = $1 AND build_id = $2 AND target_build_id = $3 \
       AND updated_at = $4"
}

/// Clear the ramp of `queue` when it still ramps `base` to `target` at `step`.
///
/// The clear is a compare-and-swap. `step` is the policy row's `updated_at`
/// that the verdict used. A ramp that an operator moved to another target,
/// another base build or a new step stays. Returns `true` when this call
/// cleared the ramp.
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
    step: chrono::DateTime<chrono::Utc>,
) -> crate::error::HarvestResult<bool> {
    use diesel::sql_types::{Text, Timestamptz};
    use diesel_async::RunQueryDsl;

    let changed = diesel::sql_query(abort_ramp_query())
        .bind::<Text, _>(queue)
        .bind::<Text, _>(base)
        .bind::<Text, _>(target)
        .bind::<Timestamptz, _>(step)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(changed > 0)
}

/// The identity of one ramp: queue, base build and target build.
#[cfg(feature = "db")]
type RampKey = (String, String, String);

/// One ramp row on one pool, with its step counts.
#[cfg(feature = "db")]
#[derive(Debug)]
struct PoolRamp {
    key: RampKey,
    step: chrono::DateTime<chrono::Utc>,
    ramp_percent: i32,
    base: BuildOutcomeStats,
    target: BuildOutcomeStats,
}

/// One ramp merged over every pool that holds it.
#[cfg(feature = "db")]
#[derive(Debug, Default)]
struct ObservedRamp {
    ramp_percent: i32,
    base: BuildOutcomeStats,
    target: BuildOutcomeStats,
    /// The pool index and the step of each pool that holds the ramp, in
    /// pool order.
    steps: Vec<(usize, chrono::DateTime<chrono::Utc>)>,
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
    let mut base = BuildOutcomeStats::default();
    let mut target_stats = BuildOutcomeStats::default();
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
///
/// The reads run in one read-only transaction with `statement_timeout` set to
/// `bound`, so the server stops a slow scan too. A ramp to its own base build
/// is a promotion in progress, not a ramp, so the read skips it.
#[cfg(feature = "db")]
async fn read_pool_ramps(
    pool: &crate::worker::DbPool,
    bound: Duration,
) -> crate::error::HarvestResult<Vec<PoolRamp>> {
    use diesel_async::RunQueryDsl;

    let mut conn = pool.get().await.map_err(crate::error::database_error)?;
    let timeout_ms = bound.as_millis().max(1);
    conn.build_transaction()
        .read_only()
        .run(async |conn| -> crate::error::HarvestResult<Vec<PoolRamp>> {
            diesel::sql_query(format!("SET LOCAL statement_timeout = {timeout_ms}"))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            let policies = crate::build_routing::list_build_policies(conn).await?;
            let mut ramps = Vec::new();
            for policy in policies {
                let (Some(target), Some(percent)) =
                    (policy.target_build_id.clone(), policy.ramp_percent)
                else {
                    continue;
                };
                if percent <= 0 || target == policy.build_id {
                    continue;
                }
                let (base, target_stats) = read_step_stats(conn, &policy, &target).await?;
                ramps.push(PoolRamp {
                    key: (policy.queue_name.clone(), policy.build_id.clone(), target),
                    step: policy.updated_at,
                    ramp_percent: percent,
                    base,
                    target: target_stats,
                });
            }
            Ok(ramps)
        })
        .await
}

/// Read every active ramp on every pool and merge the counts per ramp.
///
/// The pools are read at the same time. Returns `None` when any read fails,
/// so a pass never decides on part of the fleet.
#[cfg(feature = "db")]
async fn read_ramps(
    pools: &[crate::worker::DbPool],
    bound: Duration,
) -> Option<std::collections::BTreeMap<RampKey, ObservedRamp>> {
    let reads = pools.iter().map(|pool| read_pool_ramps(pool, bound));
    let mut merged: std::collections::BTreeMap<RampKey, ObservedRamp> =
        std::collections::BTreeMap::new();
    for (index, result) in futures::future::join_all(reads)
        .await
        .into_iter()
        .enumerate()
    {
        let ramps = match result {
            Ok(ramps) => ramps,
            Err(error) => {
                tracing::warn!(pool = index, error = %error, "ramp guard read failed; no verdict this pass");
                return None;
            }
        };
        for ramp in ramps {
            let slot = merged.entry(ramp.key).or_default();
            slot.ramp_percent = slot.ramp_percent.max(ramp.ramp_percent);
            slot.base = slot.base.plus(ramp.base);
            slot.target = slot.target.plus(ramp.target);
            slot.steps.push((index, ramp.step));
        }
    }
    Some(merged)
}

/// The result of one compare-and-swap clear on one pool.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClearOutcome {
    /// This call cleared the ramp.
    Cleared,
    /// The row changed first, so nothing was cleared.
    Lost,
    /// The clear failed or timed out. The guard retries it.
    Failed,
}

/// Clear one ramp step on one pool, within `bound`.
#[cfg(feature = "db")]
async fn clear_on_pool(
    pool: &crate::worker::DbPool,
    index: usize,
    key: &RampKey,
    step: chrono::DateTime<chrono::Utc>,
    bound: Duration,
) -> ClearOutcome {
    let (queue, base, target) = key;
    let clear = async {
        let mut conn = pool.get().await.map_err(|e| e.to_string())?;
        abort_ramp(&mut conn, queue, base, target, step)
            .await
            .map_err(|e| e.to_string())
    };
    match tokio::time::timeout(bound, clear).await {
        Ok(Ok(true)) => ClearOutcome::Cleared,
        Ok(Ok(false)) => ClearOutcome::Lost,
        Ok(Err(error)) => {
            tracing::warn!(queue = %queue, pool = index, error = %error, "ramp guard clear failed");
            ClearOutcome::Failed
        }
        Err(_) => {
            tracing::warn!(queue = %queue, pool = index, "ramp guard clear timed out");
            ClearOutcome::Failed
        }
    }
}

/// Write the audit row of one abort, within `bound`.
///
/// The write is best effort. A failed write logs a warning and does not undo
/// the abort. The summary names a pool that did not clear by its index only,
/// so no database error text reaches the audit log.
#[cfg(feature = "db")]
async fn record_abort(
    audit_pool: &crate::worker::DbPool,
    abort: &RampAbort,
    failed_pools: &[usize],
    bound: Duration,
) {
    let mut summary = format!(
        "reason={} target_build={} base_build={} ramp_percent={} \
         target_rate={:.4} target_lower_bound={:.4} base_rate={:.4} \
         target_started={} target_settled={} base_started={} base_settled={}",
        abort.reason.as_str(),
        abort.target_build_id,
        abort.base_build_id,
        abort.ramp_percent,
        abort.target_rate,
        abort.target_lower_bound,
        abort.base_rate,
        abort.target.started,
        abort.target.settled(),
        abort.base.started,
        abort.base.settled(),
    );
    if !failed_pools.is_empty() {
        let pools: Vec<String> = failed_pools.iter().map(ToString::to_string).collect();
        summary.push_str("; clear pending on pools ");
        summary.push_str(&pools.join(","));
    }
    let status = if failed_pools.is_empty() {
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
    let write = async {
        let mut conn = audit_pool.get().await.map_err(|e| e.to_string())?;
        crate::audit::insert_audit(&mut conn, &record)
            .await
            .map_err(|e| e.to_string())
    };
    match tokio::time::timeout(bound, write).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            tracing::warn!(queue = %abort.queue, error = %error, "ramp guard audit write failed");
        }
        Err(_) => tracing::warn!(queue = %abort.queue, "ramp guard audit write timed out"),
    }
}

/// The build ramp guard, with the clears that it must still retry.
///
/// [`run_ramp_guard`] keeps one guard for its whole life. Use
/// [`RampGuard::pass`] directly to drive the guard from another loop.
#[cfg(feature = "db")]
#[derive(Debug)]
pub struct RampGuard {
    config: RampGuardConfig,
    /// Aborted ramps with a pool that did not clear, by pool index and step.
    pending: std::collections::BTreeMap<RampKey, Vec<(usize, chrono::DateTime<chrono::Utc>)>>,
}

#[cfg(feature = "db")]
impl RampGuard {
    /// Make a guard with `config` and no pending clears.
    #[must_use]
    pub const fn new(config: RampGuardConfig) -> Self {
        Self {
            config,
            pending: std::collections::BTreeMap::new(),
        }
    }

    /// The number of aborted ramps that still have a pool to clear.
    #[must_use]
    pub fn pending_clears(&self) -> usize {
        self.pending.len()
    }

    /// The bound of one read or one write of a pass.
    fn bound(&self) -> Duration {
        self.config.interval().min(MAX_READ_TIMEOUT)
    }

    /// Run one guard pass over every ramp in `pools`.
    ///
    /// `pools` holds one pool per physical database, in a fixed order. The
    /// pass first retries the clears that an earlier pass could not finish.
    /// A retry needs no new verdict, and it writes no new audit row.
    ///
    /// The pass then reads every ramp and its step counts, and it aborts each
    /// ramp with an abort verdict. All reads of one pass must end within the
    /// bound. A failed, slow or cancelled read aborts nothing.
    ///
    /// The pass audits an abort only when it did not lose the clear on the
    /// first pool that holds the ramp. Of many replicas, only that winner
    /// audits. For each audited abort, the pass writes one audit row to
    /// `audit_pool`. It also counts the abort on `metrics` and logs a warning.
    /// Returns the audited aborts.
    pub async fn pass(
        &mut self,
        pools: &[crate::worker::DbPool],
        audit_pool: &crate::worker::DbPool,
        metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Vec<RampAbort> {
        let bound = self.bound();
        self.retry_pending(pools, bound).await;

        let read = tokio::select! {
            () = cancel.cancelled() => return Vec::new(),
            read = tokio::time::timeout(bound, read_ramps(pools, bound)) => read,
        };
        let Ok(read) = read else {
            tracing::warn!("ramp guard read timed out; no verdict this pass");
            return Vec::new();
        };
        let Some(ramps) = read else {
            return Vec::new();
        };

        let mut aborts = Vec::new();
        for (key, ramp) in ramps {
            if self.pending.contains_key(&key) {
                continue;
            }
            let RampVerdict::Abort {
                reason,
                base_rate,
                target_rate,
                target_lower_bound,
            } = evaluate(&ramp.base, &ramp.target, &self.config)
            else {
                continue;
            };
            if let Some(abort) = self
                .abort(
                    pools,
                    audit_pool,
                    metrics,
                    key,
                    ramp,
                    (reason, base_rate, target_rate, target_lower_bound),
                    bound,
                )
                .await
            {
                aborts.push(abort);
            }
        }
        aborts
    }

    /// Clear one ramp with an abort verdict on every pool that holds it.
    #[allow(clippy::too_many_arguments)]
    async fn abort(
        &mut self,
        pools: &[crate::worker::DbPool],
        audit_pool: &crate::worker::DbPool,
        metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
        key: RampKey,
        ramp: ObservedRamp,
        verdict: (RampAbortReason, f64, f64, f64),
        bound: Duration,
    ) -> Option<RampAbort> {
        let (reason, base_rate, target_rate, target_lower_bound) = verdict;
        let mut outcomes = Vec::with_capacity(ramp.steps.len());
        for &(index, step) in &ramp.steps {
            let outcome = clear_on_pool(&pools[index], index, &key, step, bound).await;
            outcomes.push((index, step, outcome));
        }
        let failed: Vec<(usize, chrono::DateTime<chrono::Utc>)> = outcomes
            .iter()
            .filter(|(_, _, outcome)| *outcome == ClearOutcome::Failed)
            .map(|&(index, step, _)| (index, step))
            .collect();
        if !failed.is_empty() {
            self.pending.insert(key.clone(), failed.clone());
        }
        let cleared = outcomes
            .iter()
            .any(|(_, _, outcome)| *outcome == ClearOutcome::Cleared);
        let lost_first = outcomes
            .first()
            .is_some_and(|(_, _, outcome)| *outcome == ClearOutcome::Lost);
        if lost_first || (!cleared && failed.is_empty()) {
            // Another replica or an operator changed the ramp first.
            return None;
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
            target_lower_bound,
            base: ramp.base,
            target: ramp.target,
            incomplete: !failed.is_empty(),
        };
        tracing::warn!(
            queue = %abort.queue,
            base_build = %abort.base_build_id,
            target_build = %abort.target_build_id,
            reason = abort.reason.as_str(),
            target_rate = abort.target_rate,
            target_lower_bound = abort.target_lower_bound,
            base_rate = abort.base_rate,
            incomplete = abort.incomplete,
            "ramp guard aborted a build ramp"
        );
        if let Some(m) = metrics {
            m.record_build_ramp_aborted(&abort.queue, abort.reason.as_str());
        }
        let failed_pools: Vec<usize> = failed.iter().map(|&(index, _)| index).collect();
        record_abort(audit_pool, &abort, &failed_pools, bound).await;
        Some(abort)
    }

    /// Retry the clears that an earlier pass could not finish.
    ///
    /// A pool leaves the list when its clear succeeds or when its row changed.
    async fn retry_pending(&mut self, pools: &[crate::worker::DbPool], bound: Duration) {
        let pending = std::mem::take(&mut self.pending);
        for (key, steps) in pending {
            let mut still_failed = Vec::new();
            for (index, step) in steps {
                let Some(pool) = pools.get(index) else {
                    continue;
                };
                match clear_on_pool(pool, index, &key, step, bound).await {
                    ClearOutcome::Cleared => {
                        tracing::info!(queue = %key.0, pool = index, "ramp guard finished a pending clear");
                    }
                    ClearOutcome::Lost => {}
                    ClearOutcome::Failed => still_failed.push((index, step)),
                }
            }
            if !still_failed.is_empty() {
                self.pending.insert(key, still_failed);
            }
        }
    }
}

/// Run one guard pass with a new guard that has no pending clears.
///
/// See [`RampGuard::pass`]. A clear that fails in this pass is not retried.
/// [`run_ramp_guard`] keeps one guard, so it retries such clears.
#[cfg(feature = "db")]
pub async fn guard_once(
    pools: &[crate::worker::DbPool],
    audit_pool: &crate::worker::DbPool,
    config: &RampGuardConfig,
    metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
) -> Vec<RampAbort> {
    let never = tokio_util::sync::CancellationToken::new();
    RampGuard::new(*config)
        .pass(pools, audit_pool, metrics, &never)
        .await
}

/// Run guard passes every `config.interval()` until `cancel` fires.
///
/// Returns at once when the config is disabled. A cancel stops a pass during
/// its read, which has no side effects. A pass that has started to clear
/// runs to its end, so a clear and its audit row are not split. Each write
/// has a bound, so the end is near. An overdue tick fires at once, and the
/// next tick comes one interval after it.
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
    let mut guard = RampGuard::new(config);
    let mut ticks = tokio::time::interval(config.interval());
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = ticks.tick() => {}
        }
        guard
            .pass(&pools, &audit_pool, Some(metrics.as_ref()), &cancel)
            .await;
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

    /// The reason of an abort verdict, or `None` for any other verdict.
    fn abort_reason(verdict: RampVerdict) -> Option<RampAbortReason> {
        match verdict {
            RampVerdict::Abort { reason, .. } => Some(reason),
            _ => None,
        }
    }

    #[test]
    fn wilson_lower_bound_matches_known_values() {
        assert!(wilson_lower_bound(0, 0).abs() < f64::EPSILON);
        assert!(wilson_lower_bound(0, 50).abs() < f64::EPSILON);
        // 10 of 10: the 95 % lower bound is about 0.7225.
        assert!((wilson_lower_bound(10, 10) - 0.7225).abs() < 1e-3);
        // 2 of 20: about 0.0279, well below the point rate of 0.1.
        assert!((wilson_lower_bound(2, 20) - 0.0279).abs() < 1e-3);
        // More runs at the same rate give a tighter, higher bound.
        assert!(wilson_lower_bound(20, 200) > wilson_lower_bound(2, 20));
    }

    #[test]
    fn target_failing_every_run_aborts_on_failure_rate() {
        let base = stats(90, 90, 0, 0);
        let target = stats(10, 0, 10, 0);
        match evaluate(&base, &target, &config()) {
            RampVerdict::Abort {
                reason,
                base_rate,
                target_rate,
                target_lower_bound,
            } => {
                assert_eq!(reason, RampAbortReason::FailureRate);
                assert!(base_rate.abs() < f64::EPSILON);
                assert!((target_rate - 1.0).abs() < f64::EPSILON);
                assert!((target_lower_bound - 0.7225).abs() < 1e-3);
            }
            other => panic!("expected an abort, got {other:?}"),
        }
    }

    #[test]
    fn target_nd_blocking_aborts_on_nd_block_rate() {
        // 3 of 10 blocked: the lower bound is about 0.108, over 0.05.
        let base = stats(90, 90, 0, 0);
        let target = stats(10, 7, 0, 3);
        assert_eq!(
            abort_reason(evaluate(&base, &target, &config())),
            Some(RampAbortReason::NdBlockRate)
        );
    }

    #[test]
    fn the_threshold_is_an_increase_over_the_base_build() {
        // Both builds fail 30 %: a shared outage is not the new build's fault.
        let base = stats(100, 70, 30, 0);
        let target = stats(20, 14, 6, 0);
        assert_eq!(evaluate(&base, &target, &config()), RampVerdict::Healthy);
    }

    #[test]
    fn the_lower_bound_not_the_point_rate_must_exceed_the_threshold() {
        let base = stats(100, 100, 0, 0);
        let c = config().with_max_failure_rate_increase(0.25);
        // 8 of 20 fail: the point rate 0.4 exceeds 0.25, the bound 0.219 does not.
        let target = stats(20, 12, 8, 0);
        assert_eq!(evaluate(&base, &target, &c), RampVerdict::Healthy);
        // 10 of 20 fail: the bound 0.299 exceeds 0.25.
        let target = stats(20, 10, 10, 0);
        assert_eq!(
            abort_reason(evaluate(&base, &target, &c)),
            Some(RampAbortReason::FailureRate)
        );
    }

    #[test]
    fn a_small_unlucky_target_sample_is_healthy() {
        // Base fails 3 %. Two failures in 20 target runs is 10 %, but the
        // lower bound is about 2.8 %, so the ramp stays.
        let base = stats(1000, 970, 30, 0);
        let target = stats(20, 18, 2, 0);
        assert_eq!(
            evaluate(&base, &target, &RampGuardConfig::new()),
            RampVerdict::Healthy
        );
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
    fn a_thin_base_sample_gives_no_verdict() {
        // At or near 100 % the base build gets almost no new runs. Its rate
        // is then noise, so the guard does not judge.
        let base = stats(3, 3, 0, 0);
        let target = stats(50, 40, 10, 0);
        assert_eq!(
            evaluate(&base, &target, &config()),
            RampVerdict::InsufficientData
        );
        let empty = BuildOutcomeStats::default();
        assert_eq!(
            evaluate(&empty, &target, &config()),
            RampVerdict::InsufficientData
        );
    }

    #[test]
    fn nd_block_needs_only_started_runs() {
        // Blocked runs never settle, so the ND check counts started runs.
        let base = stats(90, 90, 0, 0);
        let target = stats(10, 0, 0, 10);
        assert_eq!(
            abort_reason(evaluate(&base, &target, &config())),
            Some(RampAbortReason::NdBlockRate)
        );
    }

    #[test]
    fn failure_rate_wins_when_both_rates_exceed() {
        let base = stats(90, 90, 0, 0);
        let target = stats(20, 0, 10, 10);
        assert_eq!(
            abort_reason(evaluate(&base, &target, &config())),
            Some(RampAbortReason::FailureRate)
        );
    }

    #[test]
    fn a_threshold_of_one_turns_its_check_off() {
        let base = stats(90, 90, 0, 0);
        let target = stats(50, 0, 50, 0);
        let c = config().with_max_failure_rate_increase(1.0);
        assert_eq!(evaluate(&base, &target, &c), RampVerdict::Healthy);
    }

    #[test]
    fn stats_plus_adds_every_count() {
        let sum = stats(1, 2, 3, 4).plus(stats(10, 20, 30, 40));
        assert_eq!(sum, stats(11, 22, 33, 44));
    }

    #[test]
    fn queries_bind_every_input_and_pin_the_step() {
        let stats_sql = ramp_outcome_stats_query();
        for bind in ["$1", "$2", "$3", "$4", "$5"] {
            assert!(stats_sql.contains(bind), "stats query lacks {bind}");
        }
        assert!(stats_sql.contains("state IN ('RUNNING', 'PAUSED') AND nd_blocked_at IS NOT NULL"));
        assert!(stats_sql.contains("state IN ('FAILED', 'TIMED_OUT')"));
        let abort_sql = abort_ramp_query();
        assert!(
            abort_sql.contains("AND updated_at = $4"),
            "the clear pins the step"
        );
        assert!(abort_sql.contains("target_build_id = $3"));
    }

    #[test]
    fn abort_reason_labels_are_stable() {
        assert_eq!(RampAbortReason::FailureRate.as_str(), "failure_rate");
        assert_eq!(RampAbortReason::NdBlockRate.as_str(), "nd_block_rate");
    }
}
