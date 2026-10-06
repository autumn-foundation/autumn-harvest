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
/// Default age after which a pass reports an unreported abort.
pub const DEFAULT_REPORT_GRACE: Duration = Duration::from_secs(10 * 60);
/// Maximum report grace.
pub const MAX_REPORT_GRACE: Duration = Duration::from_secs(24 * 60 * 60);
/// Minimum age of a reported marker before a pass removes it.
///
/// A ramp fan-out that is still in flight can write the marked `ramp_id` to
/// a later pool. The marker must still be there to finish that late ramp.
/// The report grace can be zero, so this floor keeps the fence positive.
pub const MIN_MARKER_RETENTION: Duration = Duration::from_secs(10 * 60);

/// The settings of the ramp guard.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RampGuardConfig {
    enabled: bool,
    interval: Duration,
    min_samples: u64,
    max_failure_rate_increase: f64,
    max_nd_block_rate_increase: f64,
    report_grace: Duration,
}

impl Default for RampGuardConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: DEFAULT_INTERVAL,
            min_samples: DEFAULT_MIN_SAMPLES,
            max_failure_rate_increase: DEFAULT_MAX_FAILURE_RATE_INCREASE,
            max_nd_block_rate_increase: DEFAULT_MAX_ND_BLOCK_RATE_INCREASE,
            report_grace: DEFAULT_REPORT_GRACE,
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

    /// Set the age after which a pass reports an unreported abort.
    ///
    /// A guard can stop after its clear commits and before it reports the
    /// abort. The abort marker then stays unreported. Once the marker is
    /// older than this grace, a pass reports the abort with reason
    /// [`RampAbortReason::Unreported`]. It does so even while a pool still
    /// holds the ramp. The setter clamps
    /// the value to [`MAX_REPORT_GRACE`]. Zero is allowed.
    #[must_use]
    pub fn with_report_grace(mut self, grace: Duration) -> Self {
        self.report_grace = grace.min(MAX_REPORT_GRACE);
        self
    }

    /// The age after which a pass reports an unreported abort.
    #[must_use]
    pub const fn report_grace(&self) -> Duration {
        self.report_grace
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
    /// A guard cleared the ramp but stopped before it reported the abort. A
    /// later pass reports it from the abort marker, with no rates.
    Unreported,
}

impl RampAbortReason {
    /// The stable label and audit value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FailureRate => "failure_rate",
            Self::NdBlockRate => "nd_block_rate",
            Self::Unreported => "unreported",
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
/// The binds are the queue, the base build, the target build, the step
/// start and a report id. The row must still hold the same step, so a
/// verdict about an old step cannot clear a new one.
///
/// The same UPDATE adds an abort marker to the front of `ramp_aborted`, so
/// the marker commits with the clear. The marker holds `id`, `base`,
/// `target`, `reported` and `at`, the clear time in epoch milliseconds. The
/// `id` is the `ramp_id`. A ramp with no `ramp_id` gets the report id
/// instead, so its report stays recoverable. A newer abort keeps the older
/// markers. The new marker is always unreported. Only a committed report
/// marks it. The UPDATE returns the `id` of the new marker as `marker_id`.
///
/// The same statement retires the row's `ramp_caller_id`. A retry after a
/// base change derives another stored id, so only the caller id refuses it.
/// It retires the [`ramp_caller_target_id`] of the caller id and the target,
/// not the raw caller id. One policy fan-out can give one caller id to two
/// targets, and an abort of one must not refuse the other. The marker keeps
/// that id as `caller`. A later pass retires it on every pool, also on a pool
/// that the fan-out missed. The subquery is the SQL form of that id.
///
/// [`ramp_caller_target_id`]: crate::build_routing::ramp_caller_target_id
/// The retire reads the output of the clear, so it retires an id only when
/// the clear changed that row. The subquery reads the caller id before the
/// clear. A concurrent writer changes `updated_at`, so the pinned step
/// rejects a row that changed after that read.
#[must_use]
pub const fn abort_ramp_query() -> &'static str {
    "WITH cleared AS ( \
         UPDATE harvest_build_policies \
         SET ramp_aborted = jsonb_build_array(jsonb_build_object( \
                     'id', COALESCE(ramp_id, $5), 'base', build_id, \
                     'target', target_build_id, 'caller', old.caller, \
                     'reported', false, \
                     'at', (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint)) \
                     || ramp_aborted, \
             ramp_id = NULL, ramp_caller_id = NULL, target_build_id = NULL, \
             ramp_percent = NULL, updated_at = NOW() \
         FROM (SELECT encode(substring(sha256(convert_to(ramp_caller_id::text \
                   || '/' || octet_length(convert_to(queue_name, 'UTF8')) || ':' || queue_name \
                   || '/0:' \
                   || '/' || octet_length(convert_to(target_build_id, 'UTF8')) || ':' \
                   || target_build_id, 'UTF8')) FROM 1 FOR 16), 'hex')::uuid AS caller \
               FROM harvest_build_policies WHERE queue_name = $1) AS old \
         WHERE queue_name = $1 AND build_id = $2 AND target_build_id = $3 \
           AND updated_at = $4 \
         RETURNING queue_name, old.caller, (ramp_aborted -> 0 ->> 'id')::uuid AS marker_id \
     ), retired AS ( \
         INSERT INTO harvest_ramp_retired_ids (queue_name, ramp_id) \
         SELECT queue_name, caller FROM cleared WHERE caller IS NOT NULL \
         ON CONFLICT (queue_name, ramp_id) DO NOTHING \
     ) \
     SELECT marker_id FROM cleared"
}

/// SQL that removes finished abort markers from one policy row.
///
/// The binds are the queue and the `ramp_id`s of the finished markers. The
/// UPDATE keeps the order of the other markers. It does not change
/// `updated_at`, so the ramp step stays the same.
#[must_use]
pub const fn prune_abort_markers_query() -> &'static str {
    "UPDATE harvest_build_policies \
     SET ramp_aborted = COALESCE( \
             (SELECT jsonb_agg(entry ORDER BY position) \
              FROM jsonb_array_elements(ramp_aborted) WITH ORDINALITY AS m(entry, position) \
              WHERE NOT (entry->>'id' = ANY($2))), \
             '[]'::jsonb) \
     WHERE queue_name = $1"
}

/// SQL that marks the abort markers of one `ramp_id` as reported.
///
/// The binds are the queue and the `ramp_id` as text. The UPDATE changes a
/// row only when it holds an unreported marker for that id. So the update
/// is also a claim: of two guards, only one changes the row. It does not
/// change `updated_at`.
#[must_use]
pub const fn mark_abort_reported_query() -> &'static str {
    "UPDATE harvest_build_policies \
     SET ramp_aborted = \
             (SELECT jsonb_agg(CASE WHEN entry->>'id' = $2 \
                                    THEN jsonb_set(entry, '{reported}', 'true'::jsonb) \
                                    ELSE entry END \
                               ORDER BY position) \
              FROM jsonb_array_elements(ramp_aborted) WITH ORDINALITY AS m(entry, position)) \
     WHERE queue_name = $1 \
       AND ramp_aborted @> jsonb_build_array( \
               jsonb_build_object('id', $2::text, 'reported', false))"
}

/// Mark the abort markers of `ramp_id` on `queue` as reported.
///
/// Returns `true` when this call changed the row. A guard that recovers an
/// unreported abort calls this first, so only one guard reports it.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn mark_abort_reported(
    conn: &mut diesel_async::AsyncPgConnection,
    queue: &str,
    ramp_id: uuid::Uuid,
    bound: Duration,
) -> crate::error::HarvestResult<bool> {
    use diesel::sql_types::Text;
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let timeout_ms = bound.as_millis().max(1);
    let id = ramp_id.to_string();
    conn.transaction(async |conn| -> crate::error::HarvestResult<bool> {
        for setting in ["lock_timeout", "statement_timeout"] {
            diesel::sql_query(format!("SET LOCAL {setting} = {timeout_ms}"))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
        }
        let changed = diesel::sql_query(mark_abort_reported_query())
            .bind::<Text, _>(queue)
            .bind::<Text, _>(&id)
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
        Ok(changed > 0)
    })
    .await
}

/// SQL that claims the recovery of one unreported abort.
///
/// The binds are the queue, the `ramp_id` as text and the lease in
/// milliseconds. The UPDATE sets `claim` to the pool clock on the markers of
/// that id. It changes the row only when a marker of that id is unreported
/// and has no claim younger than the lease. So of two guards, only one
/// claims. A claim does not mark the abort as reported. A guard that claims
/// and then stops leaves the marker unreported, and after the lease another
/// guard can claim it again. The UPDATE does not change `updated_at`.
#[must_use]
pub const fn claim_unreported_abort_query() -> &'static str {
    "UPDATE harvest_build_policies \
     SET ramp_aborted = \
             (SELECT jsonb_agg(CASE WHEN entry->>'id' = $2 \
                                    THEN jsonb_set(entry, '{claim}', \
                                             to_jsonb((EXTRACT(EPOCH FROM NOW()) * 1000)::bigint)) \
                                    ELSE entry END \
                               ORDER BY position) \
              FROM jsonb_array_elements(ramp_aborted) WITH ORDINALITY AS m(entry, position)) \
     WHERE queue_name = $1 \
       AND EXISTS (SELECT 1 FROM jsonb_array_elements(ramp_aborted) AS c(entry) \
                   WHERE entry->>'id' = $2 \
                     AND entry->'reported' = 'false'::jsonb \
                     AND (entry->'claim' IS NULL \
                          OR (entry->>'claim')::bigint \
                             <= (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint - $3))"
}

/// Claim the recovery of the unreported abort of `ramp_id` on `queue`.
///
/// Returns `true` when this call took the claim. The claim is a lease of
/// `lease`. The caller reports the abort and then calls
/// [`mark_abort_reported`].
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn claim_unreported_abort(
    conn: &mut diesel_async::AsyncPgConnection,
    queue: &str,
    ramp_id: uuid::Uuid,
    lease: Duration,
    bound: Duration,
) -> crate::error::HarvestResult<bool> {
    use diesel::sql_types::{BigInt, Text};
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let timeout_ms = bound.as_millis().max(1);
    let lease_ms = i64::try_from(lease.as_millis()).unwrap_or(i64::MAX);
    let id = ramp_id.to_string();
    conn.transaction(async |conn| -> crate::error::HarvestResult<bool> {
        for setting in ["lock_timeout", "statement_timeout"] {
            diesel::sql_query(format!("SET LOCAL {setting} = {timeout_ms}"))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
        }
        let changed = diesel::sql_query(claim_unreported_abort_query())
            .bind::<Text, _>(queue)
            .bind::<Text, _>(&id)
            .bind::<BigInt, _>(lease_ms)
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
        Ok(changed > 0)
    })
    .await
}

/// Record the report of the abort of `ramp_id` on `queue` in the report
/// ledger `harvest_ramp_abort_reports`.
///
/// Returns `true` when this call added the row, and `false` when the ledger
/// already held it. Only the guard whose call returns `true` reports the
/// abort, so a report is exactly-once. The guard calls this in the same
/// transaction as its audit row.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn record_abort_report(
    conn: &mut diesel_async::AsyncPgConnection,
    queue: &str,
    ramp_id: uuid::Uuid,
) -> crate::error::HarvestResult<bool> {
    use diesel::sql_types::Text;
    use diesel_async::RunQueryDsl;

    let added = diesel::sql_query(
        "INSERT INTO harvest_ramp_abort_reports (ramp_id, queue_name) VALUES ($1, $2) \
         ON CONFLICT (ramp_id) DO NOTHING",
    )
    .bind::<diesel::sql_types::Uuid, _>(ramp_id)
    .bind::<Text, _>(queue)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(added > 0)
}

/// Clear the ramp of `queue` when it still ramps `base` to `target` at `step`.
///
/// The clear is a compare-and-swap. `step` is the policy row's `updated_at`
/// that the verdict used. A ramp that an operator moved to another target,
/// another base build or a new step stays. Returns the id of the new abort
/// marker when this call cleared the ramp, and `None` when it did not.
///
/// The clear runs in one transaction with `lock_timeout` and
/// `statement_timeout` set to `bound`. A clear that waits too long therefore
/// fails on the server and rolls back. It cannot commit later, after the
/// caller has given up on it.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure, also when the server stops
/// the clear at `bound`.
///
/// The marker of this clear is unreported. The caller reports the abort and
/// then calls [`mark_abort_reported`] with the returned id. The id is the
/// `ramp_id`. A ramp with no `ramp_id` gets a fresh report id instead.
#[cfg(feature = "db")]
pub async fn abort_ramp(
    conn: &mut diesel_async::AsyncPgConnection,
    queue: &str,
    base: &str,
    target: &str,
    step: chrono::DateTime<chrono::Utc>,
    bound: Duration,
) -> crate::error::HarvestResult<Option<uuid::Uuid>> {
    clear_ramp(conn, queue, base, target, step, bound, uuid::Uuid::new_v4()).await
}

/// [`abort_ramp`] with the report id for a ramp with no `ramp_id`. Every
/// pool of one abort gets the same report id.
#[cfg(feature = "db")]
async fn clear_ramp(
    conn: &mut diesel_async::AsyncPgConnection,
    queue: &str,
    base: &str,
    target: &str,
    step: chrono::DateTime<chrono::Utc>,
    bound: Duration,
    report_id: uuid::Uuid,
) -> crate::error::HarvestResult<Option<uuid::Uuid>> {
    use diesel::OptionalExtension;
    use diesel::sql_types::{Text, Timestamptz};
    use diesel_async::{AsyncConnection, RunQueryDsl};

    #[derive(diesel::QueryableByName)]
    struct Cleared {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        marker_id: uuid::Uuid,
    }

    let timeout_ms = bound.as_millis().max(1);
    conn.transaction(
        async |conn| -> crate::error::HarvestResult<Option<uuid::Uuid>> {
            for setting in ["lock_timeout", "statement_timeout"] {
                diesel::sql_query(format!("SET LOCAL {setting} = {timeout_ms}"))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            }
            let cleared: Option<Cleared> = diesel::sql_query(abort_ramp_query())
                .bind::<Text, _>(queue)
                .bind::<Text, _>(base)
                .bind::<Text, _>(target)
                .bind::<Timestamptz, _>(step)
                .bind::<diesel::sql_types::Uuid, _>(report_id)
                .get_result(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            Ok(cleared.map(|row| row.marker_id))
        },
    )
    .await
}

/// The identity of one ramp: queue, base build and target build.
#[cfg(feature = "db")]
type RampKey = (String, String, String);

/// One ramp generation: the ramp key and the `ramp_id`.
///
/// A partial fan-out can leave two ramps with the same builds and different
/// `ramp_id`s on different pools. The guard judges each generation on its own
/// counts. A ramp set before the `ramp_id` column existed has `None`.
#[cfg(feature = "db")]
type GenerationKey = (RampKey, Option<uuid::Uuid>);

/// One ramp row on one pool, with its step counts.
#[cfg(feature = "db")]
#[derive(Debug)]
struct PoolRamp {
    key: RampKey,
    step: chrono::DateTime<chrono::Utc>,
    /// The ramp's identity, or `None` for a ramp set before the column
    /// existed.
    ramp_id: Option<uuid::Uuid>,
    /// The [`crate::build_routing::ramp_caller_target_id`] of the request
    /// that set the ramp.
    caller: Option<uuid::Uuid>,
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
    /// `true` when a guard abort marker holds the `ramp_id` of this
    /// generation. That is the trace of a partial clear.
    abort_marked: bool,
    /// The [`crate::build_routing::ramp_caller_target_id`]s of the requests
    /// that the pools hold for this generation.
    callers: std::collections::BTreeSet<uuid::Uuid>,
}

/// A guard abort marker on one pool: the queue, the base build and the
/// `ramp_id` that the guard cleared.
///
/// A base-build change keeps the `ramp_id` but starts a new step. The base
/// build is part of the marker, so an old marker does not match the new step.
#[cfg(feature = "db")]
type AbortMarker = (String, String, uuid::Uuid);

/// Merge the id-less part of a partly stamped ramp into its stamped part
/// (issue #1814).
///
/// A guard stamps the report id of an id-less ramp on every pool before it
/// clears. It can stop, or fail on one pool, after it stamped only some.
/// A later read then sees one generation with that id and one with none.
/// The stamp does not change a step. So when the derived id over the steps
/// of both parts equals the stamped id, both parts are one ramp. They merge
/// under the stamped id, and no guard derives a second id from the smaller
/// set of pools.
#[cfg(feature = "db")]
fn merge_partial_stamps(merged: &mut std::collections::BTreeMap<GenerationKey, ObservedRamp>) {
    let id_less: Vec<RampKey> = merged
        .keys()
        .filter(|(_, ramp_id)| ramp_id.is_none())
        .map(|(key, _)| key.clone())
        .collect();
    for key in id_less {
        let Some(unstamped) = merged.get(&(key.clone(), None)) else {
            continue;
        };
        let stamped = merged.iter().find_map(|((other, ramp_id), ramp)| {
            let id = (*ramp_id)?;
            if *other != key {
                return None;
            }
            let mut steps: Vec<_> = unstamped.steps.iter().chain(&ramp.steps).copied().collect();
            steps.sort_unstable();
            (id_less_report_id(&key, &steps) == id).then_some(id)
        });
        let Some(id) = stamped else {
            continue;
        };
        let Some(part) = merged.remove(&(key.clone(), None)) else {
            continue;
        };
        if let Some(whole) = merged.get_mut(&(key, Some(id))) {
            whole.ramp_percent = whole.ramp_percent.max(part.ramp_percent);
            whole.base = whole.base.plus(part.base);
            whole.target = whole.target.plus(part.target);
            whole.steps.extend(part.steps);
            whole.steps.sort_unstable();
            whole.callers.extend(part.callers);
        }
    }
}

/// One abort marker as stored on a pool.
#[cfg(feature = "db")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredMarker {
    /// The base build of the cleared ramp.
    base: String,
    /// The `ramp_id` of the cleared ramp.
    id: uuid::Uuid,
    /// The target build of the cleared ramp, when the marker holds it.
    target: Option<String>,
    /// The [`crate::build_routing::ramp_caller_target_id`] of the cleared
    /// ramp, when the marker holds it.
    caller: Option<uuid::Uuid>,
    /// `true` when a guard reported the abort.
    reported: bool,
    /// The age of the marker in milliseconds, by the clock of its own pool.
    age_ms: i64,
    /// The age of the recovery claim in milliseconds, when a guard claimed
    /// the marker.
    claim_age_ms: Option<i64>,
}

/// What one pool holds: its active ramps and its guard abort markers, each
/// with its queue.
#[cfg(feature = "db")]
type PoolRead = (Vec<PoolRamp>, Vec<(String, StoredMarker)>);

/// Read the outcome counts of the two builds of one ramp step on one pool.
#[cfg(feature = "db")]
async fn read_step_stats(
    conn: &mut diesel_async::AsyncPgConnection,
    policy: &PolicyRow,
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

/// The policy columns that the guard reads.
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct PolicyRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    queue_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    build_id: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    updated_at: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    target_build_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    ramp_percent: Option<i32>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    ramp_id: Option<uuid::Uuid>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    ramp_caller_id: Option<uuid::Uuid>,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    ramp_aborted: serde_json::Value,
    /// The pool clock in epoch milliseconds, to age the markers.
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    now_ms: i64,
}

/// Parse the abort markers of one policy row. `now_ms` is the pool clock.
///
/// An entry without a valid `id` or `base` is skipped. An entry without
/// `reported` counts as reported, so it never causes a report. An entry
/// without `at` has age 0.
#[cfg(feature = "db")]
fn abort_markers(
    ramp_aborted: &serde_json::Value,
    now_ms: i64,
) -> impl Iterator<Item = StoredMarker> + '_ {
    ramp_aborted
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(move |entry| {
            let base = entry.get("base")?.as_str()?.to_owned();
            let id = entry.get("id")?.as_str()?.parse().ok()?;
            let target = entry
                .get("target")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let caller = entry
                .get("caller")
                .and_then(serde_json::Value::as_str)
                .and_then(|caller| caller.parse().ok());
            let reported = entry
                .get("reported")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            let age_ms = entry
                .get("at")
                .and_then(serde_json::Value::as_i64)
                .map_or(0, |at| now_ms.saturating_sub(at));
            let claim_age_ms = entry
                .get("claim")
                .and_then(serde_json::Value::as_i64)
                .map(|claim| now_ms.saturating_sub(claim));
            Some(StoredMarker {
                base,
                id,
                target,
                caller,
                reported,
                age_ms,
                claim_age_ms,
            })
        })
}

/// Read every active ramp, its step counts and every abort marker on one pool.
///
/// The reads run in one read-only transaction with `statement_timeout` set to
/// `bound`, so the server stops a slow scan too. A ramp to its own base build
/// is a promotion in progress, not a ramp, so the read skips it.
#[cfg(feature = "db")]
async fn read_pool_ramps(
    pool: &crate::worker::DbPool,
    bound: Duration,
) -> crate::error::HarvestResult<PoolRead> {
    use diesel_async::RunQueryDsl;

    let mut conn = pool.get().await.map_err(crate::error::database_error)?;
    let timeout_ms = bound.as_millis().max(1);
    conn.build_transaction()
        .read_only()
        .run(async |conn| -> crate::error::HarvestResult<PoolRead> {
            diesel::sql_query(format!("SET LOCAL statement_timeout = {timeout_ms}"))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            let policies: Vec<PolicyRow> = diesel::sql_query(
                "SELECT queue_name, build_id, updated_at, target_build_id, ramp_percent, \
                        ramp_id, ramp_caller_id, ramp_aborted, \
                        (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint AS now_ms \
                 FROM harvest_build_policies ORDER BY queue_name",
            )
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;
            let mut ramps = Vec::new();
            let mut markers = Vec::new();
            for policy in policies {
                // A marker stays valid when a newer ramp is active on the
                // same row, so read it first.
                markers.extend(
                    abort_markers(&policy.ramp_aborted, policy.now_ms)
                        .map(|marker| (policy.queue_name.clone(), marker)),
                );
                let (Some(target), Some(percent)) =
                    (policy.target_build_id.clone(), policy.ramp_percent)
                else {
                    continue;
                };
                if percent <= 0 || target == policy.build_id {
                    continue;
                }
                let (base, target_stats) = read_step_stats(conn, &policy, &target).await?;
                let caller = policy.ramp_caller_id.map(|caller| {
                    crate::build_routing::ramp_caller_target_id(caller, &policy.queue_name, &target)
                });
                ramps.push(PoolRamp {
                    key: (policy.queue_name.clone(), policy.build_id.clone(), target),
                    step: policy.updated_at,
                    ramp_id: policy.ramp_id,
                    caller,
                    ramp_percent: percent,
                    base,
                    target: target_stats,
                });
            }
            Ok((ramps, markers))
        })
        .await
}

/// Read every active ramp on every pool and merge the counts per ramp.
///
/// The pools are read at the same time. Returns `None` when any read fails,
/// so a pass never decides on part of the fleet.
///
/// The counts merge per generation, not per ramp key. So the evidence of one
/// `ramp_id` never counts for another.
///
/// A generation is marked when a guard abort marker on the same queue and
/// base build holds its `ramp_id`. The match is by id, so clock skew between pools does not
/// matter. An operator ramp set after the abort has a new id, so it is not
/// marked.
///
/// The read groups the abort markers of one abort over all pools. Every pool
/// was read, so the read knows whether a pool still holds the ramp:
///
/// - When every marker is reported and older than both `report_grace` and
///   [`MIN_MARKER_RETENTION`], the markers can go. The wait lets them finish
///   a late fan-out write. A marker whose ramp a pool still holds stays.
/// - When some are reported, a guard reported the abort and stopped while
///   it marked them. The rest only need the mark.
/// - When none is reported, the abort is due for recovery once a marker is
///   older than `report_grace`. A claim younger than `claim_lease` on any
///   pool holds the whole abort back, because its guard can still report.
#[cfg(feature = "db")]
async fn read_ramps(
    pools: &[crate::worker::DbPool],
    bound: Duration,
    report_grace: Duration,
    claim_lease: Duration,
) -> Option<FleetRead> {
    let reads = pools.iter().map(|pool| read_pool_ramps(pool, bound));
    let mut merged: std::collections::BTreeMap<GenerationKey, ObservedRamp> =
        std::collections::BTreeMap::new();
    let mut markers: std::collections::BTreeSet<AbortMarker> = std::collections::BTreeSet::new();
    let mut pool_markers_by_index = Vec::with_capacity(pools.len());
    for (index, result) in futures::future::join_all(reads)
        .await
        .into_iter()
        .enumerate()
    {
        let (ramps, pool_markers) = match result {
            Ok(read) => read,
            Err(error) => {
                tracing::warn!(pool = index, error = %error, "ramp guard read failed; no verdict this pass");
                return None;
            }
        };
        for ramp in ramps {
            let slot = merged.entry((ramp.key, ramp.ramp_id)).or_default();
            slot.ramp_percent = slot.ramp_percent.max(ramp.ramp_percent);
            slot.base = slot.base.plus(ramp.base);
            slot.target = slot.target.plus(ramp.target);
            slot.steps.push((index, ramp.step));
            slot.callers.extend(ramp.caller);
        }
        markers.extend(
            pool_markers
                .iter()
                .map(|(queue, marker)| (queue.clone(), marker.base.clone(), marker.id)),
        );
        pool_markers_by_index.push(pool_markers);
    }
    merge_partial_stamps(&mut merged);
    for (((queue, base, _), ramp_id), ramp) in &mut merged {
        ramp.abort_marked = ramp_id
            .is_some_and(|ramp_id| markers.contains(&(queue.clone(), base.clone(), ramp_id)));
    }
    let live: std::collections::BTreeSet<AbortMarker> = merged
        .keys()
        .filter_map(|((queue, base, _), ramp_id)| {
            ramp_id.map(|ramp_id| (queue.clone(), base.clone(), ramp_id))
        })
        .collect();
    let (unreported, half_marked, finished_markers, marker_callers) =
        classify_finished_markers(pool_markers_by_index, &live, report_grace, claim_lease);
    Some(FleetRead {
        ramps: merged,
        finished_markers,
        unreported,
        half_marked,
        marker_callers,
    })
}

/// The marker work of one pass: the unreported aborts, the half-marked
/// aborts, the finished markers and the caller ids of every marker.
#[cfg(feature = "db")]
type MarkerWork = (
    Vec<UnreportedAbort>,
    Vec<(String, uuid::Uuid, Vec<usize>)>,
    Vec<(usize, String, Vec<uuid::Uuid>)>,
    MarkerCallers,
);

/// The caller ids of the abort markers on any pool, per queue.
#[cfg(feature = "db")]
type MarkerCallers = std::collections::BTreeMap<String, std::collections::BTreeSet<uuid::Uuid>>;

/// A finished abort that no guard reported, from its unreported markers.
#[cfg(feature = "db")]
#[derive(Debug)]
struct UnreportedAbort {
    queue: String,
    base: String,
    target: Option<String>,
    ramp_id: uuid::Uuid,
    /// The pools that hold an unreported marker for it, in pool order.
    pools: Vec<usize>,
}

/// Group the finished abort markers of every pool per abort, and sort the
/// aborts by what a pass must do with them. See [`read_ramps`].
#[cfg(feature = "db")]
fn classify_finished_markers(
    pool_markers_by_index: Vec<Vec<(String, StoredMarker)>>,
    live: &std::collections::BTreeSet<AbortMarker>,
    report_grace: Duration,
    claim_lease: Duration,
) -> MarkerWork {
    let grace_ms = i64::try_from(report_grace.as_millis()).unwrap_or(i64::MAX);
    let lease_ms = i64::try_from(claim_lease.as_millis()).unwrap_or(i64::MAX);
    let retain_ms =
        i64::try_from(report_grace.max(MIN_MARKER_RETENTION).as_millis()).unwrap_or(i64::MAX);
    // Group the finished markers per abort, in pool order.
    let mut groups: std::collections::BTreeMap<(String, uuid::Uuid), Vec<(usize, StoredMarker)>> =
        std::collections::BTreeMap::new();
    // A pool can still hold the ramp of a marker. Its markers are never
    // pruned, but an unreported abort is still recovered. A guard can lose
    // its pending report in a restart while that pool rejects the clear.
    let mut held = std::collections::BTreeSet::new();
    let mut callers = MarkerCallers::new();
    for (index, pool_markers) in pool_markers_by_index.into_iter().enumerate() {
        for (queue, marker) in pool_markers {
            if let Some(caller) = marker.caller {
                callers.entry(queue.clone()).or_default().insert(caller);
            }
            if live.contains(&(queue.clone(), marker.base.clone(), marker.id)) {
                held.insert((queue.clone(), marker.id));
            }
            groups
                .entry((queue, marker.id))
                .or_default()
                .push((index, marker));
        }
    }
    let mut finished: std::collections::BTreeMap<(usize, String), Vec<uuid::Uuid>> =
        std::collections::BTreeMap::new();
    let mut unreported = Vec::new();
    let mut half_marked = Vec::new();
    for ((queue, ramp_id), entries) in groups {
        let reported = entries.iter().filter(|(_, marker)| marker.reported).count();
        if reported == entries.len() {
            if held.contains(&(queue.clone(), ramp_id)) {
                continue;
            }
            // A ramp fan-out that is still in flight can write this
            // `ramp_id` to a later pool. The markers therefore stay for the
            // report grace, and at least for `MIN_MARKER_RETENTION`, so they
            // can still finish such a late ramp.
            if entries.iter().all(|(_, marker)| marker.age_ms >= retain_ms) {
                for (index, _) in entries {
                    finished
                        .entry((index, queue.clone()))
                        .or_default()
                        .push(ramp_id);
                }
            }
        } else if reported > 0 {
            // A guard reported the abort and stopped while it marked the
            // markers. The rest only need the mark.
            let pools = entries
                .iter()
                .filter(|(_, marker)| !marker.reported)
                .map(|&(index, _)| index)
                .collect();
            half_marked.push((queue, ramp_id, pools));
        } else {
            // A fresh claim on any pool means that a guard reports the abort
            // now, so no pool is eligible.
            let claimed = entries
                .iter()
                .any(|(_, marker)| marker.claim_age_ms.is_some_and(|claim| claim < lease_ms));
            let due = entries.iter().any(|(_, marker)| marker.age_ms >= grace_ms);
            if due && !claimed {
                let target = entries.iter().find_map(|(_, marker)| marker.target.clone());
                let pools = entries.iter().map(|&(index, _)| index).collect();
                let base = entries
                    .into_iter()
                    .next()
                    .map(|(_, marker)| marker.base)
                    .unwrap_or_default();
                unreported.push(UnreportedAbort {
                    queue,
                    base,
                    target,
                    ramp_id,
                    pools,
                });
            }
        }
    }
    let finished = finished
        .into_iter()
        .map(|((index, queue), ids)| (index, queue, ids))
        .collect();
    (unreported, half_marked, finished, callers)
}

/// One read of every pool.
#[cfg(feature = "db")]
struct FleetRead {
    /// The active ramps, merged per generation.
    ramps: std::collections::BTreeMap<GenerationKey, ObservedRamp>,
    /// The finished, reported abort markers: the pool index, the queue and
    /// the marker ids. No pool holds the ramp of such a marker, and every
    /// marker of its abort is reported.
    finished_markers: Vec<(usize, String, Vec<uuid::Uuid>)>,
    /// The aborts that no guard reported within the grace. A pool can still
    /// hold the ramp of such an abort.
    unreported: Vec<UnreportedAbort>,
    /// The finished aborts that a guard reported but did not mark on every
    /// pool. Each holds the queue, the `ramp_id` and the pools that still
    /// need the mark.
    half_marked: Vec<(String, uuid::Uuid, Vec<usize>)>,
    /// The caller ids of the abort markers on any pool, per queue.
    marker_callers: MarkerCallers,
}

/// Write a tombstone for each of `ramp_ids` into the report ledger of one
/// pool, within `bound`. Returns `true` when the write committed.
///
/// The guard calls it on every pool before it prunes the markers of a
/// reported abort. Under the same lock it clears a live ramp with a
/// tombstoned id. Only a write after the fleet read makes such a ramp, for
/// example on a pool that the first fan-out missed. It would otherwise
/// outlive the pruned markers. A ramp write then still refuses the aborted generation
/// on that pool, as [`crate::build_routing::set_build_ramp_with_id`] reads
/// the local ledger. Without the tombstone, only the audit pool would know
/// the abort once the markers are gone. A tombstone row never starts a
/// report. Only the audit pool's ledger elects a reporter, and the guard
/// prunes only the markers of a reported abort.
///
/// The same write retires each of `callers` on the pool, and clears a live
/// ramp of one. Each is a [`crate::build_routing::ramp_caller_target_id`]. A
/// fan-out can miss a pool, so that pool holds no row of the aborted ramp. A
/// late retry there can have another base and so another stored id. Only
/// its caller id and target refuse it.
#[cfg(feature = "db")]
async fn record_abort_tombstones(
    pool: &crate::worker::DbPool,
    index: usize,
    queue: &str,
    ramp_ids: &[uuid::Uuid],
    callers: &[uuid::Uuid],
    bound: Duration,
) -> bool {
    use diesel::sql_types::{Array, Text};
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let timeout_ms = bound.as_millis().max(1);
    let caller_target = crate::build_routing::ramp_caller_target_id_sql(
        "ramp_caller_id",
        "queue_name",
        "target_build_id",
    );
    let caller_target = caller_target.as_str();
    let write = async {
        let mut conn = pool.get().await.map_err(|e| e.to_string())?;
        conn.transaction(async |conn| -> crate::error::HarvestResult<()> {
            for setting in ["lock_timeout", "statement_timeout"] {
                diesel::sql_query(format!("SET LOCAL {setting} = {timeout_ms}"))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            }
            // A ramp writer of the queue holds this lock for its whole write.
            // So no write can read the ledger before the tombstone and the
            // row after the prune.
            crate::build_routing::lock_ramp_generations(conn, queue).await?;
            diesel::sql_query(
                "INSERT INTO harvest_ramp_abort_reports (ramp_id, queue_name) \
                 SELECT id, $1 FROM unnest($2::uuid[]) AS t(id) \
                 ON CONFLICT (ramp_id) DO NOTHING",
            )
            .bind::<Text, _>(queue)
            .bind::<Array<diesel::sql_types::Uuid>, _>(ramp_ids)
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
            diesel::sql_query(
                "INSERT INTO harvest_ramp_retired_ids (queue_name, ramp_id) \
                 SELECT $1, id FROM unnest($2::uuid[]) AS t(id) \
                 ON CONFLICT (queue_name, ramp_id) DO NOTHING",
            )
            .bind::<Text, _>(queue)
            .bind::<Array<diesel::sql_types::Uuid>, _>(callers)
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
            // A late write of an aborted ramp holds a caller id too. Retire it,
            // so a retry after a base change is refused as well.
            diesel::sql_query(format!(
                "INSERT INTO harvest_ramp_retired_ids (queue_name, ramp_id) \
                 SELECT queue_name, {caller_target} FROM harvest_build_policies \
                 WHERE queue_name = $1 AND ramp_id = ANY($2::uuid[]) \
                   AND ramp_caller_id IS NOT NULL \
                 ON CONFLICT (queue_name, ramp_id) DO NOTHING"
            ))
            .bind::<Text, _>(queue)
            .bind::<Array<diesel::sql_types::Uuid>, _>(ramp_ids)
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
            let cleared = diesel::sql_query(format!(
                "UPDATE harvest_build_policies \
                 SET target_build_id = NULL, ramp_percent = NULL, ramp_id = NULL, \
                     ramp_caller_id = NULL, updated_at = NOW() \
                 WHERE queue_name = $1 \
                   AND (ramp_id = ANY($2::uuid[]) OR {caller_target} = ANY($3::uuid[]))"
            ))
            .bind::<Text, _>(queue)
            .bind::<Array<diesel::sql_types::Uuid>, _>(ramp_ids)
            .bind::<Array<diesel::sql_types::Uuid>, _>(callers)
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
            if cleared > 0 {
                tracing::info!(queue = %queue, pool = index, "ramp guard cleared a late write of an aborted ramp");
            }
            Ok(())
        })
        .await
        .map_err(|e| e.to_string())
    };
    match tokio::time::timeout(bound.saturating_mul(2), write).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::warn!(queue = %queue, pool = index, error = %error, "ramp guard tombstone write failed");
            false
        }
        Err(_) => {
            tracing::warn!(queue = %queue, pool = index, "ramp guard tombstone write timed out");
            false
        }
    }
}

/// The report id of an abort of a ramp with no `ramp_id` (issue #1814).
///
/// The id derives from the queue, the base, the target and the step of each
/// pool that holds the ramp, in pool order. Every replica reads the same
/// rows, so every replica derives the same id with no coordination. Two
/// replicas therefore cannot stamp two ids on different pools. A later
/// ramp with the same builds has new steps and so a new id, and no old
/// marker matches it. Each part has a length prefix, as in
/// [`crate::build_routing::ramp_generation_id`].
#[cfg(feature = "db")]
fn id_less_report_id(
    (queue, base, target): &RampKey,
    steps: &[(usize, chrono::DateTime<chrono::Utc>)],
) -> uuid::Uuid {
    use std::fmt::Write as _;

    let mut name = format!(
        "harvest-ramp-abort/{}:{queue}/{}:{base}/{}:{target}",
        queue.len(),
        base.len(),
        target.len()
    );
    for (index, step) in steps {
        let _ = write!(name, "/{index}@{}", step.timestamp_micros());
    }
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, name.as_bytes())
}

/// Give the ramp of one pool step the report id, when it has no `ramp_id`,
/// within `bound`.
///
/// The guard calls it on every pool before it clears a ramp with no
/// `ramp_id`. The clear then writes a marker with the same id, so a pool
/// that did not clear holds a ramp that the marker matches. A later guard
/// can finish that pool after a restart. The write changes only
/// `ramp_id`, so the step and the reset trigger leave the ramp as is. A
/// failure only logs a warning: that pool then stays unmatched, as before.
///
/// Returns the id that the row holds after the write. Another guard can
/// stamp the row first. The caller then adopts that id, so the clear, the
/// report and the mark of both guards use one id.
#[cfg(feature = "db")]
async fn stamp_report_id(
    pool: &crate::worker::DbPool,
    index: usize,
    (queue, base, target): &RampKey,
    step: chrono::DateTime<chrono::Utc>,
    report_id: uuid::Uuid,
    bound: Duration,
) -> Option<uuid::Uuid> {
    use diesel::OptionalExtension;
    use diesel::sql_types::{Nullable, Text, Timestamptz};
    use diesel_async::{AsyncConnection, RunQueryDsl};

    #[derive(diesel::QueryableByName)]
    struct Stamped {
        #[diesel(sql_type = Nullable<diesel::sql_types::Uuid>)]
        ramp_id: Option<uuid::Uuid>,
    }

    let timeout_ms = bound.as_millis().max(1);
    let stamp = async {
        let mut conn = pool.get().await.map_err(|e| e.to_string())?;
        conn.transaction(
            async |conn| -> crate::error::HarvestResult<Option<uuid::Uuid>> {
                for setting in ["lock_timeout", "statement_timeout"] {
                    diesel::sql_query(format!("SET LOCAL {setting} = {timeout_ms}"))
                        .execute(conn)
                        .await
                        .map_err(crate::error::database_error)?;
                }
                let stamped: Option<Stamped> = diesel::sql_query(
                    "UPDATE harvest_build_policies SET ramp_id = $5 \
                     WHERE queue_name = $1 AND build_id = $2 AND target_build_id = $3 \
                       AND updated_at = $4 AND ramp_id IS NULL \
                     RETURNING ramp_id",
                )
                .bind::<Text, _>(queue)
                .bind::<Text, _>(base)
                .bind::<Text, _>(target)
                .bind::<Timestamptz, _>(step)
                .bind::<diesel::sql_types::Uuid, _>(report_id)
                .get_result(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
                if let Some(row) = stamped {
                    return Ok(row.ramp_id);
                }
                // A new statement sees a stamp that another guard committed
                // while this UPDATE waited.
                let current: Option<Stamped> = diesel::sql_query(
                    "SELECT ramp_id FROM harvest_build_policies \
                     WHERE queue_name = $1 AND build_id = $2 AND target_build_id = $3 \
                       AND updated_at = $4",
                )
                .bind::<Text, _>(queue)
                .bind::<Text, _>(base)
                .bind::<Text, _>(target)
                .bind::<Timestamptz, _>(step)
                .get_result(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
                Ok(current.and_then(|row| row.ramp_id))
            },
        )
        .await
        .map_err(|e| e.to_string())
    };
    match tokio::time::timeout(bound.saturating_mul(2), stamp).await {
        Ok(Ok(id)) => id,
        Ok(Err(error)) => {
            tracing::warn!(queue = %queue, pool = index, error = %error, "ramp guard report id stamp failed");
            None
        }
        Err(_) => {
            tracing::warn!(queue = %queue, pool = index, "ramp guard report id stamp timed out");
            None
        }
    }
}

/// Remove finished abort markers from one pool, within `bound`.
///
/// The removal is best effort. A failure logs a warning, and the next pass
/// tries again. A kept marker does no harm, because no pool holds its ramp.
#[cfg(feature = "db")]
async fn prune_finished_markers(
    pool: &crate::worker::DbPool,
    index: usize,
    queue: &str,
    ramp_ids: &[uuid::Uuid],
    bound: Duration,
) {
    use diesel::sql_types::{Array, Text};
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let ids: Vec<String> = ramp_ids.iter().map(ToString::to_string).collect();
    let timeout_ms = bound.as_millis().max(1);
    let prune = async {
        let mut conn = pool.get().await.map_err(|e| e.to_string())?;
        conn.transaction(async |conn| -> crate::error::HarvestResult<()> {
            for setting in ["lock_timeout", "statement_timeout"] {
                diesel::sql_query(format!("SET LOCAL {setting} = {timeout_ms}"))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            }
            diesel::sql_query(prune_abort_markers_query())
                .bind::<Text, _>(queue)
                .bind::<Array<Text>, _>(&ids)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(())
        })
        .await
        .map_err(|e| e.to_string())
    };
    match tokio::time::timeout(bound.saturating_mul(2), prune).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(queue = %queue, pool = index, error = %error, "ramp guard marker prune failed");
        }
        Err(_) => tracing::warn!(queue = %queue, pool = index, "ramp guard marker prune timed out"),
    }
}

/// The result of one compare-and-swap clear on one pool.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClearOutcome {
    /// This call cleared the ramp.
    Cleared,
    /// Another guard cleared the ramp first, so it owns the report.
    Lost,
    /// An operator changed the row first, and no guard cleared this ramp.
    /// Nothing was cleared, and the change says nothing about who reports.
    Moved,
    /// The row changed first, and the guard cannot tell who changed it. The
    /// ramp has no `ramp_id`, or the marker read failed. Like `Moved`, it
    /// does not decide who reports. Like `Lost`, a retry after an ambiguous
    /// attempt counts it as this guard's clear. A duplicate report is better
    /// than an abort with none.
    Changed,
    /// The server failed or stopped the clear, so nothing changed. The guard
    /// retries it.
    Failed,
    /// The client gave up before the server answered. The clear can still
    /// have committed. The guard retries it, and it counts a lost retry as
    /// its own clear.
    Ambiguous,
}

/// Return `true` when a guard abort marker on this pool holds `ramp_id`.
///
/// A lost clear calls this to learn who changed the row. A guard clear sets
/// the marker in the same UPDATE, and an operator change does not.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn ramp_aborted_by_guard(
    conn: &mut diesel_async::AsyncPgConnection,
    queue: &str,
    ramp_id: uuid::Uuid,
) -> crate::error::HarvestResult<bool> {
    use diesel::sql_types::{Bool, Text};
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Bool)]
        marked: bool,
    }

    let row: Row = diesel::sql_query(
        "SELECT EXISTS (SELECT 1 FROM harvest_build_policies \
                        WHERE queue_name = $1 \
                          AND ramp_aborted @> jsonb_build_array( \
                                  jsonb_build_object('id', $2::text))) AS marked",
    )
    .bind::<Text, _>(queue)
    .bind::<diesel::sql_types::Uuid, _>(ramp_id)
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(row.marked)
}

/// Clear one ramp step on one pool.
///
/// The server stops the clear at `bound`. The client waits twice as long, so
/// a client timeout means that the server did not answer at all.
///
/// When the row changed first, the marker tells a guard clear (`Lost`) from
/// an operator change (`Moved`). The marker read has its own timeout of
/// `bound`. A ramp with no `ramp_id`, or a marker read that fails or times
/// out, gives `Changed`. The miss is known, so it is never ambiguous.
#[cfg(feature = "db")]
async fn clear_on_pool(
    pool: &crate::worker::DbPool,
    index: usize,
    (key, ramp_id): (&RampKey, Option<uuid::Uuid>),
    report_id: uuid::Uuid,
    step: chrono::DateTime<chrono::Utc>,
    bound: Duration,
) -> ClearOutcome {
    let (queue, base, target) = key;
    // A checkout that fails or times out sent nothing to the server, so it
    // is a plain failure. Only the clear itself can be ambiguous.
    let mut conn = match tokio::time::timeout(bound, pool.get()).await {
        Ok(Ok(conn)) => conn,
        Ok(Err(error)) => {
            tracing::warn!(queue = %queue, pool = index, error = %error, "ramp guard clear checkout failed");
            return ClearOutcome::Failed;
        }
        Err(_) => {
            tracing::warn!(queue = %queue, pool = index, "ramp guard clear checkout timed out");
            return ClearOutcome::Failed;
        }
    };
    // The client waits twice as long as the server bound for the clear, so
    // only a clear with no answer at all is ambiguous.
    let cas = match tokio::time::timeout(
        bound.saturating_mul(2),
        clear_ramp(&mut conn, queue, base, target, step, bound, report_id),
    )
    .await
    {
        Ok(Ok(marker_id)) => Step::Done(marker_id.is_some()),
        Ok(Err(error)) => {
            tracing::warn!(queue = %queue, pool = index, error = %error, "ramp guard clear failed");
            Step::Failed
        }
        Err(_) => {
            tracing::warn!(queue = %queue, pool = index, "ramp guard clear timed out; outcome unknown");
            Step::TimedOut
        }
    };
    // The marker read runs only after a known miss, under its own timeout.
    let lookup = match (&cas, ramp_id) {
        (Step::Done(false), Some(ramp_id)) => Some(
            match tokio::time::timeout(bound, ramp_aborted_by_guard(&mut conn, queue, ramp_id))
                .await
            {
                Ok(Ok(marked)) => Step::Done(marked),
                Ok(Err(_)) => Step::Failed,
                Err(_) => Step::TimedOut,
            },
        ),
        _ => None,
    };
    clear_outcome(cas, lookup)
}

/// The result of one step of a clear on one pool.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step<T> {
    /// The server answered.
    Done(T),
    /// The server answered with an error.
    Failed,
    /// The client gave up before the server answered.
    TimedOut,
}

/// The outcome of a clear from its compare-and-swap and its marker read.
///
/// `lookup` is `None` when no marker read ran: the clear did not miss, or the
/// ramp has no `ramp_id`. Only a compare-and-swap with no answer is
/// ambiguous. A marker read runs after a known miss, so its failure or
/// timeout gives `Changed`.
#[cfg(feature = "db")]
const fn clear_outcome(cas: Step<bool>, lookup: Option<Step<bool>>) -> ClearOutcome {
    match (cas, lookup) {
        (Step::TimedOut, _) => ClearOutcome::Ambiguous,
        (Step::Failed, _) => ClearOutcome::Failed,
        (Step::Done(true), _) => ClearOutcome::Cleared,
        (Step::Done(false), Some(Step::Done(true))) => ClearOutcome::Lost,
        (Step::Done(false), Some(Step::Done(false))) => ClearOutcome::Moved,
        (Step::Done(false), None | Some(Step::Failed | Step::TimedOut)) => ClearOutcome::Changed,
    }
}

/// Mark the abort markers of `ramp_id` on one pool as reported, within
/// `bound`. Returns `true` when this call changed the row.
///
/// A failure logs a warning and returns `false`. The marker then stays
/// unreported, and a later pass can report the abort a second time. An extra
/// audit row is better than an abort with none.
#[cfg(feature = "db")]
async fn mark_reported_on_pool(
    pool: &crate::worker::DbPool,
    index: usize,
    queue: &str,
    ramp_id: uuid::Uuid,
    bound: Duration,
) -> bool {
    let mark = async {
        let mut conn = pool.get().await.map_err(|e| e.to_string())?;
        mark_abort_reported(&mut conn, queue, ramp_id, bound)
            .await
            .map_err(|e| e.to_string())
    };
    match tokio::time::timeout(bound.saturating_mul(2), mark).await {
        Ok(Ok(changed)) => changed,
        Ok(Err(error)) => {
            tracing::warn!(queue = %queue, pool = index, error = %error, "ramp guard report mark failed");
            false
        }
        Err(_) => {
            tracing::warn!(queue = %queue, pool = index, "ramp guard report mark timed out");
            false
        }
    }
}

/// The result of one recovery claim on one pool.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimOutcome {
    /// This guard took the claim.
    Claimed,
    /// The claim did not apply. Another guard holds a fresh claim, or the
    /// marker is reported or gone.
    Refused,
    /// The claim failed or timed out. Another pool can still take it.
    Failed,
}

/// Claim the recovery of the unreported abort of `ramp_id` on one pool,
/// within `bound`.
///
/// A failure logs a warning and returns [`ClaimOutcome::Failed`].
#[cfg(feature = "db")]
async fn claim_on_pool(
    pool: &crate::worker::DbPool,
    index: usize,
    queue: &str,
    ramp_id: uuid::Uuid,
    lease: Duration,
    bound: Duration,
) -> ClaimOutcome {
    let claim = async {
        let mut conn = pool.get().await.map_err(|e| e.to_string())?;
        claim_unreported_abort(&mut conn, queue, ramp_id, lease, bound)
            .await
            .map_err(|e| e.to_string())
    };
    match tokio::time::timeout(bound.saturating_mul(2), claim).await {
        Ok(Ok(true)) => ClaimOutcome::Claimed,
        Ok(Ok(false)) => ClaimOutcome::Refused,
        Ok(Err(error)) => {
            tracing::warn!(queue = %queue, pool = index, error = %error, "ramp guard recovery claim failed");
            ClaimOutcome::Failed
        }
        Err(_) => {
            tracing::warn!(queue = %queue, pool = index, "ramp guard recovery claim timed out");
            ClaimOutcome::Failed
        }
    }
}

/// The part of an abort's audit summary that names its two builds.
#[cfg(feature = "db")]
fn abort_summary_tag(target: &str, base: &str) -> String {
    format!("target_build={target} base_build={base} ")
}

/// The result of one attempt to report an abort.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReportOutcome {
    /// This guard wrote the ledger row and the audit row.
    Recorded,
    /// The report ledger already held the abort. Another guard reported it.
    AlreadyReported,
    /// The write failed or timed out. Nothing committed.
    Failed,
}

/// Write the ledger row and the audit row of one abort in one transaction,
/// within `bound`.
///
/// For a ramp with a `ramp_id`, the ledger row comes first. When the ledger
/// already holds the abort, the call writes nothing and returns
/// `AlreadyReported`. A ramp with no `ramp_id` has no ledger row, so its
/// report is not deduplicated.
///
/// A failed write logs a warning and does not undo the abort. The summary
/// names a pool that did not clear by its index only, so no database error
/// text reaches the audit log.
#[cfg(feature = "db")]
async fn record_abort(
    audit_pool: &crate::worker::DbPool,
    abort: &RampAbort,
    ramp_id: Option<uuid::Uuid>,
    failed_pools: &[usize],
    bound: Duration,
) -> ReportOutcome {
    let mut summary = format!(
        "reason={} {}ramp_percent={} \
         target_rate={:.4} target_lower_bound={:.4} base_rate={:.4} \
         target_started={} target_settled={} base_started={} base_settled={}",
        abort.reason.as_str(),
        abort_summary_tag(&abort.target_build_id, &abort.base_build_id),
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
        use diesel_async::AsyncConnection;

        let mut conn = audit_pool.get().await.map_err(|e| e.to_string())?;
        conn.transaction(async |conn| -> crate::error::HarvestResult<ReportOutcome> {
            if let Some(ramp_id) = ramp_id
                && !record_abort_report(conn, &abort.queue, ramp_id).await?
            {
                return Ok(ReportOutcome::AlreadyReported);
            }
            crate::audit::insert_audit(conn, &record).await?;
            Ok(ReportOutcome::Recorded)
        })
        .await
        .map_err(|e| e.to_string())
    };
    match tokio::time::timeout(bound, write).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => {
            tracing::warn!(queue = %abort.queue, error = %error, "ramp guard audit write failed");
            ReportOutcome::Failed
        }
        Err(_) => {
            tracing::warn!(queue = %abort.queue, "ramp guard audit write timed out");
            ReportOutcome::Failed
        }
    }
}

/// What a guard pass does with an abort after its clears.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    /// This guard cleared a pool, so it reports the abort now.
    Report,
    /// No pool cleared yet, but a clear failed. The guard reports the abort
    /// when a retry clears a pool.
    Defer,
    /// Another replica or an operator changed the ramp first. This guard
    /// does not report the abort.
    Drop,
}

/// Decide what to do with an abort from the outcomes of its clears.
///
/// `outcomes` is in pool order. The first decisive outcome elects the
/// reporter. A decisive outcome is `Cleared` (this guard won that pool) or
/// `Lost` (another guard won it). A failure, an operator move or an unknown
/// change decides nothing. So two guards that race over the same pools
/// agree on one reporter, on a first attempt and on a retry alike.
///
/// With no decisive outcome, a failed or ambiguous clear defers the report
/// to a retry. A guard therefore never reports a change that did not happen.
#[cfg(feature = "db")]
fn disposition(outcomes: &[ClearOutcome]) -> Disposition {
    let decisive = outcomes
        .iter()
        .find(|outcome| matches!(outcome, ClearOutcome::Cleared | ClearOutcome::Lost));
    match decisive {
        Some(ClearOutcome::Cleared) => Disposition::Report,
        None if outcomes
            .iter()
            .any(|outcome| matches!(outcome, ClearOutcome::Failed | ClearOutcome::Ambiguous)) =>
        {
            Disposition::Defer
        }
        _ => Disposition::Drop,
    }
}

/// Audit, count and log one abort, once over all replicas.
///
/// The ledger row and the audit row commit together. Only `Recorded` counts
/// the metric and logs the abort. After `Recorded` or `AlreadyReported`,
/// the caller marks the markers as reported. A failed write keeps them
/// unreported, so a later pass reports the abort.
#[cfg(feature = "db")]
async fn report_abort(
    abort: &RampAbort,
    ramp_id: Option<uuid::Uuid>,
    audit_pool: &crate::worker::DbPool,
    metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
    failed_pools: &[usize],
    bound: Duration,
) -> ReportOutcome {
    let outcome = record_abort(audit_pool, abort, ramp_id, failed_pools, bound).await;
    if outcome != ReportOutcome::Recorded {
        return outcome;
    }
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
    outcome
}

/// One pool clear to retry: the pool index, the step, and `true` when an
/// earlier attempt was ambiguous.
#[cfg(feature = "db")]
type PendingStep = (usize, chrono::DateTime<chrono::Utc>, bool);

/// The outcome of a retry, given whether an earlier attempt was ambiguous.
///
/// An ambiguous attempt can have committed after the client gave up. A retry
/// then finds the row changed. The change can be this guard's own, so a lost
/// retry after an ambiguous attempt counts as a clear. A report is better
/// than an abort with no audit row.
#[cfg(feature = "db")]
const fn retry_outcome(outcome: ClearOutcome, was_ambiguous: bool) -> ClearOutcome {
    match outcome {
        ClearOutcome::Lost | ClearOutcome::Changed if was_ambiguous => ClearOutcome::Cleared,
        other => other,
    }
}

/// The pool clears that an abort still needs.
#[cfg(feature = "db")]
#[derive(Debug)]
struct PendingAbort {
    /// The `ramp_id` of the aborted generation.
    ramp_id: Option<uuid::Uuid>,
    /// The id of the markers and the report ledger row: the `ramp_id`, or a
    /// fresh id for a ramp with no `ramp_id`.
    report_id: uuid::Uuid,
    /// The pools that did not clear.
    steps: Vec<PendingStep>,
    /// The abort, while no report of it has committed. Without `elected`,
    /// the guard reports it when a retry clears a pool.
    unreported: Option<RampAbort>,
    /// This guard cleared a pool, so it owns the report. A failed report is
    /// then retried on each pass, whatever the pending clears do.
    elected: bool,
}

/// The build ramp guard, with the clears that it must still retry.
///
/// [`run_ramp_guard`] keeps one guard for its whole life. Use
/// [`RampGuard::pass`] directly to drive the guard from another loop.
#[cfg(feature = "db")]
#[derive(Debug)]
pub struct RampGuard {
    config: RampGuardConfig,
    /// Aborted ramp generations with a pool that did not clear. The key is
    /// the whole generation, so a pending clear blocks only its own
    /// generation. A newer ramp with the same builds is still judged.
    pending: std::collections::BTreeMap<GenerationKey, PendingAbort>,
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

    /// The lease of a recovery claim over `pool_count` pools.
    ///
    /// A claimer can still write for one claim, one audit row and one mark
    /// per pool. Each claim and mark waits at most twice the bound, and the
    /// audit write at most one bound. The lease covers all of them, so no
    /// other guard reclaims while the claimer still reports. It is never
    /// shorter than `report_grace`.
    fn claim_lease(&self, pool_count: usize) -> Duration {
        let writes =
            u32::try_from(pool_count.saturating_mul(2).saturating_add(3)).unwrap_or(u32::MAX);
        self.config
            .report_grace()
            .max(self.bound().saturating_mul(writes))
    }

    /// Run one guard pass over every ramp in `pools`.
    ///
    /// `pools` holds one pool per physical database, in a fixed order. The
    /// pass first retries the clears that an earlier pass could not finish.
    /// A retry needs no new verdict.
    ///
    /// The pass then reads every ramp and its step counts, and it aborts each
    /// ramp with an abort verdict. All reads of one pass must end within the
    /// bound. A failed, slow or cancelled read aborts nothing.
    ///
    /// The guard reports an abort only after it cleared a pool itself. It
    /// does not report when it lost the clear on the first pool that holds
    /// the ramp, because another replica owns that report. A report writes
    /// one audit row to `audit_pool`, counts the abort on `metrics` and logs a
    /// warning. Returns the aborts that this pass reported.
    pub async fn pass(
        &mut self,
        pools: &[crate::worker::DbPool],
        audit_pool: &crate::worker::DbPool,
        metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Vec<RampAbort> {
        let bound = self.bound();
        let mut aborts = self
            .retry_pending(pools, audit_pool, metrics, bound, cancel)
            .await;

        let read = tokio::select! {
            () = cancel.cancelled() => return aborts,
            read = tokio::time::timeout(
                bound,
                read_ramps(pools, bound, self.config.report_grace(), self.claim_lease(pools.len())),
            ) => read,
        };
        let Ok(read) = read else {
            tracing::warn!("ramp guard read timed out; no verdict this pass");
            return aborts;
        };
        let Some(FleetRead {
            ramps,
            finished_markers,
            unreported,
            half_marked,
            marker_callers,
        }) = read
        else {
            return aborts;
        };

        for (generation, ramp) in ramps {
            let key = &generation.0;
            // A pending clear blocks its own generation until the retry
            // ends, so one generation never has two pending entries.
            if self.pending.contains_key(&generation) {
                continue;
            }
            if ramp.abort_marked {
                self.finish_marked_abort(pools, &generation, &ramp.steps, bound, cancel)
                    .await;
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
            let (queue, base_build_id, target_build_id) = key.clone();
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
                incomplete: false,
            };
            let callers: Vec<uuid::Uuid> = ramp.callers.iter().copied().collect();
            if let Some(abort) = self
                .abort(
                    pools,
                    audit_pool,
                    metrics,
                    generation,
                    (&ramp.steps, &callers),
                    abort,
                    bound,
                    cancel,
                )
                .await
            {
                aborts.push(abort);
            }
        }
        aborts.extend(
            self.recover_and_prune(
                pools,
                audit_pool,
                metrics,
                (unreported, half_marked, finished_markers, marker_callers),
                bound,
                cancel,
            )
            .await,
        );
        aborts
    }

    /// Report the unreported aborts, finish half-marked ones and remove the
    /// finished markers.
    ///
    /// A cancel starts no new write, so the rest waits for a later pass.
    async fn recover_and_prune(
        &self,
        pools: &[crate::worker::DbPool],
        audit_pool: &crate::worker::DbPool,
        metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
        (unreported, half_marked, finished_markers, marker_callers): MarkerWork,
        bound: Duration,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Vec<RampAbort> {
        let mut aborts = Vec::new();
        for (queue, ramp_id, marker_pools) in half_marked {
            mark_reported(pools, &marker_pools, &queue, Some(ramp_id), bound, cancel).await;
        }
        for lost in unreported {
            if cancel.is_cancelled() {
                return aborts;
            }
            if let Some(abort) = report_unreported(
                pools,
                audit_pool,
                metrics,
                lost,
                self.claim_lease(pools.len()),
                bound,
                cancel,
            )
            .await
            {
                aborts.push(abort);
            }
        }
        // Every pool gets a tombstone of a finished abort before any pool
        // loses its marker. A pool that misses the tombstone keeps the
        // markers of that queue until a later pass writes it. The caller ids
        // of every marker go to every pool on each pass. So a pool that a
        // fan-out missed, or that was down, still retires them.
        let mut finished = MarkerCallers::new();
        for (_, queue, ramp_ids) in &finished_markers {
            finished
                .entry(queue.clone())
                .or_default()
                .extend(ramp_ids.iter().copied());
        }
        let queues: std::collections::BTreeSet<&String> =
            finished.keys().chain(marker_callers.keys()).collect();
        let mut tombstoned = std::collections::BTreeSet::new();
        for queue in queues {
            let ramp_ids: Vec<uuid::Uuid> = finished
                .get(queue)
                .map(|ids| ids.iter().copied().collect())
                .unwrap_or_default();
            let callers: Vec<uuid::Uuid> = marker_callers
                .get(queue)
                .map(|ids| ids.iter().copied().collect())
                .unwrap_or_default();
            let mut all = true;
            for (index, pool) in pools.iter().enumerate() {
                if cancel.is_cancelled()
                    || !record_abort_tombstones(pool, index, queue, &ramp_ids, &callers, bound)
                        .await
                {
                    all = false;
                }
            }
            if all {
                tombstoned.insert(queue.clone());
            }
        }
        for (index, queue, ramp_ids) in finished_markers {
            if cancel.is_cancelled() {
                break;
            }
            if !tombstoned.contains(&queue) {
                continue;
            }
            if let Some(pool) = pools.get(index) {
                prune_finished_markers(pool, index, &queue, &ramp_ids, bound).await;
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
        (key, ramp_id): GenerationKey,
        (steps, callers): (&[(usize, chrono::DateTime<chrono::Utc>)], &[uuid::Uuid]),
        mut abort: RampAbort,
        bound: Duration,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Option<RampAbort> {
        let mut report_id = ramp_id.unwrap_or_else(|| id_less_report_id(&key, steps));
        if ramp_id.is_none() {
            // Every replica derives the same id, so a stamp normally finds
            // no other id. The first id on a row still wins, so a guard that
            // meets another id adopts it for the other pools.
            let mut adopted = None;
            for &(index, step) in steps {
                if cancel.is_cancelled() {
                    break;
                }
                if let Some(pool) = pools.get(index) {
                    let stamp = adopted.unwrap_or(report_id);
                    if let Some(id) = stamp_report_id(pool, index, &key, step, stamp, bound).await {
                        adopted.get_or_insert(id);
                    }
                }
            }
            if let Some(id) = adopted {
                report_id = id;
            }
        }
        let mut outcomes = Vec::with_capacity(steps.len());
        let mut failed: Vec<PendingStep> = Vec::new();
        for &(index, step) in steps {
            // A cancel lets the clear in flight finish and starts no new one.
            // The skipped pool counts as a failed clear.
            let outcome = if cancel.is_cancelled() {
                ClearOutcome::Failed
            } else {
                clear_on_pool(
                    &pools[index],
                    index,
                    (&key, ramp_id),
                    report_id,
                    step,
                    bound,
                )
                .await
            };
            if matches!(outcome, ClearOutcome::Failed | ClearOutcome::Ambiguous) {
                failed.push((index, step, outcome == ClearOutcome::Ambiguous));
            }
            outcomes.push(outcome);
        }
        // Retire the caller ids on every pool now, also on a pool that the
        // fan-out missed. A failed write is retried on the next pass from
        // the markers.
        if !callers.is_empty() && outcomes.contains(&ClearOutcome::Cleared) {
            for (index, pool) in pools.iter().enumerate() {
                if cancel.is_cancelled() {
                    break;
                }
                record_abort_tombstones(pool, index, &key.0, &[], callers, bound).await;
            }
        }
        abort.incomplete = !failed.is_empty();
        let decision = disposition(&outcomes);
        if !failed.is_empty() {
            let unreported = (decision == Disposition::Defer).then(|| abort.clone());
            self.pending.insert(
                (key.clone(), ramp_id),
                PendingAbort {
                    ramp_id,
                    report_id,
                    steps: failed.clone(),
                    unreported,
                    elected: false,
                },
            );
        }
        match decision {
            Disposition::Report => {
                let failed_pools: Vec<usize> = failed.iter().map(|&(index, _, _)| index).collect();
                let outcome = report_abort(
                    &abort,
                    Some(report_id),
                    audit_pool,
                    metrics,
                    &failed_pools,
                    bound,
                )
                .await;
                if outcome == ReportOutcome::Failed {
                    // A pool that stays pending keeps the live ramp, so marker
                    // recovery cannot report the abort. The pending entry
                    // retries the report instead.
                    if let Some(entry) = self.pending.get_mut(&(key.clone(), ramp_id)) {
                        entry.unreported = Some(abort.clone());
                        entry.elected = true;
                    }
                } else {
                    let all: Vec<usize> = (0..pools.len()).collect();
                    mark_reported(pools, &all, &key.0, Some(report_id), bound, cancel).await;
                }
                (outcome == ReportOutcome::Recorded).then_some(abort)
            }
            // Another guard owns the report. The markers of this guard's
            // clears stay unreported until a report commits, so a guard that
            // stops before its report cannot hide the abort.
            Disposition::Drop | Disposition::Defer => None,
        }
    }

    /// Finish a partial clear that an abort marker records, with no new
    /// verdict.
    ///
    /// A guard can stop after it cleared some pools of an abort, for example
    /// in a restart. The other pools then still hold the ramp, and their own
    /// counts can be too few for a verdict. The clear of each cleared pool set
    /// its abort marker in the same UPDATE, so the marker is durable. The
    /// guard clears the other pools too. It writes no new audit row, because
    /// the guard that cleared the first pool reported the abort.
    ///
    /// `steps` holds the steps of the marked generation only. A newer ramp
    /// with the same builds has another generation, so this call does not
    /// clear it.
    async fn finish_marked_abort(
        &mut self,
        pools: &[crate::worker::DbPool],
        (key, ramp_id): &GenerationKey,
        steps: &[(usize, chrono::DateTime<chrono::Utc>)],
        bound: Duration,
        cancel: &tokio_util::sync::CancellationToken,
    ) {
        // A marked generation always has a `ramp_id`.
        let report_id = ramp_id.unwrap_or_else(uuid::Uuid::new_v4);
        let mut failed: Vec<PendingStep> = Vec::new();
        for &(index, step) in steps {
            if cancel.is_cancelled() {
                failed.push((index, step, false));
                continue;
            }
            let outcome = clear_on_pool(
                &pools[index],
                index,
                (key, *ramp_id),
                report_id,
                step,
                bound,
            )
            .await;
            match outcome {
                ClearOutcome::Cleared => {
                    tracing::info!(queue = %key.0, pool = index, "ramp guard finished a marked abort");
                }
                ClearOutcome::Lost | ClearOutcome::Moved | ClearOutcome::Changed => {}
                ClearOutcome::Failed | ClearOutcome::Ambiguous => {
                    failed.push((index, step, outcome == ClearOutcome::Ambiguous));
                }
            }
        }
        if !failed.is_empty() {
            self.pending.insert(
                (key.clone(), *ramp_id),
                PendingAbort {
                    ramp_id: *ramp_id,
                    report_id,
                    steps: failed,
                    unreported: None,
                    elected: false,
                },
            );
        }
    }

    /// Retry the clears that an earlier pass could not finish.
    ///
    /// A pool leaves the list when its clear succeeds or when its row changed.
    /// After a cancel, the retry starts no new clear.
    /// An abort that no clear of this guard had changed yet is reported once a
    /// retry clears a pool. Returns those reports.
    async fn retry_pending(
        &mut self,
        pools: &[crate::worker::DbPool],
        audit_pool: &crate::worker::DbPool,
        metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
        bound: Duration,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Vec<RampAbort> {
        let mut reported = Vec::new();
        let pending = std::mem::take(&mut self.pending);
        for ((key, generation_id), entry) in pending {
            let mut outcomes = Vec::with_capacity(entry.steps.len());
            let mut still_failed: Vec<PendingStep> = Vec::new();
            for (index, step, was_ambiguous) in entry.steps {
                let Some(pool) = pools.get(index) else {
                    continue;
                };
                // A cancel lets the clear in flight finish and starts no new
                // one. The skipped pool stays pending.
                if cancel.is_cancelled() {
                    still_failed.push((index, step, was_ambiguous));
                    outcomes.push(ClearOutcome::Failed);
                    continue;
                }
                let raw = clear_on_pool(
                    pool,
                    index,
                    (&key, entry.ramp_id),
                    entry.report_id,
                    step,
                    bound,
                )
                .await;
                let outcome = retry_outcome(raw, was_ambiguous);
                match outcome {
                    ClearOutcome::Cleared => {
                        tracing::info!(queue = %key.0, pool = index, "ramp guard finished a pending clear");
                    }
                    ClearOutcome::Lost | ClearOutcome::Moved | ClearOutcome::Changed => {}
                    ClearOutcome::Failed | ClearOutcome::Ambiguous => still_failed.push((
                        index,
                        step,
                        was_ambiguous || outcome == ClearOutcome::Ambiguous,
                    )),
                }
                outcomes.push(outcome);
            }
            let mut unreported = entry.unreported;
            let mut elected = entry.elected;
            if let Some(mut abort) = unreported.take() {
                let decision = if elected {
                    Disposition::Report
                } else {
                    disposition(&outcomes)
                };
                match decision {
                    Disposition::Report => {
                        abort.incomplete = !still_failed.is_empty();
                        let failed_pools: Vec<usize> =
                            still_failed.iter().map(|&(index, _, _)| index).collect();
                        let outcome = report_abort(
                            &abort,
                            Some(entry.report_id),
                            audit_pool,
                            metrics,
                            &failed_pools,
                            bound,
                        )
                        .await;
                        if outcome == ReportOutcome::Failed {
                            unreported = Some(abort);
                            elected = true;
                        } else {
                            let all: Vec<usize> = (0..pools.len()).collect();
                            mark_reported(
                                pools,
                                &all,
                                &key.0,
                                Some(entry.report_id),
                                bound,
                                cancel,
                            )
                            .await;
                            if outcome == ReportOutcome::Recorded {
                                reported.push(abort);
                            }
                        }
                    }
                    Disposition::Defer => unreported = Some(abort),
                    Disposition::Drop => {}
                }
            }
            if !still_failed.is_empty() {
                self.pending.insert(
                    (key, generation_id),
                    PendingAbort {
                        ramp_id: entry.ramp_id,
                        report_id: entry.report_id,
                        steps: still_failed,
                        unreported,
                        elected,
                    },
                );
            }
        }
        reported
    }
}

/// Mark the markers of `ramp_id` as reported on each pool in `indices`.
///
/// A ramp with no `ramp_id` has no marker, so the call does nothing. After
/// a cancel the call starts no new write. The markers then stay unreported,
/// and a later pass can report the abort a second time.
#[cfg(feature = "db")]
async fn mark_reported(
    pools: &[crate::worker::DbPool],
    indices: &[usize],
    queue: &str,
    ramp_id: Option<uuid::Uuid>,
    bound: Duration,
    cancel: &tokio_util::sync::CancellationToken,
) {
    let Some(ramp_id) = ramp_id else {
        return;
    };
    for &index in indices {
        if cancel.is_cancelled() {
            return;
        }
        if let Some(pool) = pools.get(index) {
            mark_reported_on_pool(pool, index, queue, ramp_id, bound).await;
        }
    }
}

/// Report a finished abort that no guard reported.
///
/// The guard claims the abort first. It tries the pools that hold an
/// unreported marker, in order. A claim that fails or times out moves to the
/// next pool, so one pool that rejects writes cannot block the report. A
/// refused claim stops the attempt, because another guard holds the abort.
/// The claim is a lease of `lease`. Only the guard that took the claim
/// reports. The report has reason
/// [`RampAbortReason::Unreported`] and no rates, because the verdict is
/// gone. The report ledger makes the report exactly-once. After a committed
/// report, or when the ledger already held it, the guard marks every marker
/// as reported. A guard that stops before that leaves the markers
/// unreported, and after the lease another guard tries again. Returns the
/// abort when this guard reported it.
#[cfg(feature = "db")]
async fn report_unreported(
    pools: &[crate::worker::DbPool],
    audit_pool: &crate::worker::DbPool,
    metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
    lost: UnreportedAbort,
    lease: Duration,
    bound: Duration,
    cancel: &tokio_util::sync::CancellationToken,
) -> Option<RampAbort> {
    let mut claimed = false;
    for &index in &lost.pools {
        if cancel.is_cancelled() {
            return None;
        }
        let Some(pool) = pools.get(index) else {
            continue;
        };
        match claim_on_pool(pool, index, &lost.queue, lost.ramp_id, lease, bound).await {
            ClaimOutcome::Claimed => {
                claimed = true;
                break;
            }
            ClaimOutcome::Refused => return None,
            ClaimOutcome::Failed => {}
        }
    }
    if !claimed {
        return None;
    }
    let abort = RampAbort {
        queue: lost.queue,
        base_build_id: lost.base,
        target_build_id: lost.target.unwrap_or_default(),
        ramp_percent: 0,
        reason: RampAbortReason::Unreported,
        base_rate: 0.0,
        target_rate: 0.0,
        target_lower_bound: 0.0,
        base: BuildOutcomeStats::default(),
        target: BuildOutcomeStats::default(),
        incomplete: false,
    };
    let outcome = report_abort(&abort, Some(lost.ramp_id), audit_pool, metrics, &[], bound).await;
    if outcome != ReportOutcome::Failed {
        mark_reported(
            pools,
            &lost.pools,
            &abort.queue,
            Some(lost.ramp_id),
            bound,
            cancel,
        )
        .await;
    }
    (outcome == ReportOutcome::Recorded).then_some(abort)
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
/// its read, which has no side effects. After a cancel, a pass lets the clear
/// in flight finish and starts no new clear. It still writes the audit row of
/// a clear that it made, within its bound. A clear that it skipped stays
/// recoverable, because each finished clear set its abort marker. An overdue
/// tick fires at once, and the next tick comes one interval after it.
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

    #[cfg(feature = "db")]
    #[test]
    fn only_a_clear_with_no_answer_is_ambiguous() {
        use super::Step::{Done, Failed, TimedOut};
        assert_eq!(clear_outcome(TimedOut, None), ClearOutcome::Ambiguous);
        assert_eq!(clear_outcome(Failed, None), ClearOutcome::Failed);
        assert_eq!(clear_outcome(Done(true), None), ClearOutcome::Cleared);
        assert_eq!(clear_outcome(Done(false), None), ClearOutcome::Changed);
        assert_eq!(
            clear_outcome(Done(false), Some(Done(true))),
            ClearOutcome::Lost
        );
        assert_eq!(
            clear_outcome(Done(false), Some(Done(false))),
            ClearOutcome::Moved
        );
        assert_eq!(
            clear_outcome(Done(false), Some(Failed)),
            ClearOutcome::Changed
        );
        // The miss is known, so a slow marker read cannot make it ambiguous.
        // A retry would count an ambiguous miss as this guard's clear.
        assert_eq!(
            clear_outcome(Done(false), Some(TimedOut)),
            ClearOutcome::Changed
        );
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
        // The clear never evicts a marker. Only a prune of a finished
        // marker removes one, and a prune keeps the step.
        assert!(!abort_sql.contains("jsonb_path_query_array"));
        let prune_sql = prune_abort_markers_query();
        assert!(prune_sql.contains("$1") && prune_sql.contains("$2"));
        assert!(!prune_sql.contains("updated_at"), "a prune keeps the step");
        assert!(
            abort_sql.contains("'reported', false"),
            "a clear writes an unreported marker"
        );
        let mark_sql = mark_abort_reported_query();
        assert!(!mark_sql.contains("updated_at"), "a mark keeps the step");
        assert!(
            mark_sql.contains("'reported', false"),
            "only an unreported marker matches"
        );
        let claim_sql = claim_unreported_abort_query();
        assert!(!claim_sql.contains("updated_at"), "a claim keeps the step");
        assert!(
            !claim_sql.contains("'{reported}'"),
            "a claim leaves the marker unreported"
        );
        assert!(claim_sql.contains("- $3"), "a claim is a lease");
    }

    #[cfg(feature = "db")]
    #[test]
    fn abort_markers_parse_each_valid_entry() {
        let id = uuid::Uuid::new_v4();
        let list = serde_json::json!([
            {"id": id.to_string(), "base": "a"},
            {"id": "not-a-uuid", "base": "a"},
            {"base": "a"},
            {"id": id.to_string()},
        ]);
        let markers: Vec<_> = abort_markers(&list, 0).collect();
        assert_eq!(
            markers,
            vec![StoredMarker {
                base: "a".to_owned(),
                id,
                target: None,
                caller: None,
                reported: true,
                age_ms: 0,
                claim_age_ms: None,
            }]
        );
        assert_eq!(abort_markers(&serde_json::json!({}), 0).count(), 0);
        // The report state, the target and the age come from the entry.
        let list = serde_json::json!([
            {"id": id.to_string(), "base": "a", "target": "b", "reported": false, "at": 400},
        ]);
        let marker = abort_markers(&list, 1000).next().expect("one marker");
        assert!(!marker.reported);
        assert_eq!(marker.target.as_deref(), Some("b"));
        assert_eq!(marker.age_ms, 600);
        assert_eq!(marker.claim_age_ms, None);
        let list = serde_json::json!([
            {"id": id.to_string(), "base": "a", "reported": false, "at": 0, "claim": 900},
        ]);
        let marker = abort_markers(&list, 1000).next().expect("one marker");
        assert_eq!(marker.claim_age_ms, Some(100));
        // The caller id comes from the entry too.
        let caller = uuid::Uuid::new_v4();
        let list = serde_json::json!([
            {"id": id.to_string(), "base": "a", "caller": caller.to_string()},
        ]);
        let marker = abort_markers(&list, 0).next().expect("one marker");
        assert_eq!(marker.caller, Some(caller));
    }

    #[cfg(feature = "db")]
    #[test]
    fn disposition_reports_only_after_this_guard_cleared_a_pool() {
        use ClearOutcome::{Cleared, Failed, Lost};
        // A clear on any pool, with the first pool not lost: report.
        assert_eq!(disposition(&[Cleared, Failed]), Disposition::Report);
        assert_eq!(disposition(&[Failed, Cleared]), Disposition::Report);
        // The first pool was lost to another replica: it owns the report.
        assert_eq!(disposition(&[Lost, Cleared]), Disposition::Drop);
        // A failed first pool with a lost later pool: another guard won.
        assert_eq!(disposition(&[Failed, Lost]), Disposition::Drop);
        // Nothing decisive, but a clear failed: wait for a retry.
        assert_eq!(disposition(&[Failed, Failed]), Disposition::Defer);
        // Every clear lost: report nothing.
        assert_eq!(disposition(&[Lost, Lost]), Disposition::Drop);
        assert_eq!(disposition(&[]), Disposition::Drop);
        // A retry follows the same rule: a lost first pool means another
        // guard won.
        assert_eq!(disposition(&[Lost, Cleared]), Disposition::Drop);
        assert_eq!(disposition(&[Lost]), Disposition::Drop);
        assert_eq!(disposition(&[Failed]), Disposition::Defer);
        // A client timeout is unknown, so it defers like a failure.
        assert_eq!(disposition(&[ClearOutcome::Ambiguous]), Disposition::Defer);
    }

    #[cfg(feature = "db")]
    #[test]
    fn an_operator_change_on_the_first_pool_does_not_decide_the_reporter() {
        use ClearOutcome::{Changed, Cleared, Failed, Lost, Moved};
        // The first decisive outcome elects the reporter. A failure, an
        // operator move or an unknown change on an earlier pool decides
        // nothing, so a later lost clear still means another guard won.
        assert_eq!(disposition(&[Failed, Lost, Cleared]), Disposition::Drop);
        assert_eq!(disposition(&[Moved, Lost, Cleared]), Disposition::Drop);
        assert_eq!(disposition(&[Failed, Cleared, Lost]), Disposition::Report);
        // The same holds on a retry.
        assert_eq!(disposition(&[Lost, Cleared]), Disposition::Drop);
        // An unknown change, such as on a ramp with no id, behaves the same.
        assert_eq!(disposition(&[Changed, Cleared]), Disposition::Report);
        assert_eq!(disposition(&[Changed]), Disposition::Drop);
        assert_eq!(retry_outcome(Changed, true), Cleared);
        assert_eq!(retry_outcome(Changed, false), Changed);
        // An operator moved the first pool, and this guard cleared another.
        assert_eq!(disposition(&[Moved, Cleared]), Disposition::Report);
        assert_eq!(disposition(&[Moved, Failed]), Disposition::Defer);
        assert_eq!(disposition(&[Moved, Moved]), Disposition::Drop);
        // Another guard cleared the first pool: it still owns the report.
        assert_eq!(disposition(&[Lost, Moved, Cleared]), Disposition::Drop);
    }

    #[cfg(feature = "db")]
    #[test]
    fn a_lost_retry_after_an_ambiguous_clear_counts_as_this_guards_clear() {
        use ClearOutcome::{Ambiguous, Cleared, Failed, Lost};
        assert_eq!(retry_outcome(Lost, true), Cleared);
        assert_eq!(retry_outcome(Lost, false), Lost);
        // A moved row has no guard marker, so the ambiguous clear did not
        // commit.
        for outcome in [Cleared, Failed, Ambiguous, ClearOutcome::Moved] {
            assert_eq!(retry_outcome(outcome, true), outcome);
        }
        // So an unreported abort is reported, not dropped.
        assert_eq!(
            disposition(&[retry_outcome(Lost, true)]),
            Disposition::Report
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn abort_summary_tag_names_both_builds() {
        assert_eq!(
            abort_summary_tag("b2", "b1"),
            "target_build=b2 base_build=b1 "
        );
    }

    #[test]
    fn abort_reason_labels_are_stable() {
        assert_eq!(RampAbortReason::FailureRate.as_str(), "failure_rate");
        assert_eq!(RampAbortReason::NdBlockRate.as_str(), "nd_block_rate");
    }
}
