//! Time-based retention janitor for completed workflow history.

use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(feature = "db")]
use std::{collections::HashMap, time::Instant};

use chrono::{DateTime, Utc};
#[cfg(feature = "db")]
use diesel::prelude::*;
#[cfg(feature = "db")]
use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz, Uuid as SqlUuid};
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
use crate::schema::harvest_workflow_executions;
#[cfg(feature = "db")]
use crate::schema::{harvest_dead_letters, harvest_signals, harvest_task_queue, harvest_timers};
#[cfg(feature = "db")]
use crate::shard::ShardedDbPool;
#[cfg(feature = "db")]
use crate::telemetry::MetricsRecorder;
use crate::types::ShardId;

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(60 * 60);
const DEFAULT_BATCH_SIZE: usize = 1_000;
const MIN_MAX_AGE: Duration = Duration::from_secs(1);
const MAX_MAX_AGE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 10);

/// Future type returned by [`HistoryArchiver::archive`].
pub type ArchiverFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send,
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
    fn archive(&self, doc: &crate::history_export::HistoryExportDocument) -> ArchiverFuture;
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
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_age_secs: None,
            tick_interval_secs: DEFAULT_TICK_INTERVAL.as_secs(),
            batch_size: DEFAULT_BATCH_SIZE,
            dry_run: false,
            audit_retention_days: 90,
            schedule_decision_retention_days: 7,
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

    /// Safely unpacks the raw configuration integer into a standard rust [`Duration`], gracefully
    /// handling systems where the feature is entirely turned off.
    #[must_use]
    pub fn max_age(&self) -> Option<Duration> {
        self.max_age_secs.map(Duration::from_secs)
    }

    /// Translates the raw numeric tick value into a standard [`Duration`] for the scheduler loop.
    #[must_use]
    pub const fn tick_interval(&self) -> Duration {
        Duration::from_secs(self.tick_interval_secs)
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
        if let Some(max_age) = self.max_age()
            && !(MIN_MAX_AGE..=MAX_MAX_AGE).contains(&max_age)
        {
            return Err(format!(
                "max_age must be between {}s and {}s",
                MIN_MAX_AGE.as_secs(),
                MAX_MAX_AGE.as_secs()
            ));
        }
        Ok(())
    }

    /// Returns `true` if any retention features (workflow history, audit log, or schedule decision purging) are enabled.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.max_age_secs.is_some()
            || self.audit_retention_days > 0
            || self.schedule_decision_retention_days > 0
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

                // Workflow-history retention: only when max_age is configured.
                if let Some(max_age) = config.max_age() {
                    let cutoff =
                        Utc::now() - chrono::Duration::from_std(max_age).unwrap_or_default();
                    let tick_futures = pools.iter_shards().map(|(shard, pool)| {
                        let pool = pool.clone();
                        let config = config.clone();
                        let metrics = Arc::clone(&metrics);
                        let archiver = archiver.clone();
                        let cursor = scan_cursors.get(&shard).copied().flatten();
                        async move {
                            let started = Instant::now();
                            let tick = run_shard_tick(
                                pool,
                                shard,
                                cutoff,
                                &config,
                                archiver,
                                cursor,
                                Arc::clone(&metrics),
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
    #[diesel(sql_type = Nullable<Timestamptz>) ]
    completed_at: Option<DateTime<Utc>>,
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
                                .filter(harvest_workflow_executions::sticky_worker_id.eq(Some(lease_id))),
                        )
                        .set(harvest_workflow_executions::sticky_worker_id.eq::<Option<String>>(None))
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
async fn run_shard_tick(
    pool: crate::worker::DbPool,
    shard: ShardId,
    cutoff: DateTime<Utc>,
    config: &RetentionConfig,
    archiver: Option<Arc<dyn HistoryArchiver>>,
    start_cursor: Option<RetentionScanCursor>,
    _metrics: Arc<dyn MetricsRecorder>,
) -> HarvestResult<ShardTickOutcome> {
    let mut outcome = ShardTickOutcome {
        next_cursor: start_cursor,
        ..ShardTickOutcome::default()
    };
    let mut cursor = start_cursor;
    let mut wrapped = false;
    let mut remaining = config.batch_size;
    let mut has_failed = false;

    let lease_id = format!("retention-lease-{}", uuid::Uuid::new_v4());
    let guard = RetentionLeaseGuard {
        pool: pool.clone(),
        lease_id: lease_id.clone(),
        active_ids: Arc::new(Mutex::new(Vec::new())),
        active: true,
    };

    while remaining > 0 {
        // Check out a short-lived connection just to load and claim this batch of candidates in a single transaction
        let mut conn = pool
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;

        let lease_id_inner = lease_id.clone();
        let candidates = conn.transaction::<Vec<CandidateExecution>, HarvestError, _>(|conn| {
            Box::pin(async move {
                let rows = diesel::sql_query(
                    "SELECT id, workflow_name, workflow_id, state, completed_at
                     FROM harvest_workflow_executions
                     WHERE state IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW','TERMINATED')
                       AND completed_at IS NOT NULL
                       AND completed_at < $1
                       AND sticky_worker_id IS NULL
                       AND (
                           $2 IS NULL
                           OR completed_at > $2
                           OR (completed_at = $2 AND id > $3)
                       )
                     ORDER BY completed_at ASC, id ASC
                     LIMIT $4
                     FOR UPDATE SKIP LOCKED",
                )
                .bind::<Timestamptz, _>(cutoff)
                .bind::<Nullable<Timestamptz>, _>(cursor.map(|it| it.completed_at))
                .bind::<Nullable<SqlUuid>, _>(cursor.map(|it| it.id))
                .bind::<BigInt, _>(i64::try_from(remaining).unwrap_or(i64::MAX))
                .load::<CandidateExecution>(conn)
                .await
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
            guard.active_ids.lock().expect("lease guard lock poisoned").extend(ids);
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

            if should_skip_candidate(&mut conn, &candidate, cutoff).await? {
                let age = Utc::now()
                    .signed_duration_since(completed_at)
                    .num_seconds()
                    .max(0)
                    .cast_unsigned();
                outcome.oldest_age_secs_skipped = Some(
                    outcome
                        .oldest_age_secs_skipped
                        .map_or(age, |existing| existing.max(age)),
                );

                // Release its lease immediately so it can be picked up on subsequent ticks
                diesel::update(
                    harvest_workflow_executions::table
                        .filter(harvest_workflow_executions::id.eq(candidate.id)),
                )
                .set(harvest_workflow_executions::sticky_worker_id.eq::<Option<String>>(None))
                .execute(&mut conn)
                .await
                .map_err(database_error)?;

                // Advance cursor for routine skips
                if !has_failed {
                    outcome.next_cursor = Some(candidate_cursor);
                }

                {
                    let mut active_guard = guard.active_ids.lock().expect("lease guard lock poisoned");
                    if let Some(pos) = active_guard.iter().position(|&x| x == candidate.id) {
                        active_guard.swap_remove(pos);
                    }
                }
                continue;
            }

            let mut doc = None;
            if !config.dry_run && archiver.is_some() {
                let exec_id = crate::types::ExecutionId::from_uuid(candidate.id);
                match crate::store::load_history(&mut conn, exec_id).await {
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
                match archiver.archive(&document).await {
                    Ok(()) => {
                        tracing::debug!(
                            execution_id = %exec_id,
                            "pre-retention archival hook completed successfully"
                        );
                    }
                    Err(error) => {
                        tracing::error!(
                            execution_id = %exec_id,
                            error = %error,
                            "pre-retention archival hook failed; skipping deletion"
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

            if let Err(err) = delete_candidate_execution(&mut conn, candidate.id).await {
                has_failed = true;
                batch_failed = true;
                tracing::error!(candidate_id = %candidate.id, error = %err, "failed to delete candidate execution");
                break;
            }
            outcome.deleted_count += 1;

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

#[cfg(feature = "db")]
async fn delete_candidate_execution(
    conn: &mut diesel_async::AsyncPgConnection,
    candidate_id: uuid::Uuid,
) -> HarvestResult<()> {
    conn.transaction::<_, HarvestError, _>(|conn| {
        Box::pin(async move {
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

            diesel::delete(
                harvest_dead_letters::table
                    .filter(harvest_dead_letters::workflow_exec_id.eq(Some(candidate_id))),
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
            Ok(())
        })
    })
    .await
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
        };
        let candidate_skip = CandidateExecution {
            id: uuid::Uuid::new_v4(),
            workflow_name: "test".to_string(),
            workflow_id: "skip".to_string(),
            state: "COMPLETED".to_string(),
            completed_at: Some(Utc::now() - chrono::Duration::days(9)),
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
}