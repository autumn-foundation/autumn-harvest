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
use crate::schema::{
    harvest_dag_runs, harvest_dead_letters, harvest_signals, harvest_task_queue, harvest_timers,
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
#[derive(Debug, Clone, Serialize)]
pub struct RetentionConfig {
    pub max_age_secs: Option<u64>,
    pub tick_interval_secs: u64,
    pub batch_size: usize,
    pub dry_run: bool,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_age_secs: None,
            tick_interval_secs: DEFAULT_TICK_INTERVAL.as_secs(),
            batch_size: DEFAULT_BATCH_SIZE,
            dry_run: false,
        }
    }
}

impl RetentionConfig {
    #[must_use]
    pub fn with_max_age(max_age: Duration) -> Self {
        Self {
            max_age_secs: Some(max_age.as_secs()),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn max_age(&self) -> Option<Duration> {
        self.max_age_secs.map(Duration::from_secs)
    }

    #[must_use]
    pub const fn tick_interval(&self) -> Duration {
        Duration::from_secs(self.tick_interval_secs)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.tick_interval_secs == 0 {
            return Err("tick_interval must be >= 1s".to_string());
        }
        if self.batch_size == 0 {
            return Err("batch_size must be >= 1".to_string());
        }
        if let Some(max_age) = self.max_age() {
            if !(MIN_MAX_AGE..=MAX_MAX_AGE).contains(&max_age) {
                return Err(format!(
                    "max_age must be between {}s and {}s",
                    MIN_MAX_AGE.as_secs(),
                    MAX_MAX_AGE.as_secs()
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.max_age_secs.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RetentionTickResult {
    pub shard: u16,
    pub ran_at: Option<DateTime<Utc>>,
    pub candidate_count: usize,
    pub deleted_count: usize,
    pub oldest_age_secs_skipped: Option<u64>,
    pub duration_ms: u128,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RetentionStatus {
    pub config: RetentionConfig,
    pub per_shard: Vec<RetentionTickResult>,
}

#[derive(Debug, Clone)]
pub struct RetentionMonitor {
    inner: Arc<Mutex<RetentionStatus>>,
}

impl RetentionMonitor {
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

#[cfg(feature = "db")]
pub struct RetentionRuntime {
    shutdown: CancellationToken,
    trigger_tx: mpsc::UnboundedSender<()>,
    handle: JoinHandle<()>,
    monitor: RetentionMonitor,
}

#[cfg(feature = "db")]
impl RetentionRuntime {
    #[must_use]
    pub fn spawn(
        pools: ShardedDbPool,
        config: RetentionConfig,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Option<Self> {
        if !config.enabled() {
            return None;
        }
        let monitor = RetentionMonitor::new(config.clone(), pools.shard_ids().into_iter());
        let shutdown = CancellationToken::new();
        let shutdown_task = shutdown.clone();
        let monitor_task = monitor.clone();
        let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            let mut scan_cursors: HashMap<ShardId, Option<RetentionScanCursor>> = HashMap::new();
            loop {
                tokio::select! {
                    () = shutdown_task.cancelled() => break,
                    _ = tokio::time::sleep(config.tick_interval()) => {},
                    Some(_) = trigger_rx.recv() => {
                        while trigger_rx.try_recv().is_ok() {}
                    }
                }

                let cutoff = Utc::now()
                    - chrono::Duration::from_std(
                        config.max_age().expect("enabled config has max_age"),
                    )
                    .unwrap_or_default();
                let tick_futures = pools.iter_shards().map(|(shard, pool)| {
                    let pool = pool.clone();
                    let config = config.clone();
                    let metrics = Arc::clone(&metrics);
                    let cursor = scan_cursors.get(&shard).copied().flatten();
                    async move {
                        let started = Instant::now();
                        let tick = run_shard_tick(
                            pool,
                            shard,
                            cutoff,
                            &config,
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
        });

        Some(Self {
            shutdown,
            trigger_tx,
            handle,
            monitor,
        })
    }

    #[must_use]
    pub fn monitor(&self) -> RetentionMonitor {
        self.monitor.clone()
    }

    pub fn run_now(&self) {
        let _ = self.trigger_tx.send(());
    }

    #[must_use]
    pub fn trigger_sender(&self) -> mpsc::UnboundedSender<()> {
        self.trigger_tx.clone()
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

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
    #[diesel(sql_type = Nullable<Timestamptz>) ]
    completed_at: Option<DateTime<Utc>>,
}

#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy)]
struct RetentionScanCursor {
    completed_at: DateTime<Utc>,
    id: uuid::Uuid,
}

#[cfg(feature = "db")]
async fn run_shard_tick(
    pool: crate::worker::DbPool,
    shard: ShardId,
    cutoff: DateTime<Utc>,
    config: &RetentionConfig,
    start_cursor: Option<RetentionScanCursor>,
    _metrics: Arc<dyn MetricsRecorder>,
) -> HarvestResult<ShardTickOutcome> {
    let mut conn = pool
        .get()
        .await
        .map_err(|error| HarvestError::Database(error.to_string()))?;
    let mut outcome = ShardTickOutcome::default();
    let mut cursor = start_cursor;
    let mut wrapped = false;
    let mut remaining = config.batch_size;
    let mut saw_skipped_candidate = false;

    while remaining > 0 {
        let candidates = diesel::sql_query(
            "SELECT id, workflow_name, workflow_id, completed_at
             FROM harvest_workflow_executions
             WHERE state IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW')
               AND completed_at IS NOT NULL
               AND completed_at < $1
               AND (
                   $2 IS NULL
                   OR completed_at > $2
                   OR (completed_at = $2 AND id > $3)
               )
             ORDER BY completed_at ASC, id ASC
             LIMIT $4",
        )
        .bind::<Timestamptz, _>(cutoff)
        .bind::<Nullable<Timestamptz>, _>(cursor.map(|it| it.completed_at))
        .bind::<Nullable<SqlUuid>, _>(cursor.map(|it| it.id))
        .bind::<BigInt, _>(remaining as i64)
        .load::<CandidateExecution>(&mut conn)
        .await
        .map_err(database_error)?;

        if candidates.is_empty() {
            if cursor.is_some() && !wrapped {
                cursor = None;
                wrapped = true;
                continue;
            }
            outcome.next_cursor = cursor;
            break;
        }

        for candidate in candidates {
            let completed_at = candidate
                .completed_at
                .expect("retention candidate query enforces completed_at IS NOT NULL");
            cursor = Some(RetentionScanCursor {
                completed_at,
                id: candidate.id,
            });
            outcome.next_cursor = cursor;
            outcome.candidate_count += 1;
            remaining = remaining.saturating_sub(1);

            if should_skip_candidate(&mut conn, &candidate, cutoff).await? {
                let age = Utc::now()
                    .signed_duration_since(completed_at)
                    .num_seconds()
                    .max(0) as u64;
                saw_skipped_candidate = true;
                outcome.oldest_age_secs_skipped = Some(
                    outcome
                        .oldest_age_secs_skipped
                        .map_or(age, |existing| existing.max(age)),
                );
                continue;
            }

            if config.dry_run {
                outcome.deleted_count += 1;
                continue;
            }

            conn.transaction::<_, HarvestError, _>(|conn| {
                Box::pin(async move {
                    diesel::delete(
                        harvest_dead_letters::table
                            .filter(harvest_dead_letters::workflow_exec_id.eq(Some(candidate.id))),
                    )
                    .execute(conn)
                    .await
                    .map_err(database_error)?;

                    diesel::delete(
                        harvest_workflow_executions::table
                            .filter(harvest_workflow_executions::id.eq(candidate.id)),
                    )
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                    Ok(())
                })
            })
            .await?;
            outcome.deleted_count += 1;
        }
    }

    if saw_skipped_candidate {
        outcome.next_cursor = None;
    }

    tracing::debug!(shard = %shard, candidates = outcome.candidate_count, deleted = outcome.deleted_count, "retention shard tick");
    Ok(outcome)
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
         WHERE parent_id = $1",
    )
    .bind::<SqlUuid, _>(candidate.id)
    .get_result::<CountRow>(conn)
    .await
    .map_err(database_error)?
    .count;

    if active_parent_ref_count > 0 {
        return Ok(true);
    }

    let dag_run_ref_count = harvest_dag_runs::table
        .filter(harvest_dag_runs::workflow_exec_id.eq(Some(candidate.id)))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    if dag_run_ref_count > 0 {
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
               state NOT IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW')
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
