//! Time-based retention janitor for completed workflow history.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, RunQueryDsl};
use futures::future::join_all;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{HarvestError, HarvestResult, database_error};
use crate::schema::harvest_workflow_executions;
use crate::schema::{harvest_dag_runs, harvest_signals, harvest_task_queue, harvest_timers};
use crate::shard::ShardedDbPool;
use crate::telemetry::MetricsRecorder;
use crate::types::ShardId;

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(60 * 60);
const DEFAULT_BATCH_SIZE: usize = 1_000;
const MIN_MAX_AGE: Duration = Duration::from_secs(1);
const MAX_MAX_AGE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 10);
const TERMINAL_STATES: [&str; 5] = [
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMED_OUT",
    "CONTINUED_AS_NEW",
];

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

pub struct RetentionRuntime {
    shutdown: CancellationToken,
    trigger_tx: mpsc::UnboundedSender<()>,
    handle: JoinHandle<()>,
    monitor: RetentionMonitor,
}

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
                    async move {
                        let started = Instant::now();
                        let tick =
                            run_shard_tick(pool, shard, cutoff, &config, Arc::clone(&metrics))
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

#[derive(Debug, Default)]
struct ShardTickOutcome {
    candidate_count: usize,
    deleted_count: usize,
    oldest_age_secs_skipped: Option<u64>,
}

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

async fn run_shard_tick(
    pool: crate::worker::DbPool,
    shard: ShardId,
    cutoff: DateTime<Utc>,
    config: &RetentionConfig,
    _metrics: Arc<dyn MetricsRecorder>,
) -> HarvestResult<ShardTickOutcome> {
    let mut conn = pool
        .get()
        .await
        .map_err(|error| HarvestError::Database(error.to_string()))?;
    let candidates = diesel::sql_query(
        "SELECT id, workflow_name, workflow_id, completed_at
         FROM harvest_workflow_executions
         WHERE state = ANY($1)
           AND completed_at IS NOT NULL
           AND completed_at < $2
         ORDER BY completed_at ASC
         LIMIT $3",
    )
    .bind::<diesel::sql_types::Array<Text>, _>(TERMINAL_STATES.to_vec())
    .bind::<Timestamptz, _>(cutoff)
    .bind::<BigInt, _>(config.batch_size as i64)
    .load::<CandidateExecution>(&mut conn)
    .await
    .map_err(database_error)?;

    let mut outcome = ShardTickOutcome {
        candidate_count: candidates.len(),
        ..ShardTickOutcome::default()
    };

    for candidate in candidates {
        if should_skip_candidate(&mut conn, &candidate, cutoff).await? {
            if let Some(completed_at) = candidate.completed_at {
                let age = (Utc::now() - completed_at).num_seconds().max(0) as u64;
                outcome.oldest_age_secs_skipped = Some(
                    outcome
                        .oldest_age_secs_skipped
                        .map_or(age, |existing| existing.max(age)),
                );
            }
            continue;
        }

        if config.dry_run {
            outcome.deleted_count += 1;
            continue;
        }

        conn.transaction::<_, HarvestError, _>(|conn| {
            Box::pin(async move {
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

    tracing::debug!(shard = %shard, candidates = outcome.candidate_count, deleted = outcome.deleted_count, "retention shard tick");
    Ok(outcome)
}

async fn should_skip_candidate(
    conn: &mut diesel_async::AsyncPgConnection,
    candidate: &CandidateExecution,
    cutoff: DateTime<Utc>,
) -> HarvestResult<bool> {
    let active_parent_ref_count = diesel::sql_query(
        "SELECT COUNT(*) AS count
         FROM harvest_workflow_executions
         WHERE parent_id = $1
           AND state NOT IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW')",
    )
    .bind::<SqlUuid, _>(candidate.id)
    .get_result::<CountRow>(conn)
    .await
    .map_err(database_error)?
    .count;

    if active_parent_ref_count > 0 {
        return Ok(true);
    }

    let inflight_dag_run_count = harvest_dag_runs::table
        .filter(harvest_dag_runs::workflow_exec_id.eq(Some(candidate.id)))
        .filter(harvest_dag_runs::state.eq_any(["QUEUED", "RUNNING"]))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    if inflight_dag_run_count > 0 {
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

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}
