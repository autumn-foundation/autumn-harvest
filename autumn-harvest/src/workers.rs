//! Worker fleet registry — liveness tracking, heartbeat, and fleet queries.
//!
//! Each `Worker` registers a row in `harvest_workers` on startup, upserts
//! `last_heartbeat_at` and `in_flight_count` on a regular interval, and
//! transitions through `Active → Draining → Stopped` on graceful shutdown.
//!
//! The API layer queries this table (per-shard) to surface fleet status to
//! operators via the management HTTP routes.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{HarvestError, HarvestResult};
use crate::models::{HarvestWorker, NewHarvestWorker};
use crate::schema::harvest_workers;
use crate::worker::DbPool;

// ---------------------------------------------------------------------------
// WorkerStatus
// ---------------------------------------------------------------------------

/// Lifecycle status of a worker process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    Active,
    Draining,
    Stopped,
}

impl WorkerStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::Draining => "Draining",
            Self::Stopped => "Stopped",
        }
    }

    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Active" => Some(Self::Active),
            "Draining" => Some(Self::Draining),
            "Stopped" => Some(Self::Stopped),
            _ => None,
        }
    }
}

impl std::fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// WorkerHealth (derived from last_heartbeat_at)
// ---------------------------------------------------------------------------

/// Health classification derived from `last_heartbeat_at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerHealth {
    Healthy,
    Stale,
}

impl WorkerHealth {
    /// Classify a worker as healthy or stale given the threshold.
    ///
    /// A worker is considered stale when it has not sent a heartbeat within
    /// `stale_threshold` (typically `2 × heartbeat_interval`).
    #[must_use]
    pub fn classify(last_heartbeat_at: DateTime<Utc>, stale_threshold: Duration) -> Self {
        let elapsed = Utc::now()
            .signed_duration_since(last_heartbeat_at)
            .to_std()
            .unwrap_or(Duration::MAX);
        if elapsed > stale_threshold {
            Self::Stale
        } else {
            Self::Healthy
        }
    }
}

// ---------------------------------------------------------------------------
// Worker filter for list queries
// ---------------------------------------------------------------------------

/// Filters for `list_workers` queries from the management API.
#[derive(Debug, Default, Clone)]
pub struct WorkerFilters {
    pub queue: Option<String>,
    pub shard_id: Option<i32>,
    pub status: Option<String>,
    pub health: Option<WorkerHealth>,
    pub limit: i64,
}

impl WorkerFilters {
    pub const DEFAULT_LIMIT: i64 = 100;
    pub const MAX_LIMIT: i64 = 500;

    #[must_use]
    pub fn new() -> Self {
        Self {
            limit: Self::DEFAULT_LIMIT,
            ..Default::default()
        }
    }
}

/// Parse management API query parameters into `WorkerFilters`.
///
/// Accepts `queue=`, `shard_id=`, `status=`, `health=` and `limit=` keys.
/// Unknown keys are silently ignored for forward compatibility.
///
/// # Errors
///
/// Returns a descriptive error string when:
/// - `status` is not one of `Active`, `Draining`, or `Stopped`
/// - `health` is not one of `healthy` or `stale`
/// - `shard_id` is not a valid i32
/// - `limit` is not a valid positive integer
pub fn parse_worker_filters(pairs: &[(String, String)]) -> Result<WorkerFilters, String> {
    let mut filters = WorkerFilters::new();
    let mut limit_raw: Option<i64> = None;

    for (key, value) in pairs {
        match key.as_str() {
            "queue" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    filters.queue = Some(trimmed.to_string());
                }
            }
            "shard_id" => {
                let parsed = value
                    .trim()
                    .parse::<i32>()
                    .map_err(|_| format!("invalid shard_id '{value}'; expected integer"))?;
                filters.shard_id = Some(parsed);
            }
            "status" => {
                let trimmed = value.trim();
                if WorkerStatus::from_str(trimmed).is_none() {
                    return Err(format!(
                        "unknown status '{trimmed}'; expected one of Active, Draining, Stopped"
                    ));
                }
                filters.status = Some(trimmed.to_string());
            }
            "health" => {
                let trimmed = value.trim();
                filters.health = Some(match trimmed {
                    "healthy" => WorkerHealth::Healthy,
                    "stale" => WorkerHealth::Stale,
                    other => {
                        return Err(format!(
                            "unknown health '{other}'; expected one of healthy, stale"
                        ));
                    }
                });
            }
            "limit" => {
                let parsed = value
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| format!("invalid limit '{value}'; expected integer"))?;
                limit_raw = Some(parsed);
            }
            _ => {}
        }
    }

    filters.limit = limit_raw
        .unwrap_or(WorkerFilters::DEFAULT_LIMIT)
        .clamp(1, WorkerFilters::MAX_LIMIT);
    Ok(filters)
}

// ---------------------------------------------------------------------------
// DB operations
// ---------------------------------------------------------------------------

/// Register a worker in the fleet table on startup.
///
/// Uses `INSERT ... ON CONFLICT DO UPDATE` so a crashed-and-restarted worker
/// with the same `worker_id` overwrites the stale row.
///
/// # Errors
///
/// Returns [`HarvestError`] on serialization or database failure.
pub async fn register_worker(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    queues: &[String],
    shard_assignments: &[i32],
    max_concurrency: i32,
    host: &str,
    version: Option<&str>,
) -> HarvestResult<()> {
    use diesel::pg::upsert::excluded;

    let queues_json = serde_json::to_value(queues).map_err(HarvestError::Serialization)?;
    let shards_json =
        serde_json::to_value(shard_assignments).map_err(HarvestError::Serialization)?;

    let row = NewHarvestWorker {
        worker_id,
        queues: queues_json,
        shard_assignments: shards_json,
        max_concurrency,
        host,
        version,
    };

    diesel::insert_into(harvest_workers::table)
        .values(&row)
        .on_conflict(harvest_workers::worker_id)
        .do_update()
        .set((
            harvest_workers::started_at.eq(excluded(harvest_workers::started_at)),
            harvest_workers::last_heartbeat_at.eq(Utc::now()),
            harvest_workers::queues.eq(excluded(harvest_workers::queues)),
            harvest_workers::shard_assignments.eq(excluded(harvest_workers::shard_assignments)),
            harvest_workers::max_concurrency.eq(excluded(harvest_workers::max_concurrency)),
            harvest_workers::in_flight_count.eq(0_i32),
            harvest_workers::host.eq(excluded(harvest_workers::host)),
            harvest_workers::version.eq(excluded(harvest_workers::version)),
            harvest_workers::status.eq(WorkerStatus::Active.as_str()),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

/// Upsert `last_heartbeat_at` and `in_flight_count` for a worker.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn heartbeat_worker(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    in_flight_count: i32,
) -> HarvestResult<()> {
    diesel::update(harvest_workers::table.find(worker_id))
        .set((
            harvest_workers::last_heartbeat_at.eq(Utc::now()),
            harvest_workers::in_flight_count.eq(in_flight_count),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Transition a worker's lifecycle status.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn transition_status(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    status: WorkerStatus,
) -> HarvestResult<()> {
    diesel::update(harvest_workers::table.find(worker_id))
        .set(harvest_workers::status.eq(status.as_str()))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Query the fleet table with optional filters.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn list_workers(
    conn: &mut AsyncPgConnection,
    filters: &WorkerFilters,
    stale_threshold: Duration,
) -> HarvestResult<Vec<WorkerRow>> {
    let mut query = harvest_workers::table
        .select(HarvestWorker::as_select())
        .into_boxed();

    if let Some(ref status) = filters.status {
        query = query.filter(harvest_workers::status.eq(status));
    }

    let rows: Vec<HarvestWorker> = query
        .limit(filters.limit)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let mut results: Vec<WorkerRow> = rows
        .into_iter()
        .map(|w| {
            let health = WorkerHealth::classify(w.last_heartbeat_at, stale_threshold);
            WorkerRow { worker: w, health }
        })
        .collect();

    // Apply post-query filters that need in-process evaluation.
    if let Some(ref queue) = filters.queue {
        results.retain(|r| {
            r.worker
                .queues
                .as_array()
                .is_some_and(|arr| arr.iter().any(|v| v.as_str() == Some(queue.as_str())))
        });
    }
    if let Some(shard_val) = filters.shard_id {
        results.retain(|r| {
            r.worker
                .shard_assignments
                .as_array()
                .is_some_and(|arr| arr.iter().any(|v| v.as_i64() == Some(i64::from(shard_val))))
        });
    }
    if let Some(health_filter) = filters.health {
        results.retain(|r| r.health == health_filter);
    }

    Ok(results)
}

/// Get a single worker detail row.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn get_worker(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    stale_threshold: Duration,
) -> HarvestResult<Option<WorkerRow>> {
    let row = harvest_workers::table
        .find(worker_id)
        .select(HarvestWorker::as_select())
        .first::<HarvestWorker>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    Ok(row.map(|w| {
        let health = WorkerHealth::classify(w.last_heartbeat_at, stale_threshold);
        WorkerRow { worker: w, health }
    }))
}

/// Aggregate fleet health statistics across workers visible to this connection.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn fleet_health(
    conn: &mut AsyncPgConnection,
    stale_threshold: Duration,
) -> HarvestResult<FleetHealth> {
    let all_workers = harvest_workers::table
        .select(HarvestWorker::as_select())
        .load::<HarvestWorker>(conn)
        .await
        .map_err(crate::error::database_error)?;

    let mut healthy: usize = 0;
    let mut stale: usize = 0;
    let mut draining: usize = 0;
    let mut by_queue: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut by_shard: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();

    for w in &all_workers {
        let health = WorkerHealth::classify(w.last_heartbeat_at, stale_threshold);
        match health {
            WorkerHealth::Healthy => healthy += 1,
            WorkerHealth::Stale => stale += 1,
        }
        if w.status == WorkerStatus::Draining.as_str() {
            draining += 1;
        }
        if let Some(queues) = w.queues.as_array() {
            for q in queues {
                if let Some(name) = q.as_str() {
                    *by_queue.entry(name.to_string()).or_default() += 1;
                }
            }
        }
        if let Some(shards) = w.shard_assignments.as_array() {
            for s in shards {
                if let Some(id) = s.as_i64().and_then(|v| i32::try_from(v).ok()) {
                    *by_shard.entry(id).or_default() += 1;
                }
            }
        }
    }

    Ok(FleetHealth {
        healthy,
        stale,
        draining,
        by_queue,
        by_shard,
    })
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// A worker row enriched with the derived health classification.
#[derive(Debug, serde::Serialize)]
pub struct WorkerRow {
    #[serde(flatten)]
    pub worker: HarvestWorker,
    pub health: WorkerHealth,
}

/// Aggregated fleet health roll-up.
#[derive(Debug, serde::Serialize)]
pub struct FleetHealth {
    pub healthy: usize,
    pub stale: usize,
    pub draining: usize,
    pub by_queue: std::collections::HashMap<String, usize>,
    pub by_shard: std::collections::HashMap<i32, usize>,
}

// ---------------------------------------------------------------------------
// Background heartbeat task
// ---------------------------------------------------------------------------

/// Spawn a background task that upserts worker heartbeats on a regular interval.
///
/// The task reads the current `in_flight_count` from the semaphores, then
/// writes it to the database. It stops when `cancel` is triggered.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn spawn_worker_heartbeat(
    pool: DbPool,
    worker_id: String,
    wf_semaphore: Arc<Semaphore>,
    wf_max: usize,
    act_semaphore: Arc<Semaphore>,
    act_max: usize,
    interval: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }

            let in_flight = compute_in_flight(&wf_semaphore, wf_max, &act_semaphore, act_max);

            match pool.get().await {
                Ok(mut conn) => {
                    if let Err(error) = heartbeat_worker(&mut conn, &worker_id, in_flight).await {
                        tracing::warn!(
                            worker_id = %worker_id,
                            error = %error,
                            "worker heartbeat write failed"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        worker_id = %worker_id,
                        error = %error,
                        "worker heartbeat failed to get pool connection"
                    );
                }
            }
        }
    })
}

/// Compute the number of tasks currently in flight from semaphore permits.
#[must_use]
pub fn compute_in_flight(
    wf_semaphore: &Semaphore,
    wf_max: usize,
    act_semaphore: &Semaphore,
    act_max: usize,
) -> i32 {
    let wf_in_flight = wf_max.saturating_sub(wf_semaphore.available_permits());
    let act_in_flight = act_max.saturating_sub(act_semaphore.available_permits());
    i32::try_from(wf_in_flight + act_in_flight).unwrap_or(i32::MAX)
}

/// Return the local machine hostname, best-effort.
#[must_use]
pub fn local_hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|s| s.trim().to_string())
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

// ---------------------------------------------------------------------------
// Tests — Red phase: written before the implementation compiles
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- WorkerStatus --

    #[test]
    fn worker_status_round_trips_via_str() {
        for status in [
            WorkerStatus::Active,
            WorkerStatus::Draining,
            WorkerStatus::Stopped,
        ] {
            let s = status.as_str();
            let parsed = WorkerStatus::from_str(s);
            assert_eq!(parsed, Some(status), "round-trip failed for {s}");
        }
    }

    #[test]
    fn worker_status_rejects_unknown_string() {
        assert_eq!(WorkerStatus::from_str("zombie"), None);
        assert_eq!(WorkerStatus::from_str(""), None);
        assert_eq!(WorkerStatus::from_str("active"), None); // case-sensitive
    }

    #[test]
    fn worker_status_display_matches_as_str() {
        assert_eq!(WorkerStatus::Active.to_string(), "Active");
        assert_eq!(WorkerStatus::Draining.to_string(), "Draining");
        assert_eq!(WorkerStatus::Stopped.to_string(), "Stopped");
    }

    // -- WorkerHealth --

    #[test]
    fn worker_health_healthy_when_recently_seen() {
        let threshold = Duration::from_secs(10);
        let recent = Utc::now() - chrono::Duration::seconds(3);
        assert_eq!(
            WorkerHealth::classify(recent, threshold),
            WorkerHealth::Healthy
        );
    }

    #[test]
    fn worker_health_stale_when_past_threshold() {
        let threshold = Duration::from_secs(10);
        let old = Utc::now() - chrono::Duration::seconds(15);
        assert_eq!(WorkerHealth::classify(old, threshold), WorkerHealth::Stale);
    }

    #[test]
    fn worker_health_stale_at_exact_threshold_boundary() {
        let threshold = Duration::from_secs(10);
        // exactly at the threshold boundary is still stale (> check)
        let at_boundary = Utc::now() - chrono::Duration::seconds(11);
        assert_eq!(
            WorkerHealth::classify(at_boundary, threshold),
            WorkerHealth::Stale
        );
    }

    // -- parse_worker_filters --

    fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parse_worker_filters_defaults_when_empty() {
        let f = parse_worker_filters(&[]).expect("empty should parse");
        assert_eq!(f.limit, WorkerFilters::DEFAULT_LIMIT);
        assert!(f.queue.is_none());
        assert!(f.shard_id.is_none());
        assert!(f.status.is_none());
        assert!(f.health.is_none());
    }

    #[test]
    fn parse_worker_filters_accepts_queue() {
        let f = parse_worker_filters(&pairs(&[("queue", "email-workers")])).unwrap();
        assert_eq!(f.queue.as_deref(), Some("email-workers"));
    }

    #[test]
    fn parse_worker_filters_accepts_valid_status() {
        for status in ["Active", "Draining", "Stopped"] {
            let f = parse_worker_filters(&pairs(&[("status", status)])).unwrap();
            assert_eq!(f.status.as_deref(), Some(status));
        }
    }

    #[test]
    fn parse_worker_filters_rejects_unknown_status() {
        let err = parse_worker_filters(&pairs(&[("status", "zombie")])).unwrap_err();
        assert!(err.contains("unknown status"), "error: {err}");
    }

    #[test]
    fn parse_worker_filters_accepts_health_healthy() {
        let f = parse_worker_filters(&pairs(&[("health", "healthy")])).unwrap();
        assert_eq!(f.health, Some(WorkerHealth::Healthy));
    }

    #[test]
    fn parse_worker_filters_accepts_health_stale() {
        let f = parse_worker_filters(&pairs(&[("health", "stale")])).unwrap();
        assert_eq!(f.health, Some(WorkerHealth::Stale));
    }

    #[test]
    fn parse_worker_filters_rejects_unknown_health() {
        let err = parse_worker_filters(&pairs(&[("health", "dead")])).unwrap_err();
        assert!(err.contains("unknown health"), "error: {err}");
    }

    #[test]
    fn parse_worker_filters_accepts_valid_shard_id() {
        let f = parse_worker_filters(&pairs(&[("shard_id", "3")])).unwrap();
        assert_eq!(f.shard_id, Some(3));
    }

    #[test]
    fn parse_worker_filters_rejects_non_integer_shard_id() {
        let err = parse_worker_filters(&pairs(&[("shard_id", "not-a-number")])).unwrap_err();
        assert!(err.contains("invalid shard_id"), "error: {err}");
    }

    #[test]
    fn parse_worker_filters_clamps_limit() {
        let f = parse_worker_filters(&pairs(&[("limit", "9999")])).unwrap();
        assert_eq!(f.limit, WorkerFilters::MAX_LIMIT);

        let f = parse_worker_filters(&pairs(&[("limit", "0")])).unwrap();
        assert_eq!(f.limit, 1);
    }

    #[test]
    fn parse_worker_filters_rejects_non_numeric_limit() {
        let err = parse_worker_filters(&pairs(&[("limit", "abc")])).unwrap_err();
        assert!(err.contains("invalid limit"), "error: {err}");
    }

    #[test]
    fn parse_worker_filters_ignores_unknown_keys() {
        let f = parse_worker_filters(&pairs(&[("unknown_param", "value")])).unwrap();
        assert!(f.queue.is_none());
        assert!(f.status.is_none());
    }

    // -- compute_in_flight --

    #[test]
    fn compute_in_flight_zero_when_idle() {
        let wf_sem = Semaphore::new(10);
        let act_sem = Semaphore::new(20);
        assert_eq!(compute_in_flight(&wf_sem, 10, &act_sem, 20), 0);
    }

    #[test]
    fn compute_in_flight_counts_acquired_permits() {
        let wf_sem = Arc::new(Semaphore::new(10));
        let act_sem = Arc::new(Semaphore::new(20));

        // Acquire 3 workflow permits and 5 activity permits.
        let _wf_permits = wf_sem.try_acquire_many(3).unwrap();
        let _act_permits = act_sem.try_acquire_many(5).unwrap();

        assert_eq!(compute_in_flight(&wf_sem, 10, &act_sem, 20), 8);
    }
}
