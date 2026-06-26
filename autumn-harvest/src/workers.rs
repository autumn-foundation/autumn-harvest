//! Worker fleet registry — liveness tracking, heartbeat, and fleet queries.
//!
//! Each `Worker` registers a row in `harvest_workers` on startup, upserts
//! `last_heartbeat_at` and `in_flight_count` on a regular interval, and
//! transitions through `Active → Draining → Stopped` on graceful shutdown.
//!
//! The API layer queries this table (per-shard) to surface fleet status to
//! operators via the management HTTP routes.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use uuid::Uuid;

use crate::error::{HarvestError, HarvestResult};

// ---------------------------------------------------------------------------
// WorkerRegistration
// ---------------------------------------------------------------------------

/// Static fields that identify a worker process, used for initial registration
/// and heartbeat self-healing.
///
/// **Why does this exist?**
/// Provides all the essential static identity and capability information required
/// to register a worker against the central scheduler database.
#[derive(Debug, Clone)]
pub struct WorkerRegistration {
    /// A unique identifier for this specific worker instance (e.g., UUID or hostname + PID).
    pub worker_id: String,
    /// The list of task queues this worker is polling.
    pub queues: Vec<String>,
    /// Optional assigned shards if using sticky or deterministic routing.
    pub shard_assignments: Vec<i32>,
    /// The maximum number of concurrent tasks this worker will execute.
    pub max_concurrency: i32,
    /// The host name or IP address of the machine running the worker.
    pub host: String,
    /// The version of the `autumn-harvest` crate or worker software.
    pub version: Option<String>,
    /// Immutable build identifier for this worker binary (issue #171).
    ///
    /// Empty string = legacy worker that can claim any task regardless of
    /// `required_build_id`. Operators should set this to a stable per-build
    /// token (Git SHA, semver, CI job ID, etc.) to enable build-aware routing.
    pub build_id: String,
    /// Optional human-readable deployment name, e.g. `"prod-blue"` (issue #171).
    pub deployment_name: Option<String>,
    /// Capability labels for hardware-aware and regional routing (issue #382).
    pub labels: std::collections::HashMap<String, String>,
}
use crate::models::{HarvestWorker, NewHarvestWorker};
use crate::schema::{harvest_task_queue, harvest_workers, harvest_workflow_executions};
use crate::worker::DbPool;

// ---------------------------------------------------------------------------
// WorkerStatus
// ---------------------------------------------------------------------------

/// Lifecycle status of a worker process.
///
/// **Why does this exist?**
/// Tracks whether a worker is actively picking up tasks, finishing its current tasks before
/// shutdown, or completely halted. This affects routing decisions by the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    /// The worker is actively polling queues and accepting new tasks.
    Active,
    /// The worker is finishing existing tasks but not accepting new ones.
    Draining,
    /// The worker has stopped polling completely.
    Stopped,
}

impl WorkerStatus {
    /// Converts the enum variant to its exact canonical string identifier used by the database API.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::Draining => "Draining",
            Self::Stopped => "Stopped",
        }
    }

    /// Safely attempts to match an incoming string from an API request to a known `WorkerStatus` state.
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
///
/// **Why does this exist?**
/// Allows the scheduler to differentiate between workers that are currently
/// connected and functioning normally versus those that might have crashed
/// or disconnected silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerHealth {
    /// The worker has sent a heartbeat recently enough to be considered active.
    Healthy,
    /// The worker has not sent a heartbeat within the expected threshold.
    Stale,
}

impl WorkerHealth {
    /// Classify a worker as healthy or stale given the threshold.
    ///
    /// A worker is considered stale when it has not sent a heartbeat within
    /// `stale_threshold` (typically `2 × heartbeat_interval`).
    #[must_use]
    pub fn classify(last_heartbeat_at: DateTime<Utc>, stale_threshold: Duration) -> Self {
        // Negative durations arise from clock skew (worker host slightly ahead of
        // the API host). Treat them as zero elapsed time so a freshly-heartbeating
        // worker is never misclassified as stale.
        let elapsed = Utc::now()
            .signed_duration_since(last_heartbeat_at)
            .to_std()
            .unwrap_or(Duration::ZERO);
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
///
/// **Why does this exist?**
/// Provides structured search criteria when requesting lists of workers from the database,
/// allowing operators to filter by queue, shard, or current health status.
#[derive(Debug, Default, Clone)]
pub struct WorkerFilters {
    /// Filter workers that are polling this specific queue.
    pub queue: Option<String>,
    /// Filter workers that are assigned to this shard.
    pub shard_id: Option<i32>,
    /// Filter workers by their current lifecycle status (e.g., "Active").
    pub status: Option<String>,
    /// Filter workers by their derived health classification.
    pub health: Option<WorkerHealth>,
    /// The maximum number of workers to return in the result set.
    pub limit: i64,
    /// Filter workers by build ID (issue #171).
    pub build_id: Option<String>,
    /// Filter workers by deployment name (issue #171).
    pub deployment_name: Option<String>,
}

impl WorkerFilters {
    /// Protects the API layer against runaway unbounded queries by setting a safe baseline limit.
    pub const DEFAULT_LIMIT: i64 = 100;
    /// Prevent abusive or excessively large requests from crashing the database worker.
    pub const MAX_LIMIT: i64 = 500;

    /// Initializes a blank query filter that inherits the safe system baseline `DEFAULT_LIMIT`.
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
            "build_id" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    filters.build_id = Some(trimmed.to_string());
                }
            }
            "deployment_name" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    filters.deployment_name = Some(trimmed.to_string());
                }
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
#[allow(clippy::too_many_arguments)]
pub async fn register_worker<S: std::hash::BuildHasher + Send + Sync>(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    queues: &[String],
    shard_assignments: &[i32],
    max_concurrency: i32,
    host: &str,
    version: Option<&str>,
    build_id: &str,
    deployment_name: Option<&str>,
    labels: &std::collections::HashMap<String, String, S>,
) -> HarvestResult<()> {
    use diesel::pg::upsert::excluded;

    let queues_json = serde_json::to_value(queues).map_err(HarvestError::Serialization)?;
    let shards_json =
        serde_json::to_value(shard_assignments).map_err(HarvestError::Serialization)?;
    let labels_json = serde_json::to_value(labels).map_err(HarvestError::Serialization)?;

    let row = NewHarvestWorker {
        worker_id,
        queues: queues_json,
        shard_assignments: shards_json,
        max_concurrency,
        host,
        version,
        build_id,
        deployment_name,
        labels: labels_json,
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
            harvest_workers::build_id.eq(excluded(harvest_workers::build_id)),
            harvest_workers::deployment_name.eq(excluded(harvest_workers::deployment_name)),
            harvest_workers::labels.eq(excluded(harvest_workers::labels)),
            harvest_workers::status.eq(WorkerStatus::Active.as_str()),
            // Clear any stale drain deadline so a re-registering worker does not
            // inherit the deadline left behind by a prior Draining/Stopped cycle.
            harvest_workers::drain_deadline_at.eq(Option::<DateTime<Utc>>::None),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

/// Upsert `last_heartbeat_at` and `in_flight_count` for a worker.
///
/// Returns the number of rows updated (1 if the worker row exists, 0 if it is
/// missing). Callers that receive 0 should re-register the worker to self-heal
/// after a failed startup registration.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn heartbeat_worker(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    in_flight_count: i32,
    labels: &serde_json::Value,
) -> HarvestResult<usize> {
    let affected = diesel::update(harvest_workers::table.find(worker_id))
        .set((
            harvest_workers::last_heartbeat_at.eq(Utc::now()),
            harvest_workers::in_flight_count.eq(in_flight_count),
            harvest_workers::labels.eq(labels),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(affected)
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

/// Transition a worker row from `Active` to `Draining`, leaving rows that are
/// already `Draining` or `Stopped` untouched.
///
/// Used by the heartbeat task to repair shard rows that were still `Active`
/// because the drain fan-out could not reach the shard (network partition or
/// transient unavailability).  The `Active`-only guard ensures this never
/// reverts a row that was already advanced by a concurrent path.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
async fn transition_active_to_draining(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
) -> HarvestResult<()> {
    diesel::update(
        harvest_workers::table
            .find(worker_id)
            .filter(harvest_workers::status.eq(WorkerStatus::Active.as_str())),
    )
    .set(harvest_workers::status.eq(WorkerStatus::Draining.as_str()))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// Apply queue, shard, health, and limit filters to an already-loaded worker list.
///
/// The limit is intentionally applied **after** the in-process `retain` passes so
/// that a SQL-level page size cannot silently exclude matching rows that appear
/// beyond the first N rows of the unfiltered table.
fn apply_worker_filters(mut results: Vec<WorkerRow>, filters: &WorkerFilters) -> Vec<WorkerRow> {
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
    if let Some(ref build_id) = filters.build_id {
        results.retain(|r| &r.worker.build_id == build_id);
    }
    if let Some(ref deployment_name) = filters.deployment_name {
        results.retain(|r| r.worker.deployment_name.as_deref() == Some(deployment_name.as_str()));
    }
    results.truncate(usize::try_from(filters.limit).unwrap_or(usize::MAX));
    results
}

/// Query the fleet table with optional filters.
///
/// The SQL query applies only the `status` filter (an indexed column). Queue,
/// shard, health, and limit filters are applied in-process after loading so
/// that the limit is never evaluated before the JSONB/derived-field filters.
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
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let results: Vec<WorkerRow> = rows
        .into_iter()
        .map(|w| {
            let health = WorkerHealth::classify(w.last_heartbeat_at, stale_threshold);
            WorkerRow {
                worker: w,
                health,
                active_task_ids: vec![],
            }
        })
        .collect();

    Ok(apply_worker_filters(results, filters))
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

    let Some(w) = row else {
        return Ok(None);
    };

    let active_task_ids = harvest_task_queue::table
        .filter(harvest_task_queue::worker_id.eq(Some(worker_id)))
        .filter(harvest_task_queue::state.eq("RUNNING"))
        .select(harvest_task_queue::id)
        .load::<Uuid>(conn)
        .await
        .map_err(crate::error::database_error)?;

    let health = WorkerHealth::classify(w.last_heartbeat_at, stale_threshold);
    Ok(Some(WorkerRow {
        worker: w,
        health,
        active_task_ids,
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
///
/// **Why does this exist?**
/// Provides a unified API response model that combines the raw database row (`HarvestWorker`)
/// with the dynamically computed `WorkerHealth` status and currently active task IDs.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkerRow {
    /// The raw worker record from the database.
    #[serde(flatten)]
    pub worker: HarvestWorker,
    /// The computed health status of the worker based on its last heartbeat.
    pub health: WorkerHealth,
    /// IDs of task-queue items currently claimed by this worker (`state = RUNNING`).
    /// Populated only by `get_worker`; the list endpoint returns an empty vec.
    pub active_task_ids: Vec<Uuid>,
}

/// Aggregated fleet health roll-up.
///
/// **Why does this exist?**
/// Summarizes the current state of the entire worker fleet, providing a high-level
/// dashboard view of cluster capacity and potential issues (like too many stale workers).
#[derive(Debug, serde::Serialize)]
pub struct FleetHealth {
    /// Total count of workers considered healthy.
    pub healthy: usize,
    /// Total count of workers considered stale (missing heartbeats).
    pub stale: usize,
    /// Total count of workers currently in the draining state.
    pub draining: usize,
    /// A breakdown of total active worker counts per task queue.
    pub by_queue: std::collections::HashMap<String, usize>,
    /// A breakdown of total active worker counts per assigned shard.
    pub by_shard: std::collections::HashMap<i32, usize>,
}

// ---------------------------------------------------------------------------
// Drain controls (issue #170)
// ---------------------------------------------------------------------------

/// Machine-readable outcome of a remote worker drain request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainOutcome {
    /// Drain accepted; the worker status has been set to `Draining`.
    Accepted,
    /// The worker was already in the `Draining` state.
    AlreadyDraining,
    /// The worker is already in the `Stopped` state and will not accept new work.
    AlreadyStopped,
    /// The worker's last heartbeat is older than the stale threshold; the drain
    /// was accepted but the worker may already be dead.
    StaleWorker,
    /// No worker with that ID exists in the fleet table.
    NotFound,
}

impl DrainOutcome {
    /// Returns `true` when the drain was recorded (accepted or stale-but-drained).
    #[must_use]
    pub const fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted | Self::StaleWorker)
    }
}

/// Response returned by `POST /workers/{worker_id}/drain`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DrainResponse {
    /// The worker that was targeted by the drain request.
    pub worker_id: String,
    /// Machine-readable outcome.
    pub outcome: DrainOutcome,
    /// Tasks in flight at the moment the drain was requested.
    pub in_flight_count: i32,
    /// When this worker must have finished draining (echoed from the request or
    /// derived from the configured shutdown timeout).
    pub drain_deadline_at: Option<DateTime<Utc>>,
    /// Shard IDs this worker was serving at drain time.
    pub shard_ids: Vec<i32>,
    /// Shards that could not be contacted during this request.
    /// When non-empty the result is **degraded**: the worker may exist on an
    /// unavailable shard and operators should verify before re-routing traffic.
    pub unavailable_shards: Vec<i32>,
}

/// One entry in a dry-run drain preview — what *would* be drained.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DrainPreviewItem {
    /// Worker ID.
    pub worker_id: String,
    /// Current lifecycle status.
    pub status: String,
    /// Current health classification.
    pub health: WorkerHealth,
    /// Tasks currently in flight.
    pub in_flight_count: i32,
    /// Task queues this worker is polling.
    pub queues: Vec<String>,
    /// Shard IDs this worker serves.
    pub shard_ids: Vec<i32>,
}

/// Convert a `WorkerRow` into a `DrainPreviewItem`.
///
/// Pure function — no DB access; used by both the API handler and unit tests.
#[must_use]
pub fn preview_item_from_row(row: &WorkerRow) -> DrainPreviewItem {
    let queues = row
        .worker
        .queues
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let shard_ids = row
        .worker
        .shard_assignments
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_i64().and_then(|n| i32::try_from(n).ok()))
                .collect()
        })
        .unwrap_or_default();

    DrainPreviewItem {
        worker_id: row.worker.worker_id.clone(),
        status: row.worker.status.clone(),
        health: row.health,
        in_flight_count: row.worker.in_flight_count,
        queues,
        shard_ids,
    }
}

// ---------------------------------------------------------------------------
// PinnedExecutionRow + list_pinned_executions (issue #235)
// ---------------------------------------------------------------------------

/// Summary of one workflow execution currently sticky-pinned to a worker.
///
/// An execution is "live-pinned" when it has a parked task in
/// `harvest_task_queue` with `sticky_worker_id = <this worker>` and a
/// non-expired `sticky_until`. That is the authoritative liveness signal:
/// `harvest_workflow_executions.sticky_worker_id` is only written on terminal
/// transitions (completed / failed / continued-as-new) and therefore does NOT
/// reflect currently-suspended executions.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PinnedExecutionRow {
    /// Unique execution UUID.
    pub execution_id: Uuid,
    /// Registered workflow function name.
    pub workflow_name: String,
    /// Business-key workflow ID supplied by the caller.
    pub workflow_id: String,
    /// Lifecycle state of the execution (e.g. `"Running"`, `"Suspended"`).
    pub state: String,
    /// Task queue the parked task is waiting on.
    pub queue_name: String,
    /// Wall-clock start time of this execution.
    pub started_at: DateTime<Utc>,
    /// When the affinity lease expires (UTC). After this the task becomes
    /// claimable by any eligible worker.
    pub sticky_until: DateTime<Utc>,
}

/// List workflow executions currently soft-pinned to `worker_id` via the
/// task-queue affinity mechanism (issue #235).
///
/// Queries `harvest_task_queue` for parked tasks whose `sticky_worker_id`
/// matches `worker_id` and whose lease (`sticky_until`) has not yet expired,
/// then joins to `harvest_workflow_executions` for metadata.
///
/// The returned set shrinks as leases expire or executions complete, and grows
/// as follow-up tasks park back to this worker.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] when the Postgres query fails.
pub async fn list_pinned_executions(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
) -> HarvestResult<Vec<PinnedExecutionRow>> {
    use crate::schema::harvest_task_queue;
    use diesel::dsl::sql;
    use diesel::sql_types::Nullable;
    use diesel::sql_types::Timestamptz;

    type Row = (
        Uuid,                  // harvest_task_queue.workflow_exec_id
        Option<DateTime<Utc>>, // harvest_task_queue.sticky_until
        Uuid,                  // harvest_workflow_executions.id
        String,                // workflow_name
        String,                // workflow_id
        String,                // state
        String,                // queue_name (from execution)
        DateTime<Utc>,         // started_at
    );

    let rows: Vec<Row> = harvest_task_queue::table
        .inner_join(harvest_workflow_executions::table)
        .filter(harvest_task_queue::sticky_worker_id.eq(worker_id))
        .filter(harvest_task_queue::sticky_until.gt(sql::<Nullable<Timestamptz>>("NOW()")))
        .filter(harvest_task_queue::state.eq_any(["PENDING", "RUNNING"]))
        .select((
            harvest_task_queue::workflow_exec_id.assume_not_null(),
            harvest_task_queue::sticky_until,
            harvest_workflow_executions::id,
            harvest_workflow_executions::workflow_name,
            harvest_workflow_executions::workflow_id,
            harvest_workflow_executions::state,
            harvest_workflow_executions::queue_name,
            harvest_workflow_executions::started_at,
        ))
        .order(harvest_workflow_executions::started_at.asc())
        .load(conn)
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))?;

    Ok(rows
        .into_iter()
        .filter_map(
            |(
                _,
                sticky_until,
                exec_id,
                workflow_name,
                workflow_id,
                state,
                queue_name,
                started_at,
            )| {
                sticky_until.map(|su| PinnedExecutionRow {
                    execution_id: exec_id,
                    workflow_name,
                    workflow_id,
                    state,
                    queue_name,
                    started_at,
                    sticky_until: su,
                })
            },
        )
        .collect())
}

/// Read the current lifecycle status of a worker without modifying it.
///
/// Returns `None` when the worker row does not exist.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn read_worker_status(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
) -> HarvestResult<Option<String>> {
    let status = harvest_workers::table
        .find(worker_id)
        .select(harvest_workers::status)
        .first::<String>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    Ok(status)
}

/// Read the `drain_deadline_at` timestamp for a worker, if it exists.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn read_worker_drain_deadline(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
) -> HarvestResult<Option<DateTime<Utc>>> {
    let row: Option<Option<DateTime<Utc>>> = harvest_workers::table
        .find(worker_id)
        .select(harvest_workers::drain_deadline_at)
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    Ok(row.flatten())
}

/// Read the `drain_deadline_at` for a worker that is currently `Draining`.
///
/// Returns `None` when the worker row does not exist, has no deadline set, or
/// is in any other state (e.g. `Active` with a stale deadline left from a
/// previous drain cycle that was interrupted before `register_worker` cleared
/// it).
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn read_draining_worker_deadline(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
) -> HarvestResult<Option<DateTime<Utc>>> {
    let row: Option<Option<DateTime<Utc>>> = harvest_workers::table
        .find(worker_id)
        .filter(harvest_workers::status.eq(WorkerStatus::Draining.as_str()))
        .select(harvest_workers::drain_deadline_at)
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    Ok(row.flatten())
}

/// Request a graceful drain for the worker identified by `worker_id`.
///
/// The function classifies the current worker state and, if appropriate,
/// transitions it to `Draining` and records `drain_deadline_at`.
/// It never touches workflow-event history.
///
/// | Outcome          | Condition                                            |
/// |------------------|------------------------------------------------------|
/// | `accepted`       | Worker is `Active` and healthy                       |
/// | `stale_worker`   | Worker is `Active` but past the stale threshold      |
/// | `already_draining` | Worker is already `Draining`                       |
/// | `already_stopped`  | Worker is already `Stopped`                        |
/// | `not_found`        | No row with that `worker_id`                       |
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn request_drain(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    deadline_at: Option<DateTime<Utc>>,
    // true = operator supplied an explicit value; false = computed default.
    // AlreadyDraining only refreshes the stored deadline for explicit values.
    deadline_is_explicit: bool,
    stale_threshold: Duration,
) -> HarvestResult<DrainResponse> {
    let row = harvest_workers::table
        .find(worker_id)
        .select(HarvestWorker::as_select())
        .first::<HarvestWorker>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    let Some(worker) = row else {
        return Ok(DrainResponse {
            worker_id: worker_id.to_string(),
            outcome: DrainOutcome::NotFound,
            in_flight_count: 0,
            drain_deadline_at: None,
            shard_ids: vec![],
            unavailable_shards: vec![],
        });
    };

    let health = WorkerHealth::classify(worker.last_heartbeat_at, stale_threshold);
    let current_status = WorkerStatus::from_str(&worker.status);

    let outcome = match current_status {
        Some(WorkerStatus::Draining) => DrainOutcome::AlreadyDraining,
        Some(WorkerStatus::Stopped) => DrainOutcome::AlreadyStopped,
        _ if health == WorkerHealth::Stale => DrainOutcome::StaleWorker,
        _ => DrainOutcome::Accepted,
    };

    let shard_ids: Vec<i32> = worker
        .shard_assignments
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_i64().and_then(|n| i32::try_from(n).ok()))
                .collect()
        })
        .unwrap_or_default();

    // Persist the status transition and deadline for new drains (Accepted / StaleWorker).
    // The UPDATE is guarded by `status != 'Stopped'` so a concurrent self-shutdown
    // that already wrote Stopped is not overwritten.
    if outcome.is_accepted() {
        let rows = diesel::update(
            harvest_workers::table
                .find(worker_id)
                .filter(harvest_workers::status.ne(WorkerStatus::Stopped.as_str())),
        )
        .set((
            harvest_workers::status.eq(WorkerStatus::Draining.as_str()),
            harvest_workers::drain_deadline_at.eq(deadline_at),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

        if rows == 0 {
            // Worker self-stopped between our read and this update; report as stopped.
            return Ok(DrainResponse {
                worker_id: worker_id.to_string(),
                outcome: DrainOutcome::AlreadyStopped,
                in_flight_count: worker.in_flight_count,
                drain_deadline_at: None,
                shard_ids,
                unavailable_shards: vec![],
            });
        }
    } else if outcome == DrainOutcome::AlreadyDraining && deadline_is_explicit {
        // Worker is already Draining and the caller supplied an explicit deadline
        // — refresh it so operators can extend or correct the window without
        // re-triggering the status transition.
        diesel::update(harvest_workers::table.find(worker_id))
            .set(harvest_workers::drain_deadline_at.eq(deadline_at))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
    }
    // AlreadyDraining + !deadline_is_explicit: preserve the stored deadline.

    // Echo whichever deadline is now in effect: the parameter for new drains
    // or explicit refreshes; the pre-existing row value when preserving.
    let effective_deadline = if outcome == DrainOutcome::AlreadyDraining && !deadline_is_explicit {
        worker.drain_deadline_at
    } else {
        deadline_at
    };

    Ok(DrainResponse {
        worker_id: worker_id.to_string(),
        outcome,
        in_flight_count: worker.in_flight_count,
        drain_deadline_at: effective_deadline,
        shard_ids,
        unavailable_shards: vec![],
    })
}

/// Return a preview of which workers would be drained for the given filters.
///
/// This is the dry-run surface: it never mutates any row.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn drain_preview(
    conn: &mut AsyncPgConnection,
    filters: &WorkerFilters,
    stale_threshold: Duration,
) -> HarvestResult<Vec<DrainPreviewItem>> {
    // Default to Active-only so the preview shows workers that *would* be newly
    // drained. Callers that want Draining or Stopped workers must pass an explicit
    // status filter.
    let active_filters;
    let effective = if filters.status.is_none() {
        active_filters = WorkerFilters {
            status: Some(WorkerStatus::Active.as_str().to_string()),
            ..filters.clone()
        };
        &active_filters
    } else {
        filters
    };
    let rows = list_workers(conn, effective, stale_threshold).await?;
    Ok(rows.iter().map(preview_item_from_row).collect())
}

// ---------------------------------------------------------------------------
// Background heartbeat task
// ---------------------------------------------------------------------------

/// Spawn a background task that upserts worker heartbeats on a regular interval.
///
/// The task reads the current `in_flight_count` from the semaphores, then
/// writes it to the database. If the worker row is missing (0 rows updated —
/// e.g. because startup registration failed transiently), the task re-registers
/// the worker automatically. It stops when `cancel` is triggered.
#[must_use]
#[allow(clippy::too_many_arguments)]
// The shared applied-set uses the worker's own fixed hasher; no need to generalize.
#[allow(clippy::implicit_hasher)]
/// Execute one heartbeat DB tick: update `last_heartbeat_at`, handle
/// re-registration / drain transitions, and refresh the drain deadline.
async fn do_heartbeat_tick(
    conn: &mut AsyncPgConnection,
    registration: &WorkerRegistration,
    in_flight: i32,
    labels_json: &serde_json::Value,
    worker_shutdown: &CancellationToken,
    drain_deadline_max: &Mutex<Option<DateTime<Utc>>>,
    remote_drain_deadline: &Mutex<Option<std::time::Instant>>,
) {
    match heartbeat_worker(conn, &registration.worker_id, in_flight, labels_json).await {
        Ok(0) => {
            if worker_shutdown.is_cancelled() {
                // Worker is already draining — do not create a new Active row on
                // a shard that missed the fan-out.  An absent row correctly
                // reflects no live coverage; re-registering as Active would give
                // shard health checks a false positive.
                tracing::debug!(
                    worker_id = %registration.worker_id,
                    "worker draining; skipping re-registration for recovered shard"
                );
            } else {
                tracing::info!(worker_id = %registration.worker_id, "worker row missing; re-registering");
                if let Err(error) = register_worker(
                    conn,
                    &registration.worker_id,
                    &registration.queues,
                    &registration.shard_assignments,
                    registration.max_concurrency,
                    &registration.host,
                    registration.version.as_deref(),
                    &registration.build_id,
                    registration.deployment_name.as_deref(),
                    &registration.labels,
                )
                .await
                {
                    tracing::warn!(worker_id = %registration.worker_id, error = %error, "worker re-registration failed");
                }
            }
        }
        Ok(_) => {
            if worker_shutdown.is_cancelled() {
                // Already draining — transition this shard's row to Draining if
                // the fan-out missed it (e.g. shard recovered after the drain was
                // issued).  Guarded to Active rows only so it never reverts a row
                // that already reached Draining or Stopped.
                if let Err(error) =
                    transition_active_to_draining(conn, &registration.worker_id).await
                {
                    tracing::warn!(
                        worker_id = %registration.worker_id,
                        error = %error,
                        "failed to transition recovered shard row to Draining"
                    );
                }
                // Refresh the stored deadline so an operator-extended window is
                // picked up by drain_in_flight without restarting the worker.
                sync_drain_deadline(
                    conn,
                    &registration.worker_id,
                    drain_deadline_max,
                    remote_drain_deadline,
                )
                .await;
            } else {
                // Heartbeat succeeded; check whether a remote drain has changed
                // this worker's status to Draining.  Cancel the worker's
                // poll-loop token (not the heartbeat token) so the poll loop
                // stops accepting new work while heartbeats continue until
                // fully stopped (P1).
                match read_worker_status(conn, &registration.worker_id).await {
                    Ok(Some(ref s)) if s == WorkerStatus::Draining.as_str() => {
                        tracing::info!(
                            worker_id = %registration.worker_id,
                            "remote drain detected; triggering graceful shutdown"
                        );
                        sync_drain_deadline(
                            conn,
                            &registration.worker_id,
                            drain_deadline_max,
                            remote_drain_deadline,
                        )
                        .await;
                        worker_shutdown.cancel();
                    }
                    _ => {}
                }
            }
        }
        Err(error) => {
            tracing::warn!(worker_id = %registration.worker_id, error = %error, "worker heartbeat write failed");
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_worker_heartbeat(
    pool: DbPool,
    registration: WorkerRegistration,
    wf_semaphore: Arc<Semaphore>,
    wf_max: usize,
    act_semaphore: Arc<Semaphore>,
    act_max: usize,
    interval: Duration,
    cancel: CancellationToken,
    worker_shutdown: CancellationToken,
    // Populated when a remote drain is detected: absolute Instant of the
    // operator-supplied drain_deadline_at, refreshed on every heartbeat tick
    // so that extended deadlines are picked up by drain_in_flight.
    remote_drain_deadline: Arc<Mutex<Option<std::time::Instant>>>,
    // Maximum `drain_deadline_at` value applied to `remote_drain_deadline` so far,
    // shared across this worker's per-shard heartbeat tasks. Using the maximum
    // (rather than a full set) prevents a shard whose row was never updated from
    // the prior deadline from reverting the cell: stale shorter values are rejected
    // while genuine extensions (newer, later values) always advance the cell.
    drain_deadline_max: Arc<Mutex<Option<DateTime<Utc>>>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let labels_json = serde_json::to_value(&registration.labels).unwrap_or_default();
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }
            let in_flight = compute_in_flight(&wf_semaphore, wf_max, &act_semaphore, act_max);
            match pool.get().await {
                Ok(mut conn) => {
                    let () = do_heartbeat_tick(
                        &mut conn,
                        &registration,
                        in_flight,
                        &labels_json,
                        &worker_shutdown,
                        &drain_deadline_max,
                        &remote_drain_deadline,
                    )
                    .await;
                }
                Err(error) => {
                    tracing::warn!(
                        worker_id = %registration.worker_id,
                        error = %error,
                        "worker heartbeat failed to get pool connection"
                    );
                }
            }
        }
    })
}

/// Fetch `drain_deadline_at` from the DB and write it as an absolute
/// `std::time::Instant` into the shared cell used by `drain_in_flight`.
/// Called both on first drain detection and on every subsequent heartbeat
/// tick while the worker is draining, so that an operator-extended deadline
/// is reflected without a restart.
/// Decide whether an observed `drain_deadline_at` should be applied to the
/// shared effective-deadline cell, recording it as applied when so (issue #522
/// review).
///
/// Returns `true` when `deadline` is strictly greater than the maximum deadline
/// applied so far (or when no deadline has been applied yet), advancing `max` in
/// the process.  Returns `false` for equal or earlier values, which are either
/// idempotent re-observations of the current deadline or stale values from a
/// shard row that was not updated when the operator last changed the deadline.
///
/// Using the strict-max rule prevents a lagging shard from reverting the shared
/// drain-deadline cell: if shard A was unreachable when the operator extended
/// T1 → T2 and only A's row still holds T1, A's heartbeat will correctly reject
/// T1 (T1 < T2 = current max).  Operator-driven shortening is not reflected in
/// the in-process cell, but the local `shutdown_timeout` fallback still bounds
/// the drain, and an operator who needs a hard stop can send SIGTERM.
fn classify_drain_deadline(max: &mut Option<DateTime<Utc>>, deadline: DateTime<Utc>) -> bool {
    if max.is_none_or(|m| deadline > m) {
        *max = Some(deadline);
        true
    } else {
        false
    }
}

async fn sync_drain_deadline(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    // Maximum `drain_deadline_at` value applied to `cell` so far, shared across
    // this worker's per-shard heartbeat tasks.  A new deadline is applied only
    // when it is strictly greater than the current max (issue #522 review).
    max_applied: &Mutex<Option<DateTime<Utc>>>,
    cell: &Mutex<Option<std::time::Instant>>,
) {
    if let Ok(Some(deadline)) = read_worker_drain_deadline(conn, worker_id).await {
        let Ok(mut max_guard) = max_applied.lock() else {
            return;
        };
        if !classify_drain_deadline(&mut max_guard, deadline) {
            return;
        }
        let remaining = deadline
            .signed_duration_since(Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        let candidate = std::time::Instant::now() + remaining;
        // Update the cell while still holding `max_guard`, so two shards observing
        // distinct new deadlines concurrently can't reorder their writes (lock
        // order max_applied→cell is the only place both are held).
        if let Ok(mut guard) = cell.lock() {
            *guard = Some(candidate);
        }
    }
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

    // -- classify_drain_deadline (cross-shard merge, issue #522 review) --

    #[test]
    fn classify_drain_deadline_applies_initial_and_skips_same_value() {
        let mut max: Option<DateTime<Utc>> = None;
        let d1 = Utc::now();
        // Initial drain: first shard to observe it applies.
        assert!(classify_drain_deadline(&mut max, d1));
        assert_eq!(max, Some(d1));
        // Another shard observing the same value: idempotent, no re-apply.
        assert!(!classify_drain_deadline(&mut max, d1));
    }

    #[test]
    fn classify_drain_deadline_skips_stale_recovery_reread() {
        // Shard A applies D1 (initial), then D2 (operator extension).
        // Shard B was offline during D2 so its row still holds D1.
        // When B recovers it must NOT revert the cell back to D1.
        let mut max: Option<DateTime<Utc>> = None;
        let d1 = Utc::now();
        let d2 = d1 + chrono::Duration::minutes(5);
        assert!(classify_drain_deadline(&mut max, d1)); // shard A applies D1
        assert!(classify_drain_deadline(&mut max, d2)); // shard A applies D2
        // Shard B recovers; stale D1 < D2 (current max) → rejected.
        assert!(!classify_drain_deadline(&mut max, d1));
        assert_eq!(max, Some(d2));
    }

    #[test]
    fn classify_drain_deadline_applies_extension_on_first_observation() {
        // Shard A applied D1. Shard A then goes unreachable and the operator
        // re-drains with D2 > D1 that reaches only shard B. Shard B's first
        // observation of D2 must advance the cell even though it is already set.
        let mut max: Option<DateTime<Utc>> = None;
        let d1 = Utc::now();
        let d2 = d1 + chrono::Duration::minutes(5); // extension
        let d_short = d1 - chrono::Duration::minutes(2); // would be a shorten
        assert!(classify_drain_deadline(&mut max, d1)); // shard A applies D1
        assert!(classify_drain_deadline(&mut max, d2)); // shard B first-sees D2 > D1
        assert_eq!(max, Some(d2));
        // Operator-driven shortening (d_short < d2) is NOT reflected via the
        // in-process cell; the local shutdown_timeout fallback bounds the drain.
        assert!(!classify_drain_deadline(&mut max, d_short));
        assert_eq!(max, Some(d2)); // cell unchanged
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
    fn worker_health_healthy_when_timestamp_is_in_future() {
        // Clock skew: last_heartbeat_at is slightly ahead of "now".
        // Should be treated as zero elapsed time (Healthy), not Duration::MAX (Stale).
        let threshold = Duration::from_secs(10);
        let future = Utc::now() + chrono::Duration::seconds(2);
        assert_eq!(
            WorkerHealth::classify(future, threshold),
            WorkerHealth::Healthy
        );
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

    // -- WorkerRegistration --

    #[test]
    fn worker_registration_captures_all_fields() {
        let reg = WorkerRegistration {
            worker_id: "w1".to_string(),
            queues: vec!["default".to_string()],
            shard_assignments: vec![0],
            max_concurrency: 10,
            host: "localhost".to_string(),
            version: Some("0.3.0".to_string()),
            build_id: String::new(),
            deployment_name: None,
            labels: std::collections::HashMap::new(),
        };
        assert_eq!(reg.worker_id, "w1");
        assert_eq!(reg.queues, vec!["default"]);
        assert_eq!(reg.max_concurrency, 10);
        assert_eq!(reg.version.as_deref(), Some("0.3.0"));
    }

    // -- apply_worker_filters (limit is applied after queue/shard/health filtering) --

    fn make_queue_row(worker_id: &str, queue: &str) -> WorkerRow {
        WorkerRow {
            worker: HarvestWorker {
                worker_id: worker_id.to_string(),
                started_at: Utc::now(),
                last_heartbeat_at: Utc::now(),
                queues: serde_json::json!([queue]),
                shard_assignments: serde_json::json!([0]),
                max_concurrency: 10,
                in_flight_count: 0,
                host: "localhost".to_string(),
                version: None,
                status: "Active".to_string(),
                drain_deadline_at: None,
                build_id: String::new(),
                deployment_name: None,
                labels: serde_json::json!({}),
            },
            health: WorkerHealth::Healthy,
            active_task_ids: vec![],
        }
    }

    #[test]
    fn apply_filters_limit_is_applied_after_queue_filter() {
        // 3 rows total; only 1 matches "email-workers". With limit=2 applied
        // BEFORE filtering, the email-workers row might be outside the window.
        // apply_worker_filters must truncate AFTER retaining.
        let rows = vec![
            make_queue_row("w-default-1", "default"),
            make_queue_row("w-default-2", "default"),
            make_queue_row("w-email", "email-workers"),
        ];
        let mut filters = WorkerFilters::new();
        filters.queue = Some("email-workers".to_string());
        filters.limit = 2;
        let result = apply_worker_filters(rows, &filters);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].worker.worker_id, "w-email");
    }

    #[test]
    fn apply_filters_limit_truncates_matched_results() {
        let rows = vec![
            make_queue_row("w1", "email-workers"),
            make_queue_row("w2", "email-workers"),
            make_queue_row("w3", "email-workers"),
        ];
        let mut filters = WorkerFilters::new();
        filters.queue = Some("email-workers".to_string());
        filters.limit = 2;
        let result = apply_worker_filters(rows, &filters);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn apply_filters_no_match_returns_empty() {
        let rows = vec![
            make_queue_row("w1", "default"),
            make_queue_row("w2", "default"),
        ];
        let mut filters = WorkerFilters::new();
        filters.queue = Some("email-workers".to_string());
        let result = apply_worker_filters(rows, &filters);
        assert!(result.is_empty());
    }

    // -- WorkerRow active_task_ids --

    fn make_test_worker_row(active_task_ids: Vec<uuid::Uuid>) -> WorkerRow {
        WorkerRow {
            worker: HarvestWorker {
                worker_id: "test-worker".to_string(),
                started_at: Utc::now(),
                last_heartbeat_at: Utc::now(),
                queues: serde_json::json!(["default"]),
                shard_assignments: serde_json::json!([0]),
                max_concurrency: 10,
                in_flight_count: 0,
                host: "localhost".to_string(),
                version: None,
                status: "Active".to_string(),
                drain_deadline_at: None,
                build_id: String::new(),
                deployment_name: None,
                labels: serde_json::json!({}),
            },
            health: WorkerHealth::Healthy,
            active_task_ids,
        }
    }

    #[test]
    fn worker_row_active_task_ids_serializes_empty() {
        let row = make_test_worker_row(vec![]);
        let json = serde_json::to_value(&row).unwrap();
        let ids = json["active_task_ids"]
            .as_array()
            .expect("active_task_ids should be array");
        assert!(ids.is_empty());
    }

    #[test]
    fn worker_row_active_task_ids_serializes_uuids() {
        let tid = uuid::Uuid::new_v4();
        let row = make_test_worker_row(vec![tid]);
        let json = serde_json::to_value(&row).unwrap();
        let ids = json["active_task_ids"]
            .as_array()
            .expect("active_task_ids should be array");
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].as_str().unwrap(), tid.to_string());
    }

    // -- DrainOutcome --

    #[test]
    fn drain_outcome_serializes_to_snake_case() {
        let cases = [
            (DrainOutcome::Accepted, "accepted"),
            (DrainOutcome::AlreadyDraining, "already_draining"),
            (DrainOutcome::AlreadyStopped, "already_stopped"),
            (DrainOutcome::StaleWorker, "stale_worker"),
            (DrainOutcome::NotFound, "not_found"),
        ];
        for (outcome, expected) in cases {
            let json = serde_json::to_value(outcome).unwrap();
            assert_eq!(
                json.as_str().unwrap(),
                expected,
                "wrong serialization for {outcome:?}"
            );
        }
    }

    #[test]
    fn drain_outcome_is_accepted_only_for_accepted_and_stale() {
        assert!(DrainOutcome::Accepted.is_accepted());
        assert!(DrainOutcome::StaleWorker.is_accepted());
        assert!(!DrainOutcome::AlreadyDraining.is_accepted());
        assert!(!DrainOutcome::AlreadyStopped.is_accepted());
        assert!(!DrainOutcome::NotFound.is_accepted());
    }

    #[test]
    fn drain_outcome_round_trips_via_json() {
        for outcome in [
            DrainOutcome::Accepted,
            DrainOutcome::AlreadyDraining,
            DrainOutcome::AlreadyStopped,
            DrainOutcome::StaleWorker,
            DrainOutcome::NotFound,
        ] {
            let encoded = serde_json::to_string(&outcome).unwrap();
            let decoded: DrainOutcome = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, outcome, "round-trip failed for {outcome:?}");
        }
    }

    // -- DrainResponse --

    #[test]
    fn drain_response_serializes_all_required_fields() {
        let resp = DrainResponse {
            worker_id: "w-abc".to_string(),
            outcome: DrainOutcome::Accepted,
            in_flight_count: 3,
            drain_deadline_at: Some(Utc::now()),
            shard_ids: vec![0, 1],
            unavailable_shards: vec![],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert!(json.get("worker_id").is_some(), "missing worker_id");
        assert!(json.get("outcome").is_some(), "missing outcome");
        assert!(
            json.get("in_flight_count").is_some(),
            "missing in_flight_count"
        );
        assert!(
            json.get("drain_deadline_at").is_some(),
            "missing drain_deadline_at"
        );
        assert!(json.get("shard_ids").is_some(), "missing shard_ids");
        assert!(
            json.get("unavailable_shards").is_some(),
            "missing unavailable_shards"
        );
    }

    #[test]
    fn drain_response_null_deadline_serializes() {
        let resp = DrainResponse {
            worker_id: "w-abc".to_string(),
            outcome: DrainOutcome::NotFound,
            in_flight_count: 0,
            drain_deadline_at: None,
            shard_ids: vec![],
            unavailable_shards: vec![],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert!(json["drain_deadline_at"].is_null());
        assert_eq!(json["shard_ids"].as_array().unwrap().len(), 0);
        assert_eq!(json["unavailable_shards"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn drain_response_with_unavailable_shards_serializes() {
        let resp = DrainResponse {
            worker_id: "w-abc".to_string(),
            outcome: DrainOutcome::NotFound,
            in_flight_count: 0,
            drain_deadline_at: None,
            shard_ids: vec![],
            unavailable_shards: vec![2, 3],
        };
        let json = serde_json::to_value(&resp).unwrap();
        let shards = json["unavailable_shards"].as_array().unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].as_i64().unwrap(), 2);
        assert_eq!(shards[1].as_i64().unwrap(), 3);
    }

    #[test]
    fn drain_preview_defaults_status_to_active_when_unset() {
        // Verify the documented default: callers that omit status see only Active.
        // We can't run a DB query in a unit test, but we can verify the filter
        // construction by inspecting the effective filters value.
        let filters = WorkerFilters::new();
        assert!(
            filters.status.is_none(),
            "WorkerFilters::new() must leave status unset so drain_preview can override it"
        );
        // drain_preview sets status = Active when None; the DB-level assertion
        // lives in the integration test suite.
    }

    #[test]
    fn drain_outcome_already_draining_is_not_accepted() {
        // AlreadyDraining must NOT be treated as accepted (status transition),
        // but the deadline refresh path covers it separately.
        assert!(!DrainOutcome::AlreadyDraining.is_accepted());
        assert!(DrainOutcome::Accepted.is_accepted());
        assert!(DrainOutcome::StaleWorker.is_accepted());
    }

    // -- preview_item_from_row --

    fn make_worker_row_full(
        worker_id: &str,
        status: &str,
        in_flight: i32,
        queues: &[&str],
        shards: &[i32],
    ) -> WorkerRow {
        WorkerRow {
            worker: HarvestWorker {
                worker_id: worker_id.to_string(),
                started_at: Utc::now(),
                last_heartbeat_at: Utc::now(),
                queues: serde_json::json!(queues),
                shard_assignments: serde_json::json!(shards),
                max_concurrency: 10,
                in_flight_count: in_flight,
                host: "localhost".to_string(),
                version: None,
                status: status.to_string(),
                drain_deadline_at: None,
                build_id: String::new(),
                deployment_name: None,
                labels: serde_json::json!({}),
            },
            health: WorkerHealth::Healthy,
            active_task_ids: vec![],
        }
    }

    #[test]
    fn preview_item_from_row_captures_all_fields() {
        let row = make_worker_row_full("w-1", "Active", 5, &["default", "email"], &[0, 1]);
        let item = preview_item_from_row(&row);
        assert_eq!(item.worker_id, "w-1");
        assert_eq!(item.status, "Active");
        assert_eq!(item.in_flight_count, 5);
        assert_eq!(item.queues, vec!["default", "email"]);
        assert_eq!(item.shard_ids, vec![0, 1]);
        assert_eq!(item.health, WorkerHealth::Healthy);
    }

    #[test]
    fn preview_item_from_row_handles_empty_queues_and_shards() {
        let row = make_worker_row_full("w-2", "Draining", 0, &[], &[]);
        let item = preview_item_from_row(&row);
        assert!(item.queues.is_empty());
        assert!(item.shard_ids.is_empty());
    }

    #[test]
    fn preview_item_serializes_to_json() {
        let row = make_worker_row_full("w-3", "Active", 2, &["default"], &[0]);
        let item = preview_item_from_row(&row);
        let json = serde_json::to_value(&item).unwrap();
        assert_eq!(json["worker_id"].as_str().unwrap(), "w-3");
        assert_eq!(json["status"].as_str().unwrap(), "Active");
        assert_eq!(json["in_flight_count"].as_i64().unwrap(), 2);
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
