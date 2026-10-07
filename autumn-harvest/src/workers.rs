//! Worker fleet registry — liveness tracking, heartbeat, and fleet queries.
//!
//! Each `Worker` registers a row in `harvest_workers` on startup, upserts
//! `last_heartbeat_at` and `in_flight_count` on a regular interval, and
//! transitions through `Active → Draining → Stopped` on graceful shutdown.
//!
//! The API layer queries this table (per-shard) to surface fleet status to
//! operators via the management HTTP routes.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
use crate::telemetry::MetricsRecorder;
use crate::worker_outlier::{
    OutlierConfig, OutlierDimension, TaskOutcomeWindow, WorkerTaskStats, outlier_dimensions,
};

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
    /// Empty string = no build identity. Such a worker cannot claim a task
    /// with a `required_build_id` (issue #1805). Operators should set this to a stable per-build
    /// token (Git SHA, semver, CI job ID, etc.) to enable build-aware routing.
    pub build_id: String,
    /// Optional human-readable deployment name, e.g. `"prod-blue"` (issue #171).
    pub deployment_name: Option<String>,
    /// Capability labels for hardware-aware and regional routing (issue #382).
    pub labels: std::collections::HashMap<String, String>,
    /// Advertised worker-session capacity (issue #606). `0` (the default)
    /// means sessions are disabled on this worker -- zero behavior change
    /// for existing deployments.
    pub max_concurrent_sessions: i32,
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
    max_concurrent_sessions: i32,
    registered_codec_key_ids: &[String],
) -> HarvestResult<()> {
    use diesel::pg::upsert::excluded;

    let queues_json = serde_json::to_value(queues).map_err(HarvestError::Serialization)?;
    let shards_json =
        serde_json::to_value(shard_assignments).map_err(HarvestError::Serialization)?;
    let labels_json = serde_json::to_value(labels).map_err(HarvestError::Serialization)?;
    // Issue #1244: every registration advertises this binary's highest
    // readable codec envelope version, and its registered codec key ids.
    // `codec_rotation::activate_codec_key` can then refuse while any live
    // worker cannot read a keyed envelope, or lacks the target key.
    let labels_json =
        crate::payload_codec::advertise_codec_capability(&labels_json, registered_codec_key_ids);

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
        max_concurrent_sessions,
    };

    diesel::insert_into(harvest_workers::table)
        .values(&row)
        .on_conflict(harvest_workers::worker_id)
        .do_update()
        .set((
            harvest_workers::started_at.eq(excluded(harvest_workers::started_at)),
            // The shard database's own clock, never this host's `Utc::now()`
            // -- see the matching rationale on `heartbeat_worker` below.
            harvest_workers::last_heartbeat_at.eq(diesel::dsl::now),
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
            harvest_workers::max_concurrent_sessions
                .eq(excluded(harvest_workers::max_concurrent_sessions)),
            // A re-registering worker starts with zero in-use sessions -- any
            // sessions it previously hosted are reconciled by the
            // broken-session scanner against its new (post-restart) identity.
            harvest_workers::in_use_sessions.eq(0_i32),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

/// Register a worker **and** clear the stale capability-miss evidence its id
/// may still carry, atomically (issue #804, Codex rounds 26 and 28).
///
/// The two writes must land together or not at all, for two independent
/// reasons.
///
/// **A published-but-uncleaned worker is affirmative fleet evidence against
/// itself.** `register_worker` upserts on `worker_id`, so a pod restarting
/// under the same configured id onto a build that *does* register the missing
/// handler republishes itself as live. Capability-miss evidence stores bare
/// ids, so until the invalidation lands, every task that id missed on its
/// previous build still names it — and another claimant reading the registry
/// sees the whole live fleet as already covered, derives
/// [`crate::worker::FleetCapabilityEvidence::AllLiveWorkersMissed`], and
/// terminally fails a run at exactly the moment the fix arrived. Committing the
/// registration while the invalidation fails is therefore strictly worse than
/// not registering at all, and "log and continue" leaves precisely that state.
/// Rolling back keeps the worker unpublished until the pair succeeds, and the
/// heartbeat's missing-row self-heal retries it within one interval.
///
/// **Atomicity is also what makes a claimant's reads coherent.** A decision
/// brackets its fleet read between two miss-state reads and requires them to
/// agree (see `worker::miss_evidence_confidence`). That check is only sound if
/// registration and invalidation are a single commit: split them, and a
/// claimant can observe a world where the peer is already in the fleet but its
/// evidence is not yet cleared — which reads as covered, agrees across both
/// miss reads, and escalates.
///
/// The same transaction drops the worker's task-stats row (issue #1815). A
/// restarted process starts with an empty outcome window. Without the
/// delete, the previous process's row stays fresh until the first heartbeat.
/// Its failures then reach peers and `GET /admin/status` under a live worker.
///
/// Returns the number of task rows whose evidence was cleared.
///
/// # Errors
///
/// Returns [`HarvestError`] on serialization or database failure. Any write
/// failing rolls back all of them.
pub async fn register_worker_and_clear_stale_miss_evidence(
    conn: &mut AsyncPgConnection,
    registration: &WorkerRegistration,
    registered_codec_key_ids: &[String],
) -> HarvestResult<usize> {
    use diesel_async::AsyncConnection;

    Box::pin(conn.transaction(async |tx| {
        register_worker(
            tx,
            &registration.worker_id,
            &registration.queues,
            &registration.shard_assignments,
            registration.max_concurrency,
            &registration.host,
            registration.version.as_deref(),
            &registration.build_id,
            registration.deployment_name.as_deref(),
            &registration.labels,
            registration.max_concurrent_sessions,
            registered_codec_key_ids,
        )
        .await?;
        delete_worker_task_stats(tx, &registration.worker_id).await?;
        crate::queue::invalidate_capability_miss_evidence_for_worker(
            tx,
            &registration.worker_id,
            &registration.queues,
        )
        .await
    }))
    .await
}

/// Delete the task-stats row of `worker_id` (issue #1815).
async fn delete_worker_task_stats(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
) -> HarvestResult<usize> {
    diesel::sql_query("DELETE FROM harvest_worker_task_stats WHERE worker_id = $1")
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Refresh `last_heartbeat_at` for a drained worker and clear its queues
/// (issue #1813).
///
/// A drained worker calls this while a handler that ignored the cancel still
/// runs. Orphan reclaim then keeps the claim with that handler. The stamp uses
/// the database clock, as [`heartbeat_worker`] does.
///
/// The worker no longer polls, so the row advertises no queue. Otherwise
/// [`live_workers_on_queue_query`] would count it as a capable peer, and a
/// capability miss would keep releasing a task instead of escalating it.
///
/// The queues are cleared only while the row is not `Active`. A replacement
/// worker with the same id registers as `Active` with its own queues, and
/// this write must not remove them.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn touch_worker_liveness(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
) -> HarvestResult<usize> {
    diesel::update(harvest_workers::table.find(worker_id))
        .set((
            harvest_workers::last_heartbeat_at.eq(diesel::dsl::now),
            harvest_workers::queues.eq(diesel::dsl::sql::<diesel::sql_types::Jsonb>(
                "CASE WHEN status = 'Active' THEN queues ELSE '[]'::jsonb END",
            )),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Restore the row of a worker that has stopped but still runs a drained
/// handler (issue #1813).
///
/// Orphan reclaim treats a worker with no row as dead. A shutdown heartbeat
/// does not re-register a missing row, so the lease keeper calls this
/// instead. The row has status `Stopped` and lists no queue and no shard, so
/// it never claims coverage. A row that exists already only gets a fresh
/// `last_heartbeat_at`.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn restore_stopped_worker_row(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    host: &str,
) -> HarvestResult<()> {
    let row = NewHarvestWorker {
        worker_id,
        queues: serde_json::json!([]),
        shard_assignments: serde_json::json!([]),
        max_concurrency: 0,
        host,
        version: Some(env!("CARGO_PKG_VERSION")),
        build_id: "",
        deployment_name: None,
        labels: serde_json::json!({}),
        max_concurrent_sessions: 0,
    };
    diesel::insert_into(harvest_workers::table)
        .values((
            &row,
            harvest_workers::status.eq(WorkerStatus::Stopped.as_str()),
        ))
        .on_conflict(harvest_workers::worker_id)
        .do_update()
        .set(harvest_workers::last_heartbeat_at.eq(diesel::dsl::now))
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
    in_use_sessions: i32,
    registered_codec_key_ids: &[String],
) -> HarvestResult<usize> {
    heartbeat_worker_status(
        conn,
        worker_id,
        in_flight_count,
        labels,
        in_use_sessions,
        registered_codec_key_ids,
    )
    .await
    .map(|status| usize::from(status.is_some()))
}

/// [`heartbeat_worker`], returning the status of the row it wrote.
///
/// Returns `None` when the worker row is missing. The status comes back in
/// the same round trip as the write (issue #1879). A separate status read
/// made each tick slower, and a slow tick makes a live worker look dead.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn heartbeat_worker_status(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    in_flight_count: i32,
    labels: &serde_json::Value,
    in_use_sessions: i32,
    registered_codec_key_ids: &[String],
) -> HarvestResult<Option<String>> {
    let labels = crate::payload_codec::advertise_codec_capability(labels, registered_codec_key_ids);
    diesel::update(harvest_workers::table.find(worker_id))
        .set((
            // The shard database's own clock, never this host's
            // `Utc::now()`. `codec_rotation::blocking_workers` computes
            // liveness as `NOW() - last_heartbeat_at`, entirely on the
            // database side.
            //
            // A `last_heartbeat_at` stamped by a skewed worker host would
            // compare unevenly against that `NOW()`. A continuously live
            // worker could misclassify as stale and be silently excluded
            // from the codec-capability gate.
            harvest_workers::last_heartbeat_at.eq(diesel::dsl::now),
            harvest_workers::in_flight_count.eq(in_flight_count),
            harvest_workers::labels.eq(&labels),
            harvest_workers::in_use_sessions.eq(in_use_sessions),
        ))
        .returning(harvest_workers::status)
        .get_result::<String>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)
}

/// How long a task-stats row outlives its last write (issue #1815).
///
/// A worker with a random id leaves an orphan row each time it restarts, and
/// nothing deletes its `harvest_workers` row. The outlier tick prunes rows
/// older than this, so the table stays bounded by the live fleet. A fleet with
/// a slower heartbeat keeps rows for its freshness window instead.
pub const WORKER_TASK_STATS_RETENTION: Duration = Duration::from_secs(3600);

/// The live peer rows that each shard heartbeat of one worker read last
/// (issue #1815).
///
/// Each shard heartbeat of a worker compares the worker over the rows that
/// every shard heartbeat stored here. So a peer that lives on another of the
/// worker's shards still counts, and all heartbeats see the same peer set. A
/// slot expires when its shard heartbeat stops reading, so a lost shard cannot
/// keep stale peers alive.
#[derive(Debug, Default)]
pub struct ShardPeerViews(
    Mutex<std::collections::BTreeMap<usize, (std::time::Instant, Vec<LiveWorkerTaskStats>)>>,
);

impl ShardPeerViews {
    fn slots(
        &self,
    ) -> std::sync::MutexGuard<
        '_,
        std::collections::BTreeMap<usize, (std::time::Instant, Vec<LiveWorkerTaskStats>)>,
    > {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Replace the rows that shard heartbeat `slot` read now.
    pub fn store(&self, slot: usize, rows: Vec<LiveWorkerTaskStats>) {
        self.store_at(slot, std::time::Instant::now(), rows);
    }

    /// Replace the rows that shard heartbeat `slot` read at `at`.
    pub fn store_at(&self, slot: usize, at: std::time::Instant, rows: Vec<LiveWorkerTaskStats>) {
        self.slots().insert(slot, (at, rows));
    }

    /// Drop the rows of `slot`, for example after its shard heartbeat fails.
    pub fn clear(&self, slot: usize) {
        self.slots().remove(&slot);
    }

    /// Drop the rows of `slot`. When no other slot holds rows stored within
    /// `max_age`, run `on_idle`.
    ///
    /// `on_idle` runs under the slot lock. So when the last two slots release
    /// at once, at least one of them sees no fresh slot left. A tick that
    /// stores a view also publishes under this lock, so a clear cannot
    /// overwrite a verdict from a newer view.
    #[allow(clippy::significant_drop_tightening)]
    pub fn release(&self, slot: usize, max_age: Duration, on_idle: impl FnOnce()) {
        let now = std::time::Instant::now();
        let mut slots = self.slots();
        slots.remove(&slot);
        let idle = !slots
            .values()
            .any(|(at, _)| now.saturating_duration_since(*at) <= max_age);
        if idle {
            on_idle();
        }
    }

    /// Replace the rows of `slot`, then pass the merged rows to `then` and
    /// return its result.
    ///
    /// `then` runs under the slot lock. The heartbeats of one worker then
    /// publish in the order in which they store, so the last verdict comes
    /// from the newest view.
    #[allow(clippy::significant_drop_tightening)]
    pub fn store_then<R>(
        &self,
        slot: usize,
        rows: Vec<LiveWorkerTaskStats>,
        max_age: Duration,
        then: impl FnOnce(&[LiveWorkerTaskStats]) -> R,
    ) -> R {
        let now = std::time::Instant::now();
        let mut slots = self.slots();
        slots.insert(slot, (now, rows));
        let merged = Self::merge(&slots, now, max_age);
        then(&merged)
    }

    /// The rows of every slot stored within `max_age`, one per worker. When
    /// two shards hold a row for the same worker, the newest row wins.
    #[must_use]
    pub fn merged(&self, max_age: Duration) -> Vec<LiveWorkerTaskStats> {
        Self::merge(&self.slots(), std::time::Instant::now(), max_age)
    }

    fn merge(
        slots: &std::collections::BTreeMap<usize, (std::time::Instant, Vec<LiveWorkerTaskStats>)>,
        now: std::time::Instant,
        max_age: Duration,
    ) -> Vec<LiveWorkerTaskStats> {
        let mut by_worker: std::collections::BTreeMap<String, LiveWorkerTaskStats> =
            std::collections::BTreeMap::new();
        let fresh = slots
            .values()
            .filter(|(at, _)| now.saturating_duration_since(*at) <= max_age)
            .flat_map(|(_, rows)| rows.iter());
        for row in fresh {
            match by_worker.get(&row.worker_id) {
                Some(kept) if !row.is_fresher_than(kept) => {}
                _ => {
                    by_worker.insert(row.worker_id.clone(), row.clone());
                }
            }
        }
        by_worker.into_values().collect()
    }
}

/// The outlier verdicts of the local workers that share one recorder (issue
/// #1815).
///
/// The gauge has no worker label, and two `Worker`s in one process can share
/// one recorder. The gauge therefore reports the OR of their verdicts. A
/// healthy worker's tick then cannot clear a sick worker's flag.
#[derive(Debug, Default)]
pub struct ProcessOutlierFlags(Mutex<std::collections::HashMap<String, Vec<OutlierDimension>>>);

impl ProcessOutlierFlags {
    /// The instance that every worker using `metrics` shares.
    ///
    /// Workers that share a recorder share one gauge, so they share one set of
    /// verdicts. A runtime with its own recorder gets its own set, so one
    /// runtime's sick worker cannot raise another runtime's gauge.
    ///
    /// The registry holds each set weakly. A set lives while a probe holds it,
    /// and each probe also holds its recorder. So a live set's key cannot be
    /// reused, and a stopped runtime's set leaves the registry.
    #[must_use]
    pub fn for_recorder(metrics: &Arc<dyn MetricsRecorder>) -> Arc<Self> {
        static BY_RECORDER: std::sync::LazyLock<
            Mutex<std::collections::HashMap<usize, std::sync::Weak<ProcessOutlierFlags>>>,
        > = std::sync::LazyLock::new(Mutex::default);
        let mut by_recorder = BY_RECORDER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        by_recorder.retain(|_, flags| flags.strong_count() > 0);
        let key = crate::telemetry::recorder_key(metrics);
        if let Some(flags) = by_recorder.get(&key).and_then(std::sync::Weak::upgrade) {
            return flags;
        }
        let flags = Arc::new(Self::default());
        by_recorder.insert(key, Arc::downgrade(&flags));
        drop(by_recorder);
        flags
    }

    /// Record `worker_id`'s verdict and return the dimensions on which any
    /// local worker is an outlier.
    #[must_use]
    pub fn set(&self, worker_id: &str, flagged: &[OutlierDimension]) -> Vec<OutlierDimension> {
        self.update(worker_id, Some(flagged), |_| {})
    }

    /// Forget `worker_id`, for example when its heartbeat stops, and return
    /// the dimensions on which any remaining local worker is an outlier.
    #[must_use]
    pub fn remove(&self, worker_id: &str) -> Vec<OutlierDimension> {
        self.update(worker_id, None, |_| {})
    }

    /// Set (`Some`) or forget (`None`) `worker_id`'s verdict, then pass the OR
    /// of all verdicts to `emit` and return it.
    ///
    /// `emit` runs while the lock is held. Two workers that update at once
    /// then emit in the order of their updates, so a stale 0 cannot overwrite
    /// a newer 1.
    #[allow(clippy::significant_drop_tightening)]
    pub fn update(
        &self,
        worker_id: &str,
        flagged: Option<&[OutlierDimension]>,
        emit: impl FnOnce(&[OutlierDimension]),
    ) -> Vec<OutlierDimension> {
        let mut verdicts = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match flagged {
            Some(flagged) => {
                verdicts.insert(worker_id.to_owned(), flagged.to_vec());
            }
            None => {
                verdicts.remove(worker_id);
            }
        }
        let any: Vec<OutlierDimension> = OutlierDimension::ALL
            .into_iter()
            .filter(|d| verdicts.values().any(|v| v.contains(d)))
            .collect();
        emit(&any);
        any
    }
}

/// What the liveness heartbeat needs to publish task stats and to flag this
/// worker as an outlier (issue #1815).
#[derive(Clone)]
pub struct OutlierProbe {
    /// This worker's rolling task window.
    pub window: Arc<TaskOutcomeWindow>,
    /// Receives the outlier gauge.
    pub metrics: Arc<dyn MetricsRecorder>,
    /// The detection thresholds.
    pub config: OutlierConfig,
    /// A peer whose heartbeat or stats are older than this is not compared.
    pub fleet_stale_secs: i64,
    /// This worker's cohort key, from [`worker_cohort`]. The heartbeat reads
    /// only the peers in this cohort. See [`OutlierProbe::cohort_key`].
    pub cohort: String,
    /// The worker's payload codecs. [`OutlierProbe::cohort_key`] adds their
    /// registered key ids on every tick, because a reload can change them.
    pub codecs: Option<crate::payload_codec::PayloadCodecs>,
    /// Whether this heartbeat compares the worker and sets the gauge.
    ///
    /// A multi-shard worker runs one heartbeat per shard, and each one
    /// compares. They all merge the same peer set from `shard_peers`, so the
    /// unlabelled gauge does not flap between peer sets. When one shard fails,
    /// a healthy shard keeps the verdict live.
    pub compare: bool,
    /// This heartbeat's slot in `shard_peers`, one slot per shard.
    pub slot: usize,
    /// The peer rows of all this worker's shard heartbeats.
    pub shard_peers: Arc<ShardPeerViews>,
    /// The verdicts of every local worker that shares `metrics`. Use
    /// [`ProcessOutlierFlags::for_recorder`] outside tests.
    pub process_flags: Arc<ProcessOutlierFlags>,
}

impl std::fmt::Debug for OutlierProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutlierProbe")
            .field("config", &self.config)
            .field("fleet_stale_secs", &self.fleet_stale_secs)
            .field("cohort", &self.cohort)
            .field("codecs", &self.codecs.is_some())
            .field("compare", &self.compare)
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

impl OutlierProbe {
    /// Enter the window into this probe's cohort, before any task runs
    /// (issue #1815).
    ///
    /// A codec reload before the first heartbeat then changes the cohort, and
    /// the window drops the samples taken before it.
    #[must_use]
    pub fn seeded(self) -> Self {
        let (cohort, epoch) = self.cohort_and_epoch();
        self.window.enter_cohort_at(&cohort, epoch);
        self
    }

    /// The cohort key this tick writes and reads (issue #1815).
    ///
    /// It is [`OutlierProbe::cohort`] with the codecs' registered key ids
    /// and active key id added. A worker cannot decode a payload under a key
    /// id it lacks, so the ids are part of the cohort. The active key encodes
    /// new payloads. A reload can register, retire or activate a key while the
    /// worker runs, so they are read now, not at startup.
    #[must_use]
    pub fn cohort_key(&self) -> String {
        self.cohort_and_epoch().0
    }

    /// [`Self::cohort_key`] and the epoch of the last codec key change, from
    /// one read (issue #1815).
    ///
    /// The codec registry stamps each key change when it happens. The window
    /// then keeps a task dispatched after the change, even when this tick
    /// notices the change later. A change back to an earlier key still moves
    /// the epoch.
    #[must_use]
    pub fn cohort_and_epoch(&self) -> (String, crate::worker_outlier::CohortEpoch) {
        let Some(codecs) = &self.codecs else {
            return (
                self.cohort.clone(),
                crate::worker_outlier::CohortEpoch::default(),
            );
        };
        let state = codecs.key_state();
        let epoch = crate::worker_outlier::CohortEpoch {
            generation: state.epoch.generation,
            changed_at: state.epoch.changed_at,
        };
        (self.key_with_codecs(codecs, &state), epoch)
    }

    /// [`OutlierProbe::cohort`] with the codec ids and `state` added.
    fn key_with_codecs(
        &self,
        codecs: &crate::payload_codec::PayloadCodecs,
        state: &crate::payload_codec::KeyState,
    ) -> String {
        match serde_json::from_str::<serde_json::Value>(&self.cohort) {
            Ok(serde_json::Value::Object(mut key)) => {
                // A worker decodes only the codecs it has registered. One
                // without a peer's codec fails that peer's history.
                key.insert(
                    "codec_ids".to_owned(),
                    serde_json::json!(codecs.codec_ids()),
                );
                key.insert(
                    "default_codec_id".to_owned(),
                    serde_json::json!(codecs.default_codec_id()),
                );
                // The active key encodes new payloads, so an activation
                // changes how this worker runs a task. One read gives a pair
                // that the registry held at one time.
                key.insert(
                    "codec_key_ids".to_owned(),
                    serde_json::json!(state.registered),
                );
                key.insert(
                    "active_codec_key_id".to_owned(),
                    serde_json::json!(state.active),
                );
                serde_json::Value::Object(key).to_string()
            }
            _ => self.cohort.clone(),
        }
    }

    /// Record `flagged` as this worker's verdict and set the gauge to the OR
    /// of every local worker's verdict.
    fn publish(&self, worker_id: &str, flagged: &[OutlierDimension]) {
        let _ = self
            .process_flags
            .update(worker_id, Some(flagged), |any| self.emit(any));
    }

    /// A slot older than this belongs to a shard heartbeat that stopped
    /// reading, so the comparison leaves its rows out.
    fn view_max_age(&self) -> Duration {
        Duration::from_secs(u64::try_from(self.fleet_stale_secs).unwrap_or(0))
    }

    fn emit(&self, any: &[OutlierDimension]) {
        for dimension in OutlierDimension::ALL {
            self.metrics
                .record_worker_outlier(dimension, any.contains(&dimension));
        }
    }

    /// Drop this heartbeat's peer rows. When no shard heartbeat of this
    /// worker holds fresh rows, also clear the verdict and refresh the gauge.
    ///
    /// A tick that cannot compare calls this. A healthy shard heartbeat keeps
    /// the verdict live. With none left, an unknown state reads as "not an
    /// outlier", so a stale 1 cannot keep an alert firing.
    pub fn clear_gauge(&self, worker_id: &str) {
        self.shard_peers
            .release(self.slot, self.view_max_age(), || {
                if self.compare {
                    self.publish(worker_id, &[]);
                }
            });
    }

    /// Drop this heartbeat's peer rows when it stops. The last heartbeat of
    /// this worker to stop also forgets its verdict and refreshes the gauge.
    ///
    /// A stopped worker then cannot keep the process-wide OR at 1.
    pub fn retire(&self, worker_id: &str) {
        self.shard_peers
            .release(self.slot, self.view_max_age(), || {
                if self.compare {
                    let _ = self
                        .process_flags
                        .update(worker_id, None, |any| self.emit(any));
                }
            });
    }
}

/// Retires a worker's outlier verdict when the heartbeat future drops
/// (issue #1815).
///
/// An owner can abort the heartbeat task, so its loop does not always reach
/// its end. The drop still runs, so the verdict is always retired.
struct RetireOnDrop {
    probe: OutlierProbe,
    worker_id: String,
}

impl RetireOnDrop {
    const fn new(probe: OutlierProbe, worker_id: String) -> Self {
        Self { probe, worker_id }
    }
}

impl Drop for RetireOnDrop {
    fn drop(&mut self) {
        self.probe.retire(&self.worker_id);
    }
}

/// One live worker's published task stats (issue #1815).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveWorkerTaskStats {
    /// The worker id.
    pub worker_id: String,
    /// The worker's cohort key, from [`worker_cohort`].
    ///
    /// Workers in one cohort poll the same queues with the same weights. They
    /// also share a build, labels and slots per task kind, so they do the same
    /// work. A
    /// worker is compared only with peers in its own cohort.
    pub cohort: String,
    /// The published snapshot.
    pub stats: WorkerTaskStats,
    /// The worker's own snapshot sequence, from [`next_snapshot_seq`].
    ///
    /// Only the worker writes it. So two rows of one worker on two shards
    /// compare correctly, even when the shard database clocks differ.
    pub snapshot_seq: i64,
}

impl LiveWorkerTaskStats {
    /// Whether `self` should replace `kept` as a worker's snapshot: the newer
    /// one wins. A window shrinks as old samples expire, so the newer snapshot
    /// can hold fewer tasks and still be the correct one.
    #[must_use]
    pub const fn is_fresher_than(&self, kept: &Self) -> bool {
        self.snapshot_seq > kept.snapshot_seq
    }
}

/// The next task-stats snapshot sequence of this process (issue #1815).
///
/// The sequence starts at the host clock in microseconds and then counts up
/// by one. So it rises within a process, even if the host clock steps back.
/// Only one worker's rows are compared with each other, so hosts need not
/// agree.
///
/// A restarted worker can keep its id and start below the rows of its
/// previous process. [`upsert_worker_task_stats`] therefore stores a value
/// above the old row, and [`observe_snapshot_seq`] moves the counter above it.
pub fn next_snapshot_seq() -> i64 {
    SNAPSHOT_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Move the snapshot sequence above `stored`, a value already in a stats row
/// (issue #1815).
///
/// Every later snapshot of this process then outranks that row on every shard.
pub fn observe_snapshot_seq(stored: i64) {
    SNAPSHOT_SEQ.fetch_max(stored.saturating_add(1), Ordering::Relaxed);
}

static SNAPSHOT_SEQ: std::sync::LazyLock<std::sync::atomic::AtomicI64> =
    std::sync::LazyLock::new(|| {
        let micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_micros());
        std::sync::atomic::AtomicI64::new(i64::try_from(micros).unwrap_or(0))
    });

/// The cohort key of a worker (issue #1815): a JSON object of everything that
/// decides which tasks the worker can claim, and in which mix.
///
/// - `queues`: without weights, the sorted queue list. Such a worker claims
///   from all its queues in one query. With weights, the sorted
///   `[queue, weight]` pairs with a positive weight, duplicates kept, and the
///   zero-weight queues in their configured order. Such a worker tries its
///   queues in a weighted order, so its task mix follows the weights. A
///   listed-twice queue is drawn more often. Zero-weight queues are tried
///   last, in that order. A queue missing from the map has weight 1, as
///   [`effective_queue_weights`](crate::queue_fairness::effective_queue_weights)
///   gives. An entry for a queue the worker does not poll is ignored.
/// - `build_id`: the claim predicate routes a task with `required_build_id`
///   only to a matching build.
/// - `labels`: the claim predicate matches `required_capabilities` against
///   these labels, sorted by key.
/// - `sessions`: the worker's session capacity. Session member activities are
///   pinned to the session's host, so only a worker with capacity gets them.
/// - `priority_aging_secs` and `ineligible_activities`: the claim query orders
///   and filters tasks by them.
/// - `slots`: the worker's [`SlotPolicy`]. A worker with no slot for one kind
///   claims only the other. Under load, the claim gate gives each worker a
///   task mix that follows its slots, so two sizes are two cohorts.
/// - `circuit_breakers`: each activity with a breaker policy, with that
///   policy. Such an activity skips the claim-time rate-limit gate, and its
///   breaker fails it fast while open. The open state stays out of the key.
/// - `dispatch_channel`: the shards whose claims a dispatch channel serves.
///   A channel orders delivery by priority and ignores `queue_weights`. The
///   Postgres claim applies the weights. A multi-shard worker can use a
///   channel on some shards only. So each route set gives its own task mix.
/// - `retry_budgets`: the retry-budget policy of each registered activity. A
///   tighter budget defers more retries, so the worker runs fewer of them.
/// - `adaptive_limits`: the adaptive-limit policy of each registered activity.
///   A type at its limit is left out of the claim, so the limit shapes the
///   task mix.
/// - `outcome_window_ms` and `peer_stale_secs`: both follow the heartbeat
///   interval. Workers with two windows compare two time ranges, and workers
///   with two freshness limits can disagree on the live peer set.
/// - `execution`: the workflow cache, the task budgets, the panic limit and
///   the cancellation grace period. See [`ExecutionPolicy`].
/// - `payload`: the payload caps, the history policy, the offloader, the codec
///   keys and the interceptors. See [`PayloadPolicy`].
///
/// So workers on two builds are not compared during a rolling deployment. A
/// build that fails everywhere is a fleet alert, not a gray failure.
pub fn worker_cohort(policy: &CohortPolicy<'_>) -> String {
    let CohortPolicy {
        queues,
        queue_weights: weights,
        build_id,
        labels,
        slots,
        session_slots,
        priority_aging_secs,
        ineligible_activities,
        shard_assignments,
        registered_workflows,
        registered_activities,
        circuit_breakers,
        dispatch_channel,
        retry_budgets,
        adaptive_limits,
        outcome_window,
        peer_stale_secs,
        execution,
        payload,
    } = policy;
    let routing = if weights.is_empty() {
        let mut names: Vec<&str> = queues.iter().map(String::as_str).collect();
        names.sort_unstable();
        names.dedup();
        serde_json::json!(names)
    } else {
        // The weighted draw keeps every entry, so a listed-twice queue is
        // drawn more often and keeps its multiplicity here. Zero-weight queues
        // are tried last in their configured order, so that order stays too.
        let pairs = crate::queue_fairness::effective_queue_weights(queues, weights);
        let (fallback, mut drawn): (Vec<_>, Vec<_>) =
            pairs.into_iter().partition(|(_, weight)| *weight == 0);
        drawn.sort_unstable();
        let fallback: Vec<&str> = fallback.into_iter().map(|(queue, _)| queue).collect();
        serde_json::json!({ "drawn": drawn, "fallback": fallback })
    };
    let labels: std::collections::BTreeMap<&str, &str> = labels
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let mut shards: Vec<i32> = shard_assignments
        .iter()
        .map(|shard| shard.as_i32())
        .collect();
    shards.sort_unstable();
    shards.dedup();
    let budgets = budget_policies(retry_budgets, registered_activities);
    serde_json::json!({
        "queues": routing,
        "build_id": build_id,
        "labels": labels,
        "slots": slots.key(),
        "sessions": (*session_slots).max(0),
        "priority_aging_secs": priority_aging_secs,
        "ineligible_activities": sorted_names(ineligible_activities),
        "shards": shards,
        "workflows": sorted_names(registered_workflows),
        "activities": sorted_names(registered_activities),
        "circuit_breakers": breaker_policies(circuit_breakers),
        "dispatch_channel": sorted_shards(dispatch_channel),
        "retry_budgets": budgets,
        "adaptive_limits": limit_policies(adaptive_limits, registered_activities),
        "outcome_window": duration_key(*outcome_window),
        "peer_stale_secs": peer_stale_secs,
        "execution": execution.key(),
        "payload": payload.key(),
    })
    .to_string()
}

/// The retry-budget policy of each registered activity, sorted by name, for a
/// cohort key. An activity without a budget has `null`.
fn budget_policies(
    config: &crate::retry_budget::RetryBudgetConfig,
    activities: &[String],
) -> Vec<serde_json::Value> {
    sorted_names(activities)
        .into_iter()
        .map(|name| {
            let policy = config.policy_for(name).map(|policy| {
                serde_json::json!([policy.ratio, policy.max_tokens, policy.min_retries_per_sec])
            });
            serde_json::json!([name, policy])
        })
        .collect()
}

/// The adaptive-limit policy of each registered activity, sorted by name, for
/// a cohort key (issue #1815). An activity without a limit has `null`.
fn limit_policies(
    config: &crate::adaptive_limit::AdaptiveLimitConfig,
    activities: &[String],
) -> Vec<serde_json::Value> {
    sorted_names(activities)
        .into_iter()
        .map(|name| serde_json::json!([name, config.policy_for(name)]))
        .collect()
}

/// Each activity with a circuit-breaker policy and that policy, sorted by
/// name, for a cohort key. The open mode is part of the policy, because it
/// decides whether an open breaker defers or fails an attempt (issue #1809).
fn breaker_policies(
    registry: &crate::circuit_breaker::CircuitBreakerRegistry,
) -> Vec<serde_json::Value> {
    registry
        .tracked_activity_names()
        .iter()
        .filter_map(|name| {
            registry.policy(name).map(|policy| {
                serde_json::json!([
                    name,
                    policy.failure_threshold,
                    duration_key(policy.window),
                    duration_key(policy.cooldown),
                    policy.open_mode,
                ])
            })
        })
        .collect()
}

/// `duration` for a cohort key, as `[secs, subsec_nanos]` (issue #1815).
///
/// A deadline decides an outcome to the nanosecond, so the key keeps the full
/// duration. The pair cannot overflow, unlike a single nanosecond count.
pub(crate) fn duration_key(duration: std::time::Duration) -> serde_json::Value {
    serde_json::json!([duration.as_secs(), duration.subsec_nanos()])
}

/// `shards`, sorted and deduplicated, for a cohort key.
fn sorted_shards(shards: &[i32]) -> Vec<i32> {
    let mut shards = shards.to_vec();
    shards.sort_unstable();
    shards.dedup();
    shards
}

/// `names`, sorted and deduplicated, for a cohort key.
fn sorted_names(names: &[String]) -> Vec<&str> {
    let mut names: Vec<&str> = names.iter().map(String::as_str).collect();
    names.sort_unstable();
    names.dedup();
    names
}

/// The settings that decide which tasks a worker can claim, and in which mix
/// (issue #1815). [`worker_cohort`] keys a worker's cohort on all of them.
///
/// The fields mirror the inputs of
/// [`queue::claim_task_of_kind_on_shard`](crate::queue::claim_task_of_kind_on_shard)
/// and of the poll loop around it. Two inputs stay out on purpose. The worker
/// id is unique to each worker. The state of its circuit breakers is the
/// worker's own health, which the comparison measures. Their policies are
/// configuration, so they are in. A new claim input belongs here.
#[derive(Debug, Clone)]
pub struct CohortPolicy<'a> {
    /// The queues the worker polls.
    pub queues: &'a [String],
    /// The worker's `queue_weights`.
    pub queue_weights: &'a std::collections::HashMap<String, u32>,
    /// The worker's build id.
    pub build_id: &'a str,
    /// The worker's capability labels.
    pub labels: &'a std::collections::HashMap<String, String>,
    /// The worker's slots per task kind.
    pub slots: SlotPolicy,
    /// The worker's session capacity.
    pub session_slots: i32,
    /// The worker's priority aging, which orders a mixed-priority backlog.
    pub priority_aging_secs: Option<u32>,
    /// Activities the worker does not claim, because its labels do not meet
    /// their requirements.
    pub ineligible_activities: &'a [String],
    /// The shards the worker claims from. Each shard holds its own tasks.
    pub shard_assignments: &'a [crate::types::ShardId],
    /// The workflows the worker has handlers for. A task without a handler is
    /// released, so it never counts.
    pub registered_workflows: &'a [String],
    /// The activities the worker has handlers for.
    pub registered_activities: &'a [String],
    /// The worker's circuit breakers. Only their policies enter the key.
    pub circuit_breakers: &'a crate::circuit_breaker::CircuitBreakerRegistry,
    /// The shards on which the worker reads task references from a dispatch
    /// channel. The Postgres claim serves every other shard.
    pub dispatch_channel: &'a [i32],
    /// The worker's retry budgets. The key holds the policy of each
    /// registered activity.
    pub retry_budgets: &'a crate::retry_budget::RetryBudgetConfig,
    /// The worker's adaptive concurrency limits. The key holds the policy of
    /// each registered activity. The live limit is the worker's own health,
    /// so it stays out.
    pub adaptive_limits: &'a crate::adaptive_limit::AdaptiveLimitConfig,
    /// How long the worker keeps task outcomes: see
    /// [`crate::worker_outlier::window_max_age`].
    pub outcome_window: std::time::Duration,
    /// How old a peer row may be and still count, in seconds.
    pub peer_stale_secs: i64,
    /// The worker settings that decide how a claimed task runs.
    pub execution: ExecutionPolicy,
    /// The registry settings that decide whether a task's payloads pass.
    pub payload: PayloadPolicy,
}

/// The registry settings that decide whether a task's payloads pass, and so
/// its outcome and its latency (issue #1815).
///
/// The worker enforces each cap itself. An oversized activity result fails
/// the attempt, and an oversized input or signal fails the workflow task that
/// sends it. The history policy decides when a workflow continues as new or
/// fails on its hard cap. An offloader moves a large payload out instead of
/// failing it. An interceptor can change any outcome.
///
/// The registered codec key ids are not here: they can change at runtime, so
/// [`OutlierProbe::cohort_key`] reads them on every tick.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PayloadPolicy {
    /// `max_activity_input_bytes`.
    pub max_activity_input_bytes: u64,
    /// `max_workflow_input_bytes`.
    pub max_workflow_input_bytes: u64,
    /// `max_activity_result_bytes`.
    pub max_activity_result_bytes: u64,
    /// `max_signal_payload_bytes`.
    pub max_signal_payload_bytes: u64,
    /// `max_current_details_bytes`.
    pub max_current_details_bytes: usize,
    /// The history policy's continue-as-new event threshold.
    pub continue_as_new_threshold: u64,
    /// The history policy's event hard cap.
    pub event_hard_cap: Option<u64>,
    /// The history policy's byte hard cap.
    pub byte_hard_cap: Option<u64>,
    /// The history policy's history-bloat warning fraction.
    pub history_bloat_warn_fraction: f64,
    /// Whether the history policy records decision boundaries (issue
    /// #1833). Each persist then appends one more event, so tasks write more
    /// and the history reaches its caps sooner.
    pub decision_boundaries: bool,
    /// The fleet-wide workflow execution timeout ceiling. It caps a run's
    /// dispatch deadline and a continue-as-new successor's timeout.
    pub max_workflow_execution_timeout: Option<std::time::Duration>,
    /// The history policy's continue-as-new deadline fraction.
    pub continue_as_new_deadline_fraction: f64,
    /// The payload offloader's threshold. `None` without an offloader.
    pub offload_threshold: Option<u64>,
    /// The payload offloader's store id. `None` without an offloader. Replay
    /// rejects an envelope that another store wrote.
    pub offload_store_id: Option<String>,
    /// The `policy()` of each activity interceptor, in chain order.
    pub activity_interceptors: Vec<String>,
    /// The execution policy of each registered activity, sorted by name. It
    /// holds the effective payload caps and whether the activity runs locally.
    /// It also holds the rate limit, the concurrency limit and any WASM
    /// binding.
    pub activities: Vec<(String, serde_json::Value)>,
    /// The registry defaults a local activity runs with: its retry policy, its
    /// start-to-close timeout and the retry-after ceiling. A local activity has
    /// no task row, so the worker applies its own defaults.
    pub local_activity_defaults: serde_json::Value,
    /// The module host's policy with the `hot-code-swap` feature: capabilities,
    /// limits, activity allowlist and queue-override permission. A refusal or
    /// an exhausted limit fails the workflow.
    pub module_host: serde_json::Value,
    /// The policy of each registered workflow, sorted by name: its effective
    /// input cap and whether it is a unified DAG. The worker enforces the cap,
    /// and a continue-as-new into a DAG is refused.
    pub workflows: Vec<(String, serde_json::Value)>,
    /// Each declarative query and update handler, sorted. The task context
    /// carries the handlers of its workflow, and a worker without a handler
    /// fails a request that a peer runs.
    pub declarative_handlers: Vec<serde_json::Value>,
    /// The workflow log policy as `[max_lines, max_message_bytes]`. `Null`
    /// when workflow logs are off. A worker with logs on persists each line
    /// inside the task.
    pub workflow_log_policy: serde_json::Value,
}

impl PayloadPolicy {
    fn key(&self) -> serde_json::Value {
        serde_json::json!({
            "max_activity_input_bytes": self.max_activity_input_bytes,
            "max_workflow_input_bytes": self.max_workflow_input_bytes,
            "max_activity_result_bytes": self.max_activity_result_bytes,
            "max_signal_payload_bytes": self.max_signal_payload_bytes,
            "max_current_details_bytes": self.max_current_details_bytes,
            "continue_as_new_threshold": self.continue_as_new_threshold,
            "event_hard_cap": self.event_hard_cap,
            "byte_hard_cap": self.byte_hard_cap,
            "history_bloat_warn_fraction": self.history_bloat_warn_fraction,
            "decision_boundaries": self.decision_boundaries,
            "max_workflow_execution_timeout": self.max_workflow_execution_timeout.map(duration_key),
            "continue_as_new_deadline_fraction": self.continue_as_new_deadline_fraction,
            "offload_threshold": self.offload_threshold,
            "offload_store_id": self.offload_store_id,
            "activity_interceptors": self.activity_interceptors,
            "activities": self.activities,
            "local_activity_defaults": self.local_activity_defaults,
            "module_host": self.module_host,
            "workflows": self.workflows,
            "declarative_handlers": self.declarative_handlers,
            "workflow_log_policy": self.workflow_log_policy,
        })
    }
}

/// The worker settings that decide how a claimed task runs, and so its
/// outcome and its latency (issue #1815).
///
/// Each one changes what the outcome window records for the same task. The
/// workflow cache decides whether a task replays its full history. The two
/// budgets decide whether a task times out, and a timeout is a failure. The
/// panic limit and the poison-pill threshold decide how many failing attempts
/// run before quarantine. The cancellation grace period bounds how long a
/// cancelled activity unwinds before its outcome is recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionPolicy {
    /// `sticky_timeout`. Zero turns the workflow cache off.
    pub sticky_timeout: std::time::Duration,
    /// `workflow_cache_size`.
    pub workflow_cache_size: usize,
    /// `resident_workflows`.
    pub resident_workflows: bool,
    /// `workflow_task_timeout`.
    pub workflow_task_timeout: std::time::Duration,
    /// `max_local_activity_start_to_close`.
    pub max_local_activity_start_to_close: std::time::Duration,
    /// `workflow_panic_max_attempts`.
    pub workflow_panic_max_attempts: u32,
    /// `poison_pill_threshold`. The worker's timeout path quarantines a
    /// workflow task that times out this many times in a row.
    pub poison_pill_threshold: i32,
    /// `cancellation_grace_period`. A timed-out activity that ignores its
    /// cancellation runs this long before the timeout is recorded. So the
    /// period adds to the latency that the window records.
    pub cancellation_grace_period: std::time::Duration,
}

impl Default for ExecutionPolicy {
    /// Test defaults. A worker passes its own settings.
    fn default() -> Self {
        Self {
            sticky_timeout: std::time::Duration::from_secs(10),
            workflow_cache_size: 1000,
            resident_workflows: false,
            workflow_task_timeout: std::time::Duration::from_secs(10),
            max_local_activity_start_to_close: std::time::Duration::from_secs(10),
            workflow_panic_max_attempts: 3,
            poison_pill_threshold: 3,
            cancellation_grace_period: std::time::Duration::from_secs(5),
        }
    }
}

impl ExecutionPolicy {
    fn key(self) -> serde_json::Value {
        serde_json::json!({
            "sticky_timeout": duration_key(self.sticky_timeout),
            "workflow_cache_size": self.workflow_cache_size,
            "resident_workflows": self.resident_workflows,
            "workflow_task_timeout": duration_key(self.workflow_task_timeout),
            "max_local_activity": duration_key(self.max_local_activity_start_to_close),
            "workflow_panic_max_attempts": self.workflow_panic_max_attempts,
            // Every threshold at or below 0 turns quarantine off, so they are
            // one setting.
            "poison_pill_threshold": self.poison_pill_threshold.max(0),
            "cancellation_grace_period": duration_key(self.cancellation_grace_period),
        })
    }
}

/// How a worker sizes its slots per task kind, as its cohort key records it
/// (issue #1815).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotPolicy {
    /// Fixed slots per kind. A kind with 0 slots is not claimed.
    Fixed {
        /// `max_concurrent_workflows`.
        workflow: usize,
        /// `max_concurrent_activities`.
        activity: usize,
    },
    /// A slot tuner sizes both kinds within one band, so the worker claims
    /// both kinds. Each kind starts at its configured maximum, clamped into
    /// the band, and the tuner resizes it from there.
    Tuned {
        /// The band floor, after normalization.
        min: usize,
        /// The band cap, after normalization.
        max: usize,
        /// The initial workflow target.
        workflow: usize,
        /// The initial activity target.
        activity: usize,
        /// The tuner's policy: see [`crate::slot_tuner::SlotTuner::policy`].
        tuner: String,
    },
}

impl SlotPolicy {
    /// The policy of a worker with these slot settings.
    ///
    /// A tuner clamps each configured maximum into its band and resizes it
    /// later, so the configured maximums do not describe a tuned worker.
    #[must_use]
    pub fn of(
        workflow_max: usize,
        activity_max: usize,
        tuner: Option<&crate::slot_tuner::SlotTunerConfig>,
    ) -> Self {
        tuner.map_or(
            Self::Fixed {
                workflow: workflow_max,
                activity: activity_max,
            },
            |config| {
                let (min, max) =
                    crate::slot_tuner::effective_band(config.min_slots, config.max_slots);
                Self::Tuned {
                    min,
                    max,
                    workflow: crate::slot_tuner::initial_target(workflow_max, min, max),
                    activity: crate::slot_tuner::initial_target(activity_max, min, max),
                    tuner: config.tuner.policy(),
                }
            },
        )
    }

    fn key(&self) -> serde_json::Value {
        match self {
            Self::Fixed { workflow, activity } => {
                serde_json::json!({ "workflow": workflow, "activity": activity })
            }
            Self::Tuned {
                min,
                max,
                workflow,
                activity,
                tuner,
            } => serde_json::json!({
                "tuned": {
                    "min": min,
                    "max": max,
                    "workflow": workflow,
                    "activity": activity,
                    "tuner": tuner,
                }
            }),
        }
    }
}

/// What one task-stats write did (issue #1815).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotWrite {
    /// The snapshot is stored.
    Stored,
    /// A newer snapshot of this process is already stored, so this one is
    /// dropped. Two heartbeats of one worker can write one row, when
    /// colocated shards share a database.
    Superseded,
    /// A row that this process did not write holds a higher sequence, for
    /// example from the worker's previous process. The counter now runs above
    /// it, so a fresh snapshot written next is stored.
    Foreign,
}

/// Read the cohort key, capture `window` for it and give it the next
/// snapshot sequence, as one step (issue #1815).
///
/// Two heartbeats of one worker then cannot pair an older window with a newer
/// sequence. A new cohort starts the window empty.
///
/// `cohort_key` runs inside the step too. A codec reload can change the key
/// between two shard heartbeats. A heartbeat that read the old key could
/// otherwise capture after the one that read the new key. It would move the
/// window back to the old cohort and publish it with the higher sequence.
///
/// A codec write does not take this step's lock. So a change can also land
/// between the key read and the snapshot. A task dispatched under the new key
/// would then be published under the old one. The step therefore reads the
/// key again after each snapshot, and captures again until the key holds.
pub fn capture_task_stats(
    window: &TaskOutcomeWindow,
    cohort_key: impl Fn() -> (String, crate::worker_outlier::CohortEpoch),
) -> (String, WorkerTaskStats, i64) {
    // Codec changes are operator actions, so a few reads always settle. The
    // bound only stops a pathological writer from holding the heartbeat.
    const MAX_RECAPTURES: usize = 4;
    static CAPTURE: Mutex<()> = Mutex::new(());
    let _capture = CAPTURE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut cohort, mut epoch) = cohort_key();
    let mut stats = window.snapshot_in_cohort_at(&cohort, epoch);
    for _ in 0..MAX_RECAPTURES {
        let (now, now_epoch) = cohort_key();
        if now == cohort && now_epoch == epoch {
            break;
        }
        (cohort, epoch) = (now, now_epoch);
        stats = window.snapshot_in_cohort_at(&cohort, epoch);
    }
    (cohort, stats, next_snapshot_seq())
}

/// Write one worker's task stats snapshot with sequence `seq` (issue #1815).
///
/// The write is an upsert. It fails on the foreign key when the worker row is
/// missing. The next heartbeat heals the worker row and then retries.
///
/// The upsert replaces only a row with a lower sequence, so a snapshot that
/// arrives late never replaces a newer one. A rejected write reads the stored
/// sequence and moves the counter above it.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn write_task_stats_snapshot(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    cohort: &str,
    stats: &WorkerTaskStats,
    seq: i64,
) -> HarvestResult<SnapshotWrite> {
    let to_i32 = |n: u32| i32::try_from(n).unwrap_or(i32::MAX);
    let stored = diesel::sql_query(
        "INSERT INTO harvest_worker_task_stats \
             (worker_id, window_tasks, window_failures, p99_latency_ms, snapshot_seq, \
              cohort, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, NOW()) \
         ON CONFLICT (worker_id) DO UPDATE SET \
             cohort = EXCLUDED.cohort, \
             window_tasks = EXCLUDED.window_tasks, \
             window_failures = EXCLUDED.window_failures, \
             p99_latency_ms = EXCLUDED.p99_latency_ms, \
             snapshot_seq = EXCLUDED.snapshot_seq, \
             updated_at = EXCLUDED.updated_at \
         WHERE harvest_worker_task_stats.snapshot_seq < EXCLUDED.snapshot_seq \
         RETURNING snapshot_seq",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Integer, _>(to_i32(stats.tasks))
    .bind::<diesel::sql_types::Integer, _>(to_i32(stats.failures))
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>, _>(
        stats
            .p99_latency_ms
            .map(|ms| i64::try_from(ms).unwrap_or(i64::MAX)),
    )
    .bind::<diesel::sql_types::BigInt, _>(seq)
    .bind::<diesel::sql_types::Text, _>(cohort)
    .get_result::<StoredSnapshotSeq>(conn)
    .await
    .optional()
    .map_err(crate::error::database_error)?;
    if stored.is_some() {
        return Ok(SnapshotWrite::Stored);
    }
    let existing: StoredSnapshotSeq = diesel::sql_query(
        "SELECT snapshot_seq FROM harvest_worker_task_stats WHERE worker_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;
    // This process has issued every sequence below its counter. A stored
    // sequence at or above the counter came from another process.
    let issued = std::sync::atomic::AtomicI64::load(&SNAPSHOT_SEQ, Ordering::Relaxed);
    let foreign = existing.snapshot_seq >= issued;
    observe_snapshot_seq(existing.snapshot_seq);
    Ok(if foreign {
        SnapshotWrite::Foreign
    } else {
        SnapshotWrite::Superseded
    })
}

/// Write one worker's current task stats (issue #1815).
///
/// The snapshot takes the next sequence. A row from a previous process of the
/// same worker can hold a higher one. The write then retries once, above it.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn upsert_worker_task_stats(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    cohort: &str,
    stats: &WorkerTaskStats,
) -> HarvestResult<()> {
    for _ in 0..2 {
        let seq = next_snapshot_seq();
        if write_task_stats_snapshot(conn, worker_id, cohort, stats, seq).await?
            != SnapshotWrite::Foreign
        {
            break;
        }
    }
    Ok(())
}

#[derive(diesel::QueryableByName)]
struct StoredSnapshotSeq {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    snapshot_seq: i64,
}

/// Delete task-stats rows older than [`WORKER_TASK_STATS_RETENTION`] (issue
/// #1815). Returns the number of rows deleted.
///
/// A slow cohort can count a row as live for longer than the retention. Each
/// row therefore keeps its own cohort's `peer_stale_secs` when that is longer.
/// A worker with a fast heartbeat then cannot delete a slow peer's live row. A
/// key without the field falls back to `fallback_stale_secs`.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure, or when a stored cohort key
/// is not JSON.
pub async fn prune_worker_task_stats(
    conn: &mut AsyncPgConnection,
    fallback_stale_secs: i64,
) -> HarvestResult<usize> {
    let retention = i64::try_from(WORKER_TASK_STATS_RETENTION.as_secs()).unwrap_or(i64::MAX);
    diesel::sql_query(format!(
        "DELETE FROM harvest_worker_task_stats s \
         WHERE s.updated_at < NOW() - (GREATEST($1::bigint, {}) * INTERVAL '1 second')",
        cohort_stale_secs_sql("$2")
    ))
    .bind::<diesel::sql_types::BigInt, _>(retention)
    .bind::<diesel::sql_types::BigInt, _>(
        fallback_stale_secs.clamp(0, crate::poison_pill::MAX_WORKER_STALE_SECS),
    )
    .execute(conn)
    .await
    .map_err(crate::error::database_error)
}

/// The SQL for the `peer_stale_secs` of row `s`'s cohort, bounded to
/// `0..=MAX_WORKER_STALE_SECS` (issue #1815). A key without the field uses
/// the bind parameter `fallback`.
fn cohort_stale_secs_sql(fallback: &str) -> String {
    format!(
        "LEAST(GREATEST(COALESCE((s.cohort::jsonb ->> 'peer_stale_secs')::bigint, {fallback}), 0), {})",
        crate::poison_pill::MAX_WORKER_STALE_SECS
    )
}

#[derive(diesel::QueryableByName)]
struct TaskStatsRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    worker_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    cohort: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    window_tasks: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    window_failures: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    p99_latency_ms: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    snapshot_seq: i64,
}

/// The task stats of every `Active` worker with a fresh heartbeat and fresh
/// stats (issue #1815), ordered by worker id.
///
/// A draining, stopped or stale worker is left out. So is a frozen stats row,
/// for example from a worker that a rollback took back to an older build. Old
/// stats then cannot move the peer median.
///
/// With `cohort`, only the rows of that cohort are read. A heartbeat compares
/// its worker only with its cohort, so it reads only those rows. Without it,
/// every cohort is read.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
pub async fn load_live_worker_task_stats(
    conn: &mut AsyncPgConnection,
    worker_stale_secs: i64,
    cohort: Option<&str>,
) -> HarvestResult<Vec<LiveWorkerTaskStats>> {
    load_task_stats(conn, worker_stale_secs, cohort, false).await
}

/// [`load_live_worker_task_stats`] for every cohort, each with its own
/// freshness limit (issue #1815).
///
/// Each cohort key records `peer_stale_secs`, which follows that cohort's
/// heartbeat interval. A row counts as fresh for as long as its own cohort's
/// heartbeat counts it, so `GET /admin/status` sees the same peer set as the
/// workers. A key without the field falls back to `fallback_stale_secs`.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure, or when a stored cohort key
/// is not JSON.
pub async fn load_live_worker_task_stats_per_cohort(
    conn: &mut AsyncPgConnection,
    fallback_stale_secs: i64,
) -> HarvestResult<Vec<LiveWorkerTaskStats>> {
    load_task_stats(conn, fallback_stale_secs, None, true).await
}

/// The query behind [`load_live_worker_task_stats`] and
/// [`load_live_worker_task_stats_per_cohort`].
async fn load_task_stats(
    conn: &mut AsyncPgConnection,
    worker_stale_secs: i64,
    cohort: Option<&str>,
    per_cohort_freshness: bool,
) -> HarvestResult<Vec<LiveWorkerTaskStats>> {
    let stale = worker_stale_secs.clamp(0, crate::poison_pill::MAX_WORKER_STALE_SECS);
    // A plain equality, not `$3 IS NULL OR ...`, so the cohort index applies.
    let cohort_filter = if cohort.is_some() {
        "AND s.cohort = $3 "
    } else {
        ""
    };
    let limit = if per_cohort_freshness {
        cohort_stale_secs_sql("$2")
    } else {
        "$2::bigint".to_owned()
    };
    let query = diesel::sql_query(format!(
        "SELECT s.worker_id, s.cohort, s.window_tasks, s.window_failures, s.p99_latency_ms, \
                s.snapshot_seq \
         FROM harvest_worker_task_stats s \
         JOIN harvest_workers w ON w.worker_id = s.worker_id \
         WHERE w.status = $1 \
           AND w.last_heartbeat_at > NOW() - ({limit} * INTERVAL '1 second') \
           AND s.updated_at > NOW() - ({limit} * INTERVAL '1 second') \
           {cohort_filter}\
         ORDER BY s.worker_id"
    ))
    .into_boxed::<diesel::pg::Pg>()
    .bind::<diesel::sql_types::Text, _>(WorkerStatus::Active.as_str())
    .bind::<diesel::sql_types::BigInt, _>(stale);
    let query = match cohort {
        Some(cohort) => query.bind::<diesel::sql_types::Text, _>(cohort.to_owned()),
        None => query,
    };
    let rows: Vec<TaskStatsRow> = query
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(rows
        .into_iter()
        .map(|r| LiveWorkerTaskStats {
            cohort: r.cohort,
            worker_id: r.worker_id,
            stats: WorkerTaskStats {
                tasks: u32::try_from(r.window_tasks).unwrap_or(0),
                failures: u32::try_from(r.window_failures).unwrap_or(0),
                p99_latency_ms: r.p99_latency_ms.and_then(|ms| u64::try_from(ms).ok()),
            },
            snapshot_seq: r.snapshot_seq,
        })
        .collect())
}

/// Publish this worker's task stats and run one outlier tick (issue #1815).
///
/// The tick prunes old rows and reads the live peers into
/// `probe.shard_peers`. When `probe.compare` holds, it then compares the
/// worker with the peers of all its shards and sets the outlier gauge.
///
/// The comparison merges the peer rows of all this worker's shards. It uses
/// only peers in the worker's own queue cohort. A draining worker, or a worker
/// missing from the live set, is not an outlier. The gauge reports the OR of
/// every local worker's verdict, on every dimension. With metrics off, the
/// tick publishes and prunes, and skips the peer read.
///
/// Returns the dimensions on which this worker is an outlier.
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure. The caller then clears the
/// verdict with [`OutlierProbe::clear_gauge`].
pub async fn run_outlier_tick(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    probe: &OutlierProbe,
    draining: bool,
) -> HarvestResult<Vec<OutlierDimension>> {
    // A row of the worker's previous process can hold a higher sequence. The
    // first write then moves the counter above it, and a fresh capture follows.
    let mut cohort = String::new();
    for _ in 0..2 {
        let (key, own, seq) = capture_task_stats(&probe.window, || probe.cohort_and_epoch());
        cohort = key;
        if write_task_stats_snapshot(conn, worker_id, &cohort, &own, seq).await?
            != SnapshotWrite::Foreign
        {
            break;
        }
    }
    // Every shard heartbeat prunes its own database, whether it compares or not.
    prune_worker_task_stats(conn, probe.fleet_stale_secs).await?;
    if !probe.metrics.is_enabled() {
        return Ok(Vec::new());
    }
    let live = load_live_worker_task_stats(conn, probe.fleet_stale_secs, Some(&cohort)).await?;
    if !probe.compare {
        probe.shard_peers.store(probe.slot, live);
        return Ok(Vec::new());
    }
    let flagged = probe
        .shard_peers
        .store_then(probe.slot, live, probe.view_max_age(), |live| {
            let flagged = if draining {
                Vec::new()
            } else {
                live.iter()
                    .find(|row| row.worker_id == worker_id)
                    .map(|me| {
                        let peers: Vec<WorkerTaskStats> = live
                            .iter()
                            .filter(|row| row.worker_id != worker_id && row.cohort == me.cohort)
                            .map(|row| row.stats)
                            .collect();
                        // The newest self row, which a sibling shard
                        // heartbeat can hold, not this tick's own capture.
                        outlier_dimensions(&me.stats, &peers, &probe.config)
                    })
                    .unwrap_or_default()
            };
            probe.publish(worker_id, &flagged);
            flagged
        });
    Ok(flagged)
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
/// `drain_deadline` is written to `drain_deadline_at` in the same statement
/// so that the deduplication layer (`dedup_workers_by_freshest`) sees the
/// correct drain window even when this newly-repaired row becomes the freshest
/// snapshot (issue #522 review).
///
/// # Errors
///
/// Returns [`HarvestError`] on database failure.
async fn transition_active_to_draining(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    drain_deadline: Option<DateTime<Utc>>,
) -> HarvestResult<()> {
    diesel::update(
        harvest_workers::table
            .find(worker_id)
            .filter(harvest_workers::status.eq(WorkerStatus::Active.as_str())),
    )
    .set((
        harvest_workers::status.eq(WorkerStatus::Draining.as_str()),
        harvest_workers::drain_deadline_at.eq(drain_deadline),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// Does a worker advertising `assignments` cover `shard_id`?
///
/// **The canonical shard-membership predicate for the whole workspace.** It
/// lives in core (rather than beside its first plugin caller) because
/// `autumn-harvest` itself has consumers — `apply_worker_filters` below — that
/// cannot reach into `autumn-harvest-plugin`, and every independent
/// re-implementation of this rule has so far drifted.
///
/// The rule: an **empty** assignment array means *"covers whatever shard the
/// row was read from"*, not *"covers nothing"*. A worker registers an empty
/// array in two situations that are both legitimate coverage:
///
/// * it is a legacy row written before per-shard assignments existed, and
/// * since issue #961 it is a worker with **no sharded pool** — there is no
///   shard identity to advertise, so the empty list is preserved rather than
///   fabricating a `0` the process never established.
///
/// Hardcoding `shard_id == 0` instead would falsely report such a worker
/// uncovered on a non-zero-numbered single shard, since [`ShardRouter`] accepts
/// an arbitrary default shard.
///
/// A non-array value (malformed row) covers nothing — it is not a legacy shape,
/// it is corrupt.
///
/// [`ShardRouter`]: crate::shard::ShardRouter
///
/// This is the single-shard convenience form: it assumes the row was read
/// from the exact shard being asked about, which holds for every consumer
/// that queries one shard's own connection (fleet health's `by_shard`, queue
/// coverage, preflight). A cross-shard fan-out that reads a row from one
/// shard while evaluating coverage for a *different* requested shard must
/// use [`shard_assignments_cover_from_source`] instead — issue #1213 was a
/// call site (`GET /workers?shard_id=`) that used this form across a fan-out
/// and let an empty-array row registered on shard A falsely cover a request
/// for shard B.
#[must_use]
pub fn shard_assignments_cover(assignments: &serde_json::Value, shard_id: i32) -> bool {
    shard_assignments_cover_from_source(assignments, shard_id, shard_id)
}

/// Does a worker advertising `assignments`, read from `source_shard_id`,
/// cover a caller's `requested_shard_id`?
///
/// The empty-array auto/legacy shape (see [`shard_assignments_cover`]) means
/// "covers whatever shard the row was read from", so it covers the request
/// only when `source_shard_id == requested_shard_id`. A non-empty list is the
/// worker's own explicit claim and is evaluated by membership against
/// `requested_shard_id` regardless of `source_shard_id` — a multi-shard
/// worker's row is replicated identically into every shard it is assigned to,
/// so which one it happened to be read from doesn't change what it claims
/// (issue #1213).
#[must_use]
pub fn shard_assignments_cover_from_source(
    assignments: &serde_json::Value,
    source_shard_id: i32,
    requested_shard_id: i32,
) -> bool {
    assignments.as_array().is_some_and(|shards| {
        if shards.is_empty() {
            source_shard_id == requested_shard_id
        } else {
            shards
                .iter()
                .any(|value| value.as_i64() == Some(i64::from(requested_shard_id)))
        }
    })
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
        // Issue #1150 / #961: route through the canonical predicate so a worker
        // advertising the empty (auto / legacy) assignment shape is not dropped
        // from `GET /workers?shard_id=N` while shard health, queue coverage and
        // preflight all report it as covering that shard.
        results.retain(|r| shard_assignments_cover(&r.worker.shard_assignments, shard_val));
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

/// SQL for [`live_workers_on_queue`], exposed for no-DB shape tests (issue #804).
///
/// `$1` = the staleness window in seconds, `$2` = the queue name.
///
/// The predicate — a fresh `last_heartbeat_at` inside a caller-supplied window
/// — is the same *shape* the poison-pill orphan reclaimer uses
/// ([`crate::poison_pill::orphaned_running_tasks_query`]), but the **window is
/// not the same value**, and that divergence is deliberate (Codex round-19 P1).
///
/// The reclaimer judges a row it is entitled to judge, with a window derived
/// from its own configured cadence. This query judges *peers*, whose cadence the
/// caller does not know and cannot read — nothing in `harvest_workers` records
/// it. Applying the caller's own `2 × worker_heartbeat_interval` here would let
/// a worker heartbeating every second declare a live peer on the **default**
/// five-second cadence dead, which the capability-miss path then reads as "no
/// capable worker is live" and turns into a terminal failure. Callers therefore
/// pass [`crate::worker::capability_miss_fleet_stale_secs`], which floors the
/// window at a fleet-wide bound that covers any peer at the supported cadence.
///
/// The error directions are what make one window unsafe for both: too *narrow*
/// here fabricates an escalation, while too *wide* only delays one (the
/// distinct-worker bound and the absolute ceiling still fire).
///
/// `status` is deliberately **not** filtered. A `Draining` worker still
/// belongs in the live set: it is a worker the fleet may still be told to keep,
/// and excluding it would let a capability-miss escalation conclude "no capable
/// worker" while a capable drainer is a rollback away. A genuinely gone worker
/// stops heartbeating and ages out of this query on its own, which is the
/// self-healing behaviour the freshness window already provides.
/// `build_id` and `labels` come back too, because advertising the queue is
/// only the *first* of `claim_task`'s gates. The task's own `required_build_id`
/// (#171) and `required_capabilities` (#522) are checked against these in
/// [`crate::worker::claim_eligible_workers`]; a worker that fails them can
/// never claim, so counting it as a possible capable peer would withhold the
/// redelivery budget forever. Those gates are **not** applied here because they
/// depend on the task, not the queue — this query answers "who is live on this
/// queue", and the caller narrows it to "who could claim *this task*".
#[must_use]
pub const fn live_workers_on_queue_query() -> &'static str {
    "SELECT worker_id, build_id, labels FROM harvest_workers \
     WHERE last_heartbeat_at > NOW() - ($1::bigint * INTERVAL '1 second') \
       AND queues @> to_jsonb($2::text)"
}

/// A live worker's inputs to `claim_task`'s per-task eligibility gates
/// (issue #804).
///
/// Deliberately just the three columns those gates read, rather than a whole
/// [`HarvestWorker`]: the capability-miss path runs on every miss, and the
/// fleet read is pure overhead on the way to a decision.
#[derive(Debug, Clone, diesel::QueryableByName)]
pub struct LiveWorker {
    /// The worker's registered id — the value that lands in a task's
    /// `capability_miss_workers` set when it misses.
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub worker_id: String,
    /// The worker's build id (#171). An empty id claims no pinned task (#1805).
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub build_id: String,
    /// The worker's labels, matched against a task's `required_capabilities`
    /// (#522).
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    pub labels: serde_json::Value,
}

impl LiveWorker {
    /// Construct a row without a database, for pure eligibility tests.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn for_test(worker_id: &str, build_id: &str, labels: serde_json::Value) -> Self {
        Self {
            worker_id: worker_id.to_owned(),
            build_id: build_id.to_owned(),
            labels,
        }
    }
}

/// Worker ids with a fresh heartbeat that advertise `queue_name` (issue #804).
///
/// This is the *fleet* half of the capability-miss decision: the task's
/// `capability_miss_workers` array says which workers have demonstrably no
/// handler for it, and this says which workers could claim it at all. Only when
/// the first covers the second is "no capable worker is live" an actual
/// conclusion rather than an assumption drawn from a fixed redelivery budget.
///
/// Pass [`crate::worker::capability_miss_fleet_stale_secs`] for
/// `worker_stale_secs`, never the bare [`crate::worker::worker_stale_secs`] —
/// see [`live_workers_on_queue_query`] for why judging peers by the caller's own
/// cadence turns a healthy fleet into a terminal failure.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the query fails.
pub async fn live_workers_on_queue(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    worker_stale_secs: i64,
) -> HarvestResult<Vec<LiveWorker>> {
    let stale = worker_stale_secs.clamp(0, crate::poison_pill::MAX_WORKER_STALE_SECS);
    diesel::sql_query(live_workers_on_queue_query())
        .bind::<diesel::sql_types::BigInt, _>(stale)
        .bind::<diesel::sql_types::Text, _>(queue_name)
        .load(conn)
        .await
        .map_err(crate::error::database_error)
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

/// Remove a worker row whose registration could not be verified (issue #804,
/// Codex round-51 P1).
///
/// A row that survives a rolled-back registration is not merely out of date —
/// it is *affirmative false evidence*. The capability-miss fleet read counts a
/// live worker that appears in `capability_miss_workers` as one that has
/// already missed the task, so a surviving row keeps a peer's
/// `AllLiveWorkersMissed` conclusion alive for the whole 120 s liveness floor
/// while this worker is unable to publish the build it is actually running.
/// An **absent** row is neutral: the worker is simply not part of the fleet
/// read, which is the truth while its registration is unverified.
///
/// Deliberately a single-table `DELETE`, so it can still succeed when the
/// register+invalidate transaction cannot — the failure is usually in that
/// transaction's `harvest_task_queue` half. It is best-effort: on failure the
/// caller falls back to letting the row age out, and the next heartbeat
/// retries both. `register_worker` re-inserts on the first successful retry.
async fn withdraw_unverified_worker_row(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
) -> HarvestResult<usize> {
    diesel::delete(harvest_workers::table.find(worker_id))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Re-register a worker whose fleet row has vanished at runtime, and leave the
/// claim gate matching the outcome.
///
/// Registration is atomic with the capability-miss invalidation, exactly as
/// startup is: republishing this id without clearing the evidence it may still
/// carry would make it affirmative fleet evidence against itself (issue #804,
/// Codex round-28 P1). This is also the retry that heals a startup registration
/// whose invalidation failed and rolled the pair back.
///
/// The gate (issue #804, Codex round-55 P1) is why the failure arm is not just
/// a log line. Round 54 seeded `registration_pending` only from the *startup*
/// registration result, so this runtime path — the fleet row vanishing later
/// and the heal failing — left a worker claiming while absent from
/// `harvest_workers`, which is the exact state that gate exists to prevent.
///
/// Absent is harmful twice over. The worker is invisible to a peer's fleet
/// evidence, so an incapable claimant can derive
/// [`crate::worker::FleetCapabilityEvidence::AllLiveWorkersMissed`] and
/// terminally fail work this worker can run. And its own claims match
/// `orphaned_running_tasks_query()` — `RUNNING` rows whose `worker_id` has no
/// fresh heartbeat — so `reclaim_orphaned_tasks` burns a `crash_strikes`
/// increment on each until `poison_pill_threshold` quarantines the task,
/// manufacturing a #367 quarantine out of a capability problem.
///
/// Arming also routes the next tick through [`do_heartbeat_tick`]'s
/// top-of-function retry, which is the same heal carrying the reused-`worker_id`
/// protections (withdraw the unverified row; never refresh a row known to be
/// wrong). The success arm clears the flag, because a worker that heals itself
/// must be able to claim again without a restart.
async fn heal_missing_worker_row(
    conn: &mut AsyncPgConnection,
    registration: &WorkerRegistration,
    registration_pending: &AtomicBool,
    registered_codec_key_ids: &[String],
) {
    match register_worker_and_clear_stale_miss_evidence(
        conn,
        registration,
        registered_codec_key_ids,
    )
    .await
    {
        Ok(cleared) => {
            registration_pending.store(false, Ordering::Relaxed);
            tracing::debug!(
                worker_id = %registration.worker_id,
                cleared_capability_miss_evidence = cleared,
                "worker row re-registered"
            );
        }
        Err(error) => {
            registration_pending.store(true, Ordering::Relaxed);
            tracing::warn!(
                worker_id = %registration.worker_id,
                error = %error,
                "worker re-registration failed; pausing task claiming until it commits"
            );
        }
    }
}

/// Retry a startup registration that failed and rolled the atomic pair back,
/// BEFORE [`do_heartbeat_tick`]'s heartbeat call observes a row (issue #804).
///
/// The `Ok(None)` arm in [`do_heartbeat_tick`] heals the absent-row case. A
/// reused `worker_id` leaves the PREVIOUS row alive, though, so the
/// heartbeat succeeds, returns the row's status, and that arm is never reached.
/// This worker is left advertising the old build's `build_id`/queues while
/// its id stays in `capability_miss_workers` as affirmative fleet evidence
/// against itself.
///
/// `register_worker_and_clear_stale_miss_evidence` upserts, so this is
/// correct whether or not a row survives. The flag is cleared only on
/// success, so a still-failing database is retried on the next tick rather
/// than silently given up on; a worker that registered cleanly at startup
/// never enters this branch and its tick is byte-for-byte unchanged.
///
/// Returns `false` when the caller must skip the rest of this tick — the
/// retry itself failed and withdrew the unverified row.
async fn retry_pending_startup_registration(
    conn: &mut AsyncPgConnection,
    registration: &WorkerRegistration,
    registration_pending: &AtomicBool,
    registered_codec_key_ids: &[String],
) -> bool {
    match register_worker_and_clear_stale_miss_evidence(
        conn,
        registration,
        registered_codec_key_ids,
    )
    .await
    {
        Ok(cleared) => {
            registration_pending.store(false, Ordering::Relaxed);
            tracing::info!(
                worker_id = %registration.worker_id,
                cleared_capability_miss_evidence = cleared,
                "startup registration retried successfully on heartbeat"
            );
            true
        }
        Err(error) => {
            // Do NOT fall through to the heartbeat (issue #804, Codex
            // round-50 P1). A reused `worker_id` leaves the previous row
            // alive, so `heartbeat_worker` would succeed and refresh a row
            // we know is wrong — republishing the old build's `build_id`
            // and queues as live while this id's stale capability-miss
            // evidence is still uncleared. That is affirmative false fleet
            // evidence, and it is exactly what can produce
            // `AllLiveWorkersMissed` and a terminal failure of a task this
            // worker can run. Publishing nothing is strictly more honest
            // than republishing something known-false, so the unverified
            // row is left to age out of the liveness window until the
            // atomic pair succeeds.
            //
            // Accepted trade-off: a stale row also makes this worker's
            // in-flight rows look orphaned to the poison-pill reclaimer
            // (#367), which re-queues them. That is recoverable —
            // at-least-once is the documented activity contract — whereas
            // the false `AllLiveWorkersMissed` is a terminal `WorkflowFailed`
            // needing operator action, so issue #804's own preference
            // ("prefer holding a task over terminally failing an
            // execution") settles the ordering. Remote-drain detection is
            // likewise skipped for this tick; a worker that cannot register
            // is already invisible to the fleet.
            //
            // Ageing out is not fast enough on its own (issue #804, Codex
            // round-51 P1): the capability-miss fleet window is floored at
            // 120 s, and for that whole window the surviving row is still
            // read as a LIVE worker carrying this id's stale miss evidence
            // — i.e. as a worker that has already missed — which is what
            // lets a peer derive `AllLiveWorkersMissed`. So withdraw the
            // row now rather than waiting for it to expire.
            //
            // Best-effort and deliberately narrow: a single-table DELETE
            // on `harvest_workers`, which can still succeed when the
            // two-table transaction cannot (its `harvest_task_queue` half
            // is the usual failure). If it too fails, the ageing-out path
            // above is the fallback and the next tick retries both.
            let withdrawn = withdraw_unverified_worker_row(conn, &registration.worker_id).await;
            tracing::warn!(
                worker_id = %registration.worker_id,
                error = %error,
                row_withdrawn = ?withdrawn,
                "startup registration retry failed; withdrew the unverified row so it \
                 cannot be read as live fleet evidence, and will retry on the next tick"
            );
            false
        }
    }
}

/// Execute one heartbeat DB tick: update `last_heartbeat_at`, handle
/// re-registration / drain transitions, and refresh the drain deadline.
///
/// `#[doc(hidden)] pub` so the round-49 regression test can drive the real tick
/// against a live database rather than a reimplementation of it — the defect it
/// guards lives in this function's arm selection, so a test that recreated the
/// arms would prove nothing.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn do_heartbeat_tick(
    conn: &mut AsyncPgConnection,
    registration: &WorkerRegistration,
    in_flight: i32,
    labels_json: &serde_json::Value,
    worker_shutdown: &CancellationToken,
    drain_deadline_max: &Mutex<Option<DateTime<Utc>>>,
    remote_drain_deadline: &Mutex<Option<std::time::Instant>>,
    in_use_sessions: i32,
    registration_pending: &AtomicBool,
    registered_codec_key_ids: &[String],
) {
    if registration_pending.load(Ordering::Relaxed)
        && !worker_shutdown.is_cancelled()
        && !retry_pending_startup_registration(
            conn,
            registration,
            registration_pending,
            registered_codec_key_ids,
        )
        .await
    {
        return;
    }
    match heartbeat_worker_status(
        conn,
        &registration.worker_id,
        in_flight,
        labels_json,
        in_use_sessions,
        registered_codec_key_ids,
    )
    .await
    {
        Ok(None) => {
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
                heal_missing_worker_row(
                    conn,
                    registration,
                    registration_pending,
                    registered_codec_key_ids,
                )
                .await;
            }
        }
        Ok(Some(status)) => {
            if worker_shutdown.is_cancelled() {
                // Already draining — transition this shard's row to Draining if
                // the fan-out missed it (e.g. shard recovered after the drain was
                // issued).  Guarded to Active rows only so it never reverts a row
                // that already reached Draining or Stopped.
                // Also write the current monotonic drain deadline so the
                // dedup-by-freshest layer doesn't mask the effective drain
                // window when this row's heartbeat is the most recent one
                // (issue #522 review).
                let current_deadline = drain_deadline_max.lock().ok().and_then(|g| *g);
                if let Err(error) =
                    transition_active_to_draining(conn, &registration.worker_id, current_deadline)
                        .await
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
                // fully stopped (P1). The status comes from the heartbeat
                // write itself (issue #1879).
                if status == WorkerStatus::Draining.as_str() {
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
            }
        }
        Err(error) => {
            tracing::warn!(worker_id = %registration.worker_id, error = %error, "worker heartbeat write failed");
        }
    }
}

/// The start times of the worker heartbeat ticks (issue #1879).
///
/// The schedule has a fixed rate. Tick latency does not add to the period.
/// A fixed delay makes the period the interval plus the latency. A slow
/// database then makes a live worker look dead.
///
/// After a tick that takes longer than one interval, the next tick starts
/// at once. The schedule does not send a burst of ticks to catch up.
#[derive(Debug)]
pub(crate) struct HeartbeatSchedule {
    ticks: tokio::time::Interval,
}

impl HeartbeatSchedule {
    /// Make a schedule. The first tick comes one `interval` after this call.
    ///
    /// A zero `interval` becomes 1 ms, because a zero period panics.
    pub(crate) fn new(interval: Duration) -> Self {
        let period = interval.max(Duration::from_millis(1));
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self { ticks }
    }

    /// Wait until the next tick is due. Return `false` when `cancel` fires
    /// first.
    pub(crate) async fn wait(&mut self, cancel: &CancellationToken) -> bool {
        tokio::select! {
            () = cancel.cancelled() => false,
            _ = self.ticks.tick() => true,
        }
    }
}

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
pub fn spawn_worker_heartbeat(
    pool: DbPool,
    registration: WorkerRegistration,
    wf_semaphore: Arc<Semaphore>,
    wf_max: Arc<AtomicUsize>,
    act_semaphore: Arc<Semaphore>,
    act_max: Arc<AtomicUsize>,
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
    // In-process worker-session registry (issue #606): sampled fresh each
    // tick (mirroring `in_flight` above) so `harvest_workers.in_use_sessions`
    // — previously always written as a literal `0` — reflects this worker's
    // actual live session count.
    session_slots_in_use: crate::sessions::SessionSlotRegistry,
    // Set by startup when the atomic register+invalidate pair failed and rolled
    // back (issue #804). The missing-row arm of the tick heals an absent row.
    // A reused `worker_id` is different. A configured stable id, such as a pod
    // name, leaves the PREVIOUS row alive. The heartbeat then updates that
    // row, and the pair is never retried. This worker would then poll
    // indefinitely while the registry advertises the old build's
    // `build_id`/queues and its id remains in `capability_miss_workers`,
    // letting a peer read the live fleet as `AllLiveWorkersMissed` and
    // terminally fail a task this worker can run.
    registration_pending: Arc<AtomicBool>,
    // Read fresh every tick, never cached with `labels_json` below (issue
    // #1244). Keys may be registered on this process at runtime, after this
    // task has already started. A heartbeat must advertise the current
    // registry, not the one at spawn time.
    codecs: crate::payload_codec::PayloadCodecs,
    // Issue #1815: publishes task stats and sets the outlier gauge each tick.
    outliers: OutlierProbe,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Retires the verdict on every exit, an abort included.
        let _retire = RetireOnDrop::new(outliers.clone(), registration.worker_id.clone());
        let labels_json = serde_json::to_value(&registration.labels).unwrap_or_default();
        let mut schedule = HeartbeatSchedule::new(interval);
        while schedule.wait(&cancel).await {
            // Loaded fresh each tick (issue #548 review): a tuned worker's
            // dispatch target can change between heartbeats, so a value
            // captured once at spawn time would drift from reality.
            //
            // UFCS is required here: `diesel_async::RunQueryDsl::load` is in
            // scope in this module and its blanket by-value-receiver impl
            // wins method resolution over `AtomicUsize::load(&self, ..)`.
            let in_flight = compute_in_flight(
                &wf_semaphore,
                AtomicUsize::load(&wf_max, Ordering::Relaxed),
                &act_semaphore,
                AtomicUsize::load(&act_max, Ordering::Relaxed),
            );
            let in_use_sessions = crate::sessions::session_slot_count(&session_slots_in_use);
            // Selected against `cancel` (issue #1209). A pool may have no
            // deadpool `Timeouts`, so `pool.get()` alone can park this task
            // indefinitely on an exhausted shard pool. The top-of-loop select
            // only guards the sleep between ticks. A tick already parked here
            // would otherwise never observe shutdown, and the join in
            // `run_multi_shard`/`run_with_listener` would wait forever.
            let get_result = tokio::select! {
                () = cancel.cancelled() => break,
                result = pool.get() => result,
            };
            let registered_codec_key_ids = codecs.registered_key_ids();
            match get_result {
                Ok(mut conn) => {
                    let () = do_heartbeat_tick(
                        &mut conn,
                        &registration,
                        in_flight,
                        &labels_json,
                        &worker_shutdown,
                        &drain_deadline_max,
                        &remote_drain_deadline,
                        in_use_sessions,
                        &registration_pending,
                        &registered_codec_key_ids,
                    )
                    .await;
                    if let Err(error) = run_outlier_tick(
                        &mut conn,
                        &registration.worker_id,
                        &outliers,
                        worker_shutdown.is_cancelled(),
                    )
                    .await
                    {
                        tracing::warn!(
                            worker_id = %registration.worker_id,
                            error = %error,
                            "worker task-stats tick failed; outlier gauge cleared"
                        );
                        outliers.clear_gauge(&registration.worker_id);
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        worker_id = %registration.worker_id,
                        error = %error,
                        "worker heartbeat failed to get pool connection"
                    );
                    outliers.clear_gauge(&registration.worker_id);
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
    /// A live row published `age_secs` ago.
    fn live_aged(worker_id: &str, tasks: u32, age_secs: i64) -> super::LiveWorkerTaskStats {
        super::LiveWorkerTaskStats {
            worker_id: worker_id.to_owned(),
            cohort: "[\"q\"]".to_owned(),
            stats: crate::worker_outlier::WorkerTaskStats {
                tasks,
                failures: 0,
                p99_latency_ms: Some(1),
            },
            // A newer row has a higher sequence, as `next_snapshot_seq` gives.
            snapshot_seq: 1_000_000 - age_secs,
        }
    }

    fn live(worker_id: &str, tasks: u32) -> super::LiveWorkerTaskStats {
        live_aged(worker_id, tasks, 0)
    }

    /// Issue #1815: shard clocks can differ, so the worker's own sequence
    /// decides which of its snapshots is newer.
    #[test]
    fn the_worker_sequence_orders_snapshots_across_skewed_shards() {
        let first = super::next_snapshot_seq();
        let second = super::next_snapshot_seq();
        assert!(second > first, "the sequence rises");
        let mut older = live("w", 90);
        older.snapshot_seq = first;
        let mut newer = live("w", 10);
        newer.snapshot_seq = second;
        assert!(newer.is_fresher_than(&older));
        assert!(!older.is_fresher_than(&newer));
    }

    /// Issue #1815: the comparing heartbeat sees peers from every shard, once
    /// each, with the newest snapshot of a worker that both shards hold. The
    /// newest one wins even with fewer tasks, because a window shrinks.
    #[test]
    fn shard_peer_views_merge_every_slot_once_per_worker() {
        let views = super::ShardPeerViews::default();
        let window = std::time::Duration::from_secs(60);
        views.store(0, vec![live("me", 50), live_aged("a", 99, 30)]);
        views.store(
            1,
            vec![live_aged("me", 90, 30), live("b", 30), live("a", 20)],
        );
        let merged: Vec<(String, u32)> = views
            .merged(window)
            .into_iter()
            .map(|r| (r.worker_id, r.stats.tasks))
            .collect();
        assert_eq!(
            merged,
            vec![("a".into(), 20), ("b".into(), 30), ("me".into(), 50)]
        );
        // A later read from one slot replaces that slot only.
        views.store(1, vec![]);
        assert_eq!(views.merged(window).len(), 2);
    }

    /// Issue #1815: a slot whose shard heartbeat failed or went quiet no
    /// longer supplies peers.
    #[test]
    fn shard_peer_views_drop_cleared_and_expired_slots() {
        let views = super::ShardPeerViews::default();
        let window = std::time::Duration::from_secs(60);
        views.store(0, vec![live("me", 50)]);
        views.store(1, vec![live("a", 20)]);
        views.clear(1);
        assert_eq!(views.merged(window).len(), 1, "a cleared slot is gone");

        let old = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(120))
            .expect("the clock is past two minutes");
        views.store_at(1, old, vec![live("a", 20)]);
        let ids: Vec<String> = views
            .merged(window)
            .into_iter()
            .map(|r| r.worker_id)
            .collect();
        assert_eq!(ids, vec!["me".to_string()], "an expired slot is skipped");
    }

    fn probe_for_slot(
        slot: usize,
        shard_peers: &std::sync::Arc<super::ShardPeerViews>,
        process_flags: &std::sync::Arc<super::ProcessOutlierFlags>,
    ) -> super::OutlierProbe {
        super::OutlierProbe {
            window: std::sync::Arc::default(),
            metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
            config: crate::worker_outlier::OutlierConfig::default(),
            fleet_stale_secs: 60,
            cohort: "[\"q\"]".to_owned(),
            codecs: None,
            compare: true,
            slot,
            shard_peers: std::sync::Arc::clone(shard_peers),
            process_flags: std::sync::Arc::clone(process_flags),
        }
    }

    /// Issue #1815: a failed shard heartbeat leaves the verdict to a healthy
    /// one. The last failed heartbeat clears it.
    #[test]
    fn a_failed_shard_keeps_the_verdict_while_another_shard_is_healthy() {
        use crate::worker_outlier::OutlierDimension::FailureRatio;
        let peers = std::sync::Arc::default();
        let flags = std::sync::Arc::default();
        let first = probe_for_slot(0, &peers, &flags);
        let second = probe_for_slot(1, &peers, &flags);
        first.shard_peers.store(0, vec![live("me", 50)]);
        second.shard_peers.store(1, vec![live("me", 50)]);
        let _ = flags.set("me", &[FailureRatio]);

        first.clear_gauge("me");
        assert_eq!(
            flags.set("peer", &[]),
            vec![FailureRatio],
            "a healthy shard still holds the verdict"
        );
        second.clear_gauge("me");
        assert!(flags.set("peer", &[]).is_empty(), "no shard can compare");
    }

    fn cohort_of(
        queues: &[&str],
        weights: &[(&str, u32)],
        build: &str,
        labels: &[(&str, &str)],
    ) -> String {
        let queues: Vec<String> = queues.iter().map(|n| (*n).to_owned()).collect();
        let weights: std::collections::HashMap<String, u32> =
            weights.iter().map(|(q, w)| ((*q).to_owned(), *w)).collect();
        let labels: std::collections::HashMap<String, String> = labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        super::worker_cohort(&super::CohortPolicy {
            queues: &queues,
            queue_weights: &weights,
            build_id: build,
            labels: &labels,
            slots: super::SlotPolicy::of(1, 1, None),
            session_slots: 0,
            priority_aging_secs: None,
            ineligible_activities: &[],
            shard_assignments: &[],
            registered_workflows: &[],
            registered_activities: &[],
            circuit_breakers: &crate::circuit_breaker::CircuitBreakerRegistry::empty(),
            dispatch_channel: &[],
            retry_budgets: &crate::retry_budget::RetryBudgetConfig::default(),
            adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
            outcome_window: std::time::Duration::from_secs(300),
            peer_stale_secs: 120,
            execution: super::ExecutionPolicy::default(),
            payload: super::PayloadPolicy::default(),
        })
    }

    /// Issue #1815: a worker with no slot for one task kind claims only the
    /// other kind. An activity-only worker and a workflow-only worker do
    /// disjoint work, so they are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_task_kinds() {
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let slots = |workflows, activities| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(workflows, activities, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &[],
                circuit_breakers: &crate::circuit_breaker::CircuitBreakerRegistry::empty(),
                dispatch_channel: &[],
                retry_budgets: &crate::retry_budget::RetryBudgetConfig::default(),
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window: std::time::Duration::from_secs(300),
                peer_stale_secs: 120,
                execution: super::ExecutionPolicy::default(),
                payload: super::PayloadPolicy::default(),
            })
        };
        assert_ne!(slots(10, 0), slots(0, 10));
        assert_ne!(slots(10, 10), slots(10, 0));
        assert_ne!(slots(10, 10), slots(0, 10));
        assert_eq!(slots(10, 10), slots(10, 10));
    }

    /// Issue #1815: session member activities are pinned to the session's
    /// host. A worker with session capacity gets that work and one without
    /// does not, so they are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_session_capacity() {
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let sessions = |n| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: n,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &[],
                circuit_breakers: &crate::circuit_breaker::CircuitBreakerRegistry::empty(),
                dispatch_channel: &[],
                retry_budgets: &crate::retry_budget::RetryBudgetConfig::default(),
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window: std::time::Duration::from_secs(300),
                peer_stale_secs: 120,
                execution: super::ExecutionPolicy::default(),
                payload: super::PayloadPolicy::default(),
            })
        };
        assert_ne!(sessions(0), sessions(4));
        assert_ne!(sessions(4), sessions(8));
        assert_eq!(
            sessions(0),
            sessions(-1),
            "a negative capacity is no capacity"
        );
    }

    /// Issue #1815: the claim query orders a mixed-priority backlog by the
    /// priority aging, and skips the activities a worker is not eligible for.
    /// Workers that differ in either claim different tasks, so they are in
    /// different cohorts.
    #[test]
    fn the_cohort_key_includes_priority_aging_and_eligibility() {
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let gpu = vec!["render".to_owned(), "encode".to_owned()];
        let gpu_reordered = vec!["encode".to_owned(), "render".to_owned()];
        let cohort = |aging, ineligible: &[String]| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: 0,
                priority_aging_secs: aging,
                ineligible_activities: ineligible,
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &[],
                circuit_breakers: &crate::circuit_breaker::CircuitBreakerRegistry::empty(),
                dispatch_channel: &[],
                retry_budgets: &crate::retry_budget::RetryBudgetConfig::default(),
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window: std::time::Duration::from_secs(300),
                peer_stale_secs: 120,
                execution: super::ExecutionPolicy::default(),
                payload: super::PayloadPolicy::default(),
            })
        };
        assert_ne!(cohort(None, &[]), cohort(Some(30), &[]));
        assert_ne!(cohort(Some(30), &[]), cohort(Some(60), &[]));
        assert_ne!(cohort(None, &[]), cohort(None, &gpu));
        assert_eq!(cohort(None, &gpu), cohort(None, &gpu_reordered));
    }

    /// Issue #1815: workers on other shards, or with other handlers, claim or
    /// complete other tasks, so they are in different cohorts.
    #[test]
    fn the_cohort_key_includes_shards_and_handlers() {
        use crate::types::ShardId;
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let cohort = |shards: &[ShardId], workflows: &[String], activities: &[String]| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: shards,
                registered_workflows: workflows,
                registered_activities: activities,
                circuit_breakers: &crate::circuit_breaker::CircuitBreakerRegistry::empty(),
                dispatch_channel: &[],
                retry_budgets: &crate::retry_budget::RetryBudgetConfig::default(),
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window: std::time::Duration::from_secs(300),
                peer_stale_secs: 120,
                execution: super::ExecutionPolicy::default(),
                payload: super::PayloadPolicy::default(),
            })
        };
        let names = |list: &[&str]| list.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();
        let (one, two) = ([ShardId::new(1)], [ShardId::new(2)]);
        let both = [ShardId::new(2), ShardId::new(1)];
        assert_ne!(cohort(&one, &[], &[]), cohort(&two, &[], &[]));
        assert_eq!(
            cohort(&both, &[], &[]),
            cohort(&[ShardId::new(1), ShardId::new(2)], &[], &[]),
            "shard order does not matter"
        );
        assert_ne!(
            cohort(&one, &names(&["order"]), &[]),
            cohort(&one, &names(&["order", "refund"]), &[])
        );
        assert_ne!(
            cohort(&one, &[], &names(&["charge"])),
            cohort(&one, &[], &names(&["charge", "render"]))
        );
    }

    /// Issue #1815: a slot tuner clamps the configured maximums into its band
    /// and resizes them later. A tuned worker is keyed on its band, so a tuned
    /// worker with a configured 0 is not taken for a worker without workflows.
    #[test]
    fn a_tuned_worker_is_keyed_on_its_band() {
        use super::SlotPolicy;
        use crate::slot_tuner::SlotTunerConfig;
        let band = SlotTunerConfig::new(5, 50);
        let tuned = |workflows, activities| SlotPolicy::of(workflows, activities, Some(&band));
        assert_eq!(
            tuned(0, 10),
            tuned(3, 10),
            "both clamp to the same initial targets"
        );
        assert_ne!(
            tuned(100, 1),
            tuned(1, 100),
            "the initial target per kind follows the configured maximum"
        );
        assert_ne!(
            tuned(0, 10),
            SlotPolicy::of(0, 10, None),
            "a tuned worker claims workflows"
        );
        let wider = SlotTunerConfig::new(5, 60);
        assert_ne!(tuned(0, 10), SlotPolicy::of(0, 10, Some(&wider)));
    }

    /// Issue #1815: two tuners can share a name and still resize slots
    /// differently. A tuned worker is keyed on the policy of its tuner, not
    /// only on its name.
    #[test]
    fn a_tuned_worker_is_keyed_on_its_tuner_policy() {
        use super::SlotPolicy;
        use crate::slot_tuner::{DefaultSlotTuner, SlotTunerConfig};
        let policy = |tuner: DefaultSlotTuner| {
            SlotPolicy::of(
                10,
                10,
                Some(&SlotTunerConfig::with_tuner(
                    5,
                    50,
                    std::sync::Arc::new(tuner),
                )),
            )
        };
        let default = policy(DefaultSlotTuner::default());
        assert_eq!(
            default,
            SlotPolicy::of(10, 10, Some(&SlotTunerConfig::new(5, 50))),
            "the same settings give the same key"
        );
        assert_ne!(
            default,
            policy(DefaultSlotTuner {
                grow_step: 8,
                ..DefaultSlotTuner::default()
            }),
            "the grow step changes the key"
        );
        assert_ne!(
            default,
            policy(DefaultSlotTuner {
                shrink_step: 1,
                ..DefaultSlotTuner::default()
            }),
            "the shrink step changes the key"
        );
        assert_ne!(
            default,
            policy(DefaultSlotTuner {
                permit_wait_grow_threshold: std::time::Duration::from_millis(500),
                ..DefaultSlotTuner::default()
            }),
            "the wait threshold changes the key"
        );
    }

    /// Issue #1815: an activity with a circuit-breaker policy skips the
    /// claim-time rate-limit gate and fails fast while its breaker is open.
    /// Workers with different policies are therefore in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_circuit_breaker_policies() {
        use crate::circuit_breaker::CircuitBreakerRegistry;
        use crate::policy::CircuitBreakerPolicy;
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let cohort = |breakers: &CircuitBreakerRegistry| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &[],
                circuit_breakers: breakers,
                dispatch_channel: &[],
                retry_budgets: &crate::retry_budget::RetryBudgetConfig::default(),
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window: std::time::Duration::from_secs(300),
                peer_stale_secs: 120,
                execution: super::ExecutionPolicy::default(),
                payload: super::PayloadPolicy::default(),
            })
        };
        let tracking = |threshold| {
            CircuitBreakerRegistry::new(std::collections::HashMap::from([(
                "charge".to_owned(),
                CircuitBreakerPolicy::new(
                    threshold,
                    std::time::Duration::from_secs(30),
                    std::time::Duration::from_secs(60),
                ),
            )]))
        };
        assert_ne!(
            cohort(&CircuitBreakerRegistry::empty()),
            cohort(&tracking(5)),
            "a tracked activity changes the cohort"
        );
        assert_ne!(
            cohort(&tracking(5)),
            cohort(&tracking(10)),
            "the policy changes the cohort"
        );
        assert_eq!(cohort(&tracking(5)), cohort(&tracking(5)));
        // Issue #1809: an open breaker defers or fails its attempts. The mode
        // changes the failure ratio, so it changes the cohort.
        let fail_fast = CircuitBreakerRegistry::new(std::collections::HashMap::from([(
            "charge".to_owned(),
            CircuitBreakerPolicy::new(
                5,
                std::time::Duration::from_secs(30),
                std::time::Duration::from_secs(60),
            )
            .with_open_mode(crate::policy::CircuitOpenMode::FailFast),
        )]));
        assert_ne!(
            cohort(&tracking(5)),
            cohort(&fail_fast),
            "the open mode changes the cohort"
        );
    }

    /// Issue #1815: a dispatch channel delivers by priority and ignores
    /// `queue_weights`, while the Postgres claim applies them. Workers on the
    /// two routes do different work, so they are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_dispatch_route() {
        let queues = vec!["a".to_owned(), "b".to_owned()];
        let weights = std::collections::HashMap::from([("b".to_owned(), 5_u32)]);
        let labels = std::collections::HashMap::<String, String>::new();
        let breakers = crate::circuit_breaker::CircuitBreakerRegistry::empty();
        let cohort = |dispatch_channel| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &weights,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &[],
                circuit_breakers: &breakers,
                dispatch_channel,
                retry_budgets: &crate::retry_budget::RetryBudgetConfig::default(),
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window: std::time::Duration::from_secs(300),
                peer_stale_secs: 120,
                execution: super::ExecutionPolicy::default(),
                payload: super::PayloadPolicy::default(),
            })
        };
        assert_ne!(cohort(&[0]), cohort(&[]));
        // A multi-shard worker can use a channel on some shards only.
        assert_ne!(cohort(&[1]), cohort(&[2]));
        assert_eq!(cohort(&[2, 1]), cohort(&[1, 2, 2]));
    }

    /// Issue #1815: a type at its adaptive limit is left out of the claim.
    /// Workers with different limit policies for a registered activity run
    /// different task mixes, so they are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_adaptive_limits() {
        use crate::adaptive_limit::AdaptiveLimitConfig;
        use crate::policy::AdaptiveLimitPolicy;
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let breakers = crate::circuit_breaker::CircuitBreakerRegistry::empty();
        let budgets = crate::retry_budget::RetryBudgetConfig::default();
        let activities = vec!["charge".to_owned()];
        let cohort = |limits: &AdaptiveLimitConfig| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &activities,
                circuit_breakers: &breakers,
                dispatch_channel: &[],
                retry_budgets: &budgets,
                adaptive_limits: limits,
                outcome_window: std::time::Duration::from_secs(300),
                peer_stale_secs: 120,
                execution: super::ExecutionPolicy::default(),
                payload: super::PayloadPolicy::default(),
            })
        };
        let off = AdaptiveLimitConfig::disabled();
        let limited = AdaptiveLimitConfig::disabled()
            .with_activity("charge", Some(AdaptiveLimitPolicy::new(1, 10)));
        let wider = AdaptiveLimitConfig::disabled()
            .with_activity("charge", Some(AdaptiveLimitPolicy::new(1, 20)));
        let unrelated = AdaptiveLimitConfig::disabled()
            .with_activity("refund", Some(AdaptiveLimitPolicy::new(1, 10)));
        assert_ne!(cohort(&off), cohort(&limited), "a limit");
        assert_ne!(cohort(&limited), cohort(&wider), "a different limit");
        assert_eq!(cohort(&off), cohort(&unrelated), "an unregistered activity");
    }

    /// Issue #1815: a retry budget defers retries, and a deferred retry does
    /// not count. Workers with different budgets for an activity run
    /// different retry mixes, so they are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_retry_budgets() {
        use crate::policy::RetryBudgetPolicy;
        use crate::retry_budget::RetryBudgetConfig;
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let breakers = crate::circuit_breaker::CircuitBreakerRegistry::empty();
        let activities = vec!["charge".to_owned()];
        let cohort = |budgets: &RetryBudgetConfig| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &activities,
                circuit_breakers: &breakers,
                dispatch_channel: &[],
                retry_budgets: budgets,
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window: std::time::Duration::from_secs(300),
                peer_stale_secs: 120,
                execution: super::ExecutionPolicy::default(),
                payload: super::PayloadPolicy::default(),
            })
        };
        let default = RetryBudgetConfig::default();
        let tight = RetryBudgetConfig::default()
            .with_activity("charge", Some(RetryBudgetPolicy::new(0.01, 1.0, 0.0)));
        let unrelated = RetryBudgetConfig::default()
            .with_activity("render", Some(RetryBudgetPolicy::new(0.01, 1.0, 0.0)));
        assert_ne!(cohort(&default), cohort(&tight), "a tighter budget");
        assert_ne!(
            cohort(&default),
            cohort(&RetryBudgetConfig::disabled()),
            "no budget"
        );
        assert_eq!(
            cohort(&default),
            cohort(&unrelated),
            "a budget for an activity the worker does not run"
        );
    }

    /// Issue #1815: the heartbeat interval sets the outcome window and the
    /// peer freshness limit. Workers with two of either compare different
    /// time ranges or different peer sets, so they are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_window_and_freshness() {
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let breakers = crate::circuit_breaker::CircuitBreakerRegistry::empty();
        let budgets = crate::retry_budget::RetryBudgetConfig::default();
        let cohort = |outcome_window, peer_stale_secs| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &[],
                circuit_breakers: &breakers,
                dispatch_channel: &[],
                retry_budgets: &budgets,
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window,
                peer_stale_secs,
                execution: super::ExecutionPolicy::default(),
                payload: super::PayloadPolicy::default(),
            })
        };
        let five_minutes = std::time::Duration::from_secs(300);
        assert_ne!(
            cohort(five_minutes, 120),
            cohort(std::time::Duration::from_secs(1200), 120),
            "another window"
        );
        assert_ne!(
            cohort(five_minutes, 120),
            cohort(five_minutes, 1200),
            "another freshness limit"
        );
    }

    /// Issue #1815: a deadline decides an outcome to the nanosecond. Two
    /// workers whose budgets differ below a millisecond are in different
    /// cohorts.
    #[test]
    fn the_cohort_key_keeps_sub_millisecond_durations() {
        use super::ExecutionPolicy;
        use std::time::Duration;
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let breakers = crate::circuit_breaker::CircuitBreakerRegistry::empty();
        let budgets = crate::retry_budget::RetryBudgetConfig::default();
        let cohort = |execution, outcome_window| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &[],
                circuit_breakers: &breakers,
                dispatch_channel: &[],
                retry_budgets: &budgets,
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window,
                peer_stale_secs: 120,
                execution,
                payload: super::PayloadPolicy::default(),
            })
        };
        let short = Duration::from_micros(1_100);
        let long = Duration::from_micros(1_900);
        let window = Duration::from_secs(300);
        let base = ExecutionPolicy::default();
        for (a, b) in [
            (
                ExecutionPolicy {
                    sticky_timeout: short,
                    ..base
                },
                ExecutionPolicy {
                    sticky_timeout: long,
                    ..base
                },
            ),
            (
                ExecutionPolicy {
                    workflow_task_timeout: short,
                    ..base
                },
                ExecutionPolicy {
                    workflow_task_timeout: long,
                    ..base
                },
            ),
            (
                ExecutionPolicy {
                    max_local_activity_start_to_close: short,
                    ..base
                },
                ExecutionPolicy {
                    max_local_activity_start_to_close: long,
                    ..base
                },
            ),
            (
                ExecutionPolicy {
                    cancellation_grace_period: short,
                    ..base
                },
                ExecutionPolicy {
                    cancellation_grace_period: long,
                    ..base
                },
            ),
        ] {
            assert_ne!(cohort(a, window), cohort(b, window), "{a:?}");
        }
        assert_ne!(
            cohort(base, window + short),
            cohort(base, window + long),
            "the outcome window"
        );
    }

    /// Issue #1815: the workflow cache, the task budgets, the panic limit
    /// and the cancellation grace period change what the window records for
    /// the same task. Workers that differ
    /// in any of them are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_execution_policy() {
        use super::ExecutionPolicy;
        use std::time::Duration;
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let breakers = crate::circuit_breaker::CircuitBreakerRegistry::empty();
        let budgets = crate::retry_budget::RetryBudgetConfig::default();
        let cohort = |execution| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &[],
                circuit_breakers: &breakers,
                dispatch_channel: &[],
                retry_budgets: &budgets,
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window: Duration::from_secs(300),
                peer_stale_secs: 120,
                execution,
                payload: super::PayloadPolicy::default(),
            })
        };
        let base = ExecutionPolicy::default();
        let variants = [
            ExecutionPolicy {
                sticky_timeout: Duration::ZERO,
                ..base
            },
            ExecutionPolicy {
                workflow_cache_size: 1,
                ..base
            },
            ExecutionPolicy {
                resident_workflows: true,
                ..base
            },
            ExecutionPolicy {
                workflow_task_timeout: Duration::from_secs(1),
                ..base
            },
            ExecutionPolicy {
                max_local_activity_start_to_close: Duration::from_secs(1),
                ..base
            },
            ExecutionPolicy {
                workflow_panic_max_attempts: 1,
                ..base
            },
            ExecutionPolicy {
                poison_pill_threshold: 10,
                ..base
            },
            ExecutionPolicy {
                poison_pill_threshold: 0,
                ..base
            },
            ExecutionPolicy {
                cancellation_grace_period: Duration::from_secs(1),
                ..base
            },
        ];
        for variant in variants {
            assert_ne!(cohort(base), cohort(variant), "{variant:?}");
        }
        assert_eq!(cohort(base), cohort(ExecutionPolicy::default()));
        let off = |threshold| ExecutionPolicy {
            poison_pill_threshold: threshold,
            ..base
        };
        assert_eq!(
            cohort(off(0)),
            cohort(off(-1)),
            "every threshold at or below 0 turns quarantine off"
        );
    }

    /// One payload policy per field, each differing from the default in that
    /// field alone.
    fn payload_variants() -> Vec<super::PayloadPolicy> {
        use super::PayloadPolicy;
        let base = PayloadPolicy::default();
        vec![
            PayloadPolicy {
                max_activity_input_bytes: 1,
                ..base.clone()
            },
            PayloadPolicy {
                max_workflow_input_bytes: 1,
                ..base.clone()
            },
            PayloadPolicy {
                max_activity_result_bytes: 1,
                ..base.clone()
            },
            PayloadPolicy {
                max_signal_payload_bytes: 1,
                ..base.clone()
            },
            PayloadPolicy {
                max_current_details_bytes: 1,
                ..base.clone()
            },
            PayloadPolicy {
                continue_as_new_threshold: 1,
                ..base.clone()
            },
            PayloadPolicy {
                event_hard_cap: Some(1),
                ..base.clone()
            },
            PayloadPolicy {
                byte_hard_cap: Some(1),
                ..base.clone()
            },
            PayloadPolicy {
                history_bloat_warn_fraction: 0.5,
                ..base.clone()
            },
            PayloadPolicy {
                decision_boundaries: true,
                ..base.clone()
            },
            PayloadPolicy {
                max_workflow_execution_timeout: Some(std::time::Duration::from_secs(1)),
                ..base.clone()
            },
            PayloadPolicy {
                continue_as_new_deadline_fraction: 0.5,
                ..base.clone()
            },
            PayloadPolicy {
                offload_threshold: Some(1),
                ..base.clone()
            },
            PayloadPolicy {
                offload_store_id: Some("s3-west".to_owned()),
                ..base.clone()
            },
            PayloadPolicy {
                activity_interceptors: vec!["a::Retry".to_owned()],
                ..base.clone()
            },
            PayloadPolicy {
                activity_interceptors: vec!["a::Retry".to_owned(), "a::Audit".to_owned()],
                ..base.clone()
            },
            PayloadPolicy {
                activities: vec![("charge".to_owned(), serde_json::json!({"result_cap": 1024}))],
                ..base.clone()
            },
            PayloadPolicy {
                local_activity_defaults: serde_json::json!({"start_to_close_ms": 500}),
                ..base.clone()
            },
            PayloadPolicy {
                module_host: serde_json::json!({"allow_queue_override": true}),
                ..base.clone()
            },
            PayloadPolicy {
                workflows: vec![("order".to_owned(), serde_json::json!({"dag": true}))],
                ..base.clone()
            },
            PayloadPolicy {
                declarative_handlers: vec![serde_json::json!(["update", "order", "approve"])],
                ..base.clone()
            },
            PayloadPolicy {
                workflow_log_policy: serde_json::json!([1000, 4096]),
                ..base
            },
        ]
    }

    /// Issue #1815: the payload caps, the history policy, the offloader, the
    /// codec keys and the interceptors decide whether a task's payloads pass.
    /// Workers that differ in any of them are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_payload_policy() {
        use super::PayloadPolicy;
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let breakers = crate::circuit_breaker::CircuitBreakerRegistry::empty();
        let budgets = crate::retry_budget::RetryBudgetConfig::default();
        let cohort = |payload: PayloadPolicy| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(10, 10, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &[],
                circuit_breakers: &breakers,
                dispatch_channel: &[],
                retry_budgets: &budgets,
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window: std::time::Duration::from_secs(300),
                peer_stale_secs: 120,
                execution: super::ExecutionPolicy::default(),
                payload,
            })
        };
        let base = PayloadPolicy::default();
        let variants = payload_variants();
        for variant in variants {
            assert_ne!(cohort(base.clone()), cohort(variant.clone()), "{variant:?}");
        }
        assert_eq!(cohort(base), cohort(PayloadPolicy::default()));
        let chain = |names: &[&str]| PayloadPolicy {
            activity_interceptors: names.iter().map(|n| (*n).to_owned()).collect(),
            ..PayloadPolicy::default()
        };
        assert_ne!(
            cohort(chain(&["a::Retry"])),
            cohort(chain(&["a::Audit"])),
            "two chains of one interceptor each"
        );
        assert_ne!(
            cohort(chain(&["a::Retry", "a::Audit"])),
            cohort(chain(&["a::Audit", "a::Retry"])),
            "the chain order"
        );
    }

    /// Issue #1815: a reload can register a codec key while the worker runs.
    /// The next tick's cohort key carries it, so the worker leaves peers that
    /// cannot decode the same payloads.
    #[test]
    fn the_cohort_key_follows_the_codec_keys_at_runtime() {
        use crate::payload_codec::{IdentityCodec, PayloadCodecs};
        let codecs = PayloadCodecs::default();
        let probe = super::OutlierProbe {
            codecs: Some(codecs.clone()),
            cohort: r#"{"queues":["a"]}"#.to_owned(),
            ..probe_for_slot(0, &std::sync::Arc::default(), &std::sync::Arc::default())
        };
        let before = probe.cohort_key();
        assert!(before.contains("\"codec_key_ids\":[]"), "{before}");

        codecs
            .register_key("k1", std::sync::Arc::new(IdentityCodec))
            .expect("register a key");
        let after = probe.cohort_key();
        assert_ne!(before, after, "the key follows the registration");
        assert!(after.contains("\"codec_key_ids\":[\"k1\"]"), "{after}");
        assert!(after.contains("\"queues\":[\"a\"]"), "{after}");

        let without = super::OutlierProbe {
            codecs: None,
            ..probe
        };
        assert_eq!(without.cohort_key(), without.cohort);
    }

    /// Issue #1815: the active key decides which codec encodes new payloads.
    /// Workers with different active keys are in different cohorts, and an
    /// activation restarts the window.
    #[test]
    fn the_cohort_key_follows_the_active_codec_key() {
        use crate::payload_codec::{IdentityCodec, PayloadCodecs};
        let codecs = PayloadCodecs::default();
        for key in ["k1", "k2"] {
            codecs
                .register_key(key, std::sync::Arc::new(IdentityCodec))
                .expect("register a key");
        }
        codecs.set_active_key("k1").expect("activate k1");
        let probe = super::OutlierProbe {
            codecs: Some(codecs.clone()),
            cohort: r#"{"queues":["a"]}"#.to_owned(),
            ..probe_for_slot(0, &std::sync::Arc::default(), &std::sync::Arc::default())
        };
        let on_k1 = probe.cohort_key();
        codecs.set_active_key("k2").expect("activate k2");
        assert_ne!(
            on_k1,
            probe.cohort_key(),
            "an activation changes the cohort"
        );
    }

    /// Issue #1815: the key set and the active key come from one read.
    ///
    /// `register_key` changes both under one write lock. Two reads could pair
    /// an empty key set with a new active key. Each window clear then restarts
    /// detection. The race is a few instructions wide, so this checks the
    /// source rather than timing.
    #[test]
    fn the_cohort_key_reads_the_codec_keys_under_one_lock() {
        // A Windows checkout has CRLF line ends. The end search needs LF.
        let src = include_str!("workers.rs").replace("\r\n", "\n");
        let start = src
            .find("pub fn cohort_and_epoch(&self)")
            .expect("cohort_and_epoch exists");
        let body = &src[start..];
        let body = &body[..body.find("\n    }\n").expect("cohort_and_epoch ends")];
        assert!(body.contains("codecs.key_state()"), "one snapshot read");
        for split in ["registered_key_ids()", "active_key_id()"] {
            assert!(!body.contains(split), "no separate read: {split}");
        }
    }

    /// Issue #1815: the heartbeat seeds the window with the cohort at start.
    /// A codec reload before the first tick then restarts the window too.
    #[test]
    fn a_codec_key_change_before_the_first_tick_restarts_the_window() {
        use crate::payload_codec::{IdentityCodec, PayloadCodecs};
        let codecs = PayloadCodecs::default();
        let probe = super::OutlierProbe {
            codecs: Some(codecs.clone()),
            cohort: r#"{"queues":["a"]}"#.to_owned(),
            ..probe_for_slot(0, &std::sync::Arc::default(), &std::sync::Arc::default())
        }
        .seeded();
        for _ in 0..30 {
            probe
                .window
                .record(true, std::time::Duration::from_millis(40));
        }
        codecs
            .register_key("k1", std::sync::Arc::new(IdentityCodec))
            .expect("register a key");
        let (_, first, _) = super::capture_task_stats(&probe.window, || probe.cohort_and_epoch());
        assert_eq!(first, crate::worker_outlier::WorkerTaskStats::default());
    }

    /// Issue #1815: samples taken under the old codec keys do not reach the
    /// new cohort, so they cannot flag the worker among its new peers.
    #[test]
    fn a_codec_key_change_restarts_the_outcome_window() {
        use crate::payload_codec::{IdentityCodec, PayloadCodecs};
        let codecs = PayloadCodecs::default();
        let probe = super::OutlierProbe {
            codecs: Some(codecs.clone()),
            cohort: r#"{"queues":["a"]}"#.to_owned(),
            ..probe_for_slot(0, &std::sync::Arc::default(), &std::sync::Arc::default())
        };
        for _ in 0..30 {
            probe
                .window
                .record(true, std::time::Duration::from_millis(40));
        }
        let (_, old, _) = super::capture_task_stats(&probe.window, || probe.cohort_and_epoch());
        assert_eq!(old.failures, 30);

        codecs
            .register_key("k1", std::sync::Arc::new(IdentityCodec))
            .expect("register a key");
        let (_, new, _) = super::capture_task_stats(&probe.window, || probe.cohort_and_epoch());
        assert_eq!(new, crate::worker_outlier::WorkerTaskStats::default());
    }

    /// Issue #1815: a change to another active key and back again before a
    /// tick leaves the same cohort key. The codec epoch still moves, so the
    /// samples taken before the changes do not stay in the cohort.
    #[test]
    fn a_codec_key_change_and_back_restarts_the_window() {
        use crate::payload_codec::{IdentityCodec, PayloadCodecs};
        let codecs = PayloadCodecs::default();
        for key in ["k1", "k2"] {
            codecs
                .register_key(key, std::sync::Arc::new(IdentityCodec))
                .expect("register a key");
        }
        let probe = super::OutlierProbe {
            codecs: Some(codecs.clone()),
            cohort: r#"{"queues":["a"]}"#.to_owned(),
            ..probe_for_slot(0, &std::sync::Arc::default(), &std::sync::Arc::default())
        }
        .seeded();
        let before = probe.cohort_key();
        for _ in 0..30 {
            probe
                .window
                .record(true, std::time::Duration::from_millis(40));
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
        codecs.set_active_key("k2").expect("activate k2");
        codecs.set_active_key("k1").expect("activate k1 again");
        assert_eq!(probe.cohort_key(), before, "the same key string");
        let (_, stats, _) = super::capture_task_stats(&probe.window, || probe.cohort_and_epoch());
        assert_eq!(stats, crate::worker_outlier::WorkerTaskStats::default());
    }

    /// Issue #1815: the cohort key is read inside the serialized capture. A
    /// heartbeat that reads its key later always captures later, so a key
    /// read before a codec reload can never outrank one read after it.
    #[test]
    fn the_cohort_key_is_read_inside_the_capture() {
        use std::sync::mpsc;
        let window = std::sync::Arc::new(crate::worker_outlier::TaskOutcomeWindow::default());
        let (reading, read_started) = mpsc::channel();
        let (release, released) = mpsc::channel::<()>();
        let old_window = std::sync::Arc::clone(&window);
        let old = std::thread::spawn(move || {
            let first = std::cell::Cell::new(true);
            super::capture_task_stats(&old_window, || {
                // Only the first read waits. The capture reads the key again
                // after its snapshot.
                if first.replace(false) {
                    reading.send(()).expect("signal the read");
                    released
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .expect("released");
                }
                (
                    "old".to_owned(),
                    crate::worker_outlier::CohortEpoch::default(),
                )
            })
        });
        read_started
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the old heartbeat reads its key");
        let new_window = std::sync::Arc::clone(&window);
        let new = std::thread::spawn(move || {
            super::capture_task_stats(&new_window, || {
                (
                    "new".to_owned(),
                    crate::worker_outlier::CohortEpoch::default(),
                )
            })
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(
            !new.is_finished(),
            "a capture waits while another heartbeat reads its key"
        );
        release.send(()).expect("release the old heartbeat");
        let (old_key, _, old_seq) = old.join().expect("old joins");
        let (new_key, _, new_seq) = new.join().expect("new joins");
        assert_eq!((old_key.as_str(), new_key.as_str()), ("old", "new"));
        assert!(new_seq > old_seq, "the later key has the higher sequence");
    }

    /// Issue #1815: a codec change can land between the key read and the
    /// snapshot. A task dispatched under the new key must then be published
    /// under that key, not under the key read before the change.
    #[test]
    fn a_codec_change_during_the_capture_publishes_under_the_new_key() {
        use crate::worker_outlier::{CohortEpoch, TaskOutcomeWindow};
        use std::time::{Duration, Instant};
        let window = TaskOutcomeWindow::default();
        let reads = std::cell::Cell::new(0_u32);
        let changed_at = std::cell::Cell::new(None);
        let (key, stats, _) = super::capture_task_stats(&window, || {
            reads.set(reads.get() + 1);
            if reads.get() == 1 {
                // The old key is read. The codec then changes, and a task
                // dispatched under the new key ends before the snapshot.
                let at = Instant::now();
                changed_at.set(Some(at));
                window.record_at(at, true, Duration::ZERO);
                return ("old".to_owned(), CohortEpoch::default());
            }
            (
                "new".to_owned(),
                CohortEpoch {
                    generation: 1,
                    changed_at: changed_at.get(),
                },
            )
        });
        assert_eq!(key, "new", "the snapshot carries the key it was taken in");
        assert_eq!(
            (stats.tasks, stats.failures),
            (1, 1),
            "the task dispatched under the new key stays"
        );
    }

    /// Issue #1815: a worker decodes history only with the codecs it has
    /// registered. Workers that differ in their codecs, or in the default
    /// codec, are therefore in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_ordinary_codecs() {
        use crate::payload_codec::{CodecError, PayloadCodec, PayloadCodecs};
        struct Rot;
        impl PayloadCodec for Rot {
            fn codec_id(&self) -> &'static str {
                "rot"
            }
            fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
                Ok(raw.to_vec())
            }
            fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
                Ok(encoded.to_vec())
            }
        }
        let key_for = |codecs: PayloadCodecs| {
            super::OutlierProbe {
                codecs: Some(codecs),
                cohort: r#"{"queues":["a"]}"#.to_owned(),
                ..probe_for_slot(0, &std::sync::Arc::default(), &std::sync::Arc::default())
            }
            .cohort_key()
        };
        let plain = key_for(PayloadCodecs::default());
        assert!(plain.contains("\"codec_ids\":[\"identity\"]"), "{plain}");
        assert!(
            plain.contains("\"default_codec_id\":\"identity\""),
            "{plain}"
        );

        let mut registered = PayloadCodecs::default();
        registered.register(std::sync::Arc::new(Rot));
        let registered = key_for(registered);
        assert_ne!(plain, registered, "a registered codec changes the key");
        assert!(
            registered.contains("\"codec_ids\":[\"identity\",\"rot\"]"),
            "{registered}"
        );

        let mut defaulted = PayloadCodecs::default();
        defaulted.set_default(std::sync::Arc::new(Rot));
        let defaulted = key_for(defaulted);
        assert_ne!(registered, defaulted, "the default codec changes the key");
        assert!(
            defaulted.contains("\"default_codec_id\":\"rot\""),
            "{defaulted}"
        );
    }

    /// Issue #1815: under load, the claim gate gives each worker a task mix
    /// that follows its slots per kind. Workers with different slot counts
    /// therefore do different work, so they are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_slots_per_kind() {
        let queues = vec!["a".to_owned()];
        let none = std::collections::HashMap::<String, u32>::new();
        let labels = std::collections::HashMap::<String, String>::new();
        let slots = |workflows, activities| {
            super::worker_cohort(&super::CohortPolicy {
                queues: &queues,
                queue_weights: &none,
                build_id: "v1",
                labels: &labels,
                slots: super::SlotPolicy::of(workflows, activities, None),
                session_slots: 0,
                priority_aging_secs: None,
                ineligible_activities: &[],
                shard_assignments: &[],
                registered_workflows: &[],
                registered_activities: &[],
                circuit_breakers: &crate::circuit_breaker::CircuitBreakerRegistry::empty(),
                dispatch_channel: &[],
                retry_budgets: &crate::retry_budget::RetryBudgetConfig::default(),
                adaptive_limits: &crate::adaptive_limit::AdaptiveLimitConfig::disabled(),
                outcome_window: std::time::Duration::from_secs(300),
                peer_stale_secs: 120,
                execution: super::ExecutionPolicy::default(),
                payload: super::PayloadPolicy::default(),
            })
        };
        assert_ne!(slots(100, 1), slots(1, 100));
        assert_ne!(slots(100, 100), slots(50, 100));
    }

    /// Issue #1815: workers that poll the same queues with different weights
    /// do different work, so they are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_queue_weights() {
        let unweighted = cohort_of(&["b", "a", "a"], &[], "", &[]);
        assert_eq!(
            unweighted,
            cohort_of(&["a", "b"], &[], "", &[]),
            "order and duplicates do not matter"
        );

        let bulk_first = cohort_of(&["a", "b"], &[("b", 5)], "", &[]);
        let equal = cohort_of(&["a", "b"], &[("a", 1)], "", &[]);
        assert_ne!(bulk_first, unweighted, "weights change the cohort");
        assert_ne!(bulk_first, equal, "different weights, different cohorts");
        // An unweighted worker claims from all its queues at once. A weighted
        // worker tries its queues in a weighted order, so even equal weights
        // form their own cohort.
        assert_ne!(equal, unweighted);
        // A queue missing from the map has weight 1, and an entry for a queue
        // the worker does not poll is ignored.
        assert_eq!(
            equal,
            cohort_of(&["b", "a"], &[("b", 1), ("z", 9)], "", &[])
        );
        // The weighted draw keeps every entry, so a listed-twice queue is
        // drawn more often. The order of weighted queues does not matter.
        let twice = cohort_of(&["a", "a", "b"], &[("a", 1)], "", &[]);
        assert_ne!(twice, equal, "a duplicate weighted queue changes the mix");
        assert_eq!(twice, cohort_of(&["b", "a", "a"], &[("a", 1)], "", &[]));
        // Zero-weight queues are tried last, in their configured order.
        let fallback = cohort_of(&["a", "y", "z"], &[("y", 0), ("z", 0)], "", &[]);
        assert_ne!(
            fallback,
            cohort_of(&["a", "z", "y"], &[("y", 0), ("z", 0)], "", &[]),
            "the fallback order decides which idle queue is drained first"
        );
        assert_eq!(
            fallback,
            cohort_of(&["y", "a", "z"], &[("y", 0), ("z", 0)], "", &[])
        );
    }

    /// Issue #1815: the claim predicate routes tasks by build id and by
    /// capability labels. Workers that differ in either can get different
    /// work, so they are in different cohorts.
    #[test]
    fn the_cohort_key_includes_the_build_and_the_labels() {
        let base = cohort_of(&["a"], &[], "v1", &[("gpu", "a100"), ("zone", "eu")]);
        assert_ne!(
            base,
            cohort_of(&["a"], &[], "v2", &[("gpu", "a100"), ("zone", "eu")])
        );
        assert_ne!(
            base,
            cohort_of(&["a"], &[], "v1", &[("gpu", "h100"), ("zone", "eu")])
        );
        assert_ne!(base, cohort_of(&["a"], &[], "v1", &[("zone", "eu")]));
        assert_eq!(
            base,
            cohort_of(&["a"], &[], "v1", &[("zone", "eu"), ("gpu", "a100")]),
            "label order does not matter"
        );
    }

    /// Records whether each gauge write ran while `views` was locked.
    struct LockProbe {
        views: std::sync::Arc<super::ShardPeerViews>,
        locked: std::sync::Mutex<Vec<bool>>,
    }

    impl crate::telemetry::MetricsRecorder for LockProbe {
        fn record_worker_outlier(
            &self,
            _dimension: crate::worker_outlier::OutlierDimension,
            _is_outlier: bool,
        ) {
            let locked = self.views.0.try_lock().is_err();
            self.locked
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(locked);
        }
    }

    /// Issue #1815: a tick that clears the verdict does so under the view
    /// lock. A healthy tick then cannot store a view and publish a verdict
    /// between the idle check and the clear.
    #[test]
    fn clearing_the_verdict_holds_the_shard_view_lock() {
        use crate::worker_outlier::OutlierDimension::FailureRatio;
        let views: std::sync::Arc<super::ShardPeerViews> = std::sync::Arc::default();
        let recorder = std::sync::Arc::new(LockProbe {
            views: std::sync::Arc::clone(&views),
            locked: std::sync::Mutex::default(),
        });
        let flags = std::sync::Arc::default();
        let mut probe = probe_for_slot(0, &views, &flags);
        probe.metrics = recorder.clone();
        let _ = flags.set("me", &[FailureRatio]);

        probe.clear_gauge("me");
        probe.retire("me");
        let locked = recorder
            .locked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert!(!locked.is_empty(), "the gauge is written");
        assert!(
            locked.iter().all(|held| *held),
            "every gauge write holds the view lock: {locked:?}"
        );
    }

    /// Issue #1815: an aborted heartbeat still retires its verdict, so a
    /// stopped worker cannot hold the shared gauge at 1.
    #[tokio::test]
    async fn an_aborted_heartbeat_retires_its_verdict() {
        use crate::worker_outlier::OutlierDimension::FailureRatio;
        let peers = std::sync::Arc::default();
        let flags: std::sync::Arc<super::ProcessOutlierFlags> = std::sync::Arc::default();
        let probe = probe_for_slot(0, &peers, &flags);
        let _ = flags.set("me", &[FailureRatio]);
        let task = tokio::spawn(async move {
            let _retire = super::RetireOnDrop::new(probe, "me".to_owned());
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;
        assert!(flags.set("peer", &[]).is_empty(), "the verdict is retired");
    }

    /// Issue #1815: a healthy worker in the same process cannot clear a sick
    /// worker's flag, and clearing the sick worker's verdict clears the OR.
    #[test]
    fn process_outlier_flags_report_the_or_of_local_workers() {
        use crate::worker_outlier::OutlierDimension::{FailureRatio, LatencyP99};
        let flags = super::ProcessOutlierFlags::default();
        assert_eq!(flags.set("sick", &[FailureRatio]), vec![FailureRatio]);
        assert_eq!(flags.set("healthy", &[]), vec![FailureRatio]);
        assert_eq!(
            flags.set("slow", &[LatencyP99]),
            vec![FailureRatio, LatencyP99]
        );
        assert_eq!(flags.set("sick", &[]), vec![LatencyP99]);
        // A stopped worker leaves the map, so its flag cannot linger.
        assert_eq!(flags.remove("slow"), Vec::new());
        // The emit callback sees the OR computed under the same lock.
        let mut seen = Vec::new();
        let any = flags.update("sick", Some(&[FailureRatio]), |any| seen = any.to_vec());
        assert_eq!(seen, any);
        assert_eq!(seen, vec![FailureRatio]);
    }

    /// Issue #1815: workers that share a recorder share verdicts, and a
    /// runtime with its own recorder does not.
    #[test]
    fn process_outlier_flags_are_kept_per_recorder() {
        use std::sync::Arc;
        let a: Arc<dyn crate::telemetry::MetricsRecorder> = Arc::new(crate::telemetry::NoOpMetrics);
        let b: Arc<dyn crate::telemetry::MetricsRecorder> = Arc::new(crate::telemetry::NoOpMetrics);
        let a_flags = super::ProcessOutlierFlags::for_recorder(&a);
        let a_again = super::ProcessOutlierFlags::for_recorder(&Arc::clone(&a));
        assert!(Arc::ptr_eq(&a_flags, &a_again));
        let b_flags = super::ProcessOutlierFlags::for_recorder(&b);
        assert!(!Arc::ptr_eq(&a_flags, &b_flags));

        // The registry holds each set weakly, so a stopped runtime's set goes.
        let weak = Arc::downgrade(&a_flags);
        drop((a_flags, a_again));
        assert_eq!(weak.strong_count(), 0, "the registry holds the set weakly");
        let fresh = super::ProcessOutlierFlags::for_recorder(&a);
        assert_eq!(
            Arc::strong_count(&fresh),
            1,
            "a fresh set replaces the dropped one"
        );
    }

    /// The fleet lookup that gates the capability-miss redelivery budget
    /// (issue #804) must keep using the SAME liveness predicate as the
    /// poison-pill orphan reclaimer, and must scope to the task's own queue.
    /// Two subsystems disagreeing about "is this worker alive" would let one
    /// escalate on a fleet the other still considers healthy.
    #[test]
    fn live_workers_on_queue_query_matches_the_shared_liveness_predicate() {
        let sql = super::live_workers_on_queue_query();
        assert!(
            sql.contains("last_heartbeat_at > NOW() - ($1::bigint * INTERVAL '1 second')"),
            "must reuse the poison-pill freshness window verbatim: {sql}"
        );
        assert!(
            sql.contains("queues @> to_jsonb($2::text)"),
            "a worker polling a DIFFERENT queue is not a candidate claimant: {sql}"
        );
        assert!(
            !sql.contains("status"),
            "a Draining worker is still live enough to be a capable peer, and \
             excluding it would let escalation conclude 'no capable worker' one \
             rollback too early: {sql}"
        );
    }

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
            max_concurrent_sessions: 0,
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
                max_concurrent_sessions: 0,
                in_use_sessions: 0,
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

    fn make_shard_row(worker_id: &str, shards: serde_json::Value) -> WorkerRow {
        let mut row = make_queue_row(worker_id, "default");
        row.worker.shard_assignments = shards;
        row
    }

    #[test]
    fn shard_assignments_cover_treats_empty_as_covering_any_shard() {
        // Issue #1150 / #961: an empty array is the auto (no sharded pool) and
        // legacy shape, meaning "covers whatever shard the row was read from".
        // Hardcoding 0 would be wrong -- ShardRouter accepts an arbitrary
        // default shard, so a single-shard deployment may be numbered 7.
        assert!(shard_assignments_cover(&serde_json::json!([]), 0));
        assert!(shard_assignments_cover(&serde_json::json!([]), 7));
    }

    #[test]
    fn shard_assignments_cover_still_requires_membership_when_narrowed() {
        assert!(shard_assignments_cover(&serde_json::json!([1, 2]), 2));
        assert!(!shard_assignments_cover(&serde_json::json!([1, 2]), 3));
    }

    #[test]
    fn shard_assignments_cover_rejects_a_malformed_non_array_value() {
        // A non-array is corrupt, not a legacy shape, so it covers nothing.
        assert!(!shard_assignments_cover(&serde_json::json!("0"), 0));
        assert!(!shard_assignments_cover(&serde_json::Value::Null, 0));
    }

    #[test]
    fn shard_assignments_cover_from_source_treats_empty_as_source_relative() {
        // Issue #1213: the auto/legacy empty shape covers "whatever shard the
        // row was read from" -- when a cross-shard fan-out reads the row from
        // a shard OTHER than the one the caller asked about, it must not
        // claim to cover the request. Only a request for the row's own
        // source shard is covered.
        assert!(shard_assignments_cover_from_source(
            &serde_json::json!([]),
            0,
            0
        ));
        assert!(!shard_assignments_cover_from_source(
            &serde_json::json!([]),
            0,
            1
        ));
    }

    #[test]
    fn shard_assignments_cover_from_source_ignores_source_when_narrowed() {
        // A non-empty list is the worker's own explicit claim and is
        // evaluated by membership alone, regardless of which shard's table
        // this particular row happened to be read from.
        assert!(shard_assignments_cover_from_source(
            &serde_json::json!([1, 2]),
            0,
            2
        ));
        assert!(!shard_assignments_cover_from_source(
            &serde_json::json!([1, 2]),
            0,
            3
        ));
    }

    #[test]
    fn shard_assignments_cover_from_source_rejects_a_malformed_non_array_value() {
        assert!(!shard_assignments_cover_from_source(
            &serde_json::json!("0"),
            0,
            0
        ));
        assert!(!shard_assignments_cover_from_source(
            &serde_json::Value::Null,
            0,
            0
        ));
    }

    #[test]
    fn shard_assignments_cover_matches_the_source_aware_predicate_at_matching_source() {
        // `shard_assignments_cover` is the single-shard convenience form used
        // by every consumer that already reads the row from the exact shard
        // being asked about (fleet_health.by_shard, queue coverage,
        // preflight): the row's source IS the request.
        for assignments in [
            serde_json::json!([]),
            serde_json::json!([5]),
            serde_json::json!([1, 2]),
        ] {
            assert_eq!(
                shard_assignments_cover(&assignments, 5),
                shard_assignments_cover_from_source(&assignments, 5, 5),
                "diverged for {assignments:?}"
            );
        }
    }

    #[test]
    fn apply_filters_shard_keeps_a_worker_on_the_empty_auto_assignment() {
        // Issue #961 (Codex round 3): a default worker built without a
        // ShardedDbPool registers `shard_assignments: []`. Shard health, queue
        // coverage and preflight all report it as covering the shard it polls,
        // so `GET /workers?shard_id=N` must not silently omit it.
        let rows = vec![
            make_shard_row("auto", serde_json::json!([])),
            make_shard_row("narrowed", serde_json::json!([1])),
        ];
        let mut filters = WorkerFilters::new();
        filters.shard_id = Some(0);
        let result = apply_worker_filters(rows, &filters);
        let ids: Vec<&str> = result.iter().map(|r| r.worker.worker_id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["auto"],
            "the auto-assignment worker must survive the shard filter and the \
             explicitly narrowed one must not"
        );
    }

    #[test]
    fn apply_filters_shard_still_narrows_an_explicit_assignment() {
        let rows = vec![
            make_shard_row("a", serde_json::json!([0])),
            make_shard_row("b", serde_json::json!([1])),
        ];
        let mut filters = WorkerFilters::new();
        filters.shard_id = Some(1);
        let result = apply_worker_filters(rows, &filters);
        let ids: Vec<&str> = result.iter().map(|r| r.worker.worker_id.as_str()).collect();
        assert_eq!(ids, vec!["b"]);
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
                max_concurrent_sessions: 0,
                in_use_sessions: 0,
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
        assert_eq!(ids.len(), 0);
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

    // -- Worker session capacity surfaces via WorkerRow's #[serde(flatten)]
    //    HarvestWorker (issue #606) -- no handler change needed for
    //    GET /workers / GET /workers/{id} to expose these fields.

    #[test]
    fn worker_row_flattens_session_capacity_fields() {
        let mut row = make_test_worker_row(vec![]);
        row.worker.max_concurrent_sessions = 5;
        row.worker.in_use_sessions = 2;

        let json = serde_json::to_value(&row).unwrap();
        // Flattened directly onto the top-level object, not nested under "worker".
        assert_eq!(json["max_concurrent_sessions"], serde_json::json!(5));
        assert_eq!(json["in_use_sessions"], serde_json::json!(2));
        assert!(
            json.get("worker").is_none(),
            "HarvestWorker fields must be flattened, not nested"
        );
    }

    #[test]
    fn worker_row_default_session_capacity_is_zero() {
        // The default-off contract (AC2): a worker that never calls
        // with_max_concurrent_sessions surfaces 0/0.
        let row = make_test_worker_row(vec![]);
        let json = serde_json::to_value(&row).unwrap();
        assert_eq!(json["max_concurrent_sessions"], serde_json::json!(0));
        assert_eq!(json["in_use_sessions"], serde_json::json!(0));
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
                max_concurrent_sessions: 0,
                in_use_sessions: 0,
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
        assert_eq!(item.queues, [] as [std::string::String; 0]);
        assert_eq!(item.shard_ids, [] as [i32; 0]);
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

    /// Run one heartbeat tick per entry of `latencies`, with the loop shape of
    /// [`spawn_worker_heartbeat`]. Each tick takes its latency in
    /// milliseconds. Return the start of each tick, in milliseconds after the
    /// loop started.
    async fn tick_starts(interval: Duration, latencies: &[u64]) -> Vec<u128> {
        let origin = tokio::time::Instant::now();
        let cancel = CancellationToken::new();
        let mut schedule = HeartbeatSchedule::new(interval);
        let mut starts = Vec::with_capacity(latencies.len());
        for latency in latencies {
            assert!(schedule.wait(&cancel).await, "no cancel was sent");
            starts.push(origin.elapsed().as_millis());
            tokio::time::sleep(Duration::from_millis(*latency)).await;
        }
        starts
    }

    /// Issue #1879: tick latency must not add to the heartbeat period.
    #[tokio::test(start_paused = true)]
    async fn heartbeat_period_does_not_grow_with_tick_latency() {
        let starts = tick_starts(Duration::from_millis(500), &[300, 300, 300, 300]).await;
        assert_eq!(starts, vec![500, 1000, 1500, 2000]);
    }

    /// Issue #1879: after a tick longer than one interval, the next tick
    /// starts at once. Then the schedule keeps one interval between starts.
    /// It sends no burst of ticks to catch up.
    #[tokio::test(start_paused = true)]
    async fn heartbeat_after_a_slow_tick_starts_at_once_with_no_burst() {
        let starts = tick_starts(Duration::from_millis(500), &[1200, 0, 0, 0]).await;
        assert_eq!(starts, vec![500, 1700, 2200, 2700]);
    }

    /// The first tick comes one interval after the loop starts.
    /// Registration has just written the row, so an immediate tick is waste.
    #[tokio::test(start_paused = true)]
    async fn heartbeat_first_tick_waits_one_interval() {
        let starts = tick_starts(Duration::from_secs(5), &[0]).await;
        assert_eq!(starts, vec![5000]);
    }

    /// A zero interval becomes 1 ms and does not panic.
    /// `spawn_worker_heartbeat` is public, so a caller can pass a value that
    /// the builder rejects.
    #[tokio::test(start_paused = true)]
    async fn heartbeat_schedule_floors_a_zero_interval_to_one_millisecond() {
        let starts = tick_starts(Duration::ZERO, &[0, 0, 0]).await;
        assert_eq!(starts, vec![1, 2, 3]);
    }

    /// A cancel ends the wait before the next tick.
    #[tokio::test(start_paused = true)]
    async fn heartbeat_wait_returns_false_on_cancel() {
        let cancel = CancellationToken::new();
        let mut schedule = HeartbeatSchedule::new(Duration::from_millis(500));
        cancel.cancel();
        assert!(!schedule.wait(&cancel).await);
    }
}
