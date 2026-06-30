//! Worker build-id routing for safe rolling deploys (issue #171).
//!
//! Harvest routes workflow tasks only to workers whose **build identity** is
//! declared compatible with the build that originally started the execution.
//! This prevents a worker running a semantically incompatible binary from
//! resuming in-flight executions and causing non-deterministic replay failures.
//!
//! ## Key concepts
//!
//! * **`BuildId`** — immutable identifier advertised by a worker (e.g. Git SHA,
//!   semver tag, or CI job ID). Stored in `harvest_workers.build_id`.
//! * **Build policy** — per-queue record in `harvest_build_policies` saying
//!   "new starts on this queue get `assigned_build_id = X`". Updated by
//!   operators when they want new executions to land on a new build.
//! * **Compatibility declaration** — row in `harvest_build_compat` saying
//!   "workers running build B are allowed to process executions assigned to
//!   build A". Operators add this *after* replay tests prove safety.
//! * **`required_build_id`** — denormalized onto `harvest_task_queue` so the
//!   claiming SQL can filter without a JOIN.
//! * **Build reachability** — counts of open executions, pending tasks, active
//!   workers, and stale workers by build. Used to decide when it is safe to
//!   retire old-build workers.
//!
//! ## Eligibility rule (implemented in `BuildCompatibilitySet`)
//!
//! A worker is eligible to claim a task when **any** of the following hold:
//! 1. The task has no `required_build_id` (legacy / pre-policy execution).
//! 2. The worker's `build_id` is empty (legacy worker, pre-dates routing).
//! 3. The worker's `build_id` exactly equals the task's `required_build_id`.
//! 4. There is an explicit compatibility declaration: worker build is declared
//!    compatible with the task's `required_build_id`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::{HarvestResult, database_error};

// ── BuildCompatibilitySet ─────────────────────────────────────────────────────

/// Pure in-memory representation of the declared build compatibility graph.
///
/// Built from `harvest_build_compat` rows via [`load_compat_set`]. Workers
/// call [`BuildCompatibilitySet::is_eligible`] once per task-claim cycle; the struct is cheap to
/// clone and can be refreshed periodically without holding a DB connection.
///
/// ## Eligibility rule
///
/// ```text
/// eligible = task.required_build_id is None
///          OR worker.build_id == ""           -- legacy worker
///          OR worker.build_id == required
///          OR compat_set contains (worker.build_id → required)
/// ```
#[derive(Debug, Clone, Default)]
pub struct BuildCompatibilitySet {
    /// Maps `worker_build_id` → set of `execution_build_id` values it can process.
    declarations: HashMap<String, HashSet<String>>,
}

impl BuildCompatibilitySet {
    /// Create an empty compatibility set (no cross-build declarations).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that workers running `worker_build` may process executions
    /// assigned to `compatible_with`.
    pub fn add_declaration(&mut self, worker_build: &str, compatible_with: &str) {
        self.declarations
            .entry(worker_build.to_string())
            .or_default()
            .insert(compatible_with.to_string());
    }

    /// Remove a previously added declaration.
    pub fn remove_declaration(&mut self, worker_build: &str, compatible_with: &str) {
        if let Some(set) = self.declarations.get_mut(worker_build) {
            set.remove(compatible_with);
        }
    }

    /// Check whether a worker is eligible to claim a task.
    ///
    /// `required` is `None` when the task has no build requirement (legacy
    /// executions started before a build policy was registered, or executions
    /// deliberately untagged).
    #[must_use]
    pub fn is_eligible(&self, worker_build: &str, required: Option<&str>) -> bool {
        match required {
            // No requirement — any worker may claim.
            None => true,
            Some(req) => {
                // Legacy worker (empty build_id) can claim anything.
                if worker_build.is_empty() {
                    return true;
                }
                // Same build always compatible.
                if worker_build == req {
                    return true;
                }
                // Explicit declaration.
                self.declarations
                    .get(worker_build)
                    .is_some_and(|set| set.contains(req))
            }
        }
    }
}

// ── DB model structs ──────────────────────────────────────────────────────────

/// Active build policy for a task queue.
///
/// New workflow executions started on `queue_name` receive
/// `assigned_build_id = build_id`. This does **not** affect existing
/// in-flight executions; those keep whatever build they were started with.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuildPolicy {
    pub id: Uuid,
    pub queue_name: String,
    pub build_id: String,
    pub deployment_name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A single row in `harvest_build_compat`.
///
/// Means: "workers running `build_id` are allowed to process executions
/// whose `assigned_build_id` equals `compatible_with`."
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuildCompatEntry {
    pub id: Uuid,
    /// Worker build that may process the other build's tasks.
    pub build_id: String,
    /// Execution build whose tasks the worker may process.
    pub compatible_with: String,
    pub declared_at: DateTime<Utc>,
}

/// Per-build reachability snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuildReachability {
    pub build_id: String,
    /// Workflow executions in a non-terminal state with this `assigned_build_id`.
    pub open_executions: i64,
    /// Tasks in `PENDING` state with this `required_build_id`.
    pub pending_tasks: i64,
    /// Workers in `Active` status with this `build_id` that have heartbeated
    /// recently (within `stale_threshold`).
    pub active_workers: i64,
    /// Workers with this `build_id` whose last heartbeat exceeds `stale_threshold`.
    pub stale_workers: i64,
    /// `true` when there are no open executions and no pending tasks for this
    /// build. Old-build workers can be safely stopped once this flips to `true`.
    pub safe_to_retire: bool,
}

// ── DB functions ──────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
use diesel_async::{AsyncPgConnection, RunQueryDsl};

/// Upsert the active build policy for a queue.
///
/// After this call, new workflow executions started on `queue_name` will have
/// `assigned_build_id = build_id` and task queue entries will carry
/// `required_build_id = build_id`. Existing in-flight executions are not
/// affected.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn set_build_policy(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    build_id: &str,
    deployment_name: Option<&str>,
) -> HarvestResult<BuildPolicy> {
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        build_id: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        deployment_name: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        created_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        updated_at: DateTime<Utc>,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "INSERT INTO harvest_build_policies (id, queue_name, build_id, deployment_name) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (queue_name) DO UPDATE \
             SET build_id = EXCLUDED.build_id, \
                 deployment_name = EXCLUDED.deployment_name, \
                 updated_at = NOW() \
         RETURNING id, queue_name, build_id, deployment_name, created_at, updated_at",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Text, _>(queue_name)
    .bind::<diesel::sql_types::Text, _>(build_id)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(deployment_name)
    .load(conn)
    .await
    .map_err(database_error)?;

    rows.into_iter()
        .next()
        .map(|r| BuildPolicy {
            id: r.id,
            queue_name: r.queue_name,
            build_id: r.build_id,
            deployment_name: r.deployment_name,
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
        .ok_or_else(|| database_error("set_build_policy: no row returned"))
}

/// Retrieve the active build policy for a queue, if one has been registered.
///
/// Returns `None` when no policy exists for `queue_name` (pre-policy
/// deployments behave as before: any worker can claim any task).
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn get_build_policy(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
) -> HarvestResult<Option<BuildPolicy>> {
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        build_id: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        deployment_name: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        created_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        updated_at: DateTime<Utc>,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT id, queue_name, build_id, deployment_name, created_at, updated_at \
         FROM harvest_build_policies \
         WHERE queue_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(queue_name)
    .load(conn)
    .await
    .map_err(database_error)?;

    Ok(rows.into_iter().next().map(|r| BuildPolicy {
        id: r.id,
        queue_name: r.queue_name,
        build_id: r.build_id,
        deployment_name: r.deployment_name,
        created_at: r.created_at,
        updated_at: r.updated_at,
    }))
}

/// List all registered build policies across all queues.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn list_build_policies(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<BuildPolicy>> {
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        build_id: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        deployment_name: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        created_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        updated_at: DateTime<Utc>,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT id, queue_name, build_id, deployment_name, created_at, updated_at \
         FROM harvest_build_policies \
         ORDER BY queue_name",
    )
    .load(conn)
    .await
    .map_err(database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| BuildPolicy {
            id: r.id,
            queue_name: r.queue_name,
            build_id: r.build_id,
            deployment_name: r.deployment_name,
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
        .collect())
}

/// Declare that workers running `build_id` are compatible with executions
/// that were assigned `compatible_with`.
///
/// Idempotent: re-declaring an existing entry is a no-op.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn declare_compat(
    conn: &mut AsyncPgConnection,
    build_id: &str,
    compatible_with: &str,
) -> HarvestResult<BuildCompatEntry> {
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        build_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        compatible_with: String,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        declared_at: DateTime<Utc>,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "INSERT INTO harvest_build_compat (id, build_id, compatible_with) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (build_id, compatible_with) DO UPDATE \
             SET declared_at = harvest_build_compat.declared_at \
         RETURNING id, build_id, compatible_with, declared_at",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Text, _>(build_id)
    .bind::<diesel::sql_types::Text, _>(compatible_with)
    .load(conn)
    .await
    .map_err(database_error)?;

    rows.into_iter()
        .next()
        .map(|r| BuildCompatEntry {
            id: r.id,
            build_id: r.build_id,
            compatible_with: r.compatible_with,
            declared_at: r.declared_at,
        })
        .ok_or_else(|| database_error("declare_compat: no row returned"))
}

/// Revoke a compatibility declaration.
///
/// Returns `true` if the row existed and was deleted, `false` if it was not
/// present.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn revoke_compat(
    conn: &mut AsyncPgConnection,
    build_id: &str,
    compatible_with: &str,
) -> HarvestResult<bool> {
    let count: usize = diesel::sql_query(
        "DELETE FROM harvest_build_compat \
         WHERE build_id = $1 AND compatible_with = $2",
    )
    .bind::<diesel::sql_types::Text, _>(build_id)
    .bind::<diesel::sql_types::Text, _>(compatible_with)
    .execute(conn)
    .await
    .map_err(database_error)?;

    Ok(count > 0)
}

/// Load the full compatibility graph from the database into a
/// [`BuildCompatibilitySet`] for in-process use.
///
/// Workers can cache this and refresh it periodically to avoid per-claim DB
/// round-trips (the claim SQL already enforces the rule server-side; this is
/// for application-layer checks and logging).
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn load_compat_set(conn: &mut AsyncPgConnection) -> HarvestResult<BuildCompatibilitySet> {
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        build_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        compatible_with: String,
    }

    let rows: Vec<Row> =
        diesel::sql_query("SELECT build_id, compatible_with FROM harvest_build_compat")
            .load(conn)
            .await
            .map_err(database_error)?;

    let mut set = BuildCompatibilitySet::new();
    for row in rows {
        set.add_declaration(&row.build_id, &row.compatible_with);
    }
    Ok(set)
}

/// Return a reachability snapshot for a single build.
///
/// The snapshot includes:
/// - `open_executions`: executions in a non-terminal state with this `assigned_build_id`
/// - `pending_tasks`: tasks in `PENDING` state with this `required_build_id`
/// - `active_workers`: workers in `Active` status that heartbeated within `stale_threshold`
/// - `stale_workers`: workers with this `build_id` whose heartbeat exceeded `stale_threshold`
/// - `safe_to_retire`: `true` iff `open_executions == 0` AND `pending_tasks == 0`
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn build_reachability(
    conn: &mut AsyncPgConnection,
    build_id: &str,
    stale_threshold: Duration,
) -> HarvestResult<BuildReachability> {
    #[derive(diesel::QueryableByName, Debug)]
    struct ReachRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        open_executions: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        pending_tasks: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        active_workers: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        stale_workers: i64,
    }

    let threshold_secs = i64::try_from(stale_threshold.as_secs()).unwrap_or(i64::MAX);

    let rows: Vec<ReachRow> = diesel::sql_query(
        "SELECT \
             (SELECT COUNT(*) FROM harvest_workflow_executions \
              WHERE assigned_build_id = $1 \
                AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TERMINATED', 'TIMED_OUT', 'CONTINUED_AS_NEW')) \
             AS open_executions, \
             (SELECT COUNT(*) FROM harvest_task_queue \
              WHERE required_build_id = $1 AND state = 'PENDING') \
             AS pending_tasks, \
             (SELECT COUNT(*) FROM harvest_workers \
              WHERE build_id = $1 \
                AND status = 'Active' \
                AND NOW() - last_heartbeat_at <= make_interval(secs => $2::float8)) \
             AS active_workers, \
             (SELECT COUNT(*) FROM harvest_workers \
              WHERE build_id = $1 \
                AND NOW() - last_heartbeat_at > make_interval(secs => $2::float8)) \
             AS stale_workers",
    )
    .bind::<diesel::sql_types::Text, _>(build_id)
    .bind::<diesel::sql_types::BigInt, _>(threshold_secs)
    .load(conn)
    .await
    .map_err(database_error)?;

    let row = rows.into_iter().next().unwrap_or(ReachRow {
        open_executions: 0,
        pending_tasks: 0,
        active_workers: 0,
        stale_workers: 0,
    });

    Ok(BuildReachability {
        build_id: build_id.to_string(),
        open_executions: row.open_executions,
        pending_tasks: row.pending_tasks,
        active_workers: row.active_workers,
        stale_workers: row.stale_workers,
        safe_to_retire: row.open_executions == 0 && row.pending_tasks == 0,
    })
}

/// List all compatibility declarations stored in `harvest_build_compat`.
///
/// Rows are ordered by `(build_id, compatible_with)` so the result is
/// deterministic and easy to page through in the UI.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn list_build_compat(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Vec<BuildCompatEntry>> {
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        build_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        compatible_with: String,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        declared_at: DateTime<Utc>,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT id, build_id, compatible_with, declared_at \
         FROM harvest_build_compat \
         ORDER BY build_id, compatible_with",
    )
    .load(conn)
    .await
    .map_err(database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| BuildCompatEntry {
            id: r.id,
            build_id: r.build_id,
            compatible_with: r.compatible_with,
            declared_at: r.declared_at,
        })
        .collect())
}

/// Return reachability snapshots for all distinct build IDs present across
/// `harvest_workflow_executions`, `harvest_task_queue`, and `harvest_workers`
/// on a **single shard**.
///
/// For multi-shard deployments prefer [`all_build_reachability_sharded`], which
/// fans out to every shard and merges the per-build counters.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
#[cfg(feature = "db")]
pub async fn all_build_reachability(
    conn: &mut AsyncPgConnection,
    stale_threshold: Duration,
) -> HarvestResult<Vec<BuildReachability>> {
    #[derive(diesel::QueryableByName, Debug)]
    struct IdRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        build_id: String,
    }

    // Collect all distinct non-empty build IDs from the three tables.
    let id_rows: Vec<IdRow> = diesel::sql_query(
        "SELECT DISTINCT build_id FROM ( \
             SELECT assigned_build_id AS build_id \
             FROM harvest_workflow_executions \
             WHERE assigned_build_id IS NOT NULL AND assigned_build_id <> '' \
             UNION \
             SELECT required_build_id AS build_id \
             FROM harvest_task_queue \
             WHERE required_build_id IS NOT NULL AND required_build_id <> '' \
             UNION \
             SELECT build_id \
             FROM harvest_workers \
             WHERE build_id IS NOT NULL AND build_id <> '' \
         ) sub \
         ORDER BY build_id",
    )
    .load(conn)
    .await
    .map_err(database_error)?;

    let mut result = Vec::with_capacity(id_rows.len());
    for row in id_rows {
        result.push(build_reachability(conn, &row.build_id, stale_threshold).await?);
    }
    Ok(result)
}

/// Return reachability snapshots for all distinct build IDs, aggregated across
/// every shard in `pool`.
///
/// Each shard is queried concurrently. Per-build counters
/// (`open_executions`, `pending_tasks`, `active_workers`, `stale_workers`) are
/// summed across shards; `safe_to_retire` is recomputed from the merged totals.
///
/// Single-shard deployments produce identical results to [`all_build_reachability`]
/// called directly; the extra indirection costs one pool connection checkout.
///
/// # Errors
///
/// Returns `HarvestError::Database` on the first shard that fails. Errors from
/// unreachable shards bubble up rather than being silently skipped so that
/// callers get an honest picture of fleet state (a failed shard means the
/// reachability data is incomplete).
#[cfg(feature = "db")]
pub async fn all_build_reachability_sharded(
    pool: &crate::shard::ShardedDbPool,
    stale_threshold: Duration,
) -> HarvestResult<Vec<BuildReachability>> {
    use futures::future::try_join_all;

    // Fan out to all shards concurrently.
    let shard_futures: Vec<_> = pool
        .iter_shards()
        .map(|(_, shard_pool)| {
            let shard_pool = shard_pool.clone();
            async move {
                let mut conn = shard_pool
                    .get()
                    .await
                    .map_err(|e| crate::error::HarvestError::Database(e.to_string()))?;
                all_build_reachability(&mut conn, stale_threshold).await
            }
        })
        .collect();

    let per_shard: Vec<Vec<BuildReachability>> = try_join_all(shard_futures).await?;

    Ok(merge_reachability(per_shard))
}

/// Merge per-shard reachability vectors into a single list.
///
/// Numeric counters are summed per `build_id`; `safe_to_retire` is
/// recomputed from the merged totals. The result is sorted by `build_id`.
#[must_use]
pub fn merge_reachability(per_shard: Vec<Vec<BuildReachability>>) -> Vec<BuildReachability> {
    let mut merged: BTreeMap<String, BuildReachability> = BTreeMap::new();
    for shard_results in per_shard {
        for r in shard_results {
            let entry = merged
                .entry(r.build_id.clone())
                .or_insert_with(|| BuildReachability {
                    build_id: r.build_id.clone(),
                    open_executions: 0,
                    pending_tasks: 0,
                    active_workers: 0,
                    stale_workers: 0,
                    safe_to_retire: false,
                });
            entry.open_executions += r.open_executions;
            entry.pending_tasks += r.pending_tasks;
            entry.active_workers += r.active_workers;
            entry.stale_workers += r.stale_workers;
        }
    }

    merged
        .into_values()
        .map(|mut r| {
            r.safe_to_retire = r.open_executions == 0 && r.pending_tasks == 0;
            r
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_compatibility_set_is_eligible() {
        let mut compat = BuildCompatibilitySet::new();

        // Rule 1: No requirement
        assert!(compat.is_eligible("build-A", None));
        assert!(compat.is_eligible("", None));

        // Rule 2: Legacy worker (empty build_id)
        assert!(compat.is_eligible("", Some("build-A")));

        // Rule 3: Exact match
        assert!(compat.is_eligible("build-A", Some("build-A")));
        assert!(!compat.is_eligible("build-B", Some("build-A")));

        // Rule 4: Explicit declaration
        compat.add_declaration("build-B", "build-A");
        assert!(compat.is_eligible("build-B", Some("build-A")));
        assert!(!compat.is_eligible("build-C", Some("build-A")));

        // Test removing declaration
        compat.remove_declaration("build-B", "build-A");
        assert!(!compat.is_eligible("build-B", Some("build-A")));

        // Remove non-existent declaration shouldn't panic
        compat.remove_declaration("build-Z", "build-A");
    }

    #[test]
    fn test_merge_reachability_sums_counters() {
        let r1 = BuildReachability {
            build_id: "build-A".to_string(),
            open_executions: 10,
            pending_tasks: 5,
            active_workers: 2,
            stale_workers: 1,
            safe_to_retire: true, // Should be recomputed
        };

        let r2 = BuildReachability {
            build_id: "build-A".to_string(),
            open_executions: 5,
            pending_tasks: 2,
            active_workers: 3,
            stale_workers: 0,
            safe_to_retire: true,
        };

        let merged = merge_reachability(vec![vec![r1], vec![r2]]);

        assert_eq!(merged.len(), 1);
        let m = &merged[0];
        assert_eq!(m.build_id, "build-A");
        assert_eq!(m.open_executions, 15);
        assert_eq!(m.pending_tasks, 7);
        assert_eq!(m.active_workers, 5);
        assert_eq!(m.stale_workers, 1);
        assert!(!m.safe_to_retire); // 15 + 7 > 0
    }

    #[test]
    fn test_merge_reachability_computes_safe_to_retire() {
        let r1 = BuildReachability {
            build_id: "build-A".to_string(),
            open_executions: 0,
            pending_tasks: 0,
            active_workers: 0,
            stale_workers: 0,
            safe_to_retire: false, // Initial value
        };

        let r2 = BuildReachability {
            build_id: "build-A".to_string(),
            open_executions: 0,
            pending_tasks: 0,
            active_workers: 0,
            stale_workers: 0,
            safe_to_retire: false,
        };

        let merged = merge_reachability(vec![vec![r1], vec![r2]]);

        assert_eq!(merged.len(), 1);
        assert!(merged[0].safe_to_retire); // 0 + 0 == 0
    }

    #[test]
    fn test_merge_reachability_sorts_by_build_id() {
        let r_b = BuildReachability {
            build_id: "build-B".to_string(),
            open_executions: 0,
            pending_tasks: 0,
            active_workers: 0,
            stale_workers: 0,
            safe_to_retire: false,
        };
        let r_a = BuildReachability {
            build_id: "build-A".to_string(),
            open_executions: 0,
            pending_tasks: 0,
            active_workers: 0,
            stale_workers: 0,
            safe_to_retire: false,
        };

        let merged = merge_reachability(vec![vec![r_b, r_a]]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].build_id, "build-A");
        assert_eq!(merged[1].build_id, "build-B");
    }
}
