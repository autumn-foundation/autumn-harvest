//! Read-only shard readiness aggregation for rollout gates.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use autumn_harvest::dlq;
use autumn_harvest::policy::Schedule;
use autumn_harvest::scheduler::SchedulerMonitor;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use autumn_harvest::workers::{WorkerFilters, WorkerHealth, WorkerRow, WorkerStatus, list_workers};
use chrono::{DateTime, Utc};
use diesel::migration::MigrationSource;
use diesel::pg::Pg;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use futures::future::join_all;
use serde::Serialize;

use crate::api::{HarvestApiRuntime, HarvestApiState};
use crate::shard_fanout;

/// Deployment readiness for a shard.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ShardReadiness {
    Ready,
    Degraded,
    Unavailable,
}

/// Operational role for a configured shard.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ShardRole {
    Readable,
    Writable,
    Default,
}

/// Response returned by `GET /admin/shards/health`.
#[derive(Debug, Clone, Serialize)]
pub struct ShardHealthReport {
    pub overall_readiness: ShardReadiness,
    pub observed_at: DateTime<Utc>,
    pub freshness_window_secs: u64,
    pub candidate_shard: Option<i32>,
    pub shards: Vec<ShardHealthRow>,
}

/// One shard row in the health response.
#[derive(Debug, Clone, Serialize)]
pub struct ShardHealthRow {
    pub shard_id: i32,
    pub roles: Vec<ShardRole>,
    pub candidate: bool,
    pub reachable: bool,
    pub active_worker_count: usize,
    pub stale_worker_count: usize,
    pub schema: ShardSchemaHealth,
    pub worker_coverage: Vec<QueueWorkerCoverage>,
    pub scheduler: ShardSchedulerCoverage,
    pub queue_depth: QueueDepthSummary,
    pub dlq: DlqSummary,
    pub last_health_sample_time: Option<DateTime<Utc>>,
    pub readiness: ShardReadiness,
    pub reason_codes: Vec<String>,
    pub blocking_reasons: Vec<String>,
    pub error_summary: Option<String>,
}

/// Migration/schema readiness details for a shard.
#[derive(Debug, Clone, Serialize)]
pub struct ShardSchemaHealth {
    pub ready: bool,
    pub applied_count: Option<usize>,
    pub missing_migrations: Vec<String>,
    pub error: Option<String>,
}

/// Worker coverage for a required queue on one shard.
#[derive(Debug, Clone, Serialize)]
pub struct QueueWorkerCoverage {
    pub queue: String,
    pub ready: bool,
    pub healthy_active: usize,
    pub stale: usize,
    pub draining: usize,
    pub stopped: usize,
    pub total_matching: usize,
}

/// Scheduler coverage for a shard when schedules exist.
#[derive(Debug, Clone, Serialize)]
pub struct ShardSchedulerCoverage {
    pub enabled: bool,
    pub ready: bool,
    pub running: bool,
    pub last_tick_at: Option<DateTime<Utc>>,
    pub tick_interval_ms: u64,
    pub freshness_window_secs: u64,
    pub schedule_count: usize,
    pub error: Option<String>,
}

/// Pending task count summary for a shard.
#[derive(Debug, Clone, Serialize)]
pub struct QueueDepthSummary {
    pub total_pending: i64,
    pub by_queue: BTreeMap<String, i64>,
    /// Distinct constraint requirements among claimable pending tasks, grouped
    /// by `(queue_name, required_capabilities, required_build_id)` (issue #522
    /// review). Used by the coverage gate to detect a queued task that no
    /// covering worker can actually claim — because of its labels OR its build.
    /// Empty when no pending task carries a capability or build-id constraint.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraint_demands: Vec<ConstraintDemand>,
    pub error: Option<String>,
}

/// A distinct constraint set among claimable pending tasks on a queue
/// (issue #522 review).
///
/// `claim_task` only lets a worker whose labels satisfy `required_capabilities`
/// AND whose build is eligible for `required_build_id` AND — when the row is
/// held by an unexpired sticky lease — *is* `sticky_owner` claim the task, so a
/// covering worker that polls the queue but fails any constraint does not
/// actually drain it. At least one of the three fields is non-empty (rows with
/// no constraint are covered by the basic per-queue check).
#[derive(Debug, Clone, Serialize)]
pub struct ConstraintDemand {
    pub queue_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_capabilities: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_build_id: Option<String>,
    /// `Some(worker_id)` when an unexpired sticky lease binds the row to a
    /// specific worker; only that worker can claim it until the lease expires.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sticky_owner: Option<String>,
    pub count: i64,
}

/// Dead-letter count summary for a shard.
#[derive(Debug, Clone, Serialize)]
pub struct DlqSummary {
    pub count: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
struct ScheduleProbe {
    schedule_expr: Option<String>,
    is_paused: bool,
    workflow_name: Option<String>,
    queue_name: Option<String>,
}

struct WorkerReadiness {
    coverage: Vec<QueueWorkerCoverage>,
    active_worker_count: usize,
    stale_worker_count: usize,
    last_health_sample_time: Option<DateTime<Utc>>,
    reason_codes: Vec<String>,
    blocking_reasons: Vec<String>,
    error_summary: Option<String>,
}

const REASON_CANDIDATE_SHARD_MISSING: &str = "candidate_shard_missing";
const REASON_SCHEDULER_NOT_RUNNING: &str = "scheduler_not_running";
const REASON_SCHEDULER_STALE: &str = "scheduler_stale";
const REASON_SCHEMA_MIGRATION_MISSING: &str = "schema_migration_missing";
const REASON_SCHEMA_UNREADABLE: &str = "schema_unreadable";
const REASON_SHARD_POOL_MISSING: &str = "shard_pool_missing";
pub(crate) const REASON_SHARD_UNREACHABLE: &str = "shard_unreachable";
const REASON_STORAGE_POOL_MISSING: &str = "storage_pool_missing";
const REASON_NO_LIVE_WORKER: &str = "no_live_worker";
const REASON_QUEUE_DEPTH_UNREADABLE: &str = "queue_depth_unreadable";
const REASON_WORKER_COVERAGE_UNREADABLE: &str = "worker_coverage_unreadable";
const REASON_WORKER_HEALTH_STALE: &str = "worker_health_stale";
const REASON_WORKER_QUEUE_UNCOVERED: &str = "worker_queue_uncovered";

/// Build the shard readiness report without mutating workflow state.
pub async fn build_shard_health_report(
    api_state: &HarvestApiState,
    candidate_shard: Option<i32>,
) -> ShardHealthReport {
    let observed_at = Utc::now();
    let freshness_window = api_state.worker_stale_threshold();
    let runtime = api_state.runtime().ok();
    let required_migrations = required_migration_versions();

    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    if let Ok(pool) = api_state.storage_pool() {
        let shard_observations = pool
            .iter_shards()
            .map(|(shard, shard_pool)| {
                observe_shard(
                    shard.as_i32(),
                    shard_pool,
                    runtime.as_ref(),
                    candidate_shard,
                    freshness_window,
                    &required_migrations,
                    observed_at,
                )
            })
            .collect::<Vec<_>>();
        for row in join_all(shard_observations).await {
            seen.insert(row.shard_id);
            rows.push(row);
        }
        if let Some(runtime) = runtime.as_ref() {
            for shard_id in router_shard_ids(runtime) {
                if seen.insert(shard_id) {
                    rows.push(unavailable_row(
                        shard_id,
                        roles_for_shard(shard_id, Some(runtime)),
                        candidate_shard == Some(shard_id),
                        freshness_window,
                        REASON_SHARD_POOL_MISSING,
                        format!(
                            "shard {shard_id} is configured in router but has no installed pool"
                        ),
                    ));
                }
            }
        }
    } else {
        let fallback = runtime.as_ref().map_or_else(
            || vec![ShardId::new(0)],
            |runtime| {
                router_shard_ids(runtime)
                    .into_iter()
                    .map(ShardId::new)
                    .collect()
            },
        );
        for shard in fallback {
            let shard_id = shard.as_i32();
            seen.insert(shard_id);
            rows.push(unavailable_row(
                shard_id,
                roles_for_shard(shard_id, runtime.as_ref()),
                candidate_shard == Some(shard_id),
                freshness_window,
                REASON_STORAGE_POOL_MISSING,
                "harvest storage pool is not configured".to_string(),
            ));
        }
    }

    if let Some(candidate) = candidate_shard
        && !seen.contains(&candidate)
    {
        rows.push(unavailable_row(
            candidate,
            roles_for_shard(candidate, runtime.as_ref()),
            true,
            freshness_window,
            REASON_CANDIDATE_SHARD_MISSING,
            format!("candidate shard {candidate} is not configured"),
        ));
    }

    rows.sort_by_key(|row| row.shard_id);
    let overall_readiness = rows
        .iter()
        .filter(|row| gated_row(row))
        .map(|row| row.readiness)
        .fold(ShardReadiness::Ready, worst_readiness);

    ShardHealthReport {
        overall_readiness,
        observed_at,
        freshness_window_secs: freshness_window.as_secs(),
        candidate_shard,
        shards: rows,
    }
}

fn gated_row(row: &ShardHealthRow) -> bool {
    row.candidate || row.roles.contains(&ShardRole::Writable)
}

const fn readiness_rank(readiness: ShardReadiness) -> u8 {
    match readiness {
        ShardReadiness::Ready => 0,
        ShardReadiness::Degraded => 1,
        ShardReadiness::Unavailable => 2,
    }
}

const fn worst_readiness(left: ShardReadiness, right: ShardReadiness) -> ShardReadiness {
    if readiness_rank(right) > readiness_rank(left) {
        right
    } else {
        left
    }
}

fn push_reason_code(reason_codes: &mut Vec<String>, reason_code: &'static str) {
    if !reason_codes.iter().any(|existing| existing == reason_code) {
        reason_codes.push(reason_code.to_string());
    }
}

/// Gate readiness when a writable shard has claimable work but no live covering
/// worker (issue #522). A stale (unresponsive) worker does not count.
/// Whether an unreadable claimable-queue-depth query should degrade this shard's
/// readiness (issue #522).
///
/// Only writable shards — and candidate shards being evaluated for promotion to
/// writable — must prove there is no claimable stranded work before reporting
/// Ready, so only they fail closed when the depth query fails. A read-only shard
/// takes no new starts and is outside the no-live-worker gate's scope, so an
/// unreadable depth there is recorded in `error_summary` without blocking.
fn queue_depth_unreadable_degrades(roles: &[ShardRole], candidate: bool) -> bool {
    roles.contains(&ShardRole::Writable) || candidate
}

#[allow(clippy::too_many_arguments)]
fn check_no_live_worker_gate(
    shard_id: i32,
    roles: &[ShardRole],
    candidate: bool,
    queue_depth: &QueueDepthSummary,
    workers: &Result<Vec<WorkerRow>, String>,
    compat: &autumn_harvest::build_routing::BuildCompatibilitySet,
    reason_codes: &mut Vec<String>,
    blocking_reasons: &mut Vec<String>,
) {
    // Writable shards take new starts, and a candidate shard being evaluated for
    // promotion to writable must prove the same "no stranded claimable work"
    // invariant *before* the flip (issue #522 review) — so a candidate that
    // already has claimable rows (prior test writes, migration validation) with
    // no covering worker must not report ready. Read-only, non-candidate shards
    // are outside this gate's scope.
    if (!roles.contains(&ShardRole::Writable) && !candidate) || queue_depth.total_pending == 0 {
        return;
    }

    // The queues that actually have claimable pending tasks.
    let pending_queues: std::collections::HashSet<&str> = queue_depth
        .by_queue
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(q, _)| q.as_str())
        .collect();

    // If worker coverage is unreadable, the worker-readiness path already emits
    // REASON_WORKER_COVERAGE_UNREADABLE. Bail out here so an unreadable query is
    // not mistaken for "no worker covers this shard" (a false no_live_worker).
    let Ok(ws) = workers.as_ref() else {
        return;
    };

    // Coverage is per pending queue, not shard-wide (fix #8 + per-queue,
    // issue #522): a worker assigned to this shard that polls `default` does
    // not drain pending `email` tasks, so checking that *any* worker covers
    // *any* pending queue would hide stranded work on an uncovered queue. Flag
    // when any queue with claimable work lacks a healthy, active, assigned
    // worker that polls it.
    let queue_covered = |queue: &str| {
        ws.iter().any(|w| {
            worker_assigned_to_shard(w, shard_id)
                && w.health == WorkerHealth::Healthy
                && w.worker.status == WorkerStatus::Active.as_str()
                && w.worker
                    .queues
                    .as_array()
                    .is_some_and(|qs| qs.iter().any(|v| v.as_str() == Some(queue)))
        })
    };
    let mut uncovered: Vec<&str> = pending_queues
        .iter()
        .copied()
        .filter(|q| !queue_covered(q))
        .collect();
    if !uncovered.is_empty() {
        uncovered.sort_unstable();
        push_reason_code(reason_codes, REASON_NO_LIVE_WORKER);
        blocking_reasons.push(format!(
            "{} claimable task(s) queued but no live worker assigned to this shard \
             polls queue(s) [{}]",
            queue_depth.total_pending,
            uncovered.join(", ")
        ));
    }

    // Constraint-aware coverage (issue #522 review): a worker that polls the
    // queue may still be unable to claim a pending task because of its
    // `required_capabilities` (Exact/In label match), its `required_build_id`
    // (exact / declared-compatible / legacy rule), or an unexpired sticky lease
    // binding it to a specific owner (only that worker can claim it until the
    // lease expires). claim_task enforces all three, so e.g. a GPU-only or
    // v2-only task, or a row leased to a now-stale worker, is stranded even
    // though `default` looks covered by the per-queue check above. All
    // constraints are checked against the *same* worker so a task needing
    // several is not falsely covered by different workers each satisfying only
    // one. Skip queues already flagged with no covering worker at all to avoid a
    // duplicate reason for the same demand.
    let already_uncovered: std::collections::HashSet<&str> = uncovered.iter().copied().collect();
    let mut uncovered_constraints: Vec<String> = Vec::new();
    for demand in &queue_depth.constraint_demands {
        if already_uncovered.contains(demand.queue_name.as_str()) {
            continue;
        }
        // Parse the capability requirements once. `None` ⇒ no label constraint.
        // An unparseable value is treated as "no label constraint" so the gate
        // never fabricates a coverage failure from a value it cannot read.
        let reqs: Option<Vec<autumn_harvest::eligibility::Requirement>> = demand
            .required_capabilities
            .as_ref()
            .and_then(|caps| serde_json::from_value(caps.clone()).ok());
        let satisfied = ws.iter().any(|w| {
            worker_assigned_to_shard(w, shard_id)
                && w.health == WorkerHealth::Healthy
                && w.worker.status == WorkerStatus::Active.as_str()
                && w.worker.queues.as_array().is_some_and(|qs| {
                    qs.iter()
                        .any(|v| v.as_str() == Some(demand.queue_name.as_str()))
                })
                && demand
                    .sticky_owner
                    .as_deref()
                    .is_none_or(|owner| owner == w.worker.worker_id)
                && compat.is_eligible(&w.worker.build_id, demand.required_build_id.as_deref())
                && reqs.as_ref().is_none_or(|reqs| {
                    let labels: std::collections::HashMap<String, String> =
                        serde_json::from_value(w.worker.labels.clone()).unwrap_or_default();
                    autumn_harvest::eligibility::matches_requirements(reqs, &labels)
                })
        });
        if !satisfied {
            uncovered_constraints.push(demand.queue_name.clone());
        }
    }
    if !uncovered_constraints.is_empty() {
        uncovered_constraints.sort_unstable();
        uncovered_constraints.dedup();
        push_reason_code(reason_codes, REASON_NO_LIVE_WORKER);
        blocking_reasons.push(format!(
            "claimable task(s) with capability/build/sticky requirements queued on queue(s) [{}] \
             but no live worker assigned to this shard can claim them",
            uncovered_constraints.join(", ")
        ));
    }
}

#[allow(clippy::too_many_lines)]
async fn observe_shard(
    shard_id: i32,
    shard_pool: &DbPool,
    runtime: Option<&HarvestApiRuntime>,
    candidate_shard: Option<i32>,
    freshness_window: Duration,
    required_migrations: &Result<Vec<String>, String>,
    observed_at: DateTime<Utc>,
) -> ShardHealthRow {
    let roles = roles_for_shard(shard_id, runtime);
    let candidate = candidate_shard == Some(shard_id);
    let Ok(mut conn) = shard_pool.get().await else {
        return unavailable_row(
            shard_id,
            roles,
            candidate,
            freshness_window,
            REASON_SHARD_UNREACHABLE,
            "database connection could not be acquired".to_string(),
        );
    };

    let schema = observe_schema(&mut conn, required_migrations).await;
    let schedules = load_schedule_probes(&mut conn).await;
    let required_queues = required_queues(runtime, schedules.as_deref().ok());
    let workers = load_workers(&mut conn, freshness_window).await;
    let workers_snapshot = workers.clone();
    // Activity names with a circuit-breaker policy skip the rate-limit gate at
    // claim, so the queue-depth query must exempt them from its rate-limit
    // filter to match claim_task. Empty when no runtime or no breakers.
    let circuit_breaker_activities: Vec<String> = runtime
        .map(|r| {
            r.registry()
                .circuit_breakers()
                .tracked_activity_names()
                .to_vec()
        })
        .unwrap_or_default();
    // Registered activity requirements, to back-fill eligibility for activity
    // rows that left required_capabilities NULL. Empty when no runtime.
    let activity_requirements = runtime
        .map(|r| r.registry().activity_requirements_json())
        .unwrap_or_default();
    let queue_depth = load_queue_depth(
        &mut conn,
        &circuit_breaker_activities,
        &activity_requirements,
    )
    .await;
    // Build compatibility set for the no_live_worker gate's build-routing check.
    // On load failure fall back to an empty set (exact-match / legacy rules).
    let compat = autumn_harvest::build_routing::load_compat_set(&mut conn)
        .await
        .unwrap_or_default();
    let dlq = load_dlq(&mut conn).await;

    let mut blocking_reasons = Vec::new();
    let mut reason_codes = Vec::new();
    let mut error_summary = None;

    if !schema.ready {
        if let Some(error) = &schema.error {
            push_reason_code(&mut reason_codes, REASON_SCHEMA_UNREADABLE);
            blocking_reasons.push(format!("schema readiness could not be confirmed: {error}"));
            error_summary.get_or_insert_with(|| error.clone());
        } else {
            push_reason_code(&mut reason_codes, REASON_SCHEMA_MIGRATION_MISSING);
            blocking_reasons.push(format!(
                "schema is missing required migrations: {}",
                schema.missing_migrations.join(", ")
            ));
        }
    }

    let mut worker_readiness = worker_readiness(
        shard_id,
        &required_queues,
        workers,
        freshness_window,
        observed_at,
    );
    reason_codes.append(&mut worker_readiness.reason_codes);
    blocking_reasons.append(&mut worker_readiness.blocking_reasons);
    if let Some(error) = worker_readiness.error_summary {
        error_summary.get_or_insert(error);
    }

    // Gate: writable shard with claimable work but no live covering worker (issue #522).
    check_no_live_worker_gate(
        shard_id,
        &roles,
        candidate,
        &queue_depth,
        &workers_snapshot,
        &compat,
        &mut reason_codes,
        &mut blocking_reasons,
    );

    let scheduler_status = scheduler_coverage(
        runtime,
        schedules.as_deref().ok(),
        shard_id,
        freshness_window,
        observed_at,
    );
    if !scheduler_status.ready {
        if scheduler_status.running {
            push_reason_code(&mut reason_codes, REASON_SCHEDULER_STALE);
        } else {
            push_reason_code(&mut reason_codes, REASON_SCHEDULER_NOT_RUNNING);
        }
        blocking_reasons.push(
            scheduler_status
                .error
                .clone()
                .unwrap_or_else(|| "scheduler coverage is not fresh".to_string()),
        );
    }

    if let Some(error) = &queue_depth.error {
        error_summary.get_or_insert_with(|| error.clone());
        // An unreadable queue-depth query forces `total_pending` to 0, which
        // makes `check_no_live_worker_gate` bail and would otherwise let a
        // writable/candidate shard report Ready (issue #522). In that failure
        // mode the gate cannot prove there is no claimable stranded work, so
        // fail closed: degrade readiness rather than attaching the error to an
        // otherwise-ready row.
        if queue_depth_unreadable_degrades(&roles, candidate) {
            push_reason_code(&mut reason_codes, REASON_QUEUE_DEPTH_UNREADABLE);
            blocking_reasons.push(format!(
                "claimable queue depth could not be read, so stranded work on this \
                 shard cannot be ruled out: {error}"
            ));
        }
    }
    if let Some(error) = &dlq.error {
        error_summary.get_or_insert_with(|| error.clone());
    }

    let reachable = true;
    let readiness = if blocking_reasons.is_empty() {
        ShardReadiness::Ready
    } else {
        ShardReadiness::Degraded
    };

    ShardHealthRow {
        shard_id,
        roles,
        candidate,
        reachable,
        active_worker_count: worker_readiness.active_worker_count,
        stale_worker_count: worker_readiness.stale_worker_count,
        schema,
        worker_coverage: worker_readiness.coverage,
        scheduler: scheduler_status,
        queue_depth,
        dlq,
        last_health_sample_time: worker_readiness.last_health_sample_time,
        readiness,
        reason_codes,
        blocking_reasons,
        error_summary,
    }
}

fn unavailable_row(
    shard_id: i32,
    roles: Vec<ShardRole>,
    candidate: bool,
    freshness_window: Duration,
    reason_code: &'static str,
    error_summary: String,
) -> ShardHealthRow {
    ShardHealthRow {
        shard_id,
        roles,
        candidate,
        reachable: false,
        active_worker_count: 0,
        stale_worker_count: 0,
        schema: ShardSchemaHealth {
            ready: false,
            applied_count: None,
            missing_migrations: Vec::new(),
            error: Some(error_summary.clone()),
        },
        worker_coverage: Vec::new(),
        scheduler: ShardSchedulerCoverage {
            enabled: false,
            ready: false,
            running: false,
            last_tick_at: None,
            tick_interval_ms: 0,
            freshness_window_secs: freshness_window.as_secs(),
            schedule_count: 0,
            error: Some(error_summary.clone()),
        },
        queue_depth: QueueDepthSummary {
            total_pending: 0,
            by_queue: BTreeMap::new(),
            constraint_demands: Vec::new(),
            error: Some(error_summary.clone()),
        },
        dlq: DlqSummary {
            count: None,
            error: Some(error_summary.clone()),
        },
        last_health_sample_time: None,
        readiness: ShardReadiness::Unavailable,
        reason_codes: vec![reason_code.to_string()],
        blocking_reasons: vec![error_summary.clone()],
        error_summary: Some(error_summary),
    }
}

fn roles_for_shard(shard_id: i32, runtime: Option<&HarvestApiRuntime>) -> Vec<ShardRole> {
    let Some(runtime) = runtime else {
        return if shard_id == 0 {
            vec![ShardRole::Readable, ShardRole::Writable, ShardRole::Default]
        } else {
            Vec::new()
        };
    };

    let shard = ShardId::new(shard_id);
    let mut roles = Vec::new();
    if runtime.router().readable_shards().contains(&shard) {
        roles.push(ShardRole::Readable);
    }
    if runtime.router().writable_shards().contains(&shard) {
        roles.push(ShardRole::Writable);
    }
    if runtime.router().default_shard() == shard {
        roles.push(ShardRole::Default);
    }
    roles
}

fn router_shard_ids(runtime: &HarvestApiRuntime) -> BTreeSet<i32> {
    let mut shards = BTreeSet::new();
    shards.extend(
        runtime
            .router()
            .readable_shards()
            .iter()
            .map(|shard| shard.as_i32()),
    );
    shards.extend(
        runtime
            .router()
            .writable_shards()
            .iter()
            .map(|shard| shard.as_i32()),
    );
    shards.insert(runtime.router().default_shard().as_i32());
    shards
}

#[derive(diesel::QueryableByName)]
struct MigrationVersionRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    version: String,
}

fn required_migration_versions() -> Result<Vec<String>, String> {
    let migrations = <diesel_migrations::EmbeddedMigrations as MigrationSource<Pg>>::migrations(
        &autumn_harvest::MIGRATIONS,
    )
    .map_err(|error| error.to_string())?;

    Ok(migrations
        .iter()
        .map(|migration| {
            let name = migration.name().to_string();
            name.split('_').next().unwrap_or(&name).to_string()
        })
        .collect())
}

async fn observe_schema(
    conn: &mut AsyncPgConnection,
    required_migrations: &Result<Vec<String>, String>,
) -> ShardSchemaHealth {
    let Ok(required) = required_migrations else {
        return ShardSchemaHealth {
            ready: false,
            applied_count: None,
            missing_migrations: Vec::new(),
            error: Some("Harvest embedded migration metadata could not be loaded".to_string()),
        };
    };

    let required_set = required.iter().cloned().collect::<HashSet<_>>();
    let rows = diesel::sql_query("SELECT version::TEXT AS version FROM __diesel_schema_migrations")
        .load::<MigrationVersionRow>(conn)
        .await;
    let Ok(rows) = rows else {
        return ShardSchemaHealth {
            ready: false,
            applied_count: None,
            missing_migrations: required.clone(),
            error: Some("migration table is not readable".to_string()),
        };
    };

    let present = rows
        .into_iter()
        .map(|row| row.version)
        .collect::<HashSet<_>>();
    let mut missing = required_set
        .difference(&present)
        .cloned()
        .collect::<Vec<_>>();
    missing.sort();
    ShardSchemaHealth {
        ready: missing.is_empty(),
        applied_count: Some(present.len()),
        missing_migrations: missing,
        error: None,
    }
}

async fn load_workers(
    conn: &mut AsyncPgConnection,
    freshness_window: Duration,
) -> Result<Vec<WorkerRow>, String> {
    let filters = WorkerFilters {
        limit: i64::MAX,
        ..WorkerFilters::new()
    };
    list_workers(conn, &filters, freshness_window)
        .await
        .map_err(|error| error.to_string())
}

fn worker_readiness(
    shard_id: i32,
    required_queues: &BTreeSet<String>,
    workers: Result<Vec<WorkerRow>, String>,
    freshness_window: Duration,
    observed_at: DateTime<Utc>,
) -> WorkerReadiness {
    let mut blocking_reasons = Vec::new();
    match workers {
        Ok(workers) => {
            let coverage = worker_coverage_by_queue(shard_id, required_queues, &workers);
            let active_worker_count = active_worker_count_for_shard(shard_id, &workers);
            let stale_worker_count = stale_worker_count_for_shard(shard_id, &workers);
            let mut reason_codes = Vec::new();
            for queue in &coverage {
                if !queue.ready {
                    push_reason_code(&mut reason_codes, REASON_WORKER_QUEUE_UNCOVERED);
                    blocking_reasons.push(format!(
                        "no healthy active worker covers required queue '{}' on shard {shard_id}",
                        queue.queue
                    ));
                }
            }
            let last_seen = workers
                .iter()
                .filter(|worker| worker_assigned_to_shard(worker, shard_id))
                .map(|worker| worker.worker.last_heartbeat_at)
                .max();
            if !required_queues.is_empty() {
                push_health_freshness_blocker(
                    last_seen,
                    freshness_window,
                    observed_at,
                    &mut reason_codes,
                    &mut blocking_reasons,
                );
            }
            WorkerReadiness {
                coverage,
                active_worker_count,
                stale_worker_count,
                last_health_sample_time: last_seen,
                reason_codes,
                blocking_reasons,
                error_summary: None,
            }
        }
        Err(error) => {
            let reason_codes = vec![REASON_WORKER_COVERAGE_UNREADABLE.to_string()];
            blocking_reasons.push(format!("worker coverage could not be read: {error}"));
            WorkerReadiness {
                coverage: Vec::new(),
                active_worker_count: 0,
                stale_worker_count: 0,
                last_health_sample_time: None,
                reason_codes,
                blocking_reasons,
                error_summary: Some(error),
            }
        }
    }
}

fn push_health_freshness_blocker(
    last_seen: Option<DateTime<Utc>>,
    freshness_window: Duration,
    observed_at: DateTime<Utc>,
    reason_codes: &mut Vec<String>,
    blocking_reasons: &mut Vec<String>,
) {
    let Some(last_seen) = last_seen else {
        push_reason_code(reason_codes, REASON_WORKER_HEALTH_STALE);
        blocking_reasons.push("no worker health data recorded on shard".to_string());
        return;
    };
    let elapsed = observed_at
        .signed_duration_since(last_seen)
        .to_std()
        .unwrap_or(Duration::ZERO);
    if elapsed > freshness_window {
        push_reason_code(reason_codes, REASON_WORKER_HEALTH_STALE);
        blocking_reasons.push(format!(
            "health data is stale; latest worker heartbeat exceeds {}s freshness window",
            freshness_window.as_secs()
        ));
    }
}

fn worker_coverage_by_queue(
    shard_id: i32,
    required_queues: &BTreeSet<String>,
    workers: &[WorkerRow],
) -> Vec<QueueWorkerCoverage> {
    required_queues
        .iter()
        .map(|queue| {
            let matching = workers
                .iter()
                .filter(|worker| worker_can_cover(worker, queue, shard_id))
                .collect::<Vec<_>>();
            let healthy_active = matching
                .iter()
                .filter(|worker| {
                    worker.health == WorkerHealth::Healthy
                        && worker.worker.status == WorkerStatus::Active.as_str()
                })
                .count();
            let stale = matching
                .iter()
                .filter(|worker| worker.health == WorkerHealth::Stale)
                .count();
            let draining = matching
                .iter()
                .filter(|worker| worker.worker.status == WorkerStatus::Draining.as_str())
                .count();
            let stopped = matching
                .iter()
                .filter(|worker| worker.worker.status == WorkerStatus::Stopped.as_str())
                .count();
            QueueWorkerCoverage {
                queue: queue.clone(),
                ready: healthy_active > 0,
                healthy_active,
                stale,
                draining,
                stopped,
                total_matching: matching.len(),
            }
        })
        .collect()
}

fn active_worker_count_for_shard(shard_id: i32, workers: &[WorkerRow]) -> usize {
    workers
        .iter()
        .filter(|worker| {
            worker_assigned_to_shard(worker, shard_id)
                && worker.worker.status == WorkerStatus::Active.as_str()
        })
        .count()
}

fn stale_worker_count_for_shard(shard_id: i32, workers: &[WorkerRow]) -> usize {
    workers
        .iter()
        .filter(|worker| {
            worker_assigned_to_shard(worker, shard_id) && worker.health == WorkerHealth::Stale
        })
        .count()
}

fn worker_can_cover(worker: &WorkerRow, queue: &str, shard_id: i32) -> bool {
    let has_queue = worker
        .worker
        .queues
        .as_array()
        .is_some_and(|queues| queues.iter().any(|value| value.as_str() == Some(queue)));
    has_queue && worker_assigned_to_shard(worker, shard_id)
}

/// Delegates to the shared [`shard_fanout::worker_covers_shard`] predicate --
/// see its doc comment for why an empty `shard_assignments` array must be
/// treated as covering *any* shard being inspected, not "assigned to
/// nothing" or hardcoded to shard 0 (issue #1150).
fn worker_assigned_to_shard(worker: &WorkerRow, shard_id: i32) -> bool {
    shard_fanout::worker_covers_shard(worker, shard_id)
}

async fn load_queue_depth(
    conn: &mut AsyncPgConnection,
    circuit_breaker_activities: &[String],
    activity_requirements: &std::collections::HashMap<String, serde_json::Value>,
) -> QueueDepthSummary {
    // Derive both the per-queue depth and the constraint demands from the single
    // shared core query, so the basic per-queue coverage check and the
    // capability/build constraint check see exactly the same claimable rows the
    // stranded-work sampler sees (issue #522). The query mirrors every claim_task
    // gate: PAUSED workflow tasks, expired `schedule_to_close_at`, concurrency-cap
    // saturation, and rate-limit exhaustion (with the circuit-breaker exemption).
    // A row no worker could claim right now never degrades readiness.
    let mut demands = match autumn_harvest::queue::claimable_pending_demand_by_queue(
        conn,
        circuit_breaker_activities,
    )
    .await
    {
        Ok(demands) => demands,
        Err(error) => {
            return QueueDepthSummary {
                total_pending: 0,
                by_queue: BTreeMap::new(),
                constraint_demands: Vec::new(),
                error: Some(error.to_string()),
            };
        }
    };

    // Back-fill effective requirements for activity rows that didn't snapshot
    // required_capabilities (legacy/manual enqueues), mirroring claim_task's
    // ineligible-activities gate so the constraint check below catches them.
    autumn_harvest::queue::apply_activity_requirements(&mut demands, activity_requirements);

    let mut total_pending = 0;
    let mut by_queue: BTreeMap<String, i64> = BTreeMap::new();
    for demand in &demands {
        total_pending += demand.count;
        *by_queue.entry(demand.queue_name.clone()).or_default() += demand.count;
    }

    // Keep only the rows that carry a capability, build-id, OR sticky-lease
    // constraint; the fully-unconstrained rows are already covered by the basic
    // per-queue check.
    let constraint_demands = demands
        .into_iter()
        .filter(|d| {
            d.required_capabilities.is_some()
                || d.required_build_id.is_some()
                || d.sticky_owner.is_some()
        })
        .map(|d| ConstraintDemand {
            queue_name: d.queue_name,
            required_capabilities: d.required_capabilities,
            required_build_id: d.required_build_id,
            sticky_owner: d.sticky_owner,
            count: d.count,
        })
        .collect();

    QueueDepthSummary {
        total_pending,
        by_queue,
        constraint_demands,
        error: None,
    }
}

async fn load_dlq(conn: &mut AsyncPgConnection) -> DlqSummary {
    dlq::dead_letter_count(conn).await.map_or_else(
        |error| DlqSummary {
            count: None,
            error: Some(error.to_string()),
        },
        |count| DlqSummary {
            count: Some(count),
            error: None,
        },
    )
}

#[derive(diesel::QueryableByName)]
struct ScheduleProbeRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    schedule_expr: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    is_paused: bool,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    workflow_name: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    queue_name: Option<String>,
}

async fn load_schedule_probes(conn: &mut AsyncPgConnection) -> Result<Vec<ScheduleProbe>, String> {
    let rows = diesel::sql_query(
        "SELECT schedule_expr::TEXT AS schedule_expr, \
                is_paused, \
                workflow_name::TEXT AS workflow_name, \
                queue_name::TEXT AS queue_name \
         FROM harvest_schedules",
    )
    .load::<ScheduleProbeRow>(conn)
    .await
    .map_err(|error| error.to_string())?;

    Ok(rows
        .into_iter()
        .map(|row| ScheduleProbe {
            schedule_expr: row.schedule_expr,
            is_paused: row.is_paused,
            workflow_name: row.workflow_name,
            queue_name: row.queue_name,
        })
        .collect())
}

fn required_queues(
    runtime: Option<&HarvestApiRuntime>,
    schedules: Option<&[ScheduleProbe]>,
) -> BTreeSet<String> {
    let mut queues = BTreeSet::new();
    if let Some(runtime) = runtime {
        queues.extend(runtime.queues().iter().cloned());
        if !runtime.registry().workflows.is_empty() && queues.is_empty() {
            queues.insert("default".to_string());
        }
        for activity in runtime.registry().activities.values() {
            // See the matching comment in `preflight::required_queues`: the
            // two reserved worker-session internal activities dispatch on
            // the caller-supplied session queue, never `default_queue`.
            // Synthetic liveness canary (issue #796): the built-in canary
            // activity has `default_queue: None` but is dispatched on the
            // probe's target queue (from the workflow input), never
            // `default_queue` — counting it would inject a phantom `"default"`
            // required queue and flip a healthy deployment to a missing-worker
            // readiness failure. See `preflight::required_queues`.
            if !activity.is_local
                && !autumn_harvest::is_reserved_session_activity_name(activity.name)
                && !autumn_harvest::canary::is_reserved_canary_name(activity.name)
            {
                queues.insert(activity.default_queue.unwrap_or("default").to_string());
            }
        }
        for schedule in runtime.workflow_schedules() {
            queues.insert(schedule.queue_name.clone());
        }
    }
    if let Some(schedules) = schedules {
        for schedule in schedules {
            if let Some(queue) = &schedule.queue_name {
                queues.insert(queue.clone());
            } else if persisted_workflow_schedule_uses_default_queue(schedule) {
                queues.insert("default".to_string());
            }
        }
    }
    queues.retain(|queue| !queue.trim().is_empty());

    queues
}

fn persisted_workflow_schedule_uses_default_queue(schedule: &ScheduleProbe) -> bool {
    !schedule.is_paused
        && schedule.workflow_name.is_some()
        && schedule
            .schedule_expr
            .as_deref()
            .is_some_and(|expr| !expr.eq_ignore_ascii_case("manual"))
}

fn scheduler_coverage(
    runtime: Option<&HarvestApiRuntime>,
    schedules: Option<&[ScheduleProbe]>,
    shard_id: i32,
    freshness_window: Duration,
    observed_at: DateTime<Utc>,
) -> ShardSchedulerCoverage {
    let snapshot = runtime.map_or_else(
        || SchedulerMonitor::offline().snapshot(),
        HarvestApiRuntime::scheduler_snapshot,
    );
    let schedule_count = schedule_count_for_shard(runtime, schedules, shard_id);
    if schedule_count == 0 {
        return ShardSchedulerCoverage {
            enabled: false,
            ready: true,
            running: snapshot.running,
            last_tick_at: snapshot.last_tick_at,
            tick_interval_ms: snapshot.tick_interval_ms,
            freshness_window_secs: freshness_window.as_secs(),
            schedule_count,
            error: None,
        };
    }

    let scheduler_window =
        Duration::from_millis(snapshot.tick_interval_ms.saturating_mul(2)).max(freshness_window);
    let mut error = None;
    if !snapshot.running {
        error = Some("scheduler coverage is required but scheduler is not running".to_string());
    } else if let Some(last_tick_at) = snapshot.last_tick_at {
        let elapsed = observed_at
            .signed_duration_since(last_tick_at)
            .to_std()
            .unwrap_or(Duration::ZERO);
        if elapsed > scheduler_window {
            error = Some(format!(
                "scheduler coverage is stale; last tick exceeds {}s freshness window",
                scheduler_window.as_secs()
            ));
        }
    } else {
        error =
            Some("scheduler coverage is stale; no scheduler tick has been recorded".to_string());
    }

    ShardSchedulerCoverage {
        enabled: true,
        ready: error.is_none(),
        running: snapshot.running,
        last_tick_at: snapshot.last_tick_at,
        tick_interval_ms: snapshot.tick_interval_ms,
        freshness_window_secs: scheduler_window.as_secs(),
        schedule_count,
        error,
    }
}

fn schedule_count_for_shard(
    runtime: Option<&HarvestApiRuntime>,
    schedules: Option<&[ScheduleProbe]>,
    shard_id: i32,
) -> usize {
    let mut count = 0;
    if let Some(runtime) = runtime {
        // Count a workflow schedule only on the shard that actually owns its
        // rows, mirroring scheduler::workflow_schedule_shard /
        // register_workflow_schedules_for_shard: workflow-only schedules route to
        // the router's default shard, DAG-derived schedules to pick_for_dag.
        // Without this filter every shard counts every schedule, so an offline
        // local scheduler monitor on a non-owning shard would degrade readiness
        // with `scheduler_not_running` for rows that live on another shard
        // (issue #522 review).
        let target = ShardId::new(shard_id);
        count += runtime
            .workflow_schedules()
            .iter()
            .filter(|schedule| {
                !schedule.paused
                    && !matches!(schedule.schedule, Schedule::Manual)
                    && schedule.dag_name.as_deref().map_or_else(
                        || runtime.router().default_shard(),
                        |dag| runtime.router().pick_for_dag(dag),
                    ) == target
            })
            .count();
        count += runtime
            .dags()
            .values()
            .filter(|dag| {
                dag.schedule
                    .as_ref()
                    .is_some_and(|schedule| !matches!(schedule, Schedule::Manual))
                    && runtime.router().pick_for_dag(&dag.name) == target
            })
            .count();
    }
    if let Some(schedules) = schedules {
        count += schedules
            .iter()
            .filter(|schedule| {
                !schedule.is_paused
                    && schedule
                        .schedule_expr
                        .as_deref()
                        .is_some_and(|expr| !expr.eq_ignore_ascii_case("manual"))
            })
            .count();
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    fn persisted_schedule(
        schedule_expr: Option<&str>,
        is_paused: bool,
        workflow_name: Option<&str>,
        queue_name: Option<&str>,
    ) -> ScheduleProbe {
        ScheduleProbe {
            schedule_expr: schedule_expr.map(ToOwned::to_owned),
            is_paused,
            workflow_name: workflow_name.map(ToOwned::to_owned),
            queue_name: queue_name.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn required_queues_includes_default_for_persisted_workflow_schedule_without_queue() {
        let schedules = [persisted_schedule(
            Some("interval:60"),
            false,
            Some("billing_workflow"),
            None,
        )];

        let queues = required_queues(None, Some(&schedules));

        assert!(queues.contains("default"));
    }

    #[test]
    fn required_queues_does_not_infer_default_for_paused_manual_or_dag_schedule_rows() {
        let schedules = [
            persisted_schedule(Some("interval:60"), true, Some("paused_workflow"), None),
            persisted_schedule(Some("manual"), false, Some("manual_workflow"), None),
            persisted_schedule(Some("interval:60"), false, None, None),
        ];

        let queues = required_queues(None, Some(&schedules));

        assert!(!queues.contains("default"));
    }

    // -------------------------------------------------------------------------
    // no_live_worker gate unit tests (issue #522)
    // -------------------------------------------------------------------------

    fn make_worker_row(shard_id: i32, status: &str, health: WorkerHealth) -> WorkerRow {
        use autumn_harvest::models::HarvestWorker;
        let now = chrono::Utc::now();
        WorkerRow {
            worker: HarvestWorker {
                worker_id: format!("test-worker-{shard_id}"),
                started_at: now,
                last_heartbeat_at: now,
                queues: serde_json::json!(["default"]),
                shard_assignments: serde_json::json!([shard_id]),
                max_concurrency: 10,
                in_flight_count: 0,
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
            health,
            active_task_ids: Vec::new(),
        }
    }

    fn pending_queue_depth(total: i64) -> QueueDepthSummary {
        // Populate by_queue so the queue-intersection check in
        // check_no_live_worker_gate can match workers that serve "default".
        let mut by_queue = std::collections::BTreeMap::new();
        if total > 0 {
            by_queue.insert("default".to_string(), total);
        }
        QueueDepthSummary {
            total_pending: total,
            by_queue,
            constraint_demands: Vec::new(),
            error: None,
        }
    }

    /// An empty build-compatibility set: only exact-build and legacy-worker
    /// (empty `build_id`) rules apply, matching a deployment with no declared
    /// cross-build compatibility.
    fn empty_compat() -> autumn_harvest::build_routing::BuildCompatibilitySet {
        autumn_harvest::build_routing::BuildCompatibilitySet::default()
    }

    // ── worker_assigned_to_shard / worker_can_cover: legacy empty-array
    //    shard_assignments (issue #1150, discovered via #774's review) ────

    fn worker_row_with_shard_assignments(shard_assignments: &[i64], queues: &[&str]) -> WorkerRow {
        use autumn_harvest::models::HarvestWorker;
        let now = chrono::Utc::now();
        WorkerRow {
            worker: HarvestWorker {
                worker_id: "legacy-worker".to_string(),
                started_at: now,
                last_heartbeat_at: now,
                queues: serde_json::json!(queues),
                shard_assignments: serde_json::json!(shard_assignments),
                max_concurrency: 10,
                in_flight_count: 0,
                host: "localhost".to_string(),
                version: None,
                status: WorkerStatus::Active.as_str().to_string(),
                drain_deadline_at: None,
                build_id: String::new(),
                deployment_name: None,
                labels: serde_json::json!({}),
                max_concurrent_sessions: 0,
                in_use_sessions: 0,
            },
            health: WorkerHealth::Healthy,
            active_task_ids: Vec::new(),
        }
    }

    #[test]
    fn worker_assigned_to_shard_treats_empty_array_as_covering_shard_zero() {
        // A `Worker::run` legacy single-shard worker (no explicit
        // `shard_assignments` configured) registers with a literal `[]`, but
        // its poll loop falls back to the default pool and claims work
        // against whatever database that pool points at -- this shard-
        // membership check must treat that the same way, or a perfectly
        // healthy legacy worker's readiness/coverage counts under-report it
        // (issue #1150).
        let worker = worker_row_with_shard_assignments(&[], &["default"]);
        assert!(worker_assigned_to_shard(&worker, 0));
    }

    #[test]
    fn worker_assigned_to_shard_empty_array_is_not_hardcoded_to_shard_zero() {
        // The empty-array normalization must NOT be hardcoded to shard 0 --
        // this predicate is only ever evaluated with a worker/shard_id pair
        // already scoped to one shard's own connection (`observe_shard`), so
        // an empty-array worker found there covers whatever `shard_id` is
        // being evaluated, including a deployment whose single/default shard
        // is numbered something other than 0 (`ShardRouter::new` accepts an
        // arbitrary `default_shard`; see `queue_coverage.rs`'s identical
        // fix and its doc comment for the full rationale).
        let worker = worker_row_with_shard_assignments(&[], &["default"]);
        assert!(worker_assigned_to_shard(&worker, 5));
    }

    #[test]
    fn worker_assigned_to_shard_nonempty_array_still_matches_only_its_own_shards() {
        // Regression guard: the empty-array special case must not widen a
        // worker with an EXPLICIT, non-empty assignment list into matching
        // every shard.
        let worker = worker_row_with_shard_assignments(&[2], &["default"]);
        assert!(worker_assigned_to_shard(&worker, 2));
        assert!(!worker_assigned_to_shard(&worker, 0));
    }

    #[test]
    fn worker_can_cover_legacy_empty_array_worker_covers_its_queue() {
        let worker = worker_row_with_shard_assignments(&[], &["orphan_queue"]);
        assert!(worker_can_cover(&worker, "orphan_queue", 3));
    }

    #[test]
    fn no_live_worker_gate_fires_when_writable_pending_and_no_covering_worker() {
        let roles = vec![ShardRole::Writable];
        let queue_depth = pending_queue_depth(5);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should emit no_live_worker when no worker covers the shard"
        );
        assert!(!blocking_reasons.is_empty());
    }

    #[test]
    fn no_live_worker_gate_fires_for_candidate_shard_with_uncovered_pending() {
        // A read-only shard offered as a promotion candidate must prove the
        // no-stranded-work invariant before the flip (issue #522 review): pending
        // rows with no covering worker block readiness even though it is not yet
        // Writable.
        let roles = vec![ShardRole::Readable];
        let queue_depth = pending_queue_depth(5);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            true, // candidate
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "candidate shard with uncovered pending work should fire no_live_worker"
        );
        assert!(!blocking_reasons.is_empty());
    }

    #[test]
    fn no_live_worker_gate_skips_readonly_non_candidate_shard() {
        // A plain read-only shard that is not a candidate takes no new starts and
        // is outside this gate's scope — pending rows there do not block.
        let roles = vec![ShardRole::Readable];
        let queue_depth = pending_queue_depth(5);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false, // not a candidate
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            !reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "read-only non-candidate shard should not fire no_live_worker"
        );
        assert!(blocking_reasons.is_empty());
    }

    #[test]
    fn queue_depth_unreadable_degrades_writable_and_candidate_shards() {
        // Writable shard: must prove no stranded work → fail closed.
        assert!(queue_depth_unreadable_degrades(
            &[ShardRole::Writable],
            false
        ));
        // Read-only candidate being evaluated for promotion → fail closed.
        assert!(queue_depth_unreadable_degrades(
            &[ShardRole::Readable],
            true
        ));
        // Writable candidate → fail closed.
        assert!(queue_depth_unreadable_degrades(
            &[ShardRole::Writable],
            true
        ));
        // Plain read-only, non-candidate shard takes no new starts → not blocking.
        assert!(!queue_depth_unreadable_degrades(
            &[ShardRole::Readable],
            false
        ));
        // No roles, non-candidate → not blocking.
        assert!(!queue_depth_unreadable_degrades(&[], false));
    }

    #[test]
    fn no_live_worker_gate_suppressed_when_healthy_active_worker_covers_shard() {
        let roles = vec![ShardRole::Writable];
        let queue_depth = pending_queue_depth(5);
        let worker = make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![worker]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            !reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should not fire when a healthy active worker covers the shard"
        );
        assert!(blocking_reasons.is_empty());
    }

    #[test]
    fn no_live_worker_gate_suppressed_when_shard_is_not_writable() {
        let roles = vec![ShardRole::Readable];
        let queue_depth = pending_queue_depth(5);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            !reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should not fire for non-writable shards"
        );
    }

    #[test]
    fn no_live_worker_gate_suppressed_when_queue_is_empty() {
        let roles = vec![ShardRole::Writable];
        let queue_depth = pending_queue_depth(0);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            !reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should not fire when there is no pending work"
        );
    }

    #[test]
    fn no_live_worker_gate_fires_when_stale_worker_covers_shard() {
        // A stale (unresponsive) worker should not count as "live" coverage.
        let roles = vec![ShardRole::Writable];
        let queue_depth = pending_queue_depth(3);
        let stale_worker = make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Stale);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![stale_worker]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "stale worker should not count as live coverage"
        );
    }

    #[test]
    fn no_live_worker_gate_fires_when_worker_covers_different_shard() {
        // A healthy active worker assigned to shard 1 should not cover shard 0.
        let roles = vec![ShardRole::Writable];
        let queue_depth = pending_queue_depth(3);
        let worker_on_other_shard =
            make_worker_row(1, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![worker_on_other_shard]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0, // checking shard 0
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "worker on different shard should not count as coverage"
        );
    }

    fn two_queue_depth(default_pending: i64, email_pending: i64) -> QueueDepthSummary {
        let mut by_queue = std::collections::BTreeMap::new();
        if default_pending > 0 {
            by_queue.insert("default".to_string(), default_pending);
        }
        if email_pending > 0 {
            by_queue.insert("email".to_string(), email_pending);
        }
        QueueDepthSummary {
            total_pending: default_pending + email_pending,
            by_queue,
            constraint_demands: Vec::new(),
            error: None,
        }
    }

    #[test]
    fn no_live_worker_gate_fires_when_one_pending_queue_is_uncovered() {
        // Pending work on `default` and `email`; the only worker polls
        // `default`, so the `email` tasks are stranded even though some
        // pending work is covered.
        let roles = vec![ShardRole::Writable];
        let queue_depth = two_queue_depth(3, 2);
        let worker = make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        // make_worker_row already polls only ["default"].
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![worker]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should fire when a pending queue has no covering worker"
        );
        assert!(
            blocking_reasons
                .iter()
                .any(|r| r.contains("email") && !r.contains("default")),
            "blocking reason should name the uncovered queue only: {blocking_reasons:?}"
        );
    }

    #[test]
    fn no_live_worker_gate_suppressed_when_every_pending_queue_is_covered() {
        // Pending work on `default` and `email`; two workers cover them
        // between them, so no work is stranded.
        let roles = vec![ShardRole::Writable];
        let queue_depth = two_queue_depth(3, 2);
        let default_worker =
            make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        let mut email_worker =
            make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        email_worker.worker.queues = serde_json::json!(["email"]);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![default_worker, email_worker]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            !reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should not fire when every pending queue has a covering worker"
        );
        assert!(blocking_reasons.is_empty());
    }

    /// Build a `QueueDepthSummary` with one capability-bearing pending task on
    /// `default` requiring label `gpu = "true"`.
    fn gpu_queue_depth() -> QueueDepthSummary {
        let mut by_queue = std::collections::BTreeMap::new();
        by_queue.insert("default".to_string(), 1);
        QueueDepthSummary {
            total_pending: 1,
            by_queue,
            constraint_demands: vec![ConstraintDemand {
                queue_name: "default".to_string(),
                required_capabilities: Some(
                    serde_json::json!([{"Exact": {"key": "gpu", "value": "true"}}]),
                ),
                required_build_id: None,
                sticky_owner: None,
                count: 1,
            }],
            error: None,
        }
    }

    /// Build a `QueueDepthSummary` with one build-routed pending task on
    /// `default` requiring build id `v2`.
    fn build_routed_queue_depth() -> QueueDepthSummary {
        let mut by_queue = std::collections::BTreeMap::new();
        by_queue.insert("default".to_string(), 1);
        QueueDepthSummary {
            total_pending: 1,
            by_queue,
            constraint_demands: vec![ConstraintDemand {
                queue_name: "default".to_string(),
                required_capabilities: None,
                required_build_id: Some("v2".to_string()),
                sticky_owner: None,
                count: 1,
            }],
            error: None,
        }
    }

    /// Build a `QueueDepthSummary` with one pending task on `default` held by an
    /// unexpired sticky lease owned by `worker_id`.
    fn sticky_queue_depth(owner: &str) -> QueueDepthSummary {
        let mut by_queue = std::collections::BTreeMap::new();
        by_queue.insert("default".to_string(), 1);
        QueueDepthSummary {
            total_pending: 1,
            by_queue,
            constraint_demands: vec![ConstraintDemand {
                queue_name: "default".to_string(),
                required_capabilities: None,
                required_build_id: None,
                sticky_owner: Some(owner.to_string()),
                count: 1,
            }],
            error: None,
        }
    }

    #[test]
    fn no_live_worker_gate_fires_when_capability_requirement_unsatisfied() {
        // A GPU-only activity is queued on `default`; the only worker polls
        // `default` but has no `gpu` label, so claim_task would never let it
        // claim the task — the work is stranded even though `default` looks
        // covered by the basic per-queue check.
        let roles = vec![ShardRole::Writable];
        let queue_depth = gpu_queue_depth();
        let worker = make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        // make_worker_row polls ["default"] with empty labels.
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![worker]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should fire when no worker satisfies the pending task's capabilities"
        );
        assert!(
            blocking_reasons
                .iter()
                .any(|r| r.contains("capability") && r.contains("default")),
            "blocking reason should name the capability-uncovered queue: {blocking_reasons:?}"
        );
    }

    #[test]
    fn no_live_worker_gate_suppressed_when_capability_requirement_satisfied() {
        // The same GPU-only activity, but the covering worker carries the
        // `gpu = "true"` label, so it can claim the task — no stranded work.
        let roles = vec![ShardRole::Writable];
        let queue_depth = gpu_queue_depth();
        let mut worker = make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        worker.worker.labels = serde_json::json!({"gpu": "true"});
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![worker]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            !reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should not fire when a covering worker satisfies the capabilities"
        );
        assert!(blocking_reasons.is_empty());
    }

    #[test]
    fn no_live_worker_gate_fires_when_build_requirement_unsatisfied() {
        // A v2-only task is queued on `default`; the only worker polls `default`
        // but runs build `v1` with no compatibility declared, so claim_task
        // would never let it claim the task — stranded even though `default`
        // looks covered by the basic per-queue check.
        let roles = vec![ShardRole::Writable];
        let queue_depth = build_routed_queue_depth();
        let mut worker = make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        worker.worker.build_id = "v1".to_string();
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![worker]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should fire when no worker's build is eligible for the pending task"
        );
        assert!(
            blocking_reasons
                .iter()
                .any(|r| r.contains("build") && r.contains("default")),
            "blocking reason should name the build-uncovered queue: {blocking_reasons:?}"
        );
    }

    #[test]
    fn no_live_worker_gate_suppressed_when_build_requirement_satisfied() {
        // The same v2-only task, but the covering worker runs build `v2`, so it
        // can claim it — no stranded work.
        let roles = vec![ShardRole::Writable];
        let queue_depth = build_routed_queue_depth();
        let mut worker = make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        worker.worker.build_id = "v2".to_string();
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![worker]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            !reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should not fire when a covering worker's build is eligible"
        );
        assert!(blocking_reasons.is_empty());
    }

    #[test]
    fn no_live_worker_gate_suppressed_when_build_compat_declared() {
        // The v2-only task with only a v1 worker, but a compatibility
        // declaration (v1 may process v2) makes the worker eligible — mirroring
        // claim_task's harvest_build_compat lookup.
        let roles = vec![ShardRole::Writable];
        let queue_depth = build_routed_queue_depth();
        let mut worker = make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        worker.worker.build_id = "v1".to_string();
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![worker]);
        let mut compat = autumn_harvest::build_routing::BuildCompatibilitySet::default();
        compat.add_declaration("v1", "v2");
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &compat,
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            !reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should not fire when a compat declaration makes the worker build-eligible"
        );
        assert!(blocking_reasons.is_empty());
    }

    #[test]
    fn no_live_worker_gate_fires_when_sticky_lease_owner_is_not_covering() {
        // The pending row is held by an unexpired sticky lease owned by a worker
        // that is not present (stale/gone). The only live worker on `default`
        // (test-worker-0) is NOT the owner, so claim_task would not let it claim
        // the row until the lease expires — the work is stranded.
        let roles = vec![ShardRole::Writable];
        let queue_depth = sticky_queue_depth("test-worker-stale");
        let worker = make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![worker]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should fire when the sticky-lease owner is not a live covering worker"
        );
        assert!(
            blocking_reasons
                .iter()
                .any(|r| r.contains("sticky") && r.contains("default")),
            "blocking reason should name the sticky-uncovered queue: {blocking_reasons:?}"
        );
    }

    #[test]
    fn no_live_worker_gate_suppressed_when_sticky_lease_owner_is_covering() {
        // The same sticky-leased row, but the lease owner (test-worker-0) is the
        // live covering worker, so it can claim the row — no stranded work.
        let roles = vec![ShardRole::Writable];
        let queue_depth = sticky_queue_depth("test-worker-0");
        let worker = make_worker_row(0, WorkerStatus::Active.as_str(), WorkerHealth::Healthy);
        let workers: Result<Vec<WorkerRow>, String> = Ok(vec![worker]);
        let mut reason_codes = Vec::new();
        let mut blocking_reasons = Vec::new();

        check_no_live_worker_gate(
            0,
            &roles,
            false,
            &queue_depth,
            &workers,
            &empty_compat(),
            &mut reason_codes,
            &mut blocking_reasons,
        );

        assert!(
            !reason_codes.contains(&REASON_NO_LIVE_WORKER.to_string()),
            "should not fire when the sticky-lease owner is the live covering worker"
        );
        assert!(blocking_reasons.is_empty());
    }
}
