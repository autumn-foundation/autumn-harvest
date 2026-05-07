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
    pub error: Option<String>,
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
const REASON_SHARD_UNREACHABLE: &str = "shard_unreachable";
const REASON_STORAGE_POOL_MISSING: &str = "storage_pool_missing";
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
    let queue_depth = load_queue_depth(&mut conn).await;
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

fn worker_assigned_to_shard(worker: &WorkerRow, shard_id: i32) -> bool {
    worker
        .worker
        .shard_assignments
        .as_array()
        .is_some_and(|shards| {
            shards
                .iter()
                .any(|value| value.as_i64() == Some(i64::from(shard_id)))
        })
}

#[derive(diesel::QueryableByName)]
struct QueueDepthRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    queue_name: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    depth: i64,
}

async fn load_queue_depth(conn: &mut AsyncPgConnection) -> QueueDepthSummary {
    let rows = diesel::sql_query(
        "SELECT queue_name::TEXT AS queue_name, COUNT(*)::BIGINT AS depth \
         FROM harvest_task_queue \
         WHERE state = 'PENDING' \
           AND scheduled_at <= NOW() \
         GROUP BY queue_name \
         ORDER BY queue_name",
    )
    .load::<QueueDepthRow>(conn)
    .await;

    rows.map_or_else(
        |error| QueueDepthSummary {
            total_pending: 0,
            by_queue: BTreeMap::new(),
            error: Some(error.to_string()),
        },
        |rows| {
            let mut total_pending = 0;
            let mut by_queue = BTreeMap::new();
            for row in rows {
                total_pending += row.depth;
                by_queue.insert(row.queue_name, row.depth);
            }
            QueueDepthSummary {
                total_pending,
                by_queue,
                error: None,
            }
        },
    )
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
    queue_name: Option<String>,
}

async fn load_schedule_probes(conn: &mut AsyncPgConnection) -> Result<Vec<ScheduleProbe>, String> {
    let rows = diesel::sql_query(
        "SELECT schedule_expr::TEXT AS schedule_expr, \
                is_paused, \
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
            if !activity.is_local {
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
            }
        }
    }
    queues.retain(|queue| !queue.trim().is_empty());

    queues
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
        count += runtime
            .workflow_schedules()
            .iter()
            .filter(|schedule| !schedule.paused && !matches!(schedule.schedule, Schedule::Manual))
            .count();
        count += runtime
            .dags()
            .values()
            .filter(|dag| {
                dag.schedule
                    .as_ref()
                    .is_some_and(|schedule| !matches!(schedule, Schedule::Manual))
                    && runtime.router().pick_for_dag(&dag.name) == ShardId::new(shard_id)
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
