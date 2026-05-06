//! Read-only deployment preflight checks for the Harvest management API.

use std::collections::{BTreeSet, HashSet};

use autumn_harvest::dlq;
use autumn_harvest::models::HarvestSchedule;
use autumn_harvest::schema::harvest_schedules;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use autumn_harvest::workers::{WorkerFilters, WorkerHealth, WorkerRow, WorkerStatus, list_workers};
use chrono::{DateTime, Utc};
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel::migration::MigrationSource;
use diesel::pg::Pg;
use diesel_async::RunQueryDsl;
use serde::Serialize;
use serde_json::{Value, json};

use crate::api::HarvestApiState;

/// Status for the overall preflight report and each individual check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PreflightStatus {
    Pass,
    Warn,
    Fail,
}

impl PreflightStatus {
    const fn rank(self) -> u8 {
        match self {
            Self::Pass => 0,
            Self::Warn => 1,
            Self::Fail => 2,
        }
    }
}

/// Version metadata returned with every preflight report.
#[derive(Debug, Clone, Serialize)]
pub struct PreflightVersion {
    pub package: &'static str,
    pub version: &'static str,
    pub core_version: &'static str,
}

/// One preflight check result.
#[derive(Debug, Clone, Serialize)]
pub struct PreflightCheckResult {
    pub name: String,
    pub status: PreflightStatus,
    pub summary: String,
    pub remediation: Option<String>,
    pub affected_shards: Vec<i32>,
    pub details: Value,
}

/// Deployment-readiness report returned by `GET /admin/preflight`.
#[derive(Debug, Clone, Serialize)]
pub struct PreflightReport {
    pub overall_status: PreflightStatus,
    pub observed_at: DateTime<Utc>,
    pub version: PreflightVersion,
    pub checks: Vec<PreflightCheckResult>,
}

/// Build a read-only deployment preflight report.
pub async fn build_preflight_report(api_state: &HarvestApiState) -> PreflightReport {
    let mut checks = Vec::new();
    checks.push(check_api_reachability(api_state));
    checks.push(check_migrations(api_state).await);
    checks.push(check_shard_availability(api_state).await);
    checks.push(check_catalog_consistency(api_state));
    checks.push(check_schedule_resolvability(api_state).await);
    checks.push(check_worker_coverage(api_state).await);
    checks.push(check_dlq_read_access(api_state).await);
    checks.push(check_retention_visibility(api_state));
    checks.push(check_admin_auth_boundary(api_state));

    let overall_status = checks
        .iter()
        .map(|check| check.status)
        .max_by_key(|status| status.rank())
        .unwrap_or(PreflightStatus::Fail);

    PreflightReport {
        overall_status,
        observed_at: Utc::now(),
        version: PreflightVersion {
            package: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            core_version: env!("CARGO_PKG_VERSION"),
        },
        checks,
    }
}

fn check(
    name: impl Into<String>,
    status: PreflightStatus,
    summary: impl Into<String>,
    remediation: Option<&str>,
    mut affected_shards: Vec<i32>,
    details: Value,
) -> PreflightCheckResult {
    affected_shards.sort_unstable();
    affected_shards.dedup();
    PreflightCheckResult {
        name: name.into(),
        status,
        summary: summary.into(),
        remediation: remediation.map(str::to_string),
        affected_shards,
        details,
    }
}

fn check_api_reachability(api_state: &HarvestApiState) -> PreflightCheckResult {
    api_state.runtime().map_or_else(
        |_| {
            check(
                "api_reachability",
                PreflightStatus::Fail,
                "management API is reachable but the Harvest runtime is not installed",
                Some("Start HarvestPlugin before running deployment preflight."),
                Vec::new(),
                json!({ "runtime_ready": false }),
            )
        },
        |runtime| {
            check(
                "api_reachability",
                PreflightStatus::Pass,
                "management API is reachable and a Harvest runtime snapshot is installed",
                None,
                Vec::new(),
                json!({
                    "runtime_ready": true,
                    "queues": runtime.queues(),
                    "workflow_count": runtime.registry().workflows.len(),
                    "activity_count": runtime.registry().activities.len(),
                    "dag_count": runtime.dags().len(),
                }),
            )
        },
    )
}

#[derive(diesel::QueryableByName)]
struct MigrationVersionRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    version: String,
}

async fn check_migrations(api_state: &HarvestApiState) -> PreflightCheckResult {
    let required = match required_migration_versions() {
        Ok(required) => required,
        Err(reason) => {
            return check(
                "migrations",
                PreflightStatus::Fail,
                "Harvest embedded migration metadata could not be loaded",
                Some("Rebuild the binary with the packaged autumn-harvest migrations."),
                Vec::new(),
                json!({ "error": reason }),
            );
        }
    };
    let required_set = required.iter().cloned().collect::<HashSet<_>>();
    let Ok(pool) = api_state.storage_pool() else {
        return check(
            "migrations",
            PreflightStatus::Fail,
            "Harvest storage pool is not configured",
            Some("Install the Harvest storage pool before mounting the management API."),
            Vec::new(),
            json!({ "required_migrations": required }),
        );
    };

    let mut affected = Vec::new();
    let mut shards = Vec::new();
    for (shard, shard_pool) in pool.iter_shards() {
        let shard_id = shard.as_i32();
        let observation = observe_migration_shard(shard_id, shard_pool, &required_set).await;
        if !observation.passed {
            affected.push(shard_id);
        }
        shards.push(observation.details);
    }

    let status = if affected.is_empty() {
        PreflightStatus::Pass
    } else {
        PreflightStatus::Fail
    };
    check(
        "migrations",
        status,
        if status == PreflightStatus::Pass {
            "required Harvest migrations are present on every configured shard"
        } else {
            "one or more configured shards are missing required Harvest migrations"
        },
        (status == PreflightStatus::Fail)
            .then_some("Run the Harvest migration stack on every affected shard before promotion."),
        affected,
        json!({
            "required_migrations": required,
            "shards": shards,
        }),
    )
}

struct ShardObservation {
    passed: bool,
    details: Value,
}

async fn observe_migration_shard(
    shard_id: i32,
    shard_pool: &DbPool,
    required_set: &HashSet<String>,
) -> ShardObservation {
    let Ok(mut conn) = shard_pool.get().await else {
        return ShardObservation {
            passed: false,
            details: json!({
                "shard_id": shard_id,
                "status": "fail",
                "error": "database connection could not be acquired",
            }),
        };
    };

    let rows = diesel::sql_query("SELECT version::TEXT AS version FROM __diesel_schema_migrations")
        .load::<MigrationVersionRow>(&mut conn)
        .await;
    let Ok(rows) = rows else {
        return ShardObservation {
            passed: false,
            details: json!({
                "shard_id": shard_id,
                "status": "fail",
                "error": "migration table is not readable",
            }),
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
    if missing.is_empty() {
        ShardObservation {
            passed: true,
            details: json!({
                "shard_id": shard_id,
                "status": "pass",
                "applied_count": present.len(),
            }),
        }
    } else {
        ShardObservation {
            passed: false,
            details: json!({
                "shard_id": shard_id,
                "status": "fail",
                "missing_versions": missing,
            }),
        }
    }
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

#[derive(diesel::QueryableByName)]
struct ReadOnlyRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    transaction_read_only: String,
}

#[derive(diesel::QueryableByName)]
struct RecoveryRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    in_recovery: bool,
}

async fn check_shard_availability(api_state: &HarvestApiState) -> PreflightCheckResult {
    let Ok(pool) = api_state.storage_pool() else {
        return check(
            "shard_availability",
            PreflightStatus::Fail,
            "Harvest storage pool is not configured",
            Some("Install the Harvest storage pool before mounting the management API."),
            Vec::new(),
            json!({ "shards": [] }),
        );
    };

    let mut affected = Vec::new();
    let mut shards = Vec::new();
    for (shard, shard_pool) in pool.iter_shards() {
        let shard_id = shard.as_i32();
        let observation = observe_shard_availability(shard_id, shard_pool).await;
        if !observation.passed {
            affected.push(shard_id);
        }
        shards.push(observation.details);
    }

    let status = if affected.is_empty() {
        PreflightStatus::Pass
    } else {
        PreflightStatus::Fail
    };
    check(
        "shard_availability",
        status,
        if status == PreflightStatus::Pass {
            "every configured shard is readable and writable"
        } else {
            "one or more configured shards are not readable and writable"
        },
        (status == PreflightStatus::Fail).then_some(
            "Fix the affected shard connection or promote a writable primary before deployment.",
        ),
        affected,
        json!({ "shards": shards }),
    )
}

async fn observe_shard_availability(shard_id: i32, shard_pool: &DbPool) -> ShardObservation {
    let Ok(mut conn) = shard_pool.get().await else {
        return ShardObservation {
            passed: false,
            details: json!({
                "shard_id": shard_id,
                "status": "fail",
                "readable": false,
                "writable": false,
                "error": "database connection could not be acquired",
            }),
        };
    };

    let read_only = diesel::sql_query(
        "SELECT current_setting('transaction_read_only') AS transaction_read_only",
    )
    .get_result::<ReadOnlyRow>(&mut conn)
    .await;
    let recovery = diesel::sql_query("SELECT pg_is_in_recovery() AS in_recovery")
        .get_result::<RecoveryRow>(&mut conn)
        .await;
    match (read_only, recovery) {
        (Ok(read_only), Ok(recovery))
            if read_only.transaction_read_only == "off" && !recovery.in_recovery =>
        {
            ShardObservation {
                passed: true,
                details: json!({
                    "shard_id": shard_id,
                    "status": "pass",
                    "readable": true,
                    "writable": true,
                }),
            }
        }
        (Ok(read_only), Ok(recovery)) => ShardObservation {
            passed: false,
            details: json!({
                "shard_id": shard_id,
                "status": "fail",
                "readable": true,
                "writable": false,
                "transaction_read_only": read_only.transaction_read_only,
                "in_recovery": recovery.in_recovery,
            }),
        },
        _ => ShardObservation {
            passed: false,
            details: json!({
                "shard_id": shard_id,
                "status": "fail",
                "readable": false,
                "writable": false,
                "error": "read-only availability probe failed",
            }),
        },
    }
}

fn check_catalog_consistency(api_state: &HarvestApiState) -> PreflightCheckResult {
    let Ok(runtime) = api_state.runtime() else {
        return check(
            "catalog_consistency",
            PreflightStatus::Fail,
            "Harvest runtime catalog is unavailable",
            Some(
                "Start HarvestPlugin so workflow, activity, and DAG registrations can be inspected.",
            ),
            Vec::new(),
            json!({}),
        );
    };

    let mut failures = Vec::new();
    for dag in runtime.dags().values() {
        for task in dag.definition.tasks() {
            if !runtime
                .registry()
                .activities
                .contains_key(&task.activity_name)
            {
                failures.push(format!(
                    "dag '{}' references unregistered activity '{}'",
                    dag.name, task.activity_name
                ));
            }
        }
    }
    for activity in runtime.registry().activities.values() {
        if activity.default_queue == Some("") {
            failures.push(format!(
                "activity '{}' declares an empty default queue",
                activity.name
            ));
        }
    }

    let status = if failures.is_empty() {
        PreflightStatus::Pass
    } else {
        PreflightStatus::Fail
    };
    check(
        "catalog_consistency",
        status,
        if status == PreflightStatus::Pass {
            "registered workflows, activities, and DAGs are internally consistent"
        } else {
            "registered catalog contains unresolved workflow runtime references"
        },
        (status == PreflightStatus::Fail)
            .then_some("Fix the affected registration before starting workflows."),
        Vec::new(),
        json!({
            "workflow_count": runtime.registry().workflows.len(),
            "activity_count": runtime.registry().activities.len(),
            "dag_count": runtime.dags().len(),
            "failures": failures,
        }),
    )
}

async fn check_schedule_resolvability(api_state: &HarvestApiState) -> PreflightCheckResult {
    let Ok(runtime) = api_state.runtime() else {
        return check(
            "schedule_resolvability",
            PreflightStatus::Fail,
            "Harvest runtime catalog is unavailable",
            Some("Start HarvestPlugin so schedules can be resolved against registrations."),
            Vec::new(),
            json!({}),
        );
    };
    let (db_schedules, schedule_read_failures) = load_schedules_from_shards(api_state).await;
    let mut failures = Vec::new();
    let mut affected = schedule_read_failures
        .iter()
        .map(|failure| failure.shard_id)
        .collect::<Vec<_>>();

    for schedule in runtime.workflow_schedules() {
        if !runtime
            .registry()
            .workflows
            .contains_key(&schedule.workflow_name)
        {
            failures.push(json!({
                "kind": "workflow",
                "name": schedule.workflow_name,
                "source": "runtime",
            }));
        }
    }
    for row in &db_schedules {
        if let Some(workflow_name) = row.schedule.workflow_name.as_deref()
            && !runtime.registry().workflows.contains_key(workflow_name)
        {
            affected.push(row.shard.as_i32());
            failures.push(json!({
                "kind": "workflow",
                "name": workflow_name,
                "source": "database",
                "shard_id": row.shard.as_i32(),
            }));
        }
        if let Some(dag_name) = row.schedule.dag_name.as_deref()
            && !runtime.dags().contains_key(dag_name)
        {
            affected.push(row.shard.as_i32());
            failures.push(json!({
                "kind": "dag",
                "name": dag_name,
                "source": "database",
                "shard_id": row.shard.as_i32(),
            }));
        }
    }

    let schedule_count = runtime.workflow_schedules().len() + db_schedules.len();
    let scheduler = runtime.scheduler_snapshot();
    if schedule_count > 0 && !scheduler.running {
        failures.push(json!({
            "kind": "scheduler",
            "name": "scheduler_path",
            "source": "runtime",
        }));
    }

    let status = if failures.is_empty() && schedule_read_failures.is_empty() {
        PreflightStatus::Pass
    } else {
        PreflightStatus::Fail
    };
    check(
        "schedule_resolvability",
        status,
        if status == PreflightStatus::Pass {
            if schedule_count == 0 {
                "no registered schedules require scheduler coverage"
            } else {
                "registered schedules resolve to runtime registrations and scheduler coverage is available"
            }
        } else {
            "one or more registered schedules cannot be resolved or evaluated"
        },
        (status == PreflightStatus::Fail).then_some(
            "Register the missing workflow or DAG, or enable the scheduler path before deployment.",
        ),
        affected,
        json!({
            "runtime_schedule_count": runtime.workflow_schedules().len(),
            "database_schedule_count": db_schedules.len(),
            "scheduler": scheduler,
            "failures": failures,
            "read_failures": schedule_read_failures,
        }),
    )
}

#[derive(Debug, Serialize)]
struct ShardReadFailure {
    shard_id: i32,
    error: &'static str,
}

struct ScheduleShardRow {
    shard: ShardId,
    schedule: HarvestSchedule,
}

async fn load_schedules_from_shards(
    api_state: &HarvestApiState,
) -> (Vec<ScheduleShardRow>, Vec<ShardReadFailure>) {
    let Ok(pool) = api_state.storage_pool() else {
        return (
            Vec::new(),
            vec![ShardReadFailure {
                shard_id: -1,
                error: "storage pool is not configured",
            }],
        );
    };
    let mut schedules = Vec::new();
    let mut failures = Vec::new();
    for (shard, shard_pool) in pool.iter_shards() {
        match shard_pool.get().await {
            Ok(mut conn) => {
                let rows = harvest_schedules::table
                    .select(HarvestSchedule::as_select())
                    .load::<HarvestSchedule>(&mut conn)
                    .await;
                match rows {
                    Ok(rows) => schedules.extend(
                        rows.into_iter()
                            .map(|schedule| ScheduleShardRow { shard, schedule }),
                    ),
                    Err(_) => failures.push(ShardReadFailure {
                        shard_id: shard.as_i32(),
                        error: "schedule table is not readable",
                    }),
                }
            }
            Err(_) => failures.push(ShardReadFailure {
                shard_id: shard.as_i32(),
                error: "database connection could not be acquired",
            }),
        }
    }
    (schedules, failures)
}

async fn check_worker_coverage(api_state: &HarvestApiState) -> PreflightCheckResult {
    let Ok(runtime) = api_state.runtime() else {
        return check(
            "worker_coverage",
            PreflightStatus::Fail,
            "Harvest runtime catalog is unavailable",
            Some("Start HarvestPlugin so required task queues can be inspected."),
            Vec::new(),
            json!({}),
        );
    };
    let Ok(pool) = api_state.storage_pool() else {
        return check(
            "worker_coverage",
            PreflightStatus::Fail,
            "Harvest storage pool is not configured",
            Some("Install the Harvest storage pool so worker liveness can be read."),
            Vec::new(),
            json!({}),
        );
    };

    let mut required_queues = required_queues(&runtime);
    let (db_schedules, _) = load_schedules_from_shards(api_state).await;
    for row in db_schedules {
        if let Some(queue_name) = row.schedule.queue_name {
            required_queues.insert(queue_name);
        }
    }

    if required_queues.is_empty() {
        return check(
            "worker_coverage",
            PreflightStatus::Pass,
            "no runtime queues are referenced by the current Harvest catalog",
            None,
            Vec::new(),
            json!({ "required_queues": [] }),
        );
    }

    let mut observations = Vec::new();
    let mut hard_failures = Vec::new();
    let mut warnings = Vec::new();
    let mut affected = Vec::new();
    let stale_threshold = api_state.worker_stale_threshold();
    for (shard, shard_pool) in pool.iter_shards() {
        let shard_id = shard.as_i32();
        let shard_coverage =
            observe_worker_coverage_shard(shard_id, shard_pool, stale_threshold, &required_queues)
                .await;
        if shard_coverage.affected {
            affected.push(shard_id);
        }
        observations.extend(shard_coverage.observations);
        warnings.extend(shard_coverage.warnings);
        hard_failures.extend(shard_coverage.hard_failures);
    }

    let status = if !hard_failures.is_empty() {
        PreflightStatus::Fail
    } else if !warnings.is_empty() {
        PreflightStatus::Warn
    } else {
        PreflightStatus::Pass
    };
    check(
        "worker_coverage",
        status,
        match status {
            PreflightStatus::Pass => {
                "active fresh worker coverage exists for every referenced queue and shard"
            }
            PreflightStatus::Warn => {
                "worker coverage exists but at least one matching worker is stale or draining"
            }
            PreflightStatus::Fail => "at least one referenced queue has no active worker coverage",
        },
        match status {
            PreflightStatus::Pass => None,
            PreflightStatus::Warn => Some(
                "Restart stale workers or wait for draining workers to be replaced before promotion.",
            ),
            PreflightStatus::Fail => Some(
                "Start at least one active worker for every referenced queue on each affected shard.",
            ),
        },
        affected,
        json!({
            "required_queues": required_queues,
            "observations": observations,
            "warnings": warnings,
            "failures": hard_failures,
        }),
    )
}

struct WorkerCoverageShard {
    affected: bool,
    observations: Vec<Value>,
    warnings: Vec<Value>,
    hard_failures: Vec<Value>,
}

async fn observe_worker_coverage_shard(
    shard_id: i32,
    shard_pool: &DbPool,
    stale_threshold: std::time::Duration,
    required_queues: &BTreeSet<String>,
) -> WorkerCoverageShard {
    let Ok(workers) = list_shard_workers(shard_pool, stale_threshold).await else {
        return WorkerCoverageShard {
            affected: true,
            observations: Vec::new(),
            warnings: Vec::new(),
            hard_failures: vec![json!({
                "shard_id": shard_id,
                "reason": "worker table is not readable",
            })],
        };
    };

    let mut observations = Vec::new();
    let mut warnings = Vec::new();
    let mut hard_failures = Vec::new();
    let mut affected = false;
    for queue in required_queues {
        let matching = workers
            .iter()
            .filter(|worker| worker_can_cover(worker, queue, shard_id))
            .collect::<Vec<_>>();
        if matching.is_empty() {
            affected = true;
            hard_failures.push(json!({
                "queue": queue,
                "shard_id": shard_id,
                "reason": "no active worker registration covers this queue and shard",
            }));
            observations.push(json!({
                "queue": queue,
                "shard_id": shard_id,
                "status": "fail",
            }));
        } else if matching.iter().any(|worker| {
            worker.health == WorkerHealth::Stale
                || worker.worker.status == WorkerStatus::Draining.as_str()
        }) {
            affected = true;
            warnings.push(json!({
                "queue": queue,
                "shard_id": shard_id,
                "reason": "coverage exists but at least one worker is stale or draining",
            }));
            observations.push(json!({
                "queue": queue,
                "shard_id": shard_id,
                "status": "warn",
            }));
        } else {
            observations.push(json!({
                "queue": queue,
                "shard_id": shard_id,
                "status": "pass",
            }));
        }
    }

    WorkerCoverageShard {
        affected,
        observations,
        warnings,
        hard_failures,
    }
}

fn required_queues(runtime: &crate::api::HarvestApiRuntime) -> BTreeSet<String> {
    let mut queues = runtime.queues().iter().cloned().collect::<BTreeSet<_>>();
    if !runtime.registry().workflows.is_empty() {
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
    queues.retain(|queue| !queue.trim().is_empty());
    queues
}

async fn list_shard_workers(
    pool: &DbPool,
    stale_threshold: std::time::Duration,
) -> Result<Vec<WorkerRow>, ()> {
    let mut conn = pool.get().await.map_err(|_| ())?;
    let filters = WorkerFilters {
        limit: i64::MAX,
        ..WorkerFilters::new()
    };
    list_workers(&mut conn, &filters, stale_threshold)
        .await
        .map_err(|_| ())
}

fn worker_can_cover(worker: &WorkerRow, queue: &str, shard_id: i32) -> bool {
    if worker.worker.status == WorkerStatus::Stopped.as_str() {
        return false;
    }
    let has_queue = worker
        .worker
        .queues
        .as_array()
        .is_some_and(|queues| queues.iter().any(|value| value.as_str() == Some(queue)));
    let has_shard = worker
        .worker
        .shard_assignments
        .as_array()
        .is_some_and(|shards| {
            shards
                .iter()
                .any(|value| value.as_i64() == Some(i64::from(shard_id)))
        });
    has_queue && has_shard
}

async fn check_dlq_read_access(api_state: &HarvestApiState) -> PreflightCheckResult {
    let Ok(pool) = api_state.storage_pool() else {
        return check(
            "dlq_read_access",
            PreflightStatus::Fail,
            "Harvest storage pool is not configured",
            Some("Install the Harvest storage pool so DLQ visibility can be checked."),
            Vec::new(),
            json!({}),
        );
    };

    let mut affected = Vec::new();
    let mut shards = Vec::new();
    for (shard, shard_pool) in pool.iter_shards() {
        let shard_id = shard.as_i32();
        let observation = observe_dlq_read_access(shard_id, shard_pool).await;
        if !observation.passed {
            affected.push(shard_id);
        }
        shards.push(observation.details);
    }
    let status = if affected.is_empty() {
        PreflightStatus::Pass
    } else {
        PreflightStatus::Fail
    };
    check(
        "dlq_read_access",
        status,
        if status == PreflightStatus::Pass {
            "dead-letter queue read access is available on every configured shard"
        } else {
            "dead-letter queue read access failed on one or more shards"
        },
        (status == PreflightStatus::Fail).then_some(
            "Apply Harvest migrations and verify database permissions on affected shards.",
        ),
        affected,
        json!({ "shards": shards }),
    )
}

async fn observe_dlq_read_access(shard_id: i32, shard_pool: &DbPool) -> ShardObservation {
    let Ok(mut conn) = shard_pool.get().await else {
        return ShardObservation {
            passed: false,
            details: json!({
                "shard_id": shard_id,
                "status": "fail",
                "error": "database connection could not be acquired",
            }),
        };
    };

    dlq::dead_letter_count(&mut conn).await.map_or_else(
        |_| ShardObservation {
            passed: false,
            details: json!({
                "shard_id": shard_id,
                "status": "fail",
                "error": "dead-letter table is not readable",
            }),
        },
        |count| ShardObservation {
            passed: true,
            details: json!({
                "shard_id": shard_id,
                "status": "pass",
                "dead_letter_count": count,
            }),
        },
    )
}

fn check_retention_visibility(api_state: &HarvestApiState) -> PreflightCheckResult {
    api_state.runtime().map_or_else(
        |_| {
            check(
                "retention_visibility",
                PreflightStatus::Fail,
                "retention configuration is not visible because the runtime is unavailable",
                Some("Start HarvestPlugin so retention configuration can be inspected."),
                Vec::new(),
                json!({}),
            )
        },
        |runtime| {
            check(
                "retention_visibility",
                PreflightStatus::Pass,
                "retention configuration is visible to the management API",
                None,
                Vec::new(),
                json!({ "config": runtime.retention_config() }),
            )
        },
    )
}

fn check_admin_auth_boundary(api_state: &HarvestApiState) -> PreflightCheckResult {
    let profile = api_state.deployment_profile();
    let has_boundary = api_state.admin_auth_boundary();
    let is_dev = profile == "dev";
    let status = if is_dev || has_boundary {
        PreflightStatus::Pass
    } else if profile == "unknown" {
        PreflightStatus::Warn
    } else {
        PreflightStatus::Fail
    };

    check(
        "admin_auth_boundary",
        status,
        match status {
            PreflightStatus::Pass if is_dev => {
                "admin API auth boundary is optional for the dev profile"
            }
            PreflightStatus::Pass => "admin API is mounted with an auth boundary",
            PreflightStatus::Warn => {
                "admin API auth boundary cannot be confirmed because the deployment profile is unknown"
            }
            PreflightStatus::Fail => {
                "admin API is mounted without an auth boundary in a non-dev profile"
            }
        },
        match status {
            PreflightStatus::Pass => None,
            PreflightStatus::Warn => {
                Some("Set the deployment profile or mark the admin auth boundary explicitly.")
            }
            PreflightStatus::Fail => Some(
                "Use HarvestPlugin::api_with_auth or mount equivalent middleware before the Harvest admin API.",
            ),
        },
        Vec::new(),
        json!({
            "profile": profile,
            "auth_boundary_present": has_boundary,
        }),
    )
}
