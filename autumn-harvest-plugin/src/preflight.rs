//! Read-only deployment preflight checks for the Harvest management API.

use std::collections::{BTreeMap, BTreeSet, HashSet};

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
    checks.push(check_history_ceiling_config(api_state));

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

const HARVEST_WRITE_PRIVILEGE_REQUIREMENTS: &[(&str, &[&str])] = &[
    (
        "harvest_workflow_executions",
        &["SELECT", "INSERT", "UPDATE", "DELETE"],
    ),
    ("harvest_events", &["SELECT", "INSERT"]),
    ("harvest_task_queue", &["SELECT", "INSERT", "UPDATE"]),
    ("harvest_schedules", &["SELECT", "INSERT", "UPDATE"]),
    ("harvest_signals", &["SELECT", "INSERT", "UPDATE"]),
    ("harvest_timers", &["SELECT", "INSERT", "UPDATE", "DELETE"]),
    ("harvest_dead_letters", &["SELECT", "INSERT", "DELETE"]),
    ("harvest_external_tasks", &["SELECT", "INSERT", "UPDATE"]),
    ("harvest_workers", &["SELECT", "INSERT", "UPDATE"]),
    ("harvest_batch_jobs", &["SELECT", "INSERT", "UPDATE"]),
    ("harvest_audit_log", &["SELECT", "INSERT", "DELETE"]),
];

const HARVEST_SEQUENCE_PRIVILEGE_REQUIREMENTS: &[(&str, &[&str])] =
    &[("harvest_events_id_seq", &["USAGE"])];

#[derive(diesel::QueryableByName, Debug, Clone)]
struct TablePrivilegeRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    table_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    privilege: String,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct MissingWritePrivilege {
    table: String,
    privileges: Vec<String>,
}

#[derive(diesel::QueryableByName, Debug, Clone)]
struct SequencePrivilegeRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    sequence_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    privilege: String,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct MissingSequencePrivilege {
    sequence: String,
    privileges: Vec<String>,
}

fn harvest_write_privilege_query() -> String {
    let mut values = String::new();
    for (table, privileges) in HARVEST_WRITE_PRIVILEGE_REQUIREMENTS {
        for privilege in *privileges {
            if !values.is_empty() {
                values.push_str(", ");
            }
            values.push_str("('");
            values.push_str(table);
            values.push_str("', '");
            values.push_str(privilege);
            values.push_str("')");
        }
    }

    format!(
        "WITH required(table_name, privilege) AS (VALUES {values}) \
         SELECT table_name::TEXT AS table_name, \
                privilege::TEXT AS privilege, \
                COALESCE( \
                    has_table_privilege( \
                        to_regclass(table_name::TEXT), \
                        privilege::TEXT \
                    ), \
                    false \
                ) AS granted \
         FROM required \
         ORDER BY table_name, privilege"
    )
}

fn harvest_sequence_privilege_query() -> String {
    let mut values = String::new();
    for (sequence, privileges) in HARVEST_SEQUENCE_PRIVILEGE_REQUIREMENTS {
        for privilege in *privileges {
            if !values.is_empty() {
                values.push_str(", ");
            }
            values.push_str("('");
            values.push_str(sequence);
            values.push_str("', '");
            values.push_str(privilege);
            values.push_str("')");
        }
    }

    format!(
        "WITH required(sequence_name, privilege) AS (VALUES {values}) \
         SELECT sequence_name::TEXT AS sequence_name, \
                privilege::TEXT AS privilege, \
                COALESCE( \
                    has_sequence_privilege( \
                        to_regclass(sequence_name::TEXT), \
                        privilege::TEXT \
                    ), \
                    false \
                ) AS granted \
         FROM required \
         ORDER BY sequence_name, privilege"
    )
}

fn missing_write_privileges(rows: Vec<TablePrivilegeRow>) -> Vec<MissingWritePrivilege> {
    let mut by_table = BTreeMap::<String, Vec<String>>::new();
    for row in rows {
        if !row.granted {
            by_table
                .entry(row.table_name)
                .or_default()
                .push(row.privilege);
        }
    }
    for privileges in by_table.values_mut() {
        privileges.sort();
    }
    by_table
        .into_iter()
        .map(|(table, privileges)| MissingWritePrivilege { table, privileges })
        .collect()
}

fn missing_sequence_privileges(rows: Vec<SequencePrivilegeRow>) -> Vec<MissingSequencePrivilege> {
    let mut by_sequence = BTreeMap::<String, Vec<String>>::new();
    for row in rows {
        if !row.granted {
            by_sequence
                .entry(row.sequence_name)
                .or_default()
                .push(row.privilege);
        }
    }
    for privileges in by_sequence.values_mut() {
        privileges.sort();
    }
    by_sequence
        .into_iter()
        .map(|(sequence, privileges)| MissingSequencePrivilege {
            sequence,
            privileges,
        })
        .collect()
}

async fn missing_harvest_write_privileges(
    conn: &mut diesel_async::AsyncPgConnection,
) -> Result<Vec<MissingWritePrivilege>, diesel::result::Error> {
    let rows = diesel::sql_query(harvest_write_privilege_query())
        .load::<TablePrivilegeRow>(conn)
        .await?;
    Ok(missing_write_privileges(rows))
}

async fn missing_harvest_sequence_privileges(
    conn: &mut diesel_async::AsyncPgConnection,
) -> Result<Vec<MissingSequencePrivilege>, diesel::result::Error> {
    let rows = diesel::sql_query(harvest_sequence_privilege_query())
        .load::<SequencePrivilegeRow>(conn)
        .await?;
    Ok(missing_sequence_privileges(rows))
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
            "every configured shard is readable and writable by the Harvest role"
        } else {
            "one or more configured shards are not readable and writable by the Harvest role"
        },
        (status == PreflightStatus::Fail).then_some(
            "Fix the affected shard connection, promote a writable primary, or grant the Harvest role required table and sequence privileges before deployment.",
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
        (Ok(read_only), Ok(recovery)) => {
            let missing_write_privileges = missing_harvest_write_privileges(&mut conn).await;
            let missing_sequence_privileges = missing_harvest_sequence_privileges(&mut conn).await;
            match (missing_write_privileges, missing_sequence_privileges) {
                (Ok(missing_write_privileges), Ok(missing_sequence_privileges))
                    if read_only.transaction_read_only == "off"
                        && !recovery.in_recovery
                        && missing_write_privileges.is_empty()
                        && missing_sequence_privileges.is_empty() =>
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
                (Ok(missing_write_privileges), Ok(missing_sequence_privileges)) => {
                    ShardObservation {
                        passed: false,
                        details: json!({
                            "shard_id": shard_id,
                            "status": "fail",
                            "readable": true,
                            "writable": false,
                            "transaction_read_only": read_only.transaction_read_only,
                            "in_recovery": recovery.in_recovery,
                            "missing_write_privileges": missing_write_privileges,
                            "missing_sequence_privileges": missing_sequence_privileges,
                        }),
                    }
                }
                (Err(_), _) => ShardObservation {
                    passed: false,
                    details: json!({
                        "shard_id": shard_id,
                        "status": "fail",
                        "readable": true,
                        "writable": false,
                        "transaction_read_only": read_only.transaction_read_only,
                        "in_recovery": recovery.in_recovery,
                        "error": "write privilege probe failed",
                    }),
                },
                (_, Err(_)) => ShardObservation {
                    passed: false,
                    details: json!({
                        "shard_id": shard_id,
                        "status": "fail",
                        "readable": true,
                        "writable": false,
                        "transaction_read_only": read_only.transaction_read_only,
                        "in_recovery": recovery.in_recovery,
                        "error": "sequence privilege probe failed",
                    }),
                },
            }
        }
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

/// Collect preflight failures for DAG task references to unregistered
/// activities.
fn dag_unregistered_activity_failures<'a>(
    dags: impl IntoIterator<Item = (&'a str, &'a [autumn_harvest::DagTask])>,
    is_registered_activity: impl Fn(&str) -> bool,
) -> Vec<String> {
    let mut failures = Vec::new();
    for (dag_name, tasks) in dags {
        for task in tasks {
            // A signal/timer gate (issue #746) stores its *signal* name in
            // `activity_name` but dispatches no activity, so its identifier must
            // not be validated against the activity catalog — otherwise a valid
            // signal-gate DAG false-fails preflight before rollout. Mirror of the
            // builder's `validate_dags_do_not_use_local_activities` gate skip.
            if task.signal.is_some() {
                continue;
            }
            if !is_registered_activity(&task.activity_name) {
                failures.push(format!(
                    "dag '{dag_name}' references unregistered activity '{}'",
                    task.activity_name
                ));
            }
        }
    }
    failures
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

    let mut failures = dag_unregistered_activity_failures(
        runtime
            .dags()
            .values()
            .map(|dag| (dag.name.as_str(), dag.definition.tasks())),
        |name| runtime.registry().activities.contains_key(name),
    );
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
            && !runtime.is_registered_dag(dag_name)
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
            PreflightStatus::Fail => {
                "at least one referenced queue has no healthy active worker coverage"
            }
        },
        match status {
            PreflightStatus::Pass => None,
            PreflightStatus::Warn => Some(
                "Restart stale workers or wait for draining workers to be replaced before promotion.",
            ),
            PreflightStatus::Fail => Some(
                "Start at least one healthy active worker for every referenced queue on each affected shard.",
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
        let has_healthy_active_worker = matching.iter().any(|worker| {
            worker.health == WorkerHealth::Healthy
                && worker.worker.status == WorkerStatus::Active.as_str()
        });
        if !has_healthy_active_worker {
            affected = true;
            hard_failures.push(json!({
                "queue": queue,
                "shard_id": shard_id,
                "reason": "no healthy active worker registration covers this queue and shard",
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
    if !runtime.registry().workflows.is_empty() && queues.is_empty() {
        queues.insert("default".to_string());
    }
    for activity in runtime.registry().activities.values() {
        // Worker sessions (issue #606): the two reserved internal
        // activities (`__harvest_session_acquire`/`__harvest_session_release`)
        // are always registered so the enqueue-time handler lookup never
        // hard-fails on them, but they're dispatched on the *caller-supplied*
        // session queue at the point `create_session`/`Session::complete` is
        // called, never on `default_queue`. Counting them here would
        // spuriously report a deployment with no session-based workflow at
        // all as requiring a worker on `"default"`.
        if !activity.is_local && !autumn_harvest::is_reserved_session_activity_name(activity.name) {
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

fn check_history_ceiling_config(api_state: &HarvestApiState) -> PreflightCheckResult {
    let ceiling = api_state.max_workflow_history_events();
    let soft_threshold = api_state
        .runtime()
        .ok()
        .map(|rt| rt.registry().history_policy().continue_as_new_threshold());

    match (ceiling, soft_threshold) {
        (Some(ceiling), Some(threshold)) => check(
            "history_ceiling_config",
            PreflightStatus::Pass,
            "hard history event ceiling is configured and validated above the soft threshold",
            None,
            Vec::new(),
            json!({
                "ceiling_enabled": true,
                "max_workflow_history_events": ceiling,
                "continue_as_new_threshold": threshold,
                "headroom": ceiling.saturating_sub(threshold),
            }),
        ),
        (Some(ceiling), None) => check(
            "history_ceiling_config",
            PreflightStatus::Pass,
            "hard history event ceiling is configured",
            None,
            Vec::new(),
            json!({
                "ceiling_enabled": true,
                "max_workflow_history_events": ceiling,
                "continue_as_new_threshold": null,
            }),
        ),
        (None, Some(threshold)) => check(
            "history_ceiling_config",
            PreflightStatus::Pass,
            "history ceiling is disabled; soft continue-as-new threshold is the only guard",
            None,
            Vec::new(),
            json!({
                "ceiling_enabled": false,
                "continue_as_new_threshold": threshold,
                "note": "set max_workflow_history_events on HarvestBuilder to enable the hard ceiling",
            }),
        ),
        (None, None) => check(
            "history_ceiling_config",
            PreflightStatus::Pass,
            "history ceiling is disabled and no runtime is available to inspect the soft threshold",
            None,
            Vec::new(),
            json!({
                "ceiling_enabled": false,
                "continue_as_new_threshold": null,
            }),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn privilege_row(table_name: &str, privilege: &str, granted: bool) -> TablePrivilegeRow {
        TablePrivilegeRow {
            table_name: table_name.to_string(),
            privilege: privilege.to_string(),
            granted,
        }
    }

    fn sequence_privilege_row(
        sequence_name: &str,
        privilege: &str,
        granted: bool,
    ) -> SequencePrivilegeRow {
        SequencePrivilegeRow {
            sequence_name: sequence_name.to_string(),
            privilege: privilege.to_string(),
            granted,
        }
    }

    #[test]
    fn missing_write_privileges_groups_denied_privileges_by_table() {
        let missing = missing_write_privileges(vec![
            privilege_row("harvest_task_queue", "INSERT", true),
            privilege_row("harvest_task_queue", "UPDATE", false),
            privilege_row("harvest_dead_letters", "INSERT", false),
            privilege_row("harvest_dead_letters", "DELETE", false),
        ]);

        assert_eq!(
            missing,
            vec![
                MissingWritePrivilege {
                    table: "harvest_dead_letters".to_string(),
                    privileges: vec!["DELETE".to_string(), "INSERT".to_string()],
                },
                MissingWritePrivilege {
                    table: "harvest_task_queue".to_string(),
                    privileges: vec!["UPDATE".to_string()],
                },
            ]
        );
    }

    #[test]
    fn missing_write_privileges_is_empty_when_every_probe_is_granted() {
        let missing = missing_write_privileges(vec![
            privilege_row("harvest_task_queue", "INSERT", true),
            privilege_row("harvest_task_queue", "UPDATE", true),
        ]);

        assert!(missing.is_empty());
    }

    #[test]
    fn write_privilege_query_checks_harvest_runtime_write_tables() {
        let sql = harvest_write_privilege_query();

        assert!(sql.contains("has_table_privilege"));
        assert!(sql.contains("harvest_workflow_executions"));
        assert!(sql.contains("harvest_task_queue"));
        assert!(sql.contains("harvest_dead_letters"));
        assert!(sql.contains("harvest_workers"));
        assert!(sql.contains("INSERT"));
        assert!(sql.contains("UPDATE"));
        assert!(sql.contains("DELETE"));
    }

    #[test]
    fn every_harvest_runtime_table_requires_select_privilege() {
        for (table, privileges) in HARVEST_WRITE_PRIVILEGE_REQUIREMENTS {
            assert!(
                privileges.contains(&"SELECT"),
                "{table} must require SELECT so readable/writable preflight matches runtime access"
            );
        }
    }

    #[test]
    fn missing_sequence_privileges_groups_denied_privileges_by_sequence() {
        let missing = missing_sequence_privileges(vec![
            sequence_privilege_row("harvest_events_id_seq", "USAGE", false),
            sequence_privilege_row("harvest_events_id_seq", "SELECT", true),
        ]);

        assert_eq!(
            missing,
            vec![MissingSequencePrivilege {
                sequence: "harvest_events_id_seq".to_string(),
                privileges: vec!["USAGE".to_string()],
            }]
        );
    }

    #[test]
    fn missing_sequence_privileges_is_empty_when_every_probe_is_granted() {
        let missing = missing_sequence_privileges(vec![sequence_privilege_row(
            "harvest_events_id_seq",
            "USAGE",
            true,
        )]);

        assert!(missing.is_empty());
    }

    #[test]
    fn sequence_privilege_query_checks_harvest_event_id_sequence() {
        let sql = harvest_sequence_privilege_query();

        assert!(sql.contains("has_sequence_privilege"));
        assert!(sql.contains("harvest_events_id_seq"));
        assert!(sql.contains("USAGE"));
    }

    fn activity_task(name: &str) -> autumn_harvest::DagTask {
        autumn_harvest::DagTask {
            activity_name: name.to_string(),
            upstreams: Vec::new(),
            trigger_rule: autumn_harvest::TriggerRule::AllSuccess,
            retry_policy: None,
            start_to_close: None,
            queue: None,
            map_upstream: None,
            map_failure_policy: autumn_harvest::MapFailurePolicy::FailFast,
            condition: None,
            signal: None,
            input_from: None,
        }
    }

    fn signal_gate_task(signal_name: &str) -> autumn_harvest::DagTask {
        autumn_harvest::DagTask {
            signal: Some(autumn_harvest::DagSignalGate {
                signal_name: signal_name.to_string(),
                timeout: None,
                on_timeout: autumn_harvest::GateTimeoutAction::FailRun,
            }),
            ..activity_task(signal_name)
        }
    }

    #[test]
    fn catalog_check_skips_signal_gate_nodes() {
        // A signal/timer gate (issue #746) stores its *signal* name in
        // `activity_name` but dispatches no activity, so preflight must not
        // report the gate identifier as an unregistered activity.
        let registered: HashSet<&str> = ["extract", "load"].into_iter().collect();
        let tasks = vec![
            activity_task("extract"),
            signal_gate_task("approval"), // signal name is NOT a registered activity
            activity_task("load"),
        ];

        let failures = dag_unregistered_activity_failures(
            std::iter::once(("etl_with_gate", tasks.as_slice())),
            |name| registered.contains(name),
        );

        assert!(
            failures.is_empty(),
            "signal gate node must not be flagged as an unregistered activity: {failures:?}"
        );
    }

    #[test]
    fn catalog_check_still_flags_genuinely_unregistered_activities() {
        let registered: HashSet<&str> = std::iter::once("extract").collect();
        let tasks = vec![
            activity_task("extract"),
            signal_gate_task("approval"),
            activity_task("missing_activity"),
        ];

        let failures = dag_unregistered_activity_failures(
            std::iter::once(("etl_with_gate", tasks.as_slice())),
            |name| registered.contains(name),
        );

        assert_eq!(
            failures,
            vec![
                "dag 'etl_with_gate' references unregistered activity 'missing_activity'"
                    .to_string()
            ]
        );
    }
}
