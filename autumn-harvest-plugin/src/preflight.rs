//! Read-only deployment preflight checks for the Harvest management API.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use autumn_harvest::dlq;
use autumn_harvest::models::HarvestSchedule;
use autumn_harvest::scanner_health::{
    ScannerLivenessVerdict, ScannerStatus, classify_scanner, global_scanner_liveness,
    staleness_threshold,
};
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

/// Wall-clock budget for the database-dependent preflight checks as a group.
///
/// Deliberately generous: these run several queries fanned across shards, and
/// the budget exists to bound a *pathological* stall, not to police a slow but
/// working database.
///
/// Without it the whole endpoint can hang indefinitely. Every DB check begins
/// with `pool.get().await`, and deadpool waits for a free connection with no
/// timeout by default, so an exhausted pool blocks the request forever. That
/// matters most for `scanner_liveness` (issue #797): pool exhaustion is a
/// listed cause of a wedged control loop, so the one diagnostic that would
/// name the stalled loop must not be suppressed by the very condition it is
/// reporting on.
const DB_CHECK_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// Build a read-only deployment preflight report.
///
/// The in-memory checks are **always** present in the report; the
/// database-dependent ones are bounded by [`DB_CHECK_BUDGET`] as a group. On
/// the happy path the returned checks are identical, and in the same order, as
/// if they had been awaited inline.
pub async fn build_preflight_report(api_state: &HarvestApiState) -> PreflightReport {
    // Evaluated up front, before anything can block: a wedged database must
    // never be able to hide these. All five read configuration or registry
    // state that does not age while the DB group runs, so sampling them here
    // costs nothing in freshness.
    let api_reachability = check_api_reachability(api_state);
    let catalog_consistency = check_catalog_consistency(api_state);
    let retention_visibility = check_retention_visibility(api_state);
    let admin_auth_boundary = check_admin_auth_boundary(api_state);
    let history_ceiling_config = check_history_ceiling_config(api_state);

    let db_checks = tokio::time::timeout(DB_CHECK_BUDGET, async {
        (
            check_migrations(api_state).await,
            check_shard_availability(api_state).await,
            check_schedule_resolvability(api_state).await,
            check_worker_coverage(api_state).await,
            check_dlq_read_access(api_state).await,
        )
    })
    .await;

    // Sampled AFTER the bounded await, unlike the checks above (issue #797).
    //
    // Scanner liveness is the one in-memory check whose answer *ages*: it is a
    // function of wall-clock time since each loop's last tick. Sampling it
    // before a wait that can legitimately consume the full DB_CHECK_BUDGET
    // would let a loop cross its staleness threshold *during* that wait and
    // still be reported healthy in a report whose `observed_at` is stamped
    // afterwards -- so the first report an operator pulls would omit the very
    // loop that just stalled. That is most likely under pool exhaustion, which
    // is both a listed cause of a wedged loop and the reason the DB group is
    // slow, so the two conditions co-occur precisely when the answer matters.
    //
    // This does not weaken the anti-suppression guarantee that put the check
    // outside the DB group in the first place: `check_scanner_liveness` is
    // synchronous and touches no pool, and `tokio::time::timeout` always
    // returns within the budget, so this line is reached whether the DB checks
    // completed, errored, or timed out.
    let scanner_liveness = check_scanner_liveness();

    let mut checks = Vec::new();
    checks.push(api_reachability);
    if let Ok((migrations, shards, schedules, workers, dlq)) = db_checks {
        checks.push(migrations);
        checks.push(shards);
        checks.push(catalog_consistency);
        checks.push(schedules);
        checks.push(workers);
        checks.push(dlq);
    } else {
        checks.push(db_checks_timed_out());
        checks.push(catalog_consistency);
    }
    checks.push(retention_visibility);
    checks.push(admin_auth_boundary);
    checks.push(history_ceiling_config);
    checks.push(scanner_liveness);

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

/// Stand-in emitted when the database-dependent checks exceed
/// [`DB_CHECK_BUDGET`] as a group.
///
/// Returning a `fail` check is strictly better than hanging: the report still
/// reaches the operator carrying every in-memory verdict — including
/// `scanner_liveness`, which is what names the stalled loop.
fn db_checks_timed_out() -> PreflightCheckResult {
    check(
        "db_checks",
        PreflightStatus::Fail,
        format!(
            "database-dependent preflight checks did not complete within {}s",
            DB_CHECK_BUDGET.as_secs()
        ),
        Some(
            "The connection pool is likely exhausted or the database is \
             unreachable. Check the scanner_liveness entry in this same report \
             — a wedged control loop holding connections is a common cause — \
             then inspect pool saturation and long-running queries.",
        ),
        Vec::new(),
        json!({ "budget_secs": DB_CHECK_BUDGET.as_secs() }),
    )
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
            // A node's compensator (issue #780) is dispatched through the same
            // DAG activity-queue lowering on the terminal-failure unwind, so an
            // unregistered compensator must be flagged BEFORE rollout —
            // otherwise the miss only surfaces mid-unwind, exactly when the
            // state is already dangling.
            if let Some(compensate) = &task.compensate
                && !is_registered_activity(compensate)
            {
                failures.push(format!(
                    "dag '{dag_name}' references unregistered compensator '{compensate}' for task '{}'",
                    task.activity_name
                ));
            }
        }
    }
    failures
}

/// Collect preflight failures for a workflow's **opt-in** declared dependencies
/// (issue #802).
///
/// A workflow that never opted in carries `None` on both fields, and `None`
/// yields an empty name list to walk — that is the zero-false-positive guarantee
/// (AC4): an unopted-in workflow can never contribute a failure, regardless of
/// what is registered. `Some(&[])` is distinct from `None` on the wire and at
/// the macro layer, but resolves identically here: an explicit "this workflow
/// depends on nothing" has nothing to check. The up-front `filter` is a
/// short-circuit for the common all-legacy catalog, not the guarantee itself.
///
/// Ordering is imposed here rather than inherited: `registry().workflows` is a
/// `HashMap`, so without sorting the operator-facing `details.failures` list
/// would reshuffle between calls. Workflows are sorted by name; within a
/// workflow, activities are reported before children, each in declaration order.
/// Duplicate declarations report their miss once.
fn workflow_unregistered_dependency_failures<'a>(
    workflows: impl IntoIterator<Item = &'a autumn_harvest::WorkflowInfo>,
    is_registered_activity: impl Fn(&str) -> bool,
    is_registered_workflow: impl Fn(&str) -> bool,
) -> Vec<String> {
    // Only workflows that actually opted in can contribute, so filter before
    // sorting — an all-legacy catalog does no work beyond the scan.
    let mut opted_in: Vec<&autumn_harvest::WorkflowInfo> = workflows
        .into_iter()
        .filter(|info| info.declared_activities.is_some() || info.declared_children.is_some())
        .collect();
    opted_in.sort_by_key(|info| info.name);

    let mut failures = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for info in opted_in {
        let workflow = info.name;
        // Activities and children resolve against separate catalogs but are
        // otherwise identical, so walk them through one loop keyed by the
        // catalog predicate and the noun used in the operator-facing message.
        for (declared, kind, is_registered) in [
            (
                info.declared_activities,
                "activity",
                &is_registered_activity as &dyn Fn(&str) -> bool,
            ),
            (
                info.declared_children,
                "child workflow",
                &is_registered_workflow as &dyn Fn(&str) -> bool,
            ),
        ] {
            for name in declared.unwrap_or_default() {
                let failure = if name.trim().is_empty() {
                    // The macro rejects a blank name at compile time, so this is
                    // only reachable through the fluent builder — report it as
                    // the authoring error it is, not as "unregistered ''".
                    format!("workflow '{workflow}' declares an empty {kind} name")
                } else if is_registered(name) {
                    continue;
                } else {
                    format!("workflow '{workflow}' references unregistered {kind} '{name}'")
                };
                // A duplicate declaration is an authoring smell, not an error —
                // the operator should still see each miss exactly once.
                if seen.insert(failure.clone()) {
                    failures.push(failure);
                }
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
    // Issue #802: opt-in declared dependencies. A workflow that never declared
    // any contributes nothing, so this is a pure addition for existing catalogs.
    failures.extend(workflow_unregistered_dependency_failures(
        runtime.registry().workflows.values(),
        |name| runtime.registry().activities.contains_key(name),
        |name| runtime.registry().workflows.contains_key(name),
    ));
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
        // Names both branches: preflight does not read workflow bodies, so it
        // cannot tell "you forgot to register the handler" (a runtime hazard)
        // from "you left a stale declaration behind" (cosmetic). Failing closed
        // is correct, but the operator needs to know the fix is one line either
        // way — the generic "fix the registration" wording named only the first.
        (status == PreflightStatus::Fail).then_some(
            "Register the named handler in activities![…] / workflows![…], or — if the \
             workflow no longer references it — delete the stale entry from its declared \
             dependencies.",
        ),
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
        // Synthetic liveness canary (issue #796): the built-in canary activity
        // has `default_queue: None` but is always dispatched on the probe's
        // *target* queue (carried in the workflow input), never on
        // `default_queue`. Counting it here would inject a phantom `"default"`
        // required queue, flipping a healthy non-default-queue deployment RED.
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

/// Liveness of the background control loops running in this process
/// (issue #797).
///
/// Every loop registers itself at spawn time and ticks at the end of each
/// iteration, so a loop that panicked, deadlocked, or stalled on a
/// never-returning query stops advancing its timestamp while everything else
/// in the process keeps working. A scanner that has not ticked within
/// `max(2 × poll_interval, 60s)` is `warn`; past twice that it is `fail`.
///
/// Reads the in-process registry directly — deliberately **not** gated on a
/// metrics recorder being configured, so the check works on a deployment with
/// no telemetry pipeline at all.
fn check_scanner_liveness() -> PreflightCheckResult {
    scanner_liveness_check(&global_scanner_liveness().snapshot())
}

/// Pure classification half of [`check_scanner_liveness`], split out so the
/// warn/fail policy is unit-testable without a live runtime or the
/// process-global registry.
fn scanner_liveness_check(statuses: &[ScannerStatus]) -> PreflightCheckResult {
    let mut worst = PreflightStatus::Pass;
    let mut stale_scanners = Vec::new();
    // Human-facing labels for the summary line, carrying the wedged instance's
    // shard when there is one. Kept separate from `stale_scanners` so that
    // field stays a stable, machine-readable list of bare scanner names.
    let mut stale_labels = Vec::new();
    // The standard `affected_shards` contract field: the shards of every stale
    // instance. `check()` sorts and dedups it, and the CLI's SCOPE column reads
    // exactly this field, so a per-shard wedge must land here and not only in
    // the summary and details.
    let mut affected_shards = Vec::new();
    let mut entries = Vec::with_capacity(statuses.len());

    for status in statuses {
        let verdict = classify_scanner(status);
        let (verdict_label, verdict_status) = match verdict {
            ScannerLivenessVerdict::Healthy => ("healthy", PreflightStatus::Pass),
            ScannerLivenessVerdict::Stale => ("stale", PreflightStatus::Warn),
            ScannerLivenessVerdict::Wedged => ("wedged", PreflightStatus::Fail),
        };
        if verdict_status.rank() > worst.rank() {
            worst = verdict_status;
        }
        if verdict != ScannerLivenessVerdict::Healthy {
            stale_scanners.push(status.scanner.as_str());
            // Name the shard in the summary when the wedged instance is one of
            // the per-shard loops (issue #797): a multi-shard worker runs N
            // timeout checkers under one label, so "timeout is wedged" alone
            // does not tell an operator which database is unprotected.
            stale_labels.push(status.shard.map_or_else(
                || status.scanner.as_str().to_owned(),
                |shard| format!("{} (shard {})", status.scanner.as_str(), shard.as_i32()),
            ));
            // EVERY stale instance's shard, not just the worst one's: the
            // worst-instance fold picks one owner for the verdict, so pushing
            // only `status.shard` would report shard 1 while shard 2 was
            // equally unprotected -- an understated blast radius on the exact
            // surface an operator uses to decide what to restart.
            affected_shards.extend(status.stale_shards.iter().copied().map(ShardId::as_i32));
        }
        entries.push(json!({
            "scanner": status.scanner.as_str(),
            // The worst instance's shard, so an operator can go straight to the
            // unprotected database. Absent for the process-wide loops
            // (retention, schedule) and on single-shard deployments.
            "shard": status.shard.map(ShardId::as_i32),
            "verdict": verdict_label,
            "tick_count": status.tick_count,
            "has_ticked": status.has_ticked,
            "age_secs": status.age.as_secs(),
            "poll_interval_secs": status.poll_interval.as_secs(),
            "staleness_threshold_secs": staleness_threshold(status.poll_interval).as_secs(),
            "last_tick_at": status.last_tick_at,
        }));
    }

    let summary = if statuses.is_empty() {
        // Two legitimate shapes reach here: an API-only replica that spawns no
        // control loops, and a process that gracefully drained its worker
        // while continuing to serve HTTP (a clean stop deregisters). Reporting
        // seven phantom wedged scanners in either case would be pure alarm
        // noise, so an empty registry passes with an explanation.
        "no background control loops are registered in this process".to_owned()
    } else if stale_scanners.is_empty() {
        format!(
            "all {} background control loops ticked within their staleness threshold",
            statuses.len()
        )
    } else {
        format!(
            "{} of {} background control loops are stale: {}",
            stale_labels.len(),
            statuses.len(),
            stale_labels.join(", ")
        )
    };

    check(
        "scanner_liveness",
        worst,
        summary,
        if stale_scanners.is_empty() {
            None
        } else {
            Some(
                "A background control loop has stopped ticking: enforcement, SLA, reclaim, \
                 outbox, retention, schedule, or auto-resume work is silently not running. \
                 Check the worker logs for a panic or a stalled query and restart the worker \
                 process; see docs/runbooks/harvest-alerts.md#harvest_scanner_stalled.",
            )
        },
        affected_shards,
        json!({
            "scanners_registered": statuses.len(),
            "stale_scanners": stale_scanners,
            "scanners": entries,
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
    use std::time::Duration;

    use autumn_harvest::scanner_health::Scanner;

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

    // ── scanner_liveness (issue #797) ────────────────────────────────────

    fn status(
        scanner: Scanner,
        poll_interval_secs: u64,
        age_secs: u64,
        tick_count: u64,
    ) -> ScannerStatus {
        status_on_shard(scanner, poll_interval_secs, age_secs, tick_count, None)
    }

    fn status_on_shard(
        scanner: Scanner,
        poll_interval_secs: u64,
        age_secs: u64,
        tick_count: u64,
        shard: Option<ShardId>,
    ) -> ScannerStatus {
        // Mirror `snapshot_as_of`: a single-instance status is stale on its own
        // shard exactly when its own reading is not healthy.
        let mut built = status_with_stale_shards(
            scanner,
            poll_interval_secs,
            age_secs,
            tick_count,
            shard,
            Vec::new(),
        );
        if classify_scanner(&built) != ScannerLivenessVerdict::Healthy {
            built.stale_shards = shard.into_iter().collect();
        }
        built
    }

    /// Build a status whose `stale_shards` is set explicitly, for the
    /// multi-shard folds `snapshot_as_of` produces but a single-instance
    /// helper cannot express.
    fn status_with_stale_shards(
        scanner: Scanner,
        poll_interval_secs: u64,
        age_secs: u64,
        tick_count: u64,
        shard: Option<ShardId>,
        stale_shards: Vec<ShardId>,
    ) -> ScannerStatus {
        ScannerStatus {
            scanner,
            poll_interval: Duration::from_secs(poll_interval_secs),
            tick_count,
            last_tick_at: (tick_count > 0).then(Utc::now),
            age: Duration::from_secs(age_secs),
            has_ticked: tick_count > 0,
            shard,
            stale_shards,
        }
    }

    #[test]
    fn scanner_liveness_passes_when_every_loop_ticked_recently() {
        let result = scanner_liveness_check(&[
            status(Scanner::Timeout, 30, 5, 12),
            status(Scanner::Retention, 30, 1, 3),
        ]);

        assert_eq!(result.name, "scanner_liveness");
        assert_eq!(result.status, PreflightStatus::Pass);
        assert_eq!(result.details["scanners_registered"], 2);
        assert!(
            result.details["stale_scanners"]
                .as_array()
                .expect("stale_scanners must be an array")
                .is_empty()
        );
        assert!(result.remediation.is_none());
    }

    /// Issue #797, Codex review: a multi-shard worker runs one timeout checker
    /// per assigned shard, all under one `Scanner` label, and the tick counter
    /// carries no shard label — the dashboard and the alert notes both send
    /// operators here precisely because this check is the only surface that can
    /// localize a single-shard wedge. It has to actually say which shard.
    #[test]
    fn scanner_liveness_names_the_shard_of_the_wedged_instance() {
        let result = scanner_liveness_check(&[status_on_shard(
            Scanner::Timeout,
            30,
            200,
            4,
            Some(ShardId::new(1)),
        )]);

        assert_eq!(result.status, PreflightStatus::Fail);
        assert!(
            result.summary.contains("timeout (shard 1)"),
            "the summary must name the wedged shard so an operator knows which \
             database is unprotected: {}",
            result.summary
        );
        assert_eq!(
            result.details["scanners"][0]["shard"], 1,
            "the per-scanner entry must carry the wedged instance's shard"
        );
        // `stale_scanners` stays a stable, machine-readable list of bare
        // scanner names — the shard rides the summary and the entry.
        assert_eq!(result.details["stale_scanners"][0], "timeout");
    }

    #[test]
    fn scanner_liveness_populates_the_affected_shards_contract_field() {
        // `affected_shards` is the STANDARD localization field every preflight
        // check shares, and the CLI's SCOPE column derives from it exclusively
        // (it prints `-` for an empty vec). Putting the shard only in the
        // summary and the nested details would leave `harvest preflight` and
        // every machine consumer without it.
        let result = scanner_liveness_check(&[
            status_on_shard(Scanner::Timeout, 30, 200, 4, Some(ShardId::new(1))),
            status_on_shard(Scanner::PoisonPill, 30, 300, 2, Some(ShardId::new(2))),
        ]);

        assert_eq!(result.status, PreflightStatus::Fail);
        assert_eq!(
            result.affected_shards,
            vec![1, 2],
            "every stale per-shard instance must appear in the standard \
             affected_shards field, sorted and deduped"
        );
    }

    #[test]
    fn scanner_liveness_affected_shards_covers_only_the_stale_instances() {
        // A healthy per-shard loop must not widen the blast radius: an operator
        // reading SCOPE should see the shards that are actually unprotected.
        let result = scanner_liveness_check(&[
            status_on_shard(Scanner::Timeout, 30, 200, 4, Some(ShardId::new(1))),
            status_on_shard(Scanner::PoisonPill, 30, 1, 99, Some(ShardId::new(2))),
        ]);

        assert_eq!(result.status, PreflightStatus::Fail);
        assert_eq!(
            result.affected_shards,
            vec![1],
            "shard 2 is ticking normally, so it must not be reported as affected"
        );
    }

    /// Issue #797, Codex review: `snapshot_as_of` folds the per-shard
    /// instances of one scanner down to the **worst** owner so the verdict is
    /// reproducible from the folded status. That fold is right for the
    /// verdict and wrong for the blast radius — if two shards are wedged at
    /// once, reporting only the worse one tells the operator to fix one
    /// database while the other stays unprotected. `stale_shards` carries all
    /// of them through, and `affected_shards` must surface all of them.
    #[test]
    fn scanner_liveness_affected_shards_covers_every_stale_shard_not_just_the_worst() {
        // One folded status for `timeout`, whose worst instance is shard 1 but
        // whose shard 2 instance is also stale -- exactly what `snapshot_as_of`
        // now produces for a two-shard worker with both loops wedged.
        let result = scanner_liveness_check(&[status_with_stale_shards(
            Scanner::Timeout,
            30,
            200,
            4,
            Some(ShardId::new(1)),
            vec![ShardId::new(1), ShardId::new(2)],
        )]);

        assert_eq!(result.status, PreflightStatus::Fail);
        assert_eq!(
            result.affected_shards,
            vec![1, 2],
            "both wedged shards must be in the blast radius -- reporting only \
             the worst instance's shard understates it and sends the operator \
             to one database while the other stays unprotected"
        );
    }

    #[test]
    fn scanner_liveness_affected_shards_is_empty_for_a_process_wide_loop() {
        // Retention and schedule are process-wide, so there is no shard to
        // localize -- the field stays empty and the CLI renders `-`.
        let result = scanner_liveness_check(&[status(Scanner::Retention, 30, 200, 4)]);

        assert_eq!(result.status, PreflightStatus::Fail);
        assert!(
            result.affected_shards.is_empty(),
            "a process-wide loop has no shard to attribute: {:?}",
            result.affected_shards
        );
    }

    /// A process-wide loop has no shard to report and must say so rather than
    /// inventing one — the common single-shard case.
    #[test]
    fn scanner_liveness_omits_the_shard_for_a_process_wide_loop() {
        let result = scanner_liveness_check(&[status(Scanner::Retention, 30, 200, 4)]);

        assert_eq!(result.status, PreflightStatus::Fail);
        assert!(
            result.summary.contains("retention") && !result.summary.contains("shard"),
            "a process-wide loop must not claim a shard: {}",
            result.summary
        );
        assert!(
            result.details["scanners"][0]["shard"].is_null(),
            "a process-wide loop must report a null shard"
        );
    }

    #[test]
    fn scanner_liveness_passes_on_a_replica_that_runs_no_control_loops() {
        // An API-only replica legitimately spawns no scanners. Reporting
        // seven phantom wedged loops there would be pure alarm noise.
        let result = scanner_liveness_check(&[]);

        assert_eq!(result.status, PreflightStatus::Pass);
        assert_eq!(result.details["scanners_registered"], 0);
        assert!(
            result.summary.contains("no background control loops"),
            "summary must explain why zero scanners is not a failure: {}",
            result.summary
        );
    }

    #[test]
    fn db_check_timeout_stand_in_points_at_the_scanner_verdict() {
        // The stand-in is what makes `scanner_liveness` observable when the
        // pool is exhausted: without it the endpoint hangs on
        // `pool.get().await` (deadpool waits with no timeout by default) and
        // the report never reaches the operator at all.
        //
        // Pool exhaustion is a listed cause of a wedged control loop, so the
        // one diagnostic that names the stalled loop must not be suppressed by
        // the very condition it is reporting on. The remediation therefore
        // routes the reader to that entry in the same report.
        let result = db_checks_timed_out();

        assert_eq!(result.status, PreflightStatus::Fail);
        assert_eq!(result.name, "db_checks");
        assert_eq!(
            result.details["budget_secs"],
            DB_CHECK_BUDGET.as_secs(),
            "the report must state the budget it exceeded"
        );
        let remediation = result.remediation.expect("a fail check must remediate");
        assert!(
            remediation.contains("scanner_liveness"),
            "a stalled-DB report must route the operator to the scanner \
             verdict in the same report: {remediation}"
        );
    }

    /// Scanner liveness is the one in-memory check whose answer *ages*, so it
    /// must be sampled **after** the bounded DB group — otherwise a loop that
    /// crosses its staleness threshold during a slow (up to `DB_CHECK_BUDGET`)
    /// wait is reported healthy in a report stamped afterwards.
    ///
    /// Asserted against the source rather than by driving the clock: the
    /// staleness floor is 60 s and the registry reads `std::time::Instant`,
    /// which `tokio`'s paused clock does not virtualize, so a timing test
    /// would have to sleep for real. This mirrors the anti-drift source scan
    /// in `scanner_liveness_tests.rs`, and fails if the call is moved back
    /// above the await.
    #[test]
    fn scanner_liveness_is_sampled_after_the_bounded_db_group() {
        const SOURCE: &str = include_str!("preflight.rs");

        let body_start = SOURCE
            .find("pub async fn build_preflight_report")
            .expect("build_preflight_report must exist");
        let body = &SOURCE[body_start..];

        let db_await = body
            .find("tokio::time::timeout(DB_CHECK_BUDGET")
            .expect("the DB group must still be bounded by DB_CHECK_BUDGET");
        let sample = body
            .find("let scanner_liveness = check_scanner_liveness();")
            .expect("the liveness sample must still be bound in this function");

        assert!(
            sample > db_await,
            "check_scanner_liveness() must be called AFTER the bounded DB \
             await so a loop that goes stale during the wait is reported; \
             it is synchronous and touches no pool, and timeout() always \
             returns, so moving it later cannot let the DB suppress it",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_wedged_db_check_cannot_suppress_the_in_memory_checks() {
        // Reproduces the shape of the bug: a DB-dependent check that never
        // returns (an exhausted pool blocking in `pool.get().await`). The
        // report must still be produced, with every in-memory check intact.
        //
        // `start_paused` auto-advances the clock, so the 30s budget elapses
        // instantly rather than actually sleeping.
        let hung = tokio::time::timeout(DB_CHECK_BUDGET, async {
            std::future::pending::<()>().await;
        })
        .await;

        assert!(
            hung.is_err(),
            "the budget must bound a never-returning check"
        );

        // And the check that has to survive it is a pure function of in-memory
        // state — no pool, no await, so nothing the database does can stop it.
        let scanner = scanner_liveness_check(&[status(Scanner::Timeout, 30, 300, 7)]);
        assert_eq!(
            scanner.status,
            PreflightStatus::Fail,
            "the wedged loop must still be reported while the DB is stalled"
        );
        assert!(
            scanner.details["stale_scanners"]
                .as_array()
                .is_some_and(|s| s.iter().any(|v| v == "timeout")),
            "and it must still name the stalled loop: {}",
            scanner.details
        );
    }

    #[test]
    fn scanner_liveness_warns_and_names_a_stale_scanner() {
        // 30s interval => 60s threshold; 90s is stale but not yet wedged.
        let result = scanner_liveness_check(&[
            status(Scanner::Timeout, 30, 90, 4),
            status(Scanner::Retention, 30, 1, 9),
        ]);

        assert_eq!(result.status, PreflightStatus::Warn);
        assert_eq!(
            result.details["stale_scanners"]
                .as_array()
                .expect("stale_scanners must be an array"),
            &vec![Value::from("timeout")],
            "the stale scanner must be named in details"
        );
        assert!(result.summary.contains("timeout"));
        assert!(result.remediation.is_some());
    }

    #[test]
    fn scanner_liveness_fails_when_a_scanner_is_wedged() {
        // 30s interval => 60s threshold; 200s is past 2x => wedged.
        let result = scanner_liveness_check(&[
            status(Scanner::Timeout, 30, 200, 4),
            status(Scanner::Sla, 30, 90, 4),
        ]);

        assert_eq!(result.status, PreflightStatus::Fail);
        let stale = result.details["stale_scanners"]
            .as_array()
            .expect("stale_scanners must be an array");
        assert!(stale.contains(&Value::from("timeout")));
        assert!(
            stale.contains(&Value::from("sla")),
            "a merely-stale scanner is still reported alongside a wedged one"
        );
    }

    #[test]
    fn scanner_liveness_details_carry_per_scanner_diagnostics() {
        let result = scanner_liveness_check(&[status(Scanner::Schedule, 30, 7, 42)]);

        let entry = &result.details["scanners"][0];
        assert_eq!(entry["scanner"], "schedule");
        assert_eq!(entry["verdict"], "healthy");
        assert_eq!(entry["tick_count"], 42);
        assert_eq!(entry["age_secs"], 7);
        assert_eq!(entry["poll_interval_secs"], 30);
        assert_eq!(
            entry["staleness_threshold_secs"], 60,
            "the threshold an operator is being judged against must be visible"
        );
        assert_eq!(entry["has_ticked"], true);
    }

    #[test]
    fn scanner_liveness_reports_a_never_ticked_scanner_inside_its_grace_window() {
        let result = scanner_liveness_check(&[status(Scanner::PoisonPill, 30, 3, 0)]);

        assert_eq!(result.status, PreflightStatus::Pass);
        assert_eq!(result.details["scanners"][0]["has_ticked"], false);
        assert_eq!(result.details["scanners"][0]["tick_count"], 0);
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
            compensate: None,
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

    // ── Issue #802 — opt-in declared workflow dependencies ────────────────
    //
    // These drive the pure helper directly (the `dag_unregistered_activity_failures`
    // pattern), so they need no database and run under the CI step
    // `cargo test -p autumn-harvest-plugin --lib`. `preflight_integration.rs` is
    // on the `ci_run_coverage` ALLOWLIST as debt and does NOT run in CI, so a
    // DB-gated test would be compile-checked only.
    mod declared_deps {
        use std::collections::HashSet;

        use autumn_harvest::prelude::*;
        use autumn_harvest::{ActivityInfo, WorkflowInfo};

        use super::super::{
            PreflightStatus, check_catalog_consistency, workflow_unregistered_dependency_failures,
        };
        use crate::api::HarvestApiState;

        // A minimal real workflow — its `_info()` supplies a genuine
        // `WorkflowHandlerFn` so tests mutate the two declaration fields on the
        // real type instead of hand-rolling a 20-field literal (mirrors the
        // sqlite crate's `feature_gate_tests::base_wf_info`).
        #[workflow]
        #[allow(clippy::unused_async)]
        async fn base_wf(_ctx: &WorkflowContext, input: ()) -> Result<(), String> {
            // `let () = input;` rather than `_input`: the macro's dispatch shim
            // reads the binding, which trips `clippy::used_underscore_binding`
            // (mirrors `transactional_activity_tests::happy_txn_workflow`).
            let () = input;
            Ok(())
        }

        // Ditto for a real `ActivityHandlerFn`.
        #[activity]
        #[allow(clippy::unused_async)]
        async fn base_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
            Ok(n)
        }

        fn activity_named(name: &'static str) -> ActivityInfo {
            let mut info = base_act_info();
            info.name = name;
            info
        }

        /// The operator-facing `details.failures` list, as plain strings.
        fn failure_strings(result: &super::super::PreflightCheckResult) -> Vec<String> {
            result.details["failures"]
                .as_array()
                .expect("details.failures must be an array")
                .iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        }

        fn wf(
            name: &'static str,
            activities: Option<&'static [&'static str]>,
            children: Option<&'static [&'static str]>,
        ) -> WorkflowInfo {
            let mut info = base_wf_info();
            info.name = name;
            info.declared_activities = activities;
            info.declared_children = children;
            info
        }

        fn failures_for(
            workflows: &[WorkflowInfo],
            registered_activities: &[&str],
            registered_workflows: &[&str],
        ) -> Vec<String> {
            let acts: HashSet<&str> = registered_activities.iter().copied().collect();
            let wfs: HashSet<&str> = registered_workflows.iter().copied().collect();
            workflow_unregistered_dependency_failures(
                workflows,
                |name| acts.contains(name),
                |name| wfs.contains(name),
            )
        }

        #[test]
        fn ac6a_flags_a_declared_activity_that_is_not_registered() {
            let workflows = [wf("onboarding", Some(&["send_emial"]), None)];
            assert_eq!(
                failures_for(&workflows, &["send_email"], &["onboarding"]),
                vec![
                    "workflow 'onboarding' references unregistered activity 'send_emial'"
                        .to_string()
                ]
            );
        }

        #[test]
        fn ac6b_flags_a_declared_child_that_is_not_registered() {
            let workflows = [wf("onboarding", None, Some(&["generate_reprot"]))];
            assert_eq!(
                failures_for(&workflows, &[], &["onboarding", "generate_report"]),
                vec![
                    "workflow 'onboarding' references unregistered child workflow 'generate_reprot'"
                        .to_string()
                ]
            );
        }

        #[test]
        fn ac6c_passes_when_every_declared_dependency_resolves() {
            let workflows = [wf(
                "onboarding",
                Some(&["send_email", "charge_card"]),
                Some(&["generate_report"]),
            )];
            assert!(
                failures_for(
                    &workflows,
                    &["send_email", "charge_card"],
                    &["onboarding", "generate_report"],
                )
                .is_empty()
            );
        }

        #[test]
        fn ac6d_a_workflow_that_never_opted_in_is_never_flagged() {
            // The zero-false-positive guarantee: `None` is skipped outright, so
            // an empty registry cannot produce a failure for it.
            let workflows = [wf("legacy", None, None)];
            assert!(failures_for(&workflows, &[], &[]).is_empty());
        }

        #[test]
        fn intent_pin_an_explicitly_empty_declaration_resolves_trivially() {
            // INTENT PIN, not evidence: `Some(&[])` and `None` both yield zero
            // failures *by construction* here, so this cannot fail under any
            // implementation of the helper. The load-bearing `Some(&[])` vs
            // `None` distinction is proven where it is actually observable — at
            // the macro layer by `workflow_empty_declaration_is_opt_in_not_absent`
            // and on the wire by
            // `registered_workflow_record_distinguishes_empty_declaration_from_absent`.
            let workflows = [wf("noop", Some(&[]), Some(&[]))];
            assert!(failures_for(&workflows, &[], &[]).is_empty());
        }

        #[test]
        fn two_workflows_missing_the_same_activity_are_each_reported() {
            // Dedup is keyed on the whole failure STRING, not the bare name —
            // keying on the name alone would silently swallow the second
            // workflow's miss and under-report the blast radius of a forgotten
            // registration, which is exactly what this check exists to size.
            let workflows = [
                wf("alpha", Some(&["gone"]), None),
                wf("beta", Some(&["gone"]), None),
            ];
            assert_eq!(
                failures_for(&workflows, &[], &[]),
                vec![
                    "workflow 'alpha' references unregistered activity 'gone'".to_string(),
                    "workflow 'beta' references unregistered activity 'gone'".to_string(),
                ]
            );
        }

        #[test]
        fn a_repeated_missing_reference_is_reported_once() {
            // A duplicate in the declaration is an authoring smell, not an
            // error; the operator should still see each miss exactly once.
            let workflows = [wf("onboarding", Some(&["missing", "missing"]), None)];
            assert_eq!(
                failures_for(&workflows, &[], &[]),
                vec![
                    "workflow 'onboarding' references unregistered activity 'missing'".to_string()
                ]
            );
        }

        #[test]
        fn a_blank_declared_name_is_reported_as_such() {
            // A blank name can never resolve; report it as the authoring error
            // it is rather than as a confusing "unregistered ''".
            let workflows = [wf("onboarding", Some(&["   "]), Some(&[""]))];
            assert_eq!(
                failures_for(&workflows, &[], &[]),
                vec![
                    "workflow 'onboarding' declares an empty activity name".to_string(),
                    "workflow 'onboarding' declares an empty child workflow name".to_string(),
                ]
            );
        }

        #[test]
        fn failures_are_ordered_deterministically_by_workflow_name() {
            // `registry().workflows` is a HashMap, so the helper must impose its
            // own order or the operator-facing list reshuffles between calls.
            let workflows = [
                wf("zeta", Some(&["missing_z"]), None),
                wf("alpha", Some(&["missing_a"]), None),
                wf("mid", Some(&["missing_m"]), None),
            ];
            assert_eq!(
                failures_for(&workflows, &[], &[]),
                vec![
                    "workflow 'alpha' references unregistered activity 'missing_a'".to_string(),
                    "workflow 'mid' references unregistered activity 'missing_m'".to_string(),
                    "workflow 'zeta' references unregistered activity 'missing_z'".to_string(),
                ]
            );
        }

        #[test]
        fn a_workflow_may_declare_itself_as_a_child() {
            // Self-recursion is legal (`spawn_child_workflow` of one's own type);
            // the name resolves against the registry like any other.
            let workflows = [wf("recursive", None, Some(&["recursive"]))];
            assert!(failures_for(&workflows, &[], &["recursive"]).is_empty());
        }

        #[test]
        fn activities_and_children_resolve_against_separate_catalogs() {
            // An activity name registered only as a WORKFLOW (and vice versa)
            // must still fail — the two namespaces are distinct.
            let workflows = [wf(
                "x",
                Some(&["only_a_workflow"]),
                Some(&["only_an_activity"]),
            )];
            assert_eq!(
                failures_for(&workflows, &["only_an_activity"], &["only_a_workflow"]),
                vec![
                    "workflow 'x' references unregistered activity 'only_a_workflow'".to_string(),
                    "workflow 'x' references unregistered child workflow 'only_an_activity'"
                        .to_string(),
                ]
            );
        }

        // ── Success metric: 100% precision for opted-in, 0 false positives ────
        //
        // Asserting one example proves neither claim. This drives a mixed
        // catalog and asserts the failure set EXACTLY: every genuine miss is
        // reported (recall) and nothing else is (precision), with the
        // non-opted-in rows present precisely so a regression that stopped
        // skipping `None` would show up as extra entries.
        // ── AC2/AC3 wiring: the helper is actually reached by the check, and
        // its strings land in `details.failures` with the right verdict ───────
        //
        // Pure: `check_catalog_consistency` reads only `api_state.runtime()`, so
        // no storage pool (and no database) is installed.
        fn api_state_with(
            workflows: Vec<WorkflowInfo>,
            activities: Vec<ActivityInfo>,
        ) -> HarvestApiState {
            use std::sync::Arc;

            use autumn_harvest::retention::RetentionConfig;
            use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
            use autumn_harvest::shard::ShardRouter;
            use autumn_harvest::worker::HandlerRegistry;

            use crate::api::{HarvestApiRuntime, HarvestRetentionRuntime};

            let runtime = HarvestApiRuntime::new(
                Arc::new(HandlerRegistry::new(workflows, activities)),
                Arc::new(DagCatalog::new()),
                Arc::new(Vec::new()),
                None,
                vec!["default".to_string()],
                SchedulerMonitor::offline(),
                HarvestRetentionRuntime::disabled(RetentionConfig::default()),
                ShardRouter::single(),
            );
            let state = HarvestApiState::new();
            state.install(runtime);
            state
        }

        #[test]
        fn ac3_catalog_consistency_fails_and_names_the_unresolved_reference() {
            let state = api_state_with(
                vec![wf(
                    "onboarding",
                    Some(&["send_emial"]),
                    Some(&["missing_child"]),
                )],
                Vec::new(),
            );

            let result = check_catalog_consistency(&state);

            assert_eq!(result.status, PreflightStatus::Fail);
            let failures = failure_strings(&result);
            assert!(
                failures.contains(
                    &"workflow 'onboarding' references unregistered activity 'send_emial'"
                        .to_string()
                ),
                "missing activity must be named in details.failures: {failures:?}"
            );
            assert!(
                failures.contains(
                    &"workflow 'onboarding' references unregistered child workflow 'missing_child'"
                        .to_string()
                ),
                "missing child must be named in details.failures: {failures:?}"
            );
        }

        #[test]
        fn ac3_catalog_consistency_passes_for_a_complete_opted_in_catalog() {
            let state = api_state_with(
                vec![
                    wf("onboarding", Some(&["send_email"]), Some(&["child_wf"])),
                    wf("child_wf", None, None),
                ],
                vec![activity_named("send_email")],
            );

            let result = check_catalog_consistency(&state);

            assert_eq!(
                result.status,
                PreflightStatus::Pass,
                "details: {}",
                result.details
            );
        }

        #[test]
        fn ac2_declared_dep_failures_coexist_with_preexisting_failures() {
            // The new failures must be APPENDED to the pre-existing ones, never
            // replace them: `failures.extend(..)` written as `failures = ..`
            // would silently drop every DAG / empty-default-queue failure the
            // check already reports, and no other test would notice.
            let mut empty_queue_activity = activity_named("bad_queue_act");
            empty_queue_activity.default_queue = Some("");
            let state = api_state_with(
                vec![wf("onboarding", Some(&["send_emial"]), None)],
                vec![empty_queue_activity],
            );

            let result = check_catalog_consistency(&state);

            assert_eq!(result.status, PreflightStatus::Fail);
            let failures = failure_strings(&result);
            assert!(
                failures.contains(
                    &"workflow 'onboarding' references unregistered activity 'send_emial'"
                        .to_string()
                ),
                "the new declared-dependency failure must be present: {failures:?}"
            );
            assert!(
                failures.contains(
                    &"activity 'bad_queue_act' declares an empty default queue".to_string()
                ),
                "the pre-existing failure must NOT be clobbered: {failures:?}"
            );
        }

        #[test]
        fn ac3_mixed_catalog_failure_set_is_exact_through_the_real_check() {
            // The success metric (100% precision, ZERO false positives) claimed
            // of the pure helper, re-asserted through the operator-facing
            // entry point over a real `HashMap`-backed registry — which is also
            // the only place the name sort is exercised against genuinely
            // nondeterministic iteration order.
            let state = api_state_with(
                vec![
                    wf("complete", Some(&["act_ok"]), Some(&["wf_ok"])),
                    wf("missing_act", Some(&["act_ok", "act_gone"]), None),
                    wf("missing_child", None, Some(&["wf_gone"])),
                    wf("empty_decl", Some(&[]), Some(&[])),
                    wf("legacy_a", None, None),
                    wf("legacy_b", None, None),
                    wf("wf_ok", None, None),
                ],
                vec![activity_named("act_ok")],
            );

            let result = check_catalog_consistency(&state);

            assert_eq!(result.status, PreflightStatus::Fail);
            assert_eq!(
                failure_strings(&result),
                vec![
                    "workflow 'missing_act' references unregistered activity 'act_gone'"
                        .to_string(),
                    "workflow 'missing_child' references unregistered child workflow 'wf_gone'"
                        .to_string(),
                ],
                "exactly the two genuine misses, in workflow-name order — no false \
                 positives from the four non-offending workflows"
            );
        }

        #[test]
        fn ac4_catalog_consistency_is_unchanged_for_a_workflow_that_never_opted_in() {
            // Same empty activity catalog as the failing case above — the ONLY
            // difference is that this workflow declared nothing.
            let state = api_state_with(vec![wf("legacy", None, None)], Vec::new());

            let result = check_catalog_consistency(&state);

            assert_eq!(
                result.status,
                PreflightStatus::Pass,
                "a non-opted-in workflow must never fail preflight: {}",
                result.details
            );
        }

        #[test]
        fn success_metric_exact_failure_set_over_a_mixed_catalog() {
            let workflows = [
                // opted in, fully registered → contributes nothing
                wf("complete", Some(&["act_ok"]), Some(&["wf_ok"])),
                // opted in, one missing activity
                wf("missing_act", Some(&["act_ok", "act_gone"]), None),
                // opted in, one missing child
                wf("missing_child", None, Some(&["wf_gone"])),
                // opted in with an empty list → contributes nothing
                wf("empty_decl", Some(&[]), Some(&[])),
                // NOT opted in, and nothing it could reference is registered
                wf("legacy_a", None, None),
                wf("legacy_b", None, None),
            ];

            let failures = failures_for(&workflows, &["act_ok"], &["wf_ok"]);

            assert_eq!(
                failures,
                vec![
                    "workflow 'missing_act' references unregistered activity 'act_gone'"
                        .to_string(),
                    "workflow 'missing_child' references unregistered child workflow 'wf_gone'"
                        .to_string(),
                ],
                "exactly the two genuine misses — no false positives, no missed detections"
            );
        }
    }

    // ── Issue #780 — declarative DAG node compensation ─────────────────────

    fn compensated_task(name: &str, compensate: &str) -> autumn_harvest::DagTask {
        autumn_harvest::DagTask {
            compensate: Some(compensate.to_string()),
            ..activity_task(name)
        }
    }

    /// T23 — a node's compensator is dispatched through the ordinary DAG
    /// activity-queue lowering on the terminal-failure unwind, so preflight must
    /// flag an UNREGISTERED compensator before rollout — otherwise the miss only
    /// surfaces mid-unwind, exactly when the state is already dangling.
    #[test]
    fn preflight_flags_an_unregistered_compensator() {
        let registered: HashSet<&str> =
            ["reserve_inventory", "release_inventory", "charge_payment"]
                .into_iter()
                .collect();
        let tasks = vec![
            // Registered compensator → not flagged.
            compensated_task("reserve_inventory", "release_inventory"),
            // Unregistered compensator on a registered forward node → flagged.
            compensated_task("charge_payment", "refund_payment"),
        ];

        let failures = dag_unregistered_activity_failures(
            std::iter::once(("fulfillment", tasks.as_slice())),
            |name| registered.contains(name),
        );

        assert_eq!(
            failures.len(),
            1,
            "exactly the unregistered compensator must be flagged, got {failures:?}"
        );
        assert!(
            failures[0].contains("refund_payment") && failures[0].contains("fulfillment"),
            "the failure must name the missing compensator and its DAG, got {:?}",
            failures[0]
        );
        assert!(
            !failures[0].contains("release_inventory"),
            "a REGISTERED compensator must not be flagged, got {:?}",
            failures[0]
        );
    }
}
