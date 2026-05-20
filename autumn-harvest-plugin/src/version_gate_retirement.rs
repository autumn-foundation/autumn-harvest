//! Management read model for the version-gate retirement check.
//!
//! Answers "is change id X safe to retire below version N?" by aggregating
//! [`load_retirement_check`] results across all configured shards and surfacing
//! per-shard availability status so callers know whether the result is
//! conclusive.

use std::collections::{BTreeMap, BTreeSet};

use autumn_harvest::version_gate_retirement::{
    RetirementCheckFilters, RetirementCheckShardRow, load_retirement_check,
};
use autumn_harvest::version_usage::VersionExecutionStateGroup;
use autumn_harvest::worker::DbPool;
use autumn_web::error::AutumnError;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::{HarvestApiState, map_error};

/// Query string accepted by `GET /admin/version-gates/retirement-check`.
#[derive(Debug, Clone, Deserialize)]
pub struct RetirementCheckQuery {
    /// Change id to inspect (required).
    pub change_id: String,
    /// Versions strictly below this value are considered "old branches to retire".
    pub min_safe_version: u32,
    /// Narrow results to this workflow name.
    pub workflow_name: Option<String>,
    /// `"active"`, `"terminal"`, or `"all"` (default `"all"`).
    pub state_group: Option<String>,
    /// Restrict inspection to one shard.
    pub shard_id: Option<i32>,
}

/// Overall retirement-check report status.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetirementCheckStatus {
    /// All shards were inspected and no active old-version executions remain.
    Safe,
    /// All shards were inspected but active old-version executions still exist.
    Blocked,
    /// Some shards were inspected; at least one was unavailable.
    Partial,
    /// No shards could be inspected.
    Unavailable,
}

impl RetirementCheckStatus {
    #[must_use]
    pub const fn is_safe(self) -> bool {
        matches!(self, Self::Safe)
    }
}

/// Per-shard inspection status.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RetirementShardInspectionStatus {
    Inspected,
    Unavailable,
}

/// Top-level report returned by `GET /admin/version-gates/retirement-check`.
#[derive(Debug, Clone, Serialize)]
pub struct RetirementCheckReport {
    pub status: RetirementCheckStatus,
    /// `true` only when `status == "safe"` — all shards inspected, zero active blockers.
    pub safe_to_retire: bool,
    pub observed_at: DateTime<Utc>,
    pub filters: RetirementCheckReportFilters,
    /// One row per `(workflow_name, recorded_version)` group that has at least one
    /// execution with a version below `min_safe_version`.
    pub blockers: Vec<RetirementBlockerReportRow>,
    pub shards: Vec<RetirementShardInspection>,
}

/// Echo of the filters applied to the report.
#[derive(Debug, Clone, Serialize)]
pub struct RetirementCheckReportFilters {
    pub change_id: String,
    pub min_safe_version: u32,
    pub workflow_name: Option<String>,
    pub state_group: VersionExecutionStateGroup,
    pub shard_id: Option<i32>,
}

/// Aggregated blocker row across all inspected shards.
#[derive(Debug, Clone, Serialize)]
pub struct RetirementBlockerReportRow {
    pub workflow_name: String,
    pub change_id: String,
    pub recorded_version: u32,
    pub active_executions: i64,
    pub terminal_executions: i64,
    pub oldest_blocker_started_at: DateTime<Utc>,
    pub newest_blocker_started_at: DateTime<Utc>,
    pub oldest_blocker_age_secs: i64,
    pub newest_blocker_age_secs: i64,
    /// Sample of active execution IDs (up to 10 per shard, deduplicated).
    pub sample_active_execution_ids: Vec<Uuid>,
    pub shard_coverage: RetirementShardCoverage,
}

/// Per-row shard coverage metadata.
#[derive(Debug, Clone, Serialize)]
pub struct RetirementShardCoverage {
    /// The shard IDs that were successfully queried.
    pub inspected_shards: Vec<i32>,
    /// The shard IDs that contained at least one blocker.
    pub matched_shards: Vec<i32>,
    /// The shard IDs that could not be queried due to errors.
    pub unavailable_shards: Vec<i32>,
}

/// Per-shard inspection summary included in every report.
#[derive(Debug, Clone, Serialize)]
pub struct RetirementShardInspection {
    /// The ID of the shard.
    pub shard_id: i32,
    /// Whether the shard was successfully inspected or was unavailable.
    pub status: RetirementShardInspectionStatus,
    /// The number of distinct `(workflow_name, recorded_version)` groups found on this shard, if successful.
    pub matched_groups: Option<usize>,
    /// Detailed error message if the shard was unavailable.
    pub error: Option<String>,
}

#[derive(Debug)]
struct ShardObservation {
    shard_id: i32,
    rows: Vec<RetirementCheckShardRow>,
    error: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct RetirementKey {
    workflow_name: String,
    change_id: String,
    recorded_version: u32,
}

#[derive(Debug)]
struct RetirementAccumulator {
    active_executions: i64,
    terminal_executions: i64,
    oldest_blocker_started_at: DateTime<Utc>,
    newest_blocker_started_at: DateTime<Utc>,
    matched_shards: BTreeSet<i32>,
    sample_active_execution_ids: Vec<Uuid>,
}

/// Build the retirement-check report without mutating workflow state.
///
/// # Errors
///
/// Returns an [`AutumnError`] when the `state_group` query parameter contains
/// an unknown value.
pub async fn build_retirement_check_report(
    api_state: &HarvestApiState,
    query: RetirementCheckQuery,
) -> Result<RetirementCheckReport, AutumnError> {
    let observed_at = Utc::now();
    let state_group = query
        .state_group
        .as_deref()
        .unwrap_or("all")
        .parse::<VersionExecutionStateGroup>()
        .map_err(map_error)?;

    let filters = RetirementCheckFilters {
        workflow_name: query.workflow_name.clone(),
        change_id: query.change_id.clone(),
        min_safe_version: query.min_safe_version,
        state_group,
        shard_id: query.shard_id,
    };

    let expected_shards = expected_shards(api_state, query.shard_id);
    let pools = pools_by_shard(api_state);

    let observations = expected_shards
        .iter()
        .map(|shard_id| {
            let pool = pools.get(shard_id).cloned();
            observe_shard(*shard_id, pool, filters.clone())
        })
        .collect::<Vec<_>>();
    let observations = join_all(observations).await;

    Ok(build_report_from_observations(
        observed_at,
        RetirementCheckReportFilters {
            change_id: query.change_id,
            min_safe_version: query.min_safe_version,
            workflow_name: query.workflow_name,
            state_group,
            shard_id: query.shard_id,
        },
        observations,
    ))
}

fn expected_shards(api_state: &HarvestApiState, shard_filter: Option<i32>) -> BTreeSet<i32> {
    if let Some(shard_id) = shard_filter {
        return BTreeSet::from([shard_id]);
    }

    let mut shards = BTreeSet::new();
    if let Ok(pool) = api_state.storage_pool() {
        shards.extend(pool.iter_shards().map(|(shard, _)| shard.as_i32()));
    }
    if let Ok(runtime) = api_state.runtime() {
        shards.extend(
            runtime
                .router()
                .readable_shards()
                .iter()
                .map(|shard| shard.as_i32()),
        );
        shards.insert(runtime.router().default_shard().as_i32());
    }
    if shards.is_empty() {
        shards.insert(0);
    }
    shards
}

fn pools_by_shard(api_state: &HarvestApiState) -> BTreeMap<i32, DbPool> {
    api_state.storage_pool().map_or_else(
        |_| BTreeMap::new(),
        |pool| {
            pool.iter_shards()
                .map(|(shard, db_pool)| (shard.as_i32(), db_pool.clone()))
                .collect()
        },
    )
}

async fn observe_shard(
    shard_id: i32,
    pool: Option<DbPool>,
    mut filters: RetirementCheckFilters,
) -> ShardObservation {
    let Some(pool) = pool else {
        return ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(format!("shard {shard_id} has no configured storage pool")),
        };
    };

    filters.shard_id = Some(shard_id);
    let Ok(mut conn) = pool.get().await else {
        return ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(format!(
                "database connection for shard {shard_id} could not be acquired"
            )),
        };
    };

    match load_retirement_check(&mut conn, &filters).await {
        Ok(rows) => ShardObservation {
            shard_id,
            rows,
            error: None,
        },
        Err(error) => ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(error.to_string()),
        },
    }
}

fn build_report_from_observations(
    observed_at: DateTime<Utc>,
    filters: RetirementCheckReportFilters,
    observations: Vec<ShardObservation>,
) -> RetirementCheckReport {
    let inspected_shards = observations
        .iter()
        .filter(|o| o.error.is_none())
        .map(|o| o.shard_id)
        .collect::<BTreeSet<_>>();
    let unavailable_shards = observations
        .iter()
        .filter(|o| o.error.is_some())
        .map(|o| o.shard_id)
        .collect::<BTreeSet<_>>();

    let mut rows = BTreeMap::<RetirementKey, RetirementAccumulator>::new();
    for observation in &observations {
        for row in &observation.rows {
            let key = RetirementKey {
                workflow_name: row.workflow_name.clone(),
                change_id: row.change_id.clone(),
                recorded_version: row.recorded_version,
            };
            rows.entry(key)
                .and_modify(|acc| merge_row(acc, row))
                .or_insert_with(|| accumulator_from_row(row));
        }
    }

    let blockers = rows
        .into_iter()
        .map(|(key, acc)| {
            blocker_from_accumulator(
                key,
                &acc,
                &inspected_shards,
                &unavailable_shards,
                observed_at,
            )
        })
        .collect::<Vec<_>>();

    let shards = observations
        .into_iter()
        .map(|o| RetirementShardInspection {
            shard_id: o.shard_id,
            status: if o.error.is_some() {
                RetirementShardInspectionStatus::Unavailable
            } else {
                RetirementShardInspectionStatus::Inspected
            },
            matched_groups: o.error.as_ref().map_or(Some(o.rows.len()), |_| None),
            error: o.error,
        })
        .collect::<Vec<_>>();

    let status = compute_status(
        blockers.iter().map(|b| b.active_executions).sum::<i64>(),
        inspected_shards.is_empty(),
        unavailable_shards.is_empty(),
    );

    RetirementCheckReport {
        safe_to_retire: status.is_safe(),
        status,
        observed_at,
        filters,
        blockers,
        shards,
    }
}

fn merge_row(acc: &mut RetirementAccumulator, row: &RetirementCheckShardRow) {
    acc.active_executions += row.active_executions;
    acc.terminal_executions += row.terminal_executions;
    acc.oldest_blocker_started_at = acc
        .oldest_blocker_started_at
        .min(row.oldest_blocker_started_at);
    acc.newest_blocker_started_at = acc
        .newest_blocker_started_at
        .max(row.newest_blocker_started_at);
    acc.matched_shards.insert(row.shard_id);
    acc.sample_active_execution_ids
        .extend_from_slice(&row.sample_active_execution_ids);
    acc.sample_active_execution_ids.truncate(10);
}

fn accumulator_from_row(row: &RetirementCheckShardRow) -> RetirementAccumulator {
    RetirementAccumulator {
        active_executions: row.active_executions,
        terminal_executions: row.terminal_executions,
        oldest_blocker_started_at: row.oldest_blocker_started_at,
        newest_blocker_started_at: row.newest_blocker_started_at,
        matched_shards: BTreeSet::from([row.shard_id]),
        sample_active_execution_ids: row.sample_active_execution_ids.clone(),
    }
}

fn blocker_from_accumulator(
    key: RetirementKey,
    acc: &RetirementAccumulator,
    inspected_shards: &BTreeSet<i32>,
    unavailable_shards: &BTreeSet<i32>,
    observed_at: DateTime<Utc>,
) -> RetirementBlockerReportRow {
    RetirementBlockerReportRow {
        workflow_name: key.workflow_name,
        change_id: key.change_id,
        recorded_version: key.recorded_version,
        active_executions: acc.active_executions,
        terminal_executions: acc.terminal_executions,
        oldest_blocker_started_at: acc.oldest_blocker_started_at,
        newest_blocker_started_at: acc.newest_blocker_started_at,
        oldest_blocker_age_secs: age_secs(observed_at, acc.oldest_blocker_started_at),
        newest_blocker_age_secs: age_secs(observed_at, acc.newest_blocker_started_at),
        sample_active_execution_ids: acc.sample_active_execution_ids.clone(),
        shard_coverage: RetirementShardCoverage {
            inspected_shards: inspected_shards.iter().copied().collect(),
            matched_shards: acc.matched_shards.iter().copied().collect(),
            unavailable_shards: unavailable_shards.iter().copied().collect(),
        },
    }
}

fn age_secs(observed_at: DateTime<Utc>, started_at: DateTime<Utc>) -> i64 {
    observed_at
        .signed_duration_since(started_at)
        .num_seconds()
        .max(0)
}

const fn compute_status(
    total_active: i64,
    no_inspected_shards: bool,
    no_unavailable_shards: bool,
) -> RetirementCheckStatus {
    if no_inspected_shards {
        RetirementCheckStatus::Unavailable
    } else if !no_unavailable_shards {
        RetirementCheckStatus::Partial
    } else if total_active > 0 {
        RetirementCheckStatus::Blocked
    } else {
        RetirementCheckStatus::Safe
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

    fn shard_row(
        shard_id: i32,
        workflow_name: &str,
        change_id: &str,
        recorded_version: u32,
        active_executions: i64,
        terminal_executions: i64,
    ) -> RetirementCheckShardRow {
        RetirementCheckShardRow {
            workflow_name: workflow_name.to_string(),
            change_id: change_id.to_string(),
            recorded_version,
            active_executions,
            terminal_executions,
            oldest_blocker_started_at: at(100),
            newest_blocker_started_at: at(200),
            sample_active_execution_ids: Vec::new(),
            shard_id,
        }
    }

    fn default_filters() -> RetirementCheckReportFilters {
        RetirementCheckReportFilters {
            change_id: "tax_v2".to_string(),
            min_safe_version: 2,
            workflow_name: None,
            state_group: VersionExecutionStateGroup::All,
            shard_id: None,
        }
    }

    #[test]
    fn safe_status_when_no_active_executions_and_all_shards_inspected() {
        let report = build_report_from_observations(
            at(300),
            default_filters(),
            vec![ShardObservation {
                shard_id: 0,
                rows: vec![shard_row(0, "billing", "tax_v2", 1, 0, 5)],
                error: None,
            }],
        );
        assert_eq!(report.status, RetirementCheckStatus::Safe);
        assert!(report.safe_to_retire);
    }

    #[test]
    fn blocked_status_when_active_executions_exist() {
        let report = build_report_from_observations(
            at(300),
            default_filters(),
            vec![ShardObservation {
                shard_id: 0,
                rows: vec![shard_row(0, "billing", "tax_v2", 1, 3, 7)],
                error: None,
            }],
        );
        assert_eq!(report.status, RetirementCheckStatus::Blocked);
        assert!(!report.safe_to_retire);
        assert_eq!(report.blockers[0].active_executions, 3);
    }

    #[test]
    fn unavailable_status_when_all_shards_fail() {
        let report = build_report_from_observations(
            at(300),
            default_filters(),
            vec![ShardObservation {
                shard_id: 0,
                rows: Vec::new(),
                error: Some("connection refused".to_string()),
            }],
        );
        assert_eq!(report.status, RetirementCheckStatus::Unavailable);
        assert!(!report.safe_to_retire);
    }

    #[test]
    fn partial_status_when_one_shard_unavailable() {
        let report = build_report_from_observations(
            at(300),
            default_filters(),
            vec![
                ShardObservation {
                    shard_id: 0,
                    rows: vec![shard_row(0, "billing", "tax_v2", 1, 0, 5)],
                    error: None,
                },
                ShardObservation {
                    shard_id: 1,
                    rows: Vec::new(),
                    error: Some("pool missing".to_string()),
                },
            ],
        );
        assert_eq!(report.status, RetirementCheckStatus::Partial);
        assert!(!report.safe_to_retire);
        assert_eq!(
            report.blockers[0].shard_coverage.unavailable_shards,
            vec![1]
        );
    }

    #[test]
    fn empty_report_when_no_markers_found() {
        let report = build_report_from_observations(
            at(300),
            default_filters(),
            vec![ShardObservation {
                shard_id: 0,
                rows: Vec::new(),
                error: None,
            }],
        );
        assert_eq!(report.status, RetirementCheckStatus::Safe);
        assert!(report.safe_to_retire);
        assert!(report.blockers.is_empty());
    }

    #[test]
    fn cross_shard_active_counts_are_summed() {
        let report = build_report_from_observations(
            at(300),
            default_filters(),
            vec![
                ShardObservation {
                    shard_id: 0,
                    rows: vec![shard_row(0, "billing", "tax_v2", 1, 2, 3)],
                    error: None,
                },
                ShardObservation {
                    shard_id: 1,
                    rows: vec![shard_row(1, "billing", "tax_v2", 1, 4, 1)],
                    error: None,
                },
            ],
        );
        assert_eq!(report.blockers.len(), 1);
        assert_eq!(report.blockers[0].active_executions, 6);
        assert_eq!(report.blockers[0].terminal_executions, 4);
        assert_eq!(report.blockers[0].shard_coverage.matched_shards, vec![0, 1]);
    }

    #[test]
    fn multiple_old_versions_produce_separate_blocker_rows() {
        let report = build_report_from_observations(
            at(300),
            default_filters(),
            vec![ShardObservation {
                shard_id: 0,
                rows: vec![
                    shard_row(0, "billing", "tax_v2", 1, 1, 0),
                    shard_row(0, "billing", "tax_v2", 0, 0, 2),
                ],
                error: None,
            }],
        );
        assert_eq!(report.blockers.len(), 2);
        let versions: Vec<u32> = report.blockers.iter().map(|b| b.recorded_version).collect();
        assert!(versions.contains(&0));
        assert!(versions.contains(&1));
    }

    #[test]
    fn status_serializes_as_snake_case() {
        let blocked = serde_json::to_value(RetirementCheckStatus::Blocked).unwrap();
        assert_eq!(blocked, serde_json::json!("blocked"));

        let safe = serde_json::to_value(RetirementCheckStatus::Safe).unwrap();
        assert_eq!(safe, serde_json::json!("safe"));

        let partial = serde_json::to_value(RetirementCheckStatus::Partial).unwrap();
        assert_eq!(partial, serde_json::json!("partial"));

        let unavailable = serde_json::to_value(RetirementCheckStatus::Unavailable).unwrap();
        assert_eq!(unavailable, serde_json::json!("unavailable"));
    }
}
