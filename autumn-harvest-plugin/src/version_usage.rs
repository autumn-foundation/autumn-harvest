//! Management read model for recorded version-gate usage.

use std::collections::{BTreeMap, BTreeSet};

use autumn_harvest::version_usage::{
    VersionExecutionStateGroup, VersionUsageFilters, VersionUsageShardRow, load_version_usage,
};
use autumn_harvest::worker::DbPool;
use autumn_web::error::AutumnError;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use serde::{Deserialize, Serialize};

use crate::api::{HarvestApiState, map_error};
use crate::shard_fanout;

/// Query string accepted by `GET /admin/version-gates/usage`.
#[derive(Debug, Clone, Deserialize)]
pub struct VersionUsageQuery {
    /// Optional filter for a specific workflow name.
    pub workflow_name: Option<String>,
    /// Optional filter for a specific change ID.
    pub change_id: Option<String>,
    /// Optional filter for a specific recorded version.
    pub recorded_version: Option<u32>,
    /// Generic field documentation.
    pub state_group: Option<String>,
    /// Optional filter to restrict the query to a specific shard ID.
    pub shard_id: Option<i32>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Status of the version usage report.
pub enum VersionUsageReportStatus {
    /// Report is complete.
    Complete,
    /// No matches were found for the report.
    NoMatches,
    /// The report is partial (e.g., some shards were unavailable).
    Partial,
    /// The shard was unavailable or failed to respond.
    Unavailable,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
/// Status of inspecting a specific shard for version usage.
pub enum VersionUsageShardInspectionStatus {
    /// The shard was successfully inspected.
    Inspected,
    /// The shard was unavailable or failed to respond.
    Unavailable,
}

#[derive(Debug, Clone, Serialize)]
/// Report representing version usage across executions.
pub struct VersionUsageReport {
    /// Overarching status of the report generation.
    pub status: VersionUsageReportStatus,
    /// Timestamp when this report was observed.
    pub observed_at: DateTime<Utc>,
    /// Filters used to generate this report.
    pub filters: VersionUsageReportFilters,
    /// The individual usage records returned by the report.
    pub items: Vec<VersionUsageReportRow>,
    /// Details regarding which shards were inspected and their status.
    pub shards: Vec<VersionUsageShardInspection>,
}

#[derive(Debug, Clone, Serialize)]
/// Filters used when querying for version usage reports.
pub struct VersionUsageReportFilters {
    /// Optional filter for a specific workflow name.
    pub workflow_name: Option<String>,
    /// Optional filter for a specific change ID.
    pub change_id: Option<String>,
    /// Optional filter for a specific recorded version.
    pub recorded_version: Option<u32>,
    /// Defines which execution states are included (e.g., active, terminal).
    pub state_group: VersionExecutionStateGroup,
    /// Optional filter to restrict the query to a specific shard ID.
    pub shard_id: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
/// A single row within a version usage report representing usage for a specific workflow, change, and version.
pub struct VersionUsageReportRow {
    /// The name of the workflow.
    pub workflow_name: String,
    /// The change ID associated with the version usage.
    pub change_id: String,
    /// The recorded version number.
    pub recorded_version: u32,
    /// Number of active executions using this version.
    pub active_executions: i64,
    /// Number of terminal executions using this version.
    pub terminal_executions: i64,
    /// The start time of the oldest execution matching this version.
    pub oldest_matching_started_at: DateTime<Utc>,
    /// The start time of the newest execution matching this version.
    pub newest_matching_started_at: DateTime<Utc>,
    /// Age in seconds of the oldest matching execution.
    pub oldest_matching_execution_age_secs: i64,
    /// Age in seconds of the newest matching execution.
    pub newest_matching_execution_age_secs: i64,
    /// Details on the shard coverage for this row.
    pub shard_coverage: VersionUsageShardCoverage,
}

#[derive(Debug, Clone, Serialize)]
/// Represents which shards were inspected, matched, or unavailable for a specific usage report row.
pub struct VersionUsageShardCoverage {
    /// List of shard IDs that were successfully inspected.
    pub inspected_shards: Vec<i32>,
    /// List of shard IDs where this version usage was matched.
    pub matched_shards: Vec<i32>,
    /// List of shard IDs that were unavailable during inspection.
    pub unavailable_shards: Vec<i32>,
}

#[derive(Debug, Clone, Serialize)]
/// Details regarding the inspection of a single shard.
pub struct VersionUsageShardInspection {
    /// The ID of the inspected shard.
    pub shard_id: i32,
    /// The inspection status of the shard.
    pub status: VersionUsageShardInspectionStatus,
    /// The number of matched groups found in the shard, if successfully inspected.
    pub matched_groups: Option<usize>,
    /// Any error message encountered during the shard's inspection.
    pub error: Option<String>,
}

type ShardObservation = shard_fanout::ShardObservation<VersionUsageShardRow>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct VersionUsageKey {
    workflow_name: String,
    change_id: String,
    recorded_version: u32,
}

#[derive(Debug)]
struct VersionUsageAccumulator {
    active_executions: i64,
    terminal_executions: i64,
    oldest_matching_started_at: DateTime<Utc>,
    newest_matching_started_at: DateTime<Utc>,
    matched_shards: BTreeSet<i32>,
}

/// Build the version-gate usage report without mutating workflow state.
///
/// # Errors
///
/// Returns an [`AutumnError`] when the query contains an unknown
/// `state_group` value.
pub async fn build_version_usage_report(
    api_state: &HarvestApiState,
    query: VersionUsageQuery,
) -> Result<VersionUsageReport, AutumnError> {
    let observed_at = Utc::now();
    let state_group = query
        .state_group
        .as_deref()
        .unwrap_or("all")
        .parse::<VersionExecutionStateGroup>()
        .map_err(map_error)?;
    let filters = VersionUsageFilters {
        workflow_name: query.workflow_name.clone(),
        change_id: query.change_id.clone(),
        recorded_version: query.recorded_version,
        state_group,
        shard_id: query.shard_id,
    };
    let expected_shards = expected_shards(api_state, query.shard_id);
    let pools = shard_fanout::pools_by_shard(api_state);

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
        VersionUsageReportFilters {
            workflow_name: query.workflow_name,
            change_id: query.change_id,
            recorded_version: query.recorded_version,
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

async fn observe_shard(
    shard_id: i32,
    pool: Option<DbPool>,
    mut filters: VersionUsageFilters,
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
    match load_version_usage(&mut conn, &filters).await {
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
    filters: VersionUsageReportFilters,
    observations: Vec<ShardObservation>,
) -> VersionUsageReport {
    let inspected_shards = observations
        .iter()
        .filter(|observation| observation.error.is_none())
        .map(|observation| observation.shard_id)
        .collect::<BTreeSet<_>>();
    let unavailable_shards = observations
        .iter()
        .filter(|observation| observation.error.is_some())
        .map(|observation| observation.shard_id)
        .collect::<BTreeSet<_>>();

    let mut rows = BTreeMap::<VersionUsageKey, VersionUsageAccumulator>::new();
    for observation in &observations {
        for row in &observation.rows {
            let key = VersionUsageKey {
                workflow_name: row.workflow_name.clone(),
                change_id: row.change_id.clone(),
                recorded_version: row.recorded_version,
            };
            rows.entry(key)
                .and_modify(|acc| merge_row(acc, row))
                .or_insert_with(|| accumulator_from_row(row));
        }
    }

    let items = rows
        .into_iter()
        .map(|(key, acc)| {
            row_from_accumulator(
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
        .map(|observation| VersionUsageShardInspection {
            shard_id: observation.shard_id,
            status: if observation.error.is_some() {
                VersionUsageShardInspectionStatus::Unavailable
            } else {
                VersionUsageShardInspectionStatus::Inspected
            },
            matched_groups: observation
                .error
                .as_ref()
                .map_or(Some(observation.rows.len()), |_| None),
            error: observation.error,
        })
        .collect::<Vec<_>>();
    let status = report_status(
        items.is_empty(),
        inspected_shards.is_empty(),
        unavailable_shards.is_empty(),
    );

    VersionUsageReport {
        status,
        observed_at,
        filters,
        items,
        shards,
    }
}

fn merge_row(acc: &mut VersionUsageAccumulator, row: &VersionUsageShardRow) {
    acc.active_executions += row.active_executions;
    acc.terminal_executions += row.terminal_executions;
    acc.oldest_matching_started_at = acc
        .oldest_matching_started_at
        .min(row.oldest_matching_started_at);
    acc.newest_matching_started_at = acc
        .newest_matching_started_at
        .max(row.newest_matching_started_at);
    acc.matched_shards.insert(row.shard_id);
}

fn accumulator_from_row(row: &VersionUsageShardRow) -> VersionUsageAccumulator {
    VersionUsageAccumulator {
        active_executions: row.active_executions,
        terminal_executions: row.terminal_executions,
        oldest_matching_started_at: row.oldest_matching_started_at,
        newest_matching_started_at: row.newest_matching_started_at,
        matched_shards: BTreeSet::from([row.shard_id]),
    }
}

fn row_from_accumulator(
    key: VersionUsageKey,
    acc: &VersionUsageAccumulator,
    inspected_shards: &BTreeSet<i32>,
    unavailable_shards: &BTreeSet<i32>,
    observed_at: DateTime<Utc>,
) -> VersionUsageReportRow {
    VersionUsageReportRow {
        workflow_name: key.workflow_name,
        change_id: key.change_id,
        recorded_version: key.recorded_version,
        active_executions: acc.active_executions,
        terminal_executions: acc.terminal_executions,
        oldest_matching_started_at: acc.oldest_matching_started_at,
        newest_matching_started_at: acc.newest_matching_started_at,
        oldest_matching_execution_age_secs: shard_fanout::age_secs(
            observed_at,
            acc.oldest_matching_started_at,
        ),
        newest_matching_execution_age_secs: shard_fanout::age_secs(
            observed_at,
            acc.newest_matching_started_at,
        ),
        shard_coverage: VersionUsageShardCoverage {
            inspected_shards: inspected_shards.iter().copied().collect(),
            matched_shards: acc.matched_shards.iter().copied().collect(),
            unavailable_shards: unavailable_shards.iter().copied().collect(),
        },
    }
}

const fn report_status(
    no_items: bool,
    no_inspected_shards: bool,
    no_unavailable_shards: bool,
) -> VersionUsageReportStatus {
    if no_inspected_shards {
        VersionUsageReportStatus::Unavailable
    } else if !no_unavailable_shards {
        VersionUsageReportStatus::Partial
    } else if no_items {
        VersionUsageReportStatus::NoMatches
    } else {
        VersionUsageReportStatus::Complete
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
    ) -> VersionUsageShardRow {
        VersionUsageShardRow {
            workflow_name: workflow_name.to_string(),
            change_id: change_id.to_string(),
            recorded_version,
            active_executions,
            terminal_executions,
            oldest_matching_started_at: at(100),
            newest_matching_started_at: at(200),
            shard_id,
        }
    }

    fn default_filters() -> VersionUsageReportFilters {
        VersionUsageReportFilters {
            workflow_name: None,
            change_id: None,
            recorded_version: None,
            state_group: VersionExecutionStateGroup::All,
            shard_id: None,
        }
    }

    #[test]
    fn report_status_distinguishes_no_matches_from_unavailable_shards() {
        let no_matches = build_report_from_observations(
            at(300),
            default_filters(),
            vec![ShardObservation {
                shard_id: 0,
                rows: Vec::new(),
                error: None,
            }],
        );
        assert_eq!(no_matches.status, VersionUsageReportStatus::NoMatches);

        let unavailable = build_report_from_observations(
            at(300),
            default_filters(),
            vec![ShardObservation {
                shard_id: 1,
                rows: Vec::new(),
                error: Some("connection refused".to_string()),
            }],
        );
        assert_eq!(unavailable.status, VersionUsageReportStatus::Unavailable);
        assert_eq!(unavailable.shards[0].shard_id, 1);
        assert_eq!(
            unavailable.shards[0].status,
            VersionUsageShardInspectionStatus::Unavailable
        );
    }

    #[test]
    fn no_matches_status_serializes_as_snake_case() {
        let value = serde_json::to_value(VersionUsageReportStatus::NoMatches)
            .expect("status should serialize");

        assert_eq!(value, serde_json::json!("no_matches"));
    }

    #[test]
    fn partial_report_keeps_rows_and_names_unavailable_shard() {
        let report = build_report_from_observations(
            at(300),
            default_filters(),
            vec![
                ShardObservation {
                    shard_id: 0,
                    rows: vec![shard_row(0, "billing", "tax_v2", 1, 1, 0)],
                    error: None,
                },
                ShardObservation {
                    shard_id: 1,
                    rows: Vec::new(),
                    error: Some("pool missing".to_string()),
                },
            ],
        );

        assert_eq!(report.status, VersionUsageReportStatus::Partial);
        assert_eq!(report.items.len(), 1);
        assert_eq!(report.items[0].active_executions, 1);
        assert_eq!(report.items[0].shard_coverage.matched_shards, vec![0]);
        assert_eq!(report.items[0].shard_coverage.unavailable_shards, vec![1]);
    }

    #[test]
    fn independent_change_ids_remain_separate_rows() {
        let report = build_report_from_observations(
            at(300),
            default_filters(),
            vec![ShardObservation {
                shard_id: 0,
                rows: vec![
                    shard_row(0, "billing", "tax_v2", 1, 1, 0),
                    shard_row(0, "billing", "invoice_v3", 2, 0, 1),
                ],
                error: None,
            }],
        );

        assert_eq!(report.status, VersionUsageReportStatus::Complete);
        let change_ids = report
            .items
            .iter()
            .map(|item| item.change_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(change_ids, vec!["invoice_v3", "tax_v2"]);
    }
}
