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

/// Defines the filters used when querying version gate usage via the management API.
///
/// This structure is typically deserialized from query parameters in a `GET` request to
/// `/admin/version-gates/usage`. It allows operators to narrow down the usage report
/// to specific workflows, changes, or database shards.
///
/// **Why does this exist?**
/// When deprecating a version gate, you need to know if any active executions are still
/// relying on the old behavior. This query provides the knobs to target that search
/// efficiently across a distributed system.
///
/// ## Examples
///
/// Parsing a query string into a `VersionUsageQuery`:
///
/// ```rust
/// use autumn_harvest_plugin::version_usage::VersionUsageQuery;
///
/// let query_str = "workflow_name=billing&state_group=active";
/// let query: VersionUsageQuery = serde_urlencoded::from_str(query_str).unwrap();
///
/// assert_eq!(query.workflow_name.as_deref(), Some("billing"));
/// assert_eq!(query.state_group.as_deref(), Some("active"));
/// assert_eq!(query.shard_id, None);
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct VersionUsageQuery {
    /// Restricts the report to a specific workflow name (e.g. `"invoice_processing"`).
    /// If omitted, usage across all registered workflows is returned.
    pub workflow_name: Option<String>,
    /// Restricts the report to a specific version gate identifier (e.g. `"tax_calculation_v2"`).
    pub change_id: Option<String>,
    /// Restricts the report to executions that recorded a specific version number for the gate.
    pub recorded_version: Option<u32>,
    /// Defines which execution states to include in the aggregated counts.
    /// Accepted values: `"active"`, `"terminal"`, or `"all"`. Defaults to `"all"`.
    pub state_group: Option<String>,
    /// Restricts the database scan to a single physical shard.
    /// Useful for debugging connection issues or localized usage spikes.
    pub shard_id: Option<i32>,
}

/// Represents the overall success and completeness of a distributed version usage query.
///
/// **Why does this exist?**
/// Because Autumn Harvest shards execution data, a single report might be constructed
/// from multiple database instances. If one database is unreachable, the report is
/// "partial" rather than completely failed, allowing operators to still see *some* data.
/// This enum strictly defines how reliable the returned rows are.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest_plugin::version_usage::VersionUsageReportStatus;
///
/// let status = VersionUsageReportStatus::Partial;
///
/// match status {
///     VersionUsageReportStatus::Complete => println!("Data is 100% accurate"),
///     VersionUsageReportStatus::Partial => println!("Warning: Some shards failed!"),
///     _ => {}
/// }
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionUsageReportStatus {
    /// Every expected shard was successfully queried, and at least one matching row was found.
    Complete,
    /// Every expected shard was successfully queried, but zero rows matched the filters.
    NoMatches,
    /// At least one shard failed to respond (e.g. connection timeout), but others succeeded.
    /// The data in the report is incomplete and should not be used for strict retirement decisions.
    Partial,
    /// Every single shard failed to respond. No usage data could be retrieved.
    Unavailable,
}

/// Represents the query success status for a single database shard.
///
/// **Why does this exist?**
/// When a `VersionUsageReportStatus` is `Partial`, this enum is used in the shard
/// breakdown to explicitly identify *which* shards failed, allowing operators to
/// investigate specific database issues.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest_plugin::version_usage::VersionUsageShardInspectionStatus;
///
/// let status = VersionUsageShardInspectionStatus::Inspected;
/// assert_eq!(status, VersionUsageShardInspectionStatus::Inspected);
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VersionUsageShardInspectionStatus {
    /// The shard's database pool was acquired and the usage query executed successfully.
    Inspected,
    /// The query failed, typically due to a connection error or missing connection pool.
    Unavailable,
}

/// The top-level payload returned by the `/admin/version-gates/usage` endpoint.
///
/// **Why does this exist?**
/// Rather than just returning an array of raw database rows, this struct encapsulates
/// the context of the query (filters, timestamp) and the health of the underlying
/// distributed system (shard statuses). This prevents operators from making dangerous
/// decisions based on incomplete data.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest_plugin::version_usage::{
///     VersionUsageReport, VersionUsageReportStatus, VersionUsageReportFilters,
/// };
/// use autumn_harvest::version_usage::VersionExecutionStateGroup;
/// use chrono::Utc;
///
/// let report = VersionUsageReport {
///     status: VersionUsageReportStatus::NoMatches,
///     observed_at: Utc::now(),
///     filters: VersionUsageReportFilters {
///         workflow_name: None,
///         change_id: None,
///         recorded_version: None,
///         state_group: VersionExecutionStateGroup::Active,
///         shard_id: None,
///     },
///     items: vec![],
///     shards: vec![],
/// };
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct VersionUsageReport {
    /// Indicates whether all shards were successfully queried, or if the report is partial.
    pub status: VersionUsageReportStatus,
    /// The exact timestamp when this report was built. Useful for determining data staleness.
    pub observed_at: DateTime<Utc>,
    /// An echo of the query filters used to generate this report.
    pub filters: VersionUsageReportFilters,
    /// The aggregated usage rows. If a specific version gate is used across multiple shards,
    /// it is aggregated into a single row here.
    pub items: Vec<VersionUsageReportRow>,
    /// A breakdown of the health and inspection status of every configured shard.
    pub shards: Vec<VersionUsageShardInspection>,
}

/// Echoes the filters that were actually applied to generate a specific report.
///
/// **Why does this exist?**
/// It ensures that whoever views a serialized JSON report has the full context of how
/// the data was queried, avoiding mistakes where a filtered report is misinterpreted
/// as a global report.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest_plugin::version_usage::VersionUsageReportFilters;
/// use autumn_harvest::version_usage::VersionExecutionStateGroup;
///
/// let filters = VersionUsageReportFilters {
///     workflow_name: Some("billing".to_string()),
///     change_id: None,
///     recorded_version: None,
///     state_group: VersionExecutionStateGroup::All,
///     shard_id: None,
/// };
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct VersionUsageReportFilters {
    /// The workflow name that was filtered on, if any.
    pub workflow_name: Option<String>,
    /// The specific change id that was queried, if any.
    pub change_id: Option<String>,
    /// The exact recorded version that was targeted, if any.
    pub recorded_version: Option<u32>,
    /// The execution state group (e.g. `Active`, `Terminal`) targeted by the query.
    pub state_group: VersionExecutionStateGroup,
    /// The specific physical shard id targeted, if any.
    pub shard_id: Option<i32>,
}

/// Represents an aggregated usage count for a single `(workflow_name, change_id, version)` group.
///
/// **Why does this exist?**
/// Rather than dumping thousands of individual executions that hit a version gate, this struct
/// aggregates the data so operators can see high-level metrics (like total active executions
/// and oldest age) to make a safe retirement decision.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest_plugin::version_usage::{
///     VersionUsageReportRow, VersionUsageShardCoverage
/// };
/// use chrono::Utc;
///
/// let row = VersionUsageReportRow {
///     workflow_name: "billing".to_string(),
///     change_id: "tax_v2".to_string(),
///     recorded_version: 1,
///     active_executions: 45,
///     terminal_executions: 1002,
///     oldest_matching_started_at: Utc::now(),
///     newest_matching_started_at: Utc::now(),
///     oldest_matching_execution_age_secs: 10,
///     newest_matching_execution_age_secs: 1,
///     shard_coverage: VersionUsageShardCoverage {
///         inspected_shards: vec![0],
///         matched_shards: vec![0],
///         unavailable_shards: vec![],
///     },
/// };
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct VersionUsageReportRow {
    /// The unique workflow identifier this gate was triggered in.
    pub workflow_name: String,
    /// The string identifier for the version gate (e.g. `"stripe_api_v2"`).
    pub change_id: String,
    /// The specific integer version number that was recorded by the executions.
    pub recorded_version: u32,
    /// The number of currently running executions that recorded this version.
    /// This is the most critical metric for safe retirement.
    pub active_executions: i64,
    /// The number of completed or failed executions that recorded this version.
    pub terminal_executions: i64,
    /// The timestamp of the oldest execution included in this row.
    pub oldest_matching_started_at: DateTime<Utc>,
    /// The timestamp of the most recent execution included in this row.
    pub newest_matching_started_at: DateTime<Utc>,
    /// Convenience field showing the age of the oldest execution in seconds.
    pub oldest_matching_execution_age_secs: i64,
    /// Convenience field showing the age of the newest execution in seconds.
    pub newest_matching_execution_age_secs: i64,
    /// Identifies exactly which shards contributed to this aggregated row.
    pub shard_coverage: VersionUsageShardCoverage,
}

/// Tracks which physical shards contributed to a specific aggregated row.
///
/// **Why does this exist?**
/// If an operator sees `active_executions: 0` for a version gate, they might think
/// it's safe to retire. However, if `unavailable_shards` is non-empty, that zero count
/// is dangerous to act on because the missing shards might contain active executions.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest_plugin::version_usage::VersionUsageShardCoverage;
///
/// let coverage = VersionUsageShardCoverage {
///     inspected_shards: vec![0, 1],
///     matched_shards: vec![1],
///     unavailable_shards: vec![2],
/// };
///
/// assert!(!coverage.unavailable_shards.is_empty(), "Unsafe to make decisions!");
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct VersionUsageShardCoverage {
    /// The physical shard IDs that successfully responded to the query.
    pub inspected_shards: Vec<i32>,
    /// The physical shard IDs that actually contained matching rows for this group.
    pub matched_shards: Vec<i32>,
    /// The physical shard IDs that could not be queried (e.g. database down).
    pub unavailable_shards: Vec<i32>,
}

/// A health and summary report for a single database shard during the global query.
///
/// **Why does this exist?**
/// Exposing connection errors and individual shard row counts allows operators
/// to quickly debug misconfigured or overloaded database instances without digging
/// through application logs.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest_plugin::version_usage::{
///     VersionUsageShardInspection, VersionUsageShardInspectionStatus
/// };
///
/// let inspection = VersionUsageShardInspection {
///     shard_id: 2,
///     status: VersionUsageShardInspectionStatus::Unavailable,
///     matched_groups: None,
///     error: Some("connection pool exhausted".to_string()),
/// };
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct VersionUsageShardInspection {
    /// The physical identifier of the shard being inspected.
    pub shard_id: i32,
    /// Indicates whether the query to this specific shard succeeded or failed.
    pub status: VersionUsageShardInspectionStatus,
    /// If the query succeeded, the number of distinct `(workflow, change_id, version)`
    /// groups returned by this shard.
    pub matched_groups: Option<usize>,
    /// If the query failed, the stringified error reason (e.g. timeout, auth failure).
    pub error: Option<String>,
}

#[derive(Debug)]
struct ShardObservation {
    shard_id: i32,
    rows: Vec<VersionUsageShardRow>,
    error: Option<String>,
}

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
        oldest_matching_execution_age_secs: age_secs(observed_at, acc.oldest_matching_started_at),
        newest_matching_execution_age_secs: age_secs(observed_at, acc.newest_matching_started_at),
        shard_coverage: VersionUsageShardCoverage {
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
