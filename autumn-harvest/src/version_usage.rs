//! Read-only inventory for recorded workflow version-gate markers.

use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Integer, Nullable, Text, Timestamptz};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};

use crate::error::{HarvestError, HarvestResult, database_error};

/// Execution-state scope for a version-gate usage report.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VersionExecutionStateGroup {
    /// Include only executions that may still replay code.
    Active,
    /// Include only terminal executions.
    Terminal,
    /// Include active and terminal executions.
    #[default]
    All,
}

impl VersionExecutionStateGroup {
    /// Return the string representation of the state group.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Terminal => "terminal",
            Self::All => "all",
        }
    }
}

impl std::str::FromStr for VersionExecutionStateGroup {
    type Err = HarvestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "active" => Ok(Self::Active),
            "terminal" => Ok(Self::Terminal),
            "all" => Ok(Self::All),
            other => Err(HarvestError::Config(format!(
                "unknown version usage state_group '{other}'"
            ))),
        }
    }
}

/// Filters applied when reading version-gate marker usage from one shard.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct VersionUsageFilters {
    /// Filter results to a specific workflow name, if any.
    pub workflow_name: Option<String>,
    /// Filter results to a specific change ID, if any.
    pub change_id: Option<String>,
    /// Filter results to a specific recorded version, if any.
    pub recorded_version: Option<u32>,
    /// Filter results by the execution state group (active, terminal, or all).
    pub state_group: VersionExecutionStateGroup,
    /// Filter results to a specific logical database shard, if any.
    pub shard_id: Option<i32>,
}

/// One grouped version-gate usage row read from a single database shard.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct VersionUsageShardRow {
    /// The name of the workflow using the version gate.
    pub workflow_name: String,
    /// The change ID associated with the version gate.
    pub change_id: String,
    /// The version number recorded in the marker.
    pub recorded_version: u32,
    /// The count of active executions with this version.
    pub active_executions: i64,
    /// The count of terminal executions with this version.
    pub terminal_executions: i64,
    /// The start time of the oldest matching execution.
    pub oldest_matching_started_at: DateTime<Utc>,
    /// The start time of the newest matching execution.
    pub newest_matching_started_at: DateTime<Utc>,
    /// The ID of the database shard where these executions reside.
    pub shard_id: i32,
}

#[derive(Debug, diesel::QueryableByName)]
struct VersionUsageSqlRow {
    #[diesel(sql_type = Text)]
    workflow_name: String,
    #[diesel(sql_type = Text)]
    change_id: String,
    #[diesel(sql_type = BigInt)]
    recorded_version: i64,
    #[diesel(sql_type = BigInt)]
    active_executions: i64,
    #[diesel(sql_type = BigInt)]
    terminal_executions: i64,
    #[diesel(sql_type = Timestamptz)]
    oldest_matching_started_at: DateTime<Utc>,
    #[diesel(sql_type = Timestamptz)]
    newest_matching_started_at: DateTime<Utc>,
    #[diesel(sql_type = Integer)]
    shard_id: i32,
}

const VERSION_USAGE_SQL: &str = r"
WITH version_markers AS (
    SELECT DISTINCT
        w.id AS workflow_exec_id,
        w.workflow_name,
        w.state,
        w.started_at,
        w.shard_id,
        substring(e.event_data #>> '{data,name}' FROM 9) AS change_id,
        (e.event_data #>> '{data,details}')::BIGINT AS recorded_version
    FROM harvest_events e
    INNER JOIN harvest_workflow_executions w
        ON w.id = e.workflow_exec_id
    WHERE e.event_type = 'MarkerRecorded'
      AND e.event_data #>> '{data,name}' LIKE 'version:%'
      AND e.event_data #>> '{data,details}' ~ '^[0-9]{1,19}$'
      AND (e.event_data #>> '{data,details}')::NUMERIC <= 4294967295
      AND ($1::TEXT IS NULL OR w.workflow_name = $1::TEXT)
      AND ($2::TEXT IS NULL OR substring(e.event_data #>> '{data,name}' FROM 9) = $2::TEXT)
      AND ($3::BIGINT IS NULL OR (e.event_data #>> '{data,details}')::BIGINT = $3::BIGINT)
      AND (
          $4::TEXT = 'all'
          OR (
              $4::TEXT = 'active'
              AND w.state NOT IN (
                  'COMPLETED',
                  'FAILED',
                  'CANCELLED',
                  'TIMED_OUT',
                  'CONTINUED_AS_NEW',
                  'TERMINATED'
              )
          )
          OR (
              $4::TEXT = 'terminal'
              AND w.state IN (
                  'COMPLETED',
                  'FAILED',
                  'CANCELLED',
                  'TIMED_OUT',
                  'CONTINUED_AS_NEW',
                  'TERMINATED'
              )
          )
      )
      AND ($5::INT4 IS NULL OR w.shard_id = $5::INT4)
)
SELECT
    workflow_name::TEXT AS workflow_name,
    change_id::TEXT AS change_id,
    recorded_version::BIGINT AS recorded_version,
    COUNT(*) FILTER (
        WHERE state NOT IN (
            'COMPLETED',
            'FAILED',
            'CANCELLED',
            'TIMED_OUT',
            'CONTINUED_AS_NEW',
            'TERMINATED'
        )
    )::BIGINT AS active_executions,
    COUNT(*) FILTER (
        WHERE state IN (
            'COMPLETED',
            'FAILED',
            'CANCELLED',
            'TIMED_OUT',
            'CONTINUED_AS_NEW',
            'TERMINATED'
        )
    )::BIGINT AS terminal_executions,
    MIN(started_at) AS oldest_matching_started_at,
    MAX(started_at) AS newest_matching_started_at,
    shard_id::INT4 AS shard_id
FROM version_markers
GROUP BY workflow_name, change_id, recorded_version, shard_id
ORDER BY workflow_name, change_id, recorded_version, shard_id
";

/// Load grouped version-marker usage from a single shard without mutating state.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the database query fails.
pub async fn load_version_usage(
    conn: &mut AsyncPgConnection,
    filters: &VersionUsageFilters,
) -> HarvestResult<Vec<VersionUsageShardRow>> {
    let version_filter = filters.recorded_version.map(i64::from);
    let rows = diesel::sql_query(VERSION_USAGE_SQL)
        .bind::<Nullable<Text>, _>(filters.workflow_name.as_deref())
        .bind::<Nullable<Text>, _>(filters.change_id.as_deref())
        .bind::<Nullable<BigInt>, _>(version_filter)
        .bind::<Text, _>(filters.state_group.as_str())
        .bind::<Nullable<Integer>, _>(filters.shard_id)
        .load::<VersionUsageSqlRow>(conn)
        .await
        .map_err(database_error)?;

    rows.into_iter()
        .map(|row| {
            let recorded_version = u32::try_from(row.recorded_version).map_err(|_| {
                HarvestError::Database(format!(
                    "recorded version {} is outside u32 range",
                    row.recorded_version
                ))
            })?;
            Ok(VersionUsageShardRow {
                workflow_name: row.workflow_name,
                change_id: row.change_id,
                recorded_version,
                active_executions: row.active_executions,
                terminal_executions: row.terminal_executions,
                oldest_matching_started_at: row.oldest_matching_started_at,
                newest_matching_started_at: row.newest_matching_started_at,
                shard_id: row.shard_id,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_execution_state_group_as_str() {
        assert_eq!(VersionExecutionStateGroup::Active.as_str(), "active");
        assert_eq!(VersionExecutionStateGroup::Terminal.as_str(), "terminal");
        assert_eq!(VersionExecutionStateGroup::All.as_str(), "all");
    }

    #[test]
    fn test_version_execution_state_group_from_str() {
        use std::str::FromStr;

        assert_eq!(
            VersionExecutionStateGroup::from_str("active").expect("should succeed"),
            VersionExecutionStateGroup::Active
        );
        assert_eq!(
            VersionExecutionStateGroup::from_str("Active").expect("should succeed"),
            VersionExecutionStateGroup::Active
        );
        assert_eq!(
            VersionExecutionStateGroup::from_str("terminal").expect("should succeed"),
            VersionExecutionStateGroup::Terminal
        );
        assert_eq!(
            VersionExecutionStateGroup::from_str("TERMINAL").expect("should succeed"),
            VersionExecutionStateGroup::Terminal
        );
        assert_eq!(
            VersionExecutionStateGroup::from_str("all").expect("should succeed"),
            VersionExecutionStateGroup::All
        );
        assert_eq!(
            VersionExecutionStateGroup::from_str("ALL").expect("should succeed"),
            VersionExecutionStateGroup::All
        );

        let err = VersionExecutionStateGroup::from_str("invalid").expect_err("should fail");
        assert!(matches!(err, HarvestError::Config(_)));
    }
}
