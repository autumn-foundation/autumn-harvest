//! Read-only retirement-check query for recorded workflow version-gate markers.
//!
//! Answers "is change id X safe to retire below version N?" by scanning
//! `harvest_events` for `MarkerRecorded` rows whose recorded version is below the
//! caller-supplied `min_safe_version` threshold and joining back to
//! `harvest_workflow_executions` for execution state and start timestamp.

use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Integer, Nullable, Text, Timestamptz};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::Serialize;
use uuid::Uuid;

use crate::error::{HarvestError, HarvestResult, database_error};
use crate::version_usage::VersionExecutionStateGroup;

/// Filters applied when reading retirement-check data from one shard.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RetirementCheckFilters {
    /// Limit results to this workflow name.
    pub workflow_name: Option<String>,
    /// Change id to inspect (maps to `MarkerRecorded` name `"version:{change_id}"`).
    pub change_id: String,
    /// Executions whose recorded version is **below** this value are considered
    /// to be taking the old (to-be-retired) branch.
    pub min_safe_version: u32,
    /// Which execution states to include.
    pub state_group: VersionExecutionStateGroup,
    /// Restrict to a single shard.
    pub shard_id: Option<i32>,
}

/// One grouped retirement-check row read from a single database shard.
///
/// A row exists for each `(workflow_name, recorded_version, shard_id)` triple
/// where `recorded_version < min_safe_version`.
///
/// When a non-identity [`PayloadCodec`] is configured, the `details` field of a
/// `MarkerRecorded` event is stored as a codec envelope object rather than a bare
/// integer.  Those rows cannot be decoded in SQL, so they are included
/// conservatively with `recorded_version = 0` — always below any meaningful
/// `min_safe_version`.  Operators should treat `recorded_version == 0` as
/// *version unknown (opaque codec envelope)* and investigate before retiring.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct RetirementCheckShardRow {
    pub workflow_name: String,
    pub change_id: String,
    pub recorded_version: u32,
    pub active_executions: i64,
    pub terminal_executions: i64,
    pub oldest_blocker_started_at: DateTime<Utc>,
    pub newest_blocker_started_at: DateTime<Utc>,
    /// Up to 10 active (non-terminal) execution IDs for manual investigation.
    pub sample_active_execution_ids: Vec<Uuid>,
    pub shard_id: i32,
}

#[derive(Debug, diesel::QueryableByName)]
struct RetirementCheckSqlRow {
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
    oldest_blocker_started_at: DateTime<Utc>,
    #[diesel(sql_type = Timestamptz)]
    newest_blocker_started_at: DateTime<Utc>,
    /// JSON array of UUID strings, e.g. `["uuid1","uuid2"]`.
    #[diesel(sql_type = Text)]
    sample_active_execution_ids: String,
    #[diesel(sql_type = Integer)]
    shard_id: i32,
}

/// Maximum number of sample blocker IDs returned per row.
const SAMPLE_BLOCKER_LIMIT: i64 = 10;

const RETIREMENT_CHECK_SQL: &str = r"
WITH version_markers AS (
    SELECT DISTINCT
        w.id AS workflow_exec_id,
        w.workflow_name,
        w.state,
        w.started_at,
        w.shard_id,
        substring(e.event_data #>> '{data,name}' FROM 9) AS change_id,
        CASE
            WHEN e.event_data #>> '{data,details,_harvest_codec_envelope}' IS NOT NULL
            THEN 0
            ELSE (e.event_data #>> '{data,details}')::BIGINT
        END AS recorded_version
    FROM harvest_events e
    INNER JOIN harvest_workflow_executions w
        ON w.id = e.workflow_exec_id
    WHERE e.event_type = 'MarkerRecorded'
      AND e.event_data #>> '{data,name}' = $1
      AND (
          -- Plain numeric details: must be a valid u32 below the safe threshold.
          (
              e.event_data #>> '{data,details}' ~ '^[0-9]{1,19}$'
              AND (e.event_data #>> '{data,details}')::NUMERIC <= 4294967295
              AND (e.event_data #>> '{data,details}')::BIGINT < $2
          )
          OR
          -- Codec-envelope details: version is opaque; include conservatively
          -- (mapped to recorded_version = 0 above) so they are never silently
          -- excluded from the blocker set.
          e.event_data #>> '{data,details,_harvest_codec_envelope}' IS NOT NULL
      )
      AND ($3::TEXT IS NULL OR w.workflow_name = $3::TEXT)
      AND (
          $4::TEXT = 'all'
          OR (
              $4::TEXT = 'active'
              AND w.state NOT IN (
                  'COMPLETED', 'FAILED', 'CANCELLED',
                  'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED'
              )
          )
          OR (
              $4::TEXT = 'terminal'
              AND w.state IN (
                  'COMPLETED', 'FAILED', 'CANCELLED',
                  'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED'
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
            'COMPLETED', 'FAILED', 'CANCELLED',
            'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED'
        )
    )::BIGINT AS active_executions,
    COUNT(*) FILTER (
        WHERE state IN (
            'COMPLETED', 'FAILED', 'CANCELLED',
            'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED'
        )
    )::BIGINT AS terminal_executions,
    MIN(started_at) AS oldest_blocker_started_at,
    MAX(started_at) AS newest_blocker_started_at,
    COALESCE(
        (
            SELECT json_agg(eid)
            FROM (
                SELECT vm2.workflow_exec_id::TEXT AS eid
                FROM version_markers vm2
                WHERE vm2.workflow_name = vm.workflow_name
                  AND vm2.recorded_version = vm.recorded_version
                  AND vm2.shard_id = vm.shard_id
                  AND vm2.state NOT IN (
                      'COMPLETED', 'FAILED', 'CANCELLED',
                      'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED'
                  )
                ORDER BY vm2.started_at
                LIMIT $6
            ) sub
        )::TEXT,
        '[]'
    ) AS sample_active_execution_ids,
    shard_id::INT4 AS shard_id
FROM version_markers vm
GROUP BY workflow_name, change_id, recorded_version, shard_id
ORDER BY workflow_name, change_id, recorded_version, shard_id
";

/// Load retirement-check rows from a single shard without mutating state.
///
/// Returns one row per `(workflow_name, recorded_version, shard_id)` triple
/// where `recorded_version < filters.min_safe_version`.  An empty `Vec`
/// signals that no old-version markers exist on this shard under the given
/// filters — a safe result when combined with a fully-inspected shard set.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the query fails.
pub async fn load_retirement_check(
    conn: &mut AsyncPgConnection,
    filters: &RetirementCheckFilters,
) -> HarvestResult<Vec<RetirementCheckShardRow>> {
    let marker_name = format!("version:{}", filters.change_id);
    let min_safe_version = i64::from(filters.min_safe_version);
    let version_filter = min_safe_version;
    let rows = diesel::sql_query(RETIREMENT_CHECK_SQL)
        .bind::<Text, _>(marker_name)
        .bind::<BigInt, _>(version_filter)
        .bind::<Nullable<Text>, _>(filters.workflow_name.as_deref())
        .bind::<Text, _>(filters.state_group.as_str())
        .bind::<Nullable<Integer>, _>(filters.shard_id)
        .bind::<BigInt, _>(SAMPLE_BLOCKER_LIMIT)
        .load::<RetirementCheckSqlRow>(conn)
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
            let sample_active_execution_ids = parse_sample_ids(&row.sample_active_execution_ids)
                .map_err(|e| HarvestError::Database(format!("failed to parse sample ids: {e}")))?;
            Ok(RetirementCheckShardRow {
                workflow_name: row.workflow_name,
                change_id: row.change_id,
                recorded_version,
                active_executions: row.active_executions,
                terminal_executions: row.terminal_executions,
                oldest_blocker_started_at: row.oldest_blocker_started_at,
                newest_blocker_started_at: row.newest_blocker_started_at,
                sample_active_execution_ids,
                shard_id: row.shard_id,
            })
        })
        .collect()
}

fn parse_sample_ids(json_text: &str) -> Result<Vec<Uuid>, String> {
    let value: serde_json::Value = serde_json::from_str(json_text).map_err(|e| e.to_string())?;
    let arr = value
        .as_array()
        .ok_or_else(|| "expected JSON array".to_string())?;
    arr.iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| "expected string element".to_string())
                .and_then(|s| Uuid::parse_str(s).map_err(|e| e.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sample_ids_handles_empty_array() {
        let ids = parse_sample_ids("[]").expect("empty array is valid");
        assert!(ids.is_empty());
    }

    #[test]
    fn parse_sample_ids_parses_uuid_strings() {
        let uuid = Uuid::new_v4();
        let json = format!("[\"{uuid}\"]");
        let ids = parse_sample_ids(&json).expect("array of one UUID");
        assert_eq!(ids, vec![uuid]);
    }

    #[test]
    fn parse_sample_ids_rejects_non_uuid() {
        let result = parse_sample_ids("[\"not-a-uuid\"]");
        assert!(result.is_err());
    }

    #[test]
    fn parse_sample_ids_rejects_non_array() {
        let result = parse_sample_ids("\"single-string\"");
        assert!(result.is_err());
    }

    #[test]
    fn retirement_check_filters_default_state_group() {
        let filters = RetirementCheckFilters {
            workflow_name: None,
            change_id: "my_change".to_string(),
            min_safe_version: 2,
            state_group: VersionExecutionStateGroup::All,
            shard_id: None,
        };
        assert_eq!(filters.state_group.as_str(), "all");
    }

    #[test]
    fn sql_contains_codec_envelope_branch() {
        // Verify that the SQL guard against opaque codec-envelope details is
        // present so that non-identity PayloadCodec deployments are never
        // silently excluded from the blocker set.
        assert!(
            RETIREMENT_CHECK_SQL.contains("_harvest_codec_envelope"),
            "SQL must detect codec-envelope details to avoid false-safe reports"
        );
    }
}
