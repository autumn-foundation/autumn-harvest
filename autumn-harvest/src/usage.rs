//! Read-only per-tenant/per-workflow usage aggregation (issue #596).
//!
//! The *historical* companion to the *point-in-time* `GET /admin/concurrency`
//! endpoint (issue #247): answers "how much did tenant X actually consume
//! between two dates?" by aggregating already-durable data
//! (`harvest_workflow_executions` + `harvest_events`) over a caller-supplied
//! time window, grouped by `workflow_name` or by a `search_attrs` JSON key
//! (e.g. a tenant id). Read-only by construction: no new `WorkflowEvent`
//! variant, no migration, no replay-determinism impact.
//!
//! ## Metric semantics
//!
//! - `workflow_starts`: executions whose `started_at` falls in `[from, to]`.
//! - `completed`/`failed`/`cancelled`/`timed_out`: executions whose
//!   `completed_at` falls in `[from, to]` and whose terminal `state` matches.
//!   Each terminal transition is counted exactly once, in the window it
//!   actually occurred — the chargeback-consistent choice. `TERMINATED` and
//!   `CONTINUED_AS_NEW` are intentionally not broken out (the issue's AC
//!   lists exactly these four terminal buckets).
//! - `activity_executions`: count of `ActivityStarted` events in the window
//!   (one per dispatch attempt — retries reuse the same `activity_id` but
//!   each attempt appends a fresh `ActivityStarted`).
//! - `activity_executions_failed`: count of terminal `ActivityFailed` +
//!   `ActivityTimedOut` events in the window (non-final retry attempts do
//!   not append an event, so this is exhausted-retry-or-timeout only).
//! - `activity_compute_seconds`: for each activity whose terminal event
//!   (`ActivityCompleted`/`ActivityFailed`/`ActivityTimedOut`) falls in the
//!   window, the wall-clock span from that activity's most recent
//!   `ActivityStarted` (its final attempt) to the terminal event —
//!   `harvest_events.timestamp` deltas, since event payloads carry no
//!   timestamps of their own. Retry backoff wall time between earlier
//!   attempts is excluded by construction (only the final attempt's span is
//!   summed).
//!
//! Local activities (no `ActivityStarted`/worker compute) and
//! externally-completed activities (`ActivityAwaitingExternal` and
//! siblings) are excluded from the activity counters — they never emit
//! `ActivityStarted`, so they naturally fall out of every query above.

use chrono::{DateTime, Utc};
#[cfg(feature = "db")]
use diesel::sql_types::{BigInt, Double, Integer, Nullable, Text, Timestamptz};
#[cfg(feature = "db")]
use diesel_async::AsyncPgConnection;
#[cfg(feature = "db")]
use diesel_async::RunQueryDsl;

#[cfg(feature = "db")]
use crate::error::{HarvestResult, database_error};

/// Explicit bucket for executions grouped by a `search_attr:<key>` that some
/// executions lack — never silently dropped.
pub const UNATTRIBUTED_GROUP: &str = "(unattributed)";

/// Default ceiling on the `[from, to]` usage window: 90 days.
#[must_use]
pub const fn default_usage_window_ceiling() -> std::time::Duration {
    std::time::Duration::from_secs(90 * 24 * 60 * 60)
}

/// How `GET /admin/usage` groups executions (issue #596).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageGroupBy {
    /// Group by `harvest_workflow_executions.workflow_name`.
    WorkflowName,
    /// Group by a key inside the `search_attrs` JSONB object (e.g. tenant id).
    /// Executions missing the key are bucketed under [`UNATTRIBUTED_GROUP`].
    SearchAttr(String),
}

impl UsageGroupBy {
    /// Wire form used in the `group_by` query parameter.
    #[must_use]
    pub fn as_wire(&self) -> String {
        match self {
            Self::WorkflowName => "workflow_name".to_string(),
            Self::SearchAttr(key) => format!("search_attr:{key}"),
        }
    }

    /// Parse the `group_by` wire form.
    ///
    /// `"workflow_name"` (also the empty string, for a friendly default) maps
    /// to [`Self::WorkflowName`]. `"search_attr:<key>"` maps to
    /// [`Self::SearchAttr`] with the key trimmed; an empty key is rejected.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message for an unknown dimension or an empty
    /// `search_attr:` key.
    pub fn from_wire(value: &str) -> Result<Self, String> {
        let trimmed = value.trim();
        if trimmed.is_empty() || trimmed == "workflow_name" {
            return Ok(Self::WorkflowName);
        }
        if let Some(key) = trimmed.strip_prefix("search_attr:") {
            let key = key.trim();
            if key.is_empty() {
                return Err(
                    "invalid group_by 'search_attr:'; a search_attr key is required, e.g. \
                     'search_attr:tenant_id'"
                        .to_string(),
                );
            }
            return Ok(Self::SearchAttr(key.to_string()));
        }
        Err(format!(
            "unknown group_by '{trimmed}'; expected 'workflow_name' or 'search_attr:<key>'"
        ))
    }

    /// The `search_attrs` key this dimension extracts, if any.
    #[must_use]
    pub const fn search_attr_key(&self) -> Option<&str> {
        match self {
            Self::WorkflowName => None,
            Self::SearchAttr(key) => Some(key.as_str()),
        }
    }
}

/// Filters for one shard's usage aggregation query.
#[derive(Debug, Clone)]
pub struct UsageQuery {
    pub group_by: UsageGroupBy,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

/// One grouped usage row read from a single database shard (issue #596).
#[derive(Debug, Clone, PartialEq)]
pub struct UsageShardRow {
    pub group: String,
    pub workflow_starts: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
    pub timed_out: i64,
    pub activity_executions: i64,
    pub activity_executions_failed: i64,
    pub activity_compute_seconds: f64,
}

#[cfg(feature = "db")]
#[derive(Debug, diesel::QueryableByName)]
struct UsageSqlRow {
    #[diesel(sql_type = Text)]
    grp: String,
    #[diesel(sql_type = BigInt)]
    workflow_starts: i64,
    #[diesel(sql_type = BigInt)]
    completed: i64,
    #[diesel(sql_type = BigInt)]
    failed: i64,
    #[diesel(sql_type = BigInt)]
    cancelled: i64,
    #[diesel(sql_type = BigInt)]
    timed_out: i64,
    #[diesel(sql_type = BigInt)]
    activity_executions: i64,
    #[diesel(sql_type = BigInt)]
    activity_executions_failed: i64,
    #[diesel(sql_type = Double)]
    activity_compute_seconds: f64,
}

// Shared group-key expression: `workflow_name` when `$2` (the search_attr
// key) is NULL, else `COALESCE(search_attrs ->> $2, '(unattributed)')`.
#[cfg(feature = "db")]
const GROUP_KEY_EXPR: &str = "\
    CASE WHEN $2::TEXT IS NULL THEN w.workflow_name \
    ELSE COALESCE(w.search_attrs ->> $2::TEXT, '(unattributed)') END";

#[cfg(feature = "db")]
fn usage_sql() -> String {
    format!(
        r"
WITH execution_counts AS (
    SELECT
        {GROUP_KEY_EXPR} AS grp,
        COUNT(*) FILTER (WHERE w.started_at BETWEEN $3 AND $4)::BIGINT AS workflow_starts,
        COUNT(*) FILTER (
            WHERE w.state = 'COMPLETED' AND w.completed_at BETWEEN $3 AND $4
        )::BIGINT AS completed,
        COUNT(*) FILTER (
            WHERE w.state = 'FAILED' AND w.completed_at BETWEEN $3 AND $4
        )::BIGINT AS failed,
        COUNT(*) FILTER (
            WHERE w.state = 'CANCELLED' AND w.completed_at BETWEEN $3 AND $4
        )::BIGINT AS cancelled,
        COUNT(*) FILTER (
            WHERE w.state = 'TIMED_OUT' AND w.completed_at BETWEEN $3 AND $4
        )::BIGINT AS timed_out
    FROM harvest_workflow_executions w
    WHERE w.shard_id = $1::INT4
      AND (
          w.started_at BETWEEN $3 AND $4
          OR w.completed_at BETWEEN $3 AND $4
      )
    GROUP BY 1
),
activity_counts AS (
    SELECT
        {GROUP_KEY_EXPR} AS grp,
        COUNT(*) FILTER (WHERE e.event_type = 'ActivityStarted')::BIGINT AS activity_executions,
        COUNT(*) FILTER (
            WHERE e.event_type IN ('ActivityFailed', 'ActivityTimedOut')
        )::BIGINT AS activity_executions_failed
    FROM harvest_events e
    INNER JOIN harvest_workflow_executions w ON w.id = e.workflow_exec_id
    WHERE w.shard_id = $1::INT4
      AND e.event_type IN ('ActivityStarted', 'ActivityFailed', 'ActivityTimedOut')
      AND e.timestamp BETWEEN $3 AND $4
    GROUP BY 1
),
activity_terminal AS (
    SELECT
        e.workflow_exec_id,
        e.event_data #>> '{{data,activity_id}}' AS activity_id,
        e.timestamp AS terminal_at
    FROM harvest_events e
    WHERE e.event_type IN ('ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut')
      AND e.timestamp BETWEEN $3 AND $4
),
activity_compute AS (
    SELECT
        {GROUP_KEY_EXPR} AS grp,
        COALESCE(
            SUM(EXTRACT(EPOCH FROM (t.terminal_at - s.last_started_at))),
            0
        )::DOUBLE PRECISION AS activity_compute_seconds
    FROM activity_terminal t
    INNER JOIN harvest_workflow_executions w ON w.id = t.workflow_exec_id
    INNER JOIN LATERAL (
        SELECT MAX(e2.timestamp) AS last_started_at
        FROM harvest_events e2
        WHERE e2.workflow_exec_id = t.workflow_exec_id
          AND e2.event_type = 'ActivityStarted'
          AND e2.event_data #>> '{{data,activity_id}}' = t.activity_id
          AND e2.timestamp <= t.terminal_at
    ) s ON s.last_started_at IS NOT NULL
    WHERE w.shard_id = $1::INT4
    GROUP BY 1
)
SELECT
    COALESCE(ec.grp, ac.grp, cp.grp)::TEXT AS grp,
    COALESCE(ec.workflow_starts, 0)::BIGINT AS workflow_starts,
    COALESCE(ec.completed, 0)::BIGINT AS completed,
    COALESCE(ec.failed, 0)::BIGINT AS failed,
    COALESCE(ec.cancelled, 0)::BIGINT AS cancelled,
    COALESCE(ec.timed_out, 0)::BIGINT AS timed_out,
    COALESCE(ac.activity_executions, 0)::BIGINT AS activity_executions,
    COALESCE(ac.activity_executions_failed, 0)::BIGINT AS activity_executions_failed,
    COALESCE(cp.activity_compute_seconds, 0)::DOUBLE PRECISION AS activity_compute_seconds
FROM execution_counts ec
FULL OUTER JOIN activity_counts ac ON ac.grp = ec.grp
FULL OUTER JOIN activity_compute cp ON cp.grp = COALESCE(ec.grp, ac.grp)
ORDER BY 1
"
    )
}

/// Load grouped usage rows from a single shard without mutating state.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the database query fails.
#[cfg(feature = "db")]
pub async fn load_usage_grouped(
    conn: &mut AsyncPgConnection,
    shard_id: i32,
    query: &UsageQuery,
) -> HarvestResult<Vec<UsageShardRow>> {
    let attr_key = query.group_by.search_attr_key();
    let rows = diesel::sql_query(usage_sql())
        .bind::<Integer, _>(shard_id)
        .bind::<Nullable<Text>, _>(attr_key)
        .bind::<Timestamptz, _>(query.from)
        .bind::<Timestamptz, _>(query.to)
        .load::<UsageSqlRow>(conn)
        .await
        .map_err(database_error)?;

    Ok(rows
        .into_iter()
        .map(|row| UsageShardRow {
            group: row.grp,
            workflow_starts: row.workflow_starts,
            completed: row.completed,
            failed: row.failed,
            cancelled: row.cancelled,
            timed_out: row.timed_out,
            activity_executions: row.activity_executions,
            activity_executions_failed: row.activity_executions_failed,
            activity_compute_seconds: row.activity_compute_seconds,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_wire_defaults_empty_to_workflow_name() {
        assert_eq!(
            UsageGroupBy::from_wire("").unwrap(),
            UsageGroupBy::WorkflowName
        );
    }

    #[test]
    fn from_wire_parses_workflow_name() {
        assert_eq!(
            UsageGroupBy::from_wire("workflow_name").unwrap(),
            UsageGroupBy::WorkflowName
        );
    }

    #[test]
    fn from_wire_parses_search_attr_key() {
        assert_eq!(
            UsageGroupBy::from_wire("search_attr:tenant_id").unwrap(),
            UsageGroupBy::SearchAttr("tenant_id".to_string())
        );
    }

    #[test]
    fn from_wire_trims_search_attr_key() {
        assert_eq!(
            UsageGroupBy::from_wire("search_attr: tenant_id ").unwrap(),
            UsageGroupBy::SearchAttr("tenant_id".to_string())
        );
    }

    #[test]
    fn from_wire_rejects_empty_search_attr_key() {
        let err = UsageGroupBy::from_wire("search_attr:").unwrap_err();
        assert!(err.contains("search_attr key is required"), "{err}");
    }

    #[test]
    fn from_wire_rejects_unknown_dimension() {
        let err = UsageGroupBy::from_wire("queue_name").unwrap_err();
        assert!(err.contains("unknown group_by"), "{err}");
    }

    #[test]
    fn as_wire_round_trips_workflow_name() {
        assert_eq!(UsageGroupBy::WorkflowName.as_wire(), "workflow_name");
        assert_eq!(
            UsageGroupBy::from_wire(&UsageGroupBy::WorkflowName.as_wire()).unwrap(),
            UsageGroupBy::WorkflowName
        );
    }

    #[test]
    fn as_wire_round_trips_search_attr() {
        let original = UsageGroupBy::SearchAttr("tenant_id".to_string());
        assert_eq!(original.as_wire(), "search_attr:tenant_id");
        assert_eq!(
            UsageGroupBy::from_wire(&original.as_wire()).unwrap(),
            original
        );
    }

    #[test]
    fn search_attr_key_extracts_only_for_search_attr_variant() {
        assert_eq!(UsageGroupBy::WorkflowName.search_attr_key(), None);
        assert_eq!(
            UsageGroupBy::SearchAttr("tenant_id".to_string()).search_attr_key(),
            Some("tenant_id")
        );
    }

    #[test]
    fn unattributed_group_constant_matches_ac_wording() {
        assert_eq!(UNATTRIBUTED_GROUP, "(unattributed)");
    }

    #[test]
    fn default_usage_window_ceiling_is_ninety_days() {
        assert_eq!(
            default_usage_window_ceiling(),
            std::time::Duration::from_secs(90 * 24 * 60 * 60)
        );
    }
}
