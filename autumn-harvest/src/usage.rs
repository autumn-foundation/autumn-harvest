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
//!   Counts every `harvest_workflow_executions` row, including
//!   continue-as-new and issue #523 auto-retry successor rows — each such
//!   row represents separately-billable compute, so this is a deliberate
//!   consumption-accuracy choice, not an oversight (consistent with how
//!   `activity_executions` below already counts every retry attempt).
//! - `completed`/`failed`/`cancelled`/`timed_out`: derived from the durable
//!   terminal events (`WorkflowCompleted`/`WorkflowFailed`/`WorkflowCancelled`/
//!   `WorkflowExecutionTimedOut`) whose `harvest_events.timestamp` falls in
//!   `[from, to]` — **not** from the mutable `harvest_workflow_executions`
//!   row. This makes the counters immutable historical facts: an operator
//!   redriving a `FAILED` execution back to `RUNNING` via
//!   `reactivate_failed_execution` clears the row's `state`/`completed_at`
//!   but never rewrites or removes the original `WorkflowFailed` event, so a
//!   usage report re-run for the same window still reports the failure that
//!   actually happened in it. If the redriven run later succeeds, its
//!   `WorkflowCompleted` event is counted separately in whichever window
//!   *that* event falls in — each terminal event is one billable/reportable
//!   occurrence, not a mutually-exclusive final-state bucket. `cancelled` is
//!   the one bucket that still consults the row: `terminate` (issue #504)
//!   reuses the same `WorkflowCancelled` event type as a genuine cancel (no
//!   new event variant), so a `WorkflowCancelled` event only counts toward
//!   `cancelled` when the execution did *not* end up sealed `TERMINATED` by
//!   a genuine terminate. Reaching `TERMINATED` by a *reset* instead (issue
//!   #366's DAG retry / #148's reset-from-failed, `allow_terminal_source`)
//!   does **not** disqualify it: a reset seal always appends a
//!   `WorkflowResetTerminated` event on the source execution (unlike
//!   `terminate_workflow_execution`, which never does), so the query can
//!   tell "genuinely terminated" apart from "this old cancelled run was
//!   later reforked for retry" and keep counting the latter — otherwise a
//!   reset would retroactively erase a historical cancellation from past
//!   report windows, the same immutability bug as the `failed` fix above.
//!   `TERMINATED` and `CONTINUED_AS_NEW` are intentionally not broken out
//!   (the issue's AC lists exactly these four terminal buckets).
//! - `activity_executions`: count of `ActivityStarted` events in the window
//!   (one per dispatch attempt — retries reuse the same `activity_id` but
//!   each attempt appends a fresh `ActivityStarted`).
//! - `activity_executions_failed`: count of terminal `ActivityFailed` +
//!   `ActivityTimedOut` events in the window that have a matching
//!   `ActivityStarted` (non-final retry attempts do not append an event, so
//!   this is exhausted-retry-or-timeout only). The start-match requirement
//!   excludes external activities whose `ActivityTimedOut` is appended by
//!   `enforce_external_task_timeouts` with no `ActivityStarted` at all
//!   (external activities are dispatched via `ActivityAwaitingExternal`,
//!   never claimed/started by a worker) — otherwise they would be charged
//!   as a failure despite `activity_executions`/`activity_compute_seconds`
//!   correctly excluding them below.
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
//!
//! ## Known limitation: `search_attr` value collision with the sentinel
//!
//! An execution whose `search_attrs` value for the requested key literally
//! equals [`crate::usage::UNATTRIBUTED_GROUP`] (`"(unattributed)"`) is indistinguishable
//! from an execution that lacks the key entirely — both merge into the same
//! `"(unattributed)"` group. This is an accepted, documented limitation, not
//! a defect: issue #596's AC mandates this exact literal for missing-key
//! executions, so any internal separation of the two cases would still
//! collapse to one flat string in the final JSON response. Operators
//! choosing a `search_attr:<key>` should avoid keys whose legitimate values
//! could collide with the sentinel string.

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

/// Default cap on the number of distinct groups `GET /admin/usage` will
/// return before failing loudly with a `413`-mappable error (issue #596).
///
/// Unlike `GET /workflows/count`'s top-N + `other` rollup, a chargeback
/// report must never silently drop a low-volume tenant's data — so
/// exceeding this cap is a hard error naming the ceiling rather than a
/// silent rollup. The cap is enforced at two layers: `usage_sql` binds it
/// (plus one) as a `LIMIT` so a single high-cardinality shard can't
/// materialize an unbounded result set, and the plugin's
/// `build_usage_response` checks the merged cross-shard count before
/// constructing the response — the DB-side `LIMIT` bounds the expensive
/// part (the query and its memory), the merge-side check produces the
/// accurate 413.
#[must_use]
pub const fn default_usage_max_groups() -> usize {
    10_000
}

/// How `GET /admin/usage` groups executions (issue #596).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageGroupBy {
    /// Group by `harvest_workflow_executions.workflow_name`.
    WorkflowName,
    /// Group by a key inside the `search_attrs` JSONB object (e.g. tenant id).
    ///
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
// key) is NULL, else `COALESCE(search_attrs ->> $2, UNATTRIBUTED_GROUP)`.
// Derived from the `UNATTRIBUTED_GROUP` constant via interpolation (rather
// than duplicating the literal) so the two can never drift out of sync.
#[cfg(feature = "db")]
fn group_key_expr() -> String {
    format!(
        "CASE WHEN $2::TEXT IS NULL THEN w.workflow_name \
         ELSE COALESCE(w.search_attrs ->> $2::TEXT, '{UNATTRIBUTED_GROUP}') END"
    )
}

#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
fn usage_sql() -> String {
    let group_key_expr = group_key_expr();
    format!(
        r"
WITH execution_starts AS (
    SELECT
        {group_key_expr} AS grp,
        COUNT(*)::BIGINT AS workflow_starts
    FROM harvest_workflow_executions w
    WHERE w.shard_id = $1::INT4
      AND w.started_at BETWEEN $3 AND $4
    GROUP BY 1
),
reset_terminated_execs AS (
    -- Executions later sealed TERMINATED by a workflow reset (issue #366's
    -- DAG retry / #148's reset-from-failed, `allow_terminal_source`), which
    -- is distinct from a genuine `terminate` (issue #504): a reset seal
    -- always appends `WorkflowResetTerminated` on the source execution,
    -- while `terminate_workflow_execution` never does. Used below so a
    -- WorkflowCancelled event's `cancelled` classification survives the
    -- source row being later reforked, instead of silently flipping to
    -- uncounted once the row's state moves from CANCELLED to TERMINATED.
    SELECT DISTINCT e.workflow_exec_id
    FROM harvest_events e
    INNER JOIN harvest_workflow_executions w ON w.id = e.workflow_exec_id
    WHERE w.shard_id = $1::INT4
      AND e.event_type = 'WorkflowResetTerminated'
),
terminal_counts AS (
    SELECT
        {group_key_expr} AS grp,
        COUNT(*) FILTER (WHERE e.event_type = 'WorkflowCompleted')::BIGINT AS completed,
        COUNT(*) FILTER (WHERE e.event_type = 'WorkflowFailed')::BIGINT AS failed,
        COUNT(*) FILTER (
            WHERE e.event_type = 'WorkflowCancelled'
              AND NOT (w.state = 'TERMINATED' AND rt.workflow_exec_id IS NULL)
        )::BIGINT AS cancelled,
        COUNT(*) FILTER (WHERE e.event_type = 'WorkflowExecutionTimedOut')::BIGINT AS timed_out
    FROM harvest_events e
    INNER JOIN harvest_workflow_executions w ON w.id = e.workflow_exec_id
    LEFT JOIN reset_terminated_execs rt ON rt.workflow_exec_id = e.workflow_exec_id
    WHERE w.shard_id = $1::INT4
      AND e.event_type IN (
        'WorkflowCompleted', 'WorkflowFailed', 'WorkflowCancelled', 'WorkflowExecutionTimedOut'
    )
      AND e.timestamp BETWEEN $3 AND $4
    GROUP BY 1
),
activity_events AS (
    SELECT
        e.workflow_exec_id,
        e.event_type,
        e.event_data #>> '{{data,activity_id}}' AS activity_id,
        e.timestamp,
        {group_key_expr} AS grp
    FROM harvest_events e
    INNER JOIN harvest_workflow_executions w ON w.id = e.workflow_exec_id
    WHERE w.shard_id = $1::INT4
      AND e.event_type IN (
        'ActivityStarted', 'ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut'
    )
      AND e.timestamp BETWEEN $3 AND $4
),
activity_metrics AS (
    SELECT
        ae.grp,
        COUNT(*) FILTER (WHERE ae.event_type = 'ActivityStarted')::BIGINT AS activity_executions,
        -- Requires a matching ActivityStarted (via the same unwindowed
        -- lookback used by activity_compute_seconds below), so an external
        -- activity's ActivityTimedOut (enforce_external_task_timeouts
        -- appends it with no ActivityStarted — external activities are
        -- dispatched via ActivityAwaitingExternal, never claimed/started by
        -- a worker) is excluded, matching the module doc's stated exclusion
        -- of externally-completed/no-worker-compute activities.
        COUNT(*) FILTER (
            WHERE ae.event_type IN ('ActivityFailed', 'ActivityTimedOut')
              AND s.last_started_at IS NOT NULL
        )::BIGINT AS activity_executions_failed,
        COALESCE(
            SUM(EXTRACT(EPOCH FROM (ae.timestamp - s.last_started_at))) FILTER (
                WHERE ae.event_type IN ('ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut')
            ),
            0
        )::DOUBLE PRECISION AS activity_compute_seconds
    FROM activity_events ae
    LEFT JOIN LATERAL (
        SELECT MAX(e2.timestamp) AS last_started_at
        FROM harvest_events e2
        WHERE e2.workflow_exec_id = ae.workflow_exec_id
          AND e2.event_type = 'ActivityStarted'
          AND e2.event_data #>> '{{data,activity_id}}' = ae.activity_id
          AND e2.timestamp <= ae.timestamp
    ) s ON true
    GROUP BY 1
)
SELECT
    COALESCE(es.grp, tc.grp, am.grp)::TEXT AS grp,
    COALESCE(es.workflow_starts, 0)::BIGINT AS workflow_starts,
    COALESCE(tc.completed, 0)::BIGINT AS completed,
    COALESCE(tc.failed, 0)::BIGINT AS failed,
    COALESCE(tc.cancelled, 0)::BIGINT AS cancelled,
    COALESCE(tc.timed_out, 0)::BIGINT AS timed_out,
    COALESCE(am.activity_executions, 0)::BIGINT AS activity_executions,
    COALESCE(am.activity_executions_failed, 0)::BIGINT AS activity_executions_failed,
    COALESCE(am.activity_compute_seconds, 0)::DOUBLE PRECISION AS activity_compute_seconds
FROM execution_starts es
FULL OUTER JOIN terminal_counts tc ON tc.grp = es.grp
FULL OUTER JOIN activity_metrics am ON am.grp = COALESCE(es.grp, tc.grp)
ORDER BY 1
LIMIT $5
"
    )
}

/// Load grouped usage rows from a single shard without mutating state.
///
/// `row_limit` bounds the number of grouped rows this single shard's query
/// can return (a `LIMIT` clause in `usage_sql`) — callers should pass the
/// configured group cap plus one, so the plugin can still detect and report
/// an over-cap condition without ever materializing an unbounded result set
/// for a single high-cardinality shard.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the database query fails.
#[cfg(feature = "db")]
pub async fn load_usage_grouped(
    conn: &mut AsyncPgConnection,
    shard_id: i32,
    query: &UsageQuery,
    row_limit: i64,
) -> HarvestResult<Vec<UsageShardRow>> {
    let attr_key = query.group_by.search_attr_key();
    let rows = diesel::sql_query(usage_sql())
        .bind::<Integer, _>(shard_id)
        .bind::<Nullable<Text>, _>(attr_key)
        .bind::<Timestamptz, _>(query.from)
        .bind::<Timestamptz, _>(query.to)
        .bind::<BigInt, _>(row_limit)
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

    #[cfg(feature = "db")]
    #[test]
    fn group_key_expr_sql_derives_from_unattributed_group_constant() {
        assert!(group_key_expr().contains(&format!("'{UNATTRIBUTED_GROUP}'")));
    }

    #[cfg(feature = "db")]
    #[test]
    fn usage_sql_merges_activity_scans_into_one_cte() {
        let sql = usage_sql();
        assert!(
            sql.contains("activity_events"),
            "merged activity CTE must exist"
        );
        assert!(
            !sql.contains("activity_terminal"),
            "activity_terminal CTE must be gone after the F10 merge"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn usage_sql_derives_terminal_outcomes_from_durable_events_not_row_state() {
        let sql = usage_sql();
        assert!(
            sql.contains("'WorkflowCompleted'")
                && sql.contains("'WorkflowFailed'")
                && sql.contains("'WorkflowCancelled'")
                && sql.contains("'WorkflowExecutionTimedOut'"),
            "terminal_counts must scan the four durable terminal event types"
        );
        assert!(
            !sql.contains("w.completed_at BETWEEN"),
            "terminal outcomes must no longer be windowed by the mutable \
             completed_at column, so a redrive that clears it cannot erase \
             a historical failure"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn usage_sql_reset_terminated_execs_disambiguates_cancel_from_terminate() {
        let sql = usage_sql();
        assert!(
            sql.contains("reset_terminated_execs") && sql.contains("'WorkflowResetTerminated'"),
            "cancelled must not be disqualified just because a later reset \
             sealed the row to TERMINATED"
        );
        assert!(
            !sql.contains("w.state = 'CANCELLED'"),
            "the cancelled filter must no longer hinge on the row still \
             being CANCELLED, since a reset can move it to TERMINATED"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn usage_sql_activity_failed_count_requires_a_matching_start() {
        let sql = usage_sql();
        assert!(
            sql.contains("activity_metrics") && !sql.contains("activity_counts"),
            "activity_counts must be merged into activity_metrics so the \
             failed count can share the started-lookback LATERAL join"
        );
        assert!(
            sql.contains("s.last_started_at IS NOT NULL"),
            "activity_executions_failed must require a matching ActivityStarted, \
             excluding external activities whose ActivityTimedOut has none"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn usage_sql_has_a_limit_clause_bound_to_a_parameter() {
        let sql = usage_sql();
        assert!(
            sql.trim_end().ends_with("LIMIT $5"),
            "the shard query must cap its own row count via a bound LIMIT \
             parameter, not rely solely on the plugin's post-merge cap \
             check, so a single high-cardinality shard can't materialize an \
             unbounded result set before the cap is ever consulted"
        );
    }
}
