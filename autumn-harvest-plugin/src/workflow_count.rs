//! Management read model for the grouped workflow-count fleet snapshot (issue #544).
//!
//! Answers the first question an on-call operator or dashboard asks: *"right
//! now, how many executions are RUNNING / FAILED / SUSPENDED per workflow
//! type, across every shard?"* Today that requires paginating `GET /workflows`
//! or hand-querying every shard database. `GET /workflows/count` groups
//! `harvest_workflow_executions` rows by `state` and/or `workflow_name` with a
//! real per-shard SQL `GROUP BY … COUNT(*)`
//! ([`count_workflow_executions_grouped`](autumn_harvest::execution::count_workflow_executions_grouped))
//! and sums the per-group counts across shards here.
//!
//! ## Response shape
//!
//! The response is an **eventually-consistent point-in-time snapshot**: it
//! reflects committed `harvest_workflow_executions.state` at query time and
//! carries no replay or ordering guarantee under concurrent writes.
//!
//! ## Bounded cardinality
//!
//! The number of distinct groups returned is capped at `limit_groups`
//! (default [`DEFAULT_LIMIT_GROUPS`], max [`MAX_LIMIT_GROUPS`]); the long tail
//! is rolled into a single `{"other": true}` group so a pathological number of
//! workflow types can never produce an unbounded payload, via the same
//! [`rollup_top_n`](autumn_harvest::dlq::rollup_top_n) helper the DLQ
//! aggregation endpoint's `_other` rollup (issue #385) uses.
//!
//! ## Partial answers
//!
//! Every shard the router knows about (`readable_shards`/`default_shard`),
//! not just the ones with a live connection pool right now, is included in
//! the fan-out via [`shard_fanout::expected_shards`] — a shard mid a
//! shard-add rollout that has no pool wired up yet is reported `unavailable`
//! rather than silently omitted, so `status` can never read `complete` while
//! a known shard was never queried. When a shard is unreachable it is named
//! in the `unavailable_shards` field (never silently dropped) and `status`
//! becomes `partial` (some shards inspected) or `unavailable` (none
//! inspected) — the call never fails wholesale, matching the shard-health
//! `degraded` contract.

use std::collections::HashMap;

use autumn_harvest::dlq::rollup_top_n;
use autumn_harvest::execution::{
    WorkflowCountDimension, WorkflowCountQuery, WorkflowCountRow, count_workflow_executions_grouped,
};
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use serde::Serialize;

use crate::api::HarvestApiState;
use crate::shard_fanout::{self, FanoutStatus, ShardObservation};

/// Default cap on the number of `(state, workflow_name)` groups returned.
pub const DEFAULT_LIMIT_GROUPS: u32 = 50;
/// Hard ceiling on `limit_groups`.
pub const MAX_LIMIT_GROUPS: u32 = 500;

/// Cross-shard completeness of a count response.
///
/// A type alias onto the shared [`FanoutStatus`] (rather than its own enum)
/// so the complete/partial/unavailable derivation rule lives in exactly one
/// place, alongside `workflow_reachability`'s and `schedule_runs`' reports.
pub type WorkflowCountReportStatus = FanoutStatus;

// serde's `skip_serializing_if` calls this with `&bool` (a reference to the
// field) — the reference signature is required by serde, not optional.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_false(b: &bool) -> bool {
    !*b
}

/// One grouped count in the merged response.
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowCountGroupEntry {
    /// Grouped state value; omitted when not part of `group_by`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Grouped workflow name value; omitted when not part of `group_by`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_name: Option<String>,
    /// Number of executions in this group, summed across shards.
    pub count: i64,
    /// `true` for the long-tail rollup group (bounded cardinality).
    #[serde(skip_serializing_if = "is_false", default)]
    pub other: bool,
}

/// The full grouped workflow-count response (issue #544).
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowCountResponse {
    /// Cross-shard completeness of this snapshot.
    pub status: WorkflowCountReportStatus,
    /// When the snapshot was assembled. Eventually-consistent point-in-time —
    /// no replay or ordering guarantee under concurrent writes.
    pub as_of: DateTime<Utc>,
    /// Returned groups, ordered by descending count.
    pub groups: Vec<WorkflowCountGroupEntry>,
    /// Total executions matching the filters, summed across inspected shards
    /// (reconciles with the sum of `groups[].count`, including the rollup).
    pub total: i64,
    /// `true` when the long tail was rolled into an `other` group.
    pub truncated: bool,
    /// Shards that could not be queried, named with a reason. Never silently
    /// dropped.
    pub unavailable_shards: Vec<shard_fanout::UnavailableShard>,
}

/// Parsed, validated query parameters for `GET /workflows/count` (issue #544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowCountParams {
    /// Ordered, de-duplicated grouping dimensions. Defaults to `[State]`.
    pub group_by: Vec<WorkflowCountDimension>,
    /// Filter: exact workflow name.
    pub workflow_name: Option<String>,
    /// Filter: restrict to these states (empty = all states).
    pub states: Vec<String>,
    /// Filter: inclusive lower bound on `started_at`.
    pub started_after: Option<DateTime<Utc>>,
    /// Filter: inclusive upper bound on `started_at`.
    pub started_before: Option<DateTime<Utc>>,
    /// Cap on returned groups; long tail rolls into `other`.
    pub limit_groups: u32,
}

impl Default for WorkflowCountParams {
    fn default() -> Self {
        Self {
            group_by: vec![WorkflowCountDimension::State],
            workflow_name: None,
            states: Vec::new(),
            started_after: None,
            started_before: None,
            limit_groups: DEFAULT_LIMIT_GROUPS,
        }
    }
}

impl WorkflowCountParams {
    /// Parse and validate query-string pairs into count parameters.
    ///
    /// `known_states` is the caller's authoritative state vocabulary (mirrors
    /// `KNOWN_WORKFLOW_STATES` used by `GET /workflows`) — an unknown `state`
    /// value returns `Err` so the caller can answer `400 Bad Request` rather
    /// than silently matching nothing.
    ///
    /// # Errors
    ///
    /// Returns a human-readable error message on any invalid parameter value:
    /// an unknown `group_by` dimension, an unknown `state`, a malformed
    /// timestamp, or an out-of-range `limit_groups`.
    pub fn from_query_pairs(
        pairs: &[(String, String)],
        known_states: &[&str],
    ) -> Result<Self, String> {
        let mut params = Self::default();
        let mut group_by: Vec<WorkflowCountDimension> = Vec::new();
        let mut group_by_seen = false;

        for (key, value) in pairs {
            match key.as_str() {
                "group_by" => {
                    group_by_seen = true;
                    for raw in value.split(',') {
                        let trimmed = raw.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        let dim = WorkflowCountDimension::from_wire(trimmed).ok_or_else(|| {
                            format!(
                                "unknown group_by dimension '{trimmed}'; expected 'state' or \
                                 'workflow_name'"
                            )
                        })?;
                        if !group_by.contains(&dim) {
                            group_by.push(dim);
                        }
                    }
                }
                "workflow_name" => {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        params.workflow_name = Some(trimmed.to_string());
                    }
                }
                "state" => {
                    for raw in value.split(',') {
                        let trimmed = raw.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if !known_states.contains(&trimmed) {
                            return Err(format!(
                                "unknown workflow state '{trimmed}'; expected one of \
                                 {known_states:?}"
                            ));
                        }
                        let owned = trimmed.to_string();
                        if !params.states.contains(&owned) {
                            params.states.push(owned);
                        }
                    }
                }
                "started_after" => {
                    let dt = DateTime::parse_from_rfc3339(value.trim())
                        .map_err(|_| {
                            format!(
                                "invalid started_after '{value}'; expected RFC 3339 \
                                 (e.g. 2026-01-01T00:00:00Z)"
                            )
                        })?
                        .with_timezone(&Utc);
                    params.started_after = Some(dt);
                }
                "started_before" => {
                    let dt = DateTime::parse_from_rfc3339(value.trim())
                        .map_err(|_| {
                            format!(
                                "invalid started_before '{value}'; expected RFC 3339 \
                                 (e.g. 2026-01-01T00:00:00Z)"
                            )
                        })?
                        .with_timezone(&Utc);
                    params.started_before = Some(dt);
                }
                "limit_groups" => {
                    let n: u32 = value.trim().parse().map_err(|_| {
                        format!(
                            "invalid limit_groups '{value}'; expected an integer in \
                             [1, {MAX_LIMIT_GROUPS}]"
                        )
                    })?;
                    if !(1..=MAX_LIMIT_GROUPS).contains(&n) {
                        return Err(format!(
                            "invalid limit_groups '{value}'; expected an integer in \
                             [1, {MAX_LIMIT_GROUPS}]"
                        ));
                    }
                    params.limit_groups = n;
                }
                // Unknown keys are ignored for forward-compatibility.
                _ => {}
            }
        }

        if group_by_seen && !group_by.is_empty() {
            params.group_by = group_by;
        }

        Ok(params)
    }
}

/// A merge key: `(state, workflow_name)`, each `None` when not grouped by.
type CountKey = (Option<String>, Option<String>);

/// Merge per-shard grouped-count observations into the final response (pure,
/// no DB).
///
/// Counts sum across shards; groups are ordered by descending count (ties
/// broken by key for determinism) and rolled up via the shared
/// [`rollup_top_n`] bounded-cardinality helper. `status` is `complete` only
/// when every shard was inspected.
#[must_use]
#[allow(clippy::needless_pass_by_value)]
pub fn build_count_response(
    as_of: DateTime<Utc>,
    limit_groups: usize,
    observations: Vec<ShardObservation<WorkflowCountRow>>,
) -> WorkflowCountResponse {
    let (inspected, unavailable_shards) = shard_fanout::summarize_shard_errors(&observations);

    let mut merged: HashMap<CountKey, i64> = HashMap::new();
    for observation in &observations {
        for row in &observation.rows {
            *merged
                .entry((row.state.clone(), row.workflow_name.clone()))
                .or_insert(0) += row.count;
        }
    }

    let total: i64 = merged.values().sum();
    let groups: Vec<(CountKey, i64)> = merged.into_iter().collect();

    let (out, truncated) = rollup_top_n(
        groups,
        limit_groups,
        |count| *count,
        |(state, workflow_name), count| WorkflowCountGroupEntry {
            state,
            workflow_name,
            count,
            other: false,
        },
        |other_count| WorkflowCountGroupEntry {
            state: None,
            workflow_name: None,
            count: other_count,
            other: true,
        },
    );

    WorkflowCountResponse {
        status: FanoutStatus::from_counts(inspected, unavailable_shards.len()),
        as_of,
        groups: out,
        total,
        truncated,
        unavailable_shards,
    }
}

/// Query one shard for its grouped counts (issue #544).
///
/// Returns a [`ShardObservation`] rather than propagating errors so a single
/// unreachable shard never fails the whole fan-out — the caller folds it into
/// `unavailable_shards` and a `partial`/`unavailable` status instead.
async fn observe_shard(
    shard_id: i32,
    pool: Option<DbPool>,
    query: &WorkflowCountQuery,
) -> ShardObservation<WorkflowCountRow> {
    let mut conn = match shard_fanout::acquire_shard_conn(shard_id, pool).await {
        Ok(conn) => conn,
        Err(observation) => return observation,
    };
    match count_workflow_executions_grouped(&mut conn, shard_id, query).await {
        Ok(rows) => ShardObservation {
            shard_id,
            rows,
            error: None,
        },
        Err(e) => ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(e.to_string()),
        },
    }
}

/// Build the grouped workflow-count report for `GET /workflows/count`
/// (issue #544).
///
/// Fans the query out across [`shard_fanout::expected_shards`] — every shard
/// with a live pool *and* every shard the router already knows about — so a
/// shard mid a shard-add rollout is reported `unavailable` rather than
/// silently skipped, then queries all of them concurrently (`join_all`)
/// rather than one at a time.
///
/// Never fails outright: a missing storage pool (or an unreachable shard)
/// surfaces as `status: unavailable`/`partial` in the response, matching
/// `workflow_reachability`'s and the shard-health "never fail wholesale"
/// contract.
pub async fn build_workflow_count_report(
    api_state: &HarvestApiState,
    params: WorkflowCountParams,
) -> WorkflowCountResponse {
    let as_of = Utc::now();
    let pools = shard_fanout::pools_by_shard(api_state);
    let expected = shard_fanout::expected_shards(api_state, &pools);
    let limit_groups = params.limit_groups as usize;

    let query = WorkflowCountQuery {
        group_by: params.group_by,
        workflow_name: params.workflow_name,
        states: params.states,
        started_after: params.started_after,
        started_before: params.started_before,
    };

    let observations = expected
        .iter()
        .map(|shard_id| {
            let pool = pools.get(shard_id).cloned();
            observe_shard(*shard_id, pool, &query)
        })
        .collect::<Vec<_>>();
    let observations = join_all(observations).await;

    build_count_response(as_of, limit_groups, observations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

    fn row(state: Option<&str>, workflow_name: Option<&str>, count: i64) -> WorkflowCountRow {
        WorkflowCountRow {
            state: state.map(ToString::to_string),
            workflow_name: workflow_name.map(ToString::to_string),
            count,
        }
    }

    fn obs(
        shard_id: i32,
        rows: Vec<WorkflowCountRow>,
        error: Option<&str>,
    ) -> ShardObservation<WorkflowCountRow> {
        ShardObservation {
            shard_id,
            rows,
            error: error.map(ToOwned::to_owned),
        }
    }

    fn pairs(kvs: &[(&str, &str)]) -> Vec<(String, String)> {
        kvs.iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    const STATES: &[&str] = &["RUNNING", "FAILED", "COMPLETED", "SUSPENDED"];

    // ── build_count_response ─────────────────────────────────────────────

    #[test]
    fn sums_counts_across_shards_and_sorts_descending() {
        let resp = build_count_response(
            at(1000),
            50,
            vec![
                obs(0, vec![row(Some("RUNNING"), None, 5)], None),
                obs(
                    1,
                    vec![row(Some("RUNNING"), None, 3), row(Some("FAILED"), None, 10)],
                    None,
                ),
            ],
        );

        assert_eq!(resp.status, WorkflowCountReportStatus::Complete);
        assert_eq!(resp.total, 18);
        assert_eq!(resp.groups.len(), 2);
        // FAILED=10 sorts before RUNNING=8 (descending count).
        assert_eq!(resp.groups[0].state.as_deref(), Some("FAILED"));
        assert_eq!(resp.groups[0].count, 10);
        assert_eq!(resp.groups[1].state.as_deref(), Some("RUNNING"));
        assert_eq!(resp.groups[1].count, 8);
        assert!(resp.unavailable_shards.is_empty());
        assert!(!resp.truncated);
    }

    #[test]
    fn groups_by_both_state_and_workflow_name() {
        let resp = build_count_response(
            at(1000),
            50,
            vec![obs(
                0,
                vec![
                    row(Some("RUNNING"), Some("onboarding"), 412),
                    row(Some("FAILED"), Some("billing"), 2),
                ],
                None,
            )],
        );

        assert_eq!(resp.groups.len(), 2);
        let onboarding = resp
            .groups
            .iter()
            .find(|g| g.workflow_name.as_deref() == Some("onboarding"))
            .expect("onboarding group present");
        assert_eq!(onboarding.state.as_deref(), Some("RUNNING"));
        assert_eq!(onboarding.count, 412);
    }

    #[test]
    fn unavailable_shard_makes_partial_and_is_named_with_reason() {
        let resp = build_count_response(
            at(1000),
            50,
            vec![
                obs(0, vec![row(Some("RUNNING"), None, 4)], None),
                obs(1, vec![], Some("connection refused")),
            ],
        );

        assert_eq!(resp.status, WorkflowCountReportStatus::Partial);
        assert_eq!(resp.unavailable_shards.len(), 1);
        assert_eq!(resp.unavailable_shards[0].shard_id, 1);
        assert_eq!(
            resp.unavailable_shards[0].reason.as_str(),
            "connection refused"
        );
        // The call does not fail wholesale: the reachable shard's data still flows through.
        assert_eq!(resp.total, 4);
    }

    #[test]
    fn all_shards_unavailable_is_unavailable_not_a_hard_failure() {
        let resp = build_count_response(at(1000), 50, vec![obs(0, vec![], Some("pool missing"))]);

        assert_eq!(resp.status, WorkflowCountReportStatus::Unavailable);
        assert_eq!(resp.total, 0);
        assert!(resp.groups.is_empty());
        assert_eq!(resp.unavailable_shards.len(), 1);
    }

    #[test]
    fn bounded_cardinality_rolls_long_tail_into_other() {
        let rows: Vec<WorkflowCountRow> = (0..5)
            .map(|i| row(None, Some(&format!("wf_{i}")), i64::from(i) + 1))
            .collect();
        let resp = build_count_response(at(1000), 2, vec![obs(0, rows, None)]);

        // Top 2 by count (wf_4=5, wf_3=4) kept; the rest (1+2+3=6) rolled into `other`.
        assert_eq!(resp.groups.len(), 3);
        assert!(resp.truncated);
        let other = resp
            .groups
            .iter()
            .find(|g| g.other)
            .expect("other rollup group present");
        assert_eq!(other.count, 6);
        assert!(other.state.is_none());
        assert!(other.workflow_name.is_none());
        // Reconciles: sum of all groups (including other) == total.
        let sum: i64 = resp.groups.iter().map(|g| g.count).sum();
        assert_eq!(sum, resp.total);
    }

    #[test]
    fn no_rollup_when_within_limit() {
        let resp = build_count_response(
            at(1000),
            50,
            vec![obs(0, vec![row(Some("RUNNING"), None, 1)], None)],
        );
        assert!(!resp.truncated);
        assert!(resp.groups.iter().all(|g| !g.other));
    }

    #[test]
    fn status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(WorkflowCountReportStatus::Complete).unwrap(),
            serde_json::json!("complete")
        );
        assert_eq!(
            serde_json::to_value(WorkflowCountReportStatus::Partial).unwrap(),
            serde_json::json!("partial")
        );
        assert_eq!(
            serde_json::to_value(WorkflowCountReportStatus::Unavailable).unwrap(),
            serde_json::json!("unavailable")
        );
    }

    #[test]
    fn group_entry_omits_ungrouped_dimensions_from_json() {
        let entry = WorkflowCountGroupEntry {
            state: Some("RUNNING".to_string()),
            workflow_name: None,
            count: 3,
            other: false,
        };
        let value = serde_json::to_value(&entry).unwrap();
        assert!(value.get("state").is_some());
        assert!(value.get("workflow_name").is_none());
        assert!(value.get("other").is_none(), "other:false is omitted");
    }

    // ── WorkflowCountParams::from_query_pairs ─────────────────────────────

    #[test]
    fn params_default_group_by_is_state() {
        let params = WorkflowCountParams::from_query_pairs(&[], STATES).unwrap();
        assert_eq!(params.group_by, vec![WorkflowCountDimension::State]);
        assert_eq!(params.limit_groups, DEFAULT_LIMIT_GROUPS);
        assert!(params.states.is_empty());
        assert!(params.workflow_name.is_none());
    }

    #[test]
    fn params_group_by_accepts_state_and_workflow_name_csv() {
        let params = WorkflowCountParams::from_query_pairs(
            &pairs(&[("group_by", "state,workflow_name")]),
            STATES,
        )
        .unwrap();
        assert_eq!(
            params.group_by,
            vec![
                WorkflowCountDimension::State,
                WorkflowCountDimension::WorkflowName
            ]
        );
    }

    #[test]
    fn params_group_by_accepts_workflow_name_only() {
        let params =
            WorkflowCountParams::from_query_pairs(&pairs(&[("group_by", "workflow_name")]), STATES)
                .unwrap();
        assert_eq!(params.group_by, vec![WorkflowCountDimension::WorkflowName]);
    }

    #[test]
    fn params_rejects_unknown_group_by_dimension() {
        let err =
            WorkflowCountParams::from_query_pairs(&pairs(&[("group_by", "queue_name")]), STATES)
                .unwrap_err();
        assert!(err.contains("unknown group_by dimension"));
    }

    #[test]
    fn params_state_filter_repeatable_and_csv() {
        let params = WorkflowCountParams::from_query_pairs(
            &pairs(&[("state", "RUNNING,FAILED"), ("state", "COMPLETED")]),
            STATES,
        )
        .unwrap();
        assert_eq!(params.states, vec!["RUNNING", "FAILED", "COMPLETED"]);
    }

    #[test]
    fn params_rejects_unknown_state() {
        let err = WorkflowCountParams::from_query_pairs(&pairs(&[("state", "BOGUS")]), STATES)
            .unwrap_err();
        assert!(err.contains("unknown workflow state"));
    }

    #[test]
    fn params_workflow_name_filter() {
        let params = WorkflowCountParams::from_query_pairs(
            &pairs(&[("workflow_name", "onboarding")]),
            STATES,
        )
        .unwrap();
        assert_eq!(params.workflow_name.as_deref(), Some("onboarding"));
    }

    #[test]
    fn params_started_after_and_before_rfc3339() {
        let params = WorkflowCountParams::from_query_pairs(
            &pairs(&[
                ("started_after", "2026-01-01T00:00:00Z"),
                ("started_before", "2026-02-01T00:00:00Z"),
            ]),
            STATES,
        )
        .unwrap();
        assert!(params.started_after.is_some());
        assert!(params.started_before.is_some());
        assert!(params.started_after.unwrap() < params.started_before.unwrap());
    }

    #[test]
    fn params_rejects_invalid_started_after() {
        assert!(
            WorkflowCountParams::from_query_pairs(
                &pairs(&[("started_after", "not-a-date")]),
                STATES
            )
            .is_err()
        );
    }

    #[test]
    fn params_limit_groups_clamped_range() {
        assert!(
            WorkflowCountParams::from_query_pairs(&pairs(&[("limit_groups", "0")]), STATES)
                .is_err()
        );
        assert!(
            WorkflowCountParams::from_query_pairs(&pairs(&[("limit_groups", "99999")]), STATES)
                .is_err()
        );
        let params =
            WorkflowCountParams::from_query_pairs(&pairs(&[("limit_groups", "10")]), STATES)
                .unwrap();
        assert_eq!(params.limit_groups, 10);
    }

    #[test]
    fn params_ignores_unknown_query_keys() {
        let params =
            WorkflowCountParams::from_query_pairs(&pairs(&[("bogus_param", "x")]), STATES).unwrap();
        assert_eq!(params, WorkflowCountParams::default());
    }
}
