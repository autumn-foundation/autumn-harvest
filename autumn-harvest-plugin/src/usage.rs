//! Management read model for the per-tenant/per-workflow usage report (issue #596).
//!
//! The *historical* companion to the *point-in-time* `GET /admin/concurrency`
//! endpoint (issue #247): `GET /admin/usage` answers "how much did tenant X
//! actually consume between two dates?" by aggregating already-durable data
//! (`harvest_workflow_executions` + `harvest_events`) over a caller-supplied
//! `[from, to]` window, grouped by `workflow_name` (default) or by a
//! `search_attrs` key (e.g. `search_attr:tenant_id`), fanned out across every
//! shard with [`autumn_harvest::usage::load_usage_grouped`] and merged here.
//!
//! ## Partial answers
//!
//! Every shard the router knows about (`readable_shards`/`default_shard`),
//! not just the ones with a live connection pool right now, is included in
//! the fan-out via [`shard_fanout::expected_shards`]. When a shard is
//! unreachable it is named in `unavailable_shards` (never silently dropped)
//! and `status` becomes `partial` or `unavailable` — the call never fails
//! wholesale.
//!
//! ## Bounded window
//!
//! `from`/`to` windows wider than a configurable ceiling (default 90 days,
//! see [`autumn_harvest::usage::default_usage_window_ceiling`]) are rejected
//! with a `400`-mappable error naming the ceiling, so an operator cannot
//! accidentally trigger a full-table scan across every shard.
//!
//! ## No bounded-cardinality rollup
//!
//! Unlike `GET /workflows/count` (issue #544), this report does **not** roll
//! a long tail into an `other` group: a chargeback report that silently
//! drops low-volume tenants would be actively wrong. Operators choosing
//! `search_attr:<key>` are responsible for using a bounded-cardinality key
//! (a tenant id, not e.g. an execution id).

use std::collections::BTreeMap;

use autumn_harvest::usage::{UsageGroupBy, UsageQuery, UsageShardRow};
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use serde::Serialize;

use crate::api::HarvestApiState;
use crate::shard_fanout::{self, FanoutStatus, ShardObservation};

/// Cross-shard completeness of a usage report.
///
/// A type alias onto the shared [`FanoutStatus`] (rather than its own enum)
/// so the complete/partial/unavailable derivation rule lives in exactly one
/// place, alongside `workflow_count`'s and `workflow_reachability`'s reports.
pub type UsageReportStatus = FanoutStatus;

/// One merged usage record — exactly the fields named in issue #596's AC.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UsageGroupRecord {
    pub group: String,
    /// Count of `harvest_workflow_executions` rows whose `started_at` falls
    /// in the window — including continue-as-new and auto-retry (issue
    /// #523) successor rows, each counted as a separate, billable start.
    /// See `autumn_harvest::usage`'s module doc for full metric semantics.
    pub workflow_starts: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
    pub timed_out: i64,
    pub activity_executions: i64,
    pub activity_executions_failed: i64,
    pub activity_compute_seconds: f64,
}

/// The full `GET /admin/usage` response (issue #596).
#[derive(Debug, Clone, Serialize)]
pub struct UsageResponse {
    /// Cross-shard completeness of this report.
    pub status: UsageReportStatus,
    /// Inclusive lower bound of the aggregation window, as requested.
    pub from: DateTime<Utc>,
    /// Inclusive upper bound of the aggregation window, as requested.
    pub to: DateTime<Utc>,
    /// The grouping dimension actually used (wire form).
    pub group_by: String,
    /// Records, sorted by `group` ascending.
    pub groups: Vec<UsageGroupRecord>,
    /// Shards that could not be queried, named with a reason. Never silently
    /// dropped.
    pub unavailable_shards: Vec<shard_fanout::UnavailableShard>,
}

/// Returned by [`build_usage_response`]/[`build_usage_report`] when the
/// merged group count would exceed the configured cap (issue #596).
///
/// A hard, loud failure rather than a silent top-N rollup — see the module
/// doc's "No bounded-cardinality rollup" section for why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageGroupCapExceeded {
    pub group_count: usize,
    pub cap: usize,
}

impl std::fmt::Display for UsageGroupCapExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "usage report would return {} distinct groups, exceeding the \
             configured cap of {}; narrow the [from, to] window or choose a \
             lower-cardinality group_by key",
            self.group_count, self.cap
        )
    }
}

impl std::error::Error for UsageGroupCapExceeded {}

/// Format a ceiling `Duration` for the window-exceeded error message
/// (issue #596). Expresses `d` in the largest whole unit that evenly
/// divides it — days, else hours, else seconds — so a 30-hour ceiling
/// reports "30-hour" instead of being truncated to "1-day". Not a
/// general-purpose duration formatter; kept local to this one use case.
fn format_ceiling(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    if secs != 0 && secs.is_multiple_of(24 * 60 * 60) {
        format!("{}-day", secs / (24 * 60 * 60))
    } else if secs != 0 && secs.is_multiple_of(60 * 60) {
        format!("{}-hour", secs / (60 * 60))
    } else {
        format!("{secs}-second")
    }
}

/// Parsed, validated query parameters for `GET /admin/usage` (issue #596).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageParams {
    pub group_by: UsageGroupBy,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl UsageParams {
    /// Parse and validate query-string pairs into usage-report parameters.
    ///
    /// `from` and `to` are required (RFC 3339, or a relative duration like
    /// `24h` measured back from `now` — see [`autumn_harvest::parse_instant`]).
    /// `group_by` defaults to `workflow_name`.
    ///
    /// # Errors
    ///
    /// Returns a human-readable error message when `from`/`to` is missing or
    /// malformed, `from` is after `to`, the window exceeds `ceiling` (the
    /// message names the ceiling), or `group_by` is not a recognized
    /// dimension.
    pub fn from_query_pairs(
        pairs: &[(String, String)],
        now: DateTime<Utc>,
        ceiling: std::time::Duration,
    ) -> Result<Self, String> {
        let mut from: Option<DateTime<Utc>> = None;
        let mut to: Option<DateTime<Utc>> = None;
        let mut group_by = UsageGroupBy::WorkflowName;

        for (key, value) in pairs {
            match key.as_str() {
                "from" => from = Some(autumn_harvest::parse_instant("from", value, now)?),
                "to" => to = Some(autumn_harvest::parse_instant("to", value, now)?),
                "group_by" => group_by = UsageGroupBy::from_wire(value)?,
                // Unknown keys are ignored for forward-compatibility.
                _ => {}
            }
        }

        let from = from.ok_or_else(|| "missing required parameter 'from'".to_string())?;
        let to = to.ok_or_else(|| "missing required parameter 'to'".to_string())?;

        if from > to {
            return Err(format!(
                "invalid window: 'from' ({from}) is after 'to' ({to})"
            ));
        }

        let window = to.signed_duration_since(from);
        let ceiling_chrono = chrono::Duration::from_std(ceiling).unwrap_or(chrono::Duration::MAX);
        if window > ceiling_chrono {
            let ceiling_label = format_ceiling(ceiling);
            return Err(format!(
                "invalid window: 'to' - 'from' exceeds the {ceiling_label} usage window \
                 ceiling; narrow the window or use a smaller range"
            ));
        }

        Ok(Self { group_by, from, to })
    }
}

/// Merge per-shard usage observations into the final response (pure, no DB).
///
/// Counts sum across shards; groups are ordered lexicographically by `group`
/// for a stable, deterministic response. `status` is `complete` only when
/// every shard was inspected.
///
/// # Errors
///
/// Returns [`UsageGroupCapExceeded`] when the merged group count exceeds
/// `max_groups`, checked after the cross-shard merge but before the
/// response is constructed (no extra DB round-trip).
pub fn build_usage_response(
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    group_by: &UsageGroupBy,
    observations: Vec<ShardObservation<UsageShardRow>>,
    max_groups: usize,
) -> Result<UsageResponse, UsageGroupCapExceeded> {
    let (inspected, unavailable_shards) = shard_fanout::summarize_shard_errors(&observations);

    let mut merged: BTreeMap<String, UsageGroupRecord> = BTreeMap::new();
    for observation in observations {
        for row in observation.rows {
            let entry = merged
                .entry(row.group)
                .or_insert_with_key(|group| UsageGroupRecord {
                    group: group.clone(),
                    workflow_starts: 0,
                    completed: 0,
                    failed: 0,
                    cancelled: 0,
                    timed_out: 0,
                    activity_executions: 0,
                    activity_executions_failed: 0,
                    activity_compute_seconds: 0.0,
                });
            entry.workflow_starts += row.workflow_starts;
            entry.completed += row.completed;
            entry.failed += row.failed;
            entry.cancelled += row.cancelled;
            entry.timed_out += row.timed_out;
            entry.activity_executions += row.activity_executions;
            entry.activity_executions_failed += row.activity_executions_failed;
            entry.activity_compute_seconds += row.activity_compute_seconds;
        }
    }

    if merged.len() > max_groups {
        return Err(UsageGroupCapExceeded {
            group_count: merged.len(),
            cap: max_groups,
        });
    }

    Ok(UsageResponse {
        status: FanoutStatus::from_counts(inspected, unavailable_shards.len()),
        from,
        to,
        group_by: group_by.as_wire(),
        groups: merged.into_values().collect(),
        unavailable_shards,
    })
}

/// Query one shard for its grouped usage rows (issue #596).
///
/// Returns a [`ShardObservation`] rather than propagating errors so a single
/// unreachable shard never fails the whole fan-out — the caller folds it into
/// `unavailable_shards` and a `partial`/`unavailable` status instead.
///
/// `row_limit` bounds the shard query's `LIMIT` (issue #596 PR review: the
/// group cap must bound the DB round-trip and memory, not just the response
/// — passing this straight through to
/// [`autumn_harvest::usage::load_usage_grouped`] instead of letting it fetch
/// every row before `build_usage_response` checks the merged count).
async fn observe_shard(
    shard_id: i32,
    pool: Option<DbPool>,
    query: &UsageQuery,
    row_limit: i64,
) -> ShardObservation<UsageShardRow> {
    let mut conn = match shard_fanout::acquire_shard_conn(shard_id, pool).await {
        Ok(conn) => conn,
        Err(observation) => return observation,
    };
    match autumn_harvest::usage::load_usage_grouped(&mut conn, shard_id, query, row_limit).await {
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

/// Build the usage report for `GET /admin/usage` (issue #596).
///
/// Fans the query out across [`shard_fanout::expected_shards`] — every shard
/// with a live pool *and* every shard the router already knows about — so a
/// shard mid a shard-add rollout is reported `unavailable` rather than
/// silently skipped, then queries all of them concurrently (`join_all`)
/// rather than one at a time.
///
/// Never fails outright for a partial/unreachable shard: that surfaces as
/// `status: unavailable`/`partial` in the response. Does fail loudly (see
/// [`UsageGroupCapExceeded`]) when the merged group count exceeds the
/// configured cap.
///
/// # Errors
///
/// Returns [`UsageGroupCapExceeded`] when the merged group count exceeds
/// `api_state`'s configured `usage_max_groups`.
pub async fn build_usage_report(
    api_state: &HarvestApiState,
    params: UsageParams,
) -> Result<UsageResponse, UsageGroupCapExceeded> {
    let pools = shard_fanout::pools_by_shard(api_state);
    let expected = shard_fanout::expected_shards(api_state, &pools);
    let max_groups = api_state.usage_max_groups();
    // Bound each shard's own result set at the cap, not just the merged
    // response: a `+1` (not the cap itself) so a shard sitting exactly at
    // the cap still cleanly merges under it, while any shard that would
    // exceed the cap on its own is truncated rather than materializing an
    // unbounded row set (issue #596 PR review).
    let row_limit = i64::try_from(max_groups.saturating_add(1)).unwrap_or(i64::MAX);

    let query = UsageQuery {
        group_by: params.group_by.clone(),
        from: params.from,
        to: params.to,
    };

    let observations = expected
        .iter()
        .map(|shard_id| {
            let pool = pools.get(shard_id).cloned();
            observe_shard(*shard_id, pool, &query, row_limit)
        })
        .collect::<Vec<_>>();
    let observations = join_all(observations).await;

    build_usage_response(
        params.from,
        params.to,
        &params.group_by,
        observations,
        max_groups,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

    fn row(group: &str, starts: i64, completed: i64, failed: i64) -> UsageShardRow {
        UsageShardRow {
            group: group.to_string(),
            workflow_starts: starts,
            completed,
            failed,
            cancelled: 0,
            timed_out: 0,
            activity_executions: 0,
            activity_executions_failed: 0,
            activity_compute_seconds: 0.0,
        }
    }

    fn obs(
        shard_id: i32,
        rows: Vec<UsageShardRow>,
        error: Option<&str>,
    ) -> ShardObservation<UsageShardRow> {
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

    const CEILING: std::time::Duration = std::time::Duration::from_secs(90 * 24 * 60 * 60);
    const TEST_MAX_GROUPS: usize = 1000;

    // ── UsageParams::from_query_pairs ──────────────────────────────────────

    #[test]
    fn params_requires_from() {
        let err = UsageParams::from_query_pairs(
            &pairs(&[("to", "2026-01-02T00:00:00Z")]),
            at(0),
            CEILING,
        )
        .unwrap_err();
        assert!(err.contains("missing required parameter 'from'"), "{err}");
    }

    #[test]
    fn params_requires_to() {
        let err = UsageParams::from_query_pairs(
            &pairs(&[("from", "2026-01-01T00:00:00Z")]),
            at(0),
            CEILING,
        )
        .unwrap_err();
        assert!(err.contains("missing required parameter 'to'"), "{err}");
    }

    #[test]
    fn params_rejects_malformed_from() {
        let err = UsageParams::from_query_pairs(
            &pairs(&[("from", "not-a-date"), ("to", "2026-01-02T00:00:00Z")]),
            at(0),
            CEILING,
        )
        .unwrap_err();
        assert!(err.contains("invalid from"), "{err}");
    }

    #[test]
    fn params_rejects_from_after_to() {
        let err = UsageParams::from_query_pairs(
            &pairs(&[
                ("from", "2026-01-02T00:00:00Z"),
                ("to", "2026-01-01T00:00:00Z"),
            ]),
            at(0),
            CEILING,
        )
        .unwrap_err();
        assert!(err.contains("is after"), "{err}");
    }

    #[test]
    fn params_rejects_window_wider_than_ceiling_and_names_it() {
        let err = UsageParams::from_query_pairs(
            &pairs(&[
                ("from", "2026-01-01T00:00:00Z"),
                ("to", "2026-05-01T00:00:00Z"),
            ]),
            at(0),
            CEILING,
        )
        .unwrap_err();
        assert!(err.contains("90-day"), "expected ceiling named, got: {err}");
    }

    #[test]
    fn params_accepts_window_exactly_at_ceiling() {
        let from = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = from + chrono::Duration::from_std(CEILING).unwrap();
        let params = UsageParams::from_query_pairs(
            &pairs(&[("from", &from.to_rfc3339()), ("to", &to.to_rfc3339())]),
            at(0),
            CEILING,
        )
        .expect("window exactly at the ceiling should be accepted");
        assert_eq!(params.from, from);
        assert_eq!(params.to, to);
    }

    #[test]
    fn params_default_group_by_is_workflow_name() {
        let params = UsageParams::from_query_pairs(
            &pairs(&[
                ("from", "2026-01-01T00:00:00Z"),
                ("to", "2026-01-02T00:00:00Z"),
            ]),
            at(0),
            CEILING,
        )
        .unwrap();
        assert_eq!(params.group_by, UsageGroupBy::WorkflowName);
    }

    #[test]
    fn params_parses_search_attr_group_by() {
        let params = UsageParams::from_query_pairs(
            &pairs(&[
                ("from", "2026-01-01T00:00:00Z"),
                ("to", "2026-01-02T00:00:00Z"),
                ("group_by", "search_attr:tenant_id"),
            ]),
            at(0),
            CEILING,
        )
        .unwrap();
        assert_eq!(
            params.group_by,
            UsageGroupBy::SearchAttr("tenant_id".to_string())
        );
    }

    #[test]
    fn params_rejects_invalid_group_by() {
        let err = UsageParams::from_query_pairs(
            &pairs(&[
                ("from", "2026-01-01T00:00:00Z"),
                ("to", "2026-01-02T00:00:00Z"),
                ("group_by", "bogus"),
            ]),
            at(0),
            CEILING,
        )
        .unwrap_err();
        assert!(err.contains("unknown group_by"), "{err}");
    }

    #[test]
    fn params_accepts_relative_duration_since_now() {
        let now = DateTime::parse_from_rfc3339("2026-02-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let params =
            UsageParams::from_query_pairs(&pairs(&[("from", "24h"), ("to", "1h")]), now, CEILING)
                .unwrap();
        assert_eq!(params.to, now - chrono::Duration::hours(1));
        assert_eq!(params.from, now - chrono::Duration::hours(24));
    }

    #[test]
    fn params_ignores_unknown_query_keys() {
        let params = UsageParams::from_query_pairs(
            &pairs(&[
                ("from", "2026-01-01T00:00:00Z"),
                ("to", "2026-01-02T00:00:00Z"),
                ("bogus_param", "x"),
            ]),
            at(0),
            CEILING,
        )
        .unwrap();
        assert_eq!(params.group_by, UsageGroupBy::WorkflowName);
    }

    // ── format_ceiling (issue #596 F5) ───────────────────────────────────────

    #[test]
    fn format_ceiling_prefers_whole_days() {
        assert_eq!(
            format_ceiling(std::time::Duration::from_secs(2 * 86_400)),
            "2-day"
        );
    }

    #[test]
    fn format_ceiling_falls_back_to_whole_hours() {
        assert_eq!(
            format_ceiling(std::time::Duration::from_secs(30 * 3600)),
            "30-hour"
        );
    }

    #[test]
    fn format_ceiling_falls_back_to_seconds() {
        assert_eq!(
            format_ceiling(std::time::Duration::from_secs(90)),
            "90-second"
        );
    }

    #[test]
    fn params_rejects_window_wider_than_non_whole_day_ceiling_and_names_hours_not_misrounded_days()
    {
        let ceiling = std::time::Duration::from_secs(30 * 3600); // 30h — not a whole day
        let err = UsageParams::from_query_pairs(
            &pairs(&[
                ("from", "2026-01-01T00:00:00Z"),
                ("to", "2026-01-05T00:00:00Z"),
            ]),
            at(0),
            ceiling,
        )
        .unwrap_err();
        assert!(
            err.contains("30-hour"),
            "expected 30-hour named, got: {err}"
        );
        assert!(!err.contains("1-day"), "must not mis-round to 1-day: {err}");
    }

    // ── build_usage_response ───────────────────────────────────────────────

    #[test]
    fn sums_counts_across_shards() {
        let resp = build_usage_response(
            at(0),
            at(1000),
            &UsageGroupBy::WorkflowName,
            vec![
                obs(0, vec![row("onboarding", 5, 3, 1)], None),
                obs(1, vec![row("onboarding", 2, 2, 0)], None),
            ],
            TEST_MAX_GROUPS,
        )
        .unwrap();

        assert_eq!(resp.status, UsageReportStatus::Complete);
        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group, "onboarding");
        assert_eq!(resp.groups[0].workflow_starts, 7);
        assert_eq!(resp.groups[0].completed, 5);
        assert_eq!(resp.groups[0].failed, 1);
        assert!(resp.unavailable_shards.is_empty());
    }

    #[test]
    fn sums_activity_compute_seconds_as_f64() {
        let mut a = row("acme", 1, 1, 0);
        a.activity_compute_seconds = 12.5;
        let mut b = row("acme", 1, 1, 0);
        b.activity_compute_seconds = 7.5;
        let resp = build_usage_response(
            at(0),
            at(1000),
            &UsageGroupBy::WorkflowName,
            vec![obs(0, vec![a], None), obs(1, vec![b], None)],
            TEST_MAX_GROUPS,
        )
        .unwrap();
        assert!((resp.groups[0].activity_compute_seconds - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn groups_sorted_lexicographically_by_group() {
        let resp = build_usage_response(
            at(0),
            at(1000),
            &UsageGroupBy::WorkflowName,
            vec![obs(
                0,
                vec![
                    row("zeta", 1, 0, 0),
                    row("alpha", 1, 0, 0),
                    row("mid", 1, 0, 0),
                ],
                None,
            )],
            TEST_MAX_GROUPS,
        )
        .unwrap();
        let names: Vec<&str> = resp.groups.iter().map(|g| g.group.as_str()).collect();
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
    }

    #[test]
    fn unattributed_group_passes_through_unchanged() {
        let resp = build_usage_response(
            at(0),
            at(1000),
            &UsageGroupBy::SearchAttr("tenant_id".to_string()),
            vec![obs(
                0,
                vec![row(autumn_harvest::usage::UNATTRIBUTED_GROUP, 3, 1, 0)],
                None,
            )],
            TEST_MAX_GROUPS,
        )
        .unwrap();
        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group, "(unattributed)");
    }

    #[test]
    fn unavailable_shard_makes_partial_and_is_named_with_reason() {
        let resp = build_usage_response(
            at(0),
            at(1000),
            &UsageGroupBy::WorkflowName,
            vec![
                obs(0, vec![row("onboarding", 4, 0, 0)], None),
                obs(1, vec![], Some("connection refused")),
            ],
            TEST_MAX_GROUPS,
        )
        .unwrap();

        assert_eq!(resp.status, UsageReportStatus::Partial);
        assert_eq!(resp.unavailable_shards.len(), 1);
        assert_eq!(resp.unavailable_shards[0].shard_id, 1);
        assert_eq!(resp.unavailable_shards[0].reason, "connection refused");
        // The call does not fail wholesale: the reachable shard's data still flows through.
        assert_eq!(resp.groups[0].workflow_starts, 4);
    }

    #[test]
    fn all_shards_unavailable_is_unavailable_not_a_hard_failure() {
        let resp = build_usage_response(
            at(0),
            at(1000),
            &UsageGroupBy::WorkflowName,
            vec![obs(0, vec![], Some("pool missing"))],
            TEST_MAX_GROUPS,
        )
        .unwrap();

        assert_eq!(resp.status, UsageReportStatus::Unavailable);
        assert!(resp.groups.is_empty());
        assert_eq!(resp.unavailable_shards.len(), 1);
    }

    // ── usage_max_groups cap (issue #596) ────────────────────────────────────

    #[test]
    fn cap_exceeded_at_cap_plus_one_groups() {
        let rows: Vec<UsageShardRow> = (0..11).map(|i| row(&format!("wf_{i}"), 1, 0, 0)).collect();
        let err = build_usage_response(
            at(0),
            at(1000),
            &UsageGroupBy::WorkflowName,
            vec![obs(0, rows, None)],
            10,
        )
        .unwrap_err();
        assert_eq!(err.group_count, 11);
        assert_eq!(err.cap, 10);
    }

    #[test]
    fn succeeds_at_exactly_cap_groups() {
        let rows: Vec<UsageShardRow> = (0..10).map(|i| row(&format!("wf_{i}"), 1, 0, 0)).collect();
        let resp = build_usage_response(
            at(0),
            at(1000),
            &UsageGroupBy::WorkflowName,
            vec![obs(0, rows, None)],
            10,
        )
        .expect("exactly at cap must succeed");
        assert_eq!(resp.groups.len(), 10);
    }

    #[test]
    fn cap_exceeded_display_names_the_cap() {
        let err = UsageGroupCapExceeded {
            group_count: 12_345,
            cap: 10_000,
        };
        let msg = err.to_string();
        assert!(msg.contains("12345"), "{msg}");
        assert!(msg.contains("10000"), "{msg}");
    }

    #[test]
    fn record_serializes_exactly_the_nine_ac_fields() {
        let record = UsageGroupRecord {
            group: "acme".to_string(),
            workflow_starts: 1,
            completed: 1,
            failed: 0,
            cancelled: 0,
            timed_out: 0,
            activity_executions: 2,
            activity_executions_failed: 0,
            activity_compute_seconds: 3.5,
        };
        let value = serde_json::to_value(&record).unwrap();
        let obj = value.as_object().unwrap();
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        let mut expected = vec![
            "group",
            "workflow_starts",
            "completed",
            "failed",
            "cancelled",
            "timed_out",
            "activity_executions",
            "activity_executions_failed",
            "activity_compute_seconds",
        ];
        expected.sort_unstable();
        assert_eq!(keys, expected);
    }

    #[test]
    fn status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(UsageReportStatus::Complete).unwrap(),
            serde_json::json!("complete")
        );
        assert_eq!(
            serde_json::to_value(UsageReportStatus::Partial).unwrap(),
            serde_json::json!("partial")
        );
        assert_eq!(
            serde_json::to_value(UsageReportStatus::Unavailable).unwrap(),
            serde_json::json!("unavailable")
        );
    }
}
