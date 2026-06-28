//! Management read model for per-schedule run history (issue #534).
//!
//! Answers the first triage question an operator asks of a misbehaving cron:
//! *"show me this schedule's last N runs and which ones failed."* A schedule's
//! runs are attributed at start time via
//! [`schedule_id`](autumn_harvest::StartWorkflowParams::schedule_id) +
//! [`origin`](autumn_harvest::StartWorkflowParams::origin), so this endpoint can
//! list exactly the executions a schedule launched — without conflating them with
//! manually-started, signal-with-start, or unrelated runs of the same workflow
//! type — and break a backfill storm or ad-hoc trigger out from normal cadence.
//!
//! The runs may be spread across shards, so the handler fans
//! [`list_schedule_runs`](autumn_harvest::list_schedule_runs) and
//! [`schedule_run_state_summary`](autumn_harvest::schedule_run_state_summary) out
//! across [`iter_shards`](crate::state::HarvestDbPool::iter_shards) and merges the
//! per-shard observations here with [`build_runs_response`]. The merge is a pure
//! function (no DB) so the keyset-merge ordering, limit/`next_cursor` reporting,
//! cadence-summary summation, and partial-shard status are all unit-testable.
//!
//! ## Partial answers
//!
//! When a shard is unreachable it is reported in `shards` (never silently
//! dropped) and the report `status` becomes `partial` (some inspected) or
//! `unavailable` (none inspected) — never a hard 500. The cadence summary and run
//! list then reflect only the inspected shards.

use autumn_harvest::{ScheduleRunRow, ScheduleRunStateCount};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use uuid::Uuid;

/// Cross-shard completeness of a run-history response.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunsReportStatus {
    /// Every expected shard was inspected.
    Complete,
    /// At least one shard was inspected and at least one was unavailable.
    Partial,
    /// No shard could be inspected.
    Unavailable,
}

/// Per-shard run + summary observation fed into [`build_runs_response`].
#[derive(Debug, Clone)]
pub struct ShardRunsObservation {
    /// Shard identifier.
    pub shard_id: i32,
    /// Runs this shard returned (already limited to `limit + 1` by the query).
    pub runs: Vec<ScheduleRunRow>,
    /// Per-state cadence counts (`scheduled`-origin only) this shard returned.
    pub summary: Vec<ScheduleRunStateCount>,
    /// Error description when the shard could not be queried.
    pub error: Option<String>,
}

/// One run in the response list.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleRunEntry {
    /// Execution id of the run.
    pub execution_id: Uuid,
    /// Logical schedule slot the run fired for (`null` for a `manual_trigger`).
    pub nominal_fire_time: Option<DateTime<Utc>>,
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When the run reached a terminal state, or `null` while active.
    pub completed_at: Option<DateTime<Utc>>,
    /// Current execution state.
    pub state: String,
    /// Dispatch origin (`scheduled`/`backfill`/`manual_trigger`).
    pub origin: Option<String>,
}

impl From<ScheduleRunRow> for ScheduleRunEntry {
    fn from(row: ScheduleRunRow) -> Self {
        Self {
            execution_id: row.execution_id,
            nominal_fire_time: row.nominal_fire_time,
            started_at: row.started_at,
            completed_at: row.completed_at,
            state: row.state,
            origin: row.origin,
        }
    }
}

/// Per-terminal-state counts of the schedule's scheduled cadence.
///
/// Counts the **scheduled-origin** runs over the queried window. Backfill and
/// manual-trigger runs are excluded so the failure ratio an operator reads here
/// reflects the true cadence.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ScheduleRunSummary {
    /// `COMPLETED` runs.
    pub succeeded: i64,
    /// `FAILED` runs.
    pub failed: i64,
    /// `TIMED_OUT` runs.
    pub timed_out: i64,
    /// `CANCELLED` runs.
    pub cancelled: i64,
    /// `TERMINATED` runs.
    pub terminated: i64,
    /// `CONTINUED_AS_NEW` runs.
    pub continued_as_new: i64,
    /// `RUNNING` runs.
    pub running: i64,
    /// `PAUSED` runs.
    pub paused: i64,
    /// Any state not in the known set above (forward-compatibility).
    pub other: i64,
    /// Total scheduled-origin runs counted in the window.
    pub total: i64,
}

impl ScheduleRunSummary {
    fn add(&mut self, state: &str, count: i64) {
        match state {
            "COMPLETED" => self.succeeded += count,
            "FAILED" => self.failed += count,
            "TIMED_OUT" => self.timed_out += count,
            "CANCELLED" => self.cancelled += count,
            "TERMINATED" => self.terminated += count,
            "CONTINUED_AS_NEW" => self.continued_as_new += count,
            "RUNNING" => self.running += count,
            "PAUSED" => self.paused += count,
            _ => self.other += count,
        }
        self.total += count;
    }
}

/// Per-shard inspection record surfaced in the response.
#[derive(Debug, Clone, Serialize)]
pub struct RunsShardInspection {
    /// Shard identifier.
    pub shard_id: i32,
    /// `"inspected"` or `"unavailable"`.
    pub status: &'static str,
    /// Error detail when unavailable.
    pub error: Option<String>,
}

/// Read-only per-schedule run-history response.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleRunsResponse {
    /// The schedule whose runs these are.
    pub schedule_id: Uuid,
    /// Cross-shard completeness of this response.
    pub status: RunsReportStatus,
    /// The schedule's runs, newest-first, capped at `limit`.
    pub runs: Vec<ScheduleRunEntry>,
    /// Per-state cadence summary over the window (scheduled-origin only).
    pub summary: ScheduleRunSummary,
    /// The applied row cap. Reported (not silently truncated): when more rows
    /// exist than `limit`, `next_cursor` is set.
    pub limit: i64,
    /// Opaque keyset cursor for the next page, or `null` on the last page.
    pub next_cursor: Option<String>,
    /// Per-shard inspection outcomes, including any unavailable shard.
    pub shards: Vec<RunsShardInspection>,
}

/// Default page size when `limit` is omitted.
pub const DEFAULT_LIMIT: i64 = 100;
/// Hard cap on `limit`; larger requests are clamped down (and reported via the
/// echoed `limit` + `next_cursor`, never silently truncated to a smaller number).
pub const MAX_LIMIT: i64 = 1000;

/// Parsed query parameters for `GET /admin/schedules/{id}/runs` (issue #534).
///
/// Pure and testable: `state`/`origin` are repeatable (and each value may be a
/// comma-separated list); `since`/`until` accept an RFC 3339 timestamp or a
/// relative duration (`24h`); `limit` is clamped to `[1, MAX_LIMIT]`. Domain
/// validation of `state`/`origin` values is left to the caller (it owns the
/// authoritative state/origin vocabularies).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScheduleRunsParams {
    /// State filter (empty = all states).
    pub states: Vec<String>,
    /// Origin filter (empty = all origins).
    pub origins: Vec<String>,
    /// Inclusive lower bound on `started_at`.
    pub since: Option<DateTime<Utc>>,
    /// Exclusive upper bound on `started_at`.
    pub until: Option<DateTime<Utc>>,
    /// Keyset cursor `(started_at, execution_id)`.
    pub cursor: Option<(DateTime<Utc>, Uuid)>,
    /// Applied row cap.
    pub limit: i64,
}

fn parse_instant(field: &str, value: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    let trimmed = value.trim();
    if let Ok(parsed) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(parsed.with_timezone(&Utc));
    }
    if let Some(dur) = autumn_harvest::task_duration(trimmed)
        && let Ok(chrono_dur) = chrono::Duration::from_std(dur)
    {
        return Ok(now - chrono_dur);
    }
    Err(format!(
        "invalid {field} '{value}'; expected an RFC 3339 timestamp or a relative duration like '24h'"
    ))
}

fn push_csv(target: &mut Vec<String>, value: &str) {
    for part in value.split(',') {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            target.push(trimmed.to_string());
        }
    }
}

impl ScheduleRunsParams {
    /// Parse the raw query pairs. `now` anchors relative `since`/`until` durations.
    ///
    /// # Errors
    ///
    /// Returns `Err` with a human-readable message for a malformed `limit`,
    /// `since`, `until`, or `cursor` — the handler maps this to `400`, never `500`.
    pub fn from_query_pairs(
        pairs: &[(String, String)],
        now: DateTime<Utc>,
    ) -> Result<Self, String> {
        let mut out = Self {
            limit: DEFAULT_LIMIT,
            ..Self::default()
        };
        for (key, value) in pairs {
            match key.as_str() {
                "state" => push_csv(&mut out.states, value),
                "origin" => push_csv(&mut out.origins, value),
                "since" => out.since = Some(parse_instant("since", value, now)?),
                "until" => out.until = Some(parse_instant("until", value, now)?),
                "cursor" => out.cursor = Some(parse_cursor(value)?),
                "limit" => {
                    let n: i64 = value
                        .trim()
                        .parse()
                        .map_err(|_| format!("invalid limit '{value}'; expected an integer"))?;
                    if n < 1 {
                        return Err(format!("invalid limit '{value}'; must be >= 1"));
                    }
                    out.limit = n.min(MAX_LIMIT);
                }
                // Unknown keys are ignored for forward-compatibility.
                _ => {}
            }
        }
        Ok(out)
    }
}

/// Encode a `(started_at, execution_id)` keyset cursor into the opaque token
/// format `"<rfc3339_micros>|<uuid>"` (matching the #514 workflow-list cursor).
#[must_use]
pub fn encode_cursor(started_at: DateTime<Utc>, execution_id: Uuid) -> String {
    format!(
        "{}|{}",
        started_at.to_rfc3339_opts(SecondsFormat::Micros, true),
        execution_id
    )
}

/// Parse an opaque keyset cursor token back into `(started_at, execution_id)`.
///
/// # Errors
///
/// Returns `Err` with a human-readable reason when the token is malformed.
pub fn parse_cursor(raw: &str) -> Result<(DateTime<Utc>, Uuid), String> {
    let (ts_part, id_part) = raw
        .split_once('|')
        .ok_or_else(|| "invalid cursor; expected '<started_at>|<execution_id>'".to_string())?;
    let started_at = DateTime::parse_from_rfc3339(ts_part)
        .map_err(|_| "invalid cursor: bad timestamp".to_string())?
        .with_timezone(&Utc);
    let execution_id = id_part
        .parse::<Uuid>()
        .map_err(|_| "invalid cursor: bad execution id".to_string())?;
    Ok((started_at, execution_id))
}

/// Merge per-shard observations into the final run-history response (pure, no DB).
///
/// Runs are merged across shards, sorted newest-first (`started_at DESC,
/// execution_id DESC`), and truncated to `limit`. Each shard is queried for
/// `limit + 1` rows, so a merged total exceeding `limit` means a further page
/// exists: the list is truncated and `next_cursor` is derived from the last kept
/// row. The cadence summary sums each shard's scheduled-origin per-state counts.
/// `status` is `complete` only when every shard was inspected.
#[must_use]
pub fn build_runs_response(
    schedule_id: Uuid,
    limit: i64,
    observations: Vec<ShardRunsObservation>,
) -> ScheduleRunsResponse {
    let limit = limit.max(0);

    let inspected = observations.iter().filter(|o| o.error.is_none()).count();
    let unavailable = observations.iter().filter(|o| o.error.is_some()).count();

    // Merge runs across shards and sort newest-first by the keyset key.
    let mut merged: Vec<ScheduleRunRow> = observations
        .iter()
        .flat_map(|o| o.runs.iter().cloned())
        .collect();
    merged.sort_by(|a, b| {
        b.started_at
            .cmp(&a.started_at)
            .then_with(|| b.execution_id.cmp(&a.execution_id))
    });

    // A merged total beyond `limit` means at least one more row exists in the
    // window: truncate and emit a cursor anchored on the last kept row.
    let keep = usize::try_from(limit).unwrap_or(usize::MAX);
    let has_more = merged.len() > keep;
    if has_more {
        merged.truncate(keep);
    }
    let next_cursor = if has_more {
        merged
            .last()
            .map(|row| encode_cursor(row.started_at, row.execution_id))
    } else {
        None
    };

    // Sum each shard's scheduled-origin per-state counts into the cadence summary.
    let mut summary = ScheduleRunSummary::default();
    for observation in &observations {
        for ScheduleRunStateCount { state, count } in &observation.summary {
            summary.add(state, *count);
        }
    }

    let mut shards: Vec<RunsShardInspection> = observations
        .into_iter()
        .map(|o| RunsShardInspection {
            shard_id: o.shard_id,
            status: if o.error.is_some() {
                "unavailable"
            } else {
                "inspected"
            },
            error: o.error,
        })
        .collect();
    shards.sort_by_key(|s| s.shard_id);

    let status = if inspected == 0 {
        RunsReportStatus::Unavailable
    } else if unavailable > 0 {
        RunsReportStatus::Partial
    } else {
        RunsReportStatus::Complete
    };

    ScheduleRunsResponse {
        schedule_id,
        status,
        runs: merged.into_iter().map(ScheduleRunEntry::from).collect(),
        summary,
        limit,
        next_cursor,
        shards,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn uid(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn run(exec: u128, started: i64, state: &str, origin: &str) -> ScheduleRunRow {
        ScheduleRunRow {
            execution_id: uid(exec),
            nominal_fire_time: Some(at(started)),
            started_at: at(started),
            completed_at: None,
            state: state.to_string(),
            origin: Some(origin.to_string()),
        }
    }

    fn counts(pairs: &[(&str, i64)]) -> Vec<ScheduleRunStateCount> {
        pairs
            .iter()
            .map(|(s, c)| ScheduleRunStateCount {
                state: (*s).to_string(),
                count: *c,
            })
            .collect()
    }

    fn obs(
        shard_id: i32,
        runs: Vec<ScheduleRunRow>,
        summary: Vec<ScheduleRunStateCount>,
        error: Option<&str>,
    ) -> ShardRunsObservation {
        ShardRunsObservation {
            shard_id,
            runs,
            summary,
            error: error.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn merges_runs_newest_first_across_shards() {
        let resp = build_runs_response(
            uid(1),
            10,
            vec![
                obs(
                    0,
                    vec![run(10, 100, "COMPLETED", "scheduled")],
                    counts(&[]),
                    None,
                ),
                obs(
                    1,
                    vec![
                        run(11, 300, "FAILED", "scheduled"),
                        run(12, 200, "RUNNING", "scheduled"),
                    ],
                    counts(&[]),
                    None,
                ),
            ],
        );

        let order: Vec<i64> = resp.runs.iter().map(|r| r.started_at.timestamp()).collect();
        assert_eq!(order, vec![300, 200, 100], "newest-first across shards");
        assert_eq!(resp.status, RunsReportStatus::Complete);
        assert!(resp.next_cursor.is_none());
    }

    #[test]
    fn limit_is_reported_with_next_cursor_when_truncated() {
        // 3 rows available, limit 2: each shard returned up to limit+1=3.
        let resp = build_runs_response(
            uid(1),
            2,
            vec![obs(
                0,
                vec![
                    run(10, 300, "COMPLETED", "scheduled"),
                    run(11, 200, "FAILED", "scheduled"),
                    run(12, 100, "COMPLETED", "scheduled"),
                ],
                counts(&[]),
                None,
            )],
        );

        assert_eq!(
            resp.runs.len(),
            2,
            "truncated to limit, not silently dropped"
        );
        assert_eq!(resp.limit, 2);
        let cursor = resp.next_cursor.expect("more rows -> cursor present");
        // Cursor anchors on the last kept row (started_at=200, exec=11).
        let (ts, id) = parse_cursor(&cursor).unwrap();
        assert_eq!(ts, at(200));
        assert_eq!(id, uid(11));
    }

    #[test]
    fn no_cursor_when_exactly_at_limit() {
        let resp = build_runs_response(
            uid(1),
            2,
            vec![obs(
                0,
                vec![
                    run(10, 200, "COMPLETED", "scheduled"),
                    run(11, 100, "FAILED", "scheduled"),
                ],
                counts(&[]),
                None,
            )],
        );
        assert_eq!(resp.runs.len(), 2);
        assert!(
            resp.next_cursor.is_none(),
            "exactly limit rows -> no next page"
        );
    }

    #[test]
    fn summary_sums_scheduled_counts_across_shards() {
        let resp = build_runs_response(
            uid(1),
            10,
            vec![
                obs(0, vec![], counts(&[("COMPLETED", 20), ("FAILED", 2)]), None),
                obs(
                    1,
                    vec![],
                    counts(&[("COMPLETED", 7), ("RUNNING", 1), ("TIMED_OUT", 0)]),
                    None,
                ),
            ],
        );

        assert_eq!(resp.summary.succeeded, 27);
        assert_eq!(resp.summary.failed, 2);
        assert_eq!(resp.summary.running, 1);
        assert_eq!(resp.summary.timed_out, 0);
        assert_eq!(resp.summary.total, 30);
    }

    #[test]
    fn unavailable_shard_makes_partial_and_is_named() {
        let resp = build_runs_response(
            uid(1),
            10,
            vec![
                obs(
                    0,
                    vec![run(10, 100, "COMPLETED", "scheduled")],
                    counts(&[("COMPLETED", 1)]),
                    None,
                ),
                obs(1, vec![], vec![], Some("connection refused")),
            ],
        );

        assert_eq!(resp.status, RunsReportStatus::Partial);
        let shard1 = resp.shards.iter().find(|s| s.shard_id == 1).unwrap();
        assert_eq!(shard1.status, "unavailable");
        assert_eq!(shard1.error.as_deref(), Some("connection refused"));
        // The one inspected shard's data still flows through.
        assert_eq!(resp.runs.len(), 1);
        assert_eq!(resp.summary.succeeded, 1);
    }

    #[test]
    fn all_shards_unavailable_is_unavailable_not_error() {
        let resp = build_runs_response(
            uid(1),
            10,
            vec![obs(0, vec![], vec![], Some("pool missing"))],
        );
        assert_eq!(resp.status, RunsReportStatus::Unavailable);
        assert!(resp.runs.is_empty());
        assert_eq!(resp.summary.total, 0);
    }

    #[test]
    fn cursor_round_trips() {
        let token = encode_cursor(at(12345), uid(99));
        let (ts, id) = parse_cursor(&token).unwrap();
        assert_eq!(ts, at(12345));
        assert_eq!(id, uid(99));
    }

    #[test]
    fn parse_cursor_rejects_garbage() {
        assert!(parse_cursor("not-a-cursor").is_err(), "missing delimiter");
        assert!(parse_cursor("not-a-time|abc").is_err(), "bad timestamp");
        assert!(
            parse_cursor("2026-01-01T00:00:00Z|not-a-uuid").is_err(),
            "bad uuid"
        );
    }

    fn pairs(kvs: &[(&str, &str)]) -> Vec<(String, String)> {
        kvs.iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn params_default_limit_when_omitted() {
        let p = ScheduleRunsParams::from_query_pairs(&[], at(0)).unwrap();
        assert_eq!(p.limit, DEFAULT_LIMIT);
        assert!(p.states.is_empty());
        assert!(p.cursor.is_none());
    }

    #[test]
    fn params_repeatable_and_csv_states() {
        let p = ScheduleRunsParams::from_query_pairs(
            &pairs(&[("state", "FAILED,TIMED_OUT"), ("state", "CANCELLED")]),
            at(0),
        )
        .unwrap();
        assert_eq!(p.states, vec!["FAILED", "TIMED_OUT", "CANCELLED"]);
    }

    #[test]
    fn params_limit_clamped_to_max() {
        let p =
            ScheduleRunsParams::from_query_pairs(&pairs(&[("limit", "100000")]), at(0)).unwrap();
        assert_eq!(p.limit, MAX_LIMIT);
    }

    #[test]
    fn params_reject_bad_limit() {
        assert!(ScheduleRunsParams::from_query_pairs(&pairs(&[("limit", "abc")]), at(0)).is_err());
        assert!(ScheduleRunsParams::from_query_pairs(&pairs(&[("limit", "0")]), at(0)).is_err());
    }

    #[test]
    fn params_relative_since_duration() {
        let now = at(1_000_000);
        let p = ScheduleRunsParams::from_query_pairs(&pairs(&[("since", "1h")]), now).unwrap();
        assert_eq!(p.since, Some(now - chrono::Duration::hours(1)));
    }

    #[test]
    fn params_rfc3339_until() {
        let p = ScheduleRunsParams::from_query_pairs(
            &pairs(&[("until", "2026-01-01T00:00:00Z")]),
            at(0),
        )
        .unwrap();
        assert!(p.until.is_some());
    }

    #[test]
    fn params_reject_bad_cursor() {
        assert!(
            ScheduleRunsParams::from_query_pairs(&pairs(&[("cursor", "garbage")]), at(0)).is_err()
        );
    }

    #[test]
    fn status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(RunsReportStatus::Complete).unwrap(),
            serde_json::json!("complete")
        );
        assert_eq!(
            serde_json::to_value(RunsReportStatus::Partial).unwrap(),
            serde_json::json!("partial")
        );
    }
}
