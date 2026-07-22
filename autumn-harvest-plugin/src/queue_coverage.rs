//! Management read model for queue coverage — "which task queues have pending
//! work but no worker draining them?" (issue #774).
//!
//! Answers the operator question *"right now, across every shard, which task
//! queues have pending, claimable work that no live worker is polling?"* — the
//! classic silent-stall failure mode where executions pile up on a queue name
//! that no deployed worker binds. This module owns the **pure** cross-shard
//! merge over per-shard uncovered-queue candidates; the per-shard SQL and the
//! shard fan-out are wired in a later milestone.
//!
//! ## Response shape
//!
//! The response is an **eventually-consistent point-in-time snapshot**: it
//! reflects committed queue/worker state at query time and carries no replay
//! or ordering guarantee under concurrent writes.
//!
//! ## Partial answers
//!
//! A shard that could not be queried is folded into `unavailable_shards`
//! (named with a reason, sorted by `shard_id`) and drops the report to
//! `partial`/`unavailable` — it is **never silently dropped** (issue #774,
//! AC6). On a `partial` answer the reported `uncovered_queues` and counts
//! cover only the **reachable** shards (a documented lower bound): a queue
//! uncovered exclusively on an unreachable shard cannot appear.

use std::collections::BTreeMap;
use std::time::Duration;

use autumn_harvest::error::HarvestResult;
use autumn_harvest::{queue, workers};
use diesel_async::AsyncPgConnection;
use serde::Serialize;

use crate::shard_fanout::{self, FanoutStatus, ShardObservation, UnavailableShard};

/// One uncovered task queue in the merged response: a queue with pending,
/// claimable work but no live worker draining it.
#[derive(Debug, Clone, Serialize)]
pub struct UncoveredQueue {
    /// Task queue name.
    pub queue_name: String,
    /// Number of pending executions on this queue, summed across shards.
    pub pending_count: i64,
    /// A bounded, deterministic sample of execution ids waiting on this queue,
    /// concatenated across shards (in shard-iteration order) and truncated to
    /// the request's sample cap.
    pub sample_execution_ids: Vec<String>,
}

/// The full queue-coverage response (issue #774).
#[derive(Debug, Clone, Serialize)]
pub struct QueueCoverageResponse {
    /// Cross-shard completeness of this snapshot.
    pub status: FanoutStatus,
    /// `true` when at least one uncovered queue was found (across the reachable
    /// shards).
    pub uncovered: bool,
    /// Number of distinct uncovered queues returned.
    pub total_uncovered_queues: usize,
    /// Uncovered queues, ordered by `queue_name` ascending (deterministic).
    pub uncovered_queues: Vec<UncoveredQueue>,
    /// Shards that could not be queried, named with a reason. Never silently
    /// dropped; on a non-`complete` status the counts above are a lower bound
    /// over the reachable shards only.
    pub unavailable_shards: Vec<UnavailableShard>,
}

/// A single shard's report of one uncovered queue.
///
/// This is the per-shard row the fan-out query produces. A shard reports a
/// `Vec<QueueCandidate>` — one entry per uncovered queue it sees — carried as
/// the `rows` of a [`ShardObservation`], mirroring `workflow_count`'s
/// `ShardObservation<WorkflowCountRow>` shape.
#[derive(Debug, Clone)]
pub struct QueueCandidate {
    /// Task queue name.
    pub queue_name: String,
    /// Pending executions on this queue on the reporting shard.
    pub pending_count: i64,
    /// A bounded sample of execution ids on this queue on the reporting shard.
    pub sample_execution_ids: Vec<String>,
}

/// Merge per-shard uncovered-queue candidates into the final response (pure,
/// no DB).
///
/// Candidates are merged **across shards by `queue_name`**: `pending_count` is
/// summed and `sample_execution_ids` are concatenated (in shard-iteration
/// order) then truncated to `sample_cap`. The `uncovered_queues` list is
/// ordered by `queue_name` ascending (a `BTreeMap` accumulator gives this for
/// free) so the output is deterministic. `status`/`unavailable_shards` derive
/// from the observations exactly like `workflow_count`'s builder: `complete`
/// only when every shard was inspected, `partial` when at least one was
/// inspected and at least one was unavailable, `unavailable` when none could
/// be. An unreachable shard therefore degrades the report rather than failing
/// it wholesale, and its uncovered queues are absent (documented lower bound).
#[must_use]
#[allow(clippy::needless_pass_by_value)]
pub fn build_queue_coverage_response(
    observations: Vec<ShardObservation<QueueCandidate>>,
    sample_cap: usize,
) -> QueueCoverageResponse {
    let (inspected, unavailable_shards) = shard_fanout::summarize_shard_errors(&observations);

    // BTreeMap keyed by queue_name gives ascending, deterministic ordering.
    let mut merged: BTreeMap<String, (i64, Vec<String>)> = BTreeMap::new();
    for observation in observations {
        for candidate in observation.rows {
            let entry = merged
                .entry(candidate.queue_name)
                .or_insert_with(|| (0, Vec::new()));
            entry.0 += candidate.pending_count;
            entry.1.extend(candidate.sample_execution_ids);
        }
    }

    let uncovered_queues: Vec<UncoveredQueue> = merged
        .into_iter()
        .map(|(queue_name, (pending_count, mut sample_execution_ids))| {
            sample_execution_ids.truncate(sample_cap);
            UncoveredQueue {
                queue_name,
                pending_count,
                sample_execution_ids,
            }
        })
        .collect();

    let total_uncovered_queues = uncovered_queues.len();

    QueueCoverageResponse {
        status: FanoutStatus::from_counts(inspected, unavailable_shards.len()),
        uncovered: total_uncovered_queues > 0,
        total_uncovered_queues,
        uncovered_queues,
        unavailable_shards,
    }
}

/// Per-shard uncovered-queue diff: queues with claimable pending work that no
/// live worker on this shard polls (issue #774).
///
/// Reads the shard's claimable-pending demand with execution-id samples
/// ([`queue::pending_demand_by_queue_with_samples`]) and the set of queues
/// polled by a live worker ([`workers::live_polled_queue_names`], AC1: fresh
/// heartbeat within `stale_threshold` and status not `Stopped`, so `Draining`
/// counts), then returns the demand queues **not** in the live-polled set as
/// [`QueueCandidate`]s. This is the per-shard row the cross-shard fan-out
/// (wired in a later milestone) calls, feeding
/// [`build_queue_coverage_response`].
///
/// `circuit_breaker_activities` mirrors `claim_task`'s breaker-exempt set (see
/// [`queue::pending_demand_by_queue_with_samples`]); `sample_cap` bounds the
/// per-shard sample size.
///
/// This is a read-only diff over two read-only queries.
///
/// # Errors
///
/// Returns [`HarvestResult`] error on database failure of either read.
pub async fn shard_uncovered_candidates(
    conn: &mut AsyncPgConnection,
    circuit_breaker_activities: &[String],
    stale_threshold: Duration,
    sample_cap: usize,
) -> HarvestResult<Vec<QueueCandidate>> {
    let demand =
        queue::pending_demand_by_queue_with_samples(conn, circuit_breaker_activities, sample_cap)
            .await?;
    let covered = workers::live_polled_queue_names(conn, stale_threshold).await?;

    Ok(demand
        .into_iter()
        .filter(|d| !covered.contains(&d.queue_name))
        .map(|d| QueueCandidate {
            queue_name: d.queue_name,
            pending_count: d.pending_count,
            sample_execution_ids: d
                .sample_execution_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(queue: &str, pending: i64, samples: &[&str]) -> QueueCandidate {
        QueueCandidate {
            queue_name: queue.to_string(),
            pending_count: pending,
            sample_execution_ids: samples.iter().map(ToString::to_string).collect(),
        }
    }

    fn ok_obs(shard_id: i32, rows: Vec<QueueCandidate>) -> ShardObservation<QueueCandidate> {
        ShardObservation {
            shard_id,
            rows,
            error: None,
        }
    }

    fn err_obs(shard_id: i32, reason: &str) -> ShardObservation<QueueCandidate> {
        ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(reason.to_string()),
        }
    }

    #[test]
    fn same_queue_on_two_shards_is_merged() {
        let resp = build_queue_coverage_response(
            vec![
                ok_obs(0, vec![cand("email", 3, &["a", "b"])]),
                ok_obs(1, vec![cand("email", 5, &["c"])]),
            ],
            10,
        );

        assert_eq!(resp.status, FanoutStatus::Complete);
        assert!(resp.uncovered);
        assert_eq!(resp.total_uncovered_queues, 1);
        assert!(resp.unavailable_shards.is_empty());

        let q = &resp.uncovered_queues[0];
        assert_eq!(q.queue_name, "email");
        assert_eq!(q.pending_count, 8);
        // samples concatenated in shard-iteration order
        assert_eq!(q.sample_execution_ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn distinct_queues_across_shards_are_sorted_by_name() {
        let resp = build_queue_coverage_response(
            vec![
                ok_obs(0, vec![cand("sms", 2, &["x"])]),
                ok_obs(1, vec![cand("email", 4, &["y"])]),
            ],
            10,
        );

        assert_eq!(resp.status, FanoutStatus::Complete);
        assert_eq!(resp.total_uncovered_queues, 2);
        let names: Vec<&str> = resp
            .uncovered_queues
            .iter()
            .map(|q| q.queue_name.as_str())
            .collect();
        assert_eq!(names, vec!["email", "sms"]);
    }

    #[test]
    fn no_uncovered_queues_reports_covered() {
        let resp = build_queue_coverage_response(vec![ok_obs(0, vec![]), ok_obs(1, vec![])], 10);

        assert_eq!(resp.status, FanoutStatus::Complete);
        assert!(!resp.uncovered);
        assert_eq!(resp.total_uncovered_queues, 0);
        assert!(resp.uncovered_queues.is_empty());
        assert!(resp.unavailable_shards.is_empty());
    }

    #[test]
    fn one_shard_errored_degrades_to_partial_and_names_the_shard() {
        let resp = build_queue_coverage_response(
            vec![
                ok_obs(0, vec![cand("email", 3, &["a"])]),
                err_obs(1, "connection refused"),
            ],
            10,
        );

        assert_eq!(resp.status, FanoutStatus::Partial);
        // uncovered reflects the reachable shard only (documented lower bound)
        assert!(resp.uncovered);
        assert_eq!(resp.total_uncovered_queues, 1);
        assert_eq!(resp.uncovered_queues[0].pending_count, 3);

        assert_eq!(resp.unavailable_shards.len(), 1);
        assert_eq!(resp.unavailable_shards[0].shard_id, 1);
        assert_eq!(resp.unavailable_shards[0].reason, "connection refused");
    }

    #[test]
    fn all_shards_errored_reports_unavailable() {
        let resp =
            build_queue_coverage_response(vec![err_obs(0, "down"), err_obs(1, "timeout")], 10);

        assert_eq!(resp.status, FanoutStatus::Unavailable);
        // lower-bound behavior: no shard was inspected, so nothing is uncovered
        assert!(!resp.uncovered);
        assert_eq!(resp.total_uncovered_queues, 0);
        assert!(resp.uncovered_queues.is_empty());
        // unavailable shards are named and sorted by shard_id
        assert_eq!(resp.unavailable_shards.len(), 2);
        assert_eq!(resp.unavailable_shards[0].shard_id, 0);
        assert_eq!(resp.unavailable_shards[1].shard_id, 1);
    }

    #[test]
    fn sample_cap_truncates_to_exactly_cap() {
        let resp = build_queue_coverage_response(
            vec![ok_obs(
                0,
                vec![cand("email", 5, &["1", "2", "3", "4", "5"])],
            )],
            3,
        );

        assert_eq!(resp.total_uncovered_queues, 1);
        let q = &resp.uncovered_queues[0];
        assert_eq!(q.pending_count, 5);
        // exactly `sample_cap` ids, keeping the first N deterministically
        assert_eq!(q.sample_execution_ids, vec!["1", "2", "3"]);
    }
}
