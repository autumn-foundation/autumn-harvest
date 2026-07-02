//! Shared helpers for cross-shard fan-out read operations.
//!
//! Several management-API read models (`version_usage`, `workflow_reachability`,
//! `schedule_runs`, `workflow_count`) fan out the same query across every
//! configured shard, collect results, and fold unreachable shards into a
//! `partial`/`unavailable` status. This module centralises the common
//! scaffolding so each read model only has to implement its own per-shard query
//! and accumulator logic.

use std::collections::{BTreeMap, BTreeSet};

use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::api::HarvestApiState;

/// Outcome of a single-shard query: either a successful row set or an error.
#[derive(Debug)]
pub struct ShardObservation<R> {
    /// Shard identifier.
    pub shard_id: i32,
    /// Rows returned by the query (empty when `error.is_some()`).
    pub rows: Vec<R>,
    /// Error description when the shard could not be queried.
    pub error: Option<String>,
}

/// Collect per-shard connection pools available in `api_state`.
///
/// Returns an empty map when no storage pool is installed.
#[must_use]
pub fn pools_by_shard(api_state: &HarvestApiState) -> BTreeMap<i32, DbPool> {
    api_state.storage_pool().map_or_else(
        |_| BTreeMap::new(),
        |pool| {
            pool.iter_shards()
                .map(|(shard, db_pool)| (shard.as_i32(), db_pool.clone()))
                .collect()
        },
    )
}

/// Build the full set of shard ids a fan-out read should attempt to inspect:
/// every shard with a live connection pool, plus every shard the router
/// already knows about (`readable_shards`/`default_shard`).
///
/// A shard the router knows about but for which this process has no pool yet
/// (e.g. mid a shard-add rollout — the router's `readable_shards` is widened
/// before every process has the new shard's pool wired up, see the workspace
/// CLAUDE.md "add a shard" procedure) must still appear in the returned set so
/// callers report it `unavailable` rather than silently omitting it from the
/// fan-out — an omitted shard would let a completeness `status` read
/// `complete` even though that shard was never queried.
#[must_use]
pub fn expected_shards(
    api_state: &HarvestApiState,
    pools: &BTreeMap<i32, DbPool>,
) -> BTreeSet<i32> {
    let mut shards: BTreeSet<i32> = pools.keys().copied().collect();
    if let Ok(runtime) = api_state.runtime() {
        let router = runtime.router();
        shards.extend(router.readable_shards().iter().map(|s| s.as_i32()));
        shards.insert(router.default_shard().as_i32());
    }
    if shards.is_empty() {
        shards.insert(0);
    }
    shards
}

/// Seconds elapsed from `started_at` to `observed_at`, clamped to zero.
#[must_use]
pub fn age_secs(observed_at: DateTime<Utc>, started_at: DateTime<Utc>) -> i64 {
    observed_at
        .signed_duration_since(started_at)
        .num_seconds()
        .max(0)
}

/// Cross-shard completeness of a fanned-out read.
///
/// Shared by every read model in this module family (`workflow_reachability`,
/// `schedule_runs`, `workflow_count`) so a policy change (e.g. a future
/// "degraded" tier) is made in exactly one place instead of drifting across
/// independent copies.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FanoutStatus {
    /// Every expected shard was inspected.
    Complete,
    /// At least one shard was inspected and at least one was unavailable.
    Partial,
    /// No shard could be inspected.
    Unavailable,
}

impl FanoutStatus {
    /// Derive the status from how many shards were inspected vs. unavailable.
    #[must_use]
    pub const fn from_counts(inspected: usize, unavailable: usize) -> Self {
        if inspected == 0 {
            Self::Unavailable
        } else if unavailable > 0 {
            Self::Partial
        } else {
            Self::Complete
        }
    }
}

// The "top-N + other" bounded-cardinality rollup used by `workflow_count` and
// `dlq::merge_dlq_aggregates` lives in `autumn_harvest::dlq::rollup_top_n`
// (core crate) rather than here: this plugin crate depends on the core
// `autumn-harvest` crate, not the reverse, and `dlq.rs`'s own merge function
// needs the same helper.

/// A shard that could not be queried for a fan-out read.
#[derive(Debug, Clone, Serialize)]
pub struct UnavailableShard {
    /// Shard identifier.
    pub shard_id: i32,
    /// Reason the shard could not be queried.
    pub reason: String,
}

/// Count inspected shards and collect unavailable ones (sorted by
/// `shard_id`), from a fan-out's per-shard observations.
///
/// Shared by every read model whose error handling depends only on
/// `ShardObservation<R>.shard_id`/`.error` (not `R` itself) — currently
/// `usage` and `workflow_count`. `workflow_reachability`'s shard-inspection
/// summary has a different shape (a combined status enum folding two
/// counts) and is intentionally not forced into this helper.
#[must_use]
pub fn summarize_shard_errors<R>(
    observations: &[ShardObservation<R>],
) -> (usize, Vec<UnavailableShard>) {
    let inspected = observations.iter().filter(|o| o.error.is_none()).count();
    let mut unavailable: Vec<UnavailableShard> = observations
        .iter()
        .filter_map(|o| {
            o.error.as_ref().map(|reason| UnavailableShard {
                shard_id: o.shard_id,
                reason: reason.clone(),
            })
        })
        .collect();
    unavailable.sort_by_key(|s| s.shard_id);
    (inspected, unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs<R>(shard_id: i32, rows: Vec<R>, error: Option<&str>) -> ShardObservation<R> {
        ShardObservation {
            shard_id,
            rows,
            error: error.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn summarize_shard_errors_counts_inspected_and_collects_unavailable() {
        let observations: Vec<ShardObservation<i64>> = vec![
            obs(0, vec![1], None),
            obs(1, vec![], Some("connection refused")),
            obs(2, vec![2], None),
        ];
        let (inspected, unavailable) = summarize_shard_errors(&observations);
        assert_eq!(inspected, 2);
        assert_eq!(unavailable.len(), 1);
        assert_eq!(unavailable[0].shard_id, 1);
        assert_eq!(unavailable[0].reason, "connection refused");
    }

    #[test]
    fn summarize_shard_errors_sorts_unavailable_by_shard_id() {
        let observations: Vec<ShardObservation<i64>> = vec![
            obs(3, vec![], Some("b")),
            obs(1, vec![], Some("a")),
            obs(2, vec![], Some("c")),
        ];
        let (_, unavailable) = summarize_shard_errors(&observations);
        let ids: Vec<i32> = unavailable.iter().map(|s| s.shard_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn summarize_shard_errors_empty_when_all_shards_ok() {
        let observations: Vec<ShardObservation<i64>> =
            vec![obs(0, vec![1], None), obs(1, vec![2], None)];
        let (inspected, unavailable) = summarize_shard_errors(&observations);
        assert_eq!(inspected, 2);
        assert!(unavailable.is_empty());
    }
}
