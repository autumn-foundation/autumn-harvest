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
