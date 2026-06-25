//! Shared helpers for cross-shard fan-out read operations.
//!
//! Several management-API read models (`version_usage`, `workflow_reachability`,
//! `shard_health`) fan out the same query across every configured shard, collect
//! results, and fold unreachable shards into a `partial`/`unavailable` status.
//! This module centralises the common scaffolding so each read model only has to
//! implement its own per-shard query and accumulator logic.

use std::collections::BTreeMap;

use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};

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

/// Seconds elapsed from `started_at` to `observed_at`, clamped to zero.
#[must_use]
pub fn age_secs(observed_at: DateTime<Utc>, started_at: DateTime<Utc>) -> i64 {
    observed_at
        .signed_duration_since(started_at)
        .num_seconds()
        .max(0)
}
