//! Shared helpers for cross-shard fan-out read operations.
//!
//! Several management-API read models (`version_usage`, `workflow_reachability`,
//! `schedule_runs`, `workflow_count`) fan out the same query across every
//! configured shard, collect results, and fold unreachable shards into a
//! `partial`/`unavailable` status. This module centralises the common
//! scaffolding so each read model only has to implement its own per-shard query
//! and accumulator logic.

use std::collections::{BTreeMap, BTreeSet};

use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use autumn_harvest::workers::WorkerRow;
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
    // Delegates to the canonical core rule (issue #1146). The engine's own
    // by-business-key resolution
    // (`external_target_placement::resolve_placement_by_workflow_id`) fans out
    // over exactly this set, and a management-API read that inspected a
    // different set of shards than the engine would answer differently about
    // the same key. Keeping one definition removes that drift by construction,
    // the way `select_resolved_run` already does for the ranking.
    let pool_shards: Vec<ShardId> = pools.keys().copied().map(ShardId::new).collect();
    let router = api_state
        .runtime()
        .ok()
        .map(|runtime| runtime.router().clone());
    autumn_harvest::external_target_placement::fanout_shards(&pool_shards, router.as_ref())
        .into_iter()
        .map(ShardId::as_i32)
        .collect()
}

/// Seconds elapsed from `started_at` to `observed_at`, clamped to zero.
#[must_use]
pub fn age_secs(observed_at: DateTime<Utc>, started_at: DateTime<Utc>) -> i64 {
    observed_at
        .signed_duration_since(started_at)
        .num_seconds()
        .max(0)
}

/// Whether `worker`'s registered `shard_assignments` cover `shard_id`.
///
/// An **empty** `shard_assignments` array is a special case, not "assigned to
/// nothing". Since issue #961 an empty `WorkerConfig::shard_assignments` is the
/// *auto* sentinel, resolved by `Worker::new` to every shard in the process's
/// pool (or `[ShardId::new(0)]` when there is none) -- so a `Worker::new`-built
/// worker never registers an empty array. Pre-#961 rows can still carry one,
/// and a worker started via the raw `Worker::run` legacy single-shard path
/// with no explicit assignment still registers with a literal `[]` -- even
/// though its poll loop (`claim_pool = match shard_targets.as_slice() { [] =>
/// pool }` in `autumn-harvest/src/worker.rs`) falls back to the caller's
/// default `pool` argument and actually claims work against whatever database
/// that pool points at ("legacy behavior, byte-for-byte unchanged"). That is
/// also the SAME pool `run_with_listener` uses to call `register_in_fleet`, so
/// an empty-array worker's registry row is written into that exact database.
///
/// Every caller of this function loads `worker` from a connection already
/// scoped to one shard's own database (`observe_shard` in both
/// `queue_coverage.rs` and `shard_health.rs`), so an empty-array worker found
/// there is unconditionally covering `shard_id`, whatever numeric value the
/// deployment's single/default shard happens to carry (`ShardRouter::new`
/// accepts an arbitrary `default_shard`, not just `0` -- see
/// `ShardRouter::single()` vs. the general constructor). Hardcoding
/// `shard_id == 0` here would falsely report a healthy legacy worker as
/// uncovered on any deployment whose single shard isn't literally numbered
/// `0`. Without this normalization at all, a perfectly healthy legacy
/// single-shard worker would be reported as covering no shard whatsoever,
/// falsely flagging every queue it actually polls.
///
/// Shared by `queue_coverage.rs::worker_covers_queue` (issue #774) and
/// `shard_health.rs::worker_assigned_to_shard`/`worker_can_cover` (issue
/// #522; the empty-array gap here was issue #1150, discovered as a
/// duplicate of the identical bug pattern while implementing #774) so the
/// two shard-membership predicates cannot drift again.
#[must_use]
pub fn worker_covers_shard(worker: &WorkerRow, shard_id: i32) -> bool {
    // Delegates to the canonical core predicate (issue #1150 / #961). It lives
    // in `autumn_harvest::workers` because core has consumers of its own --
    // `apply_worker_filters` -- that cannot reach into this crate, and every
    // independent re-implementation of this rule has so far drifted.
    autumn_harvest::workers::shard_assignments_cover(&worker.worker.shard_assignments, shard_id)
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

/// The union of a fan-out's reachable-shard rows plus a completeness verdict
/// (issue #756).
///
/// This is the shared merge shape for the *list-style* cross-shard read
/// endpoints (`GET /workflows`, `GET /workers`, `GET /admin/schedules`,
/// `GET /dead-letters`, …) that turn an unreachable shard into a `partial`
/// degradation rather than a whole-request `500`. Each caller flattens its
/// per-shard [`ShardObservation`]s through [`collect_fanout_rows`], then
/// applies its own sort/truncate/cursor over `rows` and decides whether to
/// serialize a bare array (happy path — every shard inspected) or a degraded
/// envelope naming `unavailable_shards`.
///
/// Unlike `workflow_count`'s grouped merge, this helper does *not* sort or cap
/// `rows` — the ordering and pagination contract differs per endpoint and is
/// applied by the caller after the union.
#[derive(Debug)]
pub struct FanoutRows<R> {
    /// The union of rows from every reachable shard, in per-shard order (the
    /// caller re-sorts for its own total ordering).
    pub rows: Vec<R>,
    /// Cross-shard completeness of this read.
    pub status: FanoutStatus,
    /// Shards that could not be queried, named with a reason and sorted by
    /// `shard_id`. Empty when `status == Complete`; also empty in the
    /// degenerate `Unavailable`-with-no-observations case (nothing to name).
    pub unavailable_shards: Vec<UnavailableShard>,
}

impl<R> FanoutRows<R> {
    /// `true` when every expected shard was inspected — the happy path on
    /// which a bare-array endpoint keeps its legacy response shape (issue
    /// #756, AC3).
    ///
    /// Keyed on `status == Complete`, NOT on `unavailable_shards.is_empty()`:
    /// the two diverge in the `Unavailable`-with-empty-`unavailable_shards`
    /// case (no expected shard could even be resolved), which is decidedly not
    /// a happy path and must not be reported complete.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self.status, FanoutStatus::Complete)
    }
}

/// Flatten per-shard observations into the union of reachable rows plus a
/// completeness verdict (pure, no DB).
///
/// A shard whose observation carries an `error` contributes no rows and is
/// folded into `unavailable_shards`; a reachable shard's rows are appended in
/// shard-iteration order. `status` is `complete` only when every shard was
/// inspected, `partial` when at least one was inspected and at least one was
/// unavailable, and `unavailable` when none could be inspected — the call
/// therefore never fails wholesale on a single unreachable shard.
#[must_use]
pub fn collect_fanout_rows<R>(observations: Vec<ShardObservation<R>>) -> FanoutRows<R> {
    let (inspected, unavailable_shards) = summarize_shard_errors(&observations);
    let status = FanoutStatus::from_counts(inspected, unavailable_shards.len());
    let rows: Vec<R> = observations.into_iter().flat_map(|o| o.rows).collect();
    FanoutRows {
        rows,
        status,
        unavailable_shards,
    }
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

    // ── collect_fanout_rows (issue #756) ─────────────────────────────────

    #[test]
    fn collect_fanout_rows_all_reachable_is_complete_and_unions_rows() {
        let merged = collect_fanout_rows(vec![
            obs(0, vec![1, 2], None),
            obs(1, vec![3], None),
            obs(2, vec![], None),
        ]);
        assert_eq!(merged.status, FanoutStatus::Complete);
        assert!(merged.is_complete());
        assert!(merged.unavailable_shards.is_empty());
        assert_eq!(merged.rows, vec![1, 2, 3]);
    }

    #[test]
    fn collect_fanout_rows_one_error_is_partial_and_names_the_shard() {
        let merged = collect_fanout_rows(vec![
            obs(0, vec![10], None),
            obs(1, vec![], Some("connection refused")),
            obs(2, vec![20], None),
        ]);
        assert_eq!(merged.status, FanoutStatus::Partial);
        assert!(!merged.is_complete());
        // Reachable shards' rows still flow through — the call never fails
        // wholesale.
        assert_eq!(merged.rows, vec![10, 20]);
        assert_eq!(merged.unavailable_shards.len(), 1);
        assert_eq!(merged.unavailable_shards[0].shard_id, 1);
        assert_eq!(merged.unavailable_shards[0].reason, "connection refused");
    }

    #[test]
    fn collect_fanout_rows_all_error_is_unavailable() {
        let merged: FanoutRows<i64> = collect_fanout_rows(vec![
            obs(0, vec![], Some("pool missing")),
            obs(1, vec![], Some("connection refused")),
        ]);
        assert_eq!(merged.status, FanoutStatus::Unavailable);
        assert!(!merged.is_complete());
        assert!(merged.rows.is_empty());
        // Unavailable shards are sorted by shard_id.
        let ids: Vec<i32> = merged
            .unavailable_shards
            .iter()
            .map(|s| s.shard_id)
            .collect();
        assert_eq!(ids, vec![0, 1]);
    }

    #[test]
    fn collect_fanout_rows_empty_observations_is_unavailable() {
        let merged: FanoutRows<i64> = collect_fanout_rows(Vec::new());
        assert_eq!(merged.status, FanoutStatus::Unavailable);
        assert!(merged.rows.is_empty());
        assert!(merged.unavailable_shards.is_empty());
    }
}
