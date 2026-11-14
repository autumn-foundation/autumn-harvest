//! Shared helpers for cross-shard fan-out read operations.
//!
//! Several management-API read models fan out one query across every
//! configured shard. Examples are `version_usage`, `workflow_reachability`,
//! `schedule_runs` and `workflow_count`. They fold unreachable shards into a
//! `partial`/`unavailable` status. This module holds that common scaffolding.
//! Each read model implements only its own per-shard query and accumulator.

use std::collections::{BTreeMap, BTreeSet};

use autumn_harvest::HarvestResult;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use autumn_harvest::workers::WorkerRow;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::api::{HarvestApiRuntime, HarvestApiState, PoolConn, acquire_conn};

/// Outcome of a single-shard query: either a successful row set or an error.
#[derive(Debug)]
pub struct ShardObservation<R> {
    /// The shard that was queried.
    pub shard_id: i32,
    /// Rows returned by the query (empty when `error.is_some()`).
    pub rows: Vec<R>,
    /// Error description when the shard could not be queried.
    pub error: Option<String>,
}

/// Resolve `pool` to a live connection for `shard_id`, or the
/// [`ShardObservation`] error a fan-out read should record for this shard.
///
/// A shard the router has no pool for is `"has no configured storage pool"`.
/// A pool that fails to hand out a connection is `"could not be acquired"`.
/// A new fan-out read model implements only its own query against the
/// returned connection, not this guard.
///
/// # Errors
///
/// Returns the [`ShardObservation`] to record for this shard. That happens
/// when no pool is configured for it, or when no connection can be acquired.
pub async fn acquire_shard_conn<R>(
    shard_id: i32,
    pool: Option<DbPool>,
) -> Result<PoolConn, ShardObservation<R>> {
    let Some(pool) = pool else {
        return Err(ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(format!("shard {shard_id} has no configured storage pool")),
        });
    };
    acquire_conn(&pool).await.map_err(|_| ShardObservation {
        shard_id,
        rows: Vec::new(),
        error: Some(format!(
            "database connection for shard {shard_id} could not be acquired"
        )),
    })
}

/// Fold a per-shard query's [`HarvestResult`] into a [`ShardObservation`].
///
/// A successful query becomes `error: None` with its rows attached. A failed
/// one becomes an empty-rows observation whose `error` carries the failure's
/// `Display` text. Call it right after [`acquire_shard_conn`].
#[must_use]
pub fn observe_shard_result<R>(
    shard_id: i32,
    result: HarvestResult<Vec<R>>,
) -> ShardObservation<R> {
    match result {
        Ok(rows) => ShardObservation {
            shard_id,
            rows,
            error: None,
        },
        Err(error) => ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(error.to_string()),
        },
    }
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

/// Build the full set of shard ids a fan-out read should attempt to inspect.
///
/// The set is every shard with a live connection pool, plus every shard the
/// router already knows about (`readable_shards`/`default_shard`).
///
/// A shard-add rollout widens the router's `readable_shards` before every
/// process gets the new shard's pool wired up. See the workspace
/// `docs/architecture.md` "add a shard" procedure. During that window the
/// router knows about a shard this process has no pool for yet.
///
/// That shard must still appear in the returned set. A caller then reports
/// it `unavailable` instead of silently omitting it from the fan-out. An
/// omitted shard would let a completeness `status` read `complete` even
/// though the caller never queried that shard.
#[must_use]
pub fn expected_shards(
    api_state: &HarvestApiState,
    pools: &BTreeMap<i32, DbPool>,
) -> BTreeSet<i32> {
    expected_shards_for(api_state.runtime().ok().as_ref(), pools)
}

/// Like [`expected_shards`], but against a runtime the caller already holds.
///
/// A handler validates a request against `runtime` first -- an activity's
/// declared rate limit, or a workflow's declared throttle. The handler
/// must reuse that same `runtime` snapshot here. Do not pass `api_state`
/// and let this function re-read it.
///
/// `HarvestApiState::runtime()` and `storage_pool()` guard independent
/// locks. Plugin shutdown clears them one after the other
/// (`HarvestApiState::clear()`).
///
/// A second, independent re-read here can race that clear. It can then
/// fall back to pool-only shards, silently, for this one request. That
/// resurrects the exact omission [`expected_shards`] exists to prevent
/// (issue #1229 finding 1), only in that narrow window.
#[must_use]
pub fn expected_shards_for(
    runtime: Option<&HarvestApiRuntime>,
    pools: &BTreeMap<i32, DbPool>,
) -> BTreeSet<i32> {
    // Delegates to the canonical core rule (issue #1146). The engine resolves
    // a business key over exactly this set
    // (`external_target_location::resolve_location_by_workflow_id`). A read
    // that inspected a different set could answer differently about the same
    // key. One definition removes that drift by construction.
    let pool_shards: Vec<ShardId> = pools.keys().copied().map(ShardId::new).collect();
    let router_parts = runtime.map(|runtime| {
        let router = runtime.router();
        (router.readable_shards(), router.default_shard())
    });
    autumn_harvest::external_target_location::fanout_shards_from_parts(&pool_shards, router_parts)
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
/// *auto* sentinel. `Worker::new` resolves it to every shard in the process's
/// pool, or `[ShardId::new(0)]` when there is none. So a `Worker::new`-built
/// worker never registers an empty array.
///
/// Pre-#961 rows can still carry one. A worker started via the raw
/// `Worker::run` legacy single-shard path with no explicit assignment also
/// registers with a literal `[]`. Its poll loop (`claim_pool = match
/// shard_targets.as_slice() { [] => pool }` in `autumn-harvest/src/worker.rs`)
/// falls back to the caller's default `pool` argument. It claims work against
/// whatever database that pool points at. `run_with_listener` calls
/// `register_in_fleet` on that SAME pool. So the registry row of an
/// empty-array worker is in that exact database.
///
/// Every caller loads `worker` from a connection scoped to one shard's own
/// database (`observe_shard` in `queue_coverage.rs` and `shard_health.rs`).
/// So an empty-array worker found there always covers `shard_id`. That holds
/// whatever number the single/default shard carries. `ShardRouter::new`
/// accepts an arbitrary `default_shard`, not just `0`. Hardcoding
/// `shard_id == 0` would report a healthy legacy worker as uncovered when the
/// single shard is not numbered `0`. Without this normalization, a healthy
/// legacy single-shard worker covers no shard. Every queue it polls is then
/// falsely flagged.
///
/// Shared by `queue_coverage.rs::worker_covers_queue` (issue #774) and
/// `shard_health.rs::worker_assigned_to_shard`/`worker_can_cover` (issue
/// #522), so the two predicates cannot drift again. The empty-array gap here
/// was issue #1150.
#[must_use]
pub fn worker_covers_shard(worker: &WorkerRow, shard_id: i32) -> bool {
    // Delegates to the canonical core predicate (issue #1150 / #961). It lives
    // in `autumn_harvest::workers` because core consumers such as
    // `apply_worker_filters` cannot reach into this crate. Every independent
    // re-implementation of this rule has so far drifted.
    autumn_harvest::workers::shard_assignments_cover(&worker.worker.shard_assignments, shard_id)
}

/// Cross-shard completeness of a fanned-out read.
///
/// Every read model in this module family shares it. So a policy change,
/// such as a future "degraded" tier, is made in exactly one place.
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

// The "top-N + other" rollup lives in `autumn_harvest::dlq::rollup_top_n`,
// not here. The core `dlq.rs` merge needs it too, and core cannot depend on
// this plugin crate.

/// A shard that could not be queried for a fan-out read.
#[derive(Debug, Clone, Serialize)]
pub struct UnavailableShard {
    /// The shard that could not be queried.
    pub shard_id: i32,
    /// Why the shard could not be queried.
    pub reason: String,
}

/// Count inspected shards and collect unavailable ones (sorted by
/// `shard_id`), from a fan-out's per-shard observations.
///
/// It serves read models whose error handling reads only
/// `ShardObservation<R>.shard_id`/`.error`, not `R`. `workflow_reachability`
/// folds two counts into one status, so it does not use this helper.
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
/// It is the merge shape for the *list-style* cross-shard read endpoints,
/// such as `GET /workflows` and `GET /dead-letters`. They turn an unreachable
/// shard into a `partial` result, not a whole-request `500`.
///
/// Each caller flattens its [`ShardObservation`]s through
/// [`collect_fanout_rows`]. It then sorts, truncates and pages `rows` itself.
/// It serializes a bare array when every shard was inspected. Otherwise it
/// serializes an envelope that names `unavailable_shards`.
///
/// This type does *not* sort or cap `rows`. The ordering and pagination
/// contract differs per endpoint.
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
    /// Keyed on `status == Complete`, NOT on `unavailable_shards.is_empty()`.
    /// The two diverge when the status is `Unavailable` and
    /// `unavailable_shards` is empty, because no expected shard resolved.
    /// That is not a happy path and must not report complete.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self.status, FanoutStatus::Complete)
    }
}

/// Flatten per-shard observations into the union of reachable rows plus a
/// completeness verdict (pure, no DB).
///
/// A shard whose observation carries an `error` contributes no rows. It goes
/// into `unavailable_shards`. Reachable rows are appended in shard order.
/// [`FanoutStatus::from_counts`] sets `status`, so one unreachable shard never
/// fails the whole call.
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
    use std::sync::Arc;

    use autumn_harvest::RetentionConfig;
    use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
    use autumn_harvest::shard::ShardRouter;
    use autumn_harvest::worker::HandlerRegistry;

    use super::*;
    use crate::api::HarvestRetentionRuntime;

    /// A `DbPool` that never connects. Standing in for a real pool is enough
    /// here -- these tests only check which shard ids `expected_shards_for`
    /// selects, never the pool's connections.
    fn unreachable_test_pool() -> DbPool {
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new("postgres://user:pass@127.0.0.1:1/db");
        deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("failed to build unreachable test pool")
    }

    fn runtime_with_router(router: ShardRouter) -> HarvestApiRuntime {
        HarvestApiRuntime::new(
            Arc::new(HandlerRegistry::new(Vec::new(), Vec::new())),
            Arc::new(DagCatalog::default()),
            Arc::new(Vec::new()),
            None,
            Vec::new(),
            SchedulerMonitor::offline(),
            HarvestRetentionRuntime::disabled(RetentionConfig::default()),
            router,
        )
    }

    #[test]
    fn expected_shards_for_adds_router_known_shard_with_no_pool() {
        // The router already advertises shard 0, but this process has no
        // pool for it (issue #1229 finding 1). Uses `ShardRouter::single()`,
        // not a distinct multi-shard router. `HarvestApiRuntime::new`
        // installs its router into the process-global `GLOBAL_SHARD_ROUTER`.
        // That is a real side effect. A `cargo test` run shares this global
        // across every concurrently running test in this crate. `single()`
        // is the same router every other such test in this crate already
        // installs, so this test adds no new cross-test hazard.
        let runtime = runtime_with_router(ShardRouter::single());
        let pools: BTreeMap<i32, DbPool> = BTreeMap::new();

        let expected = expected_shards_for(Some(&runtime), &pools);

        assert_eq!(expected, BTreeSet::from([0]));
    }

    #[test]
    fn expected_shards_for_falls_back_to_pool_shards_without_a_runtime() {
        // A caller with no runtime snapshot at all (mirrors the old
        // `api_state.runtime()` lookup failing) still sees its own pools.
        // Builds no `HarvestApiRuntime`, so this test installs no global
        // router.
        let mut pools: BTreeMap<i32, DbPool> = BTreeMap::new();
        pools.insert(0, unreachable_test_pool());

        let expected = expected_shards_for(None, &pools);

        assert_eq!(expected, BTreeSet::from([0]));
    }

    #[tokio::test]
    async fn acquire_shard_conn_no_pool_reports_shard_id_and_message() {
        let result: Result<PoolConn, ShardObservation<i64>> = acquire_shard_conn(7, None).await;
        let Err(observation) = result else {
            panic!("no pool must be an error observation");
        };
        assert_eq!(observation.shard_id, 7);
        assert_eq!(observation.rows, [] as [i64; 0]);
        assert_eq!(
            observation.error.as_deref(),
            Some("shard 7 has no configured storage pool")
        );
    }

    #[tokio::test]
    async fn acquire_shard_conn_unreachable_pool_reports_shard_id_and_message() {
        // Nothing listens on this port, so `pool.get()` fails fast (connection
        // refused) without needing a live database.
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new("postgres://user:pass@127.0.0.1:1/db");
        let pool: DbPool = deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("failed to build unreachable test pool");

        let result: Result<PoolConn, ShardObservation<i64>> =
            acquire_shard_conn(3, Some(pool)).await;
        let Err(observation) = result else {
            panic!("unreachable pool must be an error observation");
        };
        assert_eq!(observation.shard_id, 3);
        assert_eq!(observation.rows, [] as [i64; 0]);
        assert_eq!(
            observation.error.as_deref(),
            Some("database connection for shard 3 could not be acquired")
        );
    }

    #[test]
    fn observe_shard_result_ok_attaches_rows_with_no_error() {
        let observation = observe_shard_result(5, Ok(vec!["a".to_string(), "b".to_string()]));
        assert_eq!(observation.shard_id, 5);
        assert_eq!(observation.rows, ["a".to_string(), "b".to_string()]);
        assert_eq!(observation.error, None);
    }

    #[test]
    fn observe_shard_result_ok_empty_rows_still_reports_no_error() {
        let observation: ShardObservation<i64> = observe_shard_result(2, Ok(Vec::new()));
        assert_eq!(observation.shard_id, 2);
        assert_eq!(observation.rows, [] as [i64; 0]);
        assert_eq!(observation.error, None);
    }

    #[test]
    fn observe_shard_result_err_reports_empty_rows_and_display_text() {
        let observation: ShardObservation<i64> = observe_shard_result(
            9,
            Err(autumn_harvest::error::database_error("connection reset")),
        );
        assert_eq!(observation.shard_id, 9);
        assert_eq!(observation.rows, [] as [i64; 0]);
        assert_eq!(
            observation.error.as_deref(),
            Some("database error: connection reset")
        );
    }

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
        assert_eq!(merged.rows, [] as [i64; 0]);
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
        assert_eq!(merged.rows, [] as [i64; 0]);
        assert!(merged.unavailable_shards.is_empty());
    }
}
