//! Shard-placement-aware resolution for `workflow_id`-addressed external
//! targets (issue #1146).
//!
//! Issue #751 lets one workflow address another by its stable business key
//! `(workflow_name, workflow_id)` instead of an [`ExecutionId`]. To deliver,
//! the engine must know which shard owns the target. The original answer was
//! [`crate::shard::external_target_owning_shard`], which re-derives
//! [`ShardRouter::pick_for_new_workflow`] — the rendezvous hash a *fresh start*
//! of that key would use.
//!
//! That is a **prediction of where new work would be placed**. It is not an
//! observation of where existing work *is*. The two diverge in two ways:
//!
//! 1. **Explicit shard placement (issue #697).** A workflow started under
//!    [`crate::shard::ShardPlacement::Shard`] or
//!    [`crate::shard::ShardPlacement::ResidencyKey`] is deliberately pinned to
//!    a shard the pure hash may never compute.
//! 2. **Writable-set drift.** [`ShardRouter::pick_for_new_workflow`] re-hashes
//!    over the *current* `writable_shards` when the readable-set hash lands
//!    outside it. Draining a shard then moves where a key resolves *after* a
//!    workflow was already placed there.
//!
//! In both cases the hash names a shard the target does not live on, and the
//! delivery attempt finds nothing. Once the unknown-target grace window
//! elapses, the caller's history durably records `target_unknown`. The target
//! was running the whole time.
//!
//! # The rule this module implements
//!
//! Resolve by **observation**. Fan out across every shard the deployment
//! expects to exist, and ask each for its best run under this business key.
//! Merge the answers with the canonical [`select_resolved_run`] ranking
//! (active-run-first, else most-recent terminal). The by-id resolution of the
//! management API already uses this rule (`api::resolve_workflow_by_business_id`,
//! issue #805). The engine now has it too, so the two cannot disagree about
//! *which shards count* or *which run wins*. The traversal is still separate.
//! The management API keeps its own loop. That loop fails closed with a 503 on
//! the first un-poolable shard and has no alias dedupe. So this is a shared
//! rule, not a shared implementation. Converging the two is worth doing and is
//! not done here.
//!
//! Issue #1308 reuses the same fan-out for a second question, via
//! [`check_cross_shard_occupancy`]: not "where does this key live", but "is
//! this key occupied anywhere". Two cross-shard uniqueness guards
//! (`worker::reject_cross_shard_continue_as_new`, `execution`'s re-run
//! `workflow_id`-override guard) used a hash-based prediction for that
//! question. They refused whenever it diverged. Observing the real answer
//! lets them refuse only when the key is genuinely occupied.
//!
//! Two properties are load-bearing and are what a naive fan-out gets wrong:
//!
//! * **No first-hit short circuit.** Uniqueness is shard-local, and a
//!   `(workflow_name, workflow_id)` chain can move shards. A stale terminal run
//!   on one shard and the live run on another is then ordinary. Stopping at the
//!   first shard that returns a row would signal the dead one and report
//!   `not_running` while the target is alive. So every expected shard is asked
//!   before a terminal answer is accepted.
//! * **"Could not inspect" is not "not there."** A shard with no pool or no
//!   connection leaves the answer [`TargetLocation::Indeterminate`], never
//!   [`TargetLocation::NotFound`]. The outbox converts `NotFound` into a
//!   permanent `target_unknown` once the grace window expires. Treating an
//!   outage as absence would turn a transient, retryable condition into a wrong
//!   answer. That answer is written irreversibly into the caller's append-only
//!   history.
//!
//! # Cost, and the connection budget
//!
//! One or two queries per expected shard, per by-id delivery attempt.
//! [`crate::execution::resolve_execution_id_by_workflow_id`] probes
//! active-first. Only when a shard holds no active run does it probe for the
//! most-recent terminal run. By-id signal/cancel delivery is asynchronous and
//! runs in the outbox scanners, off the hot dispatch path. That makes an
//! O(shards) resolution acceptable here, and not, say, on task claim.
//!
//! **Connections are the scarce resource, not queries.** The outbox calls this
//! from inside a transaction on a connection it already holds from its own
//! shard's pool. A pool may have no deadpool `Timeouts`. Then a bare
//! `pool.get().await` is an *unbounded* wait. A sweep that reached back into
//! its own pool for a second connection parks forever on a one-connection
//! pool. It then wedges every later resident of that scanner tick. That is the
//! hazard `codec_rotation.rs` and `audit_export.rs` were both fixed for. Three rules
//! keep that from happening here:
//!
//! * The caller's own shard is probed on the **connection the caller already
//!   holds** (the `held` argument of [`resolve_location_by_workflow_id_with`]),
//!   so the fan-out never re-enters that pool.
//! * `resolve_delivery_route` short-circuits when only one shard is expected.
//!   A single-shard deployment then issues no fan-out at all and takes the
//!   same route it took pre-#1146.
//! * [`FANOUT_PEER_BOUND`] bounds every remaining peer probe, acquisition
//!   *and* query. A shard that does not answer in time becomes an
//!   [`UninspectedShard`], i.e. `Indeterminate` and a retry, not an indefinite
//!   park. This covers only the probes this module makes. The cross-shard
//!   *delivery* query that follows a resolution is deliberately unbounded.
//!   Timing out mid-write is a different risk than timing out a read-only probe.
//!
//! [`UninspectableShards`] then memoizes the shards that already failed, for
//! the length of one sweep. A backlog of N pending rows pays that bound once,
//! not N times.

use diesel_async::AsyncPgConnection;

use crate::execution::{ResolvedRun, select_resolved_run};
use crate::shard::{ShardRouter, ShardedDbPool};
use crate::types::ShardId;
use crate::worker::DbPool;

/// Why a shard could not be inspected, as a bounded label a metric can carry
/// (issue #1307).
///
/// `#[non_exhaustive]`: a probe fails in exactly these three ways today. A new
/// failure shape must slot in as a new variant. A caller must never have to
/// guess from the prose of `reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum UninspectedReasonKind {
    /// This process has no configured pool for the shard.
    NoPool,
    /// A connection could not be acquired, or the probe did not answer, in
    /// time.
    AcquireTimeout,
    /// The connection was acquired but the resolution query itself failed.
    QueryError,
}

impl UninspectedReasonKind {
    /// The bounded metric-label value for this kind (issue #1307).
    ///
    /// Per ADR-0001 §7, only low-cardinality values may label a metric. This
    /// is that value. It stays separate from [`std::fmt::Display`], so a future
    /// operator-facing rendering of `self` cannot silently change a metric label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::NoPool => "no_pool",
            Self::AcquireTimeout => "acquire_timeout",
            Self::QueryError => "query_error",
        }
    }
}

/// A shard a by-business-key fan-out expected to inspect but could not.
///
/// Its presence in a [`TargetLocation::Indeterminate`] is the reason the
/// resolution is being reported as inconclusive rather than as an answer.
///
/// `#[non_exhaustive]` deliberately: `reason` is prose for an operator log
/// line. [`Self::kind`] is its machine-readable twin (issue #1307). A caller
/// that must act differently on a no-pool vs. a timed-out vs. a failed-query
/// shard uses `kind`. The log line still reads `reason`. Adding a third
/// field must not be a breaking change.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UninspectedShard {
    /// The shard that could not be queried.
    pub shard: ShardId,
    /// Why it could not be queried (no configured pool, connection failure,
    /// query failure), for the operator-facing log line.
    pub reason: String,
    /// The bounded discriminant behind `reason`, for metrics (issue #1307).
    pub kind: UninspectedReasonKind,
}

/// Where a `(workflow_name, workflow_id)`-addressed target actually lives.
///
/// Named *location*, not *placement*. That distinction is the whole point of
/// this module. [`crate::shard::ShardPlacement`] is a policy for where **new**
/// work should go, decided before an execution exists. This is an observation
/// of where an **existing** run is. Issue #1146 is what happens when the first
/// answers the second.
///
/// `#[non_exhaustive]`: a design note, not a promise of future variants.
/// `Found` already carries "several live runs" as its `other_live` field
/// (issue #1146). A signal still needs one winner to
/// deliver to. Ranking therefore stays a field on `Found`, not a fork in
/// the enum.
///
/// The still-open gap is issue #1313's race: a run that starts on an
/// already-read shard, mid fan-out. No enum variant can name that. A
/// single fan-out cannot tell it apart from an ordinary complete answer.
/// Closing it needs cross-shard coordination, not a smarter classification
/// here.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TargetLocation {
    /// The key resolves to `run`, which lives on `shard`.
    ///
    /// `run` may be terminal. The caller applies its own semantics to that
    /// (issue #751). A terminal current run fails a signal `not_running` and
    /// satisfies a cancel as a no-op success.
    #[non_exhaustive]
    Found {
        /// The shard whose database holds `run`.
        shard: ShardId,
        /// The winning run under the [`select_resolved_run`] ranking.
        run: ResolvedRun,
        /// Shards that could not be inspected, if any. **Non-empty means this
        /// answer is a live run found over a partial view.** The caller must
        /// decide whether its operation may act on that (issue #1146).
        ///
        /// A **signal** may. Delivering to a live run in hand is real delivery.
        /// Re-delivery is not idempotent without an idempotency key. So
        /// "deliver but stay pending" would duplicate the signal on the retry.
        ///
        /// A **cancel** may act but must not *report*. `ExternalCancelDelivered`
        /// asserts "nothing is running under it" about the whole business key.
        /// `(workflow_name, workflow_id)` uniqueness is shard-local, so the
        /// unread shard may hold another live run. Cancelling is idempotent. The
        /// cancel path cancels what it found and leaves the request pending. A
        /// later **complete** fan-out then makes the assertion.
        uninspected: Vec<UninspectedShard>,
        /// Shards that hold a **further live run** of the same key, besides the
        /// one on `shard` (issue #1146).
        ///
        /// Empty in every healthy deployment. Non-empty is the state
        /// [`merge_locations`] warns about. `(workflow_name, workflow_id)`
        /// uniqueness is shard-local. A key pinned to one shard (#697) and an
        /// unpinned start of the same key on another shard give two live runs.
        /// No single shard's index can see both.
        ///
        /// `run` is still the most recently started of them. That is the
        /// documented "current run" rule, which a **signal** follows. A
        /// **cancel** must not report on a fan-out that saw more than one, for
        /// the same reason as a partial one. Cancelling the newest leaves the
        /// other live, and *that* run then becomes the current run for the key.
        /// So the cancel path cancels what it found, withholds the terminal, and
        /// retries. Each sweep removes one live copy, until none is left and the
        /// answer is unambiguous.
        other_live: Vec<ShardId>,
    },
    /// **Every** expected shard was inspected and none holds a run for this
    /// key. Only this outcome may become a permanent `target_unknown`.
    NotFound,
    /// At least one expected shard could not be inspected, and what *was*
    /// inspected does not settle the question. The caller must retry rather
    /// than conclude anything.
    Indeterminate {
        /// The shards that could not be inspected, ascending by shard id.
        uninspected: Vec<UninspectedShard>,
    },
    /// A keyed signal already landed on `shard`, found while fanning out for
    /// the current run (issue #1318).
    ///
    /// Only ever produced when the caller passed an `idempotency_key`. Each
    /// per-shard probe this fan-out already runs also checks for a prior
    /// delivery of that key. The check runs on the same connection, so it
    /// costs no extra connection. It short-circuits the fan-out at the first
    /// hit. Unlike [`Self::NotFound`], a positive answer does not need every
    /// shard to agree.
    ///
    /// `shard` names where the PRIOR delivery landed. It is deliberately not
    /// the shard of the current run, and that is the gap this closes. A
    /// continue-as-new or a terminate-and-restart can cross shards between
    /// two delivery attempts. That moves the current run, but a signal
    /// already queued under this key must still dedupe. The shard-local
    /// pre-check of [`crate::signal::resolve_and_signal_by_workflow_id`]
    /// cannot see across a shard boundary.
    #[non_exhaustive]
    AlreadyDelivered {
        /// The shard the earlier delivery's signal row lives on.
        shard: ShardId,
    },
}

impl TargetLocation {
    /// The shard holding the resolved run, if the resolution found one.
    #[must_use]
    pub const fn found_shard(&self) -> Option<ShardId> {
        match self {
            Self::Found { shard, .. } => Some(*shard),
            // `AlreadyDelivered` names where a PRIOR delivery landed, not a
            // shard to deliver to now (code review, issue #1318). Answering
            // `None` here keeps that distinction so a future caller of
            // `found_shard` cannot mistake it for a deliverable location.
            Self::AlreadyDelivered { .. } | Self::NotFound | Self::Indeterminate { .. } => None,
        }
    }

    /// May a caller assert something about the **whole business key** from this
    /// answer, as `ExternalCancelDelivered` does when it claims nothing runs
    /// under it?
    ///
    /// Two things can make an answer inauthoritative in that sense. One
    /// predicate covers both, because both have the same consequence: a live
    /// run of this key that the assertion would ignore.
    ///
    /// * a shard that could not be inspected, which **may** hold one; and
    /// * a shard that was inspected and **does** hold one, besides the winner.
    ///
    /// `true` for [`Self::NotFound`] (every shard answered, none holds the key)
    /// and for an unambiguous [`Self::Found`]; `false` for every
    /// [`Self::Indeterminate`].
    ///
    /// This says nothing about whether a caller may *act*. Cancelling the run
    /// in hand is idempotent progress either way. It governs only what may be
    /// recorded.
    ///
    /// # What `true` does not mean (issue #1146)
    ///
    /// It is authoritative over **the observations this fan-out made**, not over
    /// an instant in time. The shards are read sequentially, on separate
    /// connections to separate databases, with no shared snapshot. Postgres has
    /// no cross-shard transaction, and this engine deliberately adds no
    /// coordinator. So a run of the key can start on shard 0 *after* shard 0
    /// answered "nothing" and *before* shard 1 answers. The merge does not see
    /// it. A cancel can then report success while that run is live.
    ///
    /// Two things bound how much that matters. First, it needs two live runs of
    /// one business key. That needs a deployment that mixes pinned and unpinned
    /// starts of the same `workflow_id`. `docs/sharding.md` already asks
    /// operators to avoid that, because `(workflow_name, workflow_id)`
    /// uniqueness is shard-local. Second, it is strictly better than the
    /// pre-#1146 behaviour. That consulted exactly one hash-derived shard, so it
    /// missed a second live run always, not only under a race.
    ///
    /// The cancel path narrows it further, and that rule lives with the route
    /// rather than here. A cancel that ends a live run reports from a LATER
    /// fan-out, which observes the whole window this one ran in. See
    /// `timeout::DeliveryRoute::CrossShard::reverify_after_cancel` (issue
    /// #1313). This predicate is unchanged by that rule. A `true` from it still
    /// means only what the paragraph above says.
    ///
    /// Closing the window outright means cross-shard key uniqueness, which is an
    /// architectural addition rather than a fix — tracked as issue #1313.
    #[must_use]
    pub const fn is_authoritative_for_key(&self) -> bool {
        match self {
            Self::Found {
                uninspected,
                other_live,
                ..
            } => uninspected.is_empty() && other_live.is_empty(),
            Self::NotFound => true,
            // `AlreadyDelivered` answers a delivery-dedup question, not a
            // key-state one. The cancel path is the only caller of this method.
            // Cancel carries no idempotency key, so it never sees this variant.
            // `false` is the conservative default regardless.
            Self::Indeterminate { .. } | Self::AlreadyDelivered { .. } => false,
        }
    }
}

/// The shards a by-business-key resolution must inspect.
///
/// Every shard this process has a pool for, unioned with every shard the router
/// knows about (`readable_shards` plus `default_shard`), ascending and
/// deduplicated.
///
/// The union, not just the local pools, makes [`TargetLocation::Indeterminate`]
/// meaningful. During a shard-add rollout, the router widens `readable_shards`
/// before every process has a pool for the new shard. See the "add a shard"
/// procedure in `docs/sharding.md`. A fan-out that silently omitted such a
/// shard would report a confident `NotFound` for a key that lives there.
/// Naming it here instead turns that into an explicit uninspected shard.
///
/// Falls back to `[ShardId(0)]` when neither a pool nor a router is available,
/// matching the pre-sharding default every other lookup path uses.
#[must_use]
pub fn fanout_shards(pool_shards: &[ShardId], router: Option<&ShardRouter>) -> Vec<ShardId> {
    fanout_shards_from_parts(
        pool_shards,
        router.map(|r| (r.readable_shards(), r.default_shard())),
    )
}

/// [`fanout_shards`], taking the router's placement-relevant parts directly.
///
/// Same rule, same result. Some callers hold a `&ShardRouter` behind a guard or
/// a `Result` they cannot keep borrowed across the call. This form spares them
/// a `ShardRouter` clone. `ShardRouter` owns a `residency_map`, so a clone per
/// management-API request to answer a set question is pure waste.
#[must_use]
pub fn fanout_shards_from_parts(
    pool_shards: &[ShardId],
    router_parts: Option<(&[ShardId], ShardId)>,
) -> Vec<ShardId> {
    let mut shards: std::collections::BTreeSet<ShardId> = pool_shards.iter().copied().collect();
    if let Some((readable, default_shard)) = router_parts {
        shards.extend(readable.iter().copied());
        shards.insert(default_shard);
    }
    if shards.is_empty() {
        shards.insert(ShardId::new(0));
    }
    shards.into_iter().collect()
}

/// Merge a fan-out's per-shard candidates into a single placement (pure, no
/// DB).
///
/// Ranking is delegated to [`select_resolved_run`], the function that the by-id
/// resolution of the management API uses. So the engine and the HTTP surface
/// cannot drift about which run of a business key is "the current one".
///
/// The completeness rules on top of that ranking:
///
/// * A **live (non-terminal) winner is delivered to even when a shard was
///   missed.** Note what this does *not* rest on. `(workflow_name,
///   workflow_id)` uniqueness is shard-local. So two live runs of one key are
///   possible in exactly the topologies this module exists for. Example: pin
///   key K to shard 2 while an unpinned start of K hashes to shard 0. The
///   partial unique index of shard 0 cannot see shard 2. The rule is therefore
///   "deliver to the most recently started live run that was found". A shard
///   that could not be inspected may hold a newer one. That is deliberate and
///   conservative in the direction that matters. A live run in hand is a real
///   target. Withholding delivery because an *unrelated* shard is unreachable
///   would strand a signal whose recipient is right there.
///   [`resolve_location_by_workflow_id_with`] logs the incomplete fan-out, so
///   the ambiguity is visible, not silent.
/// * A **terminal winner with a shard missed is `Indeterminate`.** A terminal
///   run on an inspected shard does not rule out a live run on the shard that
///   was not inspected. The two lead to opposite outcomes for a signal
///   (`not_running` failure vs. delivery).
/// * **No candidates at all with a shard missed is `Indeterminate`**, never
///   `NotFound`.
/// * **A `keyed` request with a shard missed is always `Indeterminate`**,
///   live winner or not (issue #1318, code review on PR #1593). The
///   idempotency-key check this fan-out runs alongside run resolution only
///   covers the shards it actually reached. A shard it could not reach might
///   hold the prior delivery the key names. Delivering fresh to a live
///   winner found elsewhere would then double-deliver. `keyed` is `false` for
///   a cancel (no idempotency-key concept) and for an unkeyed signal. Neither
///   loses the live-winner-wins rule above.
#[must_use]
pub fn merge_locations(
    candidates: Vec<(ShardId, ResolvedRun)>,
    uninspected: Vec<UninspectedShard>,
    keyed: bool,
) -> TargetLocation {
    let runs: Vec<ResolvedRun> = candidates.iter().map(|(_, run)| run.clone()).collect();
    let Some(winner) = select_resolved_run(runs) else {
        return if uninspected.is_empty() {
            TargetLocation::NotFound
        } else {
            TargetLocation::Indeterminate { uninspected }
        };
    };

    if !uninspected.is_empty() && (keyed || crate::erase::is_terminal_state(&winner.state)) {
        return TargetLocation::Indeterminate { uninspected };
    }

    // Any OTHER live run of this key that a shard answered with. It is
    // reachable only when the winner itself is live (the ranking prefers live).
    // It needs the topologies this module exists for: an explicit pin plus an
    // unpinned start of the same key. Shard-local uniqueness cannot see it, so
    // the fan-out is the first thing in the engine that can.
    let other_live: Vec<ShardId> = candidates
        .iter()
        .filter(|(_, run)| {
            run.exec_id != winner.exec_id && !crate::erase::is_terminal_state(&run.state)
        })
        .map(|(shard, _)| *shard)
        .collect();
    if !other_live.is_empty() {
        tracing::warn!(
            winner_shard = ?candidates
                .iter()
                .find(|(_, run)| run.exec_id == winner.exec_id)
                .map(|(shard, _)| *shard),
            ?other_live,
            "by-id resolution found more than one LIVE run of this business key; \
             (workflow_name, workflow_id) uniqueness is shard-local, so this is \
             reachable by pinning a key to one shard while an unpinned start of it \
             hashes to another"
        );
    }

    // `select_resolved_run` returns one of the candidates verbatim, so the
    // owning shard is the one that contributed it. Match on `exec_id`, a UUID
    // primary key unique across every shard, not on the whole struct. The
    // fallback is unreachable while `select_resolved_run` returns one of its
    // inputs. If that changes, the shard bits of the id are the closest answer.
    let shard = candidates
        .into_iter()
        .find(|(_, run)| run.exec_id == winner.exec_id)
        .map_or_else(|| winner.exec_id.shard(), |(shard, _)| shard);
    TargetLocation::Found {
        shard,
        run: winner,
        uninspected,
        other_live,
    }
}

/// How long a fan-out will wait for a **peer** shard's connection before giving
/// up on that shard for this attempt (issue #1146).
///
/// Deliberately far shorter than [`crate::audit_export::SHARD_ACQUIRE_BOUND`],
/// and for a different reason. That bound stops an *indefinite* park and is
/// generous on purpose, so a merely-busy shard is not starved. This one stops a
/// **circular wait between per-shard scanners in one process**. There, a
/// generous bound is the problem, not the cure.
///
/// The cycle: `Worker` spawns one timeout checker per assigned shard. Each
/// holds a connection of its own shard pool for the whole
/// `enforce_timeouts_once` pass. With `shard_assignments = [0, 1]` and one
/// connection per pool, checker 0 can wait for pool 1 while checker 1 waits
/// for pool 0. Nothing deadlocks, because both bounds expire. But with a
/// *generous* bound, both scanners stall on every tick. That delays every other
/// resident of that pass.
///
/// **What this bound guarantees is bounded return, not progress.** A
/// one-connection pool has nothing to spare while its own scanner is mid-pass.
/// So a peer read succeeds only if it lands in the sleep window of that
/// scanner. That is likely. Passes are short relative to `poll_interval`, and
/// two independent tasks that do different work do not stay in phase. But it is
/// a probability, not a guarantee, and this constant does nothing to create it.
///
/// **The guarantee is capacity, not timing.** A process that runs checkers for
/// more than one shard should size each shard pool at **2 or more**. One
/// connection serves that shard's own scanner, and one serves a peer's
/// cross-shard read. `docs/sharding.md` says so.
/// `Worker::run_multi_shard` warns at startup when a multi-shard process is
/// configured below it, so the degradation is visible. This bound keeps such a
/// deployment degraded-and-retrying, not stalled. That is all it is for.
///
/// The same circular shape applies to the *delivery* acquisition in the outbox
/// scanners. That acquisition predates this issue (it existed for cross-shard
/// `ExecutionId` targets) and was unbounded. `timeout.rs` now bounds it with
/// this constant too.
pub const FANOUT_ACQUIRE_BOUND: std::time::Duration = std::time::Duration::from_millis(250);

/// Total budget for one **peer** shard probe — acquisition *and* query
/// (issue #1146).
///
/// [`FANOUT_ACQUIRE_BOUND`] bounds only the checkout. A connection that is
/// handed over and then never answers is just as fatal. The peer database can
/// become a network black hole after checkout. An `ACCESS EXCLUSIVE` DDL lock
/// can block the read. Then the `await` on the query hangs forever. That wedges
/// the timeout checker of the *caller* shard. It also runs task timeouts, SLA
/// enforcement, session reclaim and every other outbox. So one unhealthy peer
/// halts unrelated enforcement on a healthy shard indefinitely.
///
/// The whole peer probe is therefore wrapped in this bound, with the tighter
/// acquisition bound nested inside it. The two bounds answer different
/// questions. The inner one keeps per-shard scanners from waiting on each
/// other's connections at all (a circular wait). This one caps what a single
/// unhealthy peer can cost. Expiry is classified exactly like a failed
/// acquisition: an [`UninspectedShard`], hence `Indeterminate` and a retry,
/// never "the key is not there".
///
/// Generous relative to a healthy probe (an indexed lookup on
/// `(workflow_name, workflow_id)`), so ordinary load never trips it. Small
/// enough that N shards cannot dominate a scanner tick. The per-sweep
/// [`UninspectableShards`] memo caps the cost at one bound per shard per sweep,
/// not per row.
pub const FANOUT_PEER_BOUND: std::time::Duration = std::time::Duration::from_secs(2);

/// Shards a sweep has already found uninspectable, memoized for the length of
/// that sweep (issue #1146).
///
/// An outbox sweep processes one pending row per step and may see hundreds in a
/// backlog. A shard that failed to hand over a connection on the first row has
/// not recovered by the two-hundredth. Each re-probe costs the full
/// [`FANOUT_PEER_BOUND`]. Without this memo, one unreachable shard turns a
/// sweep into `rows × bound` of dead wall-clock. That starves every scanner
/// resident sequenced after the outbox.
///
/// Memoizing can only push an answer toward [`TargetLocation::Indeterminate`],
/// which is the safe direction. A shard recorded here is reported uninspected,
/// so the row is retried, not resolved from a partial view. The memo is
/// per-sweep, never process-wide, so a shard that comes back is re-probed on the
/// very next sweep.
#[derive(Clone, Debug, Default)]
pub struct UninspectableShards {
    shards: std::sync::Arc<
        std::sync::Mutex<std::collections::BTreeMap<ShardId, (String, UninspectedReasonKind)>>,
    >,
}

impl UninspectableShards {
    /// A fresh, empty memo for one sweep.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The recorded reason and kind `shard` could not be inspected earlier in
    /// this sweep.
    #[must_use]
    pub fn recorded(&self, shard: ShardId) -> Option<(String, UninspectedReasonKind)> {
        self.shards
            .lock()
            .ok()
            .and_then(|guard| guard.get(&shard).cloned())
    }

    /// Record that `shard` could not be inspected. The first reason wins, so the
    /// log names the original failure rather than a later re-derivation of it.
    pub fn record(&self, shard: ShardId, reason: String, kind: UninspectedReasonKind) {
        if let Ok(mut guard) = self.shards.lock() {
            guard.entry(shard).or_insert((reason, kind));
        }
    }
}

/// Resolve where `(workflow_name, workflow_id)` actually lives, by inspecting
/// every expected shard (issue #1146).
///
/// Convenience wrapper over [`resolve_location_by_workflow_id_with`] for
/// callers that hold no connection of their own and need no per-sweep memo.
/// **The outbox must not use this form** — see the connection-budget rules in
/// the module docs.
pub async fn resolve_location_by_workflow_id(
    pool: &ShardedDbPool,
    router: Option<&ShardRouter>,
    workflow_name: &str,
    workflow_id: &str,
) -> TargetLocation {
    resolve_location_by_workflow_id_with(pool, router, workflow_name, workflow_id, None, None, None)
        .await
}

/// Resolve where `(workflow_name, workflow_id)` actually lives, reusing a
/// connection the caller already holds and memoizing unreachable shards
/// (issue #1146).
///
/// `router` is the topology snapshot that widens the fan-out beyond this
/// process's own pools. Pass `None` when no router is installed (tests,
/// embedders that never call [`crate::shard::install_global_router`]). Then
/// only the configured pools are inspected.
///
/// `held` names a shard the caller is **already connected to**, with that
/// connection. That shard is probed on it, with no second connection from its
/// pool. This is not an optimisation. It is the fix for a self-deadlock. The
/// outbox calls this inside a transaction on a connection from that very pool,
/// and `pool.get()` is an unbounded wait. Reading through the held connection
/// is equivalent. The transaction is READ COMMITTED, so each statement takes a
/// fresh snapshot and sees exactly what a separate connection would.
///
/// `memo` carries the shards this sweep has already failed to reach, so a
/// backlog pays [`FANOUT_PEER_BOUND`] once per shard rather than once per row.
/// The cross-shard *delivery* acquisition that follows a resolution is not
/// memoized. It is a different pool, reached on a different code path. A
/// target pool that was reachable during the fan-out and busy at delivery
/// still costs its own bound per row.
///
/// The fan-out is **sequential**, and holds at most one connection beyond the
/// caller's own at a time. Read-only: no row is locked and nothing is written.
///
/// `idempotency_key`, when `Some`, is checked on every shard this fan-out
/// already visits, before that shard's run-resolution query. A prior
/// delivery short-circuits the whole fan-out with
/// [`TargetLocation::AlreadyDelivered`] (issue #1318). Pass `None` for a
/// cancel (which has no such key) or an unkeyed signal.
pub async fn resolve_location_by_workflow_id_with(
    pool: &ShardedDbPool,
    router: Option<&ShardRouter>,
    workflow_name: &str,
    workflow_id: &str,
    held: Option<(ShardId, &mut AsyncPgConnection)>,
    memo: Option<&UninspectableShards>,
    idempotency_key: Option<&str>,
) -> TargetLocation {
    let (candidates, uninspected) = match fan_out_candidates(
        pool,
        router,
        workflow_name,
        workflow_id,
        held,
        memo,
        idempotency_key,
    )
    .await
    {
        Ok(FanOutOutcome::Candidates(candidates, uninspected)) => (candidates, uninspected),
        Ok(FanOutOutcome::AlreadyDelivered { shard }) => {
            return TargetLocation::AlreadyDelivered { shard };
        }
        Err(uninspected) => return TargetLocation::Indeterminate { uninspected },
    };

    let placement = merge_locations(candidates, uninspected.clone(), idempotency_key.is_some());

    // A `Found` over an incomplete fan-out is an answer with a caveat.
    // Uniqueness is shard-local, so the unread shard might hold a newer live
    // run of the same key. Delivering to the run in hand is the right call (see
    // `merge_locations`), but the ambiguity must not be silent.
    if !uninspected.is_empty()
        && let Some(shard) = placement.found_shard()
    {
        tracing::warn!(
            workflow_name,
            workflow_id,
            resolved_shard = %shard,
            uninspected = %uninspected
                .iter()
                .map(|u| format!("{} ({})", u.shard, u.reason))
                .collect::<Vec<_>>()
                .join(", "),
            "by-id target resolved over an incomplete shard fan-out"
        );
    }

    placement
}

/// The success-path outcome of [`fan_out_candidates`].
///
/// `AlreadyDelivered` only ever arises when the caller passed an
/// `idempotency_key`. So [`check_cross_shard_occupancy`] — which never does —
/// cannot observe it (issue #1308 review finding; issue #1318 added the
/// variant).
enum FanOutOutcome {
    /// Every candidate the fan-out found, plus every shard it could not
    /// inspect. Delivery reduces this via [`merge_locations`]; occupancy
    /// reduces it via [`classify_occupancy_from_candidates`].
    Candidates(Vec<(ShardId, ResolvedRun)>, Vec<UninspectedShard>),
    /// A keyed signal already landed on `shard` (issue #1318). The fan-out
    /// stopped the instant it found this — see the inline comment where it
    /// is produced below.
    AlreadyDelivered {
        /// The shard the earlier delivery's signal row lives on.
        shard: ShardId,
    },
}

/// The raw per-shard fan-out behind two different reductions.
/// [`resolve_location_by_workflow_id_with`] reduces it to a single winning
/// run via [`merge_locations`], for delivery.
/// [`check_cross_shard_occupancy`] asks a different question over the same
/// raw candidates instead. Does ANY of them still occupy the key, not which
/// one wins (issue #1308 review finding).
///
/// `Err(uninspected)` is the held-connection-failed case: the caller's own
/// transaction is now aborted, so the fan-out stops immediately rather than
/// touching `conn` again. See the inline comment on that arm below.
///
/// `Ok` carries every candidate the fan-out found and every shard it could
/// not inspect, for either reduction to use. Unless it short-circuited on a
/// prior delivery of `idempotency_key` first (issue #1318).
#[allow(clippy::too_many_lines)]
async fn fan_out_candidates(
    pool: &ShardedDbPool,
    router: Option<&ShardRouter>,
    workflow_name: &str,
    workflow_id: &str,
    mut held: Option<(ShardId, &mut AsyncPgConnection)>,
    memo: Option<&UninspectableShards>,
    idempotency_key: Option<&str>,
) -> Result<FanOutOutcome, Vec<UninspectedShard>> {
    // Empty-string normalizes to "no key", matching `send_signal_idempotent`
    // (issue #521). An empty key is never stored -- it becomes `NULL` --
    // so a check for it would run a query that can never match.
    let idempotency_key = idempotency_key.filter(|k| !k.is_empty());
    let expected = fanout_shards(&pool.shard_ids(), router);
    let mut candidates: Vec<(ShardId, ResolvedRun)> = Vec::new();
    let mut uninspected: Vec<UninspectedShard> = Vec::new();
    // Underlying pools already probed by this fan-out. Clones of ONE physical
    // pool may back several logical shards. That is a supported topology: a
    // pre-split staging deployment maps several shard ids onto one database.
    // `ShardedDbPool::from_map` produces it whenever the same `DbPool` is
    // inserted under more than one id.
    //
    // Probing such an alias again is not merely wasteful. It is a bug. The
    // resolver filters on `(workflow_name, workflow_id)` with no shard
    // predicate, so an alias returns the rows its sibling already returned.
    // But the acquisition it needs comes from the very pool that supplied
    // `held`. On a one-connection pool, that acquisition cannot succeed until
    // this function returns. So the alias was reported uninspected on every
    // sweep, and a by-id cancel never reached an authoritative answer. It
    // cancelled the live run and then withheld its terminal forever.
    //
    // An alias is *inspected*, not uninspected: its database was read through
    // its sibling. So it is skipped silently and contributes nothing new.
    let mut probed: Vec<&DbPool> = Vec::new();

    // SEED with the held connection's own pool, before the loop. The fan-out
    // visits shards in ascending order. Seeding inside the `held_here` branch
    // is too late when an alias sorts BEFORE the held shard. Example: a checker
    // holds shard 1 of an aliased {0, 1} pair. It would try to acquire
    // "shard 0", the pool it is itself holding. On a one-connection pool it
    // times out and marks it uninspected on every sweep. A by-id cancel then
    // withholds its terminal forever. Order must not decide correctness here
    // (issue #1146).
    if let Some((held_shard, _)) = held.as_ref()
        && let Some(held_pool) = pool.exact_pool_for(*held_shard)
    {
        probed.push(held_pool);
    }

    for shard in expected {
        // 1. The caller's own shard: probe it on the connection already in hand.
        //    Never re-enter that pool — see this function's doc comment.
        let held_here = held
            .as_ref()
            .is_some_and(|(held_shard, _)| *held_shard == shard);
        if held_here {
            let Some((_, conn)) = held.as_mut() else {
                unreachable!("held_here implies held is Some")
            };
            // The idempotency-key check runs FIRST, on the same held
            // connection, before the run-resolution query below (issue
            // #1318). A hit ends the fan-out immediately: the caller must
            // record delivery, not resolve a shard to deliver against.
            if let Some(key) = idempotency_key {
                match crate::execution::lookup_idempotent_signal_dedupe(
                    conn,
                    workflow_name,
                    workflow_id,
                    key,
                )
                .await
                {
                    Ok(Some(_)) => return Ok(FanOutOutcome::AlreadyDelivered { shard }),
                    Ok(None) => {}
                    Err(e) => {
                        // Same abort hazard as the run-resolution query's own
                        // error arm below. A failed statement on the HELD
                        // connection aborts the caller's whole transaction.
                        // Stop here rather than run the next statement on it.
                        uninspected.push(UninspectedShard {
                            shard,
                            reason: format!(
                                "idempotency-key lookup failed on the caller's own connection, \
                                 aborting its transaction: {e}"
                            ),
                            kind: UninspectedReasonKind::QueryError,
                        });
                        return Err(uninspected);
                    }
                }
            }
            match crate::execution::resolve_execution_id_by_workflow_id(
                conn,
                workflow_name,
                workflow_id,
            )
            .await
            {
                Ok(Some(run)) => candidates.push((shard, run)),
                Ok(None) => {}
                Err(e) => {
                    // A failed query on the HELD connection is not merely "this
                    // shard is uninspected". It ran inside the open transaction
                    // of the caller. In Postgres, any statement error aborts
                    // that transaction. Every later statement on `conn` fails
                    // with 25P02, including the append of the terminal event.
                    //
                    // Carrying on would be actively unsafe, not merely
                    // incomplete. A live winner on a peer shard yields `Found`.
                    // The delivery runs on the connection of the PEER and
                    // commits there independently. Only afterwards does the
                    // caller find it can no longer record that it delivered.
                    // The row stays pending and the next sweep delivers again.
                    // A signal without an idempotency key is not idempotent.
                    // So that is a duplicate signal, the exact hazard the
                    // signal path refuses to take elsewhere.
                    //
                    // Stop the fan-out instead. `Indeterminate` routes to
                    // `DeliveryRoute::Retry`, which returns without touching
                    // `conn` again. The aborted transaction rolls back
                    // harmlessly and the row is retried intact.
                    //
                    // Deliberately NOT memoized: the abort scopes to the
                    // transaction of this row, not to the sweep. The next row
                    // opens a fresh one. Recording the shard of the caller as
                    // uninspectable would degrade every remaining row over a
                    // fault that is already gone.
                    uninspected.push(UninspectedShard {
                        shard,
                        reason: format!(
                            "resolution query failed on the caller's own connection,                              aborting its transaction: {e}"
                        ),
                        kind: UninspectedReasonKind::QueryError,
                    });
                    return Err(uninspected);
                }
            }
            continue;
        }

        // 2. Already known bad this sweep: report it without paying the bound
        //    a second time. (Recorded already, so it does not re-memoize.)
        if let Some((reason, kind)) = memo.and_then(|m| m.recorded(shard)) {
            uninspected.push(UninspectedShard {
                shard,
                reason,
                kind,
            });
            continue;
        }

        let Some(shard_pool) = pool.exact_pool_for(shard) else {
            mark_uninspected(
                &mut uninspected,
                memo,
                shard,
                "no storage pool configured in this process".to_string(),
                UninspectedReasonKind::NoPool,
            );
            continue;
        };

        // An alias of a pool this fan-out has already probed, including the
        // one that supplied `held`. Note "probed", not "read". The entry is
        // pushed before the probe runs, so a sibling whose probe FAILED also
        // suppresses its aliases. That is deliberately correct. The failed
        // sibling is already in `uninspected`, so the fan-out is
        // non-authoritative either way. The reason to skip is "this database
        // is already paid for", not "it was read". Re-acquiring would at best
        // duplicate a row. At worst (a one-connection pool) it would deadlock
        // against the connection the caller still holds.
        if probed
            .iter()
            .any(|probed_pool| same_underlying_pool(probed_pool, shard_pool))
        {
            continue;
        }
        probed.push(shard_pool);

        // 3. The peer probe, entirely inside its own budget.
        match probe_peer_shard(shard_pool, workflow_name, workflow_id, idempotency_key).await {
            Ok(ProbeOutcome::AlreadyDelivered) => {
                return Ok(FanOutOutcome::AlreadyDelivered { shard });
            }
            Ok(ProbeOutcome::Run(Some(run))) => candidates.push((shard, run)),
            Ok(ProbeOutcome::Run(None)) => {}
            Err((reason, kind)) => mark_uninspected(&mut uninspected, memo, shard, reason, kind),
        }
    }

    Ok(FanOutOutcome::Candidates(candidates, uninspected))
}

/// Whether a `(workflow_name, workflow_id)` key is held by a live run
/// anywhere. For a caller deciding whether a cross-shard operation on that
/// key is safe (issue #1308).
///
/// Replaces a hash-based *prediction* of where the key would resolve. Uses an
/// *observation* of where it actually lives instead, via the same fan-out
/// [`resolve_location_by_workflow_id_with`] uses for by-id delivery.
///
/// `#[non_exhaustive]`: [`TargetLocation`] may grow an ambiguous-live-runs
/// verdict (see its own doc comment). A caller that must react to it
/// differently from [`Self::Occupied`] should not compile silently once it
/// does.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CrossShardOccupancy {
    /// No shard holds a run of the key in a state that still occupies it —
    /// see [`crate::execution::workflow_id_slot_is_released`]. The caller's
    /// operation may proceed.
    Free,
    /// A run of the key already occupies the uniqueness slot, on `shard`.
    /// Not only a live run: an ordinary terminal one (`COMPLETED`, `FAILED`,
    /// `CANCELLED`, `TIMED_OUT`) occupies it too, matching
    /// `resolve_successor_slot` and the re-run `RejectDuplicate` check. Only
    /// `CONTINUED_AS_NEW` and `TERMINATED` release it. The caller must not
    /// create a second row under this key.
    Occupied {
        /// The shard holding the occupying run.
        shard: ShardId,
    },
    /// A shard could not be inspected, so occupancy is unknown. The caller
    /// must fail closed — matching the pre-#1308 hash-based rejection —
    /// rather than risk two runs sharing one business key.
    Indeterminate {
        /// The shards that could not be inspected.
        uninspected: Vec<UninspectedShard>,
    },
}

/// Pure decision behind [`check_cross_shard_occupancy`], split out so it is
/// unit-testable without a database.
///
/// Deliberately does NOT go through [`merge_locations`]'s single-winner
/// ranking. Delivery asks "which run is the current one", so picking the
/// most recently started candidate is correct there. Occupancy asks a
/// different question: "does ANY candidate still occupy the key". Answering
/// that from the ranked winner alone is wrong. A newer *released* row on one
/// shard (issue #1308 review finding) can outrank an older *unreleased* one
/// on another. That hides the very occupant this check exists to find. So
/// this scans every candidate directly, using
/// [`crate::execution::workflow_id_slot_is_released`]. That is narrower than
/// [`crate::erase::is_terminal_state`], which governs replay and delivery,
/// not uniqueness.
///
/// An unreleased candidate settles the question immediately: `Occupied`, even
/// over an incomplete fan-out, since occupancy is already proven. Only when
/// every candidate found is released does an uninspected shard matter — it
/// might hold an unreleased one — so that case is `Indeterminate`.
#[must_use]
fn classify_occupancy_from_candidates(
    candidates: &[(ShardId, ResolvedRun)],
    uninspected: Vec<UninspectedShard>,
) -> CrossShardOccupancy {
    if let Some(shard) = candidates
        .iter()
        .find(|(_, run)| !crate::execution::workflow_id_slot_is_released(&run.state))
        .map(|(shard, _)| *shard)
    {
        return CrossShardOccupancy::Occupied { shard };
    }
    if uninspected.is_empty() {
        CrossShardOccupancy::Free
    } else {
        CrossShardOccupancy::Indeterminate { uninspected }
    }
}

/// Check whether `(workflow_name, workflow_id)` is held by an occupying run
/// on any expected shard, reusing a connection the caller already holds
/// (issue #1308).
///
/// A thin wrapper over [`fan_out_candidates`] and
/// [`classify_occupancy_from_candidates`] — see both for the fan-out's
/// connection-budget rules and the free/occupied/indeterminate decision.
/// Built for a caller that today rejects on a hash mismatch alone
/// ([`crate::shard::external_target_owning_shard`]) and wants to reject only
/// when the key is genuinely occupied.
pub async fn check_cross_shard_occupancy(
    pool: &ShardedDbPool,
    router: Option<&ShardRouter>,
    workflow_name: &str,
    workflow_id: &str,
    held: Option<(ShardId, &mut AsyncPgConnection)>,
) -> CrossShardOccupancy {
    match fan_out_candidates(pool, router, workflow_name, workflow_id, held, None, None).await {
        Ok(FanOutOutcome::Candidates(candidates, uninspected)) => {
            classify_occupancy_from_candidates(&candidates, uninspected)
        }
        Ok(FanOutOutcome::AlreadyDelivered { .. }) => {
            unreachable!(
                "this call passes idempotency_key = None, so AlreadyDelivered cannot occur"
            )
        }
        Err(uninspected) => CrossShardOccupancy::Indeterminate { uninspected },
    }
}

/// Are these two handles the **same underlying pool** — clones sharing one
/// connection set — rather than two pools that merely look alike?
///
/// A `deadpool` `Pool` is a thin handle over an `Arc<PoolInner>`.
/// `Pool::manager()` borrows out of that shared allocation. So two clones
/// return the same address, and two independently built pools cannot.
/// Comparing the handles themselves would not work. `ShardedDbPool` stores the
/// pool of each shard in its own map slot. So `std::ptr::eq` on `&DbPool` is
/// really shard-id equality, and it reports two aliases of one pool as
/// different (issue #1146, noted in review, load-bearing here).
pub(crate) fn same_underlying_pool(a: &DbPool, b: &DbPool) -> bool {
    std::ptr::eq(a.manager(), b.manager())
}

/// What one shard probe found (issue #1146; extended issue #1318).
enum ProbeOutcome {
    /// The run-resolution query's answer for this shard — `None` when the
    /// shard holds no run under this key.
    Run(Option<ResolvedRun>),
    /// The idempotency-key lookup, run first when a key is given, found a
    /// prior delivery on this shard. The run-resolution query never runs —
    /// this outcome makes it moot (issue #1318).
    AlreadyDelivered,
}

/// Probe one **peer** shard for `(workflow_name, workflow_id)`, entirely inside
/// a bounded budget.
///
/// When `idempotency_key` is `Some`, the SAME connection first checks for a
/// prior delivery of that key on this shard (issue #1318). That costs an
/// extra query, not an extra connection. It is skipped entirely for an
/// unkeyed signal or a cancel, which never supplies one.
///
/// `Err` is the single "this shard could not be inspected" outcome. It carries
/// the operator-facing reason: a failed or timed-out acquisition, a failed
/// query, or a probe that never returned. With all four in one error, the
/// caller cannot forget to memoize one of them. That is exactly the bug this
/// shape replaced. Only the acquisition failures were recorded, so a peer whose
/// *query* failed slowly was re-probed once per pending row, not once per sweep
/// (issue #1146).
///
/// A query that returns an error is `Err`, never `Ok(None)`. A shard that
/// answered with a failure has not said that the key is absent.
///
/// Two nested bounds cover two different hazards. [`FANOUT_ACQUIRE_BOUND`]
/// covers per-shard scanners that wait on each other's connections.
/// [`FANOUT_PEER_BOUND`] covers one unhealthy peer that wedges the scanner of
/// this shard, wherever it stalls.
async fn probe_peer_shard(
    shard_pool: &DbPool,
    workflow_name: &str,
    workflow_id: &str,
    idempotency_key: Option<&str>,
) -> Result<ProbeOutcome, (String, UninspectedReasonKind)> {
    let probe = tokio::time::timeout(FANOUT_PEER_BOUND, async {
        let acquire_bound = peer_acquire_bound(shard_pool);
        let mut conn = match crate::replication::fenced_get_within(shard_pool, acquire_bound).await
        {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                return Err((
                    format!("could not acquire a connection: {e}"),
                    UninspectedReasonKind::AcquireTimeout,
                ));
            }
            Err(_elapsed) => {
                return Err((
                    format!(
                        "no connection available within {acquire_bound:?} (pool busy or \
                     unreachable)"
                    ),
                    UninspectedReasonKind::AcquireTimeout,
                ));
            }
        };
        if let Some(key) = idempotency_key {
            let hit = crate::execution::lookup_idempotent_signal_dedupe(
                &mut conn,
                workflow_name,
                workflow_id,
                key,
            )
            .await
            .map_err(|e| {
                (
                    format!("idempotency-key lookup failed: {e}"),
                    UninspectedReasonKind::QueryError,
                )
            })?;
            if hit.is_some() {
                return Ok(ProbeOutcome::AlreadyDelivered);
            }
        }
        crate::execution::resolve_execution_id_by_workflow_id(&mut conn, workflow_name, workflow_id)
            .await
            .map(ProbeOutcome::Run)
            .map_err(|e| {
                (
                    format!("resolution query failed: {e}"),
                    UninspectedReasonKind::QueryError,
                )
            })
    })
    .await;

    match probe {
        Ok(result) => result,
        Err(_elapsed) => Err((
            format!("shard did not answer within {FANOUT_PEER_BOUND:?}"),
            UninspectedReasonKind::AcquireTimeout,
        )),
    }
}

/// How long may this peer pool be given to hand over a connection?
///
/// [`FANOUT_ACQUIRE_BOUND`] exists for exactly one hazard: two per-shard
/// scanners waiting on each other's **held** connections. That hazard needs the
/// pool to be at capacity with nothing free — only then does acquiring mean
/// waiting for another task to release something.
///
/// When the pool has an idle connection, or room to open one, `Pool::get()`
/// queues behind nobody. It is a DNS + TCP + TLS + Postgres startup and auth
/// handshake. Holding that to 250 ms is a different decision than bounding a
/// wait. It is a badly wrong one, because it does not converge. `deadpool`
/// records a new object only *after* `Manager::create()` resolves. So a
/// cancelled connect leaves the pool exactly as cold as it found it. The
/// per-sweep memo then allows only one attempt per shard per sweep. A peer
/// whose handshake reliably takes longer than the bound is then uninspectable
/// **forever**, not slow. By-id delivery to it never completes. Peer pools are
/// routinely cold. `docs/sharding.md` recommends one process per shard. In
/// that deployment, only this fan-out ever uses them.
///
/// So the tight bound is applied only in the case it was written for, and
/// everything else gets the whole probe budget. [`FANOUT_PEER_BOUND`] still
/// wraps the result either way, so the worst case per shard is unchanged
/// (issue #1146).
pub(crate) fn peer_acquire_bound(pool: &DbPool) -> std::time::Duration {
    let status = pool.status();
    if status.available > 0 || status.size < status.max_size {
        FANOUT_PEER_BOUND
    } else {
        FANOUT_ACQUIRE_BOUND
    }
}

/// Record that `shard` could not be inspected, in the fan-out's own list **and**
/// in the per-sweep memo.
///
/// The single funnel for every uninspected outcome. The previous shape had four
/// of them written out separately. One of them, a failed resolution query,
/// silently skipped the memo. So a peer failing slowly just under the probe
/// bound was re-probed once per pending row, not once per sweep (issue #1146).
/// Routing them all through one place removes the class rather than the
/// instance.
fn mark_uninspected(
    uninspected: &mut Vec<UninspectedShard>,
    memo: Option<&UninspectableShards>,
    shard: ShardId,
    reason: String,
    kind: UninspectedReasonKind,
) {
    if let Some(memo) = memo {
        memo.record(shard, reason.clone(), kind);
    }
    uninspected.push(UninspectedShard {
        shard,
        reason,
        kind,
    });
}

/// May a delivery to `target` be attempted **inline** (issue #1146)?
///
/// "Inline" means inside the caller's own decision transaction, on the caller's
/// own shard connection, rather than left to the cross-shard outbox.
///
/// * An [`crate::types::ExternalTarget::ExecutionId`] may, exactly when its
///   encoded shard is the caller's. Unchanged from issue #492/#751: the id is
///   authoritative, so this comparison can never be wrong.
/// * An [`crate::types::ExternalTarget::WorkflowId`] may only in a
///   **single-shard** deployment.
///
/// The `WorkflowId` rule is the deliberate part. Inline delivery resolves the
/// business key against the shard of the caller alone.
/// `(workflow_name, workflow_id)` uniqueness is *shard-local*. So in a
/// multi-shard deployment, the shard of the caller can hold a stale
/// **terminal** run of the key while the live run sits elsewhere. Inline
/// delivery from that view records
/// `ExternalSignalFailed { reason_code: "not_running" }` against a running
/// target. That record is permanent in the append-only history of the caller.
/// The previous rule compared the shard of the caller against the rendezvous
/// hash of the key. It did not prevent this. The hash lands on the shard of the
/// caller for 1-in-N keys, wherever the target was actually placed.
///
/// With one shard, the view of the caller *is* the whole deployment, so the
/// hazard cannot arise. Single-shard deployments keep inline delivery and are
/// byte-for-byte unchanged. That covers every pre-sharding deployment, and the
/// default. Multi-shard deployments defer to the outbox. That costs one sweep
/// of latency on an already-asynchronous path. Most of them went there
/// already. The pre-#1146 rule sent every key whose hash missed the shard of
/// the caller, `(N-1)/N` of them, down exactly this path.
#[must_use]
pub(crate) fn inline_delivery_allowed(
    target: &crate::types::ExternalTarget,
    caller_shard: ShardId,
    multi_shard: bool,
) -> bool {
    match target {
        crate::types::ExternalTarget::ExecutionId(id) => id.shard() == caller_shard,
        crate::types::ExternalTarget::WorkflowId { .. } => !multi_shard,
    }
}

/// Snapshot of the process-global [`ShardRouter`], if one is installed.
///
/// Exists so callers inside the engine can take the snapshot once and pass it
/// down by reference rather than each re-locking the global.
#[must_use]
pub(crate) fn global_router_snapshot() -> Option<ShardRouter> {
    crate::shard::GLOBAL_SHARD_ROUTER
        .read()
        .ok()
        .and_then(|guard| guard.as_ref().cloned())
}

/// Does this deployment span more than one shard?
///
/// Used to decide whether a `WorkflowId`-addressed delivery may be attempted
/// **inline**, inside the decision transaction of the caller, against its own
/// shard. It may only when there is exactly one shard. A single shard is
/// trivially the shard the target lives on. With two or more, the
/// shard-local view of the caller can hold a stale terminal run of the key.
/// The live run then sits on another shard, and inline delivery cannot see it.
/// Multi-shard deployments therefore defer every by-id delivery to the outbox,
/// where the full [`resolve_location_by_workflow_id`] fan-out runs.
///
/// Derived from the same expected-shard set the fan-out itself uses, so the
/// two cannot disagree about what "single shard" means.
///
/// **Fails closed.** The two answers are not symmetric in cost. A wrong
/// "multi-shard" only routes an inline-capable delivery through the outbox,
/// which costs one poll interval. A wrong "single-shard" re-opens issue #1146
/// on the inline path, which writes a wrong terminal to an append-only
/// history. So an unreadable `GLOBAL_SHARDED_POOL` lock (poisoned by a panic
/// while a writer held it) means "assume multi-shard". It must not collapse to
/// the single-shard default via an empty shard list.
#[must_use]
pub(crate) fn deployment_is_multi_shard() -> bool {
    let Ok(guard) = crate::shard::GLOBAL_SHARDED_POOL.read() else {
        return true;
    };
    let pool_shards = guard
        .as_ref()
        .map(ShardedDbPool::shard_ids)
        .unwrap_or_default();
    drop(guard);
    fanout_shards(&pool_shards, global_router_snapshot().as_ref()).len() > 1
}

/// [`deployment_is_multi_shard`] for integration tests in other crates.
///
/// The gate is crate-internal. Its answer for an installed topology is still
/// observable behaviour worth pinning from outside.
/// `autumn-harvest-plugin/tests/resolve_installs_selected_pool.rs` asserts that
/// a runtime resolving a multi-shard pool gates as multi-shard. Not part of the
/// operator-facing surface.
#[doc(hidden)]
#[must_use]
pub fn deployment_is_multi_shard_for_tests() -> bool {
    deployment_is_multi_shard()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ExecutionId;

    fn run(shard: i32, state: &str, secs: i64) -> ResolvedRun {
        ResolvedRun {
            exec_id: ExecutionId::new_for_shard(ShardId::new(shard)),
            state: state.to_string(),
            started_at: chrono::DateTime::from_timestamp(1_800_000_000 + secs, 0)
                .expect("valid timestamp"),
        }
    }

    fn uninspected(shard: i32) -> UninspectedShard {
        uninspected_with_kind(shard, UninspectedReasonKind::QueryError)
    }

    fn uninspected_with_kind(shard: i32, kind: UninspectedReasonKind) -> UninspectedShard {
        UninspectedShard {
            shard: ShardId::new(shard),
            reason: "test".to_string(),
            kind,
        }
    }

    #[test]
    fn a_complete_fanout_that_finds_nothing_is_not_found() {
        assert_eq!(
            merge_locations(Vec::new(), Vec::new(), false),
            TargetLocation::NotFound
        );
    }

    #[test]
    fn an_incomplete_fanout_that_finds_nothing_is_indeterminate() {
        assert_eq!(
            merge_locations(Vec::new(), vec![uninspected(2)], false),
            TargetLocation::Indeterminate {
                uninspected: vec![uninspected(2)]
            }
        );
    }

    #[test]
    fn a_terminal_winner_with_an_uninspected_shard_is_indeterminate() {
        let terminal = run(0, "COMPLETED", 1);
        assert_eq!(
            merge_locations(
                vec![(ShardId::new(0), terminal)],
                vec![uninspected(1)],
                false
            ),
            TargetLocation::Indeterminate {
                uninspected: vec![uninspected(1)]
            }
        );
    }

    #[test]
    fn an_answer_reports_whether_it_is_authoritative_for_the_whole_key() {
        let live = run(0, "RUNNING", 1);
        assert!(
            merge_locations(vec![(ShardId::new(0), live.clone())], Vec::new(), false)
                .is_authoritative_for_key(),
            "every shard answered and exactly one live run exists"
        );
        assert!(
            !merge_locations(vec![(ShardId::new(0), live)], vec![uninspected(1)], false)
                .is_authoritative_for_key(),
            "a live run found over a PARTIAL view — the un-inspected shard MAY hold \
             another live run of the same key, since uniqueness is shard-local"
        );
        assert!(
            merge_locations(Vec::new(), Vec::new(), false).is_authoritative_for_key(),
            "`NotFound` is only ever reached from a complete fan-out"
        );
        assert!(
            !merge_locations(Vec::new(), vec![uninspected(1)], false).is_authoritative_for_key(),
            "`Indeterminate` never is"
        );
    }

    #[test]
    fn issue_1313_a_start_racing_the_fanout_is_invisible_and_the_answer_is_still_authoritative() {
        // Issue #1313. The fan-out reads shards sequentially, on separate
        // connections, with no shared snapshot. Walk the exact race from the
        // issue:
        //
        // 1. Shard 0 is read and reports no run of key K. It contributes no
        //    candidate — indistinguishable, from here, from a shard that was
        //    never asked before K started.
        // 2. A run of K starts on shard 0, after the read. This fan-out never
        //    asks shard 0 again.
        // 3. Shard 1 is read and reports its live run of K.
        // 4. The merge sees one live candidate. Neither safety net fires.
        //    `uninspected` is empty. Shard 0 answered, just before the race.
        //    `other_live` is empty too. Shard 0's run had not started yet,
        //    so this fan-out never saw it.
        let live_on_shard_1 = run(1, "RUNNING", 1);
        let merged = merge_locations(vec![(ShardId::new(1), live_on_shard_1)], Vec::new(), false);

        assert_eq!(merged.found_shard(), Some(ShardId::new(1)));
        assert!(
            merged.is_authoritative_for_key(),
            "a complete, race-free-looking fan-out is reported authoritative even \
             though a run the fan-out could not have seen may be live on shard 0"
        );
        match merged {
            TargetLocation::Found {
                uninspected,
                other_live,
                ..
            } => {
                assert!(uninspected.is_empty(), "shard 0 was read; nothing to retry");
                assert!(
                    other_live.is_empty(),
                    "shard 0's run started after its read, so this fan-out has no way \
                     to have observed it"
                );
            }
            other => panic!("expected Found, got {other:?}"),
        }

        // This is accepted, not overlooked: a cancel here may report
        // `ExternalCancelDelivered` while shard 0's run stays live. Closing it
        // needs cross-shard uniqueness for the business key — a coordination
        // primitive this engine deliberately does not have. See
        // `TargetLocation::is_authoritative_for_key` and `docs/sharding.md`'s
        // "Business-key addressing finds a pinned run wherever it is" section
        // for the decision.
    }

    #[test]
    fn two_live_runs_on_different_shards_are_not_authoritative_even_when_complete() {
        // A key pinned to one shard (#697) and an unpinned start of the same key
        // on another shard give two live runs. No shard-local unique index can
        // see both. The fan-out DOES see both. The ranking still picks the most
        // recently started as the current run, the documented rule that a
        // signal follows. But a cancel that reported success here would leave
        // the older run live. Once the newest is cancelled, that older one
        // becomes the current run for the key.
        let newer = run(1, "RUNNING", 50);
        let older = run(0, "RUNNING", 10);
        let merged = merge_locations(
            vec![(ShardId::new(0), older), (ShardId::new(1), newer)],
            Vec::new(),
            false,
        );
        assert_eq!(
            merged.found_shard(),
            Some(ShardId::new(1)),
            "the most recently started live run is still the current run"
        );
        assert!(
            !merged.is_authoritative_for_key(),
            "but the answer cannot support a claim that nothing is running under \
             the key — another live run is right there on shard 0"
        );
        match merged {
            TargetLocation::Found { other_live, .. } => {
                assert_eq!(other_live, vec![ShardId::new(0)], "and it is named");
            }
            other => panic!("expected Found, got {other:?}"),
        }
    }

    #[test]
    fn a_single_live_run_beside_terminals_is_still_authoritative() {
        // Terminal siblings are not "another live run". They cannot become the
        // current run for the key, so they must not block a cancel from
        // reporting. Without this, the rule would stall every cancel of a key
        // that has ever continued-as-new across shards.
        let live = run(1, "RUNNING", 50);
        let dead = run(0, "COMPLETED", 90);
        assert!(
            merge_locations(
                vec![(ShardId::new(0), dead), (ShardId::new(1), live)],
                Vec::new(),
                false
            )
            .is_authoritative_for_key()
        );
    }

    #[test]
    fn a_live_winner_settles_the_question_despite_an_uninspected_shard() {
        let live = run(0, "RUNNING", 1);
        assert_eq!(
            merge_locations(
                vec![(ShardId::new(0), live.clone())],
                vec![uninspected(1)],
                false
            ),
            TargetLocation::Found {
                shard: ShardId::new(0),
                run: live,
                uninspected: vec![uninspected(1)],
                other_live: Vec::new()
            }
        );
    }

    #[test]
    fn a_keyed_request_never_trusts_a_live_winner_over_an_uninspected_shard() {
        // Code review finding (PR #1593, issue #1318). The uninspected
        // shard's idempotency-key check never ran. It might hold the prior
        // delivery the key names. Delivering fresh to the live winner found
        // elsewhere would then double-deliver. So `keyed = true` must NOT
        // take the "live winner settles it" shortcut. The sibling test
        // above proves that shortcut for an unkeyed signal and for a
        // cancel.
        let live = run(0, "RUNNING", 1);
        assert_eq!(
            merge_locations(vec![(ShardId::new(0), live)], vec![uninspected(1)], true),
            TargetLocation::Indeterminate {
                uninspected: vec![uninspected(1)]
            },
            "a keyed request must retry rather than risk delivering fresh to a \
             winner found while another shard -- possibly the one already \
             holding this key -- went uninspected"
        );
    }

    #[test]
    fn the_live_run_wins_over_a_more_recent_terminal_on_another_shard() {
        let live = run(1, "RUNNING", 1);
        let recent_terminal = run(0, "COMPLETED", 99);
        assert_eq!(
            merge_locations(
                vec![
                    (ShardId::new(0), recent_terminal),
                    (ShardId::new(1), live.clone()),
                ],
                Vec::new(),
                false
            ),
            TargetLocation::Found {
                shard: ShardId::new(1),
                run: live,
                uninspected: Vec::new(),
                other_live: Vec::new()
            },
            "a first-hit-wins fan-out would signal the dead run instead"
        );
    }

    #[test]
    fn the_reported_shard_is_the_one_that_contributed_the_winner() {
        // An unencoded execution id carries no shard of its own, so the only
        // way to report the right shard is to remember which shard answered.
        let unencoded = ResolvedRun {
            exec_id: ExecutionId::new(),
            state: "RUNNING".to_string(),
            started_at: chrono::DateTime::from_timestamp(1_800_000_000, 0).expect("valid"),
        };
        assert_eq!(
            merge_locations(vec![(ShardId::new(3), unencoded)], Vec::new(), false).found_shard(),
            Some(ShardId::new(3))
        );
    }

    #[test]
    fn the_fanout_set_unions_pools_with_the_routers_topology() {
        let router = ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            vec![ShardId::new(0)],
            ShardId::new(0),
        );
        assert_eq!(
            fanout_shards(&[ShardId::new(0), ShardId::new(7)], Some(&router)),
            vec![
                ShardId::new(0),
                ShardId::new(1),
                ShardId::new(2),
                ShardId::new(7)
            ]
        );
    }

    #[test]
    fn a_single_shard_deployment_fans_out_to_exactly_one_query() {
        assert_eq!(
            fanout_shards(&[ShardId::new(0)], Some(&ShardRouter::single())),
            vec![ShardId::new(0)]
        );
    }

    #[test]
    fn the_fanout_set_falls_back_to_shard_zero_with_no_pool_and_no_router() {
        assert_eq!(fanout_shards(&[], None), vec![ShardId::new(0)]);
    }

    // ── inline-vs-outbox gate ────────────────────────────────────────────

    fn by_id() -> crate::types::ExternalTarget {
        crate::types::ExternalTarget::WorkflowId {
            workflow_name: "entity".to_string(),
            workflow_id: "e-1".to_string(),
        }
    }

    #[test]
    fn an_execution_id_target_on_the_callers_shard_still_delivers_inline() {
        let same = ExecutionId::new_for_shard(ShardId::new(2));
        assert!(inline_delivery_allowed(
            &crate::types::ExternalTarget::ExecutionId(same),
            ShardId::new(2),
            true
        ));
    }

    #[test]
    fn an_execution_id_target_on_another_shard_never_delivers_inline() {
        let elsewhere = ExecutionId::new_for_shard(ShardId::new(3));
        assert!(!inline_delivery_allowed(
            &crate::types::ExternalTarget::ExecutionId(elsewhere),
            ShardId::new(2),
            false
        ));
    }

    #[test]
    fn a_by_id_target_delivers_inline_only_in_a_single_shard_deployment() {
        assert!(
            inline_delivery_allowed(&by_id(), ShardId::new(0), false),
            "single-shard deployments keep the pre-#1146 inline fast path"
        );
        assert!(
            !inline_delivery_allowed(&by_id(), ShardId::new(0), true),
            "with more than one shard the caller's shard-local view can hold a \
             stale terminal run of the key while the live one is elsewhere"
        );
    }

    // ---- The per-sweep memo (issue #1146). ----
    //
    // Its whole purpose is to make a failing shard cost `shards × bound` rather
    // than `rows × bound` across one sweep. Every DB test seeds exactly one
    // pending row, so nothing there can tell a working memo from a memo that is
    // never consulted.

    #[test]
    fn the_memo_reports_back_a_shard_it_recorded() {
        let memo = UninspectableShards::new();
        assert_eq!(memo.recorded(ShardId::new(1)), None);
        memo.record(
            ShardId::new(1),
            "no pool".to_string(),
            UninspectedReasonKind::NoPool,
        );
        assert_eq!(
            memo.recorded(ShardId::new(1)),
            Some(("no pool".to_string(), UninspectedReasonKind::NoPool))
        );
        assert_eq!(
            memo.recorded(ShardId::new(2)),
            None,
            "recording one shard must not implicate another"
        );
    }

    #[test]
    fn the_memo_keeps_the_first_reason_and_kind_it_was_given() {
        // The short-circuit path deliberately does not re-record, so an operator
        // sees the original diagnosis rather than a later re-derivation of it.
        let memo = UninspectableShards::new();
        memo.record(
            ShardId::new(1),
            "could not acquire a connection".to_string(),
            UninspectedReasonKind::AcquireTimeout,
        );
        memo.record(
            ShardId::new(1),
            "something later and vaguer".to_string(),
            UninspectedReasonKind::QueryError,
        );
        assert_eq!(
            memo.recorded(ShardId::new(1)),
            Some((
                "could not acquire a connection".to_string(),
                UninspectedReasonKind::AcquireTimeout
            ))
        );
    }

    #[test]
    fn uninspected_reason_kind_has_a_bounded_metric_label_per_variant() {
        // issue #1307: the label is the metric-facing value, kept distinct from
        // any future Display/Debug rendering.
        assert_eq!(UninspectedReasonKind::NoPool.as_label(), "no_pool");
        assert_eq!(
            UninspectedReasonKind::AcquireTimeout.as_label(),
            "acquire_timeout"
        );
        assert_eq!(UninspectedReasonKind::QueryError.as_label(), "query_error");
    }

    #[test]
    fn one_run_reported_by_two_aliased_shards_is_not_two_live_runs() {
        // Two logical shard ids each return the SAME row. Their pools are two
        // independently built pools on one URL, so this fan-out cannot prove
        // they are the same database. That must not read as the ambiguous
        // "two live runs of one key" state, which would withhold every cancel's
        // terminal forever. `other_live` filters on `exec_id`, so it does not.
        let only = run(0, "RUNNING", 10);
        let merged = merge_locations(
            vec![(ShardId::new(0), only.clone()), (ShardId::new(1), only)],
            Vec::new(),
            false,
        );
        assert!(
            merged.is_authoritative_for_key(),
            "the same run seen twice is one run"
        );
        match merged {
            TargetLocation::Found { other_live, .. } => {
                assert_eq!(other_live, [] as [crate::types::ShardId; 0]);
            }
            other => panic!("expected Found, got {other:?}"),
        }
    }

    #[test]
    fn a_pool_with_room_to_open_a_connection_gets_the_full_probe_budget() {
        // Regression for the cold-connect trap: the tight acquisition bound is
        // for waiting on another task's connection, never for a handshake. A
        // pool that has not opened one yet is not waiting on anybody.
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new("postgres://unused:unused@127.0.0.1:1/unused");
        let pool: DbPool = deadpool::managed::Pool::builder(manager)
            .max_size(2)
            .build()
            .expect("pool builds");
        assert_eq!(pool.status().size, 0, "a fresh pool has opened nothing");
        assert_eq!(
            peer_acquire_bound(&pool),
            FANOUT_PEER_BOUND,
            "an unopened pool must be allowed to finish its handshake"
        );
    }

    // ── UninspectedReasonKind classification (issue #1307) ─────────────────

    fn unreachable_pool() -> DbPool {
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new("postgres://unused:unused@127.0.0.1:1/unused");
        deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("pool builds")
    }

    #[tokio::test]
    async fn a_shard_with_no_configured_pool_is_classified_as_no_pool() {
        // `ShardedDbPool` requires at least one configured pool. Shard 0
        // carries one (unreachable), but this test is about shard 1, for
        // which no pool is configured at all. The router's fan-out is the
        // union of the pool's shards and its own. So shard 0 is unavoidably
        // probed too. That classification is covered by
        // `a_peer_shard_whose_connection_cannot_be_acquired_is_classified_as_acquire_timeout`.
        let mut pools = std::collections::BTreeMap::new();
        pools.insert(ShardId::new(0), unreachable_pool());
        let pool = ShardedDbPool::from_map(pools, ShardId::new(0));
        let router = ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        );

        let placement = resolve_location_by_workflow_id_with(
            &pool,
            Some(&router),
            "wf",
            "id",
            None,
            None,
            None,
        )
        .await;

        match placement {
            TargetLocation::Indeterminate { uninspected } => {
                let shard_1 = uninspected
                    .iter()
                    .find(|u| u.shard == ShardId::new(1))
                    .expect("shard 1 has no configured pool and must be reported uninspected");
                assert_eq!(shard_1.kind, UninspectedReasonKind::NoPool);
            }
            other => panic!("expected Indeterminate, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_peer_shard_whose_connection_cannot_be_acquired_is_classified_as_acquire_timeout() {
        // A pool that CANNOT hand over a connection (nothing listens on the
        // configured address) must be reported `AcquireTimeout`, never
        // `QueryError` — no query ever ran.
        let mut pools = std::collections::BTreeMap::new();
        pools.insert(ShardId::new(0), unreachable_pool());
        let pool = ShardedDbPool::from_map(pools, ShardId::new(0));

        let placement =
            resolve_location_by_workflow_id_with(&pool, None, "wf", "id", None, None, None).await;

        match placement {
            TargetLocation::Indeterminate { uninspected } => {
                assert_eq!(uninspected.len(), 1);
                assert_eq!(uninspected[0].kind, UninspectedReasonKind::AcquireTimeout);
            }
            other => panic!("expected Indeterminate, got {other:?}"),
        }
    }

    // -- classify_occupancy_from_candidates (issue #1308) ------------------

    #[test]
    fn no_candidates_and_nothing_uninspected_is_free() {
        assert_eq!(
            classify_occupancy_from_candidates(&[], Vec::new()),
            CrossShardOccupancy::Free
        );
    }

    #[test]
    fn an_ordinary_terminal_candidate_still_occupies_the_key() {
        // COMPLETED/FAILED/CANCELLED/TIMED_OUT are terminal but NOT released
        // (issue #1308 review finding). `resolve_successor_slot` and the
        // re-run `RejectDuplicate` check both reject on a same-shard row in
        // any of these states. So a cross-shard one must not be Free either.
        for state in ["COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT"] {
            let candidates = [(ShardId::new(0), run(0, state, 1))];
            assert_eq!(
                classify_occupancy_from_candidates(&candidates, Vec::new()),
                CrossShardOccupancy::Occupied {
                    shard: ShardId::new(0)
                },
                "state {state} must still occupy the key"
            );
        }
    }

    #[test]
    fn a_sealed_candidate_is_free() {
        // Only CONTINUED_AS_NEW and TERMINATED release the uniqueness slot.
        for state in ["CONTINUED_AS_NEW", "TERMINATED"] {
            let candidates = [(ShardId::new(0), run(0, state, 1))];
            assert_eq!(
                classify_occupancy_from_candidates(&candidates, Vec::new()),
                CrossShardOccupancy::Free,
                "state {state} must release the key"
            );
        }
    }

    #[test]
    fn a_live_candidate_is_occupied_on_its_shard() {
        let candidates = [(ShardId::new(0), run(0, "RUNNING", 1))];
        assert_eq!(
            classify_occupancy_from_candidates(&candidates, Vec::new()),
            CrossShardOccupancy::Occupied {
                shard: ShardId::new(0)
            }
        );
    }

    #[test]
    fn a_live_candidate_is_occupied_even_over_an_incomplete_fanout() {
        // A live run in hand is a real, certain occupant. An unrelated shard
        // being unreachable does not make that less true. This mirrors the
        // delivery-side rule: a live winner is not withheld for that reason
        // either.
        let candidates = [(ShardId::new(0), run(0, "RUNNING", 1))];
        assert_eq!(
            classify_occupancy_from_candidates(&candidates, vec![uninspected(1)]),
            CrossShardOccupancy::Occupied {
                shard: ShardId::new(0)
            }
        );
    }

    #[test]
    fn an_older_unreleased_candidate_is_not_masked_by_a_newer_released_one() {
        // Issue #1308 review finding: `merge_locations`' delivery ranking
        // picks the most recently started candidate. So a newer
        // CONTINUED_AS_NEW/TERMINATED row can outrank an older
        // COMPLETED/FAILED/CANCELLED/TIMED_OUT one. Occupancy must not reuse
        // that ranking. The older row still occupies the key on ITS shard,
        // regardless of what a newer, unrelated, released row exists
        // elsewhere.
        let older_unreleased = run(1, "COMPLETED", 1);
        let newer_released = run(0, "CONTINUED_AS_NEW", 2);
        let candidates = [
            (ShardId::new(1), older_unreleased),
            (ShardId::new(0), newer_released),
        ];
        assert_eq!(
            classify_occupancy_from_candidates(&candidates, Vec::new()),
            CrossShardOccupancy::Occupied {
                shard: ShardId::new(1)
            },
            "the older COMPLETED row on shard 1 must still occupy the key, \
             even though shard 0's newer CONTINUED_AS_NEW row would win \
             merge_locations' delivery ranking"
        );
    }

    #[test]
    fn several_live_candidates_are_occupied_on_the_first_found() {
        let candidates = [
            (ShardId::new(1), run(1, "RUNNING", 1)),
            (ShardId::new(0), run(0, "RUNNING", 2)),
        ];
        assert_eq!(
            classify_occupancy_from_candidates(&candidates, Vec::new()),
            CrossShardOccupancy::Occupied {
                shard: ShardId::new(1)
            }
        );
    }

    #[test]
    fn an_uninspected_shard_with_no_candidates_is_indeterminate() {
        assert_eq!(
            classify_occupancy_from_candidates(&[], vec![uninspected(1)]),
            CrossShardOccupancy::Indeterminate {
                uninspected: vec![uninspected(1)]
            }
        );
    }

    #[test]
    fn a_released_candidate_with_an_uninspected_shard_is_indeterminate() {
        // A released candidate does not settle the question when another
        // shard could not be checked. That shard might hold an unreleased
        // row of the same key.
        let candidates = [(ShardId::new(0), run(0, "COMPLETED", 1))];
        assert_eq!(
            classify_occupancy_from_candidates(&candidates, vec![uninspected(1)]),
            CrossShardOccupancy::Occupied {
                shard: ShardId::new(0)
            },
            "COMPLETED is unreleased, so this must be Occupied regardless of \
             the uninspected shard"
        );

        let candidates = [(ShardId::new(0), run(0, "TERMINATED", 1))];
        assert_eq!(
            classify_occupancy_from_candidates(&candidates, vec![uninspected(1)]),
            CrossShardOccupancy::Indeterminate {
                uninspected: vec![uninspected(1)]
            },
            "TERMINATED releases shard 0, so the uninspected shard is what \
             decides this: Indeterminate, not Free"
        );
    }
}
