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
//! That is a **prediction of where new work would be placed**, not an
//! observation of where existing work *is*, and the two diverge in two ways:
//!
//! 1. **Explicit shard placement (issue #697).** A workflow started under
//!    [`crate::shard::ShardPlacement::Shard`] or
//!    [`crate::shard::ShardPlacement::ResidencyKey`] is deliberately pinned to
//!    a shard the pure hash may never compute.
//! 2. **Writable-set drift.** [`ShardRouter::pick_for_new_workflow`] re-hashes
//!    over the *current* `writable_shards` when the readable-set hash lands
//!    outside it, so draining a shard moves where a key resolves *after* a
//!    workflow was already placed there.
//!
//! In both cases the hash names a shard the target does not live on, the
//! delivery attempt finds nothing, and — once the unknown-target grace window
//! elapses — the caller's history durably records `target_unknown` for a
//! target that was running the whole time.
//!
//! # The rule this module implements
//!
//! Resolve by **observation**: fan out across every shard the deployment
//! expects to exist, ask each for its best run under this business key, and
//! merge the answers with the canonical [`select_resolved_run`] ranking
//! (active-run-first, else most-recent terminal). This is the same rule the
//! management API's by-id resolution already uses
//! (`api::resolve_workflow_by_business_id`, issue #805), now available to the
//! engine itself so the two cannot disagree about where a business key lives.
//!
//! Two properties are load-bearing and are what a naive fan-out gets wrong:
//!
//! * **No first-hit short circuit.** A stale terminal run of the same key on
//!   one shard and the live run on another is an ordinary state after a
//!   `(workflow_name, workflow_id)` chain has moved shards (uniqueness is
//!   shard-local). Stopping at the first shard that returns a row would signal
//!   the dead one and report `not_running` while the target is alive, so every
//!   expected shard is asked before a terminal answer is accepted.
//! * **"Could not inspect" is not "not there."** A shard this process has no
//!   pool for, or cannot get a connection to, leaves the answer
//!   [`TargetLocation::Indeterminate`] — never [`TargetLocation::NotFound`].
//!   `NotFound` is what the outbox converts into a permanent `target_unknown`
//!   once the grace window expires, so treating an outage as absence would
//!   turn a transient, retryable condition into a wrong answer written
//!   irreversibly into the caller's append-only history.
//!
//! # Cost, and the connection budget
//!
//! One or two queries per expected shard, per by-id delivery attempt
//! ([`crate::execution::resolve_execution_id_by_workflow_id`] probes
//! active-first and then, only when a shard holds no active run, most-recent
//! terminal). By-id signal/cancel delivery is asynchronous and off the hot
//! dispatch path — it runs in the outbox scanners — which is what makes an
//! O(shards) resolution acceptable here and not, say, on task claim.
//!
//! **Connections are the scarce resource, not queries.** The outbox calls this
//! from inside a transaction on a connection it already holds from its own
//! shard's pool, and Harvest configures no deadpool `Timeouts`, so a bare
//! `pool.get().await` is an *unbounded* wait — a sweep that reached back into
//! its own pool for a second connection parks forever on a one-connection pool
//! and wedges every later resident of that scanner tick (the hazard
//! `codec_rotation.rs` and `audit_export.rs` were both fixed for). Three rules
//! keep that from happening here:
//!
//! * The caller's own shard is probed on the **connection the caller already
//!   holds** ([`resolve_location_by_workflow_id_with`]'s `held` argument), so
//!   the fan-out never re-enters that pool.
//! * `resolve_delivery_route` short-circuits entirely when only one shard is
//!   expected, so a single-shard deployment issues no fan-out at all and is
//!   byte-for-byte what it was before this module existed.
//! * Every remaining acquisition is bounded by
//!   [`crate::audit_export::SHARD_ACQUIRE_BOUND`]; a shard that does not yield
//!   a connection in time becomes an [`UninspectedShard`] — i.e. `Indeterminate`
//!   and a retry — rather than an indefinite park.
//!
//! [`UninspectableShards`] then memoizes, for the length of one sweep, the
//! shards that already failed, so a backlog of N pending rows pays that bound
//! once rather than N times.

use diesel_async::AsyncPgConnection;

use crate::execution::{ResolvedRun, select_resolved_run};
use crate::shard::{ShardRouter, ShardedDbPool};
use crate::types::ShardId;

/// A shard a by-business-key fan-out expected to inspect but could not.
///
/// Its presence in a [`TargetLocation::Indeterminate`] is the reason the
/// resolution is being reported as inconclusive rather than as an answer.
///
/// `#[non_exhaustive]` deliberately: `reason` is prose for an operator log line
/// today, and the obvious next addition is a machine-readable discriminant
/// (no-pool vs. acquisition-timeout vs. query-error) that callers can act on
/// differently. Adding it must not be a breaking change.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UninspectedShard {
    /// The shard that could not be queried.
    pub shard: ShardId,
    /// Why it could not be queried (no configured pool, connection failure,
    /// query failure), for the operator-facing log line.
    pub reason: String,
}

/// Where a `(workflow_name, workflow_id)`-addressed target actually lives.
///
/// Named *location*, not *placement*, and the distinction is the whole point of
/// this module: [`crate::shard::ShardPlacement`] is a policy for where **new**
/// work should go, decided before an execution exists. This is an observation
/// of where an **existing** run is. Issue #1146 is precisely what happens when
/// the first is used to answer the second.
///
/// `#[non_exhaustive]`: a fourth outcome is one bug report away — an
/// "ambiguous, several live runs across shards" verdict is the obvious
/// candidate, since `(workflow_name, workflow_id)` uniqueness is shard-local
/// and today's rule silently takes the most recently started of them.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TargetLocation {
    /// The key resolves to `run`, which lives on `shard`.
    ///
    /// `run` may be terminal — the caller applies its own semantics to that
    /// (issue #751: a terminal current run fails a signal `not_running` and
    /// satisfies a cancel as a no-op success).
    Found {
        /// The shard whose database holds `run`.
        shard: ShardId,
        /// The winning run under the [`select_resolved_run`] ranking.
        run: ResolvedRun,
        /// Shards that could not be inspected, if any. **Non-empty means this
        /// answer is a live run found over a partial view**, and the caller must
        /// decide whether its operation may act on that (issue #1146, Codex
        /// round 2).
        ///
        /// A **signal** may. Delivering to a live run in hand is real delivery,
        /// and re-delivery is not idempotent without an idempotency key — so
        /// "deliver but stay pending" would duplicate the signal on the retry.
        ///
        /// A **cancel** may act but must not *report*. `ExternalCancelDelivered`
        /// asserts something about the whole business key — "nothing is running
        /// under it" — and because `(workflow_name, workflow_id)` uniqueness is
        /// shard-local, the shard that could not be read may hold another live
        /// run. Cancelling is idempotent, so the cancel path cancels what it
        /// found and leaves the request pending, letting a later **complete**
        /// fan-out be what makes the assertion.
        uninspected: Vec<UninspectedShard>,
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
}

impl TargetLocation {
    /// The shard holding the resolved run, if the resolution found one.
    #[must_use]
    pub const fn found_shard(&self) -> Option<ShardId> {
        match self {
            Self::Found { shard, .. } => Some(*shard),
            _ => None,
        }
    }

    /// Did the fan-out behind this answer inspect **every** expected shard?
    ///
    /// `false` on a [`Self::Found`] reached over a partial view, and on every
    /// [`Self::Indeterminate`]. A caller whose success event asserts something
    /// about the business key as a whole must not record it while this is
    /// `false` — see [`Self::Found`]'s `uninspected`.
    #[must_use]
    pub const fn fanout_was_complete(&self) -> bool {
        match self {
            Self::Found { uninspected, .. } => uninspected.is_empty(),
            Self::NotFound => true,
            Self::Indeterminate { .. } => false,
        }
    }
}

/// The shards a by-business-key resolution must inspect.
///
/// Every shard this process has a pool for, unioned with every shard the router
/// knows about (`readable_shards` plus `default_shard`), ascending and
/// deduplicated.
///
/// The union — rather than just the local pools — is what makes
/// [`TargetLocation::Indeterminate`] meaningful. Mid a shard-add rollout the
/// router's `readable_shards` is widened before every process has the new
/// shard's pool wired up (see the "add a shard" procedure in
/// `docs/sharding.md`); a resolution that silently omitted such a shard from
/// its fan-out would report a confident `NotFound` for a key that lives there.
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
/// Same rule, same result. Exists for callers that hold a `&ShardRouter` behind
/// a guard or a `Result` they cannot keep borrowed across the call, so that
/// reaching the canonical rule does not cost a `ShardRouter` clone —
/// `ShardRouter` owns a `residency_map`, so cloning it per management-API
/// request to answer a set question is pure waste.
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
/// Ranking is delegated to [`select_resolved_run`] — the same function the
/// management API's by-id resolution uses — so the engine and the HTTP surface
/// cannot drift about which run of a business key is "the current one".
///
/// The completeness rules on top of that ranking:
///
/// * A **live (non-terminal) winner is delivered to even when a shard was
///   missed.** Note what this does *not* rest on: `(workflow_name,
///   workflow_id)` uniqueness is shard-local, so two live runs of one key are
///   possible in exactly the topologies this module exists for (pin key K to
///   shard 2 while an unpinned start of K hashes to shard 0 — shard 0's partial
///   unique index cannot see shard 2). The rule is therefore "deliver to the
///   most recently started live run that was found", and a shard that could not
///   be inspected may hold a newer one. That is deliberate and conservative in
///   the direction that matters: a live run in hand is a real target, and
///   withholding delivery from it because an *unrelated* shard is unreachable
///   would strand a signal whose recipient is right there. The incomplete
///   fan-out is logged by [`resolve_location_by_workflow_id_with`] so the
///   ambiguity is visible rather than silent.
/// * A **terminal winner with a shard missed is `Indeterminate`.** A terminal
///   run on an inspected shard does not rule out a live run on the shard that
///   was not inspected, and the two lead to opposite outcomes for a signal
///   (`not_running` failure vs. delivery).
/// * **No candidates at all with a shard missed is `Indeterminate`**, never
///   `NotFound`.
#[must_use]
pub fn merge_locations(
    candidates: Vec<(ShardId, ResolvedRun)>,
    uninspected: Vec<UninspectedShard>,
) -> TargetLocation {
    let runs: Vec<ResolvedRun> = candidates.iter().map(|(_, run)| run.clone()).collect();
    let Some(winner) = select_resolved_run(runs) else {
        return if uninspected.is_empty() {
            TargetLocation::NotFound
        } else {
            TargetLocation::Indeterminate { uninspected }
        };
    };

    if !uninspected.is_empty() && crate::erase::is_terminal_state(&winner.state) {
        return TargetLocation::Indeterminate { uninspected };
    }

    // `select_resolved_run` returns one of the candidates verbatim, so the
    // owning shard is the one that contributed it. Match on `exec_id`, which is
    // unique across every shard (it is a UUID primary key), rather than on the
    // whole struct. The fallback is unreachable while `select_resolved_run`
    // returns one of its inputs; decoding the id's own shard bits is the
    // closest correct answer if that ever changes.
    let shard = candidates
        .into_iter()
        .find(|(_, run)| run.exec_id == winner.exec_id)
        .map_or_else(|| winner.exec_id.shard(), |(shard, _)| shard);
    TargetLocation::Found {
        shard,
        run: winner,
        uninspected,
    }
}

/// How long a fan-out will wait for a **peer** shard's connection before giving
/// up on that shard for this attempt (issue #1146, Codex round 1 P1).
///
/// Deliberately far shorter than [`crate::audit_export::SHARD_ACQUIRE_BOUND`],
/// and for a different reason. That bound exists to stop an *indefinite* park
/// and is generous on purpose, so a merely-busy shard is not starved. This one
/// exists to stop a **circular wait between per-shard scanners in one process**,
/// where being generous is the problem rather than the cure.
///
/// The cycle: `Worker` spawns one timeout checker per assigned shard, and each
/// holds its own shard pool's connection for the whole `enforce_timeouts_once`
/// pass. With `shard_assignments = [0, 1]` and one connection per pool, checker
/// 0 can sit waiting for pool 1 while checker 1 waits for pool 0. Nothing is
/// deadlocked — both bounds expire — but with a generous bound both scanners
/// stall for it on every tick, delaying every other scanner resident, and by-id
/// delivery makes no progress. Failing fast means neither ever *waits* on the
/// other, so the cycle cannot form: a peer whose only connection is busy right
/// now is simply uninspected, the row is retried, and the next tick — whose
/// phase has drifted — finds the pool free.
///
/// **The deterministic answer is capacity, not timing.** A process that runs
/// checkers for more than one shard should size each shard pool at **2 or
/// more**: one connection for that shard's own scanner, one for a peer's
/// cross-shard read. `docs/sharding.md` says so. This bound is what keeps a
/// deployment that has not done so degraded rather than stalled.
///
/// Note the same circular shape applies to the *delivery* acquisition in the
/// outbox scanners, which predates this issue (it existed for cross-shard
/// `ExecutionId` targets) and was unbounded; `timeout.rs` now bounds it with
/// this constant too.
pub const FANOUT_ACQUIRE_BOUND: std::time::Duration = std::time::Duration::from_millis(250);

/// Total budget for one **peer** shard probe — acquisition *and* query
/// (issue #1146, Codex round 2 P1).
///
/// [`FANOUT_ACQUIRE_BOUND`] bounds only the checkout. A connection that is
/// handed over and then never answers is just as fatal: the peer database
/// becomes a network black hole after checkout, or an `ACCESS EXCLUSIVE` DDL
/// lock blocks the read, and the `await` on the query hangs forever. That
/// wedges the *caller* shard's timeout checker, which also runs task timeouts,
/// SLA enforcement, session reclaim and every other outbox — so one unhealthy
/// peer halts unrelated enforcement on a healthy shard indefinitely.
///
/// The whole peer probe is therefore wrapped in this bound, with the tighter
/// acquisition bound nested inside it. Two bounds rather than one because they
/// answer different questions: the inner one keeps per-shard scanners from
/// waiting on each other's connections at all (a circular wait), while this one
/// caps what a single unhealthy peer can cost. Expiry is classified exactly like
/// a failed acquisition — an [`UninspectedShard`], hence `Indeterminate` and a
/// retry, never "the key is not there".
///
/// Generous relative to a healthy probe (an indexed lookup on
/// `(workflow_name, workflow_id)`) so ordinary load never trips it, and small
/// enough that N shards cannot dominate a scanner tick — the per-sweep
/// [`UninspectableShards`] memo caps the cost at one bound per shard per sweep
/// rather than per row.
pub const FANOUT_PEER_BOUND: std::time::Duration = std::time::Duration::from_secs(2);

/// Shards a sweep has already found uninspectable, memoized for the length of
/// that sweep (issue #1146).
///
/// An outbox sweep processes one pending row per step and may see hundreds in a
/// backlog. A shard that failed to hand over a connection on the first row has
/// not recovered by the two-hundredth, and each re-probe costs the full
/// [`crate::audit_export::SHARD_ACQUIRE_BOUND`] — so without this, one
/// unreachable shard turns a sweep into `rows × bound` of dead wall-clock,
/// starving every scanner resident sequenced after the outbox.
///
/// Memoizing can only push an answer toward [`TargetLocation::Indeterminate`],
/// which is the safe direction: a shard recorded here is reported uninspected,
/// so the row is retried rather than resolved from a partial view. The memo is
/// per-sweep, never process-wide, so a shard that comes back is re-probed on the
/// very next sweep.
#[derive(Clone, Debug, Default)]
pub struct UninspectableShards {
    shards: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<ShardId, String>>>,
}

impl UninspectableShards {
    /// A fresh, empty memo for one sweep.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The recorded reason `shard` could not be inspected earlier in this sweep.
    #[must_use]
    pub fn recorded(&self, shard: ShardId) -> Option<String> {
        self.shards
            .lock()
            .ok()
            .and_then(|guard| guard.get(&shard).cloned())
    }

    /// Record that `shard` could not be inspected. The first reason wins, so the
    /// log names the original failure rather than a later re-derivation of it.
    pub fn record(&self, shard: ShardId, reason: String) {
        if let Ok(mut guard) = self.shards.lock() {
            guard.entry(shard).or_insert(reason);
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
    resolve_location_by_workflow_id_with(pool, router, workflow_name, workflow_id, None, None).await
}

/// Resolve where `(workflow_name, workflow_id)` actually lives, reusing a
/// connection the caller already holds and memoizing unreachable shards
/// (issue #1146).
///
/// `router` is the topology snapshot used to widen the fan-out beyond this
/// process's own pools; pass `None` when no router is installed (tests,
/// embedders that never call [`crate::shard::install_global_router`]), in which
/// case only the configured pools are inspected.
///
/// `held` names a shard the caller is **already connected to**, with that
/// connection. That shard is probed on it instead of acquiring a second
/// connection from its pool — which is not an optimisation but the fix for a
/// self-deadlock: the outbox calls this from inside a transaction on a
/// connection checked out of that very pool, and `pool.get()` is an unbounded
/// wait. Reading through the held connection is equivalent: the transaction is
/// READ COMMITTED, so each statement takes a fresh snapshot and sees exactly
/// what a separate connection would.
///
/// `memo` carries the shards this sweep has already failed to reach, so a
/// backlog pays [`crate::audit_export::SHARD_ACQUIRE_BOUND`] once per shard
/// rather than once per row.
///
/// The fan-out is **sequential**, and holds at most one connection beyond the
/// caller's own at a time. Read-only: no row is locked and nothing is written.
pub async fn resolve_location_by_workflow_id_with(
    pool: &ShardedDbPool,
    router: Option<&ShardRouter>,
    workflow_name: &str,
    workflow_id: &str,
    mut held: Option<(ShardId, &mut AsyncPgConnection)>,
    memo: Option<&UninspectableShards>,
) -> TargetLocation {
    let expected = fanout_shards(&pool.shard_ids(), router);
    let mut candidates: Vec<(ShardId, ResolvedRun)> = Vec::new();
    let mut uninspected: Vec<UninspectedShard> = Vec::new();

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
            record_shard_answer(
                &mut candidates,
                &mut uninspected,
                shard,
                crate::execution::resolve_execution_id_by_workflow_id(
                    conn,
                    workflow_name,
                    workflow_id,
                )
                .await,
            );
            continue;
        }

        // 2. Already known bad this sweep: report it without paying the bound
        //    a second time.
        if let Some(reason) = memo.and_then(|m| m.recorded(shard)) {
            uninspected.push(UninspectedShard { shard, reason });
            continue;
        }

        let Some(shard_pool) = pool.exact_pool_for(shard) else {
            let reason = "no storage pool configured in this process".to_string();
            if let Some(memo) = memo {
                memo.record(shard, reason.clone());
            }
            uninspected.push(UninspectedShard { shard, reason });
            continue;
        };

        // 3. The whole peer probe — acquisition AND query — under one budget,
        //    with the tighter acquisition bound nested inside it. See
        //    `FANOUT_ACQUIRE_BOUND` (circular wait) and `FANOUT_PEER_BOUND`
        //    (an unhealthy peer wedging this shard's scanner).
        let probe = tokio::time::timeout(FANOUT_PEER_BOUND, async {
            let conn = tokio::time::timeout(FANOUT_ACQUIRE_BOUND, shard_pool.get()).await;
            let mut conn = match conn {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => {
                    return Err(format!("could not acquire a connection: {e}"));
                }
                Err(_elapsed) => {
                    return Err(format!(
                        "no connection available within {FANOUT_ACQUIRE_BOUND:?} (pool busy \
                         or unreachable)"
                    ));
                }
            };
            Ok(crate::execution::resolve_execution_id_by_workflow_id(
                &mut conn,
                workflow_name,
                workflow_id,
            )
            .await)
        })
        .await;

        match probe {
            Ok(Ok(answer)) => {
                record_shard_answer(&mut candidates, &mut uninspected, shard, answer);
            }
            Ok(Err(reason)) => {
                if let Some(memo) = memo {
                    memo.record(shard, reason.clone());
                }
                uninspected.push(UninspectedShard { shard, reason });
            }
            Err(_elapsed) => {
                // A connection was (or was not) handed over and the probe never
                // returned. Classified exactly like a failed acquisition:
                // uninspected, never "absent".
                let reason = format!("shard did not answer within {FANOUT_PEER_BOUND:?}");
                if let Some(memo) = memo {
                    memo.record(shard, reason.clone());
                }
                uninspected.push(UninspectedShard { shard, reason });
            }
        }
    }

    let placement = merge_locations(candidates, uninspected.clone());

    // A `Found` reached over an incomplete fan-out is an answer with a caveat:
    // shard-local uniqueness means the shard we could not read might hold a
    // newer live run of the same key. Delivering to the run in hand is the right
    // call (see `merge_locations`), but the ambiguity must not be silent.
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

/// Fold one shard's answer into the fan-out accumulators.
fn record_shard_answer(
    candidates: &mut Vec<(ShardId, ResolvedRun)>,
    uninspected: &mut Vec<UninspectedShard>,
    shard: ShardId,
    answer: crate::error::HarvestResult<Option<ResolvedRun>>,
) {
    match answer {
        Ok(Some(run)) => candidates.push((shard, run)),
        Ok(None) => {}
        // A shard that answered with an error has NOT told us the key is
        // absent, so it counts as uninspected, never as "not there".
        Err(e) => uninspected.push(UninspectedShard {
            shard,
            reason: format!("resolution query failed: {e}"),
        }),
    }
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
/// business key against the caller's shard alone, and
/// `(workflow_name, workflow_id)` uniqueness is *shard-local* — so in a
/// multi-shard deployment the caller's shard can hold a stale **terminal** run
/// of the key while the live run sits elsewhere. Delivering inline from that
/// view records `ExternalSignalFailed { reason_code: "not_running" }` against a
/// target that is running, permanently, in the caller's append-only history.
/// The previous rule (compare the caller's shard against the rendezvous hash of
/// the key) did not prevent this: the hash lands on the caller's shard for
/// 1-in-N keys regardless of where the target was actually placed.
///
/// One shard means the caller's view *is* the whole deployment, so the
/// hazard cannot arise and single-shard deployments — every pre-sharding
/// deployment, and the default — keep inline delivery and are byte-for-byte
/// unchanged. Multi-shard deployments defer to the outbox, which is one sweep
/// of latency on an already-asynchronous path and is where they mostly went
/// already: the pre-#1146 rule sent every key whose hash missed the caller's
/// shard — `(N-1)/N` of them — down exactly this path.
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
/// **inline**, inside the caller's own decision transaction, against the
/// caller's own shard. It may only when there is exactly one shard, because a
/// single shard is trivially the shard the target lives on; with two or more,
/// the caller's shard-local view can hold a stale terminal run of the same key
/// while the live run sits on another shard, and inline delivery has no way to
/// see that. Multi-shard deployments therefore defer every by-id delivery to
/// the outbox, where the full [`resolve_location_by_workflow_id`] fan-out
/// runs.
///
/// Derived from the same expected-shard set the fan-out itself uses, so the
/// two cannot disagree about what "single shard" means.
#[must_use]
pub(crate) fn deployment_is_multi_shard() -> bool {
    let pool_shards = crate::shard::GLOBAL_SHARDED_POOL
        .read()
        .ok()
        .and_then(|guard| guard.as_ref().map(ShardedDbPool::shard_ids))
        .unwrap_or_default();
    fanout_shards(&pool_shards, global_router_snapshot().as_ref()).len() > 1
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
        UninspectedShard {
            shard: ShardId::new(shard),
            reason: "test".to_string(),
        }
    }

    #[test]
    fn a_complete_fanout_that_finds_nothing_is_not_found() {
        assert_eq!(
            merge_locations(Vec::new(), Vec::new()),
            TargetLocation::NotFound
        );
    }

    #[test]
    fn an_incomplete_fanout_that_finds_nothing_is_indeterminate() {
        assert_eq!(
            merge_locations(Vec::new(), vec![uninspected(2)]),
            TargetLocation::Indeterminate {
                uninspected: vec![uninspected(2)]
            }
        );
    }

    #[test]
    fn a_terminal_winner_with_an_uninspected_shard_is_indeterminate() {
        let terminal = run(0, "COMPLETED", 1);
        assert_eq!(
            merge_locations(vec![(ShardId::new(0), terminal)], vec![uninspected(1)]),
            TargetLocation::Indeterminate {
                uninspected: vec![uninspected(1)]
            }
        );
    }

    #[test]
    fn a_found_answer_reports_whether_its_fanout_was_complete() {
        let live = run(0, "RUNNING", 1);
        assert!(
            merge_locations(vec![(ShardId::new(0), live.clone())], Vec::new())
                .fanout_was_complete(),
            "every shard answered"
        );
        assert!(
            !merge_locations(vec![(ShardId::new(0), live)], vec![uninspected(1)])
                .fanout_was_complete(),
            "a live run found over a PARTIAL view — the un-inspected shard may hold \
             another live run of the same key, since uniqueness is shard-local. A \
             cancel must not report success on this; a signal may still deliver."
        );
        assert!(
            merge_locations(Vec::new(), Vec::new()).fanout_was_complete(),
            "`NotFound` is only ever reached from a complete fan-out"
        );
        assert!(
            !merge_locations(Vec::new(), vec![uninspected(1)]).fanout_was_complete(),
            "`Indeterminate` is never complete"
        );
    }

    #[test]
    fn a_live_winner_settles_the_question_despite_an_uninspected_shard() {
        let live = run(0, "RUNNING", 1);
        assert_eq!(
            merge_locations(vec![(ShardId::new(0), live.clone())], vec![uninspected(1)]),
            TargetLocation::Found {
                shard: ShardId::new(0),
                run: live,
                uninspected: vec![uninspected(1)]
            }
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
                Vec::new()
            ),
            TargetLocation::Found {
                shard: ShardId::new(1),
                run: live,
                uninspected: Vec::new()
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
            merge_locations(vec![(ShardId::new(3), unencoded)], Vec::new()).found_shard(),
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
}
