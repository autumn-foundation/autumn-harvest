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
//!   [`TargetPlacement::Indeterminate`] — never [`TargetPlacement::NotFound`].
//!   `NotFound` is what the outbox converts into a permanent `target_unknown`
//!   once the grace window expires, so treating an outage as absence would
//!   turn a transient, retryable condition into a wrong answer written
//!   irreversibly into the caller's append-only history.
//!
//! # Cost
//!
//! One query per expected shard, per by-id delivery attempt. A single-shard
//! deployment expects exactly one shard, so it makes exactly the one query it
//! already made and is unchanged. By-id signal/cancel delivery is asynchronous
//! and off the hot dispatch path (it runs in the outbox scanners), which is
//! what makes an O(shards) resolution acceptable here and not, say, on task
//! claim.

use crate::execution::{ResolvedRun, select_resolved_run};
use crate::shard::{ShardRouter, ShardedDbPool};
use crate::types::ShardId;

/// A shard a by-business-key fan-out expected to inspect but could not.
///
/// Its presence in a [`TargetPlacement::Indeterminate`] is the reason the
/// resolution is being reported as inconclusive rather than as an answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UninspectedShard {
    /// The shard that could not be queried.
    pub shard: ShardId,
    /// Why it could not be queried (no configured pool, connection failure,
    /// query failure), for the operator-facing log line.
    pub reason: String,
}

/// Where a `(workflow_name, workflow_id)`-addressed target actually lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetPlacement {
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

impl TargetPlacement {
    /// The shard holding the resolved run, if the resolution found one.
    #[must_use]
    pub const fn found_shard(&self) -> Option<ShardId> {
        match self {
            Self::Found { shard, .. } => Some(*shard),
            _ => None,
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
/// [`TargetPlacement::Indeterminate`] meaningful. Mid a shard-add rollout the
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
    let mut shards: std::collections::BTreeSet<ShardId> = pool_shards.iter().copied().collect();
    if let Some(router) = router {
        shards.extend(router.readable_shards().iter().copied());
        shards.insert(router.default_shard());
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
/// * A **live (non-terminal) winner is authoritative even when a shard was
///   missed.** At most one run per business key is active, so a live run found
///   *is* the target; refusing to deliver to it because an unrelated shard is
///   down would be strictly worse than delivering.
/// * A **terminal winner with a shard missed is `Indeterminate`.** A terminal
///   run on an inspected shard does not rule out a live run on the shard that
///   was not inspected, and the two lead to opposite outcomes for a signal
///   (`not_running` failure vs. delivery).
/// * **No candidates at all with a shard missed is `Indeterminate`**, never
///   `NotFound`.
#[must_use]
pub fn merge_placement(
    candidates: Vec<(ShardId, ResolvedRun)>,
    uninspected: Vec<UninspectedShard>,
) -> TargetPlacement {
    let runs: Vec<ResolvedRun> = candidates.iter().map(|(_, run)| run.clone()).collect();
    let Some(winner) = select_resolved_run(runs) else {
        return if uninspected.is_empty() {
            TargetPlacement::NotFound
        } else {
            TargetPlacement::Indeterminate { uninspected }
        };
    };

    if !uninspected.is_empty() && crate::erase::is_terminal_state(&winner.state) {
        return TargetPlacement::Indeterminate { uninspected };
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
    TargetPlacement::Found { shard, run: winner }
}

/// Resolve where `(workflow_name, workflow_id)` actually lives, by inspecting
/// every expected shard (issue #1146).
///
/// `router` is the topology snapshot used to widen the fan-out beyond this
/// process's own pools; pass `None` when no router is installed (tests,
/// embedders that never call [`crate::shard::install_global_router`]), in which
/// case only the configured pools are inspected.
///
/// The fan-out is **sequential** — one connection held at a time — so it is
/// safe against a pool sized down to a single connection, matching
/// `api::resolve_workflow_by_business_id`'s established shape. Read-only: no
/// row is locked and nothing is written.
pub async fn resolve_placement_by_workflow_id(
    pool: &ShardedDbPool,
    router: Option<&ShardRouter>,
    workflow_name: &str,
    workflow_id: &str,
) -> TargetPlacement {
    let expected = fanout_shards(&pool.shard_ids(), router);
    let mut candidates: Vec<(ShardId, ResolvedRun)> = Vec::new();
    let mut uninspected: Vec<UninspectedShard> = Vec::new();

    for shard in expected {
        let Some(shard_pool) = pool.exact_pool_for(shard) else {
            uninspected.push(UninspectedShard {
                shard,
                reason: "no storage pool configured in this process".to_string(),
            });
            continue;
        };
        let mut conn = match shard_pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                uninspected.push(UninspectedShard {
                    shard,
                    reason: format!("could not acquire a connection: {e}"),
                });
                continue;
            }
        };
        match crate::execution::resolve_execution_id_by_workflow_id(
            &mut conn,
            workflow_name,
            workflow_id,
        )
        .await
        {
            Ok(Some(run)) => candidates.push((shard, run)),
            Ok(None) => {}
            Err(e) => uninspected.push(UninspectedShard {
                shard,
                reason: format!("resolution query failed: {e}"),
            }),
        }
    }

    merge_placement(candidates, uninspected)
}

/// May a delivery to `target` be attempted **inline** (issue #1146)?
///
/// "Inline" means inside the caller's own decision transaction, on the caller's
/// own shard connection, rather than left to the cross-shard outbox.
///
/// * An [`ExternalTarget::ExecutionId`] may, exactly when its encoded shard is
///   the caller's. Unchanged from issue #492/#751: the id is authoritative, so
///   this comparison can never be wrong.
/// * An [`ExternalTarget::WorkflowId`] may only in a **single-shard**
///   deployment.
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
pub fn inline_delivery_allowed(
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
pub fn global_router_snapshot() -> Option<ShardRouter> {
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
/// the outbox, where the full [`resolve_placement_by_workflow_id`] fan-out
/// runs.
///
/// Derived from the same expected-shard set the fan-out itself uses, so the
/// two cannot disagree about what "single shard" means.
#[must_use]
pub fn deployment_is_multi_shard() -> bool {
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
            merge_placement(Vec::new(), Vec::new()),
            TargetPlacement::NotFound
        );
    }

    #[test]
    fn an_incomplete_fanout_that_finds_nothing_is_indeterminate() {
        assert_eq!(
            merge_placement(Vec::new(), vec![uninspected(2)]),
            TargetPlacement::Indeterminate {
                uninspected: vec![uninspected(2)]
            }
        );
    }

    #[test]
    fn a_terminal_winner_with_an_uninspected_shard_is_indeterminate() {
        let terminal = run(0, "COMPLETED", 1);
        assert_eq!(
            merge_placement(vec![(ShardId::new(0), terminal)], vec![uninspected(1)]),
            TargetPlacement::Indeterminate {
                uninspected: vec![uninspected(1)]
            }
        );
    }

    #[test]
    fn a_live_winner_settles_the_question_despite_an_uninspected_shard() {
        let live = run(0, "RUNNING", 1);
        assert_eq!(
            merge_placement(vec![(ShardId::new(0), live.clone())], vec![uninspected(1)]),
            TargetPlacement::Found {
                shard: ShardId::new(0),
                run: live
            }
        );
    }

    #[test]
    fn the_live_run_wins_over_a_more_recent_terminal_on_another_shard() {
        let live = run(1, "RUNNING", 1);
        let recent_terminal = run(0, "COMPLETED", 99);
        assert_eq!(
            merge_placement(
                vec![
                    (ShardId::new(0), recent_terminal),
                    (ShardId::new(1), live.clone()),
                ],
                Vec::new()
            ),
            TargetPlacement::Found {
                shard: ShardId::new(1),
                run: live
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
            merge_placement(vec![(ShardId::new(3), unencoded)], Vec::new()).found_shard(),
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
