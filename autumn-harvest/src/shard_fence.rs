//! A task-local fence on the shards one request may reach (issue #1803).
//!
//! The authorizer hook in the management API resolves the shards a request
//! names, and the policy decides on each one. The handler then resolves the
//! same execution again on its own. A rebalance cutover between the two
//! resolutions can move the run to a shard the policy never saw. The handler
//! would follow the forwarding pointer onto that shard.
//!
//! The fence closes that window. The hook installs the allowed shards around
//! the handler with [`ShardFence::scope`]. Every shard-named checkout in the
//! engine calls [`check`] first. A shard outside the fence fails with
//! [`HarvestError::OutsideShardFence`] before any read or write. The
//! management API maps that error to `503` with a retry hint. The retry
//! resolves the run on its new shard, and the policy decides on that shard.
//!
//! # What the fence does not cover
//!
//! - No fence means no check. Workers, the scheduler, and a management API
//!   with no authorizer install none, so their behaviour is unchanged. A
//!   request whose policy saw `shard: None` gets no fence either, because
//!   its handler reads every shard by design.
//! - Control rows (audit, tokens, gates) live on the default shard. Their
//!   callers reach them through the default pool, which names no shard, so
//!   they never meet the fence.
//! - A spawned task does not inherit a task-local. A handler that spawns a
//!   task captures [`current`] first and runs the task under [`scoped`].
//!   The stream routes do that, so a stream follows no forward the policy
//!   never saw.
//!
//! The fence is not the write-authority fence
//! ([`HarvestError::ShardFenced`]). That one says this process lost a shard.
//! This one says this request may not reach a shard.

use std::future::Future;
use std::sync::{Arc, Mutex};

use crate::error::{HarvestError, HarvestResult};
use crate::types::ShardId;

tokio::task_local! {
    /// The fence of the current request, if one is installed.
    static FENCE: ShardFence;

    /// Collects every shard a checkout names, while [`record`] runs.
    static RECORDER: Arc<Mutex<Vec<ShardId>>>;
}

/// Run `future` and collect every shard a checkout named inside it.
///
/// The authorizer hook resolves a request's execution with the same walks
/// the handler uses. Recording the shards those walks name gives the policy,
/// and then the fence, exactly the set the handler will name. A hop's routed
/// origin and each intermediate forward are in it, so a legitimate request
/// never trips its own fence.
///
/// The recorder does not nest. An inner `record` replaces the outer one for
/// the length of its future.
pub async fn record<F: Future>(future: F) -> (F::Output, Vec<ShardId>) {
    let recorder: Arc<Mutex<Vec<ShardId>>> = Arc::default();
    let output = RECORDER.scope(recorder.clone(), future).await;
    let mut shards = recorder.lock().map_or_else(
        |poisoned| poisoned.into_inner().clone(),
        |guard| guard.clone(),
    );
    shards.sort_unstable_by_key(|s| s.as_i32());
    shards.dedup();
    (output, shards)
}

/// The set of shards one request may reach.
#[derive(Clone, Debug)]
pub struct ShardFence {
    /// Sorted, with no duplicates.
    allowed: Arc<[ShardId]>,
}

impl ShardFence {
    /// A fence that allows exactly `shards`.
    #[must_use]
    pub fn new(shards: impl IntoIterator<Item = ShardId>) -> Self {
        let mut allowed: Vec<ShardId> = shards.into_iter().collect();
        allowed.sort_unstable_by_key(|s| s.as_i32());
        allowed.dedup();
        Self {
            allowed: allowed.into(),
        }
    }

    /// Whether `shard` is inside the fence.
    #[must_use]
    pub fn allows(&self, shard: ShardId) -> bool {
        self.allowed.contains(&shard)
    }

    /// The allowed shards, in ascending order.
    #[must_use]
    pub fn shards(&self) -> &[ShardId] {
        &self.allowed
    }

    /// Run `future` with this fence installed for the current task.
    ///
    /// A fence already installed is replaced for the length of `future`.
    pub async fn scope<F: Future>(self, future: F) -> F::Output {
        FENCE.scope(self, future).await
    }
}

/// Whether the current task has a fence installed.
#[must_use]
pub fn is_active() -> bool {
    FENCE.try_with(|_| ()).is_ok()
}

/// The fence of the current task, if one is installed.
///
/// A handler that spawns a task captures this first. The new task does not
/// inherit a task-local on its own. Run it under [`scoped`].
#[must_use]
pub fn current() -> Option<ShardFence> {
    FENCE.try_with(Clone::clone).ok()
}

/// Run `future` under `fence`, or as is when `fence` is `None`.
pub async fn scoped<F: Future>(fence: Option<ShardFence>, future: F) -> F::Output {
    match fence {
        Some(fence) => fence.scope(future).await,
        None => future.await,
    }
}

/// Refuse `shard` when a fence is installed and does not allow it.
///
/// Inside [`record`], also note `shard` as one the caller named.
///
/// # Errors
///
/// [`HarvestError::OutsideShardFence`] when the fence excludes `shard`. With
/// no fence installed, every shard passes.
pub fn check(shard: ShardId) -> HarvestResult<()> {
    let _ = RECORDER.try_with(|recorder| {
        if let Ok(mut shards) = recorder.lock() {
            shards.push(shard);
        }
    });
    match FENCE.try_with(|fence| fence.allows(shard)) {
        Ok(true) | Err(_) => Ok(()),
        Ok(false) => Err(HarvestError::OutsideShardFence {
            shard_id: shard.as_i32(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_fence_allows_every_shard() {
        assert!(!is_active());
        assert!(check(ShardId::new(0)).is_ok());
        assert!(check(ShardId::new(99)).is_ok());
    }

    #[tokio::test]
    async fn fence_allows_only_its_shards() {
        let fence = ShardFence::new([ShardId::new(7), ShardId::new(0), ShardId::new(7)]);
        assert_eq!(fence.shards(), &[ShardId::new(0), ShardId::new(7)]);
        fence
            .scope(async {
                assert!(is_active());
                assert!(check(ShardId::new(0)).is_ok());
                assert!(check(ShardId::new(7)).is_ok());
                let err = check(ShardId::new(3)).unwrap_err();
                assert!(
                    matches!(err, HarvestError::OutsideShardFence { shard_id: 3 }),
                    "{err:?}"
                );
                assert!(err.to_string().contains("retry the request"));
            })
            .await;
        // The fence ends with its scope.
        assert!(!is_active());
        assert!(check(ShardId::new(3)).is_ok());
    }

    #[tokio::test]
    async fn a_spawned_task_runs_under_the_captured_fence() {
        assert!(current().is_none());
        let fence = ShardFence::new([ShardId::new(2)]);
        let outcome = fence
            .scope(async {
                let captured = current();
                assert!(captured.is_some());
                tokio::spawn(scoped(captured, async {
                    assert!(is_active());
                    check(ShardId::new(5)).is_err()
                }))
                .await
                .unwrap()
            })
            .await;
        assert!(outcome, "the spawned task was fenced");
        // Without a captured fence, a spawned task checks nothing.
        let outcome = tokio::spawn(scoped(None, async { check(ShardId::new(5)).is_ok() }))
            .await
            .unwrap();
        assert!(outcome);
    }

    #[tokio::test]
    async fn record_collects_the_named_shards() {
        let ((), named) = record(async {
            check(ShardId::new(7)).unwrap();
            check(ShardId::new(0)).unwrap();
            check(ShardId::new(7)).unwrap();
        })
        .await;
        assert_eq!(named, vec![ShardId::new(0), ShardId::new(7)]);
        // Outside a recording, a check records nothing.
        let ((), named) = record(async {}).await;
        assert_eq!(named, Vec::<ShardId>::new());
        check(ShardId::new(1)).unwrap();
    }

    #[tokio::test]
    async fn empty_fence_allows_nothing() {
        ShardFence::new([])
            .scope(async {
                assert!(check(ShardId::new(0)).is_err());
            })
            .await;
    }
}
