//! The [`TaskQueueAdapter`] trait and the value types that flow through it.
//!
//! The trait captures the operations that any backend-agnostic Harvest worker
//! performs on a task queue. A Postgres implementation in the `autumn-harvest`
//! core crate needs a refactor of the function-shaped API that `worker.rs`
//! uses today. So the trait has one production implementation:
//! [`crate::RedisTaskQueue`]. The crate-level `README` documents worker
//! integration, which is the next step.
//!
//! Returning [`ClaimedTask`] instead of a bare `TaskEnvelope` keeps the
//! ack-handle (Redis stream entry id, in our case) opaque to the worker. The
//! worker only needs to know how to call `complete`, `fail`, or
//! `requeue_for_retry` with the same `ClaimedTask` it was handed.

use std::time::Duration;

use uuid::Uuid;

use crate::envelope::{EnqueueParams, TaskEnvelope};
use crate::error::RedisAdapterResult;

/// A task returned by [`TaskQueueAdapter::claim`].
///
/// The `entry_id` is opaque to the caller. It identifies the stream entry (or
/// row, in a SQL backend), so the adapter can later XACK or XCLAIM that entry
/// on `complete`/`fail`/`record_heartbeat`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedTask {
    /// Backend-specific entry handle. For Redis Streams this is the
    /// `<ms>-<seq>` ID returned by XREADGROUP.
    pub entry_id: String,
    /// The deserialized task envelope.
    pub envelope: TaskEnvelope,
}

impl ClaimedTask {
    /// Stable producer-assigned UUID for this task.
    #[must_use]
    pub const fn task_id(&self) -> Uuid {
        self.envelope.task_id
    }

    /// Logical queue name this task was claimed from.
    #[must_use]
    pub fn queue_name(&self) -> &str {
        &self.envelope.queue_name
    }
}

/// The future that a [`TaskQueueAdapter`] method returns.
pub type AdapterFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Backend-agnostic task queue interface.
///
/// Implementors must provide at-least-once delivery. A claimed task must
/// become claimable again if its visibility timeout expires before
/// [`Self::complete`], [`Self::fail`] or [`Self::requeue_for_retry`] acknowledges it.
///
/// Implement it with `#[async_trait]` on the `impl` block and `async fn`
/// methods. The method signatures here are the ones that `#[async_trait]`
/// generates. They are written out because `#[async_trait]` on the trait adds
/// a `#[must_use]` that clippy rejects as `double_must_use`.
pub trait TaskQueueAdapter: Send + Sync {
    /// Add a new task to the queue.
    ///
    /// Returns the producer-assigned `task_id`. A task with a future
    /// `params.scheduled_at` waits in a delayed set until it is due, and only
    /// then goes on the claimable stream.
    fn enqueue<'life0, 'async_trait>(
        &'life0 self,
        params: EnqueueParams,
    ) -> AdapterFuture<'async_trait, RedisAdapterResult<Uuid>>
    where
        'life0: 'async_trait,
        Self: 'async_trait;

    /// Claim the next available task from any of `queues`.
    ///
    /// Returns `Ok(None)` when no task is currently due. Implementations are
    /// expected to give the caller exclusive ownership of the returned
    /// envelope until the visibility timeout elapses or the caller
    /// acknowledges it.
    fn claim<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        queues: &'life1 [String],
        worker_id: &'life2 str,
    ) -> AdapterFuture<'async_trait, RedisAdapterResult<Option<ClaimedTask>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait;

    /// Acknowledge successful processing.
    fn complete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        task: &'life1 ClaimedTask,
        output: serde_json::Value,
    ) -> AdapterFuture<'async_trait, RedisAdapterResult<()>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait;

    /// Acknowledge a failure that does not warrant a retry.
    fn fail<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        task: &'life1 ClaimedTask,
        error: &'life2 str,
    ) -> AdapterFuture<'async_trait, RedisAdapterResult<()>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait;

    /// Acknowledge the current attempt and requeue with a delay so the next
    /// attempt fires no earlier than `now + delay`.
    fn requeue_for_retry<'life0, 'life1, 'async_trait>(
        &'life0 self,
        task: &'life1 ClaimedTask,
        delay: Duration,
    ) -> AdapterFuture<'async_trait, RedisAdapterResult<()>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait;

    /// Refresh the visibility timeout on a task that is still being worked on.
    ///
    /// Equivalent to the heartbeat path on the Postgres adapter. It tells the
    /// queue that the worker is still alive, so the queue must not reclaim
    /// this task yet.
    fn record_heartbeat<'life0, 'life1, 'async_trait>(
        &'life0 self,
        task: &'life1 ClaimedTask,
    ) -> AdapterFuture<'async_trait, RedisAdapterResult<()>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait;

    /// Return the depth (claimable + pending) of each queue.
    fn queue_depths<'life0, 'life1, 'async_trait>(
        &'life0 self,
        queues: &'life1 [String],
    ) -> AdapterFuture<'async_trait, RedisAdapterResult<Vec<(String, i64)>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::TaskType;

    #[test]
    fn claimed_task_exposes_task_id_and_queue_name() {
        let env = TaskEnvelope::from_params(
            EnqueueParams::new("default", TaskType::Workflow, serde_json::Value::Null),
            Uuid::nil(),
        )
        .unwrap();
        let claimed = ClaimedTask {
            entry_id: "1-0".into(),
            envelope: env,
        };
        assert_eq!(claimed.task_id(), Uuid::nil());
        assert_eq!(claimed.queue_name(), "default");
    }
}
