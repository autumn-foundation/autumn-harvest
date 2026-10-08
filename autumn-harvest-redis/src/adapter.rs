//! The [`TaskQueueAdapter`] trait and the value types that flow through it.
//!
//! The trait captures the operations that any backend-agnostic Harvest worker
//! performs on a task queue. The Postgres-backed implementation in the
//! `autumn-harvest` core crate is a non-trivial refactor away from the
//! function-shaped API used by `worker.rs` today, so the trait currently has
//! one production implementation: [`crate::RedisTaskQueue`]. Worker integration
//! is documented in the crate-level `README` and is intended as the next step
//! after this scaffolding lands.
//!
//! Returning [`ClaimedTask`] instead of a bare `TaskEnvelope` keeps the
//! ack-handle (Redis stream entry id, in our case) opaque to the worker. The
//! worker only needs to know how to call `complete`, `fail`, or
//! `requeue_for_retry` with the same `ClaimedTask` it was handed.

use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use crate::envelope::{EnqueueParams, TaskEnvelope};
use crate::error::RedisAdapterResult;

/// A task returned by [`TaskQueueAdapter::claim`].
///
/// The `entry_id` is opaque to the caller -- it identifies the underlying
/// stream entry (or row, in a SQL backend) so the adapter can later XACK or
/// XCLAIM the same entry on `complete`/`fail`/`record_heartbeat`.
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

/// Backend-agnostic task queue interface.
///
/// Implementors must provide at-least-once delivery: a task that has been
/// claimed but not subsequently acknowledged via [`Self::complete`],
/// [`Self::fail`], or [`Self::requeue_for_retry`] before its visibility
/// timeout expires must become claimable again.
#[async_trait]
pub trait TaskQueueAdapter: Send + Sync {
    /// Add a new task to the queue.
    ///
    /// Returns the producer-assigned `task_id`. If `params.scheduled_at` is in
    /// the future, the task is parked in a delayed set until it becomes due
    /// rather than appearing immediately on the claimable stream.
    async fn enqueue(&self, params: EnqueueParams) -> RedisAdapterResult<Uuid>;

    /// Claim the next available task from any of `queues`.
    ///
    /// Returns `Ok(None)` when no task is currently due. Implementations are
    /// expected to give the caller exclusive ownership of the returned
    /// envelope until the visibility timeout elapses or the caller
    /// acknowledges it.
    async fn claim(
        &self,
        queues: &[String],
        worker_id: &str,
    ) -> RedisAdapterResult<Option<ClaimedTask>>;

    /// Acknowledge successful processing.
    async fn complete(
        &self,
        task: &ClaimedTask,
        output: serde_json::Value,
    ) -> RedisAdapterResult<()>;

    /// Acknowledge a failure that does not warrant a retry.
    async fn fail(&self, task: &ClaimedTask, error: &str) -> RedisAdapterResult<()>;

    /// Acknowledge the current attempt and requeue with a delay so the next
    /// attempt fires no earlier than `now + delay`.
    async fn requeue_for_retry(
        &self,
        task: &ClaimedTask,
        delay: Duration,
    ) -> RedisAdapterResult<()>;

    /// Refresh the visibility timeout on a task that is still being worked on.
    ///
    /// Equivalent to the heartbeat path on the Postgres adapter: it tells the
    /// queue "I'm still alive, don't reclaim this task yet".
    async fn record_heartbeat(&self, task: &ClaimedTask) -> RedisAdapterResult<()>;

    /// Return the depth (claimable + pending) of each queue.
    async fn queue_depths(&self, queues: &[String]) -> RedisAdapterResult<Vec<(String, i64)>>;
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
