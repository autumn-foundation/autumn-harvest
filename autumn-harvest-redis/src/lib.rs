//! Redis Streams task queue adapter for `autumn-harvest`.
//!
//! This crate provides a high-throughput "escape hatch" for the
//! `autumn-harvest` workflow engine. The default Postgres-backed queue (using
//! `SELECT ... FOR UPDATE SKIP LOCKED`) is operationally simple but its
//! measured claim throughput (`docs/performance.md`) falls well short of the
//! "ten thousand task claims per second" figure once cited here — see
//! `docs/assays/0001-redis-adapter-throughput-ceiling.md` for this crate's own
//! measured standalone throughput and how it compares. Moving the ephemeral
//! task queue onto Redis Streams raises that ceiling substantially, while
//! leaving Postgres as the sole source of truth for workflow state and event
//! history.
//!
//! ## Scope
//!
//! - **In scope**: enqueue, claim, complete, fail, retry-with-delay, heartbeat,
//!   and visibility-timeout based recovery for the task queue.
//! - **Out of scope**: workflow state, event history, signals, timers,
//!   schedules, the DAG runtime. These continue to live on Postgres exactly as
//!   they do today.
//!
//! ## Usage sketch
//!
//! ```no_run
//! use std::time::Duration;
//! use autumn_harvest_redis::{
//!     EnqueueParams, RedisTaskQueue, RedisTaskQueueConfig, TaskQueueAdapter, TaskType,
//! };
//!
//! # async fn run() -> Result<(), autumn_harvest_redis::RedisAdapterError> {
//! let queue = RedisTaskQueue::connect(
//!     "redis://127.0.0.1/",
//!     RedisTaskQueueConfig::default(),
//! ).await?;
//!
//! let task_id = queue
//!     .enqueue(EnqueueParams::new(
//!         "default",
//!         TaskType::Activity,
//!         serde_json::json!({"to": "alice"}),
//!     ))
//!     .await?;
//!
//! if let Some(claim) = queue.claim(&["default".into()], "worker-1").await? {
//!     // ... do work ...
//!     queue.complete(&claim, serde_json::json!({"ok": true})).await?;
//! }
//!
//! // Reclaim crashed peers' work after the visibility timeout elapses.
//! let _ = queue.recover_pending(&["default".into()]).await?;
//! # Ok::<_, autumn_harvest_redis::RedisAdapterError>(())
//! # }
//! ```
//!
//! ## Worker integration
//!
//! [`RedisDispatch`] wires this crate into the `autumn-harvest` worker as a
//! **dispatch channel** (issue #1312). It implements
//! `autumn_harvest::dispatch::TaskDispatch`.
//!
//! Postgres keeps every `harvest_task_queue` row and stays the source of
//! truth. A stream entry carries a reference only: the task id, the queue and
//! the due time. A worker reads a reference, claims the named row in Postgres
//! with the full claim predicate, and then acks the reference. Workflow state
//! and event history stay in the Postgres transactions that exist today, so
//! no transactional boundary moves.
//!
//! The channel is a latency and throughput optimization, never a durability
//! store. The worker's reconcile sweep republishes due `PENDING` rows, so a
//! lost entry, a dropped hint or a Redis restart all converge. If Redis is
//! unreachable the worker falls back to the Postgres claim path.
//!
//! ### Limits in v1
//!
//! - **Single shard only.** A hint carries a shard slot, but a sharded
//!   runtime rejects Redis dispatch at validation.
//! - **Priority is best effort.** One stream per queue delivers in arrival
//!   order. The reconcile sweep publishes in priority order, which is the
//!   only priority signal the channel carries.
//! - **Sticky affinity is best effort.** Any worker in the consumer group may
//!   read any reference. The Postgres claim predicate still enforces the
//!   affinity gate, and a rejected reference is released with backoff.
//!
//! ### Standalone adapter
//!
//! [`RedisTaskQueue`] and [`TaskQueueAdapter`] are unchanged and stay
//! supported for callers that use this crate as a task queue on its own. The
//! two key families do not overlap.

#![cfg_attr(not(feature = "test-utils"), allow(rustdoc::private_intra_doc_links))]

mod adapter;
mod dispatch;
mod envelope;
mod error;
mod naming;
mod redis_queue;

pub use adapter::{ClaimedTask, TaskQueueAdapter};
pub use dispatch::{RedisDispatch, RedisDispatchConfig};
pub use envelope::{EnqueueParams, TaskEnvelope, TaskType};
pub use error::{RedisAdapterError, RedisAdapterResult};
pub use naming::{
    dispatch_delayed_key, dispatch_marker_key, dispatch_payloads_key, dispatch_stream_key, dlq_key,
    scheduled_payloads_key, scheduled_zset_key, stream_key,
};
pub use redis_queue::{RedisTaskQueue, RedisTaskQueueConfig};
