//! Redis Streams task queue adapter for `autumn-harvest`.
//!
//! This crate provides a high-throughput "escape hatch" for the
//! `autumn-harvest` workflow engine. The default Postgres-backed queue (using
//! `SELECT ... FOR UPDATE SKIP LOCKED`) is operationally simple but eventually
//! hits a ceiling around ten thousand task claims per second due to lock
//! contention. Moving the ephemeral task queue onto Redis Streams lifts that
//! ceiling while leaving Postgres as the sole source of truth for workflow
//! state and event history.
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
//! ## Worker integration status
//!
//! `autumn-harvest`'s worker currently calls Postgres queue functions directly
//! and interleaves them with event-store writes inside a single Diesel
//! transaction. Threading this crate through the existing `worker.rs` requires
//! splitting those transactional boundaries -- in particular, the worker's
//! "append events + update queue row" pattern becomes "append events, commit,
//! then ack the Redis stream" with idempotency on replay. That refactor is a
//! follow-up step and is intentionally not part of this crate's first
//! delivery; this crate ships the adapter and its tests so the integration
//! work can build on a stable foundation.

#![cfg_attr(not(feature = "test-utils"), allow(rustdoc::private_intra_doc_links))]

mod adapter;
mod envelope;
mod error;
mod naming;
mod redis_queue;

pub use adapter::{ClaimedTask, TaskQueueAdapter};
pub use envelope::{EnqueueParams, TaskEnvelope, TaskType};
pub use error::{RedisAdapterError, RedisAdapterResult};
pub use naming::{dlq_key, scheduled_payloads_key, scheduled_zset_key, stream_key};
pub use redis_queue::{RedisTaskQueue, RedisTaskQueueConfig};
