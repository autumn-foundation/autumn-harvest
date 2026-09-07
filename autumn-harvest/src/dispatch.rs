//! Task dispatch channel seam (issue #1312).
//!
//! Postgres holds every `harvest_task_queue` row and stays the source of
//! truth. A [`TaskDispatch`] implementation carries small references to
//! claimable rows between processes. A worker reads a reference, claims the
//! named row in Postgres with the full claim predicate, and then acks the
//! reference. The Redis Streams implementation lives in `autumn-harvest-redis`.
//!
//! The channel is a latency and throughput optimization, never a durability
//! store. A lost reference converges through the reconcile sweep in the
//! worker, which republishes due `PENDING` rows. See
//! `docs/plans/2026-09-07-redis-dispatch-worker-integration.md`.
//!
//! The installed channel is process-global, like the mutex lease TTL and the
//! DR config. The worker reads it at run time, and every enqueue path in the
//! same process publishes through it.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::HarvestResult;

/// Default wait for one blocking read on the channel.
pub const DEFAULT_DISPATCH_POLL_INTERVAL: Duration = Duration::from_millis(20);
/// Default interval for the reconcile sweep over due `PENDING` rows.
pub const DEFAULT_DISPATCH_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
/// Default row cap for one reconcile sweep per queue.
pub const DEFAULT_DISPATCH_RECONCILE_BATCH: usize = 1000;
/// Default cap for the release backoff of a gated reference.
pub const DEFAULT_DISPATCH_RELEASE_BACKOFF_CAP: Duration = Duration::from_secs(30);

/// A reference to a claimable `harvest_task_queue` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchHint {
    /// Primary key of the row.
    pub task_id: Uuid,
    /// Logical queue the row belongs to.
    pub queue_name: String,
    /// Time the row becomes claimable.
    pub scheduled_at: DateTime<Utc>,
    /// Row priority. Implementations may use it for ordering.
    pub priority: i32,
    /// Shard the row lives on. `None` for a single-shard runtime.
    pub shard: Option<crate::types::ShardId>,
}

/// One delivered reference. The worker must `ack` or `release` it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchLease {
    /// Primary key of the row.
    pub task_id: Uuid,
    /// Logical queue the reference was read from.
    pub queue_name: String,
    /// Number of times this reference was delivered before this one.
    pub redeliveries: u32,
    /// Implementation-specific handle for the delivered entry.
    pub handle: String,
    /// Shard the row lives on. `None` for a single-shard runtime.
    pub shard: Option<crate::types::ShardId>,
}

/// Counters returned by one maintenance pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DispatchMaintenance {
    /// Delayed references that became claimable.
    pub promoted: usize,
    /// References recovered from a crashed consumer.
    pub recovered: usize,
}

/// Worker-side tuning for the channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchSettings {
    /// Wait for one blocking read when the channel is idle.
    pub poll_interval: Duration,
    /// Interval for the reconcile sweep over due `PENDING` rows.
    pub reconcile_interval: Duration,
    /// Row cap for one reconcile sweep per queue.
    pub reconcile_batch: usize,
    /// Cap for the exponential release backoff of a gated reference.
    pub release_backoff_cap: Duration,
}

impl Default for DispatchSettings {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_DISPATCH_POLL_INTERVAL,
            reconcile_interval: DEFAULT_DISPATCH_RECONCILE_INTERVAL,
            reconcile_batch: DEFAULT_DISPATCH_RECONCILE_BATCH,
            release_backoff_cap: DEFAULT_DISPATCH_RELEASE_BACKOFF_CAP,
        }
    }
}

/// A channel that carries task references between processes.
///
/// Implementations deliver each published reference at least once. They do
/// not need to persist references: the worker's reconcile sweep republishes
/// every due `PENDING` row that the channel does not hold.
#[async_trait]
pub trait TaskDispatch: Send + Sync + std::fmt::Debug {
    /// Publish references. A reference that the channel already holds is a
    /// no-op, unless the new `scheduled_at` is earlier than the held one.
    async fn publish(&self, hints: &[DispatchHint]) -> HarvestResult<()>;

    /// Read up to `max` due references for `queues`. Wait up to `wait` when
    /// the channel is empty. `consumer` names the caller for recovery.
    async fn next(
        &self,
        queues: &[String],
        consumer: &str,
        max: usize,
        wait: Duration,
    ) -> HarvestResult<Vec<DispatchLease>>;

    /// Drop a reference. The row was claimed, or it is no longer claimable.
    async fn ack(&self, lease: &DispatchLease) -> HarvestResult<()>;

    /// Give a reference back so it is delivered again after `delay`.
    async fn release(&self, lease: &DispatchLease, delay: Duration) -> HarvestResult<()>;

    /// Promote due delayed references and recover references held by a
    /// consumer that stopped acking.
    async fn maintain(&self, queues: &[String]) -> HarvestResult<DispatchMaintenance>;
}

/// The installed channel and its settings.
#[derive(Debug, Clone)]
pub struct InstalledDispatch {
    /// The channel.
    pub channel: Arc<dyn TaskDispatch>,
    /// Worker-side tuning.
    pub settings: DispatchSettings,
}

static INSTALLED: RwLock<Option<InstalledDispatch>> = RwLock::new(None);

/// Install the process-global channel. A later call replaces the earlier one.
pub fn install(channel: Arc<dyn TaskDispatch>, settings: DispatchSettings) {
    if let Ok(mut slot) = INSTALLED.write() {
        *slot = Some(InstalledDispatch { channel, settings });
    }
}

/// Remove the process-global channel. Tests use this between cases.
pub fn uninstall() {
    if let Ok(mut slot) = INSTALLED.write() {
        *slot = None;
    }
}

/// The installed channel, if any.
#[must_use]
pub fn installed() -> Option<InstalledDispatch> {
    INSTALLED.read().ok().and_then(|slot| slot.clone())
}
