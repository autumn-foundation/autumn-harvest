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

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;

    /// Serializes the cases that install the process-global channel.
    static INSTALL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn hint(queue: &str, at: DateTime<Utc>) -> DispatchHint {
        DispatchHint {
            task_id: Uuid::new_v4(),
            queue_name: queue.to_string(),
            scheduled_at: at,
            priority: 0,
            shard: None,
        }
    }

    fn queues() -> Vec<String> {
        vec!["q".to_string()]
    }

    async fn read_one(channel: &MemoryDispatch) -> Option<DispatchLease> {
        channel
            .next(&queues(), "c", 8, Duration::from_millis(0))
            .await
            .expect("read")
            .into_iter()
            .next()
    }

    #[test]
    fn release_delay_doubles_and_then_holds_at_the_cap() {
        let base = Duration::from_millis(20);
        let cap = Duration::from_secs(1);
        assert_eq!(release_delay(0, base, cap), Duration::from_millis(20));
        assert_eq!(release_delay(1, base, cap), Duration::from_millis(40));
        assert_eq!(release_delay(4, base, cap), Duration::from_millis(320));
        assert_eq!(release_delay(6, base, cap), cap);
        assert_eq!(release_delay(1_000, base, cap), cap);
    }

    #[test]
    fn release_delay_saturates_instead_of_overflowing() {
        let base = Duration::from_secs(u64::MAX / 2);
        let cap = Duration::from_secs(30);
        assert_eq!(release_delay(31, base, cap), cap);
        assert_eq!(release_delay(u32::MAX, base, cap), cap);
    }

    #[tokio::test]
    async fn a_hint_outside_a_scope_reaches_the_channel() {
        let _guard = INSTALL_LOCK.lock().await;
        let channel = Arc::new(MemoryDispatch::new());
        install(
            Arc::clone(&channel) as Arc<dyn TaskDispatch>,
            DispatchSettings::default(),
        );

        let one = hint("q", Utc::now());
        record_hint(one.clone());
        for _ in 0..100 {
            if channel.published_ids().contains(&one.task_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(channel.published_ids(), vec![one.task_id]);
        uninstall();
    }

    #[tokio::test]
    async fn a_scope_holds_hints_until_it_flushes() {
        let _guard = INSTALL_LOCK.lock().await;
        let channel = Arc::new(MemoryDispatch::new());
        install(
            Arc::clone(&channel) as Arc<dyn TaskDispatch>,
            DispatchSettings::default(),
        );

        let one = hint("q", Utc::now());
        let inner = one.clone();
        let observed = Arc::clone(&channel);
        let ((), leftover) = buffered(async move {
            record_hint(inner);
            assert!(scope_active());
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(
                observed.published_ids().is_empty(),
                "a scoped hint must not reach the channel before the flush"
            );
        })
        .await;
        assert_eq!(leftover, vec![one.clone()]);
        assert!(channel.published_ids().is_empty());

        publish_now(leftover).await;
        assert_eq!(channel.published_ids(), vec![one.task_id]);
        uninstall();
    }

    #[tokio::test]
    async fn a_nested_scope_leaves_the_hints_with_the_outer_owner() {
        let _guard = INSTALL_LOCK.lock().await;
        let channel = Arc::new(MemoryDispatch::new());
        install(
            Arc::clone(&channel) as Arc<dyn TaskDispatch>,
            DispatchSettings::default(),
        );

        let one = hint("q", Utc::now());
        let inner = one.clone();
        let ((), outer) = buffered(async move {
            let ((), nested) = buffered(async move {
                record_hint(inner);
            })
            .await;
            assert!(nested.is_empty(), "a SAVEPOINT owner never takes the hints");
        })
        .await;
        assert_eq!(outer, vec![one]);
        uninstall();
    }

    #[tokio::test]
    async fn flush_scope_publishes_and_empties_the_buffer() {
        let _guard = INSTALL_LOCK.lock().await;
        let channel = Arc::new(MemoryDispatch::new());
        install(
            Arc::clone(&channel) as Arc<dyn TaskDispatch>,
            DispatchSettings::default(),
        );

        let one = hint("q", Utc::now());
        let inner = one.clone();
        let ((), leftover) = buffered(async move {
            record_hint(inner);
            flush_scope().await;
        })
        .await;
        assert!(leftover.is_empty());
        assert_eq!(channel.published_ids(), vec![one.task_id]);
        uninstall();
    }

    #[tokio::test]
    async fn no_channel_makes_every_hook_a_no_op() {
        let _guard = INSTALL_LOCK.lock().await;
        uninstall();
        assert!(!is_installed());
        record_hint(hint("q", Utc::now()));
        publish_now(vec![hint("q", Utc::now())]).await;
    }

    #[tokio::test]
    async fn memory_dispatch_delivers_a_published_reference_once() {
        let channel = MemoryDispatch::new();
        let one = hint("q", Utc::now());
        channel
            .publish(std::slice::from_ref(&one))
            .await
            .expect("publish");

        let lease = read_one(&channel).await.expect("one lease");
        assert_eq!(lease.task_id, one.task_id);
        assert_eq!(lease.redeliveries, 0);
        assert!(read_one(&channel).await.is_none());
        assert_eq!(channel.outstanding_leases(), 1);

        channel.ack(&lease).await.expect("ack");
        assert_eq!(channel.acked_ids(), vec![one.task_id]);
        assert!(channel.is_drained());
    }

    #[tokio::test]
    async fn memory_dispatch_dedupes_a_repeated_publish() {
        let channel = MemoryDispatch::new();
        let one = hint("q", Utc::now());
        channel
            .publish(std::slice::from_ref(&one))
            .await
            .expect("publish");
        channel
            .publish(std::slice::from_ref(&one))
            .await
            .expect("republish");

        assert_eq!(channel.pending_references(), 1);
        assert!(read_one(&channel).await.is_some());
        assert!(read_one(&channel).await.is_none());
    }

    #[tokio::test]
    async fn memory_dispatch_moves_a_parked_reference_to_an_earlier_due_time() {
        let channel = MemoryDispatch::new();
        let later = hint("q", Utc::now() + chrono::Duration::seconds(60));
        channel
            .publish(std::slice::from_ref(&later))
            .await
            .expect("publish");
        assert!(read_one(&channel).await.is_none());

        let sooner = DispatchHint {
            scheduled_at: Utc::now() - chrono::Duration::seconds(1),
            ..later.clone()
        };
        channel.publish(&[sooner]).await.expect("republish");
        channel.maintain(&queues()).await.expect("maintain");

        let lease = read_one(&channel).await.expect("promoted lease");
        assert_eq!(lease.task_id, later.task_id);
        assert_eq!(channel.pending_references(), 0);
    }

    #[tokio::test]
    async fn memory_dispatch_promotes_a_delayed_reference_when_it_is_due() {
        let channel = MemoryDispatch::new();
        let soon = hint("q", Utc::now() + chrono::Duration::milliseconds(60));
        channel
            .publish(std::slice::from_ref(&soon))
            .await
            .expect("publish");

        assert_eq!(
            channel.maintain(&queues()).await.expect("maintain"),
            DispatchMaintenance {
                promoted: 0,
                recovered: 0
            }
        );
        assert!(read_one(&channel).await.is_none());

        tokio::time::sleep(Duration::from_millis(90)).await;
        assert_eq!(
            channel.maintain(&queues()).await.expect("maintain"),
            DispatchMaintenance {
                promoted: 1,
                recovered: 0
            }
        );
        assert_eq!(
            read_one(&channel).await.expect("lease").task_id,
            soon.task_id
        );
    }

    #[tokio::test]
    async fn memory_dispatch_counts_a_redelivery_after_a_release() {
        let channel = MemoryDispatch::new();
        let one = hint("q", Utc::now());
        channel
            .publish(std::slice::from_ref(&one))
            .await
            .expect("publish");

        let first = read_one(&channel).await.expect("first");
        assert_eq!(first.redeliveries, 0);
        channel
            .release(&first, Duration::from_millis(0))
            .await
            .expect("release");
        assert_eq!(channel.released_ids(), vec![one.task_id]);

        let second = read_one(&channel).await.expect("second");
        assert_eq!(second.redeliveries, 1);
    }

    #[tokio::test]
    async fn memory_dispatch_recovers_a_lease_a_dead_consumer_left_behind() {
        let channel = MemoryDispatch::with_visibility_timeout(Duration::from_millis(40));
        let one = hint("q", Utc::now());
        channel
            .publish(std::slice::from_ref(&one))
            .await
            .expect("publish");

        let lease = read_one(&channel).await.expect("lease");
        assert_eq!(channel.outstanding_leases(), 1);
        assert!(read_one(&channel).await.is_none());

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            channel
                .maintain(&queues())
                .await
                .expect("maintain")
                .recovered,
            1
        );

        let recovered = read_one(&channel).await.expect("recovered");
        assert_eq!(recovered.task_id, lease.task_id);
        assert_eq!(recovered.redeliveries, 1);
    }

    #[tokio::test]
    async fn memory_dispatch_drop_all_wipes_every_reference() {
        let channel = MemoryDispatch::new();
        let ready = hint("q", Utc::now());
        let parked = hint("q", Utc::now() + chrono::Duration::seconds(60));
        channel
            .publish(&[ready.clone(), parked])
            .await
            .expect("publish");
        let lease = read_one(&channel).await.expect("lease");

        channel.drop_all();

        assert!(channel.is_drained());
        assert!(read_one(&channel).await.is_none());
        // Acking a wiped lease is harmless, exactly as it is against Redis.
        channel.ack(&lease).await.expect("ack");
    }

    #[tokio::test]
    async fn memory_dispatch_fail_next_fails_reads_and_publishes() {
        let channel = MemoryDispatch::new();
        channel.fail_next(2);

        let one = hint("q", Utc::now());
        assert!(matches!(
            channel.publish(std::slice::from_ref(&one)).await,
            Err(crate::error::HarvestError::Dispatch(_))
        ));
        assert!(matches!(
            channel.next(&queues(), "c", 1, Duration::ZERO).await,
            Err(crate::error::HarvestError::Dispatch(_))
        ));

        channel
            .publish(std::slice::from_ref(&one))
            .await
            .expect("publish");
        assert_eq!(
            read_one(&channel).await.expect("lease").task_id,
            one.task_id
        );
    }

    #[tokio::test]
    async fn memory_dispatch_keeps_queue_order_and_honours_max() {
        let channel = MemoryDispatch::new();
        let first = hint("q", Utc::now());
        let second = hint("q", Utc::now());
        let other = hint("other", Utc::now());
        channel
            .publish(&[first.clone(), second.clone(), other])
            .await
            .expect("publish");

        let leases = channel
            .next(&queues(), "c", 1, Duration::ZERO)
            .await
            .expect("read");
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].task_id, first.task_id);

        let leases = channel
            .next(&queues(), "c", 8, Duration::ZERO)
            .await
            .expect("read");
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].task_id, second.task_id);
        assert_eq!(channel.delivered_ids().len(), 2);
    }
}
