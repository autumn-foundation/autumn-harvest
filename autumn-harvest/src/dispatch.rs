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

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
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
        ANY_INSTALLED.store(true, Ordering::Relaxed);
    }
}

/// Remove the process-global channel. Tests use this between cases.
///
/// The background publisher is stopped as well, so the next install starts
/// with an empty publisher queue. A hint still in that queue is dropped; the
/// reconcile sweep republishes its row.
pub fn uninstall() {
    if let Ok(mut slot) = INSTALLED.write() {
        *slot = None;
        ANY_INSTALLED.store(false, Ordering::Relaxed);
    }
    let publisher = lock(&PUBLISHER).take();
    if let Some(publisher) = publisher {
        publisher.task.abort();
    }
}

/// The installed channel, if any.
#[must_use]
pub fn installed() -> Option<InstalledDispatch> {
    INSTALLED.read().ok().and_then(|slot| slot.clone())
}

// ---------------------------------------------------------------------------
// Fast path guard
// ---------------------------------------------------------------------------

/// Mirror of [`INSTALLED`] as one atomic flag.
///
/// Every publish hook in `queue.rs` reads this before it does any work. A
/// deployment with no channel therefore pays one relaxed load per hook, which
/// is what keeps the Postgres path unchanged.
static ANY_INSTALLED: AtomicBool = AtomicBool::new(false);

/// True when a channel is installed in this process.
#[must_use]
pub fn is_installed() -> bool {
    ANY_INSTALLED.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Buffering scope
// ---------------------------------------------------------------------------

tokio::task_local! {
    /// Hints raised by the current task while a buffering scope is active.
    ///
    /// The value is an [`Arc`] so [`buffered`] keeps a handle after the scope
    /// ends. `tokio::task_local!` moves the value into the scope and never
    /// gives it back.
    static HINT_BUFFER: Arc<Mutex<Vec<DispatchHint>>>;
}

/// True when the current task runs inside a [`buffered`] scope.
#[must_use]
pub fn scope_active() -> bool {
    HINT_BUFFER.try_with(|_| ()).is_ok()
}

/// Run `f` with a buffering scope installed and return its output plus every
/// hint the scope still holds.
///
/// A hint raised inside the scope waits in a task-local buffer instead of
/// going to the channel. The owner of the transaction publishes the buffer
/// after the transaction commits, so the channel never names a row that the
/// database has not committed.
///
/// The scope does not nest. When one is already active the future runs against
/// the outer buffer and the returned vector is empty. A SAVEPOINT owner
/// therefore never flushes the hints of the transaction that encloses it.
pub async fn buffered<F: Future>(f: F) -> (F::Output, Vec<DispatchHint>) {
    if scope_active() {
        return (f.await, Vec::new());
    }
    let buffer: Arc<Mutex<Vec<DispatchHint>>> = Arc::new(Mutex::new(Vec::new()));
    let handle = Arc::clone(&buffer);
    let output = HINT_BUFFER.scope(buffer, f).await;
    let hints = std::mem::take(&mut *lock(&handle));
    (output, hints)
}

/// Remove every hint the active scope holds and return it.
///
/// Returns an empty vector when no scope is active.
#[must_use]
pub fn take_scoped_hints() -> Vec<DispatchHint> {
    HINT_BUFFER
        .try_with(|buffer| std::mem::take(&mut *lock(buffer)))
        .unwrap_or_default()
}

/// Publish every hint the active scope holds.
///
/// Call this after the transaction that raised the hints commits. A failed
/// transaction discards its hints instead, because the scope is dropped
/// without a flush.
pub async fn flush_scope() {
    publish_now(take_scoped_hints()).await;
}

/// Publish the active scope's hints when `outcome` is `Ok`, discard them when
/// it is `Err`.
///
/// Call this on the result of a transaction the scope owns. A transaction that
/// rolled back left no `PENDING` row behind, so its hints name nothing and are
/// dropped rather than published.
///
/// # Errors
///
/// Returns `outcome` unchanged.
pub async fn settle_scope<T, E>(outcome: Result<T, E>) -> Result<T, E> {
    let hints = take_scoped_hints();
    if outcome.is_ok() {
        publish_now(hints).await;
    }
    outcome
}

/// Publish `hints` on the installed channel now.
///
/// Errors are logged and dropped. The reconcile sweep in the worker is the
/// durability floor. It republishes every due `PENDING` row the channel does
/// not hold. A lost publish therefore costs latency, never work.
pub async fn publish_now(hints: Vec<DispatchHint>) {
    if hints.is_empty() {
        return;
    }
    let Some(installed) = installed() else {
        return;
    };
    if let Err(error) = installed.channel.publish(&hints).await {
        tracing::warn!(
            error = %error,
            count = hints.len(),
            "dispatch publish failed; the reconcile sweep republishes these rows"
        );
    }
}

/// Record one hint for a row a write left `PENDING`.
///
/// Inside a [`buffered`] scope the hint waits for the flush after commit.
/// Outside one it goes to a background publisher that batches hints, so the
/// caller never awaits the channel. The call is a single atomic load when no
/// channel is installed.
pub fn record_hint(hint: DispatchHint) {
    if !is_installed() {
        return;
    }
    let mut carried = Some(hint);
    let buffered = HINT_BUFFER.try_with(|buffer| {
        if let Some(hint) = carried.take() {
            lock(buffer).push(hint);
        }
    });
    if buffered.is_ok() {
        return;
    }
    if let Some(hint) = carried {
        publish_in_background(hint);
    }
}

/// Record several hints. Equivalent to [`record_hint`] per element.
pub fn record_hints(hints: Vec<DispatchHint>) {
    // One guard for the whole batch, so a deployment with no channel does not
    // pay one load per hint.
    if !is_installed() {
        return;
    }
    for hint in hints {
        record_hint(hint);
    }
}

// ---------------------------------------------------------------------------
// Background publisher
// ---------------------------------------------------------------------------

/// Largest number of hints one background `publish` call carries.
const PUBLISH_BATCH_MAX: usize = 256;

/// The background publisher task and the channel that feeds it.
#[derive(Debug)]
struct Publisher {
    sender: tokio::sync::mpsc::UnboundedSender<DispatchHint>,
    task: tokio::task::JoinHandle<()>,
}

static PUBLISHER: Mutex<Option<Publisher>> = Mutex::new(None);

/// Lock a mutex and take the value back after a panic elsewhere.
///
/// A poisoned buffer holds hints, never workflow state, so the worst outcome
/// of using it is a duplicate publish, which the channel dedupes.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Hand `hint` to the background publisher, spawning it when needed.
///
/// The task is spawned lazily from inside a Tokio runtime. A caller with no
/// current runtime cannot spawn one, so the hint is dropped and the reconcile
/// sweep republishes the row.
fn publish_in_background(mut hint: DispatchHint) {
    let mut slot = lock(&PUBLISHER);
    if let Some(publisher) = slot.as_ref()
        && !publisher.task.is_finished()
    {
        match publisher.sender.send(hint) {
            Ok(()) => return,
            Err(returned) => hint = returned.0,
        }
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        tracing::debug!(
            "no Tokio runtime for the dispatch publisher; the reconcile sweep republishes this row"
        );
        *slot = None;
        return;
    };
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let task = runtime.spawn(publisher_loop(receiver));
    // The receiver is alive, so this send cannot fail.
    let _ = sender.send(hint);
    *slot = Some(Publisher { sender, task });
}

/// Drain the publisher channel and issue one `publish` call per batch.
async fn publisher_loop(mut receiver: tokio::sync::mpsc::UnboundedReceiver<DispatchHint>) {
    while let Some(first) = receiver.recv().await {
        let mut batch = Vec::with_capacity(1);
        batch.push(first);
        while batch.len() < PUBLISH_BATCH_MAX {
            match receiver.try_recv() {
                Ok(hint) => batch.push(hint),
                Err(_) => break,
            }
        }
        publish_now(batch).await;
    }
}

// ---------------------------------------------------------------------------
// Release backoff
// ---------------------------------------------------------------------------

/// Delay before a released reference is delivered again.
///
/// `min(cap, base * 2^redeliveries)`, saturating at `cap`. A gated row
/// therefore backs off instead of cycling once per poll.
#[must_use]
pub fn release_delay(redeliveries: u32, base: Duration, cap: Duration) -> Duration {
    let factor = 1_u32.checked_shl(redeliveries).unwrap_or(u32::MAX);
    base.checked_mul(factor).unwrap_or(cap).min(cap)
}

// ---------------------------------------------------------------------------
// In-memory channel for tests
// ---------------------------------------------------------------------------

/// Default visibility timeout for [`MemoryDispatch`] leases.
#[cfg(feature = "testing")]
pub const DEFAULT_MEMORY_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(60);

/// One reference held by [`MemoryDispatch`].
#[cfg(feature = "testing")]
#[derive(Debug, Clone)]
struct MemoryEntry {
    task_id: Uuid,
    queue_name: String,
    due: DateTime<Utc>,
    redeliveries: u32,
    shard: Option<crate::types::ShardId>,
}

/// A reference delivered to a consumer and not yet acked.
#[cfg(feature = "testing")]
#[derive(Debug, Clone)]
struct MemoryInflight {
    entry: MemoryEntry,
    leased_at: std::time::Instant,
}

/// Everything [`MemoryDispatch`] holds, behind one lock.
#[cfg(feature = "testing")]
#[derive(Debug, Default)]
struct MemoryState {
    /// Claimable references per queue, in publish order.
    ready: std::collections::HashMap<String, std::collections::VecDeque<Uuid>>,
    /// References parked until their due time.
    delayed: std::collections::BTreeSet<(DateTime<Utc>, Uuid)>,
    /// Every reference the channel holds and has not delivered.
    entries: std::collections::HashMap<Uuid, MemoryEntry>,
    /// Delivered references, keyed by lease handle.
    inflight: std::collections::HashMap<String, MemoryInflight>,
    /// Remaining injected failures for `next` and `publish`.
    fail_next: usize,
    /// Handle counter, so every lease handle is unique.
    next_handle: u64,
    published: Vec<Uuid>,
    delivered: Vec<Uuid>,
    acked: Vec<Uuid>,
    released: Vec<Uuid>,
}

/// An in-memory [`TaskDispatch`] for tests.
///
/// It reproduces the parts of the Redis Streams implementation the worker
/// depends on. Those are per-queue order, a delayed set, dedupe by task id,
/// leases with redelivery counts, and recovery of a lease a crashed consumer
/// left behind. The test knobs [`MemoryDispatch::fail_next`] and
/// [`MemoryDispatch::drop_all`] reproduce a channel error and a channel wipe.
#[cfg(feature = "testing")]
#[derive(Debug)]
pub struct MemoryDispatch {
    state: Mutex<MemoryState>,
    visibility_timeout: Duration,
}

#[cfg(feature = "testing")]
impl Default for MemoryDispatch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "testing")]
impl MemoryDispatch {
    /// A channel with the default visibility timeout.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MemoryState::default()),
            visibility_timeout: DEFAULT_MEMORY_VISIBILITY_TIMEOUT,
        }
    }

    /// A channel that recovers a lease older than `visibility_timeout`.
    #[must_use]
    pub fn with_visibility_timeout(visibility_timeout: Duration) -> Self {
        Self {
            state: Mutex::new(MemoryState::default()),
            visibility_timeout,
        }
    }

    /// Make the next `n` calls to `next` or `publish` return
    /// [`crate::error::HarvestError::Dispatch`].
    pub fn fail_next(&self, n: usize) {
        lock(&self.state).fail_next = n;
    }

    /// Discard every reference, as a Redis restart without persistence does.
    ///
    /// Counters are kept, so a test can still assert what was published.
    pub fn drop_all(&self) {
        let mut state = lock(&self.state);
        state.ready.clear();
        state.delayed.clear();
        state.entries.clear();
        state.inflight.clear();
    }

    /// Task ids accepted by `publish`, in call order.
    #[must_use]
    pub fn published_ids(&self) -> Vec<Uuid> {
        lock(&self.state).published.clone()
    }

    /// Task ids handed to a consumer by `next`, in delivery order.
    #[must_use]
    pub fn delivered_ids(&self) -> Vec<Uuid> {
        lock(&self.state).delivered.clone()
    }

    /// Task ids acked, in call order.
    #[must_use]
    pub fn acked_ids(&self) -> Vec<Uuid> {
        lock(&self.state).acked.clone()
    }

    /// Task ids released, in call order.
    #[must_use]
    pub fn released_ids(&self) -> Vec<Uuid> {
        lock(&self.state).released.clone()
    }

    /// Number of delivered references that are not acked or released.
    #[must_use]
    pub fn outstanding_leases(&self) -> usize {
        lock(&self.state).inflight.len()
    }

    /// Number of references the channel holds and has not delivered.
    #[must_use]
    pub fn pending_references(&self) -> usize {
        lock(&self.state).entries.len()
    }

    /// True when the channel holds no reference at all.
    #[must_use]
    pub fn is_drained(&self) -> bool {
        let state = lock(&self.state);
        state.entries.is_empty() && state.inflight.is_empty()
    }

    /// Take one injected failure. True when the caller must fail.
    const fn take_failure(state: &mut MemoryState) -> bool {
        if state.fail_next == 0 {
            return false;
        }
        state.fail_next -= 1;
        true
    }

    /// Put `entry` in the ready queue or the delayed set, by its due time.
    fn place(state: &mut MemoryState, entry: MemoryEntry, now: DateTime<Utc>) {
        let task_id = entry.task_id;
        if entry.due <= now {
            state
                .ready
                .entry(entry.queue_name.clone())
                .or_default()
                .push_back(task_id);
        } else {
            state.delayed.insert((entry.due, task_id));
        }
        state.entries.insert(task_id, entry);
    }

    /// Remove `task_id` from whichever holder it sits in.
    fn unplace(state: &mut MemoryState, task_id: Uuid) -> Option<MemoryEntry> {
        let entry = state.entries.remove(&task_id)?;
        state.delayed.remove(&(entry.due, task_id));
        if let Some(queue) = state.ready.get_mut(&entry.queue_name) {
            queue.retain(|id| *id != task_id);
        }
        Some(entry)
    }

    /// Move every due delayed reference into its ready queue.
    fn promote_due(state: &mut MemoryState, now: DateTime<Utc>) -> usize {
        let due: Vec<(DateTime<Utc>, Uuid)> = state
            .delayed
            .iter()
            .take_while(|(due, _)| *due <= now)
            .copied()
            .collect();
        for key in &due {
            state.delayed.remove(key);
            if let Some(entry) = state.entries.get(&key.1) {
                let queue_name = entry.queue_name.clone();
                state.ready.entry(queue_name).or_default().push_back(key.1);
            }
        }
        due.len()
    }
}

#[cfg(feature = "testing")]
#[async_trait]
impl TaskDispatch for MemoryDispatch {
    async fn publish(&self, hints: &[DispatchHint]) -> HarvestResult<()> {
        let mut state = lock(&self.state);
        if Self::take_failure(&mut state) {
            return Err(crate::error::HarvestError::Dispatch(
                "injected publish failure".to_string(),
            ));
        }
        let now = Utc::now();
        for hint in hints {
            state.published.push(hint.task_id);
            // A reference the channel already holds is a no-op. An earlier due
            // time moves a parked reference forward. A signal that arrives
            // while a row is parked is therefore not held back by that entry.
            if let Some(held) = state.entries.get(&hint.task_id) {
                if hint.scheduled_at >= held.due {
                    continue;
                }
                Self::unplace(&mut state, hint.task_id);
            }
            // A delivered reference is held by its consumer. Publishing again
            // must not create a second copy of it.
            if state
                .inflight
                .values()
                .any(|held| held.entry.task_id == hint.task_id)
            {
                continue;
            }
            let entry = MemoryEntry {
                task_id: hint.task_id,
                queue_name: hint.queue_name.clone(),
                due: hint.scheduled_at,
                redeliveries: 0,
                shard: hint.shard,
            };
            Self::place(&mut state, entry, now);
        }
        drop(state);
        Ok(())
    }

    async fn next(
        &self,
        queues: &[String],
        _consumer: &str,
        max: usize,
        wait: Duration,
    ) -> HarvestResult<Vec<DispatchLease>> {
        let deadline = std::time::Instant::now() + wait;
        loop {
            {
                let mut state = lock(&self.state);
                if Self::take_failure(&mut state) {
                    return Err(crate::error::HarvestError::Dispatch(
                        "injected read failure".to_string(),
                    ));
                }
                let mut leases = Vec::new();
                'queues: for queue in queues {
                    while leases.len() < max {
                        let Some(task_id) = state
                            .ready
                            .get_mut(queue)
                            .and_then(std::collections::VecDeque::pop_front)
                        else {
                            continue 'queues;
                        };
                        let Some(entry) = state.entries.remove(&task_id) else {
                            continue;
                        };
                        state.next_handle += 1;
                        let handle = format!("m-{}", state.next_handle);
                        state.delivered.push(task_id);
                        leases.push(DispatchLease {
                            task_id,
                            queue_name: entry.queue_name.clone(),
                            redeliveries: entry.redeliveries,
                            handle: handle.clone(),
                            shard: entry.shard,
                        });
                        state.inflight.insert(
                            handle,
                            MemoryInflight {
                                entry,
                                leased_at: std::time::Instant::now(),
                            },
                        );
                    }
                    break;
                }
                if !leases.is_empty() {
                    return Ok(leases);
                }
                drop(state);
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Ok(Vec::new());
            }
            let step = (deadline - now).min(Duration::from_millis(5));
            tokio::time::sleep(step).await;
        }
    }

    async fn ack(&self, lease: &DispatchLease) -> HarvestResult<()> {
        let mut state = lock(&self.state);
        state.inflight.remove(&lease.handle);
        state.acked.push(lease.task_id);
        drop(state);
        Ok(())
    }

    async fn release(&self, lease: &DispatchLease, delay: Duration) -> HarvestResult<()> {
        let mut state = lock(&self.state);
        let Some(held) = state.inflight.remove(&lease.handle) else {
            return Ok(());
        };
        state.released.push(lease.task_id);
        let now = Utc::now();
        let due = chrono::Duration::from_std(delay)
            .map_or(now, |delay| now.checked_add_signed(delay).unwrap_or(now));
        let entry = MemoryEntry {
            due,
            redeliveries: held.entry.redeliveries.saturating_add(1),
            ..held.entry
        };
        Self::place(&mut state, entry, now);
        drop(state);
        Ok(())
    }

    async fn maintain(&self, _queues: &[String]) -> HarvestResult<DispatchMaintenance> {
        let mut state = lock(&self.state);
        let promoted = Self::promote_due(&mut state, Utc::now());

        let visibility_timeout = self.visibility_timeout;
        let expired: Vec<String> = state
            .inflight
            .iter()
            .filter(|(_, held)| held.leased_at.elapsed() >= visibility_timeout)
            .map(|(handle, _)| handle.clone())
            .collect();
        let recovered = expired.len();
        let now = Utc::now();
        for handle in expired {
            let Some(held) = state.inflight.remove(&handle) else {
                continue;
            };
            let entry = MemoryEntry {
                due: now,
                redeliveries: held.entry.redeliveries.saturating_add(1),
                ..held.entry
            };
            Self::place(&mut state, entry, now);
        }
        drop(state);

        Ok(DispatchMaintenance {
            promoted,
            recovered,
        })
    }
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
