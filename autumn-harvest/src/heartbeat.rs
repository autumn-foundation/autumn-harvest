//! Background heartbeat flusher for activities.
//!
//! Activities send heartbeat payloads via an mpsc channel. This module spawns
//! a background Tokio task that receives those payloads, debounces them (keeping
//! only the most recent), and periodically flushes the heartbeat timestamp and
//! payload to the database.
//!
//! The flusher runs every 1 second, draining all pending heartbeats and keeping
//! only the last one. This avoids hammering Postgres with per-heartbeat writes
//! while still providing timely liveness detection.
//!
//! The flush loop, [`run_heartbeat_flusher`], has no DB dependency. It writes
//! through a [`HeartbeatSink`]. The Postgres sink needs the `db` feature. The
//! Shuttle models in `tests/shuttle_models.rs` drive the same loop with a
//! recording sink (issue #1800).
//!
//! The worker uses [`spawn_heartbeat_flusher_with`] instead (issue #1788). Its
//! payloads carry the time the activity sent them, and a failed flush keeps
//! its payload for the next tick. Both need the `db` feature.

use std::future::Future;
use std::time::Duration;

use serde_json::Value;

// Shuttle swaps these types under `cfg(shuttle)` (issue #1800). See
// `crate::shuttle_sync`.
use crate::shuttle_sync::{CancellationToken, mpsc, select, sleep};

#[cfg(feature = "db")]
use std::sync::{Arc, Mutex, PoisonError};

#[cfg(feature = "db")]
use crate::context::StampedHeartbeat;
#[cfg(feature = "db")]
use crate::error::{HarvestError, HarvestResult};
#[cfg(feature = "db")]
use crate::queue::{ClaimWrite, TaskClaim};
#[cfg(feature = "db")]
use crate::telemetry::MetricsRecorder;
#[cfg(feature = "db")]
use diesel_async::AsyncPgConnection;
#[cfg(feature = "db")]
use diesel_async::pooled_connection::deadpool::Pool;

/// The time between two drains of the heartbeat channel.
#[cfg(feature = "db")]
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// What the flusher does after one flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlushOutcome {
    /// Keep flushing. A failed write also continues. The flusher drops the
    /// failed payload, and the next heartbeat replaces it.
    Continue,
    /// The claim is no longer current (issue #1789). The flusher cancels the
    /// activity and stops.
    LeaseLost,
}

/// Why [`run_heartbeat_flusher`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlusherExit {
    /// The cancellation token fired.
    Cancelled,
    /// The sink reported [`FlushOutcome::LeaseLost`]. The flusher cancelled
    /// the token.
    LeaseLost,
}

/// The destination of the newest heartbeat payload.
///
/// This is an engine seam for tests (issue #1800), not a stable extension
/// point.
pub trait HeartbeatSink: Send {
    /// Write one heartbeat payload.
    fn flush(&mut self, payload: Value) -> impl Future<Output = FlushOutcome> + Send;
}

/// Drain `rx` every `flush_interval` and flush the newest payload to `sink`.
///
/// The loop stops when `cancel` fires, or when the sink reports
/// [`FlushOutcome::LeaseLost`]. In the second case it cancels `cancel` first,
/// so the activity stops too.
///
/// A drain keeps only the newest payload. The channel is FIFO, so each flush
/// carries a payload newer than the one before it.
pub async fn run_heartbeat_flusher<S: HeartbeatSink>(
    mut rx: mpsc::Receiver<Value>,
    cancel: CancellationToken,
    flush_interval: Duration,
    mut sink: S,
) -> FlusherExit {
    loop {
        select! {
            () = cancel.cancelled() => return FlusherExit::Cancelled,
            () = sleep(flush_interval) => {}
        }

        if let Some(payload) = drain_latest(&mut rx)
            && sink.flush(payload).await == FlushOutcome::LeaseLost
        {
            cancel.cancel();
            return FlusherExit::LeaseLost;
        }

        if cancel.is_cancelled() {
            return FlusherExit::Cancelled;
        }
    }
}

/// Drain every pending payload and return the newest one.
fn drain_latest(rx: &mut mpsc::Receiver<Value>) -> Option<Value> {
    let mut latest = None;
    while let Ok(payload) = rx.try_recv() {
        latest = Some(payload);
    }
    latest
}

/// Spawn a background heartbeat flusher for the task that `claim` holds.
///
/// Returns an `mpsc::Sender<Value>` that the activity should use to send
/// heartbeat payloads. The flusher task will:
///
/// 1. Wait up to 1 second for heartbeats to arrive.
/// 2. Drain all pending heartbeats, keeping only the most recent.
/// 3. Call `queue::record_heartbeat()` to update the DB timestamp and payload.
/// 4. Repeat until `cancel` fires or the claim is lost.
///
/// `claim` fences the write (issue #1789). When the claim is no longer
/// current, the flusher cancels `cancel` and stops.
///
/// The returned sender has a buffer of 64 messages -- if the activity sends
/// heartbeats faster than that without the flusher draining, sends will
/// await (backpressure).
///
/// [`crate::pool::acquire_bound`] limits each pool acquire (issue #1788). The
/// write stamps the time of the flush, and a failed flush drops its payload.
/// The worker uses [`spawn_heartbeat_flusher_with`], which keeps both.
#[cfg(feature = "db")]
#[must_use]
pub fn spawn_heartbeat_flusher(
    claim: TaskClaim,
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
) -> mpsc::Sender<Value> {
    let (tx, rx) = mpsc::channel(64);

    // Plain tokio, not the shim: the `db` build never sets `--cfg shuttle`.
    tokio::spawn(heartbeat_loop(claim, pool, rx, cancel));

    tx
}

/// The main heartbeat flushing loop.
#[cfg(feature = "db")]
async fn heartbeat_loop(
    claim: TaskClaim,
    pool: Pool<AsyncPgConnection>,
    rx: mpsc::Receiver<Value>,
    cancel: CancellationToken,
) {
    let task_id = claim.task_id;
    let acquire_timeout = crate::pool::acquire_bound(&pool);
    let sink = PgHeartbeatSink {
        claim,
        pool,
        acquire_timeout,
    };
    if run_heartbeat_flusher(rx, cancel, FLUSH_INTERVAL, sink).await == FlusherExit::Cancelled {
        tracing::debug!(task_id = %task_id, "heartbeat flusher cancelled");
    }
}

/// Writes heartbeats to `harvest_task_queue` through `queue::record_heartbeat`.
#[cfg(feature = "db")]
struct PgHeartbeatSink {
    claim: TaskClaim,
    pool: Pool<AsyncPgConnection>,
    /// The bound on each pool acquire (issue #1788).
    acquire_timeout: Duration,
}

#[cfg(feature = "db")]
impl HeartbeatSink for PgHeartbeatSink {
    async fn flush(&mut self, payload: Value) -> FlushOutcome {
        let task_id = self.claim.task_id;
        match crate::pool::acquire(&self.pool, self.acquire_timeout).await {
            Ok(mut conn) => {
                match crate::queue::record_heartbeat(&mut conn, &self.claim, payload).await {
                    Ok(ClaimWrite::Applied) => FlushOutcome::Continue,
                    // The claim is no longer current (issue #1789). The loop
                    // then cancels the activity, so this stale attempt does
                    // no more work.
                    Ok(ClaimWrite::LeaseLost) => {
                        tracing::warn!(
                            task_id = %task_id,
                            worker_id = %self.claim.worker_id,
                            attempt = self.claim.attempt,
                            "activity lease lost on heartbeat; cancelling the activity"
                        );
                        FlushOutcome::LeaseLost
                    }
                    Err(e) => {
                        tracing::warn!(
                            task_id = %task_id,
                            error = %e,
                            "failed to flush heartbeat to database"
                        );
                        FlushOutcome::Continue
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    task_id = %task_id,
                    error = %e,
                    "failed to acquire DB connection for heartbeat flush"
                );
                FlushOutcome::Continue
            }
        }
    }
}

/// Options for [`spawn_heartbeat_flusher_with`] (issue #1788).
#[cfg(feature = "db")]
#[derive(Clone)]
pub struct HeartbeatFlushOptions {
    /// The bound on each pool acquire.
    pub acquire_timeout: Duration,
    /// Receives `harvest.heartbeat.flush_failed` and
    /// `harvest.db.pool_acquire_timeout{site="heartbeat_flush"}`.
    pub metrics: Arc<dyn MetricsRecorder>,
    /// The `shard` labels of the pool (issue #1815). Each flush records its
    /// pool wait and its write latency under each of them. A pool that serves
    /// several shards carries each shard's label, as its gauges do.
    pub shards: Arc<[u16]>,
}

/// A heartbeat flusher for payloads that their sender stamps (issue #1788).
///
/// The worker uses it. It takes an explicit acquire bound and a metrics sink.
///
/// A failed flush keeps its payload. The next tick sends it again, unless a
/// newer payload replaces it. A lost claim cancels `cancel` and stops the
/// flusher.
///
/// Each payload carries the time its sender stamped. The flush writes the
/// database clock minus the age of that time. A payload that waits in the
/// slot thus keeps its real age.
#[cfg(feature = "db")]
#[must_use]
pub fn spawn_heartbeat_flusher_with(
    claim: TaskClaim,
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
    options: HeartbeatFlushOptions,
) -> HeartbeatSlot {
    let latest = LatestHeartbeat::default();
    tokio::spawn(stamped_heartbeat_loop(
        claim,
        pool,
        Arc::clone(&latest),
        cancel,
        options,
    ));
    HeartbeatSlot(latest)
}

/// The activity side of [`spawn_heartbeat_flusher_with`] (issue #1788).
///
/// A send puts the heartbeat in the flusher's slot before it returns. No task
/// stands between the activity and the flush loop. The flush loop thus sees
/// every heartbeat whose send has returned.
#[cfg(feature = "db")]
#[derive(Clone)]
pub struct HeartbeatSlot(LatestHeartbeat);

#[cfg(feature = "db")]
impl HeartbeatSlot {
    /// Send `beat` to the flusher. Returns `false` once the flusher stopped.
    pub fn send(&self, beat: impl Into<StampedHeartbeat>) -> bool {
        self.0.publish(beat.into())
    }
}

#[cfg(feature = "db")]
impl std::fmt::Debug for HeartbeatSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeartbeatSlot").finish_non_exhaustive()
    }
}

/// The newest heartbeat not yet taken by the flush loop.
#[cfg(feature = "db")]
type LatestHeartbeat = Arc<Latest>;

/// The slot that the activity fills and the flush loop takes.
#[cfg(feature = "db")]
#[derive(Default)]
struct Latest {
    state: Mutex<SlotState>,
    /// Wakes a flush loop that waits after a blocked write.
    published: tokio::sync::Notify,
}

#[cfg(feature = "db")]
#[derive(Default)]
struct SlotState {
    /// The newest heartbeat not yet taken.
    pending: Option<Pending>,
    /// The sequence of the newest heartbeat seen.
    newest_sequence: Option<u64>,
    /// Set when the flush loop stops.
    closed: bool,
}

#[cfg(feature = "db")]
impl Latest {
    /// Keep `beat` if it is the newest send so far (issue #1788). Then wake a
    /// waiting flush loop. Returns `false` once the flush loop stopped.
    ///
    /// Newest means the latest send, not the latest call. A manual heartbeat
    /// and the auto-heartbeat ticker stamp before they send, so they can call
    /// out of order. A heartbeat sent before one already seen is dropped, also
    /// when a flush already took the newer one. A write of it would move
    /// `last_heartbeat_at` backwards.
    ///
    /// The order comes from `sequence`, one counter for every stamp. Two
    /// senders can read the same `Instant`, but never the same sequence. The
    /// wall clock plays no part, because it can step back, for example after
    /// an NTP correction.
    fn publish(&self, beat: StampedHeartbeat) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return false;
        }
        if state
            .newest_sequence
            .is_some_and(|newest| beat.sequence < newest)
        {
            return true;
        }
        state.newest_sequence = Some(beat.sequence);
        state.pending = Some(Pending {
            payload: beat.details,
            sent_order: beat.sent_order,
        });
        drop(state);
        self.published.notify_waiters();
        true
    }

    /// Take the heartbeat from the slot.
    fn take(&self) -> Option<Pending> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pending
            .take()
    }

    /// Whether a heartbeat waits in the slot.
    fn is_full(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pending
            .is_some()
    }

    /// Mark the flush loop as stopped. Later sends return `false`.
    fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed = true;
    }
}

/// Write one heartbeat for `claim`. `acquire_timeout` limits the pool
/// acquire.
///
/// # Errors
///
/// [`crate::error::HarvestError::PoolAcquireTimeout`] when the acquire bound
/// elapses. [`crate::error::HarvestError::PoolAcquireFailed`] when the pool
/// fails in another way. [`crate::error::HarvestError::Database`] when the
/// write fails.
#[cfg(feature = "db")]
pub async fn flush_heartbeat(
    pool: &Pool<AsyncPgConnection>,
    claim: &TaskClaim,
    payload: Value,
    acquire_timeout: Duration,
) -> HarvestResult<ClaimWrite> {
    let mut pending = Pending {
        payload,
        sent_order: std::time::Instant::now(),
    };
    flush(
        pool,
        claim,
        &mut pending,
        &Latest::default(),
        &HeartbeatFlushOptions {
            acquire_timeout,
            metrics: Arc::new(crate::telemetry::NoOpMetrics),
            shards: Arc::from([0]),
        },
        Duration::MAX,
    )
    .await
    .map_err(|failure| *failure.error)
}

/// A failed flush and its `harvest.heartbeat.flush_failed` reason label.
///
/// The error is boxed, because `HarvestError` is large
/// (`clippy::result_large_err`).
#[cfg(feature = "db")]
struct FlushFailure {
    reason: &'static str,
    error: Box<HarvestError>,
}

/// A heartbeat not yet written, with the time the activity sent it.
#[cfg(feature = "db")]
struct Pending {
    payload: Value,
    sent_order: std::time::Instant,
}

/// Write `beat`, or a newer heartbeat from `latest`, on one connection.
///
/// A heartbeat can reach `latest` while the acquire waits. The flush writes
/// that newer heartbeat instead (issue #1788). A write that blocks for
/// `interval` or more is followed on the same connection by any newer
/// heartbeat. A scanner that waits for the slot thus never reads the older
/// send time. A quick write keeps the rate of one write per interval.
///
/// A blocked write that fails on a session timeout is followed too. The
/// timeout keeps the session, so the connection still works. The `metrics`
/// of `options` count that failure, because the caller sees only the last
/// write. They also get the pool wait and the write latency of each flush,
/// under the `shard` of `options` (issue #1815).
///
/// `beat` holds the last heartbeat written or tried.
#[cfg(feature = "db")]
async fn flush(
    pool: &Pool<AsyncPgConnection>,
    claim: &TaskClaim,
    beat: &mut Pending,
    latest: &Latest,
    options: &HeartbeatFlushOptions,
    interval: Duration,
) -> Result<ClaimWrite, FlushFailure> {
    let metrics = options.metrics.as_ref();
    let shards = &options.shards;
    let mut started = tokio::time::Instant::now();
    let wait_started = std::time::Instant::now();
    let acquired = crate::pool::acquire(pool, options.acquire_timeout).await;
    let waited = wait_started.elapsed().as_secs_f64();
    for shard in shards.iter() {
        metrics.record_db_pool_wait(*shard, waited);
    }
    let mut conn = acquired.map_err(|error| FlushFailure {
        reason: if error.is_pool_acquire_timeout() {
            "acquire_timeout"
        } else {
            "acquire_error"
        },
        error: Box::new(error),
    })?;
    loop {
        if let Some(newer) = latest.take() {
            *beat = newer;
        }
        let write_started = std::time::Instant::now();
        let written = crate::queue::record_heartbeat_sent_ago(
            &mut conn,
            claim,
            beat.payload.clone(),
            beat.sent_order.elapsed(),
        )
        .await;
        let wrote = write_started.elapsed().as_secs_f64();
        for shard in shards.iter() {
            metrics.record_db_query_duration(crate::telemetry::DbOp::Heartbeat, *shard, wrote);
        }
        let connection_works = match &written {
            Ok(write) => *write == ClaimWrite::Applied,
            Err(error) => crate::pool::is_session_timeout(error),
        };
        if !connection_works || !write_blocked(started.elapsed(), interval) || !latest.is_full() {
            return written.map_err(|error| FlushFailure {
                reason: "write_error",
                error: Box::new(error),
            });
        }
        if written.is_err() {
            metrics.record_heartbeat_flush_failed("write_error");
        }
        started = tokio::time::Instant::now();
    }
}

/// The flush loop of [`spawn_heartbeat_flusher_with`].
#[cfg(feature = "db")]
async fn stamped_heartbeat_loop(
    claim: TaskClaim,
    pool: Pool<AsyncPgConnection>,
    latest: LatestHeartbeat,
    cancel: CancellationToken,
    options: HeartbeatFlushOptions,
) {
    let flush_interval = Duration::from_secs(1);
    let task_id = claim.task_id;
    // The newest payload not yet written, with its send time. A failed flush
    // puts it back here. A retry writes that time, not the retry time.
    let mut pending: Option<Pending> = None;
    // Set when a write that blocked for an interval or more succeeds. The
    // next wait then ends as soon as a newer heartbeat is in the slot.
    let mut after_blocked_write = false;

    loop {
        if cancel.is_cancelled() {
            tracing::debug!(task_id = %task_id, "heartbeat flusher cancelled");
            break;
        }
        // Wait for the interval, or for a newer heartbeat after a blocked write.
        let waited = if std::mem::take(&mut after_blocked_write) {
            wait_for_newer_heartbeat(&latest, flush_interval, &cancel).await
        } else {
            tokio::select! {
                () = cancel.cancelled() => false,
                () = tokio::time::sleep(flush_interval) => true,
            }
        };
        if !waited {
            tracing::debug!(task_id = %task_id, "heartbeat flusher cancelled");
            break;
        }

        // Take the newest heartbeat. It replaces an unwritten older one.
        let newest = latest.take();
        if newest.is_some() {
            pending = newest;
        }

        // If we got at least one heartbeat, flush to DB.
        if let Some(mut beat) = pending.take() {
            let started = tokio::time::Instant::now();
            let outcome = flush(&pool, &claim, &mut beat, &latest, &options, flush_interval).await;
            // A write that blocked leaves the row with an old send time,
            // whether it then succeeds or fails. A newer heartbeat then goes
            // at once, so a timeout scanner does not see a live activity as
            // stale (issue #1788).
            let blocked = write_blocked(started.elapsed(), flush_interval);
            match outcome {
                Ok(ClaimWrite::Applied) => after_blocked_write = blocked,
                // The claim is no longer current (issue #1789). Stop the
                // activity, so this stale attempt does no more work.
                Ok(ClaimWrite::LeaseLost) => {
                    tracing::warn!(
                        task_id = %task_id,
                        worker_id = %claim.worker_id,
                        attempt = claim.attempt,
                        "activity lease lost on heartbeat; cancelling the activity"
                    );
                    cancel.cancel();
                    break;
                }
                Err(failure) => {
                    options
                        .metrics
                        .record_heartbeat_flush_failed(failure.reason);
                    if failure.error.is_pool_acquire_timeout() {
                        options
                            .metrics
                            .record_db_pool_acquire_timeout(SITE_HEARTBEAT_FLUSH);
                    }
                    tracing::warn!(
                        task_id = %task_id,
                        reason = failure.reason,
                        error = %failure.error,
                        "failed to flush heartbeat to database; retrying on the next tick"
                    );
                    after_blocked_write = blocked;
                    pending = Some(beat);
                }
            }
        }

        // Check cancellation after flush.
        if cancel.is_cancelled() {
            break;
        }
    }
    latest.close();
}

/// Whether the stamped flush loop writes the next heartbeat without the
/// usual pause (issue #1788).
///
/// Only a write that took `interval` or more qualifies. A quick write keeps
/// the steady rate of one write per interval.
#[cfg(feature = "db")]
fn write_blocked(took: Duration, interval: Duration) -> bool {
    took >= interval
}

/// Wait for the next flush after a blocked write (issue #1788).
///
/// The wait ends at once when a heartbeat is in the slot. It also ends when
/// the activity sends one, or after `interval`.
///
/// Returns `false` when `cancel` fires.
#[cfg(feature = "db")]
async fn wait_for_newer_heartbeat(
    latest: &Latest,
    interval: Duration,
    cancel: &CancellationToken,
) -> bool {
    let deadline = tokio::time::Instant::now() + interval;
    loop {
        // Register before the check, so a publish between the two still wakes.
        let published = latest.published.notified();
        tokio::pin!(published);
        published.as_mut().enable();
        if latest.is_full() {
            return true;
        }
        tokio::select! {
            () = cancel.cancelled() => return false,
            () = tokio::time::sleep_until(deadline) => return true,
            () = &mut published => {}
        }
    }
}

/// The `site` label for a heartbeat flush acquire timeout.
#[cfg(feature = "db")]
const SITE_HEARTBEAT_FLUSH: &str = "heartbeat_flush";

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that the debounce logic keeps only the most recent payload.
    ///
    /// This test exercises the mpsc channel draining without a real database --
    /// it sends multiple payloads and verifies that `drain_latest` keeps only
    /// the last one.
    #[tokio::test]
    async fn heartbeat_batcher_debounces() {
        let (tx, mut rx) = mpsc::channel::<Value>(64);

        // Send 5 heartbeats rapidly.
        for i in 0..5 {
            tx.send(serde_json::json!({"progress": i}))
                .await
                .expect("send should succeed");
        }

        // Only the last payload should be kept.
        let latest = drain_latest(&mut rx).expect("should have received at least one heartbeat");
        assert_eq!(
            latest,
            serde_json::json!({"progress": 4}),
            "debounce should keep only the most recent heartbeat"
        );
    }

    /// A sink that counts its flushes.
    struct CountingSink(usize);

    impl HeartbeatSink for CountingSink {
        fn flush(&mut self, _payload: Value) -> impl Future<Output = FlushOutcome> + Send {
            self.0 += 1;
            std::future::ready(FlushOutcome::Continue)
        }
    }

    /// Cancellation stops the flusher at once, not at the next tick.
    ///
    /// Shuttle does not model time, so the Shuttle models cannot see a late
    /// stop. Paused tokio time can (issue #1800).
    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_the_flusher_before_the_next_tick() {
        let (_tx, rx) = mpsc::channel::<Value>(64);
        let cancel = CancellationToken::new();
        let interval = Duration::from_secs(3600);
        let start = tokio::time::Instant::now();
        let flusher = tokio::spawn(run_heartbeat_flusher(
            rx,
            cancel.clone(),
            interval,
            CountingSink(0),
        ));
        tokio::task::yield_now().await;

        cancel.cancel();
        let exit = flusher.await.expect("the flusher must not panic");

        assert_eq!(exit, FlusherExit::Cancelled);
        assert!(
            start.elapsed() < interval,
            "the flusher must stop before its next tick, not after {:?}",
            start.elapsed()
        );
    }

    /// Verify that an empty channel results in no flush.
    #[tokio::test]
    async fn heartbeat_empty_channel_no_flush() {
        let (_tx, mut rx) = mpsc::channel::<Value>(64);

        assert!(
            drain_latest(&mut rx).is_none(),
            "empty channel should produce no payload"
        );
    }

    // -- Issue #1788: the stamped flusher that the worker uses -------------

    #[cfg(feature = "db")]
    mod stamped {
        use super::*;

        /// Send `beats` in order. Take the slot after the beat at
        /// `take_after`, as a flush would.
        fn keep_newest_of(
            beats: Vec<StampedHeartbeat>,
            take_after: Option<usize>,
        ) -> Option<std::time::Instant> {
            let latest = Latest::default();
            for (index, beat) in beats.into_iter().enumerate() {
                assert!(latest.publish(beat));
                if take_after == Some(index) {
                    latest.take();
                }
            }
            latest.take().map(|beat| beat.sent_order)
        }

        /// A send puts the heartbeat in the slot before it returns (issue
        /// #1788). A flush that checks the slot after a write thus sees every
        /// heartbeat sent before the check.
        #[test]
        fn a_sent_heartbeat_is_in_the_slot_when_the_send_returns() {
            let slot = HeartbeatSlot(LatestHeartbeat::default());
            assert!(slot.send(beat_at(1)));
            assert!(slot.0.is_full());
            assert_eq!(
                slot.0.take().map(|beat| beat.sent_order),
                Some(beat_at(1).sent_order)
            );
        }

        /// A send after the flush loop stops returns `false`, so the activity
        /// learns that its heartbeats go nowhere.
        #[test]
        fn a_send_after_the_flusher_stops_fails() {
            let slot = HeartbeatSlot(LatestHeartbeat::default());
            slot.0.close();
            assert!(!slot.send(beat_at(1)));
        }

        /// One process start, shared by every test beat, so their send
        /// order compares.
        static BASE: std::sync::LazyLock<std::time::Instant> =
            std::sync::LazyLock::new(std::time::Instant::now);

        /// A beat sent `secs` seconds after `BASE`, stamped in that order.
        fn beat_at(secs: u64) -> StampedHeartbeat {
            StampedHeartbeat {
                details: Value::Null,
                sent_order: *BASE + Duration::from_secs(secs),
                sequence: secs,
            }
        }

        /// Two senders can read the same `Instant` (issue #1788). The stamp
        /// sequence still orders them, so the later stamp stays when the
        /// earlier one is published after it.
        #[test]
        fn a_tied_send_time_keeps_the_later_stamp() {
            let latest = Latest::default();
            let later = StampedHeartbeat {
                details: serde_json::json!({"step": 2}),
                sent_order: *BASE,
                sequence: 2,
            };
            let earlier = StampedHeartbeat {
                details: serde_json::json!({"step": 1}),
                sent_order: *BASE,
                sequence: 1,
            };
            assert!(latest.publish(later));
            assert!(latest.publish(earlier));
            assert_eq!(
                latest.take().map(|beat| beat.payload),
                Some(serde_json::json!({"step": 2}))
            );
        }

        /// A heartbeat sent while the loop waits after a blocked write ends
        /// the wait at once (issue #1788).
        #[tokio::test(start_paused = true)]
        async fn a_publish_after_a_blocked_write_ends_the_wait() {
            let interval = Duration::from_secs(1);
            let latest = LatestHeartbeat::default();
            let cancel = CancellationToken::new();
            let start = tokio::time::Instant::now();
            let waiter = {
                let latest = Arc::clone(&latest);
                tokio::spawn(
                    async move { wait_for_newer_heartbeat(&latest, interval, &cancel).await },
                )
            };
            tokio::time::sleep(interval / 10).await;
            assert!(latest.publish(StampedHeartbeat::now(Value::Null)));
            assert!(waiter.await.expect("join"));
            assert!(
                start.elapsed() < interval,
                "the wait took {:?}; the publish must end it",
                start.elapsed()
            );
        }

        /// Only a write that took an interval or more skips the pause
        /// (issue #1788).
        #[test]
        fn only_a_slow_write_counts_as_blocked() {
            let interval = Duration::from_secs(1);
            assert!(write_blocked(interval * 3, interval));
            assert!(write_blocked(interval, interval));
            assert!(!write_blocked(interval / 10, interval));
        }

        /// A heartbeat that arrives late keeps the newer one (issue #1788). Two
        /// senders stamp before they send, so they can arrive out of order.
        #[test]
        fn an_older_heartbeat_does_not_replace_a_newer_one() {
            let newest = keep_newest_of(vec![beat_at(20), beat_at(10)], None);
            assert_eq!(newest, Some(beat_at(20).sent_order));
        }

        /// An older heartbeat that arrives after a flush took the newer one is
        /// dropped. A write of it would move `last_heartbeat_at` backwards.
        #[test]
        fn an_older_heartbeat_after_a_flush_is_dropped() {
            let newest = keep_newest_of(vec![beat_at(20), beat_at(10)], Some(0));
            assert_eq!(newest, None);
        }

        // -- Issue #1788: bounded flush and failure counter ---------------------

        use std::time::Instant;

        #[derive(Default)]
        struct FlushFailures(Mutex<Vec<String>>);

        impl crate::telemetry::MetricsRecorder for FlushFailures {
            fn record_heartbeat_flush_failed(&self, reason: &str) {
                self.0.lock().expect("lock").push(reason.to_owned());
            }

            fn record_db_pool_acquire_timeout(&self, site: &str) {
                self.0.lock().expect("lock").push(format!("site:{site}"));
            }
        }

        /// A pool aimed at a listener that never answers. Its only slot never
        /// frees, which models a pool whose every connection is held.
        async fn silent_pool() -> (tokio::net::TcpListener, Pool<AsyncPgConnection>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind an ephemeral loopback port");
            let addr = listener.local_addr().expect("listener has a local address");
            let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
                AsyncPgConnection,
            >::new(format!("postgres://silent@{addr}/silent"));
            let pool = Pool::builder(manager)
                .max_size(1)
                .build()
                .expect("pool builds without connecting");
            (listener, pool)
        }

        #[tokio::test]
        async fn flush_heartbeat_returns_a_typed_timeout_within_the_bound() {
            let (_listener, pool) = silent_pool().await;
            let bound = Duration::from_millis(200);
            let started = Instant::now();
            let outcome = tokio::time::timeout(
                Duration::from_secs(5),
                flush_heartbeat(
                    &pool,
                    &TaskClaim::new(uuid::Uuid::new_v4(), "w-1", 1),
                    serde_json::json!({"p": 1}),
                    bound,
                ),
            )
            .await
            .expect("a heartbeat flush must not hang past its bound");
            let err = outcome.expect_err("a silent database cannot take a heartbeat");
            assert!(err.is_pool_acquire_timeout(), "{err}");
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "{:?}",
                started.elapsed()
            );
        }

        /// Records when each flush failure happens.
        #[derive(Default)]
        struct FailureTimes(Mutex<Vec<Instant>>);

        impl crate::telemetry::MetricsRecorder for FailureTimes {
            fn record_heartbeat_flush_failed(&self, _reason: &str) {
                self.0.lock().expect("lock").push(Instant::now());
            }
        }

        /// A flush that blocks for an interval and then fails is followed at
        /// once by a newer heartbeat (issue #1788). The old row is stale by
        /// then, so a further pause could let a scanner reclaim the activity.
        #[tokio::test]
        async fn a_newer_heartbeat_follows_a_blocked_failed_flush_at_once() {
            let (_listener, pool) = silent_pool().await;
            let failures = Arc::new(FailureTimes::default());
            let cancel = CancellationToken::new();
            let bound = Duration::from_millis(1500);
            let tx = spawn_heartbeat_flusher_with(
                TaskClaim::new(uuid::Uuid::new_v4(), "w-1", 1),
                pool,
                cancel.clone(),
                HeartbeatFlushOptions {
                    acquire_timeout: bound,
                    metrics: Arc::clone(&failures) as Arc<dyn crate::telemetry::MetricsRecorder>,
                    shards: Arc::from([0]),
                },
            );
            assert!(tx.send(serde_json::json!({"p": 1})));
            // The first flush starts after one interval and blocks for `bound`.
            tokio::time::sleep(Duration::from_millis(1500)).await;
            assert!(tx.send(serde_json::json!({"p": 2})));

            let deadline = Instant::now() + Duration::from_secs(10);
            let times = loop {
                let times = failures.0.lock().expect("lock").clone();
                if times.len() >= 2 {
                    break times;
                }
                assert!(Instant::now() < deadline, "failures seen: {}", times.len());
                tokio::time::sleep(Duration::from_millis(20)).await;
            };
            cancel.cancel();
            let gap = times[1] - times[0];
            assert!(
                gap < bound + Duration::from_millis(700),
                "the retry came {gap:?} after the blocked failure; it must not wait an interval"
            );
        }

        /// One payload, two failed ticks: the flusher keeps the payload and
        /// counts each failure.
        #[tokio::test]
        async fn a_failed_flush_is_counted_and_retried_on_the_next_tick() {
            let (_listener, pool) = silent_pool().await;
            let failures = Arc::new(FlushFailures::default());
            let cancel = CancellationToken::new();
            let tx = spawn_heartbeat_flusher_with(
                TaskClaim::new(uuid::Uuid::new_v4(), "w-1", 1),
                pool,
                cancel.clone(),
                HeartbeatFlushOptions {
                    acquire_timeout: Duration::from_millis(100),
                    metrics: Arc::clone(&failures) as Arc<dyn crate::telemetry::MetricsRecorder>,
                    shards: Arc::from([0]),
                },
            );
            assert!(tx.send(serde_json::json!({"p": 1})));

            let deadline = Instant::now() + Duration::from_secs(6);
            loop {
                let seen = failures.0.lock().expect("lock").clone();
                let reasons: Vec<&String> =
                    seen.iter().filter(|r| !r.starts_with("site:")).collect();
                if reasons.len() >= 2 {
                    assert!(reasons.iter().all(|r| *r == "acquire_timeout"), "{seen:?}");
                    assert!(seen.iter().any(|r| r == "site:heartbeat_flush"), "{seen:?}");
                    break;
                }
                assert!(Instant::now() < deadline, "flush failures seen: {seen:?}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            cancel.cancel();
        }

        /// Records each pool wait with its shard label.
        #[derive(Default)]
        struct PoolWaits(Mutex<Vec<u16>>);

        impl crate::telemetry::MetricsRecorder for PoolWaits {
            fn record_db_pool_wait(&self, shard: u16, _seconds: f64) {
                self.0.lock().expect("lock").push(shard);
            }
        }

        /// The flusher records its pool wait under its own shard, even when the
        /// acquire times out (issue #1815).
        #[tokio::test]
        async fn a_flush_records_its_pool_wait_under_its_shard() {
            let (_listener, pool) = silent_pool().await;
            let waits = Arc::new(PoolWaits::default());
            let cancel = CancellationToken::new();
            let tx = spawn_heartbeat_flusher_with(
                TaskClaim::new(uuid::Uuid::new_v4(), "w-1", 1),
                pool,
                cancel.clone(),
                HeartbeatFlushOptions {
                    acquire_timeout: Duration::from_millis(100),
                    metrics: Arc::clone(&waits) as Arc<dyn crate::telemetry::MetricsRecorder>,
                    shards: Arc::from([3]),
                },
            );
            assert!(tx.send(serde_json::json!({"p": 1})));

            let deadline = Instant::now() + Duration::from_secs(6);
            loop {
                let seen = waits.0.lock().expect("lock").clone();
                if !seen.is_empty() {
                    assert!(seen.iter().all(|shard| *shard == 3), "{seen:?}");
                    break;
                }
                assert!(Instant::now() < deadline, "no pool wait recorded");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            cancel.cancel();
        }
    }
}
