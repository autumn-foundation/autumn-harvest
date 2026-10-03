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

use std::future::Future;
use std::time::Duration;

use serde_json::Value;

// Shuttle swaps these types under `cfg(shuttle)` (issue #1800). See
// `crate::shuttle_sync`.
use crate::shuttle_sync::{CancellationToken, mpsc, select, sleep};

#[cfg(feature = "db")]
use crate::queue::{ClaimWrite, TaskClaim};
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
#[cfg(feature = "db")]
#[must_use]
pub fn spawn_heartbeat_flusher(
    claim: TaskClaim,
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
) -> mpsc::Sender<Value> {
    spawn_heartbeat_flusher_with_metrics(
        claim,
        pool,
        cancel,
        std::sync::Arc::new(crate::telemetry::NoOpMetrics),
        0,
    )
}

/// [`spawn_heartbeat_flusher`] that also records each flush (issue #1815).
///
/// Each flush records its pool wait under `shard` and its write as the
/// `heartbeat` op.
#[cfg(feature = "db")]
#[must_use]
pub fn spawn_heartbeat_flusher_with_metrics(
    claim: TaskClaim,
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
    metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
    shard: u16,
) -> mpsc::Sender<Value> {
    let (tx, rx) = mpsc::channel(64);

    // Plain tokio, not the shim: the `db` build never sets `--cfg shuttle`.
    tokio::spawn(heartbeat_loop(claim, pool, rx, cancel, metrics, shard));

    tx
}

/// The main heartbeat flushing loop.
#[cfg(feature = "db")]
async fn heartbeat_loop(
    claim: TaskClaim,
    pool: Pool<AsyncPgConnection>,
    rx: mpsc::Receiver<Value>,
    cancel: CancellationToken,
    metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
    shard: u16,
) {
    let task_id = claim.task_id;
    let sink = PgHeartbeatSink {
        claim,
        pool,
        metrics,
        shard,
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
    metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
    shard: u16,
}

#[cfg(feature = "db")]
impl HeartbeatSink for PgHeartbeatSink {
    async fn flush(&mut self, payload: Value) -> FlushOutcome {
        let task_id = self.claim.task_id;
        let wait_started = std::time::Instant::now();
        let conn = self.pool.get().await;
        self.metrics
            .record_db_pool_wait(self.shard, wait_started.elapsed().as_secs_f64());
        match conn {
            Ok(mut conn) => {
                let write_started = std::time::Instant::now();
                let written = crate::queue::record_heartbeat(&mut conn, &self.claim, payload).await;
                self.metrics.record_db_query_duration(
                    crate::telemetry::DbOp::Heartbeat,
                    write_started.elapsed().as_secs_f64(),
                );
                match written {
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
}
