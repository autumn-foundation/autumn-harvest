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

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::deadpool::Pool;

use crate::error::{HarvestError, HarvestResult};
use crate::telemetry::MetricsRecorder;

/// Spawn a background heartbeat flusher for the given task.
///
/// Returns an `mpsc::Sender<Value>` that the activity should use to send
/// heartbeat payloads. The flusher task will:
///
/// 1. Wait up to 1 second for heartbeats to arrive.
/// 2. Drain all pending heartbeats, keeping only the most recent.
/// 3. Call `queue::record_heartbeat()` to update the DB timestamp and payload.
/// 4. Repeat until the cancellation token is triggered.
///
/// The returned sender has a buffer of 64 messages -- if the activity sends
/// heartbeats faster than that without the flusher draining, sends will
/// await (backpressure).
///
/// [`crate::pool::acquire_bound`] limits each pool acquire. This variant
/// records no metrics. Use [`spawn_heartbeat_flusher_with`] to count failures.
#[must_use]
pub fn spawn_heartbeat_flusher(
    task_id: Uuid,
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
) -> mpsc::Sender<Value> {
    let options = HeartbeatFlushOptions {
        acquire_timeout: crate::pool::acquire_bound(&pool),
        metrics: Arc::new(crate::telemetry::NoOpMetrics),
        claim: None,
    };
    spawn_heartbeat_flusher_with(task_id, pool, cancel, options)
}

/// Options for [`spawn_heartbeat_flusher_with`] (issue #1788).
#[derive(Clone)]
pub struct HeartbeatFlushOptions {
    /// The bound on each pool acquire.
    pub acquire_timeout: Duration,
    /// Receives `harvest.heartbeat.flush_failed` and
    /// `harvest.db.pool_acquire_timeout{site="heartbeat_flush"}`.
    pub metrics: Arc<dyn MetricsRecorder>,
    /// The claim that owns the task. `Some` makes each write check it, so a
    /// late heartbeat cannot reach a newer attempt. `None` checks only that
    /// the task is `RUNNING`.
    pub claim: Option<HeartbeatClaim>,
}

/// The claim a heartbeat belongs to: the task row's `attempt` and `worker_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeartbeatClaim {
    /// `harvest_task_queue.attempt` at claim time.
    pub attempt: i32,
    /// The claiming worker.
    pub worker_id: String,
}

/// [`spawn_heartbeat_flusher`] with an explicit acquire bound and metrics
/// sink (issue #1788).
///
/// A failed flush keeps its payload. The next tick sends it again, unless a
/// newer payload replaces it. A task that is no longer `RUNNING` drops the
/// payload, because no later flush can succeed.
#[must_use]
pub fn spawn_heartbeat_flusher_with(
    task_id: Uuid,
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
    options: HeartbeatFlushOptions,
) -> mpsc::Sender<Value> {
    let (tx, rx) = mpsc::channel(64);

    tokio::spawn(heartbeat_loop(task_id, pool, rx, cancel, options));

    tx
}

/// Write one heartbeat. `acquire_timeout` limits the pool acquire.
///
/// # Errors
///
/// [`crate::error::HarvestError::PoolAcquireTimeout`] when the acquire bound
/// elapses. [`crate::error::HarvestError::PoolAcquireFailed`] when the pool
/// fails in another way. [`crate::error::HarvestError::Database`] when the
/// write fails.
pub async fn flush_heartbeat(
    pool: &Pool<AsyncPgConnection>,
    task_id: Uuid,
    payload: Value,
    acquire_timeout: Duration,
) -> HarvestResult<()> {
    flush(pool, task_id, None, payload, acquire_timeout)
        .await
        .map_err(|failure| *failure.error)
}

/// A failed flush and its `harvest.heartbeat.flush_failed` reason label.
///
/// The error is boxed, because `HarvestError` is large
/// (`clippy::result_large_err`).
struct FlushFailure {
    reason: &'static str,
    error: Box<HarvestError>,
}

async fn flush(
    pool: &Pool<AsyncPgConnection>,
    task_id: Uuid,
    claim: Option<&HeartbeatClaim>,
    payload: Value,
    acquire_timeout: Duration,
) -> Result<(), FlushFailure> {
    let mut conn = crate::pool::acquire(pool, acquire_timeout)
        .await
        .map_err(|error| FlushFailure {
            reason: if error.is_pool_acquire_timeout() {
                "acquire_timeout"
            } else {
                "acquire_error"
            },
            error: Box::new(error),
        })?;
    let written = match claim {
        Some(claim) => {
            crate::queue::record_heartbeat_for_claim(
                &mut conn,
                task_id,
                claim.attempt,
                &claim.worker_id,
                payload,
            )
            .await
        }
        None => crate::queue::record_heartbeat(&mut conn, task_id, payload).await,
    };
    written.map_err(|error| FlushFailure {
        reason: "write_error",
        error: Box::new(error),
    })
}

/// The main heartbeat flushing loop.
async fn heartbeat_loop(
    task_id: Uuid,
    pool: Pool<AsyncPgConnection>,
    mut rx: mpsc::Receiver<Value>,
    cancel: CancellationToken,
    options: HeartbeatFlushOptions,
) {
    let flush_interval = Duration::from_secs(1);
    // The newest payload not yet written. A failed flush puts it back here.
    let mut pending: Option<Value> = None;

    loop {
        // Wait for either: a heartbeat arrives, the interval expires, or cancellation.
        tokio::select! {
            () = cancel.cancelled() => {
                tracing::debug!(task_id = %task_id, "heartbeat flusher cancelled");
                break;
            }
            () = tokio::time::sleep(flush_interval) => {
                // Interval elapsed -- drain and flush.
            }
        }

        // Drain all pending heartbeats, keeping only the most recent.
        while let Ok(payload) = rx.try_recv() {
            pending = Some(payload);
        }

        // If we got at least one heartbeat, flush to DB.
        if let Some(payload) = pending.take()
            && let Err(failure) = flush(
                &pool,
                task_id,
                options.claim.as_ref(),
                payload.clone(),
                options.acquire_timeout,
            )
            .await
        {
            if matches!(*failure.error, HarvestError::NotFound(_)) {
                // The task finished, went back to the queue, or has a newer
                // claim. A retry cannot succeed.
                tracing::debug!(
                    task_id = %task_id,
                    "task is no longer running; dropping the heartbeat"
                );
            } else {
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
                pending = Some(payload);
            }
        }

        // Check cancellation after flush.
        if cancel.is_cancelled() {
            break;
        }
    }
}

/// The `site` label for a heartbeat flush acquire timeout.
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
    /// it sends multiple payloads and verifies that `try_recv` draining keeps
    /// only the last one.
    #[tokio::test]
    async fn heartbeat_batcher_debounces() {
        let (tx, mut rx) = mpsc::channel::<Value>(64);

        // Send 5 heartbeats rapidly.
        for i in 0..5 {
            tx.send(serde_json::json!({"progress": i}))
                .await
                .expect("send should succeed");
        }

        // Simulate the flusher's drain logic: keep only the most recent.
        let mut latest: Option<Value> = None;
        while let Ok(payload) = rx.try_recv() {
            latest = Some(payload);
        }

        // Only the last payload should be kept.
        let latest = latest.expect("should have received at least one heartbeat");
        assert_eq!(
            latest,
            serde_json::json!({"progress": 4}),
            "debounce should keep only the most recent heartbeat"
        );
    }

    /// Verify that an empty channel results in no flush.
    #[tokio::test]
    async fn heartbeat_empty_channel_no_flush() {
        let (_tx, mut rx) = mpsc::channel::<Value>(64);

        // Drain an empty channel.
        let mut latest: Option<Value> = None;
        while let Ok(payload) = rx.try_recv() {
            latest = Some(payload);
        }

        assert!(latest.is_none(), "empty channel should produce no payload");
    }

    // -- Issue #1788: bounded flush and failure counter ---------------------

    use std::sync::Mutex;
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
            flush_heartbeat(&pool, Uuid::new_v4(), serde_json::json!({"p": 1}), bound),
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

    /// One payload, two failed ticks: the flusher keeps the payload and
    /// counts each failure.
    #[tokio::test]
    async fn a_failed_flush_is_counted_and_retried_on_the_next_tick() {
        let (_listener, pool) = silent_pool().await;
        let failures = Arc::new(FlushFailures::default());
        let cancel = CancellationToken::new();
        let tx = spawn_heartbeat_flusher_with(
            Uuid::new_v4(),
            pool,
            cancel.clone(),
            HeartbeatFlushOptions {
                acquire_timeout: Duration::from_millis(100),
                metrics: Arc::clone(&failures) as Arc<dyn crate::telemetry::MetricsRecorder>,
                claim: None,
            },
        );
        tx.send(serde_json::json!({"p": 1})).await.expect("send");

        let deadline = Instant::now() + Duration::from_secs(6);
        loop {
            let seen = failures.0.lock().expect("lock").clone();
            let reasons: Vec<&String> = seen.iter().filter(|r| !r.starts_with("site:")).collect();
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
}
