//! Background heartbeat flusher for activities.
//!
//! Activities send heartbeat payloads via an mpsc channel. This module spawns
//! a background Tokio task that receives those payloads, debounces them (keeping
//! only the most recent), and periodically flushes the heartbeat timestamp to
//! the database.
//!
//! The flusher runs every 1 second, draining all pending heartbeats and keeping
//! only the last one. This avoids hammering Postgres with per-heartbeat writes
//! while still providing timely liveness detection.

use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::deadpool::Pool;

/// Spawn a background heartbeat flusher for the given task.
///
/// Returns an `mpsc::Sender<Value>` that the activity should use to send
/// heartbeat payloads. The flusher task will:
///
/// 1. Wait up to 1 second for heartbeats to arrive.
/// 2. Drain all pending heartbeats, keeping only the most recent.
/// 3. Call `queue::record_heartbeat()` to update the DB timestamp.
/// 4. Repeat until the cancellation token is triggered.
///
/// The returned sender has a buffer of 64 messages -- if the activity sends
/// heartbeats faster than that without the flusher draining, sends will
/// await (backpressure).
#[must_use]
pub fn spawn_heartbeat_flusher(
    task_id: Uuid,
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
) -> mpsc::Sender<Value> {
    let (tx, rx) = mpsc::channel(64);

    tokio::spawn(heartbeat_loop(task_id, pool, rx, cancel));

    tx
}

/// The main heartbeat flushing loop.
async fn heartbeat_loop(
    task_id: Uuid,
    pool: Pool<AsyncPgConnection>,
    mut rx: mpsc::Receiver<Value>,
    cancel: CancellationToken,
) {
    let flush_interval = std::time::Duration::from_secs(1);

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
        let mut latest: Option<Value> = None;
        while let Ok(payload) = rx.try_recv() {
            latest = Some(payload);
        }

        // If we got at least one heartbeat, flush to DB.
        if latest.is_some() {
            match pool.get().await {
                Ok(mut conn) => {
                    if let Err(e) = crate::queue::record_heartbeat(&mut conn, task_id).await {
                        tracing::warn!(
                            task_id = %task_id,
                            error = %e,
                            "failed to flush heartbeat to database"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        task_id = %task_id,
                        error = %e,
                        "failed to acquire DB connection for heartbeat flush"
                    );
                }
            }
        }

        // Check cancellation after flush.
        if cancel.is_cancelled() {
            break;
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
}
