//! Live progress from inside an activity (issue #2268).
//!
//! [`ActivityContext::publish_progress`](crate::context::ActivityContext::publish_progress)
//! sends a chunk while the activity runs, for example a model token. The
//! chunk is best-effort and never stored: it goes to an
//! [`ActivityProgressSink`], and nothing reaches history or
//! `harvest_stream_chunks`.
//!
//! Each chunk names its activity, its attempt and its index in the attempt.
//! A retry starts again at index 0 with a new attempt number, so a client
//! can drop the partial text of a failed attempt.
//!
//! On Postgres the default sink sends each chunk with `pg_notify` on the
//! progress channel of the run, the channel that `GET /workflows/{id}/stream`
//! reads. The route sends it as an `activity` frame with no `id:`, so it never
//! moves the `seq` of the workflow chunks. The frame data is the
//! [`ActivityProgress`] envelope under [`ACTIVITY_PROGRESS_KEY`] and the chunk
//! under `data`. A background task sends the chunks, so a slow database never
//! slows the activity. When its queue is full, a chunk drops.
//!
//! The task sends every chunk that waits in one statement, so a burst of
//! tokens costs one transaction. It takes a pool connection when a chunk
//! comes and gives it back when its queue is empty. When it cannot get a
//! connection, the chunks that wait drop. The next chunk tries again.

use serde::{Deserialize, Serialize};

use crate::types::{ActivityExecId, ExecutionId};

/// The key of the activity envelope in a chunk on the progress channel.
pub const ACTIVITY_PROGRESS_KEY: &str = "_harvest_activity";

/// The most chunks that wait for the background sender of one attempt.
pub const ACTIVITY_PROGRESS_QUEUE: usize = 256;

/// One live chunk of one activity attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityProgress {
    /// The run that scheduled the activity.
    pub execution_id: ExecutionId,
    /// The activity invocation.
    pub activity_id: ActivityExecId,
    /// The registered activity name.
    pub activity_type: String,
    /// The attempt, from 1. A retry has a higher number.
    pub attempt: u32,
    /// The index of the chunk in its attempt, from 0.
    pub index: u64,
    /// The chunk. A chunk over the size cap is a truncation marker.
    pub chunk: serde_json::Value,
}

impl ActivityProgress {
    /// The chunk as it goes on the progress channel: the envelope under
    /// [`ACTIVITY_PROGRESS_KEY`], and the chunk under `data`.
    #[must_use]
    pub fn wire_chunk(&self) -> serde_json::Value {
        serde_json::json!({
            ACTIVITY_PROGRESS_KEY: {
                "activity_id": self.activity_id,
                "activity_type": self.activity_type,
                "attempt": self.attempt,
                "index": self.index,
            },
            "data": self.chunk,
        })
    }
}

/// Where the live chunks of an activity go.
///
/// A sink must not block. It drops a chunk that it cannot send.
pub trait ActivityProgressSink: Send + Sync {
    /// Send one chunk, or drop it.
    fn publish(&self, progress: ActivityProgress);
}

/// A sink that keeps every chunk, for tests.
#[derive(Debug, Default)]
pub struct CaptureProgressSink {
    chunks: std::sync::Mutex<Vec<ActivityProgress>>,
}

impl CaptureProgressSink {
    /// Every chunk so far, in order.
    #[must_use]
    pub fn chunks(&self) -> Vec<ActivityProgress> {
        self.chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ActivityProgressSink for CaptureProgressSink {
    fn publish(&self, progress: ActivityProgress) {
        self.chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(progress);
    }
}

/// The Postgres sink: `pg_notify` on the progress channel of the run, from
/// a background task.
///
/// The task sends every chunk that waits in one statement. It holds a pool
/// connection only while chunks wait, so an idle stream holds none. When it
/// cannot get a connection, every chunk that waits drops.
#[cfg(feature = "db")]
pub struct NotifyProgressSink {
    tx: tokio::sync::mpsc::Sender<ActivityProgress>,
}

#[cfg(feature = "db")]
impl NotifyProgressSink {
    /// Start the background sender on the current Tokio runtime. It ends
    /// when the sink drops and its queue is empty.
    ///
    /// # Panics
    ///
    /// Panics when no Tokio runtime runs.
    #[must_use]
    pub fn spawn(pool: crate::worker::DbPool) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ActivityProgress>(ACTIVITY_PROGRESS_QUEUE);
        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(ACTIVITY_PROGRESS_QUEUE);
            while let Some(first) = rx.recv().await {
                batch.push(first);
                let mut conn = match crate::pool::acquire_within_pool_bound(&pool).await {
                    Ok(conn) => conn,
                    Err(err) => {
                        // One failed acquire drops every chunk that waits.
                        // A busy pool then gets one acquire, not one per chunk.
                        take_waiting(&mut rx, &mut batch);
                        tracing::debug!(
                            error = %err,
                            dropped = batch.len(),
                            "no connection for live activity chunks; dropped"
                        );
                        batch.clear();
                        continue;
                    }
                };
                // The chunks that came during the acquire join the batch.
                take_waiting(&mut rx, &mut batch);
                while !batch.is_empty() {
                    let sent =
                        crate::notify::notify_activity_progress_batch(&mut conn, &batch).await;
                    if let Err(err) = sent {
                        tracing::debug!(
                            error = %err,
                            dropped = batch.len(),
                            "live activity chunks were dropped"
                        );
                        // The connection can be broken. The pool checks it
                        // before it gives it out again.
                        batch.clear();
                        break;
                    }
                    batch.clear();
                    take_waiting(&mut rx, &mut batch);
                }
                // The queue is empty. The connection goes back to the pool,
                // so a long model turn does not hold a worker connection.
                drop(conn);
            }
        });
        Self { tx }
    }
}

/// Move the chunks that wait in `rx` to `batch`, up to the queue size.
#[cfg(feature = "db")]
fn take_waiting(
    rx: &mut tokio::sync::mpsc::Receiver<ActivityProgress>,
    batch: &mut Vec<ActivityProgress>,
) {
    while batch.len() < ACTIVITY_PROGRESS_QUEUE {
        match rx.try_recv() {
            Ok(progress) => batch.push(progress),
            Err(_) => break,
        }
    }
}

#[cfg(feature = "db")]
impl ActivityProgressSink for NotifyProgressSink {
    fn publish(&self, progress: ActivityProgress) {
        // A full queue drops the chunk. Live chunks are best-effort.
        let _ = self.tx.try_send(progress);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wire_chunk_holds_the_envelope_and_the_data() {
        let progress = ActivityProgress {
            execution_id: ExecutionId::new(),
            activity_id: ActivityExecId::new(),
            activity_type: "agent_model_turn".to_owned(),
            attempt: 2,
            index: 7,
            chunk: serde_json::json!({"text": "hel"}),
        };
        let wire = progress.wire_chunk();
        assert_eq!(wire[ACTIVITY_PROGRESS_KEY]["attempt"], 2);
        assert_eq!(wire[ACTIVITY_PROGRESS_KEY]["index"], 7);
        assert_eq!(wire["data"]["text"], "hel");
    }

    #[cfg(feature = "db")]
    #[test]
    fn the_envelope_fits_the_notify_limit_at_the_size_cap() {
        let progress = ActivityProgress {
            execution_id: ExecutionId::new(),
            activity_id: ActivityExecId::new(),
            activity_type: "agent_model_turn".to_owned(),
            attempt: u32::MAX,
            index: u64::MAX,
            chunk: serde_json::Value::Null,
        };
        let big = ActivityProgress {
            chunk: serde_json::json!("x".repeat(crate::context::PROGRESS_CHUNK_MAX_BYTES - 2)),
            ..progress
        };
        let bytes = serde_json::to_vec(&crate::notify::ProgressNotifyPayload {
            seq: u64::MAX,
            chunk: big.wire_chunk(),
            activity: true,
        })
        .unwrap()
        .len();
        assert!(bytes < 8_000, "{bytes}");
    }
}
