//! Postgres LISTEN/NOTIFY helpers for wake-on-enqueue.
//!
//! Instead of polling the task queue on a fixed interval, workers can subscribe
//! to a Postgres NOTIFY channel and wake immediately when a new task is enqueued.
//! This module provides the channel naming convention, the notification payload
//! type, and a [`QueueListener`] that wraps `tokio-postgres` for async LISTEN.

use std::time::Duration;

use diesel::sql_types::Text;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::error::{HarvestError, HarvestResult};

// ---------------------------------------------------------------------------
// Channel naming
// ---------------------------------------------------------------------------

/// Convert a queue name to its Postgres NOTIFY channel name.
///
/// The convention is `harvest_queue_{name}` with hyphens replaced by
/// underscores (Postgres identifiers cannot contain hyphens).
///
/// # Examples
///
/// ```
/// # use autumn_harvest::notify::queue_channel;
/// assert_eq!(queue_channel("email-queue"), "harvest_queue_email_queue");
/// ```
#[must_use]
pub fn queue_channel(queue_name: &str) -> String {
    format!("harvest_queue_{}", queue_name.replace('-', "_"))
}

#[must_use]
fn quote_pg_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// NotifyPayload
// ---------------------------------------------------------------------------

/// Payload sent via Postgres NOTIFY when a task is enqueued.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NotifyPayload {
    /// The UUID of the newly enqueued task.
    pub task_id: Uuid,
}

/// Outcome of waiting on a queue listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueWaitOutcome {
    /// A notification payload arrived before the timeout elapsed.
    Notification(NotifyPayload),
    /// No payload arrived before `poll_interval` elapsed.
    TimedOut,
    /// The listener channel closed because the underlying connection died.
    ChannelClosed,
}

// ---------------------------------------------------------------------------
// Send notification (via diesel connection)
// ---------------------------------------------------------------------------

/// Send a `NOTIFY` on the appropriate channel for the given queue.
///
/// This is typically called immediately after [`crate::queue::enqueue()`] to
/// wake any listening workers.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the `NOTIFY` SQL fails.
pub async fn notify_task_enqueued(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    task_id: Uuid,
) -> HarvestResult<()> {
    let channel = queue_channel(queue_name);
    let payload = serde_json::to_string(&NotifyPayload { task_id })
        .map_err(|e| HarvestError::Database(format!("failed to serialize notify payload: {e}")))?;

    diesel::sql_query("SELECT pg_notify($1, $2)")
        .bind::<Text, _>(&channel)
        .bind::<Text, _>(&payload)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// QueueListener (using tokio-postgres)
// ---------------------------------------------------------------------------

/// Async listener for Postgres NOTIFY events on task queue channels.
///
/// Uses a dedicated `tokio-postgres` connection (separate from the diesel pool)
/// because `LISTEN` requires a long-lived connection that receives async
/// notifications. The connection is driven by a background task that forwards
/// notifications through an `mpsc` channel.
pub struct QueueListener {
    /// Client handle kept alive so the LISTEN connection stays open.
    _client: tokio_postgres::Client,
    /// Receiver for notifications forwarded by the connection driver task.
    rx: tokio::sync::mpsc::Receiver<tokio_postgres::Notification>,
    /// Background connection driver handle -- kept alive for the connection's lifetime.
    _connection_handle: tokio::task::JoinHandle<()>,
    /// Queue names this listener is subscribed to.
    queues: Vec<String>,
}

impl QueueListener {
    /// Connect to Postgres and subscribe to NOTIFY channels for the given queues.
    ///
    /// Spawns a background task that drives the connection and forwards
    /// [`Notification`]s through an internal channel. The connection stays
    /// alive as long as this `QueueListener` is held.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the connection or LISTEN fails.
    pub async fn connect(database_url: &str, queues: &[String]) -> HarvestResult<Self> {
        let (client, mut connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
            .await
            .map_err(|e| HarvestError::Database(format!("pg connect failed: {e}")))?;

        // Channel for forwarding notifications from the connection driver.
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        // Spawn a task that drives the connection via poll_message() so we can
        // intercept Notification async messages instead of discarding them (which
        // is what the default Future impl does).
        let handle = tokio::spawn(async move {
            use futures::future::poll_fn;

            loop {
                let msg = poll_fn(|cx| connection.poll_message(cx)).await;
                match msg {
                    Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                        if tx.send(n).await.is_err() {
                            // Receiver dropped -- listener was dropped, shut down.
                            break;
                        }
                    }
                    Some(Ok(_)) => {
                        // Notices and other async messages -- ignore.
                    }
                    Some(Err(e)) => {
                        tracing::error!(error = %e, "postgres listener connection error");
                        break;
                    }
                    None => {
                        // Connection closed cleanly.
                        break;
                    }
                }
            }
        });

        // Subscribe to all queue channels.
        for queue in queues {
            let channel = queue_channel(queue);
            let quoted_channel = quote_pg_identifier(&channel);
            client
                .batch_execute(&format!("LISTEN {quoted_channel}"))
                .await
                .map_err(|e| {
                    HarvestError::Database(format!("LISTEN {quoted_channel} failed: {e}"))
                })?;
        }

        Ok(Self {
            _client: client,
            rx,
            _connection_handle: handle,
            queues: queues.to_vec(),
        })
    }

    /// Wait for a notification or timeout after `poll_interval`.
    ///
    /// Returns `Some(payload)` if a notification arrived, or `None` on timeout.
    /// Workers use this in a loop: wake on notification or fall back to polling.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload fails to
    /// deserialize.
    pub async fn wait_for_notification(
        &mut self,
        poll_interval: Duration,
    ) -> HarvestResult<Option<NotifyPayload>> {
        match self.wait_for_notification_outcome(poll_interval).await? {
            QueueWaitOutcome::Notification(payload) => Ok(Some(payload)),
            QueueWaitOutcome::TimedOut | QueueWaitOutcome::ChannelClosed => Ok(None),
        }
    }

    /// Wait for a notification and distinguish timeout from listener shutdown.
    ///
    /// This is useful for callers that need to reconnect after the underlying
    /// LISTEN connection dies instead of treating every wake miss as a normal
    /// timeout.
    pub async fn wait_for_notification_outcome(
        &mut self,
        poll_interval: Duration,
    ) -> HarvestResult<QueueWaitOutcome> {
        match tokio::time::timeout(poll_interval, self.rx.recv()).await {
            Ok(Some(notification)) => {
                let payload: NotifyPayload = serde_json::from_str(notification.payload())
                    .map_err(|e| HarvestError::Database(format!("bad notify payload: {e}")))?;
                Ok(QueueWaitOutcome::Notification(payload))
            }
            Ok(None) => Ok(QueueWaitOutcome::ChannelClosed),
            Err(_elapsed) => Ok(QueueWaitOutcome::TimedOut),
        }
    }

    /// The queue names this listener is subscribed to.
    #[must_use]
    pub fn queues(&self) -> &[String] {
        &self.queues
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_name_for_queue() {
        assert_eq!(queue_channel("default"), "harvest_queue_default");
        assert_eq!(queue_channel("email-queue"), "harvest_queue_email_queue");
        assert_eq!(
            queue_channel("billing-high-priority"),
            "harvest_queue_billing_high_priority"
        );
    }

    #[test]
    fn notify_payload_roundtrips() {
        let original = NotifyPayload {
            task_id: Uuid::new_v4(),
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let deserialized: NotifyPayload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original.task_id, deserialized.task_id);
    }

    #[test]
    fn channel_name_no_hyphens_in_output() {
        let channel = queue_channel("a-b-c");
        assert!(
            !channel.contains('-'),
            "channel name must not contain hyphens: {channel}"
        );
    }

    #[test]
    fn quoted_identifier_escapes_embedded_quotes() {
        assert_eq!(
            quote_pg_identifier("harvest_queue_priority\"queue"),
            "\"harvest_queue_priority\"\"queue\""
        );
    }
}
