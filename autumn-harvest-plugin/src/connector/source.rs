//! The broker adapter seam (issue #944).
//!
//! [`EventSource`] is the *only* thing a new broker adapter has to implement.
//! Everything else — binding descriptors, idempotency-key derivation, ack
//! ordering, poison isolation, backpressure, metrics — is broker-agnostic and
//! shared, so NATS, `RabbitMQ`, Pub/Sub and Kinesis adapters are follow-ups
//! rather than rewrites.

use async_trait::async_trait;

use super::message::{InboundMessage, MessageHandle};

/// Errors an event source can surface.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// The broker client failed (connection, auth, protocol).
    #[error("broker error: {0}")]
    Broker(String),
    /// The adapter is misconfigured (bad brokers list, missing queue URL).
    #[error("connector configuration error: {0}")]
    Config(String),
    /// The source has been shut down and will yield no further messages.
    #[error("connector source is closed")]
    Closed,
}

/// A broker-agnostic pull-based message source.
///
/// # Ack contract
///
/// The runtime calls [`Self::ack`] **only after** the message's dispatch is
/// durable in Postgres (or was recognized as an idempotent replay, or was
/// durably parked by a throttle). A message whose dispatch failed transiently
/// is left for [`Self::abandon`] or simply not acknowledged, so the broker's
/// own redelivery brings it back. An adapter must therefore never
/// auto-acknowledge on receive.
///
/// # Ordering
///
/// [`Self::receive`] may return messages from several partitions. The runtime
/// dispatches up to the binding's `max_in_flight` concurrently and does
/// **not** preserve broker partition ordering across them — see the ordering
/// caveat in `docs/getting-started/13-broker-connectors.md`. Adapters whose
/// acknowledgement is a high-water mark (Kafka) must use
/// [`super::disposition::OffsetTracker`] so a committed offset can never run
/// ahead of an in-flight message.
#[async_trait]
pub trait EventSource: Send + Sync {
    /// The logical stream (topic or queue) this source consumes, matched
    /// against [`super::binding::SourceBinding::stream`].
    fn stream(&self) -> &str;

    /// Pull up to `max` messages, waiting at most `timeout` for the first.
    ///
    /// Returning an empty vec means "nothing available right now" and is not
    /// an error.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Broker`] when the client fails, or
    /// [`ConnectorError::Closed`] once the source has shut down.
    async fn receive(
        &self,
        max: usize,
        timeout: std::time::Duration,
    ) -> Result<Vec<InboundMessage>, ConnectorError>;

    /// Acknowledge a message: commit the offset, delete from the queue.
    ///
    /// Called only after the dispatch outcome is durable.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Broker`] when the acknowledgement fails. The
    /// runtime logs and continues — a failed ack is safe, because the message
    /// will simply be redelivered and dedupe as an idempotent replay.
    async fn ack(&self, handle: &MessageHandle) -> Result<(), ConnectorError>;

    /// Return a message to the broker for redelivery (nack / reset visibility).
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Broker`] when the operation fails. Adapters
    /// with no explicit nack may implement this as a no-op: not acknowledging
    /// is already sufficient for redelivery.
    async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError>;

    /// Current consumer lag, for adapters whose client exposes it.
    ///
    /// The default returns `None`, so an adapter that cannot report lag simply
    /// never emits the `harvest.connector.lag` gauge.
    async fn lag(&self) -> Option<i64> {
        None
    }
}
