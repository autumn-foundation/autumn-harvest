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

    /// Return a message to the broker for **retry** after a transient
    /// harvest-side failure.
    ///
    /// The message should come back *eventually*, with whatever natural
    /// backoff the broker provides — an SQS visibility timeout lapsing, a
    /// Kafka offset simply not being committed. Deliberately **not** an
    /// "immediately redeliver" request: a transient failure is usually harvest
    /// being under pressure, and hammering it with a tight redelivery loop
    /// makes that worse. Use [`Self::nack_for_dead_letter`] when the intent is
    /// to push a *poison* message toward the broker's own redrive policy.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Broker`] when the operation fails. Adapters
    /// with no explicit nack may implement this as a no-op: not acknowledging
    /// is already sufficient for redelivery.
    async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError>;

    /// Return a **poison** message to the broker so its own dead-letter
    /// routing claims it.
    ///
    /// Only called for a binding in
    /// [`DeadLetterMode::BrokerNative`][bn], which
    /// [`Self::has_native_dead_letter`] gates. Unlike [`Self::abandon`] this
    /// *does* want the fastest possible redelivery, because each one advances
    /// the broker's receive count toward the threshold that moves the message
    /// to its DLQ.
    ///
    /// The default delegates to [`Self::abandon`], which is right for any
    /// adapter whose redelivery mechanism is the same either way.
    ///
    /// [bn]: super::binding::DeadLetterMode::BrokerNative
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Broker`] when the operation fails.
    async fn nack_for_dead_letter(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
        self.abandon(handle).await
    }

    /// Current consumer lag, for adapters whose client exposes it.
    ///
    /// The default returns `None`, so an adapter that cannot report lag simply
    /// never emits the `harvest.connector.lag` gauge.
    async fn lag(&self) -> Option<i64> {
        None
    }

    /// Whether this broker has a dead-letter destination of its own that
    /// [`Self::abandon`] actually feeds.
    ///
    /// This gates [`super::binding::DeadLetterMode::BrokerNative`], which
    /// quarantines a poison message by *abandoning* it and letting the broker
    /// route it away. That only terminates if abandoning increments something
    /// the broker eventually acts on — SQS's `ApproximateReceiveCount` against
    /// a redrive policy's `maxReceiveCount`.
    ///
    /// Kafka has no such thing: `abandon` is a no-op (not committing is the
    /// whole mechanism), so a poison message would be re-read **forever** and
    /// never reach any dead-letter destination — precisely the partition wedge
    /// the poison-message handling exists to prevent. The default is therefore
    /// `false`, and a binding that asks for broker-native dead-lettering on
    /// such a source is rejected at build time rather than silently wedging.
    fn has_native_dead_letter(&self) -> bool {
        false
    }
}
