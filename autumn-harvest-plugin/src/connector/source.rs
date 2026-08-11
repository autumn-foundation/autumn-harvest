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
    /// A partition's commit prefix is permanently blocked: a message was
    /// retried rather than settled, and on a positional broker nothing hands
    /// it back until the consumer is rebuilt.
    ///
    /// Distinct from [`Self::Broker`] because the response differs: a broker
    /// error is transient and worth re-polling the same consumer after a
    /// backoff, whereas re-polling a wedged consumer accomplishes nothing —
    /// it has to be rebuilt (see [`EventSource::recover`]).
    #[error(
        "partition {partition} commit prefix is blocked: {held} completed offsets held behind an \
         unsettled head (bound {threshold}); the consumer must be rebuilt to redeliver it"
    )]
    Stalled {
        /// The partition whose prefix cannot advance.
        partition: i32,
        /// How many completed offsets are held behind the blocked head.
        held: usize,
        /// The configured bound that was exceeded.
        threshold: usize,
    },
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

    /// Rebuild this source's underlying client, so messages it has already
    /// drained but not settled are handed back.
    ///
    /// Called when the runtime detects that a partition's commit prefix is
    /// **permanently blocked**: a message was retried rather than settled, and
    /// on a positional broker [`Self::abandon`] cannot force redelivery
    /// in-session because the local consumer position has already advanced
    /// past it. Rebuilding the consumer makes it re-read from the last
    /// committed offset, which is what actually performs the retry.
    ///
    /// Returns `true` when the source rebuilt itself, `false` when it has no
    /// such capability — the two must be distinguishable, or the runtime would
    /// loop forever believing it had fixed something. The default is `false`,
    /// which is correct for every source that does not need this: SQS's
    /// visibility timeout already redelivers an abandoned message, so its
    /// prefix cannot wedge in the first place.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Config`] or [`ConnectorError::Broker`] when
    /// the rebuild itself fails.
    async fn recover(&self) -> Result<bool, ConnectorError> {
        Ok(false)
    }

    /// Whether [`Self::abandon`] actually causes the message to come back.
    ///
    /// True for any broker with per-message acknowledgement and its own
    /// redelivery clock — SQS, whose visibility timeout lapses and hands the
    /// message back with no further action from us.
    ///
    /// **False for a positional broker.** On Kafka `abandon` is a no-op by
    /// construction (not committing *is* the mechanism), and the consumer's
    /// local read position has already advanced past the message, so nothing
    /// hands it back within the session: the message is not retried, it is
    /// dropped, and its offset blocks the partition's commit prefix forever.
    /// The runtime treats a retry on such a source as an immediate wedge and
    /// rebuilds the consumer (see [`Self::recover`]), which re-reads from the
    /// last commit and is what actually performs the retry.
    ///
    /// The default is `true` — the conservative choice, because it only ever
    /// declines to rebuild. An adapter that returns `true` when it should not
    /// silently loses retried messages, so a positional adapter must override
    /// this.
    fn abandon_redelivers(&self) -> bool {
        true
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

    /// A stable name for the **physical subscription** this source consumes,
    /// if the adapter can state one.
    ///
    /// Two sources sharing an identity compete for the same messages, so each
    /// binding sees an arbitrary subset rather than the whole stream. The
    /// plugin rejects such a pair at build time. Object identity cannot catch
    /// it: separately constructed sources over one queue or one consumer group
    /// have distinct pointers.
    ///
    /// It is a *subscription* identity, not a *stream* identity — the two
    /// differ wherever the broker has a consumer-group concept. Two Kafka
    /// consumers on one topic under **distinct group ids** each receive the
    /// whole stream, which is the sanctioned way to fan one topic out to two
    /// targets, so Kafka's identity pairs the group with the topic. SQS has no
    /// such concept — every receiver on a queue competes — so its identity is
    /// the queue URL.
    ///
    /// The default is `None`: an adapter that cannot name its subscription is
    /// not evidence of a clash, and a `None` never matches another `None`.
    /// Such a source keeps the object-identity check only. Override this when
    /// the adapter knows what it is subscribed to; returning an identity that
    /// is *coarser* than the real subscription would reject legitimate
    /// fan-out, so include everything that makes two consumers independent.
    fn subscription_identity(&self) -> Option<String> {
        None
    }
}
