//! The broker adapter seam (issue #944).
//!
//! [`EventSource`] is the *only* thing a new broker adapter has to implement.
//! Everything else is broker-agnostic and shared: binding descriptors,
//! idempotency-key derivation, ack ordering, poison isolation, backpressure and
//! metrics. So NATS, `RabbitMQ`, Pub/Sub and Kinesis adapters are follow-ups,
//! not rewrites.

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
    /// A partition's commit prefix is permanently blocked.
    ///
    /// A message was retried rather than settled. On a positional broker,
    /// nothing hands it back until the consumer is rebuilt.
    ///
    /// It is distinct from [`Self::Broker`] because the response differs. A
    /// broker error is transient, so the runtime re-polls the same consumer
    /// after a backoff. A re-poll of a wedged consumer does nothing. The
    /// consumer must be rebuilt (see [`EventSource::recover`]).
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

/// The future that an [`EventSource`] or a dead-letter sink method returns.
pub type ConnectorFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// A broker-agnostic pull-based message source.
///
/// # Ack contract
///
/// The runtime calls [`Self::ack`] **only after** the message's dispatch is
/// durable in Postgres. An idempotent replay and a durable throttle park also
/// count. A message whose dispatch failed transiently goes to
/// [`Self::abandon`] or stays unacknowledged, so the broker's own redelivery
/// brings it back. An adapter must therefore never auto-acknowledge on receive.
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
///
/// Implement it with `#[async_trait]` on the `impl` block and `async fn`
/// methods. The method signatures here are the ones that `#[async_trait]`
/// generates. They are written out because `#[async_trait]` on the trait adds
/// a `#[must_use]` that clippy rejects as `double_must_use`.
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
    fn receive<'life0, 'async_trait>(
        &'life0 self,
        max: usize,
        timeout: std::time::Duration,
    ) -> ConnectorFuture<'async_trait, Result<Vec<InboundMessage>, ConnectorError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait;

    /// Acknowledge a message: commit the offset, delete from the queue.
    ///
    /// Called only after the dispatch outcome is durable.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Broker`] when the acknowledgement fails. The
    /// runtime logs and continues — a failed ack is safe, because the message
    /// will simply be redelivered and dedupe as an idempotent replay.
    fn ack<'life0, 'life1, 'async_trait>(
        &'life0 self,
        handle: &'life1 MessageHandle,
    ) -> ConnectorFuture<'async_trait, Result<(), ConnectorError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait;

    /// Return a message to the broker for **retry** after a transient
    /// harvest-side failure.
    ///
    /// The message should come back *eventually*, with the natural backoff of
    /// the broker: an SQS visibility timeout lapses, or a Kafka offset stays
    /// uncommitted. This is deliberately **not** an "immediately redeliver"
    /// request. A transient failure usually means harvest is under pressure,
    /// and a tight redelivery loop makes that worse. Use
    /// [`Self::nack_for_dead_letter`] to push a *poison* message toward the
    /// broker's own redrive policy.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Broker`] when the operation fails. Adapters
    /// with no explicit nack may implement this as a no-op: not acknowledging
    /// is already sufficient for redelivery.
    fn abandon<'life0, 'life1, 'async_trait>(
        &'life0 self,
        handle: &'life1 MessageHandle,
    ) -> ConnectorFuture<'async_trait, Result<(), ConnectorError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait;

    /// Return a **poison** message to the broker so its own dead-letter
    /// routing claims it.
    ///
    /// Only called for a binding in
    /// [`DeadLetterMode::BrokerNative`][bn], which
    /// [`Self::has_native_dead_letter`] gates. Unlike [`Self::abandon`], this
    /// *does* want the fastest possible redelivery. Each redelivery advances
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
    fn nack_for_dead_letter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        handle: &'life1 MessageHandle,
    ) -> ConnectorFuture<'async_trait, Result<(), ConnectorError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.abandon(handle).await })
    }

    /// Commit a **high-water mark** for `partition`, asserting that everything
    /// at or below `position` is durably handled.
    ///
    /// It is deliberately separate from [`Self::ack`], because it does not
    /// acknowledge one message. The runtime commits the *contiguous completed
    /// prefix*. The offset that prefix reaches is usually not the one whose
    /// settlement triggered the advance. When 5 completes before 4, the
    /// completion of 4 carries the mark to 5. The runtime may hold no handle
    /// for `position`, because its message settled earlier and was released.
    /// So there is nothing honest to pass to `ack`.
    ///
    /// Only a positional broker ever reaches this. A handle with no
    /// `partition`/`position` is acknowledged per-message through
    /// [`Self::ack`] and never comes here.
    ///
    /// The default delegates to [`Self::ack`] with a **token-less** handle
    /// that carries only the coordinates. That is right for any adapter whose
    /// commit is addressed by `(partition, position)`, such as the in-tree
    /// Kafka adapter. The token is empty, not borrowed from another message.
    /// So an adapter that *does* key off it cannot silently acknowledge the
    /// wrong record. It sees an obviously absent token, not a plausible wrong
    /// one. Such an adapter should override this.
    ///
    /// An adapter that cannot commit by position is not positional. It should
    /// leave `partition`/`position` unset on its handles
    /// ([`MessageHandle::opaque`]), so its messages are acknowledged
    /// individually.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Broker`] when the commit fails. Like a failed
    /// ack this is safe: the messages below the mark are redelivered and dedupe
    /// as idempotent replays.
    fn commit_position<'life0, 'async_trait>(
        &'life0 self,
        partition: i32,
        position: i64,
    ) -> ConnectorFuture<'async_trait, Result<(), ConnectorError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.ack(&MessageHandle {
                token: String::new(),
                partition: Some(partition),
                position: Some(position),
            })
            .await
        })
    }

    /// Current consumer lag, for adapters whose client exposes it.
    ///
    /// The default returns `None`, so an adapter that cannot report lag simply
    /// never emits the `harvest.connector.lag` gauge.
    ///
    /// # Contract: bound your own sample
    ///
    /// An implementation must abort within
    /// [`ADAPTER_LAG_SAMPLE_CEILING`](super::runtime::ADAPTER_LAG_SAMPLE_CEILING).
    /// The runtime budgets this call, but a timeout only drops the runtime's
    /// own future. It cannot stop work already under way. A sample on
    /// `spawn_blocking` is never stopped, because a blocking task is never
    /// cancelled. The runtime floors its budget at that ceiling, so it never
    /// gives up first. An implementation that outruns the ceiling breaks the
    /// other half of the bargain. Then abandoned samples accumulate against a
    /// broker that already struggles.
    fn lag<'life0, 'async_trait>(&'life0 self) -> ConnectorFuture<'async_trait, Option<i64>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { None })
    }

    /// Rebuild this source's underlying client, so messages it has already
    /// drained but not settled are handed back.
    ///
    /// The runtime calls this when a partition's commit prefix is
    /// **permanently blocked**. A message was retried rather than settled. On
    /// a positional broker, [`Self::abandon`] cannot force redelivery
    /// in-session, because the local consumer position is already past it. A
    /// rebuilt consumer re-reads from the last committed offset, and that
    /// performs the retry.
    ///
    /// Returns `true` when the source rebuilt itself, and `false` when it has
    /// no such capability. The two must differ, or the runtime loops forever
    /// in the belief that it fixed something. The default is `false`. That is
    /// correct for every source that does not need this. The SQS visibility
    /// timeout already redelivers an abandoned message, so its prefix cannot
    /// wedge.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Config`] or [`ConnectorError::Broker`] when
    /// the rebuild itself fails.
    fn recover<'life0, 'async_trait>(
        &'life0 self,
    ) -> ConnectorFuture<'async_trait, Result<bool, ConnectorError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { Ok(false) })
    }

    /// Whether [`Self::abandon`] actually causes the message to come back.
    ///
    /// True for any broker with per-message acknowledgement and its own
    /// redelivery clock. On SQS the visibility timeout lapses and hands the
    /// message back with no further action.
    ///
    /// **False for a positional broker.** On Kafka `abandon` is a no-op by
    /// construction, because not committing *is* the mechanism. The local read
    /// position of the consumer is already past the message. So nothing hands
    /// it back within the session. The message is dropped, not retried, and
    /// its offset blocks the partition's commit prefix forever. The runtime
    /// treats a retry on such a source as an immediate wedge. It rebuilds the
    /// consumer (see [`Self::recover`]), which re-reads from the last commit
    /// and so performs the retry.
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
    /// Kafka has no such counter. Its `abandon` is a no-op, because not
    /// committing is the whole mechanism. So a poison message is re-read
    /// **forever** and never reaches a dead-letter destination. That is the
    /// partition wedge that the poison-message handling exists to prevent. The
    /// default is therefore `false`. A binding that asks for broker-native
    /// dead-lettering on such a source fails at build time, so it cannot wedge
    /// silently.
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
    /// It is a *subscription* identity, not a *stream* identity. The two
    /// differ wherever the broker has a consumer-group concept. Two Kafka
    /// consumers on one topic under **distinct group ids** each receive the
    /// whole stream. That is the sanctioned way to fan one topic out to two
    /// targets, so the Kafka identity pairs the group with the topic. SQS has
    /// no such concept, because every receiver on a queue competes. So its
    /// identity is the queue URL.
    ///
    /// The default is `None`: an adapter that cannot name its subscription is
    /// not evidence of a clash, and a `None` never matches another `None`.
    /// Such a source keeps the object-identity check only. Override this when
    /// the adapter knows what it is subscribed to. An identity *coarser* than
    /// the real subscription rejects legitimate fan-out. So include everything
    /// that makes two consumers independent.
    fn subscription_identity(&self) -> Option<String> {
        None
    }
}
