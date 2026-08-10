//! Broker-agnostic inbound-message types (issue #944).
//!
//! Everything here is deliberately free of any broker client type so the
//! binding/dispatch layer stays adapter-independent: Kafka, SQS, and any
//! follow-up adapter (NATS, `RabbitMQ`, Pub/Sub, Kinesis) all lower their
//! native message onto [`InboundMessage`].

use std::collections::BTreeMap;

/// Stable broker coordinates identifying *one* message.
///
/// These are what the connector derives its idempotency key from (see
/// [`crate::connector::idempotency`]), so they must be stable across a
/// redelivery of the same logical message:
///
/// * **Kafka** — `topic` / `partition` / `offset`, or the message key when the
///   binding is configured to dedupe by key instead.
/// * **SQS** — `MessageDeduplicationId` when present (FIFO queues), else
///   `MessageId`.
///
/// The variants are deliberately *not* `#[non_exhaustive]`: a new adapter that
/// cannot express its coordinates in one of these shapes should use
/// [`Self::Opaque`] rather than growing the enum.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessageCoordinates {
    /// Kafka's natural per-partition monotonic coordinate.
    KafkaOffset {
        /// The topic the message was consumed from.
        topic: String,
        /// The partition within `topic`.
        partition: i32,
        /// The message's offset within `partition`.
        offset: i64,
    },
    /// A broker-supplied opaque identifier: SQS `MessageDeduplicationId` /
    /// `MessageId`, a Kafka message key when the binding dedupes by key, or
    /// any other adapter-native stable id.
    Opaque {
        /// The logical stream the message came from (topic / queue name).
        stream: String,
        /// The broker-stable identifier for this message.
        id: String,
    },
}

impl MessageCoordinates {
    /// The logical stream (topic or queue) this message came from.
    #[must_use]
    pub fn stream(&self) -> &str {
        match self {
            Self::KafkaOffset { topic, .. } => topic,
            Self::Opaque { stream, .. } => stream,
        }
    }

    /// A stable, human-readable rendering used for logs, dead-letter records
    /// and (after bounding) the idempotency key.
    ///
    /// **Never** use this as a metric label — it is unbounded by construction
    /// (ADR-0001 §7).
    #[must_use]
    pub fn render(&self) -> String {
        match self {
            Self::KafkaOffset {
                topic,
                partition,
                offset,
            } => format!("{topic}:{partition}:{offset}"),
            Self::Opaque { stream, id } => format!("{stream}:{id}"),
        }
    }
}

/// One message pulled off a broker, lowered onto the connector's
/// adapter-independent shape.
#[derive(Debug, Clone)]
pub struct InboundMessage {
    /// Stable broker coordinates — the idempotency-key source.
    pub coordinates: MessageCoordinates,
    /// The raw message body. The binding's mapping function owns decoding;
    /// the connector never assumes a wire format (issue #944 scopes
    /// schema-registry decoding out of this slice).
    pub payload: Vec<u8>,
    /// Broker-supplied headers/attributes, if the adapter exposes them.
    pub headers: BTreeMap<String, String>,
    /// The adapter's own handle for this message, used to acknowledge or
    /// abandon it. Opaque to the runtime.
    pub handle: MessageHandle,
}

/// An adapter-owned acknowledgement token for a single message.
///
/// The runtime treats this as opaque and hands it straight back to
/// [`crate::connector::source::EventSource::ack`] /
/// [`crate::connector::source::EventSource::abandon`]. Adapters that need
/// richer state (an rdkafka `BorrowedMessage`, an SQS receipt handle) key
/// their own side table off [`Self::token`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageHandle {
    /// Adapter-defined token uniquely identifying the in-flight message.
    pub token: String,
    /// The partition this message belongs to, when the broker has a partition
    /// concept. Used by ordered-commit adapters (Kafka) to advance a
    /// per-partition high-water mark; `None` for partition-less brokers (SQS).
    pub partition: Option<i32>,
    /// The message's position within `partition`, when the broker has one.
    pub position: Option<i64>,
}

impl MessageHandle {
    /// A handle for a broker without partitions or positions (e.g. SQS).
    #[must_use]
    pub fn opaque(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            partition: None,
            position: None,
        }
    }

    /// A handle for a partitioned, positionally-ordered broker (e.g. Kafka).
    #[must_use]
    pub fn positioned(token: impl Into<String>, partition: i32, position: i64) -> Self {
        Self {
            token: token.into(),
            partition: Some(partition),
            position: Some(position),
        }
    }
}

/// The read-only context a binding's mapping function sees.
///
/// Mirrors [`autumn_harvest::WebhookCtx`]'s shape (issue #344): metadata plus
/// the raw body, with the typed decode left to the mapping function.
#[derive(Debug, Clone)]
pub struct MessageCtx {
    /// The operator-declared binding name this message arrived on.
    pub binding: &'static str,
    /// Stable broker coordinates for this message.
    pub coordinates: MessageCoordinates,
    /// Broker-supplied headers/attributes.
    pub headers: BTreeMap<String, String>,
    /// The raw message body.
    pub raw_body: Vec<u8>,
}

impl MessageCtx {
    /// Build a mapping context from a binding name and an inbound message.
    #[must_use]
    pub fn new(binding: &'static str, message: &InboundMessage) -> Self {
        Self {
            binding,
            coordinates: message.coordinates.clone(),
            headers: message.headers.clone(),
            raw_body: message.payload.clone(),
        }
    }

    /// Look up a broker header/attribute by name.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_coordinates_render_topic_partition_offset() {
        let c = MessageCoordinates::KafkaOffset {
            topic: "orders".to_string(),
            partition: 3,
            offset: 91,
        };
        assert_eq!(c.render(), "orders:3:91");
        assert_eq!(c.stream(), "orders");
    }

    #[test]
    fn opaque_coordinates_render_stream_and_id() {
        let c = MessageCoordinates::Opaque {
            stream: "orders-queue".to_string(),
            id: "dedup-1".to_string(),
        };
        assert_eq!(c.render(), "orders-queue:dedup-1");
        assert_eq!(c.stream(), "orders-queue");
    }

    #[test]
    fn message_handles_carry_partition_position_only_when_ordered() {
        let opaque = MessageHandle::opaque("receipt-abc");
        assert_eq!(opaque.partition, None);
        assert_eq!(opaque.position, None);

        let positioned = MessageHandle::positioned("orders:0:7", 0, 7);
        assert_eq!(positioned.partition, Some(0));
        assert_eq!(positioned.position, Some(7));
    }

    #[test]
    fn ctx_exposes_headers_and_raw_body() {
        let msg = InboundMessage {
            coordinates: MessageCoordinates::Opaque {
                stream: "q".to_string(),
                id: "1".to_string(),
            },
            payload: b"{\"a\":1}".to_vec(),
            headers: BTreeMap::from([("ce-type".to_string(), "order.created".to_string())]),
            handle: MessageHandle::opaque("1"),
        };
        let ctx = MessageCtx::new("orders", &msg);
        assert_eq!(ctx.binding, "orders");
        assert_eq!(ctx.header("ce-type"), Some("order.created"));
        assert_eq!(ctx.header("missing"), None);
        assert_eq!(ctx.raw_body, b"{\"a\":1}");
    }
}
