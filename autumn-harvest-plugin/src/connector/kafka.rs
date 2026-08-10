//! Kafka [`EventSource`] adapter (issue #944), behind the `kafka` feature.
//!
//! The adapter is deliberately thin: it does no mapping, no idempotency, no
//! poison accounting and no backpressure — all of that is broker-agnostic and
//! lives in [`super::runtime`]. Its whole job is `receive` / `ack` / `abandon`
//! / `lag` over `rdkafka`.
//!
//! # Manual commit, and why the runtime gates it
//!
//! The consumer is configured with `enable.auto.commit = false`. A Kafka
//! commit is a **high-water mark**: committing offset N asserts that
//! everything below N is done. Because the runtime dispatches up to
//! `max_in_flight` messages concurrently, acking offset 5 while 4 is still in
//! flight would silently skip 4 after a crash. The runtime therefore routes
//! every ack through [`super::disposition::OffsetTracker`] and only calls
//! [`EventSource::ack`] once the completed prefix is contiguous — so by the
//! time this adapter commits `position + 1`, every lower offset really is
//! settled.
//!
//! # Build note
//!
//! `rdkafka`'s `cmake-build` feature compiles the vendored librdkafka, which
//! needs libcurl headers (`libcurl4-openssl-dev` on Debian/Ubuntu) even when
//! curl support is disabled. See the CI step alongside the `kafka` feature.

use async_trait::async_trait;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Headers as _;
use rdkafka::{ClientConfig, Message as _, Offset, TopicPartitionList};
use std::collections::BTreeMap;
use std::sync::Arc;

use super::message::{InboundMessage, MessageCoordinates, MessageHandle};
use super::source::{ConnectorError, EventSource};

/// Connection settings for a [`KafkaSource`].
#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    /// `bootstrap.servers`, e.g. `"localhost:9092"`.
    pub brokers: String,
    /// Consumer group id. Two connector replicas sharing a group id split the
    /// topic's partitions between them.
    pub group_id: String,
    /// The topic to consume.
    pub topic: String,
    /// Extra `librdkafka` properties (SASL, TLS, fetch tuning, …), applied
    /// after the defaults so they can override them — except
    /// `enable.auto.commit`, which is forced off (see the module docs).
    pub extra: Vec<(String, String)>,
}

impl KafkaSourceConfig {
    /// Minimal config: brokers, consumer group and topic.
    #[must_use]
    pub fn new(
        brokers: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            group_id: group_id.into(),
            topic: topic.into(),
            extra: Vec::new(),
        }
    }

    /// Set an extra `librdkafka` property.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra.push((key.into(), value.into()));
        self
    }

    /// Build the `librdkafka` client config this source will use.
    ///
    /// Exposed so a test can assert the manual-commit invariant without a
    /// broker.
    #[must_use]
    pub fn to_client_config(&self) -> ClientConfig {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group_id)
            // Start from the beginning for a brand-new group so a binding
            // added to an existing topic does not silently skip its backlog.
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false");
        for (k, v) in &self.extra {
            cfg.set(k.as_str(), v.as_str());
        }
        // Forced LAST so an `extra` entry can never re-enable auto-commit and
        // break the ack-after-commit contract (AC4).
        cfg.set("enable.auto.commit", "false");
        cfg
    }
}

/// A Kafka-backed [`EventSource`].
pub struct KafkaSource {
    consumer: Arc<StreamConsumer>,
    topic: String,
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl KafkaSource {
    /// Create a consumer and subscribe it to the configured topic.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Config`] when the client cannot be created or
    /// the subscription fails.
    pub fn connect(config: &KafkaSourceConfig) -> Result<Self, ConnectorError> {
        let consumer: StreamConsumer = config
            .to_client_config()
            .create()
            .map_err(|e| ConnectorError::Config(format!("kafka consumer: {e}")))?;
        consumer
            .subscribe(&[config.topic.as_str()])
            .map_err(|e| ConnectorError::Config(format!("kafka subscribe: {e}")))?;
        Ok(Self {
            consumer: Arc::new(consumer),
            topic: config.topic.clone(),
        })
    }
}

/// Copy one borrowed Kafka message into an owned [`InboundMessage`].
///
/// Pure over the four fields it needs, so the coordinate/handle shape is
/// testable without a broker.
fn to_inbound(
    topic: &str,
    partition: i32,
    offset: i64,
    payload: Vec<u8>,
    headers: BTreeMap<String, String>,
) -> InboundMessage {
    let coordinates = MessageCoordinates::KafkaOffset {
        topic: topic.to_string(),
        partition,
        offset,
    };
    let handle = MessageHandle::positioned(coordinates.render(), partition, offset);
    InboundMessage {
        coordinates,
        payload,
        headers,
        handle,
    }
}

#[async_trait]
impl EventSource for KafkaSource {
    fn stream(&self) -> &str {
        &self.topic
    }

    async fn receive(
        &self,
        max: usize,
        timeout: std::time::Duration,
    ) -> Result<Vec<InboundMessage>, ConnectorError> {
        let mut batch = Vec::new();
        // Wait `timeout` for the first message, then drain whatever else is
        // already buffered without blocking again.
        let mut budget = timeout;
        while batch.len() < max {
            let next = tokio::time::timeout(budget, self.consumer.recv()).await;
            let Ok(result) = next else { break };
            let message =
                result.map_err(|e| ConnectorError::Broker(format!("kafka receive: {e}")))?;

            let headers = message.headers().map_or_else(BTreeMap::new, |hs| {
                (0..hs.count())
                    .map(|i| hs.get(i))
                    .map(|h| {
                        (
                            h.key.to_string(),
                            h.value
                                .map(|v| String::from_utf8_lossy(v).into_owned())
                                .unwrap_or_default(),
                        )
                    })
                    .collect()
            });

            batch.push(to_inbound(
                message.topic(),
                message.partition(),
                message.offset(),
                message.payload().unwrap_or_default().to_vec(),
                headers,
            ));
            budget = std::time::Duration::ZERO;
        }
        Ok(batch)
    }

    async fn ack(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
        let (Some(partition), Some(offset)) = (handle.partition, handle.position) else {
            return Err(ConnectorError::Broker(format!(
                "kafka ack requires partition/offset coordinates, got token '{}'",
                handle.token
            )));
        };

        let mut tpl = TopicPartitionList::new();
        // Kafka's committed offset is the NEXT offset to read, so commit
        // `offset + 1`. The runtime has already established that every lower
        // offset in this partition is settled.
        tpl.add_partition_offset(&self.topic, partition, Offset::Offset(offset + 1))
            .map_err(|e| ConnectorError::Broker(format!("kafka offset list: {e}")))?;
        self.consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(|e| ConnectorError::Broker(format!("kafka commit: {e}")))
    }

    async fn abandon(&self, _handle: &MessageHandle) -> Result<(), ConnectorError> {
        // Kafka has no per-message nack. Simply not committing is sufficient:
        // the message is re-read after a rebalance or restart, and dedupes on
        // the connector's derived idempotency key.
        Ok(())
    }

    async fn lag(&self) -> Option<i64> {
        let consumer = Arc::clone(&self.consumer);
        let topic = self.topic.clone();
        // `fetch_watermarks` is a blocking librdkafka call.
        tokio::task::spawn_blocking(move || {
            let assignment = consumer.assignment().ok()?;
            let positions = consumer.position().ok()?;
            let mut total: i64 = 0;
            for elem in assignment.elements() {
                let partition = elem.partition();
                let (_, high) = consumer
                    .fetch_watermarks(&topic, partition, std::time::Duration::from_secs(2))
                    .ok()?;
                let position = positions
                    .find_partition(&topic, partition)
                    .map_or(Offset::Invalid, |p| p.offset());
                if let Offset::Offset(current) = position {
                    total = total.saturating_add((high - current).max(0));
                }
            }
            Some(total)
        })
        .await
        .ok()
        .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_commit_is_forced_off_even_when_a_caller_sets_it() {
        // AC4's foundation: an adapter that auto-commits acknowledges a
        // message before its dispatch is durable, so the invariant must not be
        // overridable through `extra`.
        let cfg = KafkaSourceConfig::new("localhost:9092", "g", "orders")
            .property("enable.auto.commit", "true");
        let client = cfg.to_client_config();
        assert_eq!(client.get("enable.auto.commit"), Some("false"));
    }

    #[test]
    fn config_carries_brokers_group_and_extras() {
        let cfg = KafkaSourceConfig::new("b:9092", "grp", "orders")
            .property("security.protocol", "SASL_SSL");
        let client = cfg.to_client_config();
        assert_eq!(client.get("bootstrap.servers"), Some("b:9092"));
        assert_eq!(client.get("group.id"), Some("grp"));
        assert_eq!(client.get("security.protocol"), Some("SASL_SSL"));
        // A brand-new group reads the backlog rather than skipping it.
        assert_eq!(client.get("auto.offset.reset"), Some("earliest"));
    }

    #[test]
    fn inbound_messages_carry_ordered_handles() {
        let msg = to_inbound("orders", 3, 91, b"x".to_vec(), BTreeMap::new());
        assert_eq!(msg.coordinates.render(), "orders:3:91");
        assert_eq!(msg.handle.partition, Some(3));
        assert_eq!(msg.handle.position, Some(91));
        assert_eq!(msg.handle.token, "orders:3:91");
    }

    #[tokio::test]
    async fn ack_without_coordinates_is_a_clear_error_not_a_silent_commit() {
        let cfg = KafkaSourceConfig::new("localhost:9092", "g", "orders");
        // Building the consumer needs no live broker (librdkafka connects
        // lazily), so this exercises the real ack guard.
        let Ok(source) = KafkaSource::connect(&cfg) else {
            return; // no librdkafka in this environment; nothing to assert
        };
        let err = source
            .ack(&MessageHandle::opaque("no-offset"))
            .await
            .expect_err("an opaque handle cannot address a Kafka offset");
        assert!(format!("{err}").contains("partition/offset"));
    }
}
