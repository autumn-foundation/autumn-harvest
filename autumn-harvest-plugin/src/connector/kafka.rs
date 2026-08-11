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
    /// Swappable so [`EventSource::recover`] can rebuild it in place when the
    /// commit prefix wedges. Guarded by a `std::sync::Mutex` that is only ever
    /// held long enough to clone the `Arc` out — never across an `.await`, so
    /// it cannot block the runtime or deadlock with a concurrent rebuild.
    consumer: std::sync::Mutex<Arc<StreamConsumer>>,
    topic: String,
    /// Retained so a rebuild produces an identically-configured consumer.
    config: KafkaSourceConfig,
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
        Ok(Self {
            consumer: std::sync::Mutex::new(Self::build_consumer(config)?),
            topic: config.topic.clone(),
            config: config.clone(),
        })
    }

    /// Create a consumer subscribed to `config.topic`.
    fn build_consumer(config: &KafkaSourceConfig) -> Result<Arc<StreamConsumer>, ConnectorError> {
        let consumer: StreamConsumer = config
            .to_client_config()
            .create()
            .map_err(|e| ConnectorError::Config(format!("kafka consumer: {e}")))?;
        consumer
            .subscribe(&[config.topic.as_str()])
            .map_err(|e| ConnectorError::Config(format!("kafka subscribe: {e}")))?;
        Ok(Arc::new(consumer))
    }

    /// The current consumer.
    ///
    /// Clones the `Arc` out under a short lock so callers can `.await` on it
    /// without holding the mutex.
    fn consumer(&self) -> Arc<StreamConsumer> {
        Arc::clone(
            &self
                .consumer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// Copy one borrowed Kafka message into an owned [`InboundMessage`].
///
/// Pure over the fields it needs, so the coordinate/handle shape is testable
/// without a broker.
fn to_inbound(
    topic: &str,
    partition: i32,
    offset: i64,
    key: Option<Vec<u8>>,
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
        // The record key is what Kafka partitions by, so surfacing it is what
        // makes the documented per-key ordering remedy actually reachable
        // from a mapping function (`ctx.key_str()`).
        key,
        headers,
        handle,
    }
}

/// Whether a `recv()` error should be deferred so the records already drained
/// in this pass are still returned.
///
/// **This is a message-loss guard, not a nicety.** Every record handed back by
/// `recv()` has advanced librdkafka's *local* consumer position, whether or
/// not it ever reaches the runtime. If a mid-batch error discarded them, the
/// next poll would start at a higher offset, that offset would establish the
/// [`OffsetTracker`][t]'s floor for the partition, and committing it would
/// assert every lower offset is done — silently skipping records that were
/// never dispatched. Returning the partial batch keeps them on the normal
/// dispatch-then-commit path; the error resurfaces on the next poll, which by
/// then has nothing drained to lose.
///
/// [t]: crate::connector::OffsetTracker
const fn defer_recv_error(drained: usize) -> bool {
    drained > 0
}

#[async_trait]
impl EventSource for KafkaSource {
    fn stream(&self) -> &str {
        &self.topic
    }

    fn subscription_identity(&self) -> Option<String> {
        Some(subscription_identity_for(&self.config))
    }

    async fn receive(
        &self,
        max: usize,
        timeout: std::time::Duration,
    ) -> Result<Vec<InboundMessage>, ConnectorError> {
        let mut batch = Vec::new();
        // Snapshot the consumer once: a rebuild mid-pass must not split this
        // batch across two generations.
        let consumer = self.consumer();
        // Wait `timeout` for the first message, then drain whatever else is
        // already buffered without blocking again.
        let mut budget = timeout;
        while batch.len() < max {
            let next = tokio::time::timeout(budget, consumer.recv()).await;
            let Ok(result) = next else { break };
            let message = match result {
                Ok(message) => message,
                Err(e) if defer_recv_error(batch.len()) => {
                    // Hand back what we already drained; the next poll surfaces
                    // the error with an empty batch. Dropping these records
                    // would lose them permanently (see `defer_recv_error`).
                    tracing::warn!(
                        topic = %self.topic,
                        drained = batch.len(),
                        error = %e,
                        "kafka receive failed mid-batch; yielding the drained records first"
                    );
                    return Ok(batch);
                }
                Err(e) => {
                    return Err(ConnectorError::Broker(format!("kafka receive: {e}")));
                }
            };

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
                message.key().map(<[u8]>::to_vec),
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
        self.consumer()
            .commit(&tpl, CommitMode::Async)
            .map_err(|e| ConnectorError::Broker(format!("kafka commit: {e}")))
    }

    async fn abandon(&self, _handle: &MessageHandle) -> Result<(), ConnectorError> {
        // Kafka has no per-message nack. Simply not committing is sufficient:
        // the message is re-read after a rebalance or restart, and dedupes on
        // the connector's derived idempotency key.
        Ok(())
    }

    fn abandon_redelivers(&self) -> bool {
        // Not committing is the whole mechanism, so `abandon` above is a no-op
        // and `recv()` has already advanced the local position past the
        // message: nothing hands it back while this consumer lives. The
        // runtime must treat a retry here as a wedge and rebuild the consumer
        // (`recover`), which re-reads from the last commit.
        false
    }

    /// Rebuild the consumer so the blocked offset is re-read.
    ///
    /// This is the retry a stalled prefix calls for. `recv()` has already
    /// advanced librdkafka's *local* position past the blocked message, so
    /// nothing hands it back while this consumer lives — but a fresh consumer
    /// rejoins the group and starts from the last **committed** offset, which
    /// is precisely the message the runtime is waiting on.
    ///
    /// The old consumer is dropped once the last in-flight `Arc` to it goes,
    /// which closes its group membership and triggers a rebalance.
    async fn recover(&self) -> Result<bool, ConnectorError> {
        let config = self.config.clone();
        // Creating and subscribing a consumer are blocking librdkafka calls.
        let rebuilt = tokio::task::spawn_blocking(move || Self::build_consumer(&config))
            .await
            .map_err(|e| {
                ConnectorError::Broker(format!("kafka consumer rebuild panicked: {e}"))
            })??;

        *self
            .consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = rebuilt;
        tracing::warn!(
            topic = %self.topic,
            "kafka consumer rebuilt; re-reading from the last committed offset"
        );
        Ok(true)
    }

    async fn lag(&self) -> Option<i64> {
        let consumer = self.consumer();
        let topic = self.topic.clone();
        // `fetch_watermarks` is a blocking librdkafka call.
        tokio::task::spawn_blocking(move || {
            let assignment = consumer.assignment().ok()?;
            // The DURABLE group offset, not `position()`. `position()` is the
            // local next-fetch cursor: it advances the moment a record is
            // fetched, regardless of whether that record has been dispatched,
            // is being retried, or is stuck behind a blocked commit prefix. So
            // a consumer wedged with a large uncommitted backlog — precisely
            // the condition this gauge exists to expose — would report its lag
            // falling to zero, while a restart replayed everything from the
            // last commit.
            let committed = consumer.committed(std::time::Duration::from_secs(2)).ok()?;
            let mut total: i64 = 0;
            for elem in assignment.elements() {
                let partition = elem.partition();
                let (low, high) = consumer
                    .fetch_watermarks(&topic, partition, std::time::Duration::from_secs(2))
                    .ok()?;
                let offset = committed
                    .find_partition(&topic, partition)
                    .map_or(Offset::Invalid, |p| p.offset());
                total = total.saturating_add(partition_lag(low, high, offset));
            }
            Some(total)
        })
        .await
        .ok()
        .flatten()
    }
}

/// The physical-subscription identity for a consumer group on a topic.
///
/// The group is load-bearing and cannot be dropped: two consumers on one topic
/// under **distinct** group ids each receive the whole stream, which is the
/// sanctioned way to fan one topic out to two targets. Identifying a
/// subscription by topic alone would reject exactly that. Two consumers in the
/// *same* group split the partitions between them, so each binding would see
/// only a subset — the clash this identity exists to catch.
///
/// The separator is `\u{1}`, a byte no Kafka group id or topic name may
/// contain, so `("a", "b/c")` and `("a/b", "c")` cannot alias each other.
#[must_use]
fn subscription_identity_for(config: &KafkaSourceConfig) -> String {
    // Read the values back out of the built config rather than off the struct
    // fields, so the identity is whatever librdkafka will ACTUALLY join.
    // `to_client_config` applies `extra` after the declared fields, so a
    // `.property("group.id", ...)` override wins — and reading `config.group_id`
    // here would accept two configs that land in one group, which then splits
    // the partitions between them and silently starves both targets. Deriving
    // from the built config makes the precedence single-sourced: there is no
    // second copy of the ordering rule to drift.
    let built = config.to_client_config();
    let brokers = built.get("bootstrap.servers").unwrap_or(&config.brokers);
    let group = built.get("group.id").unwrap_or(&config.group_id);
    // Brokers are load-bearing in the other direction: two independent
    // clusters exposing the same topic under the same group id are two
    // subscriptions, not one, and omitting them would reject that fan-in.
    // `\u{1}` cannot appear in a broker list, group id or topic, so no triple
    // can alias another.
    format!("kafka:{brokers}\u{1}{group}\u{1}{}", config.topic)
}

/// Outstanding records for one partition, from its watermarks and the group's
/// committed offset.
///
/// Nothing committed yet means the whole retained backlog is outstanding: this
/// consumer is pinned to `auto.offset.reset = earliest`, so a restart reads
/// from the low watermark. Reporting zero for such a partition would hide a
/// consumer that has processed nothing.
fn partition_lag(low: i64, high: i64, committed: Offset) -> i64 {
    let current = match committed {
        // Retention can advance `low` past an old commit. Those records are
        // gone from the log, so subtracting from the stale commit would report
        // a backlog the consumer can never read -- a permanent false alarm on
        // exactly the gauge operators page on.
        Offset::Offset(o) => o.max(low),
        _ => low,
    };
    (high - current).max(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_identity_uses_the_effective_group_and_brokers() {
        // `to_client_config` applies `extra` AFTER the declared fields, so a
        // `.property("group.id", ...)` override is what librdkafka actually
        // joins. Reading the declared field instead would accept two configs
        // that end up in one group, and the group would then split the
        // partitions between them -- each target silently missing messages.
        let overridden = KafkaSourceConfig::new("b:9092", "declared", "orders")
            .property("group.id", "effective");
        assert_eq!(
            subscription_identity_for(&overridden),
            subscription_identity_for(&KafkaSourceConfig::new("b:9092", "effective", "orders")),
            "an overridden group.id must be what the identity reports"
        );
        // Two DIFFERENT declared groups colliding on one override is exactly
        // the case the pointer check and the declared-field check both miss.
        let a = KafkaSourceConfig::new("b:9092", "g1", "orders").property("group.id", "shared");
        let b = KafkaSourceConfig::new("b:9092", "g2", "orders").property("group.id", "shared");
        assert_eq!(subscription_identity_for(&a), subscription_identity_for(&b));
        // A `bootstrap.servers` override is identity-bearing for the same
        // reason, in the other direction: two clusters are independent
        // subscriptions even when the group and topic match exactly, so
        // omitting the brokers would REJECT a legitimate multi-cluster fan-in.
        assert_ne!(
            subscription_identity_for(&KafkaSourceConfig::new("east:9092", "g", "orders")),
            subscription_identity_for(&KafkaSourceConfig::new("west:9092", "g", "orders")),
            "two independent clusters are not one subscription"
        );
        assert_eq!(
            subscription_identity_for(
                &KafkaSourceConfig::new("declared:9092", "g", "orders")
                    .property("bootstrap.servers", "effective:9092")
            ),
            subscription_identity_for(&KafkaSourceConfig::new("effective:9092", "g", "orders")),
        );
    }

    #[test]
    fn subscription_identity_pairs_the_group_with_the_topic() {
        // The group is load-bearing: two consumers on one topic under
        // DISTINCT group ids each get the full stream, which is the fan-out
        // the duplicate-source panic message itself recommends. Identifying a
        // subscription by topic alone would reject it.
        let id = |brokers, group, topic| {
            subscription_identity_for(&KafkaSourceConfig::new(brokers, group, topic))
        };
        assert_eq!(
            id("b:9092", "g1", "orders"),
            "kafka:b:9092\u{1}g1\u{1}orders"
        );
        assert_ne!(id("b:9092", "g1", "orders"), id("b:9092", "g2", "orders"));
        // Same group, different topics are also distinct subscriptions.
        assert_ne!(
            id("b:9092", "g1", "orders"),
            id("b:9092", "g1", "shipments")
        );
        // A `/` in a topic name must not let one triple alias another.
        assert_ne!(id("b:9092", "a", "b/c"), id("b:9092", "a/b", "c"));
    }

    #[test]
    fn a_committed_offset_below_the_low_watermark_is_clamped() {
        // Kafka retention advances `low` past a group's old commit: those
        // records are gone from the log, so counting them reports a backlog
        // the consumer can never read and cannot ever work off. Only the 100
        // records still between `low` and `high` are outstanding.
        assert_eq!(partition_lag(1000, 1100, Offset::Offset(0)), 100);
    }

    #[test]
    fn an_uncommitted_partition_reports_the_whole_retained_backlog() {
        // `auto.offset.reset = earliest`, so a restart replays from `low`.
        assert_eq!(partition_lag(1000, 1100, Offset::Invalid), 100);
        assert_eq!(partition_lag(0, 50, Offset::Beginning), 50);
    }

    #[test]
    fn a_committed_offset_inside_the_window_subtracts_normally() {
        assert_eq!(partition_lag(1000, 1100, Offset::Offset(1040)), 60);
    }

    #[test]
    fn a_caught_up_partition_never_reports_negative_lag() {
        assert_eq!(partition_lag(1000, 1100, Offset::Offset(1100)), 0);
        // A commit past `high` is not expected, but must not underflow into a
        // huge unsigned-looking number through `saturating_add`.
        assert_eq!(partition_lag(1000, 1100, Offset::Offset(1200)), 0);
    }

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
        let msg = to_inbound("orders", 3, 91, None, b"x".to_vec(), BTreeMap::new());
        assert_eq!(msg.coordinates.render(), "orders:3:91");
        assert_eq!(msg.handle.partition, Some(3));
        assert_eq!(msg.handle.position, Some(91));
        assert_eq!(msg.handle.token, "orders:3:91");
    }

    #[test]
    fn the_record_key_reaches_the_mapping_context() {
        // The documented per-key ordering remedy tells an author to derive the
        // `workflow_id` from the partition key, so the key must actually be
        // reachable from `MessageCtx` — it was not until issue #944's review.
        let msg = to_inbound(
            "orders",
            0,
            7,
            Some(b"tenant-42".to_vec()),
            b"{}".to_vec(),
            BTreeMap::new(),
        );
        assert_eq!(msg.key.as_deref(), Some(b"tenant-42".as_slice()));

        let ctx = crate::connector::MessageCtx::new("orders", &msg);
        assert_eq!(ctx.key_str(), Some("tenant-42"));
    }

    #[test]
    fn a_keyless_record_reports_no_key_rather_than_an_empty_one() {
        let msg = to_inbound("orders", 0, 7, None, b"{}".to_vec(), BTreeMap::new());
        assert!(msg.key.is_none());
        assert_eq!(
            crate::connector::MessageCtx::new("orders", &msg).key_str(),
            None
        );
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

    #[test]
    fn a_recv_error_after_partial_drain_yields_the_batch_instead_of_dropping_it() {
        // Records already drained from the consumer have advanced librdkafka's
        // LOCAL position, whether or not they ever reach the runtime. Dropping
        // them on a later `recv()` error is silent message loss: the next poll
        // starts at a higher offset, that offset establishes the tracker's
        // floor, its completion commits, and the dropped records are asserted
        // done by the high-water mark without ever having been dispatched.
        assert!(
            defer_recv_error(1),
            "a partial batch must be returned, not dropped",
        );
        assert!(defer_recv_error(12));
        // With nothing drained there is nothing to lose, so the error is the
        // only useful thing to report.
        assert!(!defer_recv_error(0));
    }
}
