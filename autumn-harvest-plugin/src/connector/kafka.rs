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

/// Where a consumer group starts when it has **no committed offset** for a
/// partition — librdkafka's `auto.offset.reset`.
///
/// Only meaningful for an uncommitted partition: once the group has committed,
/// it resumes from the commit and this policy never applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetReset {
    /// Start at the low watermark: an uncommitted partition owes its whole
    /// retained log. Harvest's default, so a binding added to an existing topic
    /// does not silently skip the backlog.
    Earliest,
    /// Start at the high watermark. An uncommitted partition this source has
    /// never read from owes nothing, because a resume would land at the tail.
    /// Once it HAS read from one, [`PartitionAnchors`] pins where that
    /// resolution actually landed and the policy no longer applies — otherwise
    /// the baseline would chase the moving tail and report zero forever.
    Latest,
}

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

    /// The `auto.offset.reset` librdkafka will actually apply.
    ///
    /// Mirrors [`Self::to_client_config`]'s precedence exactly — the `earliest`
    /// default, then `extra` overriding it, last write winning — so the lag
    /// gauge cannot disagree with the running consumer about where an
    /// uncommitted partition resumes.
    ///
    /// Deliberately NOT forced the way `enable.auto.commit` is: auto-commit
    /// would break the ack-after-commit contract unconditionally, whereas an
    /// offset-reset policy is a legitimate caller choice (a new binding on a
    /// huge existing topic may well want only new messages). `latest` DOES
    /// threaten at-least-once while the group has no committed offset — a
    /// rebuild would re-resolve it against a tail the blocked record has
    /// already advanced past — but that is closed by anchoring rather than by
    /// forbidding the policy; see [`PartitionAnchors`]. An
    /// unrecognised value maps to `Earliest`, matching the default — librdkafka
    /// rejects a genuinely invalid value at client-create time, so such a
    /// string never reaches a running consumer.
    #[must_use]
    pub fn effective_offset_reset(&self) -> OffsetReset {
        self.extra
            .iter()
            .rev()
            .find(|(k, _)| k.eq_ignore_ascii_case("auto.offset.reset"))
            .map_or(OffsetReset::Earliest, |(_, v)| {
                match v.trim().to_ascii_lowercase().as_str() {
                    "latest" | "end" | "largest" => OffsetReset::Latest,
                    _ => OffsetReset::Earliest,
                }
            })
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

/// Per-partition record of where this source's group actually resumes.
///
/// # Why this exists
///
/// `auto.offset.reset` is only consulted when a partition has **no committed
/// offset** — and for a `latest` group that window is unbounded: nothing
/// commits until the first message settles, and if that first message keeps
/// failing, nothing ever does. In that window the reset policy is not a fixed
/// position but a *moving* one, because `latest` re-resolves against whatever
/// the high watermark happens to be at the moment it is asked. Two things
/// silently follow that moving tail:
///
/// * **Recovery.** [`EventSource::recover`] rebuilds the consumer to re-read the
///   blocked record. With no commit to resume from, the rebuild re-resolves
///   `latest` against the *current* tail — which the blocked record itself has
///   already advanced past — so it skips that record permanently while the
///   runtime reports a successful retry. That is message loss, not a delayed
///   delivery.
/// * **The lag gauge.** Baselining an uncommitted partition at the live high
///   watermark makes every sample `high - high = 0`. A group that started at
///   100 and is stuck while producers push the topic to 1000 reports an
///   all-clear for the 900 records it owes — masking exactly the
///   pre-first-commit outage the gauge exists to expose.
///
/// The fix for both is one fact: **the first offset this source ever read from
/// a partition is where `latest` resolved**, and that is a fixed anchor. It is
/// recorded on receive, raised by every commit, re-asserted synchronously
/// before a rebuild (which also makes any in-flight async commit durable), and
/// used as the lag baseline.
///
/// Committing that anchor is not a loss: for a `latest` group, "everything
/// below where I started is not mine" is precisely what the policy *means*.
/// All this does is make that decision durable instead of leaving it to be
/// re-derived against a tail that has since moved.
///
/// # Scope: a local fact, not a group one
///
/// An anchor records what **this source instance** read. Nothing evicts one
/// when a rebalance moves the partition elsewhere, so a long-lived source
/// accumulates floors for partitions another replica now owns. That matters
/// asymmetrically, and the two uses are guarded differently:
///
/// * **Committing** speaks for the whole group, so it is filtered by
///   [`committable_anchors`] down to partitions this consumer still owns and to
///   floors that strictly raise what the group has committed. Unfiltered, a
///   stale floor rewinds another replica's partition and replays records whose
///   idempotency claims have long expired — duplicate executions, not
///   deduplicated retries.
/// * **The lag baseline** is read-only and per-replica, so a stale anchor there
///   can only skew one gauge sample. It is deliberately *not* filtered by the
///   assignment: mid-rebalance `assignment()` is briefly empty, and dropping
///   anchors then would re-open the `latest` under-report above — a false
///   all-clear on the gauge, which is the failure direction that hurts. A
///   best-effort local baseline is the better trade there.
///
/// # Residual
///
/// A partition this replica has never read — one owned by another replica, or
/// any partition of a fresh process whose group never committed — has no anchor
/// and falls back to the reset policy. That is the honest answer, since no
/// local fact establishes otherwise; closing it would need cross-replica state
/// this adapter deliberately does not keep.
#[derive(Debug, Default)]
pub struct PartitionAnchors {
    /// `partition -> next offset to read`. `BTreeMap` so `pending_anchors` is
    /// deterministically ordered, which keeps the commit list assertable.
    floors: BTreeMap<i32, i64>,
}

impl PartitionAnchors {
    /// Note a record read from `partition` at `offset`.
    ///
    /// First read wins: it is where the group resolved to, and later records
    /// must never raise the floor past a record still awaiting settlement.
    pub fn record_read(&mut self, partition: i32, offset: i64) {
        self.floors.entry(partition).or_insert(offset);
    }

    /// Note that `next_offset` (Kafka's next-to-read) has been committed.
    ///
    /// Monotonic: an out-of-order or stale commit never rewinds the floor.
    pub fn record_commit(&mut self, partition: i32, next_offset: i64) {
        let floor = self.floors.entry(partition).or_insert(next_offset);
        *floor = (*floor).max(next_offset);
    }

    /// The resume floor for `partition`, if this source has read from it.
    #[must_use]
    pub fn anchor_for(&self, partition: i32) -> Option<i64> {
        self.floors.get(&partition).copied()
    }

    /// Every known floor, to be filtered by [`committable_anchors`] and
    /// committed before a rebuild.
    #[must_use]
    pub fn pending_anchors(&self) -> Vec<(i32, i64)> {
        self.floors.iter().map(|(p, o)| (*p, *o)).collect()
    }
}

/// The subset of `anchors` this replica may commit on behalf of the group.
///
/// A retained anchor is a fact about **this source's read history**, not about
/// the group. Nothing evicts one when a rebalance moves the partition
/// elsewhere, so the two diverge the moment ownership changes — and a commit
/// speaks for the whole group. Two conditions, each closing a hole the other
/// leaves open:
///
/// * **Assigned.** Only a partition this consumer currently owns. A stale
///   anchor for a partition another replica is advancing would assert a resume
///   point for work this replica is not doing: rewind it, and that replica's
///   successor replays everything from the stale floor — long after the
///   connector's idempotency claims for those records expired, so the replay
///   lands as duplicate executions rather than deduplicated retries.
/// * **Raises.** Only where the anchor is strictly ahead of what the group has
///   committed. Ownership alone is not enough, because [`record_read`] is
///   first-read-wins: correct within one continuous ownership span, stale
///   across a revoke-and-return. A partition read at 100, revoked, advanced to
///   5000 by another replica and then handed back still carries a floor of 100.
///
/// Together they make an anchor commit **monotonic and local**: it can only
/// raise a partition this replica owns. The case the anchor exists for — a
/// `latest` group that has never committed anywhere — passes both trivially,
/// since the partition was just read from and any floor raises "no floor".
///
/// `committed` returns the group's durable next-offset for a partition, or
/// `None` when it has never committed one.
///
/// [`record_read`]: PartitionAnchors::record_read
fn committable_anchors(
    anchors: &[(i32, i64)],
    assigned: &[i32],
    committed: impl Fn(i32) -> Option<i64>,
) -> Vec<(i32, i64)> {
    anchors
        .iter()
        .filter(|(partition, _)| assigned.contains(partition))
        .filter(|(partition, anchor)| committed(*partition).is_none_or(|c| *anchor > c))
        .copied()
        .collect()
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
    /// Where this group actually resumes, per partition. Survives a consumer
    /// rebuild, which is the whole point: see [`PartitionAnchors`]. Guarded by
    /// a `std::sync::Mutex` held only for the map access, never across an
    /// `.await`.
    anchors: std::sync::Mutex<PartitionAnchors>,
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
            anchors: std::sync::Mutex::new(PartitionAnchors::default()),
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

    /// Run `f` against the anchor map under a short lock.
    fn with_anchors<T>(&self, f: impl FnOnce(&mut PartitionAnchors) -> T) -> T {
        f(&mut self
            .anchors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
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

            // The first offset read from a partition is where this group
            // resolved to, and is the floor a rebuild must not skip past.
            self.with_anchors(|a| a.record_read(message.partition(), message.offset()));
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
            .map_err(|e| ConnectorError::Broker(format!("kafka commit: {e}")))?;
        // Raise the resume floor. The commit above is ASYNC, so it may not be
        // durable yet; recording it here lets `recover` re-assert it
        // synchronously before a rebuild rather than racing it.
        self.with_anchors(|a| a.record_commit(partition, offset + 1));
        Ok(())
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
    /// # Anchoring first, and why it is not optional
    ///
    /// "The last committed offset" is only a resume point if one exists. A
    /// group that has never committed — the unbounded window for a `latest`
    /// binding whose very first message keeps failing — resolves
    /// `auto.offset.reset` again on the rebuild, and `latest` resolves against
    /// the *current* tail, which the blocked record has itself advanced past.
    /// The rebuild would skip that record permanently while the runtime
    /// reported a successful retry: message loss dressed as a retry.
    ///
    /// So every resume floor this replica may speak for is committed
    /// **synchronously on the old consumer, before the new one is built** (see
    /// [`PartitionAnchors`]). That makes the docstring's promise true rather
    /// than conditional, and as a side effect makes any in-flight async
    /// ack-commit durable instead of racing the rebuild.
    ///
    /// "May speak for" is [`committable_anchors`]: a partition this consumer
    /// still owns, and a floor that strictly raises the group's committed
    /// offset. An anchor is a local read-history fact, and a commit is a
    /// group-wide statement; the filter is what keeps a retained floor from
    /// rewinding a partition that has since moved to another replica.
    ///
    /// A failed anchor commit fails the whole recovery rather than rebuilding
    /// anyway. Rebuilding on an unanchored group is exactly the skip this
    /// guards against, and the runtime's response to a failed `recover` is to
    /// back off and retry — which is the correct answer for a transient commit
    /// error, and infinitely better than a silent loss.
    ///
    /// The old consumer is dropped once the last in-flight `Arc` to it goes,
    /// which closes its group membership and triggers a rebalance.
    async fn recover(&self) -> Result<bool, ConnectorError> {
        let config = self.config.clone();
        let anchors = self.with_anchors(|a| a.pending_anchors());
        let topic = self.topic.clone();
        let previous = self.consumer();
        // Committing and creating a consumer are blocking librdkafka calls.
        let rebuilt = tokio::task::spawn_blocking(move || {
            // Skipped when nothing has been read: an EMPTY offset list is not a
            // no-op to librdkafka (it commits the current assignment), and
            // there is no floor to assert anyway.
            if !anchors.is_empty() {
                // Partitions this consumer still owns. A retained anchor is a
                // fact about local read history, not about the group, and the
                // two diverge once a rebalance moves a partition away -- see
                // `committable_anchors`.
                let assigned: Vec<i32> = previous
                    .assignment()
                    .map_err(|e| ConnectorError::Broker(format!("kafka assignment: {e}")))?
                    .elements_for_topic(&topic)
                    .iter()
                    .map(rdkafka::topic_partition_list::TopicPartitionListElem::partition)
                    .collect();

                // What the group has already committed for those, so an anchor
                // can only ever raise it. Skipped when nothing is assigned:
                // `committed_offsets` on an empty list would ask about the
                // whole assignment, and there is nothing to filter anyway.
                let mut wanted = TopicPartitionList::new();
                for partition in &assigned {
                    wanted.add_partition(&topic, *partition);
                }
                let committed = if assigned.is_empty() {
                    TopicPartitionList::new()
                } else {
                    previous
                        .committed_offsets(wanted, ANCHOR_COMMIT_TIMEOUT)
                        .map_err(|e| {
                            ConnectorError::Broker(format!("kafka committed offsets: {e}"))
                        })?
                };

                let committable = committable_anchors(&anchors, &assigned, |partition| {
                    match committed
                        .find_partition(&topic, partition)
                        .map(|p| p.offset())
                    {
                        Some(Offset::Offset(o)) => Some(o),
                        _ => None,
                    }
                });

                if !committable.is_empty() {
                    let mut tpl = TopicPartitionList::new();
                    for (partition, next_offset) in committable {
                        tpl.add_partition_offset(&topic, partition, Offset::Offset(next_offset))
                            .map_err(|e| {
                                ConnectorError::Broker(format!("kafka anchor offset list: {e}"))
                            })?;
                    }
                    previous
                        .commit(&tpl, CommitMode::Sync)
                        .map_err(|e| ConnectorError::Broker(format!("kafka anchor commit: {e}")))?;
                }
            }
            Self::build_consumer(&config)
        })
        .await
        .map_err(|e| ConnectorError::Broker(format!("kafka consumer rebuild panicked: {e}")))??;

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
        // The policy the RUNNING consumer uses, so an uncommitted partition is
        // baselined where that consumer would actually resume.
        let reset = self.config.effective_offset_reset();
        // Snapshotted so the blocking task owns it without holding the lock.
        let anchors = self.with_anchors(|a| PartitionAnchors {
            floors: a.pending_anchors().into_iter().collect(),
        });
        // `fetch_watermarks` is a blocking librdkafka call.
        tokio::task::spawn_blocking(move || {
            // One overall deadline for the whole walk, so an abandoned sample
            // stops rather than running on to completion on a blocking thread.
            let deadline = std::time::Instant::now() + LAG_SAMPLE_BUDGET;
            let budget = || {
                remaining_lag_budget(deadline, std::time::Instant::now())
                    .map(|left| left.min(LAG_CALL_TIMEOUT))
            };
            let timeout = budget()?;

            // Every partition of the topic, from metadata — deliberately NOT
            // `consumer.assignment()`. Consumer lag is a property of the GROUP,
            // but an assignment-scoped sum is a property of one replica: scale
            // the connector to four replicas and each reports roughly a quarter
            // of the backlog. The dashboard aggregates with `max by (source)`
            // (correct for SQS, where every replica reads the same whole-queue
            // depth), so an assignment-scoped Kafka sample would show the
            // largest replica's subtotal and hide a partition stalled on any of
            // the others — the exact failure the gauge exists to catch. Making
            // the sample group-wide means one aggregation is right for both
            // adapters and the gauge means the same thing regardless of which
            // broker is behind it.
            //
            // The cost is that each replica asks about every partition rather
            // than its own share, so broker calls per sample scale with replica
            // count. That is bounded work on the lag interval, not the message
            // path, and it is how consumer-group lag exporters compute this.
            let metadata = consumer.fetch_metadata(Some(&topic), timeout).ok()?;
            let partitions: Vec<i32> = metadata
                .topics()
                .iter()
                .filter(|t| t.name() == topic)
                .flat_map(|t| {
                    t.partitions()
                        .iter()
                        .map(rdkafka::metadata::MetadataPartition::id)
                })
                .collect();
            if partitions.is_empty() {
                return None;
            }

            // The DURABLE group offset, not `position()`. `position()` is the
            // local next-fetch cursor: it advances the moment a record is
            // fetched, regardless of whether that record has been dispatched,
            // is being retried, or is stuck behind a blocked commit prefix. So
            // a consumer wedged with a large uncommitted backlog — precisely
            // the condition this gauge exists to expose — would report its lag
            // falling to zero, while a restart replayed everything from the
            // last commit.
            //
            // `committed_offsets` takes an explicit partition list, unlike
            // `committed`, which is scoped to the local assignment.
            let mut wanted = TopicPartitionList::new();
            for partition in &partitions {
                wanted.add_partition(&topic, *partition);
            }
            let committed = consumer.committed_offsets(wanted, budget()?).ok()?;

            group_lag(
                &topic,
                &partitions,
                &committed,
                reset,
                &anchors,
                // Re-read the remaining budget per partition: this is the loop
                // whose cost scales with the topic, so it is where the bound
                // has to bite.
                |partition| consumer.fetch_watermarks(&topic, partition, budget()?).ok(),
            )
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
    format!(
        "kafka:{}\u{1}{group}\u{1}{}",
        canonical_broker_list(brokers),
        config.topic
    )
}

/// Reduce a `bootstrap.servers` value to a form that is stable across the ways
/// one cluster can be written.
///
/// The value is a **seed** list, not an address: librdkafka contacts one entry
/// and learns the real membership from its metadata. So order, whitespace,
/// host case and repeats carry no meaning to Kafka — but they do change the
/// raw string, which would make one cluster look like several and let the
/// duplicate-subscription guard admit two runtimes into the same group. Kafka
/// then splits the partitions between them and each target silently receives a
/// fraction of the stream.
///
/// # Residual, deliberately not closed here
///
/// Two *disjoint* seed lists naming the same cluster (`a:9092` and `b:9092`,
/// both members of it; a DNS alias against a resolved name) still compare as
/// different. Only the broker can settle that — via its cluster id — and
/// fetching it means a live metadata round-trip during plugin **build**, where
/// this identity is computed and no consumer exists yet. That would make
/// startup fail when the broker is briefly unreachable: trading a rare
/// config-typo detection for a common availability failure. The narrower
/// canonical form covers every spelling of one *written* seed list, which is
/// the shape a copy-pasted duplicate binding actually takes.
fn canonical_broker_list(brokers: &str) -> String {
    let mut seeds: Vec<String> = brokers
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    seeds.sort_unstable();
    seeds.dedup();
    seeds.join(",")
}

/// Bound on the metadata round-trip that reads the group's committed offsets
/// before an anchor commit.
///
/// One call on the recovery path, which is already slow and rare, so this only
/// needs to stop a wedged broker turning a recovery into an indefinite hang:
/// the runtime's answer to a failed `recover` is to back off and retry.
const ANCHOR_COMMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Overall wall-clock budget for one lag sample.
///
/// The sample walks **every partition of the topic** (deliberately — see
/// [`EventSource::lag`]), so its cost scales with partition count, and each
/// `fetch_watermarks` is a live broker round-trip that can itself be slow. Left
/// unbounded, a large topic against a degraded broker keeps a blocking thread
/// occupied for minutes and keeps hammering the very broker that is struggling.
///
/// The runtime independently budgets the call, which is what protects the
/// message path; this bound is what makes an abandoned sample actually STOP.
/// Without it the runtime's timeout only drops the `JoinHandle` — the blocking
/// walk runs on to completion regardless, so abandoned walks pile up one per
/// sample interval and the broker load grows rather than shrinks.
const LAG_SAMPLE_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// Per-call librdkafka timeout, further clamped by whatever budget is left.
const LAG_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Time left in a sample's budget, or `None` once it is spent.
///
/// `None` aborts the walk, which surfaces as an unavailable watermark and so
/// as no sample at all — the same honest failure mode [`group_lag`] already
/// uses for a partial answer.
fn remaining_lag_budget(
    deadline: std::time::Instant,
    now: std::time::Instant,
) -> Option<std::time::Duration> {
    let left = deadline.checked_duration_since(now)?;
    (!left.is_zero()).then_some(left)
}

/// The group's outstanding records across `partitions`.
///
/// `partitions` is the topic's full partition set from metadata, so the total
/// is the GROUP's backlog rather than the calling replica's share of it. A
/// partition with no entry in `committed` — one the group has never committed,
/// which is the common shape for a partition owned by another replica — is
/// baselined by `reset`, per [`partition_lag`].
///
/// Returns `None` if any watermark is unavailable. A partial sum is
/// indistinguishable from a real drop in backlog, and reporting one would turn
/// a broker hiccup into a false all-clear on the gauge operators page on.
fn group_lag(
    topic: &str,
    partitions: &[i32],
    committed: &TopicPartitionList,
    reset: OffsetReset,
    anchors: &PartitionAnchors,
    watermarks: impl Fn(i32) -> Option<(i64, i64)>,
) -> Option<i64> {
    let mut total: i64 = 0;
    for &partition in partitions {
        let (low, high) = watermarks(partition)?;
        let offset = committed
            .find_partition(topic, partition)
            .map_or(Offset::Invalid, |p| p.offset());
        total = total.saturating_add(partition_lag(
            low,
            high,
            offset,
            reset,
            anchors.anchor_for(partition),
        ));
    }
    Some(total)
}

/// Outstanding records for one partition, from its watermarks and the group's
/// committed offset.
///
/// The baseline is the first of these that applies:
///
/// 1. **A committed offset** — the group resumes there, so the reset policy is
///    irrelevant.
/// 2. **An [anchor][PartitionAnchors]** — nothing committed, but this source has
///    read from the partition, so the first offset it read is where the group
///    resolved. Fixed, unlike the reset policy.
/// 3. **The reset policy** — nothing committed and never read here (a partition
///    owned by another replica), so the honest answer is where a resume would
///    land: `low` for `earliest`, `high` for `latest`.
///
/// Baselines 1 and 2 are floored at the low watermark: retention can advance
/// `low` past a stale position, and records gone from the log are not backlog.
fn partition_lag(
    low: i64,
    high: i64,
    committed: Offset,
    reset: OffsetReset,
    anchor: Option<i64>,
) -> i64 {
    let current = match committed {
        // Retention can advance `low` past an old commit. Those records are
        // gone from the log, so subtracting from the stale commit would report
        // a backlog the consumer can never read -- a permanent false alarm on
        // exactly the gauge operators page on.
        Offset::Offset(o) => o.max(low),
        // Nothing committed. If this source has read from the partition, the
        // first offset it read is where the group RESOLVED to, and that is a
        // fixed baseline. Falling back to `reset` here would re-resolve
        // `Latest` against the live high watermark on every sample, making the
        // answer `high - high = 0` no matter how far the topic has run ahead --
        // an all-clear for precisely the pre-first-commit outage this gauge
        // exists to expose (see [`PartitionAnchors`]).
        _ => anchor.map_or(
            match reset {
                OffsetReset::Earliest => low,
                OffsetReset::Latest => high,
            },
            // Same retention guard as a stale commit: records below the low
            // watermark are gone from the log, not backlog.
            |a| a.max(low),
        ),
    };
    (high - current).max(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A group configured to start at the tail owes nothing for a partition it
    /// has never committed.
    ///
    /// `to_client_config` applies `extra` AFTER its `earliest` default, so a
    /// caller's `.property("auto.offset.reset", "latest")` is what librdkafka
    /// actually uses — unlike `enable.auto.commit`, which is forced last
    /// because auto-commit would break the ack-after-commit contract. An
    /// offset-reset policy breaks no invariant, so it stays the caller's
    /// choice, and the gauge has to honour it: baselining an uncommitted
    /// partition at the LOW watermark would report the topic's whole retained
    /// history as backlog for a brand-new latest-starting group, and on a quiet
    /// topic nothing ever commits, so that false backlog would never drain.
    #[test]
    fn a_latest_group_owes_nothing_for_an_uncommitted_partition() {
        assert_eq!(
            partition_lag(0, 500, Offset::Invalid, OffsetReset::Latest, None),
            0,
            "a latest-starting group resumes at the high watermark, so nothing is owed"
        );
        assert_eq!(
            partition_lag(0, 500, Offset::Invalid, OffsetReset::Earliest, None),
            500,
            "an earliest-starting group would replay the whole retained log"
        );
        // Once the group HAS committed, the reset policy is irrelevant: it only
        // applies when there is no committed offset to resume from.
        assert_eq!(
            partition_lag(0, 500, Offset::Offset(450), OffsetReset::Latest, None),
            50
        );
        assert_eq!(
            partition_lag(0, 500, Offset::Offset(450), OffsetReset::Earliest, None),
            50
        );
    }

    /// A `latest` group that has read but not yet committed owes what has
    /// accrued since it started -- not zero.
    ///
    /// This is the pre-first-commit outage the gauge exists to expose. Without
    /// an anchor the uncommitted branch baselines at the LIVE high watermark,
    /// so the sample is `high - high = 0` on every pass: the group starts at
    /// 100, producers push the topic to 1000 while dispatch is failing, and the
    /// gauge reports an all-clear for the 900 records now owed. Baselining at
    /// the position the group actually resolved to makes the backlog visible.
    #[test]
    fn an_uncommitted_latest_group_owes_what_accrued_since_it_started() {
        assert_eq!(
            partition_lag(0, 1000, Offset::Invalid, OffsetReset::Latest, Some(100)),
            900,
            "the group started at 100 and the topic is at 1000: 900 records owed"
        );
        // No anchor -- a partition owned by another replica, which this one has
        // never read from -- keeps the reset-policy baseline. That residual is
        // documented on `partition_lag`; it cannot be closed without
        // cross-replica state.
        assert_eq!(
            partition_lag(0, 1000, Offset::Invalid, OffsetReset::Latest, None),
            0
        );
        // Retention can advance `low` past a stale anchor exactly as it can past
        // a stale commit, so the anchor gets the same floor guard.
        assert_eq!(
            partition_lag(400, 1000, Offset::Invalid, OffsetReset::Latest, Some(100)),
            600,
            "records below the low watermark are gone; they are not backlog"
        );
        // A committed offset always wins: the anchor only ever substitutes for
        // the reset policy, which itself only applies when nothing is committed.
        assert_eq!(
            partition_lag(0, 1000, Offset::Offset(950), OffsetReset::Latest, Some(100)),
            50
        );
    }

    /// The rebuild's resume floor, per partition.
    ///
    /// A `latest` group with no committed offset has no durable anchor, so a
    /// rebuild re-resolves `latest` against the *current* tail and skips the
    /// very record the runtime is blocked on -- silent message loss reported as
    /// a successful retry. The floor a rebuild must resume from is the first
    /// offset this source ever read from the partition (which is where `latest`
    /// resolved to), raised by anything already committed.
    #[test]
    fn partition_anchors_track_the_floor_a_rebuild_must_resume_from() {
        let mut anchors = PartitionAnchors::default();
        assert!(anchors.pending_anchors().is_empty(), "nothing read yet");
        assert_eq!(anchors.anchor_for(0), None);

        // First record read from p0 is offset 100: that is where `latest`
        // resolved, and the floor a rebuild must not skip past.
        anchors.record_read(0, 100);
        anchors.record_read(0, 101);
        anchors.record_read(0, 102);
        assert_eq!(
            anchors.anchor_for(0),
            Some(100),
            "the FIRST offset read wins; later reads do not advance the floor"
        );
        assert_eq!(anchors.pending_anchors(), vec![(0, 100)]);

        // A commit raises the floor: `ack` commits `offset + 1`, the next
        // offset to read.
        anchors.record_commit(0, 101);
        assert_eq!(anchors.anchor_for(0), Some(101));
        assert_eq!(
            anchors.pending_anchors(),
            vec![(0, 101)],
            "re-asserting a commit is idempotent and makes an async one durable"
        );

        // A stale/out-of-order commit never rewinds the floor.
        anchors.record_commit(0, 99);
        assert_eq!(anchors.anchor_for(0), Some(101));

        // Partitions are tracked independently, in a stable order.
        anchors.record_read(2, 7);
        anchors.record_read(1, 3);
        assert_eq!(anchors.pending_anchors(), vec![(0, 101), (1, 3), (2, 7)]);
    }

    /// A retained anchor is only committable for a partition this replica still
    /// owns.
    ///
    /// Nothing evicts a floor when a rebalance moves the partition elsewhere, so
    /// a long-lived source accumulates anchors for partitions another replica is
    /// now advancing. Committing one of those on behalf of the group asserts a
    /// resume point for work this replica is not doing.
    #[test]
    fn an_anchor_for_a_partition_this_replica_no_longer_owns_is_never_committed() {
        // p0 is still ours; p7 was read before a rebalance moved it away.
        let anchors = vec![(0, 101), (7, 100)];

        assert_eq!(
            committable_anchors(&anchors, &[0], |_| None),
            vec![(0, 101)],
            "an anchor for a partition owned by another replica is not ours to assert"
        );

        // Mid-rebalance the assignment can be empty. Committing nothing is the
        // conservative answer: the rebuild rejoins and resumes from the group.
        assert!(
            committable_anchors(&anchors, &[], |_| None).is_empty(),
            "with no assignment there is no partition this replica may speak for"
        );
    }

    /// An anchor commit may only ever RAISE a partition's committed offset.
    ///
    /// Ownership alone is not enough: `record_read` is first-read-wins, which is
    /// right within one continuous ownership span but stale across a
    /// revoke-and-return. A partition we read at 100, lost, and got back after
    /// another replica advanced it to 5000 still carries a floor of 100 —
    /// writing that back rewinds the group and replays settled records.
    #[test]
    fn an_anchor_never_rewinds_a_partition_the_group_has_moved_past() {
        let anchors = vec![(0, 100)];

        assert!(
            committable_anchors(&anchors, &[0], |_| Some(5_000)).is_empty(),
            "a stale floor behind the group's committed offset must not be written back"
        );
        assert!(
            committable_anchors(&anchors, &[0], |_| Some(100)).is_empty(),
            "re-writing the offset already there is not a raise, so it is not worth a commit"
        );
        assert_eq!(
            committable_anchors(&anchors, &[0], |_| Some(40)),
            vec![(0, 100)],
            "a floor ahead of the group makes this replica's settled work durable"
        );
    }

    /// The case the anchor exists for still commits.
    ///
    /// A `latest` binding whose first message keeps failing has nothing
    /// committed anywhere, so both guards pass trivially: the partition is
    /// assigned (it was just read from) and any floor raises "no floor at all".
    /// Guarding the commit must not cost the loss fix it was added for.
    #[test]
    fn an_uncommitted_owned_partition_still_commits_its_anchor() {
        assert_eq!(
            committable_anchors(&[(0, 100)], &[0], |_| None),
            vec![(0, 100)],
            "with nothing committed there is nothing to rewind, and a floor prevents the skip"
        );
    }

    /// The walk stops once its budget is spent, and each call is clamped to
    /// what is left so a single one cannot overrun the whole sample.
    #[test]
    fn a_lag_sample_walk_stops_once_its_budget_is_spent() {
        let start = std::time::Instant::now();
        let deadline = start + LAG_SAMPLE_BUDGET;

        // Plenty left: the caller clamps to the per-call timeout itself.
        let left = remaining_lag_budget(deadline, start).expect("budget remains");
        assert_eq!(left, LAG_SAMPLE_BUDGET);
        assert_eq!(left.min(LAG_CALL_TIMEOUT), LAG_CALL_TIMEOUT);

        // Nearly spent: the per-call timeout is clamped DOWN, so one slow call
        // cannot run past the sample's own deadline.
        let nearly = remaining_lag_budget(
            deadline,
            start + LAG_SAMPLE_BUDGET.saturating_sub(std::time::Duration::from_millis(500)),
        )
        .expect("budget remains");
        assert_eq!(nearly, std::time::Duration::from_millis(500));
        assert_eq!(
            nearly.min(LAG_CALL_TIMEOUT),
            std::time::Duration::from_millis(500)
        );

        // Spent, exactly and then past: both abort the walk.
        assert!(remaining_lag_budget(deadline, deadline).is_none());
        assert!(
            remaining_lag_budget(deadline, deadline + std::time::Duration::from_secs(1)).is_none()
        );
    }

    /// A walk that runs out of budget partway reports NO sample rather than a
    /// partial total — a partial sum is indistinguishable from a genuine drop
    /// in backlog, which is the wrong thing to page on.
    #[test]
    fn a_budget_exhausted_walk_reports_no_sample_not_a_partial_one() {
        let committed = TopicPartitionList::new();
        let seen = std::cell::Cell::new(0);
        let total = group_lag(
            "orders",
            &[0, 1, 2, 3],
            &committed,
            OffsetReset::Earliest,
            &PartitionAnchors::default(),
            |_p| {
                // Budget runs out after two partitions.
                seen.set(seen.get() + 1);
                (seen.get() <= 2).then_some((0, 100))
            },
        );
        assert!(total.is_none(), "a truncated walk must not report a total");
        assert_eq!(seen.get(), 3, "the walk stops at the first exhausted call");
    }

    #[test]
    fn the_effective_offset_reset_honours_a_caller_override() {
        let default = KafkaSourceConfig::new("b:9092", "g", "orders");
        assert_eq!(default.effective_offset_reset(), OffsetReset::Earliest);

        let latest =
            KafkaSourceConfig::new("b:9092", "g", "orders").property("auto.offset.reset", "latest");
        assert_eq!(latest.effective_offset_reset(), OffsetReset::Latest);

        // librdkafka's aliases, and a last-write-wins override.
        for alias in ["latest", "end", "largest", "LATEST"] {
            let cfg = KafkaSourceConfig::new("b:9092", "g", "orders")
                .property("auto.offset.reset", alias);
            assert_eq!(
                cfg.effective_offset_reset(),
                OffsetReset::Latest,
                "{alias} selects the tail"
            );
        }
        let flipped = KafkaSourceConfig::new("b:9092", "g", "orders")
            .property("auto.offset.reset", "latest")
            .property("auto.offset.reset", "earliest");
        assert_eq!(
            flipped.effective_offset_reset(),
            OffsetReset::Earliest,
            "the LAST extra wins, matching to_client_config"
        );
    }

    /// A partition the local replica has never committed still contributes its
    /// full retained backlog.
    ///
    /// This is the case that only appears once the gauge goes group-wide: with
    /// several replicas in one group, most partitions in the metadata list are
    /// owned by *some other* replica, and `committed_offsets` returns
    /// `Offset::Invalid` for a partition the group has not committed at all.
    /// Scoring those as zero would silently re-create the very under-report the
    /// group-wide change exists to fix -- the partitions we just started
    /// counting would all count as nothing.
    #[test]
    fn group_lag_counts_an_uncommitted_partition_as_its_whole_backlog() {
        let mut committed = TopicPartitionList::new();
        // Only partition 0 has a committed offset; 1 and 2 are untouched, as
        // they would be for a partition assigned to a different replica.
        committed
            .add_partition_offset("orders", 0, Offset::Offset(40))
            .expect("valid offset");

        let total = group_lag(
            "orders",
            &[0, 1, 2],
            &committed,
            OffsetReset::Earliest,
            &PartitionAnchors::default(),
            |p| match p {
                0 => Some((0, 100)),
                1 => Some((0, 7)),
                2 => Some((10, 25)),
                _ => None,
            },
        )
        .expect("watermarks available for every partition");

        // 60 outstanding on p0, then the whole backlog of the two the group has
        // never committed: 7 and 15.
        assert_eq!(total, 60 + 7 + 15);
    }

    /// The sum spans every partition it is handed, so a caller passing the
    /// topic's full metadata partition set gets the GROUP's backlog rather than
    /// one replica's subtotal.
    #[test]
    fn group_lag_spans_every_partition_it_is_given() {
        let mut committed = TopicPartitionList::new();
        for p in 0..4 {
            committed
                .add_partition_offset("orders", p, Offset::Offset(10))
                .expect("valid offset");
        }
        let watermarks = |_p: i32| Some((0, 30));

        let one_replicas_share = group_lag(
            "orders",
            &[0, 1],
            &committed,
            OffsetReset::Earliest,
            &PartitionAnchors::default(),
            watermarks,
        )
        .expect("watermarks available");
        let whole_group = group_lag(
            "orders",
            &[0, 1, 2, 3],
            &committed,
            OffsetReset::Earliest,
            &PartitionAnchors::default(),
            watermarks,
        )
        .expect("watermarks available");

        assert_eq!(
            one_replicas_share, 40,
            "two partitions, 20 outstanding each"
        );
        assert_eq!(whole_group, 80, "all four partitions");
        assert!(
            whole_group > one_replicas_share,
            "an assignment-scoped sum under-reports the group; that is the bug"
        );
    }

    /// An unavailable watermark fails the whole sample rather than silently
    /// reporting a smaller number: a partial total is indistinguishable from a
    /// genuine drop in backlog, which is the wrong thing to page on.
    #[test]
    fn group_lag_is_none_when_a_watermark_is_unavailable() {
        let committed = TopicPartitionList::new();
        assert!(
            group_lag(
                "orders",
                &[0, 1],
                &committed,
                OffsetReset::Earliest,
                &PartitionAnchors::default(),
                |p| (p == 0).then_some((0, 5))
            )
            .is_none()
        );
    }

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
    fn subscription_identity_canonicalizes_the_broker_seed_list() {
        // `bootstrap.servers` is a SEED list, not an address: librdkafka
        // contacts one entry and learns the real cluster membership from its
        // metadata. So two configs listing the same seeds in a different
        // order (or with stray whitespace, or differing in host case, or
        // repeating an entry) address the SAME cluster. Comparing the raw
        // string makes them look like independent clusters, so the
        // duplicate-subscription guard waves both through -- and then Kafka,
        // which is looking at the group id and does not care about our string,
        // splits the partitions between them. Two bindings targeting different
        // workflows each silently receive a fraction of the records: exactly
        // the failure the guard exists to prevent, with no error anywhere.
        let id =
            |brokers| subscription_identity_for(&KafkaSourceConfig::new(brokers, "g", "orders"));
        assert_eq!(
            id("broker-a:9092,broker-b:9092"),
            id("broker-b:9092,broker-a:9092"),
            "seed order does not change which cluster is addressed"
        );
        assert_eq!(id("a:9092, b:9092"), id("b:9092,a:9092"));
        assert_eq!(id("A:9092"), id("a:9092"), "hostnames are case-insensitive");
        assert_eq!(
            id("a:9092,a:9092,b:9092"),
            id("a:9092,b:9092"),
            "a repeated seed is still one cluster"
        );
        // ...and canonicalizing must not start conflating genuinely distinct
        // clusters, which would REJECT a legitimate multi-cluster fan-in.
        assert_ne!(id("east:9092"), id("west:9092"));
        assert_ne!(id("a:9092,b:9092"), id("a:9092,c:9092"));
        // The canonical form is what lands in the identity.
        assert_eq!(
            id("broker-b:9092,broker-a:9092"),
            "kafka:broker-a:9092,broker-b:9092\u{1}g\u{1}orders"
        );
    }

    #[test]
    fn a_committed_offset_below_the_low_watermark_is_clamped() {
        // Kafka retention advances `low` past a group's old commit: those
        // records are gone from the log, so counting them reports a backlog
        // the consumer can never read and cannot ever work off. Only the 100
        // records still between `low` and `high` are outstanding.
        assert_eq!(
            partition_lag(1000, 1100, Offset::Offset(0), OffsetReset::Earliest, None),
            100
        );
    }

    #[test]
    fn an_uncommitted_partition_reports_the_whole_retained_backlog() {
        // `auto.offset.reset = earliest`, so a restart replays from `low`.
        assert_eq!(
            partition_lag(1000, 1100, Offset::Invalid, OffsetReset::Earliest, None),
            100
        );
        assert_eq!(
            partition_lag(0, 50, Offset::Beginning, OffsetReset::Earliest, None),
            50
        );
    }

    #[test]
    fn a_committed_offset_inside_the_window_subtracts_normally() {
        assert_eq!(
            partition_lag(
                1000,
                1100,
                Offset::Offset(1040),
                OffsetReset::Earliest,
                None
            ),
            60
        );
    }

    #[test]
    fn a_caught_up_partition_never_reports_negative_lag() {
        assert_eq!(
            partition_lag(
                1000,
                1100,
                Offset::Offset(1100),
                OffsetReset::Earliest,
                None
            ),
            0
        );
        // A commit past `high` is not expected, but must not underflow into a
        // huge unsigned-looking number through `saturating_add`.
        assert_eq!(
            partition_lag(
                1000,
                1100,
                Offset::Offset(1200),
                OffsetReset::Earliest,
                None
            ),
            0
        );
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

    /// Kafka has no per-message nack, so there is nothing for a
    /// "broker-native" dead-letter mode to hand a poison message to: routing a
    /// DLQ topic is a *producer* concern the consumer cannot perform by
    /// abandoning. `KafkaSource` therefore never overrides
    /// [`EventSource::has_native_dead_letter`] and inherits the trait's `false`
    /// default, which makes the `BrokerNative` pairing unsupported — rejected
    /// at build time rather than silently re-reading the poison message
    /// forever.
    ///
    /// Pinned as a drift guard for
    /// [`SourceBindingBuilder::broker_native_dead_letter`]'s rustdoc, which
    /// used to advertise "a Kafka DLQ topic" and would have walked a caller
    /// straight into that build-time rejection.
    #[tokio::test]
    async fn kafka_reports_no_native_dead_letter_so_the_pairing_is_rejected() {
        let cfg = KafkaSourceConfig::new("localhost:9092", "g", "orders");
        // Building the consumer needs no live broker (librdkafka connects
        // lazily), so this exercises the real adapter, not a stand-in.
        let Ok(source) = KafkaSource::connect(&cfg) else {
            return; // no librdkafka in this environment; nothing to assert
        };
        assert!(
            !source.has_native_dead_letter(),
            "Kafka cannot nack, so it must not advertise a native dead-letter \
             destination",
        );
        assert!(
            !crate::connector::broker_native_dead_letter_is_supported(
                crate::connector::DeadLetterMode::BrokerNative,
                source.has_native_dead_letter(),
            ),
            "a Kafka binding asking for broker-native dead-lettering must be \
             rejected, so the builder rustdoc must not offer it",
        );
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
