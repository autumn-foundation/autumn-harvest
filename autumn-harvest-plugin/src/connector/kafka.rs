//! Kafka [`EventSource`] adapter (issue #944), behind the `kafka` feature.
//!
//! The adapter is thin. Mapping, idempotency, poison accounting and
//! backpressure are broker-agnostic and live in [`super::runtime`]. Its whole
//! job is `receive` / `ack` / `abandon` / `lag` over `rdkafka`.
//!
//! # Manual commit, and why the runtime gates it
//!
//! The consumer is configured with `enable.auto.commit = false`. A Kafka
//! commit is a **high-water mark**: committing offset N asserts that
//! everything below N is done. Because the runtime dispatches up to
//! `max_in_flight` messages concurrently, acking offset 5 while 4 is still in
//! flight would silently skip 4 after a crash. So the runtime routes every ack
//! through [`super::disposition::OffsetTracker`]. It calls
//! [`EventSource::ack`] only once the completed prefix is contiguous. When
//! this adapter commits `position + 1`, every lower offset is settled.
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
    /// resolution landed, and the policy no longer applies. Without the pin,
    /// the baseline would chase the moving tail and report zero forever.
    Latest,
    /// Do not start at all: librdkafka reports an offset-reset error instead of
    /// choosing a position. A valid policy, not a typo — so unlike a
    /// misspelling it survives client-create validation and reaches a running
    /// consumer.
    ///
    /// So an uncommitted partition has no resumable position and no honest
    /// lag. The retained log is not backlog the consumer can work through,
    /// and zero would read as caught-up. [`partition_lag`] declines to answer
    /// instead. A partition with a commit or an [anchor][PartitionAnchors] is
    /// unaffected: the position is known and this policy never applies.
    Error,
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
    /// Extra `librdkafka` properties (SASL, TLS, fetch tuning, …). They apply
    /// after the defaults, so they can override them. The exception is
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
    /// Mirrors the precedence of [`Self::to_client_config`] exactly: the
    /// `earliest` default, then `extra` overriding it, last write winning. So
    /// the lag gauge cannot disagree with the running consumer about where an
    /// uncommitted partition resumes.
    ///
    /// This is NOT forced the way `enable.auto.commit` is. Auto-commit always
    /// breaks the ack-after-commit contract. An offset-reset policy is a
    /// legitimate caller choice: a new binding on a huge existing topic may
    /// want only new messages. `latest` DOES threaten at-least-once while the
    /// group has no committed offset. A rebuild would re-resolve it against a
    /// tail the blocked record has already advanced past. Anchoring closes
    /// that hole without forbidding the policy; see [`PartitionAnchors`].
    ///
    /// librdkafka has exactly three policies, and all three are modelled:
    /// `earliest`/`smallest`/`beginning`, `latest`/`end`/`largest`, and
    /// `error`. Only an unrecognised value maps to `Earliest`. That fallback
    /// is unreachable in practice: librdkafka rejects such a string at
    /// client-create time. `error` is not in that bucket. It is accepted, it
    /// runs, and it means "resolve nothing", so it gets its own variant.
    #[must_use]
    pub fn effective_offset_reset(&self) -> OffsetReset {
        self.extra
            .iter()
            .rev()
            .find(|(k, _)| k.eq_ignore_ascii_case("auto.offset.reset"))
            .map_or(OffsetReset::Earliest, |(_, v)| {
                match v.trim().to_ascii_lowercase().as_str() {
                    "latest" | "end" | "largest" => OffsetReset::Latest,
                    "error" => OffsetReset::Error,
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
/// `auto.offset.reset` applies only when a partition has **no committed
/// offset**. For a `latest` group that window is unbounded. Nothing commits
/// until the first message settles, and if that message keeps failing,
/// nothing ever does. In that window the reset policy is a *moving* position.
/// `latest` re-resolves against the high watermark at the moment it is asked.
/// Two things silently follow that moving tail:
///
/// * **Recovery.** [`EventSource::recover`] rebuilds the consumer to re-read the
///   blocked record. With no commit, the rebuild re-resolves `latest` against
///   the *current* tail. The blocked record has already advanced that tail.
///   So the rebuild skips the record permanently, while the runtime reports a
///   successful retry. That is message loss, not a delayed delivery.
/// * **The lag gauge.** Baselining an uncommitted partition at the live high
///   watermark makes every sample `high - high = 0`. Take a group that started
///   at 100 and is stuck while producers push the topic to 1000. It reports
///   an all-clear for the 900 records it owes. That masks the
///   pre-first-commit outage the gauge exists to expose.
///
/// One fact fixes both: **the first offset this source ever read from a
/// partition is where `latest` resolved**. That is a fixed anchor. Receive
/// records it, and every commit raises it. A rebuild re-asserts it
/// synchronously first, which also makes any in-flight async commit durable.
/// The lag gauge uses it as the baseline.
///
/// Committing that anchor is not a loss: for a `latest` group, "everything
/// below where I started is not mine" is precisely what the policy *means*.
/// All this does is make that decision durable instead of leaving it to be
/// re-derived against a tail that has since moved.
///
/// # Durability: the map is in-process, so a crash does not run `recover`
///
/// A rebuild is an *in-process* re-assert. It covers a wedged prefix but not a
/// crash or `SIGKILL`, which never run [`EventSource::recover`]. A
/// replacement process has neither a committed offset nor this map. It
/// re-resolves `latest` against a tail the in-flight record has already
/// advanced past. That is the same permanent skip, for a record the connector
/// had already *accepted*.
///
/// So under `latest`, [`anchors_needing_durability`] commits the floor
/// **eagerly**, before the batch is dispatched. This happens once per
/// partition per process, since a landed commit retires it. `earliest` owes
/// nothing. Its baseline is the low watermark, which does not move backwards,
/// so a restart re-reads rather than skips.
///
/// # Scope: a local fact, not a group one
///
/// An anchor records what **this source instance** read. Nothing evicts one
/// when a rebalance moves the partition elsewhere. So a long-lived source
/// accumulates floors for partitions another replica now owns. That matters
/// asymmetrically, and the two uses are guarded differently:
///
/// * **Committing** speaks for the whole group. [`committable_anchors`]
///   filters it down to partitions this consumer still owns. It also keeps
///   only floors that strictly raise what the group has committed. Without
///   the filter, a stale floor rewinds another replica's partition. That
///   replays records whose idempotency claims have long expired: duplicate
///   executions, not deduplicated retries.
/// * **The lag baseline** is read-only and per-replica, so a stale anchor there
///   can only skew one gauge sample. The assignment deliberately does *not*
///   filter it. Mid-rebalance, `assignment()` is briefly empty. Dropping
///   anchors then would re-open the `latest` under-report above. A false
///   all-clear on the gauge is the failure direction that hurts. A
///   best-effort local baseline is the better trade there.
///
/// # Residual
///
/// A partition this replica has **never read** has no anchor and falls back to
/// the reset policy. That is the honest answer, and for `latest` it is also
/// correct. Records that arrived before this source existed are not its work.
/// Closing it further needs cross-replica state this adapter deliberately does
/// not keep.
///
/// The narrower residual is a partition read since the last successful eager
/// commit — one whose commit failed and is queued for retry. A crash inside that
/// window still skips its in-flight record. It is bounded by the retry on the
/// next receive, and the failure is logged rather than silent.
#[derive(Debug, Default)]
pub struct PartitionAnchors {
    /// `partition -> next offset to read`. `BTreeMap` so `pending_anchors` is
    /// deterministically ordered, which keeps the commit list assertable.
    floors: BTreeMap<i32, i64>,
    /// Partitions whose floor is known committed to the **group**, so a crash
    /// resumes there. Only [`Self::mark_durable`] populates it, and only after
    /// a synchronous commit has actually landed.
    durable: std::collections::BTreeSet<i32>,
}

impl PartitionAnchors {
    /// Note a record read from `partition` at `offset`.
    ///
    /// First read wins: it is where the group resolved to, and later records
    /// must never raise the floor past a record still awaiting settlement.
    pub fn record_read(&mut self, partition: i32, offset: i64) {
        self.floors.entry(partition).or_insert(offset);
    }

    /// Note that this partition's floor is now committed to the group.
    ///
    /// Only called after a **synchronous** commit returns success, so it means
    /// "a crash resumes here", not "a commit was queued".
    pub fn mark_durable(&mut self, partition: i32) {
        self.durable.insert(partition);
    }

    /// Whether this partition's floor is already committed to the group.
    #[must_use]
    pub fn is_durable(&self, partition: i32) -> bool {
        self.durable.contains(&partition)
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
/// elsewhere. So the two diverge the moment ownership changes, and a commit
/// speaks for the whole group. Two conditions each close a hole the other
/// leaves open:
///
/// * **Assigned.** Only a partition this consumer currently owns. A stale
///   anchor for a partition another replica is advancing asserts a resume
///   point for work this replica is not doing. If it rewinds that partition,
///   the successor replays everything from the stale floor. The connector's
///   idempotency claims for those records expired long ago. So the replay
///   lands as duplicate executions, not deduplicated retries.
/// * **Raises.** Only where the anchor is strictly ahead of what the group has
///   committed. Ownership alone is not enough, because [`record_read`] is
///   first-read-wins. That is correct within one continuous ownership span,
///   and stale across a revoke-and-return. Take a partition read at 100,
///   revoked, advanced to 5000 by another replica, then handed back. It
///   still carries a floor of 100.
///
/// Together they make an anchor commit **monotonic and local**: it can only
/// raise a partition this replica owns. The anchor exists for a `latest`
/// group that has never committed anywhere. That case passes both trivially:
/// the partition was just read from, and any floor raises "no floor".
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

/// The partitions a successful commit pass may retire as durable.
///
/// Deliberately **not** "everything we tried to commit". A commit pass filters
/// `owed` through [`committable_anchors`], and the two filters mean different
/// things for durability:
///
/// * **Not assigned** — nothing spoke for the partition. Retiring it would be a
///   silent lie with a long fuse. `durable` is permanent process state. If a
///   rebalance hands that partition back later, the eager commit stays
///   suppressed *forever*. A crash under an otherwise-uncommitted `latest`
///   group then re-resolves at the tail and skips accepted records.
/// * **Assigned but does not raise** — the group is already committed at or
///   above the floor, so a crash resumes covered. Genuinely durable, even
///   though this pass committed nothing for it.
///
/// So ownership, not the commit list, makes a partition retirable. An
/// unassigned partition that is *not* retired costs a `committed_offsets`
/// call on each next receive while the stale anchor lingers. That is a rare
/// failure-path cost (read, commit failed, then revoked), not a steady-state
/// one.
fn covered_anchor_partitions(owed: &[(i32, i64)], assigned: &[i32]) -> Vec<i32> {
    owed.iter()
        .map(|(partition, _)| *partition)
        .filter(|partition| assigned.contains(partition))
        .collect()
}

/// Whether this replica may commit a settled offset for `partition`.
///
/// The **assigned** half of [`committable_anchors`], on the path that runs far
/// more often. A commit speaks for the whole group. A dispatch routinely
/// outlives the rebalance that revoked its partition. It settles afterwards,
/// commits an offset the new owner has long since advanced past, and rewinds
/// the group. Those records then replay after their idempotency claims
/// expired. They land as **duplicate executions**, not deduplicated retries.
/// That is the `committable_anchors` hazard, reached through
/// `ack`/`commit_position` instead of through an anchor.
///
/// Kafka does not stop this. A live group member at the current generation
/// may commit an offset for a partition it does not own. The classic
/// rebalance protocol validates the generation, not the assignment. So
/// ownership must be checked here.
///
/// Suppressing is always the safe direction. Nothing is committed, so the
/// records are re-read and deduped on their idempotency key. The runtime
/// documents the same outcome for a failed ack. So a briefly-empty
/// mid-rebalance assignment needs no special case.
///
/// # Residual: revoked, advanced elsewhere, and handed back
///
/// Ownership alone is not the whole guard, so [`committable_anchors`] pairs it
/// with a **raises** check. Take a partition revoked, advanced to 5000 by
/// another replica, then reassigned here. It is owned again, so a dispatch
/// from before the gap still commits its stale offset. Closing that needs the
/// group's committed offset, a `committed_offsets` broker round-trip. The
/// anchor path can afford it because it runs rarely. This path cannot,
/// because it runs per settlement.
///
/// Generation fencing would close it cheaply, but it has nowhere to live. It
/// needs the assignment generation at read time, carried on the handle to its
/// commit. A positional commit arrives without the reading message's handle.
/// The mark it commits belongs to no single message. So the residual narrows
/// to "outlived two rebalances" instead of "outlived one", and stays
/// documented.
fn may_commit_settled(partition: i32, assigned: &[i32]) -> bool {
    assigned.contains(&partition)
}

/// The anchors that must be made durable *before* their records are dispatched.
///
/// [`PartitionAnchors`] is in-process state, and only [`EventSource::recover`]
/// commits it. A crash or SIGKILL never runs that in-process rebuild. Under
/// `latest` that leaves a real at-least-once hole. A replacement process has
/// neither a committed offset nor the map. It re-resolves `latest` against a
/// tail the in-flight record has already advanced past, and skips the record
/// permanently. The record was *accepted* and awaiting settlement, so that is
/// loss. It is not the "records that arrived before I existed are not mine"
/// the policy promises.
///
/// The commit uses the first-read offset, not `offset + 1`, so it is safe to
/// do eagerly. It asserts only that records *below* it are not ours, which is
/// what `latest` means. The record itself is re-read.
///
/// `latest` is the ONLY policy that owes this, and the guard says so positively
/// rather than excluding the others one by one:
///
/// * `earliest` re-resolves to the low watermark, which does not move
///   backwards, so a restart with no committed offset re-reads rather than
///   skips. Returning empty keeps the default path byte-for-byte free of this
///   commit.
/// * `error` resolves nowhere — librdkafka fails the consumer rather than
///   picking a position — so a restart cannot skip anything silently; it stops
///   loudly instead.
///
/// Keying on `Latest` also stops a future fourth policy from inheriting the
/// commit by default. A new variant owes nothing until someone shows it can
/// resolve forward past an accepted record.
///
/// Only partitions not yet in `durable` are returned. A landed commit is
/// retired, and a failed one stays queued for the next receive to retry.
fn anchors_needing_durability(reset: OffsetReset, anchors: &PartitionAnchors) -> Vec<(i32, i64)> {
    if !matches!(reset, OffsetReset::Latest) {
        return Vec::new();
    }
    anchors
        .pending_anchors()
        .into_iter()
        .filter(|(partition, _)| !anchors.is_durable(*partition))
        .collect()
}

/// A Kafka-backed [`EventSource`].
pub struct KafkaSource {
    /// Swappable so [`EventSource::recover`] can rebuild it in place when the
    /// commit prefix wedges. The `std::sync::Mutex` is held only to clone the
    /// `Arc` out, never across an `.await`. So it cannot block the runtime or
    /// deadlock with a concurrent rebuild.
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

    /// Commit the resume floors a `latest` group owes, before its records are
    /// dispatched.
    ///
    /// See [`anchors_needing_durability`] for why only `latest` owes anything
    /// and why the first-read offset is the right thing to commit. This runs
    /// once per partition per process: a landed commit marks the partition
    /// durable and it is never revisited.
    ///
    /// The same [`committable_anchors`] filter as [`EventSource::recover`]
    /// guards it. So an eager commit is still monotonic and local: it can only
    /// raise a partition this replica currently owns. Only the partitions the
    /// pass spoke for are retired. See [`covered_anchor_partitions`] for why
    /// "assigned" and not "committed" is the right line.
    ///
    /// **Best-effort.** A failed commit warns and leaves the partition owed, so
    /// the next receive retries. *Every* next receive retries, drained or idle.
    /// That bounds the crash window to a poll interval, not to the topic's next
    /// burst of traffic (see [`retry_owed_anchors`]).
    ///
    /// Failing the receive instead would discard the drained records. Their
    /// local consumer position has already advanced past them. That is the
    /// permanent-loss hazard [`defer_recv_error`] exists to avoid. Here it would
    /// fire on *every* commit failure, not only on a crash. The only correct
    /// way to "withhold the batch" first seeks the consumer back to each owed
    /// floor, so the records are un-read. `seek()` is a blocking librdkafka
    /// call on a broker unhealthy enough to fail a commit. Its own failure
    /// mode is the loss it sets out to prevent. Retrying promptly is the
    /// cheaper bound on the same window.
    async fn make_anchors_durable(&self, consumer: &Arc<StreamConsumer>) {
        let reset = self.config.effective_offset_reset();
        let owed = self.with_anchors(|a| anchors_needing_durability(reset, a));
        if owed.is_empty() {
            return;
        }

        let topic = self.topic.clone();
        let consumer = Arc::clone(consumer);
        let to_commit = owed.clone();
        // Committing and reading committed offsets are blocking librdkafka calls.
        let landed = tokio::task::spawn_blocking(move || -> Result<Vec<i32>, ConnectorError> {
            let assigned: Vec<i32> = consumer
                .assignment()
                .map_err(|e| ConnectorError::Broker(format!("kafka assignment: {e}")))?
                .elements_for_topic(&topic)
                .iter()
                .map(rdkafka::topic_partition_list::TopicPartitionListElem::partition)
                .collect();

            let mut wanted = TopicPartitionList::new();
            for partition in &assigned {
                wanted.add_partition(&topic, *partition);
            }
            let committed = if assigned.is_empty() {
                TopicPartitionList::new()
            } else {
                consumer
                    .committed_offsets(wanted, ANCHOR_COMMIT_TIMEOUT)
                    .map_err(|e| ConnectorError::Broker(format!("kafka committed offsets: {e}")))?
            };

            let committable =
                committable_anchors(&to_commit, &assigned, |partition| {
                    match committed
                        .find_partition(&topic, partition)
                        .map(|p| p.offset())
                    {
                        Some(Offset::Offset(o)) => Some(o),
                        _ => None,
                    }
                });

            // Retirable regardless of whether anything is left to commit: an
            // assigned partition the group already covers is durable too.
            let covered = covered_anchor_partitions(&to_commit, &assigned);

            if committable.is_empty() {
                return Ok(covered);
            }
            let mut tpl = TopicPartitionList::new();
            for (partition, floor) in committable {
                tpl.add_partition_offset(&topic, partition, Offset::Offset(floor))
                    .map_err(|e| {
                        ConnectorError::Broker(format!("kafka anchor offset list: {e}"))
                    })?;
            }
            consumer
                .commit(&tpl, CommitMode::Sync)
                .map_err(|e| ConnectorError::Broker(format!("kafka anchor commit: {e}")))?;
            Ok(covered)
        })
        .await;

        match landed {
            Ok(Ok(covered)) => {
                self.with_anchors(|a| {
                    for partition in covered {
                        a.mark_durable(partition);
                    }
                });
            }
            Ok(Err(e)) => tracing::warn!(
                topic = %self.topic,
                error = %e,
                "kafka anchor commit failed; a crash before the next attempt may skip these records"
            ),
            Err(e) => tracing::warn!(
                topic = %self.topic,
                error = %e,
                "kafka anchor commit panicked; a crash before the next attempt may skip these records"
            ),
        }
    }

    /// Drain up to `max` records, waiting `timeout` only for the first.
    ///
    /// Split out of [`EventSource::receive`] so that function has exactly ONE
    /// exit that produces a batch — see the comment there. A deferred `recv()`
    /// error returns the partial batch as `Ok`, which is what
    /// [`defer_recv_error`] is for.
    async fn drain_batch(
        &self,
        consumer: &Arc<StreamConsumer>,
        max: usize,
        timeout: std::time::Duration,
    ) -> Result<Vec<InboundMessage>, ConnectorError> {
        let mut batch = Vec::new();
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

    /// Commit a settled offset for one partition and raise its anchor.
    ///
    /// Guarded by [`may_commit_settled`]: a settlement that arrives after a
    /// rebalance took its partition away must not speak for the group. The
    /// assignment read is a local librdkafka call, not a broker round-trip.
    /// So it costs a list walk per settled prefix advance, not a request.
    ///
    /// # Residual: the check is a snapshot, and the commit is asynchronous
    ///
    /// The guard narrows the window; it does not close it. A rebalance can
    /// revoke the partition after the assignment is read. That can happen
    /// before librdkafka's background thread builds the `OffsetCommit`
    /// request. The request carries the generation current when it is built.
    /// The classic protocol validates a committing member's generation, not
    /// its assignment. So the stale offset can be accepted under the *new*
    /// generation and rewind the group.
    ///
    /// It is left as a residual rather than fenced, for three reasons.
    ///
    /// 1. **Generation fencing is not expressible here.** `Consumer::commit`
    ///    takes `(&TopicPartitionList, CommitMode)` and no generation;
    ///    librdkafka fills that field in itself. There is no "commit only if
    ///    generation == N" API to reach for.
    /// 2. **A lock against the rebalance callbacks would not cover the send.**
    ///    Serialising the check and the *enqueue* against a `ConsumerContext`
    ///    `pre_rebalance` still leaves the actual transmission on librdkafka's
    ///    thread, outside anything this side holds. Closing it properly means
    ///    `CommitMode::Sync` — a broker round-trip per settlement, on the
    ///    dispatch hot path.
    /// 3. **The blast radius is small and self-limiting.** A rewind replays
    ///    records this replica settled moments earlier. Their idempotency
    ///    claims are seconds old and well inside any window, so they dedupe.
    ///    The rewind is durable only if the stale commit lands after the new
    ///    owner's *final* commit for the partition. Otherwise the next commit
    ///    re-advances past it immediately. Without this guard, a dispatch
    ///    routinely outlived its rebalance, and the new owner had long since
    ///    advanced. The residual is materially narrower than that.
    ///
    /// The controllable part is done. The commit payload is built *before*
    /// the ownership check, so no fallible FFI work sits between the check and
    /// the commit.
    fn commit_settled_offset(&self, partition: i32, offset: i64) -> Result<(), ConnectorError> {
        let consumer = self.consumer();
        // Built first, deliberately: this is the only fallible or allocating
        // work here. Placed between the ownership check and the commit, it
        // would widen the window described above for no gain.
        let mut tpl = TopicPartitionList::new();
        // Kafka's committed offset is the NEXT offset to read, so commit
        // `offset + 1`. The runtime has already established that every lower
        // offset in this partition is settled.
        tpl.add_partition_offset(&self.topic, partition, Offset::Offset(offset + 1))
            .map_err(|e| ConnectorError::Broker(format!("kafka offset list: {e}")))?;

        let assigned: Vec<i32> = consumer
            .assignment()
            .map_err(|e| ConnectorError::Broker(format!("kafka assignment: {e}")))?
            .elements_for_topic(&self.topic)
            .iter()
            .map(rdkafka::topic_partition_list::TopicPartitionListElem::partition)
            .collect();
        if !may_commit_settled(partition, &assigned) {
            // Not an error: the settlement was real, but this replica no
            // longer speaks for the partition. `Ok` keeps the runtime's "ack
            // failed" warning for actual failures. The current owner re-reads
            // the records, and they dedupe on their idempotency key.
            tracing::warn!(
                topic = %self.topic,
                partition,
                offset,
                "kafka settlement landed after its partition was revoked; not committing, \
                 the new owner's offset stands and these records dedupe on redelivery"
            );
            return Ok(());
        }

        // The SAME consumer the assignment was read from, deliberately. A
        // rebuild between the two would commit against a consumer whose
        // assignment was never checked, which is precisely the guard above
        // being bypassed.
        consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(|e| ConnectorError::Broker(format!("kafka commit: {e}")))?;
        // Raise the resume floor. The commit above is ASYNC, so it may not be
        // durable yet. Recording it here lets `recover` re-assert it
        // synchronously before a rebuild instead of racing it.
        self.with_anchors(|a| a.record_commit(partition, offset + 1));
        Ok(())
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
        // Kafka partitions by the record key. Surfacing it lets a mapping
        // function reach the documented per-key ordering remedy
        // (`ctx.key_str()`).
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
/// not it ever reaches the runtime. Suppose a mid-batch error discarded them.
/// The next poll would start at a higher offset. That offset would set the
/// [`OffsetTracker`][t] floor for the partition. Committing it would assert
/// that every lower offset is done, silently skipping records never
/// dispatched. Returning the partial batch keeps them on the normal
/// dispatch-then-commit path. The error resurfaces on the next poll, which
/// then has nothing drained to lose.
///
/// [t]: crate::connector::OffsetTracker
const fn defer_recv_error(drained: usize) -> bool {
    drained > 0
}

/// Whether a receive pass should attempt the anchor commits it still owes.
///
/// **Always**, and the unused `drained` is the point. The obvious reading of
/// "make the floor durable before these records are dispatched" gates the
/// attempt on `drained > 0`. That is right for the *first* attempt and wrong
/// for every retry after a failure. [`anchors_needing_durability`] keeps a
/// failed partition owed so the next receive retries it. A topic that goes
/// quiet right after the failure never drains again. The owed anchor is then
/// never retried. The crash window that
/// [`KafkaSource::make_anchors_durable`] describes as "until the next attempt"
/// becomes unbounded in wall-clock time, not one poll interval.
///
/// Nothing owed is a no-op. So the quiet steady state costs one walk of the
/// anchor map and no broker call. That is the `latest`-only cost of never
/// leaving an owed floor stranded behind an idle topic.
const fn retry_owed_anchors(_drained: usize) -> bool {
    true
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
        // Snapshot the consumer once: a rebuild mid-pass must not split this
        // batch across two generations.
        let consumer = self.consumer();
        // ONE exit for a produced batch, deliberately. The drain has two ways to
        // stop early: the `max`/budget break and the deferred-error path. With
        // the anchor step at the end of that loop, the deferred path returned
        // around it. That shipped records for dispatch with no durable floor.
        // The separate drain function makes that bypass impossible to
        // reintroduce.
        let batch = self.drain_batch(&consumer, max, timeout).await?;
        // Before these records leave this function and become someone's
        // in-flight work, make the `latest` resume floor durable to the group.
        // Best-effort, and NOT gated on this pass having drained anything --
        // see `retry_owed_anchors` for why a quiet topic must still retry.
        if retry_owed_anchors(batch.len()) {
            self.make_anchors_durable(&consumer).await;
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
        self.commit_settled_offset(partition, offset)
    }

    async fn abandon(&self, _handle: &MessageHandle) -> Result<(), ConnectorError> {
        // Kafka has no per-message nack. Simply not committing is sufficient:
        // the message is re-read after a rebalance or restart, and dedupes on
        // the connector's derived idempotency key.
        Ok(())
    }
    fn abandon_redelivers(&self) -> bool {
        // Not committing is the whole mechanism, so `abandon` above is a no-op.
        // `recv()` has already advanced the local position past the message,
        // so nothing hands it back while this consumer lives. The runtime
        // treats a retry here as a wedge and rebuilds the consumer
        // (`recover`), which re-reads from the last commit.
        false
    }

    /// Rebuild the consumer so the blocked offset is re-read.
    ///
    /// This is the retry a stalled prefix calls for. `recv()` has already
    /// advanced librdkafka's *local* position past the blocked message, so
    /// nothing hands it back while this consumer lives. A fresh consumer
    /// rejoins the group and starts from the last **committed** offset. That
    /// is the message the runtime is waiting on.
    ///
    /// # Anchoring first, and why it is not optional
    ///
    /// "The last committed offset" is only a resume point if one exists. Take
    /// a group that has never committed: a `latest` binding whose first
    /// message keeps failing, in an unbounded window. The rebuild resolves
    /// `auto.offset.reset` again. `latest` resolves against the *current*
    /// tail, which the blocked record has itself advanced past. The rebuild
    /// would skip that record permanently while the runtime reported a
    /// successful retry: message loss dressed as a retry.
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
    /// group-wide statement. The filter keeps a retained floor from rewinding
    /// a partition that has since moved to another replica.
    ///
    /// A failed anchor commit fails the whole recovery instead of rebuilding
    /// anyway. Rebuilding on an unanchored group is exactly the skip this
    /// guards against. The runtime answers a failed `recover` with back-off
    /// and retry. That is correct for a transient commit error, and far
    /// better than a silent loss.
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
            // Skipped when nothing has been read. To librdkafka an EMPTY offset
            // list is not a no-op: it commits the current assignment. There is
            // also no floor to assert.
            if !anchors.is_empty() {
                // Partitions this consumer still owns. A retained anchor is a
                // fact about local read history, not about the group. The two
                // diverge once a rebalance moves a partition away; see
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
            // Durability is a write-side concern; the lag baseline only reads
            // floors, so the snapshot does not carry it.
            ..PartitionAnchors::default()
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
            // `consumer.assignment()`. Consumer lag is a property of the GROUP.
            // An assignment-scoped sum is a property of one replica. With four
            // replicas, each reports roughly a quarter of the backlog. The
            // dashboard aggregates with `max by (source)`. That is correct for
            // SQS, where every replica reads the same whole-queue depth. An
            // assignment-scoped Kafka sample would show the largest replica's
            // subtotal. It would hide a partition stalled on any other replica,
            // the exact failure the gauge exists to catch. A group-wide sample
            // makes one aggregation right for both adapters. The gauge then
            // means the same thing whichever broker is behind it.
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
            // local next-fetch cursor. It advances the moment a record is
            // fetched, whether that record is dispatched, retried, or stuck
            // behind a blocked commit prefix. Take a consumer wedged with a
            // large uncommitted backlog, the condition this gauge exists to
            // expose. It would report its lag falling to zero, while a restart
            // replayed everything from the last commit.
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
/// The group is load-bearing and cannot be dropped. Two consumers on one topic
/// under **distinct** group ids each receive the whole stream. That is the
/// sanctioned way to fan one topic out to two targets. Identifying a
/// subscription by topic alone would reject exactly that. Two consumers in the
/// *same* group split the partitions between them. Each binding then sees
/// only a subset, the clash this identity exists to catch.
///
/// The separator is `\u{1}`, a byte no Kafka group id or topic name may
/// contain, so `("a", "b/c")` and `("a/b", "c")` cannot alias each other.
#[must_use]
fn subscription_identity_for(config: &KafkaSourceConfig) -> String {
    // Read the values back out of the built config rather than off the struct
    // fields, so the identity is whatever librdkafka will ACTUALLY join.
    // `to_client_config` applies `extra` after the declared fields, so a
    // `.property("group.id", ...)` override wins. Reading `config.group_id`
    // here would accept two configs that land in one group. That group splits
    // the partitions between them and silently starves both targets. The
    // built config keeps the precedence single-sourced, with no second copy
    // of the ordering rule to drift.
    let built = config.to_client_config();
    let brokers = built.get("bootstrap.servers").unwrap_or(&config.brokers);
    let group = built.get("group.id").unwrap_or(&config.group_id);
    // Brokers are load-bearing in the other direction. Two independent
    // clusters with the same topic and group id are two subscriptions, not
    // one. Omitting the brokers would reject that fan-in.
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
/// host case and repeats carry no meaning to Kafka. They do change the raw
/// string. One cluster would then look like several, and the
/// duplicate-subscription guard would admit two runtimes into the same group.
/// Kafka then splits the partitions between them, and each target silently
/// receives a fraction of the stream.
///
/// # Residual, deliberately not closed here
///
/// Two *disjoint* seed lists for the same cluster still compare as different.
/// Examples are `a:9092` and `b:9092` as two members, or a DNS alias against
/// a resolved name. Only the broker can settle that, via its cluster id.
/// Fetching it needs a live metadata round-trip during plugin **build**. This
/// identity is computed there, and no consumer exists yet. Startup would then
/// fail when the broker is briefly unreachable. That trades a rare
/// config-typo detection for a common availability failure. The narrower
/// canonical form covers every spelling of one *written* seed list. That is
/// the shape a copy-pasted duplicate binding takes.
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
/// One call on the recovery path, which is already slow and rare. So this only
/// stops a wedged broker from turning a recovery into an indefinite hang. The
/// runtime answers a failed `recover` with back-off and retry.
const ANCHOR_COMMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Overall wall-clock budget for one lag sample.
///
/// The sample walks **every partition of the topic** (deliberately; see
/// [`EventSource::lag`]). So its cost scales with partition count. Each
/// `fetch_watermarks` is a live broker round-trip that can itself be slow. Left
/// unbounded, a large topic against a degraded broker keeps a blocking thread
/// occupied for minutes and keeps hammering the very broker that is struggling.
///
/// The runtime independently budgets the call, which protects the message
/// path. This bound makes an abandoned sample actually STOP. Without it, the
/// runtime's timeout only drops the `JoinHandle`. The blocking walk runs on
/// to completion regardless. Abandoned walks then pile up one per sample
/// interval, and the broker load grows.
///
/// Defined AS the runtime's [`ADAPTER_LAG_SAMPLE_CEILING`], not merely equal
/// to it, so the two cannot drift. The runtime floors its own budget at that
/// value so it never gives up before this bound does. A local constant would
/// let a future edit here silently reintroduce the overlap.
const LAG_SAMPLE_BUDGET: std::time::Duration =
    crate::connector::runtime::ADAPTER_LAG_SAMPLE_CEILING;

/// Per-call librdkafka timeout, further clamped by whatever budget is left.
const LAG_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Time left in a sample's budget, or `None` once it is spent.
///
/// `None` aborts the walk. That surfaces as an unavailable watermark, and so
/// as no sample at all. [`group_lag`] uses the same honest failure mode for a
/// partial answer.
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
/// is the GROUP's backlog, not the calling replica's share of it. A partition
/// with no entry in `committed` is baselined by `reset`, per
/// [`partition_lag`]. The group has never committed it, which is common for a
/// partition owned by another replica.
///
/// Returns `None` if any watermark is unavailable, or if any partition has no
/// resolvable position at all (see [`partition_lag`]). A partial sum looks
/// the same as a real drop in backlog. Reporting one would turn a broker
/// hiccup into a false all-clear on the gauge operators page on.
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
        )?);
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
///    owned by another replica). The honest answer is where a resume would
///    land: `low` for `earliest`, `high` for `latest`. Under `error` a resume
///    lands nowhere, because librdkafka fails the consumer instead. There is
///    no answer to give, so this returns `None`.
///
/// Baselines 1 and 2 are floored at the low watermark. Retention can advance
/// `low` past a stale position, and records gone from the log are not backlog.
///
/// `None` means "no honest number", never "zero": a caller must propagate it
/// rather than treating it as an empty backlog.
fn partition_lag(
    low: i64,
    high: i64,
    committed: Offset,
    reset: OffsetReset,
    anchor: Option<i64>,
) -> Option<i64> {
    let current = match committed {
        // Retention can advance `low` past an old commit. Those records are
        // gone from the log. Subtracting from the stale commit would report a
        // backlog the consumer can never read. That is a permanent false alarm
        // on the gauge operators page on.
        Offset::Offset(o) => o.max(low),
        // Nothing committed. If this source has read from the partition, the
        // first offset it read is where the group RESOLVED to. That is a fixed
        // baseline. A fallback to `reset` here would re-resolve `Latest`
        // against the live high watermark on every sample. The answer would be
        // `high - high = 0`, however far the topic has run ahead. That is an
        // all-clear for the pre-first-commit outage this gauge exists to
        // expose (see [`PartitionAnchors`]).
        _ => match anchor {
            // Same retention guard as a stale commit: records below the low
            // watermark are gone from the log, not backlog.
            Some(a) => a.max(low),
            None => match reset {
                OffsetReset::Earliest => low,
                OffsetReset::Latest => high,
                // No commit, no anchor, and a policy that resolves nothing.
                // The consumer cannot start, so neither the retained log
                // (backlog it can never reach) nor zero (an all-clear) is
                // true. Decline instead of inventing one.
                OffsetReset::Error => return None,
            },
        },
    };
    Some((high - current).max(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A group configured to start at the tail owes nothing for a partition it
    /// has never committed.
    ///
    /// `to_client_config` applies `extra` AFTER its `earliest` default. So
    /// librdkafka uses a caller's `.property("auto.offset.reset", "latest")`.
    /// `enable.auto.commit` is different: it is forced last, because
    /// auto-commit would break the ack-after-commit contract. An offset-reset
    /// policy breaks no invariant, so it stays the caller's choice, and the
    /// gauge must honour it. A LOW-watermark baseline would report the whole
    /// retained history as backlog for a new latest-starting group. On a quiet
    /// topic nothing ever commits, so that false backlog would never drain.
    #[test]
    fn a_latest_group_owes_nothing_for_an_uncommitted_partition() {
        assert_eq!(
            partition_lag(0, 500, Offset::Invalid, OffsetReset::Latest, None),
            Some(0),
            "a latest-starting group resumes at the high watermark, so nothing is owed"
        );
        assert_eq!(
            partition_lag(0, 500, Offset::Invalid, OffsetReset::Earliest, None),
            Some(500),
            "an earliest-starting group would replay the whole retained log"
        );
        // Once the group HAS committed, the reset policy is irrelevant: it only
        // applies when there is no committed offset to resume from.
        assert_eq!(
            partition_lag(0, 500, Offset::Offset(450), OffsetReset::Latest, None),
            Some(50)
        );
        assert_eq!(
            partition_lag(0, 500, Offset::Offset(450), OffsetReset::Earliest, None),
            Some(50)
        );
    }

    /// A `latest` group that has read but not yet committed owes what has
    /// accrued since it started -- not zero.
    ///
    /// This is the pre-first-commit outage the gauge exists to expose. Without
    /// an anchor, the uncommitted branch baselines at the LIVE high watermark.
    /// The sample is then `high - high = 0` on every pass. The group starts at
    /// 100, and producers push the topic to 1000 while dispatch fails. The
    /// gauge reports an all-clear for the 900 records now owed. A baseline at
    /// the position the group resolved to makes the backlog visible.
    #[test]
    fn an_uncommitted_latest_group_owes_what_accrued_since_it_started() {
        assert_eq!(
            partition_lag(0, 1000, Offset::Invalid, OffsetReset::Latest, Some(100)),
            Some(900),
            "the group started at 100 and the topic is at 1000: 900 records owed"
        );
        // No anchor -- a partition owned by another replica, which this one has
        // never read from -- keeps the reset-policy baseline. That residual is
        // documented on `partition_lag`; it cannot be closed without
        // cross-replica state.
        assert_eq!(
            partition_lag(0, 1000, Offset::Invalid, OffsetReset::Latest, None),
            Some(0)
        );
        // Retention can advance `low` past a stale anchor exactly as it can past
        // a stale commit, so the anchor gets the same floor guard.
        assert_eq!(
            partition_lag(400, 1000, Offset::Invalid, OffsetReset::Latest, Some(100)),
            Some(600),
            "records below the low watermark are gone; they are not backlog"
        );
        // A committed offset always wins: the anchor only ever substitutes for
        // the reset policy, which itself only applies when nothing is committed.
        assert_eq!(
            partition_lag(0, 1000, Offset::Offset(950), OffsetReset::Latest, Some(100)),
            Some(50)
        );
    }

    /// The rebuild's resume floor, per partition.
    ///
    /// A `latest` group with no committed offset has no durable anchor. A
    /// rebuild re-resolves `latest` against the *current* tail. It skips the
    /// very record the runtime is blocked on: silent message loss reported as
    /// a successful retry. A rebuild must resume from the first offset this
    /// source ever read from the partition, where `latest` resolved. Anything
    /// already committed raises that floor.
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
    /// revoke-and-return. Take a partition read at 100, lost, then returned
    /// after another replica advanced it to 5000. It still carries a floor of
    /// 100. Writing that back rewinds the group and replays settled records.
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

    /// A `latest` group makes its resume floor durable before dispatching.
    ///
    /// `recover()` is an *in-process* rebuild, so a crash or SIGKILL never runs
    /// it. A replacement process has neither a committed offset nor this map.
    /// It re-resolves `latest` against a tail the in-flight record has already
    /// advanced past, and skips the record permanently. That breaks
    /// at-least-once for a record the connector had already accepted.
    #[test]
    fn a_latest_group_makes_its_resume_floor_durable_before_dispatch() {
        let mut anchors = PartitionAnchors::default();
        anchors.record_read(0, 100);
        anchors.record_read(1, 7);

        assert_eq!(
            anchors_needing_durability(OffsetReset::Latest, &anchors),
            vec![(0, 100), (1, 7)],
            "a freshly-read latest partition owes a durable floor before its records are dispatched"
        );

        // A successful commit retires it; a failed one leaves it queued, so the
        // next receive retries rather than leaving the window open forever.
        anchors.mark_durable(0);
        assert_eq!(
            anchors_needing_durability(OffsetReset::Latest, &anchors),
            vec![(1, 7)],
            "only the partition whose commit landed is retired"
        );
        anchors.mark_durable(1);
        assert_eq!(
            anchors_needing_durability(OffsetReset::Latest, &anchors),
            [] as [(i32, i64); 0]
        );
    }

    /// Only a partition the commit pass actually spoke for is retired.
    ///
    /// `durable` is permanent process state. So retiring a partition no commit
    /// covered is not a harmless optimism. If a rebalance hands that partition
    /// back later, the eager commit stays suppressed forever. A crash under an
    /// otherwise-uncommitted `latest` group then re-resolves at the tail.
    #[test]
    fn only_partitions_the_commit_pass_covered_are_retired() {
        let owed = vec![(0, 100), (1, 7), (2, 42)];

        // Partition 1 was revoked between the read and the commit, so
        // `committable_anchors` filtered it out and nothing spoke for it.
        assert_eq!(
            covered_anchor_partitions(&owed, &[0, 2]),
            vec![0, 2],
            "an unassigned partition is not covered, however the commit ended",
        );

        // An assigned partition filtered out by the RAISES guard IS covered.
        // The group is already committed at or above the floor. So a crash
        // resumes covered, though this pass committed nothing for it.
        assert_eq!(
            covered_anchor_partitions(&owed, &[0, 1, 2]),
            vec![0, 1, 2],
            "ownership is what makes the pass able to speak for a partition",
        );

        assert!(
            covered_anchor_partitions(&owed, &[]).is_empty(),
            "a pass that owns nothing retires nothing",
        );
    }

    /// A settled-offset commit speaks for the group, so it needs the same
    /// ownership guard the anchor commit has.
    ///
    /// A dispatch outlives the rebalance that revoked its partition. It settles
    /// afterwards and commits an offset another replica has since advanced
    /// past, rewinding the group. Those records replay long after their
    /// idempotency claims expired. They land as duplicate executions, not
    /// deduplicated retries. That is the hazard `committable_anchors`
    /// documents, on a path that runs far more often than the anchor commit.
    #[test]
    fn a_settled_commit_is_suppressed_for_a_partition_this_replica_lost() {
        assert!(
            !may_commit_settled(1, &[0, 2]),
            "a revoked partition's late settlement must not commit: this replica \
             no longer speaks for it, and the commit would rewind whatever the \
             new owner has advanced to",
        );

        // The ordinary case is untouched: an owned partition still commits.
        assert!(may_commit_settled(0, &[0, 2]));
        assert!(may_commit_settled(2, &[0, 2]));

        // Mid-rebalance `assignment()` is briefly empty. Suppressing is the
        // safe direction and matches the anchor path. With no assignment, this
        // replica may speak for no partition. The message is redelivered and
        // deduped.
        assert!(
            !may_commit_settled(0, &[]),
            "with no assignment there is nothing this replica may commit for",
        );
    }

    /// The default policy pays nothing for this.
    ///
    /// `earliest` re-resolves to the LOW watermark, which does not move
    /// backwards, so a restart with no committed offset re-reads the record
    /// rather than skipping it. There is no window to close, and the eager
    /// commit would be a pure cost on the receive path.
    #[test]
    fn an_earliest_group_never_pays_for_an_eager_anchor_commit() {
        let mut anchors = PartitionAnchors::default();
        anchors.record_read(0, 100);

        assert!(
            anchors_needing_durability(OffsetReset::Earliest, &anchors).is_empty(),
            "earliest already resumes below the record, so no eager commit is warranted"
        );
    }

    /// Only `latest` owes an eager commit, and the guard says so positively.
    ///
    /// The eager commit is a cost paid to close one specific hazard. `latest`
    /// is the only policy that re-resolves an uncommitted partition *forward*,
    /// past records this source already accepted. `earliest` resolves
    /// backwards. `error` does not resolve at all: it fails the consumer
    /// loudly, which cannot silently skip anything. Keying on `Latest` instead
    /// of excluding `Earliest` also stops a future fourth policy from
    /// inheriting the commit by default.
    #[test]
    fn only_a_latest_group_owes_an_eager_anchor_commit() {
        let mut anchors = PartitionAnchors::default();
        anchors.record_read(0, 100);

        assert!(
            anchors_needing_durability(OffsetReset::Error, &anchors).is_empty(),
            "an error policy fails loudly rather than skipping, so nothing is owed"
        );
        assert_eq!(
            anchors_needing_durability(OffsetReset::Latest, &anchors),
            vec![(0, 100)],
            "latest is the one policy that can resolve past an accepted record"
        );
    }

    /// The case the anchor exists for still commits.
    ///
    /// A `latest` binding whose first message keeps failing has nothing
    /// committed anywhere. So both guards pass trivially. The partition is
    /// assigned (it was just read from), and any floor raises "no floor at
    /// all". Guarding the commit must not cost the loss fix it was added for.
    #[test]
    fn an_uncommitted_owned_partition_still_commits_its_anchor() {
        assert_eq!(
            committable_anchors(&[(0, 100)], &[0], |_| None),
            vec![(0, 100)],
            "with nothing committed there is nothing to rewind, and a floor prevents the skip"
        );
    }

    /// The walk stops once its budget is spent. Each call is clamped to what
    /// is left, so a single one cannot overrun the whole sample.
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

    /// A walk that runs out of budget partway reports NO sample, not a partial
    /// total. A partial sum looks the same as a genuine drop in backlog, which
    /// is the wrong thing to page on.
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

    /// `error` is a THIRD valid `auto.offset.reset`, not a typo.
    ///
    /// librdkafka accepts it. For an uncommitted partition it picks no starting
    /// position and surfaces an offset-reset error to the consumer instead. So
    /// it passes client-create validation and DOES reach a running consumer.
    /// The `_ => Earliest` fallback argues that "librdkafka rejects a genuinely
    /// invalid value". That argument does not cover this case. Folding it into
    /// `Earliest` would baseline an uncommitted partition at the low watermark.
    /// The whole retained log would then count as consumable backlog for a
    /// consumer that cannot resume at all.
    #[test]
    fn the_error_reset_policy_is_modelled_not_folded_into_earliest() {
        for spelling in ["error", "ERROR", " error "] {
            let cfg = KafkaSourceConfig::new("b:9092", "g", "orders")
                .property("auto.offset.reset", spelling);
            assert_eq!(
                cfg.effective_offset_reset(),
                OffsetReset::Error,
                "{spelling} is a valid librdkafka policy, not an unknown one"
            );
        }

        // A genuinely unrecognised value still maps to the default: librdkafka
        // rejects it at client-create time, so it never reaches a consumer.
        let bogus = KafkaSourceConfig::new("b:9092", "g", "orders")
            .property("auto.offset.reset", "sideways");
        assert_eq!(bogus.effective_offset_reset(), OffsetReset::Earliest);
    }

    /// Under `error`, a partition with no position yet has no honest lag.
    ///
    /// Baselines 1 (a committed offset) and 2 (an anchor) still resolve: the
    /// position IS known in both. Only the fall-through to the reset policy is
    /// unanswerable. The retained log there would page an operator with a
    /// backlog the consumer cannot even start reading. Zero would be an
    /// all-clear for a wedged group. Neither is true, so the total is
    /// suppressed, as it is for a missing watermark.
    #[test]
    fn an_unresolvable_position_suppresses_the_lag_rather_than_inventing_one() {
        assert_eq!(
            partition_lag(0, 500, Offset::Invalid, OffsetReset::Error, None),
            None,
            "no commit, no anchor, no reset resolution -- no honest answer"
        );

        // Baseline 1: the group committed, so `error` never applies.
        assert_eq!(
            partition_lag(0, 500, Offset::Offset(450), OffsetReset::Error, None),
            Some(50),
        );
        // Baseline 2: this source read the partition, so the resolution is
        // already pinned.
        assert_eq!(
            partition_lag(0, 500, Offset::Invalid, OffsetReset::Error, Some(400)),
            Some(100),
        );

        // And one unresolvable partition suppresses the whole topic's total,
        // rather than contributing a silent zero to it.
        let committed = TopicPartitionList::new();
        assert_eq!(
            group_lag(
                "orders",
                &[0, 1],
                &committed,
                OffsetReset::Error,
                &PartitionAnchors::default(),
                |_p| Some((0, 100)),
            ),
            None,
        );
    }

    /// A partition the local replica has never committed still contributes its
    /// full retained backlog.
    ///
    /// This case appears only once the gauge goes group-wide. With several
    /// replicas in one group, *some other* replica owns most partitions in the
    /// metadata list. `committed_offsets` returns `Offset::Invalid` for a
    /// partition the group has not committed at all. Scoring those as zero
    /// would silently re-create the under-report the group-wide change fixes.
    /// The newly counted partitions would all count as nothing.
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

    /// The sum spans every partition it is handed. A caller that passes the
    /// topic's full metadata partition set gets the GROUP's backlog, not one
    /// replica's subtotal.
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

    /// An unavailable watermark fails the whole sample instead of silently
    /// reporting a smaller number. A partial total looks the same as a genuine
    /// drop in backlog, which is the wrong thing to page on.
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
        // joins. The declared field would accept two configs that end up in
        // one group. The group would then split the partitions between them,
        // and each target would silently miss messages.
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
        // reason, in the other direction. Two clusters are independent
        // subscriptions even when the group and topic match exactly. Omitting
        // the brokers would REJECT a legitimate multi-cluster fan-in.
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
        // The group is load-bearing. Two consumers on one topic under
        // DISTINCT group ids each get the full stream. The duplicate-source
        // panic message itself recommends that fan-out. Identifying a
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
        // metadata. Two configs can list the same seeds in a different order,
        // with stray whitespace, in another host case, or with a repeat. They
        // address the SAME cluster. The raw string makes them look like
        // independent clusters, so the duplicate-subscription guard admits
        // both. Kafka looks only at the group id, so it splits the partitions
        // between them. Two bindings for different workflows each silently
        // receive a fraction of the records. That is the failure the guard
        // exists to prevent, with no error anywhere.
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
        // Kafka retention advances `low` past a group's old commit. Those
        // records are gone from the log. Counting them reports a backlog the
        // consumer can never read or work off. Only the 100 records still
        // between `low` and `high` are outstanding.
        assert_eq!(
            partition_lag(1000, 1100, Offset::Offset(0), OffsetReset::Earliest, None),
            Some(100)
        );
    }

    #[test]
    fn an_uncommitted_partition_reports_the_whole_retained_backlog() {
        // `auto.offset.reset = earliest`, so a restart replays from `low`.
        assert_eq!(
            partition_lag(1000, 1100, Offset::Invalid, OffsetReset::Earliest, None),
            Some(100)
        );
        assert_eq!(
            partition_lag(0, 50, Offset::Beginning, OffsetReset::Earliest, None),
            Some(50)
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
            Some(60)
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
            Some(0)
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
            Some(0)
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
        // The documented per-key ordering remedy derives the `workflow_id`
        // from the partition key. So `MessageCtx` must expose the key. Pins
        // the gap that the issue #944 review found.
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

    /// Kafka has no per-message nack. So a "broker-native" dead-letter mode
    /// has nothing to hand a poison message to. Routing to a DLQ topic is a
    /// *producer* concern, and abandoning cannot do it. So `KafkaSource` never
    /// overrides [`EventSource::has_native_dead_letter`]. It inherits the
    /// trait's `false` default, which makes the `BrokerNative` pairing
    /// unsupported. The build rejects it, instead of silently re-reading the
    /// poison message forever.
    ///
    /// Pinned as a drift guard for the rustdoc of
    /// [`SourceBindingBuilder::broker_native_dead_letter`]. It used to
    /// advertise "a Kafka DLQ topic". That walked a caller straight into the
    /// build-time rejection.
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
        // them on a later `recv()` error is silent message loss. The next poll
        // starts at a higher offset, which sets the tracker's floor. Its
        // completion commits. The high-water mark then asserts the dropped
        // records done, though they were never dispatched.
        assert!(
            defer_recv_error(1),
            "a partial batch must be returned, not dropped",
        );
        assert!(defer_recv_error(12));
        // With nothing drained there is nothing to lose, so the error is the
        // only useful thing to report.
        assert!(!defer_recv_error(0));
    }

    /// A quiet topic must not strand an anchor whose commit failed.
    ///
    /// `anchors_needing_durability` keeps a failed partition owed so "the next
    /// receive retries". The failure is tolerable only because that retry
    /// bounds the crash window to roughly one poll interval. The obvious
    /// reading of "commit the floor for the records just read" gates the
    /// attempt on `drained > 0`. That silently voids the bound. A topic that
    /// goes quiet right after the failed commit never drains again. The owed
    /// anchor is then never retried, and the window stays open while the topic
    /// is idle. Under an otherwise-uncommitted `latest` group, a crash in that
    /// window re-resolves at the tail. It permanently skips the in-flight
    /// records this floor exists to protect.
    #[test]
    fn an_idle_pass_still_retries_an_anchor_commit_it_owes() {
        assert!(
            retry_owed_anchors(0),
            "a pass that drained nothing must still retry an owed anchor; \
             gating on drained records leaves a failed commit stranded for as \
             long as the topic stays quiet",
        );
        // The first attempt, for the records just drained, is unchanged.
        assert!(retry_owed_anchors(1));
        assert!(retry_owed_anchors(12));
    }
}
