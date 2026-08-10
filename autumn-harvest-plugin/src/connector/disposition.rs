//! Pure decision logic for what to do with a consumed message (issue #944).
//!
//! Every correctness-critical rule the connector promises lives here as a
//! **pure function**, so each one is falsifiable without a broker or a
//! database:
//!
//! * **Ack ordering** — [`MessageDisposition::Ack`] is only ever produced for
//!   an outcome that is already durable in Postgres. The runtime calls
//!   `EventSource::ack` in exactly one place, gated on this decision, so a
//!   message can never be acknowledged before its dispatch committed.
//! * **Throttle composition** — a deferred admission
//!   ([`DispatchOutcome::Deferred`]) is a *success*: the start is durably
//!   parked in `harvest_start_throttle` and the throttle owns pacing, so the
//!   connector acks and never busy-retries (issue #607).
//! * **Poison isolation** — a deterministically-bad message dead-letters
//!   immediately; a mapping-rejected one dead-letters after `threshold`
//!   consecutive strikes (mirroring `poison_pill_threshold`, issue #367). One
//!   bad message can therefore never wedge a partition.

use autumn_harvest::telemetry::{ConnectorOutcome, PoisonReason};
use std::collections::HashMap;

/// The result of attempting to dispatch one consumed message into harvest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOutcome {
    /// A workflow execution was durably started, or a signal was durably
    /// queued, by *this* dispatch.
    Dispatched {
        /// The resulting execution id, when the dispatch path returned one.
        execution_id: Option<String>,
        /// The business workflow id the mapping function produced.
        workflow_id: String,
    },
    /// The dispatch was recognized as a redelivery of an already-committed
    /// message. Nothing new was written, but the original dispatch is durable.
    IdempotentReplay {
        /// The already-existing execution id, when the path returned one.
        execution_id: Option<String>,
        /// The business workflow id the mapping function produced.
        workflow_id: String,
    },
    /// The target workflow carries a throttle/debounce/batch policy, so the
    /// start was durably parked instead of executed immediately (an HTTP
    /// `202`-equivalent). There is no execution id yet, by design.
    Deferred {
        /// The business workflow id the deferred start will use when it fires.
        workflow_id: String,
    },
    /// The raw body could not be decoded into the shape the mapping function
    /// expects. Deterministic: a retry can never succeed.
    Malformed(String),
    /// The mapping function ran and rejected the message. Possibly transient
    /// (it may depend on external state), so this is strike-counted rather
    /// than dead-lettered on sight.
    MappingRejected(String),
    /// The dispatch target refused the message with a deterministic client
    /// error (`4xx`) — e.g. published-schema validation (issue #373) or a
    /// mutually-exclusive start option. Retrying cannot help.
    TargetRejected {
        /// The HTTP status the dispatch path returned.
        status: u16,
        /// A bounded excerpt of the refusal body, for the dead-letter record.
        detail: String,
    },
    /// A transient harvest-side failure: a `5xx`, pool exhaustion, the runtime
    /// not being up yet. The message must stay unacknowledged so the broker
    /// redelivers it.
    Transient(String),
}

/// Where poison messages go when a binding gives up on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeadLetterMode {
    /// Write a durable harvest-side dead-letter record, then acknowledge the
    /// message so it leaves the broker. The default: it works for every
    /// broker, including those with no native dead-letter concept.
    #[default]
    HarvestSink,
    /// Leave the message for the broker's own dead-letter machinery (an SQS
    /// redrive policy, a Kafka DLQ topic wired by the operator) by abandoning
    /// it rather than acknowledging it.
    BrokerNative,
}

/// What the runtime must do with a message once dispatch has been attempted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageDisposition {
    /// Dispatch is durable (or was already durable). Acknowledge the message.
    Ack,
    /// Transient failure. Leave the message unacknowledged so the broker
    /// redelivers it.
    Retry,
    /// Terminal failure with a harvest-side sink configured: write the
    /// dead-letter record, *then* acknowledge.
    DeadLetter(PoisonReason),
    /// Terminal failure with broker-native dead-lettering configured: abandon
    /// the message and let the broker's redrive policy move it.
    AbandonToBrokerDeadLetter(PoisonReason),
}

impl MessageDisposition {
    /// The bounded metric outcome this disposition reports.
    #[must_use]
    pub const fn outcome(&self) -> ConnectorOutcome {
        match self {
            Self::Ack => ConnectorOutcome::Dispatched,
            Self::Retry => ConnectorOutcome::Retried,
            Self::DeadLetter(_) | Self::AbandonToBrokerDeadLetter(_) => {
                ConnectorOutcome::DeadLettered
            }
        }
    }

    /// The poison reason, when this disposition is a dead-letter.
    #[must_use]
    pub const fn poison_reason(&self) -> Option<PoisonReason> {
        match self {
            Self::DeadLetter(r) | Self::AbandonToBrokerDeadLetter(r) => Some(*r),
            Self::Ack | Self::Retry => None,
        }
    }
}

/// The bounded metric outcome for a *successful* dispatch outcome.
///
/// Distinct from [`MessageDisposition::outcome`] because a fresh dispatch, an
/// idempotent replay and a deferred admission all resolve to
/// [`MessageDisposition::Ack`] but are worth telling apart on the counter.
#[must_use]
pub const fn success_outcome(outcome: &DispatchOutcome) -> Option<ConnectorOutcome> {
    match outcome {
        DispatchOutcome::Dispatched { .. } => Some(ConnectorOutcome::Dispatched),
        DispatchOutcome::IdempotentReplay { .. } => Some(ConnectorOutcome::IdempotentReplay),
        DispatchOutcome::Deferred { .. } => Some(ConnectorOutcome::Deferred),
        DispatchOutcome::Malformed(_)
        | DispatchOutcome::MappingRejected(_)
        | DispatchOutcome::TargetRejected { .. }
        | DispatchOutcome::Transient(_) => None,
    }
}

/// Decide what to do with a message given its dispatch outcome.
///
/// `strikes_after_increment` is this message's consecutive-rejection count
/// *including* the current attempt (mirroring
/// `poison_pill::quarantine_decision`'s parameter, issue #367). `threshold`
/// is the binding's configured poison threshold; `0` disables strike-based
/// quarantine.
///
/// # The `threshold == 0` opt-out
///
/// Setting `0` disables quarantine for [`DispatchOutcome::MappingRejected`]
/// only, matching `poison_pill_threshold`'s documented "retry forever"
/// escape hatch. [`DispatchOutcome::Malformed`] and
/// [`DispatchOutcome::TargetRejected`] are **always** dead-lettered because
/// they are deterministic by construction — retrying them forever would wedge
/// a partition, which is precisely the failure this feature exists to
/// prevent.
#[must_use]
pub const fn decide_disposition(
    outcome: &DispatchOutcome,
    strikes_after_increment: u32,
    threshold: u32,
    mode: DeadLetterMode,
) -> MessageDisposition {
    match outcome {
        // Durable, or already durable, or durably parked -> safe to ack.
        DispatchOutcome::Dispatched { .. }
        | DispatchOutcome::IdempotentReplay { .. }
        | DispatchOutcome::Deferred { .. } => MessageDisposition::Ack,

        // Deterministic failures: never retryable, never strike-counted.
        DispatchOutcome::Malformed(_) => dead_letter(PoisonReason::Malformed, mode),
        DispatchOutcome::TargetRejected { .. } => dead_letter(PoisonReason::TargetRejected, mode),

        // Possibly-transient rejection: strike-counted against the threshold.
        DispatchOutcome::MappingRejected(_) => {
            if threshold > 0 && strikes_after_increment >= threshold {
                dead_letter(PoisonReason::MappingRejected, mode)
            } else {
                MessageDisposition::Retry
            }
        }

        // Harvest-side transient failure -> broker redelivers.
        DispatchOutcome::Transient(_) => MessageDisposition::Retry,
    }
}

const fn dead_letter(reason: PoisonReason, mode: DeadLetterMode) -> MessageDisposition {
    match mode {
        DeadLetterMode::HarvestSink => MessageDisposition::DeadLetter(reason),
        DeadLetterMode::BrokerNative => MessageDisposition::AbandonToBrokerDeadLetter(reason),
    }
}

/// In-process consecutive-rejection counter, keyed by derived idempotency key.
///
/// Deliberately in-process (not durable), matching the scope of
/// `poison_pill`'s in-memory strike handling: the only strike-counted outcome
/// is [`DispatchOutcome::MappingRejected`], and a mapping function that
/// rejects is usually deterministic — in which case the *first*
/// [`DispatchOutcome::Malformed`]/[`DispatchOutcome::TargetRejected`] path
/// dead-letters immediately anyway. A connector restart resets the counter,
/// which at worst costs `threshold` extra redeliveries of one message.
#[derive(Debug, Default)]
pub struct PoisonTracker {
    strikes: HashMap<String, u32>,
}

impl PoisonTracker {
    /// A tracker with no recorded strikes.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one more consecutive rejection for `key` and return the new
    /// count (saturating, so a pathological message cannot overflow).
    pub fn strike(&mut self, key: &str) -> u32 {
        let entry = self.strikes.entry(key.to_string()).or_insert(0);
        *entry = entry.saturating_add(1);
        *entry
    }

    /// Forget `key`'s strikes — called once a message reaches any terminal
    /// disposition, so the map cannot grow without bound.
    pub fn clear(&mut self, key: &str) {
        self.strikes.remove(key);
    }

    /// Current strike count for `key`, for assertions and diagnostics.
    #[must_use]
    pub fn strikes(&self, key: &str) -> u32 {
        self.strikes.get(key).copied().unwrap_or(0)
    }

    /// Number of messages currently holding strikes.
    #[must_use]
    pub fn tracked(&self) -> usize {
        self.strikes.len()
    }
}

/// Per-partition contiguous-prefix offset tracker for ordered brokers.
///
/// # Why a contiguous prefix
///
/// Kafka's commit is a *high-water mark*: committing offset `N` asserts that
/// everything below `N` is done. With the connector dispatching several
/// messages from one partition concurrently, message `N+1` can finish before
/// `N`. Committing `N+1` immediately would silently skip `N` if the process
/// died — the message would never be redelivered and its workflow would never
/// start.
///
/// This tracker therefore only ever reports the **contiguous completed
/// prefix**, so the committed high-water mark can never run ahead of an
/// in-flight message. That is what makes "ack only after the dispatch
/// committed" true for a partitioned broker under concurrency, not just for a
/// per-message-ack broker like SQS.
///
/// # Offsets the broker never delivers
///
/// A partition's *delivered* offsets are not guaranteed contiguous. Kafka
/// reserves offsets for transaction control records, aborted-transaction
/// records are filtered out under `read_committed`, and log compaction can
/// remove a record entirely. A tracker that waited for an offset it will never
/// be handed would stall that partition's commit **permanently** — the
/// messages are all processed, but the mark never advances, so every restart
/// replays them.
///
/// So the tracker distinguishes *in flight* (delivered to us, not yet
/// completed — must block) from *never delivered* (a hole below the highest
/// offset we have actually seen — safe to step over). Only the second is
/// skipped, and only strictly below the highest delivered offset, so the mark
/// can never run ahead into offsets the broker may still hand us.
#[derive(Debug, Default)]
pub struct OffsetTracker {
    partitions: HashMap<i32, PartitionOffsets>,
}

#[derive(Debug, Default)]
struct PartitionOffsets {
    /// Offsets delivered to us but not yet completed. These block the prefix;
    /// an offset absent from BOTH this and `completed` was never delivered.
    inflight: std::collections::BTreeSet<i64>,
    /// Offsets completed out of order, awaiting a contiguous prefix.
    completed: std::collections::BTreeSet<i64>,
    /// Highest offset known contiguous-complete, if any.
    committed: Option<i64>,
    /// Lowest offset this tracker has ever seen for the partition, which
    /// anchors the prefix after a rebalance/seek.
    floor: Option<i64>,
    /// Highest offset ever delivered for the partition. A gap is only safe to
    /// step over when it lies strictly below this.
    ceiling: Option<i64>,
}

impl OffsetTracker {
    /// A tracker with no observed partitions.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Note that `offset` on `partition` is in flight.
    ///
    /// Establishes the prefix anchor so the first completed offset after a
    /// rebalance is not mistaken for a gap, and records the offset as
    /// delivered so it blocks the prefix until it completes (as distinct from
    /// an offset the broker never handed us, which is stepped over).
    ///
    /// A **fresh** delivery at or below the current mark means the partition
    /// was repositioned behind us — an operator reset the group offset, or a
    /// rebalance handed the partition back after another consumer moved it.
    /// The previous generation's mark is no longer authoritative, so the
    /// partition's state is reset and the prefix rebuilt from here. Keeping it
    /// would let one low offset's completion commit the stale higher mark and
    /// silently skip everything between. A redelivery of an offset that is
    /// still in flight (or completed and held) is *not* a reposition, so it
    /// leaves the live prefix alone.
    pub fn observe(&mut self, partition: i32, offset: i64) {
        let entry = self.partitions.entry(partition).or_default();
        if entry.committed.is_some_and(|c| offset <= c)
            && !entry.inflight.contains(&offset)
            && !entry.completed.contains(&offset)
        {
            *entry = PartitionOffsets::default();
        }
        entry.floor = Some(entry.floor.map_or(offset, |f| f.min(offset)));
        entry.ceiling = Some(entry.ceiling.map_or(offset, |c| c.max(offset)));
        // Below the mark it is already settled; re-adding would re-block a
        // prefix that has legitimately moved past it.
        if entry.committed.is_none_or(|c| offset > c) {
            entry.inflight.insert(offset);
        }
    }

    /// Mark `offset` on `partition` durably handled.
    ///
    /// Returns the committable high-water mark (the highest contiguously
    /// completed offset) when this completion makes one available, else
    /// `None` because an earlier offset is still in flight.
    pub fn complete(&mut self, partition: i32, offset: i64) -> Option<i64> {
        let entry = self.partitions.entry(partition).or_default();
        entry.floor = Some(entry.floor.map_or(offset, |f| f.min(offset)));
        entry.ceiling = Some(entry.ceiling.map_or(offset, |c| c.max(offset)));
        entry.inflight.remove(&offset);

        // A redelivery at or below the high-water mark is ALREADY durably
        // settled — a rebalance or an un-acked crash replays it. Report the
        // existing mark so the caller still acknowledges (a commit is
        // idempotent) instead of silently withholding the ack and leaving the
        // message to be redelivered forever.
        if let Some(committed) = entry.committed
            && offset <= committed
        {
            return Some(committed);
        }

        entry.completed.insert(offset);

        // The next offset we expect is one past the last commit, or the very
        // first offset we ever saw for this partition.
        let mut next = entry
            .committed
            .map_or_else(|| entry.floor.unwrap_or(offset), |c| c + 1);

        let mut advanced = None;
        loop {
            if entry.completed.remove(&next) {
                advanced = Some(next);
                entry.committed = Some(next);
                next += 1;
                continue;
            }
            // `next` is not completed. Step over it ONLY when the broker never
            // delivered it AND it lies strictly below the highest offset we
            // have seen — i.e. it is a hole the broker itself skipped (a
            // control record, an aborted-transaction record, a compacted-away
            // key), not an offset still to come. An offset that IS in flight
            // blocks, which is the whole point of the contiguous prefix.
            if entry.inflight.contains(&next) || entry.ceiling.is_none_or(|c| next >= c) {
                break;
            }
            advanced = Some(next);
            entry.committed = Some(next);
            next += 1;
        }
        advanced
    }

    /// The current committable high-water mark for `partition`.
    #[must_use]
    pub fn committable(&self, partition: i32) -> Option<i64> {
        self.partitions.get(&partition).and_then(|p| p.committed)
    }

    /// Drop all state for `partition` (a rebalance revoked it).
    pub fn forget(&mut self, partition: i32) {
        self.partitions.remove(&partition);
    }

    /// How many completed offsets `partition` is holding behind a blocked
    /// prefix head.
    ///
    /// Zero at rest. Bounded by the runtime's `max_in_flight` under healthy
    /// out-of-order settlement, because only that many messages are ever
    /// outstanding. It grows without bound only when the head *never* settles.
    #[must_use]
    pub fn held(&self, partition: i32) -> usize {
        self.partitions
            .get(&partition)
            .map_or(0, |p| p.completed.len())
    }

    /// The first partition holding at least `threshold` completed offsets.
    ///
    /// This is the stall signal. A prefix head that never settles — a message
    /// abandoned for retry on a broker whose `abandon` cannot force a
    /// redelivery (Kafka: not committing is not a nack) — blocks its
    /// partition's commit **permanently** while every later message settles
    /// behind it. The tracker cannot fix that on its own: only re-reading from
    /// the last commit will hand the message back. Reporting the stall lets
    /// the runtime fail its pass loudly so the supervisor recreates the
    /// consumer, which is what performs the retry.
    ///
    /// `threshold == 0` disables the check.
    #[must_use]
    pub fn stalled(&self, threshold: usize) -> Option<(i32, usize)> {
        if threshold == 0 {
            return None;
        }
        // Deterministic: report the lowest-numbered blocked partition rather
        // than whichever the hash map happens to yield first.
        self.partitions
            .iter()
            .filter(|(_, p)| p.completed.len() >= threshold)
            .min_by_key(|(partition, _)| **partition)
            .map(|(partition, p)| (*partition, p.completed.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dispatched() -> DispatchOutcome {
        DispatchOutcome::Dispatched {
            execution_id: Some("e1".to_string()),
            workflow_id: "w1".to_string(),
        }
    }

    fn replay() -> DispatchOutcome {
        DispatchOutcome::IdempotentReplay {
            execution_id: Some("e1".to_string()),
            workflow_id: "w1".to_string(),
        }
    }

    fn deferred() -> DispatchOutcome {
        DispatchOutcome::Deferred {
            workflow_id: "w1".to_string(),
        }
    }

    #[test]
    fn a_backward_delivery_below_the_mark_resets_the_partition_generation() {
        // A partition can be handed back to us positioned BEHIND our in-memory
        // mark: an operator resets the group offset, or a rebalance gives us a
        // partition another consumer advanced differently. Keeping the stale
        // higher mark would commit it after processing one low offset,
        // silently skipping every offset in between.
        let mut t = OffsetTracker::new();
        for offset in 0..=5 {
            t.observe(0, offset);
            t.complete(0, offset);
        }
        assert_eq!(t.committable(0), Some(5));

        // Now the broker feeds us offset 1 again as a *fresh* delivery.
        t.observe(0, 1);
        assert_eq!(
            t.committable(0),
            None,
            "a backward fresh delivery invalidates the previous generation's mark"
        );
        assert_eq!(
            t.complete(0, 1),
            Some(1),
            "the rebuilt prefix commits what this generation actually settled"
        );
    }

    #[test]
    fn a_redelivery_of_an_in_flight_offset_does_not_reset_the_generation() {
        // Same offset delivered twice before it settles (a duplicate receive,
        // not a seek). Resetting here would throw away a live prefix.
        let mut t = OffsetTracker::new();
        t.observe(0, 0);
        t.complete(0, 0);
        t.observe(0, 1);
        t.observe(0, 1);
        assert_eq!(t.committable(0), Some(0));
        assert_eq!(t.complete(0, 1), Some(1));
    }

    #[test]
    fn a_settled_redelivery_still_re_acks_after_the_reset() {
        // The reset must not break the "redelivery of an already-settled
        // offset is re-acked, never silently withheld" contract: after the
        // reset the offset is simply re-settled and reported.
        let mut t = OffsetTracker::new();
        t.observe(0, 7);
        assert_eq!(t.complete(0, 7), Some(7));
        t.observe(0, 7);
        assert_eq!(t.complete(0, 7), Some(7));
    }

    #[test]
    fn forgetting_a_revoked_partition_leaves_others_untouched() {
        let mut t = OffsetTracker::new();
        t.observe(0, 3);
        t.complete(0, 3);
        t.observe(1, 9);
        t.complete(1, 9);
        t.forget(0);
        assert_eq!(t.committable(0), None);
        assert_eq!(t.committable(1), Some(9));
    }

    // ---- Stall detection (a blocked prefix must not be silent) ----

    #[test]
    fn a_blocked_prefix_accumulates_held_completions() {
        let mut t = OffsetTracker::new();
        // Offset 0 is delivered and then abandoned (a retry): it never
        // completes, so it blocks the prefix forever on a broker whose
        // `abandon` cannot force a redelivery.
        t.observe(0, 0);
        for offset in 1..=4 {
            t.observe(0, offset);
            assert_eq!(
                t.complete(0, offset),
                None,
                "offset 0 is still in flight, so nothing is committable"
            );
        }
        assert_eq!(t.held(0), 4);
        assert_eq!(t.held(99), 0, "an unknown partition holds nothing");

        // The head settling drains everything at once.
        t.complete(0, 0);
        assert_eq!(t.held(0), 0);
    }

    #[test]
    fn stall_detection_is_opt_in_and_reports_the_blocked_partition() {
        let mut t = OffsetTracker::new();
        t.observe(0, 0);
        for offset in 1..=3 {
            t.observe(0, offset);
            t.complete(0, offset);
        }
        // Threshold 0 disables the check entirely.
        assert_eq!(t.stalled(0), None);
        // Below the bound, a held prefix is normal out-of-order settlement.
        assert_eq!(t.stalled(4), None);
        // At the bound it is reported, naming the partition and the depth.
        assert_eq!(t.stalled(3), Some((0, 3)));
        assert_eq!(t.stalled(1), Some((0, 3)));
    }

    // ---- Ack ordering (AC: ack only after a durable outcome) ----

    #[test]
    fn durable_outcomes_ack() {
        for outcome in [dispatched(), replay(), deferred()] {
            assert_eq!(
                decide_disposition(&outcome, 0, 3, DeadLetterMode::HarvestSink),
                MessageDisposition::Ack,
                "{outcome:?} is durable and must ack",
            );
        }
    }

    #[test]
    fn transient_failure_never_acks_so_the_broker_redelivers() {
        assert_eq!(
            decide_disposition(
                &DispatchOutcome::Transient("pool exhausted".to_string()),
                1,
                3,
                DeadLetterMode::HarvestSink,
            ),
            MessageDisposition::Retry,
        );
        // ...and stays Retry no matter how many strikes accumulate: a 5xx is
        // never the message's fault, so it must not be dead-lettered.
        assert_eq!(
            decide_disposition(
                &DispatchOutcome::Transient("503".to_string()),
                99,
                3,
                DeadLetterMode::HarvestSink,
            ),
            MessageDisposition::Retry,
        );
    }

    // ---- Throttle composition (AC: 202 counts as success, message acked) ----

    #[test]
    fn deferred_admission_is_success_not_a_retry() {
        // The throttle owns pacing (issue #607); busy-retrying a deferred
        // start would defeat it and stampede the admission path.
        assert_eq!(
            decide_disposition(&deferred(), 0, 3, DeadLetterMode::HarvestSink),
            MessageDisposition::Ack,
        );
        assert_eq!(
            success_outcome(&deferred()),
            Some(ConnectorOutcome::Deferred),
        );
    }

    // ---- Poison isolation (AC: one bad message never wedges a partition) ----

    #[test]
    fn malformed_dead_letters_immediately_regardless_of_strikes() {
        let outcome = DispatchOutcome::Malformed("expected object".to_string());
        for strikes in [0_u32, 1, 7] {
            assert_eq!(
                decide_disposition(&outcome, strikes, 3, DeadLetterMode::HarvestSink),
                MessageDisposition::DeadLetter(PoisonReason::Malformed),
            );
        }
    }

    #[test]
    fn target_rejection_dead_letters_immediately() {
        let outcome = DispatchOutcome::TargetRejected {
            status: 400,
            detail: "input validation failed".to_string(),
        };
        assert_eq!(
            decide_disposition(&outcome, 0, 3, DeadLetterMode::HarvestSink),
            MessageDisposition::DeadLetter(PoisonReason::TargetRejected),
        );
    }

    #[test]
    fn mapping_rejection_retries_below_threshold_then_dead_letters() {
        let outcome = DispatchOutcome::MappingRejected("no tenant".to_string());
        assert_eq!(
            decide_disposition(&outcome, 1, 3, DeadLetterMode::HarvestSink),
            MessageDisposition::Retry,
        );
        assert_eq!(
            decide_disposition(&outcome, 2, 3, DeadLetterMode::HarvestSink),
            MessageDisposition::Retry,
        );
        assert_eq!(
            decide_disposition(&outcome, 3, 3, DeadLetterMode::HarvestSink),
            MessageDisposition::DeadLetter(PoisonReason::MappingRejected),
        );
        assert_eq!(
            decide_disposition(&outcome, 4, 3, DeadLetterMode::HarvestSink),
            MessageDisposition::DeadLetter(PoisonReason::MappingRejected),
        );
    }

    #[test]
    fn zero_threshold_disables_strike_quarantine_but_not_deterministic_ones() {
        // Mirrors poison_pill_threshold = 0 (retry forever) for the
        // strike-counted case only...
        let rejected = DispatchOutcome::MappingRejected("x".to_string());
        assert_eq!(
            decide_disposition(&rejected, 99, 0, DeadLetterMode::HarvestSink),
            MessageDisposition::Retry,
        );
        // ...but a deterministically-bad message still dead-letters, because
        // retrying it forever is exactly the partition wedge we forbid.
        assert_eq!(
            decide_disposition(
                &DispatchOutcome::Malformed("x".to_string()),
                0,
                0,
                DeadLetterMode::HarvestSink,
            ),
            MessageDisposition::DeadLetter(PoisonReason::Malformed),
        );
    }

    #[test]
    fn broker_native_mode_abandons_instead_of_acking() {
        assert_eq!(
            decide_disposition(
                &DispatchOutcome::Malformed("x".to_string()),
                0,
                3,
                DeadLetterMode::BrokerNative,
            ),
            MessageDisposition::AbandonToBrokerDeadLetter(PoisonReason::Malformed),
        );
        // A durable outcome still acks in broker-native mode.
        assert_eq!(
            decide_disposition(&dispatched(), 0, 3, DeadLetterMode::BrokerNative),
            MessageDisposition::Ack,
        );
    }

    #[test]
    fn dispositions_map_to_bounded_metric_outcomes() {
        assert_eq!(
            MessageDisposition::Ack.outcome(),
            ConnectorOutcome::Dispatched
        );
        assert_eq!(
            MessageDisposition::Retry.outcome(),
            ConnectorOutcome::Retried
        );
        assert_eq!(
            MessageDisposition::DeadLetter(PoisonReason::Malformed).outcome(),
            ConnectorOutcome::DeadLettered,
        );
        assert_eq!(
            MessageDisposition::DeadLetter(PoisonReason::Malformed).poison_reason(),
            Some(PoisonReason::Malformed),
        );
        assert_eq!(MessageDisposition::Ack.poison_reason(), None);
    }

    #[test]
    fn success_outcome_distinguishes_fresh_replay_and_deferred() {
        assert_eq!(
            success_outcome(&dispatched()),
            Some(ConnectorOutcome::Dispatched)
        );
        assert_eq!(
            success_outcome(&replay()),
            Some(ConnectorOutcome::IdempotentReplay)
        );
        assert_eq!(
            success_outcome(&DispatchOutcome::Transient("x".to_string())),
            None
        );
    }

    // ---- PoisonTracker ----

    #[test]
    fn poison_tracker_counts_consecutive_strikes_and_clears() {
        let mut t = PoisonTracker::new();
        assert_eq!(t.strike("k"), 1);
        assert_eq!(t.strike("k"), 2);
        assert_eq!(t.strikes("k"), 2);
        assert_eq!(t.strike("other"), 1, "keys are independent");
        assert_eq!(t.tracked(), 2);
        t.clear("k");
        assert_eq!(t.strikes("k"), 0);
        assert_eq!(t.tracked(), 1, "cleared keys must not leak");
    }

    #[test]
    fn poison_tracker_saturates_rather_than_overflowing() {
        let mut t = PoisonTracker::new();
        t.strikes.insert("k".to_string(), u32::MAX);
        assert_eq!(t.strike("k"), u32::MAX);
    }

    // ---- OffsetTracker (Kafka contiguous-prefix commit) ----

    #[test]
    fn offsets_completed_in_order_advance_one_by_one() {
        let mut t = OffsetTracker::new();
        t.observe(0, 10);
        assert_eq!(t.complete(0, 10), Some(10));
        assert_eq!(t.complete(0, 11), Some(11));
        assert_eq!(t.committable(0), Some(11));
    }

    #[test]
    fn out_of_order_completion_never_commits_past_an_in_flight_offset() {
        // The money test: 11 and 12 finish while 10 is still in flight.
        // Committing 12 would silently skip 10 on a crash.
        let mut t = OffsetTracker::new();
        t.observe(0, 10);
        t.observe(0, 11);
        t.observe(0, 12);
        assert_eq!(t.complete(0, 11), None, "must not commit past in-flight 10");
        assert_eq!(t.complete(0, 12), None, "still blocked on 10");
        assert_eq!(t.committable(0), None);
        // 10 lands: the whole contiguous prefix becomes committable at once.
        assert_eq!(t.complete(0, 10), Some(12));
        assert_eq!(t.committable(0), Some(12));
    }

    #[test]
    fn partitions_are_tracked_independently() {
        let mut t = OffsetTracker::new();
        t.observe(0, 5);
        t.observe(1, 100);
        assert_eq!(t.complete(1, 100), Some(100));
        assert_eq!(t.committable(0), None);
        assert_eq!(t.complete(0, 5), Some(5));
        assert_eq!(t.committable(1), Some(100));
    }

    #[test]
    fn first_offset_after_a_seek_anchors_the_prefix() {
        // Consumption starting at a non-zero offset (a resumed group) must not
        // wait forever for offsets 0..N-1 that this consumer never saw.
        let mut t = OffsetTracker::new();
        t.observe(0, 5_000);
        assert_eq!(t.complete(0, 5_000), Some(5_000));
    }

    #[test]
    fn a_redelivered_offset_at_or_below_the_mark_is_still_ackable() {
        // A rebalance (or a crash between dispatch-commit and ack) replays an
        // offset the tracker already committed. The naive contiguous-prefix
        // advance returns None for it — `next` is already past `offset` — so
        // the runtime would silently withhold the ack and the broker would
        // redeliver that message forever. Report the existing mark instead: a
        // Kafka commit is idempotent, so re-committing it is free and correct.
        let mut t = OffsetTracker::new();
        t.observe(0, 10);
        assert_eq!(t.complete(0, 10), Some(10));
        assert_eq!(t.complete(0, 11), Some(11));

        // Replay of the last committed offset, and of one below it.
        assert_eq!(t.complete(0, 11), Some(11), "redelivery must stay ackable");
        assert_eq!(t.complete(0, 10), Some(11), "reports the current mark");

        // The mark never moves backwards, and a genuinely new offset still
        // advances normally afterwards.
        assert_eq!(t.committable(0), Some(11));
        assert_eq!(t.complete(0, 12), Some(12));
    }

    #[test]
    fn a_redelivery_does_not_resurrect_an_in_flight_gap() {
        // 10 is still in flight while 11 completes; a replay of an already
        // committed offset must not paper over the gap by advancing past 10.
        let mut t = OffsetTracker::new();
        t.observe(0, 9);
        t.observe(0, 10);
        t.observe(0, 11);
        assert_eq!(t.complete(0, 9), Some(9));
        assert_eq!(t.complete(0, 11), None, "blocked on in-flight 10");
        assert_eq!(t.complete(0, 9), Some(9), "replay reports the mark only");
        assert_eq!(t.committable(0), Some(9), "the gap at 10 still holds");
        assert_eq!(t.complete(0, 10), Some(11));
    }

    #[test]
    fn forget_drops_revoked_partition_state() {
        let mut t = OffsetTracker::new();
        t.observe(0, 1);
        assert_eq!(t.complete(0, 1), Some(1));
        t.forget(0);
        assert_eq!(t.committable(0), None);
        // A fresh assignment re-anchors cleanly.
        t.observe(0, 900);
        assert_eq!(t.complete(0, 900), Some(900));
    }

    #[test]
    fn completing_without_observing_still_anchors() {
        let mut t = OffsetTracker::new();
        assert_eq!(t.complete(2, 77), Some(77));
        assert_eq!(t.complete(2, 78), Some(78));
    }

    #[test]
    fn an_offset_the_broker_never_delivered_does_not_wedge_the_prefix_forever() {
        // Kafka reserves offsets for transaction control records, filters
        // aborted-transaction records under `read_committed`, and compaction
        // can remove a record entirely -- so delivered offsets are NOT
        // contiguous. Waiting for a hole we will never be handed stalls this
        // partition's commit permanently: every message IS processed, but the
        // mark never advances, so each restart replays all of them.
        let mut t = OffsetTracker::new();
        t.observe(0, 10);
        t.observe(0, 11);
        // 12 is a control record; the broker hands us 13 next.
        t.observe(0, 13);

        assert_eq!(t.complete(0, 10), Some(10));
        // 13 was already delivered, which PROVES 12 is a hole -- so the mark
        // steps over it the moment 11 lands rather than waiting for a 12 that
        // is never coming.
        assert_eq!(
            t.complete(0, 11),
            Some(12),
            "the undelivered hole at 12 must be stepped over, not waited on",
        );
        assert_eq!(t.complete(0, 13), Some(13));
        assert_eq!(t.committable(0), Some(13));
    }

    #[test]
    fn an_undelivered_hole_is_only_stepped_over_below_the_highest_delivered_offset() {
        // The mark must never run ahead into offsets the broker may still
        // hand us -- that would silently skip a message on crash.
        let mut t = OffsetTracker::new();
        t.observe(0, 5);
        assert_eq!(t.complete(0, 5), Some(5));
        assert_eq!(
            t.committable(0),
            Some(5),
            "nothing above 5 was delivered, so the mark stops at 5",
        );
    }

    #[test]
    fn an_in_flight_gap_still_blocks_the_prefix() {
        // The distinction that makes stepping over holes safe: an offset we
        // WERE handed and have not finished must keep blocking.
        let mut t = OffsetTracker::new();
        t.observe(0, 1);
        t.observe(0, 2);
        t.observe(0, 3);

        assert_eq!(t.complete(0, 1), Some(1));
        assert_eq!(
            t.complete(0, 3),
            None,
            "2 is in flight, so the mark may not pass it",
        );
        assert_eq!(t.committable(0), Some(1));
        assert_eq!(t.complete(0, 2), Some(3), "2 lands, the prefix runs to 3");
    }

    #[test]
    fn a_hole_between_two_in_flight_offsets_resolves_once_both_land() {
        let mut t = OffsetTracker::new();
        t.observe(0, 20);
        t.observe(0, 21);
        // 22 undelivered.
        t.observe(0, 23);

        assert_eq!(t.complete(0, 21), None, "20 is still in flight");
        assert_eq!(t.complete(0, 23), None, "20 is still in flight");
        assert_eq!(
            t.complete(0, 20),
            Some(23),
            "20 lands: 21 completes it, 22 is a hole, 23 completes it",
        );
    }
}
