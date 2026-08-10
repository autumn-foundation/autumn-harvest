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
#[derive(Debug, Default)]
pub struct OffsetTracker {
    partitions: HashMap<i32, PartitionOffsets>,
}

#[derive(Debug, Default)]
struct PartitionOffsets {
    /// Offsets completed out of order, awaiting a contiguous prefix.
    completed: std::collections::BTreeSet<i64>,
    /// Highest offset known contiguous-complete, if any.
    committed: Option<i64>,
    /// Lowest offset this tracker has ever seen for the partition, which
    /// anchors the prefix after a rebalance/seek.
    floor: Option<i64>,
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
    /// rebalance is not mistaken for a gap.
    pub fn observe(&mut self, partition: i32, offset: i64) {
        let entry = self.partitions.entry(partition).or_default();
        entry.floor = Some(entry.floor.map_or(offset, |f| f.min(offset)));
    }

    /// Mark `offset` on `partition` durably handled.
    ///
    /// Returns the new committable high-water mark (the highest contiguously
    /// completed offset) when it advanced, else `None`.
    pub fn complete(&mut self, partition: i32, offset: i64) -> Option<i64> {
        let entry = self.partitions.entry(partition).or_default();
        entry.floor = Some(entry.floor.map_or(offset, |f| f.min(offset)));
        entry.completed.insert(offset);

        // The next offset we expect is one past the last commit, or the very
        // first offset we ever saw for this partition.
        let mut next = entry
            .committed
            .map_or_else(|| entry.floor.unwrap_or(offset), |c| c + 1);

        let mut advanced = None;
        while entry.completed.remove(&next) {
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
}
