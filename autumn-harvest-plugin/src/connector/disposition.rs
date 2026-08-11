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
    /// Leave the message for the broker's own dead-letter machinery by
    /// abandoning it rather than acknowledging it — an SQS queue with a redrive
    /// policy, or a custom adapter whose `abandon` genuinely nacks toward a
    /// destination.
    ///
    /// **Not available on Kafka, and not a matter of wiring a DLQ topic.**
    /// Abandoning is how this mode quarantines, and on Kafka `abandon` is a
    /// no-op — *not committing* is the entire redelivery mechanism — so the
    /// message would be re-read forever and reach no destination. Nor is it
    /// unconditional on SQS: without a redrive policy there is no target to
    /// move to. The adapter answers for itself via
    /// [`EventSource::has_native_dead_letter`](crate::connector::EventSource::has_native_dead_letter),
    /// [`broker_native_dead_letter_is_supported`] decides, and an unsupported
    /// pairing is rejected at build time rather than silently wedging the
    /// partition. Use [`Self::HarvestSink`] on Kafka.
    BrokerNative,
}

/// Whether a binding's dead-letter mode is one its adapter can actually
/// honour (issue #944).
///
/// Broker-native dead-lettering quarantines a poison message by *abandoning*
/// it and letting the broker route it away. On an adapter with no real nack
/// (Kafka: not committing IS the mechanism, so `abandon` is a no-op) that
/// never terminates — the message is re-read forever and reaches no
/// dead-letter destination at all, wedging the partition. Only a binding whose
/// adapter genuinely feeds a broker DLQ may use it.
///
/// Lives here, beside [`DeadLetterMode`], so the plugin's build-time check and
/// [`ConnectorRuntime::new`](crate::connector::ConnectorRuntime::new)'s own
/// guard cannot drift apart: the runtime is exported, so an embedder driving
/// it directly must hit the same rule.
#[must_use]
pub const fn broker_native_dead_letter_is_supported(
    mode: DeadLetterMode,
    adapter_has_native: bool,
) -> bool {
    !matches!(mode, DeadLetterMode::BrokerNative) || adapter_has_native
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
    strikes: HashMap<String, StrikeState>,
    /// Expiry order for terminal entries, oldest first.
    ///
    /// `Instant` is monotonic, so pushing on every mark keeps this sorted by
    /// construction and expiry is O(expired) rather than a scan of the map. A
    /// key may appear more than once (a broker-native message is re-nacked on
    /// every redelivery); only the entry whose instant matches the key's
    /// current `terminal_at` actually retires it, so the retention window
    /// runs from the *last* delivery.
    expiry: std::collections::VecDeque<(std::time::Instant, String)>,
    /// How many times the hard cap threw away an entry that still carried a
    /// live strike count.
    ///
    /// Non-zero means the active poison working set has outgrown
    /// [`MAX_POISON_ENTRIES`] and strike-based quarantine is degraded — see
    /// that constant for what that costs and how to mitigate it. Counted
    /// because the failure is otherwise invisible: every delivery still looks
    /// like an ordinary first rejection.
    strike_progress_discarded: u64,
    /// Whether the overflow warning has already been logged, so a sustained
    /// overflow reports once rather than once per evicted message.
    warned_overflow: bool,
}

#[derive(Debug)]
struct StrikeState {
    count: u32,
    /// When this key was last handed to the broker's own redrive, if ever.
    terminal_at: Option<std::time::Instant>,
    /// When this key was last touched — by a strike or a terminal mark.
    ///
    /// The ordering key for *both* the hard cap and retention expiry, so a
    /// non-terminal entry is bounded on the same footing as a terminal one.
    /// Without this, a key that never reaches its threshold (every first
    /// rejection of a large backlog, when the threshold is > 1) would be
    /// retained forever: `Retry` never clears it and only a terminal mark
    /// used to queue it for expiry.
    last_seen: std::time::Instant,
}

impl StrikeState {
    /// A brand-new entry: no strikes, not yet handed to the broker's redrive.
    ///
    /// Both entry points create one — `strike` for a rejection that may yet
    /// recover, `mark_terminal_as_of` for a deterministic poison that was
    /// never strike-counted at all — so naming it keeps the two aligned.
    const fn fresh(now: std::time::Instant) -> Self {
        Self {
            count: 0,
            terminal_at: None,
            last_seen: now,
        }
    }
}

/// Hard ceiling on retained poison strikes, terminal or not — applied
/// independently to the live map *and* to the expiry queue.
///
/// Time-based retention alone is still unbounded against a sustained stream
/// of *distinct* poison messages arriving faster than the window; this is what
/// makes the bound hard.
///
/// # What eviction costs
///
/// While the **active** poison working set fits inside the cap, evicting an
/// entry costs its message one extra retry lap before it re-reaches the
/// threshold — an efficiency cost, not a correctness one.
///
/// Past that the cost is real, and waiting does not recover it: with more than
/// `MAX_POISON_ENTRIES` messages being rejected in round-robin order and a
/// `poison_threshold` above 1, every key is evicted before its next delivery,
/// so every attempt is strike 1 and no message ever reaches the threshold —
/// the harvest dead-letter sink never fires. This is inherent to a bounded
/// in-process counter: tracking N concurrent strike streaks costs O(N), so a
/// working set larger than the bound must lose something. Storing strikes
/// durably would fix it and is deliberately not done — it buys a write on the
/// hot path, and a table, for a case with three cheaper answers:
///
/// * `poison_threshold(1)` dead-letters on the first rejection, so nothing has
///   to survive between deliveries and the cap stops mattering at all.
/// * An SQS redrive policy is the durable backstop at that scale:
///   `ApproximateReceiveCount` lives in the broker, is per-message, and
///   survives both eviction and process restarts.
/// * Kafka cannot reach the round-robin shape — a retried message blocks its
///   partition prefix, so the stall detector fires long before a working set
///   this large accumulates.
///
/// The degradation is **reported, not silent**:
/// [`PoisonTracker::strike_progress_discarded`] counts every eviction that
/// threw away a live strike count, and the first one logs a warning naming
/// those mitigations.
///
/// Both structures need the ceiling because they grow independently: a key
/// contributes one queued record per touch, and `clear` retires the key
/// without retiring its records. Bounding only the map leaves a
/// distinct-poison stream — which holds the map near empty — free to grow the
/// queue by one owned `String` per strike until the retention window drains it.
pub const MAX_POISON_ENTRIES: usize = 10_000;

/// Shortest window an **active** (non-terminal) strike streak is held for,
/// regardless of `poison_retention`.
///
/// A terminal entry is waiting out a window the *operator* chose: nothing will
/// ever clear it, so `poison_retention` is exactly the right clock. An active
/// streak is waiting for something else entirely — the next **redelivery** —
/// and that interval belongs to the broker. SQS hides a nacked message for its
/// visibility timeout, legally up to 12 hours, while the default
/// `poison_retention` is one; sweeping both on the operator's clock therefore
/// retires every strike before its message can come back, so every delivery is
/// strike 1, no message ever reaches `poison_threshold`, and the harvest
/// dead-letter sink never fires. The failure is silent — the tracker looks
/// healthy and the message looks freshly rejected each time.
///
/// The value is **twice** [`MAX_BROKER_INVISIBILITY`], SQS's maximum
/// visibility timeout. Equalling that maximum is not enough for two compounding
/// reasons: `run_once` sweeps *before* it polls, so at exactly the maximum the
/// strike is retired in the very pass that would then have received the
/// redelivery; and the real redelivery is the visibility timeout plus the poll
/// interval and whatever scheduling and queue delay the broker adds, so it
/// always lands strictly after it. How much slack is decided by the asymmetry:
/// expiring **late** costs bounded memory, expiring **early** silently disables
/// strike-based quarantine altogether, so the margin is deliberately generous
/// rather than tight.
///
/// It is a floor, never a cap: an operator who sets a longer
/// `poison_retention` still gets the longer window for both kinds.
///
/// Holding active entries longer is safe because time was never the hard bound
/// — [`MAX_POISON_ENTRIES`] is, and its eviction stays kind-blind precisely so
/// this cannot outgrow it. What changes is *which* mechanism retires a stale
/// active entry under load, and the cap already reports that as degradation via
/// [`PoisonTracker::strike_progress_discarded`].
pub const ACTIVE_STRIKE_RETENTION_FLOOR: std::time::Duration =
    MAX_BROKER_INVISIBILITY.saturating_mul(2);

/// The longest a broker in scope may legally hide a message before redelivering
/// it: SQS's maximum `VisibilityTimeout`, a hard AWS limit.
///
/// Named rather than inlined so [`ACTIVE_STRIKE_RETENTION_FLOOR`] derives from
/// it and the two cannot drift.
const MAX_BROKER_INVISIBILITY: std::time::Duration = std::time::Duration::from_secs(12 * 60 * 60);

impl PoisonTracker {
    /// A tracker with no recorded strikes.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one more consecutive rejection for `key` at `now` and return the
    /// new count (saturating, so a pathological message cannot overflow).
    ///
    /// Takes `now` so the entry joins the same bounded expiry order terminal
    /// marks use: a rejected message that never reaches its threshold is still
    /// capped and still ages out, rather than being retained until the process
    /// restarts.
    pub fn strike(&mut self, key: &str, now: std::time::Instant) -> u32 {
        let entry = self
            .strikes
            .entry(key.to_string())
            .or_insert_with(|| StrikeState::fresh(now));
        entry.count = entry.count.saturating_add(1);
        entry.last_seen = now;
        let count = entry.count;
        self.expiry.push_back((now, key.to_string()));
        self.enforce_cap();
        count
    }

    /// Forget `key`'s strikes — called once a message reaches a terminal
    /// disposition *harvest owns*, so the map cannot grow without bound.
    pub fn clear(&mut self, key: &str) {
        self.strikes.remove(key);
    }

    /// Note that `key` was handed to the broker's own redrive at `now`.
    ///
    /// This is terminal for harvest but not for the broker, so the strikes are
    /// deliberately kept (see the call site in `ConnectorRuntime::settle`) —
    /// but nothing downstream will ever clear them, which is why they carry a
    /// retention deadline instead.
    ///
    /// Returns whether this was the **first** handoff for `key`. Because the
    /// retained strikes make every later redelivery re-nack on sight, the same
    /// physical message reaches this path once per redrive lap; the caller
    /// counts `harvest.connector.poisoned` only on the first, so one poison
    /// message contributes one sample rather than one per lap. (The
    /// `dispatched` counter is deliberately *not* gated this way — every
    /// redelivery is a received message, and that family's documented
    /// invariant is that it sums to `received`.)
    ///
    /// An **absent** key is the deterministic-poison case, not a no-op:
    /// [`DispatchOutcome::Malformed`] and [`DispatchOutcome::TargetRejected`]
    /// are dead-lettered on sight and never strike-counted, so nothing has
    /// recorded the key by the time they land here. That first handoff is a
    /// genuine poison message and must count — reading a missing entry as
    /// "already counted" would zero `harvest.connector.poisoned` for the two
    /// most obvious poison classes. The entry this creates is also what makes
    /// the *next* redrive lap of that same message report false, and it is
    /// bounded exactly like a strike-bearing one: it joins the same expiry
    /// order and retires on the same retention deadline.
    pub fn mark_terminal_as_of(&mut self, key: &str, now: std::time::Instant) -> bool {
        let state = self
            .strikes
            .entry(key.to_string())
            .or_insert_with(|| StrikeState::fresh(now));
        let first = state.terminal_at.is_none();
        state.terminal_at = Some(now);
        state.last_seen = now;
        self.expiry.push_back((now, key.to_string()));
        self.enforce_cap();
        first
    }

    /// Note that `key` was quarantined into harvest's *own* dead-letter sink
    /// at `now`, restarting its strike countdown.
    ///
    /// Shares [`Self::mark_terminal_as_of`]'s dedupe contract — one physical
    /// poison message contributes one `harvest.connector.poisoned` sample
    /// however many times it comes back — and differs in exactly one respect:
    /// the strikes do **not** survive.
    ///
    /// The broker-native handoff keeps them because re-nacking on sight is the
    /// whole mechanism that lets the queue's own `maxReceiveCount` end the
    /// message. Nothing analogous applies here: the record is already durable
    /// and the message already acked, so it only returns when that ack failed
    /// — rare, and no reason to treat a fresh delivery as though it were mid-
    /// streak. Restarting the countdown is what the pre-dedupe `clear` did,
    /// and it is still the right strike behaviour; only the *counting* needed
    /// fixing.
    ///
    /// The entry is bounded exactly like any other: same expiry order, same
    /// retention deadline.
    pub fn mark_sink_terminal_as_of(&mut self, key: &str, now: std::time::Instant) -> bool {
        let first = self.mark_terminal_as_of(key, now);
        if let Some(state) = self.strikes.get_mut(key) {
            state.count = 0;
        }
        first
    }

    /// Evict oldest-first until *both* the live map and the expiry queue are
    /// back inside the hard ceiling.
    ///
    /// Applied at touch time, not at expiry time: a burst arriving between two
    /// passes must not be able to outrun the bound.
    ///
    /// The queue needs its own gate rather than riding on the map's. A key
    /// contributes one queued record per touch and `clear` retires the key
    /// without retiring its records, so a stream of distinct poison messages
    /// each cleared at its threshold holds the map near empty while the queue
    /// grows without limit. Popping to satisfy the queue's gate is cheap when
    /// the excess is stale records (each pop is a no-op discard) and, when it
    /// does reach a live key, falls back on the same oldest-touched eviction
    /// the map's gate uses.
    ///
    /// Terminates because every iteration pops exactly one queued record until
    /// the queue is empty, and the queue is finite.
    fn enforce_cap(&mut self) {
        while self.strikes.len() > MAX_POISON_ENTRIES || self.expiry.len() > MAX_POISON_ENTRIES {
            if !self.pop_oldest(true) && self.expiry.is_empty() {
                break;
            }
        }
    }

    /// Retire entries — terminal or not — untouched for the retention window.
    ///
    /// Returns how many keys were retired, for diagnostics.
    ///
    /// Non-terminal entries age out too, but never sooner than
    /// [`ACTIVE_STRIKE_RETENTION_FLOOR`]: what advances an active streak is a
    /// *redelivery*, so the interval it has to survive belongs to the broker,
    /// not to the operator. See that constant.
    ///
    /// The queue is FIFO and this breaks at its front, so a not-yet-due active
    /// entry holds back the due terminal entries behind it. That is memory
    /// only, and [`MAX_POISON_ENTRIES`] — whose eviction is deliberately
    /// kind-blind — is what bounds it. Skipping past instead would turn an
    /// O(expired) pass into a scan of the whole queue on every sweep.
    pub fn expire_stale_as_of(
        &mut self,
        now: std::time::Instant,
        retention: std::time::Duration,
    ) -> usize {
        let mut retired = 0;
        while let Some((at, key)) = self.expiry.front() {
            let due_after = if self
                .strikes
                .get(key)
                .is_some_and(|s| s.terminal_at.is_none())
            {
                retention.max(ACTIVE_STRIKE_RETENTION_FLOOR)
            } else {
                retention
            };
            if now.saturating_duration_since(*at) < due_after {
                break;
            }
            // `false`: retiring a stale entry is by design, not degradation. A
            // strike streak is *consecutive* by definition, so a key nothing
            // has rejected for a whole retention window is not mid-streak.
            if self.pop_oldest(false) {
                retired += 1;
            }
        }
        retired
    }

    /// Pop the oldest queued expiry entry, retiring its key when that entry is
    /// still the key's most recent touch.
    ///
    /// Returns whether a key was actually retired: a stale entry (the key was
    /// struck or re-marked later, or `clear`ed and re-struck) must be a no-op
    /// rather than disturbing live state.
    /// `from_cap` distinguishes a *forced* eviction (the hard ceiling) from a
    /// retention expiry, because only the former can discard a strike streak
    /// that was still accumulating — see [`MAX_POISON_ENTRIES`].
    fn pop_oldest(&mut self, from_cap: bool) -> bool {
        let Some((at, key)) = self.expiry.pop_front() else {
            return false;
        };
        let Some(state) = self.strikes.get(&key) else {
            return false;
        };
        if state.last_seen != at {
            return false;
        }
        let discarded_progress = from_cap && state.count > 0;
        self.strikes.remove(&key);
        if discarded_progress {
            self.strike_progress_discarded = self.strike_progress_discarded.saturating_add(1);
            if !self.warned_overflow {
                self.warned_overflow = true;
                tracing::warn!(
                    cap = MAX_POISON_ENTRIES,
                    "harvest connector poison tracker hit its hard cap and is discarding \
                     in-progress strike counts: the active poison working set is larger than \
                     the tracker can hold, so a message may never reach its poison_threshold \
                     and the harvest dead-letter sink may never fire. Mitigate with \
                     poison_threshold(1) (dead-letter on first rejection, so nothing has to \
                     survive between deliveries), or rely on an SQS redrive policy as the \
                     durable backstop"
                );
            }
        }
        true
    }

    /// Current strike count for `key`, for assertions and diagnostics.
    #[must_use]
    pub fn strikes(&self, key: &str) -> u32 {
        self.strikes.get(key).map_or(0, |s| s.count)
    }

    /// Number of messages currently holding strikes.
    #[must_use]
    pub fn tracked(&self) -> usize {
        self.strikes.len()
    }

    /// How many evictions have thrown away a live strike count.
    ///
    /// Zero on every healthy binding. Non-zero means the active poison working
    /// set has outgrown [`MAX_POISON_ENTRIES`], so strike-based quarantine is
    /// degraded and a message may never reach its `poison_threshold` — see that
    /// constant for the mitigations. Exposed because the failure is otherwise
    /// indistinguishable from ordinary first-time rejections.
    #[must_use]
    pub const fn strike_progress_discarded(&self) -> u64 {
        self.strike_progress_discarded
    }

    /// Number of queued expiry records, for assertions and diagnostics.
    ///
    /// Distinct from [`Self::tracked`]: a key contributes one queued record per
    /// touch, and `clear` retires the key without retiring its records, so this
    /// can far exceed the live key count and needs its own bound.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.expiry.len()
    }

    /// Force `key`'s count, so a test can reach the saturation boundary
    /// without calling `strike` four billion times.
    #[cfg(test)]
    fn set_strikes_for_test(&mut self, key: &str, count: u32) {
        let now = std::time::Instant::now();
        self.strikes
            .entry(key.to_string())
            .or_insert_with(|| StrikeState {
                count: 0,
                terminal_at: None,
                last_seen: now,
            })
            .count = count;
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
    /// Offsets abandoned for retry and not since settled. On a positional
    /// broker these never come back on their own, so each one is a permanently
    /// blocked prefix — the stall signal, independent of how much settles
    /// behind it.
    retried: std::collections::BTreeSet<i64>,
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
    ///
    /// The generation is anchored on the committed mark when there is one, and
    /// otherwise on `floor` — the lowest offset this generation has seen. A
    /// prefix whose head never settles never commits, so `committed` stays
    /// `None` indefinitely while `floor`/`ceiling` accumulate; anchoring only
    /// on `committed` would leave that stale `ceiling` in place across a
    /// reposition, and `ceiling` is precisely what licenses stepping over an
    /// undelivered offset as a broker hole. The mark would then leap over
    /// offsets that were never delivered in this generation and are still to
    /// come — committing past messages that were never dispatched.
    pub fn observe(&mut self, partition: i32, offset: i64) {
        let entry = self.partitions.entry(partition).or_default();
        if entry
            .committed
            .or(entry.floor)
            .is_some_and(|mark| offset <= mark)
            && !entry.inflight.contains(&offset)
            && !entry.completed.contains(&offset)
        {
            *entry = PartitionOffsets::default();
        }
        // The broker handed it back, so it is no longer a wedge.
        entry.retried.remove(&offset);
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
        // It settled after all, so it is no longer a blocked head.
        entry.retried.remove(&offset);

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
            // `next` is a hole — and so is every offset up to the next one this
            // tracker knows about, by the identical test: not completed, not in
            // flight, below the ceiling. Stepping through them one at a time is
            // O(numeric gap), and the gap is not bounded by anything this
            // process controls: nothing evicts a partition's mark when a
            // rebalance moves it away (`forget` has no production caller), so a
            // replica reacquiring a partition another replica advanced meanwhile
            // faces a gap as large as that partition's traffic — billions of
            // offsets. It runs under the offsets mutex, which `ack` holds across
            // commit submission, so walking it would stall every other
            // settlement on the runtime, not just this one.
            //
            // Jumping to the next tracked offset is the SAME advance, not an
            // approximation: the prefix ends up on exactly the offset the walk
            // would have left it on, and the next iteration re-tests that
            // offset through the ordinary cases. `ceiling` is in the candidate
            // set because the loop must never advance past it, and is `Some`
            // here — the break above already ruled out `None`.
            let after = next.saturating_add(1);
            let resume = entry
                .completed
                .range(after..)
                .next()
                .copied()
                .into_iter()
                .chain(entry.inflight.range(after..).next().copied())
                .chain(entry.ceiling)
                .filter(|candidate| *candidate > next)
                .min()
                .unwrap_or(after);
            // Everything below `resume` is a hole, so the prefix reaches the
            // offset just before it.
            let through = resume.saturating_sub(1);
            advanced = Some(through);
            entry.committed = Some(through);
            next = resume;
        }
        advanced
    }

    /// The current committable high-water mark for `partition`.
    #[must_use]
    pub fn committable(&self, partition: i32) -> Option<i64> {
        self.partitions.get(&partition).and_then(|p| p.committed)
    }

    /// Note that `offset` on `partition` was abandoned for retry.
    ///
    /// On a positional broker this is a **permanent** block by construction:
    /// `abandon` cannot force a redelivery (not committing is the whole
    /// mechanism) and the consumer's local position has already advanced past
    /// the offset, so nothing hands it back until the consumer is rebuilt.
    /// Recording it makes the stall visible immediately rather than only once
    /// enough later offsets happen to settle behind it — which never happens
    /// at the tail of a quiet partition.
    ///
    /// Only called for coordinates that carry a partition and offset, and only
    /// when the source reports
    /// [`abandon_redelivers() == false`][super::source::EventSource::abandon_redelivers]
    /// — a broker that genuinely hands the message back (SQS's visibility
    /// timeout) is not wedged by a retry and must not trigger a rebuild.
    pub fn retried(&mut self, partition: i32, offset: i64) {
        self.partitions
            .entry(partition)
            .or_default()
            .retried
            .insert(offset);
    }

    /// Drop all state for `partition` (a rebalance revoked it).
    pub fn forget(&mut self, partition: i32) {
        self.partitions.remove(&partition);
    }

    /// Drop every partition's state, for a **whole-client** consumer rebuild.
    ///
    /// [`EventSource::recover`](super::source::EventSource::recover) rebuilds
    /// the entire consumer, so *every* assigned partition re-reads from its own
    /// last commit — not just the one whose stall happened to be reported.
    /// Forgetting one and keeping the rest leaves those partitions' stale
    /// in-memory marks in place, and their redelivered offsets then arrive
    /// below those marks and are mistaken for already-settled redeliveries:
    /// the exact failure [`Self::forget`] exists to prevent.
    pub fn forget_all(&mut self) {
        self.partitions.clear();
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

    /// The first partition whose commit prefix is wedged, with how many
    /// completed offsets are piled up behind it.
    ///
    /// A prefix head that never settles blocks its partition's commit
    /// **permanently**: the tracker cannot fix that on its own, because only
    /// re-reading from the last commit hands the message back. Reporting it
    /// lets the runtime rebuild the source (see
    /// [`EventSource::recover`][super::source::EventSource::recover]), which is
    /// what actually performs the retry.
    ///
    /// Two independent signals, because either alone misses real wedges:
    ///
    /// - **A retried head** ([`Self::retried`]) is a wedge *by construction* on
    ///   a positional broker, and is reported immediately. This is the only
    ///   signal that catches a retry at the tail of a quiet partition, where
    ///   nothing ever settles behind it, so a volume bound would wait forever.
    /// - **A backlog** of at least `threshold` completed offsets is a
    ///   heuristic backstop for a head that is blocked without having gone
    ///   through the retry path (a dispatch task lost to a panic, say). Under
    ///   healthy out-of-order settlement the backlog is bounded by
    ///   `max_in_flight`, so a threshold above that only fires on a genuine
    ///   wedge.
    ///
    /// `threshold == 0` disables **only** the backlog heuristic. The retried
    /// head is a correctness signal, not a tunable: suppressing it on a
    /// positional broker means silently dropping the retried message.
    #[must_use]
    pub fn stalled(&self, threshold: usize) -> Option<(i32, usize)> {
        let wedged = |p: &&PartitionOffsets| {
            !p.retried.is_empty() || (threshold > 0 && p.completed.len() >= threshold)
        };
        // Deterministic: report the lowest-numbered blocked partition rather
        // than whichever the hash map happens to yield first.
        self.partitions
            .iter()
            .filter(|(_, p)| wedged(p))
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
    fn a_backward_reposition_resets_the_generation_even_before_the_first_commit() {
        // The reset above is anchored on the committed mark, but a partition
        // can be repositioned backward while the prefix has NEVER committed —
        // exactly the shape a retried head leaves, since the head blocks the
        // prefix forever and `committed` stays `None` while later offsets
        // settle behind it. Stale `floor`/`ceiling` survive that reset, and
        // `ceiling` is what licenses stepping over a gap as a "broker hole".
        let mut t = OffsetTracker::new();
        t.observe(0, 10);
        t.observe(0, 11);
        assert_eq!(t.complete(0, 11), None, "the unsettled head blocks");
        assert_eq!(t.committable(0), None, "so nothing has committed yet");

        // A rebalance hands the partition back positioned behind us at 5.
        // 6..=9 were never delivered in THIS generation; they are still to
        // come, not holes the broker skipped.
        t.observe(0, 5);
        let advanced = t.complete(0, 5);

        assert_eq!(
            advanced,
            Some(5),
            "the rebuilt prefix commits only what this generation settled; \
             against the stale ceiling of 11 it would leap to 9 and Kafka \
             would never redeliver 6..=9"
        );
        assert_eq!(t.committable(0), Some(5));
    }

    #[test]
    fn a_retried_head_stalls_its_partition_with_nothing_piled_behind_it() {
        // The volume signal cannot see a retry at the tail of a quiet
        // partition: nothing settles behind it, so `held` stays 0 forever
        // while the message is never redelivered (Kafka's `abandon` is a
        // no-op and the consumer position already moved past it).
        let mut t = OffsetTracker::new();
        t.observe(0, 7);
        t.retried(0, 7);

        assert_eq!(t.held(0), 0, "a quiet tail piles up nothing");
        assert_eq!(
            t.stalled(64),
            Some((0, 0)),
            "the blocked head is the stall, not the backlog behind it"
        );
    }

    #[test]
    fn an_in_flight_head_that_was_never_retried_is_not_a_stall() {
        // A message merely being dispatched must not trip recovery, or every
        // healthy pass would rebuild the consumer.
        let mut t = OffsetTracker::new();
        t.observe(0, 7);
        assert_eq!(t.stalled(64), None);
    }

    #[test]
    fn forgetting_a_partition_clears_its_retry_mark() {
        // Recovery forgets the partition; if the mark survived, the very next
        // pass would report the same stall and rebuild forever.
        let mut t = OffsetTracker::new();
        t.observe(0, 7);
        t.retried(0, 7);
        assert!(t.stalled(64).is_some());
        t.forget(0);
        assert_eq!(t.stalled(64), None);
    }

    #[test]
    fn a_retried_offset_that_settles_anyway_no_longer_stalls() {
        // A retry the broker DOES redeliver (SQS's visibility timeout, or a
        // Kafka rebuild) settles normally. Once settled it is not a wedge, so
        // it must not keep reporting one.
        let mut t = OffsetTracker::new();
        t.observe(0, 7);
        t.retried(0, 7);
        t.observe(0, 7);
        assert_eq!(t.complete(0, 7), Some(7));
        assert_eq!(t.stalled(64), None);
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
    fn broker_native_dead_lettering_requires_an_adapter_that_supports_it() {
        // The rejected combination: the mode is only honourable on an adapter
        // with a real nack. Kafka has none, so abandoning never terminates.
        assert!(!broker_native_dead_letter_is_supported(
            DeadLetterMode::BrokerNative,
            false,
        ));
        assert!(broker_native_dead_letter_is_supported(
            DeadLetterMode::BrokerNative,
            true,
        ));
        // The harvest sink always works -- it is the plugin's own table.
        assert!(broker_native_dead_letter_is_supported(
            DeadLetterMode::HarvestSink,
            false,
        ));
        assert!(broker_native_dead_letter_is_supported(
            DeadLetterMode::HarvestSink,
            true,
        ));
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
        let t0 = std::time::Instant::now();
        assert_eq!(t.strike("k", t0), 1);
        assert_eq!(t.strike("k", t0), 2);
        assert_eq!(t.strikes("k"), 2);
        assert_eq!(t.strike("other", t0), 1, "keys are independent");
        assert_eq!(t.tracked(), 2);
        t.clear("k");
        assert_eq!(t.strikes("k"), 0);
        assert_eq!(t.tracked(), 1, "cleared keys must not leak");
    }

    #[test]
    fn poison_tracker_saturates_rather_than_overflowing() {
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        t.strike("k", t0);
        t.set_strikes_for_test("k", u32::MAX);
        assert_eq!(t.strike("k", t0), u32::MAX);
    }

    #[test]
    fn broker_native_terminal_strikes_expire_after_the_redrive_window() {
        // `AbandonToBrokerDeadLetter` is terminal for harvest but NOT for the
        // broker: SQS resets visibility, redelivers, and eventually moves the
        // message to its own DLQ -- without telling this process. So no later
        // delivery and no terminal path can ever clear the key, and the
        // redrive policy bounds *deliveries of one message*, not the size of
        // this map across a stream of distinct poison messages. The retention
        // window is what bounds it.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let retention = std::time::Duration::from_secs(60);

        t.strike("m-1", t0);
        t.mark_terminal_as_of("m-1", t0);

        // Inside the window the strikes survive, so a redelivery re-nacks on
        // sight instead of crawling back through the strike countdown.
        t.expire_stale_as_of(t0 + std::time::Duration::from_secs(59), retention);
        assert_eq!(t.strikes("m-1"), 1, "the redrive is still in progress");

        // Past it the entry retires: nothing else will ever clear this key.
        t.expire_stale_as_of(t0 + std::time::Duration::from_secs(61), retention);
        assert_eq!(t.tracked(), 0, "a terminal strike must not leak forever");
    }

    #[test]
    fn only_the_first_broker_native_handoff_is_reported_as_poisoned() {
        // `harvest.connector.poisoned` counts POISON MESSAGES, so one physical
        // message must contribute exactly one sample no matter how many redrive
        // laps the broker takes. With `maxReceiveCount` above the binding's
        // threshold -- the normal configuration -- the retained strikes make
        // every redelivery resolve to `AbandonToBrokerDeadLetter` again, so an
        // unconditional count would inflate poison alerts by the lap count.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();

        t.strike("m-1", t0);
        assert!(
            t.mark_terminal_as_of("m-1", t0),
            "the first handoff is the one that counts"
        );
        // Redelivered and re-nacked: same physical message, already counted.
        t.strike("m-1", t0 + std::time::Duration::from_secs(50));
        assert!(
            !t.mark_terminal_as_of("m-1", t0 + std::time::Duration::from_secs(50)),
            "a redrive lap must not report a second poison message"
        );
        // A DIFFERENT message is a different poison message and does count.
        t.strike("m-2", t0);
        assert!(t.mark_terminal_as_of("m-2", t0));
        // A key with no strikes recorded is the DETERMINISTIC poison case and
        // has its own contract -- see
        // `a_deterministic_poison_counts_on_its_first_handoff_and_not_after`.
    }

    #[test]
    fn a_sink_quarantine_dedupes_the_sample_but_restarts_the_countdown() {
        // The harvest-sink handoff shares the broker-native one's dedupe
        // contract and differs in exactly one respect: it does NOT retain the
        // strikes. Both halves matter, so both are asserted here.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();

        assert_eq!(t.strike("m-1", t0), 1);
        assert_eq!(t.strike("m-1", t0), 2);
        assert!(
            t.mark_sink_terminal_as_of("m-1", t0),
            "the first quarantine is the one that counts"
        );

        // Restarted, not retained: a message that comes back because its ack
        // failed begins a fresh countdown, exactly as the pre-dedupe `clear`
        // made it. Retaining them would make it re-quarantine on sight and
        // re-write the sink record on every lap.
        assert_eq!(
            t.strike("m-1", t0),
            1,
            "the countdown must restart, not resume mid-streak"
        );

        // ...but the sample is still deduped across that return.
        assert!(
            !t.mark_sink_terminal_as_of("m-1", t0 + std::time::Duration::from_secs(5)),
            "a redelivery caused by a failed ack is the same physical message"
        );

        // A genuinely different message counts.
        assert!(t.mark_sink_terminal_as_of("m-2", t0));

        // Bounded like any other terminal entry — nothing downstream clears it.
        t.expire_stale_as_of(
            t0 + std::time::Duration::from_secs(70),
            std::time::Duration::from_secs(60),
        );
        assert_eq!(t.tracked(), 0);
    }

    #[test]
    fn a_deterministic_poison_counts_on_its_first_handoff_and_not_after() {
        // `Malformed` and `TargetRejected` are "never retryable, never
        // strike-counted": they are dead-lettered on sight, so the tracker has
        // no entry for the key when the handoff happens. Treating a missing
        // entry as "not the first" would zero `harvest.connector.poisoned` for
        // the two most obvious poison classes -- a silent blind spot, worse
        // than the per-lap inflation the first-handoff gate exists to stop.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let retention = std::time::Duration::from_secs(60);

        assert!(
            t.mark_terminal_as_of("never-struck", t0),
            "a deterministic poison's first handoff IS a poison message"
        );
        // The entry that first mark creates is what stops the next redrive lap
        // from reporting the same physical message a second time.
        assert!(
            !t.mark_terminal_as_of("never-struck", t0 + std::time::Duration::from_secs(5)),
            "a redrive lap of the same message must not count twice"
        );

        // And it is bounded like any other terminal entry: nothing downstream
        // can ever clear it, so the retention window has to.
        t.expire_stale_as_of(t0 + std::time::Duration::from_secs(70), retention);
        assert_eq!(
            t.tracked(),
            0,
            "a strike-less terminal entry must retire like any other"
        );
    }

    #[test]
    fn a_redelivered_broker_native_message_restarts_its_retention_window() {
        // The clock has to run from the LAST delivery, not the first: a
        // message whose `maxReceiveCount` is well above the binding's
        // `poison_threshold` comes back several times, and expiring it
        // mid-redrive would restart its strike countdown -- exactly the churn
        // keeping the strikes is meant to avoid.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let retention = std::time::Duration::from_secs(60);

        t.strike("m-1", t0);
        t.mark_terminal_as_of("m-1", t0);
        // Redelivered at +50s and re-nacked.
        t.strike("m-1", t0 + std::time::Duration::from_secs(50));
        t.mark_terminal_as_of("m-1", t0 + std::time::Duration::from_secs(50));

        t.expire_stale_as_of(t0 + std::time::Duration::from_secs(70), retention);
        assert_eq!(
            t.strikes("m-1"),
            2,
            "the window runs from the last delivery, not the first"
        );

        t.expire_stale_as_of(t0 + std::time::Duration::from_secs(111), retention);
        assert_eq!(t.tracked(), 0, "and it does expire once redrive is done");
    }

    #[test]
    fn terminal_strikes_are_hard_capped_so_a_poison_storm_cannot_exhaust_memory() {
        // Time-based retention alone is still unbounded against a sustained
        // stream of DISTINCT poison messages arriving faster than the window.
        // The cap is what makes the bound hard.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        for i in 0..(MAX_POISON_ENTRIES + 500) {
            let key = format!("m-{i}");
            t.strike(&key, t0);
            t.mark_terminal_as_of(&key, t0);
        }
        assert!(
            t.tracked() <= MAX_POISON_ENTRIES,
            "terminal strikes must be hard-capped; saw {}",
            t.tracked()
        );
        assert_eq!(
            t.strikes("m-0"),
            0,
            "the cap evicts oldest-first, so the earliest entry is gone"
        );
        assert_eq!(
            t.strikes(&format!("m-{}", MAX_POISON_ENTRIES + 499)),
            1,
            "and the newest is the one retained"
        );
    }

    /// The honest bound on a fixed-size in-process strike counter.
    ///
    /// While the active poison working set fits inside the cap, eviction costs
    /// a message one extra retry lap. Past it the cost is correctness: with
    /// more keys than the cap rejected round-robin, every key is evicted before
    /// its next delivery, so every attempt is strike 1 and no message ever
    /// reaches a `poison_threshold` above 1 — the harvest sink never fires.
    ///
    /// This is inherent (tracking N concurrent streaks costs O(N)), so the test
    /// pins the limitation rather than pretending it away — and pins that it is
    /// *reported* rather than silent, which is what makes it survivable.
    #[test]
    fn a_working_set_larger_than_the_cap_cannot_accumulate_strikes() {
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let keys: Vec<String> = (0..=MAX_POISON_ENTRIES).map(|i| format!("m-{i}")).collect();

        let mut reached_threshold = 0usize;
        for _lap in 0..3 {
            for key in &keys {
                if t.strike(key, t0) >= 3 {
                    reached_threshold += 1;
                }
            }
        }

        assert_eq!(
            reached_threshold, 0,
            "documented limitation: a working set larger than the cap never \
             accumulates past strike 1, so a threshold above 1 is unreachable",
        );
        assert!(
            t.tracked() <= MAX_POISON_ENTRIES,
            "the bound itself must still hold; saw {}",
            t.tracked(),
        );
        assert!(
            t.strike_progress_discarded() > 0,
            "the degradation must be reported, not silent -- an operator whose \
             poison working set exceeds the cap gets no other signal that \
             strike-based quarantine has stopped working",
        );
    }

    /// The contrast that stops the test above from passing vacuously: inside
    /// the cap, strikes accumulate exactly as documented and nothing is
    /// discarded.
    #[test]
    fn a_working_set_inside_the_cap_accumulates_strikes_normally() {
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let keys = ["a", "b", "c"];

        let mut reached_threshold = 0usize;
        for _lap in 0..3 {
            for key in keys {
                if t.strike(key, t0) >= 3 {
                    reached_threshold += 1;
                }
            }
        }

        assert_eq!(
            reached_threshold, 3,
            "every key must reach the threshold on its third rejection",
        );
        assert_eq!(
            t.strike_progress_discarded(),
            0,
            "nothing is discarded while the working set fits",
        );
    }

    #[test]
    fn the_expiry_queue_is_bounded_even_when_every_key_is_cleared() {
        // The cap was gated on the MAP, but `clear` retires a key without
        // retiring its queued expiry records. A sustained stream of DISTINCT
        // mapping-rejected messages therefore holds `tracked()` near zero --
        // so the cap never fires -- while the queue grows one owned `String`
        // per strike until the retention window (an hour by default) finally
        // drains it. At a few thousand rejections a second that is millions of
        // retained keys, which is a memory-exhaustion path, not a slow leak.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let strikes_before_terminal = 3;
        for i in 0..(MAX_POISON_ENTRIES + 500) {
            let key = format!("m-{i}");
            for _ in 0..strikes_before_terminal {
                t.strike(&key, t0);
            }
            // Harvest owns this terminal (a `HarvestSink` dead-letter), so the
            // live entry goes -- and every queued record for it stays.
            t.clear(&key);
        }
        assert_eq!(t.tracked(), 0, "every key reached a harvest-owned terminal");
        assert!(
            t.queued() <= MAX_POISON_ENTRIES,
            "the expiry queue must be bounded on its own footing; saw {}",
            t.queued()
        );
    }

    #[test]
    fn a_cleared_key_is_not_resurrected_by_a_stale_expiry_entry() {
        // `clear` (any non-broker-native terminal) removes the strike while a
        // queued expiry entry may still name that key. Popping it later must
        // be a no-op rather than disturbing a fresh count for the same key.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        // Above `ACTIVE_STRIKE_RETENTION_FLOOR`, so the pass genuinely POPS the
        // stale entries and exercises the staleness guard, rather than stopping
        // at a front entry the floor is still holding.
        let retention = ACTIVE_STRIKE_RETENTION_FLOOR * 2;

        t.strike("k", t0);
        t.mark_terminal_as_of("k", t0);
        t.clear("k");
        // The same coordinate comes back as a genuinely new strike run.
        t.strike("k", t0 + std::time::Duration::from_secs(30));

        t.expire_stale_as_of(
            t0 + retention + std::time::Duration::from_secs(1),
            retention,
        );
        assert_eq!(
            t.strikes("k"),
            1,
            "the stale expiry entry must not drop the fresh, non-terminal count"
        );
    }

    #[test]
    fn non_terminal_strikes_are_bounded_too() {
        // A mapping rejection BELOW a nonzero threshold resolves to `Retry`,
        // which deliberately never clears the key -- and only a terminal mark
        // used to queue one for expiry. So every FIRST attempt of a large
        // rejected backlog was retained with no cap and no expiry at all,
        // which is the half the threshold-0 gate did not cover.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        for i in 0..(MAX_POISON_ENTRIES + 500) {
            t.strike(&format!("m-{i}"), t0);
        }
        assert!(
            t.tracked() <= MAX_POISON_ENTRIES,
            "non-terminal strikes must be capped too; saw {}",
            t.tracked()
        );
    }

    #[test]
    fn an_active_strike_survives_a_sweep_shorter_than_the_broker_retry_delay() {
        // An ACTIVE strike streak is only ever advanced by a REDELIVERY, so the
        // interval it has to survive is the broker's, not the operator's. SQS
        // hides a nacked message for its visibility timeout -- legally up to 12
        // hours -- and the default `poison_retention` is one. Sweeping the two
        // on the same clock therefore retires every strike before the message
        // can come back, so every delivery is strike 1, no message ever reaches
        // `poison_threshold`, and the harvest dead-letter sink never fires.
        //
        // A TERMINAL entry has the opposite need: nothing will ever clear it,
        // so it retires on exactly the window the operator configured.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let retention = std::time::Duration::from_secs(60 * 60);
        let two_hours = std::time::Duration::from_secs(2 * 60 * 60);

        // Terminal first: the queue is FIFO and breaks at its front, so a young
        // active entry ahead of this one would hold it back (see
        // `a_young_active_strike_holds_back_the_terminal_entries_behind_it`).
        t.mark_terminal_as_of("handed-to-broker", t0);
        t.strike("slow-redelivery", t0);

        t.expire_stale_as_of(t0 + two_hours, retention);

        assert_eq!(
            t.strikes("slow-redelivery"),
            1,
            "an active strike must survive until the broker could plausibly have redelivered \
             it; expiring it restarts the countdown and the threshold is never reached"
        );
        assert_eq!(
            t.tracked(),
            1,
            "a terminal entry still retires on the operator's own retention clock"
        );
    }

    #[test]
    fn an_active_strike_survives_the_maximum_visibility_timeout_with_slack() {
        // A floor equal to the broker's maximum invisibility is not enough, for
        // two compounding reasons. `run_once` sweeps BEFORE it polls, so at
        // exactly the maximum the strike is retired in the very pass that would
        // then have received the redelivery. And the real redelivery is the
        // visibility timeout PLUS the poll interval and whatever scheduling and
        // queue delay the broker adds, so it always lands strictly after it.
        //
        // The asymmetry decides how much slack: expiring LATE costs bounded
        // memory (`MAX_POISON_ENTRIES`), expiring EARLY silently disables
        // strike-based quarantine altogether.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let retention = std::time::Duration::from_secs(60 * 60);
        let max_invisibility = std::time::Duration::from_secs(12 * 60 * 60);

        t.strike("k", t0);

        t.expire_stale_as_of(t0 + max_invisibility, retention);
        assert_eq!(
            t.strikes("k"),
            1,
            "swept in the very pass that would then have received the redelivery"
        );

        t.expire_stale_as_of(
            t0 + max_invisibility + std::time::Duration::from_secs(60 * 60),
            retention,
        );
        assert_eq!(
            t.strikes("k"),
            1,
            "a redelivery an hour past the maximum must still find its streak"
        );
    }

    #[test]
    fn a_young_active_strike_holds_back_the_terminal_entries_behind_it() {
        // The cost of the active floor, pinned so it stays deliberate: expiry
        // is a FIFO queue swept from the front, so a not-yet-due active entry
        // stops the pass before the due terminal entries behind it. That is
        // memory only -- `MAX_POISON_ENTRIES` is the hard bound, and its
        // eviction is kind-blind precisely so this cannot grow past it.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let retention = std::time::Duration::from_secs(60 * 60);
        let two_hours = std::time::Duration::from_secs(2 * 60 * 60);

        t.strike("slow-redelivery", t0);
        t.mark_terminal_as_of("handed-to-broker", t0);

        t.expire_stale_as_of(t0 + two_hours, retention);

        assert_eq!(
            t.tracked(),
            2,
            "the terminal entry is retired late because the active entry ahead of it is not \
             yet due; the hard cap, not the clock, is what bounds this"
        );
    }

    #[test]
    fn a_stale_non_terminal_strike_run_ages_out_after_the_broker_retry_floor() {
        // A strike streak is CONSECUTIVE by definition, so a key nothing has
        // rejected in a whole window is not mid-streak -- it is a leak, and
        // retiring it costs that message one extra lap if it returns. The
        // window is just the broker's rather than the operator's, because a
        // redelivery is the only thing that can advance an active streak.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let retention = std::time::Duration::from_secs(60);
        let floor = ACTIVE_STRIKE_RETENTION_FLOOR;

        t.strike("k", t0);
        // Well past the operator's own retention -- the clock this used to be
        // swept on -- and nowhere near the interval a redelivery can take.
        t.expire_stale_as_of(
            t0 + retention + std::time::Duration::from_secs(1),
            retention,
        );
        assert_eq!(
            t.strikes("k"),
            1,
            "still inside the window a redelivery could arrive in"
        );

        t.expire_stale_as_of(t0 + floor + std::time::Duration::from_secs(1), retention);
        assert_eq!(
            t.tracked(),
            0,
            "a non-terminal strike must not leak forever"
        );
    }

    #[test]
    fn a_longer_configured_retention_still_wins_for_active_strikes() {
        // The floor is a floor, not a cap: an operator whose broker redelivers
        // even more slowly than SQS's maximum can still widen the window, and
        // widening it must apply to BOTH kinds.
        let mut t = PoisonTracker::new();
        let t0 = std::time::Instant::now();
        let retention = ACTIVE_STRIKE_RETENTION_FLOOR * 2;

        t.strike("k", t0);
        t.expire_stale_as_of(
            t0 + ACTIVE_STRIKE_RETENTION_FLOOR + std::time::Duration::from_secs(1),
            retention,
        );
        assert_eq!(
            t.strikes("k"),
            1,
            "the configured retention exceeds the floor, so the floor must not shorten it"
        );

        t.expire_stale_as_of(
            t0 + retention + std::time::Duration::from_secs(1),
            retention,
        );
        assert_eq!(
            t.tracked(),
            0,
            "and it still ages out at the configured window"
        );
    }

    // ---- OffsetTracker (Kafka contiguous-prefix commit) ----

    #[test]
    fn a_reassignment_gap_is_crossed_without_walking_it() {
        // Nothing evicts a partition's mark when a rebalance moves it away --
        // `forget` has no production caller, only the whole-client
        // `forget_all` on a stall rebuild. So a replica that reacquires a
        // partition another replica advanced meanwhile still holds the old
        // `committed`, and the new offset is HIGHER, so `observe` does not
        // treat it as a reposition.
        //
        // The prefix walk then has to cross the gap. Every offset in it is a
        // legitimate hole by the walk's own test -- never delivered, below the
        // ceiling -- so stepping one at a time is correct and unbounded: a busy
        // partition can advance by billions. It runs under the offsets mutex,
        // which `ack` holds across commit submission, so it does not merely
        // burn a Tokio worker, it stalls every other settlement on the runtime.
        let mut t = OffsetTracker::new();
        t.observe(0, 100);
        assert_eq!(t.complete(0, 100), Some(100));

        // A billion offsets elapsed under another replica.
        let far = 100 + 1_000_000_000;
        t.observe(0, far);

        let started = std::time::Instant::now();
        let advanced = t.complete(0, far);
        let elapsed = started.elapsed();

        assert_eq!(
            advanced,
            Some(far),
            "the mark still lands on the offset that actually settled"
        );
        // Orders of magnitude of headroom: crossing the gap without walking it
        // is microseconds, walking it is minutes. This separates a hang from a
        // slow test, not two timings.
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "crossing a reassignment gap must not scan it: took {elapsed:?}"
        );
    }

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
