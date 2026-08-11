//! The broker-agnostic consumer runtime (issue #944).
//!
//! One [`ConnectorRuntime`] drives one [`EventSource`] against one
//! [`SourceBinding`]:
//!
//! ```text
//!   receive ──▶ map ──▶ dispatch ──▶ decide ──▶ ack | abandon | dead-letter
//!                                      │
//!                              (pure, unit-tested:
//!                               connector::disposition)
//! ```
//!
//! # Ack ordering is structural
//!
//! [`EventSource::ack`] is called from exactly one place —
//! [`ConnectorRuntime::settle`]'s `Ack` arm — and only ever for a
//! [`MessageDisposition`] the pure decision function produced from an outcome
//! that is already durable in Postgres. A harvest-side failure yields
//! `Retry`, which never acks, so the broker redelivers. Killing the process
//! between the dispatch commit and the ack is therefore safe: the redelivery
//! dedupes on the derived idempotency key and resolves as an idempotent
//! replay, not a duplicate run.
//!
//! # Backpressure
//!
//! Dispatch concurrency is bounded by a semaphore sized to the binding's
//! `max_in_flight`, so a topic backlog cannot stampede the admission path.
//! Permits are acquired **before** the next message is dispatched, so the
//! runtime naturally stops pulling faster than harvest can absorb.

use autumn_harvest::telemetry::{ConnectorOutcome, MetricsRecorder, PoisonReason};
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::binding::{IdempotencyMode, MappingError, SourceBinding, resolve_idempotency_mode};
use super::dead_letter::{ConnectorDeadLetter, DeadLetterSink, UnconfiguredDeadLetterSink};
use super::dispatch::{DispatchRequest, dispatch};
use super::disposition::{
    DeadLetterMode, DispatchOutcome, MessageDisposition, OffsetTracker, PoisonTracker,
    decide_disposition, success_outcome,
};
use super::idempotency::message_idempotency_key;
use super::message::{InboundMessage, MessageCtx, MessageHandle};
use super::source::{ConnectorError, EventSource};
use crate::api::HarvestApiState;

/// Runtime knobs shared by every binding on one source.
#[derive(Debug, Clone, Copy)]
pub struct ConnectorRuntimeConfig {
    /// Longest a `receive` call waits for the first message.
    ///
    /// This is also the SQS **long-poll** window: the adapter passes it
    /// straight through as `WaitTimeSeconds` (clamped to SQS's 20s maximum),
    /// so a sub-second value degrades to *short polling* — an empty
    /// `ReceiveMessage` returned immediately, billed, and retried as fast as
    /// the network allows. The default is deliberately ≥ 1s so an idle SQS
    /// binding costs roughly one API call per second rather than thousands.
    pub poll_timeout: std::time::Duration,
    /// Most messages pulled per `receive` call.
    pub max_batch: usize,
    /// How long to back off after a broker error before polling again.
    pub error_backoff: std::time::Duration,
    /// How long to pause after a poll that returned nothing.
    ///
    /// Belt-and-braces on top of `poll_timeout`: an adapter whose `receive`
    /// returns immediately when idle (rather than blocking for the timeout)
    /// would otherwise spin. Kafka's `recv` genuinely blocks, so this is a
    /// no-op there.
    pub idle_backoff: std::time::Duration,
    /// Shortest interval between consumer-lag samples.
    ///
    /// Lag is a per-call broker query (SQS bills a `GetQueueAttributes`;
    /// Kafka does a `fetch_watermarks` round-trip), so sampling it on *every*
    /// pass doubles the connector's idle call rate for a gauge that only
    /// needs to move on a scrape cadence.
    pub lag_sample_interval: std::time::Duration,
    /// Fail the pass when a partition is holding this many completed offsets
    /// behind a prefix head that has not settled.
    ///
    /// `None` (the default) derives the bound from the binding's
    /// `max_in_flight`; `Some(0)` disables **this heuristic**; `Some(n)` sets
    /// it explicitly. See [`effective_stall_threshold`].
    ///
    /// This is the *backstop* half of stall detection — a head blocked without
    /// having gone through the retry path (a dispatch task lost to a panic).
    /// A head the runtime knows it retried is reported immediately and is not
    /// governed by this knob, because a volume bound cannot see a retry at the
    /// tail of a quiet partition: nothing ever settles behind it.
    ///
    /// Both detect a **permanently blocked prefix**. On a positionally-ordered
    /// broker the commit mark can only advance over a contiguous completed
    /// prefix, so a message that is retried rather than settled blocks its
    /// partition. On Kafka that block is permanent by construction: `abandon`
    /// is a no-op (not committing is not a nack), so nothing hands the message
    /// back until the consumer is recreated and re-reads from the last commit.
    /// Every later message keeps settling, so without this the stall is
    /// **silent** — the partition simply stops committing while the connector
    /// otherwise looks healthy.
    ///
    /// Failing the pass is the fix because the runtime's response *is* the
    /// retry: `run` rebuilds the source, which re-reads from the last commit
    /// and redelivers the blocked message.
    ///
    /// It is on by default because a stall that nobody configured a detector
    /// for is exactly the one that goes unnoticed. Set `Some(0)` to opt out.
    pub stall_threshold: Option<usize>,
    /// How long a broker-native dead-lettered message's strike history is kept
    /// after its last delivery.
    ///
    /// [`MessageDisposition::AbandonToBrokerDeadLetter`] is terminal for
    /// harvest but not for the broker: SQS resets visibility, redelivers, and
    /// eventually moves the message to its own DLQ — **without notifying this
    /// process**. So no later delivery and no terminal path can ever clear the
    /// key, and the queue's redrive policy bounds *deliveries of one message*,
    /// not the size of the in-process tracker across a stream of distinct
    /// poison messages.
    ///
    /// The window therefore has to be long enough for the broker's own redrive
    /// to finish — roughly `visibility_timeout × (maxReceiveCount −
    /// poison_threshold)` — because expiring mid-redrive restarts the strike
    /// countdown, which is the churn keeping the strikes avoids. Expiring
    /// *late* costs only memory, so the default is deliberately generous.
    /// [`MAX_TERMINAL_POISON_ENTRIES`] is the hard backstop.
    pub poison_retention: std::time::Duration,
}

/// Never derive a bound below this, so a binding with tiny `max_in_flight`
/// does not fail its pass the moment a couple of messages settle out of order.
pub const MIN_DERIVED_STALL_THRESHOLD: usize = 32;

/// How many held offsets count as a stall, given the configured knob and the
/// binding's concurrency.
///
/// The derived bound is a multiple of `max_in_flight` because that is exactly
/// what bounds *healthy* out-of-order settlement: only that many messages are
/// ever outstanding, so a held depth well past it means the head is not
/// settling at all rather than settling late. Explicit configuration always
/// wins, and `Some(0)` disables the check.
#[must_use]
pub const fn effective_stall_threshold(configured: Option<usize>, max_in_flight: usize) -> usize {
    if let Some(explicit) = configured {
        return explicit;
    }
    // Saturating: a pathological `max_in_flight` must not wrap to a tiny bound
    // and start failing healthy passes.
    let derived = max_in_flight.saturating_mul(4);
    if derived > MIN_DERIVED_STALL_THRESHOLD {
        derived
    } else {
        MIN_DERIVED_STALL_THRESHOLD
    }
}

impl Default for ConnectorRuntimeConfig {
    fn default() -> Self {
        Self {
            // ≥ 1s so SQS long-polls rather than short-polls (see the field
            // doc). One idle receive per second, not one per round-trip.
            poll_timeout: std::time::Duration::from_secs(1),
            max_batch: 32,
            error_backoff: std::time::Duration::from_secs(1),
            idle_backoff: std::time::Duration::from_millis(200),
            lag_sample_interval: std::time::Duration::from_secs(15),
            // Derived from the binding's `max_in_flight` (see
            // `effective_stall_threshold`), so the detector is on by default.
            stall_threshold: None,
            // Generous on purpose: expiring late costs memory, expiring early
            // costs a redelivered poison message a full strike countdown.
            // An hour covers a 30s visibility timeout against SQS's
            // `maxReceiveCount` maximum of 1000 with room to spare.
            poison_retention: std::time::Duration::from_secs(60 * 60),
        }
    }
}

/// Drives one event source against one binding.
pub struct ConnectorRuntime {
    binding: Arc<SourceBinding>,
    source: Arc<dyn EventSource>,
    sink: Arc<dyn DeadLetterSink>,
    api_state: HarvestApiState,
    metrics: Arc<dyn MetricsRecorder>,
    config: ConnectorRuntimeConfig,
    idempotency_mode: IdempotencyMode,
    permits: Arc<Semaphore>,
    poison: Arc<tokio::sync::Mutex<PoisonTracker>>,
    offsets: Arc<tokio::sync::Mutex<OffsetTracker>>,
    /// When the consumer-lag gauge was last sampled, so an idle binding does
    /// not issue a billed broker query on every pass.
    last_lag_sample: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

/// What one `run_once` pass did, for tests and diagnostics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PassSummary {
    /// Messages pulled from the source.
    pub received: usize,
    /// Messages acknowledged (durable dispatch, replay, or deferral).
    pub acked: usize,
    /// Messages left unacknowledged for broker redelivery.
    pub retried: usize,
    /// Messages routed to a dead-letter destination.
    pub dead_lettered: usize,
}

impl ConnectorRuntime {
    /// Build a runtime for `binding` over `source`.
    #[must_use]
    pub fn new(
        binding: Arc<SourceBinding>,
        source: Arc<dyn EventSource>,
        api_state: HarvestApiState,
        metrics: Arc<dyn MetricsRecorder>,
        idempotency_mode: IdempotencyMode,
    ) -> Self {
        // Fails rather than silently succeeding: a binding defaults to
        // `HarvestSink`, so a no-op default would acknowledge a poison message
        // with no record anywhere. See `UnconfiguredDeadLetterSink`.
        let sink: Arc<dyn DeadLetterSink> = Arc::new(UnconfiguredDeadLetterSink);
        let permits = Arc::new(Semaphore::new(binding.max_in_flight.max(1)));
        Self {
            binding,
            source,
            sink,
            api_state,
            metrics,
            config: ConnectorRuntimeConfig::default(),
            idempotency_mode,
            permits,
            poison: Arc::new(tokio::sync::Mutex::new(PoisonTracker::new())),
            offsets: Arc::new(tokio::sync::Mutex::new(OffsetTracker::new())),
            last_lag_sample: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Resolve the dedupe mode from the binding and the target workflow, then
    /// build the runtime.
    #[must_use]
    pub fn for_binding(
        binding: Arc<SourceBinding>,
        source: Arc<dyn EventSource>,
        api_state: HarvestApiState,
        metrics: Arc<dyn MetricsRecorder>,
        workflow_info: Option<&autumn_harvest::info::WorkflowInfo>,
    ) -> Self {
        let mode =
            resolve_idempotency_mode(binding.target, binding.idempotency_mode, workflow_info);
        Self::new(binding, source, api_state, metrics, mode)
    }

    /// Use `sink` for harvest-side dead letters.
    #[must_use]
    pub fn with_dead_letter_sink(mut self, sink: Arc<dyn DeadLetterSink>) -> Self {
        self.sink = sink;
        self
    }

    /// Override polling/batching knobs.
    #[must_use]
    pub const fn with_config(mut self, config: ConnectorRuntimeConfig) -> Self {
        self.config = config;
        self
    }

    /// The resolved dedupe mode, for assertions and diagnostics.
    #[must_use]
    pub const fn idempotency_mode(&self) -> IdempotencyMode {
        self.idempotency_mode
    }

    /// Consume until `cancel` fires or the source closes.
    pub async fn run(&self, cancel: CancellationToken) {
        let source_name = self.binding.name;
        tracing::info!(
            source = source_name,
            stream = %self.binding.stream,
            workflow = self.binding.target.workflow(),
            max_in_flight = self.binding.max_in_flight,
            "harvest connector started"
        );

        loop {
            if cancel.is_cancelled() {
                break;
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                result = self.run_once() => match result {
                    Ok(summary) if summary.received == 0 => {
                        // An adapter whose `receive` returns immediately when
                        // idle would otherwise spin this loop (and, for a
                        // billed API like SQS, the operator's invoice) as fast
                        // as the network allows.
                        tokio::select! {
                            () = cancel.cancelled() => break,
                            () = tokio::time::sleep(self.config.idle_backoff) => {}
                        }
                    }
                    Ok(_) => {}
                    Err(ConnectorError::Closed) => {
                        tracing::info!(source = source_name, "connector source closed");
                        break;
                    }
                    // A wedged consumer is not a transient error: re-polling
                    // the same one accomplishes nothing, which is why this
                    // cannot fall into the backoff arm below. Rebuild it so
                    // the blocked message is actually redelivered; if the
                    // source cannot rebuild itself there is no in-process
                    // recovery, so stop rather than spin forever pretending to
                    // retry.
                    Err(ConnectorError::Stalled { partition, .. }) => {
                        if !self.recover_from_stall(partition).await {
                            tracing::error!(
                                source = source_name,
                                partition,
                                "connector stalled and its source cannot rebuild itself; stopping \
                                 this binding. Restart the process, or supply a source that \
                                 implements `EventSource::recover`"
                            );
                            break;
                        }
                        // Rebuilding a Kafka consumer is not a free local
                        // operation: it triggers a GROUP rebalance, revoking
                        // and reassigning partitions across every consumer in
                        // the group. The cause of a stall is usually a
                        // downstream outage (Postgres, an admission gate), so
                        // the redelivered head fails again immediately —
                        // without this pause the loop would rebuild as fast as
                        // the group can rejoin, amplifying one binding's
                        // outage into a rebalance storm across unrelated
                        // partitions. Same knob as the transient-error arm
                        // below, for the same reason.
                        tokio::select! {
                            () = cancel.cancelled() => break,
                            () = tokio::time::sleep(self.config.error_backoff) => {}
                        }
                    }
                    Err(e) => {
                        tracing::warn!(source = source_name, error = %e, "connector poll failed");
                        tokio::select! {
                            () = cancel.cancelled() => break,
                            () = tokio::time::sleep(self.config.error_backoff) => {}
                        }
                    }
                },
            }
        }

        // Drain in-flight dispatches so a shutdown never abandons a message
        // whose dispatch already committed but whose ack has not run yet.
        let permits = u32::try_from(self.binding.max_in_flight.max(1)).unwrap_or(u32::MAX);
        let _ = self.permits.acquire_many(permits).await;
        tracing::info!(source = source_name, "harvest connector stopped");
    }

    /// One receive → dispatch → settle pass.
    ///
    /// # Errors
    ///
    /// Propagates the source's own errors; [`ConnectorError::Closed`] means
    /// the source will yield nothing further.
    pub async fn run_once(&self) -> Result<PassSummary, ConnectorError> {
        // Checked BEFORE receiving, for two reasons. A stalled partition often
        // goes quiet (its head is blocked and the producer moves on), so a
        // check that only ran when messages arrived would never report the
        // stalls that matter most. And pulling a batch we are about to drop on
        // the stall error is pure churn — on a positional broker it also
        // advances the consumer past messages that were never dispatched.
        self.check_commit_stall().await?;

        // Retire broker-native terminal strikes whose redrive window has
        // elapsed. Here rather than only at the mark, so a binding that
        // dead-letters a burst and then goes quiet still releases them: `run`
        // calls this every pass regardless of whether messages arrive. O(number
        // expired), so a pass with nothing to retire costs one front peek.
        self.poison
            .lock()
            .await
            .expire_terminal_as_of(std::time::Instant::now(), self.config.poison_retention);

        let batch = self
            .source
            .receive(
                effective_max_batch(self.config.max_batch),
                self.config.poll_timeout,
            )
            .await?;

        // Throttled: `lag()` is a billed broker round-trip, and the gauge only
        // needs to move on a scrape cadence.
        if self.lag_sample_is_due()
            && let Some(lag) = self.source.lag().await
        {
            self.metrics.record_connector_lag(self.binding.name, lag);
        }

        if batch.is_empty() {
            return Ok(PassSummary::default());
        }

        let mut summary = PassSummary {
            received: batch.len(),
            ..PassSummary::default()
        };

        let mut handles = Vec::with_capacity(batch.len());
        for message in batch {
            self.metrics.record_connector_received(self.binding.name);
            // Kept alongside the join handle: a panicked task never reaches
            // `settle`, so the join arm below has to do the recovery marking
            // itself — and `message` is moved into the spawned task.
            let positional = match (message.handle.partition, message.handle.position) {
                (Some(partition), Some(position)) => {
                    self.offsets.lock().await.observe(partition, position);
                    Some((partition, position))
                }
                _ => None,
            };

            // Backpressure: block here until a slot frees, so the runtime
            // cannot outrun harvest's admission path.
            let permit = Arc::clone(&self.permits)
                .acquire_owned()
                .await
                .map_err(|e| ConnectorError::Broker(format!("connector semaphore closed: {e}")))?;

            let this = self.clone_handles();
            handles.push((
                positional,
                tokio::spawn(async move {
                    let disposition = this.process(message).await;
                    drop(permit);
                    disposition
                }),
            ));
        }

        for (positional, handle) in handles {
            match handle.await {
                Ok(MessageDisposition::Ack) => summary.acked += 1,
                Ok(MessageDisposition::Retry) => summary.retried += 1,
                Ok(
                    MessageDisposition::DeadLetter(_)
                    | MessageDisposition::AbandonToBrokerDeadLetter(_),
                ) => summary.dead_lettered += 1,
                Err(e) => {
                    // A panicking dispatch task must not ack: leaving the
                    // message unacknowledged is the safe direction.
                    //
                    // A panic in the *mapping function* — the message-
                    // attributable, deterministic case — is already contained
                    // in `map_and_dispatch` and quarantined as `Malformed`, so
                    // it never reaches here. What is left is an engine-side
                    // panic, which is not a property of this message; retrying
                    // is correct. Known limit: a persistently panicking engine
                    // path redelivers forever rather than dead-lettering.
                    tracing::error!(
                        source = self.binding.name,
                        error = %e,
                        "connector dispatch task failed; message left for redelivery"
                    );
                    // The task died before `settle`, so the normal `Retry`
                    // path's recovery marking never ran. On a source whose
                    // `abandon` cannot force a redelivery the local position
                    // has already advanced past this record, so nothing hands
                    // it back: mark the head so recovery fires on it directly
                    // rather than waiting for later offsets to pile up behind
                    // it — at the tail of a quiet partition, none ever will.
                    //
                    // A redelivering broker (SQS's visibility timeout) is not
                    // wedged and must not trigger a rebuild, or every
                    // engine-side panic would recycle the consumer.
                    if let (false, Some((partition, position))) =
                        (self.source.abandon_redelivers(), positional)
                    {
                        self.offsets.lock().await.retried(partition, position);
                    }
                    summary.retried += 1;
                }
            }
        }

        // Re-checked after settling this batch so a stall that forms *during*
        // the pass is reported immediately rather than a poll later.
        self.check_commit_stall().await?;

        Ok(summary)
    }

    /// Fail the pass when a partition's commit prefix is permanently blocked.
    ///
    /// A prefix head that never settles blocks its partition's commit on a
    /// broker whose `abandon` cannot force a redelivery — Kafka, where not
    /// committing is not a nack. The tracker cannot resolve that on its own:
    /// only re-reading from the last commit hands the message back. So the
    /// pass fails with [`ConnectorError::Stalled`], and `run` rebuilds the
    /// source ([`EventSource::recover`]), which is what performs the retry.
    ///
    /// Two signals feed this (see [`OffsetTracker::stalled`]): a retried head,
    /// which is a wedge by construction and reported immediately, and a
    /// backlog of at least [`ConnectorRuntimeConfig::stall_threshold`]
    /// completed offsets as a backstop. `0` disables only the backlog
    /// heuristic; the retried head is a correctness signal and always fires.
    async fn check_commit_stall(&self) -> Result<(), ConnectorError> {
        let threshold =
            effective_stall_threshold(self.config.stall_threshold, self.binding.max_in_flight);
        let Some((partition, held)) = self.offsets.lock().await.stalled(threshold) else {
            return Ok(());
        };
        tracing::error!(
            source = self.binding.name,
            stream = %self.binding.stream,
            partition,
            held,
            threshold,
            "connector commit prefix is stalled; the head offset has not settled while later \
             offsets pile up behind it. Rebuilding the consumer to re-read from the last commit"
        );
        Err(ConnectorError::Stalled {
            partition,
            held,
            threshold,
        })
    }

    /// Perform the retry a detected stall calls for: rebuild the source's
    /// consumer and discard the wedged generation's offset state.
    ///
    /// Returns whether the source could rebuild itself. Clearing the tracker
    /// is what makes the rebuild stick — a fresh consumer re-reads from the
    /// last commit, so the redelivered offsets arrive *below* the stale
    /// in-memory mark and would be mistaken for already-settled redeliveries,
    /// leaving the prefix blocked exactly as it was.
    ///
    /// Only the offsets are forgotten, never the poison strikes: a message
    /// that has been rejected N times is still on strike N after the rebuild,
    /// so it still reaches its threshold and dead-letters rather than
    /// restarting its count on every recovery.
    pub async fn recover_from_stall(&self, partition: i32) -> bool {
        match self.source.recover().await {
            Ok(true) => {
                self.offsets.lock().await.forget(partition);
                tracing::warn!(
                    source = self.binding.name,
                    partition,
                    "connector consumer rebuilt after a commit stall; re-reading from the last \
                     commit so the blocked message is redelivered"
                );
                true
            }
            Ok(false) => false,
            Err(e) => {
                tracing::error!(
                    source = self.binding.name,
                    partition,
                    error = %e,
                    "connector consumer rebuild failed after a commit stall"
                );
                false
            }
        }
    }

    /// Whether enough time has passed to re-sample the consumer-lag gauge.
    ///
    /// Records the sample instant as a side effect, so a caller that gets
    /// `true` must actually take the sample. Uses a plain `std::sync::Mutex`
    /// held across two trivial operations only — never across an `.await`.
    fn lag_sample_is_due(&self) -> bool {
        let now = std::time::Instant::now();
        let mut last = self
            .last_lag_sample
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let due = last.is_none_or(|t| now.duration_since(t) >= self.config.lag_sample_interval);
        if due {
            *last = Some(now);
        }
        due
    }

    fn clone_handles(&self) -> Self {
        Self {
            binding: Arc::clone(&self.binding),
            source: Arc::clone(&self.source),
            sink: Arc::clone(&self.sink),
            api_state: self.api_state.clone(),
            metrics: Arc::clone(&self.metrics),
            config: self.config,
            idempotency_mode: self.idempotency_mode,
            permits: Arc::clone(&self.permits),
            poison: Arc::clone(&self.poison),
            offsets: Arc::clone(&self.offsets),
            last_lag_sample: Arc::clone(&self.last_lag_sample),
        }
    }

    /// Map, dispatch and settle one message.
    async fn process(&self, message: InboundMessage) -> MessageDisposition {
        let coordinates = message.coordinates.render();
        let key = message_idempotency_key(
            self.binding.name,
            self.binding.target.signal_name(),
            &coordinates,
        );

        let outcome = self.map_and_dispatch(&message, &key, &coordinates).await;

        let strikes = if matches!(outcome, DispatchOutcome::MappingRejected(_)) {
            self.poison.lock().await.strike(&key)
        } else {
            0
        };

        let decided = decide_disposition(
            &outcome,
            strikes,
            self.binding.poison_threshold,
            self.binding.dead_letter_mode,
        );

        // Settle first, then report: settling can *downgrade* a decision (a
        // dead-letter whose sink write failed becomes a retry), and both the
        // metrics and the strike bookkeeping must describe what actually
        // happened rather than what was intended.
        let effective = self
            .settle(
                &message,
                &coordinates,
                &key,
                &outcome,
                decided,
                // A mapping rejection has accumulated `strikes` deliveries; a
                // deterministic poison is quarantined on its first.
                strikes.max(1),
            )
            .await;

        // Once a message reaches a terminal disposition its strike history is
        // no longer needed; dropping it keeps the tracker bounded. A message
        // left for redelivery keeps its strikes so the threshold still bites.
        //
        // `AbandonToBrokerDeadLetter` is terminal for *harvest* but not for
        // the broker: it resets visibility so the message comes back and the
        // broker counts the receive toward its own `maxReceiveCount`. Whenever
        // that ceiling is above this binding's `poison_threshold` — the normal
        // configuration, since the binding wants to nack well before the queue
        // gives up — the message returns one or more times before the broker
        // quarantines it. Clearing here would restart the strike countdown, so
        // each redelivery would crawl back through ordinary visibility-timeout
        // retries and emit a fresh `dead_lettered` sample per lap. Keeping the
        // strikes makes every later delivery re-nack on sight and lets the
        // broker's own count be what ends it.
        //
        // Such an entry outlives harvest's view of the message, and *nothing
        // downstream can ever clear it*: SQS moves the message to its DLQ
        // without notifying this process, so there is no later delivery and no
        // terminal path to hook. The queue's redrive policy bounds deliveries
        // of one message, not this map across a stream of distinct poison
        // messages — so the entry carries a retention deadline instead, run
        // from its LAST delivery (see `ConnectorRuntimeConfig::poison_retention`).
        if matches!(effective, MessageDisposition::AbandonToBrokerDeadLetter(_)) {
            self.poison
                .lock()
                .await
                .mark_terminal_as_of(&key, std::time::Instant::now());
        } else if matches!(effective, MessageDisposition::Retry) {
            // On a source whose `abandon` cannot force a redelivery, this
            // offset is now a permanently blocked prefix head: the local
            // position has already moved past it, so nothing hands it back.
            // Record it so recovery fires on the head itself rather than
            // waiting for later offsets to pile up behind it — at the tail of
            // a quiet partition, none ever will.
            //
            // A broker that does redeliver (SQS's visibility timeout) is not
            // wedged and must not trigger a rebuild, or every transient
            // dispatch failure would recycle the consumer.
            if let (false, Some(partition), Some(position)) = (
                self.source.abandon_redelivers(),
                message.handle.partition,
                message.handle.position,
            ) {
                self.offsets.lock().await.retried(partition, position);
            }
        } else {
            self.poison.lock().await.clear(&key);
        }

        self.record_metrics(&outcome, &effective);
        effective
    }

    async fn map_and_dispatch(
        &self,
        message: &InboundMessage,
        key: &str,
        coordinates: &str,
    ) -> DispatchOutcome {
        let ctx = MessageCtx::new(self.binding.name, message);
        // A mapping function is embedder code operating on an untrusted broker
        // payload, so a stray `unwrap()` on a malformed message is a realistic
        // failure. Contain it exactly like the engine contains a `#[workflow]`
        // /`#[activity]` panic (issue #782): a panic here is deterministic in
        // the message (the same bytes panic on every redelivery), so treating
        // it as `Malformed` quarantines the one bad message rather than letting
        // it wedge the partition forever. `AssertUnwindSafe` is sound: the
        // mapper is a plain `fn` and its (discarded) output is the only state
        // crossing the boundary.
        let mapping =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.binding.mapper)(&ctx)));
        let mapped = match mapping {
            Ok(Ok(m)) => m,
            Ok(Err(MappingError::Deserialize(m))) => return DispatchOutcome::Malformed(m),
            Ok(Err(MappingError::Rejected(m))) => return DispatchOutcome::MappingRejected(m),
            Err(payload) => {
                let detail = autumn_harvest::error::panic_message(payload);
                tracing::error!(
                    source = self.binding.name,
                    coordinates,
                    detail = %detail,
                    "connector mapping function panicked; quarantining the message"
                );
                return DispatchOutcome::Malformed(format!("mapping function panicked: {detail}"));
            }
        };

        dispatch(
            &self.api_state,
            DispatchRequest {
                target: self.binding.target,
                workflow_id: mapped.workflow_id,
                payload: mapped.payload,
                queue: self.binding.queue,
                idempotency_key: key,
                idempotency_mode: self.idempotency_mode,
                coordinates,
            },
        )
        .await
    }

    fn record_metrics(&self, outcome: &DispatchOutcome, disposition: &MessageDisposition) {
        let source = self.binding.name;
        // Prefer the finer-grained success outcome (fresh / replay / deferred)
        // over the disposition's coarse "acked".
        let reported = if matches!(disposition, MessageDisposition::Ack) {
            success_outcome(outcome).unwrap_or(ConnectorOutcome::Dispatched)
        } else {
            disposition.outcome()
        };
        self.metrics.record_connector_dispatched(source, reported);
        if let Some(reason) = disposition.poison_reason() {
            self.metrics.record_connector_poisoned(source, reason);
        }
    }

    /// Apply the decided disposition to the broker, returning what actually
    /// happened.
    ///
    /// **This is the only place `ack` is called.** Keeping it in one arm of
    /// one match is what makes the ack-after-commit contract auditable.
    ///
    /// The returned disposition can differ from the decided one in exactly one
    /// case: a [`MessageDisposition::DeadLetter`] whose sink write failed is
    /// downgraded to [`MessageDisposition::Retry`], because the message was
    /// *not* recorded anywhere and must come back rather than be counted as
    /// quarantined.
    async fn settle(
        &self,
        message: &InboundMessage,
        coordinates: &str,
        key: &str,
        outcome: &DispatchOutcome,
        disposition: MessageDisposition,
        attempts: u32,
    ) -> MessageDisposition {
        match disposition {
            MessageDisposition::Ack => {
                self.ack(&message.handle).await;
                MessageDisposition::Ack
            }
            MessageDisposition::Retry => {
                tracing::warn!(
                    source = self.binding.name,
                    coordinates,
                    outcome = ?outcome,
                    "connector dispatch not durable; leaving message for redelivery"
                );
                self.abandon(&message.handle).await;
                MessageDisposition::Retry
            }
            MessageDisposition::DeadLetter(reason) => {
                let entry =
                    self.dead_letter_entry(message, coordinates, key, outcome, reason, attempts);
                match self.sink.write(&entry).await {
                    Ok(()) => {
                        tracing::warn!(
                            source = self.binding.name,
                            coordinates,
                            reason = reason.as_str(),
                            "connector message dead-lettered"
                        );
                        // Ack only after the dead-letter record is durable, so
                        // a sink failure can never silently drop the message.
                        self.ack(&message.handle).await;
                        MessageDisposition::DeadLetter(reason)
                    }
                    Err(e) => {
                        tracing::error!(
                            source = self.binding.name,
                            coordinates,
                            error = %e,
                            "dead-letter write failed; leaving message for redelivery"
                        );
                        self.abandon(&message.handle).await;
                        MessageDisposition::Retry
                    }
                }
            }
            MessageDisposition::AbandonToBrokerDeadLetter(reason) => {
                tracing::warn!(
                    source = self.binding.name,
                    coordinates,
                    reason = reason.as_str(),
                    "connector message abandoned to broker-native dead-lettering"
                );
                // The poison path wants the FASTEST redelivery (each one
                // advances the broker's receive count toward its redrive
                // threshold), unlike `abandon`, which is the gentler
                // transient-retry return.
                self.nack_for_dead_letter(&message.handle).await;
                MessageDisposition::AbandonToBrokerDeadLetter(reason)
            }
        }
    }

    fn dead_letter_entry(
        &self,
        message: &InboundMessage,
        coordinates: &str,
        key: &str,
        outcome: &DispatchOutcome,
        reason: PoisonReason,
        attempts: u32,
    ) -> ConnectorDeadLetter {
        let detail = match outcome {
            DispatchOutcome::Malformed(m) | DispatchOutcome::MappingRejected(m) => m.clone(),
            DispatchOutcome::TargetRejected { status, detail } => format!("{status}: {detail}"),
            other => format!("{other:?}"),
        };
        ConnectorDeadLetter {
            binding: self.binding.name.to_string(),
            stream: self.binding.stream.clone(),
            coordinates: coordinates.to_string(),
            idempotency_key: key.to_string(),
            workflow_name: self.binding.target.workflow().to_string(),
            reason,
            detail,
            // What actually happened, not what was configured: the strike
            // count at the moment of quarantine. A deterministic poison
            // (malformed payload, permanent rejection) is dead-lettered on
            // its FIRST delivery, so it records 1 — recording the configured
            // threshold there would tell an operator it had been retried
            // twice when it never was.
            attempts: i32::try_from(attempts.max(1)).unwrap_or(i32::MAX),
            payload: message.payload.clone(),
            failed_at: Utc::now(),
        }
    }

    async fn ack(&self, handle: &MessageHandle) {
        // For a positionally-ordered broker (Kafka), only advance the
        // high-water mark to the contiguous completed prefix — committing past
        // an in-flight lower offset would silently skip it on a crash.
        //
        // The offset actually committed is the tracker's advanced mark, NOT
        // this handle's own position: when offsets 2 then 1 settle in that
        // order, offset 2 is held, and it is 1's completion that advances the
        // prefix to 2. Committing 1's handle there would leave 2 uncommitted
        // and re-read on a crash. Kafka's `ack` addresses the commit purely by
        // `(partition, position)` (the token is unused), so a synthesized
        // handle at the mark is the correct thing to hand it.
        let mut owned;
        let mut handle = handle;
        if let (Some(partition), Some(position)) = (handle.partition, handle.position) {
            let Some(advanced) = self.offsets.lock().await.complete(partition, position) else {
                // Earlier offsets are still in flight; this one will be
                // committed as part of a later contiguous advance.
                return;
            };
            if advanced != position {
                owned = handle.clone();
                owned.position = Some(advanced);
                handle = &owned;
            }
        }
        if let Err(e) = self.source.ack(handle).await {
            // A failed ack is safe: the message is redelivered and dedupes as
            // an idempotent replay. Never escalate it into a lost message.
            tracing::warn!(
                source = self.binding.name,
                error = %e,
                "connector ack failed; message will be redelivered and deduped"
            );
        }
    }

    async fn abandon(&self, handle: &MessageHandle) {
        if let Err(e) = self.source.abandon(handle).await {
            tracing::warn!(
                source = self.binding.name,
                error = %e,
                "connector abandon failed; relying on broker-native redelivery"
            );
        }
    }

    async fn nack_for_dead_letter(&self, handle: &MessageHandle) {
        if let Err(e) = self.source.nack_for_dead_letter(handle).await {
            tracing::warn!(
                source = self.binding.name,
                error = %e,
                "connector nack-for-dead-letter failed; relying on broker-native redelivery"
            );
        }
    }
}

/// Dead-letter mode helper re-exported for the plugin wiring.
#[must_use]
pub const fn dead_letter_mode_of(binding: &SourceBinding) -> DeadLetterMode {
    binding.dead_letter_mode
}

/// The batch size actually handed to a source, floored at one.
///
/// A zero batch is a silent killer rather than a loud one: a source asked for
/// zero messages returns an empty batch, the runtime reads that as an idle
/// poll, and the binding consumes nothing forever with no error to alert on.
/// Clamping here — at the runtime's single `receive` call site — covers every
/// source including an embedder's own `EventSource`, not just the two adapters
/// shipped in-tree.
#[must_use]
pub const fn effective_max_batch(configured: usize) -> usize {
    if configured == 0 { 1 } else { configured }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::binding::SourceBinding;
    use crate::connector::dead_letter::RecordingDeadLetterSink;
    use crate::connector::mock::MockSource;
    use autumn_harvest::telemetry::NoOpMetrics;
    use std::sync::Mutex;

    /// A `MetricsRecorder` that captures every connector sample, so a test can
    /// assert what was actually emitted rather than merely that emitting did
    /// not panic.
    #[derive(Debug, Default)]
    struct RecordingMetrics {
        received: Mutex<Vec<String>>,
        dispatched: Mutex<Vec<(String, ConnectorOutcome)>>,
        poisoned: Mutex<Vec<(String, PoisonReason)>>,
        lag: Mutex<Vec<(String, i64)>>,
    }

    impl RecordingMetrics {
        fn received(&self) -> Vec<String> {
            self.received.lock().unwrap().clone()
        }
        fn dispatched(&self) -> Vec<(String, ConnectorOutcome)> {
            self.dispatched.lock().unwrap().clone()
        }
        fn poisoned(&self) -> Vec<(String, PoisonReason)> {
            self.poisoned.lock().unwrap().clone()
        }
        fn lag(&self) -> Vec<(String, i64)> {
            self.lag.lock().unwrap().clone()
        }
    }

    impl MetricsRecorder for RecordingMetrics {
        fn record_connector_received(&self, source: &str) {
            self.received.lock().unwrap().push(source.to_string());
        }
        fn record_connector_dispatched(&self, source: &str, outcome: ConnectorOutcome) {
            self.dispatched
                .lock()
                .unwrap()
                .push((source.to_string(), outcome));
        }
        fn record_connector_poisoned(&self, source: &str, reason: PoisonReason) {
            self.poisoned
                .lock()
                .unwrap()
                .push((source.to_string(), reason));
        }
        fn record_connector_lag(&self, source: &str, lag: i64) {
            self.lag.lock().unwrap().push((source.to_string(), lag));
        }
    }

    fn runtime_with_metrics(
        binding: Arc<SourceBinding>,
        source: Arc<MockSource>,
        sink: Arc<RecordingDeadLetterSink>,
        metrics: Arc<RecordingMetrics>,
    ) -> ConnectorRuntime {
        ConnectorRuntime::new(
            binding,
            source,
            HarvestApiState::new(),
            metrics,
            IdempotencyMode::BrokerCoordinates,
        )
        .with_dead_letter_sink(sink)
    }

    /// A binding whose mapper always rejects (a semantic refusal, so it is
    /// strike-counted rather than dead-lettered on sight).
    fn rejecting_binding(threshold: u32) -> Arc<SourceBinding> {
        Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Rejected("no thanks".to_string())))
                .poison_threshold(threshold),
        )
    }

    /// A binding whose mapper cannot decode the payload (deterministic).
    fn malformed_binding() -> Arc<SourceBinding> {
        Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Deserialize("not json".to_string()))),
        )
    }

    /// A sink whose `write` panics, so a dispatch task dies with a `JoinError`
    /// rather than returning a disposition.
    ///
    /// This is the one *engine-side* panic seam reachable from a unit test:
    /// the mapper's own panic is contained inside `map_and_dispatch`
    /// (quarantined as `Malformed`) and never reaches the join arm, and the
    /// dispatch path itself needs a live harvest runtime to fail. The sink is
    /// injectable, so panicking there exercises the real `run_once` join arm.
    #[derive(Debug, Default)]
    struct PanickingDeadLetterSink;

    #[async_trait::async_trait]
    impl crate::connector::dead_letter::DeadLetterSink for PanickingDeadLetterSink {
        async fn write(
            &self,
            _entry: &crate::connector::dead_letter::ConnectorDeadLetter,
        ) -> Result<(), ConnectorError> {
            panic!("engine-side dead-letter sink panic");
        }
    }

    fn runtime(
        binding: Arc<SourceBinding>,
        source: Arc<MockSource>,
        sink: Arc<RecordingDeadLetterSink>,
    ) -> ConnectorRuntime {
        ConnectorRuntime::new(
            binding,
            source,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        )
        .with_dead_letter_sink(sink)
    }

    #[tokio::test]
    async fn a_panicked_dispatch_marks_its_offset_for_recovery() {
        // A panicked task never reaches `settle`, so the normal `Retry` path's
        // recovery marking does not run. On a positional broker whose
        // `abandon` cannot force a redelivery (Kafka), the local position has
        // already advanced past this record, so nothing hands it back: the
        // offset is a permanently blocked prefix head. At the tail of a quiet
        // partition nothing ever settles behind it, so the backlog heuristic
        // alone would wait forever -- the head itself must be marked.
        let source = Arc::new(MockSource::new("orders").without_redelivery());
        source.push_kafka(0, 7, b"{{{");
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            Arc::clone(&source) as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        )
        .with_dead_letter_sink(Arc::new(PanickingDeadLetterSink));

        let outcome = rt.run_once().await;

        // Marking the head makes the pass fail as `Stalled`, which is what
        // drives the consumer rebuild. `held: 0` is the load-bearing detail:
        // nothing settled behind this offset, so the backlog heuristic cannot
        // be what fired — only the marked retried head can be. That is exactly
        // the tail-of-a-quiet-partition case a volume bound would miss.
        match outcome {
            Err(ConnectorError::Stalled {
                partition, held, ..
            }) => {
                assert_eq!(partition, 0);
                assert_eq!(
                    held, 0,
                    "nothing piled up behind it; the retried head is the only possible signal"
                );
            }
            other => panic!(
                "a panicked dispatch on a non-redelivering source must stall its \
                 partition so the consumer is rebuilt, got {other:?}"
            ),
        }

        assert_eq!(
            rt.offsets.lock().await.stalled(0).map(|(p, _)| p),
            Some(0),
            "the panicked offset must be marked for recovery, not merely observed"
        );
    }

    #[tokio::test]
    async fn a_panicked_dispatch_on_a_redelivering_source_is_not_a_stall() {
        // The mirror image: a broker whose visibility timeout hands the
        // message back (SQS) is not wedged, and must not trigger a consumer
        // rebuild -- otherwise every engine-side panic would recycle it.
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 7, b"{{{");
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            Arc::clone(&source) as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        )
        .with_dead_letter_sink(Arc::new(PanickingDeadLetterSink));

        let summary = rt.run_once().await.unwrap();

        assert_eq!(summary.retried, 1);
        assert_eq!(
            rt.offsets.lock().await.stalled(0),
            None,
            "a redelivering source is not wedged by a panicked dispatch"
        );
    }

    #[tokio::test]
    async fn broker_native_dead_letter_keeps_its_strikes_across_redeliveries() {
        // `AbandonToBrokerDeadLetter` is terminal for harvest but NOT for the
        // broker: it resets visibility so the message comes back and the
        // broker counts the receive toward its own `maxReceiveCount`. When
        // that ceiling is above this binding's `poison_threshold` the message
        // returns one or more times before the broker quarantines it. Clearing
        // the strike history on the way out would restart the countdown, so
        // each redelivery would crawl through ordinary visibility-timeout
        // retries again and emit a fresh `dead_lettered` sample per lap.
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Rejected("no thanks".to_string())))
                .poison_threshold(2)
                .broker_native_dead_letter(),
        );
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(binding, Arc::clone(&source), Arc::clone(&sink));

        // Same coordinates every time: one message, three deliveries.
        source.push_kafka(0, 1, b"{}");
        let first = rt.run_once().await.unwrap();
        source.push_kafka(0, 1, b"{}");
        let second = rt.run_once().await.unwrap();
        source.push_kafka(0, 1, b"{}");
        let third = rt.run_once().await.unwrap();

        assert_eq!(first.retried, 1, "strike 1 is below the threshold");
        assert_eq!(second.dead_lettered, 1, "strike 2 reaches the threshold");
        assert_eq!(
            third.dead_lettered, 1,
            "a redelivery of an already-nacked message must re-nack immediately, \
             not restart the strike countdown"
        );
        assert_eq!(
            third.retried, 0,
            "restarting the countdown would send it back through ordinary retries"
        );
        assert!(
            sink.entries().is_empty(),
            "broker-native dead-lettering never writes to the harvest sink"
        );
    }

    #[tokio::test]
    async fn broker_native_strikes_do_not_accumulate_without_bound() {
        // The other half of the contract above. Keeping the strikes is right
        // for the redrive lifetime, but SQS moves the message to its DLQ
        // *without telling this process*: there is no later delivery and no
        // terminal path that can ever clear the key. The redrive policy bounds
        // deliveries of one message, not this map across a stream of distinct
        // poison messages -- so the retention window has to.
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Rejected("no thanks".to_string())))
                .poison_threshold(1)
                .broker_native_dead_letter(),
        );
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(binding, Arc::clone(&source), Arc::clone(&sink)).with_config(
            ConnectorRuntimeConfig {
                poison_retention: std::time::Duration::from_millis(30),
                ..ConnectorRuntimeConfig::default()
            },
        );

        // Ten distinct poison messages, each nacked to the broker's redrive.
        for offset in 0..10 {
            source.push_kafka(0, offset, b"{}");
            assert_eq!(rt.run_once().await.unwrap().dead_lettered, 1);
        }
        assert_eq!(
            rt.poison.lock().await.tracked(),
            10,
            "each is retained so a redelivery re-nacks on sight"
        );

        // Once every redrive window has elapsed, the entries retire.
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        rt.run_once().await.unwrap();
        assert_eq!(
            rt.poison.lock().await.tracked(),
            0,
            "terminal strikes must expire; nothing else can ever clear them"
        );
    }

    #[tokio::test]
    async fn malformed_message_is_dead_lettered_then_acked() {
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 1, b"{{{");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), Arc::clone(&source), Arc::clone(&sink));

        let summary = rt.run_once().await.unwrap();

        assert_eq!(summary.received, 1);
        assert_eq!(summary.dead_lettered, 1);
        assert_eq!(summary.acked, 0, "dead-lettering is not counted as an ack");

        let entries = sink.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].reason, PoisonReason::Malformed);
        assert_eq!(entries[0].coordinates, "orders:0:1");
        assert_eq!(entries[0].payload, b"{{{");
        // Acked so the poison message stops blocking the partition.
        assert_eq!(source.acked().len(), 1);
        // The recorded attempt count is what actually happened. A malformed
        // payload is quarantined on its FIRST delivery, so recording the
        // configured `poison_threshold` (3) would tell an operator it had been
        // retried twice when it never was.
        assert_eq!(entries[0].attempts, 1);
    }

    #[tokio::test]
    async fn a_repeatedly_rejected_message_records_its_real_strike_count() {
        // The other half: a mapping rejection genuinely accumulates strikes,
        // so the dead-letter row must show the threshold it actually reached.
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Rejected("no tenant".to_string())))
                .poison_threshold(3),
        );
        let rt = runtime(binding, Arc::clone(&source), Arc::clone(&sink));

        // Redeliver the SAME coordinates three times, as a broker would.
        for _ in 0..3 {
            source.push_kafka(0, 1, b"{}");
            rt.run_once().await.unwrap();
        }

        let entries = sink.entries();
        assert_eq!(entries.len(), 1, "quarantined once, on the third strike");
        assert_eq!(entries[0].reason, PoisonReason::MappingRejected);
        assert_eq!(entries[0].attempts, 3);
    }

    #[tokio::test]
    async fn a_panicking_mapping_function_is_quarantined_not_retried_forever() {
        // A mapping function is embedder code over an untrusted payload, so a
        // stray `unwrap()` is realistic. The panic is deterministic in the
        // message, so retrying it forever would wedge the partition — the
        // engine contains it exactly as it contains a `#[workflow]` panic
        // (issue #782) and quarantines the one bad message.
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 1, b"boom");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| panic!("mapper exploded")),
        );
        let rt = runtime(binding, Arc::clone(&source), Arc::clone(&sink));

        let summary = rt.run_once().await.unwrap();

        assert_eq!(summary.received, 1);
        assert_eq!(
            summary.dead_lettered, 1,
            "a panicking mapper quarantines the message"
        );
        assert_eq!(
            summary.retried, 0,
            "and never leaves it circulating for redelivery"
        );

        let entries = sink.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].reason, PoisonReason::Malformed);
        assert!(
            entries[0].detail.contains("mapper exploded"),
            "the panic message is preserved for triage, got {:?}",
            entries[0].detail
        );
        // Acked, so one panicking payload cannot block the partition.
        assert_eq!(source.acked().len(), 1);
    }

    #[tokio::test]
    async fn dead_letter_sink_failure_never_acks() {
        // AC4's teeth: a message is only acknowledged once its outcome is
        // durable *somewhere*. A failed sink write must leave it for the
        // broker to redeliver, never silently drop it.
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 1, b"{{{");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        sink.fail_writes(true);
        let rt = runtime(malformed_binding(), Arc::clone(&source), Arc::clone(&sink));

        let summary = rt.run_once().await.unwrap();

        assert!(
            source.acked().is_empty(),
            "a message whose dead-letter record failed must NOT be acked"
        );
        assert_eq!(source.abandoned().len(), 1);
        // ...and it must be *reported* as retried, not as quarantined: the
        // `harvest.connector.poisoned` counter and the pass summary describe
        // what happened, never what was intended.
        assert_eq!(summary.retried, 1);
        assert_eq!(summary.dead_lettered, 0);
    }

    #[tokio::test]
    async fn a_failed_dead_letter_write_preserves_the_strike_count() {
        // Corollary of the downgrade above: a message left for redelivery
        // keeps its strikes, so the very next redelivery quarantines rather
        // than restarting the countdown from zero.
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(rejecting_binding(2), Arc::clone(&source), Arc::clone(&sink));

        sink.fail_writes(true);
        for _ in 0..2 {
            source.push_kafka(0, 7, b"payload");
            rt.run_once().await.unwrap();
        }
        assert!(sink.entries().is_empty());
        assert_eq!(source.abandoned().len(), 2);

        // The sink recovers; the next redelivery must quarantine immediately
        // (strikes were 2 already), not need two more attempts.
        sink.fail_writes(false);
        source.push_kafka(0, 7, b"payload");
        let summary = rt.run_once().await.unwrap();
        assert_eq!(summary.dead_lettered, 1);
        assert_eq!(sink.entries().len(), 1);
        assert_eq!(source.acked().len(), 1);
    }

    #[tokio::test]
    async fn mapping_rejection_retries_until_the_threshold_then_dead_letters() {
        // AC6: N consecutive mapping rejections quarantine the message.
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(rejecting_binding(3), Arc::clone(&source), Arc::clone(&sink));

        // Redeliver the same coordinates three times.
        for _ in 0..3 {
            source.push_kafka(0, 7, b"payload");
            rt.run_once().await.unwrap();
        }

        assert_eq!(sink.entries().len(), 1, "quarantined exactly once");
        assert_eq!(sink.entries()[0].reason, PoisonReason::MappingRejected);
        // The first two attempts were abandoned for redelivery; only the
        // quarantining attempt acked.
        assert_eq!(source.abandoned().len(), 2);
        assert_eq!(source.acked().len(), 1);
    }

    #[tokio::test]
    async fn zero_threshold_retries_a_mapping_rejection_forever() {
        // The documented opt-out (mirrors `poison_pill_threshold = 0`).
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(rejecting_binding(0), Arc::clone(&source), Arc::clone(&sink));

        for _ in 0..5 {
            source.push_kafka(0, 7, b"payload");
            rt.run_once().await.unwrap();
        }

        assert!(sink.entries().is_empty());
        assert!(source.acked().is_empty());
        assert_eq!(source.abandoned().len(), 5);
    }

    #[tokio::test]
    async fn broker_native_mode_abandons_a_poison_message_instead_of_acking() {
        // SQS redrive owns the dead-letter destination, so the runtime must
        // NOT delete the message — the queue's own maxReceiveCount moves it.
        let source = Arc::new(MockSource::new("orders"));
        source.push_opaque("m-1", b"{{{");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Deserialize("not json".to_string())))
                .broker_native_dead_letter(),
        );
        let rt = runtime(binding, Arc::clone(&source), Arc::clone(&sink));

        let summary = rt.run_once().await.unwrap();

        assert_eq!(summary.dead_lettered, 1);
        assert!(sink.entries().is_empty(), "the broker owns the DLQ here");
        assert!(source.acked().is_empty());
        // The POISON path, not the transient-retry path: the two are opposite
        // intents (drive the receive count toward the redrive threshold vs.
        // return gently with backoff) and SQS implements them differently.
        assert_eq!(source.nacked_for_dead_letter().len(), 1);
        assert!(
            source.abandoned().is_empty(),
            "a poison quarantine must not go through the gentle retry return"
        );
    }

    #[tokio::test]
    async fn a_transient_failure_uses_the_gentle_retry_return_not_the_poison_nack() {
        // The mirror image. A transient harvest failure must NOT rush the
        // message back: on SQS that would both hammer an already-struggling
        // harvest and burn `ApproximateReceiveCount` toward a redrive policy's
        // `maxReceiveCount`, eventually dead-lettering a perfectly good
        // message for a purely transient reason.
        let source = Arc::new(MockSource::new("orders"));
        source.push_opaque("m-1", b"{{{");
        // A sink that always fails downgrades the dead-letter to a Retry.
        let sink = Arc::new(RecordingDeadLetterSink::new());
        sink.fail_writes(true);
        let rt = runtime(malformed_binding(), Arc::clone(&source), Arc::clone(&sink));

        let summary = rt.run_once().await.unwrap();

        assert_eq!(summary.retried, 1);
        assert!(source.acked().is_empty());
        assert_eq!(source.abandoned().len(), 1);
        assert!(
            source.nacked_for_dead_letter().is_empty(),
            "a transient retry must not use the poison nack"
        );
    }

    #[tokio::test]
    async fn a_retry_at_the_tail_of_a_quiet_partition_still_stalls_it() {
        // The volume signal cannot see this: one message at the tail of an
        // otherwise idle partition is retried, so nothing ever settles behind
        // it and `held` stays 0 under any threshold. On Kafka that message is
        // simply lost — `abandon` is a no-op and the consumer position has
        // already moved past it — so the wedge must be reported on the head.
        let source = Arc::new(MockSource::new("orders").without_redelivery());
        source.push_kafka(0, 7, b"{{{");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        sink.fail_writes(true); // downgrades the dead-letter to a Retry
        let rt = runtime(malformed_binding(), Arc::clone(&source), Arc::clone(&sink));

        // Reported by the end-of-pass check, in the very pass that retried —
        // no second poll needed, and nothing further is pulled meanwhile.
        let err = rt
            .run_once()
            .await
            .expect_err("a retried head must stall the pass");
        assert!(
            matches!(err, ConnectorError::Stalled { partition: 0, .. }),
            "expected a stall on partition 0, got {err:?}"
        );
        assert_eq!(
            rt.offsets.lock().await.held(0),
            0,
            "and it is reported with nothing at all piled up behind it, which \
             is exactly what the backlog bound cannot see"
        );
        assert_eq!(source.abandoned().len(), 1, "the retry did happen");
    }

    #[tokio::test]
    async fn a_retry_on_a_redelivering_source_never_reports_a_stall() {
        // The mirror image, and the reason the signal is a source capability
        // rather than "any retry": SQS's visibility timeout genuinely does
        // hand the message back, so a transient dispatch failure is not a
        // wedge. Firing here would recycle the consumer on every blip.
        //
        // Kafka-shaped coordinates, so the only thing distinguishing this from
        // the test above is `abandon_redelivers`.
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 7, b"{{{");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        sink.fail_writes(true);
        let rt = runtime(malformed_binding(), Arc::clone(&source), Arc::clone(&sink));

        assert_eq!(rt.run_once().await.unwrap().retried, 1);
        rt.run_once()
            .await
            .expect("a broker that redelivers is not wedged by a retry");
    }

    #[tokio::test]
    async fn one_poison_message_does_not_block_the_rest_of_the_batch() {
        // AC6's falsifiable bar, in miniature: 1 poison + N valid must not
        // stop the N from being processed. Both mapper outcomes are terminal
        // here, so every message settles in a single pass.
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 1, b"poison");
        for offset in 2..=11 {
            source.push_kafka(0, offset, b"ok");
        }
        let sink = Arc::new(RecordingDeadLetterSink::new());
        // Everything is "malformed" here; the point under test is that the
        // runtime keeps draining rather than wedging on the first failure.
        let rt = runtime(malformed_binding(), Arc::clone(&source), Arc::clone(&sink));

        let summary = rt.run_once().await.unwrap();

        assert_eq!(summary.received, 11);
        assert_eq!(summary.dead_lettered, 11);
        assert_eq!(sink.entries().len(), 11);
    }

    #[tokio::test]
    async fn empty_poll_is_a_no_op() {
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), source, sink);
        assert_eq!(rt.run_once().await.unwrap(), PassSummary::default());
    }

    #[tokio::test]
    async fn a_closed_source_surfaces_closed_so_run_can_stop() {
        let source = Arc::new(MockSource::new("orders"));
        source.close();
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), source, sink);
        assert!(matches!(rt.run_once().await, Err(ConnectorError::Closed)));
    }

    #[tokio::test]
    async fn run_stops_promptly_when_cancelled() {
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = Arc::new(runtime(malformed_binding(), source, sink));

        let cancel = CancellationToken::new();
        let handle = {
            let rt = Arc::clone(&rt);
            let cancel = cancel.clone();
            tokio::spawn(async move { rt.run(cancel).await })
        };
        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("run must observe cancellation")
            .expect("run task must not panic");
    }

    #[tokio::test]
    async fn ack_waits_for_the_contiguous_offset_prefix() {
        // AC4's Kafka-specific hazard: committing offset 5 while 4 is still in
        // flight would silently skip 4 on a crash. The runtime must hold the
        // high-water mark back until the prefix is contiguous.
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), Arc::clone(&source), Arc::clone(&sink));

        // Observe both offsets before either completes.
        rt.offsets.lock().await.observe(0, 4);
        rt.offsets.lock().await.observe(0, 5);

        // Completing 5 first must NOT commit.
        rt.ack(&MessageHandle::positioned("orders:0:5", 0, 5)).await;
        assert!(
            source.acked().is_empty(),
            "offset 5 must not commit while 4 is in flight"
        );

        // Completing 4 advances the prefix through 5 — and the commit MUST be
        // at 5, the advanced mark, not at 4 (the handle that happened to
        // trigger the advance). Committing 4 here would leave 5 uncommitted
        // and re-read it on a crash, even though it is durably settled.
        rt.ack(&MessageHandle::positioned("orders:0:4", 0, 4)).await;
        assert_eq!(source.acked().len(), 1);
        assert_eq!(
            source.acked()[0].position,
            Some(5),
            "the commit must be at the advanced high-water mark, not the \
             triggering handle's own offset"
        );
    }

    #[tokio::test]
    async fn an_in_order_ack_commits_its_own_offset_unchanged() {
        // The common case: offsets settle in order, so the advanced mark IS
        // the handle's own position and no synthesis happens.
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), Arc::clone(&source), sink);

        rt.offsets.lock().await.observe(0, 7);
        rt.ack(&MessageHandle::positioned("orders:0:7", 0, 7)).await;

        assert_eq!(source.acked().len(), 1);
        assert_eq!(source.acked()[0].position, Some(7));
        assert_eq!(
            source.acked()[0].token,
            "orders:0:7",
            "an unsynthesized ack keeps the adapter's own token"
        );
    }

    #[tokio::test]
    async fn opaque_handles_ack_immediately_without_offset_gating() {
        // SQS deletes are per-message, so there is no prefix to respect.
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), Arc::clone(&source), sink);

        rt.ack(&MessageHandle::opaque("receipt-1")).await;
        assert_eq!(source.acked().len(), 1);
    }

    // Multi-thread for the same reason as the peak test below: on a
    // current-thread runtime every dispatch is serialized anyway, so an
    // ordering assertion would hold vacuously and prove nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn max_in_flight_one_dispatches_in_broker_order() {
        // The ordering caveat in `13-broker-connectors.md` offers exactly one
        // remedy for per-key order -- `.max_in_flight(1)` -- so that remedy
        // has to be true. A permit is acquired BEFORE the next message is
        // dispatched, so a single permit makes dispatch strictly sequential in
        // batch order, which for one partition is broker order.
        //
        // Falsifiable: raising this to 4 makes the assertion fail (the
        // observed order interleaves), so the test is measuring the bound and
        // not the mock's insertion order.
        let seen = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
        let seen_m = Arc::clone(&seen);

        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(move |ctx| {
                    // Runs while the permit is held, so it observes dispatch
                    // order rather than completion order.
                    let offset = ctx.coordinates.render();
                    let n: i64 = offset
                        .rsplit(':')
                        .next()
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(-1);
                    seen_m
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(n);
                    // Stagger the work so an unbounded runtime would visibly
                    // interleave rather than accidentally staying in order.
                    std::thread::sleep(std::time::Duration::from_millis(if n % 2 == 0 {
                        6
                    } else {
                        1
                    }));
                    Err(MappingError::Deserialize("x".to_string()))
                })
                .max_in_flight(1),
        );

        let source = Arc::new(MockSource::new("orders"));
        for offset in 0..12 {
            source.push_kafka(0, offset, b"{}");
        }
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(binding, Arc::clone(&source), sink);

        let summary = rt.run_once().await.unwrap();
        assert_eq!(summary.received, 12);

        let order = seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(
            order,
            (0..12).collect::<Vec<i64>>(),
            "max_in_flight(1) must dispatch in broker order -- it is the only \
             per-key ordering remedy the guide offers",
        );
    }

    // Multi-thread: the observation below blocks its worker thread, which on
    // the default current-thread runtime would serialize the dispatches and
    // make the test pass vacuously with a peak of 1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn in_flight_dispatch_is_bounded_by_max_in_flight() {
        // AC5: the connector bounds its own concurrency so a backlog cannot
        // stampede the admission path. Assert the OBSERVED peak, not merely
        // the semaphore's initial permit count -- a bound that is never
        // actually taken would satisfy the latter while stampeding anyway.
        const LIMIT: usize = 4;
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let live = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (peak_m, live_m) = (Arc::clone(&peak), Arc::clone(&live));

        // The mapper runs while the permit is held, so it is the observation
        // point for real in-flight concurrency.
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(move |_ctx| {
                    use std::sync::atomic::Ordering;
                    let now = live_m.fetch_add(1, Ordering::SeqCst) + 1;
                    peak_m.fetch_max(now, Ordering::SeqCst);
                    // Spin briefly so concurrent dispatches genuinely overlap.
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    live_m.fetch_sub(1, Ordering::SeqCst);
                    Err(MappingError::Deserialize("x".to_string()))
                })
                .max_in_flight(LIMIT),
        );

        let source = Arc::new(MockSource::new("orders"));
        for offset in 0..32 {
            source.push_kafka(0, offset, b"{}");
        }
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(binding, Arc::clone(&source), sink);

        let summary = rt.run_once().await.unwrap();
        assert_eq!(summary.received, 32, "the whole backlog is drained");

        let observed = peak.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            observed > 1,
            "the test must actually exercise concurrency, saw peak {observed}",
        );
        assert!(
            observed <= LIMIT,
            "peak in-flight {observed} exceeded max_in_flight {LIMIT}",
        );
    }

    #[tokio::test]
    async fn lag_is_reported_to_metrics_when_the_source_exposes_it() {
        // AC8: consumer lag reaches the recorder with the binding name as its
        // only label.
        let source = Arc::new(MockSource::new("orders"));
        source.set_lag(Some(42));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let metrics = Arc::new(RecordingMetrics::default());
        let rt = runtime_with_metrics(
            malformed_binding(),
            Arc::clone(&source),
            sink,
            Arc::clone(&metrics),
        );

        assert_eq!(rt.run_once().await.unwrap(), PassSummary::default());
        assert_eq!(
            metrics.lag(),
            vec![("orders".to_string(), 42)],
            "lag must be emitted with the binding name, not swallowed",
        );
    }

    #[tokio::test]
    async fn a_source_that_reports_no_lag_emits_no_lag_sample() {
        // A gauge fabricated from `None` would read as "zero lag" on a
        // dashboard, which is materially different from "unknown".
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let metrics = Arc::new(RecordingMetrics::default());
        let rt = runtime_with_metrics(malformed_binding(), source, sink, Arc::clone(&metrics));

        rt.run_once().await.unwrap();
        assert!(metrics.lag().is_empty());
    }

    #[tokio::test]
    async fn a_poisoned_message_emits_received_and_poisoned_with_bounded_labels() {
        // AC8: every metric carries ONLY the binding name and a closed-set
        // outcome -- never the message key, offset, or payload (ADR-0001 §7).
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 1, b"{{{");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let metrics = Arc::new(RecordingMetrics::default());
        let rt = runtime_with_metrics(
            malformed_binding(),
            Arc::clone(&source),
            Arc::clone(&sink),
            Arc::clone(&metrics),
        );

        rt.run_once().await.unwrap();

        assert_eq!(metrics.received(), vec!["orders".to_string()]);
        assert_eq!(
            metrics.poisoned(),
            vec![("orders".to_string(), PoisonReason::Malformed)],
        );
        // `harvest.connector.dispatched` is the SETTLEMENT breakdown, not a
        // count of messages that reached harvest: every received message
        // records exactly one outcome, so the series sums to
        // `harvest.connector.received` and a dashboard can show the full mix.
        assert_eq!(
            metrics.dispatched(),
            vec![("orders".to_string(), ConnectorOutcome::DeadLettered)],
        );
    }

    #[tokio::test]
    async fn a_dead_letter_sink_failure_still_counts_the_poison_but_does_not_ack() {
        // The strike bookkeeping and the metric must describe what ACTUALLY
        // happened: the message was poison, the record did not land, so the
        // message is retried rather than silently dropped.
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 1, b"{{{");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        sink.fail_writes(true);
        let metrics = Arc::new(RecordingMetrics::default());
        let rt = runtime_with_metrics(
            malformed_binding(),
            Arc::clone(&source),
            Arc::clone(&sink),
            Arc::clone(&metrics),
        );

        rt.run_once().await.unwrap();

        assert_eq!(metrics.received(), vec!["orders".to_string()]);
        assert!(
            source.acked().is_empty(),
            "acking a message whose dead-letter record failed would lose it",
        );
    }

    // ---- Stall detection ----

    /// A runtime whose partition-0 prefix head (offset 0) was delivered and
    /// never settled, while `settled` later offsets did — the exact shape a
    /// retried message leaves behind on Kafka, whose `abandon` cannot force a
    /// redelivery.
    ///
    /// Seeded through the tracker (as `ack_waits_for_the_contiguous_offset_prefix`
    /// does) rather than through a mapper, because the point under test is the
    /// commit prefix, not how a particular message came to be retried.
    async fn stalled_runtime(threshold: Option<usize>, settled: i64) -> ConnectorRuntime {
        let rt = runtime_with_metrics(
            malformed_binding(),
            Arc::new(MockSource::new("orders")),
            Arc::new(RecordingDeadLetterSink::new()),
            Arc::new(RecordingMetrics::default()),
        )
        .with_config(ConnectorRuntimeConfig {
            stall_threshold: threshold,
            ..ConnectorRuntimeConfig::default()
        });

        // The head is delivered and never completes: it blocks the prefix.
        rt.offsets.lock().await.observe(0, 0);
        for offset in 1..=settled {
            rt.offsets.lock().await.observe(0, offset);
            rt.ack(&MessageHandle::positioned(
                format!("orders:0:{offset}"),
                0,
                offset,
            ))
            .await;
        }
        rt
    }

    #[tokio::test]
    async fn a_permanently_blocked_prefix_fails_the_pass_with_its_own_error() {
        // Kafka's `abandon` is a no-op by design (not committing is not a
        // nack), so a retried message is only handed back when the consumer is
        // rebuilt and re-reads from the last commit. Left unreported the
        // partition's prefix stays blocked forever and its commit never
        // advances -- silently, because every later message settles fine.
        let rt = stalled_runtime(Some(3), 4).await;
        assert_eq!(rt.offsets.lock().await.held(0), 4);

        // Reported even on an IDLE pass: a stalled partition usually goes
        // quiet, so a check that only ran when messages arrived would miss it.
        let err = rt
            .run_once()
            .await
            .expect_err("a blocked prefix past the bound must fail the pass");

        // Structured, not a string: `run` has to tell this apart from a
        // transient broker error, because re-polling the same wedged consumer
        // accomplishes nothing.
        assert!(
            matches!(
                err,
                ConnectorError::Stalled {
                    partition: 0,
                    held: 4,
                    threshold: 3,
                }
            ),
            "the error must carry the partition, depth and bound: {err:?}"
        );
        let rendered = err.to_string();
        assert!(
            rendered.contains("partition 0") && rendered.contains("rebuilt"),
            "the message must name the blocked partition and the remedy: {rendered}"
        );
    }

    #[test]
    fn the_default_stall_bound_is_derived_from_the_concurrency_it_must_clear() {
        // On by default: an unconfigured deployment must still surface a
        // permanently blocked prefix, or the retry never happens for anyone
        // who did not know to opt in.
        assert_eq!(ConnectorRuntimeConfig::default().stall_threshold, None);

        // Healthy out-of-order settlement is bounded by `max_in_flight` (only
        // that many messages are ever outstanding), so the derived bound sits
        // well clear of it.
        assert!(effective_stall_threshold(None, 64) > 64);
        assert!(effective_stall_threshold(None, 8) >= MIN_DERIVED_STALL_THRESHOLD);
        // A tiny binding still gets a floor, so a 1-in-flight connector does
        // not fail its pass the moment two messages settle out of order.
        assert_eq!(
            effective_stall_threshold(None, 1),
            MIN_DERIVED_STALL_THRESHOLD
        );
        // Explicit wins, and 0 is the documented off switch.
        assert_eq!(effective_stall_threshold(Some(10), 64), 10);
        assert_eq!(effective_stall_threshold(Some(0), 64), 0);
        // Never overflows for a pathological binding.
        assert!(effective_stall_threshold(None, usize::MAX) > 0);
    }

    #[tokio::test]
    async fn stall_detection_can_be_disabled_and_ignores_depths_below_its_bound() {
        let disabled = stalled_runtime(Some(0), 4).await;
        assert!(
            disabled.run_once().await.is_ok(),
            "threshold 0 disables the check"
        );

        // Configured, but the held depth is below the bound: ordinary
        // out-of-order settlement must never fail a pass.
        let under = stalled_runtime(Some(10), 4).await;
        assert!(under.run_once().await.is_ok());

        // And the default bound is generous enough that a shallow backlog on
        // a default binding is not mistaken for a stall.
        let defaulted = stalled_runtime(None, 4).await;
        assert!(defaulted.run_once().await.is_ok());
    }

    #[tokio::test]
    async fn a_stall_clears_once_the_head_finally_settles() {
        // The check must not latch: once the blocked head settles (the
        // recreated consumer redelivered it and it dispatched), the prefix
        // drains and passes succeed again.
        let rt = stalled_runtime(Some(3), 4).await;
        assert!(rt.run_once().await.is_err());

        rt.ack(&MessageHandle::positioned("orders:0:0", 0, 0)).await;
        assert_eq!(rt.offsets.lock().await.held(0), 0);
        assert!(
            rt.run_once().await.is_ok(),
            "a drained prefix must stop failing passes"
        );
    }

    /// A directly-constructed runtime defaults to `DeadLetterMode::HarvestSink`
    /// (the binding default) but has no sink installed. Acknowledging there
    /// would discard the message with no record anywhere — the one outcome a
    /// dead-letter path must never produce. It has to fail loudly instead, so
    /// the existing sink-failure branch abandons for redelivery.
    #[tokio::test]
    async fn an_unconfigured_sink_never_acks_a_dead_letter_away() {
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 1, b"{{{");
        let erased: Arc<dyn EventSource> = Arc::clone(&source) as Arc<dyn EventSource>;
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            erased,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        );

        let summary = rt.run_once().await.expect("the pass itself still succeeds");

        assert_eq!(
            summary,
            PassSummary {
                received: 1,
                acked: 0,
                retried: 1,
                dead_lettered: 0,
            },
            "an unrecorded dead letter must be retried, never acked away"
        );
        assert!(
            source.acked().is_empty(),
            "the message must remain on the broker"
        );
    }

    /// The fix is scoped to the broken pairing. A broker-native binding never
    /// writes to the harvest sink at all (it abandons for the redrive policy),
    /// so leaving its sink unset stays perfectly valid.
    #[tokio::test]
    async fn a_broker_native_binding_needs_no_harvest_sink() {
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 1, b"{{{");
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Deserialize("not json".to_string())))
                .broker_native_dead_letter(),
        );
        let erased: Arc<dyn EventSource> = Arc::clone(&source) as Arc<dyn EventSource>;
        let rt = ConnectorRuntime::new(
            binding,
            erased,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        );

        let summary = rt.run_once().await.expect("pass succeeds");

        assert_eq!(
            summary.dead_lettered, 1,
            "abandoned to the broker's redrive"
        );
        assert_eq!(summary.retried, 0);
    }

    /// A zero batch size makes every adapter return nothing, so the runtime
    /// would read every pass as idle and consume forever without progress.
    /// Clamped at the runtime's single call site, so an embedder's own
    /// `EventSource` is covered too — not just the two shipped adapters.
    #[test]
    fn a_zero_batch_size_cannot_silently_stop_consumption() {
        assert_eq!(effective_max_batch(0), 1, "zero must never reach a source");
        assert_eq!(effective_max_batch(1), 1);
        assert_eq!(effective_max_batch(32), 32);
    }

    #[tokio::test]
    async fn a_zero_batch_config_still_consumes() {
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 1, b"{{{");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), Arc::clone(&source), Arc::clone(&sink)).with_config(
            ConnectorRuntimeConfig {
                max_batch: 0,
                ..ConnectorRuntimeConfig::default()
            },
        );

        let summary = rt.run_once().await.expect("pass succeeds");

        assert_eq!(
            summary.received, 1,
            "a misconfigured batch size must not stall the binding"
        );
    }

    /// A source that can rebuild its own consumer, so the runtime can perform
    /// the retry in-process instead of hoping something external notices.
    #[derive(Debug)]
    struct RecoverableSource {
        inner: MockSource,
        recovered: std::sync::atomic::AtomicUsize,
    }

    impl RecoverableSource {
        fn new(stream: &str) -> Self {
            Self {
                inner: MockSource::new(stream),
                recovered: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn recover_count(&self) -> usize {
            self.recovered.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl EventSource for RecoverableSource {
        fn stream(&self) -> &str {
            self.inner.stream()
        }
        async fn receive(
            &self,
            max: usize,
            timeout: std::time::Duration,
        ) -> Result<Vec<InboundMessage>, ConnectorError> {
            self.inner.receive(max, timeout).await
        }
        async fn ack(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
            self.inner.ack(handle).await
        }
        async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
            self.inner.abandon(handle).await
        }
        async fn recover(&self) -> Result<bool, ConnectorError> {
            self.recovered
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(true)
        }
    }

    /// A positional source that wedges itself on every pass and can rebuild.
    ///
    /// Models the shape of a downstream outage on Kafka: each pass receives a
    /// fresh record, its dispatch fails, and because `abandon` cannot force a
    /// redelivery the offset becomes a permanently blocked prefix head — so
    /// the next pass stalls, recovers, and the cycle repeats for as long as
    /// the outage lasts.
    #[derive(Debug)]
    struct WedgingRecoverableSource {
        inner: MockSource,
        next_offset: std::sync::atomic::AtomicI64,
        recovered: std::sync::atomic::AtomicUsize,
    }

    impl WedgingRecoverableSource {
        fn new(stream: &str) -> Self {
            Self {
                inner: MockSource::new(stream).without_redelivery(),
                next_offset: std::sync::atomic::AtomicI64::new(0),
                recovered: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn recover_count(&self) -> usize {
            self.recovered.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl EventSource for WedgingRecoverableSource {
        fn stream(&self) -> &str {
            self.inner.stream()
        }
        async fn receive(
            &self,
            max: usize,
            timeout: std::time::Duration,
        ) -> Result<Vec<InboundMessage>, ConnectorError> {
            let offset = self
                .next_offset
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.push_kafka(0, offset, b"{{{");
            self.inner.receive(max, timeout).await
        }
        async fn ack(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
            self.inner.ack(handle).await
        }
        async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
            self.inner.abandon(handle).await
        }
        fn abandon_redelivers(&self) -> bool {
            false
        }
        async fn recover(&self) -> Result<bool, ConnectorError> {
            self.recovered
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(true)
        }
    }

    /// Rebuilding a Kafka consumer is not a free local operation: it triggers
    /// a **group rebalance**, which revokes and reassigns partitions across
    /// every consumer in the group, not just this one. During a downstream
    /// outage the redelivered head fails again immediately, so a loop that
    /// recovers without pausing rebuilds as fast as the group can rejoin —
    /// amplifying one binding's outage into a rebalance storm that disrupts
    /// unrelated partitions and every other consumer in the group.
    ///
    /// The recovery arm therefore has to honour the same `error_backoff` the
    /// transient-error arm does.
    #[tokio::test]
    async fn a_recovered_stall_backs_off_before_retrying_the_blocked_head() {
        let source = Arc::new(WedgingRecoverableSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        // Every message ends up `Retry`, which on this source wedges the head.
        sink.fail_writes(true);
        let backoff = std::time::Duration::from_millis(50);
        let rt = Arc::new(
            ConnectorRuntime::new(
                malformed_binding(),
                Arc::clone(&source) as Arc<dyn EventSource>,
                HarvestApiState::new(),
                Arc::new(NoOpMetrics),
                IdempotencyMode::BrokerCoordinates,
            )
            .with_dead_letter_sink(sink)
            .with_config(ConnectorRuntimeConfig {
                error_backoff: backoff,
                poll_timeout: std::time::Duration::from_millis(1),
                idle_backoff: std::time::Duration::from_millis(1),
                ..ConnectorRuntimeConfig::default()
            }),
        );

        let window = std::time::Duration::from_millis(300);
        let cancel = CancellationToken::new();
        let handle = {
            let rt = Arc::clone(&rt);
            let cancel = cancel.clone();
            tokio::spawn(async move { rt.run(cancel).await })
        };
        tokio::time::sleep(window).await;
        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("run must observe cancellation")
            .expect("run task must not panic");

        let recoveries = source.recover_count();
        assert!(
            recoveries >= 1,
            "the fixture must actually wedge and recover, or this bound is \
             vacuous (saw {recoveries})"
        );
        // 300ms / 50ms = 6 in theory; 12 leaves generous slack for a loaded
        // CI runner while staying orders of magnitude below an unthrottled
        // loop, which rebuilds as fast as the runtime can schedule it.
        let ceiling = 2 * (window.as_millis() / backoff.as_millis()) as usize;
        assert!(
            recoveries <= ceiling,
            "a recovered stall must back off before rebuilding again; saw \
             {recoveries} consumer rebuilds in {window:?} with a {backoff:?} \
             backoff (ceiling {ceiling})"
        );
    }

    /// The whole justification for detecting a stall is that something then
    /// *performs the retry*. Failing the pass only signals; the runtime has to
    /// rebuild the consumer and clear the dead generation, or the partition
    /// stays wedged exactly as it was before the detector existed.
    #[tokio::test]
    async fn a_detected_stall_rebuilds_the_consumer_and_clears_the_dead_generation() {
        let source = Arc::new(RecoverableSource::new("orders"));
        let erased: Arc<dyn EventSource> = Arc::clone(&source) as Arc<dyn EventSource>;
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            erased,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        )
        .with_dead_letter_sink(Arc::new(RecordingDeadLetterSink::new()))
        .with_config(ConnectorRuntimeConfig {
            stall_threshold: Some(3),
            ..ConnectorRuntimeConfig::default()
        });

        // A blocked head with enough settled offsets behind it to trip the bound.
        rt.offsets.lock().await.observe(0, 0);
        for offset in 1..=4 {
            rt.offsets.lock().await.observe(0, offset);
            rt.ack(&MessageHandle::positioned(
                format!("orders:0:{offset}"),
                0,
                offset,
            ))
            .await;
        }
        assert_eq!(rt.offsets.lock().await.held(0), 4, "the prefix is blocked");

        let err = rt.run_once().await.expect_err("a stalled pass must fail");
        assert!(
            matches!(err, ConnectorError::Stalled { partition: 0, .. }),
            "a stall must be its own error, not a transient broker error: {err:?}"
        );

        // `run`'s handling is what actually performs the retry.
        let recovered = rt.recover_from_stall(0).await;

        assert!(recovered, "a recoverable source must be rebuilt");
        assert_eq!(source.recover_count(), 1, "the consumer was recreated once");
        assert_eq!(
            rt.offsets.lock().await.held(0),
            0,
            "the dead generation must be cleared, else the stall latches forever"
        );
    }

    /// Not every source can rebuild itself. Saying so must be distinguishable
    /// from a successful recovery, or the runtime would loop forever believing
    /// it had fixed something.
    #[tokio::test]
    async fn a_source_that_cannot_rebuild_itself_reports_so() {
        let rt = stalled_runtime(Some(3), 4).await;
        assert!(
            !rt.recover_from_stall(0).await,
            "the default source cannot self-heal and must say so"
        );
    }

    /// A wedged binding must stop pulling. Receiving a batch and then dropping
    /// it on the stall error is pure churn — and on a positional broker it
    /// advances the consumer past messages that were never dispatched.
    #[tokio::test]
    async fn a_stalled_pass_never_pulls_a_batch_it_would_only_drop() {
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(1, 1, b"{{{");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), Arc::clone(&source), Arc::clone(&sink)).with_config(
            ConnectorRuntimeConfig {
                stall_threshold: Some(3),
                ..ConnectorRuntimeConfig::default()
            },
        );
        rt.offsets.lock().await.observe(0, 0);
        for offset in 1..=4 {
            rt.offsets.lock().await.observe(0, offset);
            rt.ack(&MessageHandle::positioned(
                format!("orders:0:{offset}"),
                0,
                offset,
            ))
            .await;
        }

        let _ = rt.run_once().await.expect_err("stalled");

        assert_eq!(
            source.pending(),
            1,
            "the message must still be on the broker, not drained and discarded"
        );
    }
}
