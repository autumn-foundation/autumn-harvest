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
use super::dead_letter::{
    ConnectorDeadLetter, DeadLetterOutcome, DeadLetterSink, UnconfiguredDeadLetterSink,
};
use super::dispatch::{DispatchRequest, dispatch};
use super::disposition::{
    DeadLetterMode, DispatchOutcome, MessageDisposition, OffsetTracker, PoisonTracker,
    decide_disposition, success_outcome,
};
use super::idempotency::message_idempotency_key_in;
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
    /// [`MAX_POISON_ENTRIES`] is the hard backstop.
    pub poison_retention: std::time::Duration,
    /// Longest the shutdown drain waits for in-flight dispatches to settle.
    ///
    /// Cancelling `run` drops the in-flight pass, which **detaches** its
    /// spawned dispatches — dropping a `JoinHandle` does not abort the task —
    /// so each keeps its permit until it finishes on its own. One wedged in a
    /// database call, a broker ack or a custom sink would otherwise hold the
    /// drain forever, and `stop_harvest_runtime` awaits every connector
    /// *before* it stops the runner, so that is a permanently hung shutdown
    /// rather than a slow one.
    ///
    /// Bounding it is safe because the drain is an **optimisation, not an
    /// invariant**: it exists so a dispatch that already committed also gets
    /// acked, sparing a redelivery. Giving up early costs exactly that
    /// redelivery, which at-least-once already permits and the idempotency key
    /// already dedupes. Mirrors the engine's own `WorkerConfig::shutdown_timeout`.
    pub shutdown_timeout: std::time::Duration,
}

/// Never derive a bound below this, so a binding with tiny `max_in_flight`
/// does not fail its pass the moment a couple of messages settle out of order.
pub const MIN_DERIVED_STALL_THRESHOLD: usize = 32;

/// The longest an [`EventSource::lag`] implementation may take to abort its
/// own sample — and so the floor under the runtime's budget for one.
///
/// This is a **contract**, not merely a default. The runtime's `timeout` around
/// `lag()` drops its own future; it cannot stop the adapter's work, and for a
/// Kafka walk it definitively cannot — that runs on `spawn_blocking`, and a
/// blocking task is never cancelled. Only the adapter's own internal bound
/// stops it.
///
/// So the two have to agree: an adapter bounds its walk at or below this, the
/// runtime never gives up sooner, and an abandoned sample is therefore always
/// finished before the next one is armed. Without the floor a cadence shorter
/// than the adapter's bound lets abandoned walks accumulate — one blocking
/// thread and one round of broker traffic each, against the very broker whose
/// slowness caused the timeout.
pub const ADAPTER_LAG_SAMPLE_CEILING: std::time::Duration = std::time::Duration::from_secs(10);

/// How long a hard-stopped dispatch may spend handing its message back.
///
/// Short on purpose. By the time this runs the shutdown drain has already
/// waited its full deadline, so the broker this call talks to is quite possibly
/// the one that caused the timeout. Losing custody of one message costs a
/// stranded record until the adapter's own lease expires; blocking here costs
/// the shutdown the deadline was there to bound.
const HARD_STOP_ABANDON_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

/// How long the drain waits for the dispatches it hard-stopped to finish
/// handing their messages back.
///
/// Each straggler's recovery is itself bounded by
/// [`HARD_STOP_ABANDON_BUDGET`] and they run concurrently, so this is that
/// bound plus slack for the runtime to actually schedule them under the load
/// that caused the timeout. Defined *as* that constant rather than merely
/// sized against it, so lengthening the per-message budget cannot silently
/// outrun the wait that joins it.
const HARD_STOP_CLEANUP_BUDGET: std::time::Duration =
    HARD_STOP_ABANDON_BUDGET.saturating_add(std::time::Duration::from_secs(1));

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
            // Same 30s the engine gives its own worker drain, for the same
            // reason: long enough that an ordinary in-flight dispatch settles
            // and gets acked, short enough that a rolling deployment is not
            // held hostage by one wedged call.
            shutdown_timeout: std::time::Duration::from_secs(30),
        }
    }
}

/// Drives one event source against one binding.
///
/// **Single-use.** A runtime that has hard-stopped is spent: [`Self::run`]
/// refuses to start again and returns immediately (see the guard there for
/// why the flag cannot simply be reset). Build a fresh `ConnectorRuntime` to
/// resume consuming.
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
    /// Cancelled only when the shutdown drain gives up, to stop a dispatch
    /// that outlived its deadline.
    ///
    /// Cancelling `run` detaches the in-flight dispatches rather than aborting
    /// them, and this process may outlive harvest (an embedder stopping the
    /// runtime while its web app keeps serving), so "detached and wedged"
    /// means leaked, not merely late. Each spawned dispatch selects on this,
    /// so it unwinds at its next await point — the same effect
    /// `JoinHandle::abort` would have, without threading handles back out of
    /// the pass.
    ///
    /// Deliberately NOT the binding's own cancellation token: that fires at
    /// the *start* of shutdown, which would abandon every in-flight dispatch
    /// immediately and turn every clean stop into needless redelivery. This
    /// fires only after the drain has already waited its full deadline.
    hard_stop: Arc<CancellationToken>,
}

/// What a stall-recovery attempt achieved.
///
/// The distinction between the two failure arms is load-bearing: only
/// [`Self::Unsupported`] justifies stopping an unsupervised binding for the
/// life of the process, because it is a property of the *source type* rather
/// than of the moment. A [`Self::Failed`] rebuild is retryable — and usually
/// caused by the very outage that produced the stall.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallRecovery {
    /// The consumer was rebuilt; the blocked message will be redelivered.
    Rebuilt,
    /// This source cannot rebuild itself at all ([`EventSource::recover`]'s
    /// default). There is no in-process recovery, so the binding stops.
    Unsupported,
    /// The rebuild was attempted and failed. Transient by assumption: back
    /// off and try again rather than abandoning the stream.
    Failed,
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

/// What settling a message actually did to the broker.
///
/// Carries more than the disposition because the harvest-sink dead-letter path
/// learns something the disposition cannot express: whether the sink already
/// held a record for this idempotency key. That is the *durable* half of the
/// poison dedupe — the in-process tracker only answers it within one process,
/// and a restart between the sink write and the ack is exactly when the
/// redelivery happens.
#[derive(Debug)]
struct Settlement {
    /// What actually happened, which may be a downgrade of what was decided.
    disposition: MessageDisposition,
    /// The sink's verdict, on the one path that has one: `None` for every
    /// disposition that never reaches a sink (including a dead-letter whose
    /// write *failed*, which is downgraded to a retry and recorded nothing).
    dead_letter: Option<DeadLetterOutcome>,
}

impl Settlement {
    /// A settlement that never consulted a dead-letter sink.
    const fn of(disposition: MessageDisposition) -> Self {
        Self {
            disposition,
            dead_letter: None,
        }
    }
}

impl ConnectorRuntime {
    /// Build a runtime for `binding` over `source`.
    ///
    /// # Panics
    ///
    /// If `binding` asks for [`DeadLetterMode::BrokerNative`] but `source`
    /// reports no broker-native dead-letter destination
    /// ([`EventSource::has_native_dead_letter`]) — that pairing quarantines
    /// nothing and re-reads the poison message forever. See
    /// [`broker_native_dead_letter_is_supported`](super::broker_native_dead_letter_is_supported).
    #[must_use]
    pub fn new(
        binding: Arc<SourceBinding>,
        source: Arc<dyn EventSource>,
        api_state: HarvestApiState,
        metrics: Arc<dyn MetricsRecorder>,
        idempotency_mode: IdempotencyMode,
    ) -> Self {
        // Same rule the plugin enforces at build time, applied here because
        // this constructor is exported: an embedder driving the runtime
        // directly would otherwise reach `AbandonToBrokerDeadLetter`, whose
        // nack is a no-op on such an adapter, and report a terminal
        // disposition for a message that reached no dead-letter destination
        // and is not handed back in-session. Panicking matches the plugin's
        // posture -- this is a wiring mistake, not a runtime condition.
        assert!(
            super::broker_native_dead_letter_is_supported(
                binding.dead_letter_mode,
                source.has_native_dead_letter(),
            ),
            "harvest connector binding '{}' asks for broker-native dead-lettering, but its \
             adapter for stream '{}' reports no dead-letter destination that abandoning a \
             message feeds, so a poison message would be re-read forever instead of being \
             quarantined. Kafka has no per-message nack at all. SQS needs a redrive policy on \
             the queue: add one, or (when the source was built with `SqsSource::new`, or its \
             `GetQueueAttributes` probe was denied) declare it with \
             `SqsSourceConfig::has_redrive_policy(true)`. Otherwise drop \
             `.broker_native_dead_letter()` so poison messages land in \
             `harvest_connector_dead_letters` instead",
            binding.name,
            binding.stream,
        );

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
            hard_stop: Arc::new(CancellationToken::new()),
        }
    }

    /// Resolve the dedupe mode from the binding and the target workflow, then
    /// build the runtime.
    ///
    /// # Panics
    ///
    /// Inherits [`Self::new`]'s dead-letter-mode check.
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

    /// Tell the operator, once at startup, when this deployment narrows the
    /// `BrokerCoordinates` dedupe promise.
    ///
    /// The claim `BrokerCoordinates` writes is shard-local while the dispatch
    /// is routed by the explicit `workflow_id` (issue #808), so on a
    /// multi-shard deployment a mapper whose id drifts across a redelivery
    /// lands on a different shard and double-starts. A warning rather than a
    /// hard failure: a deterministic mapper — the common case, and what every
    /// example ships — is perfectly safe, so refusing to start would break
    /// working deployments to guard against a mapper bug.
    ///
    /// Silent when the runtime is not installed yet: this is advisory, and the
    /// dispatch path already fails loudly on its own if storage is missing.
    fn warn_if_coordinate_dedupe_is_shard_local_only(&self) {
        let Ok(runtime) = self.api_state.runtime() else {
            return;
        };
        let shards = runtime.router().readable_shards().len();
        if !super::binding::coordinate_dedupe_is_shard_local_only(self.idempotency_mode, shards) {
            return;
        }
        tracing::warn!(
            source = self.binding.name,
            stream = %self.binding.stream,
            workflow = self.binding.target.workflow(),
            readable_shards = shards,
            "connector binding dedupes on broker coordinates, but this deployment has more than \
             one shard: the start-idempotency claim is shard-local and the dispatch routes by the \
             mapped workflow_id, so a redelivery whose mapping function produces a DIFFERENT \
             workflow_id would route to another shard, miss the claim, and start a second \
             execution. Make the mapping function's workflow_id deterministic for a given message \
             (the usual case), or run this binding on a single-shard deployment"
        );
    }

    /// Consume until `cancel` fires or the source closes.
    pub async fn run(&self, cancel: CancellationToken) {
        let source_name = self.binding.name;
        // A runtime is single-use, and `hard_stop` is what records that it has
        // been spent. It is never reset — it is an `Arc` shared with every
        // dispatch already handed out, so clearing it would un-cancel
        // stragglers still unwinding. Running on top of a fired one would
        // consume messages and hand every single one straight back at its
        // first await point: a hot receive-and-abandon loop that presents as a
        // healthy consumer while making no progress, and spends a billed API's
        // request budget doing it. Build a fresh runtime instead.
        if self.hard_stop.is_cancelled() {
            tracing::error!(
                source = source_name,
                "refusing to start a connector whose runtime was already hard-stopped; \
                 a `ConnectorRuntime` is single-use — build a new one to resume consuming"
            );
            return;
        }
        tracing::info!(
            source = source_name,
            stream = %self.binding.stream,
            workflow = self.binding.target.workflow(),
            max_in_flight = self.binding.max_in_flight,
            "harvest connector started"
        );
        self.warn_if_coordinate_dedupe_is_shard_local_only();

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
                        // Only `Unsupported` stops the binding: it is a
                        // property of the source TYPE, so retrying can never
                        // change the answer. A `Failed` rebuild is the
                        // opposite -- typically the same downstream outage
                        // that caused the stall -- so it falls through to the
                        // shared backoff below and is retried on the next
                        // pass, which is what lets consumption resume once the
                        // broker comes back.
                        // Recovery must race cancellation, not merely be
                        // *reached* from a `select!` that already resolved: a
                        // rebuild is a network operation against the same
                        // broker whose outage usually caused the stall, so it
                        // can block indefinitely. Awaiting it bare would park
                        // `run` *before* it reaches the bounded
                        // `drain_in_flight` below, outside the reach of the
                        // very deadline that exists to guarantee shutdown
                        // terminates -- and `stop_harvest_runtime` awaits this
                        // handle with none of its own.
                        let recovery = tokio::select! {
                            () = cancel.cancelled() => break,
                            recovery = self.recover_from_stall(partition) => recovery,
                        };
                        if recovery == StallRecovery::Unsupported {
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

        self.drain_in_flight().await;
        tracing::info!(source = source_name, "harvest connector stopped");
    }

    /// Wait — for a bounded time — for in-flight dispatches to settle.
    ///
    /// Cancelling `run` drops the in-flight pass, which **detaches** its
    /// spawned dispatches (dropping a `JoinHandle` does not abort the task),
    /// so each holds its permit until it finishes on its own. Draining is what
    /// lets a dispatch that already committed also get acked, sparing the
    /// broker a redelivery.
    ///
    /// That is an optimisation, not an invariant, which is exactly why it may
    /// be abandoned: the contract is at-least-once, so an unacked message is
    /// redelivered and deduped by its idempotency key. An **unbounded** wait
    /// is not the safe choice — `stop_harvest_runtime` awaits every connector
    /// before it stops the runner, so one dispatch wedged in a database call,
    /// a broker ack or a custom sink hangs the whole application's shutdown,
    /// beyond the reach of the engine's own `shutdown_timeout`.
    ///
    /// On timeout the stragglers are hard-stopped rather than left detached:
    /// this process may outlive harvest, and a task that will never return is
    /// a leak, not a straggler.
    async fn drain_in_flight(&self) {
        let permits = u32::try_from(self.binding.max_in_flight.max(1)).unwrap_or(u32::MAX);
        if tokio::time::timeout(
            self.config.shutdown_timeout,
            self.permits.acquire_many(permits),
        )
        .await
        .is_err()
        {
            // `available_permits` is a snapshot, but every holder is an
            // in-flight dispatch, so it names the work being abandoned.
            let in_flight = self
                .binding
                .max_in_flight
                .max(1)
                .saturating_sub(self.permits.available_permits());
            tracing::warn!(
                source = self.binding.name,
                in_flight,
                timeout_secs = self.config.shutdown_timeout.as_secs(),
                "harvest connector gave up draining in-flight dispatches at shutdown; they are \
                 being abandoned and their messages will be redelivered"
            );
            self.hard_stop.cancel();

            // Cancelling only *wakes* the stragglers. The recovery that hands
            // each message back runs inside a task this shutdown has already
            // detached, so returning now would let `stop_harvest_runtime` see
            // this connector as finished and the process exit before any
            // `abandon` reaches the broker — trading a leaked task for a
            // leaked message, which is what the hard stop exists to prevent.
            //
            // Re-acquiring joins exactly that cleanup: a straggler releases
            // its permit only after its recovery returns. Bounded for the
            // same reason the drain above is — a hard stop already means
            // something is wedged, and the process must still be able to exit.
            if tokio::time::timeout(HARD_STOP_CLEANUP_BUDGET, self.permits.acquire_many(permits))
                .await
                .is_err()
            {
                tracing::warn!(
                    source = self.binding.name,
                    "harvest connector could not confirm its hard-stopped dispatches handed their \
                     messages back; the adapter may hold them until its own lease expires"
                );
            }
        }
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
            .expire_stale_as_of(std::time::Instant::now(), self.config.poison_retention);

        // Sampled BEFORE receiving, so no message is ever in custody while this
        // runs. `lag()` is a live broker round-trip -- SQS bills a
        // `GetQueueAttributes`, Kafka does a metadata + watermark fetch -- and
        // either can be slow or retry internally. On SQS the visibility timer
        // starts at `ReceiveMessage`, not at dispatch, so sampling with a batch
        // already in hand spends that batch's visibility budget: another
        // replica receives the messages, their `ApproximateReceiveCount` climbs
        // toward the queue's `maxReceiveCount`, and a perfectly processable
        // message reaches the broker DLQ having never once failed. The gauge is
        // throttled to a scrape cadence anyway, so which side of the receive it
        // lands on is immaterial to its accuracy.
        //
        // The sample is BUDGETED, because being on this side of the receive is
        // what makes an unbounded one lethal: it blocks the pass outright, so
        // the connector stops consuming entirely while it waits. That is worst
        // exactly when it matters most — a degraded broker makes the round-trip
        // slow AND makes the backlog grow, so the gauge you are paying for
        // reports a number you cannot act on because the connector is too busy
        // measuring it to consume. An over-budget sample is abandoned; the
        // gauge simply does not update, which is the same honest failure mode
        // the adapters already use when a partial answer is unavailable.
        if self.lag_sample_is_due() {
            match tokio::time::timeout(self.lag_sample_budget(), self.source.lag()).await {
                Ok(Some(lag)) => self.metrics.record_connector_lag(self.binding.name, lag),
                // The adapter declined (a watermark was unavailable, say). Not
                // an error — reporting a partial total would be worse.
                Ok(None) => {}
                Err(_) => tracing::warn!(
                    source = self.binding.name,
                    budget_ms = self.lag_sample_budget().as_millis(),
                    "connector lag sample exceeded its budget and was abandoned;                      the gauge will be stale. Widen lag_sample_interval, or reduce                      what the sample costs (a Kafka sample scales with partition count)"
                ),
            }
            // Re-stamp from COMPLETION so an expensive sample cannot re-arm
            // itself on the very next pass; see `record_lag_sample_completed`.
            self.record_lag_sample_completed();
        }

        let batch = self
            .source
            .receive(
                effective_max_batch(self.config.max_batch, self.binding.max_in_flight),
                self.config.poll_timeout,
            )
            .await?;

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
            // and the abandon itself — and `message` is moved into the
            // spawned task, so the handle has to be cloned out first.
            let handle = message.handle.clone();
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
            let abandon_handle = handle.clone();
            handles.push((
                handle,
                positional,
                tokio::spawn(async move {
                    // `hard_stop` fires only after the shutdown drain has
                    // already waited its full deadline for this dispatch. It
                    // gives up on the message rather than the process: no
                    // settle runs, so it stays unacked and the broker
                    // redelivers it — at-least-once, deduped by the
                    // idempotency key. The alternative is a detached task
                    // wedged forever in a process that outlives harvest.
                    let disposition = tokio::select! {
                        d = this.process(message) => d,
                        () = this.hard_stop.cancelled() => {
                            // Cutting `process` short skipped `settle`, which
                            // is the only thing that releases the adapter's
                            // custody — so the same recovery the panic arm
                            // performs is owed here, and for the same reason
                            // it has to happen INSIDE this task: the returned
                            // disposition goes to a join loop that a real
                            // shutdown has already dropped along with
                            // `run_once`.
                            this.release_custody_after_hard_stop(&abandon_handle, positional).await;
                            MessageDisposition::Retry
                        }
                    };
                    drop(permit);
                    disposition
                }),
            ));
        }

        for (handle, positional, task) in handles {
            match task.await {
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
                    // The task died before `settle`, so neither of the normal
                    // `Retry` path's two recovery steps ran. Both are needed,
                    // and which one is load-bearing depends on the source.
                    if self.source.abandon_redelivers() {
                        // `abandon_redelivers()` describes what `abandon`
                        // DOES, not a promise the message returns on its own:
                        // `MockSource` (a supported, exported adapter) and any
                        // adapter needing an explicit nack redeliver solely
                        // from that call, so skipping it strands the message
                        // where no later pass can see it. Such a source is not
                        // wedged, so no offset is marked — otherwise every
                        // engine-side panic would recycle the consumer.
                        self.abandon(&handle).await;
                    } else if let Some((partition, position)) = positional {
                        // The mirror case: `abandon` cannot force a
                        // redelivery, and the local position has already
                        // advanced past this record, so nothing hands it back.
                        // Mark the head so recovery fires on it directly
                        // rather than waiting for later offsets to pile up
                        // behind it — at the tail of a quiet partition, none
                        // ever will.
                        self.offsets.lock().await.retried(partition, position);
                    }
                    // `record_metrics` lives inside `process`, which this task
                    // died before finishing, so the settlement sample has to
                    // be emitted here. `received` was already counted before
                    // the spawn, and the dispatched series is documented as
                    // the breakdown of it (ADR-0001 §7), so skipping this
                    // would permanently widen the gap by one per panic and
                    // hide the retry the runtime just performed.
                    self.metrics
                        .record_connector_dispatched(self.binding.name, ConnectorOutcome::Retried);
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
    /// The clear is **whole-tracker**, not just the reported partition, because
    /// `recover` rebuilds the whole client: every assigned partition re-reads
    /// from its own last commit. A downstream outage wedges all of them, but
    /// `stalled` reports one at a time, so clearing only that one would leave
    /// the rest stale (the failure above, for every partition but the first)
    /// and cost one more rebuild — and one more group rebalance — per affected
    /// partition. `partition` is retained for the diagnostic.
    ///
    /// Only the offsets are forgotten, never the poison strikes: a message
    /// that has been rejected N times is still on strike N after the rebuild,
    /// so it still reaches its threshold and dead-letters rather than
    /// restarting its count on every recovery.
    pub async fn recover_from_stall(&self, partition: i32) -> StallRecovery {
        match self.source.recover().await {
            Ok(true) => {
                self.offsets.lock().await.forget_all();
                tracing::warn!(
                    source = self.binding.name,
                    partition,
                    "connector consumer rebuilt after a commit stall; re-reading from the last \
                     commit so the blocked message is redelivered"
                );
                StallRecovery::Rebuilt
            }
            Ok(false) => StallRecovery::Unsupported,
            Err(e) => {
                // Deliberately distinct from `Ok(false)`. The usual cause of a
                // stall is a downstream outage, and the same outage is what
                // makes the rebuild fail — so this is the *expected* error at
                // exactly the moment the binding must not give up. Treating it
                // as "cannot rebuild" would stop an unsupervised binding on a
                // blip and never resume it, turning a transient broker
                // hiccup into a silent permanent outage for that stream.
                tracing::warn!(
                    source = self.binding.name,
                    partition,
                    error = %e,
                    "connector consumer rebuild failed after a commit stall; retrying after \
                     backoff"
                );
                StallRecovery::Failed
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
            // Stamped on the way IN as well as out, so a concurrent pass cannot
            // start a second sample while this one is outstanding.
            *last = Some(now);
        }
        due
    }

    /// Re-stamp the throttle once a sample has finished.
    ///
    /// The gap between samples is then measured from **completion**, not from
    /// the start. Measuring from the start is what turns an expensive sample
    /// into a starvation loop: a sample that takes longer than
    /// `lag_sample_interval` is due again the instant it returns, so the very
    /// next pass samples again, and the runtime spends nearly all of its time
    /// walking the broker rather than consuming. Measuring from completion
    /// guarantees a full interval of message polling between samples however
    /// expensive one turns out to be.
    fn record_lag_sample_completed(&self) {
        *self
            .last_lag_sample
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(std::time::Instant::now());
    }

    /// How long a single lag sample may block the message path.
    ///
    /// Derived from the cadence — a sample that cannot finish within the
    /// interval it is taken at is, by definition, too expensive for that
    /// cadence, and the operator's remedy is the knob they already have —
    /// but floored at [`ADAPTER_LAG_SAMPLE_CEILING`], because a budget shorter
    /// than the adapter's own bound does not make the sample stop sooner. It
    /// only makes the runtime stop *watching*, while the walk runs on and the
    /// next one is armed on top of it.
    ///
    /// The floor raises; it never caps. A cadence already past the ceiling
    /// keeps its own budget, so the default (and every generous cadence) is
    /// unchanged. Together with the completion re-stamp this leaves the worst
    /// case at budget-then-a-full-interval-of-polling, with the added
    /// guarantee that an abandoned sample has finished before its successor
    /// starts.
    fn lag_sample_budget(&self) -> std::time::Duration {
        self.config
            .lag_sample_interval
            .max(ADAPTER_LAG_SAMPLE_CEILING)
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
            hard_stop: Arc::clone(&self.hard_stop),
        }
    }

    /// Map, dispatch and settle one message.
    async fn process(&self, message: InboundMessage) -> MessageDisposition {
        let coordinates = message.coordinates.render();
        let key = message_idempotency_key_in(
            self.binding.name,
            self.binding.target.signal_name(),
            &coordinates,
            self.binding.key_incarnation,
        );

        let outcome = self.map_and_dispatch(&message, &key, &coordinates).await;

        // `poison_threshold(0)` documents the strike counter as *disabled*, and
        // `decide_disposition` never reads the count in that case (it
        // short-circuits on `threshold > 0`). Striking anyway would record an
        // entry that nothing can ever remove: the resulting `Retry` is not a
        // terminal disposition, so neither `clear` nor the terminal-retention
        // sweep covers it, and a sustained stream of distinct rejected
        // messages would grow the tracker without bound — for a counter the
        // operator explicitly turned off.
        let strikes = if self.binding.poison_threshold > 0
            && matches!(outcome, DispatchOutcome::MappingRejected(_))
        {
            self.poison
                .lock()
                .await
                .strike(&key, std::time::Instant::now())
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
        let Settlement {
            disposition: effective,
            dead_letter,
        } = self
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
        // "Terminal" here means *the message will not come back* — which is
        // true of a successful dispatch and, as the two paragraphs below
        // explain, of neither dead-letter handoff.
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
        // One physical poison message must contribute one `poisoned` sample,
        // not one per redrive lap: the retained strikes above make every later
        // redelivery re-nack on sight, so this arm is reached repeatedly for
        // the same message until the broker's own count ends it.
        //
        // The harvest-sink handoff needs the same treatment for a different
        // reason. Its record is durable before the message is acked, and `ack`
        // is deliberately best-effort — a failed commit is warned about, never
        // escalated, precisely so a broker hiccup cannot lose a message. So
        // the broker redelivers something harvest has already quarantined, and
        // the redelivery is the same physical poison message. Marking it
        // terminal stops the second sample. It deliberately does NOT retain
        // the strikes the way the broker-native arm does: there, re-nacking on
        // sight is the mechanism that lets the queue's own count end the
        // message, whereas here the record is already durable and a return
        // means only that the commit failed. So the countdown restarts exactly
        // as the pre-dedupe `clear` made it — only the *counting* was wrong.
        let mut count_poison = true;
        if matches!(effective, MessageDisposition::AbandonToBrokerDeadLetter(_)) {
            count_poison = self
                .poison
                .lock()
                .await
                .mark_terminal_as_of(&key, std::time::Instant::now());
        } else if matches!(effective, MessageDisposition::DeadLetter(_)) {
            let first_here = self
                .poison
                .lock()
                .await
                .mark_sink_terminal_as_of(&key, std::time::Instant::now());
            // The in-process mark cannot survive a restart, and a restart is
            // exactly when the redelivery happens: the process died between
            // writing the record and acking. The sink's own conflict signal is
            // the durable half of the same question, so both must agree before
            // this counts as a new poison message.
            count_poison =
                first_here && !matches!(dead_letter, Some(DeadLetterOutcome::AlreadyRecorded));
        } else if matches!(effective, MessageDisposition::Retry) {
            // A `Retry` reaches here for three different reasons, and only two
            // of them may keep the strikes:
            //
            //   - `MappingRejected` below threshold — the strikes ARE the
            //     counter, so keeping them is the whole mechanism;
            //   - a dead-letter whose sink write failed, downgraded to
            //     `Retry` — keeping them makes the next delivery quarantine on
            //     sight instead of crawling the countdown again;
            //   - a `Transient` dispatch failure — the mapping SUCCEEDED, so
            //     the consecutive-rejection streak is broken and the strikes
            //     are stale.
            //
            // Keeping them for the third case lets a later rejection continue
            // a streak that was never consecutive, dead-lettering the message
            // before its `poison_threshold` is genuinely reached. Gated on the
            // OUTCOME rather than the disposition, because the failed
            // dead-letter write also presents as `Retry` but carries a
            // rejection outcome.
            if matches!(outcome, DispatchOutcome::Transient(_)) {
                self.poison.lock().await.clear(&key);
            }
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

        self.record_metrics(&outcome, &effective, count_poison);
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

    /// `count_poison` is false for a broker-native redrive lap whose poison
    /// message was already counted on its first handoff — see `settle`.
    fn record_metrics(
        &self,
        outcome: &DispatchOutcome,
        disposition: &MessageDisposition,
        count_poison: bool,
    ) {
        let source = self.binding.name;
        // Prefer the finer-grained success outcome (fresh / replay / deferred)
        // over the disposition's coarse "acked".
        let reported = if matches!(disposition, MessageDisposition::Ack) {
            success_outcome(outcome).unwrap_or(ConnectorOutcome::Dispatched)
        } else {
            disposition.outcome()
        };
        // Always: every received message settles exactly once, and this
        // family's documented invariant is that it sums to `received`.
        self.metrics.record_connector_dispatched(source, reported);
        if let Some(reason) = disposition.poison_reason().filter(|_| count_poison) {
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
    ///
    /// [`Settlement::dead_letter`] carries the sink's own verdict on whether
    /// the record already existed — the durable half of the poison-dedupe
    /// question the in-process tracker answers only within one process.
    async fn settle(
        &self,
        message: &InboundMessage,
        coordinates: &str,
        key: &str,
        outcome: &DispatchOutcome,
        disposition: MessageDisposition,
        attempts: u32,
    ) -> Settlement {
        match disposition {
            MessageDisposition::Ack => {
                self.ack(&message.handle).await;
                Settlement::of(MessageDisposition::Ack)
            }
            MessageDisposition::Retry => {
                tracing::warn!(
                    source = self.binding.name,
                    coordinates,
                    outcome = ?outcome,
                    "connector dispatch not durable; leaving message for redelivery"
                );
                self.abandon(&message.handle).await;
                Settlement::of(MessageDisposition::Retry)
            }
            MessageDisposition::DeadLetter(reason) => {
                let entry =
                    self.dead_letter_entry(message, coordinates, key, outcome, reason, attempts);
                match self.sink.write(&entry).await {
                    Ok(recorded) => {
                        tracing::warn!(
                            source = self.binding.name,
                            coordinates,
                            reason = reason.as_str(),
                            already_recorded = recorded == DeadLetterOutcome::AlreadyRecorded,
                            "connector message dead-lettered"
                        );
                        // Ack only after the dead-letter record is durable, so
                        // a sink failure can never silently drop the message.
                        self.ack(&message.handle).await;
                        Settlement {
                            disposition: MessageDisposition::DeadLetter(reason),
                            dead_letter: Some(recorded),
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            source = self.binding.name,
                            coordinates,
                            error = %e,
                            "dead-letter write failed; leaving message for redelivery"
                        );
                        self.abandon(&message.handle).await;
                        Settlement::of(MessageDisposition::Retry)
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
                Settlement::of(MessageDisposition::AbandonToBrokerDeadLetter(reason))
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

    // The held guard below is the entire point of this function, and the lint's
    // suggested remedy — collapse it into the `complete` call so it drops
    // immediately — is precisely the ordering bug the comment there describes.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the guard is deliberately held across the commit submission to keep it monotonic"
    )]
    async fn ack(&self, handle: &MessageHandle) {
        // For a positionally-ordered broker (Kafka), only advance the
        // high-water mark to the contiguous completed prefix — committing past
        // an in-flight lower offset would silently skip it on a crash.
        //
        // The offset actually committed is the tracker's advanced mark, NOT
        // this handle's own position: when offsets 2 then 1 settle in that
        // order, offset 2 is held, and it is 1's completion that advances the
        // prefix to 2. Committing 1's handle there would leave 2 uncommitted
        // and re-read on a crash.
        //
        // So the mark goes through `commit_position`, not `ack`. Cloning this
        // handle and overwriting its position would hand the adapter one
        // message's token paired with another's offset, and `MessageHandle`
        // documents the token as opaque, handed straight back, and the key
        // adapters build their side tables on — so a token-keyed adapter would
        // acknowledge the wrong record. There is often no handle for the mark
        // to borrow anyway: its message settled earlier and was released.
        //
        // The lock is held across the *submission*, not merely across the
        // computation of the mark. Concurrent settlements race otherwise: two
        // dispatches finishing together compute marks 5 then 6 under the lock,
        // then submit in whichever order the runtime happens to schedule them.
        // Submitting 5 after 6 moves the committed offset BACKWARD, and offset
        // 6 — durably settled — is re-read on the next crash. Only a
        // positional broker reaches this branch (an SQS handle carries no
        // partition and skips it entirely), and Kafka's commit is a local
        // enqueue rather than a broker round-trip, so serializing here costs
        // the ordering it buys and nothing more.
        let result = if let (Some(partition), Some(position)) = (handle.partition, handle.position)
        {
            let mut offsets = self.offsets.lock().await;
            let Some(advanced) = offsets.complete(partition, position) else {
                // Earlier offsets are still in flight; this one will be
                // committed as part of a later contiguous advance.
                return;
            };
            self.source.commit_position(partition, advanced).await
        } else {
            self.source.ack(handle).await
        };
        if let Err(e) = result {
            // A failed ack is safe: the message is redelivered and dedupes as
            // an idempotent replay. Never escalate it into a lost message.
            tracing::warn!(
                source = self.binding.name,
                error = %e,
                "connector ack failed; message will be redelivered and deduped"
            );
        }
    }

    /// Hand a hard-stopped message back to its adapter, bounded.
    ///
    /// Mirrors the panic arm's two-step recovery, because the cause is the
    /// same — a dispatch that ended without reaching `settle` — and so is the
    /// remedy. Which step is load-bearing depends on the source: an adapter
    /// whose redelivery *is* an explicit nack needs the `abandon`, while a
    /// positional source needs its head marked so recovery fires on it rather
    /// than waiting for offsets to pile up behind a partition that has stopped.
    ///
    /// **Bounded, unlike the panic arm's.** This runs after the shutdown drain
    /// has already spent its full deadline, so an unbounded `abandon` against
    /// the same wedged broker that caused the timeout would reintroduce exactly
    /// the hang the deadline exists to prevent. Failing to release custody
    /// costs one stranded message; failing to return costs the process. The
    /// budget is deliberately short — this is a courtesy on the way out, not a
    /// second drain.
    ///
    /// # Why the panic arm needs neither the bound nor a permit
    ///
    /// The panic arm runs its recovery in `run_once`'s join loop, which holds
    /// no permit — so [`Self::drain_in_flight`] does not wait for it, and an
    /// ordinary shutdown drops it mid-await along with `run_once`. That would
    /// matter if the call did broker I/O. For both shipped adapters it does
    /// not: Kafka overrides `abandon_redelivers()` to `false` and takes the
    /// mark-the-head branch, a local mutex write; SQS's `abandon` is a
    /// deliberate no-op returning `Ok(())` without touching the network, since
    /// its visibility timeout is what redelivers. Neither can hang the join
    /// loop and neither has custody to strand.
    ///
    /// The residual is a *custom* adapter whose `abandon` both blocks on the
    /// network and is its only redelivery mechanism. There the dropped call
    /// costs one message held until that adapter's own lease expires — the
    /// same cost this function already accepts above, minus the warning.
    /// Closing it properly means catching the dispatch panic *inside* the
    /// spawned task so the recovery runs while its permit is still held (the
    /// reason the hard-stop arm above is inline rather than in the join loop);
    /// re-acquiring a permit in the join loop does not work, because a
    /// cancelled `run` drops `run_once` and its permit before the drain starts.
    async fn release_custody_after_hard_stop(
        &self,
        handle: &MessageHandle,
        positional: Option<(i32, i64)>,
    ) {
        if self.source.abandon_redelivers() {
            if tokio::time::timeout(HARD_STOP_ABANDON_BUDGET, self.abandon(handle))
                .await
                .is_err()
            {
                tracing::warn!(
                    source = self.binding.name,
                    "connector could not hand a hard-stopped message back within its budget; \
                     the adapter may hold it until the process exits"
                );
            }
        } else if let Some((partition, position)) = positional {
            self.offsets.lock().await.retried(partition, position);
        }
        // `record_metrics` lives inside `process`, which this dispatch never
        // finished, so the settlement sample is owed here — same reason as the
        // panic arm. Without it the dispatched series silently under-counts
        // its own `received` total by one per hard stop.
        self.metrics
            .record_connector_dispatched(self.binding.name, ConnectorOutcome::Retried);
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

/// The batch size actually handed to a source: floored at one, and capped by
/// the binding's dispatch capacity.
///
/// A zero batch is a silent killer rather than a loud one: a source asked for
/// zero messages returns an empty batch, the runtime reads that as an idle
/// poll, and the binding consumes nothing forever with no error to alert on.
/// Clamping here — at the runtime's single `receive` call site — covers every
/// source including an embedder's own `EventSource`, not just the two adapters
/// shipped in-tree.
///
/// The `max_in_flight` cap exists because prefetching past dispatch capacity
/// buys nothing and costs something. The surplus cannot start until a permit
/// frees up, so it just sits in memory — but on SQS the visibility timer starts
/// at `ReceiveMessage`, not at dispatch. A message held behind the rest of the
/// batch's dispatch latency can therefore have its visibility expire, be handed
/// to another replica, and have its receive count incremented toward the
/// queue's redrive policy — sending a perfectly processable message to the
/// broker DLQ having never actually failed. The default config
/// (`max_batch: 32`, `max_in_flight: 16`) already prefetched 16 past capacity,
/// so this is the common path rather than an exotic one, and it is worst
/// exactly where the docs send people: `.max_in_flight(1)`, the per-key
/// ordering remedy, against SQS's ten-message batch.
#[must_use]
pub const fn effective_max_batch(configured: usize, max_in_flight: usize) -> usize {
    let configured = if configured == 0 { 1 } else { configured };
    let capacity = if max_in_flight == 0 { 1 } else { max_in_flight };
    if configured < capacity {
        configured
    } else {
        capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::binding::{MappedMessage, SourceBinding};
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

    /// A source with no broker-native dead-letter destination — the Kafka
    /// shape, and the trait's default.
    ///
    /// Only delegates the four required `EventSource` methods, so
    /// `has_native_dead_letter()` falls through to the `false` default rather
    /// than `MockSource`'s permissive `true`.
    #[derive(Debug)]
    struct NoNativeDeadLetterSource(MockSource);

    #[async_trait::async_trait]
    impl EventSource for NoNativeDeadLetterSource {
        fn stream(&self) -> &str {
            self.0.stream()
        }
        async fn receive(
            &self,
            max: usize,
            timeout: std::time::Duration,
        ) -> Result<Vec<crate::connector::message::InboundMessage>, ConnectorError> {
            self.0.receive(max, timeout).await
        }
        async fn ack(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
            self.0.ack(handle).await
        }
        async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
            self.0.abandon(handle).await
        }
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
        ) -> Result<DeadLetterOutcome, ConnectorError> {
            panic!("engine-side dead-letter sink panic");
        }
    }

    /// A sink that parks until released, so a dispatch can be caught mid-flight.
    ///
    /// The hard stop only means anything while a task is genuinely running;
    /// holding a bare permit stands in for that in the drain tests, but proving
    /// the *message* is handed back needs a real dispatch parked inside
    /// `process`.
    #[derive(Debug)]
    struct BlockingDeadLetterSink(Arc<tokio_util::sync::CancellationToken>);

    #[async_trait::async_trait]
    impl crate::connector::dead_letter::DeadLetterSink for BlockingDeadLetterSink {
        async fn write(
            &self,
            _entry: &crate::connector::dead_letter::ConnectorDeadLetter,
        ) -> Result<DeadLetterOutcome, ConnectorError> {
            self.0.cancelled().await;
            Ok(DeadLetterOutcome::Recorded)
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
    async fn a_panicked_dispatch_on_a_redelivering_source_is_abandoned() {
        // Marking no offset is right for a redelivering source, but doing
        // *nothing* is not: "redelivers on abandon" is a statement about what
        // `abandon` does, not a promise that the message comes back on its
        // own. `MockSource` -- an exported, supported adapter -- redelivers
        // exclusively from `abandon`, so a panicked task that never calls it
        // strands the message in `in_flight` where no later pass can see it.
        // Any custom adapter needing an explicit abandon fails the same way.
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
            source.abandoned().len(),
            1,
            "a panicked dispatch must abandon so a redelivering source hands the message back"
        );
        assert_eq!(
            source.pending(),
            1,
            "the abandoned message must be queued again, not stranded in flight"
        );
    }

    #[tokio::test]
    async fn a_panicked_dispatch_records_a_retried_sample() {
        // `received` is incremented before the task is spawned, but a panicked
        // task never reaches `record_metrics`. Both ADR-0001 §7 and
        // `docs/telemetry.md` state that `harvest.connector.dispatched` is the
        // settlement breakdown -- one sample per received message, so the
        // series sums to `harvest.connector.received`. Without a sample here
        // every panic permanently widens that gap, and the retry the runtime
        // performed is invisible on the dashboard that exists to show it.
        let source = Arc::new(MockSource::new("orders"));
        source.push_kafka(0, 7, b"{{{");
        let metrics = Arc::new(RecordingMetrics::default());
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            Arc::clone(&source) as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
            IdempotencyMode::BrokerCoordinates,
        )
        .with_dead_letter_sink(Arc::new(PanickingDeadLetterSink));

        let summary = rt.run_once().await.unwrap();
        assert_eq!(summary.retried, 1);

        assert_eq!(metrics.received().len(), 1);
        assert_eq!(
            metrics.dispatched(),
            vec![("orders".to_string(), ConnectorOutcome::Retried)],
            "a panicked dispatch must still emit its settlement sample"
        );
        // The invariant itself, stated as the test asserts it: the breakdown
        // accounts for every received message.
        assert_eq!(
            metrics.dispatched().len(),
            metrics.received().len(),
            "the dispatched breakdown must sum to received"
        );
    }

    #[test]
    #[should_panic(expected = "broker-native dead-lettering")]
    fn constructing_a_runtime_rejects_broker_native_on_a_source_without_one() {
        // The plugin refuses this pairing at build time, but `ConnectorRuntime`
        // is exported: an embedder driving it directly bypasses that guard, and
        // the disposition then calls a nack that is a no-op, reporting a
        // terminal outcome for a message that reached no dead-letter
        // destination and is never redelivered in-session.
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Rejected("no thanks".to_string())))
                .broker_native_dead_letter(),
        );
        let source = Arc::new(NoNativeDeadLetterSource(MockSource::new("orders")));
        let _ = ConnectorRuntime::new(
            binding,
            source as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        );
    }

    #[test]
    fn constructing_a_runtime_accepts_broker_native_on_a_source_with_one() {
        // The guard must not reject the supported pairing: an adapter that
        // genuinely feeds a broker DLQ (SQS with a redrive policy, which
        // `MockSource` models) is exactly what the mode is for.
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Rejected("no thanks".to_string())))
                .broker_native_dead_letter(),
        );
        let source = Arc::new(MockSource::new("orders"));
        let _ = ConnectorRuntime::new(
            binding,
            source as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        );
    }

    #[test]
    fn constructing_a_runtime_accepts_harvest_sink_on_any_source() {
        // The default mode is unaffected: harvest's own sink needs no broker
        // dead-letter destination at all.
        let source = Arc::new(NoNativeDeadLetterSource(MockSource::new("orders")));
        let _ = ConnectorRuntime::new(
            malformed_binding(),
            source as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
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

        // Same coordinates every time: one message, three deliveries. Lap 1
        // abandons, which this mock genuinely redelivers. Lap 2 nacks toward
        // the broker's redrive -- the real SQS call resets visibility so the
        // queue redelivers, which the mock cannot model without spinning, so
        // lap 3's delivery is re-pushed explicitly.
        source.push_kafka(0, 1, b"{}");
        let first = rt.run_once().await.unwrap();
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
    async fn a_harvest_sink_poison_is_counted_once_when_its_ack_fails() {
        // The harvest-sink terminal path acks after writing the record, and
        // `ack` is deliberately best-effort — a failed commit is logged, not
        // escalated, precisely so a broker hiccup cannot lose a message. The
        // consequence is a redelivery of a message harvest has already
        // quarantined, which must not contribute a SECOND `poisoned` sample:
        // it is the same physical poison message, exactly as in the
        // broker-native redrive case above.
        let source = Arc::new(MockSource::new("orders"));
        let metrics = Arc::new(RecordingMetrics::default());
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime_with_metrics(
            malformed_binding(),
            Arc::clone(&source),
            Arc::clone(&sink),
            Arc::clone(&metrics),
        );

        // A deterministic poison is quarantined on sight, so both deliveries
        // reach the terminal handoff — no strike countdown to hide behind.
        source.push_kafka(0, 1, b"not json");
        rt.run_once().await.unwrap();
        source.push_kafka(0, 1, b"not json");
        rt.run_once().await.unwrap();

        assert_eq!(
            metrics.poisoned().len(),
            1,
            "a redelivery caused by a failed ack is the same physical poison \
             message and must not be counted twice"
        );
        // Unchanged by the dedupe: every received message still settles once.
        assert_eq!(metrics.dispatched().len(), metrics.received().len());
        assert_eq!(metrics.dispatched().len(), 2);

        // A genuinely different poison message still counts.
        source.push_kafka(0, 2, b"not json");
        rt.run_once().await.unwrap();
        assert_eq!(metrics.poisoned().len(), 2);
    }

    #[tokio::test]
    async fn a_poison_already_in_the_dead_letter_table_is_not_recounted_after_a_restart() {
        // The in-process dedupe above cannot survive a restart, and the restart
        // is exactly when a redelivery is most likely: the process died between
        // writing the dead-letter record and acking the broker. The durable
        // table is what still knows, and its insert is already
        // `ON CONFLICT (idempotency_key) DO NOTHING` -- so whether that insert
        // won is the authoritative "is this a new poison message" answer, and
        // it has to reach the metric.
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let metrics = Arc::new(RecordingMetrics::default());

        let source = Arc::new(MockSource::new("orders"));
        let rt = runtime_with_metrics(
            malformed_binding(),
            Arc::clone(&source),
            Arc::clone(&sink),
            Arc::clone(&metrics),
        );
        source.push_kafka(0, 1, b"not json");
        rt.run_once().await.unwrap();
        assert_eq!(metrics.poisoned().len(), 1, "the first quarantine counts");

        // Restart: a brand-new runtime -- so an EMPTY poison tracker -- over the
        // same durable sink, and the broker hands the message back.
        let restarted_source = Arc::new(MockSource::new("orders"));
        let restarted = runtime_with_metrics(
            malformed_binding(),
            Arc::clone(&restarted_source),
            Arc::clone(&sink),
            Arc::clone(&metrics),
        );
        restarted_source.push_kafka(0, 1, b"not json");
        restarted.run_once().await.unwrap();

        assert_eq!(
            metrics.poisoned().len(),
            1,
            "the durable record already existed, so this is the same physical poison message; \
             counting it again inflates poison alerts by one per restart-redelivery"
        );
        assert_eq!(
            sink.entries().len(),
            1,
            "and dead-lettering stays idempotent -- one record per poison message"
        );

        // A genuinely different message is still a new poison message.
        restarted_source.push_kafka(0, 2, b"not json");
        restarted.run_once().await.unwrap();
        assert_eq!(metrics.poisoned().len(), 2);
    }

    #[tokio::test]
    async fn one_poison_message_is_counted_once_across_redrive_laps() {
        // `harvest.connector.poisoned` counts poison MESSAGES. Because the
        // retained strikes make every redelivery re-nack on sight, the same
        // physical message reaches the broker-native handoff once per redrive
        // lap -- and with SQS's `maxReceiveCount` above the binding threshold
        // (the normal configuration) that is several laps. Counting each one
        // would inflate poison alerts and dashboards by the lap count.
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Rejected("no thanks".to_string())))
                .poison_threshold(2)
                .broker_native_dead_letter(),
        );
        let source = Arc::new(MockSource::new("orders"));
        let metrics = Arc::new(RecordingMetrics::default());
        let rt = runtime_with_metrics(
            binding,
            Arc::clone(&source),
            Arc::new(RecordingDeadLetterSink::new()),
            Arc::clone(&metrics),
        );

        // One message, three deliveries: strike 1 retries, strike 2 hands it
        // to the broker, and lap 3 re-nacks the SAME message on sight.
        source.push_kafka(0, 1, b"{}");
        rt.run_once().await.unwrap();
        rt.run_once().await.unwrap();
        source.push_kafka(0, 1, b"{}");
        rt.run_once().await.unwrap();

        assert_eq!(
            metrics.poisoned().len(),
            1,
            "one physical poison message must contribute exactly one sample, \
             not one per redrive lap"
        );
        // `dispatched` is deliberately NOT deduped: every redelivery is a
        // received message, and that family's documented invariant is that it
        // sums to `received`.
        assert_eq!(
            metrics.dispatched().len(),
            metrics.received().len(),
            "the settlement invariant must survive the poison dedupe"
        );
        assert_eq!(metrics.dispatched().len(), 3);

        // A DIFFERENT poison message is a different message and does count.
        source.push_kafka(0, 2, b"{}");
        rt.run_once().await.unwrap();
        source.push_kafka(0, 2, b"{}");
        rt.run_once().await.unwrap();
        assert_eq!(metrics.poisoned().len(), 2);
    }

    #[tokio::test]
    async fn a_successful_mapping_breaks_the_consecutive_rejection_streak() {
        // The strike counter is documented as CONSECUTIVE rejections, and
        // `MappingRejected` is explicitly the "possibly transient" refusal --
        // a mapper waiting on a dependency that has not arrived yet. So a
        // delivery that maps successfully broke the streak, even when its
        // dispatch then fails transiently and the message is retried.
        //
        // Retaining strikes across it conflates three different reasons for
        // `Retry`: a rejection below threshold (must retain -- that IS the
        // counter), a failed dead-letter write (must retain, so the next
        // delivery quarantines on sight), and a transient dispatch failure
        // (must clear). The message would then dead-letter a delivery early,
        // on a streak that was never consecutive.
        let attempt = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = Arc::clone(&attempt);
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(move |_ctx| {
                    // reject, map-ok (dispatch then fails transiently, since
                    // no runtime is installed), reject.
                    let n = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if n == 1 {
                        Ok(MappedMessage::new("wf-1", serde_json::json!({})))
                    } else {
                        Err(MappingError::Rejected("not yet".to_string()))
                    }
                })
                .poison_threshold(2),
        );
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime_with_metrics(
            binding,
            Arc::clone(&source),
            Arc::clone(&sink),
            Arc::new(RecordingMetrics::default()),
        );

        // One physical message, three deliveries: the mock redelivers on
        // abandon, so each `Retry` hands it straight back.
        source.push_kafka(0, 1, b"{}");
        for _ in 0..3 {
            rt.run_once().await.unwrap();
        }

        assert_eq!(
            attempt.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "the fixture must actually reach the third delivery"
        );
        assert!(
            sink.entries().is_empty(),
            "the successful mapping broke the streak, so the third delivery is \
             strike 1 -- below the threshold of 2 -- and must not dead-letter"
        );
    }

    #[tokio::test]
    async fn a_deterministic_poison_is_counted_on_its_first_broker_native_handoff() {
        // The other side of the dedupe. `Malformed` and `TargetRejected` are
        // dead-lettered on sight and are never strike-counted, so the tracker
        // holds nothing for the key when the handoff happens. If a missing
        // entry read as "already counted", `harvest.connector.poisoned` would
        // sit at zero for a malformed-payload flood -- the exact alert this
        // metric exists to raise.
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Deserialize("not json".to_string())))
                .poison_threshold(2)
                .broker_native_dead_letter(),
        );
        let source = Arc::new(MockSource::new("orders"));
        let metrics = Arc::new(RecordingMetrics::default());
        let rt = runtime_with_metrics(
            binding,
            Arc::clone(&source),
            Arc::new(RecordingDeadLetterSink::new()),
            Arc::clone(&metrics),
        );

        // Quarantined on the FIRST delivery -- no strike countdown at all.
        source.push_kafka(0, 1, b"not json");
        rt.run_once().await.unwrap();
        assert_eq!(
            metrics.poisoned(),
            vec![("orders".to_string(), PoisonReason::Malformed)],
            "a deterministic poison must be counted on its first handoff"
        );

        // ...and the redrive lap that follows is still the same message.
        source.push_kafka(0, 1, b"not json");
        rt.run_once().await.unwrap();
        assert_eq!(
            metrics.poisoned().len(),
            1,
            "a redrive lap must not report a second poison message"
        );

        // A DIFFERENT malformed message is a different poison message.
        source.push_kafka(0, 2, b"also not json");
        rt.run_once().await.unwrap();
        assert_eq!(metrics.poisoned().len(), 2);

        // The settlement invariant is untouched by either gate.
        assert_eq!(metrics.dispatched().len(), metrics.received().len());
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
        source.push_kafka(0, 7, b"payload");
        for _ in 0..2 {
            rt.run_once().await.unwrap();
        }
        assert!(sink.entries().is_empty());
        assert_eq!(source.abandoned().len(), 2);

        // The sink recovers; the next redelivery must quarantine immediately
        // (strikes were 2 already), not need two more attempts. The message is
        // already back on the queue -- the failed write downgraded to Retry,
        // which abandons, and this mock redelivers.
        sink.fail_writes(false);
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

        // Pushed once: the mock genuinely redelivers an abandoned message
        // (its `abandon_redelivers()` is true), so the redelivery is the real
        // adapter behaviour rather than a hand-simulated re-push.
        source.push_kafka(0, 7, b"payload");
        for _ in 0..3 {
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

        source.push_kafka(0, 7, b"payload");
        for _ in 0..5 {
            rt.run_once().await.unwrap();
        }

        assert!(sink.entries().is_empty());
        assert!(source.acked().is_empty());
        assert_eq!(source.abandoned().len(), 5, "redelivered on every lap");
        assert_eq!(
            rt.poison.lock().await.tracked(),
            0,
            "and a disabled counter must record nothing: the Retry disposition \
             never clears, so striking here would leak an entry per message"
        );
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

    /// A dispatch that never returns must not hang the whole application.
    ///
    /// Cancelling `run` drops `run_once` mid-flight, which **detaches** its
    /// spawned dispatches — a dropped `JoinHandle` does not abort the task —
    /// and each one still holds an owned permit. If that task is wedged in a
    /// database call, a broker ack or a custom sink, its permit is never
    /// released. `stop_harvest_runtime` awaits every connector *before* it
    /// stops the runner, so an unbounded drain here is not a slow shutdown:
    /// it is a permanently hung one, outside the reach of the engine's own
    /// `shutdown_timeout`.
    #[tokio::test(start_paused = true)]
    async fn a_wedged_dispatch_cannot_hang_shutdown_forever() {
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let mut rt = runtime(malformed_binding(), source, sink);
        rt.config.shutdown_timeout = std::time::Duration::from_secs(30);

        // Stand in for the wedged task: hold a permit and never give it back.
        let wedged = Arc::clone(&rt.permits)
            .acquire_owned()
            .await
            .expect("permit");

        let started = tokio::time::Instant::now();
        rt.drain_in_flight().await;
        let waited = tokio::time::Instant::now() - started;

        assert!(
            waited >= rt.config.shutdown_timeout,
            "the drain must give in-flight work its full deadline"
        );
        assert!(
            rt.hard_stop.is_cancelled(),
            "a drain that timed out must hard-stop the stragglers, or a detached \
             task wedged forever leaks in a process that outlives harvest"
        );
        drop(wedged);
    }

    /// Giving up on a dispatch must hand the message back to the adapter.
    ///
    /// The hard stop cuts `process` short, so `settle` never runs — and
    /// `settle` is the only thing that releases the adapter's custody. On a
    /// source whose redelivery *is* an explicit `abandon` (`MockSource`, SQS,
    /// any adapter needing a real nack) the message is otherwise stranded in
    /// the adapter with nothing left to hand it back: the process is shutting
    /// down, so there is no later pass, and reusing the source across a
    /// restart keeps the leak.
    ///
    /// Returning `Retry` is not enough. That is a value handed to the join
    /// loop, and during a real shutdown the join loop has already been dropped
    /// with `run_once` — nobody is left to read it. The recovery has to happen
    /// *inside* the spawned task, which is the same conclusion the panic arm
    /// reached for the same reason.
    #[tokio::test(start_paused = true)]
    async fn a_hard_stopped_dispatch_hands_the_message_back() {
        let source = Arc::new(MockSource::new("orders"));
        source.push_opaque("m1", b"not json");
        // Park the dispatch inside the sink so the task is genuinely in flight
        // when the hard stop fires, rather than asserting against a stand-in.
        let blocker = Arc::new(tokio_util::sync::CancellationToken::new());
        let rt = Arc::new(
            runtime(
                malformed_binding(),
                Arc::clone(&source),
                Arc::new(RecordingDeadLetterSink::new()),
            )
            .with_dead_letter_sink(Arc::new(BlockingDeadLetterSink(Arc::clone(&blocker)))),
        );

        let driver = tokio::spawn({
            let rt = Arc::clone(&rt);
            async move { rt.run_once().await }
        });

        // Let the pass reach the sink.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        rt.hard_stop.cancel();

        let summary = driver.await.expect("driver").expect("pass");
        assert_eq!(
            summary.retried, 1,
            "the message must be left for redelivery"
        );

        assert_eq!(
            source.abandoned().len(),
            1,
            "a hard-stopped dispatch must abandon the message on a source whose \
             redelivery depends on it, or the adapter holds it forever"
        );
        assert!(
            source.acked().is_empty(),
            "giving up must never acknowledge"
        );
        blocker.cancel();
    }

    /// The binding's incarnation must actually reach the derived key.
    ///
    /// A rotation knob nothing threads is worse than none: the operator
    /// believes the cutover is namespaced and the silent replay-classification
    /// happens anyway. Asserted end-to-end through a pass, using the key the
    /// dead-letter record carries for exactly this correlation purpose.
    #[tokio::test]
    async fn the_binding_incarnation_reaches_the_derived_key() {
        let source = Arc::new(MockSource::new("orders"));
        source.push_opaque("m1", b"not json");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Deserialize("not json".to_string())))
                .key_incarnation("2026-08-cutover"),
        );
        let rt = runtime(binding, Arc::clone(&source), Arc::clone(&sink));

        rt.run_once().await.expect("pass");

        let entries = sink.entries();
        assert_eq!(entries.len(), 1, "the malformed message must dead-letter");
        assert_eq!(
            entries[0].idempotency_key,
            message_idempotency_key_in("orders", None, "orders:m1", Some("2026-08-cutover"),),
            "the key must be derived within the binding's incarnation, or a \
             cutover keeps colliding with the claims it meant to leave behind"
        );
    }

    /// Giving up must not outrun the handing-back it triggers.
    ///
    /// Cancelling `hard_stop` only *wakes* the stragglers. The recovery that
    /// actually returns each message runs inside a task this shutdown has
    /// already detached, so returning the instant the token is cancelled lets
    /// `stop_harvest_runtime` observe this connector as finished and the
    /// process exit before any `abandon` reaches the broker — trading a
    /// leaked task for a leaked message, which is the very thing the hard
    /// stop exists to prevent.
    #[tokio::test(start_paused = true)]
    async fn the_drain_waits_for_the_stragglers_it_hard_stopped() {
        let source = Arc::new(MockSource::new("orders"));
        source.push_opaque("m1", b"not json");
        let blocker = Arc::new(tokio_util::sync::CancellationToken::new());
        let mut rt = runtime(
            malformed_binding(),
            Arc::clone(&source),
            Arc::new(RecordingDeadLetterSink::new()),
        )
        .with_dead_letter_sink(Arc::new(BlockingDeadLetterSink(Arc::clone(&blocker))));
        rt.config.shutdown_timeout = std::time::Duration::from_secs(30);
        let rt = Arc::new(rt);

        let driver = tokio::spawn({
            let rt = Arc::clone(&rt);
            async move { rt.run_once().await }
        });

        // Let the pass reach the sink, then detach it exactly as cancelling
        // `run` does: dropping `run_once` drops the join loop, and a dropped
        // `JoinHandle` does not abort the dispatch it was holding. Awaiting
        // the aborted handle is what makes the detached state deterministic.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        driver.abort();
        assert!(driver.await.is_err(), "the pass must have been cancelled");

        rt.drain_in_flight().await;

        assert!(
            rt.hard_stop.is_cancelled(),
            "the straggler held its permit, so the drain must have given up"
        );
        assert_eq!(
            source.abandoned().len(),
            1,
            "the drain must not return until the dispatches it hard-stopped have \
             handed their messages back, or the process can exit first and the \
             adapter keeps custody"
        );
        blocker.cancel();
    }

    /// The normal path is unchanged: a clean drain never hard-stops anything.
    ///
    /// The hard stop is a last resort — it abandons a dispatch at its next
    /// await point, which costs a redelivery. Firing it on an ordinary
    /// shutdown would turn every clean stop into needless duplicate work.
    #[tokio::test(start_paused = true)]
    async fn a_clean_drain_never_hard_stops() {
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), source, sink);

        rt.drain_in_flight().await;

        assert!(
            !rt.hard_stop.is_cancelled(),
            "nothing was in flight, so nothing should have been abandoned"
        );
    }

    #[tokio::test]
    async fn a_hard_stopped_runtime_refuses_to_run_again() {
        // `hard_stop` is terminal, and it is deliberately never reset: it is
        // an `Arc` shared with every dispatch this runtime has already handed
        // out, so clearing it would un-cancel stragglers still unwinding.
        // Running again on top of it would consume messages and hand every one
        // straight back at its first await point — a hot receive-and-abandon
        // loop that reports itself as a healthy consumer while making no
        // progress at all, and burns a billed API's request budget doing it.
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), Arc::clone(&source), sink);
        source.push_kafka(0, 1, b"{}");

        rt.hard_stop.cancel();

        // An *uncancelled* token: nothing but the guard can end this.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            rt.run(CancellationToken::new()),
        )
        .await
        .expect("a hard-stopped runtime must refuse to start, not spin");

        assert!(
            source.acked().is_empty() && source.abandoned().is_empty(),
            "refusing means consuming nothing at all, not consuming and \
             abandoning in a loop"
        );
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
    async fn a_positional_commit_never_pairs_one_messages_token_with_anothers_position() {
        // `MessageHandle` documents the token as opaque and handed *straight
        // back*, and tells adapters to key their own side table off it. But the
        // contiguous prefix commits a MARK, not a message: when 5 settles
        // before 4, it is 4's completion that advances the prefix to 5, and
        // 4's handle is the only one in hand. Cloning it and overwriting the
        // position hands the adapter token-of-4 paired with position-5 -- a
        // token-keyed adapter then acknowledges the wrong record, or rejects
        // every commit.
        //
        // A positional commit is not an ack, so it must not arrive dressed as
        // one. The empty token is the signal: no message is being acknowledged.
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), Arc::clone(&source), Arc::clone(&sink));

        rt.offsets.lock().await.observe(0, 4);
        rt.offsets.lock().await.observe(0, 5);
        rt.ack(&MessageHandle::positioned("orders:0:5", 0, 5)).await;
        rt.ack(&MessageHandle::positioned("orders:0:4", 0, 4)).await;

        for handle in source.acked() {
            assert!(
                handle.token.is_empty(),
                "a positional commit must not carry a message's token: {handle:?}"
            );
        }
    }

    /// A token-keyed positional adapter: it overrides `commit_position` and
    /// refuses to acknowledge a positional handle, because for it a commit is
    /// never a message ack.
    #[derive(Debug)]
    struct PositionalCommitSource {
        inner: MockSource,
        commits: std::sync::Mutex<Vec<(i32, i64)>>,
    }

    #[async_trait::async_trait]
    impl EventSource for PositionalCommitSource {
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
            assert!(
                handle.partition.is_none(),
                "a positional commit must not arrive as a message ack: {handle:?}"
            );
            self.inner.ack(handle).await
        }
        async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
            self.inner.abandon(handle).await
        }
        async fn commit_position(
            &self,
            partition: i32,
            position: i64,
        ) -> Result<(), ConnectorError> {
            self.commits
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((partition, position));
            Ok(())
        }
    }

    #[tokio::test]
    async fn an_adapter_overriding_commit_position_receives_bare_coordinates() {
        // The affirmative half: an adapter whose commit genuinely cannot be
        // expressed as an ack overrides the default and gets the mark as plain
        // coordinates — never a handle borrowed from whichever message happened
        // to trigger the advance.
        let source = Arc::new(PositionalCommitSource {
            inner: MockSource::new("orders"),
            commits: std::sync::Mutex::new(Vec::new()),
        });
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            Arc::clone(&source) as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        );

        rt.offsets.lock().await.observe(0, 4);
        rt.offsets.lock().await.observe(0, 5);
        rt.ack(&MessageHandle::positioned("orders:0:5", 0, 5)).await;
        rt.ack(&MessageHandle::positioned("orders:0:4", 0, 4)).await;

        let commits = source
            .commits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(
            commits,
            vec![(0, 5)],
            "one commit, at the offset the contiguous prefix reached"
        );
    }

    /// A source whose commit submission is slow for one chosen offset.
    ///
    /// Stands in for the real asymmetry between two concurrent commits: they
    /// do not reach the adapter in the order their marks were computed unless
    /// something makes them.
    #[derive(Debug)]
    struct SlowAckSource {
        inner: MockSource,
        slow_position: i64,
        delay: std::time::Duration,
    }

    #[async_trait::async_trait]
    impl EventSource for SlowAckSource {
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
            if handle.position == Some(self.slow_position) {
                tokio::time::sleep(self.delay).await;
            }
            self.inner.ack(handle).await
        }
        async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
            self.inner.abandon(handle).await
        }
    }

    #[tokio::test]
    async fn concurrent_acks_never_submit_a_commit_out_of_order() {
        // Computing the marks under the lock is not enough: released before
        // the submission, two settlements that finish together can compute 0
        // then 1 and *submit* 1 then 0, moving the committed offset backward
        // and re-reading a durably settled message on the next crash.
        let source = Arc::new(SlowAckSource {
            inner: MockSource::new("orders"),
            slow_position: 0,
            delay: std::time::Duration::from_millis(200),
        });
        let rt = Arc::new(
            ConnectorRuntime::new(
                malformed_binding(),
                Arc::clone(&source) as Arc<dyn EventSource>,
                HarvestApiState::new(),
                Arc::new(NoOpMetrics),
                IdempotencyMode::BrokerCoordinates,
            )
            .with_dead_letter_sink(Arc::new(RecordingDeadLetterSink::new())),
        );

        rt.offsets.lock().await.observe(0, 0);
        rt.offsets.lock().await.observe(0, 1);

        // Offset 0 settles first and computes the lower mark, but its
        // submission is the slow one.
        let first = tokio::spawn({
            let rt = Arc::clone(&rt);
            async move { rt.ack(&MessageHandle::positioned("orders:0:0", 0, 0)).await }
        });
        // Long enough for `first` to be inside its submission, short enough to
        // be well clear of it finishing.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let second = tokio::spawn({
            let rt = Arc::clone(&rt);
            async move { rt.ack(&MessageHandle::positioned("orders:0:1", 0, 1)).await }
        });

        first.await.expect("first ack task");
        second.await.expect("second ack task");

        let positions: Vec<Option<i64>> = source.inner.acked().iter().map(|h| h.position).collect();
        assert_eq!(
            positions,
            vec![Some(0), Some(1)],
            "commits must reach the adapter in mark order; submitting the lower \
             mark after the higher one rewinds the committed offset"
        );
    }

    #[tokio::test]
    async fn a_positional_commit_is_uniform_whether_or_not_the_mark_moved() {
        // The common case: offsets settle in order, so the advanced mark IS the
        // handle's own position. It is still a high-water mark rather than an
        // acknowledgement of that message, and it must arrive looking like one.
        //
        // Passing the real token through *here* while withholding it whenever
        // the mark ran ahead would be worse than either alternative: the
        // adapter cannot tell the two cases apart, so it would have to treat a
        // present token as untrustworthy anyway — and the one time it forgot,
        // it would acknowledge the wrong record.
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), Arc::clone(&source), sink);

        rt.offsets.lock().await.observe(0, 7);
        rt.ack(&MessageHandle::positioned("orders:0:7", 0, 7)).await;

        assert_eq!(source.acked().len(), 1);
        assert_eq!(
            source.acked()[0].position,
            Some(7),
            "the mark is the offset the prefix reached"
        );
        assert!(
            source.acked()[0].token.is_empty(),
            "a positional commit is a mark, not a message ack — even when the \
             two coincide: {:?}",
            source.acked()[0]
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

        // A pass now pulls at most `max_in_flight` (see `effective_max_batch`),
        // so draining twelve messages under `.max_in_flight(1)` takes twelve
        // passes rather than one. Order across the whole drain is still what
        // this asserts -- it is now guaranteed by two mechanisms rather than
        // one: the permit, and the prefetch cap that stops the surplus being
        // pulled at all.
        let mut received = 0;
        while received < 12 {
            received += rt.run_once().await.unwrap().received;
        }
        assert_eq!(received, 12);

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

        // `effective_max_batch` caps a pass at `LIMIT`, so the backlog drains
        // over several passes. Each pass still dispatches its whole batch
        // concurrently, so the peak-concurrency assertion below keeps its teeth.
        let mut received = 0;
        while received < 32 {
            received += rt.run_once().await.unwrap().received;
        }
        assert_eq!(received, 32, "the whole backlog is drained");

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

    /// Records the order in which the runtime calls the source, so a test can
    /// assert *when* telemetry runs relative to message custody.
    #[derive(Debug)]
    struct CallOrderSource {
        inner: MockSource,
        calls: std::sync::Mutex<Vec<&'static str>>,
        /// When set, `lag()` never resolves — a broker round-trip that has
        /// stopped responding rather than merely being slow.
        lag_stalls: std::sync::atomic::AtomicBool,
    }

    impl CallOrderSource {
        fn new(stream: &str) -> Self {
            Self {
                inner: MockSource::new(stream),
                calls: std::sync::Mutex::new(Vec::new()),
                lag_stalls: std::sync::atomic::AtomicBool::new(false),
            }
        }
        fn stall_lag(&self) {
            self.lag_stalls
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        fn note(&self, what: &'static str) {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(what);
        }
        fn calls(&self) -> Vec<&'static str> {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl EventSource for CallOrderSource {
        fn stream(&self) -> &str {
            self.inner.stream()
        }
        async fn receive(
            &self,
            max: usize,
            timeout: std::time::Duration,
        ) -> Result<Vec<InboundMessage>, ConnectorError> {
            self.note("receive");
            self.inner.receive(max, timeout).await
        }
        async fn ack(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
            self.note("ack");
            self.inner.ack(handle).await
        }
        async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
            self.note("abandon");
            self.inner.abandon(handle).await
        }
        async fn lag(&self) -> Option<i64> {
            self.note("lag");
            if self.lag_stalls.load(std::sync::atomic::Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            self.inner.lag().await
        }
    }

    /// A sample that outlasts its own cadence must not re-arm immediately.
    ///
    /// The throttle stamps the sample instant, and stamping it on the way IN
    /// means the gap is measured from when the sample STARTED. A sample that
    /// takes longer than `lag_sample_interval` is therefore due again the
    /// instant it finishes, on the very next pass — so the runtime spends
    /// every pass sampling and only a sliver of each consuming. Measuring the
    /// gap from COMPLETION guarantees a full interval of message polling
    /// between samples no matter how expensive one turns out to be.
    /// A short cadence must not shorten the budget below what an adapter can
    /// actually abort within, or abandoned samples pile up.
    ///
    /// The runtime's timeout only drops its own future. It cannot stop the
    /// work: a Kafka sample runs on `spawn_blocking`, and a blocking task is
    /// never cancelled — only the adapter's own internal bound stops it. So
    /// when the runtime gives up FIRST, the walk keeps running while the next
    /// one is armed, and at a sub-ceiling cadence several overlap — each
    /// holding a blocking-pool thread and hammering the very broker whose
    /// slowness caused the timeout.
    ///
    /// Flooring the budget at the ceiling every adapter honours makes overlap
    /// impossible for ANY cadence: the next sample is armed a full interval
    /// after the budget elapses, and the walk has already stopped by then.
    /// Giving up earlier bought nothing anyway — the gauge is stale either
    /// way, and the work continues regardless.
    #[test]
    fn a_short_cadence_never_shortens_the_budget_below_the_adapter_ceiling() {
        let short = ConnectorRuntime::new(
            malformed_binding(),
            Arc::new(MockSource::new("orders")),
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        )
        .with_config(ConnectorRuntimeConfig {
            lag_sample_interval: std::time::Duration::from_secs(3),
            ..ConnectorRuntimeConfig::default()
        });

        assert_eq!(
            short.lag_sample_budget(),
            ADAPTER_LAG_SAMPLE_CEILING,
            "a 3s cadence must not abandon at 3s while the adapter's own walk \
             runs on to its ceiling — that is what lets abandoned samples overlap"
        );

        // A cadence already past the ceiling keeps its own budget: the floor
        // raises, it never caps.
        let long = ConnectorRuntime::new(
            malformed_binding(),
            Arc::new(MockSource::new("orders")),
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        )
        .with_config(ConnectorRuntimeConfig {
            lag_sample_interval: std::time::Duration::from_secs(60),
            ..ConnectorRuntimeConfig::default()
        });
        assert_eq!(long.lag_sample_budget(), std::time::Duration::from_secs(60));
    }

    #[tokio::test]
    async fn a_slow_lag_sample_does_not_immediately_re_arm_the_throttle() {
        let config = ConnectorRuntimeConfig {
            lag_sample_interval: std::time::Duration::from_millis(40),
            ..ConnectorRuntimeConfig::default()
        };
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            Arc::new(MockSource::new("orders")) as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::new(RecordingMetrics::default()) as Arc<dyn MetricsRecorder>,
            IdempotencyMode::BrokerCoordinates,
        )
        .with_config(config);

        assert!(rt.lag_sample_is_due(), "the first sample is always due");
        // A sample that itself outlasts the interval.
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        rt.record_lag_sample_completed();
        assert!(
            !rt.lag_sample_is_due(),
            "the gap must be measured from completion, or an expensive sample              re-arms itself on every pass and starves message polling",
        );
    }

    /// An unbounded lag sample must never block the message path.
    ///
    /// `lag()` is sampled before `receive` (so no message is ever in custody
    /// across a billed broker round-trip), which means an unbounded sample
    /// blocks the pass outright: the connector stops consuming entirely while
    /// it waits. That is worst precisely when it matters most — a degraded
    /// broker makes the sample slow AND makes the backlog grow. The sample is
    /// therefore budgeted, and an over-budget one is abandoned so the pass
    /// still polls.
    #[tokio::test(start_paused = true)]
    async fn an_over_budget_lag_sample_is_abandoned_so_the_pass_still_polls() {
        let source = Arc::new(CallOrderSource::new("orders"));
        source.inner.set_lag(Some(42));
        source.stall_lag();
        source.inner.push_kafka(0, 1, b"{{{");
        let metrics = Arc::new(RecordingMetrics::default());
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            Arc::clone(&source) as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
            IdempotencyMode::BrokerCoordinates,
        )
        .with_dead_letter_sink(Arc::new(RecordingDeadLetterSink::new()))
        .with_config(ConnectorRuntimeConfig {
            lag_sample_interval: std::time::Duration::from_millis(30),
            ..ConnectorRuntimeConfig::default()
        });

        // Paused clock: the budget is floored at `ADAPTER_LAG_SAMPLE_CEILING`
        // (a short cadence must not abandon before the adapter's own walk can
        // stop), so waiting it out for real would put ten seconds on every
        // run of this suite. The guard sits above that floor so it catches an
        // UNBOUNDED sample — which parks forever and so never lets the clock
        // reach the guard's own deadline either — rather than the budget
        // simply doing its job.
        tokio::time::timeout(std::time::Duration::from_secs(60), rt.run_once())
            .await
            .expect("an unbounded lag sample must not block the pass")
            .expect("the pass itself must succeed");

        let calls = source.calls();
        assert!(
            calls.contains(&"receive"),
            "the pass must still poll for messages, got {calls:?}",
        );
        assert!(
            metrics
                .lag
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "an abandoned sample must record nothing rather than a stale value",
        );
    }

    /// Telemetry must never sit between `receive` and dispatch.
    ///
    /// `lag()` is a live broker round-trip — SQS bills a `GetQueueAttributes`,
    /// Kafka does a metadata + watermark fetch — and either can be slow or
    /// retry internally. On SQS the visibility timer starts at
    /// `ReceiveMessage`, not at dispatch, so a lag call made while a batch is
    /// already in hand spends that batch's visibility budget: another replica
    /// receives the messages, their `ApproximateReceiveCount` climbs toward the
    /// queue's `maxReceiveCount`, and a perfectly processable message reaches
    /// the broker DLQ having never once failed.
    ///
    /// Sampling before `receive` costs nothing — the gauge is throttled to a
    /// scrape cadence, so which side of the receive it lands on is immaterial
    /// to its accuracy — and it means no message is ever in custody while the
    /// call is outstanding.
    #[tokio::test]
    async fn lag_is_sampled_before_any_message_is_in_custody() {
        let source = Arc::new(CallOrderSource::new("orders"));
        source.inner.set_lag(Some(42));
        source.inner.push_kafka(0, 1, b"{{{");
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let metrics = Arc::new(RecordingMetrics::default());
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            Arc::clone(&source) as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
            IdempotencyMode::BrokerCoordinates,
        )
        .with_dead_letter_sink(sink);

        rt.run_once().await.unwrap();

        let calls = source.calls();
        let lag_at = calls.iter().position(|c| *c == "lag");
        let receive_at = calls.iter().position(|c| *c == "receive");
        assert!(
            lag_at.is_some() && receive_at.is_some(),
            "both calls must happen: {calls:?}",
        );
        assert!(
            lag_at < receive_at,
            "lag() must be sampled BEFORE receive() so no message is held \
             across a billed broker round-trip, got {calls:?}",
        );
        // The sample still lands -- moving it must not silence the gauge.
        assert_eq!(metrics.lag(), vec![("orders".to_string(), 42)]);
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
        assert_eq!(
            effective_max_batch(0, 16),
            1,
            "zero must never reach a source"
        );
        assert_eq!(effective_max_batch(1, 16), 1);
        assert_eq!(effective_max_batch(32, 64), 32);
    }

    /// Never pull more than the binding can dispatch concurrently.
    ///
    /// A prefetched surplus buys nothing: it cannot start until a permit frees
    /// up, so it just sits. On SQS it is actively harmful -- the visibility
    /// timer runs from `ReceiveMessage`, so a message waiting behind the whole
    /// batch's dispatch latency can time out, be handed to another replica, and
    /// have its receive count incremented toward the queue's redrive policy.
    /// A perfectly good message reaches the broker DLQ having never failed.
    #[test]
    fn the_receive_size_never_exceeds_dispatch_capacity() {
        assert_eq!(
            effective_max_batch(32, 16),
            16,
            "the DEFAULT config prefetches 16 past capacity, so this is not an exotic case"
        );
        assert_eq!(
            effective_max_batch(10, 1),
            1,
            "the documented per-key ordering remedy against a full SQS batch"
        );
        assert_eq!(
            effective_max_batch(8, 32),
            8,
            "a batch smaller than capacity is still honoured"
        );
        assert_eq!(
            effective_max_batch(32, 0),
            1,
            "a degenerate capacity still pulls one, never zero"
        );
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

    /// A source whose rebuild fails transiently — the broker is unreachable
    /// *right now*, not incapable of ever rebuilding.
    #[derive(Debug)]
    struct RebuildFailsSource {
        inner: MockSource,
        attempts: std::sync::atomic::AtomicUsize,
    }

    impl RebuildFailsSource {
        fn new(stream: &str) -> Self {
            Self {
                inner: MockSource::new(stream),
                attempts: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn attempts(&self) -> usize {
            self.attempts.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl EventSource for RebuildFailsSource {
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
            self.attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(ConnectorError::Broker("broker unreachable".to_string()))
        }
    }

    #[tokio::test]
    async fn a_transient_rebuild_failure_is_retryable_not_terminal() {
        // `Ok(false)` means "this source can NEVER rebuild itself" -- the only
        // condition that justifies stopping an unsupervised binding for the
        // life of the process. A rebuild that fails because the broker is
        // momentarily unreachable is the opposite: retrying is exactly what
        // recovers it. Collapsing both onto `false` stops the binding on a
        // blip, and consumption never resumes even after the broker comes
        // back.
        let source = Arc::new(RebuildFailsSource::new("orders"));
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            Arc::clone(&source) as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        );

        assert_eq!(
            rt.recover_from_stall(0).await,
            StallRecovery::Failed,
            "a transient rebuild failure must be distinguishable from a source \
             that cannot rebuild at all"
        );
        assert_eq!(source.attempts(), 1);

        // ...and the default trait impl, which genuinely cannot rebuild, is
        // the one that terminates the binding.
        let rt2 = ConnectorRuntime::new(
            malformed_binding(),
            Arc::new(MockSource::new("orders")) as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        );
        assert_eq!(rt2.recover_from_stall(0).await, StallRecovery::Unsupported);
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

    /// A source that wedges and whose rebuild never returns.
    ///
    /// The realistic shape: a Kafka consumer rebuild waiting on a broker that
    /// is unreachable — which is *the usual state* when a stall happens, since
    /// the outage that blocked the prefix is generally the outage that blocks
    /// the rebuild.
    #[derive(Debug)]
    struct RebuildHangsSource {
        inner: MockSource,
        next_offset: std::sync::atomic::AtomicI64,
    }

    impl RebuildHangsSource {
        fn new(stream: &str) -> Self {
            Self {
                inner: MockSource::new(stream).without_redelivery(),
                next_offset: std::sync::atomic::AtomicI64::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl EventSource for RebuildHangsSource {
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
            std::future::pending().await
        }
    }

    /// A rebuild that never returns must not hang the whole application.
    ///
    /// The stall arm awaits `recover_from_stall` inside the *body* of a
    /// `select!` arm that has already resolved, so the cancellation branch is
    /// no longer racing it. A rebuild blocked on its broker therefore parks
    /// `run` before it ever reaches `drain_in_flight` — bypassing the bounded
    /// drain entirely — while `stop_harvest_runtime` awaits the connector
    /// handle with no deadline of its own. That is a permanently hung
    /// shutdown, and the outage that wedged the prefix is usually the same one
    /// that wedges the rebuild, so the two coincide by default rather than by
    /// coincidence.
    #[tokio::test]
    async fn a_wedged_rebuild_cannot_hang_shutdown() {
        let source = Arc::new(RebuildHangsSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        // Every message ends up `Retry`, which on this source wedges the head.
        sink.fail_writes(true);
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
                error_backoff: std::time::Duration::from_millis(10),
                poll_timeout: std::time::Duration::from_millis(1),
                idle_backoff: std::time::Duration::from_millis(1),
                ..ConnectorRuntimeConfig::default()
            }),
        );

        let cancel = CancellationToken::new();
        let handle = {
            let rt = Arc::clone(&rt);
            let cancel = cancel.clone();
            tokio::spawn(async move { rt.run(cancel).await })
        };
        // Long enough for the pass to wedge the head and enter the rebuild.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        cancel.cancel();

        tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect(
                "cancellation must reach a connector parked in a rebuild, or shutdown hangs \
                 forever outside the reach of the bounded drain",
            )
            .expect("run task must not panic");
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

    /// A downstream outage wedges *every* partition in the batch, not one.
    ///
    /// `EventSource::recover` rebuilds the whole client, so every partition
    /// re-reads from its own last commit — but the stall is reported one
    /// partition at a time (`stalled` returns the lowest wedged one). Clearing
    /// only that one leaves the others' stale in-memory marks in place, and
    /// their redelivered offsets then arrive *below* those marks and are
    /// mistaken for already-settled redeliveries: exactly the failure
    /// `forget` exists to prevent, left in place for every partition but the
    /// first. It also costs one extra rebuild — and one group rebalance — per
    /// affected partition before consumption can resume.
    #[tokio::test]
    async fn recovery_clears_every_partition_because_the_rebuild_is_whole_client() {
        let source = Arc::new(RecoverableSource::new("orders"));
        let rt = ConnectorRuntime::new(
            malformed_binding(),
            Arc::clone(&source) as Arc<dyn EventSource>,
            HarvestApiState::new(),
            Arc::new(NoOpMetrics),
            IdempotencyMode::BrokerCoordinates,
        )
        .with_dead_letter_sink(Arc::new(RecordingDeadLetterSink::new()));

        // Three partitions wedged in one pass, as a Postgres outage would.
        for partition in 0..3 {
            rt.offsets.lock().await.observe(partition, 0);
            rt.offsets.lock().await.retried(partition, 0);
        }
        assert_eq!(
            rt.offsets.lock().await.stalled(0).map(|(p, _)| p),
            Some(0),
            "the lowest wedged partition is the one reported"
        );

        assert_eq!(rt.recover_from_stall(0).await, StallRecovery::Rebuilt);

        assert_eq!(
            source.recover_count(),
            1,
            "one rebuild covers the whole client"
        );
        assert_eq!(
            rt.offsets.lock().await.stalled(0),
            None,
            "so every partition's stale generation must be cleared by it, not \
             just the one that happened to be reported"
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

        assert_eq!(
            recovered,
            StallRecovery::Rebuilt,
            "a recoverable source must be rebuilt"
        );
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
        assert_eq!(
            rt.recover_from_stall(0).await,
            StallRecovery::Unsupported,
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
