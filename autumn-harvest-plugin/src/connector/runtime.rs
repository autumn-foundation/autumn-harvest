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
use super::dead_letter::{ConnectorDeadLetter, DeadLetterSink, NoopDeadLetterSink};
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
    pub poll_timeout: std::time::Duration,
    /// Most messages pulled per `receive` call.
    pub max_batch: usize,
    /// How long to back off after a broker error before polling again.
    pub error_backoff: std::time::Duration,
}

impl Default for ConnectorRuntimeConfig {
    fn default() -> Self {
        Self {
            poll_timeout: std::time::Duration::from_millis(500),
            max_batch: 32,
            error_backoff: std::time::Duration::from_secs(1),
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
        let sink: Arc<dyn DeadLetterSink> = Arc::new(NoopDeadLetterSink);
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
                    Ok(_) => {}
                    Err(ConnectorError::Closed) => {
                        tracing::info!(source = source_name, "connector source closed");
                        break;
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
        let batch = self
            .source
            .receive(self.config.max_batch, self.config.poll_timeout)
            .await?;

        if let Some(lag) = self.source.lag().await {
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
            if let (Some(partition), Some(position)) =
                (message.handle.partition, message.handle.position)
            {
                self.offsets.lock().await.observe(partition, position);
            }

            // Backpressure: block here until a slot frees, so the runtime
            // cannot outrun harvest's admission path.
            let permit = Arc::clone(&self.permits)
                .acquire_owned()
                .await
                .map_err(|e| ConnectorError::Broker(format!("connector semaphore closed: {e}")))?;

            let this = self.clone_handles();
            handles.push(tokio::spawn(async move {
                let disposition = this.process(message).await;
                drop(permit);
                disposition
            }));
        }

        for handle in handles {
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
                    tracing::error!(
                        source = self.binding.name,
                        error = %e,
                        "connector dispatch task failed; message left for redelivery"
                    );
                    summary.retried += 1;
                }
            }
        }

        Ok(summary)
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
            .settle(&message, &coordinates, &key, &outcome, decided)
            .await;

        // Once a message reaches a terminal disposition its strike history is
        // no longer needed; dropping it keeps the tracker bounded. A message
        // left for redelivery keeps its strikes so the threshold still bites.
        if !matches!(effective, MessageDisposition::Retry) {
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
        let mapped = match (self.binding.mapper)(&ctx) {
            Ok(m) => m,
            Err(MappingError::Deserialize(m)) => return DispatchOutcome::Malformed(m),
            Err(MappingError::Rejected(m)) => return DispatchOutcome::MappingRejected(m),
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
                let entry = self.dead_letter_entry(message, coordinates, key, outcome, reason);
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
                self.abandon(&message.handle).await;
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
            attempts: i32::try_from(self.binding.poison_threshold.max(1)).unwrap_or(i32::MAX),
            payload: message.payload.clone(),
            failed_at: Utc::now(),
        }
    }

    async fn ack(&self, handle: &MessageHandle) {
        // For a positionally-ordered broker (Kafka), only advance the
        // high-water mark to the contiguous completed prefix — committing past
        // an in-flight lower offset would silently skip it on a crash.
        if let (Some(partition), Some(position)) = (handle.partition, handle.position) {
            let advanced = self.offsets.lock().await.complete(partition, position);
            if advanced.is_none() {
                // Earlier offsets are still in flight; the adapter will commit
                // this one as part of a later contiguous advance.
                return;
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
}

/// Dead-letter mode helper re-exported for the plugin wiring.
#[must_use]
pub const fn dead_letter_mode_of(binding: &SourceBinding) -> DeadLetterMode {
    binding.dead_letter_mode
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::binding::SourceBinding;
    use crate::connector::dead_letter::RecordingDeadLetterSink;
    use crate::connector::mock::MockSource;
    use autumn_harvest::telemetry::NoOpMetrics;

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
        assert_eq!(source.abandoned().len(), 1);
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

        // Completing 4 advances the prefix through 5.
        rt.ack(&MessageHandle::positioned("orders:0:4", 0, 4)).await;
        assert_eq!(source.acked().len(), 1);
        assert_eq!(source.acked()[0].position, Some(4));
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

    #[tokio::test]
    async fn in_flight_dispatch_is_bounded_by_max_in_flight() {
        // AC5: the connector bounds its own concurrency so a backlog cannot
        // stampede the admission path.
        let binding = Arc::new(
            SourceBinding::starts("orders", "orders", "order_flow")
                .map_raw(|_ctx| Err(MappingError::Deserialize("x".to_string())))
                .max_in_flight(4),
        );
        let source = Arc::new(MockSource::new("orders"));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(binding, source, sink);
        assert_eq!(rt.permits.available_permits(), 4);
    }

    #[tokio::test]
    async fn lag_is_reported_to_metrics_when_the_source_exposes_it() {
        let source = Arc::new(MockSource::new("orders"));
        source.set_lag(Some(42));
        let sink = Arc::new(RecordingDeadLetterSink::new());
        let rt = runtime(malformed_binding(), Arc::clone(&source), sink);
        // NoOpMetrics swallows it; the assertion is simply that reporting lag
        // never fails a pass.
        assert_eq!(rt.run_once().await.unwrap(), PassSummary::default());
    }
}
