//! Broker event-source connectors for workflow triggers (issue #944).
//!
//! Bind a Kafka topic or an SQS queue to a workflow so an inbound message
//! starts a run (or signal-with-starts an entity workflow), with idempotent
//! redelivery, at-least-once ack ordering, poison-message isolation, and
//! backpressure that composes with the start throttle (#607).
//!
//! # The core crate gains ZERO broker dependencies
//!
//! This is the load-bearing architectural invariant of issue #944, and it is
//! why the connector lives here rather than in `autumn-harvest`. The engine
//! crate is Postgres-only: `cargo tree -p autumn-harvest` must never show
//! `rdkafka`, `aws-sdk-sqs`, or any other broker client. Everything the engine
//! contributes is dependency-free:
//!
//! * four `harvest.connector.*` metric constants + no-op [`MetricsRecorder`]
//!   default methods (`autumn_harvest::telemetry`), and
//! * the additive `StartSource::Broker` provenance variant.
//!
//! Enforced mechanically by `tests/connector_dependency_graph.rs`, which
//! shells out to `cargo tree -p autumn-harvest` and fails if a broker client
//! ever appears. The same seam shape the engine already uses for
//! [`PayloadStore`][ps] (#524), [`CompletionCallbackDeliverer`][cb] (#605) and
//! [`HistoryArchiver`][ha] (#345): trait in the crate, implementation supplied
//! by the embedder.
//!
//! [ps]: autumn_harvest::payload_store::PayloadStore
//! [cb]: autumn_harvest::completion_callback::CompletionCallbackDeliverer
//! [ha]: autumn_harvest::retention::HistoryArchiver
//! [`MetricsRecorder`]: autumn_harvest::telemetry::MetricsRecorder
//!
//! # No new `WorkflowEvent` variant, no core migration
//!
//! A broker-triggered start is an ordinary start: it lands the same
//! `WorkflowStarted` event any HTTP caller produces. The only durable state
//! the connector owns is `harvest_connector_dead_letters`, a **plugin**-owned
//! table (alongside `harvest_workflow_outbox`) for poison messages that never
//! became executions — see [`dead_letter`] for why the engine's
//! `harvest_dead_letters` cannot hold one.
//!
//! # Layering
//!
//! ```text
//!   EventSource (adapter)   ← kafka.rs / sqs.rs / MockSource, or yours
//!         │  receive / ack / abandon
//!         ▼
//!   ConnectorRuntime        ← broker-agnostic: map, dispatch, decide, settle
//!         │
//!         ├── binding.rs      what a message maps to (the descriptor)
//!         ├── idempotency.rs  injective, bounded dedupe key
//!         ├── disposition.rs  PURE decision core (ack / retry / dead-letter)
//!         └── dispatch.rs     delegates to the plugin's own start handlers
//! ```
//!
//! Only [`EventSource`] is broker-specific, so NATS/RabbitMQ/Pub-Sub/Kinesis
//! adapters are follow-ups rather than rewrites.
//!
//! # Ordering caveat
//!
//! The runtime dispatches up to `max_in_flight` messages concurrently and does
//! **not** preserve broker partition ordering across them. For per-key
//! ordering, use a `SignalsWithStart` binding whose mapping function derives a
//! stable `workflow_id` from the partition key (`ctx.key_str()` on Kafka, or a
//! body field / header on a broker with no key): the entity workflow
//! serializes its own signals. Set `.max_in_flight(1)` as well if messages for
//! the *same* key must reach that workflow in queue order — the entity
//! workflow serializes them either way, but the order it sees them in is the
//! order they were dispatched, which concurrency does not preserve. See
//! `docs/getting-started/13-broker-connectors.md`.
//!
//! # Example
//!
//! ```no_run
//! use autumn_harvest_plugin::connector::{ConnectorTarget, MappedMessage, SourceBinding};
//!
//! #[derive(serde::Deserialize, serde::Serialize)]
//! struct OrderPlaced {
//!     order_id: String,
//! }
//!
//! let binding = SourceBinding::starts("orders", "orders.placed", "fulfil_order")
//!     .map_json(|_ctx, event: OrderPlaced| {
//!         let payload = serde_json::to_value(&event).map_err(|e| e.to_string())?;
//!         Ok::<_, String>(MappedMessage::new(format!("order-{}", event.order_id), payload))
//!     })
//!     .max_in_flight(32);
//!
//! assert!(matches!(binding.target, ConnectorTarget::Starts { .. }));
//! ```

pub mod binding;
pub mod dead_letter;
mod dispatch;
pub mod disposition;
pub mod idempotency;
pub mod message;
pub mod mock;
pub mod runtime;
pub mod source;

#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "sqs")]
pub mod sqs;

#[cfg(feature = "kafka")]
pub use kafka::{KafkaSource, KafkaSourceConfig};
#[cfg(feature = "sqs")]
pub use sqs::{SqsSource, SqsSourceConfig};

pub use binding::{
    ConnectorTarget, DEFAULT_MAX_IN_FLIGHT, DEFAULT_POISON_THRESHOLD, IdempotencyMode,
    MappedMessage, MappingError, MessageMapper, SourceBinding, has_deferred_admission,
    resolve_idempotency_mode, validate_bindings, validate_bindings_refs,
};
pub use dead_letter::{
    ConnectorDeadLetter, DeadLetterSink, NoopDeadLetterSink, PostgresDeadLetterSink,
    RecordingDeadLetterSink, insert_dead_letter,
};
pub use disposition::{
    DeadLetterMode, DispatchOutcome, MessageDisposition, OffsetTracker, PoisonTracker,
    decide_disposition, success_outcome,
};
pub use idempotency::{
    CONNECTOR_KEY_PREFIX, MAX_KEY_COMPONENT_LEN, bound_key_component, message_idempotency_key,
};
pub use message::{InboundMessage, MessageCoordinates, MessageCtx, MessageHandle};
pub use mock::MockSource;
pub use runtime::{ConnectorRuntime, ConnectorRuntimeConfig, PassSummary};
pub use source::{ConnectorError, EventSource};
