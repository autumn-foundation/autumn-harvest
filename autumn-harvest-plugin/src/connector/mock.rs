//! An in-memory [`EventSource`] for tests and local development (issue #944).
//!
//! `MockSource` is a **supported, exported** adapter, not test-only scaffolding:
//! it lets an embedder unit-test a mapping function and the whole dispatch
//! path — idempotency, poison isolation, throttle deferral, ack ordering —
//! with no broker and no Docker. It is also what makes the connector's own
//! correctness suite runnable everywhere, including the deliberate-redelivery
//! and crash-recovery scenarios.

use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use super::message::{InboundMessage, MessageCoordinates, MessageHandle};
use super::source::{ConnectorError, EventSource};

#[derive(Debug, Default)]
struct MockState {
    pending: VecDeque<InboundMessage>,
    acked: Vec<MessageHandle>,
    abandoned: Vec<MessageHandle>,
    nacked_for_dead_letter: Vec<MessageHandle>,
    closed: bool,
}

/// An in-memory message source with observable acknowledgements.
#[derive(Debug, Clone)]
pub struct MockSource {
    stream: String,
    state: Arc<Mutex<MockState>>,
    lag: Arc<Mutex<Option<i64>>>,
}

impl MockSource {
    /// A source for `stream` with no messages queued.
    #[must_use]
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            state: Arc::new(Mutex::new(MockState::default())),
            lag: Arc::new(Mutex::new(None)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MockState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Queue a message with explicit Kafka-shaped coordinates.
    pub fn push_kafka(&self, partition: i32, offset: i64, payload: impl AsRef<[u8]>) {
        self.push_kafka_keyed(partition, offset, None::<&[u8]>, payload);
    }

    /// Queue a Kafka-shaped message carrying a partition key.
    ///
    /// The keyed form exists so a test can exercise the per-key ordering
    /// remedy (mapping `ctx.key_str()` to a stable `workflow_id`) without a
    /// live broker.
    pub fn push_kafka_keyed(
        &self,
        partition: i32,
        offset: i64,
        key: Option<impl AsRef<[u8]>>,
        payload: impl AsRef<[u8]>,
    ) {
        let coordinates = MessageCoordinates::KafkaOffset {
            topic: self.stream.clone(),
            partition,
            offset,
        };
        let handle = MessageHandle::positioned(coordinates.render(), partition, offset);
        self.push_message(InboundMessage {
            coordinates,
            payload: payload.as_ref().to_vec(),
            key: key.map(|k| k.as_ref().to_vec()),
            headers: std::collections::BTreeMap::new(),
            handle,
        });
    }

    /// Queue a message with an opaque broker id (SQS-shaped).
    pub fn push_opaque(&self, id: impl Into<String>, payload: impl AsRef<[u8]>) {
        let id = id.into();
        let coordinates = MessageCoordinates::Opaque {
            stream: self.stream.clone(),
            id: id.clone(),
        };
        self.push_message(InboundMessage {
            coordinates,
            payload: payload.as_ref().to_vec(),
            key: None,
            headers: std::collections::BTreeMap::new(),
            handle: MessageHandle::opaque(id),
        });
    }

    /// Queue a fully-formed message (used to replay a redelivery verbatim).
    pub fn push_message(&self, message: InboundMessage) {
        self.lock().pending.push_back(message);
    }

    /// Set the lag this source reports.
    pub fn set_lag(&self, lag: Option<i64>) {
        *self
            .lag
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = lag;
    }

    /// Mark the source closed; further `receive` calls return
    /// [`ConnectorError::Closed`].
    pub fn close(&self) {
        self.lock().closed = true;
    }

    /// Handles the runtime acknowledged, in order.
    #[must_use]
    pub fn acked(&self) -> Vec<MessageHandle> {
        self.lock().acked.clone()
    }

    /// Handles the runtime nack'd toward broker-native dead-lettering.
    ///
    /// Distinct from [`Self::abandoned`]: the two are opposite intents (gentle
    /// transient retry vs. fastest possible redelivery so the broker's redrive
    /// claims the message), and an adapter like SQS implements them
    /// differently.
    #[must_use]
    pub fn nacked_for_dead_letter(&self) -> Vec<MessageHandle> {
        self.lock().nacked_for_dead_letter.clone()
    }

    /// Handles the runtime abandoned, in order.
    #[must_use]
    pub fn abandoned(&self) -> Vec<MessageHandle> {
        self.lock().abandoned.clone()
    }

    /// Number of messages still queued.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.lock().pending.len()
    }
}

#[async_trait]
impl EventSource for MockSource {
    fn stream(&self) -> &str {
        &self.stream
    }

    async fn receive(
        &self,
        max: usize,
        _timeout: std::time::Duration,
    ) -> Result<Vec<InboundMessage>, ConnectorError> {
        let mut state = self.lock();
        if state.closed && state.pending.is_empty() {
            return Err(ConnectorError::Closed);
        }
        let take = max.min(state.pending.len());
        Ok(state.pending.drain(..take).collect())
    }

    async fn ack(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
        self.lock().acked.push(handle.clone());
        Ok(())
    }

    fn has_native_dead_letter(&self) -> bool {
        // The mock can stand in for either adapter shape, so it advertises
        // support and lets a test choose the binding's dead-letter mode.
        true
    }

    async fn nack_for_dead_letter(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
        self.lock().nacked_for_dead_letter.push(handle.clone());
        Ok(())
    }

    async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
        self.lock().abandoned.push(handle.clone());
        Ok(())
    }

    async fn lag(&self) -> Option<i64> {
        *self
            .lag
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn receive_drains_up_to_max_in_fifo_order() {
        let src = MockSource::new("orders");
        src.push_kafka(0, 1, b"a");
        src.push_kafka(0, 2, b"b");
        src.push_kafka(0, 3, b"c");

        let batch = src
            .receive(2, std::time::Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].payload, b"a");
        assert_eq!(batch[1].payload, b"b");
        assert_eq!(src.pending(), 1);
    }

    #[tokio::test]
    async fn empty_receive_is_not_an_error() {
        let src = MockSource::new("orders");
        let batch = src
            .receive(10, std::time::Duration::from_millis(1))
            .await
            .unwrap();
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn closed_source_drains_then_reports_closed() {
        let src = MockSource::new("orders");
        src.push_opaque("m1", b"x");
        src.close();
        // Still drains what is queued...
        assert_eq!(
            src.receive(10, std::time::Duration::from_millis(1))
                .await
                .unwrap()
                .len(),
            1
        );
        // ...then reports closed.
        assert!(matches!(
            src.receive(10, std::time::Duration::from_millis(1)).await,
            Err(ConnectorError::Closed)
        ));
    }

    #[tokio::test]
    async fn acks_and_abandons_are_observable() {
        let src = MockSource::new("orders");
        src.ack(&MessageHandle::opaque("a")).await.unwrap();
        src.abandon(&MessageHandle::opaque("b")).await.unwrap();
        assert_eq!(src.acked().len(), 1);
        assert_eq!(src.acked()[0].token, "a");
        assert_eq!(src.abandoned()[0].token, "b");
    }

    #[tokio::test]
    async fn lag_is_reported_when_set() {
        let src = MockSource::new("orders");
        assert_eq!(src.lag().await, None);
        src.set_lag(Some(17));
        assert_eq!(src.lag().await, Some(17));
    }

    #[tokio::test]
    async fn kafka_push_carries_partition_and_offset_on_the_handle() {
        let src = MockSource::new("orders");
        src.push_kafka(2, 88, b"x");
        let batch = src
            .receive(1, std::time::Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(batch[0].handle.partition, Some(2));
        assert_eq!(batch[0].handle.position, Some(88));
        assert_eq!(batch[0].coordinates.render(), "orders:2:88");
    }
}
