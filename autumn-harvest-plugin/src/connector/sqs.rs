//! Amazon SQS [`EventSource`] adapter (issue #944), behind the `sqs` feature.
//!
//! Like the Kafka adapter this is deliberately thin — mapping, idempotency,
//! poison accounting and backpressure are broker-agnostic and live in
//! [`super::runtime`].
//!
//! # Ack semantics
//!
//! SQS acknowledgement is a per-message `DeleteMessage`, so there is no
//! high-water mark to protect and no offset ordering to respect: the runtime's
//! [`OffsetTracker`][ot] gate is bypassed entirely for opaque handles. A
//! message is deleted only after its dispatch is durable; otherwise its
//! visibility timeout lapses (or [`EventSource::abandon`] resets it to zero)
//! and SQS redelivers, where it dedupes on the connector's derived key.
//!
//! [ot]: super::disposition::OffsetTracker
//!
//! # Dead-lettering
//!
//! SQS has a native dead-letter destination (a redrive policy with
//! `maxReceiveCount`). Pair this adapter with
//! [`SourceBinding::broker_native_dead_letter`][bn] so a poison message is
//! *abandoned* rather than deleted, letting the queue's own redrive move it to
//! the configured DLQ. Without a redrive policy, leave the default so poison
//! messages land in `harvest_connector_dead_letters` instead.
//!
//! [bn]: super::binding::SourceBinding::broker_native_dead_letter

use async_trait::async_trait;
use aws_sdk_sqs::Client;
use std::collections::BTreeMap;

use super::message::{InboundMessage, MessageCoordinates, MessageHandle};
use super::source::{ConnectorError, EventSource};

/// Longest `WaitTimeSeconds` the SQS long-poll API accepts.
pub const MAX_SQS_WAIT_SECONDS: i32 = 20;
/// Most messages one `ReceiveMessage` call can return.
pub const MAX_SQS_BATCH: usize = 10;

/// Settings for an [`SqsSource`].
#[derive(Debug, Clone)]
pub struct SqsSourceConfig {
    /// Full queue URL.
    pub queue_url: String,
    /// The logical stream name used in metrics, coordinates and bindings.
    /// Defaults to the queue name parsed from the URL.
    pub stream: String,
    /// Visibility timeout applied to received messages, in seconds. `None`
    /// uses the queue's own default. Size it above the worst-case dispatch
    /// latency so a message is not redelivered while still in flight.
    pub visibility_timeout: Option<i32>,
}

impl SqsSourceConfig {
    /// Config for `queue_url`, deriving the stream name from the URL.
    #[must_use]
    pub fn new(queue_url: impl Into<String>) -> Self {
        let queue_url = queue_url.into();
        let stream = queue_name_from_url(&queue_url);
        Self {
            queue_url,
            stream,
            visibility_timeout: None,
        }
    }

    /// Override the logical stream name.
    #[must_use]
    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = stream.into();
        self
    }

    /// Set the per-receive visibility timeout, in seconds.
    #[must_use]
    pub const fn visibility_timeout_secs(mut self, secs: i32) -> Self {
        self.visibility_timeout = Some(secs);
        self
    }
}

/// The queue name is the last path segment of the queue URL.
///
/// Falls back to the whole URL when it has no path, so the stream name is
/// never empty (an empty stream fails binding validation).
#[must_use]
pub fn queue_name_from_url(url: &str) -> String {
    url.rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(url)
        .to_string()
}

/// An SQS-backed [`EventSource`].
#[derive(Debug, Clone)]
pub struct SqsSource {
    client: Client,
    queue_url: String,
    stream: String,
    visibility_timeout: Option<i32>,
}

impl SqsSource {
    /// Build a source over an existing SQS client.
    ///
    /// Taking the client rather than constructing it keeps credential and
    /// endpoint resolution (including a LocalStack/ElasticMQ endpoint
    /// override in tests) entirely in the embedder's hands.
    #[must_use]
    pub fn new(client: Client, config: SqsSourceConfig) -> Self {
        Self {
            client,
            queue_url: config.queue_url,
            stream: config.stream,
            visibility_timeout: config.visibility_timeout,
        }
    }

    /// Build a source using the ambient AWS config chain.
    ///
    /// # Errors
    ///
    /// Never returns an error today; the signature is fallible so credential
    /// resolution can surface failures in a later revision without a breaking
    /// change.
    pub async fn connect(config: SqsSourceConfig) -> Result<Self, ConnectorError> {
        let shared = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ok(Self::new(Client::new(&shared), config))
    }
}

/// Pick the stable broker coordinate for one SQS message.
///
/// Prefers `MessageDeduplicationId` (FIFO queues), which the *producer*
/// controls and which therefore survives a redelivery **and** a re-publish of
/// the same logical event. Falls back to `MessageId`, which is stable across
/// redeliveries of the same message but not across re-publishes — the honest
/// limit of a standard queue.
#[must_use]
pub fn coordinate_id(dedup_id: Option<&str>, message_id: Option<&str>) -> Option<String> {
    dedup_id
        .filter(|s| !s.is_empty())
        .or_else(|| message_id.filter(|s| !s.is_empty()))
        .map(str::to_string)
}

#[async_trait]
impl EventSource for SqsSource {
    fn stream(&self) -> &str {
        &self.stream
    }

    async fn receive(
        &self,
        max: usize,
        timeout: std::time::Duration,
    ) -> Result<Vec<InboundMessage>, ConnectorError> {
        let wait = i32::try_from(timeout.as_secs())
            .unwrap_or(MAX_SQS_WAIT_SECONDS)
            .clamp(0, MAX_SQS_WAIT_SECONDS);
        let batch = i32::try_from(max.clamp(1, MAX_SQS_BATCH)).unwrap_or(1);

        let mut request = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(batch)
            .wait_time_seconds(wait)
            // `MessageDeduplicationId` only arrives when explicitly requested.
            .message_system_attribute_names(
                aws_sdk_sqs::types::MessageSystemAttributeName::MessageDeduplicationId,
            )
            .message_attribute_names("All");
        if let Some(vt) = self.visibility_timeout {
            request = request.visibility_timeout(vt);
        }

        let response = request
            .send()
            .await
            .map_err(|e| ConnectorError::Broker(format!("sqs receive: {e}")))?;

        let mut out = Vec::new();
        for message in response.messages.unwrap_or_default() {
            let Some(receipt) = message.receipt_handle.clone() else {
                // Without a receipt handle the message can never be deleted;
                // skipping it lets the visibility timeout redeliver it rather
                // than dispatching something we could not acknowledge.
                tracing::warn!(
                    stream = %self.stream,
                    "sqs message has no receipt handle; skipping"
                );
                continue;
            };
            let dedup = message
                .attributes
                .as_ref()
                .and_then(|a| {
                    a.get(&aws_sdk_sqs::types::MessageSystemAttributeName::MessageDeduplicationId)
                })
                .map(String::as_str);
            let Some(id) = coordinate_id(dedup, message.message_id.as_deref()) else {
                tracing::warn!(
                    stream = %self.stream,
                    "sqs message has neither a dedup id nor a message id; skipping"
                );
                continue;
            };

            let headers: BTreeMap<String, String> = message
                .message_attributes
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(k, v)| v.string_value.map(|s| (k, s)))
                .collect();

            out.push(InboundMessage {
                coordinates: MessageCoordinates::Opaque {
                    stream: self.stream.clone(),
                    id,
                },
                payload: message.body.unwrap_or_default().into_bytes(),
                headers,
                handle: MessageHandle::opaque(receipt),
            });
        }
        Ok(out)
    }

    async fn ack(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
        self.client
            .delete_message()
            .queue_url(&self.queue_url)
            .receipt_handle(&handle.token)
            .send()
            .await
            .map_err(|e| ConnectorError::Broker(format!("sqs delete: {e}")))?;
        Ok(())
    }

    async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
        // Zero the visibility timeout so the message is redelivered
        // immediately rather than after the queue's default lapses. This also
        // increments `ApproximateReceiveCount`, which is what drives a native
        // redrive policy toward the queue's DLQ.
        self.client
            .change_message_visibility()
            .queue_url(&self.queue_url)
            .receipt_handle(&handle.token)
            .visibility_timeout(0)
            .send()
            .await
            .map_err(|e| ConnectorError::Broker(format!("sqs change visibility: {e}")))?;
        Ok(())
    }

    async fn lag(&self) -> Option<i64> {
        let response = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
            .ok()?;
        response
            .attributes?
            .get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)?
            .parse::<i64>()
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_name_defaults_to_the_queue_name() {
        let cfg = SqsSourceConfig::new("https://sqs.us-east-1.amazonaws.com/123456789012/orders");
        assert_eq!(cfg.stream, "orders");
    }

    #[test]
    fn stream_name_survives_a_trailing_slash_and_can_be_overridden() {
        assert_eq!(
            queue_name_from_url("https://host/123/orders-queue/"),
            "orders-queue"
        );
        // Never empty: an empty stream fails binding validation.
        assert_eq!(queue_name_from_url("orders"), "orders");
        assert_eq!(
            SqsSourceConfig::new("https://host/1/q")
                .stream("logical")
                .stream,
            "logical"
        );
    }

    #[test]
    fn dedup_id_is_preferred_over_message_id() {
        // AC3: the producer-controlled dedup id survives a re-publish, so it
        // is the stronger identity when the queue provides one.
        assert_eq!(
            coordinate_id(Some("dedup-1"), Some("msg-1")),
            Some("dedup-1".to_string())
        );
        assert_eq!(
            coordinate_id(None, Some("msg-1")),
            Some("msg-1".to_string())
        );
        // An empty dedup id is not an identity.
        assert_eq!(
            coordinate_id(Some(""), Some("msg-1")),
            Some("msg-1".to_string())
        );
        assert_eq!(coordinate_id(None, None), None);
        assert_eq!(coordinate_id(Some(""), Some("")), None);
    }

    #[test]
    fn visibility_timeout_is_opt_in() {
        assert_eq!(
            SqsSourceConfig::new("https://h/1/q").visibility_timeout,
            None
        );
        assert_eq!(
            SqsSourceConfig::new("https://h/1/q")
                .visibility_timeout_secs(90)
                .visibility_timeout,
            Some(90)
        );
    }
}
