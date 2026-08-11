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
//! message is deleted only after its dispatch is durable; otherwise SQS
//! redelivers it, where it dedupes on the connector's derived key.
//!
//! *When* it is redelivered differs by path, and the two are deliberately not
//! the same speed:
//!
//! * **Transient retry.** [`EventSource::abandon`] is a no-op — simply not
//!   deleting the message is already sufficient, so it reappears once its
//!   existing visibility timeout lapses. **The visibility timeout is the retry
//!   backoff**; size it with [`SqsSourceConfig::visibility_timeout_secs`].
//!   Zeroing visibility here would turn a transient failure — usually harvest
//!   under pressure — into a tight redelivery loop against an already-struggling
//!   system, and burn `ApproximateReceiveCount` toward a redrive policy's
//!   `maxReceiveCount` faster than the failure warrants.
//! * **Poison redrive.** [`EventSource::nack_for_dead_letter`] *does* reset
//!   visibility to zero, because there fast redelivery is the point: each
//!   receive drives the message toward `maxReceiveCount` and thus the queue's
//!   DLQ. Only reachable under
//!   [`SourceBinding::broker_native_dead_letter`][bn].
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
    /// Whether the queue has a redrive policy, i.e. whether SQS actually has
    /// somewhere to move a poison message.
    ///
    /// `None` means "not declared": [`SqsSource::connect`] probes the queue,
    /// while [`SqsSource::new`] cannot (it is sync and takes an already-built
    /// client), so an undeclared, unprobed source reports **no** native
    /// dead-letter destination. See [`SqsSourceConfig::has_redrive_policy`].
    pub has_redrive_policy: Option<bool>,
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
            has_redrive_policy: None,
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

    /// Declare whether the queue has a redrive policy.
    ///
    /// Only needed with [`SqsSource::new`], which takes an already-built client
    /// and so cannot probe the queue; [`SqsSource::connect`] discovers this for
    /// you. An explicit declaration always wins over the probe, so a queue
    /// whose IAM policy denies `GetQueueAttributes` can still opt in.
    ///
    /// Declaring `true` for a queue that has **no** redrive policy re-arms the
    /// exact failure `broker_native_dead_letter` exists to prevent: an
    /// abandoned poison message is redelivered forever instead of quarantined.
    #[must_use]
    pub const fn has_redrive_policy(mut self, has_redrive: bool) -> Self {
        self.has_redrive_policy = Some(has_redrive);
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
    /// `None` = not declared and not probed; see [`native_dead_letter_supported`].
    has_redrive_policy: Option<bool>,
}

impl SqsSource {
    /// Build a source over an existing SQS client.
    ///
    /// Taking the client rather than constructing it keeps credential and
    /// endpoint resolution (including a LocalStack/ElasticMQ endpoint
    /// override in tests) entirely in the embedder's hands.
    ///
    /// This constructor is sync, so it cannot probe the queue for a redrive
    /// policy. A source built this way reports **no** native dead-letter
    /// destination unless the config declares one with
    /// [`SqsSourceConfig::has_redrive_policy`] — see
    /// [`EventSource::has_native_dead_letter`].
    #[must_use]
    pub fn new(client: Client, config: SqsSourceConfig) -> Self {
        Self {
            client,
            queue_url: config.queue_url,
            stream: config.stream,
            visibility_timeout: config.visibility_timeout,
            has_redrive_policy: config.has_redrive_policy,
        }
    }

    /// Build a source using the ambient AWS config chain.
    ///
    /// Probes the queue's `RedrivePolicy` so
    /// [`EventSource::has_native_dead_letter`] reflects reality, unless the
    /// config already declared it. A probe that fails (typically an IAM policy
    /// without `sqs:GetQueueAttributes`) is logged and leaves the answer
    /// undeclared, which fails closed.
    ///
    /// # Errors
    ///
    /// Never returns an error today; the signature is fallible so credential
    /// resolution can surface failures in a later revision without a breaking
    /// change. A failed redrive probe is deliberately *not* an error — it
    /// degrades to fail-closed rather than refusing to start.
    pub async fn connect(config: SqsSourceConfig) -> Result<Self, ConnectorError> {
        let shared = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&shared);
        let mut source = Self::new(client, config);
        if source.has_redrive_policy.is_none() {
            source.has_redrive_policy = source.probe_redrive_policy().await;
        }
        Ok(source)
    }

    /// Ask the queue whether it has a redrive policy.
    ///
    /// `None` on any failure, so a denied or unreachable probe never *claims* a
    /// dead-letter destination the queue may not have.
    async fn probe_redrive_policy(&self) -> Option<bool> {
        let response = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::RedrivePolicy)
            .send()
            .await;
        match response {
            Ok(attrs) => {
                let policy = attrs
                    .attributes
                    .as_ref()
                    .and_then(|a| a.get(&aws_sdk_sqs::types::QueueAttributeName::RedrivePolicy))
                    .map(String::as_str);
                Some(redrive_policy_is_configured(policy))
            }
            Err(e) => {
                tracing::warn!(
                    stream = %self.stream,
                    error = %e,
                    "could not read the SQS queue's redrive policy; treating the queue as having \
                     no native dead-letter destination. Declare it with \
                     `SqsSourceConfig::has_redrive_policy(true)` if it does have one"
                );
                None
            }
        }
    }
}

/// Does this `RedrivePolicy` queue attribute name a real dead-letter target?
///
/// The attribute is a JSON document like
/// `{"deadLetterTargetArn":"arn:…","maxReceiveCount":"5"}`. Absent, empty,
/// unparseable, or naming no destination all mean **no**: abandoning a poison
/// message on such a queue redelivers it forever rather than quarantining it.
#[must_use]
pub fn redrive_policy_is_configured(attribute: Option<&str>) -> bool {
    let Some(raw) = attribute.map(str::trim).filter(|s| !s.is_empty()) else {
        return false;
    };
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .as_ref()
        .and_then(|v| v.get("deadLetterTargetArn"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|arn| !arn.trim().is_empty())
}

/// Resolve a possibly-undeclared redrive answer into the trait's `bool`.
///
/// Fails **closed**: an undeclared, unprobed queue reports no native
/// dead-letter destination, so `broker_native_dead_letter()` is rejected at
/// build time with an actionable message rather than silently degrading into a
/// poison-redelivery loop at runtime.
#[must_use]
pub const fn native_dead_letter_supported(has_redrive_policy: Option<bool>) -> bool {
    matches!(has_redrive_policy, Some(true))
}

/// Pick the stable broker coordinate for one SQS message.
///
/// Prefers the broker-assigned `MessageId`, which is what a connector
/// coordinate is *for*: it is stable across every redelivery of the same
/// message and distinct for every distinct message.
///
/// A FIFO `MessageDeduplicationId` looks like a stronger identity — the
/// *producer* controls it, so it survives a re-publish — but it is the wrong
/// tool here, in both directions:
///
/// * **Inside** SQS's five-minute deduplication interval, SQS already collapses
///   a re-publish carrying the same dedup id. The second publish never reaches
///   a consumer, so preferring the dedup id buys nothing in the only window
///   where it would.
/// * **Outside** that interval, a legitimately reused dedup id (a nightly job
///   keyed on a business id, say) produces a genuinely *new* message with a new
///   `MessageId`. Coordinating on the dedup id would collapse it onto the
///   earlier message's key, so the connector would ack it as an idempotent
///   replay and silently drop a valid event.
///
/// The dedup id is therefore only a fallback, for the pathological case where
/// the broker gave us no `MessageId` at all.
///
/// The honest limit this leaves: a coordinate identifies a *message*, not a
/// logical event, so a genuine re-publish of the same event is dispatched
/// again. Map a business key into the workflow id (see
/// [`IdempotencyMode::WorkflowId`](super::IdempotencyMode::WorkflowId)) when
/// event-level identity is what you need.
#[must_use]
pub fn coordinate_id(dedup_id: Option<&str>, message_id: Option<&str>) -> Option<String> {
    message_id
        .filter(|s| !s.is_empty())
        .or_else(|| dedup_id.filter(|s| !s.is_empty()))
        .map(str::to_string)
}

/// The physical-subscription identity for a queue URL.
///
/// The URL rather than the stream name: two queues can share a name across
/// accounts or regions, and the stream name is caller-overridable via
/// [`SqsSourceConfig::stream`], so neither is a reliable identity. SQS has no
/// consumer-group concept, so every receiver on one queue competes.
#[must_use]
fn subscription_identity_for(queue_url: &str) -> String {
    format!("sqs:{queue_url}")
}

#[async_trait]
impl EventSource for SqsSource {
    fn stream(&self) -> &str {
        &self.stream
    }

    fn subscription_identity(&self) -> Option<String> {
        Some(subscription_identity_for(&self.queue_url))
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
            // It is the last-resort coordinate fallback (see `coordinate_id`),
            // so ask for it even though `MessageId` normally wins.
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
                // SQS has no partition-key concept (a FIFO `MessageGroupId` is
                // an ordering group, not a routing key), so there is nothing
                // honest to surface here.
                key: None,
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

    fn has_native_dead_letter(&self) -> bool {
        // Redelivery drives `ApproximateReceiveCount` toward the queue's
        // `maxReceiveCount`, at which point SQS moves the message to the
        // redrive policy's target — that is what `DeadLetterMode::BrokerNative`
        // relies on. *Without* a redrive policy there is no target, so the
        // poison message is simply redelivered forever. Hence the answer is
        // the queue's actual configuration, not an unconditional `true`.
        native_dead_letter_supported(self.has_redrive_policy)
    }

    async fn abandon(&self, _handle: &MessageHandle) -> Result<(), ConnectorError> {
        // Deliberately a no-op. Simply *not* deleting the message is already
        // sufficient for redelivery: its visibility timeout lapses and SQS
        // hands it back. Zeroing the visibility here (as this once did) would
        // turn a transient harvest failure — usually harvest under pressure —
        // into a tight redelivery loop against an already-struggling system,
        // and would burn `ApproximateReceiveCount` faster than necessary
        // toward a redrive policy's `maxReceiveCount`.
        //
        // The visibility timeout *is* the retry backoff; size it with
        // `SqsSourceConfig::visibility_timeout_secs`.
        Ok(())
    }

    async fn nack_for_dead_letter(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
        // The poison path, where fast redelivery is exactly the point: each
        // one increments `ApproximateReceiveCount`, driving the message toward
        // the queue's redrive `maxReceiveCount` and thus its DLQ.
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

    /// Outstanding messages: visible **plus** in flight.
    ///
    /// `ApproximateNumberOfMessages` alone is the *visible* count, and a
    /// message this connector abandoned for retry is invisible until its
    /// visibility timeout lapses. During a downstream outage — precisely when
    /// the gauge matters — successive polls would therefore drive the reported
    /// lag toward zero while a large population is still in flight or waiting
    /// to reappear, masking the backlog. `NotVisible` is what makes the number
    /// mean "still owed to harvest" rather than "immediately fetchable".
    ///
    /// A missing or unparseable attribute contributes zero rather than
    /// discarding the whole sample, so losing one of the two never reports a
    /// wildly wrong total; both missing yields `None` and skips the sample.
    async fn lag(&self) -> Option<i64> {
        use aws_sdk_sqs::types::QueueAttributeName;

        let response = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
            .send()
            .await
            .ok()?;
        let attributes = response.attributes?;
        let read = |name: &QueueAttributeName| -> Option<i64> {
            attributes.get(name).and_then(|v| v.parse::<i64>().ok())
        };
        let visible = read(&QueueAttributeName::ApproximateNumberOfMessages);
        let in_flight = read(&QueueAttributeName::ApproximateNumberOfMessagesNotVisible);
        match (visible, in_flight) {
            (None, None) => None,
            (v, f) => Some(v.unwrap_or(0).saturating_add(f.unwrap_or(0))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_identity_is_the_queue_url_not_the_stream_name() {
        // SQS has no consumer-group concept: every receiver on a queue
        // competes for the same messages, so the queue URL alone identifies
        // the physical subscription. The stream *name* would be wrong twice
        // over -- two queues can share a name across regions or accounts, and
        // it is caller-overridable via `SqsSourceConfig::stream`.
        let url = "https://sqs.us-east-1.amazonaws.com/123456789012/orders";
        assert_eq!(subscription_identity_for(url), format!("sqs:{url}"));
        // Same queue name, different account: distinct subscriptions.
        let other = "https://sqs.us-east-1.amazonaws.com/999999999999/orders";
        assert_ne!(
            subscription_identity_for(url),
            subscription_identity_for(other)
        );
    }

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
    fn message_id_is_the_redelivery_identity_not_the_dedup_id() {
        // AC3: the connector coordinate identifies *this message*, so it must
        // be the broker-assigned `MessageId` even when a FIFO dedup id is
        // present. See `coordinate_id`'s doc comment for why preferring the
        // dedup id silently drops valid events.
        assert_eq!(
            coordinate_id(Some("dedup-1"), Some("msg-1")),
            Some("msg-1".to_string())
        );
        // Only when the broker gave us no message id at all does the
        // producer's dedup id become the best available identity.
        assert_eq!(
            coordinate_id(Some("dedup-1"), None),
            Some("dedup-1".to_string())
        );
        assert_eq!(
            coordinate_id(None, Some("msg-1")),
            Some("msg-1".to_string())
        );
        // An empty string is not an identity on either side.
        assert_eq!(
            coordinate_id(Some("dedup-1"), Some("")),
            Some("dedup-1".to_string())
        );
        assert_eq!(coordinate_id(None, None), None);
        assert_eq!(coordinate_id(Some(""), Some("")), None);
    }

    #[test]
    fn a_reused_dedup_id_past_the_dedup_interval_yields_distinct_coordinates() {
        // The bug this preference order exists to prevent: a FIFO producer
        // legitimately reuses a `MessageDeduplicationId` after SQS's 5-minute
        // deduplication interval. SQS accepts it as a *new* message with a new
        // `MessageId`. Coordinating on the dedup id would collapse the two
        // onto one key, so the second would be acked as an idempotent replay
        // and its event silently dropped.
        let first = coordinate_id(Some("nightly-rollup"), Some("msg-aaa"));
        let second = coordinate_id(Some("nightly-rollup"), Some("msg-bbb"));
        assert_ne!(
            first, second,
            "two distinct SQS messages must never share a connector coordinate"
        );

        // A genuine redelivery — same message, same `MessageId` — still
        // collapses, which is the at-least-once guarantee doing its job.
        assert_eq!(
            coordinate_id(Some("nightly-rollup"), Some("msg-aaa")),
            first
        );
    }

    #[test]
    fn a_redrive_policy_is_what_makes_native_dead_lettering_real() {
        // SQS only moves a message to a DLQ when the queue has a redrive
        // policy naming one. Anything else -- absent, empty, or malformed --
        // means abandoning a poison message just redelivers it forever.
        assert!(redrive_policy_is_configured(Some(
            r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:orders-dlq","maxReceiveCount":"5"}"#
        )));
        assert!(!redrive_policy_is_configured(None));
        assert!(!redrive_policy_is_configured(Some("")));
        assert!(!redrive_policy_is_configured(Some("   ")));
        // Well-formed JSON that names no destination is not a redrive policy.
        assert!(!redrive_policy_is_configured(Some("{}")));
        assert!(!redrive_policy_is_configured(Some(
            r#"{"maxReceiveCount":"5"}"#
        )));
        // An empty destination is not a destination.
        assert!(!redrive_policy_is_configured(Some(
            r#"{"deadLetterTargetArn":""}"#
        )));
        // Unparseable attribute: fail closed rather than guess.
        assert!(!redrive_policy_is_configured(Some("not json")));
    }

    #[test]
    fn native_dead_lettering_fails_closed_until_a_redrive_policy_is_known() {
        // The safety property: an unprobed source must NOT claim a native
        // dead-letter destination, or `broker_native_dead_letter()` would be
        // accepted at build time and poison messages would loop forever.
        assert!(!native_dead_letter_supported(None));
        assert!(!native_dead_letter_supported(Some(false)));
        assert!(native_dead_letter_supported(Some(true)));
    }

    #[test]
    fn an_explicit_redrive_declaration_survives_into_the_source() {
        let client = || {
            Client::from_conf(
                aws_sdk_sqs::Config::builder()
                    .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
                    .region(aws_sdk_sqs::config::Region::new("us-east-1"))
                    .credentials_provider(aws_sdk_sqs::config::Credentials::for_tests())
                    .build(),
            )
        };

        // Undeclared and unprobed: fail closed.
        let unknown = SqsSource::new(client(), SqsSourceConfig::new("https://h/1/q"));
        assert!(!unknown.has_native_dead_letter());

        // Declared: the embedder's own knowledge is honoured, so a
        // bring-your-own-client source can still opt into broker-native DLQ.
        let declared = SqsSource::new(
            client(),
            SqsSourceConfig::new("https://h/1/q").has_redrive_policy(true),
        );
        assert!(declared.has_native_dead_letter());

        let declared_absent = SqsSource::new(
            client(),
            SqsSourceConfig::new("https://h/1/q").has_redrive_policy(false),
        );
        assert!(!declared_absent.has_native_dead_letter());
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
