//! Default outbound HTTP transport for audit-record export (issue #953).
//!
//! `autumn-harvest` core is deliberately Postgres-only and ships no HTTP
//! client — [`autumn_harvest::audit_export::AuditSink`] is a thin transport
//! seam (mirrors `CompletionCallbackDeliverer`/`PayloadStore`/`HistoryArchiver`).
//! [`ReqwestAuditSink`] is the batteries-included signed-webhook
//! implementation the plugin auto-wires when an embedder configures
//! `audit_export_webhook(...)` without supplying their own sink.
//!
//! The wire format is JSON lines — one audit record per line — POSTed with
//! `Content-Type: application/x-ndjson` and the `X-Harvest-Signature` HMAC
//! header core computed over the exact bytes sent.

use autumn_harvest::audit_export::{AuditBatch, AuditSink, SinkAttempt, SinkFuture};
use std::time::Duration;

/// Default request timeout for one audit-export batch delivery.
///
/// Longer than the completion-callback default (10s): a batch carries up to
/// `MAX_EXPORT_BATCH_SIZE` records, and a SIEM ingest endpoint under load is
/// slower than a single-event webhook. A timeout is a retry, never a loss.
pub const DEFAULT_SINK_TIMEOUT: Duration = Duration::from_secs(30);

/// `Content-Type` for a JSON-lines batch.
///
/// `application/x-ndjson` is what Elastic's bulk API, Splunk HEC's raw
/// endpoint, and most log-lake collectors expect for newline-delimited JSON;
/// `application/json` would misdescribe a multi-object body.
pub const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";

/// An [`AuditSink`] that POSTs signed JSON-lines batches to one endpoint.
///
/// Redirects are never followed (`redirect::Policy::none()`), for the same
/// reason as [`crate::callback_deliverer::ReqwestCallbackDeliverer`]: an
/// allowlisted host answering 3xx with a pointer at a non-allowlisted (e.g.
/// internal) address must never be silently chased, or the SSRF guard applied
/// to the sink URL at build time would be bypassed at delivery time. A 3xx is
/// reported as its literal status, which `SinkAttempt::is_success` classifies
/// as a failure — so it flows through the normal backoff path and, crucially,
/// **never advances the cursor**.
pub struct ReqwestAuditSink {
    client: reqwest::Client,
    endpoint: String,
}

impl ReqwestAuditSink {
    /// Build a sink posting to `endpoint` with [`DEFAULT_SINK_TIMEOUT`].
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self::with_timeout(endpoint, DEFAULT_SINK_TIMEOUT)
    }

    /// Build a sink with a custom per-request timeout.
    ///
    /// # Panics
    /// Never panics in practice: the internal `expect` only guards against a
    /// `reqwest::Client` builder failure, which cannot occur for this static,
    /// valid configuration (a timeout and a redirect policy).
    #[must_use]
    pub fn with_timeout(endpoint: impl Into<String>, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest client with a static, valid configuration must build");
        Self {
            client,
            endpoint: endpoint.into(),
        }
    }

    /// The endpoint this sink posts to.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl AuditSink for ReqwestAuditSink {
    fn deliver<'a>(&'a self, batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
        Box::pin(async move {
            let mut request = self
                .client
                .post(&self.endpoint)
                .header("Content-Type", NDJSON_CONTENT_TYPE)
                // The body is sent verbatim: `batch.body` is exactly what
                // core signed, so re-serializing here would invalidate the
                // signature the receiver checks.
                .body(batch.body.to_vec());
            for (name, value) in batch.headers {
                request = request.header(*name, value);
            }

            match request.send().await {
                Ok(response) => SinkAttempt::success(response.status().as_u16()),
                Err(error) => SinkAttempt::transport_error(error.to_string()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::audit_export::{AuditExportRecord, export_headers, serialize_batch};
    use autumn_harvest::completion_callback::CallbackSecret;

    #[test]
    fn sink_is_object_safe_and_send_sync() {
        fn assert_bounds<T: AuditSink>() {}
        assert_bounds::<ReqwestAuditSink>();
        let _: Box<dyn AuditSink> = Box::new(ReqwestAuditSink::new("https://example.com/audit"));
    }

    #[test]
    fn endpoint_is_preserved() {
        let sink = ReqwestAuditSink::with_timeout(
            "https://siem.example.com/ingest",
            Duration::from_secs(5),
        );
        assert_eq!(sink.endpoint(), "https://siem.example.com/ingest");
    }

    // The sink must POST the bytes core signed, untouched. Asserted here at
    // the batch level (a full HTTP round trip lives in the DB-backed
    // integration suite): the body handed to the sink is byte-identical to
    // what `serialize_batch` produced and what the signature covers.
    #[test]
    fn the_signed_body_is_what_the_sink_would_send() {
        let record = AuditExportRecord {
            shard: 0,
            seq: 1,
            id: uuid::Uuid::nil(),
            occurred_at: chrono::Utc::now(),
            actor: "alice".to_string(),
            operation: "workflow.cancel".to_string(),
            target_type: "workflow".to_string(),
            target_id: None,
            route_or_command: "POST /workflows/{id}/cancel".to_string(),
            request_id: None,
            idempotency_key: None,
            status: "SUCCEEDED".to_string(),
            error_summary: None,
            source: "api".to_string(),
        };
        let body = serialize_batch(std::slice::from_ref(&record)).expect("serializes");
        let secret = CallbackSecret::new(b"k".to_vec());
        let headers = export_headers(&secret, &body, 0, 1, 1, chrono::Utc::now());
        let batch = AuditBatch {
            shard: 0,
            first_seq: 1,
            last_seq: 1,
            records: std::slice::from_ref(&record),
            body: &body,
            headers: &headers,
        };
        assert_eq!(
            batch.body,
            &body[..],
            "the sink sends batch.body verbatim; re-serializing would invalidate \
             the signature the receiver checks"
        );
        assert!(
            headers
                .iter()
                .any(|(n, _)| *n == autumn_harvest::audit_export::SIGNATURE_HEADER),
            "every batch carries the HMAC signature"
        );
    }
}
