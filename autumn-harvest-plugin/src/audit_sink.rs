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
///
/// **Deliberately below the exporter's default 60s claim lease**
/// ([`autumn_harvest::audit_export::DEFAULT_EXPORT_LEASE`]). A sink that can
/// outlive its lease has every attempt superseded by the next tick's claim, so
/// the cursor never advances while the sink receives the same batch forever.
/// Core bounds the call at the lease as a backstop, but a sink whose own
/// timeout fires first produces a much better error message. If you raise this
/// with [`ReqwestAuditSink::with_timeout`], raise
/// `HarvestBuilder::audit_export_lease(...)` to match.
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
                // `reqwest::Error`'s `Display` appends " for url (<url>)" with
                // the full URL, query string included (issue #953 review).
                // That string is persisted to `harvest_audit_export_cursor.
                // last_error`, served by `GET /admin/audit-export`, and logged
                // — so a SIEM endpoint that carries its ingest credential in
                // the URL (Sumo Logic's `/receiver/v1/http/<secret>`, a
                // `?dd-api-key=`, an `?api_key=`) would leak it into all
                // three, permanently. Report the error's own chain without the
                // URL instead.
                Err(error) => SinkAttempt::transport_error(describe_without_url(&error)),
            }
        })
    }
}

/// Render a `reqwest::Error` as a diagnostic that never contains the request
/// URL.
///
/// `reqwest::Error`'s own `Display` interpolates the URL; its *source* chain
/// (hyper/TLS/DNS) does not. So classify from the error's own predicates and
/// append the source chain, which is what actually says why it failed.
fn describe_without_url(error: &reqwest::Error) -> String {
    let kind = if error.is_timeout() {
        "request timed out"
    } else if error.is_connect() {
        "connection failed"
    } else if error.is_request() {
        "request could not be sent"
    } else if error.is_body() {
        "request body error"
    } else if error.is_decode() {
        "response decode error"
    } else {
        "transport error"
    };

    let mut detail = String::new();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        if !detail.is_empty() {
            detail.push_str(": ");
        }
        detail.push_str(&cause.to_string());
        source = cause.source();
    }

    if detail.is_empty() {
        kind.to_string()
    } else {
        format!("{kind}: {detail}")
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

    /// Minimal one-shot HTTP server: reads one request, replies with `status`,
    /// and hands the raw request back. Enough to prove what the sink actually
    /// puts on the wire without pulling in a mock-server dependency.
    async fn one_shot_server(status: u16) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        let handle = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut received = Vec::new();
            let mut buf = [0_u8; 4096];
            // Read until the body is complete: headers, then Content-Length
            // bytes. A tiny parser, but it keeps the assertions honest.
            loop {
                let n = socket.read(&mut buf).await.expect("read");
                if n == 0 {
                    break;
                }
                received.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&received).to_string();
                if let Some(header_end) = text.find("\r\n\r\n") {
                    let len: usize = text
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("content-length: ")
                                .or_else(|| line.strip_prefix("Content-Length: "))
                        })
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                    if received.len() >= header_end + 4 + len {
                        break;
                    }
                }
            }
            let response = format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\n\r\n");
            socket.write_all(response.as_bytes()).await.expect("write");
            socket.flush().await.expect("flush");
            String::from_utf8_lossy(&received).to_string()
        });
        (format!("http://{addr}/audit"), handle)
    }

    fn sample_record() -> AuditExportRecord {
        AuditExportRecord {
            shard: 0,
            seq: 1,
            id: uuid::Uuid::nil(),
            shard_id: Some(0),
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
        }
    }

    /// The sink must put the bytes core signed on the wire, untouched, with
    /// every header core computed — re-serializing here would invalidate the
    /// signature the receiver checks.
    #[tokio::test]
    async fn the_signed_body_and_headers_reach_the_wire_verbatim() {
        let record = sample_record();
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

        let (url, server) = one_shot_server(200).await;
        let sink = ReqwestAuditSink::new(url);
        let attempt = sink.deliver(&batch).await;
        assert!(attempt.is_success(), "200 is a success: {attempt:?}");

        let raw = server.await.expect("server task");
        let (head, sent_body) = raw.split_once("\r\n\r\n").expect("headers and body");
        assert_eq!(
            sent_body.as_bytes(),
            &body[..],
            "the body on the wire must be byte-identical to what was signed"
        );
        let lower = head.to_ascii_lowercase();
        assert!(
            lower.contains("content-type: application/x-ndjson"),
            "JSON lines must not be announced as application/json: {head}"
        );
        for (name, value) in &headers {
            assert!(
                lower.contains(&format!(
                    "{}: {}",
                    name.to_ascii_lowercase(),
                    value.to_ascii_lowercase()
                )),
                "header {name} must reach the wire: {head}"
            );
        }
    }

    /// A 3xx must surface as its literal status, never be followed. That is
    /// the whole point of `redirect::Policy::none()`: an allowlisted host
    /// answering with a pointer at an internal address must not be chased, and
    /// the non-2xx must flow into the backoff path so the cursor is held.
    #[tokio::test]
    async fn a_redirect_is_reported_as_a_failure_and_never_followed() {
        let record = sample_record();
        let body = serialize_batch(std::slice::from_ref(&record)).expect("serializes");
        let headers = export_headers(
            &CallbackSecret::new(Vec::new()),
            &body,
            0,
            1,
            1,
            chrono::Utc::now(),
        );
        let batch = AuditBatch {
            shard: 0,
            first_seq: 1,
            last_seq: 1,
            records: std::slice::from_ref(&record),
            body: &body,
            headers: &headers,
        };

        let (url, server) = one_shot_server(302).await;
        let sink = ReqwestAuditSink::new(url);
        let attempt = sink.deliver(&batch).await;
        let _ = server.await;

        assert_eq!(attempt.status, Some(302), "the literal status is reported");
        assert!(
            !attempt.is_success(),
            "a 3xx must never advance the export cursor"
        );
    }

    /// A transport error must never carry the sink URL: a SIEM endpoint that
    /// holds its ingest credential in the path or query would otherwise leak
    /// it into the cursor row, the admin API, and the logs.
    #[tokio::test]
    async fn a_transport_error_never_leaks_the_sink_url() {
        let record = sample_record();
        let body = serialize_batch(std::slice::from_ref(&record)).expect("serializes");
        let headers = export_headers(
            &CallbackSecret::new(Vec::new()),
            &body,
            0,
            1,
            1,
            chrono::Utc::now(),
        );
        let batch = AuditBatch {
            shard: 0,
            first_seq: 1,
            last_seq: 1,
            records: std::slice::from_ref(&record),
            body: &body,
            headers: &headers,
        };

        // Port 1 on loopback refuses immediately; the secret is in the path and
        // the query, exactly where real SIEM ingest credentials live.
        let sink = ReqwestAuditSink::new(
            "http://127.0.0.1:1/receiver/v1/http/SUPERSECRETTOKEN?api_key=ALSOSECRET",
        );
        let attempt = sink.deliver(&batch).await;

        let message = attempt.transport_error.expect("a transport error");
        assert!(!message.is_empty(), "the failure must still be described");
        for leak in [
            "SUPERSECRETTOKEN",
            "ALSOSECRET",
            "127.0.0.1:1",
            "/receiver/v1/http/",
        ] {
            assert!(
                !message.contains(leak),
                "the sink URL must never appear in a persisted error; found {leak:?} in \
                 {message:?}"
            );
        }
    }
}
