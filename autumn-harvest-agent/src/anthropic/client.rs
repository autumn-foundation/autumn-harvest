//! One POST to the Messages API, over the shared transport.

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use super::sse::StreamAssembler;
use super::wire::{self, API_VERSION, ENDPOINT, FALLBACK_BETA};
use crate::provider::{self, ApiError, REQUEST_TIMEOUT, Transport};
use crate::{AgentError, ErrorKind};

/// A Messages API client: an API key, an HTTP client and a body cap.
///
/// The transport is [`provider::Transport`]. It follows no redirect, reads
/// each body under a cap and refuses bytes that are not UTF-8.
#[derive(Clone, Debug)]
pub struct Client {
    transport: Transport,
}

impl Client {
    /// A client with `api_key`, [`REQUEST_TIMEOUT`] and
    /// [`MAX_BODY_BYTES`](provider::MAX_BODY_BYTES).
    ///
    /// # Errors
    ///
    /// The errors of [`with_http`](Self::with_http), and a `Config` error
    /// when the HTTP client cannot be built.
    pub fn new(api_key: &str) -> Result<Self, AgentError> {
        Self::with_http(api_key, provider::http_client(REQUEST_TIMEOUT)?)
    }

    /// A client with `api_key` that sends each request through `http`.
    ///
    /// Build `http` with [`http_client`](provider::http_client), or turn off
    /// its redirects. A redirect sends the key header to the next host.
    ///
    /// # Errors
    ///
    /// A `Config` error when the key is blank, or when it holds a byte that
    /// an HTTP header cannot carry, such as an interior newline. Each request
    /// would fail with that key, and the failure would read as retryable.
    pub fn with_http(api_key: &str, http: reqwest::Client) -> Result<Self, AgentError> {
        let transport = Transport::new(http, ENDPOINT)
            .header(
                HeaderName::from_static("x-api-key"),
                provider::secret_header(api_key, "API key")?,
            )
            .header(
                HeaderName::from_static("anthropic-version"),
                HeaderValue::from_static(API_VERSION),
            );
        Ok(Self { transport })
    }

    /// Read at most `cap` bytes of each response body.
    #[must_use]
    pub fn max_body_bytes(mut self, cap: usize) -> Self {
        self.transport = self.transport.max_body_bytes(cap);
        self
    }

    /// Send each request to `url` in place of [`ENDPOINT`], for example a
    /// gateway or a local test server. Use `https` for a remote host: the
    /// key travels in a header.
    #[must_use]
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.transport = self.transport.endpoint(url);
        self
    }

    /// The body cap.
    #[must_use]
    pub const fn body_cap(&self) -> usize {
        self.transport.body_cap()
    }

    /// Send one request `body` and return the message that the API returns.
    ///
    /// # Errors
    ///
    /// The errors of [`Transport::json`]. A body that is not a message is
    /// `Decode`, with the `2xx` status.
    pub async fn create(&self, body: &Value) -> Result<Value, ApiError> {
        let (status, message) = self.transport.json(body, &extra(body)).await?;
        check_message(message, status)
    }

    /// Send one request `body` as a stream, and return the message.
    ///
    /// The call sets `"stream": true` and asks for eager input streaming on
    /// each tool, as [`streaming_body`](super::streaming_body) does.
    /// `on_delta` gets each text delta, and `""` at each other event. The
    /// returned message has the shape of a [`create`](Self::create) answer.
    ///
    /// # Errors
    ///
    /// As [`create`](Self::create). A body that is not an object is a
    /// `Config` error. An `error` event takes its kind from
    /// [`wire::error_type_kind`]. A stream that ends before `message_stop`
    /// is `Transport`. A line that is not UTF-8 is `Decode`.
    pub async fn stream(
        &self,
        body: Value,
        on_delta: &(dyn Fn(&str) + Send + Sync),
    ) -> Result<Value, ApiError> {
        if !body.is_object() {
            return Err(ApiError::new(
                ErrorKind::Config,
                None,
                "the request body is not a JSON object",
            ));
        }
        let body = super::chat::streaming_body(body);
        let mut stream = StreamAssembler::default();
        let status = self
            .transport
            .events(&body, &extra(&body), |data| stream.event(data, on_delta))
            .await?;
        let message = stream
            .finish()
            .map_err(|e| ApiError::new(e.kind(), Some(status), e.message()))?;
        check_message(message, status)
    }
}

/// The headers that depend on the body: the fallback beta for a body that
/// asks for fallbacks.
fn extra(body: &Value) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if body.get("fallbacks").is_some() {
        headers.insert(
            HeaderName::from_static("anthropic-beta"),
            HeaderValue::from_static(FALLBACK_BETA),
        );
    }
    headers
}

/// Refuse a parsed body that is not a message.
fn check_message(message: Value, status: u16) -> Result<Value, ApiError> {
    if wire::is_message(&message) {
        Ok(message)
    } else {
        Err(ApiError::new(
            ErrorKind::Decode,
            Some(status),
            "its response is not a message: `content` and a `stop_reason` are required",
        ))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use serde_json::json;

    use super::*;
    use crate::provider::BodyError;
    use crate::provider::testing::{first, serve};

    fn client(url: &str) -> Client {
        Client::new("sk-ant-test").unwrap().endpoint(url)
    }

    fn message() -> Value {
        json!({"type": "message", "content": [{"type": "text", "text": "hi"}],
               "stop_reason": "end_turn", "usage": {"input_tokens": 1, "output_tokens": 1}})
    }

    #[tokio::test]
    async fn a_429_or_a_529_is_retryable_and_a_400_is_not() {
        for (status, retryable) in [(429, true), (529, true), (400, false)] {
            let (url, _) = serve(status, br#"{"type":"error"}"#).await;
            let error = client(&url).create(&json!({})).await.unwrap_err();
            assert_eq!(error.status(), Some(status));
            assert_eq!(error.kind().is_retryable(), retryable, "{status}");
            assert!(!error.accepted(), "{status} ran no turn");
            let agent: AgentError = error.into();
            assert_eq!(agent.kind().is_retryable(), retryable, "{status}");
        }
    }

    #[tokio::test]
    async fn a_message_comes_back_with_the_version_key_and_no_beta() {
        let (url, seen) = serve(200, message().to_string().as_bytes()).await;
        let body = client(&url).create(&json!({"model": "m"})).await.unwrap();
        assert_eq!(body["content"][0]["text"], "hi");
        let head = first(&seen);
        assert!(head.contains(&format!("anthropic-version: {API_VERSION}")));
        assert!(head.contains("x-api-key: sk-ant-test"));
        assert!(!head.contains("anthropic-beta"), "{head}");
    }

    #[tokio::test]
    async fn a_body_with_fallbacks_sends_the_beta() {
        let (url, seen) = serve(200, message().to_string().as_bytes()).await;
        client(&url)
            .create(&json!({"fallbacks": "default"}))
            .await
            .unwrap();
        let head = first(&seen);
        assert!(head.contains(&format!("anthropic-beta: {FALLBACK_BETA}")));
    }

    #[tokio::test]
    async fn a_bad_body_after_a_2xx_is_an_accepted_decode_error() {
        for body in [&b"{"[..], b"{\"content\": []}", b"\xff\xfe"] {
            let (url, _) = serve(200, body).await;
            let error = client(&url).create(&json!({})).await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Decode, "{error}");
            assert!(error.accepted(), "the turn was billed: {error}");
        }
    }

    fn sse(events: &[Value]) -> Vec<u8> {
        events
            .iter()
            .fold(String::new(), |wire, e| {
                wire + "data: " + &e.to_string() + "\n\n"
            })
            .into_bytes()
    }

    fn start() -> Value {
        json!({"type": "message_start", "message": {"type": "message", "role": "assistant",
            "content": [], "stop_reason": null, "usage": {"input_tokens": 1, "output_tokens": 1}}})
    }

    #[tokio::test]
    async fn a_stream_asks_for_eager_input_and_returns_the_message() {
        let events = [
            start(),
            json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}}),
            json!({"type": "message_stop"}),
        ];
        let (url, seen) = serve(200, &sse(&events)).await;
        let body = json!({"tools": [{"name": "t"}]});
        let message = client(&url).stream(body, &|_| {}).await.unwrap();
        assert_eq!(message["stop_reason"], "end_turn");
        let request = first(&seen);
        assert!(request.contains("\"stream\":true"), "{request}");
        assert!(
            request.contains("\"eager_input_streaming\":true"),
            "{request}"
        );
    }

    #[tokio::test]
    async fn a_stream_error_an_early_end_or_a_long_stream_is_an_accepted_error() {
        let overloaded = sse(&[
            start(),
            json!({"type": "error", "error": {"type": "overloaded_error", "message": "busy"}}),
        ]);
        let (url, _) = serve(200, &overloaded).await;
        let error = client(&url).stream(json!({}), &|_| {}).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert!(error.accepted());

        let (url, _) = serve(200, &sse(&[start()])).await;
        let error = client(&url).stream(json!({}), &|_| {}).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport, "the stream ended early");

        let (url, _) = serve(200, &sse(&[start(), start(), start()])).await;
        let error = client(&url)
            .max_body_bytes(64)
            .stream(json!({}), &|_| {})
            .await
            .unwrap_err();
        assert_eq!(error.body_error(), Some(BodyError::TooLong { cap: 64 }));
    }

    #[tokio::test]
    async fn a_body_that_is_not_an_object_is_refused_before_a_request() {
        let error = client("http://127.0.0.1:9/")
            .stream(json!([1]), &|_| {})
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Config);
        assert_eq!(error.status(), None);
    }

    #[test]
    fn a_client_with_its_own_http_client_checks_the_key_too() {
        let http = reqwest::Client::new();
        assert!(Client::with_http("sk-ant-a\nb", http.clone()).is_err());
        assert!(Client::with_http("sk-ant-ok", http).is_ok());
    }

    #[test]
    fn a_blank_key_or_a_key_with_a_newline_is_refused() {
        for bad in ["", "  ", "sk-ant-a\nb", "sk-ant-a\rb", "sk-ant-a\u{0}b"] {
            let error = Client::new(bad).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Config, "{bad:?}");
        }
        assert!(Client::new("sk-ant-api03-aAbB09_-").is_ok());
    }

    #[test]
    fn the_key_stays_out_of_debug_output() {
        let shown = format!("{:?}", Client::new("sk-ant-secret").unwrap());
        assert!(!shown.contains("sk-ant-secret"), "{shown}");
        assert!(shown.contains("redacted"));
    }
}
