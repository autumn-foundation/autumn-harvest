//! One POST to the Messages API, with a capped and strict body read.

use std::fmt;
use std::time::Duration;

use serde_json::Value;

use super::sse::{SseFrames, StreamAssembler};
use super::wire::{self, API_VERSION, ENDPOINT, FALLBACK_BETA};
use crate::{AgentError, ErrorKind};

/// The most bytes of one response body that a [`Client`] reads by default.
pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// How long one request may take by default, thinking included.
pub const REQUEST_TIMEOUT: Duration = Duration::from_mins(10);

/// How long a connection may take to open.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// The most characters of an error body that an [`ApiError`] keeps.
const ERROR_DETAIL_CHARS: usize = 500;

/// The most bytes of an error body that a [`Client`] reads. It keeps only
/// [`ERROR_DETAIL_CHARS`] of them, so a larger read only costs memory.
const ERROR_BODY_BYTES: usize = 64 * 1024;

/// The first [`ERROR_DETAIL_CHARS`] of `text`, with each control and bidi
/// character escaped.
///
/// An error body comes from the API or from a proxy before it. It goes
/// into an error message, and from there into history and logs. An escape
/// character there could rewrite the terminal of an operator.
#[must_use]
pub fn printable(text: &str) -> String {
    text.chars()
        .take(ERROR_DETAIL_CHARS)
        .flat_map(|c| {
            let hidden = c.is_control()
                || matches!(c, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}');
            let shown: Vec<char> = if hidden {
                c.escape_unicode().collect()
            } else {
                vec![c]
            };
            shown
        })
        .collect()
}

/// Why a response body cannot be read as text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyError {
    /// The body is longer than the cap.
    TooLong {
        /// The cap, in bytes.
        cap: usize,
    },
    /// The byte at this offset begins no UTF-8 character.
    NotText {
        /// The offset of the first bad byte.
        at: usize,
    },
}

impl fmt::Display for BodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong { cap } => write!(
                f,
                "its response is longer than the {cap} bytes this client reads"
            ),
            Self::NotText { at } => write!(
                f,
                "its response is not UTF-8 text: the byte at {at} begins no character"
            ),
        }
    }
}

impl std::error::Error for BodyError {}

/// Append `chunk` to `body`, up to one byte past `cap`.
///
/// Returns `true` when the body has passed the cap, which ends the read. The
/// extra byte tells a body that ends at the cap from a body that continues.
/// A body that continues past the cap is refused, and never read as its
/// prefix. JSON allows trailing space, so a prefix can parse.
pub fn push_capped(body: &mut Vec<u8>, chunk: &[u8], cap: usize) -> bool {
    let room = cap.saturating_sub(body.len()).saturating_add(1);
    if chunk.len() >= room {
        body.extend_from_slice(&chunk[..room]);
        return true;
    }
    body.extend_from_slice(chunk);
    false
}

/// Read `body` as text, or refuse it.
///
/// The decode is strict. A lossy decode puts U+FFFD in place of a bad byte.
/// That can turn a damaged body into valid JSON with a value that the model
/// did not send, such as another tool path.
///
/// # Errors
///
/// [`BodyError::TooLong`] when the body is longer than `cap`.
/// [`BodyError::NotText`] when its bytes are not UTF-8.
pub fn decode_body(body: Vec<u8>, cap: usize) -> Result<String, BodyError> {
    if body.len() > cap {
        return Err(BodyError::TooLong { cap });
    }
    String::from_utf8(body).map_err(|e| BodyError::NotText {
        at: e.utf8_error().valid_up_to(),
    })
}

/// A failed Messages API call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    kind: ErrorKind,
    status: Option<u16>,
    message: String,
    body: Option<BodyError>,
}

impl ApiError {
    fn new(kind: ErrorKind, status: Option<u16>, message: impl Into<String>) -> Self {
        Self {
            kind,
            status,
            message: message.into(),
            body: None,
        }
    }

    /// The category. [`ErrorKind::is_retryable`] reads it.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The HTTP status, when a response arrived.
    #[must_use]
    pub const fn status(&self) -> Option<u16> {
        self.status
    }

    /// Did the API accept the request?
    ///
    /// An accepted request ran and was billed. The failure came after the
    /// status, for example in the body. A retry pays for the turn again.
    #[must_use]
    pub fn accepted(&self) -> bool {
        self.status
            .is_some_and(|status| (200..300).contains(&status))
    }

    /// The reason that the body could not be read, when that is the failure.
    #[must_use]
    pub const fn body_error(&self) -> Option<BodyError> {
        self.body
    }

    /// The message, without the category.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{:?}] {}", self.kind, self.message)
    }
}

impl std::error::Error for ApiError {}

impl From<ApiError> for AgentError {
    fn from(error: ApiError) -> Self {
        Self::new(error.kind, error.message)
    }
}

/// An HTTP client for the Messages API, with `timeout` for each request.
///
/// The client follows no redirect. A redirect would send the key header and
/// the whole prompt to the next host, because a redirect keeps `x-api-key`.
///
/// The client uses `rustls` with the `ring` provider. The call installs that
/// provider as the process default when no provider is installed. An app
/// that needs another provider installs it first, then calls this, or
/// passes its own client to [`Client::with_http`].
///
/// # Errors
///
/// A `Config` error when the client cannot be built.
pub fn http_client(timeout: Duration) -> Result<reqwest::Client, AgentError> {
    // An `Err` means that a provider is already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| AgentError::new(ErrorKind::Config, format!("no HTTP client: {e}")))
}

/// A Messages API client: an API key, an HTTP client and a body cap.
#[derive(Clone)]
pub struct Client {
    api_key: reqwest::header::HeaderValue,
    http: reqwest::Client,
    endpoint: String,
    max_body_bytes: usize,
}

/// The key stays out of every log line.
impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("api_key", &"<redacted>")
            .field("endpoint", &self.endpoint)
            .field("max_body_bytes", &self.max_body_bytes)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// A client with `api_key`, [`REQUEST_TIMEOUT`] and [`MAX_BODY_BYTES`].
    ///
    /// # Errors
    ///
    /// The errors of [`with_http`](Self::with_http), and a `Config` error
    /// when the HTTP client cannot be built.
    pub fn new(api_key: &str) -> Result<Self, AgentError> {
        Self::with_http(api_key, http_client(REQUEST_TIMEOUT)?)
    }

    /// A client with `api_key` that sends each request through `http`.
    ///
    /// Build `http` with [`http_client`], or turn off its redirects. A
    /// redirect sends the key header to the next host.
    ///
    /// # Errors
    ///
    /// A `Config` error when the key is blank, or when it holds a byte that
    /// an HTTP header cannot carry, such as an interior newline. Each request
    /// would fail with that key, and the failure would read as retryable.
    pub fn with_http(api_key: &str, http: reqwest::Client) -> Result<Self, AgentError> {
        Ok(Self {
            api_key: key_header(api_key)?,
            http,
            endpoint: ENDPOINT.to_owned(),
            max_body_bytes: MAX_BODY_BYTES,
        })
    }

    /// Read at most `cap` bytes of each response body.
    #[must_use]
    pub const fn max_body_bytes(mut self, cap: usize) -> Self {
        self.max_body_bytes = cap;
        self
    }

    /// Send each request to `url` in place of [`ENDPOINT`], for example a
    /// gateway or a local test server. Use `https` for a remote host: the
    /// key travels in a header.
    #[must_use]
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = url.into();
        self
    }

    /// The body cap.
    #[must_use]
    pub const fn body_cap(&self) -> usize {
        self.max_body_bytes
    }

    /// Send one request `body` and return the message that the API returns.
    ///
    /// # Errors
    ///
    /// An [`ApiError`]. A transport failure before a response has no status
    /// and is retryable. A status that is not `2xx` takes its kind from
    /// [`wire::error_kind`]. A failure after a `2xx` has that status, so
    /// [`ApiError::accepted`] is true. A lost body is then `Transport`. A
    /// body that is too long, not text, not JSON or not a message is
    /// `Decode`.
    pub async fn create(&self, body: &Value) -> Result<Value, ApiError> {
        let response = self.send(body).await?;
        let status = response.status().as_u16();
        let text = self.read(response).await.map_err(|mut e| {
            e.status = Some(status);
            e
        })?;
        let message: Value = serde_json::from_str(&text).map_err(|e| {
            ApiError::new(
                ErrorKind::Decode,
                Some(status),
                format!("its response is not JSON: {e}"),
            )
        })?;
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
        let mut response = self.send(&body).await?;
        let status = response.status().as_u16();
        let failed = |e: AgentError| ApiError::new(e.kind(), Some(status), e.message());
        let mut frames = SseFrames::default();
        let mut stream = StreamAssembler::default();
        let mut read = 0_usize;
        while let Some(chunk) = response.chunk().await.map_err(|e| {
            ApiError::new(
                ErrorKind::Transport,
                Some(status),
                format!("the stream was lost: {e}"),
            )
        })? {
            read = read.saturating_add(chunk.len());
            if read > self.max_body_bytes {
                let cap = BodyError::TooLong {
                    cap: self.max_body_bytes,
                };
                let mut error = ApiError::new(ErrorKind::Decode, Some(status), cap.to_string());
                error.body = Some(cap);
                return Err(error);
            }
            for data in frames.push(&chunk).map_err(failed)? {
                stream.event(&data, on_delta).map_err(failed)?;
            }
        }
        for data in frames.end().map_err(failed)? {
            stream.event(&data, on_delta).map_err(failed)?;
        }
        check_message(stream.finish().map_err(failed)?, status)
    }

    /// Send `body` and return a `2xx` response, or the API error.
    async fn send(&self, body: &Value) -> Result<reqwest::Response, ApiError> {
        let bytes = serde_json::to_vec(body).map_err(|e| {
            ApiError::new(
                ErrorKind::Config,
                None,
                format!("the request is not JSON: {e}"),
            )
        })?;
        let mut call = self
            .http
            .post(&self.endpoint)
            .header("x-api-key", self.api_key.clone())
            .header("anthropic-version", API_VERSION)
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        if body.get("fallbacks").is_some() {
            call = call.header("anthropic-beta", FALLBACK_BETA);
        }
        let response = call.body(bytes).send().await.map_err(|e| {
            ApiError::new(
                ErrorKind::Transport,
                None,
                format!("the request failed: {e}"),
            )
        })?;
        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(response);
        }
        // The body names the error. It holds no secret: the key is a header.
        let detail = read_error(response).await;
        Err(ApiError::new(
            wire::error_kind(status),
            Some(status),
            format!("HTTP {status}: {detail}"),
        ))
    }

    /// Read one body under the cap, strictly. The error has no status yet.
    async fn read(&self, mut response: reqwest::Response) -> Result<String, ApiError> {
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| {
            ApiError::new(
                ErrorKind::Transport,
                None,
                format!("its response was lost: {e}"),
            )
        })? {
            if push_capped(&mut body, &chunk, self.max_body_bytes) {
                break;
            }
        }
        decode_body(body, self.max_body_bytes).map_err(|reason| {
            let mut error = ApiError::new(ErrorKind::Decode, None, reason.to_string());
            error.body = Some(reason);
            error
        })
    }
}

/// The key as a sensitive header value, or a `Config` error.
fn key_header(api_key: &str) -> Result<reqwest::header::HeaderValue, AgentError> {
    if api_key.trim().is_empty() {
        return Err(AgentError::new(ErrorKind::Config, "the API key is blank"));
    }
    let mut header = reqwest::header::HeaderValue::from_str(api_key).map_err(|_| {
        AgentError::new(
            ErrorKind::Config,
            "the API key holds a byte that cannot go in an HTTP header",
        )
    })?;
    header.set_sensitive(true);
    Ok(header)
}

/// The printable start of an error body, read up to [`ERROR_BODY_BYTES`].
///
/// The text only names the error, and no code acts on it, so a lossy decode
/// is safe here.
async fn read_error(mut response: reqwest::Response) -> String {
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if push_capped(&mut body, &chunk, ERROR_BODY_BYTES) {
                    break;
                }
            }
            Err(_) if body.is_empty() => return "the error body could not be read".to_owned(),
            Ok(None) | Err(_) => break,
        }
    }
    printable(&String::from_utf8_lossy(&body))
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

    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// A one-shot HTTP server. It answers with `head` and `body`, and keeps
    /// the head of the request it got. It reads the whole request first, so
    /// no unread byte resets the connection.
    async fn serve_raw(head: String, body: Vec<u8>) -> (String, Arc<Mutex<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/messages", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(String::new()));
        let kept = seen.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = socket.read(&mut buffer).await.unwrap();
                request.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&request).to_lowercase();
                let whole = text.find("\r\n\r\n").is_some_and(|end| {
                    let length = text
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: "))
                        .and_then(|n| n.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    request.len() >= end + 4 + length
                });
                if whole || read == 0 {
                    *kept.lock().unwrap() = text;
                    break;
                }
            }
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        });
        (url, seen)
    }

    /// A one-shot server that answers with `status` and `body`.
    async fn serve(status: u16, body: &[u8]) -> (String, Arc<Mutex<String>>) {
        let head = format!(
            "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        serve_raw(head, body.to_vec()).await
    }

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
        let head = seen.lock().unwrap().clone();
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
        let head = seen.lock().unwrap().clone();
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

    #[tokio::test]
    async fn a_body_past_the_cap_is_refused_and_names_the_cap() {
        let (url, _) = serve(200, message().to_string().as_bytes()).await;
        let error = client(&url)
            .max_body_bytes(8)
            .create(&json!({}))
            .await
            .unwrap_err();
        assert_eq!(error.body_error(), Some(BodyError::TooLong { cap: 8 }));
        assert!(error.accepted());
    }

    #[tokio::test]
    async fn a_refused_connection_is_a_retryable_transport_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        drop(listener);
        let error = client(&url).create(&json!({})).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert_eq!(error.status(), None);
        assert!(error.kind().is_retryable());
    }

    #[tokio::test]
    async fn a_redirect_is_not_followed() {
        // A redirect would send the key to the next host.
        let (other, seen) = serve(200, message().to_string().as_bytes()).await;
        let head = format!(
            "HTTP/1.1 307 X\r\nlocation: {other}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        );
        let (url, _) = serve_raw(head, Vec::new()).await;
        let error = client(&url).create(&json!({})).await.unwrap_err();
        assert_eq!(error.status(), Some(307));
        assert!(!error.kind().is_retryable());
        assert!(
            seen.lock().unwrap().is_empty(),
            "the other host got nothing"
        );
    }

    #[tokio::test]
    async fn an_error_body_is_cut_and_its_control_characters_escaped() {
        let mut body = b"bad \x1b[2J request ".to_vec();
        body.extend(std::iter::repeat_n(b'x', 200_000));
        let (url, _) = serve(400, &body).await;
        let error = client(&url).create(&json!({})).await.unwrap_err();
        assert!(!error.message().contains('\u{1b}'), "{}", error.message());
        assert!(error.message().contains("\\u{1b}"), "{}", error.message());
        assert!(error.message().len() < 1_000, "the detail is cut");
        assert_eq!(printable("a\u{202e}b"), "a\\u{202e}b");
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
        let request = seen.lock().unwrap().clone();
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

    #[test]
    fn the_read_takes_one_byte_past_the_cap_and_no_more() {
        let mut body = Vec::new();
        assert!(!push_capped(&mut body, b"abcd", 4), "the cap is not passed");
        assert!(push_capped(&mut body, b"ef", 4), "the cap is passed");
        assert_eq!(body, b"abcde");
        assert_eq!(
            decode_body(body, 4).unwrap_err(),
            BodyError::TooLong { cap: 4 }
        );
        assert_eq!(decode_body(b"abcd".to_vec(), 4).unwrap(), "abcd");
    }

    #[test]
    fn bytes_that_are_not_utf8_are_refused_with_their_offset() {
        let refused = decode_body(b"ok\xffno".to_vec(), 64).unwrap_err();
        assert_eq!(refused, BodyError::NotText { at: 2 });
        assert!(refused.to_string().contains("begins no character"));
    }
}
