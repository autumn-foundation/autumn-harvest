//! The HTTP transport of the provider models.
//!
//! [`Transport`] sends one JSON body to one endpoint. It reads the response
//! under a cap and refuses bytes that are not UTF-8. A status that is not
//! `2xx` becomes an [`ApiError`] of the kind in [`error_kind`].

use std::fmt;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use super::sse::SseFrames;
use crate::{AgentError, ErrorKind};

/// The most bytes of one response body that a [`Transport`] reads by default.
pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// How long one request may take by default, thinking included.
pub const REQUEST_TIMEOUT: Duration = Duration::from_mins(10);

/// How long a connection may take to open.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// The most characters of an error body that an [`ApiError`] keeps.
const ERROR_DETAIL_CHARS: usize = 500;

/// The most bytes of an error body that a [`Transport`] reads. It keeps only
/// [`ERROR_DETAIL_CHARS`] of them, so a larger read only costs memory.
const ERROR_BODY_BYTES: usize = 64 * 1024;

/// The error kind of an HTTP status.
///
/// The table is the retry table of the Anthropic and `OpenAI` SDKs. A timeout,
/// a conflict, a rate limit and a server fault retry. `529` is an overload,
/// so it retries too. A request that is too large is a `Budget` error.
#[must_use]
pub const fn error_kind(status: u16) -> ErrorKind {
    match status {
        429 => ErrorKind::RateLimited,
        401 | 403 => ErrorKind::Authentication,
        413 => ErrorKind::Budget,
        408 | 409 | 500..=599 => ErrorKind::Unavailable,
        _ => ErrorKind::Provider,
    }
}

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

/// A failed call to a provider API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    kind: ErrorKind,
    status: Option<u16>,
    message: String,
    body: Option<BodyError>,
}

impl ApiError {
    pub(crate) fn new(kind: ErrorKind, status: Option<u16>, message: impl Into<String>) -> Self {
        Self {
            kind,
            status,
            message: message.into(),
            body: None,
        }
    }

    /// The error of a body that could not be read.
    fn of_body(reason: BodyError, status: Option<u16>) -> Self {
        Self {
            body: Some(reason),
            ..Self::new(ErrorKind::Decode, status, reason.to_string())
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

/// An HTTP client for a provider API, with `timeout` for each request.
///
/// The client follows no redirect. A redirect would send the key header and
/// the whole prompt to the next host, because a redirect keeps a custom key
/// header.
///
/// The client uses `rustls` with the `ring` provider. The call installs that
/// provider as the process default when no provider is installed. An app
/// that needs another provider installs it first, then calls this, or
/// passes its own client to the model.
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

/// `value` as a sensitive header value, or a `Config` error that names
/// `what`.
///
/// A blank value, or one with a byte that a header cannot carry, fails each
/// request. That failure reads as retryable, so it is refused here.
///
/// # Errors
///
/// A `Config` error when `value` is blank or is not a valid header value.
pub fn secret_header(value: &str, what: &str) -> Result<HeaderValue, AgentError> {
    if value.trim().is_empty() {
        return Err(AgentError::new(
            ErrorKind::Config,
            format!("the {what} is blank"),
        ));
    }
    let mut header = HeaderValue::from_str(value).map_err(|_| {
        AgentError::new(
            ErrorKind::Config,
            format!("the {what} holds a byte that cannot go in an HTTP header"),
        )
    })?;
    header.set_sensitive(true);
    Ok(header)
}

/// One endpoint of a provider API: an HTTP client, the URL, the fixed
/// headers and a body cap.
#[derive(Clone)]
pub struct Transport {
    http: reqwest::Client,
    endpoint: String,
    headers: HeaderMap,
    max_body_bytes: usize,
}

/// The header values stay out of every log line. A key is one of them.
impl fmt::Debug for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.headers.keys().map(HeaderName::as_str).collect();
        f.debug_struct("Transport")
            .field("endpoint", &self.endpoint)
            .field("headers", &names)
            .field("header_values", &"<redacted>")
            .field("max_body_bytes", &self.max_body_bytes)
            .finish_non_exhaustive()
    }
}

impl Transport {
    /// A transport that posts to `endpoint` through `http`, with
    /// [`MAX_BODY_BYTES`].
    #[must_use]
    pub fn new(http: reqwest::Client, endpoint: impl Into<String>) -> Self {
        Self {
            http,
            endpoint: endpoint.into(),
            headers: HeaderMap::new(),
            max_body_bytes: MAX_BODY_BYTES,
        }
    }

    /// Send `value` as the header `name` on each request.
    #[must_use]
    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }

    /// Send each request through `http`.
    #[must_use]
    pub fn http(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    /// Post to `url` in place of the endpoint.
    #[must_use]
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = url.into();
        self
    }

    /// The endpoint.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.endpoint
    }

    /// Read at most `cap` bytes of each response body.
    #[must_use]
    pub const fn max_body_bytes(mut self, cap: usize) -> Self {
        self.max_body_bytes = cap;
        self
    }

    /// The body cap.
    #[must_use]
    pub const fn body_cap(&self) -> usize {
        self.max_body_bytes
    }

    /// Post `body` with the `extra` headers, and return the JSON of the
    /// response with its `2xx` status.
    ///
    /// # Errors
    ///
    /// A transport failure before a response has no status and is
    /// `Transport`. A status that is not `2xx` takes its kind from
    /// [`error_kind`]. A failure after a `2xx` has that status, so
    /// [`ApiError::accepted`] is true. A lost body is then `Transport`. A
    /// body that is too long, not text or not JSON is `Decode`.
    pub async fn json(&self, body: &Value, extra: &HeaderMap) -> Result<(u16, Value), ApiError> {
        let mut response = self.post(body, extra).await?;
        let status = response.status().as_u16();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| {
            ApiError::new(
                ErrorKind::Transport,
                Some(status),
                format!("its response was lost: {e}"),
            )
        })? {
            if push_capped(&mut bytes, &chunk, self.max_body_bytes) {
                break;
            }
        }
        let text = decode_body(bytes, self.max_body_bytes)
            .map_err(|reason| ApiError::of_body(reason, Some(status)))?;
        let value = serde_json::from_str(&text).map_err(|e| {
            ApiError::new(
                ErrorKind::Decode,
                Some(status),
                format!("its response is not JSON: {e}"),
            )
        })?;
        Ok((status, value))
    }

    /// Post `body` with the `extra` headers, and pass the `data` of each
    /// Server-Sent Event of the response to `on_data`. Returns the `2xx`
    /// status.
    ///
    /// The whole stream counts against the body cap. Each line must be
    /// UTF-8.
    ///
    /// # Errors
    ///
    /// As [`json`](Self::json). An error from `on_data` ends the stream and
    /// keeps its kind, with the status of the response.
    pub async fn events(
        &self,
        body: &Value,
        extra: &HeaderMap,
        mut on_data: impl FnMut(&str) -> Result<(), AgentError>,
    ) -> Result<u16, ApiError> {
        let mut response = self.post(body, extra).await?;
        let status = response.status().as_u16();
        let failed = |e: AgentError| ApiError::new(e.kind(), Some(status), e.message());
        let mut frames = SseFrames::default();
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
                let reason = BodyError::TooLong {
                    cap: self.max_body_bytes,
                };
                return Err(ApiError::of_body(reason, Some(status)));
            }
            for data in frames.push(&chunk).map_err(failed)? {
                on_data(&data).map_err(failed)?;
            }
        }
        for data in frames.end().map_err(failed)? {
            on_data(&data).map_err(failed)?;
        }
        Ok(status)
    }

    /// Post `body` and return a `2xx` response, or the API error.
    async fn post(&self, body: &Value, extra: &HeaderMap) -> Result<reqwest::Response, ApiError> {
        let bytes = serde_json::to_vec(body).map_err(|e| {
            ApiError::new(
                ErrorKind::Config,
                None,
                format!("the request is not JSON: {e}"),
            )
        })?;
        let mut headers = self.headers.clone();
        headers.extend(extra.clone());
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let response = self
            .http
            .post(&self.endpoint)
            .headers(headers)
            .body(bytes)
            .send()
            .await
            .map_err(|e| {
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
            error_kind(status),
            Some(status),
            format!("HTTP {status}: {detail}"),
        ))
    }
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use serde_json::json;

    use super::super::testing::{first, serve, serve_raw};
    use super::*;

    fn transport(url: &str) -> Transport {
        Transport::new(http_client(REQUEST_TIMEOUT).unwrap(), url)
    }

    #[test]
    fn the_status_table_is_the_sdk_retry_table() {
        for status in [408, 409, 429, 500, 502, 503, 504, 529] {
            assert!(error_kind(status).is_retryable(), "{status}");
        }
        for status in [400, 401, 403, 404, 413, 422, 425] {
            assert!(!error_kind(status).is_retryable(), "{status}");
        }
        assert_eq!(error_kind(429), ErrorKind::RateLimited);
        assert_eq!(error_kind(401), ErrorKind::Authentication);
        assert_eq!(error_kind(413), ErrorKind::Budget);
        assert_eq!(error_kind(400), ErrorKind::Provider);
    }

    #[tokio::test]
    async fn a_status_that_is_not_2xx_is_an_error_of_its_kind() {
        for (status, retryable) in [(429, true), (529, true), (400, false)] {
            let (url, _) = serve(status, br#"{"error":"x"}"#).await;
            let error = transport(&url)
                .json(&json!({}), &HeaderMap::new())
                .await
                .unwrap_err();
            assert_eq!(error.status(), Some(status));
            assert_eq!(error.kind().is_retryable(), retryable, "{status}");
            assert!(!error.accepted(), "{status} ran no turn");
        }
    }

    #[tokio::test]
    async fn the_fixed_and_the_extra_headers_go_out() {
        let (url, seen) = serve(200, b"{}").await;
        let mut extra = HeaderMap::new();
        extra.insert("x-extra", HeaderValue::from_static("two"));
        let (status, value) = transport(&url)
            .header(
                HeaderName::from_static("x-fixed"),
                HeaderValue::from_static("one"),
            )
            .json(&json!({"a": 1}), &extra)
            .await
            .unwrap();
        assert_eq!((status, value), (200, json!({})));
        let head = first(&seen);
        assert!(head.contains("x-fixed: one"), "{head}");
        assert!(head.contains("x-extra: two"), "{head}");
        assert!(head.contains("content-type: application/json"), "{head}");
    }

    #[tokio::test]
    async fn a_bad_body_after_a_2xx_is_an_accepted_decode_error() {
        for body in [&b"{"[..], b"\xff\xfe"] {
            let (url, _) = serve(200, body).await;
            let error = transport(&url)
                .json(&json!({}), &HeaderMap::new())
                .await
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Decode, "{error}");
            assert!(error.accepted(), "the turn was billed: {error}");
        }
    }

    #[tokio::test]
    async fn a_body_past_the_cap_is_refused_and_names_the_cap() {
        let (url, _) = serve(200, br#"{"long": "body"}"#).await;
        let error = transport(&url)
            .max_body_bytes(8)
            .json(&json!({}), &HeaderMap::new())
            .await
            .unwrap_err();
        assert_eq!(error.body_error(), Some(BodyError::TooLong { cap: 8 }));
        assert!(error.accepted());
    }

    #[tokio::test]
    async fn a_refused_connection_is_a_retryable_transport_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        drop(listener);
        let error = transport(&url)
            .json(&json!({}), &HeaderMap::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert_eq!(error.status(), None);
    }

    #[tokio::test]
    async fn a_redirect_is_not_followed() {
        // A redirect would send the key to the next host.
        let (other, seen) = serve(200, b"{}").await;
        let head = format!(
            "HTTP/1.1 307 X\r\nlocation: {other}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        );
        let (url, _) = serve_raw(head, Vec::new()).await;
        let error = transport(&url)
            .json(&json!({}), &HeaderMap::new())
            .await
            .unwrap_err();
        assert_eq!(error.status(), Some(307));
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
        let error = transport(&url)
            .json(&json!({}), &HeaderMap::new())
            .await
            .unwrap_err();
        assert!(!error.message().contains('\u{1b}'), "{}", error.message());
        assert!(error.message().contains("\\u{1b}"), "{}", error.message());
        assert!(error.message().len() < 1_000, "the detail is cut");
        assert_eq!(printable("a\u{202e}b"), "a\\u{202e}b");
    }

    #[tokio::test]
    async fn events_pass_each_data_and_keep_the_status() {
        let (url, _) = serve(200, b"data: 1\n\n: comment\n\ndata: 2\n\n").await;
        let mut seen = Vec::new();
        let status = transport(&url)
            .events(&json!({}), &HeaderMap::new(), |data| {
                seen.push(data.to_owned());
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!((status, seen), (200, vec!["1".to_owned(), "2".to_owned()]));
    }

    #[tokio::test]
    async fn an_event_error_or_a_long_stream_is_an_accepted_error() {
        let (url, _) = serve(200, b"data: 1\n\n").await;
        let error = transport(&url)
            .events(&json!({}), &HeaderMap::new(), |_| {
                Err(AgentError::new(ErrorKind::Unavailable, "busy"))
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert!(error.accepted());

        let (url, _) = serve(200, &b"data: x\n\n".repeat(20)).await;
        let error = transport(&url)
            .max_body_bytes(64)
            .events(&json!({}), &HeaderMap::new(), |_| Ok(()))
            .await
            .unwrap_err();
        assert_eq!(error.body_error(), Some(BodyError::TooLong { cap: 64 }));
    }

    #[test]
    fn a_secret_header_refuses_a_blank_value_or_a_newline() {
        for bad in ["", "  ", "k\nb", "k\rb", "k\u{0}b"] {
            let error = secret_header(bad, "API key").unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Config, "{bad:?}");
            assert!(error.message().contains("API key"));
        }
        assert!(secret_header("sk-ok_09", "API key").unwrap().is_sensitive());
    }

    #[test]
    fn the_debug_output_names_headers_and_hides_their_values() {
        let shown = format!(
            "{:?}",
            transport("http://x/").header(
                HeaderName::from_static("authorization"),
                secret_header("Bearer sk-secret", "API key").unwrap(),
            )
        );
        assert!(shown.contains("authorization"), "{shown}");
        assert!(!shown.contains("sk-secret"), "{shown}");
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
        let refused = decode_body(b"ok\xffno".to_vec(), 64).unwrap_err();
        assert_eq!(refused, BodyError::NotText { at: 2 });
    }
}
