//! JSON-RPC client for remote MCP and A2A tasks (issue #2006).
//!
//! [`HttpRemoteTasks`] implements
//! [`RemoteTaskTransport`](autumn_harvest::remote_task::RemoteTaskTransport)
//! over HTTP. Wrap it in
//! [`RemoteTasks`](autumn_harvest::remote_task::RemoteTasks) and pass that to
//! `HarvestBuilder::state`. See `docs/remote-tasks.md`.
//!
//! - MCP: `tools/call` with the Tasks extension, then `tasks/get`. Each
//!   request sends the `MCP-Protocol-Version`, `Mcp-Method` and `Mcp-Name`
//!   headers that a `2026-07-28` server needs.
//! - A2A `v0.3`: `message/send`, then `tasks/get`.
//!
//! The start sends the idempotency key in the `Idempotency-Key` header too.
//! A network error, HTTP 429 and HTTP 5xx are retryable. Any other HTTP
//! error and a JSON-RPC error are not.
//!
//! The client reads at most [`MAX_RESPONSE_BYTES`] of a response. It refuses
//! a response from another origin than the endpoint. Its error messages
//! hold no URL, so a secret in the endpoint query stays out of history.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use autumn_harvest::remote_task::{
    RemoteFuture, RemoteProtocol, RemoteTaskError, RemoteTaskHandle, RemoteTaskPoll,
    RemoteTaskRequest, RemoteTaskStart, RemoteTaskTransport, a2a, mcp,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Value, json};

/// The time limit of one HTTP request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The largest response body that the client reads, in bytes.
pub const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// The largest remote error text in an error message, in bytes.
const MAX_ERROR_TEXT: usize = 1024;

/// One remote server.
///
/// `Debug` prints the endpoint with no user info and no query, and the
/// header names only.
#[derive(Clone)]
pub struct RemoteServer {
    endpoint: String,
    protocol: RemoteProtocol,
    headers: Vec<(String, String)>,
}

/// `endpoint` with no user info, no query and no fragment.
fn redacted(endpoint: &str) -> String {
    reqwest::Url::parse(endpoint).map_or_else(
        |_| "<not a URL>".to_string(),
        |mut url| {
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.set_query(None);
            url.set_fragment(None);
            url.to_string()
        },
    )
}

impl std::fmt::Debug for RemoteServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.headers.iter().map(|(n, _)| n.as_str()).collect();
        f.debug_struct("RemoteServer")
            .field("endpoint", &redacted(&self.endpoint))
            .field("protocol", &self.protocol)
            .field("headers", &names)
            .finish()
    }
}

impl RemoteServer {
    /// An MCP server at `endpoint`.
    #[must_use]
    pub fn mcp(endpoint: &str) -> Self {
        Self::new(endpoint, RemoteProtocol::Mcp)
    }

    /// An A2A server at `endpoint`.
    #[must_use]
    pub fn a2a(endpoint: &str) -> Self {
        Self::new(endpoint, RemoteProtocol::A2a)
    }

    fn new(endpoint: &str, protocol: RemoteProtocol) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            protocol,
            headers: Vec::new(),
        }
    }

    /// Send `Authorization: Bearer <token>`.
    #[must_use]
    pub fn bearer_token(self, token: &str) -> Self {
        self.header("authorization", &format!("Bearer {token}"))
    }

    /// Send the header `name` with `value` on each request.
    ///
    /// The client marks the value as sensitive. A protocol header that the
    /// client sets itself, such as `mcp-method` or `idempotency-key`,
    /// replaces a header with the same name.
    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

/// A [`RemoteTaskTransport`] over JSON-RPC and HTTP.
#[derive(Clone)]
pub struct HttpRemoteTasks {
    client: reqwest::Client,
    servers: HashMap<String, RemoteServer>,
    next_id: Arc<AtomicU64>,
}

impl std::fmt::Debug for HttpRemoteTasks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRemoteTasks")
            .field("servers", &self.servers)
            .finish_non_exhaustive()
    }
}

impl Default for HttpRemoteTasks {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpRemoteTasks {
    /// A client with no servers. It follows no redirect, and each request
    /// has a time limit of 30 s.
    ///
    /// # Panics
    ///
    /// Panics when the TLS backend fails to start.
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "the static client configuration is valid"
    )]
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("the reqwest client builds with a fixed config");
        Self::with_client(client)
    }

    /// A client that sends its requests with `client`.
    ///
    /// Set a timeout and turn off redirects on `client`. A redirect can send
    /// custom headers and the arguments to another host. The client refuses
    /// the response of a redirect to another origin, but the request has
    /// gone out by then.
    #[must_use]
    pub fn with_client(client: reqwest::Client) -> Self {
        Self {
            client,
            servers: HashMap::new(),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Add the server `name`. It replaces a server with the same name.
    #[must_use]
    pub fn server(mut self, name: &str, server: RemoteServer) -> Self {
        self.servers.insert(name.to_string(), server);
        self
    }

    fn server_for(
        &self,
        name: &str,
        protocol: RemoteProtocol,
    ) -> Result<&RemoteServer, RemoteTaskError> {
        let server = self.servers.get(name).ok_or_else(|| {
            RemoteTaskError::non_retryable(format!("unknown remote server '{name}'"))
        })?;
        if server.protocol != protocol {
            return Err(RemoteTaskError::non_retryable(format!(
                "remote server '{name}' does not speak {protocol:?}"
            )));
        }
        Ok(server)
    }

    /// The headers of one request: the server headers, then `extra`.
    fn headers(
        server: &RemoteServer,
        method: &str,
        extra: &[(&'static str, String)],
    ) -> Result<HeaderMap, RemoteTaskError> {
        let invalid = |what: &str| {
            RemoteTaskError::non_retryable(format!("{method}: an invalid header {what}"))
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(
            reqwest::header::ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        for (name, value) in &server.headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| invalid("name"))?;
            let mut value = HeaderValue::from_str(value).map_err(|_| invalid("value"))?;
            value.set_sensitive(true);
            headers.append(name, value);
        }
        for (name, value) in extra {
            let value = HeaderValue::from_str(value).map_err(|_| invalid("value"))?;
            headers.insert(HeaderName::from_static(name), value);
        }
        Ok(headers)
    }

    /// Send one JSON-RPC request and return its `result`.
    async fn rpc(
        &self,
        server: &RemoteServer,
        method: &str,
        params: Value,
        extra: &[(&'static str, String)],
    ) -> Result<Value, RemoteTaskError> {
        let endpoint = reqwest::Url::parse(&server.endpoint).map_err(|_| {
            RemoteTaskError::non_retryable(format!("{method}: the server endpoint is not a URL"))
        })?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let bytes = serde_json::to_vec(&body)
            .map_err(|e| RemoteTaskError::non_retryable(format!("encode {method}: {e}")))?;
        let mut response = self
            .client
            .post(endpoint.clone())
            .headers(Self::headers(server, method, extra)?)
            .body(bytes)
            .send()
            .await
            .map_err(|e| {
                let retryable = !e.is_builder();
                let e = e.without_url();
                if retryable {
                    RemoteTaskError::retryable(format!("{method}: request failed: {e}"))
                } else {
                    RemoteTaskError::non_retryable(format!("{method}: invalid request: {e}"))
                }
            })?;
        if response.url().origin() != endpoint.origin() {
            return Err(RemoteTaskError::non_retryable(format!(
                "{method}: the response came from another origin"
            )));
        }
        let status = response.status();
        let event_stream = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(is_event_stream);
        let (raw, event) = read_body(&mut response, method, id, event_stream, status).await?;
        let envelope = if event_stream {
            event
        } else {
            serde_json::from_slice::<Value>(&raw).ok()
        };
        // A response for another request is a protocol error. Its result or
        // its error is not about this request.
        let envelope = envelope.filter(|e| belongs_to(e, id));
        let mismatched = envelope.is_none() && !raw.is_empty();
        let rpc_error = envelope.as_ref().and_then(|e| e.get("error")).map(|error| {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("no message");
            clip(message).to_string()
        });
        if !status.is_success() {
            let detail = rpc_error.map_or_else(String::new, |m| format!(": {m}"));
            let message = format!("{method}: the remote server answered HTTP {status}{detail}");
            return Err(if busy(status) {
                RemoteTaskError::retryable(message)
            } else {
                RemoteTaskError::non_retryable(message)
            });
        }
        if mismatched {
            return Err(RemoteTaskError::non_retryable(format!(
                "{method}: no JSON-RPC response for this request id"
            )));
        }
        if let Some(message) = rpc_error {
            return Err(RemoteTaskError::non_retryable(format!(
                "{method}: JSON-RPC error: {message}"
            )));
        }
        envelope
            .and_then(|mut e| e.get_mut("result").map(Value::take))
            .ok_or_else(|| RemoteTaskError::non_retryable(format!("{method}: no JSON-RPC result")))
    }
}

/// Read the body of `response`, at most [`MAX_RESPONSE_BYTES`].
///
/// A server may keep an event stream open after the response. So the read
/// of an event stream stops at the first full response with `id`, and
/// returns that response too.
async fn read_body(
    response: &mut reqwest::Response,
    method: &str,
    id: u64,
    event_stream: bool,
    status: reqwest::StatusCode,
) -> Result<(Vec<u8>, Option<Value>), RemoteTaskError> {
    // The status is known before the body. A busy server stays retryable,
    // and a refused request stays refused.
    let too_large = || {
        let message = format!("{method}: the response is over {MAX_RESPONSE_BYTES} bytes");
        if busy(status) {
            RemoteTaskError::retryable(message)
        } else {
            RemoteTaskError::non_retryable(message)
        }
    };
    if response
        .content_length()
        .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
    {
        return Err(too_large());
    }
    let mut raw = Vec::new();
    let mut reader = event_stream.then(|| SseReader::new(id));
    while let Some(chunk) = response.chunk().await.map_err(|e| {
        let message = format!("{method}: read failed: {}", e.without_url());
        // Only a cut 2xx body or a busy server is worth a retry.
        if status.is_success() || busy(status) {
            RemoteTaskError::retryable(message)
        } else {
            RemoteTaskError::non_retryable(message)
        }
    })? {
        if raw.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(too_large());
        }
        raw.extend_from_slice(&chunk);
        if let Some(event) = reader.as_mut().and_then(|r| r.push(&chunk)) {
            return Ok((raw, Some(event)));
        }
    }
    Ok((raw, None))
}

/// `true` for a status that asks the client to try again: 429 or 5xx.
fn busy(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

/// `text`, cut to at most [`MAX_ERROR_TEXT`] bytes on a char boundary.
fn clip(text: &str) -> &str {
    if text.len() <= MAX_ERROR_TEXT {
        return text;
    }
    let mut end = MAX_ERROR_TEXT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// `true` when `envelope` is a JSON-RPC 2.0 response to request `id`.
///
/// The one rule for every response, on the JSON and the event-stream path:
///
/// - `jsonrpc` is `"2.0"`;
/// - exactly one of `result` and `error` is present;
/// - a non-null id is the request id;
/// - a null or missing id passes on an error only, because JSON-RPC sends
///   an error that way when the server cannot read the request id.
fn belongs_to(envelope: &Value, id: u64) -> bool {
    let has_result = envelope.get("result").is_some();
    let has_error = envelope.get("error").is_some();
    if envelope.get("jsonrpc").and_then(Value::as_str) != Some("2.0") || has_result == has_error {
        return false;
    }
    match envelope.get("id") {
        None | Some(Value::Null) => has_error,
        Some(other) => other.as_u64() == Some(id),
    }
}

/// `true` when `content_type` names an event stream.
///
/// Media types are case-insensitive, and parameters follow a `;`.
fn is_event_stream(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|media| media.trim().eq_ignore_ascii_case("text/event-stream"))
}

/// An event-stream reader that keeps its state across chunks. It reads
/// each byte once and returns the JSON-RPC response with the request id.
///
/// The parse follows the event-stream interpretation rules:
///
/// - one leading BOM goes;
/// - CR, LF and CRLF each end a line, also when a chunk ends between CR
///   and LF;
/// - a line decodes as UTF-8 with replacement, so a bad byte spoils only
///   its own line;
/// - a line that starts with `:` is a comment;
/// - a field is the text before the first `:`, and its value loses one
///   leading space;
/// - `data` values join with a line feed;
/// - a blank line dispatches the event;
/// - an event with no blank line after it is not dispatched.
struct SseReader {
    id: u64,
    /// The bytes of the line that has no line end yet.
    line: Vec<u8>,
    /// The last byte was a CR, so a LF next ends no new line.
    after_cr: bool,
    /// The first line is read, so no BOM can follow.
    started: bool,
    /// The `data` values of the current event, each with a line feed.
    data: String,
}

impl SseReader {
    const fn new(id: u64) -> Self {
        Self {
            id,
            line: Vec::new(),
            after_cr: false,
            started: false,
            data: String::new(),
        }
    }

    /// Read `bytes`. Returns the first response with the request id.
    fn push(&mut self, bytes: &[u8]) -> Option<Value> {
        for &byte in bytes {
            if std::mem::take(&mut self.after_cr) && byte == b'\n' {
                continue;
            }
            if matches!(byte, b'\r' | b'\n') {
                self.after_cr = byte == b'\r';
                let line = std::mem::take(&mut self.line);
                if let Some(message) = self.end_line(&line) {
                    return Some(message);
                }
            } else {
                self.line.push(byte);
            }
        }
        None
    }

    fn end_line(&mut self, raw: &[u8]) -> Option<Value> {
        let raw = if std::mem::replace(&mut self.started, true) {
            raw
        } else {
            raw.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(raw)
        };
        let line = String::from_utf8_lossy(raw);
        if line.is_empty() {
            let data = std::mem::take(&mut self.data);
            let data = data.strip_suffix('\n')?;
            return serde_json::from_str::<Value>(data)
                .ok()
                .filter(|message| belongs_to(message, self.id));
        }
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = line.split_once(':').map_or((&*line, ""), |(field, value)| {
            (field, value.strip_prefix(' ').unwrap_or(value))
        });
        if field == "data" {
            self.data.push_str(value);
            self.data.push('\n');
        }
        None
    }
}

/// The `Mcp-Name` header value. A value that is not visible ASCII uses the
/// `=?base64?…?=` sentinel.
fn mcp_name(value: &str) -> String {
    use base64::Engine as _;
    let visible = !value.is_empty()
        && value.bytes().all(|b| (0x21..=0x7e).contains(&b))
        && !value.starts_with("=?");
    if visible {
        value.to_string()
    } else {
        format!(
            "=?base64?{}?=",
            base64::engine::general_purpose::STANDARD.encode(value)
        )
    }
}

/// The Streamable HTTP headers of a `2026-07-28` MCP request.
fn mcp_headers(method: &str, name: &str) -> Vec<(&'static str, String)> {
    vec![
        ("mcp-protocol-version", mcp::PROTOCOL_VERSION.to_string()),
        ("mcp-method", method.to_string()),
        ("mcp-name", mcp_name(name)),
    ]
}

impl RemoteTaskTransport for HttpRemoteTasks {
    fn start<'a>(
        &'a self,
        request: &'a RemoteTaskRequest,
        idempotency_key: &'a str,
    ) -> RemoteFuture<'a, Result<RemoteTaskStart, RemoteTaskError>> {
        Box::pin(async move {
            let server = self.server_for(&request.server, request.protocol)?;
            let key = ("idempotency-key", idempotency_key.to_string());
            match request.protocol {
                RemoteProtocol::Mcp => {
                    let mut headers = mcp_headers("tools/call", &request.tool);
                    headers.push(key);
                    let params = mcp::tools_call_params(request, idempotency_key);
                    let result = self.rpc(server, "tools/call", params, &headers).await?;
                    mcp::parse_tools_call_result(request, &result)
                }
                RemoteProtocol::A2a => {
                    let params = a2a::message_send_params(request, idempotency_key);
                    let result = self.rpc(server, "message/send", params, &[key]).await?;
                    a2a::parse_send_result(request, &result)
                }
            }
        })
    }

    fn get<'a>(
        &'a self,
        handle: &'a RemoteTaskHandle,
    ) -> RemoteFuture<'a, Result<RemoteTaskPoll, RemoteTaskError>> {
        Box::pin(async move {
            let server = self.server_for(&handle.server, handle.protocol)?;
            match handle.protocol {
                RemoteProtocol::Mcp => {
                    let headers = mcp_headers("tasks/get", &handle.task_id);
                    let params = mcp::tasks_get_params(handle);
                    let result = self.rpc(server, "tasks/get", params, &headers).await?;
                    Ok(RemoteTaskPoll {
                        state: mcp::parse_task(handle, &result)?,
                        poll_interval: mcp::poll_interval(&result),
                    })
                }
                RemoteProtocol::A2a => {
                    let params = a2a::tasks_get_params(handle);
                    let result = self.rpc(server, "tasks/get", params, &[]).await?;
                    a2a::parse_task(handle, &result).map(Into::into)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The response with `id` in a whole event-stream body.
    fn sse_response(body: &str, id: u64) -> Option<Value> {
        SseReader::new(id).push(body.as_bytes())
    }

    #[test]
    fn sse_response_picks_the_message_with_the_request_id() {
        let body = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n\
                    data: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}\n\n";
        assert_eq!(
            sse_response(body, 7),
            Some(json!({"jsonrpc": "2.0", "id": 7, "result": {"ok": true}}))
        );
        assert_eq!(sse_response(body, 8), None);
    }

    #[test]
    fn sse_response_joins_the_data_lines_of_one_event() {
        let body = "event: message\r\ndata: {\"jsonrpc\":\"2.0\",\r\ndata: \"id\":3,\r\ndata:\"result\":{\"ok\":true}}\r\n\r\n";
        assert_eq!(
            sse_response(body, 3),
            Some(json!({"jsonrpc": "2.0", "id": 3, "result": {"ok": true}}))
        );
    }

    #[test]
    fn a_lone_cr_ends_a_line() {
        let body = "data: {\"jsonrpc\":\"2.0\",\rdata: \"id\":5,\"result\":{}}\r\r";
        assert_eq!(
            sse_response(body, 5),
            Some(json!({"jsonrpc": "2.0", "id": 5, "result": {}}))
        );
    }

    #[test]
    fn only_a_matching_or_null_error_id_belongs_to_the_request() {
        let rpc = |id: Value, body: Value| {
            let mut envelope = json!({"jsonrpc": "2.0", "id": id});
            for (key, value) in body.as_object().expect("object") {
                envelope[key] = value.clone();
            }
            envelope
        };
        assert!(belongs_to(&rpc(json!(9), json!({"result": {}})), 9));
        assert!(belongs_to(&rpc(json!(9), json!({"error": {}})), 9));
        assert!(belongs_to(&rpc(Value::Null, json!({"error": {}})), 9));
        assert!(!belongs_to(&rpc(json!(8), json!({"error": {}})), 9));
        assert!(!belongs_to(&rpc(json!(8), json!({"result": {}})), 9));
        assert!(!belongs_to(&rpc(Value::Null, json!({"result": {}})), 9));
        assert!(!belongs_to(
            &rpc(json!(9), json!({"result": {}, "error": {}})),
            9
        ));
        assert!(!belongs_to(&rpc(json!(9), json!({})), 9));
        assert!(
            !belongs_to(&json!({"id": 9, "result": {}}), 9),
            "no jsonrpc"
        );
        assert!(!belongs_to(
            &json!({"jsonrpc": "1.0", "id": 9, "result": {}}),
            9
        ));
    }

    #[test]
    fn a_bad_byte_spoils_only_its_own_line() {
        let mut body = b": keep-alive \xFF\n".to_vec();
        body.extend_from_slice(b"data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n");
        assert_eq!(
            SseReader::new(2).push(&body),
            Some(json!({"jsonrpc": "2.0", "id": 2, "result": {}}))
        );
    }

    #[test]
    fn the_reader_keeps_its_state_across_chunks() {
        let body = "\u{feff}data: {\"jsonrpc\":\"2.0\",\r\ndata: \"id\":8,\"result\":{}}\r\n\n";
        let mut reader = SseReader::new(8);
        let bytes = body.as_bytes();
        // One byte at a time: a split BOM, a split CRLF and split lines.
        for byte in &bytes[..bytes.len() - 1] {
            assert_eq!(reader.push(std::slice::from_ref(byte)), None);
        }
        assert_eq!(
            reader.push(&bytes[bytes.len() - 1..]),
            Some(json!({"jsonrpc": "2.0", "id": 8, "result": {}}))
        );
    }

    #[test]
    fn a_null_id_error_in_an_event_stream_is_the_response() {
        let body = "data: {\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32700,\"message\":\"parse\"}}\n\n";
        assert_eq!(
            sse_response(body, 7),
            Some(
                json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "parse"}})
            )
        );
    }

    #[test]
    fn an_event_with_no_blank_line_is_not_dispatched() {
        let body = "data: {\"jsonrpc\":\"2.0\",\"id\":4,\"result\":{}}\n";
        assert_eq!(sse_response(body, 4), None);
        assert_eq!(
            sse_response(&format!("{body}\n"), 4),
            Some(json!({"jsonrpc": "2.0", "id": 4, "result": {}}))
        );
    }

    #[test]
    fn a_bom_comments_and_bare_fields_follow_the_event_stream_rules() {
        let body =
            "\u{feff}: keep-alive\nretry\ndata:{\"jsonrpc\":\"2.0\",\"id\":6,\"result\":{}}\n\n";
        assert_eq!(
            sse_response(body, 6),
            Some(json!({"jsonrpc": "2.0", "id": 6, "result": {}}))
        );
    }

    #[test]
    fn the_event_stream_media_type_is_case_insensitive() {
        assert!(is_event_stream("text/event-stream"));
        assert!(is_event_stream("Text/Event-Stream; charset=utf-8"));
        assert!(!is_event_stream("application/json"));
        assert!(!is_event_stream("text/event-streams"));
    }

    #[test]
    fn mcp_name_keeps_visible_ascii_and_encodes_the_rest() {
        assert_eq!(mcp_name("export"), "export");
        assert_eq!(mcp_name("a b"), "=?base64?YSBi?=");
        assert_eq!(mcp_name("=?x"), "=?base64?PT94?=");
    }

    #[test]
    fn debug_hides_header_values_and_endpoint_secrets() {
        let server =
            RemoteServer::mcp("https://user:pw@x.example/mcp?api_key=k1").bearer_token("secret");
        let shown = format!("{server:?}");
        assert!(shown.contains("authorization"));
        assert!(shown.contains("x.example/mcp"));
        for secret in ["secret", "pw", "user", "k1"] {
            assert!(!shown.contains(secret), "{secret} in {shown}");
        }
    }

    #[test]
    fn a_protocol_header_replaces_a_server_header() {
        let server = RemoteServer::mcp("https://x.example/mcp").header("mcp-method", "ping");
        let headers =
            HttpRemoteTasks::headers(&server, "tools/call", &mcp_headers("tools/call", "t"))
                .expect("headers");
        let methods: Vec<_> = headers.get_all("mcp-method").iter().collect();
        assert_eq!(methods, vec!["tools/call"]);
        assert!(headers.get("mcp-method").is_some_and(|v| !v.is_sensitive()));
    }

    #[test]
    fn clip_cuts_on_a_char_boundary() {
        let long = "é".repeat(MAX_ERROR_TEXT);
        assert!(clip(&long).len() <= MAX_ERROR_TEXT);
        assert_eq!(clip("short"), "short");
    }
}
