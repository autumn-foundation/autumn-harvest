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
    RemoteFuture, RemoteProtocol, RemoteTaskError, RemoteTaskHandle, RemoteTaskRequest,
    RemoteTaskStart, RemoteTaskState, RemoteTaskTransport, a2a, mcp,
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
        let too_large = || {
            RemoteTaskError::non_retryable(format!(
                "{method}: the response is over {MAX_RESPONSE_BYTES} bytes"
            ))
        };
        if response
            .content_length()
            .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
        {
            return Err(too_large());
        }
        let event_stream = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));
        let mut raw = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| {
            RemoteTaskError::retryable(format!("{method}: read failed: {}", e.without_url()))
        })? {
            if raw.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(too_large());
            }
            raw.extend_from_slice(&chunk);
            // A server may keep an event stream open after the response, so
            // the read stops at the first full response with this id.
            if event_stream
                && chunk.contains(&b'\n')
                && std::str::from_utf8(&raw).is_ok_and(|text| sse_response(text, id).is_some())
            {
                break;
            }
        }
        let envelope = if event_stream {
            std::str::from_utf8(&raw)
                .ok()
                .and_then(|text| sse_response(text, id))
        } else {
            serde_json::from_slice::<Value>(&raw).ok()
        };
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
            return Err(
                if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    RemoteTaskError::retryable(message)
                } else {
                    RemoteTaskError::non_retryable(message)
                },
            );
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

/// The JSON-RPC response with `id` in a `text/event-stream` body.
fn sse_response(body: &str, id: u64) -> Option<Value> {
    body.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|data| serde_json::from_str::<Value>(data.trim()).ok())
        .find(|message| {
            message.get("id").and_then(Value::as_u64) == Some(id)
                && (message.get("result").is_some() || message.get("error").is_some())
        })
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
    ) -> RemoteFuture<'a, Result<RemoteTaskState, RemoteTaskError>> {
        Box::pin(async move {
            let server = self.server_for(&handle.server, handle.protocol)?;
            match handle.protocol {
                RemoteProtocol::Mcp => {
                    let headers = mcp_headers("tasks/get", &handle.task_id);
                    let params = mcp::tasks_get_params(handle);
                    let result = self.rpc(server, "tasks/get", params, &headers).await?;
                    mcp::parse_task(&result)
                }
                RemoteProtocol::A2a => {
                    let params = a2a::tasks_get_params(handle);
                    let result = self.rpc(server, "tasks/get", params, &[]).await?;
                    a2a::parse_task(&result)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
