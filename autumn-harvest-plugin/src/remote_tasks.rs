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
//! - A2A: `message/send`, then `tasks/get`.
//!
//! The start sends the idempotency key in the `Idempotency-Key` header too.
//! A network error, HTTP 429 and HTTP 5xx are retryable. Any other HTTP
//! error and a JSON-RPC error are not.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use autumn_harvest::remote_task::{
    RemoteFuture, RemoteProtocol, RemoteTaskError, RemoteTaskHandle, RemoteTaskRequest,
    RemoteTaskStart, RemoteTaskState, RemoteTaskTransport, a2a, mcp,
};
use serde_json::{Value, json};

/// The time limit of one HTTP request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One remote server.
///
/// `Debug` prints the endpoint and the header names only, never a value.
#[derive(Clone)]
pub struct RemoteServer {
    endpoint: String,
    protocol: RemoteProtocol,
    headers: Vec<(String, String)>,
}

impl std::fmt::Debug for RemoteServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.headers.iter().map(|(n, _)| n.as_str()).collect();
        f.debug_struct("RemoteServer")
            .field("endpoint", &self.endpoint)
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
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("the reqwest client builds with a fixed config");
        Self::with_client(client)
    }

    /// A client that sends its requests with `client`.
    #[must_use]
    pub fn with_client(client: reqwest::Client) -> Self {
        Self {
            client,
            servers: HashMap::new(),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Add the server `name`.
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
        let server = self
            .servers
            .get(name)
            .ok_or_else(|| RemoteTaskError::permanent(format!("unknown remote server '{name}'")))?;
        if server.protocol != protocol {
            return Err(RemoteTaskError::permanent(format!(
                "remote server '{name}' does not speak {protocol:?}"
            )));
        }
        Ok(server)
    }

    /// Send one JSON-RPC request and return its `result`.
    async fn rpc(
        &self,
        server: &RemoteServer,
        method: &str,
        params: Value,
        headers: &[(&str, String)],
    ) -> Result<Value, RemoteTaskError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let mut request = self
            .client
            .post(&server.endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(
                reqwest::header::ACCEPT,
                "application/json, text/event-stream",
            );
        for (name, value) in &server.headers {
            request = request.header(name.as_str(), value.as_str());
        }
        for (name, value) in headers {
            request = request.header(*name, value.as_str());
        }
        let bytes = serde_json::to_vec(&body)
            .map_err(|e| RemoteTaskError::permanent(format!("encode {method}: {e}")))?;
        let response = request.body(bytes).send().await.map_err(|e| {
            if e.is_builder() {
                RemoteTaskError::permanent(format!("{method}: invalid request: {e}"))
            } else {
                RemoteTaskError::retryable(format!("{method}: request failed: {e}"))
            }
        })?;
        let status = response.status();
        if !status.is_success() {
            let message = format!("{method}: the remote server answered HTTP {status}");
            return Err(
                if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    RemoteTaskError::retryable(message)
                } else {
                    RemoteTaskError::permanent(message)
                },
            );
        }
        let event_stream = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));
        let text = response
            .text()
            .await
            .map_err(|e| RemoteTaskError::retryable(format!("{method}: read failed: {e}")))?;
        let envelope = if event_stream {
            sse_response(&text, id)
        } else {
            serde_json::from_str(&text).ok()
        }
        .ok_or_else(|| RemoteTaskError::permanent(format!("{method}: no JSON-RPC response")))?;
        if let Some(error) = envelope.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("no message");
            return Err(RemoteTaskError::permanent(format!(
                "{method}: JSON-RPC error: {message}"
            )));
        }
        envelope
            .get("result")
            .cloned()
            .ok_or_else(|| RemoteTaskError::permanent(format!("{method}: no result")))
    }
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
    fn debug_hides_header_values() {
        let server = RemoteServer::mcp("https://x.example/mcp").bearer_token("secret");
        let shown = format!("{server:?}");
        assert!(shown.contains("authorization"));
        assert!(!shown.contains("secret"));
    }
}
