//! MCP Tasks for `#[workflow(mcp)]` workflows (issue #2005).
//!
//! The `io.modelcontextprotocol/tasks` extension lets a server answer a
//! `tools/call` with a task handle. The client then polls `tasks/get` for the
//! result, so a long run never hits a tool timeout.
//!
//! autumn-web serves `/mcp`, and its dispatcher has no hook for the `tasks/*`
//! methods. This module therefore serves its own JSON-RPC route. It answers
//! `initialize`, `server/discover`, `ping`, `tools/list`, `tools/call`,
//! `tasks/get`, `tasks/update` and `tasks/cancel`.
//!
//! The task is the run. The task id is the execution id that the start
//! returns, and each read derives the task from the execution row. So no task
//! state is stored, and a restart loses nothing. See `DESIGN-2005.md`.
//!
//! - A retried `tools/call` with the same start key returns the same task.
//!   The key comes from the `Idempotency-Key` header or from
//!   [`START_KEY_META`] (issue #808).
//! - A run parked on `wait_for_signal` is `input_required`. The awaitables
//!   replay (issue #615) finds the wait. An `accept` answer in `tasks/update`
//!   delivers the signal, with the input key as the signal idempotency key.
//! - A workflow error is a tool error. The task is `completed` with
//!   `isError: true`. Harvest never reports `failed`.
//!
//! Like the tool routes, this module runs at the HTTP edge only. It adds no
//! `WorkflowEvent` variant and no migration.

use std::sync::Arc;

use autumn_web::reexports::axum;
use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, Query};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse as _, Response};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_web::session::Session;

use crate::api::HarvestApiState;
use crate::mcp_tools::McpWorkflowDescriptor;

/// The extension id of MCP Tasks.
pub const TASKS_EXTENSION: &str = "io.modelcontextprotocol/tasks";
/// The `_meta` key that carries the client capabilities of one request.
pub const CLIENT_CAPABILITIES_META: &str = "io.modelcontextprotocol/clientCapabilities";
/// The `_meta` key that carries a start key when the client sets no header.
pub const START_KEY_META: &str = "io.autumn-harvest/idempotencyKey";
/// JSON-RPC error code for a missing client capability.
pub const MISSING_CLIENT_CAPABILITY: i64 = -32021;
/// The poll interval sent with each task.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 5_000;
/// The one form field of a signal elicitation. It holds the payload as JSON.
pub const PAYLOAD_FIELD: &str = "payload";

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;

/// The message of the spec example for an unknown task.
const TASK_NOT_FOUND: &str = "Failed to retrieve task: Task not found";

/// The newest protocol revision this route serves.
const LATEST_PROTOCOL_VERSION: &str = "2026-07-28";
/// The revisions this route accepts in `initialize`.
const PROTOCOL_VERSIONS: &[&str] = &[
    LATEST_PROTOCOL_VERSION,
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
];

/// The largest internal response body this route reads, in bytes.
const MAX_INTERNAL_BODY: usize = 10 * 1024 * 1024;

/// The status of one task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// The run is in progress.
    Working,
    /// The run waits for client input.
    InputRequired,
    /// The run ended with a tool result.
    Completed,
    /// A JSON-RPC error stopped the request.
    Failed,
    /// The run was cancelled.
    Cancelled,
}

impl TaskStatus {
    /// The wire name of the status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Working => "working",
            Self::InputRequired => "input_required",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// `true` for a status that never changes again.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// `true` when the spec permits a move from `self` to `next`.
    ///
    /// A live status can move to any other status. A terminal status never
    /// moves.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        !self.is_terminal() && self as u8 != next as u8
    }
}

/// Map a run state to a task status.
///
/// Call this after the retry and continue-as-new chain is resolved, so a
/// `FAILED` state here has no retry left. A `FAILED` or `TIMED_OUT` run is a
/// tool error, so its task is `completed`.
#[must_use]
pub fn status_for_state(state: &str, waiting_for_input: bool) -> TaskStatus {
    match state {
        "COMPLETED" | "FAILED" | "TIMED_OUT" => TaskStatus::Completed,
        "CANCELLED" | "TERMINATED" => TaskStatus::Cancelled,
        _ if waiting_for_input => TaskStatus::InputRequired,
        _ => TaskStatus::Working,
    }
}

/// The `CallToolResult` of a run that ended with a result.
///
/// Returns `None` for a run that is live, cancelled or terminated.
#[must_use]
pub fn tool_result(state: &str, output: Option<&Value>, error: Option<&str>) -> Option<Value> {
    match state {
        "COMPLETED" => {
            let output = output.cloned().unwrap_or(Value::Null);
            let mut result = json!({
                "content": [{"type": "text", "text": output.to_string()}],
                "isError": false,
            });
            // MCP requires an object here, so a scalar or array output
            // rides in the text content only.
            if output.is_object() {
                result["structuredContent"] = output;
            }
            Some(result)
        }
        "FAILED" | "TIMED_OUT" => {
            let text = error.map_or_else(
                || {
                    if state == "TIMED_OUT" {
                        "the workflow timed out".to_string()
                    } else {
                        "the workflow failed".to_string()
                    }
                },
                ToString::to_string,
            );
            Some(json!({
                "content": [{"type": "text", "text": text}],
                "isError": true,
            }))
        }
        _ => None,
    }
}

/// The input-request key of one signal wait.
///
/// `position` is the id of the last history event when the run parks. A later
/// wait on the same name comes after a new event, so it gets a new key. A
/// timed-out wait writes a `TimerFired` event, so this holds for it too. The
/// run id makes the key new for each run of a chain.
#[must_use]
pub fn input_request_key(run: uuid::Uuid, signal: &str, position: i32) -> String {
    format!("{run}:signal:{signal}:{position}")
}

/// `true` when the request declares form-mode elicitation.
///
/// A server must not send an elicitation in a mode the client did not
/// declare. An empty `elicitation` object declares form mode. Otherwise the
/// object must name `form`.
#[must_use]
pub fn client_accepts_elicitation(params: &Value) -> bool {
    params
        .pointer("/_meta")
        .and_then(|meta| meta.get(CLIENT_CAPABILITIES_META))
        .and_then(|caps| caps.get("elicitation"))
        .and_then(Value::as_object)
        .is_some_and(|modes| modes.is_empty() || modes.get("form").is_some_and(Value::is_object))
}

/// The `_meta` key that carries the protocol version of one request.
const PROTOCOL_VERSION_META: &str = "io.modelcontextprotocol/protocolVersion";
/// JSON-RPC error code for headers that do not match the body.
pub const HEADER_MISMATCH: i64 = -32020;
/// JSON-RPC error code for a protocol version the server does not serve.
pub const UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;

/// Decode a header value that may use the `=?base64?…?=` sentinel.
fn decode_header_value(raw: &str) -> Option<String> {
    use base64::Engine as _;
    raw.strip_prefix("=?base64?")
        .and_then(|rest| rest.strip_suffix("?="))
        .map_or_else(
            || Some(raw.to_string()),
            |encoded| {
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
            },
        )
}

/// The browser origins that may call the route, from the app config.
///
/// This mirrors the guard of autumn-web's own `/mcp`. A same-origin request
/// passes only on a trusted host, because DNS rebinding makes `Origin` and
/// `Host` agree on a hostile name. Any other origin must be in the CORS
/// allowlist.
#[derive(Debug, Clone)]
pub struct OriginPolicy {
    trusted_hosts: Vec<String>,
    allowed_origins: Vec<String>,
}

impl OriginPolicy {
    /// The policy of an app config: `security.trusted_hosts`, plus the
    /// loopback names outside `prod`, and `cors.allowed_origins`.
    #[must_use]
    pub fn from_config(config: &autumn_web::config::AutumnConfig) -> Self {
        let mut trusted_hosts: Vec<String> = config
            .security
            .trusted_hosts
            .hosts
            .iter()
            .map(|h| h.trim().trim_end_matches('.').to_ascii_lowercase())
            .filter(|h| !h.is_empty())
            .collect();
        if !matches!(config.profile.as_deref(), Some("prod" | "production")) {
            trusted_hosts.extend(["localhost", "127.0.0.1", "::1"].map(String::from));
        }
        Self {
            trusted_hosts,
            allowed_origins: config.cors.allowed_origins.clone(),
        }
    }

    fn trusts_host(&self, host: &str) -> bool {
        self.trusted_hosts.iter().any(|rule| {
            rule == "*"
                || rule.strip_prefix('.').map_or_else(
                    || host == rule,
                    |suffix| {
                        host == suffix
                            || host
                                .strip_suffix(suffix)
                                .is_some_and(|prefix| prefix.ends_with('.'))
                    },
                )
        })
    }

    /// `true` when a request with this `Origin` may call the route.
    ///
    /// `host` is the request authority, and `scheme` is its scheme when known.
    #[must_use]
    pub fn allows(&self, origin: &str, host: Option<&str>, scheme: Option<&str>) -> bool {
        if let Some(host) = host
            && same_origin(origin, host, scheme)
        {
            let (name, _) = split_host_port(host);
            let name = name
                .trim_start_matches('[')
                .trim_end_matches(']')
                .trim_end_matches('.')
                .to_ascii_lowercase();
            if self.trusts_host(&name) {
                return true;
            }
        }
        self.allowed_origins
            .iter()
            .any(|allowed| allowed == "*" || allowed == origin)
    }
}

/// `true` when `origin` names the same scheme, host and port as the request.
fn same_origin(origin: &str, host: &str, scheme: Option<&str>) -> bool {
    let Some((origin_scheme, origin_authority)) = origin.split_once("://") else {
        return false;
    };
    if scheme.is_some_and(|s| !s.eq_ignore_ascii_case(origin_scheme)) {
        return false;
    }
    let (origin_host, origin_port) = split_host_port(origin_authority);
    let (request_host, request_port) = split_host_port(host);
    let request_scheme = scheme.unwrap_or(origin_scheme);
    origin_host.eq_ignore_ascii_case(request_host)
        && origin_port.or_else(|| default_port(origin_scheme))
            == request_port.or_else(|| default_port(request_scheme))
}

/// Split an authority into its host and optional port. An IPv6 literal keeps
/// its brackets.
fn split_host_port(authority: &str) -> (&str, Option<&str>) {
    if authority.starts_with('[') {
        return authority.find(']').map_or((authority, None), |close| {
            let port = authority[close + 1..]
                .strip_prefix(':')
                .filter(|p| !p.is_empty());
            (&authority[..=close], port)
        });
    }
    match authority.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|c| c.is_ascii_digit()) => {
            (host, Some(port))
        }
        _ => (authority, None),
    }
}

/// The default port of a URL scheme.
fn default_port(scheme: &str) -> Option<&'static str> {
    match scheme.to_ascii_lowercase().as_str() {
        "https" => Some("443"),
        "http" => Some("80"),
        _ => None,
    }
}

/// A 2026-07-28 request must carry its version and client capabilities in
/// `_meta`. A request without them is malformed. The schema makes the client
/// capabilities an object.
fn check_modern_meta(params: &Value) -> Result<(), (i64, String, Option<Value>)> {
    let meta = params.pointer("/_meta");
    let version_ok = meta
        .and_then(|meta| meta.get(PROTOCOL_VERSION_META))
        .is_some_and(Value::is_string);
    let caps_ok = meta
        .and_then(|meta| meta.get(CLIENT_CAPABILITIES_META))
        .is_some_and(Value::is_object);
    if version_ok && caps_ok {
        return Ok(());
    }
    Err((
        INVALID_PARAMS,
        format!(
            "Invalid params: _meta must carry {PROTOCOL_VERSION_META} and \
             {CLIENT_CAPABILITIES_META}"
        ),
        None,
    ))
}

/// The body value that the `Mcp-Name` header mirrors for `method`.
fn mirrored_name<'a>(method: &str, params: &'a Value) -> Option<&'a str> {
    let field = match method {
        "tools/call" => "name",
        "tasks/get" | "tasks/update" | "tasks/cancel" => "taskId",
        _ => return None,
    };
    params.get(field).and_then(Value::as_str)
}

/// Check the Streamable HTTP headers of one request against its body.
///
/// The body is the source of truth, but a gateway can route or authorize
/// on the headers. A mismatch could then pass a policy for one method and
/// run another, so the request is refused. A 2026-07-28 request must carry
/// the headers. An older request may omit them, but a header it sends must
/// still match. The error is a code, a message and optional data.
///
/// # Errors
///
/// Returns [`HEADER_MISMATCH`] for a missing, malformed or mismatched
/// header, and [`UNSUPPORTED_PROTOCOL_VERSION`] for a version this route
/// does not serve. A 2026-07-28 request without its version or client
/// capabilities in `_meta` gets `-32602`.
pub fn check_request_headers(
    headers: &HeaderMap,
    method: &str,
    params: &Value,
) -> Result<(), (i64, String, Option<Value>)> {
    let mismatch = |message: String| (HEADER_MISMATCH, message, None);
    let header = |name: &str| -> Result<Option<String>, (i64, String, Option<Value>)> {
        headers
            .get(name)
            .map(|value| {
                value
                    .to_str()
                    .map(ToString::to_string)
                    .map_err(|_| mismatch(format!("Header mismatch: {name} is not visible ASCII")))
            })
            .transpose()
    };

    let version = header("mcp-protocol-version")?;
    let body_version = params
        .pointer("/_meta")
        .and_then(|meta| meta.get(PROTOCOL_VERSION_META))
        .and_then(Value::as_str);
    if let Some(version) = version.as_deref() {
        // `initialize` negotiates the version in its body.
        if method != "initialize" && !PROTOCOL_VERSIONS.contains(&version) {
            return Err((
                UNSUPPORTED_PROTOCOL_VERSION,
                format!("Unsupported protocol version: {version}"),
                Some(json!({"supported": PROTOCOL_VERSIONS, "requested": version})),
            ));
        }
        if body_version.is_some_and(|body| body != version) {
            return Err(mismatch(format!(
                "Header mismatch: MCP-Protocol-Version header value '{version}' does not \
                 match body value '{}'",
                body_version.unwrap_or_default()
            )));
        }
    }
    // A version declared only in `_meta` must be one this route serves too.
    if let Some(body) = body_version
        && method != "initialize"
        && !PROTOCOL_VERSIONS.contains(&body)
    {
        return Err((
            UNSUPPORTED_PROTOCOL_VERSION,
            format!("Unsupported protocol version: {body}"),
            Some(json!({"supported": PROTOCOL_VERSIONS, "requested": body})),
        ));
    }
    let modern = version.as_deref() == Some(LATEST_PROTOCOL_VERSION)
        || body_version == Some(LATEST_PROTOCOL_VERSION);
    if modern && version.is_none() {
        return Err(mismatch(
            "Header mismatch: MCP-Protocol-Version header is missing".to_string(),
        ));
    }
    if modern {
        check_modern_meta(params)?;
    }

    match header("mcp-method")? {
        Some(value) if value != method => {
            return Err(mismatch(format!(
                "Header mismatch: Mcp-Method header value '{value}' does not match body \
                 value '{method}'"
            )));
        }
        None if modern => {
            return Err(mismatch(
                "Header mismatch: Mcp-Method header is missing".to_string(),
            ));
        }
        _ => {}
    }

    let expected = mirrored_name(method, params);
    match (header("mcp-name")?, expected) {
        (Some(raw), Some(expected)) => {
            let value = decode_header_value(&raw).ok_or_else(|| {
                mismatch("Header mismatch: Mcp-Name header is malformed".to_string())
            })?;
            if value != expected {
                return Err(mismatch(format!(
                    "Header mismatch: Mcp-Name header value '{value}' does not match body \
                     value '{expected}'"
                )));
            }
        }
        (None, Some(_)) if modern => {
            return Err(mismatch(
                "Header mismatch: Mcp-Name header is missing".to_string(),
            ));
        }
        _ => {}
    }
    Ok(())
}

/// The signal payload in the `content` of an `accept` answer.
///
/// A form client fills the one string field, [`PAYLOAD_FIELD`]. Its text is
/// parsed as JSON, and text that is not JSON is sent as a JSON string. Any
/// other `content` object is the payload as is.
#[must_use]
pub fn signal_payload(content: Option<&Value>) -> Value {
    let Some(content) = content else {
        return json!({});
    };
    match content.as_object() {
        Some(fields) if fields.len() == 1 => match fields.get(PAYLOAD_FIELD) {
            Some(Value::String(text)) => {
                serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.clone()))
            }
            _ => content.clone(),
        },
        _ => content.clone(),
    }
}

/// `true` when the request declares the Tasks extension.
#[must_use]
pub fn client_declares_tasks(params: &Value) -> bool {
    params
        .pointer("/_meta")
        .and_then(|meta| meta.get(CLIENT_CAPABILITIES_META))
        .and_then(|caps| caps.get("extensions"))
        .and_then(|extensions| extensions.get(TASKS_EXTENSION))
        .is_some_and(Value::is_object)
}

/// The task route path under a tool prefix.
#[must_use]
pub fn tasks_path(tools_prefix: &str) -> String {
    format!("{}/tasks", tools_prefix.trim_end_matches('/'))
}

/// The TTL of a task in milliseconds, or `None` for no expiry.
///
/// Retention counts from completion, and the spec counts from creation. So a
/// run with no `completed_at` yet has no TTL. Pass the completion of the row
/// that holds the task id, because the sweep deletes that row.
#[must_use]
pub fn ttl_ms(
    created_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    retention: Option<std::time::Duration>,
) -> Option<u64> {
    let completed_at = completed_at?;
    let retention = chrono::Duration::from_std(retention?).ok()?;
    let expires_at = completed_at.checked_add_signed(retention)?;
    u64::try_from((expires_at - created_at).num_milliseconds()).ok()
}

/// `true` when the live run keeps the start row from retention.
///
/// The task id names the start row, so the TTL follows that row. The sweep
/// keeps a row while another row with the same workflow name and business id
/// lives. A retry gets a new business id, and a cross-type continue-as-new
/// gets a new name. Then the start row expires with the last row of its own
/// name and business id, even while the live run goes on.
#[must_use]
pub fn live_run_guards_start_row(start: (&str, &str), live: (&str, &str)) -> bool {
    start == live
}

/// One signal wait that the client can answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalWait {
    /// The input-request key.
    pub key: String,
    /// The signal name.
    pub signal_name: String,
}

/// The state of one task at one read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSnapshot {
    /// The task id: the execution id from the start.
    pub task_id: String,
    /// The live run of the chain.
    pub run_id: uuid::Uuid,
    /// When the task was created.
    pub created_at: DateTime<Utc>,
    /// When the run last changed.
    pub last_updated_at: DateTime<Utc>,
    /// The state of the live run.
    pub state: String,
    /// The progress text of the live run.
    pub current_details: Option<String>,
    /// The output of the live run.
    pub output: Option<Value>,
    /// The error of the live run.
    pub error: Option<String>,
    /// The open signal waits.
    pub waits: Vec<SignalWait>,
    /// The TTL in milliseconds.
    pub ttl_ms: Option<u64>,
}

impl TaskSnapshot {
    /// The task status of this snapshot.
    #[must_use]
    pub fn status(&self) -> TaskStatus {
        status_for_state(&self.state, !self.waits.is_empty())
    }

    /// Show the waits as text, for a client that cannot take an elicitation.
    ///
    /// The task is then `working`, and `statusMessage` names each signal. The
    /// client can still send the signal with the `signal_{wf}` tool.
    pub fn hide_input_requests(&mut self) {
        if self.waits.is_empty() {
            return;
        }
        let names: Vec<&str> = self.waits.iter().map(|w| w.signal_name.as_str()).collect();
        self.current_details = Some(format!("waiting for signal: {}", names.join(", ")));
        self.waits.clear();
    }

    fn status_message(&self) -> Option<String> {
        match self.status() {
            TaskStatus::InputRequired => {
                let names: Vec<&str> = self.waits.iter().map(|w| w.signal_name.as_str()).collect();
                Some(format!("waiting for signal: {}", names.join(", ")))
            }
            TaskStatus::Cancelled => self.error.clone(),
            TaskStatus::Working => self.current_details.clone(),
            TaskStatus::Completed | TaskStatus::Failed => None,
        }
    }
}

fn rfc3339(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

/// The `Task` object, with no status payload.
///
/// `CreateTaskResult` carries this shape.
#[must_use]
pub fn task_object(snapshot: &TaskSnapshot) -> Value {
    let mut task = json!({
        "taskId": snapshot.task_id,
        "status": snapshot.status().as_str(),
        "createdAt": rfc3339(snapshot.created_at),
        "lastUpdatedAt": rfc3339(snapshot.last_updated_at),
        "ttlMs": snapshot.ttl_ms,
        "pollIntervalMs": DEFAULT_POLL_INTERVAL_MS,
    });
    if let Some(message) = snapshot.status_message() {
        task["statusMessage"] = Value::String(message);
    }
    task
}

/// The elicitation that asks for one signal payload.
fn elicitation(wait: &SignalWait) -> Value {
    json!({
        "method": "elicitation/create",
        "params": {
            "mode": "form",
            "message": format!(
                "The workflow waits for the '{}' signal. Send its JSON payload.",
                wait.signal_name
            ),
            "requestedSchema": {
                "type": "object",
                "properties": {
                    PAYLOAD_FIELD: {
                        "type": "string",
                        "title": format!("'{}' payload", wait.signal_name),
                        "description": "The signal payload as JSON text",
                    },
                },
                "required": [PAYLOAD_FIELD],
            },
        },
    })
}

/// The `DetailedTask` object that `tasks/get` returns.
#[must_use]
pub fn detailed_task(snapshot: &TaskSnapshot) -> Value {
    let mut task = task_object(snapshot);
    match snapshot.status() {
        TaskStatus::InputRequired => {
            let requests: serde_json::Map<String, Value> = snapshot
                .waits
                .iter()
                .map(|wait| (wait.key.clone(), elicitation(wait)))
                .collect();
            task["inputRequests"] = Value::Object(requests);
        }
        TaskStatus::Completed => {
            if let Some(result) = tool_result(
                &snapshot.state,
                snapshot.output.as_ref(),
                snapshot.error.as_deref(),
            ) {
                task["result"] = result;
            }
        }
        TaskStatus::Working | TaskStatus::Failed | TaskStatus::Cancelled => {}
    }
    task
}

/// The workflows the task route serves.
///
/// A DAG is left out. Its trigger takes no start key, so a retried create
/// could start a second run.
#[must_use]
pub fn task_descriptors(descriptors: &[McpWorkflowDescriptor]) -> Vec<McpWorkflowDescriptor> {
    descriptors.iter().filter(|d| !d.is_dag).cloned().collect()
}

// ── Route ────────────────────────────────────────────────────────────────────

/// One tool the task route serves.
struct TaskTool {
    name: String,
    workflow: &'static str,
    description: String,
    input_schema: Value,
}

/// The most runs whose signal waits [`TaskCatalog`] keeps.
const WAITS_CACHE_CAP: usize = 1024;

/// The signal names a run waits on at one history position.
type CachedWaits = (i32, Vec<String>);

/// The tools of the task route, in workflow-name order.
struct TaskCatalog {
    tools: Vec<TaskTool>,
    /// The waits of each run at its last seen history position. The replay
    /// is a pure function of the history, so a poll with no new event reuses
    /// it. This bounds the replay cost of a client that polls fast.
    waits: std::sync::Mutex<std::collections::HashMap<uuid::Uuid, CachedWaits>>,
}

impl TaskCatalog {
    fn new(descriptors: &[McpWorkflowDescriptor]) -> Self {
        let tools = task_descriptors(descriptors)
            .into_iter()
            .map(|d| {
                let component = crate::mcp_tools::input_schema_component(&d.name);
                let body_schema = d.input_schema.clone().unwrap_or_else(
                    || json!({"description": format!("Input for the '{}' workflow", d.name)}),
                );
                let about = d
                    .description
                    .as_deref()
                    .map_or_else(String::new, |text| format!(" {text}"));
                TaskTool {
                    name: format!("start_{}", d.name),
                    // `start_tool` takes a `&'static str`. One leak for each
                    // workflow, once for each plugin build.
                    workflow: Box::leak(d.name.clone().into_boxed_str()),
                    description: format!(
                        "Run the '{}' workflow as an MCP task.{about} The call returns a \
                         task at once. Poll tasks/get for the result.",
                        d.name
                    ),
                    input_schema: json!({
                        "type": "object",
                        "properties": {"body": {"$ref": format!("#/$defs/{component}")}},
                        "required": ["body"],
                        "$defs": {component: body_schema},
                    }),
                }
            })
            .collect();
        Self {
            tools,
            waits: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn cached_waits(&self, run: uuid::Uuid, position: i32) -> Option<Vec<String>> {
        let cache = self
            .waits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache
            .get(&run)
            .filter(|(at, _)| *at == position)
            .map(|(_, names)| names.clone())
    }

    fn cache_waits(&self, run: uuid::Uuid, position: i32, names: Vec<String>) {
        let mut cache = self
            .waits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cache.len() >= WAITS_CACHE_CAP && !cache.contains_key(&run) {
            cache.clear();
        }
        cache.insert(run, (position, names));
    }

    fn tool(&self, name: &str) -> Option<&TaskTool> {
        self.tools.iter().find(|tool| tool.name == name)
    }

    fn serves(&self, workflow: &str) -> bool {
        self.tools.iter().any(|tool| tool.workflow == workflow)
    }
}

/// Build the task route.
///
/// The route takes the layers of a mutating tool route. These are the
/// read-only role gate, the mutation-auth gate (issue #1802), the tenant
/// refusal (issue #1977) and the embedder auth layer. The route can start
/// and cancel runs, so every JSON-RPC method on it counts as a mutation.
#[must_use]
pub fn build_mcp_task_route(
    path: &str,
    descriptors: &[McpWorkflowDescriptor],
    api_state: &HarvestApiState,
    tool_middleware: Option<&crate::plugin::McpToolMiddlewareFn>,
    role_auth_enabled: bool,
) -> autumn_web::Route {
    let catalog = Arc::new(TaskCatalog::new(descriptors));
    let state = api_state.clone();
    let handler = axum::routing::post(
        move |axum::extract::State(app): axum::extract::State<autumn_web::AppState>,
              identity: Option<Extension<autumn_web::security::ResolvedClientIdentity>>,
              uri: axum::http::Uri,
              headers: HeaderMap,
              session: Option<Extension<Session>>,
              body: Result<Json<Value>, JsonRejection>| {
            let api_state = state.clone();
            let catalog = Arc::clone(&catalog);
            let caller = Caller {
                headers,
                session: session.map(|Extension(session)| session),
            };
            // The Streamable HTTP transport must refuse a browser `Origin` it
            // does not trust, with 403. This stops DNS rebinding.
            let origin_refused =
                caller
                    .headers
                    .get(axum::http::header::ORIGIN)
                    .is_some_and(|origin| {
                        let identity = identity.as_ref().map(|Extension(id)| id);
                        let host = identity
                            .and_then(|id| id.host.as_deref())
                            .or_else(|| uri.authority().map(axum::http::uri::Authority::as_str))
                            .or_else(|| {
                                caller
                                    .headers
                                    .get(axum::http::header::HOST)
                                    .and_then(|h| h.to_str().ok())
                            });
                        let scheme = identity.and_then(|id| id.scheme.as_deref());
                        !OriginPolicy::from_config(&app.config_arc()).allows(
                            origin.to_str().unwrap_or(""),
                            host,
                            scheme,
                        )
                    });
            // Boxed: the delegated start future is large (clippy::large_futures).
            async move {
                if origin_refused {
                    let body = json!({
                        "jsonrpc": "2.0",
                        "error": {"code": INVALID_REQUEST, "message": "origin not allowed"},
                    });
                    return (StatusCode::FORBIDDEN, Json(body)).into_response();
                }
                Box::pin(serve(api_state, catalog, caller, body)).await
            }
        },
    );
    let handler = crate::mcp_tools::layer_tool_route(
        handler,
        api_state,
        true,
        tool_middleware,
        role_auth_enabled,
    );
    let path: &'static str = Box::leak(path.to_string().into_boxed_str());
    autumn_web::Route {
        method: axum::http::Method::POST,
        path,
        handler,
        name: "harvest_mcp_tasks",
        // Not an MCP tool itself: `/mcp` must not list this route.
        api_doc: autumn_web::openapi::ApiDoc {
            method: "POST",
            path,
            operation_id: "harvest_mcp_tasks",
            summary: Some("MCP Tasks endpoint for Harvest workflows"),
            success_status: 200,
            ..Default::default()
        },
        api_version: None,
        sunset_opt_out: false,
        repository: None,
        idempotency: autumn_web::RouteIdempotency::Direct,
        timeout: autumn_web::RouteTimeout::Inherit,
        seo: autumn_web::SeoRouteDefaults::EMPTY,
    }
}

/// A JSON-RPC error: code, message and optional data.
struct RpcError {
    code: i64,
    message: String,
    data: Option<Value>,
}

impl RpcError {
    fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    fn not_found() -> Self {
        Self::new(INVALID_PARAMS, TASK_NOT_FOUND)
    }

    fn missing_capability() -> Self {
        Self {
            code: MISSING_CLIENT_CAPABILITY,
            message: "Missing required client capability".to_string(),
            data: Some(json!({
                "requiredCapabilities": {"extensions": {TASKS_EXTENSION: {}}}
            })),
        }
    }
}

type RpcResult = Result<Value, RpcError>;

fn rpc_response(id: &Value, result: RpcResult) -> Response {
    let body = match result {
        Ok(mut result) => {
            // The 2026-07-28 revision requires `resultType` on each result.
            // An older client ignores the extra field.
            if let Some(fields) = result.as_object_mut() {
                fields
                    .entry("resultType")
                    .or_insert_with(|| json!("complete"));
            }
            json!({"jsonrpc": "2.0", "id": id, "result": result})
        }
        Err(err) => {
            let mut error = json!({"code": err.code, "message": err.message});
            if let Some(data) = err.data {
                error["data"] = data;
            }
            json!({"jsonrpc": "2.0", "id": id, "error": error})
        }
    };
    Json(body).into_response()
}

/// The request context of one JSON-RPC call.
struct Caller {
    headers: HeaderMap,
    session: Option<Session>,
}

async fn serve(
    api_state: HarvestApiState,
    catalog: Arc<TaskCatalog>,
    caller: Caller,
    body: Result<Json<Value>, JsonRejection>,
) -> Response {
    let headers = &caller.headers;
    let message = match body {
        Ok(Json(message)) => message,
        // The JSON content type is the CSRF guard: a browser sends it
        // cross-site only after a CORS preflight.
        Err(JsonRejection::MissingJsonContentType(rejection)) => {
            return rejection.into_response();
        }
        Err(rejection) => {
            return rpc_response(
                &Value::Null,
                Err(RpcError::new(PARSE_ERROR, rejection.body_text())),
            );
        }
    };
    let Some(object) = message.as_object() else {
        // The 2026-07-28 revision has no JSON-RPC batch.
        let reason = if message.is_array() {
            "Invalid Request: batching is not supported"
        } else {
            "Invalid Request: expected a JSON object"
        };
        return rpc_response(&Value::Null, Err(RpcError::new(INVALID_REQUEST, reason)));
    };
    let id = object.get("id").cloned();
    // MCP forbids a null id, unlike plain JSON-RPC.
    let id_ok = id.as_ref().is_none_or(|v| v.is_string() || v.is_number());
    let method = object.get("method").and_then(Value::as_str);
    let (true, Some(method), true) = (
        object.get("jsonrpc").and_then(Value::as_str) == Some("2.0"),
        method,
        id_ok,
    ) else {
        let err_id = id.filter(|v| v.is_string() || v.is_number());
        return rpc_response(
            &err_id.unwrap_or(Value::Null),
            Err(RpcError::new(INVALID_REQUEST, "Invalid Request")),
        );
    };
    // A notification gets no response body.
    let Some(id) = id else {
        return StatusCode::ACCEPTED.into_response();
    };
    let params = object.get("params").cloned().unwrap_or(Value::Null);
    if let Err((code, message, data)) = check_request_headers(headers, method, &params) {
        let mut response = rpc_response(
            &id,
            Err(RpcError {
                code,
                message,
                data,
            }),
        );
        *response.status_mut() = StatusCode::BAD_REQUEST;
        return response;
    }
    let result = dispatch(&api_state, &catalog, &caller, method, &params).await;
    let status = match &result {
        // The 2026-07-28 transport answers an unknown method with 404.
        Err(err)
            if err.code == METHOD_NOT_FOUND
                && headers
                    .get("mcp-protocol-version")
                    .is_some_and(|v| v == LATEST_PROTOCOL_VERSION) =>
        {
            Some(StatusCode::NOT_FOUND)
        }
        // The schema requires HTTP 400 for a missing client capability.
        Err(err) if err.code == MISSING_CLIENT_CAPABILITY => Some(StatusCode::BAD_REQUEST),
        _ => None,
    };
    let mut response = rpc_response(&id, result);
    if let Some(status) = status {
        *response.status_mut() = status;
    }
    response
}

async fn dispatch(
    api_state: &HarvestApiState,
    catalog: &TaskCatalog,
    caller: &Caller,
    method: &str,
    params: &Value,
) -> RpcResult {
    let headers = &caller.headers;
    match method {
        "initialize" => Ok(initialize_result(params)),
        "server/discover" => Ok(discover_result()),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(tools_list(catalog)),
        "tools/call" => Box::pin(tools_call(api_state, catalog, headers, params)).await,
        "tasks/get" | "tasks/update" | "tasks/cancel" => {
            if !client_declares_tasks(params) {
                return Err(RpcError::missing_capability());
            }
            let task_id = params
                .get("taskId")
                .and_then(Value::as_str)
                .ok_or_else(|| RpcError::new(INVALID_PARAMS, "taskId is required"))?;
            match method {
                "tasks/get" => tasks_get(api_state, catalog, caller, task_id, params).await,
                "tasks/update" => tasks_update(api_state, catalog, headers, task_id, params).await,
                _ => tasks_cancel(api_state, catalog, headers, task_id).await,
            }
        }
        other => Err(RpcError::new(
            METHOD_NOT_FOUND,
            format!("method not found: {other}"),
        )),
    }
}

fn capabilities() -> Value {
    json!({
        "tools": {"listChanged": false},
        "extensions": {TASKS_EXTENSION: {}},
    })
}

fn server_info() -> Value {
    json!({"name": "autumn-harvest", "version": env!("CARGO_PKG_VERSION")})
}

fn initialize_result(params: &Value) -> Value {
    let requested = params.get("protocolVersion").and_then(Value::as_str);
    let version = requested
        .filter(|v| PROTOCOL_VERSIONS.contains(v))
        .unwrap_or(LATEST_PROTOCOL_VERSION);
    json!({
        "protocolVersion": version,
        "capabilities": capabilities(),
        "serverInfo": server_info(),
    })
}

/// How long a client may cache the discovery and tool-list results.
const CATALOG_TTL_MS: u64 = 60_000;

/// The cache fields of a `CacheableResult`.
///
/// The route sits behind auth, so a shared cache must not serve the result
/// across callers.
fn cacheable(mut result: Value) -> Value {
    result["ttlMs"] = json!(CATALOG_TTL_MS);
    result["cacheScope"] = json!("private");
    result
}

fn discover_result() -> Value {
    cacheable(json!({
        "supportedVersions": PROTOCOL_VERSIONS,
        "capabilities": capabilities(),
        "_meta": {"io.modelcontextprotocol/serverInfo": server_info()},
    }))
}

fn tools_list(catalog: &TaskCatalog) -> Value {
    let tools: Vec<Value> = catalog
        .tools
        .iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema,
                "annotations": {"readOnlyHint": false},
            })
        })
        .collect();
    cacheable(json!({"tools": tools}))
}

/// Read a delegated handler response as status plus JSON body.
async fn read_response(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), MAX_INTERNAL_BODY)
        .await
        .unwrap_or_default();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, body)
}

fn text_of(body: &Value) -> String {
    body.as_str()
        .map_or_else(|| body.to_string(), ToString::to_string)
}

/// The start key of a `tools/call`: the header wins, else [`START_KEY_META`].
fn start_headers(headers: &HeaderMap, params: &Value) -> Result<HeaderMap, RpcError> {
    let mut headers = headers.clone();
    let header = autumn_harvest::audit::HEADER_IDEMPOTENCY_KEY;
    if !headers.contains_key(header)
        && let Some(key) = params
            .pointer("/_meta")
            .and_then(|meta| meta.get(START_KEY_META))
    {
        let key = key.as_str().ok_or_else(|| {
            RpcError::new(INVALID_PARAMS, format!("{START_KEY_META} must be a string"))
        })?;
        let value = HeaderValue::from_str(key).map_err(|_| {
            RpcError::new(
                INVALID_PARAMS,
                format!("{START_KEY_META} is not a valid header value"),
            )
        })?;
        headers.insert(header, value);
    }
    Ok(headers)
}

async fn tools_call(
    api_state: &HarvestApiState,
    catalog: &TaskCatalog,
    headers: &HeaderMap,
    params: &Value,
) -> RpcResult {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::new(INVALID_PARAMS, "name is required"))?;
    let tool = catalog
        .tool(name)
        .ok_or_else(|| RpcError::new(INVALID_PARAMS, format!("unknown tool: {name}")))?;
    // `tools/list` marks `body` as required, so a call without it starts
    // nothing. An explicit `null` body is still a body.
    let body = params
        .get("arguments")
        .and_then(Value::as_object)
        .and_then(|arguments| arguments.get("body"))
        .cloned()
        .ok_or_else(|| RpcError::new(INVALID_PARAMS, "arguments.body is required"))?;
    let headers = start_headers(headers, params)?;
    let response = Box::pin(crate::mcp_tools::start_tool(
        api_state.clone(),
        tool.workflow,
        headers,
        body,
    ))
    .await;
    let (status, started) = read_response(response).await;
    if !status.is_success() {
        // A start that fails creates no task, so it is a tool error.
        return Ok(json!({
            "resultType": "complete",
            "content": [{"type": "text", "text": text_of(&started)}],
            "isError": true,
        }));
    }
    let task_id = started.get("execution_id").and_then(Value::as_str);
    let (true, Some(task_id)) = (client_declares_tasks(params), task_id) else {
        // A start with no run id yet, such as a deferred one, is no task.
        return Ok(plain_start_result(&started));
    };
    // The spec sends a `CreateTaskResult` only once `tasks/get` resolves, so
    // the read must succeed first. A failed read must not hide the run
    // either: a client that saw an error could retry and start a second run.
    // So after a few tries the client gets the plain handle instead.
    for delay_ms in [0, 50, 250] {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        match load_task(api_state, catalog, task_id, None).await {
            Ok((snapshot, _)) => {
                let mut task = task_object(&snapshot);
                task["resultType"] = json!("task");
                return Ok(task);
            }
            Err(err) => {
                tracing::warn!(task_id, error = %err.message, "mcp tasks: read after start failed");
            }
        }
    }
    Ok(plain_start_result(&started))
}

/// The plain `CallToolResult` of a start: the run handle, with no task.
fn plain_start_result(started: &Value) -> Value {
    json!({
        "resultType": "complete",
        "content": [{"type": "text", "text": started.to_string()}],
        "structuredContent": started,
        "isError": false,
    })
}

/// Read the task. The stored output and error are decoded only for a caller
/// that the read-path gate admits: the deployment opt-in plus an admin
/// session (issue #608). Any other caller sees the stored bytes, as on
/// `{wf}_status`.
async fn tasks_get(
    api_state: &HarvestApiState,
    catalog: &TaskCatalog,
    caller: &Caller,
    task_id: &str,
    params: &Value,
) -> RpcResult {
    let decoder = crate::api::read_path_decoder(api_state, caller.session.clone()).await;
    let decode = decoder.as_ref().map(|codecs| (codecs, &caller.headers));
    let (mut snapshot, _) = load_task(api_state, catalog, task_id, decode).await?;
    if !client_accepts_elicitation(params) {
        snapshot.hide_input_requests();
    }
    let mut task = detailed_task(&snapshot);
    task["resultType"] = json!("complete");
    Ok(task)
}

/// Deliver each `accept` answer to an open wait as its signal.
///
/// The input key is the signal idempotency key, so a retried answer is a
/// no-op. An answer to a key that is not open now is ignored, as the spec
/// asks. A `decline` or `cancel` answer, or a payload that the signal refuses,
/// is an error. Then the client knows that the run did not take the answer.
async fn tasks_update(
    api_state: &HarvestApiState,
    catalog: &TaskCatalog,
    headers: &HeaderMap,
    task_id: &str,
    params: &Value,
) -> RpcResult {
    let responses = params
        .get("inputResponses")
        .and_then(Value::as_object)
        .ok_or_else(|| RpcError::new(INVALID_PARAMS, "inputResponses is required"))?;
    let (snapshot, live_served) = load_task(api_state, catalog, task_id, None).await?;
    let answers: Vec<(&SignalWait, &Value)> = snapshot
        .waits
        .iter()
        .filter_map(|wait| responses.get(&wait.key).map(|answer| (wait, answer)))
        .collect();
    if answers.is_empty() {
        return Ok(json!({"resultType": "complete"}));
    }
    if !live_served {
        return Err(outside_catalog());
    }
    // Check every action first, so a refusal delivers no answer at all.
    if let Some((wait, _)) = answers
        .iter()
        .find(|(_, answer)| answer.get("action").and_then(Value::as_str) != Some("accept"))
    {
        return Err(RpcError::new(
            INVALID_PARAMS,
            format!(
                "the answer to {} is not accept; the run still waits. \
                 Use tasks/cancel to stop the task",
                wait.key
            ),
        ));
    }
    for (wait, answer) in answers {
        // The input key is the delivery key. A header key wins over the
        // query key, so the header from the request must not ride along.
        let mut headers = headers.clone();
        headers.remove(autumn_harvest::audit::HEADER_IDEMPOTENCY_KEY);
        let response = crate::api::signal_workflow(
            Extension(api_state.clone()),
            Path((snapshot.run_id.to_string(), wait.signal_name.clone())),
            Query(crate::api::SignalQuery::with_key(wait.key.clone())),
            headers,
            Json(signal_payload(answer.get("content"))),
        )
        .await;
        let (status, body) = read_response(response).await;
        if status.is_server_error() {
            return Err(RpcError::new(INTERNAL_ERROR, text_of(&body)));
        }
        // A run that ended after the read answers 404 or 409. The spec makes
        // the ack eventually consistent, so that is still an ack.
        if status.is_client_error()
            && status != StatusCode::NOT_FOUND
            && status != StatusCode::CONFLICT
        {
            let mut err = RpcError::new(
                INVALID_PARAMS,
                format!("the run refused the answer to {}", wait.key),
            );
            err.data = Some(body);
            return Err(err);
        }
    }
    Ok(json!({"resultType": "complete"}))
}

/// The error for a task whose live run left the catalog.
///
/// A cross-type continue-as-new can move the chain to a workflow that is not
/// an MCP workflow. The task route then does not act on it.
fn outside_catalog() -> RpcError {
    RpcError::new(
        INVALID_PARAMS,
        "the live run of this task is not an MCP workflow",
    )
}

/// Cancel the live run of the task.
///
/// Cancellation is cooperative in the spec, so a run that has already ended
/// is acknowledged too.
async fn tasks_cancel(
    api_state: &HarvestApiState,
    catalog: &TaskCatalog,
    headers: &HeaderMap,
    task_id: &str,
) -> RpcResult {
    let (snapshot, live_served) = load_task(api_state, catalog, task_id, None).await?;
    if snapshot.status().is_terminal() {
        return Ok(json!({"resultType": "complete"}));
    }
    if !live_served {
        return Err(outside_catalog());
    }
    let outcome = crate::api::cancel_workflow(
        Extension(api_state.clone()),
        Path(snapshot.run_id.to_string()),
        headers.clone(),
        Json(crate::api::CancelWorkflowRequest::with_reason(
            "cancelled by MCP tasks/cancel",
        )),
    )
    .await;
    // A run that ended after the read is a conflict, and still an ack.
    if let Err(err) = outcome
        && err.status().is_server_error()
    {
        return Err(RpcError::new(INTERNAL_ERROR, err.to_string()));
    }
    Ok(json!({"resultType": "complete"}))
}

/// Load the task: the start row, its live run and the open signal waits.
///
/// A malformed id, an unknown id and a run outside the catalog all get the
/// same error, so the route is no existence oracle. The flag is `true` when
/// the live run is also an MCP workflow. With `decode`, the payload fields of
/// the live run are decoded, and the read is audited (issue #608).
async fn load_task(
    api_state: &HarvestApiState,
    catalog: &TaskCatalog,
    task_id: &str,
    decode: Option<(&PayloadCodecs, &HeaderMap)>,
) -> Result<(TaskSnapshot, bool), RpcError> {
    let exec_id = crate::api::parse_execution_id(task_id).map_err(|_| RpcError::not_found())?;
    // The shard that hosts the run now, which a rebalance can change.
    let (mut conn, shard) = crate::api::db_conn_for_execution_with_shard(api_state, exec_id)
        .await
        .map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))?;
    let origin = match crate::api::load_execution(&mut conn, exec_id).await {
        Ok(execution) => execution,
        Err(autumn_harvest::error::HarvestError::NotFound(_)) => return Err(RpcError::not_found()),
        Err(e) => return Err(RpcError::new(INTERNAL_ERROR, e.to_string())),
    };
    if !catalog.serves(&origin.workflow_name) {
        return Err(RpcError::not_found());
    }
    // The last end in the retention group of the start row: its own name and
    // business id. Only a chained start row needs it.
    let group_completed_at = if matches!(origin.state.as_str(), "FAILED" | "CONTINUED_AS_NEW") {
        use autumn_harvest::schema::harvest_workflow_executions as wfe;
        use diesel::{ExpressionMethods as _, QueryDsl as _};
        use diesel_async::RunQueryDsl as _;
        wfe::table
            .filter(wfe::workflow_name.eq(&origin.workflow_name))
            .filter(wfe::workflow_id.eq(&origin.workflow_id))
            .select(diesel::dsl::max(wfe::completed_at))
            .first::<Option<DateTime<Utc>>>(&mut conn)
            .await
            .map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))?
    } else {
        None
    };
    drop(conn);
    let created_at = origin.created_at;
    let origin_name = origin.workflow_name.clone();
    let origin_business_id = origin.workflow_id.clone();
    let origin_completed_at = origin.completed_at;
    let mut live = crate::mcp_tools::resolve_if_chained(api_state, origin)
        .await
        .map_err(|_| RpcError::new(INTERNAL_ERROR, "could not resolve the live run"))?;
    let live_served = catalog.serves(&live.workflow_name);

    let (ttl_completed_at, ttl_workflow) = if live_run_guards_start_row(
        (&origin_name, &origin_business_id),
        (&live.workflow_name, &live.workflow_id),
    ) {
        (live.completed_at, live.workflow_name.clone())
    } else {
        (group_completed_at.or(origin_completed_at), origin_name)
    };
    if let Some((codecs, headers)) = decode {
        let outcome = crate::api::decode_workflow_execution_fields(&mut live, codecs);
        crate::api::audit_decoded_read(
            api_state,
            None,
            headers,
            autumn_harvest::audit::TARGET_WORKFLOW,
            Some(task_id),
            "MCP tasks/get",
            Some(shard),
            outcome,
            None,
        )
        .await;
    }
    let retention = api_state
        .runtime()
        .ok()
        .and_then(|rt| rt.retention_config().effective_max_age(&ttl_workflow));
    let terminal = crate::api::is_terminal_state(&live.state);
    let (waits, last_event_at) = if terminal {
        (Vec::new(), None)
    } else {
        open_signal_waits(api_state, catalog, &live).await?
    };
    let last_updated_at = live
        .completed_at
        .or(last_event_at)
        .unwrap_or(live.started_at);
    let snapshot = TaskSnapshot {
        task_id: task_id.to_string(),
        run_id: live.id,
        created_at,
        last_updated_at,
        ttl_ms: ttl_ms(created_at, ttl_completed_at, retention),
        state: live.state,
        current_details: live.current_details,
        output: live.output,
        error: live.error,
        waits,
    };
    Ok((snapshot, live_served))
}

/// The id and the time of the last history event of a run.
async fn history_head(
    api_state: &HarvestApiState,
    run: uuid::Uuid,
) -> Result<Option<(i32, DateTime<Utc>)>, RpcError> {
    use autumn_harvest::schema::harvest_events;
    use diesel::{ExpressionMethods as _, QueryDsl as _};
    use diesel_async::RunQueryDsl as _;

    let exec_id = autumn_harvest::ExecutionId::from_uuid(run);
    let mut conn = crate::api::db_conn_for_execution(api_state, exec_id)
        .await
        .map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))?;
    let (position, at): (Option<i32>, Option<DateTime<Utc>>) = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(run))
        .select((
            diesel::dsl::max(harvest_events::event_id),
            diesel::dsl::max(harvest_events::timestamp),
        ))
        .first(&mut conn)
        .await
        .map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))?;
    Ok(position.zip(at))
}

/// The open signal waits of a live run, and the time it last changed.
///
/// The awaitables replay (issue #615) names each parked `wait_for_signal`.
/// The replay degrades to a history scan when it cannot run, and that scan
/// cannot see a signal wait. The task then reads as `working`.
///
/// A wait whose signal is queued but not yet consumed is left out, because
/// the run is about to wake. The queue time then counts as a change.
async fn open_signal_waits(
    api_state: &HarvestApiState,
    catalog: &TaskCatalog,
    live: &autumn_harvest::models::WorkflowExecution,
) -> Result<(Vec<SignalWait>, Option<DateTime<Utc>>), RpcError> {
    use autumn_harvest::awaitables::AwaitableKind;
    use autumn_harvest::schema::harvest_signals;
    use diesel::{ExpressionMethods as _, QueryDsl as _};
    use diesel_async::RunQueryDsl as _;

    let Some((position, last_event_at)) = history_head(api_state, live.id).await? else {
        return Ok((Vec::new(), None));
    };
    let names = if let Some(names) = catalog.cached_waits(live.id, position) {
        names
    } else {
        let exec_id = autumn_harvest::ExecutionId::from_uuid(live.id);
        let report = crate::api::build_awaitables_report(api_state, exec_id)
            .await
            .map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))?;
        let mut names: Vec<String> = Vec::new();
        for awaitable in report.awaitables {
            if awaitable.kind == AwaitableKind::Signal
                && let Some(name) = awaitable.name
                && !names.contains(&name)
            {
                names.push(name);
            }
        }
        // An event that lands during the replay can end these waits. Their
        // keys would then name the next wait, so report none this time.
        let after = history_head(api_state, live.id).await?.map(|(at, _)| at);
        if after != Some(position) {
            return Ok((Vec::new(), Some(last_event_at)));
        }
        catalog.cache_waits(live.id, position, names.clone());
        names
    };
    if names.is_empty() {
        return Ok((Vec::new(), Some(last_event_at)));
    }

    let exec_id = autumn_harvest::ExecutionId::from_uuid(live.id);
    let mut conn = crate::api::db_conn_for_execution(api_state, exec_id)
        .await
        .map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))?;
    let queued: Vec<(String, DateTime<Utc>)> = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(live.id))
        .filter(harvest_signals::consumed.eq(false))
        .select((harvest_signals::signal_name, harvest_signals::received_at))
        .load(&mut conn)
        .await
        .map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))?;
    drop(conn);

    let mut last_updated_at = last_event_at;
    let mut waits = Vec::new();
    for name in names {
        if let Some((_, queued_at)) = queued.iter().find(|(queued, _)| *queued == name) {
            last_updated_at = last_updated_at.max(*queued_at);
        } else {
            waits.push(SignalWait {
                key: input_request_key(live.id, &name, position),
                signal_name: name,
            });
        }
    }
    Ok((waits, Some(last_updated_at)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone as _, Utc};
    use serde_json::json;

    const ALL: [TaskStatus; 5] = [
        TaskStatus::Working,
        TaskStatus::InputRequired,
        TaskStatus::Completed,
        TaskStatus::Failed,
        TaskStatus::Cancelled,
    ];

    fn at(secs: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap()
    }

    fn snapshot(state: &str) -> TaskSnapshot {
        TaskSnapshot {
            task_id: "0001aaaa-0000-4000-8000-000000000001".into(),
            run_id: uuid::Uuid::nil(),
            created_at: at(0),
            last_updated_at: at(5),
            state: state.into(),
            current_details: None,
            output: None,
            error: None,
            waits: Vec::new(),
            ttl_ms: None,
        }
    }

    #[test]
    fn status_names_match_the_spec() {
        let names: Vec<&str> = ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            names,
            [
                "working",
                "input_required",
                "completed",
                "failed",
                "cancelled"
            ]
        );
    }

    #[test]
    fn only_completed_failed_and_cancelled_are_terminal() {
        for status in ALL {
            let expected = matches!(
                status,
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
            );
            assert_eq!(status.is_terminal(), expected, "{status:?}");
        }
    }

    /// The spec state diagram, written out by hand. `W` is working, `I` is
    /// `input_required`, `C` is completed, `F` is failed and `X` is cancelled.
    #[test]
    fn transitions_follow_the_spec_state_diagram() {
        use TaskStatus::{
            Cancelled as X, Completed as C, Failed as F, InputRequired as I, Working as W,
        };
        let legal = [
            (W, I),
            (W, C),
            (W, F),
            (W, X),
            (I, W),
            (I, C),
            (I, F),
            (I, X),
        ];
        for from in ALL {
            for to in ALL {
                assert_eq!(
                    from.can_transition_to(to),
                    legal.contains(&(from, to)),
                    "{from:?} -> {to:?}"
                );
            }
        }
    }

    #[test]
    fn run_states_map_to_task_statuses() {
        let cases = [
            ("RUNNING", false, TaskStatus::Working),
            ("PAUSED", false, TaskStatus::Working),
            ("MIGRATING", false, TaskStatus::Working),
            ("RUNNING", true, TaskStatus::InputRequired),
            ("PAUSED", true, TaskStatus::InputRequired),
            ("COMPLETED", false, TaskStatus::Completed),
            ("FAILED", false, TaskStatus::Completed),
            ("TIMED_OUT", false, TaskStatus::Completed),
            ("CANCELLED", false, TaskStatus::Cancelled),
            ("TERMINATED", false, TaskStatus::Cancelled),
            ("COMPLETED", true, TaskStatus::Completed),
        ];
        for (state, waiting, expected) in cases {
            assert_eq!(
                status_for_state(state, waiting),
                expected,
                "{state} waiting={waiting}"
            );
        }
    }

    /// A workflow error is a tool error, not a protocol fault (issue #2005).
    /// No run state maps to `failed`, so a client never retries on one.
    #[test]
    fn no_run_state_maps_to_failed() {
        for state in [
            "RUNNING",
            "PAUSED",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
            "CONTINUED_AS_NEW",
            "TERMINATED",
            "MIGRATING",
            "MIGRATED",
        ] {
            for waiting in [false, true] {
                assert_ne!(
                    status_for_state(state, waiting),
                    TaskStatus::Failed,
                    "{state}"
                );
            }
        }
    }

    #[test]
    fn a_completed_run_is_a_successful_tool_result() {
        let output = json!({"approved": true});
        let result = tool_result("COMPLETED", Some(&output), None).unwrap();
        assert_eq!(result["isError"], false);
        assert_eq!(result["structuredContent"], output);
        let text = result["content"][0]["text"].as_str().unwrap();
        assert_eq!(serde_json::from_str::<Value>(text).unwrap(), output);
        assert_eq!(result["content"][0]["type"], "text");
    }

    #[test]
    fn a_scalar_output_has_no_structured_content() {
        let output = json!("ok");
        let result = tool_result("COMPLETED", Some(&output), None).unwrap();
        assert!(result.get("structuredContent").is_none(), "{result}");
        assert_eq!(result["content"][0]["text"], "\"ok\"");
    }

    #[test]
    fn a_failed_or_timed_out_run_is_a_tool_error() {
        let failed = tool_result("FAILED", None, Some("card declined")).unwrap();
        assert_eq!(failed["isError"], true);
        assert_eq!(failed["content"][0]["text"], "card declined");
        let timed_out = tool_result("TIMED_OUT", None, None).unwrap();
        assert_eq!(timed_out["isError"], true);
        assert!(
            timed_out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("timed out")
        );
    }

    #[test]
    fn a_live_or_cancelled_run_has_no_tool_result() {
        for state in ["RUNNING", "PAUSED", "CANCELLED", "TERMINATED"] {
            assert!(tool_result(state, None, None).is_none(), "{state}");
        }
    }

    #[test]
    fn an_input_key_names_the_run_the_signal_and_the_position() {
        let run = uuid::Uuid::from_u128(7);
        let first = input_request_key(run, "approval", 4);
        assert_eq!(first, format!("{run}:signal:approval:4"));
        assert_ne!(first, input_request_key(run, "approval", 9));
        assert_ne!(first, input_request_key(run, "other", 4));
        assert_ne!(
            first,
            input_request_key(uuid::Uuid::from_u128(8), "approval", 4)
        );
    }

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, HeaderValue::from_str(value).unwrap());
        }
        map
    }

    fn modern_params(extra: Value) -> Value {
        let mut params = extra;
        params["_meta"] = json!({
            PROTOCOL_VERSION_META: LATEST_PROTOCOL_VERSION,
            CLIENT_CAPABILITIES_META: {},
        });
        params
    }

    #[test]
    fn matching_headers_pass() {
        let params = modern_params(json!({"name": "start_review"}));
        let ok = headers(&[
            ("mcp-protocol-version", LATEST_PROTOCOL_VERSION),
            ("mcp-method", "tools/call"),
            ("mcp-name", "start_review"),
        ]);
        assert!(check_request_headers(&ok, "tools/call", &params).is_ok());
        // An older request may omit every header.
        assert!(check_request_headers(&HeaderMap::new(), "tools/call", &json!({})).is_ok());
    }

    #[test]
    fn a_method_header_that_differs_from_the_body_is_refused() {
        // A gateway that allows `ping` must not let a `tools/call` through.
        let sneaky = headers(&[("mcp-method", "ping")]);
        let err = check_request_headers(&sneaky, "tools/call", &json!({"name": "x"})).unwrap_err();
        assert_eq!(err.0, HEADER_MISMATCH);
    }

    #[test]
    fn a_name_header_is_compared_after_base64_decoding() {
        let params = json!({"taskId": "étape"});
        let encoded = headers(&[("mcp-name", "=?base64?w6l0YXBl?=")]);
        assert!(check_request_headers(&encoded, "tasks/get", &params).is_ok());
        let other = headers(&[("mcp-name", "etape")]);
        let err = check_request_headers(&other, "tasks/get", &params).unwrap_err();
        assert_eq!(err.0, HEADER_MISMATCH);
        let broken = headers(&[("mcp-name", "=?base64?***?=")]);
        assert_eq!(
            check_request_headers(&broken, "tasks/get", &params)
                .unwrap_err()
                .0,
            HEADER_MISMATCH
        );
    }

    #[test]
    fn a_modern_request_must_carry_the_headers() {
        let params = modern_params(json!({"name": "start_review"}));
        for missing in [
            headers(&[("mcp-method", "tools/call"), ("mcp-name", "start_review")]),
            headers(&[
                ("mcp-protocol-version", LATEST_PROTOCOL_VERSION),
                ("mcp-name", "start_review"),
            ]),
            headers(&[
                ("mcp-protocol-version", LATEST_PROTOCOL_VERSION),
                ("mcp-method", "tools/call"),
            ]),
        ] {
            let err = check_request_headers(&missing, "tools/call", &params).unwrap_err();
            assert_eq!(err.0, HEADER_MISMATCH, "{missing:?}");
        }
    }

    #[test]
    fn a_version_in_meta_only_must_be_served() {
        let params = json!({"_meta": {PROTOCOL_VERSION_META: "2099-01-01"}});
        let err = check_request_headers(&HeaderMap::new(), "ping", &params).unwrap_err();
        assert_eq!(err.0, UNSUPPORTED_PROTOCOL_VERSION);
        assert_eq!(err.2.unwrap()["requested"], "2099-01-01");
    }

    fn policy(trusted: &[&str], allowed: &[&str]) -> OriginPolicy {
        OriginPolicy {
            trusted_hosts: trusted.iter().map(ToString::to_string).collect(),
            allowed_origins: allowed.iter().map(ToString::to_string).collect(),
        }
    }

    /// DNS rebinding makes `Origin` and `Host` agree on a hostile name. Only
    /// a trusted host makes a same-origin request valid.
    #[test]
    fn a_rebound_origin_is_refused() {
        let p = policy(&["localhost", ".example.com"], &[]);
        assert!(!p.allows("http://evil.test:8080", Some("evil.test:8080"), None));
        assert!(p.allows("http://localhost:8080", Some("localhost:8080"), None));
        assert!(p.allows(
            "https://app.example.com",
            Some("app.example.com:443"),
            Some("https")
        ));
        // A trusted host does not admit a different origin.
        assert!(!p.allows("http://evil.test", Some("localhost:8080"), None));
        // A scheme mismatch is not the same origin.
        assert!(!p.allows(
            "http://app.example.com",
            Some("app.example.com"),
            Some("https")
        ));
    }

    #[test]
    fn an_allowlisted_origin_passes() {
        let p = policy(&[], &["https://console.example.com"]);
        assert!(p.allows("https://console.example.com", Some("api.example.com"), None));
        assert!(!p.allows("https://other.example.com", Some("api.example.com"), None));
        assert!(policy(&[], &["*"]).allows("https://any.test", None, None));
        assert!(policy(&["::1"], &[]).allows("http://[::1]:3000", Some("[::1]:3000"), None));
    }

    #[test]
    fn a_modern_request_must_carry_its_meta_fields() {
        let ok = headers(&[
            ("mcp-protocol-version", LATEST_PROTOCOL_VERSION),
            ("mcp-method", "ping"),
        ]);
        let no_caps = json!({"_meta": {PROTOCOL_VERSION_META: LATEST_PROTOCOL_VERSION}});
        assert_eq!(
            check_request_headers(&ok, "ping", &no_caps).unwrap_err().0,
            INVALID_PARAMS
        );
        assert_eq!(
            check_request_headers(&ok, "ping", &json!({}))
                .unwrap_err()
                .0,
            INVALID_PARAMS
        );
        let null_caps = json!({"_meta": {
            PROTOCOL_VERSION_META: LATEST_PROTOCOL_VERSION,
            CLIENT_CAPABILITIES_META: null,
        }});
        assert_eq!(
            check_request_headers(&ok, "ping", &null_caps)
                .unwrap_err()
                .0,
            INVALID_PARAMS
        );
        assert!(check_request_headers(&ok, "ping", &modern_params(json!({}))).is_ok());
    }

    #[test]
    fn a_version_header_must_match_the_body_and_be_served() {
        let params = json!({"_meta": {PROTOCOL_VERSION_META: "2025-11-25"}});
        let differs = headers(&[("mcp-protocol-version", LATEST_PROTOCOL_VERSION)]);
        assert_eq!(
            check_request_headers(&differs, "ping", &params)
                .unwrap_err()
                .0,
            HEADER_MISMATCH
        );
        let unknown = headers(&[("mcp-protocol-version", "2099-01-01")]);
        let err = check_request_headers(&unknown, "ping", &json!({})).unwrap_err();
        assert_eq!(err.0, UNSUPPORTED_PROTOCOL_VERSION);
        assert_eq!(err.2.unwrap()["requested"], "2099-01-01");
        // `initialize` negotiates in its body, so its header is not checked.
        assert!(check_request_headers(&unknown, "initialize", &json!({})).is_ok());
    }

    #[test]
    fn only_form_mode_elicitation_gets_input_requests() {
        let caps = |elicitation: Value| json!({"_meta": {CLIENT_CAPABILITIES_META: {"elicitation": elicitation}}});
        assert!(client_accepts_elicitation(&caps(json!({}))));
        assert!(client_accepts_elicitation(&caps(json!({"form": {}}))));
        assert!(client_accepts_elicitation(&caps(
            json!({"form": {}, "url": {}})
        )));
        assert!(!client_accepts_elicitation(&caps(json!({"url": {}}))));
        assert!(!client_accepts_elicitation(&caps(json!(true))));
    }

    #[test]
    fn the_client_declares_elicitation_in_request_meta() {
        let declared = json!({"_meta": {CLIENT_CAPABILITIES_META: {"elicitation": {}}}});
        assert!(client_accepts_elicitation(&declared));
        let tasks_only = json!({
            "_meta": {CLIENT_CAPABILITIES_META: {"extensions": {TASKS_EXTENSION: {}}}}
        });
        assert!(!client_accepts_elicitation(&tasks_only));
        assert!(!client_accepts_elicitation(&Value::Null));
    }

    #[test]
    fn a_form_answer_carries_the_payload_as_json_text() {
        assert_eq!(
            signal_payload(Some(&json!({PAYLOAD_FIELD: "{\"decision\": \"approve\"}"}))),
            json!({"decision": "approve"})
        );
        assert_eq!(
            signal_payload(Some(&json!({PAYLOAD_FIELD: "approve"}))),
            json!("approve")
        );
        assert_eq!(
            signal_payload(Some(&json!({PAYLOAD_FIELD: "[1, 2]"}))),
            json!([1, 2])
        );
    }

    #[test]
    fn other_answer_content_is_the_payload_as_is() {
        assert_eq!(
            signal_payload(Some(&json!({"decision": "approve"}))),
            json!({"decision": "approve"})
        );
        assert_eq!(
            signal_payload(Some(&json!({PAYLOAD_FIELD: 3}))),
            json!({PAYLOAD_FIELD: 3})
        );
        assert_eq!(
            signal_payload(Some(&json!({PAYLOAD_FIELD: "x", "more": 1}))),
            json!({PAYLOAD_FIELD: "x", "more": 1})
        );
        assert_eq!(signal_payload(None), json!({}));
    }

    #[test]
    fn hidden_input_requests_read_as_working_with_the_signal_names() {
        let mut snap = snapshot("RUNNING");
        snap.waits = vec![SignalWait {
            key: "k1".into(),
            signal_name: "approval".into(),
        }];
        snap.hide_input_requests();
        let task = detailed_task(&snap);
        assert_eq!(task["status"], "working");
        assert_eq!(task["statusMessage"], "waiting for signal: approval");
        assert!(task.get("inputRequests").is_none(), "{task}");
    }

    #[test]
    fn the_client_declares_tasks_in_request_meta() {
        let declared = json!({
            "_meta": {CLIENT_CAPABILITIES_META: {"extensions": {TASKS_EXTENSION: {}}}}
        });
        assert!(client_declares_tasks(&declared));
        for params in [
            json!({}),
            Value::Null,
            json!({"_meta": {}}),
            json!({"_meta": {CLIENT_CAPABILITIES_META: {"extensions": {}}}}),
            json!({"_meta": {CLIENT_CAPABILITIES_META: {"extensions": {TASKS_EXTENSION: false}}}}),
        ] {
            assert!(!client_declares_tasks(&params), "{params}");
        }
    }

    #[test]
    fn the_task_route_sits_under_the_tool_prefix() {
        assert_eq!(tasks_path("/api/harvest/mcp"), "/api/harvest/mcp/tasks");
        assert_eq!(tasks_path("/custom/"), "/custom/tasks");
    }

    /// Retention guards a start row only through a live row with the same
    /// workflow name and business id.
    #[test]
    fn only_a_live_run_of_the_same_group_guards_the_start_row() {
        // A same-type continue-as-new keeps the name and business id.
        assert!(live_run_guards_start_row(("a", "id"), ("a", "id")));
        // A retry gets a new business id, also after a continue-as-new.
        assert!(!live_run_guards_start_row(("a", "id"), ("a", "retry-id")));
        // A cross-type continue-as-new gets a new name.
        assert!(!live_run_guards_start_row(("a", "id"), ("b", "id")));
    }

    #[test]
    fn ttl_is_open_while_the_run_lives() {
        let day = std::time::Duration::from_secs(86_400);
        assert_eq!(ttl_ms(at(0), None, Some(day)), None);
        assert_eq!(ttl_ms(at(0), Some(at(10)), None), None);
        assert_eq!(
            ttl_ms(at(0), Some(at(10)), Some(day)),
            Some(86_400_000 + 10_000)
        );
    }

    #[test]
    fn a_working_task_carries_the_spec_fields() {
        let mut snap = snapshot("RUNNING");
        snap.current_details = Some("step 1/2".into());
        let task = detailed_task(&snap);
        assert_eq!(task["taskId"], snap.task_id);
        assert_eq!(task["status"], "working");
        assert_eq!(task["statusMessage"], "step 1/2");
        assert_eq!(task["createdAt"], "2027-01-15T08:00:00Z");
        assert_eq!(task["lastUpdatedAt"], "2027-01-15T08:00:05Z");
        assert_eq!(task["ttlMs"], Value::Null);
        assert_eq!(task["pollIntervalMs"], DEFAULT_POLL_INTERVAL_MS);
        for absent in ["result", "error", "inputRequests"] {
            assert!(task.get(absent).is_none(), "{absent}: {task}");
        }
    }

    #[test]
    fn an_input_required_task_lists_one_elicitation_per_wait() {
        let mut snap = snapshot("RUNNING");
        snap.waits = vec![SignalWait {
            key: "k1".into(),
            signal_name: "approval".into(),
        }];
        assert_eq!(snap.status(), TaskStatus::InputRequired);
        let task = detailed_task(&snap);
        assert_eq!(task["status"], "input_required");
        let request = &task["inputRequests"]["k1"];
        assert_eq!(request["method"], "elicitation/create");
        assert_eq!(request["params"]["mode"], "form");
        let schema = &request["params"]["requestedSchema"];
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"][PAYLOAD_FIELD]["type"], "string");
        assert_eq!(schema["required"], json!([PAYLOAD_FIELD]));
        assert!(schema.get("description").is_none(), "{schema}");
        assert!(
            request["params"]["message"]
                .as_str()
                .unwrap()
                .contains("approval")
        );
    }

    #[test]
    fn a_completed_task_inlines_the_tool_result() {
        let mut snap = snapshot("COMPLETED");
        snap.output = Some(json!({"ok": true}));
        let task = detailed_task(&snap);
        assert_eq!(task["status"], "completed");
        assert_eq!(task["result"]["isError"], false);
        assert_eq!(task["result"]["structuredContent"], json!({"ok": true}));
    }

    #[test]
    fn a_cancelled_task_has_no_result() {
        let mut snap = snapshot("CANCELLED");
        snap.error = Some("stopped by operator".into());
        let task = detailed_task(&snap);
        assert_eq!(task["status"], "cancelled");
        assert_eq!(task["statusMessage"], "stopped by operator");
        assert!(task.get("result").is_none(), "{task}");
    }

    #[test]
    fn the_task_object_drops_the_status_payload() {
        let mut snap = snapshot("COMPLETED");
        snap.output = Some(json!(1));
        let task = task_object(&snap);
        assert_eq!(task["status"], "completed");
        assert!(task.get("result").is_none(), "{task}");
    }

    fn descriptor(name: &str, is_dag: bool) -> McpWorkflowDescriptor {
        McpWorkflowDescriptor {
            name: name.into(),
            description: None,
            input_schema: None,
            updates: Vec::new(),
            is_dag,
            consumes_signals: false,
        }
    }

    /// A DAG trigger takes no start key, so a retried create could start a
    /// second run. The task route leaves DAGs out.
    #[test]
    fn the_task_route_serves_workflows_and_not_dags() {
        let served = task_descriptors(&[descriptor("etl", true), descriptor("review", false)]);
        let names: Vec<&str> = served.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, ["review"]);
    }
}
