//! One Claude Messages API request, as one durable activity body.
//!
//! The activity is the only part of the daemon that talks to the network. It
//! takes the whole transcript, sends it, and returns the assistant turn. The
//! runtime commits that reply to history, so a later replay never repeats the
//! request and never pays for it twice.
//!
//! With no API key the daemon registers an offline stub instead (see
//! [`offline`]). The stub drives the same loop, so the durability, approval,
//! and restart behaviour are demonstrable with no key and no network.

use std::time::Duration;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use serde_json::{Value, json};
use tokio::runtime::Handle;

use crate::session::{SYSTEM_PROMPT, ToolCall, TurnReply, TurnRequest};
use crate::tools;

/// The Messages API endpoint.
const API_URL: &str = "https://api.anthropic.com/v1/messages";
/// The API version header every request carries.
const API_VERSION: &str = "2023-06-01";
/// Server-side refusal fallbacks. A declined request is routed to a fallback
/// model by category, so one refusal does not end the session. Remove this
/// header and the `fallbacks` field together to turn the behaviour off.
const FALLBACK_BETA: &str = "server-side-fallback-2026-07-01";
/// The default model. Adaptive thinking is on by default on this model.
pub const DEFAULT_MODEL: &str = "claude-opus-5";
/// The default output cap for a non-streaming request.
pub const DEFAULT_MAX_TOKENS: u32 = 16_000;
/// The request timeout. It sits under the activity `start_to_close` budget.
const HTTP_TIMEOUT: Duration = Duration::from_secs(840);

/// Everything the model activity needs.
pub struct ModelConfig {
    /// `None` selects the offline stub.
    pub api_key: Option<String>,
    pub model: String,
    pub max_tokens: u32,
    pub http: reqwest::Client,
}

impl ModelConfig {
    /// Build the configuration from the resolved daemon options.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub fn new(api_key: Option<String>, model: String, max_tokens: u32) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|e| format!("cannot build the HTTP client: {e}"))?;
        Ok(Self {
            api_key,
            model,
            max_tokens,
            http,
        })
    }

    /// Does this configuration call the real API?
    pub const fn is_live(&self) -> bool {
        self.api_key.is_some()
    }
}

/// Build the synchronous activity body the runtime registers for `claude_turn`.
pub fn activity_body(
    config: ModelConfig,
) -> impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static {
    move |input| {
        let request: TurnRequest =
            serde_json::from_value(input).map_err(|e| format!("malformed turn request: {e}"))?;
        let reply = match config.api_key.as_deref() {
            Some(key) => call_api(&config, key, &request)?,
            None => offline::reply(&request),
        };
        serde_json::to_value(reply).map_err(|e| format!("turn reply is not JSON: {e}"))
    }
}

/// Send one request and decode the reply.
///
/// The activity body is synchronous, and the drive loop is async, so the
/// request runs through `block_in_place`. Tokio moves the other tasks of this
/// worker thread elsewhere for the duration. A multi-thread runtime is
/// therefore required, which is what `#[tokio::main]` builds by default.
fn call_api(
    config: &ModelConfig,
    api_key: &str,
    request: &TurnRequest,
) -> Result<TurnReply, String> {
    let body = request_body(config, request);
    let response = tokio::task::block_in_place(|| {
        Handle::current().block_on(async {
            config
                .http
                .post(API_URL)
                .header("x-api-key", api_key)
                .header("anthropic-version", API_VERSION)
                .header("anthropic-beta", FALLBACK_BETA)
                .json(&body)
                .send()
                .await
        })
    });

    // A transport failure is transient by nature, so it keeps the plain error
    // string and the activity retry policy applies.
    let response = response.map_err(|e| format!("the request to the Claude API failed: {e}"))?;
    let status = response.status();
    let text = tokio::task::block_in_place(|| Handle::current().block_on(response.text()))
        .map_err(|e| format!("the Claude API response body failed to read: {e}"))?;

    if !status.is_success() {
        return Err(http_failure(status, &text));
    }
    let payload: Value = serde_json::from_str(&text)
        .map_err(|e| format!("the Claude API response is not JSON: {e}"))?;
    Ok(parse_reply(&payload))
}

/// Build the request body.
fn request_body(config: &ModelConfig, request: &TurnRequest) -> Value {
    json!({
        "model": config.model,
        "max_tokens": config.max_tokens,
        "system": SYSTEM_PROMPT,
        // Adaptive thinking lets the model decide how much to reason per turn.
        "thinking": { "type": "adaptive" },
        "tools": tools::definitions(),
        "messages": request.messages,
        // Paired with the `anthropic-beta` header above.
        "fallbacks": "default",
    })
}

/// Classify a non-2xx response.
///
/// A rate limit and a server fault are transient, so they keep the plain error
/// string and the retry policy applies. Anything else is a rejected request:
/// the same bytes fail again, so the attempt fails terminally instead of
/// burning the retry curve.
fn http_failure(status: reqwest::StatusCode, body: &str) -> String {
    let detail = body.chars().take(400).collect::<String>();
    if status.as_u16() == 429 || status.is_server_error() {
        return format!("the Claude API returned {status}: {detail}");
    }
    ActivityFailure::non_retryable(
        "ClaudeApiRejected",
        format!("the Claude API returned {status}: {detail}"),
    )
    .into_error_payload()
}

/// Project one API response into the durable [`TurnReply`].
fn parse_reply(payload: &Value) -> TurnReply {
    let blocks = payload
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for block in &blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(chunk) = block.get("text").and_then(Value::as_str) {
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(chunk);
                }
            }
            Some("tool_use") => {
                tool_calls.push(ToolCall {
                    id: block
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    name: block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    input: block.get("input").cloned().unwrap_or_else(|| json!({})),
                });
            }
            _ => {}
        }
    }

    let stop_reason = payload
        .get("stop_reason")
        .and_then(Value::as_str)
        .unwrap_or("end_turn")
        .to_string();

    // A refusal carries no usable content, so the category becomes the answer.
    if stop_reason == "refusal" && text.is_empty() {
        let category = payload
            .pointer("/stop_details/category")
            .and_then(Value::as_str)
            .unwrap_or("unspecified");
        text = format!("the model declined this request (category: {category})");
    }

    TurnReply {
        content: Value::Array(blocks),
        stop_reason,
        text,
        tool_calls,
    }
}

/// A scripted model for a daemon that has no API key.
///
/// The stub is deterministic: the same transcript always produces the same
/// reply. It lists the workspace, proposes one file write, and then answers.
/// That path covers a tool call, the approval gate, and a clean finish.
pub mod offline {
    use super::{ToolCall, TurnReply, TurnRequest, Value, json, tools};

    /// The reply for the transcript so far.
    pub fn reply(request: &TurnRequest) -> TurnReply {
        let turn = request
            .messages
            .iter()
            .filter(|m| m.role == "assistant")
            .count();
        match turn {
            0 => tool_turn(
                "toolu_offline_list",
                tools::TOOL_LIST_FILES,
                json!({ "path": "." }),
                "I will look at the workspace first.",
            ),
            1 => tool_turn(
                "toolu_offline_write",
                tools::TOOL_WRITE_FILE,
                json!({
                    "path": "agent-notes.md",
                    "content": "# Offline stub\n\nThis file proves the approval gate ran.\n"
                }),
                "I will record a note. This write needs your approval.",
            ),
            _ => text_turn(
                "Done. The workspace is listed and the note is recorded. \
                 This answer comes from the offline stub, not from Claude.",
            ),
        }
    }

    /// One assistant turn that calls a tool.
    fn tool_turn(id: &str, name: &str, input: Value, preface: &str) -> TurnReply {
        let content = json!([
            { "type": "text", "text": preface },
            { "type": "tool_use", "id": id, "name": name, "input": &input },
        ]);
        TurnReply {
            content,
            stop_reason: "tool_use".to_string(),
            text: preface.to_string(),
            tool_calls: vec![ToolCall {
                id: id.to_string(),
                name: name.to_string(),
                input,
            }],
        }
    }

    /// One assistant turn that ends the session.
    fn text_turn(text: &str) -> TurnReply {
        TurnReply {
            content: json!([{ "type": "text", "text": text }]),
            stop_reason: "end_turn".to_string(),
            text: text.to_string(),
            tool_calls: Vec::new(),
        }
    }
}
