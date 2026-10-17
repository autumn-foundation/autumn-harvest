//! A Claude model for the agent loop, over the Messages API.
//!
//! [`ClaudeModel`] implements the provider-neutral `AgentModel`. The request
//! uses `claude-opus-5-5` by default, adaptive thinking (the model default),
//! an explicit effort, automatic prompt caching and the server-side refusal
//! fallback. The mapping functions are pure, so the tests need no network
//! and no key.
//!
//! The adapter's `ChatMessage` carries text, tool calls and tool results. It
//! has no place for a thinking block, and the API wants each thinking block
//! back unchanged in a tool loop. So the model keeps each raw assistant turn,
//! keyed by the hash of its text and tool calls. The next request finds the
//! turn by the same hash and sends the raw blocks. With a turn directory,
//! the turns survive a restart.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use autumn_harvest_agent::{
    AgentError, AgentModel, BoxFuture, ChatRequest, ChatResponse, ChatRole, ContentPart, ErrorKind,
    StopReason, TokenUsage,
};
use serde_json::{Value, json};

/// The default model. Only this model gets the effort and the fallback.
pub const DEFAULT_MODEL: &str = "claude-opus-5-5";
/// The Messages API endpoint.
pub const ENDPOINT: &str = "https://api.anthropic.com/v1/messages";
/// The API version header value.
pub const API_VERSION: &str = "2023-06-01";
/// The beta that turns on the server-side refusal fallback.
pub const FALLBACK_BETA: &str = "server-side-fallback-2026-07-01";
/// The output cap of one turn when the task sets none.
pub const DEFAULT_MAX_TOKENS: u32 = 16_000;
/// The effort of the default model.
pub const EFFORT: &str = "high";
/// How long one request may take, thinking included.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
/// The most bytes of one response body that the model reads.
pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// A Claude model over the Messages API.
#[derive(Clone)]
pub struct ClaudeModel {
    api_key: String,
    model: String,
    /// The model and every setting that changes its answers.
    id: String,
    client: reqwest::Client,
    turns: Turns,
}

/// The key stays out of every log line.
impl std::fmt::Debug for ClaudeModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaudeModel")
            .field("api_key", &"<redacted>")
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

impl ClaudeModel {
    /// A model with `api_key` and the default model. The raw turns live in
    /// memory only.
    #[must_use]
    pub fn new(api_key: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self {
            api_key: api_key.into(),
            model: DEFAULT_MODEL.into(),
            id: model_id(DEFAULT_MODEL),
            client,
            turns: Turns::default(),
        }
    }

    /// Use `model` in place of the default.
    #[must_use]
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self.id = model_id(&self.model);
        self
    }

    /// Keep the raw turns in `dir` too, so a restarted process can replay
    /// them.
    #[must_use]
    pub fn turn_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.turns.dir = Some(dir.into());
        self
    }

    /// Read at most [`MAX_BODY_BYTES`] of `response`.
    async fn body(response: reqwest::Response) -> Result<String, AgentError> {
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| AgentError::new(ErrorKind::Transport, format!("response lost: {e}")))?
        {
            if bytes.len() + chunk.len() > MAX_BODY_BYTES {
                return Err(AgentError::new(
                    ErrorKind::Decode,
                    format!("the response is over {MAX_BODY_BYTES} bytes"),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

/// The id of `model` with its settings. A response cache keys on it.
fn model_id(model: &str) -> String {
    if model == DEFAULT_MODEL {
        format!("{model}+effort={EFFORT}+fallbacks=default")
    } else {
        model.to_string()
    }
}

impl AgentModel for ClaudeModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let body = request_body(request, &self.model, |visible| self.turns.get(visible));
            let mut call = self
                .client
                .post(ENDPOINT)
                .header("x-api-key", &self.api_key)
                .header("anthropic-version", API_VERSION);
            if self.model == DEFAULT_MODEL {
                call = call.header("anthropic-beta", FALLBACK_BETA);
            }
            let response = call.json(&body).send().await.map_err(|e| {
                AgentError::new(ErrorKind::Transport, format!("request failed: {e}"))
            })?;
            let status = response.status().as_u16();
            let text = Self::body(response).await?;
            if !(200..300).contains(&status) {
                // The body names the error. It holds no secret: the key is a header.
                let detail: String = text.chars().take(500).collect();
                return Err(AgentError::new(
                    error_kind(status),
                    format!("HTTP {status}: {detail}"),
                ));
            }
            let body: Value = serde_json::from_str(&text)
                .map_err(|e| AgentError::new(ErrorKind::Decode, format!("not JSON: {e}")))?;
            let response = parse_response(&body)?;
            if let Some((key, raw)) = replayable(&body) {
                self.turns.put(key, raw)?;
            }
            Ok(response)
        })
    }

    fn model_id(&self) -> Option<&str> {
        Some(&self.id)
    }
}

/// The raw assistant turns that hold more than text and tool calls.
#[derive(Debug, Clone, Default)]
struct Turns {
    dir: Option<PathBuf>,
    memory: Arc<Mutex<HashMap<String, Vec<Value>>>>,
}

impl Turns {
    fn path(&self, key: &str) -> Option<PathBuf> {
        self.dir.as_ref().map(|dir| dir.join(format!("{key}.json")))
    }

    fn put(&self, key: String, raw: Vec<Value>) -> Result<(), AgentError> {
        if let Some(path) = self.path(&key) {
            let store = |e: String| AgentError::new(ErrorKind::Unavailable, e);
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir).map_err(|e| store(e.to_string()))?;
            }
            let bytes = serde_json::to_vec(&raw).map_err(|e| store(e.to_string()))?;
            crate::stores::write_atomic(&path, &bytes).map_err(store)?;
        }
        self.memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, raw);
        Ok(())
    }

    fn get(&self, visible: &[Value]) -> Option<Vec<Value>> {
        let key = turn_key(visible);
        if let Some(raw) = self
            .memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
        {
            return Some(raw.clone());
        }
        let bytes = std::fs::read(self.path(&key)?).ok()?;
        serde_json::from_slice(&bytes).ok()
    }
}

/// The key of a turn: the hash of its text and tool-call blocks.
fn turn_key(visible: &[Value]) -> String {
    crate::stores::hashed(&serde_json::to_string(visible).unwrap_or_default())
}

/// The blocks of a response that go back to the API.
///
/// After a fallback block, a model-internal block before the boundary
/// belongs to the declined attempt, so it does not go back. A text block
/// before the boundary does. The fallback block is an audit marker only.
fn turn_blocks(body: &Value) -> Vec<Value> {
    let blocks: Vec<Value> = body["content"].as_array().cloned().unwrap_or_default();
    let Some(boundary) = blocks.iter().rposition(|b| b["type"] == "fallback") else {
        return blocks;
    };
    blocks[..boundary]
        .iter()
        .filter(|b| b["type"] == "text")
        .chain(&blocks[boundary + 1..])
        .cloned()
        .collect()
}

/// The text and tool-call blocks of `blocks`, as a request sends them.
fn visible(blocks: &[Value]) -> Vec<Value> {
    blocks
        .iter()
        .filter_map(|block| match block["type"].as_str() {
            Some("text") => Some(json!({"type": "text", "text": block["text"].as_str()?})),
            Some("tool_use") => Some(json!({
                "type": "tool_use",
                "id": block["id"].as_str()?,
                "name": block["name"].as_str()?,
                "input": block["input"],
            })),
            _ => None,
        })
        .collect()
}

/// The key and the raw blocks of a response that holds more than text and
/// tool calls, such as a thinking block. `None` when nothing needs a replay.
#[must_use]
pub fn replayable(body: &Value) -> Option<(String, Vec<Value>)> {
    let blocks = turn_blocks(body);
    let shown = visible(&blocks);
    (blocks.len() > shown.len()).then(|| (turn_key(&shown), blocks))
}

/// The JSON body of one Messages API request. `replay` returns the raw
/// blocks of an assistant turn from its text and tool-call blocks.
#[must_use]
pub fn request_body(
    request: &ChatRequest,
    model: &str,
    replay: impl Fn(&[Value]) -> Option<Vec<Value>>,
) -> Value {
    let system: Vec<&str> = request
        .messages
        .iter()
        .filter(|m| m.role == ChatRole::System)
        .flat_map(|m| &m.content)
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    let messages: Vec<Value> = request
        .messages
        .iter()
        .filter(|m| m.role != ChatRole::System)
        .map(|message| {
            // Tool results go back in a user turn.
            let role = if message.role == ChatRole::Assistant {
                "assistant"
            } else {
                "user"
            };
            let content: Vec<Value> = message
                .content
                .iter()
                .map(|part| match part {
                    ContentPart::Text(text) => json!({"type": "text", "text": text}),
                    ContentPart::ToolCall {
                        id,
                        name,
                        arguments,
                    } => json!({"type": "tool_use", "id": id, "name": name, "input": arguments}),
                    ContentPart::ToolResult {
                        tool_call_id,
                        content,
                    } => json!({"type": "tool_result", "tool_use_id": tool_call_id, "content": content}),
                })
                .collect();
            let content = if message.role == ChatRole::Assistant {
                replay(&content).unwrap_or(content)
            } else {
                content
            };
            json!({"role": role, "content": content})
        })
        .collect();
    let tools: Vec<Value> = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema,
            })
        })
        .collect();
    // No `temperature`: the current models refuse sampling settings. No
    // `thinking`: the model default is adaptive thinking. The top-level
    // `cache_control` caches the growing transcript.
    let mut body = json!({
        "model": model,
        "max_tokens": request.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
        "messages": messages,
        "cache_control": {"type": "ephemeral"},
    });
    // Another model may not take these settings, so it gets its defaults.
    if model == DEFAULT_MODEL {
        body["output_config"] = json!({"effort": EFFORT});
        body["fallbacks"] = json!("default");
    }
    if !system.is_empty() {
        body["system"] = json!(system.join("\n\n"));
    }
    if !tools.is_empty() {
        body["tools"] = json!(tools);
    }
    body
}

/// Map one Messages API response body to a [`ChatResponse`].
///
/// # Errors
///
/// A `refusal` stop is a `Provider` error, so the run fails and says why.
/// A body that is not a message is a `Decode` error.
pub fn parse_response(body: &Value) -> Result<ChatResponse, AgentError> {
    if body["type"] != "message" {
        return Err(AgentError::new(
            ErrorKind::Decode,
            "the response is not a message",
        ));
    }
    let stop_reason = match body["stop_reason"].as_str() {
        Some("end_turn" | "stop_sequence") => StopReason::EndTurn,
        Some("tool_use") => StopReason::ToolUse,
        Some("max_tokens") => StopReason::MaxTokens,
        Some("refusal") => {
            let category = body["stop_details"]["category"]
                .as_str()
                .unwrap_or("unknown");
            return Err(AgentError::new(
                ErrorKind::Provider,
                format!("the model declined the request (category: {category})"),
            ));
        }
        _ => StopReason::Unknown,
    };
    // Thinking and fallback blocks have no `ChatMessage` part. See the
    // module docs.
    let content = visible(&turn_blocks(body))
        .into_iter()
        .filter_map(|block| match block["type"].as_str() {
            Some("text") => Some(ContentPart::Text(block["text"].as_str()?.to_string())),
            Some("tool_use") => Some(ContentPart::ToolCall {
                id: block["id"].as_str()?.to_string(),
                name: block["name"].as_str()?.to_string(),
                arguments: block["input"].clone(),
            }),
            _ => None,
        })
        .collect();
    let count =
        |key: &str| u32::try_from(body["usage"][key].as_u64().unwrap_or(0)).unwrap_or(u32::MAX);
    // The adapter counts the whole prompt, cached tokens included.
    let input = count("input_tokens")
        .saturating_add(count("cache_read_input_tokens"))
        .saturating_add(count("cache_creation_input_tokens"));
    let mut usage = TokenUsage::new(input, count("output_tokens"));
    usage.cache_read_tokens = count("cache_read_input_tokens");
    usage.cache_write_tokens = count("cache_creation_input_tokens");
    Ok(ChatResponse {
        content,
        stop_reason,
        usage,
    })
}

/// The error kind of an HTTP status. A timeout, a conflict, a rate limit and
/// a server fault retry, as the SDKs retry them.
#[must_use]
pub const fn error_kind(status: u16) -> ErrorKind {
    match status {
        429 => ErrorKind::RateLimited,
        401 | 403 => ErrorKind::Authentication,
        408 | 409 | 500..=599 => ErrorKind::Unavailable,
        _ => ErrorKind::Provider,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use autumn_harvest_agent::{ChatMessage, ChatRole, ContentPart, StopReason, ToolDefinition};
    use serde_json::json;

    use super::*;

    fn request() -> ChatRequest {
        ChatRequest {
            messages: vec![
                ChatMessage::text(ChatRole::System, "You are a researcher."),
                ChatMessage::text(ChatRole::User, "Run experiment 1."),
                ChatMessage {
                    role: ChatRole::Assistant,
                    content: vec![
                        ContentPart::Text("Reading.".into()),
                        ContentPart::ToolCall {
                            id: "toolu_1".into(),
                            name: "read_file".into(),
                            arguments: json!({"path": "train.py"}),
                        },
                    ],
                },
                ChatMessage {
                    role: ChatRole::Tool,
                    content: vec![ContentPart::ToolResult {
                        tool_call_id: "toolu_1".into(),
                        content: "DEPTH = 8".into(),
                    }],
                },
            ],
            tools: vec![ToolDefinition {
                name: "read_file".into(),
                description: "Read a file.".into(),
                input_schema: json!({"type": "object"}),
            }],
            max_tokens: None,
            temperature: Some(0.2),
        }
    }

    #[test]
    fn the_request_body_maps_roles_tools_and_settings() {
        let body = request_body(&request(), DEFAULT_MODEL, |_| None);
        assert_eq!(body["model"], DEFAULT_MODEL);
        assert_eq!(body["max_tokens"], DEFAULT_MAX_TOKENS);
        assert_eq!(body["system"], "You are a researcher.");
        assert_eq!(body["output_config"]["effort"], "high");
        assert_eq!(body["fallbacks"], "default");
        assert_eq!(body["cache_control"]["type"], "ephemeral");
        // Opus 5.5 refuses sampling settings and a disabled-thinking setting.
        assert!(body.get("temperature").is_none());
        assert!(body.get("thinking").is_none());
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3, "the system message is not a message");
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["content"][1]["type"], "tool_use");
        assert_eq!(messages[1]["content"][1]["input"]["path"], "train.py");
        assert_eq!(messages[2]["role"], "user");
        assert_eq!(messages[2]["content"][0]["type"], "tool_result");
        assert_eq!(messages[2]["content"][0]["tool_use_id"], "toolu_1");
        assert_eq!(body["tools"][0]["name"], "read_file");
        assert_eq!(body["tools"][0]["input_schema"]["type"], "object");
    }

    #[test]
    fn another_model_gets_no_effort_and_no_fallback() {
        let body = request_body(&request(), "claude-haiku-5-5", |_| None);
        assert!(body.get("output_config").is_none());
        assert!(body.get("fallbacks").is_none());
        assert_eq!(model_id("claude-haiku-5-5"), "claude-haiku-5-5");
        assert!(model_id(DEFAULT_MODEL).contains("effort=high"));
    }

    #[test]
    fn a_thinking_block_goes_back_unchanged_in_the_next_request() {
        let response = json!({
            "type": "message",
            "stop_reason": "tool_use",
            "content": [
                {"type": "thinking", "thinking": "", "signature": "sig"},
                {"type": "text", "text": "Reading."},
                {"type": "tool_use", "id": "toolu_1", "name": "read_file",
                 "input": {"path": "train.py"}}
            ],
            "usage": {"input_tokens": 1, "output_tokens": 1}
        });
        let turns = Turns::default();
        let (key, raw) = replayable(&response).unwrap();
        turns.put(key, raw).unwrap();
        // The adapter replays the turn as text and a tool call. The request
        // finds the raw turn and sends the thinking block first.
        let body = request_body(&request(), DEFAULT_MODEL, |visible| turns.get(visible));
        let assistant = &body["messages"][1]["content"];
        assert_eq!(assistant[0]["type"], "thinking");
        assert_eq!(assistant[0]["signature"], "sig");
        assert_eq!(assistant[2]["id"], "toolu_1");
        // A turn with text and tool calls only needs no replay.
        let plain = json!({"type": "message", "content": [{"type": "text", "text": "x"}]});
        assert!(replayable(&plain).is_none());
    }

    #[test]
    fn a_turn_survives_in_the_turn_directory() {
        let dir = tempfile::tempdir().unwrap();
        let turns = Turns {
            dir: Some(dir.path().to_path_buf()),
            ..Turns::default()
        };
        let raw = vec![json!({"type": "thinking", "thinking": "", "signature": "s"})];
        let shown = vec![json!({"type": "text", "text": "a"})];
        turns.put(turn_key(&shown), raw.clone()).unwrap();
        let restarted = Turns {
            dir: Some(dir.path().to_path_buf()),
            ..Turns::default()
        };
        assert_eq!(restarted.get(&shown), Some(raw));
    }

    #[test]
    fn blocks_before_a_fallback_boundary_keep_only_their_text() {
        let body = json!({"content": [
            {"type": "thinking", "thinking": "", "signature": "declined"},
            {"type": "text", "text": "partial"},
            {"type": "fallback"},
            {"type": "thinking", "thinking": "", "signature": "kept"},
            {"type": "text", "text": "answer"}
        ]});
        let blocks = turn_blocks(&body);
        let types: Vec<&str> = blocks.iter().filter_map(|b| b["type"].as_str()).collect();
        assert_eq!(types, ["text", "thinking", "text"]);
        assert_eq!(blocks[1]["signature"], "kept");
    }

    #[test]
    fn the_key_stays_out_of_debug_output() {
        let model = ClaudeModel::new("sk-ant-secret");
        let shown = format!("{model:?}");
        assert!(!shown.contains("sk-ant-secret"), "{shown}");
        assert!(shown.contains("redacted"));
    }

    #[test]
    fn a_tool_use_response_maps_to_calls_and_usage() {
        let body = json!({
            "type": "message",
            "role": "assistant",
            "model": "claude-opus-5-5",
            "stop_reason": "tool_use",
            "content": [
                {"type": "thinking", "thinking": "", "signature": "sig"},
                {"type": "text", "text": "Editing."},
                {"type": "tool_use", "id": "toolu_2", "name": "edit_train",
                 "input": {"old": "a", "new": "b"}}
            ],
            "usage": {"input_tokens": 100, "output_tokens": 20,
                      "cache_read_input_tokens": 30, "cache_creation_input_tokens": 5}
        });
        let response = parse_response(&body).unwrap();
        assert_eq!(response.stop_reason, StopReason::ToolUse);
        assert_eq!(
            response.content.len(),
            2,
            "thinking is not a ChatMessage part"
        );
        assert!(matches!(&response.content[1],
            ContentPart::ToolCall { name, .. } if name == "edit_train"));
        // The adapter counts the whole prompt in `input_tokens`.
        assert_eq!(response.usage.input_tokens, 135);
        assert_eq!(response.usage.output_tokens, 20);
    }

    #[test]
    fn stop_reasons_map_and_a_refusal_is_an_error() {
        let body = |stop: &str| {
            json!({"type": "message", "stop_reason": stop,
                   "content": [{"type": "text", "text": "x"}],
                   "usage": {"input_tokens": 1, "output_tokens": 1}})
        };
        assert_eq!(
            parse_response(&body("end_turn")).unwrap().stop_reason,
            StopReason::EndTurn
        );
        assert_eq!(
            parse_response(&body("max_tokens")).unwrap().stop_reason,
            StopReason::MaxTokens
        );
        assert_eq!(
            parse_response(&body("pause_turn")).unwrap().stop_reason,
            StopReason::Unknown
        );
        let refused = parse_response(&body("refusal")).unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::Provider);
        let junk = parse_response(&json!({"type": "error"})).unwrap_err();
        assert_eq!(junk.kind(), ErrorKind::Decode);
    }

    #[test]
    fn a_timeout_a_conflict_a_rate_limit_or_a_server_fault_retries() {
        assert_eq!(error_kind(429), ErrorKind::RateLimited);
        assert_eq!(error_kind(529), ErrorKind::Unavailable);
        assert_eq!(error_kind(500), ErrorKind::Unavailable);
        assert_eq!(error_kind(401), ErrorKind::Authentication);
        assert_eq!(error_kind(400), ErrorKind::Provider);
        assert_eq!(error_kind(408), ErrorKind::Unavailable);
        assert_eq!(error_kind(409), ErrorKind::Unavailable);
        assert!(error_kind(503).is_retryable());
        assert!(!error_kind(400).is_retryable());
    }
}
