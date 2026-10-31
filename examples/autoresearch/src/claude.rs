//! A Claude model for the agent loop, over the Messages API.
//!
//! [`ClaudeModel`] implements the provider-neutral `AgentModel`. The request
//! uses `claude-opus-5-5` by default, adaptive thinking (the model default),
//! an explicit effort, automatic prompt caching and the server-side refusal
//! fallback. The mapping functions are pure, so the tests need no network
//! and no key.
//!
//! `chat_stream` sends the same request with `"stream": true` and reads the
//! Server-Sent Events (issue #2268). It passes each text delta to the caller,
//! builds the message that a plain request returns, and maps that message
//! with the same code. Each tool asks for eager input streaming, so the
//! adapter parses each tool input in full and refuses one that is not JSON.
//!
//! The adapter's `ChatMessage` carries text, tool calls and tool results. It
//! has no place for a thinking block, and the API wants each thinking block
//! back unchanged in a tool loop. So the model keeps each raw assistant turn.
//! Its key is the hash of the messages before it and of its own text and
//! tool calls. Two turns with the same text in two conversations then keep
//! their own blocks. The next request finds the turn by the same hash and
//! sends the raw blocks. With a turn directory, the turns survive a restart.

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
/// The API version header value. The requests send no sampling setting
/// and no document block, so the version changes nothing else.
pub const API_VERSION: &str = "2026-10-21";
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

impl ClaudeModel {
    /// Send `body` and return the response, or the API error.
    async fn send(&self, body: &Value) -> Result<reqwest::Response, AgentError> {
        let mut call = self
            .client
            .post(ENDPOINT)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", API_VERSION);
        if self.model == DEFAULT_MODEL {
            call = call.header("anthropic-beta", FALLBACK_BETA);
        }
        let response =
            call.json(body).send().await.map_err(|e| {
                AgentError::new(ErrorKind::Transport, format!("request failed: {e}"))
            })?;
        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(response);
        }
        // The body names the error. It holds no secret: the key is a header.
        let text = Self::body(response).await?;
        let detail: String = text.chars().take(500).collect();
        Err(AgentError::new(
            error_kind(status),
            format!("HTTP {status}: {detail}"),
        ))
    }

    /// Map a whole message body, and keep its raw turn for a replay.
    fn finish(&self, request: &ChatRequest, body: &Value) -> Result<ChatResponse, AgentError> {
        let response = parse_response(body)?;
        if let Some((key, raw)) = replayable(&turn_context(request), body) {
            self.turns.put(key, raw)?;
        }
        Ok(response)
    }
}

impl AgentModel for ClaudeModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let body = request_body(request, &self.model, |before, visible| {
                self.turns.get(before, visible)
            });
            let text = Self::body(self.send(&body).await?).await?;
            let body: Value = serde_json::from_str(&text)
                .map_err(|e| AgentError::new(ErrorKind::Decode, format!("not JSON: {e}")))?;
            self.finish(request, &body)
        })
    }

    fn chat_stream<'a>(
        &'a self,
        request: &'a ChatRequest,
        on_delta: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let body = streaming_body(request_body(request, &self.model, |before, visible| {
                self.turns.get(before, visible)
            }));
            let mut response = self.send(&body).await?;
            let mut stream = StreamAssembler::default();
            let mut frames = SseFrames::default();
            let mut read = 0_usize;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|e| AgentError::new(ErrorKind::Transport, format!("stream lost: {e}")))?
            {
                read = read.saturating_add(chunk.len());
                if read > MAX_BODY_BYTES {
                    return Err(AgentError::new(
                        ErrorKind::Decode,
                        format!("the stream is over {MAX_BODY_BYTES} bytes"),
                    ));
                }
                for data in frames.push(&chunk) {
                    stream.event(&data, on_delta)?;
                }
            }
            for data in frames.end() {
                stream.event(&data, on_delta)?;
            }
            let body = stream.finish()?;
            self.finish(request, &body)
        })
    }

    fn model_id(&self) -> Option<&str> {
        Some(&self.id)
    }
}

/// The streamed form of `body`: `"stream": true`, and eager input streaming
/// on each tool. The assembler parses each tool input in full.
#[must_use]
pub fn streaming_body(mut body: Value) -> Value {
    body["stream"] = json!(true);
    if let Some(tools) = body["tools"].as_array_mut() {
        for tool in tools {
            tool["eager_input_streaming"] = json!(true);
        }
    }
    body
}

/// Splits a Server-Sent Events byte stream into the `data` of each event.
///
/// An event ends at a blank line. A line ends at `\r\n`, `\n` or `\r`. A
/// network chunk can end inside a line, inside a `\r\n` pair or inside a
/// multi-byte character. So the parser keeps bytes, and it decodes a line
/// only when the line is whole. The rest waits for the next chunk.
#[derive(Debug, Default)]
pub struct SseFrames {
    /// The bytes of a line that is not whole yet.
    pending: Vec<u8>,
    /// The `data` lines of the event that is not whole yet.
    data: Vec<String>,
    /// The last chunk ended with `\r`. A `\n` at the start of the next
    /// chunk is the rest of that line end.
    after_cr: bool,
}

impl SseFrames {
    /// Add `bytes`. Returns the data of each event that they complete.
    pub fn push(&mut self, mut bytes: &[u8]) -> Vec<String> {
        if self.after_cr && !bytes.is_empty() {
            self.after_cr = false;
            bytes = bytes.strip_prefix(b"\n").unwrap_or(bytes);
        }
        self.pending.extend_from_slice(bytes);
        let mut out = Vec::new();
        let mut start = 0;
        while let Some(at) = self.pending[start..]
            .iter()
            .position(|byte| matches!(byte, b'\n' | b'\r'))
        {
            let end = start + at;
            let mut next = end + 1;
            if self.pending[end] == b'\r' {
                match self.pending.get(next) {
                    Some(b'\n') => next += 1,
                    Some(_) => {}
                    // The `\n` of a `\r\n` pair can come in the next chunk.
                    None => self.after_cr = true,
                }
            }
            let line = String::from_utf8_lossy(&self.pending[start..end]).into_owned();
            self.line(&line, &mut out);
            start = next;
        }
        self.pending.drain(..start);
        out
    }

    /// The data of an event that the stream ended without a blank line.
    pub fn end(&mut self) -> Vec<String> {
        let rest = std::mem::take(&mut self.pending);
        self.after_cr = false;
        let mut out = Vec::new();
        if !rest.is_empty() {
            self.line(&String::from_utf8_lossy(&rest), &mut out);
        }
        self.line("", &mut out);
        out
    }

    /// Apply one whole line. A blank line ends the event.
    fn line(&mut self, line: &str, out: &mut Vec<String>) {
        if line.is_empty() {
            if !self.data.is_empty() {
                out.push(self.data.join("\n"));
                self.data.clear();
            }
        } else if let Some(data) = line.strip_prefix("data:") {
            self.data
                .push(data.strip_prefix(' ').unwrap_or(data).to_owned());
        }
    }
}

/// Builds the message of a streamed response from its events.
///
/// The result has the shape of a plain response body, so
/// [`parse_response`] and [`replayable`] read it.
#[derive(Debug, Default)]
pub struct StreamAssembler {
    message: Option<Value>,
    blocks: Vec<Value>,
    /// The input text of each tool-use block, by index.
    inputs: HashMap<usize, String>,
    stopped: bool,
}

impl StreamAssembler {
    /// Apply the data of one event. Pass each text delta to `on_delta`.
    ///
    /// Every other event passes `""` to `on_delta`. The harness joins small
    /// deltas, and it sends the text that waits only when `on_delta` runs.
    /// So the empty piece lets the text go out while the model thinks or
    /// writes a tool input.
    ///
    /// # Errors
    ///
    /// An `error` event is the error of its type. Data that is not JSON, or
    /// a delta for no block, is a `Decode` error.
    pub fn event(&mut self, data: &str, on_delta: &dyn Fn(&str)) -> Result<(), AgentError> {
        let event: Value = serde_json::from_str(data).map_err(|e| {
            AgentError::new(ErrorKind::Decode, format!("an event is not JSON: {e}"))
        })?;
        let is_text =
            event["type"] == "content_block_delta" && event["delta"]["type"] == "text_delta";
        self.apply(&event, on_delta)?;
        if !is_text {
            on_delta("");
        }
        Ok(())
    }

    /// Apply one parsed event.
    fn apply(&mut self, event: &Value, on_delta: &dyn Fn(&str)) -> Result<(), AgentError> {
        match event["type"].as_str() {
            Some("message_start") => self.message = Some(event["message"].clone()),
            Some("content_block_start") => {
                let index = block_index(event)?;
                if self.blocks.len() <= index {
                    self.blocks.resize(index + 1, Value::Null);
                }
                self.blocks[index] = event["content_block"].clone();
            }
            Some("content_block_delta") => self.delta(event, on_delta)?,
            Some("content_block_stop") => self.close_block(block_index(event)?)?,
            Some("message_delta") => {
                let message = self.message.get_or_insert_with(|| json!({}));
                if let Some(delta) = event["delta"].as_object() {
                    for (key, value) in delta {
                        message[key] = value.clone();
                    }
                }
                if let Some(usage) = event["usage"].as_object() {
                    for (key, value) in usage {
                        message["usage"][key] = value.clone();
                    }
                }
            }
            Some("message_stop") => self.stopped = true,
            Some("error") => {
                let kind = match event["error"]["type"].as_str() {
                    Some("overloaded_error" | "api_error") => ErrorKind::Unavailable,
                    Some("rate_limit_error") => ErrorKind::RateLimited,
                    _ => ErrorKind::Provider,
                };
                let detail: String = event["error"]["message"]
                    .as_str()
                    .unwrap_or("no message")
                    .chars()
                    .take(500)
                    .collect();
                return Err(AgentError::new(kind, format!("stream error: {detail}")));
            }
            // A ping, and any later event type, changes nothing.
            _ => {}
        }
        Ok(())
    }

    fn delta(&mut self, event: &Value, on_delta: &dyn Fn(&str)) -> Result<(), AgentError> {
        let index = block_index(event)?;
        let block = self
            .blocks
            .get_mut(index)
            .filter(|block| !block.is_null())
            .ok_or_else(|| AgentError::new(ErrorKind::Decode, "a delta for no block"))?;
        let delta = &event["delta"];
        let append = |block: &mut Value, key: &str, piece: &str| {
            let text = format!("{}{piece}", block[key].as_str().unwrap_or_default());
            block[key] = json!(text);
        };
        match delta["type"].as_str() {
            Some("text_delta") => {
                let piece = delta["text"].as_str().unwrap_or_default();
                append(block, "text", piece);
                on_delta(piece);
            }
            Some("thinking_delta") => {
                append(
                    block,
                    "thinking",
                    delta["thinking"].as_str().unwrap_or_default(),
                );
            }
            Some("signature_delta") => {
                append(
                    block,
                    "signature",
                    delta["signature"].as_str().unwrap_or_default(),
                );
            }
            Some("input_json_delta") => {
                self.inputs
                    .entry(index)
                    .or_default()
                    .push_str(delta["partial_json"].as_str().unwrap_or_default());
            }
            _ => {}
        }
        Ok(())
    }

    /// Parse the whole input of a tool-use block. Eager input streaming
    /// leaves the check to the client, so input that is not JSON fails.
    fn close_block(&mut self, index: usize) -> Result<(), AgentError> {
        let Some(text) = self.inputs.remove(&index) else {
            return Ok(());
        };
        let input: Value = if text.trim().is_empty() {
            json!({})
        } else {
            serde_json::from_str(&text).map_err(|e| {
                AgentError::new(
                    ErrorKind::Decode,
                    format!("a streamed tool input is not JSON: {e}"),
                )
            })?
        };
        if let Some(block) = self.blocks.get_mut(index) {
            block["input"] = input;
        }
        Ok(())
    }

    /// The whole message body.
    ///
    /// # Errors
    ///
    /// A stream with no `message_start` or no `message_stop` is a
    /// `Transport` error: it ended early, so a retry asks again.
    pub fn finish(self) -> Result<Value, AgentError> {
        let (Some(mut message), true) = (self.message, self.stopped) else {
            return Err(AgentError::new(
                ErrorKind::Transport,
                "the stream ended before the message",
            ));
        };
        message["content"] =
            Value::Array(self.blocks.into_iter().filter(|b| !b.is_null()).collect());
        Ok(message)
    }
}

/// The `index` of a content-block event.
fn block_index(event: &Value) -> Result<usize, AgentError> {
    event["index"]
        .as_u64()
        .and_then(|index| usize::try_from(index).ok())
        .filter(|index| *index < 4096)
        .ok_or_else(|| AgentError::new(ErrorKind::Decode, "a content-block event has no index"))
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

    fn get(&self, before: &[Value], visible: &[Value]) -> Option<Vec<Value>> {
        let key = turn_key(before, visible);
        if let Some(raw) = self
            .memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
        {
            return Some(raw.clone());
        }
        let bytes = crate::stores::read_regular(&self.path(&key)?).ok()??;
        serde_json::from_slice(&bytes).ok()
    }
}

/// The key of a turn: the hash of the context `before` it, as
/// [`turn_context`] gives it, and of its text and tool-call blocks.
fn turn_key(before: &[Value], visible: &[Value]) -> String {
    crate::stores::hashed(&json!([before, visible]).to_string())
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
///
/// `before` is the [`turn_context`] of the request that got the response.
#[must_use]
pub fn replayable(before: &[Value], body: &Value) -> Option<(String, Vec<Value>)> {
    let blocks = turn_blocks(body);
    let shown = visible(&blocks);
    (blocks.len() > shown.len()).then(|| (turn_key(before, &shown), blocks))
}

/// What a turn key hashes before a turn: the system prompt, then the
/// messages as [`wire_messages`] gives them. `eval` swaps the system prompt,
/// so each prompt keeps its own raw blocks.
#[must_use]
pub fn turn_context(request: &ChatRequest) -> Vec<Value> {
    std::iter::once(json!({"system": system_text(request)}))
        .chain(wire_messages(request))
        .collect()
}

/// The system prompt of `request`: its system text parts, in order.
fn system_text(request: &ChatRequest) -> Vec<&str> {
    request
        .messages
        .iter()
        .filter(|m| m.role == ChatRole::System)
        .flat_map(|m| &m.content)
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// The messages of `request` as the API takes them, with no system message
/// and no raw blocks. So a turn key does not change when a later request
/// replays a turn.
#[must_use]
pub fn wire_messages(request: &ChatRequest) -> Vec<Value> {
    request
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
            json!({"role": role, "content": content})
        })
        .collect()
}

/// The JSON body of one Messages API request. `replay` returns the raw
/// blocks of an assistant turn from the [`turn_context`] before it and its
/// text and tool-call blocks.
#[must_use]
pub fn request_body(
    request: &ChatRequest,
    model: &str,
    replay: impl Fn(&[Value], &[Value]) -> Option<Vec<Value>>,
) -> Value {
    let system = system_text(request);
    // The first entry is the system prompt, so a message at `index` of the
    // wire has the context `keyed[..=index]` before it.
    let keyed = turn_context(request);
    let messages: Vec<Value> = keyed[1..]
        .iter()
        .enumerate()
        .map(|(index, message)| {
            if message["role"] != "assistant" {
                return message.clone();
            }
            let shown = message["content"].as_array().cloned().unwrap_or_default();
            replay(&keyed[..=index], &shown).map_or_else(
                || message.clone(),
                |raw| json!({"role": "assistant", "content": raw}),
            )
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
        let body = request_body(&request(), DEFAULT_MODEL, |_, _| None);
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
        let body = request_body(&request(), "claude-haiku-5-5", |_, _| None);
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
        // The request that got the response held the messages before it.
        let mut asked = request();
        asked.messages.truncate(2);
        let (key, raw) = replayable(&turn_context(&asked), &response).unwrap();
        turns.put(key, raw).unwrap();
        // The adapter replays the turn as text and a tool call. The request
        // finds the raw turn and sends the thinking block first.
        let body = request_body(&request(), DEFAULT_MODEL, |before, visible| {
            turns.get(before, visible)
        });
        let assistant = &body["messages"][1]["content"];
        assert_eq!(assistant[0]["type"], "thinking");
        assert_eq!(assistant[0]["signature"], "sig");
        assert_eq!(assistant[2]["id"], "toolu_1");
        // A turn with text and tool calls only needs no replay.
        let plain = json!({"type": "message", "content": [{"type": "text", "text": "x"}]});
        assert!(replayable(&[], &plain).is_none());
    }

    /// Two conversations can end a turn with the same text and different
    /// thinking. Each conversation gets its own thinking block back.
    #[test]
    fn a_turn_with_the_same_text_in_another_conversation_keeps_its_own_block() {
        let turns = Turns::default();
        let conversation = |task: &str| ChatRequest {
            messages: vec![
                ChatMessage::text(ChatRole::System, "You are a researcher."),
                ChatMessage::text(ChatRole::User, task),
            ],
            ..request()
        };
        for (task, signature) in [("Run experiment 1.", "one"), ("Run experiment 2.", "two")] {
            let response = json!({
                "type": "message",
                "stop_reason": "end_turn",
                "content": [
                    {"type": "thinking", "thinking": "", "signature": signature},
                    {"type": "text", "text": "Done."}
                ],
                "usage": {"input_tokens": 1, "output_tokens": 1}
            });
            let (key, raw) = replayable(&turn_context(&conversation(task)), &response).unwrap();
            turns.put(key, raw).unwrap();
        }
        for (task, signature) in [("Run experiment 1.", "one"), ("Run experiment 2.", "two")] {
            let mut next = conversation(task);
            next.messages
                .push(ChatMessage::text(ChatRole::Assistant, "Done."));
            next.messages
                .push(ChatMessage::text(ChatRole::User, "And now?"));
            let body = request_body(&next, DEFAULT_MODEL, |before, visible| {
                turns.get(before, visible)
            });
            assert_eq!(body["messages"][1]["content"][0]["signature"], signature);
        }
    }

    /// `eval` asks again under a candidate `program.md`, with the same
    /// messages. Each system prompt gets its own thinking block back.
    #[test]
    fn a_turn_under_another_system_prompt_keeps_its_own_block() {
        let turns = Turns::default();
        let conversation = |system: &str| ChatRequest {
            messages: vec![
                ChatMessage::text(ChatRole::System, system),
                ChatMessage::text(ChatRole::User, "Run experiment 1."),
            ],
            ..request()
        };
        let prompts = [("Program A.", "a"), ("Program B.", "b")];
        for (system, signature) in prompts {
            let response = json!({
                "type": "message",
                "stop_reason": "end_turn",
                "content": [
                    {"type": "thinking", "thinking": "", "signature": signature},
                    {"type": "text", "text": "Done."}
                ],
                "usage": {"input_tokens": 1, "output_tokens": 1}
            });
            let (key, raw) = replayable(&turn_context(&conversation(system)), &response).unwrap();
            turns.put(key, raw).unwrap();
        }
        for (system, signature) in prompts {
            let mut next = conversation(system);
            next.messages
                .push(ChatMessage::text(ChatRole::Assistant, "Done."));
            next.messages
                .push(ChatMessage::text(ChatRole::User, "And now?"));
            let body = request_body(&next, DEFAULT_MODEL, |before, visible| {
                turns.get(before, visible)
            });
            assert_eq!(body["messages"][1]["content"][0]["signature"], signature);
        }
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
        turns.put(turn_key(&[], &shown), raw.clone()).unwrap();
        let restarted = Turns {
            dir: Some(dir.path().to_path_buf()),
            ..Turns::default()
        };
        assert_eq!(restarted.get(&[], &shown), Some(raw));
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

    /// One SSE event with `data`.
    fn sse(data: &Value) -> String {
        format!("event: x\ndata: {data}\n\n")
    }

    fn streamed(events: &[Value]) -> Result<(Value, Vec<String>), AgentError> {
        streamed_in(events, 7)
    }

    /// Stream `events` in network chunks of `size` bytes.
    fn streamed_in(events: &[Value], size: usize) -> Result<(Value, Vec<String>), AgentError> {
        let wire: String = events.iter().map(sse).collect();
        let mut frames = SseFrames::default();
        let mut stream = StreamAssembler::default();
        let seen = Mutex::new(Vec::new());
        let on_delta = |piece: &str| seen.lock().unwrap().push(piece.to_owned());
        // Split the wire in odd places, as a network does.
        for chunk in wire.as_bytes().chunks(size) {
            for data in frames.push(chunk) {
                stream.event(&data, &on_delta)?;
            }
        }
        for data in frames.end() {
            stream.event(&data, &on_delta)?;
        }
        Ok((stream.finish()?, seen.into_inner().unwrap()))
    }

    fn start() -> Value {
        json!({"type": "message_start", "message": {"type": "message", "role": "assistant",
            "content": [], "stop_reason": null, "usage": {"input_tokens": 40, "output_tokens": 1}}})
    }

    #[test]
    fn a_stream_builds_the_message_of_a_plain_response() {
        let (body, deltas) = streamed(&[
            start(),
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "thinking", "thinking": "", "signature": ""}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "signature_delta", "signature": "sig"}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "content_block_start", "index": 1,
                   "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 1,
                   "delta": {"type": "text_delta", "text": "Hel"}}),
            json!({"type": "ping"}),
            json!({"type": "content_block_delta", "index": 1,
                   "delta": {"type": "text_delta", "text": "lo"}}),
            json!({"type": "content_block_stop", "index": 1}),
            json!({"type": "content_block_start", "index": 2,
                   "content_block": {"type": "tool_use", "id": "toolu_1", "name": "search",
                                     "input": {}}}),
            json!({"type": "content_block_delta", "index": 2,
                   "delta": {"type": "input_json_delta", "partial_json": "{\"q\": \"a"}}),
            json!({"type": "content_block_delta", "index": 2,
                   "delta": {"type": "input_json_delta", "partial_json": "b\"}"}}),
            json!({"type": "content_block_stop", "index": 2}),
            json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"},
                   "usage": {"output_tokens": 12}}),
            json!({"type": "message_stop"}),
        ])
        .unwrap();
        let text: Vec<&String> = deltas.iter().filter(|d| !d.is_empty()).collect();
        assert_eq!(text, ["Hel", "lo"]);
        let response = parse_response(&body).unwrap();
        assert_eq!(response.stop_reason, StopReason::ToolUse);
        assert_eq!(response.content[0], ContentPart::Text("Hello".into()));
        assert!(matches!(&response.content[1],
            ContentPart::ToolCall { arguments, .. } if arguments["q"] == "ab"));
        assert_eq!(
            (response.usage.input_tokens, response.usage.output_tokens),
            (40, 12)
        );
        // The thinking block keeps its signature for the replay.
        let (_, raw) = replayable(&[], &body).unwrap();
        assert_eq!(raw[0]["signature"], "sig");
    }

    #[test]
    fn a_stream_that_breaks_off_or_fails_is_an_error() {
        let early = streamed(&[start()]).unwrap_err();
        assert_eq!(early.kind(), ErrorKind::Transport);
        let overloaded = streamed(&[
            start(),
            json!({"type": "error", "error": {"type": "overloaded_error", "message": "busy"}}),
        ])
        .unwrap_err();
        assert_eq!(overloaded.kind(), ErrorKind::Unavailable);
        let bad_input = streamed(&[
            start(),
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "tool_use", "id": "t", "name": "n", "input": {}}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "input_json_delta", "partial_json": "{\"q\": "}}),
            json!({"type": "content_block_stop", "index": 0}),
        ])
        .unwrap_err();
        assert_eq!(bad_input.kind(), ErrorKind::Decode);
        let orphan = streamed(&[
            start(),
            json!({"type": "content_block_delta", "index": 3,
                   "delta": {"type": "text_delta", "text": "x"}}),
        ])
        .unwrap_err();
        assert_eq!(orphan.kind(), ErrorKind::Decode);
    }

    #[test]
    fn a_streamed_request_asks_for_eager_tool_input() {
        let body = streaming_body(request_body(&request(), DEFAULT_MODEL, |_, _| None));
        assert_eq!(body["stream"], true);
        assert_eq!(body["tools"][0]["eager_input_streaming"], true);
        assert_eq!(API_VERSION, "2026-10-21");
    }

    #[test]
    fn sse_frames_join_data_lines_and_skip_comments() {
        let mut frames = SseFrames::default();
        assert_eq!(
            frames.push(b": ping\n\nevent: a\r\ndata: 1"),
            Vec::<String>::new()
        );
        assert_eq!(frames.push(b"\r\ndata: 2\r\n\r\n"), ["1\n2"]);
        assert_eq!(frames.push(b"data: 3"), Vec::<String>::new());
        assert_eq!(frames.end(), ["3"]);
    }

    #[test]
    fn a_line_end_split_across_chunks_or_a_bare_cr_ends_a_line_once() {
        let mut frames = SseFrames::default();
        // A `\r\n` pair split over two chunks is one line end.
        assert_eq!(frames.push(b"data: 1\r"), Vec::<String>::new());
        assert_eq!(frames.push(b"\ndata: 2\r"), Vec::<String>::new());
        assert_eq!(frames.push(b"\n\r"), ["1\n2"]);
        assert_eq!(frames.push(b"\n"), Vec::<String>::new());
        // A bare `\r` ends a line, and a blank one ends the event.
        assert_eq!(frames.push(b"data: 3\rdata: 4\r\r"), ["3\n4"]);
        assert_eq!(frames.end(), Vec::<String>::new());
    }

    #[test]
    fn a_character_split_across_chunks_stays_whole() {
        let text = "h\u{e9}llo \u{1f642}";
        let events = [
            start(),
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "text_delta", "text": text}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}}),
            json!({"type": "message_stop"}),
        ];
        // One byte per chunk splits each character of more than one byte.
        let (body, deltas) = streamed_in(&events, 1).unwrap();
        assert_eq!(deltas.concat(), text);
        assert_eq!(body["content"][0]["text"], text);
    }

    #[test]
    fn every_event_that_is_not_text_passes_an_empty_piece() {
        let (_, deltas) = streamed(&[
            start(),
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "text_delta", "text": "a"}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "content_block_start", "index": 1,
                   "content_block": {"type": "thinking", "thinking": "", "signature": ""}}),
            json!({"type": "content_block_delta", "index": 1,
                   "delta": {"type": "thinking_delta", "thinking": "hm"}}),
            json!({"type": "ping"}),
            json!({"type": "content_block_stop", "index": 1}),
            json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}}),
            json!({"type": "message_stop"}),
        ])
        .unwrap();
        // The harness sends the text that waits at each empty piece.
        assert_eq!(deltas, ["", "", "a", "", "", "", "", "", "", ""]);
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
