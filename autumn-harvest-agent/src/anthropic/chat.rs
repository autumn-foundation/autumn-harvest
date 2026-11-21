//! A [`ChatRequest`] as a Messages API body, and a body as a
//! [`ChatResponse`].
//!
//! A [`ChatMessage`](crate::ChatMessage) has no place for a thinking block.
//! The API wants each thinking block back unchanged in a tool loop. So the
//! model keeps each raw assistant turn by key. A later request sends the raw
//! blocks in place of the text and the tool calls.
//!
//! The key of a turn is the hash of the context before it and of its own
//! text and tool calls. Two turns with the same text in two conversations
//! then keep their own blocks.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::wire::{self, DEFAULT_MAX_TOKENS};
use crate::{AgentError, ChatRequest, ChatResponse, ChatRole, ContentPart, ErrorKind, StopReason};

/// A durable store of raw assistant turns.
///
/// [`ClaudeModel`](super::ClaudeModel) keeps each turn in memory too. A
/// durable store lets a restarted process replay the turns of a run.
pub trait TurnStore: Send + Sync + fmt::Debug {
    /// The raw blocks of the turn with this key, if the store has them.
    fn get(&self, key: &str) -> Option<Vec<Value>>;

    /// Keep the raw blocks of the turn with this key.
    ///
    /// # Errors
    ///
    /// An error when the store cannot keep the turn. The model call then
    /// fails, so no answer goes on without its blocks.
    fn put(&self, key: &str, raw: &[Value]) -> Result<(), AgentError>;
}

/// The most raw turns that one model keeps in memory, with a durable store.
///
/// A worker shares one model across its runs, so the copy must not grow
/// for the life of the process. The oldest turn goes first, and the store
/// still holds it. With no store, the memory copy is the only copy. A run
/// that waits on a tool still needs its turn, so nothing goes.
pub const MAX_MEMORY_TURNS: usize = 4096;

/// The raw turns of one model: a memory copy and an optional durable store.
#[derive(Debug, Clone, Default)]
pub struct Turns {
    pub store: Option<Arc<dyn TurnStore>>,
    memory: Arc<Mutex<Memory>>,
}

/// The memory copy, with the order of its keys for the bound.
#[derive(Debug, Default)]
struct Memory {
    turns: HashMap<String, Vec<Value>>,
    order: VecDeque<String>,
}

impl Memory {
    /// Keep `raw` under `key`. With `bounded`, drop the oldest turns past
    /// the bound.
    fn insert(&mut self, key: String, raw: Vec<Value>, bounded: bool) {
        if self.turns.insert(key.clone(), raw).is_none() {
            self.order.push_back(key);
        }
        while bounded && self.order.len() > MAX_MEMORY_TURNS {
            if let Some(oldest) = self.order.pop_front() {
                self.turns.remove(&oldest);
            }
        }
    }
}

impl Turns {
    /// The same durable store with an empty memory copy of its own.
    #[must_use]
    pub fn fresh(&self) -> Self {
        Self {
            store: self.store.clone(),
            memory: Arc::default(),
        }
    }

    pub fn put(&self, key: String, raw: Vec<Value>) -> Result<(), AgentError> {
        if let Some(store) = &self.store {
            store.put(&key, &raw)?;
        }
        // Only a turn that the store holds can leave memory.
        let bounded = self.store.is_some();
        self.memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, raw, bounded);
        Ok(())
    }

    pub fn get(&self, before: &[Value], visible: &[Value]) -> Option<Vec<Value>> {
        let key = turn_key(before, visible);
        if let Some(raw) = self
            .memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .turns
            .get(&key)
        {
            return Some(raw.clone());
        }
        self.store.as_ref()?.get(&key)
    }
}

/// The key of a turn: the SHA-256, in hex, of the context `before` it and
/// of its text and tool-call blocks.
///
/// A durable store keeps turns under this key. So the key must not change
/// from one release to the next.
#[must_use]
pub fn turn_key(before: &[Value], visible: &[Value]) -> String {
    let digest = Sha256::digest(json!([before, visible]).to_string().as_bytes());
    digest
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
            hex
        })
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
/// messages as [`wire_messages`] gives them. So each system prompt keeps its
/// own raw blocks.
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

/// The JSON body of one Messages API request.
///
/// `replay` returns the raw blocks of an assistant turn from the
/// [`turn_context`] before it and its text and tool-call blocks. With
/// `fallbacks`, a model that takes them asks for the refusal fallback.
#[must_use]
pub fn request_body(
    request: &ChatRequest,
    model: &str,
    fallbacks: bool,
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
    // No `temperature`: the API version refuses sampling settings. The
    // top-level `cache_control` caches the growing transcript.
    let mut body = json!({
        "model": model,
        "max_tokens": request.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
        "messages": messages,
        "cache_control": {"type": "ephemeral"},
    });
    wire::apply_settings(&mut body, model);
    if fallbacks {
        wire::apply_fallbacks(&mut body, model);
    }
    if !system.is_empty() {
        body["system"] = json!(system.join("\n\n"));
    }
    if !tools.is_empty() {
        body["tools"] = json!(tools);
    }
    body
}

/// The streamed form of `body`: `"stream": true`, and eager input streaming
/// on each tool. The assembler parses each tool input in full.
#[must_use]
pub fn streaming_body(mut body: Value) -> Value {
    let Some(map) = body.as_object_mut() else {
        return body;
    };
    map.insert("stream".to_owned(), json!(true));
    if let Some(tools) = map.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools.iter_mut().filter_map(Value::as_object_mut) {
            tool.insert("eager_input_streaming".to_owned(), json!(true));
        }
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
        Some(wire::STOP_TOOL_USE) => StopReason::ToolUse,
        Some(wire::STOP_MAX_TOKENS) => StopReason::MaxTokens,
        Some(wire::STOP_REFUSAL) => {
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
    Ok(ChatResponse {
        content,
        stop_reason,
        usage: wire::usage(body),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use serde_json::json;

    use super::*;
    use crate::anthropic::{DEFAULT_MODEL, EFFORT};
    use crate::{ChatMessage, ToolDefinition};

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
            draw: None,
        }
    }

    #[test]
    fn the_request_body_maps_roles_tools_and_settings() {
        let body = request_body(&request(), DEFAULT_MODEL, true, |_, _| None);
        assert_eq!(body["model"], DEFAULT_MODEL);
        assert_eq!(body["max_tokens"], DEFAULT_MAX_TOKENS);
        assert_eq!(body["system"], "You are a researcher.");
        assert_eq!(body["thinking"]["type"], "adaptive");
        assert_eq!(body["output_config"]["effort"], EFFORT);
        assert_eq!(body["fallbacks"], "default");
        assert_eq!(body["cache_control"]["type"], "ephemeral");
        // The API version refuses sampling settings.
        assert!(body.get("temperature").is_none());
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
    fn another_model_gets_no_settings_and_fallbacks_can_be_off() {
        let body = request_body(&request(), "claude-haiku-5-5", true, |_, _| None);
        for key in ["thinking", "output_config", "fallbacks"] {
            assert!(body.get(key).is_none(), "{key}: {body}");
        }
        let off = request_body(&request(), DEFAULT_MODEL, false, |_, _| None);
        assert!(off.get("fallbacks").is_none());
        assert_eq!(off["output_config"]["effort"], EFFORT);
    }

    #[test]
    fn a_streamed_request_asks_for_eager_tool_input() {
        let body = streaming_body(request_body(&request(), DEFAULT_MODEL, true, |_, _| None));
        assert_eq!(body["stream"], true);
        assert_eq!(body["tools"][0]["eager_input_streaming"], true);
    }

    #[test]
    fn the_turn_key_is_stable_across_releases() {
        // A durable store keeps turns under this key. A change here strands
        // every turn that a store already holds.
        let key = turn_key(&[], &[json!({"type": "text", "text": "a"})]);
        assert_eq!(key, TURN_KEY_OF_A);
    }

    /// The pinned key of [`the_turn_key_is_stable_across_releases`].
    const TURN_KEY_OF_A: &str = "b7a2938c960c1c6955c99292910d767edc89d2d117f2343da65badacccb5be11";

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
        let body = request_body(&request(), DEFAULT_MODEL, true, |before, visible| {
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
        let prompts = [("Run experiment 1.", "one"), ("Run experiment 2.", "two")];
        keeps_its_own_block(&turns, &conversation, prompts);
    }

    /// An eval asks again under another system prompt, with the same
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
        keeps_its_own_block(
            &turns,
            &conversation,
            [("Program A.", "a"), ("Program B.", "b")],
        );
    }

    /// Record one thinking turn for each `(input, signature)`, then check
    /// that the next request of each conversation sends its own signature.
    fn keeps_its_own_block(
        turns: &Turns,
        conversation: &dyn Fn(&str) -> ChatRequest,
        prompts: [(&str, &str); 2],
    ) {
        for (input, signature) in prompts {
            let response = json!({
                "type": "message",
                "stop_reason": "end_turn",
                "content": [
                    {"type": "thinking", "thinking": "", "signature": signature},
                    {"type": "text", "text": "Done."}
                ],
                "usage": {"input_tokens": 1, "output_tokens": 1}
            });
            let (key, raw) = replayable(&turn_context(&conversation(input)), &response).unwrap();
            turns.put(key, raw).unwrap();
        }
        for (input, signature) in prompts {
            let mut next = conversation(input);
            next.messages
                .push(ChatMessage::text(ChatRole::Assistant, "Done."));
            next.messages
                .push(ChatMessage::text(ChatRole::User, "And now?"));
            let body = request_body(&next, DEFAULT_MODEL, true, |before, visible| {
                turns.get(before, visible)
            });
            assert_eq!(body["messages"][1]["content"][0]["signature"], signature);
        }
    }

    /// A store that keeps turns in a map, as a durable store would.
    #[derive(Debug, Default)]
    struct MapStore(Mutex<HashMap<String, Vec<Value>>>);

    impl TurnStore for MapStore {
        fn get(&self, key: &str) -> Option<Vec<Value>> {
            self.0.lock().unwrap().get(key).cloned()
        }

        fn put(&self, key: &str, raw: &[Value]) -> Result<(), AgentError> {
            self.0.lock().unwrap().insert(key.to_owned(), raw.to_vec());
            Ok(())
        }
    }

    #[test]
    fn a_turn_survives_in_the_durable_store() {
        let store: Arc<dyn TurnStore> = Arc::new(MapStore::default());
        let turns = Turns {
            store: Some(store.clone()),
            ..Turns::default()
        };
        let raw = vec![json!({"type": "thinking", "thinking": "", "signature": "s"})];
        let shown = vec![json!({"type": "text", "text": "a"})];
        turns.put(turn_key(&[], &shown), raw.clone()).unwrap();
        let restarted = Turns {
            store: Some(store),
            ..Turns::default()
        };
        assert_eq!(restarted.get(&[], &shown), Some(raw));
    }

    #[test]
    fn with_a_store_the_memory_copy_keeps_the_newest_turns_only() {
        let store: Arc<dyn TurnStore> = Arc::new(MapStore::default());
        let turns = Turns {
            store: Some(store),
            ..Turns::default()
        };
        let shown = |n: usize| vec![json!({"type": "text", "text": n.to_string()})];
        for n in 0..=MAX_MEMORY_TURNS {
            turns.put(turn_key(&[], &shown(n)), vec![json!(n)]).unwrap();
        }
        let memory = turns.memory.lock().unwrap();
        assert_eq!(memory.turns.len(), MAX_MEMORY_TURNS, "the copy is bounded");
        assert!(!memory.turns.contains_key(&turn_key(&[], &shown(0))));
        drop(memory);
        // The oldest turn left memory, and the store still holds it.
        assert_eq!(turns.get(&[], &shown(0)), Some(vec![json!(0)]));
        assert!(
            turns.fresh().memory.lock().unwrap().turns.is_empty(),
            "a fresh copy is empty"
        );
    }

    /// With no store, the memory copy is the only copy. An evicted turn of
    /// a run that waits on a tool would lose its thinking blocks.
    #[test]
    fn with_no_store_no_turn_leaves_memory() {
        let turns = Turns::default();
        let shown = |n: usize| vec![json!({"type": "text", "text": n.to_string()})];
        for n in 0..=MAX_MEMORY_TURNS {
            turns.put(turn_key(&[], &shown(n)), vec![json!(n)]).unwrap();
        }
        assert_eq!(turns.get(&[], &shown(0)), Some(vec![json!(0)]));
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
        assert_eq!(response.usage.cache_read_tokens, 30);
        assert_eq!(response.usage.cache_write_tokens, 5);
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
}
