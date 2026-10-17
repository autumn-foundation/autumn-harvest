//! A Claude model for the agent loop, over the Messages API.
//!
//! [`ClaudeModel`] implements the provider-neutral `AgentModel`. The request
//! uses `claude-opus-5-5` by default, adaptive thinking (the model default),
//! an explicit effort, and the server-side refusal fallback. The two mapping
//! functions are pure, so the tests need no network and no key.
//!
//! One limit comes from the adapter. `ChatMessage` carries text, tool calls
//! and tool results, not thinking blocks. So a turn does not replay the
//! thinking of the turns before it. The API accepts a history with no
//! thinking blocks, and the model plans each turn again. To replay thinking
//! verbatim, see `examples/claude-agent-daemon`.

use autumn_harvest_agent::{AgentError, AgentModel, BoxFuture, ChatRequest, ChatResponse, ErrorKind};
use serde_json::Value;

/// The default model.
pub const DEFAULT_MODEL: &str = "claude-opus-5-5";
/// The Messages API endpoint.
pub const ENDPOINT: &str = "https://api.anthropic.com/v1/messages";
/// The API version header value.
pub const API_VERSION: &str = "2023-06-01";
/// The beta that turns on the server-side refusal fallback.
pub const FALLBACK_BETA: &str = "server-side-fallback-2026-07-01";
/// The output cap of one turn when the task sets none.
pub const DEFAULT_MAX_TOKENS: u32 = 16_000;

/// A Claude model over the Messages API.
#[derive(Debug, Clone)]
pub struct ClaudeModel {
    api_key: String,
    model: String,
    effort: String,
    client: reqwest::Client,
}

impl ClaudeModel {
    /// A model with `api_key`, the default model and `high` effort.
    #[must_use]
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            model: DEFAULT_MODEL.into(),
            effort: "high".into(),
            client: reqwest::Client::new(),
        }
    }

    /// Use `model` in place of the default.
    #[must_use]
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }
}

impl AgentModel for ClaudeModel {
    fn chat<'a>(&'a self, request: &'a ChatRequest) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let _ = (request, &self.api_key, &self.effort, &self.client);
        unimplemented!("green phase")
    }

    fn model_id(&self) -> Option<&str> {
        Some(&self.model)
    }
}

/// The JSON body of one Messages API request.
#[must_use]
pub fn request_body(request: &ChatRequest, model: &str, effort: &str) -> Value {
    let _ = (request, model, effort);
    unimplemented!("green phase")
}

/// Map one Messages API response body to a [`ChatResponse`].
///
/// # Errors
///
/// A `refusal` stop is a `Provider` error, so the run fails and says why.
/// A body that is not a message is a `Decode` error.
pub fn parse_response(body: &Value) -> Result<ChatResponse, AgentError> {
    let _ = body;
    unimplemented!("green phase")
}

/// The error kind of an HTTP status. A rate limit and a server fault retry.
#[must_use]
pub const fn error_kind(status: u16) -> ErrorKind {
    let _ = status;
    ErrorKind::Provider
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
        let body = request_body(&request(), DEFAULT_MODEL, "high");
        assert_eq!(body["model"], DEFAULT_MODEL);
        assert_eq!(body["max_tokens"], DEFAULT_MAX_TOKENS);
        assert_eq!(body["system"], "You are a researcher.");
        assert_eq!(body["output_config"]["effort"], "high");
        assert_eq!(body["fallbacks"], "default");
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
        assert_eq!(response.content.len(), 2, "thinking is not a ChatMessage part");
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
        assert_eq!(parse_response(&body("end_turn")).unwrap().stop_reason, StopReason::EndTurn);
        assert_eq!(parse_response(&body("max_tokens")).unwrap().stop_reason, StopReason::MaxTokens);
        assert_eq!(parse_response(&body("pause_turn")).unwrap().stop_reason, StopReason::Unknown);
        let refused = parse_response(&body("refusal")).unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::Provider);
        let junk = parse_response(&json!({"type": "error"})).unwrap_err();
        assert_eq!(junk.kind(), ErrorKind::Decode);
    }

    #[test]
    fn only_a_rate_limit_or_a_server_fault_retries() {
        assert_eq!(error_kind(429), ErrorKind::RateLimited);
        assert_eq!(error_kind(529), ErrorKind::Unavailable);
        assert_eq!(error_kind(500), ErrorKind::Unavailable);
        assert_eq!(error_kind(401), ErrorKind::Authentication);
        assert_eq!(error_kind(400), ErrorKind::Provider);
        assert!(error_kind(503).is_retryable());
        assert!(!error_kind(400).is_retryable());
    }
}
