//! The Chat Completions wire format, with no I/O.

use serde_json::{Value, json};

use crate::provider::{error_kind, printable};
use crate::{
    AgentError, ChatRequest, ChatResponse, ChatRole, ContentPart, ErrorKind, StopReason, TokenUsage,
};

/// The base URL of the `OpenAI` API.
pub const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";

/// The base URL of `OpenRouter`.
pub const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// The base URL of a local Ollama server.
pub const OLLAMA_BASE_URL: &str = "http://localhost:11434/v1";

/// The field that carries the output cap of a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TokenField {
    /// `max_tokens`. Most compatible servers take it.
    #[default]
    MaxTokens,
    /// `max_completion_tokens`. The `OpenAI` reasoning models need it.
    MaxCompletionTokens,
}

/// The settings of a request that [`ChatRequest`] does not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// The field of the output cap.
    pub token_field: TokenField,
    /// Send the draw number as `seed`, so each draw is its own sample.
    pub seed: bool,
    /// Ask a stream for its usage in a last chunk.
    pub stream_usage: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            token_field: TokenField::MaxTokens,
            seed: true,
            stream_usage: true,
        }
    }
}

/// The Chat Completions endpoint under `base_url`.
///
/// # Errors
///
/// A `Config` error when `base_url` is not an `http` or `https` URL.
pub fn endpoint(base_url: &str) -> Result<String, AgentError> {
    let url = parse(base_url)?;
    Ok(format!(
        "{}/chat/completions",
        url.as_str().trim_end_matches('/')
    ))
}

/// The host of `base_url`, with its port when the URL names one.
///
/// # Errors
///
/// As [`endpoint`].
pub fn host(base_url: &str) -> Result<String, AgentError> {
    let url = parse(base_url)?;
    let name = url.host_str().unwrap_or_default();
    Ok(url
        .port()
        .map_or_else(|| name.to_owned(), |port| format!("{name}:{port}")))
}

/// `base_url` as a URL with a host, or a `Config` error.
fn parse(base_url: &str) -> Result<reqwest::Url, AgentError> {
    let refused = || {
        AgentError::new(
            ErrorKind::Config,
            format!(
                "the base URL {} is not an http or https URL",
                printable(base_url)
            ),
        )
    };
    let url = reqwest::Url::parse(base_url.trim()).map_err(|_| refused())?;
    let web = matches!(url.scheme(), "http" | "https");
    if !web || url.host_str().is_none_or(str::is_empty) {
        return Err(refused());
    }
    Ok(url)
}

/// The text parts of `parts`, joined by a newline.
fn text_of(parts: &[ContentPart]) -> String {
    parts
        .iter()
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The messages of `request` as the API takes them.
///
/// Each tool result is a `tool` message of its own, before the other parts
/// of its message. An assistant turn carries its calls in `tool_calls`, with
/// the arguments as a JSON string.
#[must_use]
pub fn wire_messages(request: &ChatRequest) -> Vec<Value> {
    let mut out = Vec::new();
    for message in &request.messages {
        for part in &message.content {
            if let ContentPart::ToolResult {
                tool_call_id,
                content,
            } = part
            {
                out.push(json!({"role": "tool", "tool_call_id": tool_call_id, "content": content}));
            }
        }
        let text = text_of(&message.content);
        match message.role {
            ChatRole::Assistant => {
                let calls: Vec<Value> = message
                    .content
                    .iter()
                    .filter_map(|part| match part {
                        ContentPart::ToolCall {
                            id,
                            name,
                            arguments,
                        } => Some(json!({
                            "id": id,
                            "type": "function",
                            "function": {"name": name, "arguments": arguments.to_string()},
                        })),
                        _ => None,
                    })
                    .collect();
                if calls.is_empty() {
                    out.push(json!({"role": "assistant", "content": text}));
                } else {
                    let content = if text.is_empty() {
                        Value::Null
                    } else {
                        json!(text)
                    };
                    out.push(json!({"role": "assistant", "content": content, "tool_calls": calls}));
                }
            }
            role if !text.is_empty() => {
                let role = if role == ChatRole::System {
                    "system"
                } else {
                    "user"
                };
                out.push(json!({"role": role, "content": text}));
            }
            _ => {}
        }
    }
    out
}

/// The JSON body of one request.
///
/// The output cap, the temperature and the seed go only when the request
/// sets them. A server then keeps its own default.
#[must_use]
pub fn request_body(request: &ChatRequest, model: &str, settings: Settings) -> Value {
    let mut body = json!({"model": model, "messages": wire_messages(request)});
    if let Some(cap) = request.max_tokens {
        let field = match settings.token_field {
            TokenField::MaxTokens => "max_tokens",
            TokenField::MaxCompletionTokens => "max_completion_tokens",
        };
        body[field] = json!(cap);
    }
    if let Some(temperature) = request.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(draw) = request.draw.filter(|_| settings.seed) {
        body["seed"] = json!(draw.get());
    }
    let tools: Vec<Value> = request
        .tools
        .iter()
        .map(|tool| {
            json!({"type": "function", "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema,
            }})
        })
        .collect();
    if !tools.is_empty() {
        body["tools"] = json!(tools);
    }
    body
}

/// The token usage of one response body.
///
/// `input_tokens` holds the whole prompt. The cached part is
/// `prompt_tokens_details.cached_tokens`. The API bills no cache write.
#[must_use]
pub fn usage(body: &Value) -> TokenUsage {
    let count = |value: &Value| u32::try_from(value.as_u64().unwrap_or(0)).unwrap_or(u32::MAX);
    let counters = &body["usage"];
    let mut usage = TokenUsage::new(
        count(&counters["prompt_tokens"]),
        count(&counters["completion_tokens"]),
    );
    usage.cache_read_tokens = count(&counters["prompt_tokens_details"]["cached_tokens"]);
    usage
}

/// The error of an `error` object.
///
/// A number in `code` is an HTTP status, as `OpenRouter` sends it, and maps
/// through the shared table. A text code or type maps by its words.
#[must_use]
pub fn error_of(error: &Value) -> AgentError {
    let words = format!(
        "{} {}",
        error["code"].as_str().unwrap_or_default(),
        error["type"].as_str().unwrap_or_default()
    );
    let kind = error["code"]
        .as_u64()
        .and_then(|code| u16::try_from(code).ok())
        .map_or_else(|| kind_of_words(&words), error_kind);
    let message = printable(error["message"].as_str().unwrap_or("no message"));
    AgentError::new(kind, format!("the API answered an error: {message}"))
}

/// The error kind that the words of a code or a type name.
fn kind_of_words(words: &str) -> ErrorKind {
    if words.contains("rate_limit") {
        ErrorKind::RateLimited
    } else if words.contains("server_error")
        || words.contains("overloaded")
        || words.contains("timeout")
    {
        ErrorKind::Unavailable
    } else if words.contains("api_key") || words.contains("authentication") {
        ErrorKind::Authentication
    } else if words.contains("context_length") || words.contains("too_large") {
        ErrorKind::Budget
    } else {
        ErrorKind::Provider
    }
}

/// Map one response body to a [`ChatResponse`].
///
/// # Errors
///
/// A `Decode` error for a body with no choice, or a tool call whose
/// arguments are not JSON. A refusal or a content filter is a `Provider`
/// error.
pub fn parse_response(body: &Value) -> Result<ChatResponse, AgentError> {
    let choice = body["choices"]
        .as_array()
        .and_then(|choices| choices.first())
        .ok_or_else(|| AgentError::new(ErrorKind::Decode, "the response has no choice"))?;
    let message = &choice["message"];
    if let Some(refusal) = message["refusal"].as_str() {
        return Err(AgentError::new(
            ErrorKind::Provider,
            format!("the model declined the request: {}", printable(refusal)),
        ));
    }
    let stop_reason = match choice["finish_reason"].as_str() {
        Some("stop") => StopReason::EndTurn,
        Some("tool_calls" | "function_call") => StopReason::ToolUse,
        Some("length") => StopReason::MaxTokens,
        Some("content_filter") => {
            return Err(AgentError::new(
                ErrorKind::Provider,
                "a content filter stopped the answer",
            ));
        }
        _ => StopReason::Unknown,
    };
    let mut content = Vec::new();
    if let Some(text) = message["content"].as_str().filter(|text| !text.is_empty()) {
        content.push(ContentPart::Text(text.to_owned()));
    }
    for (index, call) in message["tool_calls"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        content.push(tool_call(index, call)?);
    }
    Ok(ChatResponse {
        content,
        stop_reason,
        usage: usage(body),
    })
}

/// One tool call of a response.
///
/// A missing id becomes `call_{index}`, so a result can name the call. The
/// same response gives the same id on each read.
fn tool_call(index: usize, call: &Value) -> Result<ContentPart, AgentError> {
    let function = &call["function"];
    let name = function["name"]
        .as_str()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| AgentError::new(ErrorKind::Decode, "a tool call has no name"))?;
    let arguments = match &function["arguments"] {
        Value::String(text) if text.trim().is_empty() => json!({}),
        Value::String(text) => serde_json::from_str(text).map_err(|e| {
            AgentError::new(
                ErrorKind::Decode,
                format!("the arguments of `{}` are not JSON: {e}", printable(name)),
            )
        })?,
        Value::Null => json!({}),
        other => other.clone(),
    };
    let id = call["id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .map_or_else(|| format!("call_{index}"), str::to_owned);
    Ok(ContentPart::ToolCall {
        id,
        name: name.to_owned(),
        arguments,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use autumn_harvest::llm_ledger::LlmCall;

    use super::*;
    use crate::{ChatMessage, DrawId, ToolDefinition};

    fn request() -> ChatRequest {
        ChatRequest {
            messages: vec![
                ChatMessage::text(ChatRole::System, "You are a desk agent."),
                ChatMessage::text(ChatRole::User, "Find the ticket."),
                ChatMessage {
                    role: ChatRole::Assistant,
                    content: vec![
                        ContentPart::Text("Searching.".into()),
                        ContentPart::ToolCall {
                            id: "call_1".into(),
                            name: "search".into(),
                            arguments: json!({"q": "T-2"}),
                        },
                    ],
                },
                ChatMessage {
                    role: ChatRole::Tool,
                    content: vec![ContentPart::ToolResult {
                        tool_call_id: "call_1".into(),
                        content: "T-2: printer".into(),
                    }],
                },
            ],
            tools: vec![ToolDefinition {
                name: "search".into(),
                description: "Search the tickets.".into(),
                input_schema: json!({"type": "object"}),
            }],
            max_tokens: Some(512),
            temperature: Some(0.2),
            draw: Some(DrawId::new(3)),
        }
    }

    #[test]
    fn the_endpoint_and_the_host_come_from_the_base_url() {
        assert_eq!(
            endpoint(OPENROUTER_BASE_URL).unwrap(),
            "https://openrouter.ai/api/v1/chat/completions"
        );
        assert_eq!(
            endpoint("http://localhost:11434/v1/").unwrap(),
            "http://localhost:11434/v1/chat/completions"
        );
        assert_eq!(host(OLLAMA_BASE_URL).unwrap(), "localhost:11434");
        assert_eq!(host(OPENAI_BASE_URL).unwrap(), "api.openai.com");
        for bad in ["", "localhost:11434", "ftp://x/v1", "not a url"] {
            assert_eq!(
                endpoint(bad).unwrap_err().kind(),
                ErrorKind::Config,
                "{bad}"
            );
        }
    }

    #[test]
    fn the_request_maps_roles_tools_calls_and_results() {
        let body = request_body(&request(), "gpt-5-mini", Settings::default());
        assert_eq!(body["model"], "gpt-5-mini");
        assert_eq!(body["max_tokens"], 512);
        assert!((body["temperature"].as_f64().unwrap() - 0.2).abs() < 1e-6);
        assert_eq!(body["seed"], 3);
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(
            messages[0],
            json!({"role": "system", "content": "You are a desk agent."})
        );
        assert_eq!(
            messages[1],
            json!({"role": "user", "content": "Find the ticket."})
        );
        assert_eq!(messages[2]["role"], "assistant");
        assert_eq!(messages[2]["content"], "Searching.");
        let call = &messages[2]["tool_calls"][0];
        assert_eq!(call["id"], "call_1");
        assert_eq!(call["type"], "function");
        assert_eq!(call["function"]["name"], "search");
        // The arguments travel as a JSON string.
        assert_eq!(call["function"]["arguments"], r#"{"q":"T-2"}"#);
        assert_eq!(
            messages[3],
            json!({"role": "tool", "tool_call_id": "call_1", "content": "T-2: printer"})
        );
        let tool = &body["tools"][0];
        assert_eq!(tool["type"], "function");
        assert_eq!(tool["function"]["name"], "search");
        assert_eq!(tool["function"]["parameters"]["type"], "object");
    }

    #[test]
    fn the_settings_change_the_cap_field_and_the_seed() {
        let settings = Settings {
            token_field: TokenField::MaxCompletionTokens,
            seed: false,
            stream_usage: true,
        };
        let body = request_body(&request(), "o4", settings);
        assert_eq!(body["max_completion_tokens"], 512);
        assert!(body.get("max_tokens").is_none());
        assert!(body.get("seed").is_none());
        let mut bare = request();
        bare.max_tokens = None;
        bare.temperature = None;
        bare.draw = None;
        let body = request_body(&bare, "m", Settings::default());
        for key in ["max_tokens", "temperature", "seed", "stream"] {
            assert!(body.get(key).is_none(), "{key}: {body}");
        }
    }

    #[test]
    fn an_assistant_turn_with_only_calls_sends_null_content() {
        let mut ask = request();
        ask.messages[2].content.remove(0);
        let messages = wire_messages(&ask);
        assert_eq!(messages[2]["content"], Value::Null);
        assert_eq!(messages[2]["tool_calls"][0]["id"], "call_1");
    }

    #[test]
    fn a_tool_use_response_maps_to_calls_and_usage() {
        let body = json!({
            "choices": [{"index": 0, "finish_reason": "tool_calls", "message": {
                "role": "assistant", "content": "Looking.",
                "tool_calls": [
                    {"id": "call_9", "type": "function",
                     "function": {"name": "search", "arguments": "{\"q\":\"x\"}"}},
                    {"type": "function",
                     "function": {"name": "close", "arguments": {"id": 2}}}
                ]}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 20,
                      "prompt_tokens_details": {"cached_tokens": 60}}
        });
        let response = parse_response(&body).unwrap();
        assert_eq!(response.stop_reason, StopReason::ToolUse);
        assert_eq!(response.content[0], ContentPart::Text("Looking.".into()));
        assert!(matches!(&response.content[1],
            ContentPart::ToolCall { id, arguments, .. } if id == "call_9" && arguments["q"] == "x"));
        // A missing id gets one from its place. An object argument is taken.
        assert!(matches!(&response.content[2],
            ContentPart::ToolCall { id, arguments, .. } if id == "call_1" && arguments["id"] == 2));
        assert_eq!(response.usage.input_tokens, 100);
        assert_eq!(response.usage.cache_read_tokens, 60);
        assert_eq!(response.usage.cache_write_tokens, 0);
        assert_eq!(response.usage.output_tokens, 20);
    }

    #[test]
    fn finish_reasons_map_and_a_refusal_or_a_filter_is_an_error() {
        let body = |finish: &str| {
            json!({"choices": [{"finish_reason": finish,
                "message": {"role": "assistant", "content": "x"}}]})
        };
        let stop = |finish: &str| parse_response(&body(finish)).unwrap().stop_reason;
        assert_eq!(stop("stop"), StopReason::EndTurn);
        assert_eq!(stop("length"), StopReason::MaxTokens);
        assert_eq!(stop("tool_calls"), StopReason::ToolUse);
        assert_eq!(stop("function_call"), StopReason::ToolUse);
        assert_eq!(stop("other"), StopReason::Unknown);
        let filtered = parse_response(&body("content_filter")).unwrap_err();
        assert_eq!(filtered.kind(), ErrorKind::Provider);
        let refused = json!({"choices": [{"finish_reason": "stop",
            "message": {"role": "assistant", "content": null, "refusal": "No."}}]});
        assert_eq!(
            parse_response(&refused).unwrap_err().kind(),
            ErrorKind::Provider
        );
    }

    #[test]
    fn a_body_with_no_choice_or_bad_arguments_is_a_decode_error() {
        for body in [
            json!({}),
            json!({"choices": []}),
            json!({"choices": [{"finish_reason": "tool_calls", "message": {"tool_calls": [
                {"id": "c", "function": {"name": "n", "arguments": "{\"q\": "}}]}}]}),
        ] {
            assert_eq!(
                parse_response(&body).unwrap_err().kind(),
                ErrorKind::Decode,
                "{body}"
            );
        }
    }

    #[test]
    fn an_error_object_maps_by_its_code_or_its_type() {
        let kind = |error: Value| error_of(&error).kind();
        assert_eq!(
            kind(json!({"code": 429, "message": "slow"})),
            ErrorKind::RateLimited
        );
        assert_eq!(kind(json!({"code": 502})), ErrorKind::Unavailable);
        assert_eq!(
            kind(json!({"type": "rate_limit_exceeded"})),
            ErrorKind::RateLimited
        );
        assert_eq!(
            kind(json!({"code": "rate_limit_exceeded"})),
            ErrorKind::RateLimited
        );
        assert_eq!(
            kind(json!({"type": "server_error"})),
            ErrorKind::Unavailable
        );
        assert_eq!(
            kind(json!({"code": "invalid_api_key"})),
            ErrorKind::Authentication
        );
        assert_eq!(
            kind(json!({"code": "context_length_exceeded"})),
            ErrorKind::Budget
        );
        assert_eq!(
            kind(json!({"type": "invalid_request_error"})),
            ErrorKind::Provider
        );
        let message = error_of(&json!({"message": "bad\u{1b}[2J"}))
            .message()
            .to_owned();
        assert!(!message.contains('\u{1b}'), "{message}");
    }

    #[test]
    fn usage_counts_the_whole_prompt_and_the_cached_part() {
        let body = json!({"usage": {"prompt_tokens": 10, "completion_tokens": 2}});
        assert_eq!(usage(&body), TokenUsage::new(10, 2));
        assert_eq!(usage(&json!({})), TokenUsage::new(0, 0));
        let id = "openai/gpt-5-mini@openrouter.ai";
        LlmCall::new(id, 1, 1).validate().unwrap();
    }
}
