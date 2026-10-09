//! The conversation: messages, tool calls, token counts and ids.
//!
//! These types are recorded in workflow history, so their serde shape is a
//! contract. Replay reads them back. Add a field only with `#[serde(default)]`.

use serde::{Deserialize, Serialize};

/// Identifies one agent run. The loop uses the execution id, which no other
/// run shares.
///
/// Tools receive it with the step and the call id. Together they are an
/// idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(String);

impl RunId {
    /// Wrap an id.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Identifies one conversation that spans runs. The app picks the key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(String);

impl SessionId {
    /// Wrap an id.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Who sent a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatRole {
    /// The instructions for the model.
    System,
    /// The person, or the harness.
    User,
    /// The model.
    Assistant,
    /// Tool results.
    Tool,
}

/// One part of a [`ChatMessage`].
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentPart {
    /// Plain text.
    Text(String),
    /// The model asks for a tool call.
    ToolCall {
        /// The provider id of the call. The result echoes it.
        id: String,
        /// The tool name.
        name: String,
        /// The decoded JSON arguments.
        arguments: serde_json::Value,
    },
    /// The result of one tool call.
    ToolResult {
        /// The `id` of the call that this answers.
        tool_call_id: String,
        /// The result text.
        content: String,
    },
}

/// One message: a role and its parts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Who sent it.
    pub role: ChatRole,
    /// The parts.
    pub content: Vec<ContentPart>,
}

impl ChatMessage {
    /// A message with one text part.
    #[must_use]
    pub fn text(role: ChatRole, text: impl Into<String>) -> Self {
        Self {
            role,
            content: vec![ContentPart::Text(text.into())],
        }
    }
}

/// One tool call the model asked for.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    /// The provider id of the call.
    pub id: String,
    /// The tool name.
    pub name: String,
    /// The decoded JSON arguments.
    pub arguments: serde_json::Value,
}

/// A tool as the model sees it.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    /// The tool name.
    pub name: String,
    /// What the tool does.
    pub description: String,
    /// The JSON Schema of the input object.
    pub input_schema: serde_json::Value,
}

/// Why the model stopped one turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    /// The model finished its turn.
    EndTurn,
    /// The model asked for tool calls.
    ToolUse,
    /// The turn hit the output cap.
    MaxTokens,
    /// The provider gave a reason the model does not know.
    Unknown,
}

/// Provider-reported token counts.
///
/// `input_tokens` counts the whole prompt. The two cache counts are parts of
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    /// Prompt tokens, cached or not.
    pub input_tokens: u32,
    /// Output tokens.
    pub output_tokens: u32,
    /// Prompt tokens read from the provider cache.
    #[serde(default)]
    pub cache_read_tokens: u32,
    /// Prompt tokens written to the provider cache.
    #[serde(default)]
    pub cache_write_tokens: u32,
}

impl TokenUsage {
    /// Usage with uncached input and output counts.
    #[must_use]
    pub const fn new(input_tokens: u32, output_tokens: u32) -> Self {
        Self {
            input_tokens,
            output_tokens,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        }
    }

    /// Add two usages without overflow.
    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_add(other.input_tokens),
            output_tokens: self.output_tokens.saturating_add(other.output_tokens),
            cache_read_tokens: self
                .cache_read_tokens
                .saturating_add(other.cache_read_tokens),
            cache_write_tokens: self
                .cache_write_tokens
                .saturating_add(other.cache_write_tokens),
        }
    }

    /// Input plus output tokens.
    #[must_use]
    pub const fn total(self) -> u32 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_history_shape_is_stable() {
        let message = ChatMessage {
            role: ChatRole::Assistant,
            content: vec![
                ContentPart::Text("hi".into()),
                ContentPart::ToolCall {
                    id: "c".into(),
                    name: "t".into(),
                    arguments: json!({}),
                },
            ],
        };
        assert_eq!(
            json!(message),
            json!({
                "role": "assistant",
                "content": [
                    {"text": "hi"},
                    {"tool_call": {"id": "c", "name": "t", "arguments": {}}}
                ]
            })
        );
        assert_eq!(json!(RunId::new("r")), json!("r"));
        assert_eq!(json!(StopReason::ToolUse), json!("tool_use"));
    }

    #[test]
    fn usage_adds_without_overflow() {
        let big = TokenUsage::new(u32::MAX, 1);
        let sum = big.saturating_add(TokenUsage::new(5, 2));
        assert_eq!(sum.input_tokens, u32::MAX);
        assert_eq!(sum.output_tokens, 3);
        assert_eq!(TokenUsage::new(2, 3).total(), 5);
    }

    #[test]
    fn a_usage_without_cache_counts_decodes() {
        let usage: TokenUsage =
            serde_json::from_value(json!({"input_tokens": 1, "output_tokens": 2})).unwrap();
        assert_eq!(usage, TokenUsage::new(1, 2));
    }
}
