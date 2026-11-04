//! The CI model of the recall eval: it answers only from its request.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;

use crate::consolidate::PASS_SYSTEM;
use crate::context_reset::SAVE_PROGRESS_TOOL;
use crate::error::AgentError;
use crate::message::{ChatRole, ContentPart, StopReason, TokenUsage};
use crate::model::{AgentModel, BoxFuture, ChatRequest, ChatResponse};

/// The prefix of an op that states a fact: `Remember: the <key> is <value>.`
const STATE: &str = "Remember: the ";

/// The prefix of an op that asks for a fact: `What is the <key>?`
const ASK: &str = "What is the ";

/// The prefix of the answer to a stated fact. A pass reads it in an episode.
const NOTED: &str = "Noted: the ";

/// The answer to an op that neither states nor asks.
const OK: &str = "ok";

/// A model that answers only from the text of its request.
///
/// It has no memory of its own, so a fact that is not in the request is
/// lost. That is what the eval measures. It follows the fixture format:
///
/// - `Remember: the <key> is <value>.` With the `save_progress` tool, it
///   saves the fact once, after the notes that the request shows. It then
///   answers `Noted: the <key> is <value>.`
/// - `What is the <key>?` It answers the value of the last
///   `<key> is <value>` in the request, or [`Self::UNKNOWN`].
/// - A consolidation pass. It turns each `Noted:` episode into one fact.
/// - Any other op. It answers `ok`.
#[derive(Debug, Default)]
pub struct ReaderModel {
    calls: AtomicUsize,
}

impl ReaderModel {
    /// The answer when the request holds no value for the key.
    pub const UNKNOWN: &'static str = "unknown";

    /// The model id. The model is deterministic, so a sweep may reuse its
    /// finished agents.
    pub const ID: &'static str = "recall-reader-v1";

    /// A new model with no calls.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The number of calls so far.
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl AgentModel for ReaderModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready(Ok(reply(request))))
    }

    fn model_id(&self) -> Option<&str> {
        Some(Self::ID)
    }
}

/// The reply to `request`.
fn reply(request: &ChatRequest) -> ChatResponse {
    if is_pass(request) {
        return text(&pass_facts(request));
    }
    let said = last_user_text(request).unwrap_or_default();
    if let Some((key, value)) = statement(&said) {
        let fact = format!("the {key} is {value}");
        let notes = last_notes(request);
        // The notes hold the fact when the last value of the key is this
        // value. An older equal value does not count, so a value that comes
        // back is saved again.
        let saved = notes
            .as_deref()
            .and_then(|notes| last_value(notes, &format!("{key} is ")))
            .is_some_and(|last| last == value);
        if offers(request, SAVE_PROGRESS_TOOL) && !saved {
            let notes = notes.map_or_else(|| fact.clone(), |notes| format!("{notes}\n{fact}"));
            return save(&notes, request.messages.len());
        }
        return text(&format!("{NOTED}{key} is {value}."));
    }
    if let Some(key) = question(&said) {
        let found = last_value(&request_text(request), &format!("{key} is "));
        return text(found.as_deref().unwrap_or(ReaderModel::UNKNOWN));
    }
    text(OK)
}

/// The request is a consolidation pass (issue #2454).
fn is_pass(request: &ChatRequest) -> bool {
    request.messages.first().is_some_and(|message| {
        message.role == ChatRole::System
            && message
                .content
                .iter()
                .any(|part| matches!(part, ContentPart::Text(text) if text == PASS_SYSTEM))
    })
}

/// The facts of a pass: one per `Noted:` episode, with that episode as its
/// source.
fn pass_facts(request: &ChatRequest) -> String {
    let said = last_user_text(request).unwrap_or_default();
    let mut facts = Vec::new();
    for chunk in said.split("<episode id=\"").skip(1) {
        let Some((id, rest)) = chunk.split_once('"') else {
            continue;
        };
        let Some((_, body)) = rest.split_once('>') else {
            continue;
        };
        let body = body.split("</episode>").next().unwrap_or_default();
        let Some((key, value)) = body
            .strip_prefix(NOTED)
            .and_then(|fact| fact.strip_suffix('.'))
            .and_then(|fact| fact.split_once(" is "))
        else {
            continue;
        };
        facts.push(json!({
            "subject": key,
            "relation": "is",
            "object": value,
            "episodes": [id],
        }));
    }
    json!({ "facts": facts }).to_string()
}

/// The key and the value of a stated fact.
fn statement(said: &str) -> Option<(&str, &str)> {
    said.strip_prefix(STATE)?
        .strip_suffix('.')?
        .split_once(" is ")
}

/// The key of a question.
fn question(said: &str) -> Option<&str> {
    said.strip_prefix(ASK)?.strip_suffix('?')
}

/// The text of the last user message.
fn last_user_text(request: &ChatRequest) -> Option<String> {
    request
        .messages
        .iter()
        .rev()
        .find(|message| message.role == ChatRole::User)
        .map(|message| {
            message
                .content
                .iter()
                .filter_map(|part| match part {
                    ContentPart::Text(text) => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
}

/// The notes of the last `save_progress` call in the request.
fn last_notes(request: &ChatRequest) -> Option<String> {
    request
        .messages
        .iter()
        .rev()
        .flat_map(|message| message.content.iter().rev())
        .find_map(|part| match part {
            ContentPart::ToolCall {
                name, arguments, ..
            } if name == SAVE_PROGRESS_TOOL => arguments["notes"].as_str().map(str::to_owned),
            _ => None,
        })
}

/// The request offers the tool `name`.
fn offers(request: &ChatRequest, name: &str) -> bool {
    request.tools.iter().any(|tool| tool.name == name)
}

/// Every text that the request shows the model, in order: message text,
/// the notes of each `save_progress` call, and each tool result.
fn request_text(request: &ChatRequest) -> String {
    let mut out = String::new();
    for part in request.messages.iter().flat_map(|m| m.content.iter()) {
        match part {
            ContentPart::Text(text) => out.push_str(text),
            ContentPart::ToolCall { arguments, .. } => match arguments["notes"].as_str() {
                Some(notes) => out.push_str(notes),
                None => out.push_str(&arguments.to_string()),
            },
            ContentPart::ToolResult { content, .. } => out.push_str(content),
        }
        out.push('\n');
    }
    out
}

/// The value after the last `needle` in `text` that starts a word. A value
/// is a run of letters, digits, `-` and `_`.
fn last_value(text: &str, needle: &str) -> Option<String> {
    let mut end = text.len();
    while let Some(at) = text[..end].rfind(needle) {
        let starts_word = !text[..at]
            .chars()
            .next_back()
            .is_some_and(char::is_alphanumeric);
        let value: String = text[at + needle.len()..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
            .collect();
        if starts_word && !value.is_empty() {
            return Some(value);
        }
        end = at;
    }
    None
}

/// A final text reply.
fn text(body: &str) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(body.to_owned())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(1, 1),
    }
}

/// A reply that calls `save_progress` with `notes`. The call id is the
/// message count, so it is the same on each run of the same request.
fn save(notes: &str, at: usize) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::ToolCall {
            id: format!("save-{at}"),
            name: SAVE_PROGRESS_TOOL.to_owned(),
            arguments: json!({ "notes": notes }),
        }],
        stop_reason: StopReason::ToolUse,
        usage: TokenUsage::new(1, 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{ChatMessage, ToolDefinition};

    fn request(messages: Vec<ChatMessage>, tools: Vec<ToolDefinition>) -> ChatRequest {
        ChatRequest {
            messages,
            tools,
            max_tokens: None,
            temperature: None,
            draw: None,
        }
    }

    fn user(text: &str) -> ChatMessage {
        ChatMessage::text(ChatRole::User, text)
    }

    fn reply_text(response: &ChatResponse) -> String {
        match response.content.as_slice() {
            [ContentPart::Text(text)] => text.clone(),
            other => panic!("expected one text, got {other:?}"),
        }
    }

    #[test]
    fn a_question_with_no_fact_in_the_request_is_unknown() {
        let asked = request(vec![user("What is the vault code?")], Vec::new());
        assert_eq!(reply_text(&reply(&asked)), ReaderModel::UNKNOWN);
    }

    #[test]
    fn a_question_reads_the_last_value_in_the_request() {
        let asked = request(
            vec![
                ChatMessage::text(ChatRole::Assistant, "Noted: the code is 1."),
                ChatMessage::text(ChatRole::Assistant, "Noted: the code is 2."),
                user("What is the code?"),
            ],
            Vec::new(),
        );
        assert_eq!(reply_text(&reply(&asked)), "2");
    }

    #[test]
    fn a_key_matches_only_at_the_start_of_a_word() {
        assert_eq!(last_value("the pincode is 1", "code is "), None);
        assert_eq!(
            last_value("the code is 2. pincode is 1", "code is "),
            Some("2".into())
        );
    }

    #[test]
    fn a_statement_with_no_notes_tool_is_noted() {
        let said = request(vec![user("Remember: the code is 7.")], Vec::new());
        assert_eq!(reply_text(&reply(&said)), "Noted: the code is 7.");
    }

    #[test]
    fn a_statement_with_the_notes_tool_saves_once_after_the_old_notes() {
        let tool = ToolDefinition {
            name: SAVE_PROGRESS_TOOL.to_owned(),
            description: String::new(),
            input_schema: json!({}),
        };
        let old = ChatMessage {
            role: ChatRole::Assistant,
            content: vec![ContentPart::ToolCall {
                id: "s".into(),
                name: SAVE_PROGRESS_TOOL.into(),
                arguments: json!({"notes": "the day is monday"}),
            }],
        };
        let said = request(
            vec![old.clone(), user("Remember: the code is 7.")],
            vec![tool.clone()],
        );
        match reply(&said).content.as_slice() {
            [ContentPart::ToolCall { arguments, .. }] => {
                assert_eq!(arguments["notes"], "the day is monday\nthe code is 7");
            }
            other => panic!("expected a save call, got {other:?}"),
        }
        let saved = ChatMessage {
            role: ChatRole::Assistant,
            content: vec![ContentPart::ToolCall {
                id: "t".into(),
                name: SAVE_PROGRESS_TOOL.into(),
                arguments: json!({"notes": "the day is monday\nthe code is 7"}),
            }],
        };
        let after = request(
            vec![old, user("Remember: the code is 7."), saved],
            vec![tool],
        );
        assert_eq!(reply_text(&reply(&after)), "Noted: the code is 7.");
    }

    #[test]
    fn a_value_that_comes_back_is_saved_again() {
        let tool = ToolDefinition {
            name: SAVE_PROGRESS_TOOL.to_owned(),
            description: String::new(),
            input_schema: json!({}),
        };
        let old = ChatMessage {
            role: ChatRole::Assistant,
            content: vec![ContentPart::ToolCall {
                id: "s".into(),
                name: SAVE_PROGRESS_TOOL.into(),
                arguments: json!({"notes": "the code is 1\nthe code is 2"}),
            }],
        };
        let said = request(vec![old, user("Remember: the code is 1.")], vec![tool]);
        match reply(&said).content.as_slice() {
            [ContentPart::ToolCall { arguments, .. }] => {
                let notes = arguments["notes"].as_str().expect("notes");
                assert_eq!(last_value(notes, "code is "), Some("1".into()));
            }
            other => panic!("expected a save call, got {other:?}"),
        }
    }

    #[test]
    fn a_pass_turns_each_noted_episode_into_a_fact() {
        let pass = request(
            vec![
                ChatMessage::text(ChatRole::System, PASS_SYSTEM),
                user(
                    "Episodes, oldest first:\n\
                     <episode id=\"e1\" stop=\"completed\" at=\"1\">Noted: the code is 7.</episode>\n\
                     <episode id=\"e2\" stop=\"completed\" at=\"2\">ok</episode>\n",
                ),
            ],
            Vec::new(),
        );
        let facts: serde_json::Value =
            serde_json::from_str(&reply_text(&reply(&pass))).expect("JSON");
        assert_eq!(
            facts,
            json!({"facts": [
                {"subject": "code", "relation": "is", "object": "7", "episodes": ["e1"]}
            ]})
        );
    }

    #[test]
    fn any_other_op_is_answered_ok() {
        let other = request(vec![user("Name a river.")], Vec::new());
        assert_eq!(reply_text(&reply(&other)), OK);
    }
}
