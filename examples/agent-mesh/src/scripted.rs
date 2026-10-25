//! An offline model for the desk. It needs no key and no network.
//!
//! The answer is a function of the request alone, so a replay, a retry and
//! a restart get the same answer. The role comes from the system prompt.
//! The task comes from the last user message. Each step of a role comes
//! from the tool results after that message.
//!
//! A durable agent keeps its transcript, so each request also holds the
//! earlier ops of the agent. The model reads only the current op, so a
//! check before an answer does not change the answer.
//!
//! `chat_stream` sends the text of an answer word by word, as a provider
//! with a streaming API does.

use autumn_harvest_agent::delivery::HEARTBEAT_OK;
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::{
    AgentError, AgentModel, BoxFuture, ChatRequest, ChatResponse, ChatRole, ContentPart,
    StopReason, TokenUsage,
};
use serde_json::{Value, json};

use crate::mesh::{CRITIC_SYSTEM, RESEARCHER_SYSTEM, TRIAGE_SYSTEM};

/// The tokens that the scripted model reports for each call.
pub const TOKENS_PER_CALL: u32 = 120;

/// The offline model of the desk.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScriptedDesk {
    reckless: bool,
}

impl ScriptedDesk {
    /// A model that also tries to file a reply in a scheduled check. The
    /// harness must refuse that write, because a check is unattended. The
    /// simulator uses it to test the rule.
    #[must_use]
    pub const fn reckless() -> Self {
        Self { reckless: true }
    }

    /// The answer to `request`.
    #[must_use]
    pub fn answer(&self, request: &ChatRequest) -> ChatResponse {
        let system = text_of(request, ChatRole::System);
        let user = last_user(request);
        let results = tool_results(request);
        if system.starts_with(&TRIAGE_SYSTEM[..30]) {
            if user.starts_with("This is a scheduled check") {
                return self.check(&results);
            }
            return answer_ticket(&user, &results);
        }
        if system.starts_with(&RESEARCHER_SYSTEM[..30]) {
            return research(&user, &results);
        }
        if system.starts_with(&CRITIC_SYSTEM[..30]) {
            return text("The draft is correct and clear. No fixes.");
        }
        text("I have no role for this request.")
    }

    /// A scheduled check: read the inbox, then report.
    fn check(self, results: &[String]) -> ChatResponse {
        match results.len() {
            0 => call("c1", "check_inbox", json!({})),
            1 if self.reckless => {
                let open: Vec<Value> = serde_json::from_str(&results[0]).unwrap_or_default();
                open.first().map_or_else(
                    || text(HEARTBEAT_OK),
                    |ticket| {
                        call(
                            "c2",
                            "file_reply",
                            json!({"ticket": ticket["id"], "text": "An unattended reply."}),
                        )
                    },
                )
            }
            _ => {
                let open: Vec<Value> = serde_json::from_str(&results[0]).unwrap_or_default();
                if open.is_empty() {
                    text(HEARTBEAT_OK)
                } else {
                    let list: Vec<String> = open
                        .iter()
                        .map(|t| {
                            format!(
                                "{} ({})",
                                t["id"].as_str().unwrap_or("?"),
                                t["subject"].as_str().unwrap_or("?")
                            )
                        })
                        .collect();
                    text(&format!(
                        "{} open tickets: {}.",
                        open.len(),
                        list.join(", ")
                    ))
                }
            }
        }
    }
}

/// Answer a ticket: ask the researcher, ask the critic, file the reply.
fn answer_ticket(user: &str, results: &[String]) -> ChatResponse {
    let ticket = ticket_id(user);
    let subject = user.lines().next().unwrap_or_default();
    let draft = |facts: &str| format!("Hello,\n\n{facts}\n\nThe support desk");
    match results.len() {
        0 => call(
            "d1",
            DELEGATE_TOOL,
            json!({"agent": "researcher", "task": format!("Find what our notes say about: {subject}")}),
        ),
        1 => call(
            "d2",
            DELEGATE_TOOL,
            json!({
                "agent": "critic",
                "task": format!("Check this reply to {ticket}:\n\n{}", draft(&answer_of(&results[0])))
            }),
        ),
        2 => call(
            "f1",
            "file_reply",
            json!({"ticket": ticket, "text": draft(&answer_of(&results[0]))}),
        ),
        _ => {
            let last = &results[results.len() - 1];
            if last.contains("\"filed\"") {
                text(&format!(
                    "I filed the reply to {ticket}. The researcher found the facts, and the \
                     critic checked the draft."
                ))
            } else {
                text(&format!("The reply to {ticket} was not filed: {last}"))
            }
        }
    }
}

/// Research a question: search the notes, then answer with what they say.
fn research(user: &str, results: &[String]) -> ChatResponse {
    if results.is_empty() {
        let task = user.lines().skip(1).collect::<Vec<_>>().join(" ");
        return call("s1", "search_notes", json!({"query": task}));
    }
    let found: Vec<Value> = serde_json::from_str(&results[0]).unwrap_or_default();
    let Some(note) = found.first() else {
        return text("The notes say nothing about this.");
    };
    let fact = note["text"]
        .as_str()
        .unwrap_or_default()
        .lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    text(&format!(
        "{fact} (note: {})",
        note["note"].as_str().unwrap_or("?")
    ))
}

/// The ticket id of `Answer ticket T-1: ...`.
fn ticket_id(user: &str) -> String {
    user.split_once("ticket ")
        .map(|(_, rest)| {
            rest.split(|c: char| c == ':' || c.is_whitespace())
                .next()
                .unwrap_or_default()
                .to_owned()
        })
        .unwrap_or_default()
}

/// The answer of a peer reply, or the raw result.
fn answer_of(result: &str) -> String {
    serde_json::from_str::<Value>(result)
        .ok()
        .and_then(|value| value["answer"].as_str().map(str::to_owned))
        .unwrap_or_else(|| result.to_owned())
}

fn text_of(request: &ChatRequest, role: ChatRole) -> String {
    request
        .messages
        .iter()
        .filter(|m| m.role == role)
        .flat_map(|m| &m.content)
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The position of the last user message of `request` that holds text.
fn last_user_at(request: &ChatRequest) -> Option<usize> {
    request.messages.iter().rposition(|m| {
        m.role == ChatRole::User && matches!(m.content.first(), Some(ContentPart::Text(_)))
    })
}

/// The text of the last user message of `request`: the task of the op that
/// runs. The earlier user messages belong to earlier ops.
#[must_use]
pub fn last_user(request: &ChatRequest) -> String {
    last_user_at(request)
        .and_then(|at| match request.messages[at].content.first() {
            Some(ContentPart::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// The tool results after the last user message, in order.
fn tool_results(request: &ChatRequest) -> Vec<String> {
    let from = last_user_at(request).map_or(0, |at| at + 1);
    request.messages[from..]
        .iter()
        .flat_map(|m| &m.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect()
}

fn text(reply: &str) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(reply.to_owned())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(TOKENS_PER_CALL - 20, 20),
    }
}

fn call(id: &str, name: &str, arguments: Value) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::ToolCall {
            id: id.to_owned(),
            name: name.to_owned(),
            arguments,
        }],
        stop_reason: StopReason::ToolUse,
        usage: TokenUsage::new(TOKENS_PER_CALL - 20, 20),
    }
}

impl AgentModel for ScriptedDesk {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let response = self.answer(request);
        Box::pin(async move { Ok(response) })
    }

    fn chat_stream<'a>(
        &'a self,
        request: &'a ChatRequest,
        on_delta: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let response = self.answer(request);
        Box::pin(async move {
            for part in &response.content {
                if let ContentPart::Text(text) = part {
                    for word in text.split_inclusive(' ') {
                        on_delta(word);
                    }
                }
            }
            Ok(response)
        })
    }

    fn model_id(&self) -> Option<&str> {
        Some("scripted-desk")
    }
}

#[cfg(test)]
mod tests {
    use autumn_harvest_agent::ChatMessage;

    use super::*;

    #[test]
    fn a_ticket_id_comes_from_the_request() {
        assert_eq!(ticket_id("Answer ticket T-12: Export\n\nbody"), "T-12");
        assert_eq!(ticket_id("no id here"), "");
    }

    #[test]
    fn a_peer_answer_comes_out_of_its_envelope() {
        assert_eq!(
            answer_of(r#"{"agent": "researcher", "answer": "42"}"#),
            "42"
        );
        assert_eq!(answer_of("plain"), "plain");
    }

    fn request(messages: Vec<ChatMessage>) -> ChatRequest {
        ChatRequest {
            messages,
            tools: Vec::new(),
            max_tokens: None,
            temperature: None,
        }
    }

    fn result(id: &str, content: &str) -> ChatMessage {
        ChatMessage {
            role: ChatRole::Tool,
            content: vec![ContentPart::ToolResult {
                tool_call_id: id.to_owned(),
                content: content.to_owned(),
            }],
        }
    }

    #[test]
    fn an_answer_after_a_check_reads_only_its_own_op() {
        // The saved transcript of the triage agent holds a whole check.
        let inbox = r#"[{"id": "T-1", "subject": "Export fails"}]"#;
        let messages = vec![
            ChatMessage::text(ChatRole::System, TRIAGE_SYSTEM),
            ChatMessage::text(ChatRole::User, "This is a scheduled check."),
            result("c1", inbox),
            ChatMessage::text(ChatRole::Assistant, "1 open tickets: T-1 (Export fails)."),
            ChatMessage::text(ChatRole::User, "Answer ticket T-1: Export fails\n\nbody"),
        ];
        let response = ScriptedDesk::default().answer(&request(messages));
        assert!(
            matches!(
                &response.content[0],
                ContentPart::ToolCall { name, .. } if name == DELEGATE_TOOL
            ),
            "the answer asks the researcher first: {response:?}"
        );
    }
}
