//! A scripted researcher: an offline [`AgentModel`] with no network.
//!
//! It reads the transcript and the system prompt. It answers as one of four
//! roles: the researcher, the reviewer peer, the heartbeat watcher or the
//! report writer. It tries a fixed list of ideas, so a whole night of
//! research is deterministic and testable. It is not Claude.
//!
//! [`respond`] is a pure function of the request. That keeps the response
//! cache and `eval` honest: the same request gets the same answer.

use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest_agent::memory::MEMORY_TOOL;
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::{
    AgentError, AgentModel, BoxFuture, ChatMessage, ChatRequest, ChatResponse, ChatRole,
    ContentPart, StopReason, TokenUsage,
};
use serde_json::{Value, json};

use crate::agent::{
    EDIT_TRAIN, READ_FILE, READ_PAPER, REPORTER_ROLE, REVIEWER, REVIEWER_ROLE, REWIND, SHOW_DIFF,
    WATCHER_ROLE,
};
use crate::lab::{RESULTS, TRAIN};
use crate::rules::Status;

/// The model id that the response cache keys on.
pub const MODEL_ID: &str = "scripted-researcher-v1";

/// The marker of a crash-fix task in the user input.
pub const FIX_MODE: &str = "MODE: fix";
/// The marker of a bolder idea in the user input.
pub const EXPLORE: &str = "EXPLORE: yes";
/// The marker of a hint in the user input.
pub const HINT: &str = "HINT: ";
/// The first words of an answer that proposes no change.
pub const GIVE_UP: &str = "GIVE UP";
/// A `program.md` line that puts simplifications first.
pub const PREFER_SIMPLER: &str = "Prefer simplifications.";

/// The scripted researcher. It counts its calls, so a test can prove that a
/// restart does not pay for a turn twice.
#[derive(Debug, Default)]
pub struct ScriptedResearcher {
    calls: AtomicUsize,
}

impl ScriptedResearcher {
    /// The number of `chat` calls so far.
    #[must_use]
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl AgentModel for ScriptedResearcher {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = respond(request);
        Box::pin(async move { Ok(response) })
    }

    fn model_id(&self) -> Option<&str> {
        Some(MODEL_ID)
    }
}

/// How an idea changes its setting.
#[derive(Debug, Clone, Copy)]
enum Change {
    /// Multiply the number.
    Scale(f64),
    /// Write this value.
    Set(&'static str),
    /// Delete the whole line.
    Delete,
}

/// One idea that the researcher can try.
struct Idea {
    label: &'static str,
    name: &'static str,
    change: Change,
    /// An explore turn tries these first.
    bold: bool,
    /// A program that prefers simpler code tries these first.
    simpler: bool,
    /// The paper or hint word that points at this idea.
    topic: &'static str,
}

const fn idea(
    label: &'static str,
    name: &'static str,
    change: Change,
    topic: &'static str,
) -> Idea {
    Idea {
        label,
        name,
        change,
        bold: false,
        simpler: false,
        topic,
    }
}

/// The ideas, in the order a plain night tries them.
const IDEAS: &[Idea] = &[
    idea("raise MATRIX_LR", "MATRIX_LR", Change::Scale(1.25), "muon"),
    idea(
        "longer warmdown",
        "WARMDOWN_RATIO",
        Change::Set("0.7"),
        "warmdown",
    ),
    idea(
        "halve WEIGHT_DECAY",
        "WEIGHT_DECAY",
        Change::Scale(0.5),
        "decay",
    ),
    idea("deeper model", "DEPTH", Change::Scale(1.25), "depth"),
    idea(
        "halve TOTAL_BATCH_SIZE",
        "TOTAL_BATCH_SIZE",
        Change::Set("2**18"),
        "batch",
    ),
    idea(
        "full attention",
        "WINDOW_PATTERN",
        Change::Set("\"L\""),
        "attention",
    ),
    Idea {
        simpler: true,
        ..idea(
            "drop the warmup setting",
            "WARMUP_RATIO",
            Change::Delete,
            "warmup",
        )
    },
    idea(
        "raise EMBEDDING_LR",
        "EMBEDDING_LR",
        Change::Scale(1.25),
        "embedding",
    ),
    idea("lower SCALAR_LR", "SCALAR_LR", Change::Scale(0.8), "scalar"),
    Idea {
        bold: true,
        ..idea("much deeper model", "DEPTH", Change::Set("12"), "depth")
    },
    Idea {
        bold: true,
        ..idea(
            "ten times MATRIX_LR",
            "MATRIX_LR",
            Change::Scale(10.0),
            "muon",
        )
    },
];

/// One tool call of the transcript and its result, if it has one.
struct Exchange {
    name: String,
    arguments: Value,
    result: Option<String>,
}

/// The answer to `request`. See the module docs.
#[must_use]
pub fn respond(request: &ChatRequest) -> ChatResponse {
    let system = first_text(&request.messages, ChatRole::System);
    let input = first_text(&request.messages, ChatRole::User);
    let done = exchanges(&request.messages);
    let turn = request
        .messages
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .count();
    let reply = if system.contains(REVIEWER_ROLE) {
        review(&done)
    } else if system.contains(WATCHER_ROLE) {
        watch(&done)
    } else if system.contains(REPORTER_ROLE) {
        report(&done)
    } else if input.contains(FIX_MODE) {
        fix(&input, &done)
    } else {
        research(&system, &input, &done)
    };
    let input_chars: usize = request
        .messages
        .iter()
        .flat_map(|m| &m.content)
        .map(|part| match part {
            ContentPart::Text(text) => text.len(),
            ContentPart::ToolResult { content, .. } => content.len(),
            ContentPart::ToolCall { arguments, .. } => arguments.to_string().len(),
        })
        .sum();
    let usage = TokenUsage::new(u32::try_from(input_chars / 4).unwrap_or(u32::MAX), 20);
    match reply {
        Reply::Text(text) => ChatResponse {
            content: vec![ContentPart::Text(text)],
            stop_reason: StopReason::EndTurn,
            usage,
        },
        Reply::Calls(calls) => ChatResponse {
            content: calls
                .into_iter()
                .enumerate()
                .map(|(i, (name, arguments))| ContentPart::ToolCall {
                    id: format!("call_{turn}_{i}"),
                    name: name.to_string(),
                    arguments,
                })
                .collect(),
            stop_reason: StopReason::ToolUse,
            usage,
        },
    }
}

/// What one turn does: answer, or call tools.
enum Reply {
    Text(String),
    Calls(Vec<(&'static str, Value)>),
}

fn first_text(messages: &[ChatMessage], role: ChatRole) -> String {
    messages
        .iter()
        .filter(|m| m.role == role)
        .flat_map(|m| &m.content)
        .find_map(|part| match part {
            ContentPart::Text(text) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn exchanges(messages: &[ChatMessage]) -> Vec<Exchange> {
    let parts = messages.iter().flat_map(|m| &m.content);
    let mut out: Vec<(String, Exchange)> = Vec::new();
    for part in parts {
        match part {
            ContentPart::ToolCall {
                id,
                name,
                arguments,
            } => out.push((
                id.clone(),
                Exchange {
                    name: name.clone(),
                    arguments: arguments.clone(),
                    result: None,
                },
            )),
            ContentPart::ToolResult {
                tool_call_id,
                content,
            } => {
                if let Some((_, exchange)) = out.iter_mut().find(|(id, _)| id == tool_call_id) {
                    exchange.result = Some(content.clone());
                }
            }
            ContentPart::Text(_) => {}
        }
    }
    out.into_iter().map(|(_, exchange)| exchange).collect()
}

/// The `content` field of a JSON tool result, or the raw text.
fn content_of(result: &str) -> String {
    serde_json::from_str::<Value>(result)
        .ok()
        .and_then(|v| v.get("content").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| result.to_string())
}

/// Whether a tool result says the call ran.
fn succeeded(result: Option<&str>) -> bool {
    result
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .is_some_and(|v| v.get("ok") == Some(&json!(true)))
}

fn last<'a>(done: &'a [Exchange], name: &str) -> Option<&'a Exchange> {
    done.iter().rev().find(|e| e.name == name)
}

fn hint(input: &str) -> Option<String> {
    input
        .lines()
        .find_map(|line| line.strip_prefix(HINT))
        .map(|text| text.trim().to_string())
}

fn research(system: &str, input: &str, done: &[Exchange]) -> Reply {
    let hint = hint(input);
    let rewind = hint.as_deref().and_then(|h| h.strip_prefix("rewind:"));
    let paper = hint.as_deref().and_then(|h| h.strip_prefix("paper:"));
    if let Some(commit) = rewind
        && last(done, REWIND).is_none()
    {
        let arguments = json!({"commit": commit.trim(), "reason": "the operator asked for it"});
        return Reply::Calls(vec![(REWIND, arguments)]);
    }
    let reads: Vec<&Exchange> = done.iter().filter(|e| e.name == READ_FILE).collect();
    if reads.is_empty() {
        let mut calls = vec![
            (READ_FILE, json!({"path": TRAIN})),
            (READ_FILE, json!({"path": RESULTS})),
        ];
        if let Some(name) = paper {
            calls.push((READ_PAPER, json!({"name": name.trim()})));
        }
        return Reply::Calls(calls);
    }
    let file = |path: &str| {
        reads
            .iter()
            .find(|e| e.arguments["path"] == path)
            .and_then(|e| e.result.as_deref())
            .map(content_of)
            .unwrap_or_default()
    };
    let edits: Vec<&Exchange> = done.iter().filter(|e| e.name == EDIT_TRAIN).collect();
    let Some(first_edit) = edits.first() else {
        let train = file(TRAIN);
        let topic = paper.or(hint.as_deref());
        return match choose(system, input, &train, &file(RESULTS), topic) {
            Some((description, old, new)) => Reply::Calls(vec![
                (EDIT_TRAIN, json!({"old": old, "new": new})),
                (
                    MEMORY_TOOL,
                    json!({"action": "add", "block": "memory", "text": format!("tried {description}")}),
                ),
            ]),
            None => Reply::Text(format!("{GIVE_UP}: every idea on my list is tried")),
        };
    };
    if !succeeded(first_edit.result.as_deref()) {
        let why = first_edit.result.clone().unwrap_or_default();
        return Reply::Text(format!("{GIVE_UP}: the edit did not run: {why}"));
    }
    let description = last(done, MEMORY_TOOL)
        .and_then(|e| e.arguments["text"].as_str())
        .and_then(|text| text.strip_prefix("tried "))
        .unwrap_or("an edit of train.py")
        .to_string();
    let Some(review) = last(done, DELEGATE_TOOL) else {
        let task = "Review the diff of train.py against the simplicity rule.";
        return Reply::Calls(vec![(
            DELEGATE_TOOL,
            json!({"agent": REVIEWER, "task": task}),
        )]);
    };
    let verdict = review
        .result
        .as_deref()
        .and_then(|r| serde_json::from_str::<Value>(r).ok())
        .and_then(|v| v["answer"].as_str().map(str::to_string))
        .unwrap_or_default();
    if verdict.starts_with("REJECT") {
        if edits.len() < 2 {
            let undo =
                json!({"old": first_edit.arguments["new"], "new": first_edit.arguments["old"]});
            return Reply::Calls(vec![(EDIT_TRAIN, undo)]);
        }
        return Reply::Text(format!(
            "{GIVE_UP}: the reviewer rejected the change: {verdict}"
        ));
    }
    Reply::Text(description)
}

/// Pick the first untried idea that applies to `train`. Returns the
/// description and the edit.
fn choose(
    system: &str,
    input: &str,
    train: &str,
    results: &str,
    topic: Option<&str>,
) -> Option<(String, String, String)> {
    let tried = |label: &str| {
        system.contains(&format!("tried {label}"))
            || results
                .lines()
                .filter_map(|line| line.split('\t').nth(4))
                .any(|description| description.starts_with(label))
    };
    let explore = input.contains(EXPLORE);
    let simpler = system.contains(PREFER_SIMPLER);
    let topic = topic.map(str::to_ascii_lowercase);
    let rank = |idea: &Idea| {
        let pointed = topic.as_deref().is_some_and(|t| t.contains(idea.topic));
        let preferred = (explore && idea.bold) || (simpler && idea.simpler);
        (!pointed, !preferred)
    };
    let mut ideas: Vec<&Idea> = IDEAS.iter().collect();
    ideas.sort_by_key(|idea| rank(idea));
    ideas
        .into_iter()
        .filter(|idea| !idea.bold || explore)
        .filter(|idea| !tried(idea.label))
        .find_map(|idea| apply(idea, train))
}

/// The edit that `idea` makes to `train`, if its setting is there.
fn apply(idea: &Idea, train: &str) -> Option<(String, String, String)> {
    let line = train.lines().find(|line| {
        line.strip_prefix(idea.name)
            .is_some_and(|rest| rest.trim_start().starts_with('='))
    })?;
    let code = line.split('#').next().unwrap_or(line).trim_end();
    let value = code.split_once('=')?.1.trim();
    if matches!(idea.change, Change::Delete) {
        let description = format!("{}: {} removed", idea.label, idea.name);
        return Some((description, format!("{line}\n"), String::new()));
    }
    let new_value = match idea.change {
        Change::Scale(factor) => {
            let x: f64 = value.parse().ok()?;
            short(x * factor, !value.contains(['.', 'e', 'E']))
        }
        Change::Set(text) => text.to_string(),
        Change::Delete => return None,
    };
    if new_value == value {
        return None;
    }
    let description = format!("{}: {value} -> {new_value}", idea.label);
    Some((
        description,
        code.to_string(),
        format!("{} = {new_value}", idea.name),
    ))
}

/// A short, stable form of `x`: an integer, or at most six decimals.
fn short(x: f64, integer: bool) -> String {
    if integer {
        return format!("{x:.0}");
    }
    let text = format!("{x:.6}");
    let text = text.trim_end_matches('0');
    text.strip_suffix('.').unwrap_or(text).to_string()
}

fn fix(input: &str, done: &[Exchange]) -> Reply {
    let Some(read) = last(done, READ_FILE) else {
        return Reply::Calls(vec![(READ_FILE, json!({"path": "train.py"}))]);
    };
    if let Some(edit) = last(done, EDIT_TRAIN) {
        if succeeded(edit.result.as_deref()) {
            let old = edit.arguments["old"].as_str().unwrap_or_default();
            let new = edit.arguments["new"].as_str().unwrap_or_default();
            return Reply::Text(format!("fix: {old} -> {new}"));
        }
        return Reply::Text(format!("{GIVE_UP}: the fix did not apply"));
    }
    if !input.to_ascii_lowercase().contains("out of memory") {
        let error = input
            .lines()
            .rev()
            .find(|line| line.contains("Error") || line.contains("NaN"))
            .unwrap_or("an unknown error");
        return Reply::Text(format!("{GIVE_UP}: {}", error.trim()));
    }
    let train = read.result.as_deref().map(content_of).unwrap_or_default();
    let halve = idea(
        "halve DEVICE_BATCH_SIZE",
        "DEVICE_BATCH_SIZE",
        Change::Scale(0.5),
        "",
    );
    match apply(&halve, &train) {
        Some((_, old, new)) => Reply::Calls(vec![(EDIT_TRAIN, json!({"old": old, "new": new}))]),
        None => Reply::Text(format!("{GIVE_UP}: no DEVICE_BATCH_SIZE to reduce")),
    }
}

fn review(done: &[Exchange]) -> Reply {
    let Some(diff) = last(done, SHOW_DIFF) else {
        return Reply::Calls(vec![(SHOW_DIFF, json!({}))]);
    };
    let text = diff
        .result
        .as_deref()
        .and_then(|r| serde_json::from_str::<Value>(r).ok())
        .and_then(|v| v["diff"].as_str().map(str::to_string))
        .unwrap_or_default();
    let added = text
        .lines()
        .filter(|line| line.starts_with('+') && !line.starts_with("+++"))
        .count();
    if added > 10 {
        Reply::Text(format!("REJECT: the diff adds {added} lines"))
    } else {
        Reply::Text("APPROVE: a small change".into())
    }
}

fn rows_from(done: &[Exchange]) -> Option<Vec<crate::results::ResultRow>> {
    let result = last(done, READ_FILE)?.result.as_deref()?;
    crate::results::parse(&content_of(result)).ok()
}

fn watch(done: &[Exchange]) -> Reply {
    if last(done, READ_FILE).is_none() {
        return Reply::Calls(vec![(READ_FILE, json!({"path": "results.tsv"}))]);
    }
    let rows = rows_from(done).unwrap_or_default();
    let crashed = rows.len() >= 3 && rows.iter().rev().take(3).all(|r| r.status == Status::Crash);
    if crashed {
        Reply::Text("ALERT: three runs in a row crashed. Check the GPU host.".into())
    } else {
        Reply::Text(autumn_harvest_agent::delivery::HEARTBEAT_OK.into())
    }
}

fn report(done: &[Exchange]) -> Reply {
    if last(done, READ_FILE).is_none() {
        return Reply::Calls(vec![(READ_FILE, json!({"path": "results.tsv"}))]);
    }
    let rows = rows_from(done).unwrap_or_default();
    let kept: Vec<_> = rows.iter().filter(|r| r.status == Status::Keep).collect();
    let best = kept.iter().min_by(|a, b| a.val_bpb.total_cmp(&b.val_bpb));
    let best = best.map_or_else(
        || "No run was kept.".to_string(),
        |r| {
            format!(
                "Best val_bpb {:.6} at {} ({}).",
                r.val_bpb, r.commit, r.description
            )
        },
    );
    Reply::Text(format!(
        "Morning report: {} runs, {} kept. {best}",
        rows.len(),
        kept.len()
    ))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use autumn_harvest_agent::{ChatMessage, ChatRole, ContentPart, StopReason, ToolCall};
    use serde_json::{Value, json};

    use super::*;
    use crate::agent::{
        EDIT_TRAIN, READ_FILE, READ_PAPER, REPORTER_ROLE, RESEARCHER_ROLE, REVIEWER_ROLE, REWIND,
        SHOW_DIFF, WATCHER_ROLE,
    };

    const TRAIN: &str = include_str!("../lab/train.py");

    /// A transcript builder: each `call` is answered by the next `result`.
    struct Chat {
        messages: Vec<ChatMessage>,
        next: usize,
    }

    impl Chat {
        fn new(system: &str, user: &str) -> Self {
            Self {
                messages: vec![
                    ChatMessage::text(ChatRole::System, system),
                    ChatMessage::text(ChatRole::User, user),
                ],
                next: 0,
            }
        }

        fn request(&self) -> ChatRequest {
            ChatRequest {
                messages: self.messages.clone(),
                tools: Vec::new(),
                max_tokens: None,
                temperature: None,
            }
        }

        fn answer(&self) -> ChatResponse {
            respond(&self.request())
        }

        /// Record `response` and answer each of its calls with `results`.
        fn reply(&mut self, response: &ChatResponse, results: &[Value]) {
            self.messages.push(ChatMessage {
                role: ChatRole::Assistant,
                content: response.content.clone(),
            });
            let calls = calls(response);
            assert_eq!(calls.len(), results.len(), "{calls:?}");
            let parts = calls
                .iter()
                .zip(results)
                .map(|(call, result)| {
                    self.next += 1;
                    ContentPart::ToolResult {
                        tool_call_id: call.id.clone(),
                        content: result.to_string(),
                    }
                })
                .collect();
            self.messages.push(ChatMessage {
                role: ChatRole::Tool,
                content: parts,
            });
        }
    }

    fn calls(response: &ChatResponse) -> Vec<ToolCall> {
        response
            .content
            .iter()
            .filter_map(|part| match part {
                ContentPart::ToolCall {
                    id,
                    name,
                    arguments,
                } => Some(ToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                }),
                _ => None,
            })
            .collect()
    }

    fn text(response: &ChatResponse) -> String {
        response
            .content
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    fn names(response: &ChatResponse) -> Vec<String> {
        calls(response).into_iter().map(|c| c.name).collect()
    }

    fn researcher(user: &str) -> Chat {
        Chat::new(&format!("{RESEARCHER_ROLE}\nYou are a researcher."), user)
    }

    fn file(content: &str) -> Value {
        json!({"content": content})
    }

    /// Drive the researcher to its edit. Returns the edit call.
    fn to_edit(chat: &mut Chat, results_tsv: &str) -> ToolCall {
        let first = chat.answer();
        assert_eq!(names(&first), vec![READ_FILE, READ_FILE]);
        chat.reply(&first, &[file(TRAIN), file(results_tsv)]);
        let second = chat.answer();
        assert_eq!(second.stop_reason, StopReason::ToolUse);
        let edit = calls(&second)
            .into_iter()
            .find(|c| c.name == EDIT_TRAIN)
            .unwrap();
        assert!(names(&second).contains(&"memory".to_string()));
        chat.reply(&second, &[json!({"ok": true}), json!({"ok": true})]);
        edit
    }

    #[test]
    fn the_researcher_reads_edits_asks_the_reviewer_and_answers() {
        let mut chat = researcher("Run experiment 1.");
        let edit = to_edit(&mut chat, "");
        assert_eq!(edit.arguments["old"], "MATRIX_LR = 0.04");
        assert_eq!(edit.arguments["new"], "MATRIX_LR = 0.05");

        let third = chat.answer();
        let delegate = calls(&third);
        assert_eq!(delegate[0].name, "delegate");
        assert_eq!(delegate[0].arguments["agent"], "reviewer");
        chat.reply(
            &third,
            &[json!({"agent": "reviewer", "stop": "completed", "answer": "APPROVE: small"})],
        );

        let last = chat.answer();
        assert_eq!(last.stop_reason, StopReason::EndTurn);
        assert_eq!(text(&last), "raise MATRIX_LR: 0.04 -> 0.05");
    }

    #[test]
    fn an_idea_in_the_results_or_the_memory_is_not_tried_again() {
        let tsv = "commit\tval_bpb\tmemory_gb\tstatus\tdescription\n\
                   aaaaaaa\t0.9\t44.0\tdiscard\traise MATRIX_LR: 0.04 -> 0.05\n";
        let mut chat = researcher("Run experiment 2.");
        let edit = to_edit(&mut chat, tsv);
        assert!(
            !edit.arguments["old"]
                .as_str()
                .unwrap()
                .starts_with("MATRIX_LR")
        );

        let noted = Chat::new(
            &format!("{RESEARCHER_ROLE}\n<memory>\n- tried raise MATRIX_LR\n</memory>"),
            "Run experiment 2.",
        );
        let mut noted = noted;
        let edit = to_edit(&mut noted, "");
        assert!(
            !edit.arguments["old"]
                .as_str()
                .unwrap()
                .starts_with("MATRIX_LR")
        );
    }

    #[test]
    fn a_rejected_diff_is_reverted_and_the_answer_gives_up() {
        let mut chat = researcher("Run experiment 1.");
        let edit = to_edit(&mut chat, "");
        let third = chat.answer();
        chat.reply(
            &third,
            &[json!({"agent": "reviewer", "stop": "completed", "answer": "REJECT: 30 lines"})],
        );
        let revert = chat.answer();
        let call = &calls(&revert)[0];
        assert_eq!(call.name, EDIT_TRAIN);
        assert_eq!(call.arguments["old"], edit.arguments["new"]);
        assert_eq!(call.arguments["new"], edit.arguments["old"]);
        chat.reply(&revert, &[json!({"ok": true})]);
        assert!(text(&chat.answer()).starts_with(GIVE_UP));
    }

    #[test]
    fn a_failed_edit_gives_up() {
        let mut chat = researcher("Run experiment 1.");
        let first = chat.answer();
        chat.reply(&first, &[file(TRAIN), file("")]);
        let second = chat.answer();
        chat.reply(
            &second,
            &[json!("The call was not approved."), json!({"ok": true})],
        );
        assert!(text(&chat.answer()).starts_with(GIVE_UP));
    }

    #[test]
    fn a_paper_hint_reads_the_paper_and_takes_its_idea() {
        let mut chat = researcher("Run experiment 1.\nHINT: paper:warmdown");
        let first = chat.answer();
        assert_eq!(names(&first), vec![READ_FILE, READ_FILE, READ_PAPER]);
        chat.reply(
            &first,
            &[file(TRAIN), file(""), json!({"content": "Try 0.7."})],
        );
        let second = chat.answer();
        let edit = calls(&second)
            .into_iter()
            .find(|c| c.name == EDIT_TRAIN)
            .unwrap();
        assert!(
            edit.arguments["old"]
                .as_str()
                .unwrap()
                .starts_with("WARMDOWN_RATIO")
        );
    }

    #[test]
    fn a_rewind_hint_asks_to_rewind_first() {
        let chat = researcher("Run experiment 3.\nHINT: rewind:abc1234");
        let first = chat.answer();
        let call = &calls(&first)[0];
        assert_eq!(call.name, REWIND);
        assert_eq!(call.arguments["commit"], "abc1234");
    }

    #[test]
    fn an_explore_turn_tries_a_bold_idea() {
        let mut chat = researcher(&format!("Run experiment 1.\n{EXPLORE}"));
        let edit = to_edit(&mut chat, "");
        assert_eq!(edit.arguments["new"], "DEPTH = 12");
    }

    #[test]
    fn a_program_that_prefers_simpler_code_tries_a_deletion_first() {
        let mut chat = Chat::new(
            &format!("{RESEARCHER_ROLE}\n{PREFER_SIMPLER}"),
            "Run experiment 1.",
        );
        let edit = to_edit(&mut chat, "");
        assert!(
            edit.arguments["old"]
                .as_str()
                .unwrap()
                .contains("WARMUP_RATIO")
        );
        assert_eq!(edit.arguments["new"], "");
    }

    #[test]
    fn an_out_of_memory_crash_halves_the_device_batch() {
        let mut chat = researcher(&format!(
            "{FIX_MODE}\nThe run crashed.\ntorch.OutOfMemoryError: CUDA out of memory."
        ));
        let first = chat.answer();
        assert_eq!(names(&first), vec![READ_FILE]);
        chat.reply(&first, &[file(TRAIN)]);
        let second = chat.answer();
        let edit = &calls(&second)[0];
        assert_eq!(edit.arguments["old"], "DEVICE_BATCH_SIZE = 128");
        assert_eq!(edit.arguments["new"], "DEVICE_BATCH_SIZE = 64");
        chat.reply(&second, &[json!({"ok": true})]);
        assert!(text(&chat.answer()).contains("DEVICE_BATCH_SIZE"));
    }

    #[test]
    fn any_other_crash_gives_up() {
        let mut chat = researcher(&format!("{FIX_MODE}\nThe run crashed.\nloss is NaN"));
        let first = chat.answer();
        chat.reply(&first, &[file(TRAIN)]);
        let answer = chat.answer();
        assert_eq!(calls(&answer), Vec::new());
        assert!(text(&answer).starts_with(GIVE_UP));
    }

    #[test]
    fn the_reviewer_approves_a_small_diff_and_rejects_a_large_one() {
        let mut chat = Chat::new(REVIEWER_ROLE, "Review the diff.");
        let first = chat.answer();
        assert_eq!(names(&first), vec![SHOW_DIFF]);
        chat.reply(&first, &[json!({"diff": "-a\n+b\n"})]);
        assert!(text(&chat.answer()).starts_with("APPROVE"));

        let mut chat = Chat::new(REVIEWER_ROLE, "Review the diff.");
        let first = chat.answer();
        let big: String = (0..30).fold(String::new(), |mut text, i| {
            text.push_str("+line ");
            text.push_str(&i.to_string());
            text.push('\n');
            text
        });
        chat.reply(&first, &[json!({"diff": big})]);
        assert!(text(&chat.answer()).starts_with("REJECT"));
    }

    #[test]
    fn the_watcher_is_silent_unless_three_runs_crash_in_a_row() {
        let ok = "commit\tval_bpb\tmemory_gb\tstatus\tdescription\n\
                  a\t1.0\t1.0\tkeep\tbaseline\n";
        let mut chat = Chat::new(WATCHER_ROLE, "Check the results.");
        let first = chat.answer();
        assert_eq!(names(&first), vec![READ_FILE]);
        chat.reply(&first, &[file(ok)]);
        assert_eq!(text(&chat.answer()), "HEARTBEAT_OK");

        let bad =
            format!("{ok}b\t0.0\t0.0\tcrash\tx\nc\t0.0\t0.0\tcrash\ty\nd\t0.0\t0.0\tcrash\tz\n");
        let mut chat = Chat::new(WATCHER_ROLE, "Check the results.");
        let first = chat.answer();
        chat.reply(&first, &[file(&bad)]);
        assert!(text(&chat.answer()).starts_with("ALERT"));
    }

    #[test]
    fn the_reporter_names_the_best_result() {
        let tsv = "commit\tval_bpb\tmemory_gb\tstatus\tdescription\n\
                   a1b2c3d\t0.997900\t44.0\tkeep\tbaseline\n\
                   b2c3d4e\t0.993200\t44.2\tkeep\traise MATRIX_LR: 0.04 -> 0.05\n\
                   c3d4e5f\t1.005000\t44.0\tdiscard\tdeeper\n";
        let mut chat = Chat::new(REPORTER_ROLE, "Write the morning report.");
        let first = chat.answer();
        chat.reply(&first, &[file(tsv)]);
        let report = text(&chat.answer());
        assert!(report.contains("0.993200"), "{report}");
        assert!(report.contains("b2c3d4e"));
        assert!(report.contains("3 runs"), "{report}");
    }

    #[test]
    fn the_same_request_gets_the_same_answer() {
        let chat = researcher("Run experiment 1.");
        assert_eq!(chat.answer(), chat.answer());
    }

    #[tokio::test]
    async fn the_model_counts_its_calls_and_names_itself() {
        let model = ScriptedResearcher::default();
        let chat = researcher("Run experiment 1.");
        model.chat(&chat.request()).await.unwrap();
        model.chat(&chat.request()).await.unwrap();
        assert_eq!(model.calls(), 2);
        assert_eq!(model.model_id(), Some(MODEL_ID));
    }
}
