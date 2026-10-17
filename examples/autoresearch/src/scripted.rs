//! A scripted researcher: an offline [`AgentModel`] with no network.
//!
//! It reads the transcript and the system prompt, and answers as each role
//! would: the researcher, the reviewer peer, the heartbeat watcher and the
//! report writer. It tries a fixed list of ideas, so a whole night of
//! research is deterministic and testable. It is not Claude.
//!
//! [`respond`] is a pure function of the request. That keeps the response
//! cache and `eval` honest: the same request gets the same answer.

use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest_agent::{AgentError, AgentModel, BoxFuture, ChatRequest, ChatResponse};

/// The model id that the response cache keys on.
pub const MODEL_ID: &str = "scripted-researcher-v1";

/// The marker of a crash-fix task in the user input.
pub const FIX_MODE: &str = "MODE: fix";
/// The marker of a bolder idea in the user input.
pub const EXPLORE: &str = "EXPLORE: yes";
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
    fn chat<'a>(&'a self, request: &'a ChatRequest) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = respond(request);
        Box::pin(async move { Ok(response) })
    }

    fn model_id(&self) -> Option<&str> {
        Some(MODEL_ID)
    }
}

/// The answer to `request`. See the module docs.
#[must_use]
pub fn respond(request: &ChatRequest) -> ChatResponse {
    let _ = request;
    unimplemented!("green phase")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use autumn_harvest_agent::{ChatMessage, ChatRole, ContentPart, StopReason, ToolCall};
    use serde_json::{Value, json};

    use super::*;
    use crate::agent::{
        EDIT_TRAIN, READ_FILE, READ_PAPER, REPORTER_ROLE, RESEARCHER_ROLE, REVIEWER_ROLE,
        REWIND, SHOW_DIFF, WATCHER_ROLE,
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
                ContentPart::ToolCall { id, name, arguments } => Some(ToolCall {
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
        assert!(!edit.arguments["old"].as_str().unwrap().starts_with("MATRIX_LR"));

        let noted = Chat::new(
            &format!("{RESEARCHER_ROLE}\n<memory>\n- tried raise MATRIX_LR\n</memory>"),
            "Run experiment 2.",
        );
        let mut noted = noted;
        let edit = to_edit(&mut noted, "");
        assert!(!edit.arguments["old"].as_str().unwrap().starts_with("MATRIX_LR"));
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
        chat.reply(&second, &[json!("The call was not approved."), json!({"ok": true})]);
        assert!(text(&chat.answer()).starts_with(GIVE_UP));
    }

    #[test]
    fn a_paper_hint_reads_the_paper_and_takes_its_idea() {
        let mut chat = researcher("Run experiment 1.\nHINT: paper:warmdown");
        let first = chat.answer();
        assert_eq!(names(&first), vec![READ_FILE, READ_FILE, READ_PAPER]);
        chat.reply(&first, &[file(TRAIN), file(""), json!({"content": "Try 0.7."})]);
        let second = chat.answer();
        let edit = calls(&second).into_iter().find(|c| c.name == EDIT_TRAIN).unwrap();
        assert!(edit.arguments["old"].as_str().unwrap().starts_with("WARMDOWN_RATIO"));
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
        assert!(edit.arguments["old"].as_str().unwrap().contains("WARMUP_RATIO"));
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
        assert!(calls(&answer).is_empty());
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
        let big: String = (0..30).map(|i| format!("+line {i}\n")).collect();
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

        let bad = format!("{ok}b\t0.0\t0.0\tcrash\tx\nc\t0.0\t0.0\tcrash\ty\nd\t0.0\t0.0\tcrash\tz\n");
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
        assert!(report.contains("3 experiments"));
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
