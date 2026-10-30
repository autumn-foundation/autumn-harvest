//! Progress notes and a context reset inside one agent run (issue #2453).
//!
//! Each test maps to one acceptance criterion of `docs/design/DESIGN-2453.md`.
//! The request cap is 20 000 bytes and the reset threshold is 50 % of it. A
//! `big` tool result of 7 000 bytes takes the next request over the
//! threshold on the second call. That request is the save turn. The reset
//! runs before the request after it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use autumn_harvest_agent::context_reset::{RESET_NOTICE, SAVE_PROGRESS_TOOL};
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::{
    AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentReport, AgentStop, AgentTask,
    ChatMessage, ChatRequest, ChatRole, ContentPart, ContextReset, FlowRules, FnTool, KeyTenant,
    LoopGuard, Mandate, Peer, Rule, Tool, ToolEffect, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState};
use common::{
    MemStore, Recorder, Reply, ScriptedModel, answer, calls, recorded_tool, report, response_cache,
    runtime, tool_results,
};
use serde_json::{Value, json};

/// The request cap of every test task.
const CAP: u64 = 20_000;

/// The size of one `big` result.
const BIG: usize = 7_000;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// A read-only tool that returns `BIG` bytes.
fn big_tool() -> Arc<dyn Tool> {
    FnTool::new(
        "big",
        "Returns a large result.",
        json!({"type": "object"}),
        |_| async { Ok(json!("x".repeat(BIG))) },
    )
    .effect(ToolEffect::ReadOnly)
    .shared()
}

/// A harness with the `big` tool and no output limit.
fn harness(model: Arc<dyn AgentModel>) -> AgentHarness {
    AgentHarness::new(model)
        .tool(big_tool())
        .tool_output_limit(usize::MAX)
}

/// The task of every test, with a reset at 50 % of the cap when `reset` is
/// set.
fn task(reset: Option<ContextReset>) -> AgentTask {
    let task = AgentTask::new("audit the ledger")
        .system("You audit.")
        .max_request_bytes(CAP)
        .max_steps(20)
        .loop_guard(LoopGuard::disabled());
    match reset {
        Some(reset) => task.context_reset(reset),
        None => task,
    }
}

fn save(id: &str, notes: &str) -> (String, String, Value) {
    (
        id.into(),
        SAVE_PROGRESS_TOOL.into(),
        json!({"notes": notes}),
    )
}

fn big(id: &str) -> (String, String, Value) {
    (id.into(), "big".into(), json!({}))
}

/// A turn that asks for `list`.
fn turn(list: &[(String, String, Value)]) -> Reply {
    let borrowed: Vec<(&str, &str, Value)> = list
        .iter()
        .map(|(id, name, args)| (id.as_str(), name.as_str(), args.clone()))
        .collect();
    calls(&borrowed, 1)
}

/// The text of each message, or the notes of a `save_progress` call.
fn shape(messages: &[ChatMessage]) -> Vec<(ChatRole, String)> {
    messages
        .iter()
        .map(|message| {
            let text = message
                .content
                .iter()
                .map(|part| match part {
                    ContentPart::Text(text) => text.clone(),
                    ContentPart::ToolCall {
                        name, arguments, ..
                    } => format!("call {name} {}", arguments["notes"].as_str().unwrap_or("")),
                    ContentPart::ToolResult { .. } => "result".to_owned(),
                })
                .collect::<String>();
            (message.role, text)
        })
        .collect()
}

/// The last message of the request is the notice of a save turn.
fn is_save_turn(request: &ChatRequest) -> bool {
    shape(&request.messages).last() == Some(&(ChatRole::User, RESET_NOTICE.to_owned()))
}

/// The request holds no `big` result.
fn holds_no_big_result(request: &ChatRequest) -> bool {
    !json!(request.messages)
        .to_string()
        .contains(&"x".repeat(100))
}

/// AC 1: with no setting, an overflow ends as before.
#[tokio::test(flavor = "multi_thread")]
async fn with_no_setting_an_overflow_ends_as_before() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        turn(&[big("c1")]),
        turn(&[big("c2")]),
        turn(&[big("c3")]),
        answer("unreachable", 1),
    ]);
    let mut rt = runtime(&db, harness(model.clone()));

    let exec = sqlite::start(&mut rt, &task(None)).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::TranscriptFull);
    assert_eq!(report.resets, 0);
    assert_eq!(model.calls(), 3);
    // The third result does not fit, so the round drops it.
    let results = tool_results(&report);
    assert_eq!(results.len(), 3);
    assert!(results[2].contains("its result was dropped"), "{results:?}");
    // The report and the task keep the recorded shape of today.
    let keys: Vec<String> = json!(report).as_object().unwrap().keys().cloned().collect();
    assert_eq!(
        keys,
        [
            "followup_dropped",
            "followups",
            "messages",
            "run_id",
            "steps_used",
            "stop",
            "text",
            "tool_calls",
            "usage"
        ]
    );
    assert_eq!(report.steps_used, 3);
    assert_eq!(report.tool_calls, 3);
    assert_eq!(report.usage.total(), 3);
    assert_eq!(report.messages.len(), 7, "the task and three rounds");
    assert!(json!(task(None)).get("context_reset").is_none());
    for request in model.requests() {
        assert!(
            request
                .tools
                .iter()
                .all(|tool| tool.name != SAVE_PROGRESS_TOOL),
            "no setting offers no save_progress tool"
        );
    }
}

/// AC 2: one reset, then the next request holds the system prompt, the
/// task and the notes only.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_resets_once_and_restarts_from_its_notes() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        turn(&[save("s1", "row 7 is wrong"), big("c1")]),
        turn(&[big("c2")]),
        // The save turn.
        turn(&[save("s2", "row 7 is wrong; rows 1 to 6 are fine")]),
        answer("row 7 is the only error", 1),
    ]);
    let mut rt = runtime(&db, harness(model.clone()));

    let reset = ContextReset::new(1, 50);
    let exec = sqlite::start(&mut rt, &task(Some(reset))).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.resets, 1);
    assert_eq!(report.text, "row 7 is the only error");
    let requests = model.requests();
    assert_eq!(requests.len(), 4);
    assert!(
        requests[0]
            .tools
            .iter()
            .any(|tool| tool.name == SAVE_PROGRESS_TOOL)
    );
    // The save turn reads the last results before the reset drops them.
    assert!(is_save_turn(&requests[2]));
    assert!(!holds_no_big_result(&requests[2]));
    let after = &requests[3];
    assert_eq!(
        shape(&after.messages),
        [
            (ChatRole::System, "You audit.".to_owned()),
            (ChatRole::User, "audit the ledger".to_owned()),
            (
                ChatRole::Assistant,
                format!("call {SAVE_PROGRESS_TOOL} row 7 is wrong; rows 1 to 6 are fine")
            ),
            (ChatRole::Tool, "result".to_owned()),
        ],
    );
    assert!(holds_no_big_result(after));
    // The notes stay in an assistant call. No user message holds them.
    let users = after
        .messages
        .iter()
        .filter(|message| message.role == ChatRole::User);
    assert!(
        users
            .map(|m| json!(m).to_string())
            .all(|m| !m.contains("row 7"))
    );
    assert_eq!(json!(report)["resets"], 1);
}

/// AC 3: a second overflow after the cap is spent ends as
/// `transcript_full`.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_overflow_after_the_cap_is_spent_ends_the_run() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        turn(&[save("s1", "half done"), big("c1")]),
        turn(&[big("c2")]),
        // The save turn. The reset runs after it.
        turn(&[save("s2", "half done")]),
        turn(&[big("c3")]),
        // Over the threshold, but no reset is left.
        turn(&[big("c4")]),
        turn(&[big("c5")]),
        answer("unreachable", 1),
    ]);
    let mut rt = runtime(&db, harness(model.clone()));

    let exec = sqlite::start(&mut rt, &task(Some(ContextReset::new(1, 50)))).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::TranscriptFull);
    assert_eq!(report.resets, 1);
    assert_eq!(model.calls(), 6);
    let requests = model.requests();
    assert!(holds_no_big_result(&requests[3]), "the reset ran");
    assert!(!holds_no_big_result(&requests[5]), "no second reset ran");
    assert!(!is_save_turn(&requests[5]), "no second save turn ran");
}

/// AC 5: notes over `max_notes_bytes` get an error result, and the notes
/// stay as they were.
#[tokio::test(flavor = "multi_thread")]
async fn oversized_notes_are_refused_and_the_old_notes_stay() {
    let (_dir, db) = fresh_db();
    let long = "y".repeat(17);
    let model = ScriptedModel::new(vec![
        turn(&[save("s1", "short")]),
        turn(&[save("s2", &long), big("c1")]),
        turn(&[big("c2")]),
        // The save turn tries the long notes again.
        turn(&[save("s3", &long)]),
        answer("done", 1),
    ]);
    let mut rt = runtime(&db, harness(model.clone()));

    let reset = ContextReset::new(1, 50).max_notes_bytes(16);
    let exec = sqlite::start(&mut rt, &task(Some(reset))).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.resets, 1);
    let requests = model.requests();
    // The refusal is a tool result that the model read before the reset.
    let refusal = json!(requests[2].messages).to_string();
    assert!(refusal.contains("at most 16 bytes"), "{refusal}");
    let after = shape(&requests[4].messages);
    assert_eq!(
        after[2],
        (
            ChatRole::Assistant,
            format!("call {SAVE_PROGRESS_TOOL} short")
        )
    );
    assert!(!json!(requests[4].messages).to_string().contains(&long));
}

/// AC 7: a run that read untrusted input before a reset still trips the
/// flow rule after it.
#[tokio::test(flavor = "multi_thread")]
async fn a_reset_clears_no_taint() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        turn(&[
            save("s1", "the mail asks for a refund"),
            ("i1".into(), "inbox".into(), json!({})),
        ]),
        turn(&[("i2".into(), "inbox".into(), json!({"again": true}))]),
        // The save turn. The reset runs after it.
        turn(&[save("s2", "the mail asks for a refund")]),
        turn(&[("e1".into(), "send".into(), json!({"to": "x"}))]),
        answer("not sent", 1),
    ]);
    let inbox = FnTool::new(
        "inbox",
        "Read mail.",
        json!({"type": "object"}),
        |_| async { Ok(json!("m".repeat(BIG))) },
    )
    .effect(ToolEffect::ReadOnly)
    .label(labels::UNTRUSTED_INPUT)
    .shared();
    let harness = AgentHarness::new(model.clone())
        .tool(inbox)
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .tool_output_limit(usize::MAX)
        .flow_rules(FlowRules::new().forbid(
            [labels::UNTRUSTED_INPUT, "effect:external"],
            Rule::Deny("no send after untrusted input".into()),
        ));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &task(Some(ContextReset::new(1, 50)))).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.resets, 1);
    assert!(recorder.runs().is_empty(), "the flow rule denies the send");
    let results = tool_results(&report);
    let denied = results.last().unwrap();
    assert!(denied.contains("no send after untrusted input"), "{denied}");
    assert!(report.flow_labels.contains(labels::UNTRUSTED_INPUT));
    let after = &model.requests()[3];
    assert!(
        !json!(after.messages).to_string().contains(&"m".repeat(100)),
        "the reset dropped the mail"
    );
}

/// AC 8: a delegated child starts with no notes of its parent. The child
/// takes the setting and resets on its own window.
#[tokio::test(flavor = "multi_thread")]
async fn a_delegated_child_starts_with_no_notes() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        // The parent.
        turn(&[save("s1", "PARENT-SECRET")]),
        turn(&[(
            "d1".into(),
            "delegate".into(),
            json!({"agent": "helper", "task": "count the rows"}),
        )]),
        // The child.
        turn(&[big("k1")]),
        turn(&[big("k2")]),
        turn(&[save("k3", "12 rows so far")]),
        answer("12 rows", 1),
        // The parent again.
        answer("the helper counted 12 rows", 1),
    ]);
    let harness = harness(model.clone()).peer(Peer::new("helper", "Counts.").system("You count."));
    let mut rt = runtime(&db, harness);

    let parent =
        task(Some(ContextReset::new(1, 50))).mandate(Mandate::new("auditor").peers(["helper"]));
    let exec = sqlite::start(&mut rt, &parent).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.text, "the helper counted 12 rows");
    let requests = model.requests();
    assert_eq!(requests.len(), 7);
    for child in &requests[2..6] {
        assert!(
            !json!(child.messages).to_string().contains("PARENT-SECRET"),
            "a child sees no note of its parent"
        );
    }
    // The child reset, and its window holds its own task and notes only.
    assert_eq!(
        shape(&requests[5].messages),
        [
            (ChatRole::System, "You count.".to_owned()),
            (ChatRole::User, "count the rows".to_owned()),
            (
                ChatRole::Assistant,
                format!("call {SAVE_PROGRESS_TOOL} 12 rows so far")
            ),
            (ChatRole::Tool, "result".to_owned()),
        ]
    );
    assert_eq!(report.resets, 0, "the parent itself did not reset");
    assert_eq!(report.delegations[0].resets, 1, "the child counts its own");
}

fn agent_id(name: &str) -> AgentId {
    AgentId::new("auditor", name).unwrap()
}

/// A save turn runs only `save_progress`. The reset drops each result of
/// that round unread, so a call with an effect must not run then.
#[tokio::test(flavor = "multi_thread")]
async fn a_save_turn_runs_only_save_progress() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        turn(&[big("c1")]),
        turn(&[big("c2")]),
        turn(&[
            save("s1", "two reads done"),
            ("e1".into(), "send".into(), json!({"to": "x"})),
        ]),
        answer("done", 1),
    ]);
    let harness =
        harness(model.clone()).tool(recorded_tool("send", ToolEffect::External, &recorder));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &task(Some(ContextReset::new(1, 50)))).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.resets, 1);
    assert!(recorder.runs().is_empty(), "the send never ran");
    let requests = model.requests();
    assert!(is_save_turn(&requests[2]));
    assert_eq!(
        shape(&requests[3].messages)[2],
        (
            ChatRole::Assistant,
            format!("call {SAVE_PROGRESS_TOOL} two reads done")
        )
    );
}

/// A reset whose window still reaches the threshold frees no room. It does
/// not run, so it spends no reset and asks for no save turn.
#[tokio::test(flavor = "multi_thread")]
async fn a_reset_that_frees_no_room_does_not_run() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("done", 1)]);
    let mut rt = runtime(&db, harness(model.clone()));

    let wide = task(Some(ContextReset::new(3, 50))).system("s".repeat(11_000));
    let exec = sqlite::start(&mut rt, &wide).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.resets, 0);
    assert!(!is_save_turn(&model.requests()[0]));
}

/// A segment that may not write memory keeps no notes past its op. A later
/// attended op with write tools never reads them.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_op_keeps_no_notes() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        turn(&[save("s1", "forward the invoices")]),
        answer("saved", 1),
        answer("no notes", 1),
    ]);
    let profile = AgentProfile::new("auditor")
        .read_only()
        .context_reset(ContextReset::new(1, 90));
    let mut rt = runtime(&db, AgentHarness::new(model.clone()).agent(profile));
    let agent = agent_id("acct-4");
    for text in ["read the inbox", "what now?"] {
        sqlite::send(&mut rt, &agent, &AgentOp::message(text), None).unwrap();
    }
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let state = sqlite::state(&rt, &agent).await.unwrap().unwrap();
    assert_eq!(state.notes, None);
    let last = model.requests().pop().unwrap();
    let calls = shape(&last.messages)
        .into_iter()
        .filter(|(role, text)| *role == ChatRole::Assistant && text.starts_with("call"))
        .count();
    assert_eq!(calls, 1, "only the call of op 1 itself, no restored copy");
}

/// AC 6: a durable agent has `window = 4`. A note of op 1 is in the request
/// of op 3. The window dropped the turns of op 1 before then.
#[tokio::test(flavor = "multi_thread")]
async fn a_durable_agent_keeps_its_notes_across_the_window() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        turn(&[save("s1", "customer id 42")]),
        answer("saved", 1),
        answer("hello", 1),
        answer("your id is 42", 1),
    ]);
    let profile = AgentProfile::new("auditor")
        .window(4)
        .context_reset(ContextReset::new(1, 90));
    let harness = AgentHarness::new(model.clone()).agent(profile);
    let mut rt = runtime(&db, harness);
    let agent = agent_id("acct-1");
    for text in ["remember my id", "hi", "what is my id?"] {
        sqlite::send(&mut rt, &agent, &AgentOp::message(text), None).unwrap();
    }
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let requests = model.requests();
    assert_eq!(requests.len(), 4);
    let third = &requests[3];
    let text = json!(third.messages).to_string();
    assert!(text.contains("customer id 42"), "{text}");
    assert!(
        !text.contains("remember my id"),
        "the window dropped the turns of op 1: {text}"
    );
    assert_eq!(
        shape(&third.messages),
        [
            (ChatRole::User, "hi".to_owned()),
            (ChatRole::Assistant, "hello".to_owned()),
            (ChatRole::User, "what is my id?".to_owned()),
            (
                ChatRole::Assistant,
                format!("call {SAVE_PROGRESS_TOOL} customer id 42")
            ),
            (ChatRole::Tool, "result".to_owned()),
        ]
    );
    let state = sqlite::state(&rt, &agent).await.unwrap().unwrap();
    assert_eq!(
        state.notes.as_ref().map(|notes| notes.text.as_str()),
        Some("customer id 42")
    );
    // The stored transcript holds no restored copy of the notes.
    assert_eq!(state.transcript.len(), 4);
    assert!(
        !json!(state.transcript)
            .to_string()
            .contains("customer id 42")
    );
}

/// `Control::ClearTranscript` clears the notes too.
#[tokio::test(flavor = "multi_thread")]
async fn clearing_the_transcript_clears_the_notes() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        turn(&[save("s1", "customer id 42")]),
        answer("saved", 1),
        answer("no notes", 1),
    ]);
    let profile = AgentProfile::new("auditor").context_reset(ContextReset::new(1, 90));
    let mut rt = runtime(&db, AgentHarness::new(model.clone()).agent(profile));
    let agent = agent_id("acct-2");
    sqlite::send(&mut rt, &agent, &AgentOp::message("remember"), None).unwrap();
    let clear = AgentOp::Control(autumn_harvest_agent::Control::ClearTranscript);
    sqlite::send(&mut rt, &agent, &clear, None).unwrap();
    sqlite::send(&mut rt, &agent, &AgentOp::message("what now?"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let state = sqlite::state(&rt, &agent).await.unwrap().unwrap();
    assert_eq!(state.notes, None);
    let last = model.requests().pop().unwrap();
    assert!(!json!(last.messages).to_string().contains("customer id 42"));
}

const DB_ENV: &str = "CONTEXT_RESET_CRASH_DB";
const LOG_ENV: &str = "CONTEXT_RESET_CRASH_LOG";

/// The script up to the reset, then the answer after it.
fn crash_script() -> Vec<Reply> {
    vec![
        turn(&[save("s1", "rows 1 to 9 are fine"), big("c1")]),
        turn(&[big("c2")]),
        turn(&[save("s2", "rows 1 to 9 are fine")]),
        answer("all rows are fine", 1),
    ]
}

/// A model that logs each request as one JSON line. With `abort_on` set, it
/// aborts the process inside that call, after the log.
#[derive(Debug)]
struct LoggedModel {
    inner: Arc<ScriptedModel>,
    log_path: PathBuf,
    who: &'static str,
    abort_on: Option<usize>,
}

impl AgentModel for LoggedModel {
    fn chat<'a>(&'a self, request: &'a ChatRequest) -> autumn_harvest_agent::BoxFuture<'a, Reply> {
        let line = json!({
            "who": self.who,
            "messages": request.messages,
            "tools": request.tools,
            "max_tokens": request.max_tokens,
            "temperature": request.temperature,
        })
        .to_string();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .unwrap();
        writeln!(file, "{line}").unwrap();
        file.sync_all().unwrap();
        if self.abort_on == Some(self.inner.calls() + 1) {
            std::process::abort();
        }
        self.inner.chat(request)
    }
}

fn logged(
    replies: Vec<Reply>,
    log_path: &Path,
    who: &'static str,
    abort_on: Option<usize>,
) -> Arc<LoggedModel> {
    Arc::new(LoggedModel {
        inner: ScriptedModel::new(replies),
        log_path: log_path.to_path_buf(),
        who,
        abort_on,
    })
}

/// The child half of the crash test. It does nothing unless the parent set
/// the environment. It aborts inside the first model call after the reset.
#[tokio::test(flavor = "multi_thread")]
async fn child_crashes_right_after_a_reset() {
    let (Some(db), Some(log_path)) = (std::env::var_os(DB_ENV), std::env::var_os(LOG_ENV)) else {
        return;
    };
    let model = logged(crash_script(), Path::new(&log_path), "child", Some(4));
    let mut rt = runtime(Path::new(&db), harness(model));
    let exec = sqlite::start(&mut rt, &task(Some(ContextReset::new(1, 50)))).unwrap();
    std::fs::write(Path::new(&db).with_extension("exec"), exec.to_string()).unwrap();
    let _ = rt.run_until_blocked(exec).await;
    panic!("the child must abort before the run ends");
}

/// The requests that `who` logged, in order.
fn logged_requests(log_path: &Path, who: &str) -> Vec<ChatRequest> {
    std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|line| line["who"] == who)
        .map(|line| ChatRequest {
            messages: serde_json::from_value(line["messages"].clone()).unwrap(),
            tools: serde_json::from_value(line["tools"].clone()).unwrap(),
            max_tokens: serde_json::from_value(line["max_tokens"].clone()).unwrap(),
            temperature: serde_json::from_value(line["temperature"].clone()).unwrap(),
        })
        .collect()
}

/// AC 4: a crash right after a reset replays to the same window. The cache
/// key of the next turn equals the key before the crash.
#[tokio::test(flavor = "multi_thread")]
async fn crash_after_a_reset_replays_the_same_window() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("agent.db");
    let log_path = dir.path().join("requests.log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_crashes_right_after_a_reset",
            "--nocapture",
        ])
        .env(DB_ENV, &db)
        .env(LOG_ENV, &log_path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    let before = logged_requests(&log_path, "child");
    assert_eq!(
        before.len(),
        4,
        "the child dies inside the fourth model call"
    );
    assert!(is_save_turn(&before[2]));
    assert!(
        holds_no_big_result(&before[3]),
        "the fourth call follows the reset"
    );

    let mut rest = crash_script();
    rest.drain(..3);
    let model = logged(rest, &log_path, "parent", None);
    let mut rt = runtime(&db, harness(model));
    let exec: ExecutionId = std::fs::read_to_string(db.with_extension("exec"))
        .unwrap()
        .parse()
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let report: AgentReport = report(state);
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.resets, 1);

    let after = logged_requests(&log_path, "parent");
    assert_eq!(after.len(), 1, "only the call in flight is sent again");
    assert_eq!(after[0], before[3], "replay rebuilds the same window");
    // The cache key hashes the model id, the tools and the messages.
    let cache = response_cache(&Arc::new(MemStore::default()));
    let key = |request: &ChatRequest| cache.key(KeyTenant::None, "m", request).unwrap();
    assert_eq!(key(&after[0]), key(&before[3]));
    assert_ne!(key(&after[0]), key(&before[2]), "the key sees the reset");
}

/// A reset in a follow-up segment keeps the task, the booked call and the
/// wake message. The wake message points at that call.
#[tokio::test(flavor = "multi_thread")]
async fn a_reset_in_a_followup_segment_keeps_the_followup_call() {
    let (_dir, db) = fresh_db();
    let followup = (
        "f1".to_owned(),
        autumn_harvest_agent::followup::FOLLOWUP_TOOL.to_owned(),
        json!({"prompt": "check the totals", "delay_minutes": 1}),
    );
    let model = ScriptedModel::new(vec![
        turn(&[save("s1", "rows are fine"), followup]),
        answer("booked", 1),
        // The follow-up segment.
        turn(&[big("c1")]),
        turn(&[big("c2")]),
        turn(&[save("s2", "rows are fine; totals next")]),
        answer("totals are fine", 1),
    ]);
    let mut rt = runtime(&db, harness(model.clone()));

    let followups = autumn_harvest_agent::Followups::new(std::time::Duration::from_secs(600));
    let task = task(Some(ContextReset::new(1, 50))).followups(followups);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let waiting = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(waiting, RunState::WaitingTimer), "{waiting:?}");
    let later = autumn_harvest::chrono::Utc::now() + autumn_harvest::chrono::Duration::seconds(120);
    let report = report(rt.run_until_blocked_as_of(exec, later).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.followups, 1);
    assert_eq!(report.resets, 1);
    let requests = model.requests();
    let after = shape(&requests[5].messages);
    let followup_call = format!("call {} ", autumn_harvest_agent::followup::FOLLOWUP_TOOL);
    assert_eq!(after.len(), 7, "{after:?}");
    assert_eq!(after[0], (ChatRole::System, "You audit.".to_owned()));
    assert_eq!(after[1], (ChatRole::User, "audit the ledger".to_owned()));
    // The booked call stays, so the wake message still points at it.
    assert_eq!(after[2], (ChatRole::Assistant, followup_call));
    assert_eq!(after[3], (ChatRole::Tool, "result".to_owned()));
    assert_eq!(after[4].0, ChatRole::User);
    assert!(after[4].1.starts_with("[Scheduled follow-up]"), "{after:?}");
    assert_eq!(
        after[5],
        (
            ChatRole::Assistant,
            format!("call {SAVE_PROGRESS_TOOL} rows are fine; totals next")
        )
    );
    let text = json!(requests[5].messages).to_string();
    assert!(text.contains("check the totals"), "{text}");
}

/// A reset keeps each steer of the op after the notes, so it never drops a
/// message from a person.
#[tokio::test(flavor = "multi_thread")]
async fn a_reset_keeps_the_steers_of_the_op() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        // The first attempt fails, so the op waits on its retry backoff.
        Err(autumn_harvest_agent::AgentError::new(
            autumn_harvest_agent::ErrorKind::RateLimited,
            "slow down",
        )),
        turn(&[save("s1", "rows 1 to 5 are fine"), big("c1")]),
        turn(&[big("c2")]),
        turn(&[save("s2", "rows 1 to 8 are fine")]),
        answer("row 9 is fine too", 1),
    ]);
    let mut profile = AgentProfile::new("auditor").context_reset(ContextReset::new(1, 50));
    profile.max_request_bytes = Some(CAP);
    profile.loop_guard = LoopGuard::disabled();
    let mut rt = runtime(&db, harness(model.clone()).agent(profile));
    let agent = agent_id("acct-3");
    sqlite::send(&mut rt, &agent, &AgentOp::message("audit the ledger"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let steer = AgentOp::urgent(
        "also check row 9",
        "s-1",
        autumn_harvest_agent::Urgency::Steer,
    );
    sqlite::send(&mut rt, &agent, &steer, Some("s-1")).unwrap();
    let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
    let later = autumn_harvest::chrono::Utc::now() + autumn_harvest::chrono::Duration::seconds(60);
    rt.run_until_blocked_as_of(exec, later).await.unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let requests = model.requests();
    assert_eq!(requests.len(), 5);
    assert_eq!(
        shape(&requests[4].messages),
        [
            (ChatRole::User, "audit the ledger".to_owned()),
            (
                ChatRole::Assistant,
                format!("call {SAVE_PROGRESS_TOOL} rows 1 to 8 are fine")
            ),
            (ChatRole::Tool, "result".to_owned()),
            (ChatRole::User, "also check row 9".to_owned()),
        ]
    );
    let state = sqlite::state(&rt, &agent).await.unwrap().unwrap();
    assert_eq!(state.last_answer.as_deref(), Some("row 9 is fine too"));
}
