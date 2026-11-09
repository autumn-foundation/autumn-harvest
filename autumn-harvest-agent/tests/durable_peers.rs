//! Durable peers: agents that message each other (issue #2269).
//!
//! The tests drive two agents on one SQLite runtime. A `delegate` call to a
//! durable peer sends a peer message and waits on a durable promise. The
//! peer settles the promise with its answer.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::chrono::{self, DateTime, Utc};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::agent::MAX_HELD_OPS;
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentHeartbeat, AgentId, AgentModel, AgentOp, AgentProfile,
    AgentState, AgentTask, BoxFuture, ChatRequest, ChatResponse, ChatRole, ContentPart, Control,
    ErrorKind, FlowRules, FnTool, Followups, Mandate, Peer, PeerEnvelope, Rule, StopReason,
    TokenUsage, ToolEffect, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{Recorder, ScriptedModel, TryFnModel, recorded_tool, report, runtime};
use serde_json::{Value, json};

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn triage() -> AgentId {
    AgentId::new("triage", "t1").unwrap()
}

fn research() -> AgentId {
    AgentId::new("research", "r1").unwrap()
}

fn state(rt: &SqliteRuntime, agent: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = Box::pin(sqlite::state(rt, agent));
    async move { query.await.unwrap().expect("the agent runs") }
}

type Script = dyn Fn(&ChatRequest) -> ChatResponse + Send + Sync;

/// A model that answers each request with a function of the request, and
/// keeps every request.
struct FnModel {
    answer: Box<Script>,
    requests: Mutex<Vec<ChatRequest>>,
}

impl std::fmt::Debug for FnModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FnModel")
    }
}

impl FnModel {
    fn new(answer: impl Fn(&ChatRequest) -> ChatResponse + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            answer: Box::new(answer),
            requests: Mutex::new(Vec::new()),
        })
    }

    fn requests(&self) -> Vec<ChatRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl AgentModel for FnModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.requests.lock().unwrap().push(request.clone());
        let response = (self.answer)(request);
        Box::pin(async move { Ok(response) })
    }
}

fn text(reply: &str) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(reply.to_owned())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(1, 1),
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
        usage: TokenUsage::new(1, 1),
    }
}

/// The text of the first user message of a request.
fn first_user(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .find(|message| message.role == ChatRole::User)
        .and_then(|message| match message.content.first() {
            Some(ContentPart::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// The text of the last user message of a request.
fn last_user(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .rev()
        .find(|message| message.role == ChatRole::User)
        .and_then(|message| match message.content.first() {
            Some(ContentPart::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// The text of each tool result of a request.
fn tool_results(request: &ChatRequest) -> Vec<String> {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect()
}

/// A request that answers a peer message.
fn from_peer(request: &ChatRequest) -> bool {
    first_user(request).starts_with("[A message from the agent")
}

/// The usual mesh: triage asks the durable researcher, which answers.
fn mesh_model() -> Arc<FnModel> {
    FnModel::new(|request| {
        let results = tool_results(request);
        if from_peer(request) {
            return text("The answer is 42.");
        }
        if results.is_empty() {
            call(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "find the answer"}),
            )
        } else {
            text(&format!("The researcher says: {}", results[0]))
        }
    })
}

fn mesh_harness(model: Arc<dyn AgentModel>) -> AgentHarness {
    AgentHarness::new(model)
        .peer(Peer::agent("researcher", "Finds facts.", &research()))
        .agent(AgentProfile::new("triage").mandate(Mandate::new("triage").peers(["researcher"])))
        .agent(AgentProfile::new("research").mandate(Mandate::new("researcher")))
}

/// Start an agent with a no-op op, so a peer can reach it.
async fn start(rt: &mut SqliteRuntime, agent: &AgentId) {
    sqlite::send(rt, agent, &AgentOp::Control(Control::Resume), None).unwrap();
    sqlite::drive(rt, agent).await.unwrap();
}

/// The peer messages in the history of `agent`.
fn peer_messages(rt: &SqliteRuntime, agent: &AgentId) -> Vec<PeerEnvelope> {
    let exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::SignalReceived { payload, .. } => {
                match serde_json::from_value::<Value>(payload["op"].clone()) {
                    Ok(op) if op["type"] == "peer_message" => serde_json::from_value(op).ok(),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_message_is_delivered_once_and_answered() {
    let (_dir, db) = fresh_db();
    let model = mesh_model();
    let mut rt = runtime(&db, mesh_harness(model.clone()));
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("what is it?"), None).unwrap();
    let waiting = sqlite::drive(&mut rt, &triage()).await.unwrap();
    assert!(
        matches!(&waiting, Some(RunState::WaitingSignal(name)) if name.starts_with("harvest.promise:")),
        "triage waits on the reply: {waiting:?}"
    );
    // The process stops after the send. A new one finishes the exchange.
    drop(rt);
    let mut rt = runtime(&db, mesh_harness(model.clone()));
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let sender = state(&rt, &triage()).await;
    let answer = sender.last_answer.unwrap();
    assert!(answer.contains("The answer is 42."), "{answer}");
    let received = peer_messages(&rt, &research());
    assert_eq!(received.len(), 1, "one delivery");
    assert_eq!(
        received[0].from,
        format!("{}@{}", triage(), sender.incarnation)
    );
    assert_eq!(received[0].seq, 1);
    let peer_turns = model.requests().iter().filter(|r| from_peer(r)).count();
    assert_eq!(peer_turns, 1, "the researcher answered once");
    assert_eq!(sender.outbound_seq.get(research().as_str()), Some(&1));
}

fn critic() -> AgentId {
    AgentId::new("critic", "c1").unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn each_receiver_counts_its_own_seqs() {
    // One counter for all peers leaves gaps at each receiver. Then the
    // floor of its applied set stops (formal-methods.md, `AgentMesh`).
    let (_dir, db) = fresh_db();
    let model = FnModel::new(|request| {
        if from_peer(request) {
            return text("noted");
        }
        match tool_results(request).len() {
            0 => call(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "a"}),
            ),
            1 => call("d2", DELEGATE_TOOL, json!({"agent": "critic", "task": "b"})),
            _ => text("done"),
        }
    });
    let harness = AgentHarness::new(model.clone())
        .peer(Peer::agent("researcher", "Finds facts.", &research()))
        .peer(Peer::agent("critic", "Checks facts.", &critic()))
        .agent(
            AgentProfile::new("triage")
                .mandate(Mandate::new("triage").peers(["researcher", "critic"])),
        )
        .agent(AgentProfile::new("research").mandate(Mandate::new("researcher")))
        .agent(AgentProfile::new("critic").mandate(Mandate::new("critic")));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    start(&mut rt, &critic()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &critic()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    assert_eq!(
        state(&rt, &triage()).await.last_answer.as_deref(),
        Some("done")
    );
    let seqs =
        |agent: &AgentId| -> Vec<u64> { peer_messages(&rt, agent).iter().map(|m| m.seq).collect() };
    assert_eq!(seqs(&research()), [1]);
    assert_eq!(seqs(&critic()), [1], "the critic sees no gap");
    let critic_state = state(&rt, &critic()).await;
    let applied: Vec<_> = critic_state.inbound.senders.values().collect();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].floor, 1, "the floor moves");
    assert!(applied[0].above.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recreated_sender_is_not_taken_for_a_duplicate() {
    // A new incarnation of the sender counts from 1 again. Its messages
    // must not match the keys or the applied set of the old one.
    let (_dir, db) = fresh_db();
    let model = mesh_model();
    let mut rt = runtime(&db, mesh_harness(model.clone()));
    start(&mut rt, &research()).await;
    for round in 1..=2 {
        sqlite::send(&mut rt, &triage(), &AgentOp::message("what is it?"), None).unwrap();
        sqlite::drive(&mut rt, &triage()).await.unwrap();
        sqlite::drive(&mut rt, &research()).await.unwrap();
        sqlite::drive(&mut rt, &triage()).await.unwrap();
        let answer = state(&rt, &triage()).await.last_answer.unwrap();
        assert!(
            answer.contains("The answer is 42."),
            "round {round}: {answer}"
        );
        if round == 1 {
            sqlite::delete(&mut rt, &triage(), None).unwrap();
            sqlite::drive(&mut rt, &triage()).await.unwrap();
        }
    }
    let received = peer_messages(&rt, &research());
    assert_eq!(received.len(), 2);
    assert_ne!(received[0].from, received[1].from);
    assert_eq!((received[0].seq, received[1].seq), (1, 1));
    let peer_turns = model.requests().iter().filter(|r| from_peer(r)).count();
    assert_eq!(peer_turns, 2, "the researcher answered both lives");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_is_dropped_and_a_late_message_is_kept() {
    let (_dir, db) = fresh_db();
    let model = mesh_model();
    let mut rt = runtime(&db, mesh_harness(model.clone()));
    start(&mut rt, &research()).await;
    let envelope = |seq: u64| PeerEnvelope {
        from: "triage/other".into(),
        sender: "triage".into(),
        seq,
        text: format!("message {seq}"),
        mandate: Mandate::new("researcher"),
        flow_labels: std::collections::BTreeSet::default(),
        reply_to: None,
        waiting: Vec::new(),
        mesh: None,
        sample: None,
    };
    // The outbox delivers 2 before 1, then 2 again under another key.
    for (seq, key) in [(2, "k2"), (1, "k1"), (2, "k2-again")] {
        let op = AgentOp::peer(envelope(seq));
        sqlite::send(&mut rt, &research(), &op, Some(key)).unwrap();
    }
    // The engine drops a repeat of a key on its own.
    let op = AgentOp::peer(envelope(1));
    let repeat = sqlite::send(&mut rt, &research(), &op, Some("k1")).unwrap();
    assert!(!repeat.signal_delivered);
    sqlite::drive(&mut rt, &research()).await.unwrap();

    let handled: Vec<String> = model
        .requests()
        .iter()
        .filter(|request| from_peer(request))
        .map(last_user)
        .collect();
    assert_eq!(handled.len(), 2, "each number runs once: {handled:?}");
    assert!(handled[0].contains("message 2") && handled[1].contains("message 1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_cannot_widen_a_mandate() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = FnModel::new(|request| {
        let results = tool_results(request);
        if from_peer(request) {
            return if results.is_empty() {
                call("w1", "write", json!({}))
            } else {
                text(&format!("write said: {}", results[0]))
            };
        }
        if results.is_empty() {
            call(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "write it"}),
            )
        } else {
            text("done")
        }
    });
    // Triage may call no tool. The researcher may write, on its own.
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .peer(Peer::agent("researcher", "Writes.", &research()))
        .agent(
            AgentProfile::new("triage").mandate(
                Mandate::new("triage")
                    .peers(["researcher"])
                    .tools(Vec::<String>::new()),
            ),
        )
        .agent(AgentProfile::new("research").mandate(Mandate::new("researcher")));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    assert!(recorder.runs().is_empty(), "the narrowed mandate holds");
    let peer_answer = model
        .requests()
        .iter()
        .filter(|request| from_peer(request))
        .flat_map(tool_results)
        .collect::<Vec<_>>();
    assert!(
        peer_answer[0].contains("not in this agent's mandate"),
        "{peer_answer:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_inbound_mandate_cannot_widen_the_local_one() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = FnModel::new(|request| {
        let results = tool_results(request);
        if from_peer(request) {
            return if results.is_empty() {
                call("w1", "write", json!({}))
            } else {
                text(&format!("write said: {}", results[0]))
            };
        }
        if results.is_empty() {
            call(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "write it"}),
            )
        } else {
            text("done")
        }
    });
    // Triage may call every tool. The researcher may only read, on its own.
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .peer(Peer::agent("researcher", "Reads.", &research()))
        .agent(AgentProfile::new("triage").mandate(Mandate::new("triage").peers(["researcher"])))
        .agent(AgentProfile::new("research").mandate(Mandate::new("researcher").tools(["read"])));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    assert!(recorder.runs().is_empty(), "the local mandate holds");
}

#[tokio::test(flavor = "multi_thread")]
async fn flow_labels_taint_the_receiver() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = FnModel::new(|request| {
        let results = tool_results(request);
        if from_peer(request) {
            return if results.is_empty() {
                call("s1", "send", json!({}))
            } else {
                text(&format!("send said: {}", results[0]))
            };
        }
        match results.len() {
            0 => call("i1", "inbox", json!({})),
            1 => call(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "reply to the mail"}),
            ),
            _ => text("done"),
        }
    });
    let inbox = FnTool::new(
        "inbox",
        "Read mail.",
        json!({"type": "object"}),
        |_| async { Ok(json!("mail from a stranger")) },
    )
    .effect(ToolEffect::ReadOnly)
    .label(labels::UNTRUSTED_INPUT)
    .shared();
    let harness = AgentHarness::new(model.clone())
        .tool(inbox)
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .flow_rules(FlowRules::new().forbid(
            [labels::UNTRUSTED_INPUT, "effect:external"],
            Rule::Deny("no send after untrusted input".into()),
        ))
        .peer(Peer::agent("researcher", "Sends.", &research()))
        .agent(AgentProfile::new("triage").mandate(Mandate::new("triage").peers(["researcher"])))
        .agent(AgentProfile::new("research").mandate(Mandate::new("researcher")));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(
        &mut rt,
        &triage(),
        &AgentOp::message("handle the mail"),
        None,
    )
    .unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    assert!(recorder.runs().is_empty(), "the tainted peer cannot send");
    let receiver = state(&rt, &research()).await;
    assert!(receiver.flow_labels.contains(labels::UNTRUSTED_INPUT));
    let peer_results: Vec<String> = model
        .requests()
        .iter()
        .filter(|request| from_peer(request))
        .flat_map(tool_results)
        .collect();
    assert!(
        peer_results[0].contains("no send after untrusted input"),
        "{peer_results:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_a_b_a_request_ends_with_a_deadlock_error_before_its_deadline() {
    let (_dir, db) = fresh_db();
    let model = FnModel::new(|request| {
        let results = tool_results(request);
        if from_peer(request) {
            // The researcher asks triage back, which waits on it.
            return if results.is_empty() {
                call(
                    "d2",
                    DELEGATE_TOOL,
                    json!({"agent": "triager", "task": "what did you mean?"}),
                )
            } else {
                text(&format!("asking back failed: {}", results[0]))
            };
        }
        if results.is_empty() {
            call(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "find it"}),
            )
        } else {
            text(&format!("researcher: {}", results[0]))
        }
    });
    // Triage lets the researcher ask back, so only the deadlock check stops
    // the request.
    let harness = AgentHarness::new(model.clone())
        .peer(Peer::agent("researcher", "Finds facts.", &research()).peers(["triager"]))
        .peer(Peer::agent("triager", "Triages.", &triage()))
        .agent(
            AgentProfile::new("triage")
                .mandate(Mandate::new("triage").peers(["researcher", "triager"])),
        )
        .agent(
            AgentProfile::new("research").mandate(Mandate::new("researcher").peers(["triager"])),
        );
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let triage_state = state(&rt, &triage()).await;
    let answer = triage_state.last_answer.unwrap();
    assert!(answer.contains("deadlock"), "{answer}");
    assert_eq!(
        triage_state.last_stop,
        Some(autumn_harvest_agent::AgentStop::Completed)
    );
    // No deadline fired: the refusal came at once.
    let exec = sqlite::current_run(&rt, &triage()).unwrap().unwrap();
    let fired = rt
        .load_history(exec)
        .unwrap()
        .iter()
        .filter(|event| matches!(event, WorkflowEvent::TimerFired { .. }))
        .count();
    assert_eq!(fired, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_send_to_an_agent_with_no_run_is_refused() {
    let (_dir, db) = fresh_db();
    let model = mesh_model();
    let mut rt = runtime(&db, mesh_harness(model.clone()));
    // The researcher never started.
    sqlite::send(&mut rt, &triage(), &AgentOp::message("what is it?"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    let answer = state(&rt, &triage()).await.last_answer.unwrap();
    assert!(
        answer.contains("peer_unavailable") || answer.contains("not running"),
        "{answer}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reply_after_the_deadline_is_not_waited_for() {
    let (_dir, db) = fresh_db();
    let model = mesh_model();
    let harness = AgentHarness::new(model.clone())
        .peer(
            Peer::agent("researcher", "Finds facts.", &research())
                .reply_timeout(std::time::Duration::from_secs(60)),
        )
        .agent(AgentProfile::new("triage").mandate(Mandate::new("triage").peers(["researcher"])))
        .agent(AgentProfile::new("research").mandate(Mandate::new("researcher")));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("what is it?"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    // The researcher does not run. The deadline passes.
    let exec = sqlite::current_run(&rt, &triage()).unwrap().unwrap();
    let later = autumn_harvest::chrono::Utc::now() + chrono::Duration::seconds(120);
    rt.run_until_blocked_as_of(exec, later).await.unwrap();
    let answer = state(&rt, &triage()).await.last_answer.unwrap();
    assert!(answer.contains("no reply arrived within 60s"), "{answer}");
    // Issue #2484: a durable sender reads the verdict too.
    assert!(answer.contains("reply_timeout"), "{answer}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receiver_on_a_model_that_the_sender_forbids_refuses_the_message() {
    // The sender does not know the model of a durable peer. The receiver
    // checks its own model against the mandate that it accepts (issue #2271).
    let (_dir, db) = fresh_db();
    let model = mesh_model();
    let strong = FnModel::new(|_| text("never"));
    let harness = AgentHarness::new(model.clone())
        .model_named("strong", strong.clone())
        .peer(Peer::agent("researcher", "Finds facts.", &research()))
        .agent(
            AgentProfile::new("triage").mandate(
                Mandate::new("triage")
                    .peers(["researcher"])
                    .models([autumn_harvest_agent::DEFAULT_MODEL]),
            ),
        )
        .agent(
            AgentProfile::new("research")
                .model("strong")
                .mandate(Mandate::new("researcher")),
        );
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("what is it?"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let answer = state(&rt, &triage()).await.last_answer.unwrap();
    assert!(answer.contains("model_forbidden"), "{answer}");
    assert!(
        strong.requests().is_empty(),
        "the forbidden model never runs"
    );
}

/// The tool results of every request that answers a peer message.
fn peer_tool_results(model: &FnModel) -> Vec<String> {
    model
        .requests()
        .iter()
        .filter(|request| from_peer(request))
        .flat_map(tool_results)
        .collect()
}

/// The timers that fired in the running run of `agent`.
fn fired_timers(rt: &SqliteRuntime, agent: &AgentId) -> usize {
    let exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    rt.load_history(exec)
        .unwrap()
        .iter()
        .filter(|event| matches!(event, WorkflowEvent::TimerFired { .. }))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_inbound_mandate_makes_the_receiver_read_only() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = FnModel::new(|request| {
        let results = tool_results(request);
        if from_peer(request) {
            return if results.is_empty() {
                call("w1", "write", json!({}))
            } else {
                text(&format!("write said: {}", results[0]))
            };
        }
        if results.is_empty() {
            call(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "write it"}),
            )
        } else {
            text("done")
        }
    });
    // The researcher may write on its own. Triage reaches it only through a
    // read-only peer, so the inbound mandate is read-only.
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .peer(Peer::agent("researcher", "Writes.", &research()).read_only())
        .agent(AgentProfile::new("triage").mandate(Mandate::new("triage").peers(["researcher"])))
        .agent(AgentProfile::new("research").mandate(Mandate::new("researcher")));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    assert!(recorder.runs().is_empty(), "the write tool never runs");
    let results = peer_tool_results(&model);
    assert!(results[0].contains("only read"), "{results:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_peer_segment_sends_no_answer_of_an_earlier_op() {
    let (_dir, db) = fresh_db();
    // One model serves both agents, in the order that the test drives them.
    let model = ScriptedModel::new(vec![
        Ok(text("an old answer")),
        Ok(call(
            "d1",
            DELEGATE_TOOL,
            json!({"agent": "researcher", "task": "find it"}),
        )),
        Err(AgentError::new(ErrorKind::Provider, "the provider refused")),
        Ok(text("done")),
    ]);
    let mut rt = runtime(&db, mesh_harness(model.clone()));
    // An earlier op of the researcher leaves an answer in its state.
    sqlite::send(&mut rt, &research(), &AgentOp::message("hello"), None).unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::send(&mut rt, &triage(), &AgentOp::message("what is it?"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let requests = model.requests();
    assert_eq!(requests.len(), 4, "triage reads the reply");
    let results = tool_results(&requests[3]);
    let reply: Value = serde_json::from_str(&results[0]).unwrap();
    assert_eq!(reply["stop"], Value::Null, "{reply}");
    assert_eq!(reply["answer"], json!(""), "{reply}");
    let error = reply["error"].as_str().unwrap_or_default();
    assert!(error.contains("the provider refused"), "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_segment_books_no_followup() {
    let (_dir, db) = fresh_db();
    let model = FnModel::new(|request| {
        let results = tool_results(request);
        if from_peer(request) {
            return if results.is_empty() {
                call(
                    "f1",
                    "schedule_followup",
                    json!({"prompt": "check again", "delay_minutes": 1}),
                )
            } else {
                text(&format!("the booking said: {}", results[0]))
            };
        }
        if results.is_empty() {
            call(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "check it later"}),
            )
        } else {
            text("done")
        }
    });
    // The researcher may book follow-ups on its own, but not for a peer.
    let harness = AgentHarness::new(model.clone())
        .peer(Peer::agent("researcher", "Finds facts.", &research()))
        .agent(AgentProfile::new("triage").mandate(Mandate::new("triage").peers(["researcher"])))
        .agent(
            AgentProfile::new("research")
                .mandate(Mandate::new("researcher"))
                .followups(Followups::new(Duration::from_secs(3_600))),
        );
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let receiver = state(&rt, &research()).await;
    assert_eq!(receiver.followup_due_ms, None, "no follow-up is booked");
    let results = peer_tool_results(&model);
    assert!(!results[0].contains("scheduled"), "{results:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_held_queue_answers_a_waiting_peer_at_once() {
    let (_dir, db) = fresh_db();
    let model = mesh_model();
    let mut rt = runtime(&db, mesh_harness(model.clone()));
    // The researcher is paused, and its held queue is full.
    let pause = AgentOp::Control(Control::Pause { reason: None });
    sqlite::send(&mut rt, &research(), &pause, None).unwrap();
    for n in 0..MAX_HELD_OPS {
        let op = AgentOp::message(format!("m{n}"));
        sqlite::send(&mut rt, &research(), &op, None).unwrap();
    }
    sqlite::drive(&mut rt, &research()).await.unwrap();
    assert_eq!(state(&rt, &research()).await.held.len(), MAX_HELD_OPS);

    sqlite::send(&mut rt, &triage(), &AgentOp::message("what is it?"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let answer = state(&rt, &triage()).await.last_answer.unwrap();
    assert!(answer.contains("the held queue is full"), "{answer}");
    assert_eq!(fired_timers(&rt, &triage()), 0, "the reply came at once");
    assert_eq!(
        state(&rt, &research()).await.held.len(),
        MAX_HELD_OPS,
        "the refused message is not held"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fan_out_child_that_asks_its_waiting_parent_gets_a_deadlock_error_at_once() {
    let (_dir, db) = fresh_db();
    let model = FnModel::new(|request| {
        let input = first_user(request);
        let results = tool_results(request);
        match input.as_str() {
            // Two delegations in one turn run as two child runs.
            "go" if results.is_empty() => ChatResponse {
                content: vec![
                    ContentPart::ToolCall {
                        id: "d1".to_owned(),
                        name: DELEGATE_TOOL.to_owned(),
                        arguments: json!({"agent": "flights", "task": "ask the boss"}),
                    },
                    ContentPart::ToolCall {
                        id: "d2".to_owned(),
                        name: DELEGATE_TOOL.to_owned(),
                        arguments: json!({"agent": "hotels", "task": "find hotels"}),
                    },
                ],
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::new(1, 1),
            },
            "go" => text(&format!("children: {}", results.join(" | "))),
            // The flights child asks the planner, which waits on the child.
            "ask the boss" if results.is_empty() => call(
                "b1",
                DELEGATE_TOOL,
                json!({"agent": "boss", "task": "what did you mean?"}),
            ),
            "ask the boss" => text(&format!("asking back failed: {}", results[0])),
            _ => text("hotels found"),
        }
    });
    let harness = AgentHarness::new(model.clone())
        .peer(Peer::new("flights", "Finds flights.").peers(["boss"]))
        .peer(Peer::new("hotels", "Finds hotels."))
        .peer(Peer::agent("boss", "Plans the trip.", &triage()))
        .agent(
            AgentProfile::new("triage")
                .mandate(Mandate::new("triage").peers(["flights", "hotels", "boss"]))
                .parallel_children(2),
        );
    let mut rt = runtime(&db, harness);
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let planner = state(&rt, &triage()).await;
    let answer = planner.last_answer.expect("the op ends");
    assert!(answer.contains("deadlock"), "{answer}");
    assert_eq!(fired_timers(&rt, &triage()), 0, "no reply deadline fired");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_segment_with_a_failed_fan_out_child_keeps_its_later_sibling() {
    let (_dir, db) = fresh_db();
    let model = TryFnModel::new(|request| {
        let results = tool_results(request);
        if from_peer(request) && results.is_empty() {
            // The researcher fans out to two inline children at once.
            return Ok(ChatResponse {
                content: vec![
                    ContentPart::ToolCall {
                        id: "d1".to_owned(),
                        name: DELEGATE_TOOL.to_owned(),
                        arguments: json!({"agent": "flights", "task": "find flights"}),
                    },
                    ContentPart::ToolCall {
                        id: "d2".to_owned(),
                        name: DELEGATE_TOOL.to_owned(),
                        arguments: json!({"agent": "hotels", "task": "find hotels"}),
                    },
                ],
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::new(1, 1),
            });
        }
        match (first_user(request).as_str(), results.is_empty()) {
            ("find flights", _) => Err(AgentError::new(
                ErrorKind::Provider,
                "the flights provider refused",
            )),
            ("find hotels", true) => Ok(call("h1", "find_hotels", json!({}))),
            ("find hotels", false) => Ok(text("hotels found")),
            (_, true) => Ok(call(
                "r1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "plan the trip"}),
            )),
            (_, false) => Ok(text("done")),
        }
    });
    let find_hotels = FnTool::new(
        "find_hotels",
        "Reads.",
        json!({"type": "object"}),
        |_| async { Ok(json!("data")) },
    )
    .effect(ToolEffect::ReadOnly)
    .label("hotel")
    .shared();
    // A rule that never fires turns the labels on.
    let rules = FlowRules::new().forbid(
        ["hotel", labels::UNTRUSTED_INPUT],
        Rule::Deny("never".into()),
    );
    let harness = AgentHarness::new(model)
        .tool(find_hotels)
        .flow_rules(rules)
        .peer(Peer::new("flights", "Finds flights."))
        .peer(Peer::new("hotels", "Finds hotels."))
        .peer(Peer::agent("researcher", "Plans trips.", &research()).peers(["flights", "hotels"]))
        .agent(
            AgentProfile::new("triage").mandate(Mandate::new("triage").peers([
                "researcher",
                "flights",
                "hotels",
            ])),
        )
        .agent(
            AgentProfile::new("research")
                .mandate(Mandate::new("researcher").peers(["flights", "hotels"]))
                .parallel_children(2),
        );
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("plan it"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let receiver = state(&rt, &research()).await;
    assert_eq!(
        receiver.failures, 1,
        "the failed child fails the peer segment"
    );
    // The researcher turn spent 2 tokens. Each turn of the hotels child spent 2.
    assert_eq!(
        receiver.tokens_used,
        2 + 2 + 2,
        "the reply counts the tokens of the sibling"
    );
    assert!(
        receiver.flow_labels.contains("hotel"),
        "{:?}",
        receiver.flow_labels
    );
    let sender = state(&rt, &triage()).await;
    assert!(
        sender.flow_labels.contains("hotel"),
        "the reply carries the taint of the sibling: {:?}",
        sender.flow_labels
    );
}

/// Drive `agent` at the drive clock `at`, through each checkpoint.
async fn drive_at(rt: &mut SqliteRuntime, agent: &AgentId, at: DateTime<Utc>) -> RunState {
    let mut exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    loop {
        match rt.run_until_blocked_as_of(exec, at).await.unwrap() {
            RunState::ContinuedAsNew(next) => exec = next,
            other => return other,
        }
    }
}

/// A model in which each peer message calls `write` once. `sender` answers
/// every other request.
fn writer_model(
    sender: impl Fn(&ChatRequest) -> ChatResponse + Send + Sync + 'static,
) -> Arc<FnModel> {
    FnModel::new(move |request| {
        if !from_peer(request) {
            return sender(request);
        }
        let results = tool_results(request);
        if results.is_empty() {
            call("w1", "write", json!({}))
        } else {
            text(&format!("write said: {}", results[0]))
        }
    })
}

/// The call that asks the researcher to write.
fn ask_call() -> ChatResponse {
    call(
        "d1",
        DELEGATE_TOOL,
        json!({"agent": "researcher", "task": "write it"}),
    )
}

/// The sender asks the researcher once, then ends.
fn ask_researcher(request: &ChatRequest) -> ChatResponse {
    if tool_results(request).is_empty() {
        ask_call()
    } else {
        text("done")
    }
}

/// The sender books one follow-up. The follow-up asks the researcher once.
fn ask_on_wake(request: &ChatRequest) -> ChatResponse {
    let results = tool_results(request);
    let woken = last_user(request).starts_with("[Scheduled follow-up]");
    let asked = results
        .iter()
        .any(|result| result.contains("\"agent\":\"researcher\""));
    match (woken, results.is_empty(), asked) {
        (false, true, _) => call(
            "f1",
            "schedule_followup",
            json!({"prompt": "ask the researcher", "delay_minutes": 1}),
        ),
        (false, false, _) => text("booked"),
        (true, _, false) => ask_call(),
        (true, _, true) => text("done"),
    }
}

/// A mesh with `sender` and a durable researcher that may write on its own.
fn writable_researcher(
    model: &Arc<FnModel>,
    recorder: &Arc<Recorder>,
    sender: AgentProfile,
) -> AgentHarness {
    AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, recorder))
        .peer(Peer::agent("researcher", "Writes.", &research()))
        .agent(sender)
        .agent(AgentProfile::new("research").mandate(Mandate::new("researcher")))
}

/// The envelope is read-only, the write never runs, and the researcher
/// reads the refusal.
fn assert_read_only(rt: &SqliteRuntime, model: &FnModel, recorder: &Recorder) {
    let received = peer_messages(rt, &research());
    assert_eq!(received.len(), 1, "one delivery");
    assert!(received[0].mandate.read_only, "the envelope is read-only");
    assert!(recorder.runs().is_empty(), "the write tool never runs");
    let results = peer_tool_results(model);
    assert!(
        results.iter().any(|result| result.contains("only read")),
        "{results:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_sender_cannot_write_through_a_durable_peer() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = writer_model(ask_researcher);
    let sender = AgentProfile::new("triage")
        .read_only()
        .mandate(Mandate::new("triage").peers(["researcher"]));
    let mut rt = runtime(&db, writable_researcher(&model, &recorder, sender));
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    assert_read_only(&rt, &model, &recorder);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_heartbeat_tick_cannot_write_through_a_durable_peer() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = writer_model(ask_researcher);
    // The tick has no allow_actions, so it is read-only.
    let sender = AgentProfile::new("triage")
        .heartbeat(AgentHeartbeat::every(Duration::from_secs(60)))
        .mandate(Mandate::new("triage").peers(["researcher"]));
    let mut rt = runtime(&db, writable_researcher(&model, &recorder, sender));
    start(&mut rt, &research()).await;
    let begin = Utc::now();
    sqlite::send(&mut rt, &triage(), &AgentOp::Control(Control::Resume), None).unwrap();
    drive_at(&mut rt, &triage(), begin).await;
    // The tick is due at 60 seconds. It sends to the researcher and waits.
    drive_at(&mut rt, &triage(), begin + chrono::Duration::seconds(90)).await;
    sqlite::drive(&mut rt, &research()).await.unwrap();
    drive_at(&mut rt, &triage(), begin + chrono::Duration::seconds(91)).await;

    assert_eq!(state(&rt, &triage()).await.ticks, 1);
    assert_read_only(&rt, &model, &recorder);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_followup_segment_cannot_write_through_a_durable_peer() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = writer_model(ask_on_wake);
    // The follow-up has no allow_actions, so it is read-only.
    let sender = AgentProfile::new("triage")
        .followups(Followups::new(Duration::from_secs(3_600)))
        .mandate(Mandate::new("triage").peers(["researcher"]));
    let mut rt = runtime(&db, writable_researcher(&model, &recorder, sender));
    start(&mut rt, &research()).await;
    let begin = Utc::now();
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    drive_at(&mut rt, &triage(), begin).await;
    let woken = begin + chrono::Duration::minutes(3);
    drive_at(&mut rt, &triage(), woken).await;
    sqlite::drive(&mut rt, &research()).await.unwrap();
    drive_at(&mut rt, &triage(), woken + chrono::Duration::seconds(1)).await;

    assert_eq!(state(&rt, &triage()).await.chain, 1, "the follow-up ran");
    assert_read_only(&rt, &model, &recorder);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_inline_child_of_a_read_only_sender_cannot_write_through_a_durable_peer() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = writer_model(|request| {
        let results = tool_results(request);
        match first_user(request).as_str() {
            "go" if results.is_empty() => call(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "helper", "task": "ask the researcher"}),
            ),
            "ask the researcher" if results.is_empty() => ask_researcher(request),
            _ => text("done"),
        }
    });
    let sender = AgentProfile::new("triage")
        .read_only()
        .mandate(Mandate::new("triage").peers(["helper", "researcher"]));
    // The helper runs inline. Its own mandate is not read-only.
    let harness = writable_researcher(&model, &recorder, sender)
        .peer(Peer::new("helper", "Asks the researcher.").peers(["researcher"]));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    assert_read_only(&rt, &model, &recorder);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fan_out_child_of_a_read_only_sender_cannot_write_through_a_durable_peer() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = writer_model(|request| {
        let results = tool_results(request);
        match first_user(request).as_str() {
            // Two delegations in one turn run as two child runs.
            "go" if results.is_empty() => ChatResponse {
                content: vec![
                    ContentPart::ToolCall {
                        id: "d1".to_owned(),
                        name: DELEGATE_TOOL.to_owned(),
                        arguments: json!({"agent": "helper", "task": "ask the researcher"}),
                    },
                    ContentPart::ToolCall {
                        id: "d2".to_owned(),
                        name: DELEGATE_TOOL.to_owned(),
                        arguments: json!({"agent": "hotels", "task": "find hotels"}),
                    },
                ],
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::new(1, 1),
            },
            "ask the researcher" if results.is_empty() => ask_researcher(request),
            _ => text("done"),
        }
    });
    let sender = AgentProfile::new("triage")
        .read_only()
        .mandate(Mandate::new("triage").peers(["helper", "hotels", "researcher"]))
        .parallel_children(2);
    let harness = writable_researcher(&model, &recorder, sender)
        .peer(Peer::new("helper", "Asks the researcher.").peers(["researcher"]))
        .peer(Peer::new("hotels", "Finds hotels."));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    // The helper child run sends to the researcher and waits on the reply.
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let ran_hotels = model
        .requests()
        .iter()
        .any(|request| first_user(request) == "find hotels");
    assert!(ran_hotels, "both children ran");
    assert_read_only(&rt, &model, &recorder);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_agent_loop_cannot_write_through_a_durable_peer() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = writer_model(ask_researcher);
    let harness = writable_researcher(&model, &recorder, AgentProfile::new("triage"));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    let task = AgentTask::new("go")
        .read_only()
        .mandate(Mandate::new("triage").peers(["researcher"]));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    let ended = rt.run_until_blocked(exec).await.unwrap();

    assert!(matches!(ended, RunState::Completed(_)), "{ended:?}");
    assert_read_only(&rt, &model, &recorder);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_woken_agent_loop_segment_cannot_write_through_a_durable_peer() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = writer_model(ask_on_wake);
    let harness = writable_researcher(&model, &recorder, AgentProfile::new("triage"));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    // The task is writable. Its follow-up has no allow_actions.
    let task = AgentTask::new("go")
        .followups(Followups::new(Duration::from_secs(3_600)))
        .mandate(Mandate::new("triage").peers(["researcher"]));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let begin = Utc::now();
    rt.run_until_blocked_as_of(exec, begin).await.unwrap();
    let woken = begin + chrono::Duration::minutes(3);
    rt.run_until_blocked_as_of(exec, woken).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    let ended = rt
        .run_until_blocked_as_of(exec, woken + chrono::Duration::seconds(1))
        .await
        .unwrap();

    assert!(matches!(ended, RunState::Completed(_)), "{ended:?}");
    assert_read_only(&rt, &model, &recorder);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_receiver_keeps_the_next_durable_hop_read_only() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = FnModel::new(|request| {
        let results = tool_results(request);
        if first_user(request).starts_with("[A message from the agent \"researcher\"]") {
            return if results.is_empty() {
                call("w1", "write", json!({}))
            } else {
                text(&format!("write said: {}", results[0]))
            };
        }
        if from_peer(request) {
            return if results.is_empty() {
                call(
                    "d2",
                    DELEGATE_TOOL,
                    json!({"agent": "critic", "task": "write it"}),
                )
            } else {
                text("passed on")
            };
        }
        ask_researcher(request)
    });
    // Triage is writable. The researcher profile is read-only. The critic
    // may write on its own.
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .peer(Peer::agent("researcher", "Passes work on.", &research()).peers(["critic"]))
        .peer(Peer::agent("critic", "Writes.", &critic()))
        .agent(
            AgentProfile::new("triage")
                .mandate(Mandate::new("triage").peers(["researcher", "critic"])),
        )
        .agent(
            AgentProfile::new("research")
                .read_only()
                .mandate(Mandate::new("researcher").peers(["critic"])),
        )
        .agent(AgentProfile::new("critic").mandate(Mandate::new("critic")));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    start(&mut rt, &critic()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("go"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &critic()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let received = peer_messages(&rt, &critic());
    assert_eq!(received.len(), 1, "one delivery");
    assert!(received[0].mandate.read_only, "the second hop is read-only");
    assert!(recorder.runs().is_empty(), "the write tool never runs");
    let refused = model.requests().iter().any(|request| {
        first_user(request).starts_with("[A message from the agent \"researcher\"]")
            && tool_results(request)
                .iter()
                .any(|result| result.contains("only read"))
    });
    assert!(refused, "the critic reads the refusal");
}

/// A durable answer of 9 000 characters. It ends with a tail, so a cut
/// shows.
fn long_answer() -> String {
    format!("{}TAIL", "x".repeat(8_996))
}

/// Triage asks the durable researcher once, and the researcher answers
/// with [`long_answer`].
fn long_mesh_model() -> Arc<FnModel> {
    FnModel::new(|request| {
        if from_peer(request) {
            return text(&long_answer());
        }
        if tool_results(request).is_empty() {
            call(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "find the answer"}),
            )
        } else {
            text("done")
        }
    })
}

/// Run [`long_mesh_model`] with the durable triage agent. Returns the
/// delegation result that triage read.
async fn ask_with_limit(limit: usize) -> Value {
    let (_dir, db) = fresh_db();
    let model = long_mesh_model();
    let harness = mesh_harness(model.clone()).tool_output_limit(limit);
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("ask"), None).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();
    let last = model
        .requests()
        .into_iter()
        .rfind(|request| !from_peer(request) && !tool_results(request).is_empty())
        .expect("triage read the reply");
    serde_json::from_str(&tool_results(&last)[0]).unwrap()
}

// Issue #2481: `tool_output_limit` governs a durable answer.
#[tokio::test(flavor = "multi_thread")]
async fn the_harness_limit_governs_a_durable_answer() {
    let result = ask_with_limit(20_000).await;
    let answer = result["answer"].as_str().unwrap_or_default();
    assert!(
        answer == long_answer(),
        "{} characters, whole: {}",
        answer.chars().count(),
        answer.ends_with("TAIL")
    );
    assert_eq!(result["truncated"], false);
}

// Issue #2481: a cut durable answer is marked for the model.
#[tokio::test(flavor = "multi_thread")]
async fn a_cut_durable_answer_is_marked() {
    let result = ask_with_limit(100).await;
    assert_eq!(result["answer"], format!("{}…[truncated]", "x".repeat(100)));
    assert_eq!(result["truncated"], true);
}

// Issue #2481: an `agent_loop` run that asks a durable peer flags a cut in
// its report. A durable sender keeps no records, so this run shows it.
#[tokio::test(flavor = "multi_thread")]
async fn a_one_shot_run_flags_a_cut_durable_answer() {
    for (limit, cut) in [(100, true), (20_000, false)] {
        let (_dir, db) = fresh_db();
        let model = long_mesh_model();
        let mut rt = runtime(&db, mesh_harness(model.clone()).tool_output_limit(limit));
        start(&mut rt, &research()).await;
        let task = AgentTask::new("go").mandate(Mandate::new("triage").peers(["researcher"]));
        let exec = sqlite::start(&mut rt, &task).unwrap();
        rt.run_until_blocked(exec).await.unwrap();
        sqlite::drive(&mut rt, &research()).await.unwrap();
        let ended = report(rt.run_until_blocked(exec).await.unwrap());

        assert_eq!(ended.delegations.len(), 1, "limit {limit}");
        assert_eq!(ended.delegations[0].answer_truncated, cut, "limit {limit}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delegation_to_a_durable_peer_names_why_it_ran_alone() {
    // Issue #2483: a durable peer never joins a batch. The record says so.
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, mesh_harness(mesh_model()));
    start(&mut rt, &research()).await;
    let task = AgentTask::new("what is it?")
        .mandate(Mandate::new("triage").peers(["researcher"]))
        .parallel_children(2);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    let report = common::report(rt.run_until_blocked(exec).await.unwrap());

    assert!(report.text.contains("The answer is 42."), "{}", report.text);
    let serials: Vec<_> = report.delegations.iter().map(|r| r.serial).collect();
    assert_eq!(
        serials,
        [Some(autumn_harvest_agent::SerialReason::DurablePeer)]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gated_delegation_to_a_durable_peer_names_gated() {
    // Issue #2483: a person can edit the target of a gated call, so the
    // reason names the gate, not the peer.
    use autumn_harvest_agent::{Approval, Rule, ToolRules};

    let (_dir, db) = fresh_db();
    let harness = mesh_harness(mesh_model())
        .policy(Arc::new(ToolRules::new().tool(DELEGATE_TOOL, Rule::Ask)));
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    let task = AgentTask::new("what is it?")
        .mandate(Mandate::new("triage").peers(["researcher"]))
        .parallel_children(2);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the delegation waits for a person");
    };
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    let report = common::report(rt.run_until_blocked(exec).await.unwrap());

    assert!(report.text.contains("The answer is 42."), "{}", report.text);
    let serials: Vec<_> = report.delegations.iter().map(|r| r.serial).collect();
    assert_eq!(serials, [Some(autumn_harvest_agent::SerialReason::Gated)]);
}

/// A one-shot run that asks the durable researcher, with a deadline of 60
/// seconds. Each call to it can run in a batch of 2.
fn one_shot_ask() -> AgentTask {
    AgentTask::new("what is it?")
        .mandate(Mandate::new("triage").peers(["researcher"]))
        .parallel_children(2)
}

/// The mesh with a reply deadline of 60 seconds.
fn deadline_harness(model: Arc<dyn AgentModel>) -> AgentHarness {
    AgentHarness::new(model)
        .peer(
            Peer::agent("researcher", "Finds facts.", &research())
                .reply_timeout(Duration::from_secs(60)),
        )
        .agent(AgentProfile::new("research").mandate(Mandate::new("researcher")))
}

/// The first tool result of `report`, as JSON.
fn first_result(report: &autumn_harvest_agent::AgentReport) -> Value {
    let results = common::tool_results(report);
    serde_json::from_str(results.first().expect("the model read a result")).unwrap()
}

// Issue #2484: a timeout is a named verdict, and the record keeps the key
// of the message. The receiver still applies the message once.
#[tokio::test(flavor = "multi_thread")]
async fn a_durable_peer_timeout_is_a_named_verdict_with_the_message_key() {
    use autumn_harvest_agent::{Refusal, SerialReason};

    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, deadline_harness(mesh_model()));
    start(&mut rt, &research()).await;
    let exec = sqlite::start(&mut rt, &one_shot_ask()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    // The researcher does not run. The deadline passes.
    let later = Utc::now() + chrono::Duration::seconds(120);
    let ended = report(rt.run_until_blocked_as_of(exec, later).await.unwrap());

    let [record] = ended.delegations.as_slice() else {
        panic!("one record: {:?}", ended.delegations);
    };
    assert_eq!(record.refusal, Some(Refusal::ReplyTimeout));
    assert_eq!(record.stop, None);
    assert_eq!(record.run_id.as_deref(), Some(research().as_str()));
    // The message went out, so the call ran alone as a durable peer.
    assert_eq!(record.serial, Some(SerialReason::DurablePeer));
    let run_id = ended.run_id.clone().expect("a run id");
    let key = format!("{run_id}:1");
    assert_eq!(record.idempotency_key.as_deref(), Some(key.as_str()));

    let result = first_result(&ended);
    assert_eq!(result["verdict"], "reply_timeout", "{result}");
    let error = result["error"].as_str().unwrap_or_default();
    assert!(error.contains("60s") && error.contains("twice"), "{error}");
    // The model never sees the key.
    assert!(!common::tool_results(&ended)[0].contains(&key));

    // The receiver applies the late message once, under the same key. A
    // caller finds it by the key before it asks again.
    assert!(!state(&rt, &research()).await.inbound.applied_key(&key));
    sqlite::drive(&mut rt, &research()).await.unwrap();
    let received = peer_messages(&rt, &research());
    assert_eq!(received.len(), 1);
    assert_eq!(format!("{}:{}", received[0].from, received[0].seq), key);
    assert!(state(&rt, &research()).await.inbound.applied_key(&key));
}

// Issue #2484: a rejected reply is a named verdict with its reason.
#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_reply_is_a_named_verdict_with_its_reason() {
    use autumn_harvest_agent::Refusal;

    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, deadline_harness(mesh_model()));
    start(&mut rt, &research()).await;
    let exec = sqlite::start(&mut rt, &one_shot_ask()).unwrap();
    let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the run waits on the reply");
    };
    assert!(signal.starts_with("harvest.promise:"), "{signal}");
    let rejected = json!({"outcome": "rejected", "error": "the operator said no"});
    rt.send_signal(exec, &signal, rejected).unwrap();
    let ended = report(rt.run_until_blocked(exec).await.unwrap());

    let [record] = ended.delegations.as_slice() else {
        panic!("one record: {:?}", ended.delegations);
    };
    assert_eq!(record.refusal, Some(Refusal::ReplyRejected));
    let run_id = ended.run_id.clone().expect("a run id");
    assert_eq!(record.idempotency_key, Some(format!("{run_id}:1")));
    assert_eq!(record.run_id.as_deref(), Some(research().as_str()));
    let result = first_result(&ended);
    assert_eq!(result["verdict"], "reply_rejected", "{result}");
    let error = result["error"].as_str().unwrap_or_default();
    assert!(error.contains("the operator said no"), "{error}");
}

// Issue #2484: an answer keeps the key of its message. A refusal before the
// send keeps none, because no message went out.
#[tokio::test(flavor = "multi_thread")]
async fn only_a_sent_message_puts_a_key_on_its_record() {
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, mesh_harness(mesh_model()));
    start(&mut rt, &research()).await;
    let exec = sqlite::start(&mut rt, &one_shot_ask()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    let answered = report(rt.run_until_blocked(exec).await.unwrap());
    let run_id = answered.run_id.clone().expect("a run id");
    assert_eq!(answered.delegations.len(), 1);
    assert_eq!(answered.delegations[0].refusal, None);
    assert_eq!(
        answered.delegations[0].idempotency_key,
        Some(format!("{run_id}:1"))
    );

    // The researcher never started, so the send is refused.
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, mesh_harness(mesh_model()));
    let exec = sqlite::start(&mut rt, &one_shot_ask()).unwrap();
    let refused = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(refused.delegations.len(), 1);
    assert_eq!(
        refused.delegations[0].refusal,
        Some(autumn_harvest_agent::Refusal::PeerUnavailable)
    );
    assert_eq!(refused.delegations[0].idempotency_key, None);
    // A refused delegation did not run, so it has no serial reason.
    assert_eq!(refused.delegations[0].serial, None);
}

// Issue #2484: the patch gate keeps the old text for an old history. Only a
// sent message asks the gate.
#[cfg(feature = "eval")]
#[tokio::test(flavor = "multi_thread")]
async fn the_reply_verdict_gate_keeps_the_old_text_on_replay() {
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
    use autumn_harvest_agent::workflow::agent_loop_info;

    const MARKER: &str = "patch:agent_peer_reply_verdict";
    let is_marker = |event: &WorkflowEvent| matches!(event, WorkflowEvent::MarkerRecorded { name, .. } if name == MARKER);
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, deadline_harness(mesh_model()));
    start(&mut rt, &research()).await;
    let exec = sqlite::start(&mut rt, &one_shot_ask()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let later = Utc::now() + chrono::Duration::seconds(120);
    report(rt.run_until_blocked_as_of(exec, later).await.unwrap());
    let events = rt.load_history(exec).unwrap();
    assert_eq!(events.iter().filter(|event| is_marker(event)).count(), 1);
    let replay = WorkflowReplayer::new()
        .register(vec![agent_loop_info()])
        .with_execution_id(exec)
        .replay_from_events(events.clone())
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );

    // With no marker, replay sends the old text. This history holds the
    // new text, so replay names the mismatch.
    let old: Vec<WorkflowEvent> = events
        .into_iter()
        .filter(|event| !is_marker(event))
        .collect();
    let replay = WorkflowReplayer::new()
        .register(vec![agent_loop_info()])
        .with_execution_id(exec)
        .replay_from_events(old)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::NonDeterminismDetected { .. }),
        "{replay}"
    );
    let ReplayStatus::NonDeterminismDetected { actual, .. } = &replay.status else {
        panic!("{replay}");
    };
    // The replayed model input holds the old text.
    let input = actual
        .find("input=")
        .map(|at| &actual[at + "input=".len()..actual.len() - 1])
        .expect("the mismatch names the input");
    let input: Value = serde_json::from_str(input).expect("the input is JSON");
    let result = input["messages"][1]["content"][0]["tool_result"]["content"]
        .as_str()
        .expect("a tool result");
    assert!(result.contains("gave no answer"), "{result}");
    assert!(!result.contains("reply_timeout"), "{result}");

    // A refusal before the send records no marker.
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, mesh_harness(mesh_model()));
    let exec = sqlite::start(&mut rt, &one_shot_ask()).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());
    let events = rt.load_history(exec).unwrap();
    assert!(!events.iter().any(is_marker));
}

// Issue #2484: a receiver that refuses the message got it, so the record
// keeps the key. The refusal is not unanswered, so it has no serial reason.
#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_by_the_receiver_keeps_the_key() {
    use autumn_harvest_agent::Refusal;

    let (_dir, db) = fresh_db();
    let strong = FnModel::new(|_| text("never"));
    let harness = AgentHarness::new(mesh_model())
        .model_named("strong", strong.clone())
        .peer(Peer::agent("researcher", "Finds facts.", &research()))
        .agent(
            AgentProfile::new("research")
                .model("strong")
                .mandate(Mandate::new("researcher")),
        );
    let mut rt = runtime(&db, harness);
    start(&mut rt, &research()).await;
    let task = AgentTask::new("what is it?")
        .mandate(
            Mandate::new("triage")
                .peers(["researcher"])
                .models([autumn_harvest_agent::DEFAULT_MODEL]),
        )
        .parallel_children(2);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    sqlite::drive(&mut rt, &research()).await.unwrap();
    let ended = report(rt.run_until_blocked(exec).await.unwrap());

    let [record] = ended.delegations.as_slice() else {
        panic!("one record: {:?}", ended.delegations);
    };
    assert_eq!(record.refusal, Some(Refusal::ModelForbidden));
    let run_id = ended.run_id.clone().expect("a run id");
    assert_eq!(record.idempotency_key, Some(format!("{run_id}:1")));
    assert_eq!(record.serial, None);
    assert!(
        strong.requests().is_empty(),
        "the forbidden model never runs"
    );
}

// Issue #2484: a settlement that holds no reply is a rejected reply, not a
// failed run. The message went out, so the record keeps its key.
#[tokio::test(flavor = "multi_thread")]
async fn a_settlement_that_holds_no_reply_is_a_rejected_reply() {
    use autumn_harvest_agent::Refusal;

    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, deadline_harness(mesh_model()));
    start(&mut rt, &research()).await;
    let exec = sqlite::start(&mut rt, &one_shot_ask()).unwrap();
    let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the run waits on the reply");
    };
    let resolved = json!({"outcome": "resolved", "value": {"stop": 7}});
    rt.send_signal(exec, &signal, resolved).unwrap();
    let ended = report(rt.run_until_blocked(exec).await.unwrap());

    let [record] = ended.delegations.as_slice() else {
        panic!("one record: {:?}", ended.delegations);
    };
    assert_eq!(record.refusal, Some(Refusal::ReplyRejected));
    assert!(record.idempotency_key.is_some());
    let result = first_result(&ended);
    assert_eq!(result["verdict"], "reply_rejected", "{result}");
    let error = result["error"].as_str().unwrap_or_default();
    assert!(error.contains("does not decode"), "{error}");
}

// Issue #2484: an interrupt of a durable sender that waits for a reply is a
// named verdict, and the op ends. The history replays.
#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_wait_for_a_reply_is_a_named_verdict() {
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
    use autumn_harvest_agent::{Urgency, agent_info};

    let (_dir, db) = fresh_db();
    let model = mesh_model();
    let mut rt = runtime(&db, mesh_harness(model.clone()));
    start(&mut rt, &research()).await;
    sqlite::send(&mut rt, &triage(), &AgentOp::message("what is it?"), None).unwrap();
    let waiting = sqlite::drive(&mut rt, &triage()).await.unwrap();
    assert!(
        matches!(&waiting, Some(RunState::WaitingSignal(name)) if name.starts_with("harvest.promise:")),
        "{waiting:?}"
    );
    let turns = model.requests().len();
    let stop = AgentOp::urgent("stop", "m-2", Urgency::Interrupt);
    sqlite::send(&mut rt, &triage(), &stop, Some("m-2")).unwrap();
    sqlite::drive(&mut rt, &triage()).await.unwrap();

    let sender = state(&rt, &triage()).await;
    let results: Vec<Value> = sender
        .transcript
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => serde_json::from_str(content).ok(),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0]["verdict"], "reply_interrupted", "{results:?}");
    let error = results[0]["error"].as_str().unwrap_or_default();
    assert!(error.contains("twice"), "{error}");
    // The interrupt ends the op. Each later turn answers the interrupt.
    let later = &model.requests()[turns..];
    assert_ne!(later.len(), 0, "the interrupt op ran");
    assert!(
        later
            .iter()
            .all(|request| last_user(request).contains("stop")),
        "the op ended at the interrupt"
    );

    // The run ends, so the replay can reach its end.
    let exec = sqlite::current_run(&rt, &triage()).unwrap().unwrap();
    sqlite::delete(&mut rt, &triage(), None).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let replay = WorkflowReplayer::new()
        .register(vec![agent_info()])
        .with_workflow_id(triage().as_str())
        .replay_from_events(rt.load_history(exec).unwrap())
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}
