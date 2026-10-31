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
use common::{Recorder, ScriptedModel, TryFnModel, recorded_tool, runtime};
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
    let query = sqlite::state(rt, agent);
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
