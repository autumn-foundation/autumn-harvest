//! Durable peers: agents that message each other (issue #2269).
//!
//! The tests drive two agents on one SQLite runtime. A `delegate` call to a
//! durable peer sends a peer message and waits on a durable promise. The
//! peer settles the promise with its answer.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentState, BoxFuture,
    ChatRequest, ChatResponse, ChatRole, ContentPart, Control, FlowRules, FnTool, Mandate, Peer,
    PeerEnvelope, Rule, StopReason, TokenUsage, ToolEffect, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{Recorder, recorded_tool, runtime};
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

fn mesh_harness(model: Arc<FnModel>) -> AgentHarness {
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
