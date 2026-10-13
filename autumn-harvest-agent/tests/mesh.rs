//! Agent mesh primitives on the SQLite backend: peers, delegation, mandates,
//! budgets, flow rules and replay.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::{
    AgentHarness, AgentStop, AgentTask, Approval, ChatRole, ContentPart, DELEGATE_TOOL, FlowRules,
    FnTool, Mandate, ModelTurn, ModelTurnRequest, Peer, Refusal, Rule, TokenUsage, ToolCall,
    ToolCallRequest, ToolDecision, ToolEffect, ToolRules, sqlite,
};
use autumn_harvest_sqlite::RunState;
use common::{Recorder, Reply, ScriptedModel, answer, calls, recorded_tool, report, runtime};
use serde_json::{Value, json};

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// A turn that delegates `task` to `agent`.
fn delegate(id: &str, agent: &str, task: &str, tokens: u32) -> Reply {
    calls(
        &[(id, DELEGATE_TOOL, json!({"agent": agent, "task": task}))],
        tokens,
    )
}

/// The inputs of every recorded activity named `name`, in order.
fn scheduled(history: &[WorkflowEvent], name: &str) -> Vec<Value> {
    history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled {
                name: scheduled,
                input,
                ..
            } if scheduled == name => Some(input.clone()),
            _ => None,
        })
        .collect()
}

/// The text of the tool results in the last message of `messages`.
fn last_tool_results(messages: &[autumn_harvest_agent::ChatMessage]) -> Vec<String> {
    messages
        .last()
        .map(|message| {
            message
                .content
                .iter()
                .filter_map(|part| match part {
                    ContentPart::ToolResult { content, .. } => Some(content.clone()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The text parts of one message, joined.
fn text_of(message: &autumn_harvest_agent::ChatMessage) -> String {
    message
        .content
        .iter()
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// The tool names a model request offered.
fn offered(request: &autumn_harvest_agent::ChatRequest) -> BTreeSet<String> {
    request.tools.iter().map(|tool| tool.name.clone()).collect()
}

/// A planner that may delegate to `peers`.
fn planner(peers: &[&str]) -> AgentTask {
    AgentTask::new("plan the trip")
        .system("You plan.")
        .mandate(Mandate::new("planner").peers(peers.iter().copied()))
}

fn researcher() -> Peer {
    Peer::new("researcher", "Finds facts on the web.").system("You research.")
}

// AC1, AC2, AC3, AC14
#[tokio::test(flavor = "multi_thread")]
async fn a_delegation_runs_the_peer_and_returns_only_its_answer() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "find the ferry times", 5),
        answer("The ferry leaves at 9.", 7),
        answer("Take the 9 o'clock ferry.", 3),
    ]);
    let harness = AgentHarness::new(model.clone())
        .peer(researcher())
        .peer(Peer::new("writer", "Writes prose."));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.text, "Take the 9 o'clock ferry.");
    assert_eq!(model.calls(), 3);
    let requests = model.requests();

    // The planner sees the delegate tool, with the named peer only.
    let tool = requests[0]
        .tools
        .iter()
        .find(|tool| tool.name == DELEGATE_TOOL)
        .expect("the planner gets the delegate tool");
    assert_eq!(
        tool.input_schema["properties"]["agent"]["enum"],
        json!(["researcher"])
    );
    assert!(tool.description.contains("Finds facts on the web."));
    assert!(!tool.description.contains("Writes prose."));

    // The child sees its own prompt and the task, nothing of the parent.
    let child = &requests[1];
    assert_eq!(child.messages.len(), 2, "{:?}", child.messages);
    assert_eq!(child.messages[0].role, ChatRole::System);
    assert_eq!(text_of(&child.messages[0]), "You research.");
    assert_eq!(child.messages[1].role, ChatRole::User);
    assert_eq!(text_of(&child.messages[1]), "find the ferry times");
    assert!(
        !offered(child).contains(DELEGATE_TOOL),
        "a child with no peers gets no delegate tool"
    );

    // Only the child answer returns to the parent.
    let results = last_tool_results(&requests[2].messages);
    assert_eq!(results.len(), 1);
    let result: Value = serde_json::from_str(&results[0]).unwrap();
    assert_eq!(result["agent"], "researcher");
    assert_eq!(result["stop"], "completed");
    assert_eq!(result["answer"], "The ferry leaves at 9.");

    // The tree usage rolls up.
    assert_eq!(report.usage.input_tokens, 15);
    assert_eq!(report.delegations.len(), 1);
    let record = &report.delegations[0];
    assert_eq!(record.chain, vec!["planner", "researcher"]);
    assert_eq!(record.stop, Some(AgentStop::Completed));
    assert_eq!(record.refusal, None);
    assert_eq!(record.usage.input_tokens, 7);
    let run_id = record.run_id.clone().expect("the child has a run id");
    assert!(run_id.ends_with(":d1"), "{run_id}");

    // One recorded lookup, for the planner only, of the named peer only.
    let history = rt.load_history(exec).unwrap();
    let lookups = scheduled(&history, "agent_discover_peers");
    assert_eq!(lookups, vec![json!(["researcher"])]);

    // Each model turn records the acting agent.
    let agents: Vec<Value> = scheduled(&history, "agent_model_turn")
        .into_iter()
        .map(|input| input["agent"].clone())
        .collect();
    assert_eq!(
        agents,
        vec![json!("planner"), json!("researcher"), json!("planner")]
    );
}

// AC2
#[tokio::test(flavor = "multi_thread")]
async fn a_run_with_no_mandate_records_no_mesh_fields() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("hi", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()).peer(researcher()));

    let exec = sqlite::start(&mut rt, &AgentTask::new("hello")).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());

    assert!(!offered(&model.requests()[0]).contains(DELEGATE_TOOL));
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        scheduled(&history, "agent_discover_peers"),
        Vec::<Value>::new()
    );
    let turn = &scheduled(&history, "agent_model_turn")[0];
    for key in ["agent", "allowed_tools", "exercised"] {
        assert!(turn.get(key).is_none(), "{key}");
    }
}

// AC2
#[tokio::test(flavor = "multi_thread")]
async fn a_mandate_with_no_known_peer_offers_no_delegate_tool() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("alone", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()));

    let exec = sqlite::start(&mut rt, &planner(&["ghost"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert!(!offered(&model.requests()[0]).contains(DELEGATE_TOOL));
}

/// Run `task` with `replies` and the default roster. Returns the verdict
/// code that the first delegate result names.
async fn refused(task: AgentTask, harness_peers: Vec<Peer>, replies: Vec<Reply>) -> Value {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(replies);
    let mut harness = AgentHarness::new(model.clone());
    for peer in harness_peers {
        harness = harness.peer(peer);
    }
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    let refusal = report
        .delegations
        .iter()
        .find_map(|record| record.refusal)
        .expect("one delegation is refused");
    assert!(
        report
            .delegations
            .iter()
            .filter(|record| record.refusal.is_some())
            .all(|record| record.run_id.is_none() && record.stop.is_none())
    );
    let request = model.requests().pop().unwrap();
    let result = last_tool_results(&request.messages)
        .into_iter()
        .map(|text| serde_json::from_str::<Value>(&text).unwrap())
        .find(|value| value.get("verdict").is_some())
        .expect("the model reads a verdict");
    assert_eq!(result["verdict"], json!(refusal.code()));
    result["verdict"].clone()
}

// AC7
#[tokio::test(flavor = "multi_thread")]
async fn each_refusal_names_a_distinct_verdict() {
    let ok = || answer("fine", 1);

    // The roster lost `ghost`, but the mandate still names it.
    let unknown = refused(
        planner(&["ghost", "researcher"]),
        vec![researcher()],
        vec![delegate("d", "ghost", "x", 1), ok()],
    )
    .await;
    assert_eq!(unknown, "unknown_peer");

    let not_permitted = refused(
        planner(&["researcher"]),
        vec![researcher(), Peer::new("writer", "W.")],
        vec![delegate("d", "writer", "x", 1), ok()],
    )
    .await;
    assert_eq!(not_permitted, "not_permitted");

    let cycle = refused(
        planner(&["planner", "researcher"]),
        vec![researcher(), Peer::new("planner", "P.")],
        vec![delegate("d", "planner", "x", 1), ok()],
    )
    .await;
    assert_eq!(cycle, "cycle");

    let flat =
        AgentTask::new("go").mandate(Mandate::new("planner").peers(["researcher"]).max_depth(0));
    let depth = refused(
        flat,
        vec![researcher()],
        vec![delegate("d", "researcher", "x", 1), ok()],
    )
    .await;
    assert_eq!(depth, "depth_exceeded");

    let one = AgentTask::new("go").mandate(
        Mandate::new("planner")
            .peers(["researcher"])
            .max_delegations(1),
    );
    let fan_out = refused(
        one,
        vec![researcher()],
        vec![
            delegate("d1", "researcher", "x", 1),
            answer("first", 1),
            delegate("d2", "researcher", "y", 1),
            ok(),
        ],
    )
    .await;
    assert_eq!(fan_out, "fan_out_exhausted");

    let capped = planner(&["researcher"]).max_total_tokens(10);
    let budget = refused(
        capped,
        vec![researcher()],
        vec![delegate("d", "researcher", "x", 10), ok()],
    )
    .await;
    assert_eq!(budget, "budget_exhausted");
}

// AC6
#[tokio::test(flavor = "multi_thread")]
async fn a_child_cannot_run_a_tool_outside_its_mandate() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "look", 1),
        calls(&[("c1", "wipe", json!({}))], 1),
        answer("could not wipe", 1),
        answer("ok", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .tool(recorded_tool("wipe", ToolEffect::Write, &recorder))
        .peer(researcher().tools(["lookup"]));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert!(recorder.runs().is_empty(), "wipe never runs");
    let requests = model.requests();
    assert_eq!(
        offered(&requests[1]),
        BTreeSet::from(["lookup".to_owned()]),
        "the child sees only its tools"
    );
    let results = last_tool_results(&requests[2].messages);
    assert!(results[0].contains("mandate"), "{results:?}");

    // The decision denies the call.
    let history = rt.load_history(exec).unwrap();
    let turn = history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityCompleted { output, .. } => {
                serde_json::from_value::<ModelTurn>(output.clone()).ok()
            }
            _ => None,
        })
        .find(|turn| turn.calls().iter().any(|call| call.name == "wipe"))
        .expect("the child turn is recorded");
    assert!(matches!(turn.decisions[0], ToolDecision::Deny { .. }));
}

// AC6
#[tokio::test(flavor = "multi_thread")]
async fn the_mandate_also_bounds_the_root_agent() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "wipe", json!({}))], 1),
        answer("ok", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .tool(recorded_tool("wipe", ToolEffect::Write, &recorder));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("go").mandate(Mandate::new("solo").tools(["lookup"]));

    let exec = sqlite::start(&mut rt, &task).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(recorder.runs(), Vec::<Value>::new());
    assert!(!offered(&model.requests()[0]).contains("wipe"));
}

// AC6
#[tokio::test(flavor = "multi_thread")]
async fn the_tool_call_activity_checks_the_mandate_again() {
    let recorder = Arc::new(Recorder::default());
    let harness = AgentHarness::new(ScriptedModel::new(Vec::new())).tool(recorded_tool(
        "wipe",
        ToolEffect::Write,
        &recorder,
    ));
    let outcome = harness
        .tool_call(ToolCallRequest {
            run_id: "r:d1".into(),
            session_id: None,
            step: 0,
            call: ToolCall {
                id: "c".into(),
                name: "wipe".into(),
                arguments: json!({}),
            },
            memory_scope: None,
            read_only: false,
            allowed_tools: Some(BTreeSet::from(["lookup".to_owned()])),
        })
        .await
        .unwrap();
    assert!(outcome.is_error);
    assert!(outcome.content.contains("mandate"), "{}", outcome.content);
    assert_eq!(recorder.runs(), Vec::<Value>::new());
}

// AC8, AC9
#[tokio::test(flavor = "multi_thread")]
async fn the_child_cap_is_the_parent_remainder_and_the_parent_stops_at_its_cap() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "dig", 30),
        // The child spends 80 of its 70 tokens, so it stops.
        calls(&[("c1", "lookup", json!({}))], 80),
        // No call may follow: the tree spent 110 of 100.
        answer("unreachable", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .peer(researcher().max_total_tokens(1_000));
    let mut rt = runtime(&db, harness);

    let task = planner(&["researcher"]).max_total_tokens(100);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::TokensExhausted);
    assert_eq!(model.calls(), 2, "the parent makes no call past its cap");
    assert_eq!(report.usage.total(), 110);
    let record = &report.delegations[0];
    assert_eq!(record.stop, Some(AgentStop::TokensExhausted));
    assert_eq!(record.usage.total(), 80);
    assert!(recorder.runs().is_empty(), "the capped child runs no tool");
}

// AC8
#[tokio::test(flavor = "multi_thread")]
async fn the_peer_cap_wins_when_it_is_lower() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "dig", 1),
        calls(&[("c1", "lookup", json!({}))], 25),
        answer("parent goes on", 1),
    ]);
    let recorder = Arc::new(Recorder::default());
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .peer(researcher().max_total_tokens(20));
    let mut rt = runtime(&db, harness);

    let task = planner(&["researcher"]).max_total_tokens(1_000);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.delegations[0].stop, Some(AgentStop::TokensExhausted));
    let result: Value =
        serde_json::from_str(&last_tool_results(&model.requests()[2].messages)[0]).unwrap();
    assert_eq!(result["stop"], "tokens_exhausted");
}

// AC5, AC9
#[tokio::test(flavor = "multi_thread")]
async fn a_nested_delegation_keeps_the_chain_and_rolls_usage_up() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "dig", 1),
        delegate("d2", "critic", "check", 2),
        answer("checked", 4),
        answer("dug", 8),
        answer("planned", 16),
    ]);
    let harness = AgentHarness::new(model.clone())
        .peer(researcher().peers(["critic"]))
        .peer(Peer::new("critic", "Checks claims."));
    let mut rt = runtime(&db, harness);

    let task = AgentTask::new("go").mandate(
        Mandate::new("planner")
            .peers(["researcher", "critic"])
            .max_depth(2),
    );
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.usage.input_tokens, 31);
    let chains: Vec<Vec<String>> = report
        .delegations
        .iter()
        .map(|record| record.chain.clone())
        .collect();
    assert_eq!(
        chains,
        vec![
            vec!["planner", "researcher", "critic"],
            vec!["planner", "researcher"],
        ],
        "records list each delegation in the order it ended"
    );
    assert_eq!(report.delegations[0].usage.input_tokens, 4);
    assert_eq!(
        report.delegations[1].usage.input_tokens, 14,
        "a record counts its whole subtree"
    );
    assert_ne!(report.delegations[0].run_id, report.delegations[1].run_id);
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        scheduled(&history, "agent_discover_peers"),
        vec![json!(["critic", "researcher"]), json!(["critic"])],
        "one lookup per agent that may delegate"
    );
}

/// A read-only tool that returns content from outside, with private data.
fn inbox() -> Arc<dyn autumn_harvest_agent::Tool> {
    FnTool::new(
        "inbox",
        "Read mail.",
        json!({"type": "object"}),
        |_| async { Ok(json!("mail from a stranger")) },
    )
    .effect(ToolEffect::ReadOnly)
    .label(labels::UNTRUSTED_INPUT)
    .label(labels::PRIVATE_DATA)
    .shared()
}

// AC10
#[tokio::test(flavor = "multi_thread")]
async fn the_rule_of_two_gates_the_send_after_an_untrusted_read() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        calls(&[("c2", "send", json!({"to": "x"}))], 1),
        answer("sent", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .flow_rules(FlowRules::rule_of_two());
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("triage")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::WaitingSignal(signal) = state else {
        panic!("the send waits for a person: {state:?}");
    };
    assert_eq!(recorder.runs(), Vec::<Value>::new());
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(recorder.runs().len(), 1);
}

// AC10
#[tokio::test(flavor = "multi_thread")]
async fn with_no_untrusted_read_the_send_runs_at_once() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "send", json!({}))], 1),
        answer("sent", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .flow_rules(FlowRules::rule_of_two());
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("send it")).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(recorder.runs().len(), 1);
}

// AC10
#[tokio::test(flavor = "multi_thread")]
async fn labels_a_child_exercises_count_for_its_parent() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "read the inbox", 1),
        calls(&[("c1", "inbox", json!({}))], 1),
        answer("a stranger wrote", 1),
        calls(&[("c2", "send", json!({}))], 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .peer(researcher().tools(["inbox"]))
        .flow_rules(FlowRules::new().forbid(
            [labels::UNTRUSTED_INPUT, "effect:external"],
            Rule::Deny("no send after untrusted input".into()),
        ));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.delegations[0].stop, Some(AgentStop::Completed));
    assert!(recorder.runs().is_empty(), "the parent send is denied");
    let last = model.requests().pop().unwrap();
    let results = last_tool_results(&last.messages);
    assert!(
        results[0].contains("no send after untrusted input"),
        "{results:?}"
    );
}

// AC10
#[tokio::test(flavor = "multi_thread")]
async fn labels_a_parent_exercises_count_for_its_child() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        delegate("d1", "courier", "send a reply", 1),
        calls(&[("c2", "send", json!({}))], 1),
        answer("refused", 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .peer(Peer::new("courier", "Sends mail.").tools(["send"]))
        .flow_rules(FlowRules::new().forbid(
            [labels::UNTRUSTED_INPUT, "effect:external"],
            Rule::Deny("no send after untrusted input".into()),
        ));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &planner(&["courier"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.delegations[0].stop, Some(AgentStop::Completed));
    assert!(recorder.runs().is_empty(), "the child send is denied");
    let child_last = &model.requests()[3];
    let results = last_tool_results(&child_last.messages);
    assert!(
        results[0].contains("no send after untrusted input"),
        "{results:?}"
    );
}

// AC10
#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_call_adds_no_labels() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        calls(&[("c2", "send", json!({}))], 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .policy(Arc::new(ToolRules::new().tool("inbox", Rule::Ask)))
        .flow_rules(FlowRules::new().forbid(
            [labels::UNTRUSTED_INPUT, "effect:external"],
            Rule::Deny("no send after untrusted input".into()),
        ));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("triage")).unwrap();
    let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the inbox read waits for a person");
    };
    let reject = Approval::Reject {
        reason: "not now".into(),
    };
    sqlite::decide(&mut rt, exec, &signal, &reject).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(
        recorder.runs().len(),
        1,
        "the send runs: the read never ran"
    );
}

// AC10
#[tokio::test(flavor = "multi_thread")]
async fn a_turn_records_labels_only_with_flow_rules() {
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        calls(&[("c1", "inbox", json!({}))], 1),
    ]);
    let request = ModelTurnRequest {
        run_id: "r".into(),
        session_id: None,
        steps_used: 0,
        max_steps: 8,
        usage: TokenUsage::default(),
        messages: vec![autumn_harvest_agent::ChatMessage::text(
            ChatRole::User,
            "go",
        )],
        max_output_tokens: None,
        read_only: false,
        memory_scope: None,
        extra_tools: Vec::new(),
        tenant: None,
        agent: None,
        allowed_tools: None,
        exercised: BTreeSet::new(),
    };
    let plain = AgentHarness::new(model.clone()).tool(inbox());
    let turn = plain.model_turn(request.clone()).await.unwrap();
    assert_eq!(turn.labels, Vec::<Vec<String>>::new());
    let ruled = AgentHarness::new(model)
        .tool(inbox())
        .flow_rules(FlowRules::rule_of_two());
    let turn = ruled.model_turn(request).await.unwrap();
    assert_eq!(
        turn.labels,
        vec![vec![
            "effect:read_only".to_owned(),
            "private_data".to_owned(),
            "untrusted_input".to_owned(),
        ]]
    );
}

// AC12
#[tokio::test(flavor = "multi_thread")]
async fn a_gated_call_in_a_child_waits_on_a_tree_unique_name() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        // The parent and the child both use the call id `c1`.
        calls(
            &[
                ("c1", "write", json!({"who": "parent"})),
                (
                    "c2",
                    DELEGATE_TOOL,
                    json!({"agent": "researcher", "task": "write"}),
                ),
            ],
            1,
        ),
        calls(&[("c1", "write", json!({"who": "child"}))], 1),
        answer("child wrote", 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(ToolRules::ask_before_acting()))
        .peer(researcher());
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let RunState::WaitingSignal(first) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the parent write waits");
    };
    assert_eq!(first, "tool_approval:0:0:c1");
    sqlite::decide(&mut rt, exec, &first, &Approval::Approve).unwrap();
    let RunState::WaitingSignal(second) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the child write waits");
    };
    assert_eq!(second, "tool_approval:1:0:c1");
    assert_eq!(recorder.runs(), vec![json!({"who": "parent"})]);
    sqlite::decide(&mut rt, exec, &second, &Approval::Approve).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(
        recorder.runs(),
        vec![json!({"who": "parent"}), json!({"who": "child"})]
    );
    assert_eq!(report.delegations.len(), 1);
    assert_eq!(report.delegations[0].stop, Some(AgentStop::Completed));
    let history = rt.load_history(exec).unwrap();
    let run_ids: Vec<String> = scheduled(&history, "agent_tool_call")
        .iter()
        .map(|input| input["run_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(run_ids.len(), 2);
    assert!(!run_ids[0].ends_with(":d1"), "{run_ids:?}");
    assert!(
        run_ids[1].ends_with(":d1"),
        "the second write is the child's"
    );
}

// AC1, AC4
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_inside_a_child_repays_nothing_and_looks_up_nothing() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let before = ScriptedModel::new(vec![
        delegate("d1", "researcher", "write it", 1),
        calls(&[("c1", "write", json!({}))], 1),
    ]);
    let harness = AgentHarness::new(before.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(ToolRules::ask_before_acting()))
        .peer(researcher());
    let exec;
    let signal;
    {
        let mut rt = runtime(&db, harness);
        exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
        let RunState::WaitingSignal(name) = rt.run_until_blocked(exec).await.unwrap() else {
            panic!("the child write waits");
        };
        signal = name;
        // The runtime drops here, as a crash would.
    }
    assert_eq!(before.calls(), 2);

    // The new process has no `researcher` in its roster. Replay reads the
    // recorded lookup, so the run still finishes.
    let after = ScriptedModel::new(vec![answer("written", 1), answer("done", 1)]);
    let harness = AgentHarness::new(after.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(ToolRules::ask_before_acting()));
    let mut rt = runtime(&db, harness);
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(after.calls(), 2, "only the turns after the restart run");
    assert_eq!(recorder.runs().len(), 1);
    let history = rt.load_history(exec).unwrap();
    assert_eq!(scheduled(&history, "agent_discover_peers").len(), 1);
    assert_eq!(report.delegations[0].stop, Some(AgentStop::Completed));
}

// AC3
#[tokio::test(flavor = "multi_thread")]
async fn a_child_inherits_read_only_and_gets_no_memory() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "write", 1),
        calls(&[("c1", "write", json!({}))], 1),
        answer("refused", 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .peer(researcher().read_only());
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());

    assert!(recorder.runs().is_empty(), "a read-only peer cannot write");
}

// AC7
#[test]
fn every_refusal_code_is_documented() {
    let guide = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../docs/agent-mesh.md"
    ))
    .expect("the user guide exists");
    for refusal in [
        Refusal::UnknownPeer,
        Refusal::NotPermitted,
        Refusal::Cycle,
        Refusal::DepthExceeded,
        Refusal::FanOutExhausted,
        Refusal::BudgetExhausted,
        Refusal::InvalidMandate,
    ] {
        assert!(
            guide.contains(&format!("`{}`", refusal.code())),
            "docs/agent-mesh.md names {}",
            refusal.code()
        );
    }
}
