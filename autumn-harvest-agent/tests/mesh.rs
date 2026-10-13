//! Agent mesh primitives on the SQLite backend: peers, delegation, mandates,
//! budgets, flow rules and replay.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::{
    AgentHarness, AgentStop, AgentTask, Approval, ChatRole, ContentPart, FlowRules, FnTool,
    Mandate, ModelTurn, ModelTurnRequest, Peer, Recheck, Refusal, Rule, TokenUsage, ToolCall,
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
    for key in ["agent", "allowed_tools", "flow_labels"] {
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

// AC2
#[tokio::test(flavor = "multi_thread")]
async fn a_mandate_reserves_the_delegate_name_with_no_known_peer() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        delegate("d1", "ghost", "haunt", 1),
        answer("alone", 1),
    ]);
    let harness = AgentHarness::new(model.clone()).tool(recorded_tool(
        DELEGATE_TOOL,
        ToolEffect::ReadOnly,
        &recorder,
    ));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &planner(&["ghost"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert!(
        !offered(&model.requests()[0]).contains(DELEGATE_TOOL),
        "the app tool of that name stays hidden"
    );
    assert_eq!(
        recorder.runs(),
        Vec::<Value>::new(),
        "the app tool never runs"
    );
    assert_eq!(report.delegations.len(), 1);
    assert_eq!(report.delegations[0].refusal, Some(Refusal::UnknownPeer));
}

/// Run a planner with a mandate that names only the missing peer `ghost`.
/// Returns the report and the model.
async fn delegate_to_ghost(
    mandate: Mandate,
    flow: Option<FlowRules>,
) -> (autumn_harvest_agent::AgentReport, Arc<ScriptedModel>) {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        delegate("d1", "ghost", "haunt", 1),
        answer("alone", 1),
    ]);
    let mut harness = AgentHarness::new(model.clone()).tool(recorded_tool(
        "lookup",
        ToolEffect::ReadOnly,
        &recorder,
    ));
    if let Some(flow) = flow {
        harness = harness.flow_rules(flow);
    }
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("go").mandate(mandate)).unwrap();
    (report(rt.run_until_blocked(exec).await.unwrap()), model)
}

// AC2, AC7
#[tokio::test(flavor = "multi_thread")]
async fn a_delegate_call_with_no_known_peer_gets_a_verdict_under_every_bound() {
    let allowlist = Mandate::new("planner").peers(["ghost"]).tools(["lookup"]);
    let rule_of_two = Some(FlowRules::rule_of_two());
    for (case, mandate, flow) in [
        ("tool allowlist", allowlist, None),
        (
            "flow rules",
            Mandate::new("planner").peers(["ghost"]),
            rule_of_two,
        ),
    ] {
        let (report, model) = delegate_to_ghost(mandate, flow).await;
        assert_eq!(report.stop, AgentStop::Completed, "{case}");
        assert!(
            !offered(&model.requests()[0]).contains(DELEGATE_TOOL),
            "{case}: no peer, so no delegate tool is offered"
        );
        assert_eq!(
            report.delegations.len(),
            1,
            "{case}: the refusal is recorded"
        );
        assert_eq!(
            report.delegations[0].refusal,
            Some(Refusal::UnknownPeer),
            "{case}"
        );
    }
}

// AC2
#[tokio::test(flavor = "multi_thread")]
async fn with_no_mandate_an_app_tool_named_delegate_runs() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", DELEGATE_TOOL, json!({"to": "bob"}))], 1),
        answer("handed over", 1),
    ]);
    let harness = AgentHarness::new(model.clone()).tool(recorded_tool(
        DELEGATE_TOOL,
        ToolEffect::ReadOnly,
        &recorder,
    ));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("hand it over")).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert!(offered(&model.requests()[0]).contains(DELEGATE_TOOL));
    assert_eq!(recorder.runs(), vec![json!({"to": "bob"})]);
    assert_eq!(report.delegations.len(), 0, "no delegation runs");
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

    // The parent spends exactly its cap, so no tokens are left for a child.
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
            labels: None,
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
    let history_ids = rt.load_history(exec).unwrap();
    let root = scheduled(&history_ids, "agent_model_turn")[0]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(report.delegations[1].run_id, Some(format!("{root}:d1")));
    assert_eq!(
        report.delegations[0].run_id,
        Some(format!("{root}:d2")),
        "the grandchild takes the next ordinal of the tree"
    );
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        scheduled(&history, "agent_discover_peers"),
        vec![json!(["critic", "researcher"]), json!(["critic"])],
        "one lookup per agent that may delegate"
    );
}

// AC9
#[tokio::test(flavor = "multi_thread")]
async fn the_report_counts_the_rounds_and_calls_of_the_whole_tree() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "dig", 1),
        delegate("d2", "critic", "check", 1),
        calls(&[("c1", "lookup", json!({}))], 1),
        answer("checked", 1),
        answer("dug", 1),
        answer("planned", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
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
    assert_eq!(recorder.runs().len(), 1);
    // One round and one call for each of the three agents.
    assert_eq!(report.steps_used, 3, "the report counts every agent");
    assert_eq!(report.tool_calls, 3, "the report counts every agent");
    let critic = &report.delegations[0];
    assert_eq!((critic.steps_used, critic.tool_calls), (1, 1));
    let researcher = &report.delegations[1];
    assert_eq!(
        (researcher.steps_used, researcher.tool_calls),
        (2, 2),
        "a record counts its whole subtree, as its usage does"
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
async fn one_approval_releases_one_send() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        calls(&[("c2", "send", json!({"to": "x"}))], 1),
        calls(&[("c3", "send", json!({"to": "y"}))], 1),
        answer("sent twice", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .flow_rules(FlowRules::rule_of_two());
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("triage")).unwrap();
    let RunState::WaitingSignal(first) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the first send waits for a person");
    };
    sqlite::decide(&mut rt, exec, &first, &Approval::Approve).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::WaitingSignal(second) = state else {
        panic!("the second send waits for a person too: {state:?}");
    };
    assert_ne!(first, second);
    assert_eq!(recorder.runs(), vec![json!({"to": "x"})]);
    sqlite::decide(&mut rt, exec, &second, &Approval::Approve).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(
        recorder.runs(),
        vec![json!({"to": "x"}), json!({"to": "y"})]
    );
}

// AC10
#[tokio::test(flavor = "multi_thread")]
async fn until_approved_one_approval_releases_the_later_sends() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        calls(&[("c2", "send", json!({"to": "x"}))], 1),
        calls(&[("c3", "send", json!({"to": "y"}))], 1),
        answer("sent twice", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .flow_rules(FlowRules::rule_of_two().with_recheck(Recheck::UntilApproved));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("triage")).unwrap();
    let RunState::WaitingSignal(first) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the send that completes the set waits for a person");
    };
    sqlite::decide(&mut rt, exec, &first, &Approval::Approve).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(
        report.stop,
        AgentStop::Completed,
        "the second send needs no approval"
    );
    assert_eq!(
        recorder.runs(),
        vec![json!({"to": "x"}), json!({"to": "y"})]
    );
}

// AC10: a send in the same turn as a rejected send still waits.
#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_send_releases_no_send_of_its_turn() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        calls(
            &[
                ("c2", "send", json!({"to": "x"})),
                ("c3", "send", json!({"to": "y"})),
            ],
            1,
        ),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .flow_rules(FlowRules::rule_of_two().with_recheck(Recheck::UntilApproved));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("triage")).unwrap();
    let RunState::WaitingSignal(first) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the first send waits for a person");
    };
    sqlite::decide(
        &mut rt,
        exec,
        &first,
        &Approval::Reject {
            reason: "no".into(),
        },
    )
    .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(_)),
        "the second send of the turn waits too: {state:?}"
    );
    assert_eq!(recorder.runs(), Vec::<serde_json::Value>::new());
}

// AC10: labels carried in from another run release nothing.
#[tokio::test(flavor = "multi_thread")]
async fn carried_labels_release_no_send() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "send", json!({"to": "x"}))], 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .flow_rules(FlowRules::rule_of_two().with_recheck(Recheck::UntilApproved));
    let mut rt = runtime(&db, harness);

    let task = AgentTask::new("triage").flow_labels([
        labels::UNTRUSTED_INPUT,
        labels::PRIVATE_DATA,
        "effect:external",
    ]);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(_)),
        "no person approved a send in this tree: {state:?}"
    );
    assert_eq!(recorder.runs(), Vec::<serde_json::Value>::new());
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

// AC10: a release spans the run tree.
#[tokio::test(flavor = "multi_thread")]
async fn an_approval_in_a_child_releases_the_rule_for_its_parent() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        delegate("d1", "courier", "send a reply", 1),
        calls(&[("c2", "send", json!({"to": "x"}))], 1),
        answer("sent", 1),
        calls(&[("c3", "send", json!({"to": "y"}))], 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .peer(Peer::new("courier", "Sends mail.").tools(["send"]))
        .flow_rules(FlowRules::rule_of_two().with_recheck(Recheck::UntilApproved));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &planner(&["courier"])).unwrap();
    let RunState::WaitingSignal(child) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the child send waits for a person");
    };
    sqlite::decide(&mut rt, exec, &child, &Approval::Approve).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed, "the parent send runs");
    assert_eq!(
        recorder.runs(),
        vec![json!({"to": "x"}), json!({"to": "y"})]
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
        flow_labels: BTreeSet::new(),
        flow_released: autumn_harvest_agent::Released::new(),
    };
    let plain = AgentHarness::new(model.clone()).tool(inbox());
    let turn = plain.model_turn(request.clone()).await.unwrap();
    assert_eq!(turn.labels, Vec::<BTreeSet<String>>::new());
    let ruled = AgentHarness::new(model)
        .tool(inbox())
        .flow_rules(FlowRules::rule_of_two());
    let turn = ruled.model_turn(request).await.unwrap();
    assert_eq!(
        turn.labels,
        vec![BTreeSet::from([
            "effect:read_only".to_owned(),
            "private_data".to_owned(),
            "untrusted_input".to_owned(),
        ])]
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
async fn a_restart_inside_a_child_resends_nothing_and_looks_up_nothing() {
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
    let child = &model.requests()[2];
    assert!(
        last_tool_results(&child.messages)[0].contains("only read"),
        "the read-only rule refused the write"
    );
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
        Refusal::InvalidArguments,
    ] {
        assert!(
            guide.contains(&format!("`{}`", refusal.code())),
            "docs/agent-mesh.md names {}",
            refusal.code()
        );
    }
}

// Review fixes: same-turn delegation, labels across runs, wire hardening.

// AC10: a call after a delegation in the same turn does not run on a
// decision that predates the child labels.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_after_a_delegation_in_the_same_turn_is_decided_again() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(
            &[
                (
                    "d1",
                    DELEGATE_TOOL,
                    json!({"agent": "researcher", "task": "read the inbox"}),
                ),
                ("c2", "send", json!({"to": "x"})),
            ],
            1,
        ),
        calls(&[("c1", "inbox", json!({}))], 1),
        answer("a stranger wrote", 1),
        // The parent sends again. Now the rule of two sees the child labels.
        calls(&[("c3", "send", json!({"to": "x"}))], 1),
        answer("sent", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .peer(researcher().tools(["inbox"]))
        .flow_rules(FlowRules::rule_of_two());
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::WaitingSignal(signal) = state else {
        panic!("the second send waits for a person: {state:?}");
    };
    assert_eq!(
        recorder.runs(),
        Vec::<Value>::new(),
        "the first send never ran"
    );
    let results = last_tool_results(&model.requests()[3].messages);
    assert!(results[1].contains("again"), "{results:?}");
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(recorder.runs().len(), 1);
}

// AC10: the labels of a run carry into the run that continues it.
#[tokio::test(flavor = "multi_thread")]
async fn labels_carry_into_the_run_that_continues_the_transcript() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        answer("a stranger wrote", 1),
        calls(&[("c2", "send", json!({}))], 1),
        answer("sent", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &recorder))
        .flow_rules(FlowRules::rule_of_two());
    let mut rt = runtime(&db, harness);

    let first = sqlite::start(&mut rt, &AgentTask::new("read")).unwrap();
    let first = report(rt.run_until_blocked(first).await.unwrap());
    assert!(first.flow_labels.contains(labels::UNTRUSTED_INPUT));
    assert!(first.flow_labels.contains(labels::PRIVATE_DATA));

    let next = AgentTask::new("now send it")
        .history(first.messages.clone())
        .flow_labels(first.flow_labels.clone());
    let second = sqlite::start(&mut rt, &next).unwrap();
    let state = rt.run_until_blocked(second).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(_)),
        "the send in the next run waits for a person: {state:?}"
    );
    assert_eq!(recorder.runs(), Vec::<Value>::new());
}

// AC10: with flow rules, a call to an unknown tool is denied. A worker with
// another tool list cannot run it with no labels.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_tool_is_denied_under_flow_rules() {
    let model = ScriptedModel::new(vec![calls(&[("c1", "ghost", json!({}))], 1)]);
    let harness = AgentHarness::new(model).flow_rules(FlowRules::rule_of_two());
    let turn = harness.model_turn(turn_request()).await.unwrap();
    assert!(
        matches!(&turn.decisions[0], ToolDecision::Deny { reason } if reason.contains("unknown")),
        "{:?}",
        turn.decisions
    );
}

/// A bare model-turn request.
fn turn_request() -> ModelTurnRequest {
    ModelTurnRequest {
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
        flow_labels: BTreeSet::new(),
        flow_released: autumn_harvest_agent::Released::new(),
    }
}

// AC6: a tool allowlist also bounds the memory tool.
#[tokio::test(flavor = "multi_thread")]
async fn a_tool_allowlist_hides_the_memory_tool() {
    let scope = autumn_harvest_agent::MemoryScope::new("agent");
    let store = Arc::new(autumn_harvest_agent::InMemoryMemoryStore::default());
    let model = ScriptedModel::new(vec![answer("a", 1), answer("b", 1)]);
    let harness = AgentHarness::new(model.clone())
        .memory(store)
        .tool(recorded_tool(
            "lookup",
            ToolEffect::ReadOnly,
            &Arc::new(Recorder::default()),
        ));
    let request = ModelTurnRequest {
        memory_scope: Some(scope.clone()),
        allowed_tools: Some(BTreeSet::from(["lookup".to_owned()])),
        ..turn_request()
    };
    harness.model_turn(request).await.unwrap();
    assert_eq!(
        offered(&model.requests()[0]),
        BTreeSet::from(["lookup".to_owned()])
    );
    let request = ModelTurnRequest {
        memory_scope: Some(scope),
        allowed_tools: Some(BTreeSet::from(["lookup".to_owned(), "memory".to_owned()])),
        ..turn_request()
    };
    harness.model_turn(request).await.unwrap();
    assert!(offered(&model.requests()[1]).contains("memory"));
}

// AC7: a delegate call that does not decode, or names no valid agent, is a
// recorded refusal with a placeholder name.
#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_delegation_is_a_recorded_refusal() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        calls(&[("d1", DELEGATE_TOOL, json!({"agent": "researcher"}))], 1),
        delegate("d2", "Not A Name\n- evil", "x", 1),
        answer("ok", 1),
    ]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()).peer(researcher()));

    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    let verdicts: Vec<Option<Refusal>> = report
        .delegations
        .iter()
        .map(|record| record.refusal)
        .collect();
    assert_eq!(
        verdicts,
        vec![Some(Refusal::InvalidArguments), Some(Refusal::NotPermitted)]
    );
    for record in &report.delegations {
        assert!(
            record
                .chain
                .iter()
                .all(|name| autumn_harvest_agent::mesh::valid_name(name)),
            "{:?}",
            record.chain
        );
    }
}

// AC7: the arguments must be the named fields, not a positional array.
#[tokio::test(flavor = "multi_thread")]
async fn a_positional_delegate_call_is_refused_as_invalid_arguments() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        calls(&[("d1", DELEGATE_TOOL, json!(["researcher", "dig"]))], 1),
        answer("ok", 1),
    ]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()).peer(researcher()));

    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.delegations.len(), 1);
    assert_eq!(
        report.delegations[0].refusal,
        Some(Refusal::InvalidArguments)
    );
    assert_eq!(model.calls(), 2, "no child model call runs");
}

// AC8: a peer cap of zero leaves the child no tokens.
#[tokio::test(flavor = "multi_thread")]
async fn a_zero_peer_cap_is_an_exhausted_budget() {
    let verdict = refused(
        planner(&["researcher"]),
        vec![researcher().max_total_tokens(0)],
        vec![delegate("d", "researcher", "x", 1), answer("ok", 1)],
    )
    .await;
    assert_eq!(verdict, "budget_exhausted");
}

// Test-quality gaps from review.

/// A wall-clock instant `seconds` from now, to fire a durable timer early.
fn later(seconds: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now() + chrono::Duration::seconds(seconds)
}

// AC3, AC6: a child that a woken follow-up segment starts is read-only.
#[tokio::test(flavor = "multi_thread")]
async fn a_child_in_a_woken_segment_is_read_only() {
    use autumn_harvest_agent::Followups;
    use autumn_harvest_agent::followup::FOLLOWUP_TOOL;

    let (_dir, db) = fresh_db();
    let writes = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(
            &[(
                "f",
                FOLLOWUP_TOOL,
                json!({"prompt": "write it", "delay_minutes": 1}),
            )],
            1,
        ),
        answer("Later.", 1),
        delegate("d1", "researcher", "write the note", 1),
        calls(&[("w", "write", json!({}))], 1),
        answer("could not write", 1),
        answer("Done.", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &writes))
        .peer(researcher());
    let mut rt = runtime(&db, harness);

    let task =
        planner(&["researcher"]).followups(Followups::new(std::time::Duration::from_secs(600)));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let _ = rt.run_until_blocked(exec).await.unwrap();
    let run = report(rt.run_until_blocked_as_of(exec, later(120)).await.unwrap());

    assert_eq!(run.text, "Done.");
    assert_eq!(writes.runs(), Vec::<Value>::new());
    let child = &model.requests()[4];
    assert!(
        last_tool_results(&child.messages)[0].contains("only read"),
        "{:?}",
        last_tool_results(&child.messages)
    );
}

/// A harness with `inbox`, a counted `send`, a gate on `inbox`, and a rule
/// that denies a send after untrusted input.
fn gated_inbox(model: Arc<ScriptedModel>, sends: &Arc<Recorder>) -> AgentHarness {
    AgentHarness::new(model)
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, sends))
        .policy(Arc::new(ToolRules::new().tool("inbox", Rule::Ask)))
        .flow_rules(FlowRules::new().forbid(
            [labels::UNTRUSTED_INPUT, "effect:external"],
            Rule::Deny("no send after untrusted input".into()),
        ))
}

// AC10: an approved or an edited call adds its labels.
#[tokio::test(flavor = "multi_thread")]
async fn an_approved_or_edited_call_adds_its_labels() {
    for approval in [
        Approval::Approve,
        Approval::Edit {
            arguments: json!({"folder": "spam"}),
        },
    ] {
        let (_dir, db) = fresh_db();
        let sends = Arc::new(Recorder::default());
        let model = ScriptedModel::new(vec![
            calls(&[("c1", "inbox", json!({}))], 1),
            calls(&[("c2", "send", json!({}))], 1),
            answer("done", 1),
        ]);
        let mut rt = runtime(&db, gated_inbox(model, &sends));
        let exec = sqlite::start(&mut rt, &AgentTask::new("triage")).unwrap();
        let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
            panic!("the inbox read waits for a person");
        };
        sqlite::decide(&mut rt, exec, &signal, &approval).unwrap();
        let report = report(rt.run_until_blocked(exec).await.unwrap());
        assert_eq!(report.stop, AgentStop::Completed);
        assert_eq!(sends.runs(), Vec::<Value>::new(), "{approval:?}");
        assert!(report.flow_labels.contains(labels::UNTRUSTED_INPUT));
    }
}

// AC10: in one turn, the labels of an earlier call count for a later call.
#[tokio::test(flavor = "multi_thread")]
async fn labels_count_within_one_turn() {
    let (_dir, db) = fresh_db();
    let sends = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({})), ("c2", "send", json!({}))], 1),
        answer("sent", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &sends))
        .flow_rules(FlowRules::rule_of_two());
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("triage")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(&state, RunState::WaitingSignal(name) if name.ends_with(":1:c2")),
        "{state:?}"
    );
}

// AC6, AC10: a mandate deny keeps the labels aligned, and a flow deny beats
// a policy ask.
#[tokio::test(flavor = "multi_thread")]
async fn labels_stay_aligned_and_a_flow_deny_beats_an_ask() {
    let sends = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![calls(
        &[
            ("c1", "wipe", json!({})),
            ("c2", "inbox", json!({})),
            ("c3", "send", json!({})),
        ],
        1,
    )]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("wipe", ToolEffect::Write, &sends))
        .tool(inbox())
        .tool(recorded_tool("send", ToolEffect::External, &sends))
        .policy(Arc::new(ToolRules::new().tool("send", Rule::Ask)))
        .flow_rules(FlowRules::new().forbid(
            [labels::UNTRUSTED_INPUT, "effect:external"],
            Rule::Deny("no send after untrusted input".into()),
        ));
    let request = ModelTurnRequest {
        allowed_tools: Some(BTreeSet::from(["inbox".to_owned(), "send".to_owned()])),
        ..turn_request()
    };
    let turn = harness.model_turn(request).await.unwrap();
    assert_eq!(turn.labels.len(), 3);
    assert_eq!(
        turn.labels[0],
        BTreeSet::new(),
        "the mandate deny has no labels"
    );
    assert!(turn.labels[1].contains(labels::UNTRUSTED_INPUT));
    assert!(matches!(turn.decisions[0], ToolDecision::Deny { .. }));
    assert_eq!(turn.decisions[1], ToolDecision::Allow);
    assert!(
        matches!(&turn.decisions[2], ToolDecision::Deny { reason } if reason.contains("untrusted")),
        "{:?}",
        turn.decisions[2]
    );
}

// AC8: the peer cap binds a child when the parent has no cap.
#[tokio::test(flavor = "multi_thread")]
async fn the_peer_cap_binds_when_the_parent_has_none() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "dig", 1),
        calls(&[("c1", "lookup", json!({}))], 25),
        answer("parent goes on", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .peer(researcher().max_total_tokens(20));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.delegations[0].stop, Some(AgentStop::TokensExhausted));
    assert_eq!(recorder.runs(), Vec::<Value>::new());
}

// AC8: the parent remainder binds a peer with no cap.
#[tokio::test(flavor = "multi_thread")]
async fn the_parent_remainder_binds_an_uncapped_peer() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "dig", 30),
        calls(&[("c1", "lookup", json!({}))], 80),
        answer("unreachable", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool(
            "lookup",
            ToolEffect::ReadOnly,
            &Arc::new(Recorder::default()),
        ))
        .peer(researcher());
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &planner(&["researcher"]).max_total_tokens(100)).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::TokensExhausted);
    assert_eq!(model.calls(), 2);
    assert_eq!(report.delegations[0].stop, Some(AgentStop::TokensExhausted));
}

// AC6: a tool allowlist keeps the delegate tool, and the workflow passes the
// allowlist to the tool-call activity.
#[tokio::test(flavor = "multi_thread")]
async fn an_allowlist_keeps_delegate_and_reaches_the_tool_call() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "look", 1),
        calls(&[("c1", "lookup", json!({"q": 1}))], 1),
        answer("found", 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .tool(recorded_tool("wipe", ToolEffect::Write, &recorder))
        .peer(researcher());
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("go").mandate(
        Mandate::new("planner")
            .peers(["researcher"])
            .tools(["lookup"]),
    );
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(
        offered(&model.requests()[0]),
        BTreeSet::from([DELEGATE_TOOL.to_owned(), "lookup".to_owned()])
    );
    assert_eq!(recorder.runs(), vec![json!({"q": 1})]);
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        scheduled(&history, "agent_tool_call")[0]["allowed_tools"],
        json!(["lookup"]),
        "the child inherits the root allowlist"
    );
}

// AC3, AC9: a child stops at its own step bound.
#[tokio::test(flavor = "multi_thread")]
async fn a_child_stops_at_its_own_step_bound() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "dig", 1),
        calls(&[("c1", "lookup", json!({"n": 1}))], 1),
        calls(&[("c2", "lookup", json!({"n": 2}))], 1),
        answer("parent", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool(
            "lookup",
            ToolEffect::ReadOnly,
            &Arc::new(Recorder::default()),
        ))
        .peer(researcher().max_steps(1));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    let record = &report.delegations[0];
    assert_eq!(record.stop, Some(AgentStop::StepsExhausted));
    assert_eq!(record.steps_used, 1);
    assert_eq!(record.tool_calls, 1);
}

// AC3: a child gets no memory and no follow-ups, even when the parent has
// both.
#[tokio::test(flavor = "multi_thread")]
async fn a_child_gets_no_memory_and_no_followups() {
    use autumn_harvest_agent::{Followups, InMemoryMemoryStore, MemoryScope};

    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "dig", 1),
        answer("dug", 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .memory(Arc::new(InMemoryMemoryStore::default()))
        .peer(researcher());
    let mut rt = runtime(&db, harness);
    let task = planner(&["researcher"])
        .memory(MemoryScope::new("agent"))
        .followups(Followups::new(std::time::Duration::from_secs(600)));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());

    let requests = model.requests();
    assert!(offered(&requests[0]).contains("memory"));
    let child = offered(&requests[1]);
    assert!(!child.contains("memory"), "{child:?}");
    assert!(!child.contains("schedule_followup"), "{child:?}");
    let history = rt.load_history(exec).unwrap();
    assert_eq!(scheduled(&history, "agent_memory_snapshot").len(), 1);
}

// AC9: child run ids extend the root run id with the tree ordinal.
#[tokio::test(flavor = "multi_thread")]
async fn child_run_ids_extend_the_root_run_id() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "one", 1),
        answer("first", 1),
        delegate("d2", "researcher", "two", 1),
        answer("second", 1),
        answer("done", 1),
    ]);
    let mut rt = runtime(&db, AgentHarness::new(model).peer(researcher()));
    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    let history = rt.load_history(exec).unwrap();
    let root = scheduled(&history, "agent_model_turn")[0]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let ids: Vec<Option<String>> = report
        .delegations
        .iter()
        .map(|record| record.run_id.clone())
        .collect();
    assert_eq!(
        ids,
        vec![Some(format!("{root}:d1")), Some(format!("{root}:d2"))]
    );
}

// AC12: a parent round after a child round takes the next tree round.
#[tokio::test(flavor = "multi_thread")]
async fn a_parent_round_after_a_child_takes_the_next_round() {
    let (_dir, db) = fresh_db();
    let writes = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "write", 1),
        calls(&[("c1", "write", json!({"who": "child"}))], 1),
        answer("child wrote", 1),
        calls(&[("c1", "write", json!({"who": "parent"}))], 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("write", ToolEffect::Write, &writes))
        .policy(Arc::new(ToolRules::ask_before_acting()))
        .peer(researcher());
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    let mut names = Vec::new();
    loop {
        match rt.run_until_blocked(exec).await.unwrap() {
            RunState::WaitingSignal(name) => {
                sqlite::decide(&mut rt, exec, &name, &Approval::Approve).unwrap();
                names.push(name);
            }
            RunState::Completed(_) => break,
            other => panic!("unexpected state {other:?}"),
        }
    }
    assert_eq!(names, vec!["tool_approval:1:0:c1", "tool_approval:2:0:c1"]);
    assert_eq!(writes.runs().len(), 2);
}

// AC5: an invalid task mandate fails the run before any model call.
#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_task_mandate_fails_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("never", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()));
    let task = AgentTask::new("go").mandate(Mandate::new("Bad Name"));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(&state, RunState::Failed(err) if err.contains("mandate")),
        "{state:?}"
    );
    assert_eq!(model.calls(), 0);
}

// AC1: discovery filters, deduplicates and sorts the roster.
#[test]
fn discovery_filters_and_orders_the_roster() {
    let harness = AgentHarness::new(ScriptedModel::new(Vec::new()))
        .peer(Peer::new("writer", "W."))
        .peer(Peer::new("Bad Name", "B."))
        .peer(Peer::new("researcher", "old"))
        .peer(Peer::new("researcher", "new"));
    let found = harness.discover_peers(&[
        "researcher".to_owned(),
        "writer".to_owned(),
        "Bad Name".to_owned(),
        "ghost".to_owned(),
    ]);
    let names: Vec<(&str, &str)> = found
        .iter()
        .map(|peer| (peer.name.as_str(), peer.description.as_str()))
        .collect();
    assert_eq!(names, vec![("researcher", "new"), ("writer", "W.")]);
}

// AC3: the child answer is cut to the tool output limit.
#[tokio::test(flavor = "multi_thread")]
async fn a_long_child_answer_is_cut() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        delegate("d1", "researcher", "dig", 1),
        answer(&"x".repeat(9_000), 1),
        answer("done", 1),
    ]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()).peer(researcher()));
    let exec = sqlite::start(&mut rt, &planner(&["researcher"])).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());
    let result: Value =
        serde_json::from_str(&last_tool_results(&model.requests()[2].messages)[0]).unwrap();
    let answer = result["answer"].as_str().unwrap();
    assert!(
        answer.ends_with("[truncated]"),
        "{}",
        &answer[answer.len() - 20..]
    );
    assert!(answer.chars().count() < 8_100);
}

/// A tracing subscriber that keeps the name and the fields of each span.
#[derive(Clone, Default)]
struct Spans(Arc<std::sync::Mutex<Vec<(String, String, String)>>>);

struct Fields<'a>(&'a mut Vec<(String, String)>);

impl tracing::field::Visit for Fields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.push((field.name().to_owned(), value.to_owned()));
    }
}

impl Spans {
    fn keep(&self, span: &str, record: &dyn Fn(&mut Fields<'_>)) {
        let mut fields = Vec::new();
        record(&mut Fields(&mut fields));
        let mut kept = self.0.lock().unwrap();
        for (name, value) in fields {
            kept.push((span.to_owned(), name, value));
        }
    }
}

impl tracing::Subscriber for Spans {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::always()
    }

    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let name = span.metadata().name();
        self.keep(name, &|fields| span.record(fields));
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        self.keep("agent_model_turn", &|fields| values.record(fields));
    }

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, _: &tracing::Event<'_>) {}

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

// AC14: the model-turn span names the acting agent.
#[tokio::test]
async fn the_model_turn_span_names_the_agent() {
    let spans = Spans::default();
    let _guard = tracing::subscriber::set_default(spans.clone());
    let harness = AgentHarness::new(ScriptedModel::new(vec![answer("hi", 1)]));
    let request = ModelTurnRequest {
        agent: Some("researcher".into()),
        ..turn_request()
    };
    harness.model_turn(request).await.unwrap();
    let kept = spans.0.lock().unwrap().clone();
    assert!(
        kept.contains(&(
            "agent_model_turn".to_owned(),
            "gen_ai.agent.name".to_owned(),
            "researcher".to_owned()
        )),
        "{kept:?}"
    );
}

// AC10: the tool-call worker refuses a tool whose labels grew after the
// decision, for example in a rolling deploy.
#[tokio::test(flavor = "multi_thread")]
async fn the_tool_call_activity_refuses_a_tool_whose_labels_grew() {
    let sends = Arc::new(Recorder::default());
    let harness = AgentHarness::new(ScriptedModel::new(Vec::new())).tool(recorded_tool(
        "send",
        ToolEffect::External,
        &sends,
    ));
    let request = |labels: &[&str]| ToolCallRequest {
        run_id: "r".into(),
        session_id: None,
        step: 0,
        call: ToolCall {
            id: "c".into(),
            name: "send".into(),
            arguments: json!({}),
        },
        memory_scope: None,
        read_only: false,
        allowed_tools: None,
        labels: Some(labels.iter().map(|label| (*label).to_owned()).collect()),
    };
    // The deciding worker saw a read-only tool.
    let grown = harness
        .tool_call(request(&["effect:read_only"]))
        .await
        .unwrap();
    assert!(grown.is_error);
    assert!(
        grown.content.contains("labels changed"),
        "{}",
        grown.content
    );
    assert_eq!(sends.runs(), Vec::<Value>::new());
    let same = harness
        .tool_call(request(&["effect:external"]))
        .await
        .unwrap();
    assert!(!same.is_error, "{}", same.content);
    assert_eq!(sends.runs().len(), 1);
}

// AC10: with flow rules, the workflow passes the recorded labels to the
// tool-call activity.
#[tokio::test(flavor = "multi_thread")]
async fn the_workflow_passes_the_recorded_labels_to_the_tool_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        answer("read", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(inbox())
        .flow_rules(FlowRules::rule_of_two());
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("read")).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        scheduled(&history, "agent_tool_call")[0]["labels"],
        json!(["effect:read_only", "private_data", "untrusted_input"])
    );
}
