//! Concurrent inline children (issue #2270).
//!
//! A model turn with several `delegate` calls runs them at the same time when
//! the task allows it. Each child runs as a child `agent_loop` run, so each
//! child has its own history and replay cannot mix up their results. The
//! siblings split the token budget and the delegation count of the parent.
//!
//! The wall-time test needs workers that run activities at the same time, so
//! it lives in the Postgres suite of `examples/agent-mesh`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use autumn_harvest::dst::SeedPlan;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
use autumn_harvest::{always, sometimes};
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::workflow::agent_loop_info;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentReport, AgentStop, AgentTask, BoxFuture,
    ChatRequest, ChatResponse, ChatRole, ContentPart, FlowRules, FnTool, Mandate, ModelTurn,
    ModelTurnRequest, Peer, Rule, StopReason, TokenUsage, ToolDecision, ToolEffect, sqlite,
};
use autumn_harvest_sqlite::SqliteRuntime;
use common::{report, runtime};
use serde_json::{Value, json};

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

type Script = dyn Fn(&ChatRequest) -> ChatResponse + Send + Sync;

/// A model that answers each request with a function of the request.
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

fn text(reply: &str, tokens: u32) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(reply.to_owned())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(tokens, 0),
    }
}

fn tool_calls(list: &[(&str, &str, Value)], tokens: u32) -> ChatResponse {
    ChatResponse {
        content: list
            .iter()
            .map(|(id, name, arguments)| ContentPart::ToolCall {
                id: (*id).to_owned(),
                name: (*name).to_owned(),
                arguments: arguments.clone(),
            })
            .collect(),
        stop_reason: StopReason::ToolUse,
        usage: TokenUsage::new(tokens, 0),
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

/// The tool results of a request, in order.
fn results(request: &ChatRequest) -> Vec<String> {
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

const ROOT_INPUT: &str = "plan the trip";

/// Three delegations in one turn.
fn three_delegations(tokens: u32) -> ChatResponse {
    tool_calls(
        &[
            (
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "flights", "task": "find flights"}),
            ),
            (
                "d2",
                DELEGATE_TOOL,
                json!({"agent": "hotels", "task": "find hotels"}),
            ),
            (
                "d3",
                DELEGATE_TOOL,
                json!({"agent": "visas", "task": "check visas"}),
            ),
        ],
        tokens,
    )
}

/// The root delegates to three peers, and each peer answers with its task.
fn trip_model() -> Arc<FnModel> {
    FnModel::new(|request| {
        let input = first_user(request);
        if input == ROOT_INPUT {
            return if results(request).is_empty() {
                three_delegations(3)
            } else {
                text("trip planned", 3)
            };
        }
        text(&format!("done: {input}"), 2)
    })
}

fn trip_harness(model: Arc<FnModel>) -> AgentHarness {
    AgentHarness::new(model)
        .peer(Peer::new("flights", "Finds flights."))
        .peer(Peer::new("hotels", "Finds hotels."))
        .peer(Peer::new("visas", "Checks visas."))
}

fn trip_task(parallel: u32) -> AgentTask {
    AgentTask::new(ROOT_INPUT)
        .mandate(Mandate::new("planner").peers(["flights", "hotels", "visas"]))
        .parallel_children(parallel)
}

/// Run `task` to its end. Returns the execution and its report.
async fn run(
    rt: &mut SqliteRuntime,
    task: &AgentTask,
) -> (autumn_harvest_sqlite::ExecutionId, AgentReport) {
    let exec = sqlite::start(rt, task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    (exec, report(state))
}

fn children_started(rt: &SqliteRuntime, exec: autumn_harvest_sqlite::ExecutionId) -> Vec<Value> {
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::ChildWorkflowStarted { input, .. } => Some(input),
            _ => None,
        })
        .collect()
}

/// The answer of each delegate result of the root, by agent.
fn answers(model: &FnModel) -> Vec<(String, String)> {
    let last = model
        .requests()
        .into_iter()
        .rfind(|request| first_user(request) == ROOT_INPUT)
        .unwrap();
    results(&last)
        .iter()
        .map(|result| {
            let value: Value = serde_json::from_str(result).unwrap();
            (
                value["agent"].as_str().unwrap_or_default().to_owned(),
                value["answer"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fan_out_runs_each_child_as_a_child_run_and_merges_the_results() {
    let (_dir, db) = fresh_db();
    let model = trip_model();
    let mut rt = runtime(&db, trip_harness(model.clone()));
    let (exec, report) = run(&mut rt, &trip_task(3)).await;

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.text, "trip planned");
    assert_eq!(children_started(&rt, exec).len(), 3, "one child run each");
    assert_eq!(
        answers(&model),
        [
            ("flights".to_owned(), "done: find flights".to_owned()),
            ("hotels".to_owned(), "done: find hotels".to_owned()),
            ("visas".to_owned(), "done: check visas".to_owned()),
        ]
    );
    let chains: Vec<Vec<String>> = report.delegations.iter().map(|r| r.chain.clone()).collect();
    assert_eq!(
        chains,
        [
            vec!["planner".to_owned(), "flights".to_owned()],
            vec!["planner".to_owned(), "hotels".to_owned()],
            vec!["planner".to_owned(), "visas".to_owned()],
        ]
    );
    assert!(
        report
            .delegations
            .iter()
            .all(|r| r.run_id.is_some() && r.stop == Some(AgentStop::Completed))
    );
    // The parent counts each child: 3 + 3 for the root, 2 for each child.
    assert_eq!(report.usage.total(), 12);
    // The parent history replays as it ran.
    let replay = WorkflowReplayer::new()
        .register(vec![agent_loop_info()])
        .with_execution_id(exec)
        .replay_from_events(rt.load_history(exec).unwrap())
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_bound_the_children_run_one_after_another_inline() {
    let (_dir, db) = fresh_db();
    let model = trip_model();
    let mut rt = runtime(&db, trip_harness(model.clone()));
    let (exec, report) = run(&mut rt, &trip_task(0)).await;

    assert_eq!(report.text, "trip planned");
    assert!(children_started(&rt, exec).is_empty(), "no child runs");
    assert_eq!(answers(&model).len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_never_passes_the_bound() {
    let (_dir, db) = fresh_db();
    let model = trip_model();
    let mut rt = runtime(&db, trip_harness(model.clone()));
    let (exec, report) = run(&mut rt, &trip_task(2)).await;

    assert_eq!(report.text, "trip planned");
    // Two children run together. The third one is alone, so it runs inline.
    assert_eq!(children_started(&rt, exec).len(), 2);
    assert_eq!(answers(&model).len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn siblings_split_the_token_budget_and_never_exceed_it() {
    // The root spends 6 of its 36 tokens, so each of 3 children gets 10.
    // Each child turn costs 5 tokens and asks for one more tool round, for
    // ever. A run stops after the call that passes its cap, as a run with no
    // children does. Without the split, each child would get all 30.
    let (_dir, db) = fresh_db();
    let model = FnModel::new(|request| {
        let input = first_user(request);
        if input == ROOT_INPUT {
            return if results(request).is_empty() {
                three_delegations(6)
            } else {
                text("never: the budget is spent", 1)
            };
        }
        let round = results(request).len();
        tool_calls(&[(&format!("n{round}"), "note", json!({}))], 5)
    });
    let note = FnTool::new("note", "Notes.", json!({"type": "object"}), |_| async {
        Ok(json!("noted"))
    })
    .effect(ToolEffect::ReadOnly)
    .shared();
    let harness = trip_harness(model.clone()).tool(note);
    let mut rt = runtime(&db, harness);
    let task = trip_task(3).max_total_tokens(36);
    let (exec, report) = run(&mut rt, &task).await;

    let caps: Vec<Value> = children_started(&rt, exec)
        .iter()
        .map(|input| input["max_total_tokens"].clone())
        .collect();
    assert_eq!(
        caps,
        [json!(10), json!(10), json!(10)],
        "the caps sum to the rest"
    );
    for record in &report.delegations {
        assert_eq!(record.stop, Some(AgentStop::TokensExhausted));
        // One call of 5 tokens may pass the cap of 10, as in any run.
        assert_eq!(record.usage.total(), 15, "{record:?}");
    }
    // The root makes no call after the batch: it is over its cap.
    assert_eq!(report.stop, AgentStop::TokensExhausted);
    assert_eq!(report.usage.total(), 6 + 3 * 15);
    let root_turns = model
        .requests()
        .iter()
        .filter(|request| first_user(request) == ROOT_INPUT)
        .count();
    assert_eq!(root_turns, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn siblings_split_the_delegation_count() {
    let (_dir, db) = fresh_db();
    let model = trip_model();
    let mut rt = runtime(&db, trip_harness(model.clone()));
    // Five delegations: three for the siblings, two left to split.
    let task = AgentTask::new(ROOT_INPUT)
        .mandate(
            Mandate::new("planner")
                .peers(["flights", "hotels", "visas"])
                .max_delegations(5),
        )
        .parallel_children(3);
    let (exec, _report) = run(&mut rt, &task).await;

    let shares: Vec<Value> = children_started(&rt, exec)
        .iter()
        .map(|input| input["mandate"]["max_delegations"].clone())
        .collect();
    assert_eq!(shares, [json!(1), json!(1), json!(0)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_labels_of_every_sibling_reach_the_parent() {
    let (_dir, db) = fresh_db();
    let model = FnModel::new(|request| {
        let input = first_user(request);
        if input == ROOT_INPUT {
            return if results(request).is_empty() {
                three_delegations(1)
            } else {
                text("merged", 1)
            };
        }
        if results(request).is_empty() {
            let tool = input.replace(' ', "_");
            tool_calls(&[("t1", &tool, json!({}))], 1)
        } else {
            text("read", 1)
        }
    });
    let tool = |name: &'static str, label: &'static str| {
        FnTool::new(name, "Reads.", json!({"type": "object"}), |_| async {
            Ok(json!("data"))
        })
        .effect(ToolEffect::ReadOnly)
        .label(label)
        .shared()
    };
    // A rule that never fires turns the labels on.
    let rules = FlowRules::new().forbid(
        ["airline", "hotel", "government", labels::UNTRUSTED_INPUT],
        Rule::Deny("never".into()),
    );
    let harness = trip_harness(model.clone())
        .tool(tool("find_flights", "airline"))
        .tool(tool("find_hotels", "hotel"))
        .tool(tool("check_visas", "government"))
        .flow_rules(rules);
    let mut rt = runtime(&db, harness);
    let (_exec, report) = run(&mut rt, &trip_task(3)).await;

    let expected: BTreeSet<String> = ["airline", "hotel", "government"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert!(
        report.flow_labels.is_superset(&expected),
        "{:?}",
        report.flow_labels
    );
}

// ---------------------------------------------------------------------------
// The simulator: the children end in a seed-chosen order.
// ---------------------------------------------------------------------------

/// The root turn of the simulated parent.
fn sim_turn(content: Vec<ContentPart>, stop: StopReason) -> ModelTurn {
    let decisions = content
        .iter()
        .filter(|part| matches!(part, ContentPart::ToolCall { .. }))
        .map(|_| ToolDecision::Allow)
        .collect();
    ModelTurn {
        content,
        stop,
        usage: TokenUsage::new(1, 1),
        decisions,
        cache_hit: false,
        labels: Vec::new(),
        releases: Vec::new(),
        oversized: false,
        interruptible: Vec::new(),
    }
}

/// A child report whose text and label name the child task.
fn child_report(input: &Value) -> Value {
    let task: AgentTask = serde_json::from_value(input.clone()).unwrap();
    let mut labels = BTreeSet::new();
    labels.insert(task.input.replace(' ', "_"));
    json!(AgentReport {
        stop: AgentStop::Completed,
        text: format!("done: {}", task.input),
        steps_used: 1,
        tool_calls: 0,
        usage: TokenUsage::new(1, 1),
        messages: Vec::new(),
        followups: 0,
        followup_dropped: false,
        delegations: Vec::new(),
        flow_labels: labels,
        run_id: Some(format!("child-{}", task.input.replace(' ', "-"))),
    })
}

fn sim_env() -> WorkflowTestEnv {
    WorkflowTestEnv::new()
        .mock_activity("agent_discover_peers", |input| {
            let names: Vec<String> = serde_json::from_value(input).unwrap();
            let peers: Vec<Peer> = names
                .iter()
                .map(|name| Peer::new(name, "A peer."))
                .collect();
            Ok(serde_json::to_value(peers).unwrap())
        })
        .mock_activity("agent_model_turn", |input| {
            let request: ModelTurnRequest = serde_json::from_value(input).unwrap();
            let answered = request.messages.iter().any(|m| m.role == ChatRole::Tool);
            let turn = if answered {
                sim_turn(
                    vec![ContentPart::Text("merged".into())],
                    StopReason::EndTurn,
                )
            } else {
                sim_turn(three_delegations(1).content, StopReason::ToolUse)
            };
            Ok(serde_json::to_value(turn).unwrap())
        })
        .mock_child_workflow("agent_loop", |input| Ok(child_report(&input)))
}

/// Whether each delegate result of the run names its own agent.
fn results_match(run: &autumn_harvest::testing::sim::SimOutcome) -> bool {
    let Ok(output) = run.result() else {
        return false;
    };
    let report: AgentReport = serde_json::from_value(output.clone()).unwrap();
    let Some(tool) = report.messages.iter().find(|m| m.role == ChatRole::Tool) else {
        return false;
    };
    let expected = [
        ("flights", "done: find flights"),
        ("hotels", "done: find hotels"),
        ("visas", "done: check visas"),
    ];
    tool.content.len() == 3
        && tool
            .content
            .iter()
            .zip(expected)
            .all(|(part, (agent, answer))| match part {
                ContentPart::ToolResult { content, .. } => {
                    let value: Value = serde_json::from_str(content).unwrap_or_default();
                    value["agent"] == agent && value["answer"] == answer
                }
                _ => false,
            })
        && ["find_flights", "find_hotels", "check_visas"]
            .iter()
            .all(|label| report.flow_labels.contains(*label))
}

/// Whether the children ended in another order than they started.
fn out_of_order(run: &autumn_harvest::testing::sim::SimOutcome) -> bool {
    let mut started = Vec::new();
    let mut ended = Vec::new();
    for event in run.events() {
        match event {
            WorkflowEvent::ChildWorkflowStarted { child_id, .. } => started.push(*child_id),
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. } => ended.push(*child_id),
            _ => {}
        }
    }
    started.len() == 3 && started != ended
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fan_out_replays_the_same_in_any_completion_order() {
    let env = sim_env();
    let task = trip_task(3);
    let report = env
        .simulation()
        .seeds(SeedPlan {
            first: 0,
            count: 64,
        })
        .require_reached("the children end out of order")
        .sweep(agent_loop_info().handler, json!(task), |run| {
            always!(
                results_match(run),
                "each result and label reaches the parent"
            );
            sometimes!(out_of_order(run), "the children end out of order");
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert!(report.seeds > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_agent_fans_out_too() {
    use autumn_harvest_agent::{AgentId, AgentOp, AgentProfile};

    let (_dir, db) = fresh_db();
    let model = trip_model();
    let harness = trip_harness(model.clone()).agent(
        AgentProfile::new("planner")
            .mandate(Mandate::new("planner").peers(["flights", "hotels", "visas"]))
            .parallel_children(3),
    );
    let mut rt = runtime(&db, harness);
    let planner = AgentId::new("planner", "p1").unwrap();
    sqlite::send(&mut rt, &planner, &AgentOp::message(ROOT_INPUT), None).unwrap();
    sqlite::drive(&mut rt, &planner).await.unwrap();

    let state = sqlite::state(&rt, &planner).await.unwrap().unwrap();
    assert_eq!(state.last_answer.as_deref(), Some("trip planned"));
    let exec = sqlite::current_run(&rt, &planner).unwrap().unwrap();
    assert_eq!(children_started(&rt, exec).len(), 3);
    assert_eq!(answers(&model).len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_child_starts_with_the_labels_of_its_parent() {
    // The parent read untrusted input. A child must not then write, so the
    // labels travel into each child run.
    let (_dir, db) = fresh_db();
    let model = FnModel::new(|request| {
        let input = first_user(request);
        if input == ROOT_INPUT {
            return if results(request).is_empty() {
                three_delegations(1)
            } else {
                text("done", 1)
            };
        }
        if results(request).is_empty() {
            tool_calls(&[("w1", "book", json!({}))], 1)
        } else {
            text(&results(request)[0], 1)
        }
    });
    let book = FnTool::new("book", "Books.", json!({"type": "object"}), |_| async {
        Ok(json!("booked"))
    })
    .effect(ToolEffect::Write)
    .shared();
    let rules = FlowRules::new().forbid(
        [labels::UNTRUSTED_INPUT, "effect:write"],
        Rule::Deny("no write after untrusted input".into()),
    );
    let harness = trip_harness(model.clone()).tool(book).flow_rules(rules);
    let mut rt = runtime(&db, harness);
    let task = trip_task(3).flow_labels([labels::UNTRUSTED_INPUT]);
    let (_exec, _report) = run(&mut rt, &task).await;

    let answers = answers(&model);
    assert_eq!(answers.len(), 3);
    for (agent, answer) in answers {
        assert!(answer.contains("denied"), "{agent} wrote: {answer}");
    }
}
