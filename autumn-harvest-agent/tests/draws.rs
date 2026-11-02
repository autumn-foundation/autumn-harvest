//! Draw identity (issue #2440).
//!
//! Each draw is a child `agent_loop` run with the run id `{root}:s{n}`. The
//! draw id reaches the model request and the response-cache key, so N draws
//! of one request stay N samples.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// The `#[workflow]` macro reads its input through a binding.
#![allow(clippy::used_underscore_binding)]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::workflow::agent_loop_info;
use autumn_harvest_agent::{
    AgentHarness, AgentReport, AgentStop, AgentTask, ChatRequest, ChatRole, ContentPart, DrawId,
    FnTool, Followups, MAX_PARALLEL_CHILDREN, Mandate, MemoryScope, ModelTurnRequest, Peer, Tool,
    ToolEffect, run_draws, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};
use common::{MemStore, ScriptedModel, TryFnModel, answer, calls, response_cache};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The input of the test workflow: a task and the batches of draw ids.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DrawsInput {
    task: AgentTask,
    batches: Vec<Vec<u32>>,
}

type Ended = Result<Vec<Result<AgentReport, String>>, String>;

/// Runs each batch of draws in turn. Returns every report, or the error
/// text, in order.
#[workflow]
async fn some_draws(ctx: &WorkflowContext, input: DrawsInput) -> Ended {
    let mut ended = Vec::new();
    for batch in &input.batches {
        let ids = batch.iter().copied().map(DrawId::new);
        ended.extend(run_draws(ctx, &input.task, ids).await?);
    }
    Ok(ended)
}

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn runtime(db: &std::path::Path, harness: AgentHarness) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).expect("the database opens");
    rt.register_workflow(&some_draws_info());
    sqlite::register(&mut rt, Arc::new(harness));
    rt
}

/// Start the test workflow with `batches` and drive it to its end.
async fn batches(
    rt: &mut SqliteRuntime,
    task: &AgentTask,
    batches: &[&[u32]],
) -> (ExecutionId, Ended) {
    let input = DrawsInput {
        task: task.clone(),
        batches: batches.iter().map(|batch| batch.to_vec()).collect(),
    };
    let exec = rt
        .start_workflow("some_draws", serde_json::to_value(input).unwrap())
        .unwrap();
    let ended = match rt.run_until_blocked(exec).await.unwrap() {
        RunState::Completed(value) => Ok(serde_json::from_value(value).unwrap()),
        RunState::Failed(error) => Err(error),
        other => panic!("expected an end, got {other:?}"),
    };
    (exec, ended)
}

/// Start the test workflow with one batch and drive it to its end.
async fn draw(rt: &mut SqliteRuntime, task: &AgentTask, draws: &[u32]) -> (ExecutionId, Ended) {
    batches(rt, task, &[draws]).await
}

fn reports(ended: Ended) -> Vec<AgentReport> {
    ended
        .unwrap()
        .into_iter()
        .map(|report| report.unwrap())
        .collect()
}

type Run = (ExecutionId, WorkflowInfo, Vec<WorkflowEvent>);

/// The histories of the parent run and of each draw run, each with the
/// handler that replays it.
fn histories(rt: &SqliteRuntime, exec: ExecutionId) -> Vec<Run> {
    let history = rt.load_history(exec).unwrap();
    let mut runs = Vec::new();
    for event in &history {
        if let WorkflowEvent::ChildWorkflowStarted { child_id, .. } = event {
            let child = rt.load_history(*child_id).unwrap();
            runs.push((*child_id, agent_loop_info(), child));
        }
    }
    assert!(!runs.is_empty(), "the parent started no draw");
    runs.insert(0, (exec, some_draws_info(), history));
    runs
}

/// Replay each run from its history.
async fn replays(runs: Vec<Run>) {
    for (run, info, events) in runs {
        let replay = WorkflowReplayer::new()
            .register(vec![info])
            .with_execution_id(run)
            .replay_from_events(events)
            .await;
        assert!(
            matches!(replay.status, ReplayStatus::ReplaySucceeded),
            "{replay}"
        );
    }
}

/// The root of a draw run id, and its draw suffix.
fn split(run_id: &str) -> (&str, &str) {
    run_id.rsplit_once(':').unwrap()
}

/// The run id of each report.
fn run_ids(reports: &[AgentReport]) -> Vec<String> {
    reports
        .iter()
        .map(|report| report.run_id.clone().unwrap())
        .collect()
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

/// Whether a request already holds a tool result.
fn answered(request: &ChatRequest) -> bool {
    request
        .messages
        .iter()
        .any(|message| message.role == ChatRole::Tool)
}

/// A turn that delegates each `(agent, task)` pair.
fn delegations(list: &[(&str, &str)]) -> common::Reply {
    let ids: Vec<String> = (1..=list.len()).map(|n| format!("d{n}")).collect();
    let calls_list: Vec<(&str, &str, Value)> = list
        .iter()
        .zip(&ids)
        .map(|((agent, task), id)| {
            (
                id.as_str(),
                DELEGATE_TOOL,
                json!({"agent": agent, "task": task}),
            )
        })
        .collect();
    calls(&calls_list, 1)
}

// AC: a 3-draw run with the cache on gets three different answers. The
// model answers "a", "b", "c" in order, and it gets three calls.
#[tokio::test(flavor = "multi_thread")]
async fn three_draws_with_the_cache_on_get_three_answers_from_three_calls() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("m-1", vec![answer("a", 1), answer("b", 1), answer("c", 1)]);
    let store = Arc::new(MemStore::default());
    let harness = AgentHarness::new(model.clone())
        .response_cache(response_cache(&store))
        .temperature(0.7);
    let mut rt = runtime(&db, harness);
    let (exec, ended) = draw(&mut rt, &AgentTask::new("pick a colour"), &[0, 1, 2]).await;
    let reports = reports(ended);

    // SQLite drives one draw after another, so draw 1 starts after draw 0
    // stored its answer. Without the draw in the key, draws 1 and 2 hit it.
    let mut texts: Vec<&str> = reports.iter().map(|report| report.text.as_str()).collect();
    texts.sort_unstable();
    assert_eq!(texts, ["a", "b", "c"]);
    assert_eq!(model.calls(), 3, "one model call per draw");
    let mut draws: Vec<Option<DrawId>> = model.requests().iter().map(|r| r.draw).collect();
    draws.sort_unstable();
    assert_eq!(
        draws,
        [0, 1, 2].map(|n| Some(DrawId::new(n))),
        "each draw reaches the model"
    );
    let run_ids = run_ids(&reports);
    let root = split(&run_ids[0]).0;
    assert!(
        autumn_harvest::uuid::Uuid::parse_str(root).is_ok(),
        "{root}"
    );
    assert_ne!(root, exec.to_string(), "history records the root");
    assert_eq!(
        run_ids,
        [0, 1, 2].map(|n| format!("{root}:s{n}")),
        "the reports come back in draw order"
    );
    replays(histories(&rt, exec)).await;
}

// The same draw of the same request is one sample, so the cache serves it.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_run_of_the_same_draw_hits_the_cache() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("m-1", vec![answer("a", 1), answer("never", 1)]);
    let store = Arc::new(MemStore::default());
    let harness = AgentHarness::new(model.clone())
        .response_cache(response_cache(&store))
        .temperature(0.7);
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("pick a colour");
    let (_, first) = draw(&mut rt, &task, &[0]).await;
    let (_, second) = draw(&mut rt, &task, &[0]).await;

    assert_eq!(reports(first)[0].text, "a");
    assert_eq!(reports(second)[0].text, "a");
    assert_eq!(model.calls(), 1);
}

/// A tool that records the run id and the step of each call.
fn run_id_tool(seen: &Arc<Mutex<Vec<(String, u32)>>>) -> Arc<dyn Tool> {
    let seen = Arc::clone(seen);
    FnTool::with_context(
        "lookup",
        "Looks a value up.",
        json!({"type": "object"}),
        move |_input: Value, ctx| {
            let seen = Arc::clone(&seen);
            async move {
                seen.lock()
                    .unwrap()
                    .push((ctx.run_id.as_str().to_owned(), ctx.step));
                Ok(json!("found"))
            }
        },
    )
    .effect(ToolEffect::ReadOnly)
    .shared()
}

// AC: two draws at step 0 give tools two run ids, `{root}:s0` and
// `{root}:s1`.
#[tokio::test(flavor = "multi_thread")]
async fn two_draws_at_step_zero_give_tools_two_run_ids() {
    let (_dir, db) = fresh_db();
    // Both draws ask for the same call with the same call id.
    let model = TryFnModel::new(|request| {
        if answered(request) {
            answer("done", 1)
        } else {
            calls(&[("c1", "lookup", json!({}))], 1)
        }
    });
    let seen = Arc::new(Mutex::new(Vec::new()));
    let harness = AgentHarness::new(model).tool(run_id_tool(&seen));
    let mut rt = runtime(&db, harness);
    let (exec, ended) = draw(&mut rt, &AgentTask::new("look it up"), &[0, 1]).await;
    assert!(
        reports(ended)
            .iter()
            .all(|r| r.stop == AgentStop::Completed)
    );

    let mut seen = seen.lock().unwrap().clone();
    seen.sort();
    let root = split(&seen[0].0).0.to_owned();
    assert_eq!(
        seen,
        [(format!("{root}:s0"), 0), (format!("{root}:s1"), 0)],
        "each draw has its own idempotency key"
    );
    replays(histories(&rt, exec)).await;
}

// An inline child keeps the draw id, so two draws that delegate the same
// task do not share one cached child answer. Its run id nests in the draw.
#[tokio::test(flavor = "multi_thread")]
async fn an_inline_child_keeps_the_draw_and_nests_its_run_id() {
    let (_dir, db) = fresh_db();
    let model = TryFnModel::new(|request| {
        if first_user(request) == "find flights" {
            answer("flight", 1)
        } else if answered(request) {
            answer("planned", 1)
        } else {
            delegations(&[("flights", "find flights")])
        }
    });
    let harness = AgentHarness::new(model.clone()).peer(Peer::new("flights", "Finds flights."));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("plan the trip").mandate(Mandate::new("planner").peers(["flights"]));
    let (_, ended) = draw(&mut rt, &task, &[3]).await;
    let report = reports(ended).remove(0);

    assert_eq!(report.text, "planned");
    let run_id = report.run_id.clone().unwrap();
    assert_eq!(split(&run_id).1, "s3");
    assert_eq!(
        report.delegations[0].run_id.as_deref(),
        Some(format!("{run_id}:d1").as_str())
    );
    let child = model
        .requests()
        .into_iter()
        .find(|request| first_user(request) == "find flights")
        .unwrap();
    assert_eq!(child.draw, Some(DrawId::new(3)));
}

// A fan-out child of a draw keeps the draw id. It runs under its own
// execution id, so it never reuses the run id of its draw.
#[tokio::test(flavor = "multi_thread")]
async fn a_fan_out_child_of_a_draw_has_its_own_run_id() {
    let (_dir, db) = fresh_db();
    let model = TryFnModel::new(|request| {
        let input = first_user(request);
        if input != "plan the trip" {
            answer(&format!("done: {input}"), 1)
        } else if answered(request) {
            answer("planned", 1)
        } else {
            delegations(&[("flights", "find flights"), ("hotels", "find hotels")])
        }
    });
    let harness = AgentHarness::new(model.clone())
        .peer(Peer::new("flights", "Finds flights."))
        .peer(Peer::new("hotels", "Finds hotels."));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("plan the trip")
        .mandate(Mandate::new("planner").peers(["flights", "hotels"]))
        .parallel_children(2);
    let (_, ended) = draw(&mut rt, &task, &[5]).await;
    let report = reports(ended).remove(0);

    assert_eq!(report.text, "planned");
    let draw_run = report.run_id.clone().unwrap();
    let children: Vec<String> = report
        .delegations
        .iter()
        .map(|record| record.run_id.clone().unwrap())
        .collect();
    assert_eq!(children.len(), 2);
    assert_ne!(children[0], children[1]);
    for child in &children {
        assert!(!child.starts_with(split(&draw_run).0), "{child}");
    }
    let child_draws: Vec<Option<DrawId>> = model
        .requests()
        .iter()
        .filter(|request| first_user(request) != "plan the trip")
        .map(|request| request.draw)
        .collect();
    assert_eq!(child_draws, [Some(DrawId::new(5)); 2]);
}

// A draw does not deliver its answer, because the caller picks the answer.
// It keeps no durable state, so a draw that loses leaves nothing behind.
#[tokio::test(flavor = "multi_thread")]
async fn a_draw_never_delivers_and_keeps_no_state() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("a", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model));
    let task = AgentTask::new("hi")
        .deliver()
        .memory(MemoryScope::new("team"))
        .followups(Followups::new(Duration::from_secs(60)))
        .unattended_memory_writes();
    let (exec, ended) = draw(&mut rt, &task, &[0]).await;
    assert_eq!(reports(ended)[0].text, "a");
    let started: Vec<Value> = rt
        .load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::ChildWorkflowStarted { input, .. } => Some(input),
            _ => None,
        })
        .collect();
    assert_eq!(started.len(), 1);
    let input = &started[0];
    assert_eq!(input["deliver"], json!(false));
    assert_eq!(input["report_failure"], json!(true));
    assert_eq!(input["memory_scope"], Value::Null);
    assert_eq!(input["followups"], Value::Null);
    assert_eq!(input["unattended_memory_writes"], json!(false));
    assert!(input.get("workspace").is_none());
}

// A batch over the bound, or with one draw id twice, starts no draw.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_batch_starts_no_draw() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(Vec::new());
    let mut rt = runtime(&db, AgentHarness::new(model.clone()));
    let task = AgentTask::new("hi");
    let (_, twice) = draw(&mut rt, &task, &[1, 1]).await;
    assert!(twice.unwrap_err().contains("twice"));
    let many: Vec<u32> = (0..=MAX_PARALLEL_CHILDREN).collect();
    let (_, over) = draw(&mut rt, &task, &many).await;
    assert!(over.unwrap_err().contains("at most"));
    let (_, none) = draw(&mut rt, &task, &[]).await;
    assert_eq!(none.unwrap().len(), 0);
    assert_eq!(model.calls(), 0);
}

// A batch of exactly the bound runs every draw.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_at_the_bound_runs() {
    let (_dir, db) = fresh_db();
    let model = TryFnModel::new(|_| answer("x", 1));
    let mut rt = runtime(&db, AgentHarness::new(model.clone()));
    let all: Vec<u32> = (0..MAX_PARALLEL_CHILDREN).collect();
    let (_, ended) = draw(&mut rt, &AgentTask::new("hi"), &all).await;
    assert_eq!(reports(ended).len(), all.len());
    assert_eq!(model.requests().len(), all.len());
}

// Two batches of one run never share a run id. A beam or a vote calls
// `run_draws` once per round with the same draw ids.
#[tokio::test(flavor = "multi_thread")]
async fn two_batches_of_one_run_get_two_roots() {
    let (_dir, db) = fresh_db();
    let model = TryFnModel::new(|_| answer("x", 1));
    let mut rt = runtime(&db, AgentHarness::new(model));
    let (exec, ended) = batches(&mut rt, &AgentTask::new("hi"), &[&[0], &[0]]).await;
    let run_ids = run_ids(&reports(ended));
    assert_eq!(run_ids.len(), 2);
    assert_ne!(run_ids[0], run_ids[1]);
    assert!(run_ids.iter().all(|id| split(id).1 == "s0"));
    replays(histories(&rt, exec)).await;
}

// A reset or a fork replays the parent under a new execution id. History
// holds the root, so the recorded draw inputs still match.
#[tokio::test(flavor = "multi_thread")]
async fn a_parent_replays_under_a_new_execution_id() {
    let (_dir, db) = fresh_db();
    let model = TryFnModel::new(|_| answer("x", 1));
    let mut rt = runtime(&db, AgentHarness::new(model));
    let (exec, ended) = draw(&mut rt, &AgentTask::new("hi"), &[0, 1]).await;
    assert_eq!(reports(ended).len(), 2);
    let events = rt.load_history(exec).unwrap();
    let replay = WorkflowReplayer::new()
        .register(vec![some_draws_info()])
        .with_execution_id(ExecutionId::new())
        .replay_from_events(events)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

// A root that is not a UUID never becomes a run id. A caller that writes a
// workflow id as the root would repeat the run id each day.
#[tokio::test(flavor = "multi_thread")]
async fn a_root_that_is_not_a_uuid_fails_the_run() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("never", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()));
    let mut task = serde_json::to_value(AgentTask::new("hi")).unwrap();
    task["draw"] = json!({"id": 0, "root": "daily"});
    let exec = sqlite::start(&mut rt, &serde_json::from_value(task).unwrap()).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(&state, RunState::Failed(error) if error.contains("draw root")),
        "{state:?}"
    );
    assert_eq!(model.calls(), 0);
}

/// A history that `trunk-dev` recorded at c0d70ed, before draws existed: one
/// tool round and one answer, with the cache on.
const PRE_DRAW_HISTORY: &str = include_str!("fixtures/pre_draw_history.json");

// AC: a request recorded before the change decodes with no draw, and the
// run replays with no non-determinism error.
#[tokio::test(flavor = "multi_thread")]
async fn a_history_from_before_draws_decodes_and_replays() {
    let doc: Value = serde_json::from_str(PRE_DRAW_HISTORY).unwrap();
    let exec: ExecutionId = serde_json::from_value(doc["execution_id"].clone()).unwrap();
    let events: Vec<WorkflowEvent> = serde_json::from_value(doc["events"].clone()).unwrap();
    assert!(!PRE_DRAW_HISTORY.contains("\"draw\""));

    let requests: Vec<ModelTurnRequest> = events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, input, .. } if name == "agent_model_turn" => {
                Some(serde_json::from_value(input.clone()).unwrap())
            }
            _ => None,
        })
        .collect();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.draw.is_none()));
    let task: AgentTask = match &events[0] {
        WorkflowEvent::WorkflowStarted { input, .. } => {
            serde_json::from_value(input.clone()).unwrap()
        }
        other => panic!("expected a start, got {other:?}"),
    };
    assert!(task.draw.is_none());

    let replay = WorkflowReplayer::new()
        .register(vec![agent_loop_info()])
        .with_execution_id(exec)
        .replay_from_events(events)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

// A run with no draw writes no draw field, so an older binary reads it.
#[test]
fn a_run_with_no_draw_records_no_draw_field() {
    let task = serde_json::to_value(AgentTask::new("x")).unwrap();
    assert!(task.get("draw").is_none());
    let request: ModelTurnRequest = serde_json::from_value(json!({
        "run_id": "r",
        "steps_used": 0,
        "max_steps": 1,
        "usage": {"input_tokens": 0, "output_tokens": 0},
        "messages": [],
    }))
    .unwrap();
    assert!(
        serde_json::to_value(&request)
            .unwrap()
            .get("draw")
            .is_none()
    );
}
