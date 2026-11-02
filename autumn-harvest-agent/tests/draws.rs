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

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::workflow::agent_loop_info;
use autumn_harvest_agent::{
    AgentHarness, AgentReport, AgentStop, AgentTask, ChatRole, ContentPart, DrawId, FnTool,
    Mandate, ModelTurnRequest, Peer, ToolEffect, run_draws, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};
use common::{MemStore, ScriptedModel, TryFnModel, answer, calls, response_cache};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The input of the test workflow: a task and the draw ids to run.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DrawsInput {
    task: AgentTask,
    draws: Vec<u32>,
}

/// Runs one batch of draws and returns each report, or the error text.
#[workflow]
async fn some_draws(
    ctx: &WorkflowContext,
    input: DrawsInput,
) -> Result<Vec<Result<AgentReport, String>>, String> {
    run_draws(
        ctx,
        &input.task,
        input.draws.iter().copied().map(DrawId::new),
    )
    .await
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

/// Start the test workflow and drive it to its end.
async fn draw(
    rt: &mut SqliteRuntime,
    task: &AgentTask,
    draws: &[u32],
) -> (
    ExecutionId,
    Result<Vec<Result<AgentReport, String>>, String>,
) {
    let input = DrawsInput {
        task: task.clone(),
        draws: draws.to_vec(),
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

fn reports(ended: Result<Vec<Result<AgentReport, String>>, String>) -> Vec<AgentReport> {
    ended
        .unwrap()
        .into_iter()
        .map(|report| report.unwrap())
        .collect()
}

/// Replay the parent run, then each draw run, from history.
async fn replays(rt: &SqliteRuntime, exec: ExecutionId) {
    let history = rt.load_history(exec).unwrap();
    let mut runs = vec![(exec, some_draws_info())];
    for event in &history {
        if let WorkflowEvent::ChildWorkflowStarted { child_id, .. } = event {
            runs.push((*child_id, agent_loop_info()));
        }
    }
    assert!(runs.len() > 1, "the parent started no draw");
    for (run, info) in runs {
        let replay = WorkflowReplayer::new()
            .register(vec![info])
            .with_execution_id(run)
            .replay_from_events(rt.load_history(run).unwrap())
            .await;
        assert!(
            matches!(replay.status, ReplayStatus::ReplaySucceeded),
            "{replay}"
        );
    }
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

    let mut texts: Vec<&str> = reports.iter().map(|report| report.text.as_str()).collect();
    texts.sort_unstable();
    assert_eq!(texts, ["a", "b", "c"]);
    assert_eq!(model.calls(), 3, "one model call per draw");
    let mut draws: Vec<Option<DrawId>> = model.requests().iter().map(|r| r.draw).collect();
    draws.sort_unstable();
    assert_eq!(
        draws,
        [
            Some(DrawId::new(0)),
            Some(DrawId::new(1)),
            Some(DrawId::new(2))
        ],
        "each draw reaches the model"
    );
    let run_ids: Vec<String> = reports
        .iter()
        .map(|report| report.run_id.clone().unwrap())
        .collect();
    assert_eq!(
        run_ids,
        [0, 1, 2].map(|n| format!("{exec}:s{n}")),
        "the reports come back in draw order"
    );
    replays(&rt, exec).await;
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
fn run_id_tool(seen: &Arc<Mutex<Vec<(String, u32)>>>) -> Arc<dyn autumn_harvest_agent::Tool> {
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
        let answered = request
            .messages
            .iter()
            .any(|message| message.role == ChatRole::Tool);
        if answered {
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
    assert_eq!(
        seen,
        [(format!("{exec}:s0"), 0), (format!("{exec}:s1"), 0)],
        "each draw has its own idempotency key"
    );
    replays(&rt, exec).await;
}

/// The text of the first user message of a request.
fn first_user(request: &autumn_harvest_agent::ChatRequest) -> String {
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

// A delegated child keeps the draw id, so two draws that delegate the same
// task do not share one cached child answer. Its run id nests in the draw.
#[tokio::test(flavor = "multi_thread")]
async fn a_delegated_child_keeps_the_draw_and_nests_its_run_id() {
    let (_dir, db) = fresh_db();
    let model = TryFnModel::new(|request| {
        if first_user(request) == "find flights" {
            return answer("flight", 1);
        }
        let answered = request
            .messages
            .iter()
            .any(|message| message.role == ChatRole::Tool);
        if answered {
            answer("planned", 1)
        } else {
            calls(
                &[(
                    "d1",
                    DELEGATE_TOOL,
                    json!({"agent": "flights", "task": "find flights"}),
                )],
                1,
            )
        }
    });
    let harness = AgentHarness::new(model.clone()).peer(Peer::new("flights", "Finds flights."));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("plan the trip").mandate(Mandate::new("planner").peers(["flights"]));
    let (exec, ended) = draw(&mut rt, &task, &[3]).await;
    let report = reports(ended).remove(0);

    assert_eq!(report.text, "planned");
    assert_eq!(
        report.run_id.as_deref(),
        Some(format!("{exec}:s3").as_str())
    );
    assert_eq!(
        report.delegations[0].run_id.as_deref(),
        Some(format!("{exec}:s3:d1").as_str())
    );
    let child = model
        .requests()
        .into_iter()
        .find(|request| first_user(request) == "find flights")
        .unwrap();
    assert_eq!(child.draw, Some(DrawId::new(3)));
}

// A draw does not deliver its answer. The caller picks the answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_draw_never_delivers() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("a", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model));
    let (exec, ended) = draw(&mut rt, &AgentTask::new("hi").deliver(), &[0]).await;
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
    assert_eq!(started[0]["deliver"], json!(false));
    assert_eq!(started[0]["report_failure"], json!(true));
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
    let many: Vec<u32> = (0..=autumn_harvest_agent::MAX_PARALLEL_CHILDREN).collect();
    let (_, over) = draw(&mut rt, &task, &many).await;
    assert!(over.unwrap_err().contains("at most"));
    let (_, none) = draw(&mut rt, &task, &[]).await;
    assert!(none.unwrap().is_empty());
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
