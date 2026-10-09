//! Replay-as-evaluation (issue #2001).
//!
//! Each test records a source run on the SQLite engine. It then evaluates
//! that history with a candidate model and checks the report.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::eval::{
    Candidate, Divergence, EvalError, Evaluation, RunEnd, Side, Verdict, evaluate,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentStop, AgentTask, Approval, BoxFuture, ChatRole, ContentPart,
    Delivery, Report, Rule, ToolDecision, ToolEffect, ToolRules, sqlite,
};
use autumn_harvest_sqlite::RunState;
use common::{
    CountingPolicy, Recorder, Reply, ScriptedModel, answer, calls, recorded_tool, report,
    runtime, tool_results,
};
use serde_json::{Value, json};

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// A wall-clock instant `seconds` from now, to fire a durable timer early.
fn later(seconds: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now() + chrono::Duration::seconds(seconds)
}

/// A delivery that keeps every report.
#[derive(Debug, Default)]
struct Inbox(Mutex<Vec<Report>>);

impl Delivery for Inbox {
    fn deliver<'a>(&'a self, report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        self.0.lock().unwrap().push(report.clone());
        Box::pin(async { Ok(()) })
    }
}

/// Run `task` to its end on SQLite with the scripted `replies` and a
/// read-only `lookup` tool. Returns the recorded history.
async fn record(task: &AgentTask, replies: Vec<Reply>) -> Vec<WorkflowEvent> {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let harness = AgentHarness::new(ScriptedModel::new(replies)).tool(recorded_tool(
        "lookup",
        ToolEffect::ReadOnly,
        &recorder,
    ));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(_)),
        "the source run completes: {state:?}"
    );
    rt.load_history(exec).unwrap()
}

/// A candidate harness with counted `lookup` and `pay` tools.
fn candidate(model: Arc<ScriptedModel>) -> (AgentHarness, Arc<Recorder>) {
    let recorder = Arc::new(Recorder::default());
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .tool(recorded_tool("pay", ToolEffect::Write, &recorder));
    (harness, recorder)
}

/// A source run with one `lookup` round and a final answer.
async fn one_lookup() -> Vec<WorkflowEvent> {
    record(
        &AgentTask::new("look it up").system("Be brief."),
        vec![
            calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
            answer("done", 5),
        ],
    )
    .await
}

fn first(evaluation: &Evaluation) -> &autumn_harvest_agent::eval::TurnDiff {
    let index = evaluation
        .first_divergence
        .expect("the evaluation reports a divergence");
    &evaluation.turns[index]
}

#[tokio::test(flavor = "multi_thread")]
async fn the_recorded_model_reports_no_divergence() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]);
    let (harness, recorder) = candidate(model.clone());

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(evaluation.first_divergence, None);
    assert_eq!(evaluation.turns.len(), 2);
    assert!(
        evaluation
            .turns
            .iter()
            .all(|turn| turn.verdict == Verdict::Same)
    );
    assert_eq!(evaluation.replayed_tool_calls, 1);
    assert_eq!(evaluation.stubbed_tool_calls, 0);
    assert_eq!(model.calls(), 2, "the candidate model runs live");
    assert!(recorder.runs().is_empty(), "no tool runs live");
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    assert_eq!(report.stop, AgentStop::Completed);
    assert!(
        tool_results(report)[0].contains("echo"),
        "the candidate reads the recorded tool output"
    );
    assert!(matches!(evaluation.recorded, RunEnd::Completed(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn another_tool_diverges_at_that_turn_and_never_runs() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "pay", json!({"amount": 100}))], 5),
        answer("paid", 5),
    ]);
    let (harness, recorder) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert!(evaluation.diverged());
    assert_eq!(evaluation.first_divergence, Some(0));
    assert_eq!(first(&evaluation).verdict, Verdict::Diverged(Divergence::Calls));
    assert_eq!(evaluation.replayed_tool_calls, 0);
    assert_eq!(evaluation.stubbed_tool_calls, 1);
    assert!(recorder.runs().is_empty(), "the payment never runs");
}

#[tokio::test(flavor = "multi_thread")]
async fn changed_arguments_diverge() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "y"}))], 5),
        answer("done", 5),
    ]);
    let (harness, recorder) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert_eq!(evaluation.first_divergence, Some(0));
    assert_eq!(first(&evaluation).verdict, Verdict::Diverged(Divergence::Calls));
    assert_eq!(evaluation.stubbed_tool_calls, 1);
    assert!(recorder.runs().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_later_divergence_names_its_turn() {
    let source = record(
        &AgentTask::new("two lookups"),
        vec![
            calls(&[("c1", "lookup", json!({"q": "a"}))], 1),
            calls(&[("c2", "lookup", json!({"q": "b"}))], 1),
            answer("done", 1),
        ],
    )
    .await;
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "a"}))], 1),
        answer("enough", 1),
    ]);
    let (harness, recorder) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert_eq!(evaluation.turns[0].verdict, Verdict::Same);
    assert_eq!(evaluation.first_divergence, Some(1));
    assert_eq!(first(&evaluation).verdict, Verdict::Diverged(Divergence::Shape));
    assert_eq!(
        evaluation.turns[2].verdict,
        Verdict::Diverged(Divergence::Missing(Side::Candidate))
    );
    assert_eq!(evaluation.replayed_tool_calls, 1);
    assert!(recorder.runs().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reworded_answer_is_not_a_divergence() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("all done", 5),
    ]);
    let (harness, _) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(evaluation.turns[1].verdict, Verdict::Reworded);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_policy_decision_diverges() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]);
    let policy = CountingPolicy::new(ToolDecision::Deny {
        reason: "no".into(),
    });
    let (harness, recorder) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness.policy(policy)))
        .await
        .unwrap();

    assert_eq!(evaluation.first_divergence, Some(0));
    assert_eq!(first(&evaluation).verdict, Verdict::Diverged(Divergence::Policy));
    assert!(recorder.runs().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_candidate_prompt_reaches_the_model() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]);
    let (harness, _) = candidate(model.clone());

    let candidate = Candidate::new(harness).system_prompt("Answer in French.");
    evaluate(&source, &candidate).await.unwrap();

    let first_request = &model.requests()[0];
    assert_eq!(first_request.messages[0].role, ChatRole::System);
    assert_eq!(
        first_request.messages[0].content,
        vec![ContentPart::Text("Answer in French.".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_tool_activity_executes_live_during_evaluation() {
    let source = record(
        &AgentTask::new("pay twice"),
        vec![
            calls(&[("c1", "lookup", json!({"q": "bill"}))], 1),
            answer("nothing to pay", 1),
        ],
    )
    .await;
    // The candidate takes a path the source never took. It calls a write
    // tool with no recorded output, then the recorded tool, then answers.
    let model = ScriptedModel::new(vec![
        calls(&[("p1", "pay", json!({"amount": 5}))], 1),
        calls(
            &[
                ("p2", "pay", json!({"amount": 5})),
                ("c1", "lookup", json!({"q": "bill"})),
            ],
            1,
        ),
        answer("paid", 1),
    ]);
    let (harness, recorder) = candidate(model.clone());

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert!(evaluation.diverged());
    assert_eq!(model.calls(), 3);
    assert_eq!(evaluation.stubbed_tool_calls, 3);
    assert!(
        recorder.runs().is_empty(),
        "no tool runs live: {:?}",
        recorder.runs()
    );
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    assert!(
        tool_results(report)[0].contains("not run"),
        "a stub tells the model that the call did not run: {:?}",
        tool_results(report)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_report_is_delivered_during_evaluation() {
    let source = record(&AgentTask::new("report it").deliver(), vec![answer("ok", 1)]).await;
    let inbox = Arc::new(Inbox::default());
    let model = ScriptedModel::new(vec![answer("ok", 1)]);
    let (harness, _) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness.delivery(inbox.clone())))
        .await
        .unwrap();

    assert!(!evaluation.diverged());
    assert!(inbox.0.lock().unwrap().is_empty(), "nothing is delivered");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_erased_source_is_refused() {
    let mut source = one_lookup().await;
    for event in &mut source {
        if let WorkflowEvent::ActivityCompleted { output, .. } = event {
            *output = json!({"_harvest_erased": true});
        }
    }
    let model = ScriptedModel::new(vec![answer("done", 1)]);
    let (harness, _) = candidate(model.clone());

    let result = evaluate(&source, &Candidate::new(harness)).await;

    assert!(matches!(result, Err(EvalError::ErasedSource)), "{result:?}");
    assert_eq!(model.calls(), 0, "a refused source costs no model call");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_history_that_is_not_an_agent_run_is_refused() {
    let source = vec![WorkflowEvent::WorkflowStarted {
        input: json!({"order": 7}),
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let (harness, _) = candidate(ScriptedModel::new(Vec::new()));

    let result = evaluate(&source, &Candidate::new(harness)).await;

    assert!(matches!(result, Err(EvalError::NotAnAgentRun(_))), "{result:?}");
    let empty = evaluate(&[], &Candidate::new(candidate(ScriptedModel::new(Vec::new())).0)).await;
    assert!(matches!(empty, Err(EvalError::NotAnAgentRun(_))), "{empty:?}");
}

#[tokio::test]
async fn a_current_thread_runtime_is_refused() {
    let source = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::to_value(AgentTask::new("hi")).unwrap(),
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let (harness, _) = candidate(ScriptedModel::new(vec![answer("hi", 1)]));

    let result = evaluate(&source, &Candidate::new(harness)).await;

    assert!(
        matches!(result, Err(EvalError::MultiThreadRuntimeRequired)),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_candidate_model_is_reported_not_raised() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(Vec::new());
    let (harness, recorder) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert!(
        matches!(evaluation.candidate, RunEnd::Failed(_)),
        "{:?}",
        evaluation.candidate
    );
    assert_eq!(evaluation.first_divergence, Some(0));
    assert_eq!(
        evaluation.turns[0].verdict,
        Verdict::Diverged(Divergence::Missing(Side::Candidate))
    );
    assert!(recorder.runs().is_empty());
}

/// Record a source with two gated `pay` calls. The first approval arrives
/// after its deadline. The second arrives in time.
async fn gated_source() -> Vec<WorkflowEvent> {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "pay", json!({"amount": 1}))], 1),
        calls(&[("w2", "pay", json!({"amount": 2}))], 1),
        answer("finished", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("pay", ToolEffect::Write, &recorder))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("pay it").approval_timeout(Duration::from_secs(60));
    let exec = sqlite::start(&mut rt, &task).unwrap();

    let RunState::WaitingSignal(late) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("expected the first approval wait");
    };
    // The first deadline passes. The run goes on to the second wait.
    let state = rt.run_until_blocked_as_of(exec, later(120)).await.unwrap();
    let RunState::WaitingSignal(second) = state else {
        panic!("expected the second approval wait, got {state:?}");
    };
    sqlite::decide(&mut rt, exec, &late, &Approval::Approve).unwrap();
    sqlite::decide(&mut rt, exec, &second, &Approval::Approve).unwrap();
    let final_report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(final_report.stop, AgentStop::Completed);
    assert_eq!(
        recorder.runs(),
        vec![json!({"amount": 2})],
        "only the call approved in time runs in the source"
    );
    rt.load_history(exec).unwrap()
}

fn gated_candidate(replies: Vec<Reply>) -> (AgentHarness, Arc<Recorder>, Arc<ScriptedModel>) {
    let model = ScriptedModel::new(replies);
    let recorder = Arc::new(Recorder::default());
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("pay", ToolEffect::Write, &recorder))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ));
    (harness, recorder, model)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_approval_is_not_delivered_and_new_ids_take_the_recorded_ids() {
    let source = gated_source().await;
    // The candidate model makes the same calls with new provider ids.
    let (harness, recorder, _) = gated_candidate(vec![
        calls(&[("x1", "pay", json!({"amount": 1}))], 1),
        calls(&[("x2", "pay", json!({"amount": 2}))], 1),
        answer("finished", 1),
    ]);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(
        evaluation.replayed_tool_calls, 1,
        "the approval in time releases the recorded output"
    );
    assert_eq!(
        evaluation.stubbed_tool_calls, 0,
        "the late approval releases nothing"
    );
    assert!(recorder.runs().is_empty());
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    let results = tool_results(report);
    assert!(results[0].contains("no approval arrived"), "{results:?}");
    assert!(results[1].contains("echo"), "{results:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_source_history_is_left_unchanged() {
    let source = one_lookup().await;
    let before: Vec<Value> = source
        .iter()
        .map(|event| serde_json::to_value(event).unwrap())
        .collect();
    let (harness, _) = candidate(ScriptedModel::new(vec![answer("short", 1)]));

    evaluate(&source, &Candidate::new(harness)).await.unwrap();

    let after: Vec<Value> = source
        .iter()
        .map(|event| serde_json::to_value(event).unwrap())
        .collect();
    assert_eq!(before, after);
}
