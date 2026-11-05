//! Replay-as-evaluation (issue #2001).
//!
//! Each test records a source run on the SQLite engine. It then evaluates
//! that history with a candidate model and checks the report.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::eval::anon::AnonId;
use autumn_harvest_agent::eval::{
    AnonymisedReport, Candidate, DEFAULT_EXTRA_TURNS, Divergence, EvalError, Evaluation, RunEnd,
    Side, Verdict, compare, compare_anonymised, evaluate,
};
use autumn_harvest_agent::followup::FOLLOWUP_TOOL;
use autumn_harvest_agent::memory::{MEMORY_TOOL, MemoryStore};
use autumn_harvest_agent::score::{self, ExactMatch, FiniteF64, ScoreContext, ScoreInput, Scorer};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentStop, AgentTask, Approval, BoxFuture, Ceilings,
    ChatMessage, ChatRequest, ChatResponse, ChatRole, ContentPart, Delivery, ErrorKind, FnTool,
    Followups, InMemoryMemoryStore, MemoryScope, OverlayError, ProfileOverlay, Report, Rule,
    RunInfo, StopReason, TokenUsage, Tool, ToolCall, ToolDecision, ToolEffect, ToolPolicy,
    ToolRules, sqlite,
};
use autumn_harvest_sqlite::RunState;
use common::{
    CountingPolicy, Recorder, Reply, ScriptedModel, answer, calls, recorded_tool, report, runtime,
    tool_results,
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
    let RunEnd::Completed(source_report) = &evaluation.recorded else {
        panic!("the source completed: {:?}", evaluation.recorded);
    };
    assert_eq!(
        tool_results(report),
        tool_results(source_report),
        "the candidate reads the recorded tool output"
    );
    assert!(!evaluation.end_diverged);
    assert_eq!(
        evaluation.overlay_digest,
        ProfileOverlay::default().digest(),
        "a plain candidate runs the empty overlay"
    );
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
    assert_eq!(
        first(&evaluation).verdict,
        Verdict::Diverged(Divergence::Calls)
    );
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
    assert_eq!(
        first(&evaluation).verdict,
        Verdict::Diverged(Divergence::Calls)
    );
    assert_eq!(evaluation.stubbed_tool_calls, 1);
    assert_eq!(recorder.runs(), Vec::<Value>::new(), "no tool runs live");
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
    assert_eq!(
        first(&evaluation).verdict,
        Verdict::Diverged(Divergence::Shape)
    );
    assert_eq!(
        evaluation.turns[2].verdict,
        Verdict::Diverged(Divergence::Missing(Side::Candidate))
    );
    assert_eq!(evaluation.replayed_tool_calls, 1);
    assert_eq!(recorder.runs(), Vec::<Value>::new(), "no tool runs live");
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
    assert_eq!(
        first(&evaluation).verdict,
        Verdict::Diverged(Divergence::Policy)
    );
    assert_eq!(recorder.runs(), Vec::<Value>::new(), "no tool runs live");
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
    let source = record(
        &AgentTask::new("report it").deliver(),
        vec![answer("ok", 1)],
    )
    .await;
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

    assert!(
        matches!(result, Err(EvalError::NotAnAgentRun(_))),
        "{result:?}"
    );
    let empty = evaluate(
        &[],
        &Candidate::new(candidate(ScriptedModel::new(Vec::new())).0),
    )
    .await;
    assert!(
        matches!(empty, Err(EvalError::NotAnAgentRun(_))),
        "{empty:?}"
    );
}

#[tokio::test]
async fn a_current_thread_runtime_is_refused() {
    let source = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::to_value(AgentTask::new("hi")).unwrap(),
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::WorkflowFailed {
            error: "provider down".into(),
            error_type: None,
            details: None,
            non_retryable: None,
        },
    ];
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
    assert_eq!(recorder.runs(), Vec::<Value>::new(), "no tool runs live");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rate_limited_model_turn_retries_as_in_production() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        Err(AgentError::new(ErrorKind::RateLimited, "slow down")),
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]);
    let (harness, _) = candidate(model.clone());

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(model.calls(), 3, "the rate-limited call runs again");
    assert!(!evaluation.turn_cap_reached);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_tool_failure_fails_the_candidate_at_the_same_call() {
    // The source tool activity failed for good, so the run failed.
    let mut source = with_end(
        one_lookup().await,
        Some(WorkflowEvent::WorkflowFailed {
            error: "the tool timed out".into(),
            error_type: None,
            details: None,
            non_retryable: None,
        }),
    );
    let tool_id = source
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if name == "agent_tool_call" => Some(*activity_id),
            _ => None,
        })
        .unwrap();
    let completed = source
        .iter()
        .position(|event| {
            matches!(event, WorkflowEvent::ActivityCompleted { activity_id, .. }
                if *activity_id == tool_id)
        })
        .unwrap();
    source[completed] = WorkflowEvent::ActivityFailed {
        activity_id: tool_id,
        error: "the tool timed out".into(),
        attempt: 1,
        error_type: "Timeout".into(),
        non_retryable: true,
        details: None,
    };
    // Drop the recorded answer turn: the source never reached it.
    let answer_turn = source
        .iter()
        .rposition(|event| {
            matches!(event, WorkflowEvent::ActivityScheduled { name, .. }
                if name == "agent_model_turn")
        })
        .unwrap();
    source.drain(answer_turn..answer_turn + 2);
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]);
    let (harness, recorder) = candidate(model.clone());

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert!(
        matches!(evaluation.candidate, RunEnd::Failed(_)),
        "{:?}",
        evaluation.candidate
    );
    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(model.calls(), 1, "no model call runs past the failure");
    assert_eq!(evaluation.stubbed_tool_calls, 0);
    assert_eq!(recorder.runs(), Vec::<Value>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_turn_over_the_worker_result_cap_ends_as_in_production() {
    let source = one_lookup().await;
    let long = "x".repeat(2_000);
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer(&long, 5),
    ]);
    let (harness, _) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness.max_result_bytes(1_000)))
        .await
        .unwrap();

    // A worker drops the reply and ends the run under a named stop (issue
    // #2040). It does not fail the activity.
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!(
            "the oversized turn ends the run: {:?}",
            evaluation.candidate
        );
    };
    assert_eq!(report.stop, AgentStop::ReplyTooLarge);
    assert!(evaluation.end_diverged);
    // The dropped reply is a divergence, not an empty reworded answer.
    assert_eq!(
        evaluation.turns[1].verdict,
        Verdict::Diverged(Divergence::Stop)
    );
}

/// A worker records the turn with the call ids the provider gave. The
/// evaluation gives a call that differs from the recorded one a new, longer
/// id. The cap must still measure the provider ids (issue #2040).
#[tokio::test(flavor = "multi_thread")]
async fn the_worker_result_cap_measures_the_ids_the_provider_gave() {
    let source = one_lookup().await;
    let request: autumn_harvest_agent::ModelTurnRequest = source
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, input, .. } if name == "agent_model_turn" => {
                Some(serde_json::from_value(input.clone()).unwrap())
            }
            _ => None,
        })
        .expect("the source asks the model");
    // The candidate asks for another call than the source, so its id changes.
    let replies = || {
        vec![
            calls(&[("c1", "lookup", json!({"q": "y"}))], 5),
            answer("done", 5),
        ]
    };
    let (harness, _) = candidate(ScriptedModel::new(replies()));
    let turn = harness.model_turn(request).await.unwrap();
    let as_recorded = autumn_harvest_agent::bounds::json_len(&turn);

    for (cap, stop) in [
        (as_recorded, AgentStop::Completed),
        (as_recorded - 1, AgentStop::ReplyTooLarge),
    ] {
        let (harness, _) = candidate(ScriptedModel::new(replies()));
        let evaluation = evaluate(&source, &Candidate::new(harness.max_result_bytes(cap)))
            .await
            .unwrap();
        let RunEnd::Completed(report) = &evaluation.candidate else {
            panic!("the candidate ends: {:?}", evaluation.candidate);
        };
        assert_eq!(report.stop, stop, "cap {cap}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_redriven_source_that_has_not_ended_is_refused() {
    let failed = WorkflowEvent::WorkflowFailed {
        error: "provider down".into(),
        error_type: None,
        details: None,
        non_retryable: None,
    };
    let mut source = with_end(one_lookup().await, Some(failed));
    let redriven = json!({
        "type": "WorkflowRedriven",
        "data": {
            "redriven_at": chrono::Utc::now(),
            "dead_letter_id": "00000000-0000-0000-0000-000000000000",
        },
    });
    source.push(serde_json::from_value(redriven).unwrap());
    let model = ScriptedModel::new(vec![answer("done", 1)]);
    let (harness, _) = candidate(model.clone());

    let result = evaluate(&source, &Candidate::new(harness)).await;

    assert!(
        matches!(result, Err(EvalError::InFlightSource)),
        "{result:?}"
    );
    assert_eq!(model.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_source_is_refused() {
    let cancelled = WorkflowEvent::WorkflowCancelled {
        reason: "operator".into(),
    };
    let source = with_end(one_lookup().await, Some(cancelled));
    let model = ScriptedModel::new(vec![answer("done", 1)]);
    let (harness, _) = candidate(model.clone());

    let result = evaluate(&source, &Candidate::new(harness)).await;

    assert!(
        matches!(result, Err(EvalError::InterruptedSource(_))),
        "{result:?}"
    );
    assert_eq!(model.calls(), 0, "a refused source costs no model call");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_approval_the_source_never_awaited_is_not_delivered() {
    // The source never gated `lookup`, but its history holds a stray
    // approval for that call.
    let mut source = one_lookup().await;
    let started = source.len() - 1;
    source.insert(
        started,
        WorkflowEvent::SignalReceived {
            signal_name: "tool_approval:0:0:c1".into(),
            payload: serde_json::to_value(Approval::Approve).unwrap(),
        },
    );
    // The candidate policy gates every read.
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]);
    let (harness, recorder) = candidate(model);
    let gate = ToolRules::new().effect(ToolEffect::ReadOnly, Rule::Ask);

    let evaluation = evaluate(&source, &Candidate::new(harness.policy(Arc::new(gate))))
        .await
        .unwrap();

    assert_eq!(evaluation.first_divergence, Some(0));
    assert_eq!(evaluation.replayed_tool_calls, 0, "{evaluation:#?}");
    assert_eq!(recorder.runs(), Vec::<Value>::new());
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    let results = tool_results(report);
    assert!(results[0].contains("no approval arrived"), "{results:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_approval_for_a_wait_the_source_never_opened_is_not_delivered() {
    // The source gates `lookup`, but the token budget ends the run before
    // the approval wait opens. A stray approval for that call follows.
    let (_dir, db) = fresh_db();
    let gate = || Arc::new(ToolRules::new().effect(ToolEffect::ReadOnly, Rule::Ask));
    let source_recorder = Arc::new(Recorder::default());
    let source_model = ScriptedModel::new(vec![calls(&[("c1", "lookup", json!({"q": "x"}))], 50)]);
    let harness = AgentHarness::new(source_model)
        .tool(recorded_tool(
            "lookup",
            ToolEffect::ReadOnly,
            &source_recorder,
        ))
        .policy(gate());
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("look")
        .max_total_tokens(10)
        .approval_timeout(Duration::from_secs(60));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let source_report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(source_report.stop, AgentStop::TokensExhausted);
    let mut source = rt.load_history(exec).unwrap();
    let end = source.len() - 1;
    source.insert(
        end,
        WorkflowEvent::SignalReceived {
            signal_name: "tool_approval:0:0:c1".into(),
            payload: serde_json::to_value(Approval::Approve).unwrap(),
        },
    );
    // The candidate uses fewer tokens, so it reaches the approval wait.
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 1),
        answer("done", 1),
    ]);
    let (harness, recorder) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness.policy(gate())))
        .await
        .unwrap();

    assert_eq!(evaluation.replayed_tool_calls, 0, "{evaluation:#?}");
    assert_eq!(evaluation.stubbed_tool_calls, 0, "{evaluation:#?}");
    assert_eq!(recorder.runs(), Vec::<Value>::new());
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    let results = tool_results(report);
    assert!(results[0].contains("no approval arrived"), "{results:?}");
}

/// Record a source with two gated `pay` calls. The first approval arrives
/// after its deadline. The second arrives in time.
///
/// SQLite drops a signal that arrives after its wait ends. Postgres keeps it
/// in history, unread. So the fixture adds the late signal after the first
/// deadline, where Postgres records it.
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
    sqlite::decide(&mut rt, exec, &second, &Approval::Approve).unwrap();
    let final_report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(final_report.stop, AgentStop::Completed);
    assert_eq!(
        recorder.runs(),
        vec![json!({"amount": 2})],
        "only the call approved in time runs in the source"
    );
    let mut history = rt.load_history(exec).unwrap();
    let deadline = history
        .iter()
        .position(|event| {
            matches!(event, WorkflowEvent::TimerFired { timer_id }
                if timer_id.as_str().ends_with(&late))
        })
        .expect("the first deadline fires in the source");
    history.insert(
        deadline + 1,
        WorkflowEvent::SignalReceived {
            signal_name: late,
            payload: serde_json::to_value(Approval::Approve).unwrap(),
        },
    );
    history
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
    assert_eq!(recorder.runs(), Vec::<Value>::new(), "no tool runs live");
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    let results = tool_results(report);
    assert!(results[0].contains("no approval arrived"), "{results:?}");
    assert!(results[1].contains("echo"), "{results:?}");
}

/// The history of a run that the source ended in some other way: drop the
/// terminal event and append `end`.
fn with_end(mut history: Vec<WorkflowEvent>, end: Option<WorkflowEvent>) -> Vec<WorkflowEvent> {
    let last = history.pop();
    assert!(
        matches!(last, Some(WorkflowEvent::WorkflowCompleted { .. })),
        "{last:?}"
    );
    history.extend(end);
    history
}

#[tokio::test(flavor = "multi_thread")]
async fn an_in_flight_source_is_refused() {
    let source = with_end(one_lookup().await, None);
    let model = ScriptedModel::new(vec![answer("done", 1)]);
    let (harness, _) = candidate(model.clone());

    let result = evaluate(&source, &Candidate::new(harness)).await;

    assert!(
        matches!(result, Err(EvalError::InFlightSource)),
        "{result:?}"
    );
    assert_eq!(model.calls(), 0, "a refused source costs no model call");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_source_reports_its_failure() {
    let failed = WorkflowEvent::WorkflowFailed {
        error: "provider down".into(),
        error_type: None,
        details: None,
        non_retryable: None,
    };
    let source = with_end(one_lookup().await, Some(failed));
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]);
    let (harness, _) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert_eq!(evaluation.recorded, RunEnd::Failed("provider down".into()));
    assert!(
        evaluation.end_diverged,
        "a failed source and a completed candidate differ"
    );
    assert!(evaluation.diverged());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_run_stop_diverges_when_every_turn_agrees() {
    let source = record(
        &AgentTask::new("hi").max_total_tokens(100),
        vec![answer("hello", 5)],
    )
    .await;
    // The same answer, but it uses more tokens than the run allows.
    let (harness, _) = candidate(ScriptedModel::new(vec![answer("hello", 500)]));

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert_eq!(evaluation.turns[0].verdict, Verdict::Same);
    assert_eq!(evaluation.first_divergence, None);
    assert!(evaluation.end_diverged, "{evaluation:#?}");
    assert!(evaluation.diverged());
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    assert_eq!(report.stop, AgentStop::TokensExhausted);
}

/// A policy that allows every call and keeps the run id it sees.
#[derive(Debug, Default)]
struct RunIds(Mutex<Vec<String>>);

impl ToolPolicy for RunIds {
    fn decide<'a>(
        &'a self,
        _call: &'a ToolCall,
        _tool: Option<&'a dyn Tool>,
        info: &'a RunInfo,
    ) -> BoxFuture<'a, ToolDecision> {
        self.0.lock().unwrap().push(info.run_id.as_str().to_owned());
        Box::pin(async { ToolDecision::Allow })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_candidate_policy_sees_the_recorded_run_id() {
    let source = one_lookup().await;
    let recorded_run_id = source
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, input, .. } if name == "agent_model_turn" => {
                Some(input["run_id"].as_str().unwrap().to_owned())
            }
            _ => None,
        })
        .unwrap();
    let policy = Arc::new(RunIds::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]);
    let (harness, _) = candidate(model);

    evaluate(&source, &Candidate::new(harness.policy(policy.clone())))
        .await
        .unwrap();

    assert_eq!(*policy.0.lock().unwrap(), vec![recorded_run_id]);
}

/// A policy that allows every call and keeps the call ids it sees.
#[derive(Debug, Default)]
struct CallIds(Mutex<Vec<String>>);

impl ToolPolicy for CallIds {
    fn decide<'a>(
        &'a self,
        call: &'a ToolCall,
        _tool: Option<&'a dyn Tool>,
        _info: &'a RunInfo,
    ) -> BoxFuture<'a, ToolDecision> {
        self.0.lock().unwrap().push(call.id.clone());
        Box::pin(async { ToolDecision::Allow })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_candidate_policy_sees_the_recorded_call_ids() {
    let source = one_lookup().await;
    let policy = Arc::new(CallIds::default());
    // The same calls with new provider ids.
    let model = ScriptedModel::new(vec![
        calls(
            &[
                ("n1", "lookup", json!({"q": "x"})),
                ("n2", "lookup", json!({"q": "z"})),
            ],
            5,
        ),
        answer("done", 5),
    ]);
    let (harness, _) = candidate(model);

    evaluate(&source, &Candidate::new(harness.policy(policy.clone())))
        .await
        .unwrap();

    assert_eq!(
        *policy.0.lock().unwrap(),
        vec!["c1".to_owned(), "eval_0_1".to_owned()],
        "the policy sees the ids that the transcript records"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_approval_never_releases_another_call_with_a_reused_id() {
    // The source approves `pay 5` with an edit. The provider reuses ids.
    let (_dir, db) = fresh_db();
    let (harness, source_recorder, _) = gated_candidate(vec![
        calls(&[("call_0", "pay", json!({"amount": 5}))], 1),
        answer("paid", 1),
    ]);
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("pay").approval_timeout(Duration::from_secs(60));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("expected an approval wait");
    };
    let edit = Approval::Edit {
        arguments: json!({"amount": 1}),
    };
    sqlite::decide(&mut rt, exec, &signal, &edit).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());
    let source = rt.load_history(exec).unwrap();
    assert_eq!(source_recorder.runs(), vec![json!({"amount": 1})]);

    // The candidate asks for another amount under the same provider id.
    let (harness, candidate_recorder, _) = gated_candidate(vec![
        calls(&[("call_0", "pay", json!({"amount": 999}))], 1),
        answer("paid", 1),
    ]);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert_eq!(
        first(&evaluation).verdict,
        Verdict::Diverged(Divergence::Calls)
    );
    assert_eq!(evaluation.replayed_tool_calls, 0, "{evaluation:#?}");
    assert_eq!(candidate_recorder.runs(), Vec::<Value>::new());
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    let results = tool_results(report);
    assert!(results[0].contains("no approval arrived"), "{results:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_turn_cap_stops_the_candidate_model() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]);
    let (harness, _) = candidate(model.clone());

    let evaluation = evaluate(&source, &Candidate::new(harness).max_turns(1))
        .await
        .unwrap();

    assert_eq!(model.calls(), 1);
    assert!(evaluation.turn_cap_reached);
    assert!(
        matches!(evaluation.candidate, RunEnd::Failed(_)),
        "{:?}",
        evaluation.candidate
    );
    assert_eq!(
        evaluation.turns[1].verdict,
        Verdict::Diverged(Divergence::Missing(Side::Candidate))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_default_turn_cap_bounds_a_looping_candidate() {
    let source = record(
        &AgentTask::new("look it up").max_steps(100),
        vec![
            calls(&[("c1", "lookup", json!({"q": "x"}))], 1),
            answer("done", 1),
        ],
    )
    .await;
    let replies = (0..50)
        .map(|n| calls(&[("c", "lookup", json!({"q": n}))], 1))
        .collect();
    let model = ScriptedModel::new(replies);
    let (harness, _) = candidate(model.clone());

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert_eq!(model.calls(), 2 + DEFAULT_EXTRA_TURNS);
    assert!(evaluation.turn_cap_reached);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_candidate_with_more_turns_reports_missing_recorded() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        calls(&[("c2", "lookup", json!({"q": "y"}))], 5),
        answer("done", 5),
    ]);
    let (harness, _) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert_eq!(evaluation.turns.len(), 3, "{evaluation:#?}");
    assert_eq!(
        evaluation.turns[1].verdict,
        Verdict::Diverged(Divergence::Shape)
    );
    assert_eq!(
        evaluation.turns[2].verdict,
        Verdict::Diverged(Divergence::Missing(Side::Recorded))
    );
    assert!(evaluation.turns[2].recorded.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn one_recorded_output_answers_one_identical_call() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        calls(
            &[
                ("c1", "lookup", json!({"q": "x"})),
                ("c2", "lookup", json!({"q": "x"})),
            ],
            5,
        ),
        answer("done", 5),
    ]);
    let (harness, _) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert_eq!(evaluation.replayed_tool_calls, 1);
    assert_eq!(evaluation.stubbed_tool_calls, 1);
}

/// A read-only tool that answers 1, then 2, and so on.
fn counter_tool(name: &str) -> Arc<dyn Tool> {
    let count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    FnTool::new(
        name,
        "A counter.",
        json!({"type": "object"}),
        move |_input: Value| {
            let count = Arc::clone(&count);
            async move {
                let n = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                Ok(json!({ "count": n }))
            }
        },
    )
    .effect(ToolEffect::ReadOnly)
    .shared()
}

#[tokio::test(flavor = "multi_thread")]
async fn identical_calls_take_their_outputs_in_recorded_order() {
    let script = || {
        vec![
            calls(&[("c1", "count", json!({})), ("c2", "count", json!({}))], 1),
            answer("done", 1),
        ]
    };
    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(ScriptedModel::new(script())).tool(counter_tool("count"));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("count twice")).unwrap();
    let source_report = report(rt.run_until_blocked(exec).await.unwrap());
    let source = rt.load_history(exec).unwrap();
    let harness = AgentHarness::new(ScriptedModel::new(script())).tool(counter_tool("count"));

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    let RunEnd::Completed(candidate_report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    assert_eq!(tool_results(candidate_report), tool_results(&source_report));
    assert_eq!(evaluation.replayed_tool_calls, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn each_segment_sees_its_recorded_memory_snapshot() {
    let script = || {
        vec![
            calls(
                &[
                    (
                        "m",
                        MEMORY_TOOL,
                        json!({"action": "add", "block": "user", "text": "Prefers tea."}),
                    ),
                    (
                        "f",
                        FOLLOWUP_TOOL,
                        json!({"prompt": "check", "delay_minutes": 20}),
                    ),
                ],
                1,
            ),
            answer("later", 1),
            calls(&[("c1", "lookup", json!({"q": "x"}))], 1),
            answer("done", 1),
        ]
    };
    let scope = MemoryScope::new("u");
    let task = AgentTask::new("go")
        .system("Be brief.")
        .memory(scope.clone())
        .followups(Followups::new(Duration::from_secs(3_600)));
    let (_dir, db) = fresh_db();
    let source_model = ScriptedModel::new(script());
    let source_recorder = Arc::new(Recorder::default());
    let source_harness = AgentHarness::new(source_model.clone())
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .tool(recorded_tool(
            "lookup",
            ToolEffect::ReadOnly,
            &source_recorder,
        ));
    let mut rt = runtime(&db, source_harness);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let state = rt
        .run_until_blocked_as_of(exec, later(25 * 60))
        .await
        .unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let source = rt.load_history(exec).unwrap();
    let source_requests = source_model.requests();
    assert_eq!(source_requests.len(), 4);

    let model = ScriptedModel::new(script());
    let store = Arc::new(InMemoryMemoryStore::new());
    let (harness, recorder) = candidate(model.clone());

    let evaluation = evaluate(&source, &Candidate::new(harness.memory(store.clone())))
        .await
        .unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(evaluation.turns.len(), 4);
    assert_eq!(evaluation.replayed_tool_calls, 2, "{evaluation:#?}");
    assert_eq!(evaluation.stubbed_tool_calls, 0);
    let requests = model.requests();
    assert_eq!(requests[0].messages[0], source_requests[0].messages[0]);
    assert_eq!(requests[2].messages[0], source_requests[2].messages[0]);
    assert!(format!("{:?}", requests[2].messages[0]).contains("Prefers tea."));
    assert_eq!(recorder.runs(), Vec::<Value>::new());
    let blocks = store.load(&scope).await.unwrap();
    assert!(
        blocks.iter().all(|block| block.entries.is_empty()),
        "the memory write never runs live"
    );
}

/// A delegation run evaluates with no roster on the candidate: the recorded
/// peer lookup answers it (agent mesh, AC13).
#[tokio::test(flavor = "multi_thread")]
async fn a_delegation_run_evaluates_without_divergence() {
    use autumn_harvest_agent::mesh::DELEGATE_TOOL;
    use autumn_harvest_agent::{Mandate, Peer};

    let (_dir, db) = fresh_db();
    let script = || {
        vec![
            calls(
                &[(
                    "d1",
                    DELEGATE_TOOL,
                    json!({"agent": "researcher", "task": "dig"}),
                )],
                1,
            ),
            answer("dug", 1),
            answer("done", 1),
        ]
    };
    let harness = AgentHarness::new(ScriptedModel::new(script()))
        .peer(Peer::new("researcher", "Finds facts."));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("go").mandate(Mandate::new("planner").peers(["researcher"]));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let source = rt.load_history(exec).unwrap();

    let model = ScriptedModel::new(script());
    let evaluation = evaluate(&source, &Candidate::new(AgentHarness::new(model.clone())))
        .await
        .unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(evaluation.turns.len(), 3);
    assert_eq!(model.calls(), 3);
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    assert_eq!(report.delegations.len(), 1);
}

/// In a delegation run, a recorded tool outcome answers only an equal call of
/// the same agent (agent mesh, AC13).
#[tokio::test(flavor = "multi_thread")]
async fn a_child_call_takes_the_outcome_of_the_same_agent() {
    use autumn_harvest_agent::mesh::DELEGATE_TOOL;
    use autumn_harvest_agent::{Mandate, Peer, ToolContext};

    let whoami = || {
        FnTool::with_context(
            "whoami",
            "Name the run.",
            json!({"type": "object"}),
            |_input, ctx: ToolContext| async move { Ok(json!(ctx.run_id.as_str())) },
        )
        .effect(ToolEffect::ReadOnly)
        .shared()
    };
    let script = || {
        vec![
            calls(&[("c1", "whoami", json!({}))], 1),
            calls(
                &[(
                    "d1",
                    DELEGATE_TOOL,
                    json!({"agent": "researcher", "task": "who?"}),
                )],
                1,
            ),
            calls(&[("c1", "whoami", json!({}))], 1),
            answer("child", 1),
            answer("parent", 1),
        ]
    };
    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(ScriptedModel::new(script()))
        .tool(whoami())
        .peer(Peer::new("researcher", "Finds facts."));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("go").mandate(Mandate::new("planner").peers(["researcher"]));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let source = rt.load_history(exec).unwrap();

    // The candidate parent skips its own call. Without lanes, the child call
    // would take the parent outcome, which has the same step, name and
    // arguments.
    let model = ScriptedModel::new(script().split_off(1));
    let harness = AgentHarness::new(model.clone()).tool(whoami());
    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert_eq!(evaluation.replayed_tool_calls, 1);
    assert_eq!(evaluation.stubbed_tool_calls, 0);
    // The second child request carries the replayed outcome.
    let child = &model.requests()[2];
    let result = child
        .messages
        .last()
        .and_then(|message| {
            message.content.iter().find_map(|part| match part {
                ContentPart::ToolResult { content, .. } => Some(content.clone()),
                _ => None,
            })
        })
        .expect("the child reads its tool result");
    assert!(
        result.contains(":d1"),
        "the child gets its own outcome: {result}"
    );
}

/// A nested and a refused delegation evaluate with no roster on the
/// candidate (agent mesh, AC13).
#[tokio::test(flavor = "multi_thread")]
async fn nested_and_refused_delegations_evaluate_without_divergence() {
    use autumn_harvest_agent::mesh::DELEGATE_TOOL;
    use autumn_harvest_agent::{Mandate, Peer};

    let delegate = |id: &str, agent: &str| {
        calls(
            &[(id, DELEGATE_TOOL, json!({"agent": agent, "task": "go"}))],
            1,
        )
    };
    let script = || {
        vec![
            delegate("d1", "researcher"),
            delegate("d2", "critic"),
            answer("checked", 1),
            answer("dug", 1),
            delegate("d3", "ghost"),
            answer("planned", 1),
        ]
    };
    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(ScriptedModel::new(script()))
        .peer(Peer::new("researcher", "R.").peers(["critic"]))
        .peer(Peer::new("critic", "C."));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("go").mandate(
        Mandate::new("planner")
            .peers(["researcher", "critic", "ghost"])
            .max_depth(2),
    );
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let source = rt.load_history(exec).unwrap();

    let model = ScriptedModel::new(script());
    let evaluation = evaluate(&source, &Candidate::new(AgentHarness::new(model.clone())))
        .await
        .unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(model.calls(), 6);
    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    let RunEnd::Completed(recorded) = &evaluation.recorded else {
        panic!("the source completes");
    };
    // The candidate engine has its own execution id, so compare each record
    // with the lane of its run id only.
    let shape = |report: &autumn_harvest_agent::AgentReport| {
        report
            .delegations
            .iter()
            .map(|record| {
                (
                    record.chain.clone(),
                    record.stop,
                    record.refusal,
                    record.usage,
                    record
                        .run_id
                        .as_deref()
                        .and_then(|id| id.rsplit_once(':'))
                        .map(|(_, lane)| lane.to_owned()),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(shape(report), shape(recorded));
    assert_eq!(report.delegations.len(), 3);
}

/// A recorded approval of a child call never releases a candidate call of
/// another agent, even at the same round, position and arguments.
#[tokio::test(flavor = "multi_thread")]
async fn a_child_approval_never_releases_a_root_call_in_the_candidate() {
    use autumn_harvest_agent::mesh::DELEGATE_TOOL;
    use autumn_harvest_agent::{Mandate, Peer};

    let (_dir, db) = fresh_db();
    let source_model = ScriptedModel::new(vec![
        calls(
            &[(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "researcher", "task": "pay"}),
            )],
            1,
        ),
        calls(&[("c1", "pay", json!({"amount": 1}))], 1),
        answer("paid", 1),
        answer("done", 1),
    ]);
    let source_harness = AgentHarness::new(source_model)
        .tool(recorded_tool(
            "pay",
            ToolEffect::Write,
            &Arc::new(Recorder::default()),
        ))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .peer(Peer::new("researcher", "R."));
    let mut rt = runtime(&db, source_harness);
    let task = AgentTask::new("go").mandate(Mandate::new("planner").peers(["researcher"]));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the child pay waits");
    };
    assert_eq!(signal, "tool_approval:1:0:c1");
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let source = rt.load_history(exec).unwrap();

    // The candidate root does not delegate. Its second turn makes the same
    // call as the recorded child, in the same tool slot of the tree.
    let (harness, _recorder, _model) = gated_candidate(vec![
        calls(&[("x0", "lookup", json!({}))], 1),
        calls(&[("x1", "pay", json!({"amount": 1}))], 1),
        answer("finished", 1),
    ]);
    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    let RunEnd::Completed(report) = &evaluation.candidate else {
        panic!("the candidate completes: {:?}", evaluation.candidate);
    };
    let results = tool_results(report);
    assert!(
        results[1].contains("no approval arrived"),
        "the child approval must not release the root call: {results:?}"
    );
}

/// A source that failed for good at its peer lookup fails the candidate at
/// the same lookup. No model call runs past it (agent mesh, AC13).
#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_lookup_failure_fails_the_candidate_at_the_same_lookup() {
    use autumn_harvest_agent::Mandate;

    let task = AgentTask::new("go").mandate(Mandate::new("planner").peers(["researcher"]));
    let mut source = record(&task, vec![answer("done", 1)]).await;
    let lookup = source
        .iter()
        .position(|event| {
            matches!(event, WorkflowEvent::ActivityScheduled { name, .. }
                if name == "agent_discover_peers")
        })
        .unwrap();
    let WorkflowEvent::ActivityScheduled { activity_id, .. } = source[lookup].clone() else {
        unreachable!();
    };
    let completed = source
        .iter()
        .position(|event| {
            matches!(event, WorkflowEvent::ActivityCompleted { activity_id: id, .. }
                if *id == activity_id)
        })
        .unwrap();
    source.truncate(completed);
    source.push(WorkflowEvent::ActivityFailed {
        activity_id,
        error: "no agent harness is registered".into(),
        attempt: 3,
        error_type: "Error".into(),
        non_retryable: true,
        details: None,
    });
    source.push(WorkflowEvent::WorkflowFailed {
        error: "no agent harness is registered".into(),
        error_type: None,
        details: None,
        non_retryable: None,
    });
    let model = ScriptedModel::new(vec![answer("done", 1)]);
    let (harness, _) = candidate(model.clone());

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert!(
        matches!(evaluation.candidate, RunEnd::Failed(_)),
        "the candidate fails at the lookup: {:?}",
        evaluation.candidate
    );
    assert_eq!(model.calls(), 0, "no model call runs past the failure");
    assert!(!evaluation.diverged(), "{evaluation:#?}");
}

/// Two peers can hold the same delegation ordinal. A recorded approval and
/// outcome of one peer never answer a call of the other (agent mesh, AC13).
#[tokio::test(flavor = "multi_thread")]
async fn another_peer_at_the_same_ordinal_takes_no_recorded_approval() {
    use autumn_harvest_agent::mesh::DELEGATE_TOOL;
    use autumn_harvest_agent::{Mandate, Peer};

    let delegate_to = |peer: &str| {
        calls(
            &[("d1", DELEGATE_TOOL, json!({"agent": peer, "task": "pay"}))],
            1,
        )
    };
    let (_dir, db) = fresh_db();
    let source_model = ScriptedModel::new(vec![
        delegate_to("researcher"),
        calls(&[("c1", "pay", json!({"amount": 1}))], 1),
        answer("paid", 1),
        answer("done", 1),
    ]);
    let source_harness = AgentHarness::new(source_model)
        .tool(recorded_tool(
            "pay",
            ToolEffect::Write,
            &Arc::new(Recorder::default()),
        ))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .peer(Peer::new("researcher", "R."))
        .peer(Peer::new("critic", "C."));
    let mut rt = runtime(&db, source_harness);
    let task =
        AgentTask::new("go").mandate(Mandate::new("planner").peers(["researcher", "critic"]));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the researcher pay waits");
    };
    assert_eq!(signal, "tool_approval:1:0:c1");
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let source = rt.load_history(exec).unwrap();

    // The candidate asks critic instead. Critic runs as `:d1` too, and makes
    // the same call in the same slot.
    let (harness, _recorder, model) = gated_candidate(vec![
        delegate_to("critic"),
        calls(&[("x1", "pay", json!({"amount": 1}))], 1),
        answer("paid", 1),
        answer("done", 1),
    ]);
    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert!(
        matches!(evaluation.candidate, RunEnd::Completed(_)),
        "{:?}",
        evaluation.candidate
    );
    assert_eq!(
        evaluation.replayed_tool_calls, 0,
        "researcher's outcome must not answer critic: {evaluation:#?}"
    );
    let after_pay = format!("{:?}", model.requests()[2].messages.last());
    assert!(
        after_pay.contains("no approval arrived"),
        "researcher's approval must not release critic's call: {after_pay}"
    );
}

// ---------------------------------------------------------------------------
// Two models for one agent (issue #2271)
// ---------------------------------------------------------------------------

/// A shared candidate harness with two named models and a `lookup` tool.
fn two_models(
    a: Arc<ScriptedModel>,
    b: Arc<ScriptedModel>,
) -> (Arc<AgentHarness>, Arc<ScriptedModel>) {
    let fallback = ScriptedModel::new(vec![]);
    let recorder = Arc::new(Recorder::default());
    let harness = AgentHarness::new(fallback.clone())
        .model_named("a", a)
        .model_named("b", b)
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder));
    (Arc::new(harness), fallback)
}

#[tokio::test(flavor = "multi_thread")]
async fn two_models_are_compared_on_one_source() {
    let source = one_lookup().await;
    let a = ScriptedModel::named(
        "model-a",
        vec![
            calls(&[("a1", "lookup", json!({"q": "x"}))], 5),
            answer("done", 5),
        ],
    );
    let b = ScriptedModel::named(
        "model-b",
        vec![
            calls(&[("b1", "lookup", json!({"q": "y"}))], 5),
            answer("done", 5),
        ],
    );
    let (harness, fallback) = two_models(a.clone(), b.clone());
    let first = Candidate::shared(Arc::clone(&harness)).model("a");
    let second = Candidate::shared(harness).model("b");

    let comparison = compare(&source, &first, &second).await.unwrap();

    assert!(!comparison.first.diverged(), "model a repeats the source");
    assert_eq!(
        comparison.second.first_divergence,
        Some(0),
        "model b looks up another key"
    );
    assert_eq!(comparison.first_divergence, Some(0));
    assert_eq!(
        comparison.turns[0].verdict,
        Verdict::Diverged(Divergence::Calls)
    );
    assert!(!comparison.end_diverged, "both complete");
    assert_eq!((a.calls(), b.calls(), fallback.calls()), (2, 2, 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn one_agent_of_a_tree_runs_on_another_model() {
    use autumn_harvest_agent::mesh::DELEGATE_TOOL;
    use autumn_harvest_agent::{Mandate, Peer};

    let delegate = || {
        calls(
            &[(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "planner", "task": "plan"}),
            )],
            2,
        )
    };
    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(ScriptedModel::new(vec![
        delegate(),
        answer("the plan", 2),
        answer("done", 2),
    ]))
    .peer(Peer::new("planner", "Plans."));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("go").mandate(Mandate::new("triage").peers(["planner"]));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let source = rt.load_history(exec).unwrap();

    let root = ScriptedModel::new(vec![delegate(), answer("done", 2)]);
    let planner = ScriptedModel::named("planner-b", vec![answer("another plan", 2)]);
    let candidate = AgentHarness::new(root.clone()).model_named("b", planner.clone());
    let evaluation = evaluate(
        &source,
        &Candidate::new(candidate).agent_model("planner", "b"),
    )
    .await
    .unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!((root.calls(), planner.calls()), (2, 1));
}

/// Two delegations of one turn, as `(peer, task)` pairs (issue #2270).
fn fan(a: (&str, &str), b: (&str, &str)) -> Reply {
    use autumn_harvest_agent::mesh::DELEGATE_TOOL;

    calls(
        &[
            ("d1", DELEGATE_TOOL, json!({"agent": a.0, "task": a.1})),
            ("d2", DELEGATE_TOOL, json!({"agent": b.0, "task": b.1})),
        ],
        2,
    )
}

/// A completed source whose root fans out to `flights` with "fly" and to
/// `hotels` with "stay". Each peer runs on its own model, so the answer of
/// each child is fixed.
async fn fan_out_source() -> Vec<WorkflowEvent> {
    use autumn_harvest_agent::{Mandate, Peer};

    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(ScriptedModel::new(vec![
        fan(("flights", "fly"), ("hotels", "stay")),
        answer("booked", 1),
    ]))
    .model_named("f", ScriptedModel::named("f", vec![answer("a flight", 1)]))
    .model_named("h", ScriptedModel::named("h", vec![answer("a hotel", 1)]))
    .peer(Peer::new("flights", "Flies.").model("f"))
    .peer(Peer::new("hotels", "Stays.").model("h"));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("book")
        .mandate(Mandate::new("planner").peers(["flights", "hotels"]))
        .parallel_children(2);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    rt.load_history(exec).unwrap()
}

/// The `agent` and `answer` fields of each delegation result of `end`.
fn child_answers(end: &RunEnd) -> Vec<(String, String)> {
    let RunEnd::Completed(report) = end else {
        panic!("the candidate completes: {end:?}");
    };
    tool_results(report)
        .iter()
        .map(|text| {
            let result: Value = serde_json::from_str(text).expect("a delegation result is JSON");
            let field = |name: &str| result[name].as_str().unwrap_or_default().to_owned();
            (field("agent"), field("answer"))
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fan_out_source_answers_each_child_from_its_recorded_run() {
    let source = fan_out_source().await;

    // The candidate asks only for the root turns. The children answer from
    // the recorded runs.
    let model = ScriptedModel::new(vec![
        fan(("flights", "fly"), ("hotels", "stay")),
        answer("booked", 1),
    ]);
    let evaluation = evaluate(&source, &Candidate::new(AgentHarness::new(model.clone())))
        .await
        .unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(model.calls(), 2, "no child turn runs live");
    assert_eq!(
        child_answers(&evaluation.candidate),
        [
            ("flights".to_owned(), "a flight".to_owned()),
            ("hotels".to_owned(), "a hotel".to_owned())
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reordered_fan_out_answers_each_child_from_its_own_recorded_run() {
    let source = fan_out_source().await;

    // The candidate delegates the same tasks in the other order. Each child
    // still gets the run of its own peer and task.
    let model = ScriptedModel::new(vec![
        fan(("hotels", "stay"), ("flights", "fly")),
        answer("booked", 1),
    ]);
    let evaluation = evaluate(&source, &Candidate::new(AgentHarness::new(model.clone())))
        .await
        .unwrap();

    assert_eq!(model.calls(), 2, "no child turn runs live");
    assert_eq!(
        child_answers(&evaluation.candidate),
        [
            ("hotels".to_owned(), "a hotel".to_owned()),
            ("flights".to_owned(), "a flight".to_owned())
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fan_out_child_with_no_recorded_run_fails_the_candidate() {
    use autumn_harvest_agent::eval::NO_CHILD_RUN;

    let source = fan_out_source().await;

    // The candidate changes the task of one child. No recorded run answers
    // it, so the candidate fails and does not read the answer of another.
    let model = ScriptedModel::new(vec![
        fan(("flights", "fly"), ("hotels", "stay near the airport")),
        answer("booked", 1),
    ]);
    let evaluation = evaluate(&source, &Candidate::new(AgentHarness::new(model.clone())))
        .await
        .unwrap();

    let RunEnd::Failed(error) = &evaluation.candidate else {
        panic!("the candidate fails: {:?}", evaluation.candidate);
    };
    assert!(error.contains(NO_CHILD_RUN), "{error}");
    assert!(evaluation.diverged(), "{evaluation:#?}");
}

// ---------------------------------------------------------------------------
// A whole overlay (issue #2375)
// ---------------------------------------------------------------------------

/// The scripted replies that repeat the source of [`one_lookup`].
fn one_lookup_replies() -> Vec<Reply> {
    vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]
}

/// The description of the tool `name` in `request`.
fn description(request: &autumn_harvest_agent::ChatRequest, name: &str) -> String {
    request
        .tools
        .iter()
        .find(|tool| tool.name == name)
        .unwrap_or_else(|| panic!("the model sees {name}"))
        .description
        .clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_overlay_system_reaches_the_model_on_every_turn() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(one_lookup_replies());
    let (harness, _) = candidate(model.clone());
    let overlay = ProfileOverlay {
        system: Some("Answer in French.".to_owned()),
        ..ProfileOverlay::default()
    };

    let evaluation = evaluate(&source, &Candidate::new(harness).overlay(overlay.clone()))
        .await
        .unwrap();

    let requests = model.requests();
    assert_eq!(requests.len(), 2, "{evaluation:#?}");
    for request in &requests {
        assert_eq!(request.messages[0].role, ChatRole::System);
        assert_eq!(
            request.messages[0].content,
            vec![ContentPart::Text("Answer in French.".into())]
        );
    }
    assert_eq!(evaluation.overlay_digest, overlay.digest());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_widening_overlay_is_refused_before_any_model_call() {
    let source = one_lookup().await;
    let cases = [
        (
            ProfileOverlay {
                max_steps: Some(9_999),
                ..ProfileOverlay::default()
            },
            OverlayError::AboveCeiling { field: "max_steps" },
        ),
        (
            ProfileOverlay {
                model: Some("other".to_owned()),
                ..ProfileOverlay::default()
            },
            OverlayError::ModelNotAllowed {
                model: "other".to_owned(),
            },
        ),
        (
            ProfileOverlay {
                tool_guidance: [("missing".to_owned(), "Use it.".to_owned())].into(),
                ..ProfileOverlay::default()
            },
            OverlayError::UnknownTool {
                tool: "missing".to_owned(),
            },
        ),
    ];
    for (overlay, expected) in cases {
        let model = ScriptedModel::new(one_lookup_replies());
        let (harness, recorder) = candidate(model.clone());
        let harness = harness.model_named("other", ScriptedModel::new(Vec::new()));

        let result = evaluate(&source, &Candidate::new(harness).overlay(overlay)).await;

        let Err(EvalError::Overlay(error)) = result else {
            panic!("the overlay is refused: {result:?}");
        };
        assert_eq!(error, expected);
        assert_eq!(model.calls(), 0, "a refused overlay costs no model call");
        assert_eq!(recorder.runs(), Vec::<Value>::new());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_overlay_replays_the_source() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(one_lookup_replies());
    let (harness, _) = candidate(model.clone());

    let evaluation = evaluate(
        &source,
        &Candidate::new(harness).overlay(ProfileOverlay::default()),
    )
    .await
    .unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(evaluation.stubbed_tool_calls, 0);
    assert_eq!(evaluation.replayed_tool_calls, 1);
    assert_eq!(
        evaluation.overlay_digest,
        ProfileOverlay::default().digest()
    );
    assert_eq!(model.calls(), 2);
    let (RunEnd::Completed(recorded), RunEnd::Completed(candidate)) =
        (&evaluation.recorded, &evaluation.candidate)
    else {
        panic!("both complete: {evaluation:#?}");
    };
    assert_eq!(
        candidate.run_id, recorded.run_id,
        "the candidate report shows the recorded run id"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tool_guidance_reaches_the_definitions_and_keeps_the_matching() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(one_lookup_replies());
    let (harness, recorder) = candidate(model.clone());
    let overlay = ProfileOverlay {
        tool_guidance: [("lookup".to_owned(), "Look up one key at a time.".to_owned())].into(),
        ..ProfileOverlay::default()
    };

    let evaluation = evaluate(&source, &Candidate::new(harness).overlay(overlay))
        .await
        .unwrap();

    let requests = model.requests();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        for tool in &request.tools {
            let want = if tool.name == "lookup" {
                "Look up one key at a time."
            } else {
                "A test tool."
            };
            assert_eq!(tool.description, want, "{}", tool.name);
        }
    }
    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(
        (
            evaluation.replayed_tool_calls,
            evaluation.stubbed_tool_calls
        ),
        (1, 0),
        "the recorded outcome still answers the call"
    );
    assert_eq!(recorder.runs(), Vec::<Value>::new(), "no tool runs live");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_system_prompt_is_a_one_part_overlay() {
    let source = one_lookup().await;
    let run = |candidate: fn(AgentHarness) -> Candidate| {
        let source = source.clone();
        async move {
            let model = ScriptedModel::new(one_lookup_replies());
            let (harness, _) = self::candidate(model.clone());
            let evaluation = evaluate(&source, &candidate(harness)).await.unwrap();
            (evaluation, model.requests())
        }
    };

    let short = run(|harness| Candidate::new(harness).system_prompt("Answer in French.")).await;
    let whole = run(|harness| {
        Candidate::new(harness).overlay(ProfileOverlay {
            system: Some("Answer in French.".to_owned()),
            ..ProfileOverlay::default()
        })
    })
    .await;

    assert_eq!(short.0, whole.0);
    assert_eq!(short.1, whole.1, "the model sees the same requests");
}

#[tokio::test(flavor = "multi_thread")]
async fn compare_takes_two_overlay_candidates() {
    let source = one_lookup().await;
    let first_model = ScriptedModel::new(one_lookup_replies());
    let second_model = ScriptedModel::new(vec![
        calls(&[("c9", "lookup", json!({"q": "y"}))], 5),
        answer("done", 5),
    ]);
    let terse = ProfileOverlay {
        system: Some("Be terse.".to_owned()),
        ..ProfileOverlay::default()
    };
    let guided = ProfileOverlay {
        max_steps: Some(2),
        tool_guidance: [("lookup".to_owned(), "Ask for y.".to_owned())].into(),
        ..ProfileOverlay::default()
    };
    let first = Candidate::new(candidate(first_model.clone()).0).overlay(terse.clone());
    let second = Candidate::new(candidate(second_model.clone()).0).overlay(guided.clone());

    let comparison = compare(&source, &first, &second).await.unwrap();

    assert_eq!(comparison.first.overlay_digest, terse.digest());
    assert_eq!(comparison.second.overlay_digest, guided.digest());
    assert_ne!(terse.digest(), guided.digest());
    assert!(!comparison.first.diverged());
    assert_eq!(comparison.first_divergence, Some(0));
    assert_eq!(
        comparison.turns[0].verdict,
        Verdict::Diverged(Divergence::Calls)
    );
    assert_eq!(
        description(&second_model.requests()[0], "lookup"),
        "Ask for y."
    );
    assert_eq!(comparison.second.stubbed_tool_calls, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_model_shorthand_is_an_admitted_overlay_model() {
    let source = one_lookup().await;
    let a = ScriptedModel::named("model-a", one_lookup_replies());
    let (harness, fallback) = two_models(a.clone(), ScriptedModel::new(Vec::new()));

    let evaluation = evaluate(&source, &Candidate::shared(Arc::clone(&harness)).model("a"))
        .await
        .unwrap();
    let named = ProfileOverlay {
        model: Some("a".to_owned()),
        ..ProfileOverlay::default()
    };
    assert_eq!(evaluation.overlay_digest, named.digest());
    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!((a.calls(), fallback.calls()), (2, 0));

    // An overlay that a model wrote must not pick another model.
    let refused = evaluate(
        &source,
        &Candidate::shared(Arc::clone(&harness)).overlay(named),
    )
    .await;
    assert!(
        matches!(
            refused,
            Err(EvalError::Overlay(OverlayError::ModelNotAllowed { .. }))
        ),
        "{refused:?}"
    );
    // A shorthand model that the harness does not hold fails before any call.
    let unknown = evaluate(&source, &Candidate::shared(harness).model("missing")).await;
    assert!(
        matches!(
            unknown,
            Err(EvalError::Overlay(OverlayError::UnknownModel { .. }))
        ),
        "{unknown:?}"
    );
    assert_eq!((a.calls(), fallback.calls()), (2, 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn operator_ceilings_admit_a_wider_overlay() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(one_lookup_replies());
    let (harness, _) = candidate(model.clone());
    let overlay = ProfileOverlay {
        max_steps: Some(20),
        ..ProfileOverlay::default()
    };
    let ceilings = Ceilings {
        max_steps: Some(40),
        max_total_tokens: None,
        max_output_tokens: None,
        models: [autumn_harvest_agent::DEFAULT_MODEL.to_owned()].into(),
        allow_clear_read_only: false,
    };

    let evaluation = evaluate(
        &source,
        &Candidate::new(harness).overlay(overlay).ceilings(ceilings),
    )
    .await
    .unwrap();

    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(model.calls(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_overlay_after_a_shorthand_replaces_it() {
    let source = one_lookup().await;
    let a = ScriptedModel::named("model-a", one_lookup_replies());
    let fallback = ScriptedModel::new(one_lookup_replies());
    let recorder = Arc::new(Recorder::default());
    let harness = AgentHarness::new(fallback.clone())
        .model_named("a", a.clone())
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder));

    let evaluation = evaluate(
        &source,
        &Candidate::new(harness)
            .model("a")
            .system_prompt("Old.")
            .overlay(ProfileOverlay::default()),
    )
    .await
    .unwrap();

    assert_eq!(
        evaluation.overlay_digest,
        ProfileOverlay::default().digest()
    );
    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(
        (a.calls(), fallback.calls()),
        (0, 2),
        "the overlay drops the route of the shorthand"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bounded_read_only_source_refuses_a_wider_overlay() {
    let mut task = AgentTask::new("look it up")
        .system("Be brief.")
        .max_total_tokens(1_000);
    task.read_only = true;
    let source = record(&task, one_lookup_replies()).await;
    for (overlay, expected) in [
        (
            ProfileOverlay {
                read_only: Some(false),
                ..ProfileOverlay::default()
            },
            OverlayError::ClearsReadOnly,
        ),
        (
            ProfileOverlay {
                max_total_tokens: Some(1_001),
                ..ProfileOverlay::default()
            },
            OverlayError::AboveCeiling {
                field: "max_total_tokens",
            },
        ),
    ] {
        let model = ScriptedModel::new(one_lookup_replies());
        let (harness, _) = candidate(model.clone());

        let result = evaluate(&source, &Candidate::new(harness).overlay(overlay)).await;

        let Err(EvalError::Overlay(error)) = result else {
            panic!("the overlay is refused: {result:?}");
        };
        assert_eq!(error, expected);
        assert_eq!(model.calls(), 0);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn compare_refuses_a_bad_second_overlay_before_any_model_call() {
    let source = one_lookup().await;
    let first_model = ScriptedModel::new(one_lookup_replies());
    let second_model = ScriptedModel::new(one_lookup_replies());
    let first = Candidate::new(candidate(first_model.clone()).0);
    let second = Candidate::new(candidate(second_model.clone()).0).overlay(ProfileOverlay {
        max_steps: Some(9_999),
        ..ProfileOverlay::default()
    });

    let result = compare(&source, &first, &second).await;

    assert!(
        matches!(
            result,
            Err(EvalError::Overlay(OverlayError::AboveCeiling { .. }))
        ),
        "{result:?}"
    );
    assert_eq!(
        (first_model.calls(), second_model.calls()),
        (0, 0),
        "the first candidate never runs"
    );
}

// ---------------------------------------------------------------------------
// Anonymised comparison reports (issue #2376)
// ---------------------------------------------------------------------------

/// The identity of one pool member. Each string is distinct, so a search
/// of the text finds only a real leak.
struct Member {
    route: &'static str,
    model_id: &'static str,
    label: &'static str,
    overlay: ProfileOverlay,
    replies: fn() -> Vec<Reply>,
    /// A route for the `planner` agent, with its model id.
    planner: Option<(&'static str, &'static str)>,
    max_turns: Option<usize>,
}

/// The replies of a model that looks up another key.
fn other_key_replies() -> Vec<Reply> {
    vec![
        calls(&[("c9", "lookup", json!({"q": "y"}))], 5),
        answer("done", 5),
    ]
}

fn alpha() -> Member {
    Member {
        route: "route-alpha-7f3a",
        model_id: "model-alpha-id-3c1e",
        label: "triage-overlay-v7",
        overlay: ProfileOverlay {
            system: Some("Be terse.".to_owned()),
            ..ProfileOverlay::default()
        },
        replies: one_lookup_replies,
        planner: Some(("route-planner-5e0c", "model-planner-id-8f2a")),
        max_turns: None,
    }
}

fn beta() -> Member {
    Member {
        route: "route-beta-9d2b",
        model_id: "model-beta-id-6a4f",
        label: "triage-overlay-v8",
        overlay: ProfileOverlay {
            tool_guidance: [("lookup".to_owned(), "Ask for y.".to_owned())].into(),
            ..ProfileOverlay::default()
        },
        replies: other_key_replies,
        planner: None,
        max_turns: None,
    }
}

fn gamma() -> Member {
    Member {
        route: "route-gamma-1e8c",
        model_id: "model-gamma-id-2b7d",
        label: "triage-overlay-v9",
        overlay: ProfileOverlay {
            max_steps: Some(2),
            ..ProfileOverlay::default()
        },
        replies: one_lookup_replies,
        planner: None,
        max_turns: None,
    }
}

/// A member that the turn cap stops after one turn, so its end diverges.
fn delta() -> Member {
    Member {
        route: "route-delta-4c6e",
        model_id: "model-delta-id-0d9b",
        label: "triage-overlay-v10",
        overlay: ProfileOverlay::default(),
        replies: one_lookup_replies,
        planner: None,
        max_turns: Some(1),
    }
}

/// Fresh candidates for `members`, in order, on one shared harness. Each
/// member has its own named model. Returns the models in the same order.
fn pool_of(members: &[Member]) -> (Vec<Candidate>, Vec<Arc<ScriptedModel>>) {
    let recorder = Arc::new(Recorder::default());
    let mut harness = AgentHarness::new(ScriptedModel::new(Vec::new())).tool(recorded_tool(
        "lookup",
        ToolEffect::ReadOnly,
        &recorder,
    ));
    let mut models = Vec::new();
    for member in members {
        let model = ScriptedModel::named(member.model_id, (member.replies)());
        harness = harness.model_named(member.route, model.clone());
        if let Some((route, id)) = member.planner {
            harness = harness.model_named(route, ScriptedModel::named(id, Vec::new()));
        }
        models.push(model);
    }
    let harness = Arc::new(harness);
    let candidates = members
        .iter()
        .map(|member| {
            let mut candidate = Candidate::shared(Arc::clone(&harness))
                .overlay(member.overlay.clone())
                .model(member.route)
                .label(member.label);
            if let Some((route, _)) = member.planner {
                candidate = candidate.agent_model("planner", route);
            }
            if let Some(max_turns) = member.max_turns {
                candidate = candidate.max_turns(max_turns);
            }
            candidate
        })
        .collect();
    (candidates, models)
}

/// The id of the candidate at input `index`.
fn id_of(report: &AnonymisedReport, index: usize) -> AnonId {
    report
        .key
        .iter()
        .find(|(_, entry)| entry.index == index)
        .map(|(id, _)| id.clone())
        .expect("the key names every input")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_anonymised_text_names_no_candidate() {
    let source = one_lookup().await;
    let members = [alpha(), beta()];
    let (candidates, _) = pool_of(&members);

    let report = compare_anonymised(&source, &candidates, 7).await.unwrap();

    let pool_json = serde_json::to_string(&report.pool).unwrap();
    let debug = format!("{report:?}");
    for (member, candidate) in members.iter().zip(&candidates) {
        let mut overlay = member.overlay.clone();
        overlay.model = Some(member.route.to_owned());
        let mut identities = vec![
            member.route.to_owned(),
            member.model_id.to_owned(),
            member.label.to_owned(),
            member.overlay.digest(),
            overlay.digest(),
            candidate.digest(),
        ];
        if let Some((route, id)) = member.planner {
            identities.extend([route.to_owned(), id.to_owned()]);
        }
        for entry in report.key.values() {
            identities.push(entry.overlay_digest.clone());
        }
        for identity in &identities {
            assert!(
                !report.text.contains(identity.as_str()),
                "the text names {identity}:\n{}",
                report.text
            );
            assert!(
                !pool_json.contains(identity.as_str()),
                "the pool names {identity}"
            );
            assert!(
                !debug.contains(identity.as_str()),
                "the debug form names {identity}"
            );
        }
    }
    for id in report.key.keys() {
        assert!(report.text.contains(id.as_str()), "the text shows {id}");
    }
    assert!(report.text.contains("diverged"), "{}", report.text);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_input_order_changes_no_id_and_no_text() {
    let source = one_lookup().await;
    let (ab, _) = pool_of(&[alpha(), beta()]);
    let (ba, _) = pool_of(&[beta(), alpha()]);

    let forward = compare_anonymised(&source, &ab, 11).await.unwrap();
    let backward = compare_anonymised(&source, &ba, 11).await.unwrap();

    assert_eq!(id_of(&forward, 0), id_of(&backward, 1), "a keeps its id");
    assert_eq!(id_of(&forward, 1), id_of(&backward, 0), "b keeps its id");
    assert_eq!(forward.text, backward.text);
    assert_eq!(forward.pool, backward.pool);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_seed_reorders_the_pool() {
    let source = one_lookup().await;
    let mut orders = std::collections::BTreeSet::new();
    for seed in 0..64 {
        let (candidates, _) = pool_of(&[alpha(), beta()]);
        let report = compare_anonymised(&source, &candidates, seed)
            .await
            .unwrap();
        let order: Vec<usize> = report
            .pool
            .candidates
            .iter()
            .map(|entry| report.key[&entry.id].index)
            .collect();
        orders.insert(order);
        if orders.len() == 2 {
            break;
        }
    }
    assert_eq!(orders.len(), 2, "some seed puts b before a: {orders:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_key_resolves_every_id_once() {
    let source = one_lookup().await;
    let members = [alpha(), beta(), gamma()];
    let (candidates, _) = pool_of(&members);

    let report = compare_anonymised(&source, &candidates, 3).await.unwrap();

    assert_eq!(report.key.len(), 3, "no id repeats");
    let pool_ids: std::collections::BTreeSet<&AnonId> = report
        .pool
        .candidates
        .iter()
        .map(|entry| &entry.id)
        .collect();
    assert_eq!(pool_ids.len(), 3);
    assert!(pool_ids.iter().copied().eq(report.key.keys()));
    for (id, entry) in &report.key {
        assert!(id.as_str().starts_with("c-"), "{id}");
        assert!(id.as_str().len() >= 14, "{id}");
        assert_eq!(report.resolve(id), Some(entry));
        let member = &members[entry.index];
        assert_eq!(entry.digest, candidates[entry.index].digest());
        assert_eq!(entry.label.as_deref(), Some(member.label));
        let mut overlay = member.overlay.clone();
        overlay.model = Some(member.route.to_owned());
        assert_eq!(entry.overlay_digest, overlay.digest());
    }
    let mut indices: Vec<usize> = report.key.values().map(|entry| entry.index).collect();
    indices.sort_unstable();
    assert_eq!(indices, [0, 1, 2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_report_keeps_every_field_that_is_not_an_identity() {
    let source = one_lookup().await;
    let members = [alpha(), beta(), delta()];
    let (candidates, _) = pool_of(&members);
    let report = compare_anonymised(&source, &candidates, 5).await.unwrap();

    for (id, entry) in &report.key {
        let (fresh, _) = pool_of(&members);
        let alone = evaluate(&source, &fresh[entry.index]).await.unwrap();
        // An exhaustive destructure. A new field of `Evaluation` fails the
        // build here until the test classifies it.
        let Evaluation {
            turns,
            first_divergence,
            recorded,
            candidate,
            end_diverged,
            replayed_tool_calls,
            stubbed_tool_calls,
            turn_cap_reached,
            overlay_digest: _,
            recorded_verdict,
            candidate_verdict,
            scorer_usage,
            candidate_usage,
            // A price names a model, so the cost is an identity.
            candidate_cost_usd_micros: _,
        } = alone;
        let kept = report
            .pool
            .candidates
            .iter()
            .find(|kept| &kept.id == id)
            .expect("the pool holds every id");
        assert_eq!(kept.turns, turns);
        assert_eq!(kept.first_divergence, first_divergence);
        assert_eq!(report.pool.recorded, recorded);
        assert_eq!(kept.end, candidate);
        assert_eq!(kept.end_diverged, end_diverged);
        assert_eq!(kept.replayed_tool_calls, replayed_tool_calls);
        assert_eq!(kept.stubbed_tool_calls, stubbed_tool_calls);
        assert_eq!(kept.turn_cap_reached, turn_cap_reached);
        assert_eq!(kept.recorded_verdict, recorded_verdict);
        assert_eq!(kept.candidate_verdict, candidate_verdict);
        assert_eq!(kept.scorer_usage, scorer_usage);
        assert_eq!(kept.candidate_usage, candidate_usage);
    }
    let beta = report
        .pool
        .candidates
        .iter()
        .find(|kept| report.key[&kept.id].index == 1)
        .expect("b is in the pool");
    assert_eq!(beta.first_divergence, Some(0), "b diverges");
    assert_eq!(beta.stubbed_tool_calls, 1);
    let capped = report
        .pool
        .candidates
        .iter()
        .find(|kept| report.key[&kept.id].index == 2)
        .expect("d is in the pool");
    assert!(capped.turn_cap_reached, "{capped:#?}");
    assert!(
        capped.end_diverged,
        "a capped run fails, the source completes"
    );
    assert!(
        report.text.contains("- Turn cap reached: yes"),
        "{}",
        report.text
    );
    assert!(
        report.text.contains("- End diverged: yes"),
        "{}",
        report.text
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn equal_candidates_are_refused_before_any_model_call() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(one_lookup_replies());
    let (harness, _) = candidate(model.clone());
    let harness = Arc::new(harness);
    let pool = [
        Candidate::shared(Arc::clone(&harness)),
        Candidate::shared(Arc::clone(&harness)).label("other"),
        Candidate::shared(harness),
    ];

    let result = compare_anonymised(&source, &pool, 1).await;

    assert!(
        matches!(
            result,
            Err(EvalError::DuplicateCandidate {
                first: 0,
                second: 2
            })
        ),
        "{result:?}"
    );
    assert_eq!(model.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_overlay_in_the_pool_is_refused_before_any_model_call() {
    let source = one_lookup().await;
    let (mut candidates, models) = pool_of(&[alpha(), beta()]);
    candidates.push(
        candidates[0]
            .clone()
            .label("wide")
            .ceilings(Ceilings {
                max_steps: Some(1),
                max_total_tokens: None,
                max_output_tokens: None,
                models: std::collections::BTreeSet::new(),
                allow_clear_read_only: false,
            })
            .overlay(ProfileOverlay {
                max_steps: Some(9_999),
                ..ProfileOverlay::default()
            }),
    );

    let result = compare_anonymised(&source, &candidates, 1).await;

    assert!(
        matches!(
            result,
            Err(EvalError::Overlay(OverlayError::AboveCeiling { .. }))
        ),
        "{result:?}"
    );
    let calls: Vec<usize> = models.iter().map(|model| model.calls()).collect();
    assert_eq!(calls, [0, 0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_pool_reports_no_candidate() {
    let source = one_lookup().await;

    let report = compare_anonymised(&source, &[], 1).await.unwrap();

    assert_eq!(report.key.len(), 0);
    assert_eq!(report.pool.candidates, Vec::new());
    assert!(matches!(report.pool.recorded, RunEnd::Completed(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_candidates_run_in_pool_order() {
    let source = one_lookup().await;
    // One shared model. The candidate that runs first takes the first two
    // replies, so a run in input order would change the text.
    let pool = |labels: [&str; 2]| {
        let mut replies = one_lookup_replies();
        replies.extend(other_key_replies());
        let model = ScriptedModel::new(replies);
        let harness = Arc::new(candidate(model.clone()).0);
        let pool = labels.map(|label| Candidate::shared(Arc::clone(&harness)).label(label));
        (pool, model)
    };
    let (forward, one) = pool(["label-only-one-1a", "label-only-two-2b"]);
    let (backward, two) = pool(["label-only-two-2b", "label-only-one-1a"]);

    let a = compare_anonymised(&source, &forward, 4).await.unwrap();
    let b = compare_anonymised(&source, &backward, 4).await.unwrap();

    assert_eq!(a.text, b.text);
    assert_eq!(a.pool, b.pool);
    assert_eq!((one.calls(), two.calls()), (4, 4));
    assert_eq!(a.key.len(), 2, "a label alone tells two candidates apart");
    for label in ["label-only-one-1a", "label-only-two-2b"] {
        assert!(!a.text.contains(label), "{}", a.text);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_single_candidate_gets_a_short_id() {
    let source = one_lookup().await;
    let (candidates, models) = pool_of(&[alpha()]);

    let report = compare_anonymised(&source, &candidates, 0).await.unwrap();

    let [entry] = report.pool.candidates.as_slice() else {
        panic!("one entry: {:#?}", report.pool);
    };
    assert_eq!(entry.id.as_str().len(), 2 + 12, "{}", entry.id);
    assert_eq!(report.resolve(&entry.id).map(|r| r.index), Some(0));
    assert!(!entry.end_diverged);
    assert_eq!(models[0].calls(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_agent_route_is_refused_before_any_model_call() {
    let source = one_lookup().await;
    let (candidates, models) = pool_of(&[alpha(), beta()]);
    let routed = candidates[1]
        .clone()
        .agent_model("planner", "route-missing-7b1f");

    let alone = evaluate(&source, &routed).await;
    let pooled = compare_anonymised(&source, &[candidates[0].clone(), routed], 1).await;

    for result in [alone.map(|_| ()), pooled.map(|_| ())] {
        let Err(EvalError::Overlay(OverlayError::UnknownModel { model })) = result else {
            panic!("the route is refused: {result:?}");
        };
        assert_eq!(model, "route-missing-7b1f");
    }
    let calls: Vec<usize> = models.iter().map(|model| model.calls()).collect();
    assert_eq!(calls, [0, 0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failure_text_names_no_candidate() {
    let source = one_lookup().await;
    let mut member = alpha();
    member.replies = || {
        vec![Err(AgentError::new(
            ErrorKind::Provider,
            "404: model model-alpha-id-3c1e on route-alpha-7f3a (triage-overlay-v7) not found",
        ))]
    };
    let (candidates, _) = pool_of(&[member, beta()]);

    let report = compare_anonymised(&source, &candidates, 2).await.unwrap();

    let failed = report
        .pool
        .candidates
        .iter()
        .find(|kept| report.key[&kept.id].index == 0)
        .expect("a is in the pool");
    let RunEnd::Failed(error) = &failed.end else {
        panic!("the provider error fails the run: {failed:#?}");
    };
    assert!(
        error.contains("404: model [redacted] on [redacted] ([redacted]) not found"),
        "{error}"
    );
    for identity in [
        "model-alpha-id-3c1e",
        "route-alpha-7f3a",
        "triage-overlay-v7",
    ] {
        assert!(!report.text.contains(identity), "{}", report.text);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_source_end_names_no_candidate() {
    // The source fails on a provider error that names the model of a.
    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(ScriptedModel::new(vec![Err(AgentError::new(
        ErrorKind::Provider,
        "model-alpha-id-3c1e refused the request",
    ))]));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("look it up")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Failed(_)), "{state:?}");
    let source = rt.load_history(exec).unwrap();
    let (candidates, _) = pool_of(&[alpha(), beta()]);

    let report = compare_anonymised(&source, &candidates, 3).await.unwrap();

    let RunEnd::Failed(error) = &report.pool.recorded else {
        panic!("the source failed: {:#?}", report.pool.recorded);
    };
    assert!(error.contains("[redacted] refused the request"), "{error}");
    assert!(
        !report.text.contains("model-alpha-id-3c1e"),
        "{}",
        report.text
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_inherited_route_and_its_model_id_are_redacted() {
    // The source names its model. Each candidate keeps that route, and its
    // own harness maps the route to a model that fails with its id.
    let route = "route-shared-3d5a";
    let source = {
        let (_dir, db) = fresh_db();
        let recorder = Arc::new(Recorder::default());
        let model = ScriptedModel::new(one_lookup_replies());
        let harness = AgentHarness::new(ScriptedModel::new(Vec::new()))
            .model_named(route, model)
            .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder));
        let mut rt = runtime(&db, harness);
        let task = AgentTask::new("look it up").model(route);
        let exec = sqlite::start(&mut rt, &task).unwrap();
        let state = rt.run_until_blocked(exec).await.unwrap();
        assert!(matches!(state, RunState::Completed(_)), "{state:?}");
        rt.load_history(exec).unwrap()
    };
    let failing = |id: &'static str, label: &str| {
        let model = ScriptedModel::named(
            id,
            vec![Err(AgentError::new(
                ErrorKind::Provider,
                format!("{id} on {route} is down"),
            ))],
        );
        let harness = AgentHarness::new(ScriptedModel::new(Vec::new())).model_named(route, model);
        Candidate::new(harness).label(label)
    };
    let pool = [
        failing("model-one-id-5c2e", "label-one-6b"),
        failing("model-two-id-7d4f", "label-two-8e"),
    ];

    let report = compare_anonymised(&source, &pool, 6).await.unwrap();

    for entry in &report.pool.candidates {
        let RunEnd::Failed(error) = &entry.end else {
            panic!("the candidate fails: {entry:#?}");
        };
        assert_eq!(error.matches("[redacted]").count(), 2, "{error}");
    }
    for identity in ["model-one-id-5c2e", "model-two-id-7d4f", route] {
        assert!(!report.text.contains(identity), "{}", report.text);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_peer_route_and_its_model_id_are_redacted() {
    use autumn_harvest_agent::mesh::DELEGATE_TOOL;
    use autumn_harvest_agent::{Mandate, Peer};

    // The source delegates to a peer that names its own model route.
    let route = "route-peer-9a3c";
    let delegate = || {
        calls(
            &[(
                "d1",
                DELEGATE_TOOL,
                json!({"agent": "planner", "task": "plan"}),
            )],
            2,
        )
    };
    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(ScriptedModel::new(vec![delegate(), answer("done", 2)]))
        .model_named(route, ScriptedModel::new(vec![answer("the plan", 2)]))
        .peer(Peer::new("planner", "Plans.").model(route));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("go").mandate(Mandate::new("triage").peers(["planner"]));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let source = rt.load_history(exec).unwrap();
    // Each candidate keeps the peer route. Its harness maps the route to a
    // model that fails with its own id.
    let failing = |id: &'static str, label: &str| {
        let peer = ScriptedModel::named(
            id,
            vec![Err(AgentError::new(
                ErrorKind::Provider,
                format!("{id} on {route} is down"),
            ))],
        );
        let harness = AgentHarness::new(ScriptedModel::new(vec![delegate(), answer("done", 2)]))
            .model_named(route, peer);
        Candidate::new(harness).label(label)
    };
    let pool = [
        failing("model-peer-one-id-1f", "label-peer-one-3a"),
        failing("model-peer-two-id-2e", "label-peer-two-4b"),
    ];

    let report = compare_anonymised(&source, &pool, 8).await.unwrap();

    let pool_json = serde_json::to_string(&report.pool).unwrap();
    for identity in ["model-peer-one-id-1f", "model-peer-two-id-2e", route] {
        assert!(!report.text.contains(identity), "{}", report.text);
        assert!(!pool_json.contains(identity), "{pool_json}");
    }
}

// ── Scored evaluations (issue #2447) ────────────────────────────────────────

/// The golden JSON of an evaluation with no scorer. The code from before
/// issue #2447 wrote it. Set `UPDATE_GOLDEN=1` to write it again.
const NO_SCORER_GOLDEN: &str = "tests/fixtures/eval_no_scorer.json";

/// The run id in the golden JSON. A source run on SQLite gets a new run id
/// each time.
const GOLDEN_RUN_ID: &str = "<run>";

/// Replace the run id of each report in `value` with `run_id`.
fn with_run_id(value: &mut Value, run_id: &str) {
    for end in ["/recorded/detail/run_id", "/candidate/detail/run_id"] {
        if let Some(field) = value.pointer_mut(end) {
            *field = json!(run_id);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn with_no_scorer_the_report_keeps_its_json() {
    let source = one_lookup().await;
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "y"}))], 5),
        answer("other", 7),
    ]);
    let (harness, _) = candidate(model);

    let evaluation = evaluate(&source, &Candidate::new(harness)).await.unwrap();

    assert_eq!(evaluation.recorded_verdict, None);
    assert_eq!(evaluation.candidate_verdict, None);
    assert_eq!(evaluation.scorer_usage, None);
    let mut value = serde_json::to_value(&evaluation).unwrap();
    let run_id = value
        .pointer("/recorded/detail/run_id")
        .and_then(Value::as_str)
        .expect("the source report has a run id")
        .to_owned();
    with_run_id(&mut value, GOLDEN_RUN_ID);
    // Issue #2380 adds the spend of the candidate. The golden is older, so
    // the test takes the two fields out before it compares.
    let spend = value.as_object_mut().unwrap();
    assert_eq!(
        spend.remove("candidate_usage"),
        Some(json!({
            "input_tokens": 12,
            "output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_write_tokens": 0
        }))
    );
    assert_eq!(
        spend.remove("candidate_cost_usd_micros"),
        None,
        "an unpriced model"
    );
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(NO_SCORER_GOLDEN);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let text = serde_json::to_string_pretty(&value).unwrap();
        std::fs::write(&path, text + "\n").unwrap();
    }
    let mut golden: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(value, golden, "the JSON of a report with no scorer changed");
    // The JSON from before issues #2447 and #2380 decodes to the same
    // report, with no spend.
    with_run_id(&mut golden, &run_id);
    let decoded: Evaluation = serde_json::from_value(golden).unwrap();
    let mut expected = evaluation;
    expected.candidate_usage = autumn_harvest_agent::TokenUsage::default();
    assert_eq!(decoded, expected);
}

/// A source run that answers `text` with no tool call.
async fn answers(text: &str) -> Vec<WorkflowEvent> {
    record(
        &AgentTask::new("what is six times seven?"),
        vec![answer(text, 5)],
    )
    .await
}

/// A candidate whose model answers `text` with no tool call.
fn answering(text: &str) -> (Candidate, Arc<ScriptedModel>) {
    let model = ScriptedModel::new(vec![answer(text, 9)]);
    (Candidate::new(AgentHarness::new(model.clone())), model)
}

fn scored(value: f64) -> score::Verdict {
    score::Verdict::score(value).unwrap()
}

/// `ExactMatch` that counts its calls.
#[derive(Debug)]
struct Counted {
    inner: ExactMatch,
    calls: AtomicUsize,
}

impl Counted {
    fn new(expected: &str) -> Arc<Self> {
        Arc::new(Self {
            inner: ExactMatch::new(expected),
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Scorer for Counted {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<score::Verdict, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.score(input, cx)
    }
}

/// A judge model that answers `reply` with a fixed usage.
#[derive(Debug)]
struct Judge {
    reply: &'static str,
    usage: TokenUsage,
    calls: AtomicUsize,
}

impl Judge {
    fn new(reply: &'static str) -> Arc<Self> {
        Arc::new(Self {
            reply,
            usage: TokenUsage::new(120, 3),
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl AgentModel for Judge {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (usage, reply) = (self.usage, self.reply);
        Box::pin(async move {
            Ok(ChatResponse {
                content: vec![ContentPart::Text(reply.into())],
                stop_reason: StopReason::EndTurn,
                usage,
            })
        })
    }
}

/// A judge scorer: one call to the harness model `judge`.
#[derive(Debug)]
struct JudgeScorer;

impl Scorer for JudgeScorer {
    fn id(&self) -> &'static str {
        "judge"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<score::Verdict, AgentError>> {
        Box::pin(async move {
            let request = ChatRequest {
                messages: vec![ChatMessage::text(ChatRole::User, input.answer.clone())],
                tools: Vec::new(),
                max_tokens: Some(8),
                temperature: None,
                draw: None,
            };
            let reply = cx.chat(Some("judge"), &request).await?;
            let text = match reply.content.first() {
                Some(ContentPart::Text(text)) => text.clone(),
                _ => return Ok(score::Verdict::reject("the judge gave no text")),
            };
            score::Verdict::score(text.parse::<f64>().unwrap_or(f64::NAN))
        })
    }
}

/// A scorer that fails on the answer `boom`, or on each answer.
#[derive(Debug)]
struct Faulty {
    always: bool,
}

impl Scorer for Faulty {
    fn id(&self) -> &'static str {
        "faulty"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<score::Verdict, AgentError>> {
        let fails = self.always || input.answer == "boom";
        Box::pin(async move {
            if fails {
                Err(AgentError::new(ErrorKind::Provider, "the checker is down"))
            } else {
                Ok(score::Verdict::Score(FiniteF64::ONE))
            }
        })
    }
}

/// A scorer that rejects each answer with a reason that names a model, as
/// a judge reason can.
#[derive(Debug)]
struct Naming(&'static str);

impl Scorer for Naming {
    fn id(&self) -> &'static str {
        "naming"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<score::Verdict, AgentError>> {
        let reason = format!("{} says {} is wrong", self.0, input.answer);
        Box::pin(async move { Ok(score::Verdict::reject(reason)) })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_match_rejects_the_source_and_scores_the_candidate() {
    let source = answers("41").await;
    let (candidate, _) = answering("42");
    let candidate = candidate.scorer(Arc::new(ExactMatch::new("42")));

    let evaluation = evaluate(&source, &candidate).await.unwrap();

    assert!(
        matches!(evaluation.recorded_verdict, Some(score::Verdict::Reject(_))),
        "{:?}",
        evaluation.recorded_verdict
    );
    assert_eq!(evaluation.candidate_verdict, Some(scored(1.0)));
    assert_eq!(
        evaluation.scorer_usage,
        Some(TokenUsage::default()),
        "ExactMatch calls no judge"
    );
    let json = serde_json::to_value(&evaluation).unwrap();
    assert_eq!(json["candidate_verdict"], json!({"score": 1.0}));
    let back: Evaluation = serde_json::from_value(json).unwrap();
    assert_eq!(back, evaluation, "a scored report decodes again");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_candidate_end_has_no_verdict() {
    let source = answers("42").await;
    let scorer = Counted::new("42");
    let model = ScriptedModel::new(Vec::new());
    let candidate = Candidate::new(AgentHarness::new(model)).scorer(scorer.clone());

    let evaluation = evaluate(&source, &candidate).await.unwrap();

    assert!(
        matches!(evaluation.candidate, RunEnd::Failed(_)),
        "{:?}",
        evaluation.candidate
    );
    assert_eq!(evaluation.candidate_verdict, None);
    assert_eq!(evaluation.recorded_verdict, Some(scored(1.0)));
    assert_eq!(scorer.calls(), 1, "the failed end is not scored");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_source_end_has_no_verdict() {
    let failed = WorkflowEvent::WorkflowFailed {
        error: "provider down".into(),
        error_type: None,
        details: None,
        non_retryable: None,
    };
    let source = with_end(answers("42").await, Some(failed));
    let scorer = Counted::new("42");
    let (candidate, _) = answering("42");

    let evaluation = evaluate(&source, &candidate.scorer(scorer.clone()))
        .await
        .unwrap();

    assert_eq!(evaluation.recorded_verdict, None);
    assert_eq!(evaluation.candidate_verdict, Some(scored(1.0)));
    assert_eq!(scorer.calls(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn judge_tokens_go_to_scorer_usage_not_to_the_candidate() {
    let source = answers("41").await;
    let judge = Judge::new("0.5");
    let (plain, _) = answering("42");
    let harness = AgentHarness::new(ScriptedModel::new(vec![answer("42", 9)]))
        .model_named("judge", judge.clone());
    let judged = Candidate::new(harness).scorer(Arc::new(JudgeScorer));

    let with_judge = evaluate(&source, &judged).await.unwrap();
    let without = evaluate(&source, &plain).await.unwrap();

    assert_eq!(judge.calls(), 2, "one call per end");
    assert_eq!(with_judge.scorer_usage, Some(TokenUsage::new(240, 6)));
    assert_eq!(with_judge.recorded_verdict, Some(scored(0.5)));
    assert_eq!(with_judge.candidate_verdict, Some(scored(0.5)));
    let (RunEnd::Completed(judged_report), RunEnd::Completed(plain_report)) =
        (&with_judge.candidate, &without.candidate)
    else {
        panic!("both candidates complete");
    };
    assert_eq!(judged_report.usage, TokenUsage::new(9, 0));
    assert_eq!(
        judged_report.usage, plain_report.usage,
        "the candidate usage holds no judge token"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn compare_with_a_scorer_gives_both_verdicts_per_side() {
    let source = answers("41").await;
    let scorer = Counted::new("42");
    let shared: Arc<dyn Scorer> = scorer.clone();
    let harness = Arc::new(
        AgentHarness::new(ScriptedModel::new(Vec::new()))
            .model_named("a", ScriptedModel::new(vec![answer("42", 9)]))
            .model_named("b", ScriptedModel::new(vec![answer("40", 9)])),
    );
    let first = Candidate::shared(Arc::clone(&harness))
        .model("a")
        .scorer(Arc::clone(&shared));
    let second = Candidate::shared(harness).model("b").scorer(shared);

    let comparison = compare(&source, &first, &second).await.unwrap();

    for side in [&comparison.first, &comparison.second] {
        assert!(
            matches!(side.recorded_verdict, Some(score::Verdict::Reject(_))),
            "{:?}",
            side.recorded_verdict
        );
    }
    assert_eq!(comparison.first.candidate_verdict, Some(scored(1.0)));
    assert!(
        matches!(
            comparison.second.candidate_verdict,
            Some(score::Verdict::Reject(_))
        ),
        "{:?}",
        comparison.second.candidate_verdict
    );
    assert_eq!(comparison.first_verdict(), Some(&scored(1.0)));
    assert_eq!(
        comparison.second_verdict(),
        comparison.second.candidate_verdict.as_ref()
    );
    assert_eq!(scorer.calls(), 3, "the source is scored once");
}

#[tokio::test(flavor = "multi_thread")]
async fn compare_scores_the_source_once_per_scorer() {
    let source = answers("41").await;
    let one = Counted::new("41");
    let two = Counted::new("42");
    let (first, _) = answering("42");
    let (second, _) = answering("42");

    let comparison = compare(
        &source,
        &first.scorer(one.clone()),
        &second.scorer(two.clone()),
    )
    .await
    .unwrap();

    assert_eq!(comparison.first.recorded_verdict, Some(scored(1.0)));
    assert!(
        matches!(
            comparison.second.recorded_verdict,
            Some(score::Verdict::Reject(_))
        ),
        "each scorer scores the source"
    );
    assert_eq!((one.calls(), two.calls()), (2, 2));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scorer_error_on_the_source_costs_no_candidate_call() {
    let source = answers("41").await;
    let (candidate, model) = answering("42");

    let result = evaluate(
        &source,
        &candidate.scorer(Arc::new(Faulty { always: true })),
    )
    .await;

    let Err(EvalError::Scorer { side, error, .. }) = result else {
        panic!("the scorer error fails the evaluation: {result:?}");
    };
    assert_eq!(side, Side::Recorded);
    assert_eq!(error.kind(), ErrorKind::Provider);
    assert_eq!(model.calls(), 0, "the source is scored first");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scorer_error_on_the_candidate_names_the_candidate() {
    let source = answers("41").await;
    let (candidate, _) = answering("boom");

    let result = evaluate(
        &source,
        &candidate.scorer(Arc::new(Faulty { always: false })),
    )
    .await;

    assert!(
        matches!(
            result,
            Err(EvalError::Scorer {
                side: Side::Candidate,
                ..
            })
        ),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scorer_past_its_timeout_fails_the_evaluation() {
    #[derive(Debug)]
    struct Slow;
    impl Scorer for Slow {
        fn id(&self) -> &'static str {
            "slow"
        }
        fn score<'a>(
            &'a self,
            _input: &'a ScoreInput,
            _cx: &'a ScoreContext<'_>,
        ) -> BoxFuture<'a, Result<score::Verdict, AgentError>> {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(score::Verdict::Score(FiniteF64::ONE))
            })
        }
    }
    let source = answers("41").await;
    let model = ScriptedModel::new(vec![answer("42", 9)]);
    let harness = AgentHarness::new(model.clone()).score_timeout(Duration::from_millis(50));
    let candidate = Candidate::new(harness).scorer(Arc::new(Slow));

    let result = evaluate(&source, &candidate).await;

    let Err(EvalError::Scorer { side, error, .. }) = result else {
        panic!("the timeout fails the evaluation: {result:?}");
    };
    assert_eq!(side, Side::Recorded);
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(
        model.calls(),
        0,
        "the timeout fires before the candidate run"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_first_evaluation_holds_the_tokens_of_a_shared_source_score() {
    let source = answers("41").await;
    let judge = Judge::new("0.5");
    let harness = Arc::new(
        AgentHarness::new(ScriptedModel::new(Vec::new()))
            .model_named("judge", judge.clone())
            .model_named("a", ScriptedModel::new(vec![answer("42", 9)]))
            .model_named("b", ScriptedModel::new(vec![answer("40", 9)])),
    );
    let scorer: Arc<dyn Scorer> = Arc::new(JudgeScorer);
    let first = Candidate::shared(Arc::clone(&harness))
        .model("a")
        .scorer(Arc::clone(&scorer));
    let second = Candidate::shared(harness).model("b").scorer(scorer);

    let comparison = compare(&source, &first, &second).await.unwrap();

    assert_eq!(judge.calls(), 3, "one source call and one per candidate");
    assert_eq!(
        comparison.first.scorer_usage,
        Some(TokenUsage::new(240, 6)),
        "the first holds the source score and its own"
    );
    assert_eq!(
        comparison.second.scorer_usage,
        Some(TokenUsage::new(120, 3))
    );
    assert_eq!(comparison.second.recorded_verdict, Some(scored(0.5)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_verdict_is_shared_only_with_the_same_scorer_and_harness() {
    let source = answers("41").await;
    let harness_of = || {
        Arc::new(
            AgentHarness::new(ScriptedModel::new(Vec::new()))
                .model_named("a", ScriptedModel::new(vec![answer("42", 9)]))
                .model_named("b", ScriptedModel::new(vec![answer("42", 9)])),
        )
    };

    // The same scorer on two harnesses.
    let scorer = Counted::new("41");
    let shared: Arc<dyn Scorer> = scorer.clone();
    let first = Candidate::shared(harness_of())
        .model("a")
        .scorer(Arc::clone(&shared));
    let second = Candidate::shared(harness_of()).model("b").scorer(shared);
    compare(&source, &first, &second).await.unwrap();
    assert_eq!(scorer.calls(), 4, "each harness scores the source");

    // Two scorers on one harness.
    let harness = harness_of();
    let (one, two) = (Counted::new("41"), Counted::new("41"));
    let first = Candidate::shared(Arc::clone(&harness))
        .model("a")
        .scorer(one.clone());
    let second = Candidate::shared(harness).model("b").scorer(two.clone());
    compare(&source, &first, &second).await.unwrap();
    assert_eq!(
        (one.calls(), two.calls()),
        (2, 2),
        "each scorer scores the source"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_judge_that_gives_nan_fails_with_decode() {
    let source = answers("41").await;
    let harness = AgentHarness::new(ScriptedModel::new(vec![answer("42", 9)]))
        .model_named("judge", Judge::new("NaN"));
    let candidate = Candidate::new(harness).scorer(Arc::new(JudgeScorer));

    let result = evaluate(&source, &candidate).await;

    let Err(EvalError::Scorer { side, error, .. }) = result else {
        panic!("a NaN score fails the evaluation: {result:?}");
    };
    assert_eq!(side, Side::Recorded);
    assert_eq!(error.kind(), ErrorKind::Decode);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_long_reject_reason_is_cut() {
    #[derive(Debug)]
    struct Verbose;
    impl Scorer for Verbose {
        fn id(&self) -> &'static str {
            "verbose"
        }
        fn score<'a>(
            &'a self,
            _input: &'a ScoreInput,
            _cx: &'a ScoreContext<'_>,
        ) -> BoxFuture<'a, Result<score::Verdict, AgentError>> {
            // The variant itself, so the scorer skips the cut of `reject`.
            let reason = "x".repeat(score::MAX_REJECT_REASON_BYTES * 2);
            Box::pin(async move { Ok(score::Verdict::Reject(reason)) })
        }
    }
    let source = answers("41").await;
    let (candidate, _) = answering("42");

    let evaluation = evaluate(&source, &candidate.scorer(Arc::new(Verbose)))
        .await
        .unwrap();

    for verdict in [&evaluation.recorded_verdict, &evaluation.candidate_verdict] {
        let Some(score::Verdict::Reject(reason)) = verdict else {
            panic!("a reject: {verdict:?}");
        };
        assert!(
            reason.len() <= score::MAX_REJECT_REASON_BYTES,
            "{}",
            reason.len()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pool_keeps_the_verdicts_and_redacts_each_reject_reason() {
    let source = answers("41").await;
    let named = |id: &'static str, label: &str| {
        let model = ScriptedModel::named(id, vec![answer("42", 9)]);
        Candidate::new(AgentHarness::new(model))
            .label(label)
            .scorer(Arc::new(Naming(id)))
    };
    let pool = [
        named("model-secret-one-7a", "label-one-8b"),
        named("model-secret-two-9c", "label-two-0d"),
    ];

    let report = compare_anonymised(&source, &pool, 3).await.unwrap();

    for entry in &report.pool.candidates {
        let Some(score::Verdict::Reject(reason)) = &entry.candidate_verdict else {
            panic!("the pool keeps the verdict: {entry:?}");
        };
        assert_eq!(reason, "[redacted] says 42 is wrong");
        assert_eq!(
            entry.recorded_verdict,
            Some(score::Verdict::reject("[redacted] says 41 is wrong"))
        );
        assert_eq!(entry.scorer_usage, Some(TokenUsage::default()));
    }
    assert!(
        report.text.contains("Candidate verdict: reject"),
        "{}",
        report.text
    );
    assert!(
        report.text.contains("Source verdict: reject"),
        "{}",
        report.text
    );
    let pool_json = serde_json::to_string(&report.pool).unwrap();
    for identity in ["model-secret-one-7a", "model-secret-two-9c"] {
        assert!(!report.text.contains(identity), "{}", report.text);
        assert!(!pool_json.contains(identity), "{pool_json}");
    }
}

/// A judge scorer that pays for one judge call, then fails.
#[derive(Debug)]
struct JudgeThenFail;

impl Scorer for JudgeThenFail {
    fn id(&self) -> &'static str {
        "judge_then_fail"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<score::Verdict, AgentError>> {
        Box::pin(async move {
            let request = ChatRequest {
                messages: vec![ChatMessage::text(ChatRole::User, input.answer.clone())],
                tools: Vec::new(),
                max_tokens: Some(8),
                temperature: None,
                draw: None,
            };
            cx.chat(Some("judge"), &request).await?;
            Err(AgentError::new(
                ErrorKind::Decode,
                "the judge reply does not parse",
            ))
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scorer_error_keeps_the_judge_spend() {
    let source = answers("41").await;
    let harness = AgentHarness::new(ScriptedModel::new(vec![answer("42", 9)]))
        .model_named("judge", Judge::new("0.5"));
    let candidate = Candidate::new(harness).scorer(Arc::new(JudgeThenFail));

    let result = evaluate(&source, &candidate).await;

    let Err(EvalError::Scorer { side, usage, .. }) = result else {
        panic!("the scorer error fails the evaluation: {result:?}");
    };
    assert_eq!(side, Side::Recorded);
    assert_eq!(usage, TokenUsage::new(120, 3), "the paid judge call shows");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_candidate_scorer_error_keeps_the_source_spend_too() {
    // A judge scorer that fails only on the candidate answer.
    #[derive(Debug)]
    struct FailsOnBoom;
    impl Scorer for FailsOnBoom {
        fn id(&self) -> &'static str {
            "fails_on_boom"
        }
        fn score<'a>(
            &'a self,
            input: &'a ScoreInput,
            cx: &'a ScoreContext<'_>,
        ) -> BoxFuture<'a, Result<score::Verdict, AgentError>> {
            Box::pin(async move {
                let verdict = JudgeScorer.score(input, cx).await?;
                if input.answer == "boom" {
                    return Err(AgentError::new(ErrorKind::Provider, "down"));
                }
                Ok(verdict)
            })
        }
    }
    let source = answers("41").await;
    let judge = Judge::new("0.5");
    let harness = AgentHarness::new(ScriptedModel::new(vec![answer("boom", 9)]))
        .model_named("judge", judge.clone());

    let result = evaluate(
        &source,
        &Candidate::new(harness).scorer(Arc::new(FailsOnBoom)),
    )
    .await;

    let Err(EvalError::Scorer { side, usage, .. }) = result else {
        panic!("the scorer error fails the evaluation: {result:?}");
    };
    assert_eq!(side, Side::Candidate);
    assert_eq!(judge.calls(), 2);
    assert_eq!(usage, TokenUsage::new(240, 6), "both judge calls show");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pool_redacts_the_judge_model_in_a_reject_reason() {
    /// A judge model that names itself in its reply.
    #[derive(Debug)]
    struct NamedJudge(&'static str);
    impl AgentModel for NamedJudge {
        fn chat<'a>(
            &'a self,
            _request: &'a ChatRequest,
        ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
            let text = format!("{} says no", self.0);
            Box::pin(async move {
                Ok(ChatResponse {
                    content: vec![ContentPart::Text(text)],
                    stop_reason: StopReason::EndTurn,
                    usage: TokenUsage::new(1, 1),
                })
            })
        }

        fn model_id(&self) -> Option<&str> {
            Some(self.0)
        }
    }
    /// A judge scorer that rejects with the judge reply as its reason.
    #[derive(Debug)]
    struct Quoting;
    impl Scorer for Quoting {
        fn id(&self) -> &'static str {
            "quoting"
        }
        fn score<'a>(
            &'a self,
            input: &'a ScoreInput,
            cx: &'a ScoreContext<'_>,
        ) -> BoxFuture<'a, Result<score::Verdict, AgentError>> {
            Box::pin(async move {
                let request = ChatRequest {
                    messages: vec![ChatMessage::text(ChatRole::User, input.answer.clone())],
                    tools: Vec::new(),
                    max_tokens: Some(8),
                    temperature: None,
                    draw: None,
                };
                let reply = cx.chat(Some("grader"), &request).await?;
                let text = match reply.content.first() {
                    Some(ContentPart::Text(text)) => text.clone(),
                    _ => String::new(),
                };
                Ok(score::Verdict::reject(text))
            })
        }
    }
    let source = answers("41").await;
    let member = |judge: &'static str, label: &str| {
        let harness = AgentHarness::new(ScriptedModel::new(vec![answer("42", 9)]))
            .model_named("grader", Arc::new(NamedJudge(judge)));
        Candidate::new(harness)
            .label(label)
            .scorer(Arc::new(Quoting))
    };
    let pool = [
        member("judge-secret-one-5e", "label-one-6f"),
        member("judge-secret-two-7a", "label-two-8b"),
    ];

    let report = compare_anonymised(&source, &pool, 4).await.unwrap();

    let pool_json = serde_json::to_string(&report.pool).unwrap();
    for identity in ["judge-secret-one-5e", "judge-secret-two-7a", "grader"] {
        assert!(!report.text.contains(identity), "{}", report.text);
        assert!(!pool_json.contains(identity), "{pool_json}");
    }
    for entry in &report.pool.candidates {
        assert_eq!(
            entry.candidate_verdict,
            Some(score::Verdict::reject("[redacted] says no"))
        );
    }
}
