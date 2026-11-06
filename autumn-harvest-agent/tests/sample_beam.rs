//! Step-level beam search in `agent_sample`, on SQLite.
//!
//! Each step is a child `agent_loop` run in step mode. The run scores each
//! step in a recorded activity and keeps the best beams of each round. The
//! crash test is in `sample_beam_crash.rs`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_literal_bound
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    BeamFate, InMemorySamplingPolicyStore, SampleInput, SampleOutcome, SampleReport, SampleStop,
    SampleStrategy, SamplingPolicy, SamplingPolicyStore, ScorerRef, Strategy,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentProfile, AgentStop, ChatRequest, ContentPart, DrawId, FiniteF64,
    FnTool, ScoreContext, ScoreInput, Scorer, Tool, ToolEffect, Verdict, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{Recorder, TryFnModel, answer, calls, recorded_tool, runtime, tool_results};
use futures::FutureExt as _;
use serde_json::json;

const KIND: &str = "walker";

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// A policy that allows `strategies`, up to `max` draws.
fn policy(strategies: &[Strategy], max: u32) -> SamplingPolicy {
    SamplingPolicy {
        strategies: strategies.iter().copied().collect(),
        max_draws: max,
        max_concurrent_draws: 4,
        ..SamplingPolicy::default()
    }
}

fn store_with(policy: &SamplingPolicy) -> Arc<InMemorySamplingPolicyStore> {
    let store = Arc::new(InMemorySamplingPolicyStore::new());
    store
        .set(KIND, policy, "ops@example.com")
        .now_or_never()
        .expect("the in-memory store is ready at once")
        .expect("the policy is valid");
    store
}

/// The tool results of `messages`.
fn results(messages: &[autumn_harvest_agent::ChatMessage]) -> Vec<String> {
    messages
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect()
}

/// The path of a transcript: each tool result with its quotes removed.
fn path_of(contents: &[String]) -> String {
    contents
        .iter()
        .map(|content| content.trim_matches('"'))
        .collect()
}

/// A model that walks two steps, then answers its path.
///
/// An even draw walks `L`, and an odd draw walks `R`.
fn walker() -> Arc<TryFnModel> {
    TryFnModel::new(|request: &ChatRequest| {
        let done = results(&request.messages);
        if done.len() >= 2 {
            return answer(&path_of(&done), 5);
        }
        let draw = request.draw.map_or(0, DrawId::get);
        let dir = if draw.is_multiple_of(2) { "L" } else { "R" };
        calls(&[(&format!("c{draw}"), "walk", json!({"dir": dir}))], 5)
    })
}

/// The `walk` tool. It returns its direction.
fn walk() -> Arc<dyn Tool> {
    FnTool::new(
        "walk",
        "Take one step.",
        json!({"type": "object"}),
        |input: serde_json::Value| async move { Ok(input["dir"].clone()) },
    )
    .effect(ToolEffect::ReadOnly)
    .shared()
}

/// A step scorer that scores the path of a beam from a table.
#[derive(Debug)]
struct Paths(BTreeMap<&'static str, f64>);

impl Scorer for Paths {
    fn id(&self) -> &str {
        "paths"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        let contents: Vec<String> = input.tools.iter().map(|t| t.content.clone()).collect();
        let path = path_of(&contents);
        let verdict = self.0.get(path.as_str()).map_or_else(
            || Ok(Verdict::reject(format!("no score for {path:?}"))),
            |score| Verdict::score(*score),
        );
        Box::pin(std::future::ready(verdict))
    }
}

/// The step scorer that prefers the path `RL`.
fn prefers_rl() -> Paths {
    Paths(BTreeMap::from([
        ("L", 0.4),
        ("R", 0.6),
        ("LL", 0.3),
        ("LR", 0.2),
        ("RL", 0.9),
        ("RR", 0.5),
    ]))
}

fn paths() -> ScorerRef {
    ScorerRef::deterministic("paths").unwrap()
}

fn harness(model: Arc<TryFnModel>, policy: &SamplingPolicy) -> AgentHarness {
    AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .tool(walk())
        .scorer(Arc::new(prefers_rl()))
        .sampling_policies(store_with(policy))
}

fn beam_input(width: u32, expansion: u32, max_rounds: u32) -> SampleInput {
    SampleInput::new(
        KIND,
        "Find the path.",
        SampleStrategy::beam(width, expansion, max_rounds),
        paths(),
    )
}

async fn drive(rt: &mut SqliteRuntime, input: &SampleInput) -> RunState {
    let exec = sqlite::start_sample(rt, input).expect("the run starts");
    rt.run_until_blocked(exec).await.expect("the run drives")
}

fn sample_report(state: RunState) -> SampleReport {
    match state {
        RunState::Completed(value) => serde_json::from_value(value).expect("the report decodes"),
        other => panic!("expected a completed run, got {other:?}"),
    }
}

/// The steps of each round, in round order.
fn per_round(report: &SampleReport) -> Vec<usize> {
    let mut rounds: BTreeMap<u32, usize> = BTreeMap::new();
    for draw in &report.draws {
        let step = draw.beam.as_ref().expect("a beam draw has a beam record");
        *rounds.entry(step.round).or_default() += 1;
    }
    rounds.into_values().collect()
}

// ── the search ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_beam_of_width_two_finds_the_path_that_the_step_scorer_prefers() {
    let (_dir, db) = fresh_db();
    let model = walker();
    let mut rt = runtime(&db, harness(model.clone(), &policy(&[Strategy::Beam], 12)));
    let report = sample_report(drive(&mut rt, &beam_input(2, 2, 3)).await);

    let selected = report.selected.as_ref().expect("a selected beam");
    assert_eq!(selected.text, "RL", "the result is the preferred path");
    assert_eq!(tool_results(selected), ["\"R\"", "\"L\""]);
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: DrawId::new(6)
        }
    );
    assert_eq!(report.stopped, SampleStop::Completed);

    // Round 0 expands the root twice. Rounds 1 and 2 expand 2 beams twice.
    assert_eq!(per_round(&report), [2, 4, 4]);
    let requests = model.requests();
    assert_eq!(
        requests.len(),
        report.draws.len(),
        "one model call per expansion"
    );
    assert_eq!(requests.len(), 10);
    // No turn is asked twice: each draw asks once, and the root prefix is
    // asked only by the 2 expansions of round 0.
    let draws: BTreeSet<u32> = requests
        .iter()
        .map(|request| request.draw.expect("a draw id").get())
        .collect();
    assert_eq!(draws, (0..10).collect());
    let from_root = requests
        .iter()
        .filter(|request| results(&request.messages).is_empty())
        .count();
    assert_eq!(from_root, 2, "a kept beam never asks for its prefix again");

    // Each round records its verdicts, its parents and its fates.
    let round_one: Vec<_> = report
        .draws
        .iter()
        .filter(|draw| draw.beam.as_ref().unwrap().round == 1)
        .map(|draw| {
            let beam = draw.beam.as_ref().unwrap();
            (
                draw.draw.get(),
                beam.parent.map(DrawId::get),
                beam.fate,
                draw.verdict.as_score().map(FiniteF64::get),
            )
        })
        .collect();
    assert_eq!(
        round_one,
        [
            (2, Some(1), BeamFate::Live, Some(0.9)),
            (3, Some(1), BeamFate::Live, Some(0.5)),
            (4, Some(0), BeamFate::Pruned, Some(0.3)),
            (5, Some(0), BeamFate::Pruned, Some(0.2)),
        ],
        "the parent with the higher score expands first"
    );
    let complete: Vec<u32> = report
        .draws
        .iter()
        .filter(|draw| draw.beam.as_ref().unwrap().fate == BeamFate::Complete)
        .map(|draw| draw.draw.get())
        .collect();
    assert_eq!(complete, [6, 7], "the run stops with width beams complete");
}

#[tokio::test(flavor = "multi_thread")]
async fn max_rounds_with_no_complete_beam_selects_nothing() {
    let (_dir, db) = fresh_db();
    let model = walker();
    let mut rt = runtime(&db, harness(model.clone(), &policy(&[Strategy::Beam], 8)));
    let report = sample_report(drive(&mut rt, &beam_input(2, 2, 2)).await);

    assert_eq!(report.outcome, SampleOutcome::NoAcceptedDraw);
    assert_eq!(report.stopped, SampleStop::RoundsExhausted);
    assert!(report.selected.is_none());
    assert_eq!(per_round(&report), [2, 4]);
    assert_eq!(model.requests().len(), 6);
}

// ── the gate ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_beam_over_max_draws_is_refused_before_any_model_call() {
    for (input, code) in [
        (beam_input(2, 2, 3), "too_many_draws"),
        (beam_input(u32::MAX, u32::MAX, 2), "too_many_draws"),
        (beam_input(0, 2, 3), "no_draws"),
    ] {
        let (_dir, db) = fresh_db();
        let model = walker();
        let mut rt = runtime(&db, harness(model.clone(), &policy(&[Strategy::Beam], 11)));
        match drive(&mut rt, &input).await {
            RunState::Failed(error) => {
                assert!(error.starts_with("sampling refused"), "{error}");
                assert!(error.contains(code), "{error}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert_eq!(model.requests().len(), 0, "no model call before the gate");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_that_does_not_list_beam_is_refused() {
    let (_dir, db) = fresh_db();
    let model = walker();
    let allows = policy(&[Strategy::BestOfN, Strategy::Rejection], 64);
    let mut rt = runtime(&db, harness(model.clone(), &allows));
    match drive(&mut rt, &beam_input(2, 2, 3)).await {
        RunState::Failed(error) => {
            assert!(error.contains("strategy_not_allowed"), "{error}");
            assert!(error.contains("beam"), "{error}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(model.requests().len(), 0);
}

#[test]
fn the_beam_strategy_maps_to_the_policy_strategy() {
    let beam = SampleStrategy::beam(2, 3, 4);
    assert_eq!(beam.policy_strategy(), Strategy::Beam);
    assert_eq!(beam.draws(), 24, "width × expansion × max_rounds");
    assert_eq!(SampleStrategy::beam(u32::MAX, 2, 2).draws(), u32::MAX);
    assert_eq!(
        serde_json::to_value(beam).unwrap(),
        json!({"beam": {"width": 2, "expansion": 3, "max_rounds": 4}})
    );
}

// ── effects and budget ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_default_step_gets_a_refusal_for_a_write_tool() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = TryFnModel::new(|request: &ChatRequest| {
        let draw = request.draw.map_or(0, DrawId::get);
        calls(&[(&format!("c{draw}"), "publish", json!({}))], 5)
    });
    let harness = AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .tool(recorded_tool("publish", ToolEffect::Write, &recorder))
        .scorer(Arc::new(prefers_rl()))
        .sampling_policies(store_with(&policy(&[Strategy::Beam], 4)));
    let mut rt = runtime(&db, harness);
    let report = sample_report(drive(&mut rt, &beam_input(1, 2, 2)).await);

    assert_eq!(recorder.runs().len(), 0, "the tool body never ran");
    assert_eq!(report.outcome, SampleOutcome::NoAcceptedDraw);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_token_budget_stops_the_beam_with_budget_exceeded() {
    let (_dir, db) = fresh_db();
    let model = walker();
    let mut rt = runtime(&db, harness(model.clone(), &policy(&[Strategy::Beam], 12)));
    // Each step spends 5 tokens. Round 0 spends all 10, so round 1 starts
    // no step.
    let input = beam_input(2, 2, 3).max_total_tokens(10);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.stopped, SampleStop::BudgetExceeded);
    assert_eq!(report.usage.total(), 10);
    assert_eq!(model.requests().len(), 2);
    assert_eq!(per_round(&report), [2]);
    assert_eq!(report.outcome, SampleOutcome::NoAcceptedDraw);
}

// ── determinism ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn the_beam_history_replays_to_the_same_selection() {
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
    use autumn_harvest_agent::sampling::agent_sample_info;
    use autumn_harvest_agent::workflow::agent_loop_info;

    let mut reports = Vec::new();
    for _ in 0..2 {
        let (_dir, db) = fresh_db();
        let mut rt = runtime(&db, harness(walker(), &policy(&[Strategy::Beam], 12)));
        let exec = sqlite::start_sample(&mut rt, &beam_input(2, 2, 3)).unwrap();
        let report = sample_report(rt.run_until_blocked(exec).await.unwrap());

        // Replay the recorded history. A different kept beam in round 1
        // changes the child input of round 2, so it fails the replay.
        let history = rt.load_history(exec).unwrap();
        let mut runs = vec![(exec, agent_sample_info(), history.clone())];
        for event in &history {
            if let WorkflowEvent::ChildWorkflowStarted { child_id, .. } = event {
                runs.push((
                    *child_id,
                    agent_loop_info(),
                    rt.load_history(*child_id).unwrap(),
                ));
            }
        }
        assert_eq!(runs.len(), 11, "the parent and 10 steps");
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
        reports.push(report);
    }
    let selected = |report: &SampleReport| {
        (
            report.outcome,
            report.selected.as_ref().map(|one| one.messages.clone()),
        )
    };
    assert_eq!(selected(&reports[0]), selected(&reports[1]));
    let verdicts = |report: &SampleReport| {
        report
            .draws
            .iter()
            .map(|draw| (draw.draw, draw.verdict.clone(), draw.beam))
            .collect::<Vec<_>>()
    };
    assert_eq!(verdicts(&reports[0]), verdicts(&reports[1]));
    assert_eq!(
        reports[0].draws[0].stop,
        AgentStop::StepEnded,
        "a step that ran a tool round ends step_ended"
    );
}
