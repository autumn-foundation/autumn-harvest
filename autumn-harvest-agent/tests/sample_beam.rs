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
    harness_with(model, policy, prefers_rl())
}

fn harness_with(model: Arc<TryFnModel>, policy: &SamplingPolicy, scorer: Paths) -> AgentHarness {
    AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .tool(walk())
        .scorer(Arc::new(scorer))
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

    // The first round expands the root twice. Each later round expands 2
    // beams twice.
    assert_eq!(per_round(&report), [2, 4, 4]);
    let requests = model.requests();
    assert_eq!(
        requests.len(),
        report.draws.len(),
        "one model call per expansion"
    );
    assert_eq!(requests.len(), 10);
    // No turn is asked twice: each draw asks once, and the root prefix is
    // asked only by the 2 expansions of the first round.
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
    let second_round: Vec<_> = report
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
        second_round,
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
    // The first step asks to publish. The next step answers.
    let model = TryFnModel::new(|request: &ChatRequest| {
        if results(&request.messages).is_empty() {
            let draw = request.draw.map_or(0, DrawId::get);
            calls(&[(&format!("c{draw}"), "publish", json!({}))], 5)
        } else {
            answer("done", 5)
        }
    });
    let harness = AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .tool(recorded_tool("publish", ToolEffect::Write, &recorder))
        .scorer(Arc::new(Flat))
        .sampling_policies(store_with(&policy(&[Strategy::Beam], 4)));
    let mut rt = runtime(&db, harness);
    let input = SampleInput::new(
        KIND,
        "Post it.",
        SampleStrategy::beam(1, 2, 2),
        ScorerRef::deterministic("flat").unwrap(),
    );
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(recorder.runs().len(), 0, "the tool body never ran");
    assert_eq!(report.draws[0].stop, AgentStop::StepEnded);
    let selected = report.selected.expect("the beam completes");
    let results = tool_results(&selected);
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(
        results[0].contains("denied by policy: this run may not run a tool of effect write"),
        "the step reads the effect refusal: {results:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_token_budget_stops_the_beam_with_budget_exceeded() {
    let (_dir, db) = fresh_db();
    let model = walker();
    let mut rt = runtime(&db, harness(model.clone(), &policy(&[Strategy::Beam], 12)));
    // Each step spends 5 tokens. The first round spends all 10, so the
    // second round starts no step.
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

    // The child inputs without the draw root, a fresh UUID per batch.
    let step_inputs = |history: &[WorkflowEvent]| -> Vec<serde_json::Value> {
        history
            .iter()
            .filter_map(|event| match event {
                WorkflowEvent::ChildWorkflowStarted { input, .. } => {
                    let mut input = input.clone();
                    input["draw"].as_object_mut().unwrap().remove("root");
                    Some(input)
                }
                _ => None,
            })
            .collect()
    };
    let mut reports = Vec::new();
    let mut inputs = Vec::new();
    for _ in 0..2 {
        let (_dir, db) = fresh_db();
        let mut rt = runtime(&db, harness(walker(), &policy(&[Strategy::Beam], 12)));
        let exec = sqlite::start_sample(&mut rt, &beam_input(2, 2, 3)).unwrap();
        let report = sample_report(rt.run_until_blocked(exec).await.unwrap());

        // Replay the recorded history. A different kept beam in the second round
        // changes the child input of the third round, so it fails the replay.
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
        // The recorded output is the report that the run returned.
        let recorded = history.iter().find_map(|event| match event {
            WorkflowEvent::WorkflowCompleted { output } => Some(output.clone()),
            _ => None,
        });
        assert_eq!(recorded, Some(serde_json::to_value(&report).unwrap()));
        inputs.push(step_inputs(&history));
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
    assert_eq!(inputs[0], inputs[1], "both runs start the same steps");
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

// ── flow labels ─────────────────────────────────────────────────────────────

/// A read-only tool named `name` with the flow label `label`. It counts its
/// runs.
fn labelled(name: &str, label: &str, runs: &Arc<Recorder>) -> Arc<dyn Tool> {
    let runs = Arc::clone(runs);
    FnTool::new(
        name,
        "A labelled tool.",
        json!({"type": "object"}),
        move |input| {
            let runs = Arc::clone(&runs);
            async move {
                runs.push(input);
                Ok(json!("ok"))
            }
        },
    )
    .effect(ToolEffect::ReadOnly)
    .label(label)
    .shared()
}

/// A step scorer that scores each step 1.
#[derive(Debug)]
struct Flat;

impl Scorer for Flat {
    fn id(&self) -> &str {
        "flat"
    }

    fn score<'a>(
        &'a self,
        _input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        Box::pin(std::future::ready(Verdict::score(1.0)))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_beam_carries_its_flow_labels_into_the_next_step() {
    let (_dir, db) = fresh_db();
    let reads = Arc::new(Recorder::default());
    let sends = Arc::new(Recorder::default());
    // The first step reads a secret. The second step sends.
    let model = TryFnModel::new(
        |request: &ChatRequest| match results(&request.messages).len() {
            0 => calls(&[("c0", "read", json!({}))], 5),
            1 => calls(&[("c1", "send", json!({}))], 5),
            _ => answer("done", 5),
        },
    );
    let harness = AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .tool(labelled("read", "secret", &reads))
        .tool(labelled("send", "outbound", &sends))
        .flow_rules(autumn_harvest_agent::FlowRules::new().forbid(
            ["secret", "outbound"],
            autumn_harvest_agent::Rule::Deny("no secret leaves".into()),
        ))
        .scorer(Arc::new(Flat))
        .sampling_policies(store_with(&policy(&[Strategy::Beam], 3)));
    let mut rt = runtime(&db, harness);
    let input = SampleInput::new(
        KIND,
        "Leak it.",
        SampleStrategy::beam(1, 1, 3),
        ScorerRef::deterministic("flat").unwrap(),
    );
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(reads.runs().len(), 1, "the first step reads the secret");
    assert_eq!(
        sends.runs().len(),
        0,
        "the second step holds the label of the first, so the rule denies the send"
    );
    let selected = report.selected.expect("the beam completes");
    assert!(selected.flow_labels.contains("secret"), "{selected:?}");
}

// ── review follow-ups ───────────────────────────────────────────────────────

/// A model that walks one step, then answers `done`.
fn one_step() -> Arc<TryFnModel> {
    TryFnModel::new(|request: &ChatRequest| {
        if results(&request.messages).is_empty() {
            let draw = request.draw.map_or(0, DrawId::get);
            calls(&[(&format!("c{draw}"), "walk", json!({"dir": "L"}))], 5)
        } else {
            answer("done", 5)
        }
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_built_in_scorer_keeps_a_tool_step_and_checks_the_answer() {
    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(one_step())
        .agent(AgentProfile::new(KIND))
        .tool(walk())
        .scorer(Arc::new(autumn_harvest_agent::ExactMatch::new("done")))
        .sampling_policies(store_with(&policy(&[Strategy::Beam], 3)));
    let mut rt = runtime(&db, harness);
    let input = SampleInput::new(
        KIND,
        "Walk, then say done.",
        SampleStrategy::beam(1, 1, 3),
        ScorerRef::deterministic(autumn_harvest_agent::ExactMatch::ID).unwrap(),
    );
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.draws[0].stop, AgentStop::StepEnded);
    assert_eq!(
        report.draws[0].verdict.as_score().map(FiniteF64::get),
        Some(0.0),
        "a partial beam has no answer to check yet"
    );
    assert_eq!(report.selected.expect("the beam completes").text, "done");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_beam_path_never_runs_more_tool_rounds_than_the_profile_allows() {
    let (_dir, db) = fresh_db();
    let walks = Arc::new(Recorder::default());
    let mut profile = AgentProfile::new(KIND);
    profile.max_steps = 1;
    let harness = AgentHarness::new(walker())
        .agent(profile)
        .tool(labelled("walk", "path", &walks))
        .scorer(Arc::new(Flat))
        .sampling_policies(store_with(&policy(&[Strategy::Beam], 5)));
    let mut rt = runtime(&db, harness);
    let input = SampleInput::new(
        KIND,
        "Find the path.",
        SampleStrategy::beam(1, 1, 5),
        ScorerRef::deterministic("flat").unwrap(),
    );
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(walks.runs().len(), 1, "the profile allows one tool round");
    let stops: Vec<AgentStop> = report.draws.iter().map(|draw| draw.stop).collect();
    assert_eq!(stops, [AgentStop::StepEnded, AgentStop::StepsExhausted]);
    assert_eq!(report.outcome, SampleOutcome::NoAcceptedDraw);
    assert_eq!(report.stopped, SampleStop::Completed, "no beam is live");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_round_keeps_by_score_not_by_draw_order() {
    let (_dir, db) = fresh_db();
    let model = walker();
    // The root step L scores higher, but its child RL is the best path.
    let scorer = Paths(BTreeMap::from([
        ("L", 0.6),
        ("R", 0.4),
        ("LL", 0.1),
        ("LR", 0.2),
        ("RL", 0.9),
        ("RR", 0.3),
    ]));
    let mut rt = runtime(
        &db,
        harness_with(model.clone(), &policy(&[Strategy::Beam], 12), scorer),
    );
    let report = sample_report(drive(&mut rt, &beam_input(2, 2, 3)).await);

    let fates: Vec<(u32, Option<u32>, BeamFate)> = report
        .draws
        .iter()
        .map(|draw| {
            let beam = draw.beam.unwrap();
            (draw.draw.get(), beam.parent.map(DrawId::get), beam.fate)
        })
        .collect();
    assert_eq!(
        &fates[2..6],
        [
            (2, Some(0), BeamFate::Pruned),
            (3, Some(0), BeamFate::Pruned),
            (4, Some(1), BeamFate::Live),
            (5, Some(1), BeamFate::Live),
        ],
        "the round keeps the children of the lower root, by score"
    );
    assert_eq!(report.selected.unwrap().text, "RL");
    assert_eq!(model.requests().len(), 10);
}
