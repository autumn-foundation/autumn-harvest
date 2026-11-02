//! The operator sampling policy (issue #2439).
//!
//! Each test drives `agent_draws` on SQLite. The `SQLite` runtime runs one
//! activity at a time, so the wall-clock concurrency test is in
//! `sampling_policy_postgres.rs`. This file checks the wave shape in the
//! history.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::sampling::{
    DrawsInput, DrawsReport, InMemorySamplingPolicyStore, MAX_DRAWS, PolicyAction, Refusal,
    SAMPLING_POLICY_FIELDS, SamplingPolicy, SamplingPolicyStore, ScorerId, ScorerRef, Scorers,
    SqliteSamplingPolicyStore, Strategy,
};
use autumn_harvest_agent::{
    AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentStop, AgentTask, ChatRequest,
    MAX_PARALLEL_CHILDREN, Mandate, Peer, ToolCall, ToolCallRequest, ToolEffect, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{Recorder, TryFnModel, answer, calls, recorded_tool, runtime};
use futures::FutureExt as _;
use serde_json::{Value, json};

const KIND: &str = "researcher";

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// A harness with a profile of [`KIND`], so a strategy run of the kind can
/// build its task.
fn with_profile(model: Arc<dyn AgentModel>) -> AgentHarness {
    AgentHarness::new(model).agent(AgentProfile::new(KIND))
}

/// A model that answers every request with one text and counts its calls.
fn plain_model() -> Arc<TryFnModel> {
    TryFnModel::new(|_| answer("an answer", 5))
}

fn best_of(n: u32) -> DrawsInput {
    DrawsInput::new(KIND, "Name a prime.", Strategy::BestOfN, n)
}

fn policy(edit: impl FnOnce(&mut SamplingPolicy)) -> SamplingPolicy {
    let mut policy = SamplingPolicy::default();
    edit(&mut policy);
    policy
}

fn store_with(policy: Option<SamplingPolicy>) -> Arc<InMemorySamplingPolicyStore> {
    let store = Arc::new(InMemorySamplingPolicyStore::new());
    if let Some(policy) = policy {
        store
            .set(KIND, &policy, "ops@example.com")
            .now_or_never()
            .expect("the in-memory store is ready at once")
            .expect("the policy is valid");
    }
    store
}

async fn drive(rt: &mut SqliteRuntime, input: &DrawsInput) -> RunState {
    let exec = sqlite::start_draws(rt, input).expect("the run starts");
    rt.run_until_blocked(exec).await.expect("the run drives")
}

fn draws_report(state: RunState) -> DrawsReport {
    match state {
        RunState::Completed(value) => serde_json::from_value(value).expect("the report decodes"),
        other => panic!("expected a completed run, got {other:?}"),
    }
}

fn refused(state: &RunState, code: &str) {
    match state {
        RunState::Failed(error) => assert!(
            error.contains(code),
            "the error names the refusal {code}: {error}"
        ),
        other => panic!("expected a refused run, got {other:?}"),
    }
}

// ── 1. no policy: every default ─────────────────────────────────────────────

#[test]
fn a_missing_policy_reads_every_default() {
    let store = store_with(None);
    let read = store.read(KIND).now_or_never().unwrap().unwrap();
    assert_eq!(read.revision, 0, "nothing was ever stored");
    let policy = read.policy;
    assert_eq!(policy.strategies.len(), 0, "no strategy by default");
    assert_eq!(policy.max_draws, 1);
    assert_eq!(policy.max_concurrent_draws, 1);
    assert_eq!(policy.draw_effects, BTreeSet::from([ToolEffect::ReadOnly]));
    assert_eq!(policy.scorers, Scorers::Deterministic);
    assert_eq!(policy.judge_models.len(), 0, "no judge model by default");
    assert_eq!(policy.max_answer_retries, 0);
    assert_eq!(policy, SamplingPolicy::default());
}

#[tokio::test(flavor = "multi_thread")]
async fn with_no_policy_best_of_three_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    let harness = with_profile(model.clone()).sampling_policies(store_with(None));
    let mut rt = runtime(&db, harness);
    let state = drive(&mut rt, &best_of(3)).await;
    refused(
        &state,
        Refusal::StrategyNotAllowed {
            strategy: Strategy::BestOfN,
        }
        .code(),
    );
    assert_eq!(model.requests().len(), 0, "no model call");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_harness_with_no_store_reads_every_default() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    let mut rt = runtime(&db, with_profile(model.clone()));
    let state = drive(&mut rt, &best_of(3)).await;
    refused(&state, "strategy_not_allowed");
    assert_eq!(model.requests().len(), 0, "no model call");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_with_no_profile_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    // The policy of the kind allows the run, but no profile builds a task.
    let harness =
        AgentHarness::new(model.clone()).sampling_policies(store_with(Some(best_of_n_up_to(4))));
    let mut rt = runtime(&db, harness);
    refused(
        &drive(&mut rt, &best_of(2)).await,
        Refusal::UnknownKind.code(),
    );
    assert_eq!(model.requests().len(), 0, "no model call");
}

#[tokio::test(flavor = "multi_thread")]
async fn each_draw_runs_the_task_of_the_profile() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    let mut profile = AgentProfile::new(KIND);
    profile.system = Some("You are the researcher.".to_owned());
    let harness = AgentHarness::new(model.clone())
        .agent(profile)
        .sampling_policies(store_with(Some(best_of_n_up_to(2))));
    let mut rt = runtime(&db, harness);
    assert_eq!(
        draws_report(drive(&mut rt, &best_of(2)).await).draws.len(),
        2
    );
    for request in model.requests() {
        let system = request
            .messages
            .first()
            .filter(|message| message.role == autumn_harvest_agent::ChatRole::System)
            .and_then(|message| match message.content.first() {
                Some(autumn_harvest_agent::ContentPart::Text(text)) => Some(text.clone()),
                _ => None,
            });
        assert_eq!(system.as_deref(), Some("You are the researcher."));
    }
}

// ── 2. the draw cap ─────────────────────────────────────────────────────────

fn best_of_n_up_to(max: u32) -> SamplingPolicy {
    policy(|p| {
        p.strategies = BTreeSet::from([Strategy::BestOfN]);
        p.max_draws = max;
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn max_draws_four_runs_four_and_refuses_five() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    let harness =
        with_profile(model.clone()).sampling_policies(store_with(Some(best_of_n_up_to(4))));
    let mut rt = runtime(&db, harness);

    let report = draws_report(drive(&mut rt, &best_of(4)).await);
    assert_eq!(report.draws.len(), 4, "four draws ran");
    assert!(
        report
            .draws
            .iter()
            .all(|draw| draw.stop == AgentStop::Completed)
    );
    assert_eq!(report.strategy, Strategy::BestOfN);
    assert_eq!(
        report.revision, 1,
        "the run names the revision that it read"
    );
    assert_eq!(model.requests().len(), 4, "one model call per draw");

    let state = drive(&mut rt, &best_of(5)).await;
    refused(&state, Refusal::TooManyDraws { asked: 5, max: 4 }.code());
    assert_eq!(
        model.requests().len(),
        4,
        "the refused run made no model call"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_draws_is_refused() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    let harness =
        with_profile(model.clone()).sampling_policies(store_with(Some(best_of_n_up_to(4))));
    let mut rt = runtime(&db, harness);
    refused(&drive(&mut rt, &best_of(0)).await, Refusal::NoDraws.code());
    assert_eq!(model.requests().len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_strategy_outside_the_list_is_refused() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    let harness =
        with_profile(model.clone()).sampling_policies(store_with(Some(best_of_n_up_to(4))));
    let mut rt = runtime(&db, harness);
    let vote = DrawsInput::new(KIND, "Vote.", Strategy::Vote, 2);
    refused(&drive(&mut rt, &vote).await, "strategy_not_allowed");
    assert_eq!(model.requests().len(), 0);
}

// ── 3. the concurrency bound ────────────────────────────────────────────────

/// The draw children in the order of the history. An entry is `true` for a
/// start and `false` for an end.
fn child_spans(events: &[WorkflowEvent]) -> Vec<bool> {
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ChildWorkflowStarted { .. } => Some(true),
            WorkflowEvent::ChildWorkflowCompleted { .. }
            | WorkflowEvent::ChildWorkflowFailed { .. } => Some(false),
            _ => None,
        })
        .collect()
}

/// The `SQLite` runtime runs one activity at a time, so a call counter shows
/// nothing here. The history shows the waves. The wall-clock proof is in
/// `sampling_policy_postgres.rs`.
#[tokio::test(flavor = "multi_thread")]
async fn max_concurrent_draws_two_starts_two_draws_at_most() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    let policy = policy(|p| {
        p.strategies = BTreeSet::from([Strategy::BestOfN]);
        p.max_draws = 4;
        p.max_concurrent_draws = 2;
    });
    let harness = with_profile(model.clone()).sampling_policies(store_with(Some(policy)));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start_draws(&mut rt, &best_of(4)).expect("the run starts");
    let report = draws_report(rt.run_until_blocked(exec).await.expect("the run drives"));
    assert_eq!(report.draws.len(), 4);
    assert_eq!(report.concurrency, 2);

    // The history shows the waves: at most two children run at one time.
    let spans = child_spans(&rt.load_history(exec).expect("the history loads"));
    assert_eq!(spans.iter().filter(|start| **start).count(), 4);
    let mut running = 0_i32;
    let mut most = 0_i32;
    for start in spans {
        running += if start { 1 } else { -1 };
        most = most.max(running);
    }
    assert_eq!(most, 2, "two draws ran together, never three");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_that_asks_for_more_concurrency_is_refused() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    let harness =
        with_profile(model.clone()).sampling_policies(store_with(Some(best_of_n_up_to(4))));
    let mut rt = runtime(&db, harness);
    let input = best_of(4).concurrency(2);
    refused(
        &drive(&mut rt, &input).await,
        Refusal::TooManyConcurrentDraws { asked: 2, max: 1 }.code(),
    );
    assert_eq!(model.requests().len(), 0);
}

// ── 4. the effects of a draw ────────────────────────────────────────────────

/// A model that calls the tool `publish` once, then answers.
fn calls_publish() -> Arc<TryFnModel> {
    TryFnModel::new(|request: &ChatRequest| {
        let answered = request.messages.iter().any(|message| {
            message
                .content
                .iter()
                .any(|part| matches!(part, autumn_harvest_agent::ContentPart::ToolResult { .. }))
        });
        if answered {
            answer("done", 5)
        } else {
            calls(&[("c1", "publish", json!({"post": "hello"}))], 5)
        }
    })
}

async fn publish_draw(effects: Option<BTreeSet<ToolEffect>>) -> (Arc<Recorder>, DrawsReport) {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let policy = policy(|p| {
        p.strategies = BTreeSet::from([Strategy::BestOfN]);
        if let Some(effects) = effects {
            p.draw_effects = effects;
        }
    });
    let harness = with_profile(calls_publish())
        .tool(recorded_tool("publish", ToolEffect::Write, &recorder))
        .sampling_policies(store_with(Some(policy)));
    let mut rt = runtime(&db, harness);
    let report = draws_report(drive(&mut rt, &best_of(1)).await);
    (recorder, report)
}

fn results_of(report: &DrawsReport) -> Vec<String> {
    report.draws.iter().flat_map(common::tool_results).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_default_draw_never_runs_a_write_tool() {
    let (recorder, report) = publish_draw(None).await;
    assert_eq!(recorder.runs().len(), 0, "the tool body never ran");
    let results = results_of(&report);
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(
        results[0].contains("denied by policy: this run may not run a tool of effect write"),
        "the draw reads the effect refusal: {results:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_draw_with_write_in_draw_effects_runs_the_tool() {
    let effects = BTreeSet::from([ToolEffect::ReadOnly, ToolEffect::Write]);
    let (recorder, report) = publish_draw(Some(effects)).await;
    assert_eq!(recorder.runs().len(), 1, "the tool body ran once");
    assert!(!results_of(&report)[0].contains("denied"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_profile_stays_read_only_in_a_draw_that_may_write() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let policy = policy(|p| {
        p.strategies = BTreeSet::from([Strategy::BestOfN]);
        p.draw_effects = BTreeSet::from([ToolEffect::ReadOnly, ToolEffect::Write]);
    });
    let mut profile = AgentProfile::new(KIND);
    profile.read_only = true;
    let harness = AgentHarness::new(calls_publish())
        .agent(profile)
        .tool(recorded_tool("publish", ToolEffect::Write, &recorder))
        .sampling_policies(store_with(Some(policy)));
    let mut rt = runtime(&db, harness);
    let input = best_of(1);
    let report = draws_report(drive(&mut rt, &input).await);
    assert_eq!(recorder.runs().len(), 0, "a read-only task stays read-only");
    assert!(results_of(&report)[0].contains("denied"));
}

/// The text of the first user message of a request.
fn first_user(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .find(|message| message.role == autumn_harvest_agent::ChatRole::User)
        .and_then(|message| match message.content.first() {
            Some(autumn_harvest_agent::ContentPart::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn has_results(request: &ChatRequest) -> bool {
    request.messages.iter().any(|message| {
        message
            .content
            .iter()
            .any(|part| matches!(part, autumn_harvest_agent::ContentPart::ToolResult { .. }))
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn an_inline_child_of_a_draw_keeps_the_effects_of_the_draw() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    // The draw may delegate (`internal`) and write, but not read. Only the
    // effect set of the draw refuses the read of its child.
    let policy = policy(|p| {
        p.strategies = BTreeSet::from([Strategy::BestOfN]);
        p.draw_effects = BTreeSet::from([ToolEffect::Internal, ToolEffect::Write]);
    });
    let model = TryFnModel::new(|request: &ChatRequest| {
        let child = first_user(request) == "Look it up.";
        match (child, has_results(request)) {
            (_, true) => answer("done", 5),
            (true, false) => calls(&[("l1", "lookup", json!({}))], 5),
            (false, false) => calls(
                &[(
                    "d1",
                    DELEGATE_TOOL,
                    json!({"agent": "helper", "task": "Look it up."}),
                )],
                5,
            ),
        }
    });
    let mut profile = AgentProfile::new(KIND);
    profile.mandate = Some(Mandate::new(KIND).peers(["helper"]));
    let harness = AgentHarness::new(model.clone())
        .agent(profile)
        .peer(Peer::new("helper", "Looks things up."))
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .sampling_policies(store_with(Some(policy)));
    let mut rt = runtime(&db, harness);
    let report = draws_report(drive(&mut rt, &best_of(1)).await);
    assert_eq!(recorder.runs().len(), 0, "the child never ran the read");
    let child_results: Vec<String> = model
        .requests()
        .iter()
        .filter(|request| first_user(request) == "Look it up.")
        .flat_map(|request| request.messages.iter())
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            autumn_harvest_agent::ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect();
    assert!(
        child_results
            .iter()
            .any(|text| text.contains("may not run a tool of effect read_only")),
        "the child reads the effect refusal: {child_results:?}"
    );
    assert_eq!(report.draws.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_draw_never_delegates_to_a_durable_peer() {
    let (_dir, db) = fresh_db();
    let policy = policy(|p| {
        p.strategies = BTreeSet::from([Strategy::BestOfN]);
        p.draw_effects = BTreeSet::from([ToolEffect::ReadOnly, ToolEffect::Internal]);
    });
    let model = TryFnModel::new(|request: &ChatRequest| {
        if has_results(request) {
            answer("done", 5)
        } else {
            calls(
                &[(
                    "d1",
                    DELEGATE_TOOL,
                    json!({"agent": "desk", "task": "Ask the desk."}),
                )],
                5,
            )
        }
    });
    let desk = AgentId::new("desk", "one").unwrap();
    let mut profile = AgentProfile::new(KIND);
    profile.mandate = Some(Mandate::new(KIND).peers(["desk"]));
    let harness = AgentHarness::new(model)
        .agent(profile)
        .peer(Peer::agent("desk", "The support desk.", &desk))
        .sampling_policies(store_with(Some(policy)));
    let mut rt = runtime(&db, harness);
    let report = draws_report(drive(&mut rt, &best_of(1)).await);
    let results = results_of(&report);
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(
        results[0].contains("may not delegate"),
        "the delegation is refused: {results:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_tool_call_activity_checks_the_effect_set_again() {
    let recorder = Arc::new(Recorder::default());
    let harness = AgentHarness::new(plain_model()).tool(recorded_tool(
        "publish",
        ToolEffect::Write,
        &recorder,
    ));
    let request = |effects: BTreeSet<ToolEffect>| ToolCallRequest {
        run_id: "run-1".to_owned(),
        session_id: None,
        step: 0,
        call: ToolCall {
            id: "c1".to_owned(),
            name: "publish".to_owned(),
            arguments: json!({}),
        },
        memory_scope: None,
        read_only: false,
        allowed_tools: None,
        labels: None,
        flow_labels: BTreeSet::new(),
        workspace: None,
        skill: None,
        allowed_effects: Some(effects),
    };
    // A decision recorded on a worker with other rules still cannot run it.
    let refused = harness
        .tool_call(request(BTreeSet::from([ToolEffect::ReadOnly])))
        .await
        .unwrap();
    assert!(
        refused
            .content
            .contains("may not run a tool of effect write"),
        "{refused:?}"
    );
    assert_eq!(recorder.runs().len(), 0);
    let allowed = harness
        .tool_call(request(BTreeSet::from([ToolEffect::Write])))
        .await
        .unwrap();
    assert!(!allowed.content.contains("may not run"), "{allowed:?}");
    assert_eq!(recorder.runs().len(), 1);
}

// ── 5. judge models ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_judge_outside_judge_models_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    let policy = policy(|p| {
        p.strategies = BTreeSet::from([Strategy::BestOfN]);
        p.max_draws = 2;
        p.scorers = Scorers::Only(BTreeSet::from([ScorerId::new("judge").unwrap()]));
        p.judge_models = BTreeSet::from(["small-judge".to_owned()]);
    });
    let harness = with_profile(model.clone()).sampling_policies(store_with(Some(policy)));
    let mut rt = runtime(&db, harness);

    let wide = best_of(2).scorer(ScorerRef::judge("judge", "big-judge").unwrap());
    refused(
        &drive(&mut rt, &wide).await,
        Refusal::JudgeModelNotAllowed {
            model: "big-judge".to_owned(),
        }
        .code(),
    );
    assert_eq!(model.requests().len(), 0, "no model call");

    let listed = best_of(2).scorer(ScorerRef::judge("judge", "small-judge").unwrap());
    assert_eq!(draws_report(drive(&mut rt, &listed).await).draws.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_default_scorers_refuse_every_judge() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    let harness =
        with_profile(model.clone()).sampling_policies(store_with(Some(best_of_n_up_to(2))));
    let mut rt = runtime(&db, harness);
    let judged = best_of(2).scorer(ScorerRef::judge("judge", "any").unwrap());
    refused(&drive(&mut rt, &judged).await, "scorer_not_allowed");
    let exact = best_of(2).scorer(ScorerRef::deterministic("exact_match").unwrap());
    assert_eq!(draws_report(drive(&mut rt, &exact).await).draws.len(), 2);
    assert_eq!(
        model.requests().len(),
        2,
        "only the allowed run called the model"
    );
}

// ── 6. a change from another process ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_set_from_one_process_changes_the_next_run_of_a_second_process() {
    let (_dir, db) = fresh_db();
    let model = plain_model();
    // The running process: a runtime and its own store connection.
    let running = Arc::new(SqliteSamplingPolicyStore::open(&db).expect("the store opens"));
    let harness = with_profile(model.clone()).sampling_policies(running);
    let mut rt = runtime(&db, harness);
    refused(&drive(&mut rt, &best_of(3)).await, "strategy_not_allowed");

    // The operator process: its own connection to the same file.
    let operator = SqliteSamplingPolicyStore::open(&db).expect("the store opens");
    operator
        .set(KIND, &best_of_n_up_to(3), "ops@example.com")
        .await
        .expect("the set succeeds");

    // The same runtime, with no restart, reads the new policy.
    let report = draws_report(drive(&mut rt, &best_of(3)).await);
    assert_eq!(report.draws.len(), 3);

    operator
        .reset(KIND, "ops@example.com")
        .await
        .expect("the reset succeeds");
    refused(&drive(&mut rt, &best_of(3)).await, "strategy_not_allowed");
}

// ── 7. a change during a run ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_change_during_a_run_does_not_change_that_run() {
    let (_dir, db) = fresh_db();
    let store = store_with(Some(policy(|p| {
        p.strategies = BTreeSet::from([Strategy::BestOfN]);
        p.max_draws = 3;
    })));
    let seen = Arc::new(AtomicUsize::new(0));
    let model = {
        let store = Arc::clone(&store);
        let seen = Arc::clone(&seen);
        TryFnModel::new(move |_| {
            // The first draw resets the policy. The draws after it still run.
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                store
                    .reset(KIND, "ops@example.com")
                    .now_or_never()
                    .unwrap()
                    .unwrap();
            }
            answer("an answer", 5)
        })
    };
    let harness = with_profile(model.clone()).sampling_policies(store);
    let mut rt = runtime(&db, harness);
    let report = draws_report(drive(&mut rt, &best_of(3)).await);
    assert_eq!(report.draws.len(), 3, "the run kept the policy it read");
    assert_eq!(seen.load(Ordering::SeqCst), 3);
    // The next run reads the reset policy.
    refused(&drive(&mut rt, &best_of(3)).await, "strategy_not_allowed");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_policy_is_read_once_per_run() {
    let (_dir, db) = fresh_db();
    let reads = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(CountingStore {
        inner: store_with(Some(best_of_n_up_to(4))),
        reads: Arc::clone(&reads),
    });
    let harness = with_profile(plain_model()).sampling_policies(store);
    let mut rt = runtime(&db, harness);
    let report = draws_report(drive(&mut rt, &best_of(4)).await);
    assert_eq!(report.draws.len(), 4);
    assert_eq!(reads.load(Ordering::SeqCst), 1, "one read for four draws");
}

/// A store that counts its reads.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<InMemorySamplingPolicyStore>,
    reads: Arc<AtomicUsize>,
}

impl SamplingPolicyStore for CountingStore {
    fn read<'a>(
        &'a self,
        kind: &'a str,
    ) -> autumn_harvest_agent::BoxFuture<
        'a,
        Result<
            autumn_harvest_agent::sampling::PolicyRead,
            autumn_harvest_agent::sampling::PolicyStoreError,
        >,
    > {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read(kind)
    }

    fn set<'a>(
        &'a self,
        kind: &'a str,
        policy: &'a SamplingPolicy,
        actor: &'a str,
    ) -> autumn_harvest_agent::BoxFuture<
        'a,
        Result<
            autumn_harvest_agent::sampling::PolicyChange,
            autumn_harvest_agent::sampling::PolicyStoreError,
        >,
    > {
        self.inner.set(kind, policy, actor)
    }

    fn reset<'a>(
        &'a self,
        kind: &'a str,
        actor: &'a str,
    ) -> autumn_harvest_agent::BoxFuture<
        'a,
        Result<
            autumn_harvest_agent::sampling::PolicyChange,
            autumn_harvest_agent::sampling::PolicyStoreError,
        >,
    > {
        self.inner.reset(kind, actor)
    }

    fn audit<'a>(
        &'a self,
        kind: &'a str,
        limit: u32,
    ) -> autumn_harvest_agent::BoxFuture<
        'a,
        Result<
            Vec<autumn_harvest_agent::sampling::PolicyAudit>,
            autumn_harvest_agent::sampling::PolicyStoreError,
        >,
    > {
        self.inner.audit(kind, limit)
    }
}

// ── 9. a run never carries a policy field ───────────────────────────────────

#[test]
fn the_field_list_names_every_policy_key() {
    let encoded = serde_json::to_value(SamplingPolicy::default()).unwrap();
    let keys: BTreeSet<&str> = encoded
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    for key in &keys {
        assert!(SAMPLING_POLICY_FIELDS.contains(key), "{key} is listed");
    }
    assert!(SAMPLING_POLICY_FIELDS.contains(&"sampling_policy"));
}

fn with_field(mut value: Value, field: &str) -> Value {
    value
        .as_object_mut()
        .unwrap()
        .insert(field.to_owned(), json!(4));
    value
}

#[test]
fn an_agent_task_with_a_policy_field_fails_to_decode() {
    let task = serde_json::to_value(AgentTask::new("hello")).unwrap();
    assert!(serde_json::from_value::<AgentTask>(task.clone()).is_ok());
    for field in SAMPLING_POLICY_FIELDS {
        let err = serde_json::from_value::<AgentTask>(with_field(task.clone(), field))
            .expect_err("a policy field fails the decode");
        assert!(err.to_string().contains(field), "{err}");
    }
    // Any other unknown key still decodes, as before.
    assert!(serde_json::from_value::<AgentTask>(with_field(task, "a_later_field")).is_ok());
}

#[test]
fn an_agent_op_with_a_policy_field_fails_to_decode() {
    let ops = [
        AgentOp::message("hello"),
        AgentOp::Control(autumn_harvest_agent::Control::Resume),
    ];
    for op in ops {
        let value = serde_json::to_value(&op).unwrap();
        assert_eq!(
            serde_json::from_value::<AgentOp>(value.clone()).unwrap(),
            op
        );
        for field in SAMPLING_POLICY_FIELDS {
            let err = serde_json::from_value::<AgentOp>(with_field(value.clone(), field))
                .expect_err("a policy field fails the decode");
            assert!(err.to_string().contains(field), "{field} in {op:?}: {err}");
        }
    }
}

#[test]
fn a_strategy_input_with_a_policy_field_fails_to_decode() {
    let input = serde_json::to_value(best_of(2)).unwrap();
    assert_eq!(
        serde_json::from_value::<DrawsInput>(input.clone()).unwrap(),
        best_of(2)
    );
    for field in SAMPLING_POLICY_FIELDS {
        assert!(
            serde_json::from_value::<DrawsInput>(with_field(input.clone(), field)).is_err(),
            "{field} fails the decode"
        );
    }
}

// ── validation ──────────────────────────────────────────────────────────────

#[test]
fn a_policy_out_of_bounds_is_refused_by_the_store() {
    let store = store_with(None);
    let bad = [
        policy(|p| p.max_draws = 0),
        policy(|p| p.max_draws = MAX_DRAWS + 1),
        policy(|p| p.max_concurrent_draws = 0),
        policy(|p| p.max_concurrent_draws = MAX_PARALLEL_CHILDREN + 1),
        policy(|p| p.judge_models = BTreeSet::from([String::new()])),
    ];
    for policy in bad {
        let result = store
            .set(KIND, &policy, "ops@example.com")
            .now_or_never()
            .unwrap();
        assert!(result.is_err(), "{policy:?} is refused");
    }
    let read = store.read(KIND).now_or_never().unwrap().unwrap();
    assert_eq!(read.revision, 0, "no refused set stored anything");
}

#[test]
fn a_set_and_a_reset_are_audited_with_the_old_and_new_values() {
    let (_dir, db) = fresh_db();
    let stores: Vec<Arc<dyn SamplingPolicyStore>> = vec![
        store_with(None),
        Arc::new(SqliteSamplingPolicyStore::open(&db).expect("the store opens")),
    ];
    for store in stores {
        audits_each_change(store.as_ref());
    }
}

fn audits_each_change(store: &dyn SamplingPolicyStore) {
    let first = best_of_n_up_to(2);
    let second = best_of_n_up_to(3);
    for policy in [&first, &second] {
        store
            .set(KIND, policy, "ops@example.com")
            .now_or_never()
            .unwrap()
            .unwrap();
    }
    let change = store
        .reset(KIND, "lead@example.com")
        .now_or_never()
        .unwrap()
        .unwrap();
    assert_eq!(change.old_policy(), Some(second.clone()));
    assert_eq!(change.new, None);
    assert_eq!(change.revision, 3);

    let audit = store.audit(KIND, 10).now_or_never().unwrap().unwrap();
    let rows: Vec<_> = audit
        .iter()
        .map(|row| {
            (
                row.revision,
                row.action,
                row.old_policy(),
                row.new_policy(),
                row.actor.as_str(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (
                3,
                PolicyAction::Reset,
                Some(second.clone()),
                None,
                "lead@example.com"
            ),
            (
                2,
                PolicyAction::Set,
                Some(first.clone()),
                Some(second),
                "ops@example.com"
            ),
            (1, PolicyAction::Set, None, Some(first), "ops@example.com"),
        ],
        "the newest row first"
    );
    assert_eq!(
        store.audit(KIND, 1).now_or_never().unwrap().unwrap().len(),
        1
    );
    let read = store.read(KIND).now_or_never().unwrap().unwrap();
    assert_eq!(
        read.policy,
        SamplingPolicy::default(),
        "a reset restores every default"
    );
    assert_eq!(read.revision, 3, "the revision never goes back");

    // A refused write changes nothing and writes no audit row.
    assert!(
        store
            .set(KIND, &first_bad(), "ops@example.com")
            .now_or_never()
            .unwrap()
            .is_err()
    );
    assert!(
        store
            .set(KIND, &best_of_n_up_to(2), " padded")
            .now_or_never()
            .unwrap()
            .is_err()
    );
    assert!(
        store
            .set("Not A Kind", &best_of_n_up_to(2), "ops")
            .now_or_never()
            .unwrap()
            .is_err()
    );
    assert_eq!(
        store.read(KIND).now_or_never().unwrap().unwrap().revision,
        3
    );
    assert_eq!(
        store.audit(KIND, 10).now_or_never().unwrap().unwrap().len(),
        3
    );
}

fn first_bad() -> SamplingPolicy {
    policy(|p| p.max_draws = 0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_stored_policy_fails_the_run_and_a_reset_recovers() {
    let (_dir, db) = fresh_db();
    // A newer binary stored a key that this binary does not know.
    let raw = autumn_harvest_sqlite::SqlitePolicyLog::open(&db).expect("the log opens");
    raw.write(
        KIND,
        "sampling",
        Some(r#"{"max_drawz": 3}"#),
        "ops@example.com",
    )
    .expect("the raw write succeeds")
    .expect("the counter has room");
    let store = Arc::new(SqliteSamplingPolicyStore::open(&db).expect("the store opens"));
    assert!(matches!(
        store.read(KIND).await,
        Err(autumn_harvest_agent::sampling::PolicyStoreError::Corrupt { .. })
    ));

    // The run fails closed. It never falls back to the defaults.
    let model = plain_model();
    let harness = with_profile(model.clone()).sampling_policies(store.clone());
    let mut rt = runtime(&db, harness);
    match drive(&mut rt, &best_of(1)).await {
        RunState::Failed(error) => assert!(error.contains("corrupt"), "{error}"),
        other => panic!("expected a failed run, got {other:?}"),
    }
    assert_eq!(model.requests().len(), 0, "no model call");

    // The audit stays readable, and a reset reports its success.
    assert_eq!(store.audit(KIND, 10).await.unwrap().len(), 1);
    let change = store
        .reset(KIND, "ops@example.com")
        .await
        .expect("the reset commits");
    assert_eq!(change.old, Some(json!({"max_drawz": 3})));
    assert_eq!(change.old_policy(), None, "the old body does not decode");
    assert_eq!(
        store.read(KIND).await.unwrap().policy,
        SamplingPolicy::default()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn each_draw_has_its_own_cache_entry() {
    let (_dir, db) = fresh_db();
    let replies = (0..5).map(|n| answer(&format!("answer {n}"), 5)).collect();
    let model = common::ScriptedModel::named("m1", replies);
    let cache_store = Arc::new(common::MemStore::default());
    let mut profile = AgentProfile::new(KIND);
    profile.tenant = Some("acme".to_owned());
    let harness = AgentHarness::new(model.clone())
        .agent(profile)
        .response_cache(common::response_cache(&cache_store))
        .sampling_policies(store_with(Some(best_of_n_up_to(3))));
    let mut rt = runtime(&db, harness);

    // The cache is live: a second plain run of one request is a hit.
    let plain = AgentTask::new("Name a prime.").tenant("acme");
    for _ in 0..2 {
        let exec = sqlite::start(&mut rt, &plain).expect("the run starts");
        common::report(rt.run_until_blocked(exec).await.expect("the run drives"));
    }
    assert_eq!(model.calls(), 1, "the second plain run reads the cache");

    // Three draws of the same request make three model calls.
    let report = draws_report(drive(&mut rt, &best_of(3)).await);
    let texts: BTreeSet<String> = report.draws.iter().map(|draw| draw.text.clone()).collect();
    assert_eq!(model.calls(), 4, "each draw calls the model");
    assert_eq!(texts.len(), 3, "three distinct samples: {texts:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn each_draw_runs_the_active_overlay_of_its_kind() {
    use autumn_harvest_agent::{
        InMemoryOverlayStore, ProfileOverlay, ProfileOverlayStore, Provenance,
    };

    let (_dir, db) = fresh_db();
    let model = plain_model();
    let base = AgentHarness::new(model.clone())
        .agent(AgentProfile::new(KIND).system("The base prompt."))
        .sampling_policies(store_with(Some(best_of_n_up_to(2))));
    let overlays = Arc::new(InMemoryOverlayStore::new(Arc::new(base.clone())));
    let overlay = ProfileOverlay {
        system: Some("The overlaid prompt.".to_owned()),
        ..ProfileOverlay::default()
    };
    let version = overlays
        .put(KIND, &overlay, Provenance::operator("ops@example.com"))
        .await
        .expect("the overlay is valid");
    overlays
        .activate(KIND, version, None)
        .await
        .expect("the version activates");
    let harness = base.overlay_store(overlays);
    let mut rt = runtime(&db, harness);

    assert_eq!(
        draws_report(drive(&mut rt, &best_of(2)).await).draws.len(),
        2
    );
    let systems: Vec<Option<String>> = model
        .requests()
        .iter()
        .map(|request| {
            request
                .messages
                .first()
                .filter(|message| message.role == autumn_harvest_agent::ChatRole::System)
                .and_then(|message| match message.content.first() {
                    Some(autumn_harvest_agent::ContentPart::Text(text)) => Some(text.clone()),
                    _ => None,
                })
        })
        .collect();
    assert_eq!(systems.len(), 2);
    for system in systems {
        let system = system.expect("each draw has a system prompt");
        assert!(system.starts_with("The overlaid prompt."), "{system}");
    }
}
