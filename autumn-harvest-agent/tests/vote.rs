//! The vote strategy of `agent_sample` on SQLite.
//!
//! A vote keys each final answer and takes the most common key. It needs
//! no scorer. The run starts no more draws when the leader cannot lose.
//! The crash test is in `vote_crash.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use autumn_harvest_agent::sampling::{
    AnswerKey, AnswerKeyFn, InMemorySamplingPolicyStore, SampleInput, SampleOutcome, SampleReport,
    SampleStop, SampleStrategy, SamplingPolicy, SamplingPolicyStore, ScorerRef, Strategy,
    VoteCount,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, AgentStop, ChatRequest, DrawId, ErrorKind,
    ExactMatch, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{TryFnModel, answer, runtime};
use futures::FutureExt as _;
use serde_json::json;

const KIND: &str = "researcher";

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// A policy that allows `strategies`, up to `max` draws, 4 at a time.
fn allowing(strategies: &[Strategy], max: u32) -> SamplingPolicy {
    SamplingPolicy {
        strategies: strategies.iter().copied().collect::<BTreeSet<_>>(),
        max_draws: max,
        max_concurrent_draws: 4,
        ..SamplingPolicy::default()
    }
}

fn voting(max: u32) -> SamplingPolicy {
    allowing(&[Strategy::Vote], max)
}

/// A model that answers draw `n` with `answers[n]`, with 5 tokens.
fn by_draw(answers: &'static [&'static str]) -> Arc<TryFnModel> {
    TryFnModel::new(move |request: &ChatRequest| {
        let n = request.draw.map_or(0, |draw| draw.get() as usize);
        answer(answers[n % answers.len()], 5)
    })
}

/// A key function that lower-cases the trimmed answer.
#[derive(Debug)]
struct Lower;

impl AnswerKeyFn for Lower {
    fn id(&self) -> &'static str {
        "lower"
    }

    fn key(&self, answer: &str) -> Option<String> {
        Some(answer.trim().to_lowercase())
    }
}

fn harness(model: Arc<dyn AgentModel>, policy: &SamplingPolicy) -> AgentHarness {
    let store = Arc::new(InMemorySamplingPolicyStore::new());
    store
        .set(KIND, policy, "ops@example.com")
        .now_or_never()
        .expect("the in-memory store is ready at once")
        .expect("the policy is valid");
    AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::new(ExactMatch::new("a")))
        .answer_key(Arc::new(Lower))
        .sampling_policies(store)
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

fn refusal(state: RunState) -> String {
    match state {
        RunState::Failed(error) => error,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

fn count(key: &str, votes: u32, first: u32) -> VoteCount {
    VoteCount::new(key, votes, DrawId::new(first))
}

const fn selected(draw: u32) -> SampleOutcome {
    SampleOutcome::Selected {
        draw: DrawId::new(draw),
    }
}

// ── the early stop ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn three_matching_answers_of_five_stop_the_vote_at_three_draws() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a", "a", "a", "b", "c"]);
    let mut rt = runtime(&db, harness(model.clone(), &voting(5)));
    let input = SampleInput::vote(KIND, "Pick a letter.", 5, AnswerKey::Trimmed).concurrency(1);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.attempts, 3, "exactly 3 draws");
    assert_eq!(model.requests().len(), 3, "no fourth model call");
    assert_eq!(report.stopped, SampleStop::Settled);
    assert_eq!(report.outcome, selected(0));
    assert_eq!(report.tally, [count("a", 3, 0)]);
    assert_eq!(report.selected.unwrap().text, "a");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tie_goes_to_the_key_that_voted_first() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a", "b", "a", "b", "c"]);
    let mut rt = runtime(&db, harness(model.clone(), &voting(5)));
    let input = SampleInput::vote(KIND, "Pick a letter.", 5, AnswerKey::Trimmed).concurrency(1);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.outcome, selected(0), "a voted first, at draw 0");
    assert_eq!(report.attempts, 5);
    assert_eq!(report.stopped, SampleStop::Completed);
    assert_eq!(
        report.tally,
        [count("a", 2, 0), count("b", 2, 1), count("c", 1, 4)],
        "the full tally, the winner first"
    );
    assert_eq!(model.requests().len(), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wide_wave_counts_each_draw_in_flight() {
    let (_dir, db) = fresh_db();
    // Draws 0 to 3 run in one wave. After it, a has 3 votes and 1 draw is
    // left, so the run stops. Draw 3 voted for b, and it counts.
    let model = by_draw(&["a", "a", "a", "b", "c"]);
    let mut rt = runtime(&db, harness(model.clone(), &voting(5)));
    let input = SampleInput::vote(KIND, "Pick a letter.", 5, AnswerKey::Trimmed).concurrency(4);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.attempts, 4);
    assert_eq!(report.concurrency, 4);
    assert_eq!(report.stopped, SampleStop::Settled);
    assert_eq!(report.tally, [count("a", 3, 0), count("b", 1, 3)]);
    assert_eq!(model.requests().len(), 4);
}

// ── keys ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_draw_with_no_key_casts_no_vote() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["about 7", "no idea", "7.0 apples"]);
    let mut rt = runtime(&db, harness(model, &voting(3)));
    let input = SampleInput::vote(KIND, "Count.", 3, AnswerKey::LastNumber).concurrency(1);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.tally, [count("7", 2, 0)]);
    assert_eq!(report.draws[1].key, None);
    assert!(report.draws[1].verdict.is_reject());
    assert_eq!(report.draws[2].key.as_deref(), Some("7"));
    assert!(!report.draws[2].verdict.is_reject());
    assert_eq!(report.outcome, selected(0));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_vote_with_no_key_ends_with_no_accepted_draw() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["no idea"]);
    let mut rt = runtime(&db, harness(model.clone(), &voting(3)));
    let input = SampleInput::vote(KIND, "Count.", 3, AnswerKey::LastNumber);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.outcome, SampleOutcome::NoAcceptedDraw);
    assert!(report.selected.is_none());
    assert_eq!(report.tally, Vec::new());
    assert_eq!(report.attempts, 3);
    assert_eq!(report.stopped, SampleStop::Completed);
    assert!(report.draws.iter().all(|draw| draw.verdict.is_reject()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_draw_casts_no_vote() {
    let (_dir, db) = fresh_db();
    let model =
        TryFnModel::new(
            |request: &ChatRequest| match request.draw.map_or(0, DrawId::get) {
                0 => Err(AgentError::new(ErrorKind::Provider, "the provider refused")),
                _ => answer("b", 5),
            },
        );
    let mut rt = runtime(&db, harness(model, &voting(3)));
    let input = SampleInput::vote(KIND, "Pick.", 3, AnswerKey::Trimmed).concurrency(1);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.draws[0].stop, AgentStop::Failed);
    assert_eq!(report.draws[0].key, None);
    assert_eq!(report.tally, [count("b", 2, 1)]);
    assert_eq!(report.outcome, selected(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_json_field_keys_the_field_of_a_json_answer() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&[
        r#"{"answer": 4, "why": "two and two"}"#,
        r#"{"why": "a guess", "answer": 4}"#,
        r#"{"answer": 5}"#,
    ]);
    let mut rt = runtime(&db, harness(model, &voting(3)));
    let input = SampleInput::vote(KIND, "Add.", 3, AnswerKey::json_field("/answer")).concurrency(1);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.tally, [count("4", 2, 0)]);
    assert_eq!(
        report.stopped,
        SampleStop::Settled,
        "2 votes beat 0 with 1 left"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_key_function_keys_each_answer() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["Yes", " YES ", "no"]);
    let mut rt = runtime(&db, harness(model.clone(), &voting(3)));
    let input = SampleInput::vote(KIND, "Agree?", 3, AnswerKey::custom("lower")).concurrency(1);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.tally, [count("yes", 2, 0)]);
    assert_eq!(report.attempts, 2);
    assert_eq!(model.requests().len(), 2);
}

// ── refusals ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_that_does_not_list_vote_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"]);
    let policy = allowing(&[Strategy::BestOfN, Strategy::Rejection], 5);
    let mut rt = runtime(&db, harness(model.clone(), &policy));
    let input = SampleInput::vote(KIND, "Pick.", 3, AnswerKey::Trimmed);
    let error = refusal(drive(&mut rt, &input).await);
    assert!(error.contains("strategy_not_allowed"), "{error}");
    assert!(error.contains("vote"), "{error}");
    assert_eq!(model.requests().len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_vote_with_a_scorer_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"]);
    let mut rt = runtime(&db, harness(model.clone(), &voting(3)));
    let input = SampleInput::new(
        KIND,
        "Pick.",
        SampleStrategy::vote(3, AnswerKey::Trimmed),
        ScorerRef::deterministic(ExactMatch::ID).unwrap(),
    );
    let error = refusal(drive(&mut rt, &input).await);
    assert!(error.contains("scorer_not_used"), "{error}");
    assert_eq!(model.requests().len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn best_of_n_with_no_scorer_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"]);
    let mut rt = runtime(
        &db,
        harness(model.clone(), &allowing(&[Strategy::BestOfN], 3)),
    );
    let mut input = SampleInput::vote(KIND, "Pick.", 3, AnswerKey::Trimmed);
    input.strategy = SampleStrategy::best_of_n(3);
    let error = refusal(drive(&mut rt, &input).await);
    assert!(error.contains("no_scorer"), "{error}");
    assert_eq!(model.requests().len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_app_key_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"]);
    let mut rt = runtime(&db, harness(model.clone(), &voting(3)));
    let input = SampleInput::vote(KIND, "Pick.", 3, AnswerKey::custom("missing"));
    let error = refusal(drive(&mut rt, &input).await);
    assert!(error.contains("unknown_answer_key"), "{error}");
    assert_eq!(model.requests().len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_that_is_not_valid_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"]);
    let mut rt = runtime(&db, harness(model.clone(), &voting(3)));
    for key in [
        AnswerKey::json_field("answer"),
        AnswerKey::custom("Not A Name"),
    ] {
        let input = SampleInput::vote(KIND, "Pick.", 3, key);
        let error = refusal(drive(&mut rt, &input).await);
        assert!(error.contains("invalid_answer_key"), "{error}");
    }
    assert_eq!(model.requests().len(), 0);
}

// ── the budget and the wire form ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_vote_shares_the_token_budget_of_the_run_tree() {
    let (_dir, db) = fresh_db();
    // Each draw spends 5 tokens. The tree has 10.
    let model = by_draw(&["a", "b", "b", "b"]);
    let mut rt = runtime(&db, harness(model.clone(), &voting(4)));
    let input = SampleInput::vote(KIND, "Pick.", 4, AnswerKey::Trimmed)
        .concurrency(1)
        .max_total_tokens(10);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.stopped, SampleStop::BudgetExceeded);
    assert_eq!(report.attempts, 2);
    assert_eq!(
        report.outcome,
        selected(0),
        "a and b tie, and a voted first"
    );
    assert_eq!(model.requests().len(), 2);
}

#[test]
fn a_vote_strategy_has_a_snake_case_wire_form() {
    let value = json!({"vote": {"n": 3, "key": "last_number"}});
    let strategy: SampleStrategy = serde_json::from_value(value).unwrap();
    assert_eq!(strategy, SampleStrategy::vote(3, AnswerKey::LastNumber));
    assert_eq!(strategy.policy_strategy(), Some(Strategy::Vote));
    assert_eq!(strategy.draws(), 3);

    let field = SampleStrategy::vote(2, AnswerKey::json_field("/a"));
    assert_eq!(
        serde_json::to_value(&field).unwrap(),
        json!({"vote": {"n": 2, "key": {"json_field": "/a"}}})
    );
    let custom = json!({"vote": {"n": 2, "key": {"custom": "lower"}}});
    let decoded: SampleStrategy = serde_json::from_value(custom).unwrap();
    assert_eq!(decoded, SampleStrategy::vote(2, AnswerKey::custom("lower")));
}

#[test]
fn a_sample_input_with_no_scorer_omits_the_key() {
    let input = SampleInput::vote(KIND, "Pick.", 3, AnswerKey::Trimmed);
    let value = serde_json::to_value(&input).unwrap();
    assert!(value.get("scorer").is_none(), "{value}");
    let back: SampleInput = serde_json::from_value(value).unwrap();
    assert_eq!(back, input);
}

// ── replay ──────────────────────────────────────────────────────────────────

/// The history of the run `exec` and of each draw run, with its workflow.
fn runs_of(
    rt: &SqliteRuntime,
    exec: autumn_harvest_sqlite::ExecutionId,
) -> Vec<(
    autumn_harvest_sqlite::ExecutionId,
    autumn_harvest::WorkflowInfo,
    Vec<autumn_harvest::event::WorkflowEvent>,
)> {
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest_agent::sampling::agent_sample_info;
    use autumn_harvest_agent::workflow::agent_loop_info;

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
    runs
}

/// Replay each run of `runs`, and check that each replay succeeds.
/// Returns the count of runs.
async fn replays_clean(
    runs: Vec<(
        autumn_harvest_sqlite::ExecutionId,
        autumn_harvest::WorkflowInfo,
        Vec<autumn_harvest::event::WorkflowEvent>,
    )>,
) -> usize {
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};

    let count = runs.len();
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
    count
}

#[tokio::test(flavor = "multi_thread")]
async fn the_vote_history_replays_clean() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["Yes", "no", "YES", "yes"]);
    let mut rt = runtime(&db, harness(model, &voting(4)));
    let input = SampleInput::vote(KIND, "Agree?", 4, AnswerKey::custom("lower")).concurrency(2);
    let exec = sqlite::start_sample(&mut rt, &input).unwrap();
    let report = sample_report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.tally, [count("yes", 3, 0), count("no", 1, 1)]);
    assert_eq!(
        replays_clean(runs_of(&rt, exec)).await,
        5,
        "the parent and 4 draws, in 2 waves"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_settled_vote_history_replays_clean() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["7", "7.0", "seven", "8"]);
    let mut rt = runtime(&db, harness(model, &voting(4)));
    let input = SampleInput::vote(KIND, "Count.", 4, AnswerKey::LastNumber).concurrency(1);
    let exec = sqlite::start_sample(&mut rt, &input).unwrap();
    let report = sample_report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stopped, SampleStop::Settled);
    assert_eq!(report.attempts, 3, "7 has 2 votes, 0 against, 1 left");
    assert_eq!(replays_clean(runs_of(&rt, exec)).await, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_vote_takes_the_concurrency_cap_of_the_policy_by_default() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"]);
    let mut rt = runtime(&db, harness(model, &voting(5)));
    let input = SampleInput::vote(KIND, "Pick.", 5, AnswerKey::Trimmed);
    let report = sample_report(drive(&mut rt, &input).await);
    assert_eq!(report.concurrency, 4, "the cap of the policy");
    assert_eq!(report.attempts, 4, "4 votes for a beat 0 with 1 left");
    assert_eq!(report.stopped, SampleStop::Settled);
}
