//! The `agent_sample` workflow on SQLite (issue #2443).
//!
//! Each draw is a child `agent_loop` run. The run scores each draw in a
//! recorded activity and selects one. The crash test is in
//! `sample_crash.rs`, and the Postgres test is in `sample_postgres.rs`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_literal_bound
)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    InMemorySamplingPolicyStore, SampleInput, SampleOutcome, SampleReport, SampleStop,
    SampleStrategy, SamplingPolicy, SamplingPolicyStore, ScorerId, ScorerRef, Scorers, Strategy,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, AgentStop, ChatMessage, ChatRequest,
    ChatResponse, ChatRole, ContentPart, FiniteF64, ScoreContext, ScoreInput, Scorer, StopReason,
    TokenUsage, ToolEffect, Verdict, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{Recorder, TryFnModel, answer, calls, recorded_tool, runtime};
use futures::FutureExt as _;
use serde_json::json;

const KIND: &str = "researcher";

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// A policy that allows both strategies of this issue, up to `max` draws.
fn sampling(max: u32) -> SamplingPolicy {
    SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN, Strategy::Rejection]),
        max_draws: max,
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

/// A scorer that gives each answer in `table` its score, and rejects each
/// other answer.
#[derive(Debug)]
struct Table(Vec<(&'static str, f64)>);

impl Scorer for Table {
    fn id(&self) -> &str {
        "table"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        let found = self
            .0
            .iter()
            .find(|(answer, _)| *answer == input.answer.trim());
        let verdict = match found {
            Some((_, score)) => Verdict::score(*score),
            None => Ok(Verdict::reject(format!("no score for {:?}", input.answer))),
        };
        Box::pin(std::future::ready(verdict))
    }
}

fn table() -> ScorerRef {
    ScorerRef::deterministic("table").unwrap()
}

/// A model that answers draw `n` with `answers[n]`, with 5 tokens.
fn by_draw(answers: &'static [&'static str]) -> Arc<TryFnModel> {
    TryFnModel::new(move |request: &ChatRequest| {
        let n = request.draw.map_or(0, |draw| draw.get() as usize);
        answer(answers[n % answers.len()], 5)
    })
}

fn harness(model: Arc<dyn AgentModel>, scorer: Table, policy: &SamplingPolicy) -> AgentHarness {
    AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::new(scorer))
        .sampling_policies(store_with(policy))
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

fn selected_text(report: &SampleReport) -> &str {
    &report.selected.as_ref().expect("a selected draw").text
}

fn threshold(value: f64) -> FiniteF64 {
    FiniteF64::new(value).unwrap()
}

// ── best-of-N ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn best_of_three_picks_the_answer_that_the_scorer_prefers() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a", "b", "c"]);
    let scorer = Table(vec![("a", 0.2), ("b", 0.9), ("c", 0.5)]);
    let mut rt = runtime(&db, harness(model.clone(), scorer, &sampling(3)));
    let input = SampleInput::new(
        KIND,
        "Pick a letter.",
        SampleStrategy::best_of_n(3),
        table(),
    );
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(selected_text(&report), "b");
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: autumn_harvest_agent::DrawId::new(1)
        }
    );
    assert_eq!(report.draws.len(), 3, "3 verdicts");
    let verdicts: Vec<Option<f64>> = report
        .draws
        .iter()
        .map(|draw| draw.verdict.as_score().map(FiniteF64::get))
        .collect();
    assert_eq!(verdicts, [Some(0.2), Some(0.9), Some(0.5)]);
    assert_eq!(report.attempts, 3);
    assert_eq!(report.stopped, SampleStop::Completed);
    assert_eq!(model.requests().len(), 3, "one model call per draw");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tie_goes_to_the_lower_draw_id() {
    let (_dir, db) = fresh_db();
    // Draws 1 and 2 tie on the top score. Draw 0 scores lower.
    let model = by_draw(&["a", "b", "b"]);
    let scorer = Table(vec![("a", 0.1), ("b", 0.7)]);
    let mut rt = runtime(&db, harness(model, scorer, &sampling(3)));
    let input = SampleInput::new(
        KIND,
        "Pick a letter.",
        SampleStrategy::best_of_n(3),
        table(),
    );
    let report = sample_report(drive(&mut rt, &input).await);
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: autumn_harvest_agent::DrawId::new(1)
        },
        "draw 1 and draw 2 tie, so the lower id wins"
    );
    assert!(report.draws[1].run_id.is_some());
    assert_eq!(report.selected.unwrap().run_id, report.draws[1].run_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn best_of_n_with_each_draw_rejected_selects_nothing() {
    let (_dir, db) = fresh_db();
    let mut rt = runtime(
        &db,
        harness(by_draw(&["x"]), Table(Vec::new()), &sampling(2)),
    );
    let input = SampleInput::new(
        KIND,
        "Pick a letter.",
        SampleStrategy::best_of_n(2),
        table(),
    );
    let report = sample_report(drive(&mut rt, &input).await);
    assert_eq!(report.outcome, SampleOutcome::NoAcceptedDraw);
    assert!(report.selected.is_none());
    assert_eq!(report.draws.len(), 2);
}

// ── rejection ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn rejection_stops_at_the_first_draw_that_meets_the_threshold() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["low", "high", "high"]);
    let scorer = Table(vec![("low", 0.2), ("high", 0.8)]);
    let mut rt = runtime(&db, harness(model.clone(), scorer, &sampling(4)));
    let strategy = SampleStrategy::rejection(4, threshold(0.8));
    let input = SampleInput::new(KIND, "Answer well.", strategy, table());
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.attempts, 2, "exactly 2 draws");
    assert_eq!(report.draws.len(), 2);
    assert_eq!(model.requests().len(), 2, "no third model call");
    assert_eq!(report.stopped, SampleStop::Accepted);
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: autumn_harvest_agent::DrawId::new(1)
        }
    );
    assert_eq!(selected_text(&report), "high");
}

#[tokio::test(flavor = "multi_thread")]
async fn rejection_with_each_draw_rejected_ends_with_no_accepted_draw() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["low"]);
    // A score under the threshold and a reject both fail the check.
    let scorer = Table(vec![("low", 0.2)]);
    let mut rt = runtime(&db, harness(model.clone(), scorer, &sampling(3)));
    let strategy = SampleStrategy::rejection(3, threshold(0.5));
    let input = SampleInput::new(KIND, "Answer well.", strategy, table());
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.outcome, SampleOutcome::NoAcceptedDraw);
    assert_eq!(report.attempts, 3, "max_attempts draws");
    assert_eq!(report.stopped, SampleStop::Completed);
    assert!(report.selected.is_none());
    assert_eq!(model.requests().len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn rejection_over_max_draws_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["low"]);
    let mut rt = runtime(&db, harness(model.clone(), Table(Vec::new()), &sampling(2)));
    let strategy = SampleStrategy::rejection(3, threshold(0.5));
    let input = SampleInput::new(KIND, "Answer well.", strategy, table());
    match drive(&mut rt, &input).await {
        RunState::Failed(error) => assert!(error.contains("too_many_draws"), "{error}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(model.requests().len(), 0);
}

// ── the budget tree ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_token_budget_for_two_of_four_draws_stops_with_budget_exceeded() {
    let (_dir, db) = fresh_db();
    // Each draw spends 5 tokens. The tree has 10.
    let model = by_draw(&["a", "b", "c", "d"]);
    let scorer = Table(vec![("a", 0.4), ("b", 0.6), ("c", 0.9), ("d", 0.9)]);
    let mut rt = runtime(&db, harness(model.clone(), scorer, &sampling(4)));
    let input = SampleInput::new(
        KIND,
        "Pick a letter.",
        SampleStrategy::best_of_n(4),
        table(),
    )
    .max_total_tokens(10);
    let exec = sqlite::start_sample(&mut rt, &input).unwrap();
    let report = sample_report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stopped, SampleStop::BudgetExceeded);
    assert_eq!(report.attempts, 2);
    assert_eq!(report.draws.len(), 2);
    assert_eq!(selected_text(&report), "b", "the run selects among 2 draws");
    let model_calls = ledger_rows(&rt)
        .iter()
        .filter(|row| row.activity_name == "agent_model_turn")
        .count();
    assert_eq!(model_calls, 2, "the ledger shows no third model call");
    assert_eq!(model.requests().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_profile_cap_is_the_budget_of_the_whole_tree() {
    let (_dir, db) = fresh_db();
    let mut profile = AgentProfile::new(KIND);
    profile.max_total_tokens = Some(5);
    let harness = AgentHarness::new(by_draw(&["a", "b"]))
        .agent(profile)
        .scorer(Arc::new(Table(vec![("a", 0.5), ("b", 0.6)])))
        .sampling_policies(store_with(&sampling(2)));
    let mut rt = runtime(&db, harness);
    let input = SampleInput::new(
        KIND,
        "Pick a letter.",
        SampleStrategy::best_of_n(2),
        table(),
    );
    let report = sample_report(drive(&mut rt, &input).await);
    assert_eq!(report.stopped, SampleStop::BudgetExceeded);
    assert_eq!(report.attempts, 1, "5 tokens pay for one draw");
    assert_eq!(selected_text(&report), "a");
}

// ── draw effects ────────────────────────────────────────────────────────────

/// A model that calls the tool `publish` once, then answers.
fn calls_publish() -> Arc<TryFnModel> {
    TryFnModel::new(|request: &ChatRequest| {
        let answered = request.messages.iter().any(|message| {
            message
                .content
                .iter()
                .any(|part| matches!(part, ContentPart::ToolResult { .. }))
        });
        if answered {
            answer("done", 5)
        } else {
            calls(&[("c1", "publish", json!({"post": "hello"}))], 5)
        }
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_default_draw_gets_a_refusal_for_a_write_tool() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let harness = harness(calls_publish(), Table(vec![("done", 1.0)]), &sampling(1))
        .tool(recorded_tool("publish", ToolEffect::Write, &recorder));
    let mut rt = runtime(&db, harness);
    let input = SampleInput::new(KIND, "Post it.", SampleStrategy::best_of_n(1), table());
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(recorder.runs().len(), 0, "the tool body never ran");
    let results = common::tool_results(report.selected.as_ref().unwrap());
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(
        results[0].contains("denied by policy: this run may not run a tool of effect write"),
        "the draw reads the effect refusal: {results:?}"
    );
}

// ── usage and cost ──────────────────────────────────────────────────────────

/// A model with an id and a price of 3 micro-dollars per token.
#[derive(Debug)]
struct Priced {
    id: &'static str,
    inner: Arc<TryFnModel>,
}

impl AgentModel for Priced {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.inner.chat(request)
    }

    fn model_id(&self) -> Option<&str> {
        Some(self.id)
    }

    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        Some(u64::from(usage.total()) * 3)
    }
}

/// A judge scorer: one call to the judge model, then a score of the length
/// of the answer.
#[derive(Debug)]
struct Judge;

impl Scorer for Judge {
    fn id(&self) -> &str {
        "judge"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        Box::pin(async move {
            let request = ChatRequest {
                messages: vec![ChatMessage::text(ChatRole::User, input.answer.clone())],
                tools: Vec::new(),
                max_tokens: Some(8),
                temperature: None,
                draw: None,
            };
            cx.chat(Some("judge"), &request).await?;
            #[allow(clippy::cast_precision_loss)]
            Verdict::score(input.answer.len() as f64)
        })
    }
}

fn judged_run(judge_model: &str) -> (AgentHarness, SampleInput) {
    let main = Arc::new(Priced {
        id: "main",
        inner: TryFnModel::new(|request: &ChatRequest| {
            // Draw n spends 5 + n tokens, so each draw differs.
            let n = request.draw.map_or(0, autumn_harvest_agent::DrawId::get);
            answer(&"x".repeat(n as usize + 1), 5 + n)
        }),
    });
    let judge = Arc::new(Priced {
        id: "judge",
        inner: TryFnModel::new(|_| {
            Ok(ChatResponse {
                content: vec![ContentPart::Text("ok".to_owned())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(7, 2),
            })
        }),
    });
    let policy = SamplingPolicy {
        scorers: Scorers::Only(BTreeSet::from([ScorerId::new("judge").unwrap()])),
        judge_models: BTreeSet::from(["judge".to_owned(), "other".to_owned()]),
        ..sampling(3)
    };
    let harness = AgentHarness::new(main)
        .model_named("judge", judge)
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::new(Judge))
        .sampling_policies(store_with(&policy));
    let input = SampleInput::new(
        KIND,
        "Write some x.",
        SampleStrategy::best_of_n(3),
        ScorerRef::judge("judge", judge_model).unwrap(),
    );
    (harness, input)
}

fn ledger_rows(rt: &SqliteRuntime) -> Vec<autumn_harvest_sqlite::LlmLedgerRow> {
    use autumn_harvest::chrono::{DateTime, Utc};
    rt.llm_ledger_between(DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_per_draw_usage_sums_to_the_ledger_of_the_run_tree() {
    let (_dir, db) = fresh_db();
    let (harness, input) = judged_run("judge");
    let mut rt = runtime(&db, harness);
    let report = sample_report(drive(&mut rt, &input).await);
    assert_eq!(report.draws.len(), 3);
    assert_eq!(selected_text(&report), "xxx", "the longest answer");

    // The fresh database holds only this run tree.
    let rows = ledger_rows(&rt);
    assert_eq!(rows.len(), 6, "3 model turns and 3 judge calls");
    let ledger_tokens: u64 = rows
        .iter()
        .map(|row| row.input_tokens + row.output_tokens)
        .sum();
    let ledger_cost: u64 = rows.iter().filter_map(|row| row.cost_usd_micros).sum();
    let draw_tokens: u64 = report
        .draws
        .iter()
        .map(|draw| u64::from(draw.usage.total()))
        .sum();
    let draw_cost: u64 = report.draws.iter().map(|draw| draw.cost_usd_micros).sum();
    assert_eq!(draw_tokens, ledger_tokens);
    assert_eq!(draw_cost, ledger_cost);
    assert_eq!(u64::from(report.usage.total()), ledger_tokens);
    assert_eq!(report.cost_usd_micros, ledger_cost);
    // Draw 0: 5 model tokens and 9 judge tokens, at 3 micro-dollars each.
    assert_eq!(report.draws[0].usage.total(), 14);
    assert_eq!(report.draws[0].cost_usd_micros, 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_judge_call_to_a_model_that_the_run_did_not_admit_fails_the_score() {
    let (_dir, db) = fresh_db();
    // The run admits the judge model `other`, but the scorer calls `judge`.
    let (harness, input) = judged_run("other");
    let mut rt = runtime(&db, harness);
    match drive(&mut rt, &input).await {
        RunState::Failed(error) => panic!("a failed score must not fail the run: {error}"),
        RunState::Completed(value) => {
            let report: SampleReport = serde_json::from_value(value).unwrap();
            assert_eq!(report.stopped, SampleStop::ScoreFailed);
            assert_eq!(report.attempts, 1, "the run started no more draws");
            assert_eq!(report.outcome, SampleOutcome::NoAcceptedDraw);
            let Verdict::Reject(reason) = &report.draws[0].verdict else {
                panic!("expected a reject, got {:?}", report.draws[0].verdict);
            };
            assert!(reason.contains("judge model"), "{reason}");
            assert_eq!(report.draws[0].usage.total(), 5, "the draw keeps its usage");
        }
        other => panic!("expected an end, got {other:?}"),
    }
    let judge_calls = ledger_rows(&rt)
        .iter()
        .filter(|row| row.model == "judge")
        .count();
    assert_eq!(judge_calls, 0, "the judge model was never called");
}

// ── the input ───────────────────────────────────────────────────────────────

#[test]
fn a_sample_input_with_a_policy_field_fails_to_decode() {
    let input = SampleInput::new(KIND, "x", SampleStrategy::best_of_n(3), table());
    let mut value = serde_json::to_value(&input).unwrap();
    assert_eq!(value["strategy"], json!({"best_of_n": {"n": 3}}));
    value["max_draws"] = json!(99);
    let err = serde_json::from_value::<SampleInput>(value).unwrap_err();
    assert!(err.to_string().contains("max_draws"), "{err}");
}

#[test]
fn a_threshold_that_is_not_finite_fails_to_decode() {
    let value = json!({"rejection": {"max_attempts": 2, "threshold": "NaN"}});
    assert!(serde_json::from_value::<SampleStrategy>(value).is_err());
    let value = json!({"rejection": {"max_attempts": 2, "threshold": 0.5}});
    let strategy: SampleStrategy = serde_json::from_value(value).unwrap();
    assert_eq!(strategy, SampleStrategy::rejection(2, threshold(0.5)));
    assert_eq!(strategy.policy_strategy(), Strategy::Rejection);
    assert_eq!(strategy.draws(), 2);
}

// ── review follow-ups ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn rejection_with_a_wide_policy_still_draws_one_at_a_time() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["low", "high", "high", "high"]);
    let scorer = Table(vec![("low", 0.2), ("high", 0.8)]);
    let policy = SamplingPolicy {
        max_concurrent_draws: 4,
        ..sampling(4)
    };
    let mut rt = runtime(&db, harness(model.clone(), scorer, &policy));
    let strategy = SampleStrategy::rejection(4, threshold(0.8));
    let input = SampleInput::new(KIND, "Answer well.", strategy, table());
    let report = sample_report(drive(&mut rt, &input).await);
    assert_eq!(report.attempts, 2, "rejection runs one draw at a time");
    assert_eq!(report.concurrency, 1);
    assert_eq!(model.requests().len(), 2);
    assert_eq!(report.stopped, SampleStop::Accepted);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wave_splits_the_rest_by_sibling_cap() {
    let (_dir, db) = fresh_db();
    // Each draw spends 6 tokens. The tree has 11, so draw 0 gets a cap of 6
    // and draw 1 a cap of 5.
    let model = TryFnModel::new(|request: &ChatRequest| {
        let n = request.draw.map_or(0, |draw| draw.get() as usize);
        answer(["a", "b", "c", "d"][n], 6)
    });
    let scorer = Table(vec![("a", 0.1), ("b", 0.2), ("c", 0.9), ("d", 0.9)]);
    let policy = SamplingPolicy {
        max_concurrent_draws: 2,
        ..sampling(4)
    };
    let mut rt = runtime(&db, harness(model.clone(), scorer, &policy));
    let input = SampleInput::new(
        KIND,
        "Pick a letter.",
        SampleStrategy::best_of_n(4),
        table(),
    )
    .max_total_tokens(11);
    let report = sample_report(drive(&mut rt, &input).await);
    assert_eq!(report.concurrency, 2);
    assert_eq!(report.attempts, 2, "one wave of 2 spends the budget");
    assert_eq!(report.draws[0].stop, AgentStop::Completed);
    assert_eq!(
        report.draws[1].stop,
        AgentStop::TokensExhausted,
        "draw 1 passed its share of 5"
    );
    assert_eq!(report.stopped, SampleStop::BudgetExceeded);
    assert_eq!(model.requests().len(), 2);
}

/// A scorer that counts its calls.
#[derive(Debug)]
struct Counted {
    table: Table,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl Scorer for Counted {
    fn id(&self) -> &str {
        "table"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.table.score(input, cx)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_draw_is_rejected_and_not_scored() {
    let (_dir, db) = fresh_db();
    let model = TryFnModel::new(|request: &ChatRequest| {
        match request.draw.map_or(0, autumn_harvest_agent::DrawId::get) {
            0 => answer("a", 5),
            1 => Err(AgentError::new(
                autumn_harvest_agent::ErrorKind::Provider,
                "the provider refused the request",
            )),
            _ => answer("c", 5),
        }
    });
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let scorer = Counted {
        table: Table(vec![("a", 0.3), ("c", 0.7)]),
        calls: Arc::clone(&calls),
    };
    let harness = AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::new(scorer))
        .sampling_policies(store_with(&sampling(3)));
    let mut rt = runtime(&db, harness);
    let input = SampleInput::new(
        KIND,
        "Pick a letter.",
        SampleStrategy::best_of_n(3),
        table(),
    );
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(report.draws[1].stop, AgentStop::Failed);
    assert!(report.draws[1].verdict.is_reject());
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the failed draw was never scored"
    );
    assert_eq!(selected_text(&report), "c", "the run selects among 0 and 2");
    assert_eq!(report.stopped, SampleStop::Completed);
}

#[tokio::test(flavor = "multi_thread")]
async fn judge_spend_counts_against_the_tree_budget() {
    let (_dir, db) = fresh_db();
    let (harness, input) = judged_run("judge");
    let mut rt = runtime(&db, harness);
    // Draw 0 spends 5 tokens and its judge 9, past the budget of 10.
    let report = sample_report(drive(&mut rt, &input.max_total_tokens(10)).await);
    assert_eq!(report.attempts, 1);
    assert_eq!(report.stopped, SampleStop::BudgetExceeded);
    assert_eq!(
        report.usage.total(),
        14,
        "the overshoot is one draw and its judge"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sample_history_replays_clean() {
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
    use autumn_harvest_agent::sampling::agent_sample_info;
    use autumn_harvest_agent::workflow::agent_loop_info;

    let (_dir, db) = fresh_db();
    let model = by_draw(&["a", "b", "c"]);
    let scorer = Table(vec![("a", 0.2), ("b", 0.9), ("c", 0.5)]);
    let policy = SamplingPolicy {
        max_concurrent_draws: 2,
        ..sampling(3)
    };
    let mut rt = runtime(&db, harness(model, scorer, &policy));
    let input = SampleInput::new(
        KIND,
        "Pick a letter.",
        SampleStrategy::best_of_n(3),
        table(),
    );
    let exec = sqlite::start_sample(&mut rt, &input).unwrap();
    let report = sample_report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(selected_text(&report), "b");

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
    assert_eq!(runs.len(), 4, "the parent and 3 draws, in 2 waves");
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
