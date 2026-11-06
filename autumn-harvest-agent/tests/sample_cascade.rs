//! The model cascade of `agent_sample` on SQLite.
//!
//! A cascade asks a cheap model first and scores its answer. It asks a
//! stronger model only when the score is below the tier threshold. The
//! crash test is in `sample_cascade_crash.rs`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_literal_bound
)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    DrawsInput, InMemorySamplingPolicyStore, Refusal, SampleInput, SampleOutcome, SampleReport,
    SampleStop, SampleStrategy, SamplingPolicy, SamplingPolicyStore, ScorerRef, SkippedTier,
    Strategy, Tier,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, AgentStop, ChatRequest, ChatResponse,
    DrawId, ErrorKind, FiniteF64, Mandate, ScoreContext, ScoreInput, Scorer, TokenUsage, Verdict,
    sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{answer, runtime};
use futures::FutureExt as _;

const KIND: &str = "researcher";

/// The cost of one token, in millionths of a US dollar, per model.
const CHEAP_PRICE: u64 = 1;
const STRONG_PRICE: u64 = 20;

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// A priced model with a model id. It answers each call with `text` and
/// counts its calls. With `fail` set, each call fails.
#[derive(Debug)]
struct Tiered {
    id: &'static str,
    text: &'static str,
    tokens: u32,
    price: u64,
    fail: bool,
    calls: AtomicUsize,
}

impl Tiered {
    fn new(id: &'static str, text: &'static str, tokens: u32, price: u64) -> Arc<Self> {
        Arc::new(Self {
            id,
            text,
            tokens,
            price,
            fail: false,
            calls: AtomicUsize::new(0),
        })
    }

    fn failing(id: &'static str) -> Arc<Self> {
        Arc::new(Self {
            id,
            text: "",
            tokens: 0,
            price: 0,
            fail: true,
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl AgentModel for Tiered {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let reply = if self.fail {
            Err(AgentError::new(ErrorKind::Config, "the model is down"))
        } else {
            answer(self.text, self.tokens)
        };
        Box::pin(std::future::ready(reply))
    }

    fn model_id(&self) -> Option<&str> {
        Some(self.id)
    }

    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        Some(u64::from(usage.total()) * self.price)
    }
}

/// A model that the run must never call.
#[derive(Debug)]
struct Unused;

impl AgentModel for Unused {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        panic!("a cascade asks only the models of its tiers")
    }
}

/// A scorer that gives each answer in the table its score, and rejects
/// each other answer.
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

fn tier(model: &str, threshold: f64) -> Tier {
    Tier::new(model, FiniteF64::new(threshold).unwrap())
}

/// A policy that allows a cascade over `models`.
fn cascade_policy(models: &[&str]) -> SamplingPolicy {
    SamplingPolicy {
        strategies: BTreeSet::from([Strategy::Cascade]),
        max_draws: 4,
        cascade_models: models.iter().map(|model| (*model).to_owned()).collect(),
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

/// The models of a two-tier run.
struct Models {
    cheap: Arc<Tiered>,
    strong: Arc<Tiered>,
}

impl Models {
    fn new(cheap: &'static str, strong: &'static str) -> Self {
        Self {
            cheap: Tiered::new("cheap-v1", cheap, 5, CHEAP_PRICE),
            strong: Tiered::new("strong-v1", strong, 7, STRONG_PRICE),
        }
    }

    fn harness(
        &self,
        profile: AgentProfile,
        scorer: Table,
        policy: &SamplingPolicy,
    ) -> AgentHarness {
        AgentHarness::new(Arc::new(Unused))
            .model_named("cheap", self.cheap.clone())
            .model_named("strong", self.strong.clone())
            .agent(profile)
            .scorer(Arc::new(scorer))
            .sampling_policies(store_with(policy))
    }
}

fn two_tiers(cheap: f64, strong: f64) -> SampleInput {
    SampleInput::new(
        KIND,
        "Answer well.",
        SampleStrategy::cascade([tier("cheap", cheap), tier("strong", strong)]),
        table(),
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

fn selected_text(report: &SampleReport) -> &str {
    &report.selected.as_ref().expect("a selected draw").text
}

fn ledger_rows(rt: &SqliteRuntime) -> Vec<autumn_harvest_sqlite::LlmLedgerRow> {
    use autumn_harvest::chrono::{DateTime, Utc};
    rt.llm_ledger_between(DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC)
        .unwrap()
}

// ── the acceptance tests ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn the_cheap_tier_accepts_and_the_strong_model_is_not_called() {
    let (_dir, db) = fresh_db();
    let models = Models::new("good", "great");
    let scorer = Table(vec![("good", 0.9), ("great", 0.95)]);
    let policy = cascade_policy(&["cheap", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), scorer, &policy),
    );
    let report = sample_report(drive(&mut rt, &two_tiers(0.5, 0.8)).await);

    assert_eq!(models.strong.calls(), 0, "the strong model is never called");
    assert_eq!(models.cheap.calls(), 1);
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: DrawId::new(0)
        }
    );
    assert_eq!(report.stopped, SampleStop::Accepted);
    assert_eq!(report.attempts, 1);
    assert_eq!(selected_text(&report), "good");
    assert_eq!(report.draws[0].model.as_deref(), Some("cheap"));
    assert_eq!(report.skipped, []);
    assert_eq!(report.concurrency, 1);
    let rows = ledger_rows(&rt);
    let ids: Vec<&str> = rows.iter().map(|row| row.model.as_str()).collect();
    assert_eq!(ids, ["cheap-v1"], "the ledger holds only the cheap row");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_low_cheap_score_escalates_to_the_strong_tier() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let scorer = Table(vec![("meh", 0.3), ("great", 0.95)]);
    let policy = cascade_policy(&["cheap", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), scorer, &policy),
    );
    let report = sample_report(drive(&mut rt, &two_tiers(0.5, 0.8)).await);

    assert_eq!(models.cheap.calls(), 1, "the cheap model is called once");
    assert_eq!(models.strong.calls(), 1, "the strong model is called once");
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: DrawId::new(1)
        }
    );
    assert_eq!(selected_text(&report), "great");
    assert_eq!(report.stopped, SampleStop::Accepted);
    let models: Vec<Option<&str>> = report
        .draws
        .iter()
        .map(|draw| draw.model.as_deref())
        .collect();
    assert_eq!(models, [Some("cheap"), Some("strong")]);
}

#[tokio::test(flavor = "multi_thread")]
async fn with_no_tier_accepting_the_best_answer_is_escalated() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "ok");
    let scorer = Table(vec![("meh", 0.3), ("ok", 0.6)]);
    let policy = cascade_policy(&["cheap", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), scorer, &policy),
    );
    let report = sample_report(drive(&mut rt, &two_tiers(0.5, 0.8)).await);

    assert_eq!(
        report.outcome,
        SampleOutcome::Escalated {
            draw: DrawId::new(1)
        }
    );
    assert_eq!(selected_text(&report), "ok", "the higher score");
    assert_eq!(report.stopped, SampleStop::Completed);
    assert_eq!(report.attempts, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_escalated_answer_is_the_best_score_and_not_the_last_tier() {
    let (_dir, db) = fresh_db();
    let models = Models::new("close", "poor");
    let scorer = Table(vec![("close", 0.45), ("poor", 0.2)]);
    let policy = cascade_policy(&["cheap", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), scorer, &policy),
    );
    let report = sample_report(drive(&mut rt, &two_tiers(0.5, 0.9)).await);

    assert_eq!(
        report.outcome,
        SampleOutcome::Escalated {
            draw: DrawId::new(0)
        }
    );
    assert_eq!(selected_text(&report), "close");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tier_outside_the_mandate_is_skipped_and_named() {
    let (_dir, db) = fresh_db();
    let models = Models::new("good", "great");
    let scorer = Table(vec![("good", 0.9), ("great", 0.95)]);
    let policy = cascade_policy(&["cheap", "strong"]);
    let profile = AgentProfile::new(KIND).mandate(Mandate::new(KIND).models(["strong"]));
    let mut rt = runtime(&db, models.harness(profile, scorer, &policy));
    let report = sample_report(drive(&mut rt, &two_tiers(0.5, 0.8)).await);

    assert_eq!(
        models.cheap.calls(),
        0,
        "the mandate forbids the cheap tier"
    );
    assert_eq!(models.strong.calls(), 1, "the next tier runs");
    let skipped: Vec<(u32, &str, &Refusal)> = report
        .skipped
        .iter()
        .map(|skip: &SkippedTier| (skip.tier, skip.model.as_str(), &skip.refusal))
        .collect();
    assert_eq!(skipped, [(0, "cheap", &Refusal::NotPermitted)]);
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: DrawId::new(1)
        }
    );
    assert_eq!(report.attempts, 1);
    assert_eq!(
        report.draws[0].draw,
        DrawId::new(1),
        "the draw id is the tier"
    );
    let wire = serde_json::to_value(&report.skipped).unwrap();
    assert_eq!(
        wire,
        serde_json::json!([{"tier": 0, "model": "cheap", "refusal": {"refusal": "not_permitted"}}])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ledger_has_one_row_per_tier_with_its_model_id() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let scorer = Table(vec![("meh", 0.3), ("great", 0.95)]);
    let policy = cascade_policy(&["cheap", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), scorer, &policy),
    );
    let report = sample_report(drive(&mut rt, &two_tiers(0.5, 0.8)).await);

    // The fresh database holds only this run tree.
    let rows = ledger_rows(&rt);
    let mut ids: Vec<&str> = rows.iter().map(|row| row.model.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, ["cheap-v1", "strong-v1"], "one row per tier");
    assert!(
        rows.iter()
            .all(|row| row.activity_name == "agent_model_turn")
    );
    for (draw, id, price) in [
        (&report.draws[0], "cheap-v1", CHEAP_PRICE),
        (&report.draws[1], "strong-v1", STRONG_PRICE),
    ] {
        let row = rows.iter().find(|row| row.model == id).unwrap();
        assert_eq!(
            row.cost_usd_micros,
            Some(u64::from(draw.usage.total()) * price)
        );
        assert_eq!(row.cost_usd_micros, Some(draw.cost_usd_micros));
    }
    let total: u64 = rows.iter().filter_map(|row| row.cost_usd_micros).sum();
    assert_eq!(report.cost_usd_micros, total);
    // The rows of the guide: 5 tokens at 1, then 7 tokens at 20.
    let costs: Vec<(u64, Option<u64>)> = ["cheap-v1", "strong-v1"]
        .iter()
        .map(|id| {
            let row = rows.iter().find(|row| row.model == *id).unwrap();
            (row.input_tokens + row.output_tokens, row.cost_usd_micros)
        })
        .collect();
    assert_eq!(costs, [(5, Some(5)), (7, Some(140))]);
    assert_eq!(total, 145);
}

// ── admission ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_tier_model_outside_the_policy_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let scorer = Table(vec![("meh", 0.3), ("great", 0.95)]);
    let policy = cascade_policy(&["cheap"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), scorer, &policy),
    );
    match drive(&mut rt, &two_tiers(0.5, 0.8)).await {
        RunState::Failed(error) => assert!(
            error.starts_with("sampling refused (tier_model_not_allowed)"),
            "{error}"
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(models.cheap.calls(), 0, "no model call before the refusal");
    assert_eq!(models.strong.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_default_policy_refuses_a_cascade() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::Cascade]),
        max_draws: 4,
        ..SamplingPolicy::default()
    };
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), Table(Vec::new()), &policy),
    );
    match drive(&mut rt, &two_tiers(0.5, 0.8)).await {
        RunState::Failed(error) => assert!(error.contains("tier_model_not_allowed"), "{error}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(models.cheap.calls() + models.strong.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cascade_that_asks_for_two_concurrent_tiers_is_refused() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let policy = SamplingPolicy {
        max_concurrent_draws: 4,
        ..cascade_policy(&["cheap", "strong"])
    };
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), Table(Vec::new()), &policy),
    );
    let input = two_tiers(0.5, 0.8).concurrency(2);
    match drive(&mut rt, &input).await {
        RunState::Failed(error) => {
            assert!(error.contains("too_many_concurrent_draws"), "{error}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(models.cheap.calls() + models.strong.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn more_tiers_than_max_draws_are_refused() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let policy = SamplingPolicy {
        max_draws: 1,
        ..cascade_policy(&["cheap", "strong"])
    };
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), Table(Vec::new()), &policy),
    );
    match drive(&mut rt, &two_tiers(0.5, 0.8)).await {
        RunState::Failed(error) => assert!(error.contains("too_many_draws"), "{error}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

// ── edges ───────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_tier_is_rejected_and_the_next_tier_runs() {
    let (_dir, db) = fresh_db();
    let models = Models {
        cheap: Tiered::failing("cheap-v1"),
        strong: Tiered::new("strong-v1", "great", 7, STRONG_PRICE),
    };
    let scorer = Table(vec![("great", 0.95)]);
    let policy = cascade_policy(&["cheap", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), scorer, &policy),
    );
    let report = sample_report(drive(&mut rt, &two_tiers(0.5, 0.8)).await);

    assert_eq!(report.draws[0].stop, AgentStop::Failed);
    assert!(report.draws[0].verdict.is_reject());
    assert_eq!(models.strong.calls(), 1);
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: DrawId::new(1)
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spent_budget_stops_the_cascade_before_the_strong_tier() {
    let (_dir, db) = fresh_db();
    // The cheap tier spends 5 tokens, the whole budget.
    let models = Models::new("meh", "great");
    let scorer = Table(vec![("meh", 0.3), ("great", 0.95)]);
    let policy = cascade_policy(&["cheap", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), scorer, &policy),
    );
    let input = two_tiers(0.5, 0.8).max_total_tokens(5);
    let report = sample_report(drive(&mut rt, &input).await);

    assert_eq!(
        models.strong.calls(),
        0,
        "no token is left for the strong tier"
    );
    assert_eq!(report.stopped, SampleStop::BudgetExceeded);
    assert_eq!(
        report.outcome,
        SampleOutcome::Escalated {
            draw: DrawId::new(0)
        },
        "the run keeps the best answer that it paid for"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_budget_stop_still_names_each_forbidden_tier() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let scorer = Table(vec![("meh", 0.3), ("great", 0.95)]);
    let policy = cascade_policy(&["cheap", "mid", "strong"]);
    let profile = AgentProfile::new(KIND).mandate(Mandate::new(KIND).models(["cheap", "strong"]));
    let mut rt = runtime(&db, models.harness(profile, scorer, &policy));
    // The cheap tier spends the whole budget. The mid tier is forbidden.
    let input = SampleInput::new(
        KIND,
        "Answer well.",
        SampleStrategy::cascade([tier("cheap", 0.5), tier("mid", 0.7), tier("strong", 0.8)]),
        table(),
    )
    .max_total_tokens(5);
    let report = sample_report(drive(&mut rt, &input).await);
    assert_eq!(report.stopped, SampleStop::BudgetExceeded);
    assert_eq!(models.strong.calls(), 0);
    let tiers: Vec<(u32, &str)> = report
        .skipped
        .iter()
        .map(|skip| (skip.tier, skip.model.as_str()))
        .collect();
    assert_eq!(
        tiers,
        [(1, "mid")],
        "the reason of the stop does not matter"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_strategy_check_comes_before_the_cascade_concurrency_check() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        max_concurrent_draws: 4,
        ..cascade_policy(&["cheap", "strong"])
    };
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), Table(Vec::new()), &policy),
    );
    match drive(&mut rt, &two_tiers(0.5, 0.8).concurrency(2)).await {
        RunState::Failed(error) => assert!(error.contains("strategy_not_allowed"), "{error}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn each_tier_rejected_selects_nothing() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "ok");
    let policy = cascade_policy(&["cheap", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), Table(Vec::new()), &policy),
    );
    let report = sample_report(drive(&mut rt, &two_tiers(0.5, 0.8)).await);
    assert_eq!(report.outcome, SampleOutcome::NoAcceptedDraw);
    assert!(report.selected.is_none());
    assert_eq!(report.attempts, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cascade_with_each_tier_forbidden_runs_no_model() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "ok");
    let policy = cascade_policy(&["cheap", "strong"]);
    let profile = AgentProfile::new(KIND).mandate(Mandate::new(KIND).models(["other"]));
    let mut rt = runtime(&db, models.harness(profile, Table(Vec::new()), &policy));
    let report = sample_report(drive(&mut rt, &two_tiers(0.5, 0.8)).await);
    assert_eq!(models.cheap.calls() + models.strong.calls(), 0);
    assert_eq!(report.outcome, SampleOutcome::NoAcceptedDraw);
    assert_eq!(report.stopped, SampleStop::Completed);
    let tiers: Vec<u32> = report.skipped.iter().map(|skip| skip.tier).collect();
    assert_eq!(tiers, [0, 1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tier_model_that_the_harness_lacks_fails_and_the_next_tier_runs() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let scorer = Table(vec![("great", 0.95)]);
    let policy = cascade_policy(&["absent", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), scorer, &policy),
    );
    let input = SampleInput::new(
        KIND,
        "Answer well.",
        SampleStrategy::cascade([tier("absent", 0.5), tier("strong", 0.8)]),
        table(),
    );
    let report = sample_report(drive(&mut rt, &input).await);
    assert_eq!(report.draws[0].stop, AgentStop::Failed);
    assert_eq!(report.draws[0].model.as_deref(), Some("absent"));
    assert_eq!(models.cheap.calls(), 0, "no fall back to another model");
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: DrawId::new(1)
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cascade_with_no_tier_is_refused() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let policy = cascade_policy(&["cheap"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), Table(Vec::new()), &policy),
    );
    let input = SampleInput::new(KIND, "x", SampleStrategy::cascade([]), table());
    match drive(&mut rt, &input).await {
        RunState::Failed(error) => assert!(error.contains("no_draws"), "{error}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_draws_refuses_a_cascade() {
    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let policy = cascade_policy(&["cheap", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), Table(Vec::new()), &policy),
    );
    let input = DrawsInput::new(KIND, "x", Strategy::Cascade, 2);
    let exec = sqlite::start_draws(&mut rt, &input).unwrap();
    match rt.run_until_blocked(exec).await.unwrap() {
        RunState::Failed(error) => {
            assert!(
                error.starts_with("sampling refused (sample_only)"),
                "{error}"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(models.cheap.calls() + models.strong.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_best_of_n_report_records_no_draw_model() {
    let (_dir, db) = fresh_db();
    let models = Models::new("good", "great");
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        ..cascade_policy(&["cheap"])
    };
    let profile = AgentProfile::new(KIND).model("cheap");
    let mut rt = runtime(
        &db,
        models.harness(profile, Table(vec![("good", 0.9)]), &policy),
    );
    let input = SampleInput::new(KIND, "x", SampleStrategy::best_of_n(1), table());
    let report = sample_report(drive(&mut rt, &input).await);
    assert_eq!(models.cheap.calls(), 1, "the profile model answers");
    assert_eq!(report.draws[0].model, None);
    let wire = serde_json::to_value(&report).unwrap();
    assert!(wire["draws"][0].get("model").is_none(), "the bytes stay");
    assert!(wire.get("skipped").is_none());
}

#[test]
fn the_cascade_wire_form() {
    let strategy = SampleStrategy::cascade([tier("cheap", 0.5), tier("strong", 0.8)]);
    let value = serde_json::to_value(&strategy).unwrap();
    assert_eq!(
        value,
        serde_json::json!({"cascade": {"tiers": [
            {"model": "cheap", "threshold": 0.5},
            {"model": "strong", "threshold": 0.8},
        ]}})
    );
    assert_eq!(
        serde_json::from_value::<SampleStrategy>(value).unwrap(),
        strategy
    );
    assert_eq!(strategy.policy_strategy(), Some(Strategy::Cascade));
    assert_eq!(strategy.draws(), 2);
    let bad = serde_json::json!({"cascade": {"tiers": [{"model": "m", "threshold": "NaN"}]}});
    assert!(serde_json::from_value::<SampleStrategy>(bad).is_err());
    let extra = serde_json::json!({"cascade": {"tiers": [
        {"model": "m", "threshold": 0.5, "judge_models": ["x"]}
    ]}});
    assert!(serde_json::from_value::<SampleStrategy>(extra).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cascade_history_replays_clean() {
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
    use autumn_harvest_agent::sampling::agent_sample_info;
    use autumn_harvest_agent::workflow::agent_loop_info;

    let (_dir, db) = fresh_db();
    let models = Models::new("meh", "great");
    let scorer = Table(vec![("meh", 0.3), ("great", 0.95)]);
    let policy = cascade_policy(&["cheap", "strong"]);
    let mut rt = runtime(
        &db,
        models.harness(AgentProfile::new(KIND), scorer, &policy),
    );
    let exec = sqlite::start_sample(&mut rt, &two_tiers(0.5, 0.8)).unwrap();
    let report = sample_report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(selected_text(&report), "great");

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
    assert_eq!(runs.len(), 3, "the parent and one draw per tier");
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
