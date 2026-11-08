//! One task under four inference strategies, with no API key and no
//! network.
//!
//! The example runs `agent_sample` four times on one SQLite file: a single
//! draw, best-of-3, a vote and a two-tier cascade. A scripted model stands
//! in for a provider, and `ExactMatch` scores each draw against the
//! expected answer. Each run reports the selected answer, each draw and the
//! cost.
//!
//! ```sh
//! cargo run -p agent-strategies
//! ```

use std::collections::BTreeSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest_agent::sampling::{
    AnswerKey, InMemorySamplingPolicyStore, SampleInput, SampleOutcome, SampleReport,
    SampleStrategy, SamplingPolicy, SamplingPolicyStore, ScorerRef, Strategy, Tier,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, ChatRequest, ChatResponse, ContentPart,
    DrawId, ExactMatch, FiniteF64, StopReason, TokenUsage, Verdict, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};

/// The agent kind of each run.
pub const KIND: &str = "solver";

/// The task of each run.
pub const TASK: &str = "What is 17 times 3?";

/// The answer that `ExactMatch` expects.
pub const EXPECTED: &str = "51";

/// The answers of the cheap model, by draw id. It misses on draws 0 and 4.
const CHEAP_ANSWERS: [&str; 5] = ["50", "51", "51", "51", "52"];

/// The error type of the example.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A stand-in model with a fixed answer and a price per token.
#[derive(Debug)]
struct Scripted {
    id: &'static str,
    tokens: u32,
    micros_per_token: u64,
}

impl AgentModel for Scripted {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, AgentError>> + Send + 'a>> {
        // The cheap model answers by draw id. The strong model is always
        // right.
        let answer = match self.id {
            "cheap" => {
                let draw = request.draw.map_or(0, DrawId::get);
                let index = usize::try_from(draw).unwrap_or(0) % CHEAP_ANSWERS.len();
                CHEAP_ANSWERS[index]
            }
            _ => EXPECTED,
        };
        Box::pin(std::future::ready(Ok(ChatResponse {
            content: vec![ContentPart::Text(answer.to_owned())],
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::new(self.tokens, 0),
        })))
    }

    fn model_id(&self) -> Option<&str> {
        Some(self.id)
    }

    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        Some(u64::from(usage.total()) * self.micros_per_token)
    }
}

/// The result of one strategy run.
#[derive(Debug, Clone)]
pub struct Outcome {
    /// The name of the strategy.
    pub name: &'static str,
    /// The selected answer. `None` when no draw passed.
    pub answer: Option<String>,
    /// The report of the run, with each draw and the cost.
    pub report: SampleReport,
}

/// The sampling policy of the kind: best-of-N, a vote and a cascade over
/// both models, up to 5 draws, 3 at the same time.
fn policy() -> SamplingPolicy {
    SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN, Strategy::Vote, Strategy::Cascade]),
        max_draws: 5,
        max_concurrent_draws: 3,
        cascade_models: BTreeSet::from(["cheap".to_owned(), "strong".to_owned()]),
        ..SamplingPolicy::default()
    }
}

/// A harness with one kind on the cheap model, and both models by name.
///
/// # Errors
///
/// Returns an error when the policy does not store.
async fn harness() -> Result<AgentHarness, Error> {
    let policies = Arc::new(InMemorySamplingPolicyStore::new());
    policies.set(KIND, &policy(), "ops@example.com").await?;
    let model = |id, tokens, micros_per_token| -> Arc<dyn AgentModel> {
        Arc::new(Scripted {
            id,
            tokens,
            micros_per_token,
        })
    };
    let mut profile = AgentProfile::new(KIND);
    profile.model = Some("cheap".to_owned());
    Ok(AgentHarness::new(model("cheap", 5, 1))
        .model_named("cheap", model("cheap", 5, 1))
        .model_named("strong", model("strong", 30, 10))
        .agent(profile)
        .scorer(Arc::new(ExactMatch::per_item()))
        .sampling_policies(policies))
}

/// The four strategies, each with its name.
///
/// # Errors
///
/// Returns an error when a built-in id or threshold is not valid.
fn strategies() -> Result<Vec<(&'static str, SampleInput)>, Error> {
    let scorer = ScorerRef::deterministic(ExactMatch::ID).ok_or("a valid scorer id")?;
    let pass = FiniteF64::new(1.0)?;
    let checked =
        |strategy| SampleInput::new(KIND, TASK, strategy, scorer.clone()).expected(EXPECTED);
    let cascade = SampleStrategy::cascade([Tier::new("cheap", pass), Tier::new("strong", pass)]);
    Ok(vec![
        ("single draw", checked(SampleStrategy::single())),
        ("best-of-3", checked(SampleStrategy::best_of_n(3))),
        // One draw at a time, so the vote can stop early.
        (
            "vote",
            SampleInput::vote(KIND, TASK, 5, AnswerKey::LastNumber).concurrency(1),
        ),
        ("cascade", checked(cascade)),
    ])
}

/// Run one strategy to its end.
async fn sample(rt: &mut SqliteRuntime, input: &SampleInput) -> Result<SampleReport, Error> {
    let exec = sqlite::start_sample(rt, input)?;
    match rt.run_until_blocked(exec).await? {
        RunState::Completed(value) => Ok(serde_json::from_value(value)?),
        other => Err(format!("the run did not complete: {other:?}").into()),
    }
}

/// Run each strategy on the database file `db`, in order.
///
/// # Errors
///
/// Returns an error when the database does not open or a run does not
/// complete.
pub async fn run(db: &Path) -> Result<Vec<Outcome>, Error> {
    let mut rt = SqliteRuntime::open(db)?;
    sqlite::register(&mut rt, Arc::new(harness().await?));
    let mut outcomes = Vec::new();
    for (name, input) in strategies()? {
        let report = sample(&mut rt, &input).await?;
        let answer = match report.outcome {
            SampleOutcome::Selected { .. } | SampleOutcome::Escalated { .. } => report
                .selected
                .as_ref()
                .map(|selected| selected.text.clone()),
            SampleOutcome::NoAcceptedDraw => None,
        };
        outcomes.push(Outcome {
            name,
            answer,
            report,
        });
    }
    Ok(outcomes)
}

/// The text of one verdict.
fn verdict(draw: &autumn_harvest_agent::sampling::SampleDraw) -> String {
    match (&draw.verdict, &draw.key) {
        (_, Some(key)) => format!("key {key}"),
        (Verdict::Score(score), None) => format!("score {:.1}", score.get()),
        (Verdict::Reject(_), None) => "rejected".to_owned(),
    }
}

/// The lines that show `outcome`: the answer, each draw and the cost.
#[must_use]
pub fn render(outcome: &Outcome) -> Vec<String> {
    let report = &outcome.report;
    let mut lines = vec![format!(
        "{}: answer {}, {} draws, {} µ$, stopped {}",
        outcome.name,
        outcome.answer.as_deref().unwrap_or("none"),
        report.draws.len(),
        report.cost_usd_micros,
        report.stopped.code(),
    )];
    for draw in &report.draws {
        lines.push(format!(
            "  draw {}  {:<6}  {:<9}  {} µ$",
            draw.draw.get(),
            draw.model.as_deref().unwrap_or("cheap"),
            verdict(draw),
            draw.cost_usd_micros,
        ));
    }
    lines
}
