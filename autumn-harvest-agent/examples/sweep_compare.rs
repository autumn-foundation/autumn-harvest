//! Compare two strategies on one fixed set of inputs, with no API key and
//! no network.
//!
//! Strategy A is best-of-3 with a cheap model. Strategy B is one draw with
//! a strong model. Each strategy runs as one durable `agent_sweep` over the
//! same six items. The example prints one row for each item and the totals
//! of each sweep.
//!
//! ```sh
//! cargo run -p autumn-harvest-agent --features sqlite --example sweep_compare
//! ```

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest_agent::sampling::{
    InMemorySamplingPolicyStore, SampleStrategy, SamplingPolicy, SamplingPolicyStore, ScorerRef,
    Strategy, SweepInput, SweepItem, SweepItems, SweepReport,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, ChatRequest, ChatResponse, ContentPart,
    DrawId, ExactMatch, FiniteF64, StopReason, TokenUsage, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};

/// The number of items of the fixed set.
const ITEMS: u32 = 6;

/// A stand-in model with a price per token.
///
/// The cheap model answers item `n` right only on the draw `d` for which
/// `n + d` is a multiple of 4. The strong model answers each item right except
/// item 3.
#[derive(Debug)]
struct Offline {
    id: &'static str,
    tokens: u32,
    micros_per_token: u64,
}

/// The item number in a question such as `What is 4 doubled?`.
fn item_of(request: &ChatRequest) -> u32 {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .find_map(|part| match part {
            ContentPart::Text(text) => text.split(' ').nth(2).and_then(|n| n.parse().ok()),
            _ => None,
        })
        .unwrap_or(0)
}

impl AgentModel for Offline {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, AgentError>> + Send + 'a>> {
        let n = item_of(request);
        let draw = request.draw.map_or(0, DrawId::get);
        let right = match self.id {
            "cheap" => (n + draw).is_multiple_of(4),
            _ => n != 3,
        };
        let answer = if right { n * 2 } else { n * 2 + 1 };
        Box::pin(std::future::ready(Ok(ChatResponse {
            content: vec![ContentPart::Text(answer.to_string())],
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

/// The fixed set: `What is n doubled?`, with the expected answer `2n`.
fn items() -> Vec<SweepItem> {
    (0..ITEMS)
        .map(|n| SweepItem::new(format!("What is {n} doubled?")).expected((n * 2).to_string()))
        .collect()
}

/// A harness with two kinds: `cheap` may run best-of-3, and `strong` runs
/// a single draw under the default policy.
fn harness() -> Result<AgentHarness, Box<dyn std::error::Error>> {
    let policies = Arc::new(InMemorySamplingPolicyStore::new());
    let best_of_3 = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        max_draws: 3,
        max_concurrent_draws: 3,
        ..SamplingPolicy::default()
    };
    futures::executor::block_on(policies.set("cheap", &best_of_3, "ops@example.com"))?;
    let mut cheap = AgentProfile::new("cheap");
    cheap.model = Some("cheap".to_owned());
    let mut strong = AgentProfile::new("strong");
    strong.model = Some("strong".to_owned());
    let model = |id, tokens, micros_per_token| -> Arc<dyn AgentModel> {
        Arc::new(Offline {
            id,
            tokens,
            micros_per_token,
        })
    };
    Ok(AgentHarness::new(model("cheap", 5, 1))
        .model_named("cheap", model("cheap", 5, 1))
        .model_named("strong", model("strong", 30, 10))
        .agent(cheap)
        .agent(strong)
        .scorer(Arc::new(ExactMatch::per_item()))
        .sampling_policies(policies))
}

/// Run one sweep to its end.
async fn sweep(
    rt: &mut SqliteRuntime,
    kind: &str,
    strategy: SampleStrategy,
) -> Result<SweepReport, Box<dyn std::error::Error>> {
    let scorer = ScorerRef::deterministic(ExactMatch::ID).ok_or("a valid scorer id")?;
    let input = SweepInput::new(kind, SweepItems::inline(items()), strategy, scorer);
    let exec = sqlite::start_sweep(rt, &input)?;
    match rt.run_until_blocked(exec).await? {
        RunState::Completed(value) => Ok(serde_json::from_value(value)?),
        other => Err(format!("the sweep did not complete: {other:?}").into()),
    }
}

/// Both sweeps, on one database file.
async fn compare(
    db: &std::path::Path,
) -> Result<(SweepReport, SweepReport), Box<dyn std::error::Error>> {
    let mut rt = SqliteRuntime::open(db)?;
    sqlite::register(&mut rt, Arc::new(harness()?));
    let a = sweep(&mut rt, "cheap", SampleStrategy::best_of_n(3)).await?;
    let b = sweep(&mut rt, "strong", SampleStrategy::single()).await?;
    Ok((a, b))
}

/// The text of an optional ratio.
fn shown(value: Option<FiniteF64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| format!("{:.2}", value.get()))
}

/// Print the rows and the totals of `report`.
fn print(name: &str, report: &SweepReport) {
    println!("{name}");
    println!("  item  status     accepted  answer  draws  tokens  cost (µ$)");
    for row in &report.rows {
        println!(
            "  {:>4}  {:<9}  {:<8}  {:>6}  {:>5}  {:>6}  {:>9}",
            row.item,
            format!("{:?}", row.status),
            row.accepted,
            row.answer.as_deref().unwrap_or("-"),
            row.draws,
            row.usage.total(),
            row.cost_usd_micros,
        );
    }
    let totals = &report.totals;
    println!(
        "  mean score {}, accept rate {}, total cost {} µ$, cost per accepted answer {} µ$",
        shown(totals.mean_score),
        shown(totals.accept_rate),
        totals.cost_usd_micros,
        totals
            .cost_per_accepted_usd_micros
            .map_or_else(|| "-".to_owned(), |cost| cost.to_string()),
    );
    println!();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let (a, b) = compare(&dir.path().join("sweep.db")).await?;
    print("A: best-of-3, cheap model", &a);
    print("B: one draw, strong model", &b);
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn the_example_compares_two_strategies() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = compare(&dir.path().join("sweep.db")).await.unwrap();
        assert_eq!(a.rows.len(), 6);
        assert_eq!(b.rows.len(), 6);
        // Items 1 and 5 have no right draw among 3 cheap draws.
        assert_eq!(a.totals.accepted, 4);
        assert_eq!(a.totals.cost_usd_micros, 6 * 3 * 5);
        // The strong model misses item 3 only.
        assert_eq!(b.totals.accepted, 5);
        assert_eq!(b.totals.cost_usd_micros, 6 * 30 * 10);
        assert!(a.totals.cost_per_accepted_usd_micros < b.totals.cost_per_accepted_usd_micros);
    }
}
