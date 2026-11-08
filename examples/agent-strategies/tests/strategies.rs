//! The example runs each strategy and keeps the cost of each draw.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use agent_strategies::{EXPECTED, Outcome, run};

async fn outcomes() -> Vec<Outcome> {
    let dir = tempfile::tempdir().unwrap();
    run(&dir.path().join("strategies.db")).await.unwrap()
}

fn named<'a>(outcomes: &'a [Outcome], name: &str) -> &'a Outcome {
    outcomes
        .iter()
        .find(|outcome| outcome.name == name)
        .unwrap_or_else(|| panic!("no outcome named {name}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_example_runs_four_strategies_in_order() {
    let all = outcomes().await;
    let names: Vec<&str> = all.iter().map(|outcome| outcome.name).collect();
    assert_eq!(names, ["single draw", "best-of-3", "vote", "cascade"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_best_of_three_cost_is_the_sum_of_its_draws() {
    let all = outcomes().await;
    let best = named(&all, "best-of-3");
    assert_eq!(best.report.draws.len(), 3);
    let sum: u64 = best
        .report
        .draws
        .iter()
        .map(|draw| draw.cost_usd_micros)
        .sum();
    assert_eq!(best.report.cost_usd_micros, sum);
    assert_eq!(sum, 3 * 5, "3 cheap draws of 5 tokens at 1 µ$ each");
}

#[tokio::test(flavor = "multi_thread")]
async fn each_strategy_selects_the_answer_that_its_design_predicts() {
    let all = outcomes().await;
    // The cheap model misses on its first draw, so one draw finds nothing.
    assert_eq!(named(&all, "single draw").answer, None);
    assert_eq!(named(&all, "best-of-3").answer.as_deref(), Some(EXPECTED));
    let vote = named(&all, "vote");
    assert_eq!(vote.answer.as_deref(), Some(EXPECTED));
    assert_eq!(
        vote.report.draws.len(),
        4,
        "the vote stops early, at 4 of 5"
    );
    let cascade = named(&all, "cascade");
    assert_eq!(cascade.answer.as_deref(), Some(EXPECTED));
    assert_eq!(cascade.report.draws.len(), 2, "the strong tier answers");
    assert_eq!(cascade.report.cost_usd_micros, 5 + 30 * 10);
}
