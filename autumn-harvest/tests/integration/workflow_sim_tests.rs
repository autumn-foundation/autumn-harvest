//! Seeded interleaving simulation of user workflow code (issue #2033).
//!
//! These tests need no database. Each sweep runs every seed twice, replays
//! each history and repeats each seed with a crash.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::dst::SeedPlan;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::WorkflowTestEnv;
use autumn_harvest::testing::sim::{FaultPlan, SimOutcome, SimStatus};
use autumn_harvest::{always, sometimes};
use serde_json::{Value, json};

type Fut<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// A small sweep, so the suite stays fast on every OS.
const SEEDS: SeedPlan = SeedPlan {
    first: 0,
    count: 48,
};

fn quotes_env() -> WorkflowTestEnv {
    WorkflowTestEnv::new()
        .mock_activity("quote_primary", |_| Ok(json!(100)))
        .mock_activity("quote_backup", |_| Ok(json!(90)))
        .mock_activity("book", Ok)
}

/// Books a carrier with the first quote. The bug: it always books the
/// primary carrier, whichever quote won.
fn book_first_quote_buggy<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("quote_primary", json!({}), "q")
            .activity_raw("quote_backup", json!({}), "q")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        let booking = json!({"carrier": "primary", "price": winner.value});
        ctx.execute_activity_raw("book", booking, "q")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"winner": winner.index, "carrier": "primary"}))
    })
}

/// The fixed workflow books the carrier whose quote won.
fn book_first_quote_fixed<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("quote_primary", json!({}), "q")
            .activity_raw("quote_backup", json!({}), "q")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        let carrier = ["primary", "backup"][winner.index];
        sometimes!(winner.index == 1, "the backup quote wins");
        let booking = json!({"carrier": carrier, "price": winner.value});
        ctx.execute_activity_raw("book", booking, "q")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"winner": winner.index, "carrier": carrier}))
    })
}

/// The check that the two booking workflows must pass.
fn booked_carrier_won(outcome: &SimOutcome) {
    if let Ok(output) = outcome.result() {
        let carrier = ["primary", "backup"][output["winner"].as_u64().unwrap_or(0) as usize];
        always!(
            output["carrier"] == carrier,
            "the booked carrier is the one that won"
        );
    }
}

#[tokio::test]
async fn the_default_env_never_sees_the_backup_win() {
    let outcome = quotes_env().run(book_first_quote_buggy, json!(null)).await;
    assert_eq!(
        outcome.result,
        Ok(json!({"winner": 0, "carrier": "primary"}))
    );
}

#[tokio::test]
async fn the_sim_finds_the_race_bug_the_default_env_misses() {
    let env = quotes_env();
    let failure = env
        .simulation()
        .seeds(SEEDS)
        .test_name("workflow_sim_tests::the_sim_finds_the_race_bug_the_default_env_misses")
        .sweep(book_first_quote_buggy, json!(null), booked_carrier_won)
        .await
        .expect_err("some seed lands the backup quote first");
    let message = failure.to_string();
    assert!(
        message.contains("the booked carrier is the one that won"),
        "{message}"
    );
    assert!(
        message.contains(&format!("HARVEST_SIM_SEED={} ", failure.seed)),
        "{message}"
    );
    let repro = message
        .lines()
        .find(|line| line.starts_with("reproduce: "))
        .expect("a reproduce line");
    assert!(repro.contains("cargo test"), "{repro}");
    assert!(
        repro.contains("the_sim_finds_the_race_bug_the_default_env_misses"),
        "{repro}"
    );

    // The seed alone replays the failure.
    let replay = env
        .simulation()
        .run(failure.seed, book_first_quote_buggy, json!(null))
        .await;
    assert_eq!(
        replay.result().as_ref().map(|v| v["winner"].clone()),
        Ok(json!(1)),
        "the failing seed lands the backup quote first"
    );
}

#[tokio::test]
async fn the_fixed_workflow_passes_the_sweep_and_reaches_the_backup_win() {
    let report = quotes_env()
        .simulation()
        .seeds(SEEDS)
        .sweep(book_first_quote_fixed, json!(null), booked_carrier_won)
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(report.seeds, SEEDS.count);
    assert!(report.reached.contains("the backup quote wins"));
}

#[tokio::test]
async fn an_unsatisfied_sometimes_fails_the_sweep() {
    let failure = quotes_env()
        .simulation()
        .seeds(SEEDS)
        .sweep(book_first_quote_fixed, json!(null), |outcome| {
            sometimes!(outcome.tasks() > 1_000, "a run takes a thousand tasks");
        })
        .await
        .expect_err("no run takes a thousand tasks");
    let message = failure.to_string();
    assert!(message.contains("sometimes!"), "{message}");
    assert!(message.contains("a run takes a thousand tasks"), "{message}");
}

#[tokio::test]
async fn a_false_always_in_the_body_fails_the_sweep() {
    fn wf<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
        Box::pin(async move {
            let winner = ctx
                .race()
                .activity_raw("quote_primary", json!({}), "q")
                .activity_raw("quote_backup", json!({}), "q")
                .run()
                .await
                .map_err(|e| e.to_string())?;
            always!(winner.index == 0, "the primary always wins");
            Ok(json!(winner.index))
        })
    }
    let failure = quotes_env()
        .simulation()
        .seeds(SEEDS)
        .sweep(wf, json!(null), |_| {})
        .await
        .expect_err("the backup wins on some seed");
    assert!(failure.reason.contains("the primary always wins"), "{failure}");
}

/// Fans out three quotes, then waits for the `go` signal.
fn fan_out_then_signal<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
    Box::pin(async move {
        let items = (0..3)
            .map(|i| ("quote_primary".to_string(), json!(i), "q".to_string()))
            .collect();
        let quotes = ctx
            .execute_activity_fan_out_raw(items)
            .await
            .map_err(|e| e.to_string())?;
        let go = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
        Ok(json!({"quotes": quotes, "go": go}))
    })
}

fn completed_positions(events: &[WorkflowEvent]) -> Vec<usize> {
    events
        .iter()
        .enumerate()
        .filter(|(_, e)| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
        .map(|(i, _)| i)
        .collect()
}

fn scheduled_ids(events: &[WorkflowEvent]) -> Vec<autumn_harvest::types::ActivityExecId> {
    events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityScheduled { activity_id, .. } => Some(*activity_id),
            _ => None,
        })
        .collect()
}

fn completed_ids(events: &[WorkflowEvent]) -> Vec<autumn_harvest::types::ActivityExecId> {
    events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityCompleted { activity_id, .. } => Some(*activity_id),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_fan_out_sees_partial_batches_reordered_results_and_a_signal_between_them() {
    let report = quotes_env()
        .simulation()
        .signal("go", json!("now"))
        .seeds(SEEDS)
        .sweep(fan_out_then_signal, json!(null), |outcome| {
            always!(
                outcome.status() == SimStatus::Completed,
                "the fan-out completes"
            );
            let events = outcome.events();
            sometimes!(
                completed_ids(events) != scheduled_ids(events),
                "fan-out results land out of command order"
            );
            sometimes!(
                outcome.task_of_events(&completed_positions(events)).len() > 1,
                "fan-out results land in separate tasks"
            );
            let positions = completed_positions(events);
            let signal = events
                .iter()
                .position(|e| matches!(e, WorkflowEvent::SignalReceived { .. }));
            sometimes!(
                signal.is_some_and(|s| positions.first().is_some_and(|&p| p < s)
                    && positions.last().is_some_and(|&p| s < p)),
                "the signal lands between two fan-out results"
            );
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(report.seeds, SEEDS.count);
}

/// Waits for an activity or a one-minute timer, whichever comes first.
fn activity_or_timeout<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("quote_primary", json!({}), "q")
            .timer(Duration::from_secs(60))
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(winner.index))
    })
}

#[tokio::test]
async fn a_timer_can_win_or_lose_against_an_activity() {
    quotes_env()
        .simulation()
        .seeds(SEEDS)
        .sweep(activity_or_timeout, json!(null), |outcome| {
            let won = outcome.result().clone().ok();
            sometimes!(won == Some(json!(0)), "the activity beats the timer");
            sometimes!(won == Some(json!(1)), "the timer beats the activity");
            if won == Some(json!(1)) {
                always!(
                    outcome.elapsed() == chrono::Duration::seconds(60),
                    "a timer fires only at its deadline"
                );
            }
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

/// A signal-or-deadline race: approve within ten minutes.
fn approve_or_expire<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .timer(Duration::from_secs(600))
            .signal("approve")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(if winner.index == 1 { "approved" } else { "expired" }))
    })
}

#[tokio::test]
async fn a_mid_run_signal_can_win_or_lose_against_a_timer() {
    quotes_env()
        .simulation()
        .signal("approve", json!(true))
        .seeds(SEEDS)
        .sweep(approve_or_expire, json!(null), |outcome| {
            let result = outcome.result().clone().ok();
            sometimes!(result == Some(json!("approved")), "the signal wins");
            sometimes!(result == Some(json!("expired")), "the deadline wins");
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

/// Races two child workflows.
fn first_child<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .child_workflow_raw("left", json!(0))
            .child_workflow_raw("right", json!(1))
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(winner.index))
    })
}

#[tokio::test]
async fn child_results_land_in_seeded_orders() {
    WorkflowTestEnv::new()
        .mock_child_workflow("left", |_| Ok(json!("L")))
        .mock_child_workflow("right", |_| Ok(json!("R")))
        .simulation()
        .seeds(SEEDS)
        .sweep(first_child, json!(null), |outcome| {
            let won = outcome.result().clone().ok();
            sometimes!(won == Some(json!(0)), "the left child wins");
            sometimes!(won == Some(json!(1)), "the right child wins");
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

/// Two activities in sequence.
fn two_steps<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
    Box::pin(async move {
        let a = ctx
            .execute_activity_raw("quote_primary", json!(1), "q")
            .await
            .map_err(|e| e.to_string())?;
        let b = ctx
            .execute_activity_raw("quote_backup", json!(2), "q")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!([a, b]))
    })
}

#[tokio::test]
async fn a_cancel_lands_at_a_seeded_task_and_ends_the_run() {
    quotes_env()
        .simulation()
        .cancel("operator stop")
        .seeds(SEEDS)
        .sweep(two_steps, json!(null), |outcome| {
            let events = outcome.events();
            let completed = completed_positions(events).len();
            if outcome.status() == SimStatus::Cancelled {
                always!(
                    matches!(events.last(), Some(WorkflowEvent::WorkflowCancelled { .. })),
                    "a cancel is the last event"
                );
            }
            sometimes!(
                outcome.status() == SimStatus::Cancelled && completed == 0,
                "a cancel lands before the first result"
            );
            sometimes!(
                outcome.status() == SimStatus::Cancelled && completed == 1,
                "a cancel lands between two results"
            );
            sometimes!(
                outcome.status() == SimStatus::Completed,
                "the run completes before the cancel"
            );
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

#[tokio::test]
async fn each_seed_is_deterministic_and_a_mock_that_is_not_fails_the_sweep() {
    // This mock answers differently on each call, so the two runs of a seed
    // take different paths.
    let calls = Arc::new(AtomicU32::new(0));
    let env = WorkflowTestEnv::new()
        .mock_activity("flip", move |_| {
            Ok(json!(calls.fetch_add(1, Ordering::SeqCst) % 2 == 0))
        })
        .mock_activity("quote_primary", |_| Ok(json!(1)))
        .mock_activity("quote_backup", |_| Ok(json!(2)));
    fn wf<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
        Box::pin(async move {
            let heads = ctx
                .execute_activity_raw("flip", json!(null), "q")
                .await
                .map_err(|e| e.to_string())?;
            let next = if heads == json!(true) {
                "quote_primary"
            } else {
                "quote_backup"
            };
            ctx.execute_activity_raw(next, json!(null), "q")
                .await
                .map_err(|e| e.to_string())
        })
    }
    let failure = env
        .simulation()
        .seeds(SeedPlan { first: 0, count: 4 })
        .sweep(wf, json!(null), |_| {})
        .await
        .expect_err("the two runs of a seed differ");
    assert!(failure.reason.contains("not deterministic"), "{failure}");

    // A seeded mock takes the same path on both runs of a seed.
    let seeded = WorkflowTestEnv::new()
        .mock_activity_seeded("flip", |_, mock| Ok(json!(mock.rng().chance(50))))
        .mock_activity("quote_primary", |_| Ok(json!(1)))
        .mock_activity("quote_backup", |_| Ok(json!(2)));
    seeded
        .simulation()
        .seeds(SEEDS)
        .sweep(wf, json!(null), |outcome| {
            sometimes!(outcome.result() == &Ok(json!(1)), "heads");
            sometimes!(outcome.result() == &Ok(json!(2)), "tails");
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

#[tokio::test]
async fn each_explored_history_is_replayed() {
    fn renamed<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
        Box::pin(async move {
            ctx.execute_activity_raw("quote_renamed", json!(1), "q")
                .await
                .map_err(|e| e.to_string())
        })
    }
    let report = quotes_env()
        .simulation()
        .seeds(SEEDS)
        .sweep(two_steps, json!(null), |_| {})
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(report.replayed, SEEDS.count);

    // A build that renames the first activity cannot replay the old orders.
    let failure = quotes_env()
        .simulation()
        .seeds(SEEDS)
        .replay_with(renamed)
        .sweep(two_steps, json!(null), |_| {})
        .await
        .expect_err("the renamed build diverges");
    assert!(failure.reason.contains("replay"), "{failure}");
}

#[tokio::test]
async fn a_crash_and_resume_gives_the_uncut_outcome() {
    let env = quotes_env();
    let sim = env.simulation();
    for seed in 0..16 {
        let uncut = sim.run(seed, two_steps, json!(null)).await;
        for task in 1..=uncut.tasks() {
            let resumed = sim.run_with_crash(seed, task, two_steps, json!(null)).await;
            assert!(resumed.crashed_at().is_some(), "seed {seed} task {task}");
            uncut
                .same_outcome(&resumed)
                .unwrap_or_else(|diff| panic!("seed {seed} task {task}: {diff}"));
        }
    }
}

#[tokio::test]
async fn a_body_that_counts_its_runs_fails_the_crash_check() {
    struct Runs(AtomicU32);
    fn wf<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
        Box::pin(async move {
            let runs = ctx.state::<Runs>().expect("state").0.fetch_add(1, Ordering::SeqCst);
            // Not durable: a crash runs the body once more.
            let name = if runs < 2 { "quote_primary" } else { "quote_backup" };
            ctx.execute_activity_raw(name, json!(null), "q")
                .await
                .map_err(|e| e.to_string())?;
            ctx.execute_activity_raw("book", json!(null), "q")
                .await
                .map_err(|e| e.to_string())
        })
    }
    let env = || quotes_env().with_state(Runs(AtomicU32::new(0)));
    let uncut = env().simulation().run(3, wf, json!(null)).await;
    let crashed = env()
        .simulation()
        .run_with_crash(3, 1, wf, json!(null))
        .await;
    assert!(uncut.same_outcome(&crashed).is_err());
}

#[tokio::test]
async fn the_fault_lane_retries_and_fails_activities() {
    quotes_env()
        .simulation()
        .fault_percent(40)
        .seeds(SEEDS)
        .sweep(two_steps, json!(null), |outcome| {
            let events = outcome.events();
            let started = events
                .iter()
                .filter(|e| matches!(e, WorkflowEvent::ActivityStarted { .. }))
                .count();
            sometimes!(started > 2, "a transient fault is retried");
            sometimes!(
                events
                    .iter()
                    .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
                "an activity fails after its retries"
            );
            sometimes!(outcome.status() == SimStatus::Completed, "a run completes");
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

#[tokio::test]
async fn a_fault_plan_fails_an_activity_by_ordinal() {
    let outcome = quotes_env()
        .simulation()
        .faults(FaultPlan::new().fail("quote_backup", 1, "carrier down"))
        .run(5, two_steps, json!(null))
        .await;
    assert_eq!(outcome.status(), SimStatus::Failed);
    assert!(
        outcome
            .result()
            .as_ref()
            .is_err_and(|e| e.contains("carrier down")),
        "{:?}",
        outcome.result()
    );
    assert_eq!(outcome.faults(), ["quote_backup#1 failed: carrier down"]);

    let retried = quotes_env()
        .simulation()
        .faults(FaultPlan::new().transient("quote_primary", 1, 2))
        .run(5, two_steps, json!(null))
        .await;
    assert_eq!(retried.status(), SimStatus::Completed);
    assert_eq!(
        retried.faults(),
        ["quote_primary#1 attempt 1 failed", "quote_primary#1 attempt 2 failed"]
    );
}

#[tokio::test]
async fn a_seeded_mock_sees_the_virtual_clock() {
    fn wf<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
        Box::pin(async move {
            ctx.timer("wait", 30).await.map_err(|e| e.to_string())?;
            ctx.execute_activity_raw("stamp", json!(null), "q")
                .await
                .map_err(|e| e.to_string())
        })
    }
    let env = WorkflowTestEnv::new()
        .mock_activity_seeded("stamp", |_, mock| Ok(json!(mock.now().timestamp())));
    let start = env.now().timestamp();
    let outcome = env.simulation().run(1, wf, json!(null)).await;
    assert_eq!(outcome.result(), &Ok(json!(start + 30)));
    let default = env.run(wf, json!(null)).await;
    assert_eq!(default.result, Ok(json!(start + 30)));
}

#[tokio::test]
async fn the_trace_is_stable_and_names_tasks_not_ids() {
    let env = quotes_env();
    let sim = env.simulation();
    let a = sim.run(9, book_first_quote_fixed, json!(null)).await;
    let b = sim.run(9, book_first_quote_fixed, json!(null)).await;
    assert_eq!(a.trace(), b.trace());
    let text = a.trace().join("\n");
    assert!(text.contains("ActivityScheduled a1 quote_primary"), "{text}");
    assert!(!text.contains('-'), "no uuid leaks into the trace: {text}");
}
