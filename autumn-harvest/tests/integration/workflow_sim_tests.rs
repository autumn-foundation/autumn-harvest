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
fn book_first_quote_buggy(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
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
fn book_first_quote_fixed(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
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
        let winner = output["winner"]
            .as_u64()
            .and_then(|w| usize::try_from(w).ok());
        let carrier = ["primary", "backup"][winner.unwrap_or(0)];
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
        .cargo_args("--no-default-features --features testing")
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
    assert!(repro.contains("cargo test -p autumn-harvest"), "{repro}");
    assert!(
        repro.contains("--no-default-features --features testing --test integration "),
        "{repro}"
    );
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
    assert!(report.seeds > 0);
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
    assert!(
        message.contains("a run takes a thousand tasks"),
        "{message}"
    );
}

#[tokio::test]
async fn a_false_always_in_the_body_fails_the_sweep() {
    fn wf(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
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
    assert!(
        failure.reason.contains("the primary always wins"),
        "{failure}"
    );
}

/// Fans out three quotes, then waits for the `go` signal.
fn fan_out_then_signal(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
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
                outcome.tasks_of_events(&completed_positions(events)).len() > 1,
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
    assert!(report.seeds > 0);
}

/// Waits for an activity or a one-minute timer, whichever comes first.
fn activity_or_timeout(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
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
fn approve_or_expire(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .timer(Duration::from_secs(600))
            .signal("approve")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(if winner.index == 1 {
            "approved"
        } else {
            "expired"
        }))
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
fn first_child(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
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
fn two_steps(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
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
    fn wf(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
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
    // This mock answers differently on each call, so the two runs of a seed
    // take different paths.
    let calls = Arc::new(AtomicU32::new(0));
    let env = WorkflowTestEnv::new()
        .mock_activity("flip", move |_| {
            Ok(json!(
                calls.fetch_add(1, Ordering::SeqCst).is_multiple_of(2)
            ))
        })
        .mock_activity("quote_primary", |_| Ok(json!(1)))
        .mock_activity("quote_backup", |_| Ok(json!(2)));
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
    fn renamed(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
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
    assert_eq!(report.replayed, report.seeds);

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
    fn wf(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
        Box::pin(async move {
            let runs = ctx
                .state::<Runs>()
                .expect("state")
                .0
                .fetch_add(1, Ordering::SeqCst);
            ctx.execute_activity_raw("book", json!(null), "q")
                .await
                .map_err(|e| e.to_string())?;
            // Not durable: only the last cycle reaches this line, and a crash
            // runs the body once more, so the parity changes.
            let parity = if runs % 2 == 0 { "even" } else { "odd" };
            ctx.side_effect(parity, || json!(1))
                .map_err(|e| e.to_string())
        })
    }
    let env = || quotes_env().with_state(Runs(AtomicU32::new(0)));
    let uncut = env().simulation().run(3, wf, json!(null)).await;
    let crashed = env()
        .simulation()
        .run_with_crash(3, 1, wf, json!(null))
        .await;
    assert_eq!(uncut.status(), SimStatus::Completed, "{:?}", uncut.result());
    assert_eq!(
        crashed.status(),
        SimStatus::Completed,
        "{:?}",
        crashed.result()
    );
    assert_eq!(crashed.crashed_at(), Some(1));
    let diff = uncut
        .same_outcome(&crashed)
        .expect_err("the parity differs");
    assert!(diff.contains("SideEffectRecorded"), "{diff}");
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
        [
            "quote_primary#1 attempt 1 failed",
            "quote_primary#1 attempt 2 failed"
        ]
    );
}

#[tokio::test]
async fn a_seeded_mock_sees_the_virtual_clock() {
    fn wf(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
        Box::pin(async move {
            ctx.timer("wait", 30).await.map_err(|e| e.to_string())?;
            ctx.execute_activity_raw("stamp", json!(null), "q")
                .await
                .map_err(|e| e.to_string())
        })
    }
    let env = WorkflowTestEnv::new()
        .mock_activity_seeded("stamp", |_, mock| Ok(json!(mock.now().timestamp())));
    let outcome = env.simulation().run(1, wf, json!(null)).await;
    let started = |events: &[WorkflowEvent]| match events.first() {
        Some(WorkflowEvent::WorkflowStarted { timestamp, .. }) => timestamp.timestamp(),
        other => panic!("{other:?}"),
    };
    assert_eq!(outcome.result(), &Ok(json!(started(outcome.events()) + 30)));
    let default = env.run(wf, json!(null)).await;
    assert_eq!(default.result, Ok(json!(env.now().timestamp() + 30)));
}

#[tokio::test]
async fn the_trace_is_stable_and_names_tasks_not_ids() {
    let env = quotes_env();
    let sim = env.simulation();
    let a = sim.run(9, book_first_quote_fixed, json!(null)).await;
    let b = sim.run(9, book_first_quote_fixed, json!(null)).await;
    assert_eq!(a.trace(), b.trace());
    let text = a.trace().join("\n");
    assert!(
        text.contains("ActivityScheduled a1 quote_primary"),
        "{text}"
    );
    assert!(!text.contains('-'), "no uuid leaks into the trace: {text}");
}

/// The guide names what the code ships, and each test it cites exists.
#[test]
fn the_guide_matches_the_code() {
    use autumn_harvest::testing::sim::{
        DEFAULT_MAX_ATTEMPTS, DEFAULT_SEEDS, SEED_BASE_VAR, SEED_VAR, SEEDS_VAR,
    };
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let read = |path: &str| {
        std::fs::read_to_string(root.join(path)).unwrap_or_else(|e| panic!("{path}: {e}"))
    };
    let guide = read("docs/testing/workflow-simulation.md");
    for var in [SEED_VAR, SEEDS_VAR, SEED_BASE_VAR] {
        assert!(guide.contains(&format!("`{var}`")), "the guide names {var}");
    }
    assert!(guide.contains(&format!("The default is {DEFAULT_SEEDS}.")));
    assert!(guide.contains(&format!("`DEFAULT_MAX_ATTEMPTS` ({DEFAULT_MAX_ATTEMPTS})")));
    assert!(guide.contains("What it does not cover"));
    assert!(guide.contains("Wire the sweep into your own CI"));

    let cited = [
        (
            "autumn-harvest/src/worker.rs",
            "fn sim_wake_order_matches_merge_wake_events",
        ),
        (
            "autumn-harvest/tests/integration/workflow_sim_ingest_tests.rs",
            "fn sim_wake_order_matches_the_real_ingest",
        ),
    ];
    for (path, name) in cited {
        assert!(read(path).contains(name), "{path} holds {name}");
    }
}

/// Branches on `ctx.random_u64()` and reads the clock.
fn coin_and_clock(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
    Box::pin(async move {
        let next = if ctx.random_u64().is_multiple_of(2) {
            "quote_primary"
        } else {
            "quote_backup"
        };
        let id = ctx.new_uuid();
        ctx.execute_activity_raw(next, json!(null), "q")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({
            "next": next,
            "id": id.to_string(),
            "version": id.get_version_num(),
            "now": ctx.system_now().timestamp(),
        }))
    })
}

#[tokio::test]
async fn built_in_side_effects_and_the_clock_are_seeded() {
    quotes_env()
        .simulation()
        .seeds(SEEDS)
        .sweep(coin_and_clock, json!(null), |outcome| {
            let next = outcome.result().as_ref().map(|v| v["next"].clone()).ok();
            sometimes!(next == Some(json!("quote_primary")), "heads");
            sometimes!(next == Some(json!("quote_backup")), "tails");
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));

    // Two envs built at different times give one result for one seed.
    let a = quotes_env()
        .simulation()
        .run(3, coin_and_clock, json!(null))
        .await;
    std::thread::sleep(Duration::from_millis(5));
    let b = quotes_env()
        .simulation()
        .run(3, coin_and_clock, json!(null))
        .await;
    assert_eq!(
        a.result(),
        b.result(),
        "the seed fixes the draws and the id"
    );
    let output = a.result().clone().expect("completes");
    // Monday 2024-01-01 00:00 UTC, the fixed start of a run.
    assert_eq!(output["now"], json!(1_704_067_200));
    assert_eq!(output["version"], json!(7), "new_uuid stays a v7 UUID");
    let other = quotes_env()
        .simulation()
        .run(4, coin_and_clock, json!(null))
        .await;
    assert_ne!(a.result(), other.result(), "another seed draws another id");
}

#[tokio::test]
async fn a_cancellation_from_the_env_works_in_a_sweep() {
    fn wf(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
        Box::pin(async move {
            // A cancel from the env is visible from the first cycle.
            if ctx.is_cancelled() {
                return Ok(json!("cleaned up"));
            }
            Ok(json!("ran"))
        })
    }
    quotes_env()
        .with_cancellation("stop")
        .simulation()
        .seeds(SEEDS)
        .sweep(wf, json!(null), |outcome| {
            always!(
                outcome.result() == &Ok(json!("cleaned up")),
                "the body cleans up"
            );
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

#[tokio::test]
async fn a_required_label_that_no_run_evaluates_fails_the_sweep() {
    let failure = quotes_env()
        .simulation()
        .seeds(SeedPlan { first: 0, count: 4 })
        .require_reached("an unreachable branch")
        .sweep(two_steps, json!(null), |_| {})
        .await
        .expect_err("no run evaluates the label");
    assert!(
        failure.reason.contains("an unreachable branch"),
        "{failure}"
    );
}

/// Waits on a classic 20 s timer and a cancellable 10 s timer at once.
fn two_timers(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
    Box::pin(async move {
        let short = ctx.start_timer("short", 10);
        tokio::select! {
            // `biased` polls in order, so the body is deterministic.
            biased;
            fired = short.await_fire() => {
                fired.map_err(|e| e.to_string())?;
                Ok(json!("short"))
            }
            done = ctx.timer("long", 20) => {
                done.map_err(|e| e.to_string())?;
                Ok(json!("long"))
            }
        }
    })
}

/// The timers that fired, in history order.
fn fired(events: &[WorkflowEvent]) -> Vec<&str> {
    events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::TimerFired { timer_id } => Some(timer_id.as_str()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn an_awaited_timer_fires_by_its_deadline_beside_a_classic_timer() {
    // The default env never fires an awaited cancellable timer beside another
    // wait, so the later classic timer wins. Production cannot do that.
    let default = WorkflowTestEnv::new().run(two_timers, json!(null)).await;
    assert_eq!(fired(default.events()), ["long"]);
    assert_eq!(default.result, Ok(json!("long")));

    // The sim fires the earlier deadline first, as the worker does.
    let env = WorkflowTestEnv::new();
    let sim = env.simulation();
    for seed in 0..SEEDS.count {
        let outcome = sim.run(seed, two_timers, json!(null)).await;
        assert_eq!(fired(outcome.events()), ["short"], "seed {seed}");
    }

    // That order exposes a real bug: the losing classic timer leaves its
    // `TimerStarted` unconsumed, so the history does not replay.
    let failure = sim
        .sweep(two_timers, json!(null), |_| {})
        .await
        .expect_err("the history does not replay");
    assert!(failure.reason.contains("does not replay"), "{failure}");
}

#[tokio::test]
async fn an_activity_start_can_land_before_its_result() {
    // A start lands without a wake, beside another activity's result. Its
    // own result can then land in a later task.
    quotes_env()
        .simulation()
        .signal("go", json!("now"))
        .seeds(SEEDS)
        .sweep(fan_out_then_signal, json!(null), |outcome| {
            let events = outcome.events();
            let task_of = |i: usize| outcome.tasks_of_events(&[i]);
            let split = events.iter().enumerate().any(|(s, start)| {
                let WorkflowEvent::ActivityStarted { activity_id, .. } = start else {
                    return false;
                };
                events.iter().enumerate().any(|(d, done)| {
                    matches!(done, WorkflowEvent::ActivityCompleted { activity_id: id, .. } if id == activity_id)
                        && task_of(s) != task_of(d)
                })
            });
            sometimes!(split, "a start and its result land in different tasks");
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

/// Joins a signal wait with a cancellable 10 s timer.
fn signal_and_timer(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
    Box::pin(async move {
        let timer = ctx.start_timer("pause", 10);
        let (fired, go) = tokio::join!(timer.await_fire(), ctx.wait_for_signal("go"));
        go.map_err(|e| e.to_string())?;
        fired.map_err(|e| e.to_string())?;
        Ok(json!("both"))
    })
}

#[tokio::test]
async fn a_timer_held_beside_a_signal_wait_still_fires_after_it() {
    // The first batch parks on the signal, so the timer waits for a wake.
    // Once the signal lands, the batch is timer bookkeeping only, and the
    // worker wakes the task at the deadline. A stale flag would block here.
    quotes_env()
        .simulation()
        .signal("go", json!(true))
        .seeds(SEEDS)
        .sweep(signal_and_timer, json!(null), |outcome| {
            always!(
                outcome.status() == SimStatus::Completed,
                "the join completes"
            );
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

/// Waits for a cancellable 10 s timer or an activity, timer first.
fn timer_or_activity(ctx: &WorkflowContext, _: Value) -> Fut<'_> {
    Box::pin(async move {
        let timer = ctx.start_timer("deadline", 10);
        tokio::select! {
            biased;
            fired = timer.await_fire() => {
                fired.map_err(|e| e.to_string())?;
                Ok(json!("timer"))
            }
            quote = ctx.execute_activity_raw("quote_primary", json!(null), "q") => {
                quote.map_err(|e| e.to_string())
            }
        }
    })
}

#[tokio::test]
async fn an_activity_start_alone_does_not_wake_a_parked_task() {
    // The batch parks on one activity, so the worker sets no wake-up time.
    // The timer can then fire only in the task that the result wakes.
    let env = quotes_env();
    let sim = env.simulation();
    for seed in 0..SEEDS.count {
        let outcome = sim.run(seed, timer_or_activity, json!(null)).await;
        let events = outcome.events();
        if let Some(fire) = events
            .iter()
            .position(|e| matches!(e, WorkflowEvent::TimerFired { .. }))
        {
            assert!(
                events[..fire]
                    .iter()
                    .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
                "seed {seed}: the timer fired before the result woke the task"
            );
        }
    }
}

#[tokio::test]
async fn a_mock_that_changes_a_value_on_the_same_path_fails_the_sweep() {
    // Each call returns a new number, but the workflow takes one path.
    let calls = Arc::new(AtomicU32::new(0));
    let env = WorkflowTestEnv::new()
        .mock_activity("quote_primary", move |_| {
            Ok(json!(calls.fetch_add(1, Ordering::SeqCst)))
        })
        .mock_activity("quote_backup", |_| Ok(json!(2)));
    let failure = env
        .simulation()
        .seeds(SeedPlan { first: 0, count: 4 })
        .sweep(two_steps, json!(null), |_| {})
        .await
        .expect_err("the payloads of the two runs differ");
    assert!(failure.reason.contains("not deterministic"), "{failure}");
    assert!(failure.reason.contains("payload"), "{failure}");
}

#[tokio::test]
async fn a_start_alone_runs_no_cycle() {
    // The worker appends `ActivityStarted` without a wake. So no task may
    // hold only `ActivityStarted` events: the next cycle runs only after a
    // waking delivery lands with them.
    let env = quotes_env();
    let sim = env.simulation();
    for seed in 0..SEEDS.count {
        let outcome = sim.run(seed, two_steps, json!(null)).await;
        let events = outcome.events();
        for task in 1..=outcome.tasks() {
            let in_task: Vec<&WorkflowEvent> = (0..events.len())
                .filter(|&i| outcome.tasks_of_events(&[i]).contains(&task))
                .map(|i| &events[i])
                .collect();
            assert!(
                in_task.is_empty()
                    || !in_task
                        .iter()
                        .all(|e| matches!(e, WorkflowEvent::ActivityStarted { .. })),
                "seed {seed}: task {task} ran on a start alone"
            );
        }
    }
}
