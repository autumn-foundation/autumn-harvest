//! CI-gated performance contract for the task-claim hot path (issue #786).
//!
//! `queue::claim_task` is the single most scalability-critical query in the
//! engine and has accreted roughly a `WHERE` predicate per phase since 3.7
//! (build-id routing #171, per-key concurrency #247, rate-limit gate #332/#699,
//! circuit-breaker tracked set #369, schedule-to-close #378, pause skip #383,
//! worker sessions #606, queue pauses #619). Each was added for correctness;
//! until this suite, none was measured.
//!
//! This is the gate. It runs the single published **headline scenario** and
//! fails the build when **p50** claim latency exceeds the budget in
//! `docs/performance.md`. The exploratory report — every gate at every backlog
//! depth, plus `EXPLAIN (ANALYZE, BUFFERS)` — lives in `benches/claim_bench.rs`
//! and shares this crate's [`claim_bench_support`] harness, so the gate and the
//! benchmark can never drift apart.
//!
//! # Why p50 and not p99
//!
//! The headline scenario runs 8 concurrent claimers against a 4-core reference
//! box, so the database is deliberately oversubscribed — contention is the point
//! of the scenario. That makes the *tail* a measurement of the run queue rather
//! than of the claim path: across repeated reference runs p50 moved ~2.3x
//! between an idle and a loaded box (~220 ms → ~516 ms) while p99 moved ~15x
//! (~300 ms → ~4 665 ms). A p99 gate at this budget failed about one run in
//! three during review, on the same hardware class that produced the published
//! numbers. p99 is still measured, still published, and printed below on
//! failure — it is simply not the assertion. See
//! [`HEADLINE_P50_BUDGET_MS`] for the full derivation.
//!
//! # Reading a failure
//!
//! A failure here means one of three things, in decreasing order of likelihood:
//!
//! 1. A new claim-path predicate is expensive. Run
//!    `cargo bench -p autumn-harvest --features db --bench claim_bench` and read
//!    the per-gate table and the `EXPLAIN` output.
//! 2. The runner is unusually loaded. The budget carries deliberate headroom
//!    over the reference measurement precisely so this is rare; if it happens,
//!    `HARVEST_CLAIM_BUDGET_MS` overrides it for a one-off run.
//! 3. The measurement itself is unsound — see the sample-count, truncation,
//!    `claim_ratio` and backlog-drain assertions below, which fail loudly rather
//!    than reporting a meaningless percentile.
//!
//! [`claim_bench_support`]: super::claim_bench_support

#![cfg(feature = "db")]

use std::time::Duration;

use super::claim_bench_support::db::{self, BenchDb};
use super::claim_bench_support::{
    BUDGET_ENV_VAR, BudgetVerdict, ClaimGate, HEADLINE_P50_BUDGET_MS, LatencyStats,
    MIN_MEANINGFUL_SAMPLES, SCENARIO_BUDGET_ENV_VAR, Scenario, budget_from_env, budget_verdict,
    db_name_from_url, headline_scenario, measured_claims_for, scenario_time_budget,
    sweep_probe_decoy_name, with_db_name, with_setup_deadline, with_verify_deadline,
};

// The published budget lives in the shared harness as
// `HEADLINE_P50_BUDGET_MS`, not here, so a pure test can hold it and the
// wall-clock ceiling to a coherent pair — a latency budget the wall clock does
// not admit would make the truncation assertion fire before this gate ever
// rendered a verdict. See its doc comment for how the number was derived and
// why it is a cliff detector rather than a drift detector.
//
// Override for a one-off run with `HARVEST_CLAIM_BUDGET_MS`, deliberately not
// named for a statistic so it stays honest if the gate ever asserts a different
// one.

/// Minimum fraction of measured operations that must actually return a task.
///
/// Without this floor the gate could "pass" while measuring nothing: a harness
/// that claimed zero tasks would report a tiny latency for a queue it never
/// touched.
const MIN_CLAIM_RATIO: f64 = 0.90;

/// Slack allowed above [`scenario_time_budget`] before a gate calls the
/// wall-clock ceiling violated.
///
/// Absorbs task join and report assembly under an oversubscribed runner. It is
/// far smaller than the overruns these assertions exist to catch — an unbounded
/// await runs for minutes or forever, not for seconds.
const WALL_CLOCK_SLACK: Duration = Duration::from_secs(30);

/// Acquire a benchmark database, or decide how to proceed without one.
///
/// Locally (no Docker, no `HARVEST_TEST_DATABASE_URL`) the gate skips loudly.
/// Under CI it **fails**: a performance gate that silently no-ops when its
/// dependency is missing is not a gate.
async fn bench_db_or_skip() -> Option<BenchDb> {
    match db::setup_bench_db().await {
        Ok(db) => Some(db),
        Err(reason) => {
            assert!(
                std::env::var("CI").is_err(),
                "claim budget gate could not reach a database under CI: {}. \
                 The gate must never silently pass — either provide Docker (the \
                 manifest runs this suite on Linux) or set \
                 HARVEST_TEST_DATABASE_URL to an admin connection string.",
                reason.0,
            );
            eprintln!(
                "SKIP: claim budget gate needs Postgres ({}). \
                 Set HARVEST_TEST_DATABASE_URL or start Docker.",
                reason.0,
            );
            None
        }
    }
}

fn render(stats: LatencyStats) -> String {
    format!(
        "n={} p50={:.2}ms p99={:.2}ms max={:.2}ms mean={:.2}ms",
        stats.count, stats.p50_ms, stats.p99_ms, stats.max_ms, stats.mean_ms
    )
}

/// The headline gate: 10k pending tasks, 8 concurrent claimers, 4 queues.
///
/// Asserts **p50**; see the module docs for why the tail is not the assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claim_p50_at_headline_scenario_is_within_budget() {
    let Some(db) = bench_db_or_skip().await else {
        return;
    };

    let scenario = headline_scenario();
    let report = db::run_claim_scenario(&db, scenario).await;
    let budget = budget_from_env(HEADLINE_P50_BUDGET_MS);

    eprintln!(
        "claim[{}] backlog={} claimers={} queues={} :: {} :: {:.0} claims/s \
         (claimed={} empty={} ratio={:.2}) on {}",
        scenario.gate.as_str(),
        scenario.backlog,
        scenario.claimers,
        scenario.queues,
        render(report.stats),
        report.claims_per_sec(),
        report.claimed,
        report.empty,
        report.claim_ratio(),
        db::machine_fingerprint(),
    );

    // Soundness zero: a percentile over a handful of samples is noise. The
    // harness stops early on a wall-clock budget, so a severe regression could
    // otherwise leave the gate defending a two-sample percentile.
    assert!(
        report.stats.count >= MIN_MEANINGFUL_SAMPLES,
        "measurement unsound: only {} samples collected (need >= {MIN_MEANINGFUL_SAMPLES}). \
         The scenario hit its wall-clock budget almost immediately, which is \
         itself a signal that the claim path is far slower than the budget.",
        report.stats.count,
    );

    // Soundness zero-and-a-half: `count >= MIN_MEANINGFUL_SAMPLES` can be
    // satisfied by a run that was cut off part way, and a truncated run's
    // percentiles describe a shorter, differently-warmed window than the
    // published ones. The gate defends a *complete* scenario or it says so.
    assert!(
        !report.truncated,
        "measurement unsound: the scenario hit its {}s wall-clock budget and \
         stopped early with {} samples. The percentiles below are over a partial run. \
         Raise {} for a one-off, but a headline scenario that cannot finish in \
         the default budget is itself the regression signal.",
        scenario_time_budget().as_secs(),
        report.stats.count,
        SCENARIO_BUDGET_ENV_VAR,
    );

    // Soundness zero-and-three-quarters: `truncated` is set only where the
    // harness *checks* the deadline, so it cannot catch an await that never
    // returns to a check — a stalled pool checkout, say. Assert the ceiling
    // the harness advertises directly, against the clock, so an unbounded
    // wait anywhere in the scenario surfaces as a failure rather than as a
    // job that quietly runs for minutes past its cap. See `WALL_CLOCK_SLACK`
    // for why the slack is safe.
    let ceiling = scenario_time_budget() + WALL_CLOCK_SLACK;
    assert!(
        report.wall_secs <= ceiling.as_secs_f64(),
        "measurement unsound: the scenario ran {:.1}s, past its {}s wall-clock \
         budget (+30s slack). Some await in the scenario is not bounded by the \
         deadline, so the advertised ceiling is not being honoured.",
        report.wall_secs,
        scenario_time_budget().as_secs(),
    );

    // Soundness first: a percentile over a run that claimed nothing is noise.
    assert!(
        report.claim_ratio() >= MIN_CLAIM_RATIO,
        "measurement unsound: only {:.0}% of {} operations claimed a task \
         (claimed={}, empty={}). The reported latency would not describe the claim \
         path. Investigate contention or seeding before trusting the budget.",
        report.claim_ratio() * 100.0,
        report.claimed + report.empty,
        report.claimed,
        report.empty,
    );

    // Soundness second: the backlog must still be deep. `claim_task` is
    // destructive, so a run that drained the queue would be timing claims
    // against an increasingly empty table.
    let mut conn =
        with_verify_deadline("connect", scenario_time_budget(), db::connect(&db.url)).await;
    let remaining = with_verify_deadline(
        "backlog depth check",
        scenario_time_budget(),
        db::pending_count(&mut conn),
    )
    .await;
    let floor = i64::try_from(scenario.backlog * 4 / 5).expect("backlog fits i64");
    assert!(
        remaining >= floor,
        "backlog drained below 80%: {remaining} PENDING rows remain of {} seeded; \
         the measurement no longer describes a {}-deep queue",
        scenario.backlog,
        scenario.backlog,
    );

    // The gate itself. Asserted on p50 rather than p99: at 8 claimers against a
    // 4-core box the tail measures the run queue, not the claim path (see the
    // module docs). The full stat line — p99 included — is in the message, so a
    // tail regression is still visible to whoever reads the failure.
    assert_eq!(
        budget_verdict(report.stats.p50_ms, budget),
        BudgetVerdict::WithinBudget,
        "CLAIM PATH REGRESSION: p50 {:.2}ms exceeds the {budget:.2}ms budget for \
         the headline scenario ({} pending / {} claimers / {} queues). {}\n\
         Run `cargo bench -p autumn-harvest --features db --bench claim_bench` \
         for the per-gate cost table and EXPLAIN output, and see \
         docs/performance.md. Override for a one-off run with {BUDGET_ENV_VAR}.",
        report.stats.p50_ms,
        scenario.backlog,
        scenario.claimers,
        scenario.queues,
        render(report.stats),
    );
}

/// The gate must be falsifiable against **real measured data**, not just in the
/// unit tests of the comparison function.
///
/// Runs the same measurement and asserts that a budget below the observed p50 —
/// the statistic the gate actually asserts on — would have tripped it. Without
/// this, a harness bug that reported `0.0` for every percentile would let the
/// gate pass forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gate_would_trip_on_a_budget_below_the_measured_p50() {
    let Some(db) = bench_db_or_skip().await else {
        return;
    };

    // A shallow backlog: proving the gate is falsifiable needs real measured
    // data, not the headline scale, and this keeps the CI suite cheap.
    let scenario = Scenario {
        backlog: 1_000,
        claimers: 4,
        queues: 2,
        gate: ClaimGate::Baseline,
    };
    let report = db::run_claim_scenario(&db, scenario).await;

    assert!(
        report.stats.p50_ms > 0.0,
        "measured p50 was {:.6}ms — a claim against Postgres cannot be free, so \
         the harness is not measuring anything",
        report.stats.p50_ms,
    );

    let impossible_budget = report.stats.p50_ms / 2.0;
    assert_eq!(
        budget_verdict(report.stats.p50_ms, impossible_budget),
        BudgetVerdict::Exceeded,
        "gate is vacuous: measured p50 {:.2}ms did not exceed a {impossible_budget:.2}ms budget",
        report.stats.p50_ms,
    );
}

/// Enqueue is the write side of the same hot path — a start-storm stresses it
/// directly. Gated only for soundness (it produced throughput at all), since
/// write throughput on a shared runner is too noisy to defend a number.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enqueue_throughput_is_measured_against_a_non_empty_queue() {
    let Some(db) = bench_db_or_skip().await else {
        return;
    };

    let backlog = headline_scenario().backlog;
    let writers = 8;
    let rows_per_writer = 50;
    let report = db::run_enqueue_scenario(&db, backlog, writers, rows_per_writer).await;

    eprintln!(
        "enqueue backlog={backlog} writers={writers} :: {} :: {:.0} rows/s",
        render(report.stats),
        report.rows_per_sec(),
    );

    // Same soundness contract as the claim gate: a run that stopped on the
    // wall-clock ceiling has percentiles over a partial window, so say so
    // rather than publishing them.
    assert!(
        !report.truncated,
        "measurement unsound: the enqueue scenario hit its {}s wall-clock budget \
         and stopped early after {} rows. Raise {} for a one-off, but a write \
         path that cannot finish in the default budget is itself the signal.",
        scenario_time_budget().as_secs(),
        report.rows,
        SCENARIO_BUDGET_ENV_VAR,
    );

    // And the backstop the claim gate learned the hard way: `truncated` is only
    // set where the harness *checks* the deadline, so it can never catch an
    // await that never returns to a check. Assert the ceiling directly, so a
    // future unbounded await fails here instead of hanging CI.
    let ceiling = scenario_time_budget() + WALL_CLOCK_SLACK;
    assert!(
        report.wall_secs <= ceiling.as_secs_f64(),
        "enqueue scenario ran {:.1}s, past its {}s wall-clock budget (+{}s slack): \
         some await on the write path is not bounded by the scenario deadline.",
        report.wall_secs,
        scenario_time_budget().as_secs(),
        WALL_CLOCK_SLACK.as_secs(),
    );

    assert_eq!(
        report.rows,
        writers * rows_per_writer,
        "every enqueue must be measured",
    );
    assert!(
        report.rows_per_sec() > 0.0,
        "enqueue throughput must be positive",
    );
    assert!(
        report.stats.p99_ms > 0.0,
        "enqueue p99 must be a real measurement",
    );

    // The point of the scenario is in its name: these writes went into a queue
    // that already had `backlog` rows in it, because a start-storm hits a live
    // queue, not an empty one. Without this the harness could silently stop
    // seeding and the test would still pass while measuring the easy case.
    let mut conn =
        with_verify_deadline("connect", scenario_time_budget(), db::connect(&db.url)).await;
    let pending = with_verify_deadline(
        "non-empty queue check",
        scenario_time_budget(),
        db::pending_count(&mut conn),
    )
    .await;
    let expected = i64::try_from(backlog + report.rows).expect("row count fits i64");
    assert_eq!(
        pending, expected,
        "enqueue must have written into a non-empty queue: expected the seeded \
         {backlog}-row backlog plus {} enqueued rows, found {pending} PENDING",
        report.rows,
    );
}

/// The enqueue path honours the scenario wall-clock ceiling.
///
/// Both awaits on that path — the pool checkout and `queue::enqueue` itself —
/// are unbounded, so without a deadline a stalled server hangs the benchmark
/// (and the gate above) until the outer CI workflow timeout, while
/// `docs/performance.md` advertises a per-scenario cap. The claim path learned
/// this two rounds earlier; this pins the same contract for writes.
///
/// Deterministic rather than timing-dependent: it asks for far more rows than
/// the budget can absorb, so the ceiling *must* bite. Without the deadline the
/// run writes all of them — minutes of work — which is the bug.
///
/// Existing-server mode only. The budget is *injected* rather than set in the
/// environment: `HARVEST_BENCH_SCENARIO_SECS` is process-global and `cargo
/// test` runs a binary's tests in parallel by default, so setting it here would
/// hand every sibling scenario a one-second ceiling they never asked for and
/// would report as truncated. The CI manifest happens to pass
/// `--test-threads=1`, but a plain local `cargo test ... claim_budget_tests`
/// does not, and a test that only holds under a particular harness flag is a
/// trap. Injection removes the shared mutable state instead of scheduling
/// around it — there is no window to leave open, on panic or otherwise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enqueue_stops_at_the_scenario_wall_clock_ceiling() {
    let Some(db) = bench_db_or_skip().await else {
        return;
    };

    // The floor `scenario_time_budget` allows. The measured phase starts after
    // seeding, so this bounds only the writes.
    let budget = std::time::Duration::from_secs(1);

    // Far more rows than 1s of writing can absorb at the measured ~1-3 ms each.
    let started = std::time::Instant::now();
    let report = db::run_enqueue_scenario_with_budget(&db, 100, 4, 250_000, budget).await;
    let elapsed = started.elapsed();

    assert!(
        report.truncated,
        "the enqueue scenario wrote all {} requested rows inside a {}s budget, \
         so either the deadline is not being enforced or the row count is no \
         longer large enough to bite",
        report.rows,
        budget.as_secs(),
    );

    // The ceiling is only meaningful if the run actually *stopped*. `truncated`
    // alone would be satisfied by a run that set the flag and kept writing.
    let ceiling = budget + WALL_CLOCK_SLACK;
    assert!(
        elapsed <= ceiling,
        "enqueue ran {:.1}s past a {}s budget (+{}s slack): the deadline is set \
         but some await is not bounded by it",
        elapsed.as_secs_f64(),
        budget.as_secs(),
        WALL_CLOCK_SLACK.as_secs(),
    );

    // A truncated run still reports what it managed, so the caller can say how
    // far it got rather than only that it stopped.
    assert!(
        report.rows > 0,
        "a truncated run must still report the rows it wrote",
    );
}

/// The injected budget bounds the measured writes only — never setup.
///
/// The `*_with_budget` runners exist so a test can inject a tiny ceiling and
/// assert the measured phase stops at it. Passing that same ceiling to
/// `connect_and_seed` made the test depend on how fast the server connects,
/// truncates, seeds and `ANALYZE`s: on a loaded runner or a remote server the
/// one second above is not enough for setup, and the run panics with
/// "benchmark setup stalled" before a single write is measured — the opposite
/// of the determinism the test above is written for.
///
/// A millisecond is unarguably too short for connect + seed, so this fails
/// outright against the previous behaviour rather than only on a slow machine.
/// It asserts the *measured* phase still stops, so a fix that simply removed
/// the deadline could not satisfy it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn injected_budget_bounds_the_measured_phase_not_setup() {
    let Some(db) = bench_db_or_skip().await else {
        return;
    };

    let report = db::run_enqueue_scenario_with_budget(
        &db,
        100,
        2,
        50_000,
        std::time::Duration::from_millis(1),
    )
    .await;

    assert!(
        report.truncated,
        "a 1ms measured-phase budget must truncate the writes",
    );
}

/// Every accreted gate must still be *measurable* — a scenario that silently
/// stops exercising its predicate (because a seeding column was renamed, say)
/// would leave the per-gate cost table in `docs/performance.md` lying.
///
/// Runs each gate at a small backlog and asserts it produced real claims.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_accreted_gate_scenario_still_claims_tasks() {
    let Some(db) = bench_db_or_skip().await else {
        return;
    };

    for gate in ClaimGate::all() {
        let scenario = Scenario {
            backlog: 1_000,
            claimers: 4,
            queues: 2,
            gate,
        };
        let report = db::run_claim_scenario(&db, scenario).await;
        eprintln!(
            "gate[{}] :: {} :: claimed={} empty={} seeded={} claimable={}",
            gate.as_str(),
            render(report.stats),
            report.claimed,
            report.empty,
            report.seed.seeded_rows,
            report.seed.claimable_rows,
        );
        assert!(
            report.claim_ratio() >= MIN_CLAIM_RATIO,
            "gate scenario `{}` claimed only {:.0}% of its operations — the \
             predicate is no longer being exercised against real work, so its \
             published cost is meaningless",
            gate.as_str(),
            report.claim_ratio() * 100.0,
        );
        assert!(
            report.claimed > 0,
            "gate scenario `{}` claimed nothing",
            gate.as_str(),
        );
    }
}

/// Each gate scenario must actually put its predicate on the execution path.
///
/// [`every_accreted_gate_scenario_still_claims_tasks`] proves the rows are
/// claimable, but that assertion cannot detect a scenario that stopped setting
/// its trigger column: such a run still claims 100% of its operations — the
/// claim just takes the cheap `IS NULL` leg. The published per-gate cost table
/// would then be reporting the baseline under six different names.
///
/// This test censuses the seeded rows and asserts each gate sets what it claims
/// to, and — just as importantly — that it does *not* set the others, so a
/// "seed everything always" regression is caught too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_gate_scenario_actually_seeds_its_trigger_column() {
    let Some(db) = bench_db_or_skip().await else {
        return;
    };

    let backlog = 200;
    let mut conn =
        with_verify_deadline("connect", scenario_time_budget(), db::connect(&db.url)).await;

    for gate in ClaimGate::all() {
        let scenario = Scenario {
            backlog,
            claimers: 1,
            queues: 2,
            gate,
        };
        with_verify_deadline(
            "seed",
            scenario_time_budget(),
            db::seed(&mut conn, scenario),
        )
        .await;
        let census = with_verify_deadline(
            "seed census",
            scenario_time_budget(),
            db::seed_census(&mut conn),
        )
        .await;
        let want = i64::try_from(backlog).expect("backlog fits i64");

        let expect_build = matches!(gate, ClaimGate::BuildPolicy | ClaimGate::AllGates);
        let expect_conc = matches!(gate, ClaimGate::ConcurrencyKey | ClaimGate::AllGates);
        // The circuit-breaker scenario deliberately seeds rate-limit keys: its
        // whole point is that a tracked activity SHORT-CIRCUITS the rate-limit
        // EXISTS via `= ANY($5)`. Without the keys present there would be
        // nothing for the tracked set to short-circuit past.
        let expect_rl = matches!(
            gate,
            ClaimGate::RateLimited | ClaimGate::CircuitBreakerSet | ClaimGate::AllGates
        );
        let expect_paused = matches!(gate, ClaimGate::PausedRows | ClaimGate::AllGates);
        // The equal-depth control seeds twice the claimable rows and nothing
        // else; every other gate seeds exactly one backlog of claimable rows.
        let expect_claimable = if gate == ClaimGate::DoubleBacklog {
            want * 2
        } else {
            want
        };

        let name = gate.as_str();
        assert_eq!(
            census.claimable_rows, expect_claimable,
            "gate `{name}`: claimable-row census wrong ({census:?})",
        );
        assert_eq!(
            census.with_build_id,
            if expect_build { want } else { 0 },
            "gate `{name}`: required_build_id column census wrong ({census:?})",
        );
        assert_eq!(
            census.with_concurrency_key,
            if expect_conc { want } else { 0 },
            "gate `{name}`: concurrency_key column census wrong ({census:?})",
        );
        assert_eq!(
            census.with_rate_limit_key,
            if expect_rl { want } else { 0 },
            "gate `{name}`: rate_limit_key column census wrong ({census:?})",
        );
        assert_eq!(
            census.paused_ballast,
            if expect_paused { want } else { 0 },
            "gate `{name}`: PAUSED ballast census wrong ({census:?})",
        );

        // A build-routed claim only reaches the expensive `EXISTS` branch when a
        // compat declaration exists; without it the worker's build would have to
        // match by equality and the scenario would measure the cheap leg.
        assert_eq!(
            census.build_compat_rows,
            i64::from(expect_build),
            "gate `{name}`: build-compat declaration census wrong ({census:?})",
        );
    }
}

/// `double_backlog` is only a valid control if it is genuinely equal-depth.
///
/// The `paused_rows` attribution in `docs/performance.md` is reported against
/// `double_backlog` rather than `baseline` specifically because claim latency is
/// superlinear in table depth: comparing a 2x-deep scenario against a 1x-deep
/// one charges the anti-join for the extra rows as well as for itself. That
/// correction is worthless unless the two scenarios really do seed the same
/// number of pending rows, which is what this asserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_paused_rows_control_seeds_equal_total_depth() {
    let Some(db) = bench_db_or_skip().await else {
        return;
    };

    let backlog = 200;
    let mut conn =
        with_verify_deadline("connect", scenario_time_budget(), db::connect(&db.url)).await;
    let mut totals = Vec::new();
    for gate in [ClaimGate::DoubleBacklog, ClaimGate::PausedRows] {
        let outcome = with_verify_deadline(
            "seed",
            scenario_time_budget(),
            db::seed(
                &mut conn,
                Scenario {
                    backlog,
                    claimers: 1,
                    queues: 2,
                    gate,
                },
            ),
        )
        .await;
        let pending = with_verify_deadline(
            "pending count",
            scenario_time_budget(),
            db::pending_count(&mut conn),
        )
        .await;
        assert_eq!(
            pending,
            i64::try_from(outcome.seeded_rows).expect("fits i64"),
            "gate `{}`: seeded_rows disagrees with the rows actually in the table",
            gate.as_str(),
        );
        totals.push((gate, outcome.seeded_rows, outcome.claimable_rows));
    }

    let (_, double_total, double_claimable) = totals[0];
    let (_, paused_total, paused_claimable) = totals[1];
    assert_eq!(
        double_total, paused_total,
        "control is not equal-depth: double_backlog seeded {double_total} rows, \
         paused_rows seeded {paused_total}. The per-gate delta for paused_rows \
         would be measuring table depth, not the anti-join.",
    );
    assert_eq!(
        double_total,
        backlog * 2,
        "both scenarios should seed 2x the backlog",
    );
    // And the thing that differs must actually differ: the control's rows are
    // all claimable, the paused scenario's are half blocked.
    assert_eq!(double_claimable, backlog * 2);
    assert_eq!(paused_claimable, backlog);
}

/// The gate and the benchmark must agree on what "the headline scenario" is.
///
/// Needs no database, but this file is `#[cfg(feature = "db")]` as a whole, so
/// it runs wherever the `db` feature does. The OS-independent copies of this
/// contract live in `claim_bench_support::pure_tests`.
#[test]
fn headline_scenario_matches_the_published_contract() {
    let s = headline_scenario();
    assert_eq!(
        (s.backlog, s.claimers, s.queues),
        (10_000, 8, 4),
        "docs/performance.md publishes 10k pending / 8 claimers / 4 queues",
    );
    // ...and the measurement must not drain it.
    assert!(measured_claims_for(s.backlog) * 5 <= s.backlog);
}

/// A benchmark database is defended by a live backend, not just by its name.
///
/// The stale-database sweep reclaims anything with no server-visible
/// connections. Between scenarios a run holds none of its own — each scenario
/// opens a connection, seeds, drops it, builds a pool, drops that — so without
/// a retained lease a concurrent run on another host sees zero backends and
/// `DROP DATABASE ... WITH (FORCE)`s a live working set out from under it.
///
/// Scoped to the existing-server mode on purpose: that is the only mode where
/// a foreign client can reach the database at all. On the testcontainer path
/// the server is private to this process, so there is nothing to defend
/// against and the invariant does not apply.
#[tokio::test]
async fn an_idle_bench_database_still_holds_a_visible_lease() {
    use diesel::QueryableByName;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    #[derive(QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    async fn count(admin: &mut AsyncPgConnection, datname: &str) -> i64 {
        diesel::sql_query("SELECT count(*) AS n FROM pg_stat_activity WHERE datname = $1")
            .bind::<diesel::sql_types::Text, _>(datname.to_string())
            .get_result::<CountRow>(admin)
            .await
            .expect("pg_stat_activity is always readable")
            .n
    }

    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!(
            "SKIP an_idle_bench_database_still_holds_a_visible_lease: \
             existing-server mode only (HARVEST_TEST_DATABASE_URL unset)"
        );
        return;
    };
    let Ok(bench) = db::setup_bench_db().await else {
        eprintln!("SKIP an_idle_bench_database_still_holds_a_visible_lease: no database");
        return;
    };

    // `with_db_name` put the database in the path; read it back out with the
    // parser that is its inverse. A naive `rsplit('/')` here would reintroduce
    // the exact bug `with_db_name` exists to avoid: against an admin URL like
    // `?sslrootcert=/etc/ssl/root.crt` the last slash sits inside the query, so
    // the name would come back as `root.crt` and this test would fail against a
    // perfectly healthy lease.
    let datname = db_name_from_url(&bench.url).expect("bench url always carries a database path");

    let Ok(mut admin) = AsyncPgConnection::establish(&admin_url).await else {
        eprintln!("SKIP an_idle_bench_database_still_holds_a_visible_lease: admin connect failed");
        return;
    };
    // No scenario is running, and this test holds no connection of its own to
    // the benchmark database — so any backend here is the lease.
    let idle = count(&mut admin, &datname).await;
    assert!(
        idle > 0,
        "an idle bench database reported {idle} backends: the sweep would treat \
         a live run as abandoned and drop it"
    );

    // The lease must also *end*, or a crashed run's database could never be
    // reclaimed and the sweep would be permanently defeated.
    drop(bench);
    for _ in 0..40 {
        if count(&mut admin, &datname).await == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("lease survived BenchDb drop: the stale sweep can never reclaim {datname}");
}

/// The lease survives a server that reaps idle sessions.
///
/// The lease defends a running benchmark by *being* an idle backend, which is
/// exactly what Postgres 14's `idle_session_timeout` kills — and setting it is
/// an entirely reasonable thing to do on the shared cluster existing-server mode
/// is designed for. Reaped mid-run, the lease leaves `BenchDb` holding a dead
/// handle while a concurrent sweep sees zero backends, concludes the database is
/// abandoned, and drops it. Both sides fail silently: nothing tells the victim,
/// and the sweeper is reasoning correctly from the evidence it has.
///
/// Deliberately session-scoped. The realistic trigger is a server- or role-level
/// default, but setting one here would be shared mutable state on a server other
/// tests are using concurrently — the same hazard as writing the scenario budget
/// into the environment. A session-local `SET` reproduces exactly what the lease
/// inherits, and reaches nothing else.
///
/// Falsifiable in both directions: an unarmed session under the same timeout must
/// actually die, or the armed one proves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_lease_survives_a_server_that_reaps_idle_sessions() {
    use diesel::QueryableByName;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    /// How long a session may sit idle before the reaper takes it. Paired with
    /// the idle wait below: that wait must comfortably exceed this, or the
    /// control session survives for timing reasons and proves nothing.
    const REAP_AFTER: &str = "2s";

    #[derive(QueryableByName)]
    struct PidRow {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        pid: i32,
    }
    #[derive(QueryableByName)]
    struct SettingRow {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        value: Option<String>,
    }
    #[derive(QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    async fn pid_of(conn: &mut AsyncPgConnection) -> i32 {
        diesel::sql_query("SELECT pg_backend_pid() AS pid")
            .get_result::<PidRow>(conn)
            .await
            .expect("pg_backend_pid always answers")
            .pid
    }
    async fn alive(observer: &mut AsyncPgConnection, pid: i32) -> bool {
        diesel::sql_query("SELECT count(*) AS n FROM pg_stat_activity WHERE pid = $1")
            .bind::<diesel::sql_types::Integer, _>(pid)
            .get_result::<CountRow>(observer)
            .await
            .expect("pg_stat_activity is always readable")
            .n
            > 0
    }

    let Some(bench) = bench_db_or_skip().await else {
        return;
    };
    let Ok(mut observer) = AsyncPgConnection::establish(&bench.url).await else {
        eprintln!("SKIP the_lease_survives_a_server_that_reaps_idle_sessions: observer connect");
        return;
    };

    // PG 12/13 have no idle reaper, so there is nothing to defend against and
    // the premise of the test does not exist. `missing_ok := true` yields NULL
    // for an unknown GUC, which is how we tell "too old" from "disabled".
    let known = diesel::sql_query("SELECT current_setting('idle_session_timeout', true) AS value")
        .get_result::<SettingRow>(&mut observer)
        .await
        .ok()
        .and_then(|r| r.value)
        .is_some();
    if !known {
        eprintln!(
            "SKIP the_lease_survives_a_server_that_reaps_idle_sessions: \
             server predates idle_session_timeout (PG 14+)"
        );
        return;
    }

    let idle_for = std::time::Duration::from_secs(6);

    // Control: the reaper is real and does kill an unarmed idle session.
    let Ok(mut unarmed) = AsyncPgConnection::establish(&bench.url).await else {
        eprintln!("SKIP the_lease_survives_a_server_that_reaps_idle_sessions: control connect");
        return;
    };
    diesel::sql_query(format!("SET idle_session_timeout = '{REAP_AFTER}'"))
        .execute(&mut unarmed)
        .await
        .expect("idle_session_timeout is USERSET on PG 14+");
    let unarmed_pid = pid_of(&mut unarmed).await;

    // The lease: same inherited timeout, then armed the way setup arms it.
    let Ok(mut armed) = AsyncPgConnection::establish(&bench.url).await else {
        eprintln!("SKIP the_lease_survives_a_server_that_reaps_idle_sessions: lease connect");
        return;
    };
    diesel::sql_query(format!("SET idle_session_timeout = '{REAP_AFTER}'"))
        .execute(&mut armed)
        .await
        .expect("idle_session_timeout is USERSET on PG 14+");
    db::arm_lease_session(&mut armed).await;
    let armed_pid = pid_of(&mut armed).await;

    // Both sessions now sit idle, which is the only state the reaper acts on.
    tokio::time::sleep(idle_for).await;

    assert!(
        !alive(&mut observer, unarmed_pid).await,
        "the control session survived a {REAP_AFTER} idle timeout after {}s idle: \
         the reaper is not actually firing, so this test cannot prove the lease \
         is defended against it",
        idle_for.as_secs(),
    );
    assert!(
        alive(&mut observer, armed_pid).await,
        "the armed lease was reaped after {}s idle despite a {REAP_AFTER} timeout \
         being disabled on its session. A reaped lease makes a live run look \
         abandoned, so a concurrent sweep would drop this run's database.",
        idle_for.as_secs(),
    );

    // A backend in `pg_stat_activity` is what other clients see; that the
    // handle still works is what *this* run depends on.
    assert_eq!(
        pid_of(&mut armed).await,
        armed_pid,
        "the armed lease is still listed but no longer usable from this side",
    );
}

/// A process reclaims its **own** finished databases, not just other runs'.
///
/// `run_token()` is constant for the life of a process, so an ownership-keyed
/// self-exemption in the sweep meant a process could never clean up after
/// itself: every [`db::setup_bench_db`] call left one migrated database — up to
/// 100k rows — behind until some *other* process happened to sweep. This suite
/// alone calls setup once per test, and the benchmark calls it a dozen times,
/// so the leak was per-run and cumulative rather than exotic. Measured before
/// the fix: one run of this suite left eight databases; after it, one.
///
/// The lease is what makes dropping the exemption safe, and this test pins both
/// halves of that: the first database is reclaimed only *after* its `BenchDb` is
/// dropped, so a still-live prior database is protected by its own backend
/// rather than by its name.
///
/// Existing-server mode only, matching its siblings — the testcontainer path
/// gets a private server per process and has nothing to sweep.
#[tokio::test]
async fn a_later_setup_reclaims_this_processes_finished_databases() {
    use diesel::QueryableByName;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    #[derive(QueryableByName)]
    struct ExistsRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    async fn exists(admin: &mut AsyncPgConnection, datname: &str) -> bool {
        diesel::sql_query("SELECT count(*) AS n FROM pg_database WHERE datname = $1")
            .bind::<diesel::sql_types::Text, _>(datname.to_string())
            .get_result::<ExistsRow>(admin)
            .await
            .expect("pg_database is always readable")
            .n
            > 0
    }

    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!(
            "SKIP a_later_setup_reclaims_this_processes_finished_databases: \
             existing-server mode only (HARVEST_TEST_DATABASE_URL unset)"
        );
        return;
    };
    let Ok(mut admin) = AsyncPgConnection::establish(&admin_url).await else {
        eprintln!("SKIP a_later_setup_reclaims_this_processes_finished_databases: admin connect");
        return;
    };

    let Ok(first) = db::setup_bench_db().await else {
        eprintln!("SKIP a_later_setup_reclaims_this_processes_finished_databases: no database");
        return;
    };
    let first_name =
        db_name_from_url(&first.url).expect("bench url always carries a database path");

    // While the first is still held, a second setup must leave it alone: its
    // lease says "in use", and that is the only thing that should matter.
    let Ok(second) = db::setup_bench_db().await else {
        eprintln!("SKIP a_later_setup_reclaims_this_processes_finished_databases: no database");
        return;
    };
    assert!(
        exists(&mut admin, &first_name).await,
        "a live database was reclaimed by its own process: the lease must protect \
         {first_name} while its BenchDb is held"
    );
    drop(second);

    // Now release it and sweep again. Same process, same run token — so before
    // the fix this is exactly the case that leaked.
    drop(first);
    let Ok(third) = db::setup_bench_db().await else {
        eprintln!("SKIP a_later_setup_reclaims_this_processes_finished_databases: no database");
        return;
    };
    assert!(
        !exists(&mut admin, &first_name).await,
        "{first_name} survived a later setup in the same process: a run cannot \
         clean up after itself, so every setup call leaks a migrated database"
    );
    drop(third);
}

/// A database that merely *shares our prefix* is never dropped.
///
/// The sweep is the only destructive thing this harness does to a server it
/// does not own, so what counts as "ours" has to be the full minted shape —
/// `harvest_claim_bench_{pid}_{16 hex}_{seq}` — and not a prefix plus a
/// plausible field or two. Checking only a numeric first field and the presence
/// of a second meant a name like `harvest_claim_bench_123_production` parsed as
/// pid `123`, token `production`, and an idle database nobody here minted
/// reached `pg_terminate_backend` and `DROP DATABASE`.
///
/// The decoy below is deliberately in that vulnerable class: prefixed, numeric
/// second field, and a third field that is *not* a run token.
///
/// # Why this is written so defensively
///
/// The decoy has to sit **inside** the swept `harvest_claim_bench_%` prefix or
/// it would not be a candidate and the test would prove nothing. That is also
/// what makes it dangerous: once the fix lands, no sweep will ever reclaim it,
/// so a panic between `CREATE DATABASE` and its `DROP` strands a database on a
/// shared server permanently. So nothing here panics in that span — failures
/// are collected into a value, cleanup runs unconditionally, and every
/// assertion happens after it. (An earlier probe in this file learned that the
/// hard way and left two orphans behind.)
///
/// Existing-server mode only — the testcontainer path sweeps a private server.
#[tokio::test]
async fn a_foreign_database_sharing_our_prefix_is_never_dropped() {
    use diesel::QueryableByName;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    #[derive(QueryableByName)]
    struct ExistsRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    async fn exists(admin: &mut AsyncPgConnection, datname: &str) -> Result<bool, String> {
        diesel::sql_query("SELECT count(*) AS n FROM pg_database WHERE datname = $1")
            .bind::<diesel::sql_types::Text, _>(datname.to_string())
            .get_result::<ExistsRow>(admin)
            .await
            .map(|r| r.n > 0)
            .map_err(|e| format!("read pg_database: {e}"))
    }

    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!(
            "SKIP a_foreign_database_sharing_our_prefix_is_never_dropped: \
             existing-server mode only (HARVEST_TEST_DATABASE_URL unset)"
        );
        return;
    };
    let Ok(mut admin) = AsyncPgConnection::establish(&admin_url).await else {
        eprintln!("SKIP a_foreign_database_sharing_our_prefix_is_never_dropped: admin connect");
        return;
    };

    // Prefixed and numeric-then-arbitrary, so the old two-field check accepted
    // it, yet never a name this harness could mint. Named unmistakably after
    // this probe so it cannot collide with a database an operator cares about,
    // and carrying the run token because a pid alone is not unique across hosts
    // — the drop below is unconditional, so two pid-1 runs would delete each
    // other's live decoy. See `sweep_probe_decoy_name` for both properties.
    let decoy = sweep_probe_decoy_name(std::process::id(), db::run_token());

    // A previous crashed run of this test could have stranded one.
    let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {decoy}"))
        .execute(&mut admin)
        .await;
    if let Err(e) = diesel::sql_query(format!("CREATE DATABASE {decoy}"))
        .execute(&mut admin)
        .await
    {
        eprintln!("SKIP a_foreign_database_sharing_our_prefix_is_never_dropped: create decoy: {e}");
        return;
    }

    // ---- nothing from here to `DROP DATABASE` may panic ----

    // A setup sweeps before it creates. The decoy is idle, so the server check
    // will say "no connections" — the name check is the only thing left.
    let outcome = match db::setup_bench_db().await {
        Ok(bench) => {
            let survived = exists(&mut admin, &decoy).await;
            drop(bench);
            survived
        }
        Err(e) => Err(format!("SKIP: no database: {e:?}")),
    };

    let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {decoy}"))
        .execute(&mut admin)
        .await;

    // ---- safe to panic again ----

    match outcome {
        Ok(survived) => assert!(
            survived,
            "{decoy} was dropped by the stale sweep: a database this harness \
             never minted, sharing only its name prefix, was terminated and \
             destroyed on a server the benchmark does not own",
        ),
        Err(why) => eprintln!("SKIP a_foreign_database_sharing_our_prefix_is_never_dropped: {why}"),
    }
}

/// Setup serializes against a foreign sweep, so the create-to-lease window is
/// not a hole.
///
/// A database is only defended once it has a backend in `pg_stat_activity`, and
/// it cannot have one until it exists. Between `CREATE DATABASE` and the lease
/// connection, a sweep running on another host sees a real database with zero
/// connections and correctly concludes it is abandoned — no naming scheme fixes
/// that. Setup therefore holds an advisory lock across the whole span, which is
/// the same lock every sweep must take.
///
/// This proves the mechanism rather than trying to hit the window: hold the
/// lock externally and assert setup blocks, then release it and assert setup
/// completes. If the lock is ever dropped from the setup path, setup stops
/// blocking and this fails.
///
/// The holder connects to `db::SWEEP_LOCK_DB`, not to the admin URL's own
/// database, because advisory locks are database-scoped: taking it anywhere
/// else would only conflict when the two happen to coincide.
///
/// Existing-server mode only — the testcontainer path has no foreign clients.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setup_waits_for_a_peers_sweep_before_creating_its_database() {
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!(
            "SKIP setup_waits_for_a_peers_sweep_before_creating_its_database: \
             existing-server mode only (HARVEST_TEST_DATABASE_URL unset)"
        );
        return;
    };
    let lock_url = with_db_name(&admin_url, db::SWEEP_LOCK_DB);
    let Ok(mut holder) = AsyncPgConnection::establish(&lock_url).await else {
        eprintln!("SKIP setup_waits_for_a_peers_sweep_before_creating_its_database: no server");
        return;
    };

    // Stand in for a peer that is mid-sweep.
    diesel::sql_query("SELECT pg_advisory_lock($1)")
        .bind::<diesel::sql_types::BigInt, _>(db::SWEEP_LOCK_KEY)
        .execute(&mut holder)
        .await
        .expect("taking the sweep lock must succeed");

    let mut setup = tokio::spawn(async { db::setup_bench_db().await });

    // Setup must not get past the lock. Generous enough not to flake on a
    // loaded runner, short enough to stay cheap.
    let blocked = tokio::time::timeout(std::time::Duration::from_millis(1_500), &mut setup).await;
    assert!(
        blocked.is_err(),
        "setup completed while a peer held the sweep lock: the create-to-lease \
         window is unprotected and a foreign sweep can drop the database out \
         from under a live run",
    );

    // Release, and it should now finish.
    diesel::sql_query("SELECT pg_advisory_unlock($1)")
        .bind::<diesel::sql_types::BigInt, _>(db::SWEEP_LOCK_KEY)
        .execute(&mut holder)
        .await
        .expect("releasing the sweep lock must succeed");

    let finished = tokio::time::timeout(std::time::Duration::from_secs(120), setup)
        .await
        .expect("setup must proceed once the sweep lock is free")
        .expect("setup task panicked");
    assert!(
        finished.is_ok(),
        "setup failed after the lock was released: {:?}",
        finished.err().map(|e| e.0),
    );
}

/// The sweep lock is taken in a fixed database, not in whichever one the admin
/// URL happens to name.
///
/// Postgres advisory locks are scoped to the session's database, not to the
/// cluster: two sessions on different databases hold the same key at the same
/// time without conflicting. If the lock rode the admin connection, two runs
/// reaching the same server through different admin databases would not
/// serialize at all — each would sweep while the other sat in its
/// create-to-lease window, which is exactly the hole the lock exists to close.
///
/// Falsifiable independently of how this suite is configured: the holder sits
/// in `SWEEP_LOCK_DB` while the lock is requested through an admin URL naming a
/// *different* database. Locking the passed URL's own database — the bug —
/// finds no conflict and returns immediately.
///
/// Existing-server mode only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_sweep_lock_is_taken_in_a_fixed_database_not_the_admin_url_s() {
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!(
            "SKIP the_sweep_lock_is_taken_in_a_fixed_database_not_the_admin_url_s: \
             existing-server mode only (HARVEST_TEST_DATABASE_URL unset)"
        );
        return;
    };
    let Ok(mut admin) = AsyncPgConnection::establish(&admin_url).await else {
        eprintln!(
            "SKIP the_sweep_lock_is_taken_in_a_fixed_database_not_the_admin_url_s: no server"
        );
        return;
    };

    // A scratch database standing in for an operator whose admin URL points
    // somewhere other than `SWEEP_LOCK_DB`. Named outside the bench prefix so
    // the sweep never touches it, and carrying the run token: the drop below is
    // unconditional, so a pid-only name lets two runs sharing a cluster from
    // different PID namespaces — both pid 1 under containers — delete each
    // other's live scratch mid-probe.
    //
    // The token means a strand is never reclaimed by a later same-pid run, but
    // that was never a reliable cleanup: what actually prevents strands is that
    // nothing between the create and the drop may panic (see below). Trading a
    // coincidental cleanup for the removal of a destructive collision is the
    // right way round, and both probe names are unmistakable to an operator.
    let scratch = format!(
        "harvest_sweep_scope_probe_{}_{}",
        std::process::id(),
        db::run_token(),
    );
    let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {scratch}"))
        .execute(&mut admin)
        .await;
    if diesel::sql_query(format!("CREATE DATABASE {scratch}"))
        .execute(&mut admin)
        .await
        .is_err()
    {
        eprintln!(
            "SKIP the_sweep_lock_is_taken_in_a_fixed_database_not_the_admin_url_s: \
             cannot create a scratch database"
        );
        return;
    }
    // Nothing between the create above and the drop below may panic: `scratch`
    // sits outside the swept `harvest_claim_bench_%` prefix precisely so a
    // concurrent sweep cannot delete it mid-probe, which also means nothing
    // would ever reclaim it. So the probe reports failures instead of
    // `expect`ing them, and the assertions run after cleanup.
    let outcome = probe_sweep_lock_scope(&with_db_name(&admin_url, &scratch), &admin_url).await;

    let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {scratch}"))
        .execute(&mut admin)
        .await;

    match outcome {
        Ok(timed_out) => assert!(
            timed_out,
            "taking the sweep lock through an admin URL naming `{scratch}` did \
             not block while a peer held it in `{}`: the lock is being taken in \
             the admin URL's own database, so runs reaching one cluster through \
             different admin databases never serialize",
            db::SWEEP_LOCK_DB,
        ),
        Err(e) => panic!("sweep-lock scope probe could not run: {e}"),
    }
}

/// Hold the sweep lock in [`db::SWEEP_LOCK_DB`] and report whether requesting
/// it through `probe_admin_url` blocks.
///
/// Split out so the caller can drop its scratch database before asserting: a
/// panic in here would strand that database on a shared server forever.
/// Returns `Ok(true)` when the request blocked (the contract holds).
async fn probe_sweep_lock_scope(probe_admin_url: &str, admin_url: &str) -> Result<bool, String> {
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    // The peer, holding the lock where the harness agrees it lives.
    let mut holder = AsyncPgConnection::establish(&with_db_name(admin_url, db::SWEEP_LOCK_DB))
        .await
        .map_err(|e| format!("connect {}: {e}", db::SWEEP_LOCK_DB))?;
    diesel::sql_query("SELECT pg_advisory_lock($1)")
        .bind::<diesel::sql_types::BigInt, _>(db::SWEEP_LOCK_KEY)
        .execute(&mut holder)
        .await
        .map_err(|e| format!("take the sweep lock: {e}"))?;

    let probe_url = probe_admin_url.to_string();
    let mut taking = tokio::spawn(async move { db::take_sweep_lock(&probe_url).await });
    // A timeout `Err` means still waiting on the lock, which is the pass. `Ok`
    // means it returned early — keep that join result rather than polling the
    // handle again below, which would panic and bury the real diagnosis.
    let finished_early = tokio::time::timeout(std::time::Duration::from_millis(1_500), &mut taking)
        .await
        .ok();
    let timed_out = finished_early.is_none();

    // Release before returning either way, so a failure cannot strand the lock
    // for the rest of the suite.
    let unlocked = diesel::sql_query("SELECT pg_advisory_unlock($1)")
        .bind::<diesel::sql_types::BigInt, _>(db::SWEEP_LOCK_KEY)
        .execute(&mut holder)
        .await;
    let acquired = match finished_early {
        Some(joined) => Some(joined),
        None => tokio::time::timeout(std::time::Duration::from_secs(60), taking)
            .await
            .ok(),
    };
    if let Some(Ok(Ok(lock))) = acquired {
        db::release_sweep_lock(lock).await;
    }
    drop(holder);
    unlocked.map_err(|e| format!("release the sweep lock: {e}"))?;
    Ok(timed_out)
}

/// A `dbname` query parameter must not outrank the path `with_db_name` sets.
///
/// `tokio-postgres` applies the path first and the query second, assigning the
/// same `Config::dbname`, so an admin URL carrying `?dbname=...` silently wins.
/// Every connection this harness derives — the sweep lock, the lease, the
/// migration connection — is built by `with_db_name`, and the benchmark issues
/// `TRUNCATE` through them. The consequence of getting this wrong is that a
/// benchmark run migrates and truncates the operator's *admin* database.
///
/// Asserting `current_database()` is the point: this is the one claim that
/// cannot be established by reading the URL, because the whole bug is that the
/// URL reads correctly while the connection goes somewhere else.
#[tokio::test]
async fn a_dbname_query_parameter_cannot_redirect_the_connection() {
    use diesel::QueryableByName;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    #[derive(QueryableByName)]
    struct DbRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        current_database: String,
    }

    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!(
            "SKIP a_dbname_query_parameter_cannot_redirect_the_connection: \
             existing-server mode only (HARVEST_TEST_DATABASE_URL unset)"
        );
        return;
    };
    let Ok(mut admin) = AsyncPgConnection::establish(&admin_url).await else {
        eprintln!(
            "SKIP a_dbname_query_parameter_cannot_redirect_the_connection: admin connect failed"
        );
        return;
    };

    // The decoy the contaminated URL points at. Run-unique for the same reason
    // every other probe here is: a shared admin server may be hosting a
    // concurrent run with our pid.
    let decoy = format!(
        "harvest_dbname_probe_{}_{}",
        std::process::id(),
        db::run_token(),
    );
    diesel::sql_query(format!("CREATE DATABASE {decoy}"))
        .execute(&mut admin)
        .await
        .expect("create the decoy database");

    // An operator's admin URL that selects its database by query parameter —
    // valid libpq, and the shape that triggers the bug.
    let normalized = with_db_name(&admin_url, "postgres");
    let joiner = if normalized.contains('?') { '&' } else { '?' };
    let contaminated = format!("{normalized}{joiner}dbname={decoy}");

    // Exactly what the harness does to reach the sweep-lock database.
    let rewritten = with_db_name(&contaminated, "postgres");

    let landed = async {
        let mut conn = AsyncPgConnection::establish(&rewritten).await.ok()?;
        diesel::sql_query("SELECT current_database() AS current_database")
            .get_result::<DbRow>(&mut conn)
            .await
            .ok()
            .map(|r| r.current_database)
    }
    .await;

    // Clean up before asserting, so a failure does not also leak a database.
    //
    // Version-neutral rather than `WITH (FORCE)`: that clause is PostgreSQL 13+
    // and this cleanup ignores its result, so on 12 the decoy would survive
    // silently — and unlike a stale bench database it carries no
    // `BENCH_DB_PREFIX`, so the harness sweep can never reclaim it either. The
    // name is minted a few lines above, which is what makes it safe to
    // interpolate.
    db::drop_database_version_neutral(&mut admin, &decoy).await;

    let landed = landed.expect("the rewritten URL must be connectable");
    assert_eq!(
        landed, "postgres",
        "a dbname query parameter redirected the connection to {landed}: \
         the harness would migrate and TRUNCATE there, not in the database \
         the rewritten URL names",
    );
}

// ---------------------------------------------------------------------------
// Setup-deadline bound (issue #786, round 19).
//
// These need no database, but they are async: a `#[tokio::test]` in
// `claim_bench_support.rs` would reclassify that shared harness as a suite
// (`ci_run_coverage::claim_bench_support_is_classified_as_a_harness_not_a_suite`),
// and the guard's own remedy is to put the test in a file that already has a
// manifest row. This is that file.
// ---------------------------------------------------------------------------

/// A server that stops answering must not park the bench forever.
///
/// `connect` and `seed` run before the measured phase's deadline exists, so
/// nothing bounded them: a stalled server would hold the CI gate until the
/// outer workflow timeout while the page advertises a per-scenario cap. The
/// clock is paused, so a future that never resolves is decided by the
/// deadline rather than by waiting out four real minutes.
#[tokio::test(start_paused = true)]
#[should_panic(expected = "benchmark setup stalled")]
async fn setup_deadline_refuses_to_wait_out_a_stalled_server() {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(240);
    let () = with_setup_deadline("seed", deadline, std::future::pending::<()>()).await;
}

/// The bound is a ceiling, not a delay: a step that finishes passes through.
#[tokio::test(start_paused = true)]
async fn setup_deadline_returns_a_step_that_finishes_in_time() {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(240);
    let value = with_setup_deadline("connect", deadline, async { 7_u32 }).await;
    assert_eq!(value, 7, "a completed step must return its own value");
}

/// An already-elapsed deadline is a *failure*, not "no deadline".
///
/// The remaining budget clamps to zero, and a zero timeout has to trip rather
/// than be read as unbounded — the failure mode this whole change exists to
/// remove. (It does not distinguish `saturating_duration_since` from plain
/// `Instant` subtraction: both saturate to zero since Rust 1.60.)
#[tokio::test(start_paused = true)]
#[should_panic(expected = "benchmark setup stalled")]
async fn setup_deadline_in_the_past_trips_immediately() {
    let deadline = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(1))
        .expect("the process has been up for at least a second");
    let () = with_setup_deadline("connect", deadline, std::future::pending::<()>()).await;
}

/// The *post*-measurement soundness checks must not park the gate either.
///
/// `with_setup_deadline` closed the hole at the front of a scenario. The gate
/// tests then reconnect *after* the measured phase to verify the run was sound
/// (the backlog was not drained; the enqueue really did write into a non-empty
/// queue), and those awaits sat outside every deadline: the measured phase had
/// already consumed its own, so nothing bounded them. A server that stopped
/// answering between the last claim and the verification would hold the CI gate
/// until the outer workflow timeout.
#[tokio::test(start_paused = true)]
#[should_panic(expected = "post-measurement check stalled")]
async fn verify_deadline_refuses_to_wait_out_a_stalled_server() {
    let () = with_verify_deadline(
        "backlog depth check",
        std::time::Duration::from_secs(240),
        std::future::pending::<()>(),
    )
    .await;
}

/// The bound is a ceiling, not a delay: a check that finishes passes through.
#[tokio::test(start_paused = true)]
async fn verify_deadline_returns_a_check_that_finishes_in_time() {
    let value = with_verify_deadline(
        "pending count",
        std::time::Duration::from_secs(240),
        async { 11_i64 },
    )
    .await;
    assert_eq!(value, 11, "a completed check must return its own value");
}

/// The verify ceiling is *fresh*, not what the measured phase left over.
///
/// This is the whole reason it takes a `Duration` rather than the measured
/// phase's `Instant`. A scenario is allowed to consume its entire budget — that
/// is the contract — so a deadline shared with the measurement would have zero
/// left by the time verification starts, and every soundness check would "time
/// out" against a perfectly healthy server. Simulated by burning the full
/// scenario budget on the paused clock before verifying.
#[tokio::test(start_paused = true)]
async fn verify_deadline_is_not_consumed_by_the_measured_phase() {
    tokio::time::sleep(scenario_time_budget() + Duration::from_secs(60)).await;
    let value = with_verify_deadline("connect", scenario_time_budget(), async { 3_u8 }).await;
    assert_eq!(
        value, 3,
        "a fresh ceiling must survive a measured phase that used its whole budget"
    );
}

/// Provisioning must skip rather than hang when the server stops answering.
///
/// `setup_bench_db` runs before any scenario, so its awaits sit outside both the
/// measured deadline and the setup deadline `connect_and_seed` applies. Without
/// a ceiling here a stalled server parks the bench and this gate until the outer
/// workflow timeout.
#[tokio::test(start_paused = true)]
async fn provision_deadline_skips_rather_than_hanging_on_a_stalled_server() {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(240);
    let outcome: Result<(), _> =
        db::with_provision_deadline("connect", deadline, std::future::pending::<()>()).await;

    // A `SkipReason`, deliberately not a panic: failing to provision means the
    // benchmark cannot run in this environment, which is what skipping is for.
    // `connect_and_seed` panics instead, because by then provisioning succeeded
    // and a stall there is a real fault.
    let reason = outcome.expect_err("a stalled step must not resolve");
    assert!(
        reason
            .0
            .contains("provisioning the benchmark database stalled"),
        "the skip must name the phase that stalled, got: {}",
        reason.0
    );
    assert!(
        reason.0.contains("connect"),
        "the skip must name the step that stalled, got: {}",
        reason.0
    );
}

/// The ceiling is a bound, not a delay: a step that finishes passes through,
/// and its own `Result` survives the nesting.
#[tokio::test(start_paused = true)]
async fn provision_deadline_preserves_a_step_that_finishes_in_time() {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(240);
    let inner: Result<u32, String> = Ok(7);
    let outcome =
        db::with_provision_deadline("create the benchmark database", deadline, async { inner })
            .await;
    assert_eq!(
        outcome.map_err(|r| r.0),
        Ok(Ok(7)),
        "a completed step must return its own Result untouched"
    );
}

// ---------------------------------------------------------------------------
// Version-neutral database drop (issue #786, round 20).
// ---------------------------------------------------------------------------

/// Terminate-then-drop must evict a live backend, exactly as `WITH (FORCE)` did.
///
/// The clause was doing real work: the sweep drops databases a crashed run left
/// behind, and a plain `DROP DATABASE` against one that still has a backend
/// attached fails with *"is being accessed by other users"*. Every cleanup site
/// discards its result, so that failure is silent and the database leaks.
///
/// Replacing the clause is therefore only safe if the replacement handles the
/// same case — which is what this asserts, by holding a connection open across
/// the drop. Its partner
/// `no_cleanup_emits_the_postgres_13_only_force_clause` covers the half no CI
/// server can reach: that the clause stays gone on a version old enough to
/// reject it.
///
/// Provisioned through [`db::setup_bench_db`] rather than
/// `HARVEST_TEST_DATABASE_URL` so it runs in **both** modes. The checked CI
/// manifest runs this suite Docker-backed and sets no such variable, so gating
/// on it would have meant the one test proving terminate-then-drop still works
/// never ran in CI at all. Any database can issue `CREATE`/`DROP DATABASE`, so
/// the bench database serves as the admin session here.
#[tokio::test]
async fn version_neutral_drop_evicts_a_live_backend() {
    use diesel::QueryableByName;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    #[derive(QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    let Ok(bench) = db::setup_bench_db().await else {
        eprintln!("SKIP version_neutral_drop_evicts_a_live_backend: no database available");
        return;
    };
    let Ok(mut admin) = AsyncPgConnection::establish(&bench.url).await else {
        eprintln!("SKIP version_neutral_drop_evicts_a_live_backend: admin connect failed");
        return;
    };

    // Run-unique for the same reason every other probe here is: a shared admin
    // server may be hosting a concurrent run with our pid.
    let victim = format!(
        "harvest_drop_probe_{}_{}",
        std::process::id(),
        db::run_token(),
    );
    diesel::sql_query(format!("CREATE DATABASE {victim}"))
        .execute(&mut admin)
        .await
        .expect("create the victim database");

    // Hold a connection open across the drop. Kept in scope deliberately — this
    // is the condition under test, and dropping it early would make the
    // assertion pass without exercising the terminate step at all.
    let victim_url = with_db_name(&bench.url, &victim);
    let occupant = AsyncPgConnection::establish(&victim_url)
        .await
        .expect("connect to the victim database");

    // Confirm the server can see the backend, so a silently-closed connection
    // cannot make this test vacuous.
    let attached =
        diesel::sql_query("SELECT count(*) AS n FROM pg_stat_activity WHERE datname = $1")
            .bind::<diesel::sql_types::Text, _>(&victim)
            .get_result::<CountRow>(&mut admin)
            .await
            .expect("count backends attached to the victim")
            .n;
    assert!(
        attached >= 1,
        "the probe connection is not attached to {victim}, so this test would \
         pass without ever exercising the terminate step",
    );

    db::drop_database_version_neutral(&mut admin, &victim).await;

    let survivors = diesel::sql_query("SELECT count(*) AS n FROM pg_database WHERE datname = $1")
        .bind::<diesel::sql_types::Text, _>(&victim)
        .get_result::<CountRow>(&mut admin)
        .await
        .expect("look the victim up in pg_database")
        .n;

    // Only now may the occupant go, so the drop above ran against a live backend.
    drop(occupant);

    assert_eq!(
        survivors, 0,
        "{victim} survived the drop while a backend was attached: a plain \
         DROP DATABASE fails there, and cleanup sites discard the error, so the \
         database would leak on every crashed run",
    );
}

// ---------------------------------------------------------------------------
// Provisioning connects through the fixed database (issue #786, round 21).
// ---------------------------------------------------------------------------

/// The provisioning session never sits on the database the operator named.
///
/// Pure counterpart to `a_template1_backend_blocks_create_database_but_ours_does_not`
/// below: that one proves the hazard is real against a live server, this one
/// pins the decision that avoids it, cheaply and on every run.
#[test]
fn provisioning_connects_through_the_fixed_database() {
    for admin_url in [
        "postgres://u:p@h:5432/template1",
        "postgres://u:p@h:5432/some_other_admin_db",
        "host=h user=u dbname=template1",
    ] {
        let chosen = db::admin_connection_url(admin_url);
        assert_eq!(
            db_name_from_url(&chosen).as_deref(),
            Some(db::SWEEP_LOCK_DB),
            "provisioning would connect to the operator's own database for \
             {admin_url}: a client waiting for the sweep lock would hold that \
             backend, and if it is template1 the lock holder's CREATE DATABASE \
             fails",
        );
    }
}

/// A backend on `template1` blocks `CREATE DATABASE`; ours does not.
///
/// `CREATE DATABASE` copies `template1` by default and Postgres refuses while
/// any *other* session is attached to the template. The provisioning session is
/// held across the whole sweep-lock wait, so an admin URL naming `template1`
/// would turn every waiting client into a blocker for whichever client actually
/// holds the lock.
///
/// Both halves are asserted deliberately. The first shows the hazard is real
/// rather than theoretical; the second is the regression guard — it opens its
/// connection through [`db::admin_connection_url`], so reverting that to the
/// operator's URL puts this backend back on `template1` and the create fails.
///
/// Existing-server mode only: it needs an admin URL an operator could plausibly
/// have pointed at `template1`.
#[tokio::test]
async fn a_template1_backend_blocks_create_database_but_ours_does_not() {
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!(
            "SKIP a_template1_backend_blocks_create_database_but_ours_does_not: \
             existing-server mode only (HARVEST_TEST_DATABASE_URL unset)"
        );
        return;
    };
    let Ok(mut creator) = AsyncPgConnection::establish(&with_db_name(&admin_url, "postgres")).await
    else {
        eprintln!("SKIP a_template1_backend_blocks_create_database_but_ours_does_not: no server");
        return;
    };

    let probe = format!(
        "harvest_tmpl_probe_{}_{}",
        std::process::id(),
        db::run_token(),
    );

    // An admin URL an operator could plausibly hold, pointed at the template.
    let operator_url = with_db_name(&admin_url, "template1");

    // Half one: the hazard. Under the old behaviour this is where a waiting
    // client's provisioning session sat.
    {
        let Ok(_waiter) = AsyncPgConnection::establish(&operator_url).await else {
            eprintln!(
                "SKIP a_template1_backend_blocks_create_database_but_ours_does_not: \
                 template1 not connectable"
            );
            return;
        };
        let blocked = diesel::sql_query(format!("CREATE DATABASE {probe}"))
            .execute(&mut creator)
            .await;
        assert!(
            blocked.is_err(),
            "CREATE DATABASE succeeded while another session held template1, so \
             this test cannot show the hazard it exists to guard",
        );
    }

    // Half two: the regression guard. Same operator URL, but opened the way the
    // harness now opens it.
    let Ok(_ours) = AsyncPgConnection::establish(&db::admin_connection_url(&operator_url)).await
    else {
        eprintln!(
            "SKIP a_template1_backend_blocks_create_database_but_ours_does_not: \
             fixed database not connectable"
        );
        return;
    };
    let created = diesel::sql_query(format!("CREATE DATABASE {probe}"))
        .execute(&mut creator)
        .await;
    assert!(
        created.is_ok(),
        "CREATE DATABASE failed while the provisioning session was open: it is \
         holding template1, so a client waiting for the sweep lock breaks the \
         lock holder's create ({created:?})",
    );

    db::drop_database_version_neutral(&mut creator, &probe).await;
}
