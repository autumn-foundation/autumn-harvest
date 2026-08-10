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

use super::claim_bench_support::db::{self, BenchDb};
use super::claim_bench_support::{
    BUDGET_ENV_VAR, BudgetVerdict, ClaimGate, HEADLINE_P50_BUDGET_MS, LatencyStats,
    MIN_MEANINGFUL_SAMPLES, SCENARIO_BUDGET_ENV_VAR, Scenario, budget_from_env, budget_verdict,
    headline_scenario, measured_claims_for, scenario_time_budget,
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
    let mut conn = db::connect(&db.url).await;
    let remaining = db::pending_count(&mut conn).await;
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
    let mut conn = db::connect(&db.url).await;
    let pending = db::pending_count(&mut conn).await;
    let expected = i64::try_from(backlog + report.rows).expect("row count fits i64");
    assert_eq!(
        pending, expected,
        "enqueue must have written into a non-empty queue: expected the seeded \
         {backlog}-row backlog plus {} enqueued rows, found {pending} PENDING",
        report.rows,
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
    let mut conn = db::connect(&db.url).await;

    for gate in ClaimGate::all() {
        let scenario = Scenario {
            backlog,
            claimers: 1,
            queues: 2,
            gate,
        };
        db::seed(&mut conn, scenario).await;
        let census = db::seed_census(&mut conn).await;
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
    let mut conn = db::connect(&db.url).await;
    let mut totals = Vec::new();
    for gate in [ClaimGate::DoubleBacklog, ClaimGate::PausedRows] {
        let outcome = db::seed(
            &mut conn,
            Scenario {
                backlog,
                claimers: 1,
                queues: 2,
                gate,
            },
        )
        .await;
        let pending = db::pending_count(&mut conn).await;
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
