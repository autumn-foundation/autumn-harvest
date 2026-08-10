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
//! fails the build when p99 claim latency exceeds the budget in
//! `docs/performance.md`. The exploratory report — every gate at every backlog
//! depth, plus `EXPLAIN (ANALYZE, BUFFERS)` — lives in `benches/claim_bench.rs`
//! and shares this crate's [`claim_bench_support`] harness, so the gate and the
//! benchmark can never drift apart.
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
//!    `HARVEST_CLAIM_BUDGET_P99_MS` overrides it for a one-off run.
//! 3. The measurement itself is unsound — see the `claim_ratio` and
//!    backlog-drain assertions below, which fail loudly rather than reporting a
//!    meaningless percentile.
//!
//! [`claim_bench_support`]: super::claim_bench_support

#![cfg(feature = "db")]

use super::claim_bench_support::db::{self, BenchDb};
use super::claim_bench_support::{
    BUDGET_ENV_VAR, BudgetVerdict, ClaimGate, LatencyStats, MIN_MEANINGFUL_SAMPLES, Scenario,
    budget_from_env, budget_verdict, headline_scenario, measured_claims_for,
};

/// Published p99 budget for the headline scenario, in milliseconds.
///
/// **This is a regression tripwire, not an SLO.** Derived from the reference
/// measurement in `docs/performance.md`: across 8 runs on a quiet 4-core box
/// (5 release, 3 debug) the headline p99 landed between 268ms and 599ms, median
/// ~296ms. The budget is ~2.5x the worst observation and ~5x the median.
///
/// That headroom is deliberate, and it is the reason this gate is honest about
/// what it does **not** do: it will not catch a predicate that makes claims 50%
/// slower. It catches a *cliff* — the kind of change that adds another per-row
/// subplan to a scan already walking the whole pending backlog. For drift, run
/// the benchmark and compare against the published table; a shared CI runner
/// with a containerised Postgres is far too noisy to defend a tighter number,
/// and a gate that flakes is a gate everyone learns to ignore.
///
/// Override for a one-off run with `HARVEST_CLAIM_BUDGET_P99_MS`.
const HEADLINE_P99_BUDGET_MS: f64 = 1_500.0;

/// Minimum fraction of measured operations that must actually return a task.
///
/// Without this floor the gate could "pass" while measuring nothing: a harness
/// that claimed zero tasks would report a tiny p99 for a queue it never touched.
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claim_p99_at_headline_scenario_is_within_budget() {
    let Some(db) = bench_db_or_skip().await else {
        return;
    };

    let scenario = headline_scenario();
    let report = db::run_claim_scenario(&db, scenario).await;
    let budget = budget_from_env(HEADLINE_P99_BUDGET_MS);

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
    // otherwise leave the gate defending a two-sample p99.
    assert!(
        report.stats.count >= MIN_MEANINGFUL_SAMPLES,
        "measurement unsound: only {} samples collected (need >= {MIN_MEANINGFUL_SAMPLES}). \
         The scenario hit its wall-clock budget almost immediately, which is \
         itself a signal that the claim path is far slower than the budget.",
        report.stats.count,
    );

    // Soundness first: a percentile over a run that claimed nothing is noise.
    assert!(
        report.claim_ratio() >= MIN_CLAIM_RATIO,
        "measurement unsound: only {:.0}% of {} operations claimed a task \
         (claimed={}, empty={}). The reported p99 would not describe the claim \
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

    // The gate itself.
    assert_eq!(
        budget_verdict(report.stats.p99_ms, budget),
        BudgetVerdict::WithinBudget,
        "CLAIM PATH REGRESSION: p99 {:.2}ms exceeds the {budget:.2}ms budget for \
         the headline scenario ({} pending / {} claimers / {} queues). {}\n\
         Run `cargo bench -p autumn-harvest --features db --bench claim_bench` \
         for the per-gate cost table and EXPLAIN output, and see \
         docs/performance.md. Override for a one-off run with {BUDGET_ENV_VAR}.",
        report.stats.p99_ms,
        scenario.backlog,
        scenario.claimers,
        scenario.queues,
        render(report.stats),
    );
}

/// The gate must be falsifiable against **real measured data**, not just in the
/// unit tests of the comparison function.
///
/// Runs the same measurement and asserts that a budget below the observed p99
/// would have tripped it. Without this, a harness bug that reported `0.0` for
/// every percentile would let the gate pass forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gate_would_trip_on_a_budget_below_the_measured_p99() {
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
        report.stats.p99_ms > 0.0,
        "measured p99 was {:.6}ms — a claim against Postgres cannot be free, so \
         the harness is not measuring anything",
        report.stats.p99_ms,
    );

    let impossible_budget = report.stats.p99_ms / 2.0;
    assert_eq!(
        budget_verdict(report.stats.p99_ms, impossible_budget),
        BudgetVerdict::Exceeded,
        "gate is vacuous: measured p99 {:.2}ms did not exceed a {impossible_budget:.2}ms budget",
        report.stats.p99_ms,
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

/// The gate and the benchmark must agree on what "the headline scenario" is.
/// A pure assertion, so it runs on every OS with no database.
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
