//! Task-claim and enqueue throughput benchmark (issue #786).
//!
//! Harvest publishes a CPU-path budget (replay of a 10k-event history < 200ms,
//! issue #135) but had no number at all for `queue::claim_task` — the single
//! most scalability-critical query in the engine, and the one that has accreted
//! roughly a `WHERE` predicate per phase since 3.7. This benchmark produces the
//! reference numbers published in `docs/performance.md`, attributes cost to each
//! accreted gate, and prints the claim plan so the next contributor to touch
//! that query can see what they are changing.
//!
//! # Running
//!
//! ```text
//! # Against a throwaway Docker Postgres (default):
//! cargo bench -p autumn-harvest --features db --bench claim_bench
//!
//! # Against an existing server (the URL is used as an ADMIN connection; a
//! # fresh, uniquely-named database is created and migrated per run, so a
//! # 100k-row backlog can never leak into a shared database):
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo bench -p autumn-harvest --features db --bench claim_bench
//! ```
//!
//! With neither available the benchmark prints a skip notice and exits 0, so
//! `cargo bench` on a laptop without Docker is not a failure.
//!
//! # Why not criterion
//!
//! `claim_task` is destructive — it moves a row `PENDING -> RUNNING`. Criterion
//! runs the measured closure thousands of times, which would drain the seeded
//! backlog and end up timing "claim against an empty queue". See the module doc
//! on the shared harness for the full measurement contract.
//!
//! # Scope
//!
//! This benchmark **measures**; it does not tune. Per issue #786 the claim query
//! is deliberately left byte-for-byte unchanged by this slice.

// The harness is shared verbatim with the CI gate in
// `tests/integration/claim_budget_tests.rs` so the published numbers and the
// gated number can never be produced by different code. It lives under `tests/`
// because that is where the manifest-driven CI runner
// (`.github/ci/integration-suites.txt`) can see and execute the gate; the bench
// reaches across to it rather than the harness being duplicated.
#[path = "../tests/integration/claim_bench_support.rs"]
mod support;

use support::db::{self, BenchDb, ClaimReport, EnqueueReport};
use support::{BACKLOG_SWEEP, ClaimGate, LatencyStats, Scenario, headline_scenario};

/// Writers used for the enqueue measurement.
const ENQUEUE_WRITERS: usize = 8;
/// Rows each writer enqueues.
const ENQUEUE_ROWS_PER_WRITER: usize = 100;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(run());
}

async fn run() {
    let db = match db::setup_bench_db().await {
        Ok(db) => db,
        Err(reason) => {
            println!("SKIP: claim_bench needs Postgres — {}", reason.0);
            println!(
                "      Start Docker, or set HARVEST_TEST_DATABASE_URL to an admin \
                 connection string."
            );
            return;
        }
    };

    println!("# Harvest claim / enqueue benchmark (issue #786)");
    println!();
    println!("machine: {}", db::machine_fingerprint());
    println!("postgres: {}", server_version(&db).await);
    println!(
        "build profile: {} (benchmarks run in the `bench` profile; the CI gate \
         runs in the `test` profile and is therefore slower)",
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release/bench"
        }
    );
    println!();

    scaling_sweep(&db).await;
    gate_breakdown(&db).await;
    enqueue_section(&db).await;
    explain_section(&db).await;
}

async fn server_version(db: &BenchDb) -> String {
    let mut conn = db::connect(&db.url).await;
    db::server_version(&mut conn).await
}

/// Render the p50/p99/max cells of a table row.
///
/// A scenario that collected no post-warmup samples MUST NOT print `0.00` —
/// that would publish a fabricated "instantaneous" measurement for a run that
/// measured nothing at all. `n/a` is the honest cell.
fn stats_cells(stats: LatencyStats) -> String {
    if stats.count == 0 {
        return "n/a | n/a | n/a".to_string();
    }
    format!(
        "{:.2} | {:.2} | {:.2}",
        stats.p50_ms, stats.p99_ms, stats.max_ms
    )
}

/// Marks a row whose measurement was cut short by the wall-clock budget.
///
/// Such a row is still real (its percentiles describe the claims it *did*
/// observe) but it is not a full sample, so it must never be quoted as a
/// steady-state number without the caveat.
const fn truncation_note(truncated: bool) -> &'static str {
    if truncated { " ⚠" } else { "" }
}

/// How claim latency scales with backlog depth — the number that answers
/// "when do I add a shard?".
async fn scaling_sweep(db: &BenchDb) {
    println!("## Claim latency vs backlog depth (baseline gate)");
    println!();
    println!(
        "| backlog | claimers | queues | p50 ms | p99 ms | max ms | claims/s | claimed | empty |"
    );
    println!("|--:|--:|--:|--:|--:|--:|--:|--:|--:|");

    let headline = headline_scenario();
    for backlog in BACKLOG_SWEEP {
        let scenario = Scenario {
            backlog,
            claimers: headline.claimers,
            queues: headline.queues,
            gate: ClaimGate::Baseline,
        };
        let report = db::run_claim_scenario(db, scenario).await;
        print_claim_row(&report, &format!("{backlog}"));
    }
    println!();
    println!(
        "> The headline scenario CI defends is the {} row: {} pending / {} claimers / {} queues.",
        headline.backlog, headline.backlog, headline.claimers, headline.queues
    );
    println!(
        "> A `⚠` marks a row cut short by the per-scenario wall-clock budget \
         (`{}`, default {}s): its percentiles describe the claims it did observe, \
         but it is not a full sample. `n/a` means the scenario collected no \
         post-warmup samples at all.",
        support::SCENARIO_BUDGET_ENV_VAR,
        support::scenario_time_budget().as_secs(),
    );
    println!();
}

fn print_claim_row(report: &ClaimReport, label: &str) {
    println!(
        "| {label}{} | {} | {} | {} | {:.0} | {} | {} |",
        truncation_note(report.truncated),
        report.scenario.claimers,
        report.scenario.queues,
        stats_cells(report.stats),
        report.claims_per_sec(),
        report.claimed,
        report.empty,
    );
}

/// Claimers used for per-gate attribution.
///
/// Deliberately **below** the headline scenario's 8 and below the core count of
/// a typical CI/dev box. Attribution asks "what does this predicate cost?", and
/// at 8 claimers on 4 cores the answer is swamped by run-queue scheduling: the
/// tail columns stopped reproducing between runs (a baseline max of 285ms in
/// one run and 3093ms in the next, for an identical scenario) even though the
/// p50 deltas held steady. Measuring the predicate below saturation isolates the
/// query cost from the contention cost. The headline scenario keeps 8 claimers —
/// contention is the point *there*.
const ATTRIBUTION_CLAIMERS: usize = 2;

/// Per-gate attribution: what does each accreted predicate cost?
async fn gate_breakdown(db: &BenchDb) {
    let headline = Scenario {
        claimers: ATTRIBUTION_CLAIMERS,
        ..headline_scenario()
    };
    println!(
        "## Incremental cost of the accreted claim-path gates (backlog {}, {} claimers, {} queues)",
        headline.backlog, headline.claimers, headline.queues
    );
    println!();
    println!(
        "| gate | seeded rows | claimable | n | p50 ms | p99 ms | max ms | p50 vs baseline | claims/s |"
    );
    println!("|:--|--:|--:|--:|--:|--:|--:|--:|--:|");

    let mut baseline_p50: Option<f64> = None;
    for gate in ClaimGate::all() {
        let scenario = Scenario { gate, ..headline };
        let report = db::run_claim_scenario(db, scenario).await;
        // A delta is only meaningful when BOTH sides are real measurements.
        let delta = match baseline_p50 {
            None => "—".to_string(),
            Some(base) if base > 0.0 && report.stats.count > 0 => {
                format!("{:+.0}%", (report.stats.p50_ms / base - 1.0) * 100.0)
            }
            Some(_) => "n/a".to_string(),
        };
        if gate == ClaimGate::Baseline && report.stats.count > 0 {
            baseline_p50 = Some(report.stats.p50_ms);
        }
        println!(
            "| `{}`{} | {} | {} | {} | {} | {delta} | {:.0} |",
            gate.as_str(),
            truncation_note(report.truncated),
            report.seed.seeded_rows,
            report.seed.claimable_rows,
            report.stats.count,
            stats_cells(report.stats),
            report.claims_per_sec(),
        );
    }
    println!();
    println!(
        "> `paused_rows` and `all_gates` seed unclaimable PAUSED-execution ballast \
         *in addition to* the claimable backlog, so their claimable pool matches \
         the baseline and the only variable is the extra rows the scan walks past."
    );
    println!(
        "> Attribution runs at {ATTRIBUTION_CLAIMERS} claimers rather than the \
         headline scenario's {}, so the numbers isolate predicate cost instead of \
         run-queue contention. **`p50 vs baseline` is the attribution statistic**; \
         tail columns at this backlog depth still pick up background stalls \
         (autovacuum, the OS scheduler) and are reported for completeness only.",
        headline_scenario().claimers,
    );
    println!();
}

/// Enqueue is the write side of the same hot path: start-storms stress it.
async fn enqueue_section(db: &BenchDb) {
    println!("## Enqueue throughput into a non-empty queue");
    println!();
    println!("| backlog | writers | rows | p50 ms | p99 ms | max ms | rows/s |");
    println!("|--:|--:|--:|--:|--:|--:|--:|");
    for backlog in BACKLOG_SWEEP {
        let report: EnqueueReport =
            db::run_enqueue_scenario(db, backlog, ENQUEUE_WRITERS, ENQUEUE_ROWS_PER_WRITER).await;
        println!(
            "| {backlog} | {} | {} | {} | {:.0} |",
            report.writers,
            report.rows,
            stats_cells(report.stats),
            report.rows_per_sec(),
        );
    }
    println!();
}

/// The plan. The single most useful artifact for a contributor about to add
/// predicate number twelve.
async fn explain_section(db: &BenchDb) {
    let headline = headline_scenario();
    println!("## `EXPLAIN (ANALYZE, BUFFERS)` — headline claim");
    println!();
    let mut conn = db::connect(&db.url).await;
    db::seed(&mut conn, headline).await;
    let plan = db::explain_claim(&mut conn, headline).await;
    println!("```text");
    println!("{plan}");
    println!("```");
    println!();
    println!(
        "> Read the top `Sort` / scan nodes first: they show whether the claim \
         still rides an index or has fallen back to scanning and sorting the \
         whole pending backlog."
    );
}
