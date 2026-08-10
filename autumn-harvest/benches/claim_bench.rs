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

use std::collections::HashMap;

use support::db::{self, BenchDb, ClaimReport, EnqueueReport};
use support::{
    BACKLOG_SWEEP, ClaimGate, LatencyStats, MIN_MEANINGFUL_SAMPLES, Scenario, headline_scenario,
};

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

/// Marks a row whose sample count is below [`MIN_MEANINGFUL_SAMPLES`].
///
/// The gate refuses to defend a budget on fewer samples than this; the report
/// must not silently publish one either, or a thin row gets transcribed into
/// `docs/performance.md` and quoted as though it were a steady-state number.
const fn thin_sample_note(stats: LatencyStats) -> &'static str {
    if stats.count > 0 && stats.count < MIN_MEANINGFUL_SAMPLES {
        " ‡"
    } else {
        ""
    }
}

/// Legend for the two row markers, printed under every table that can emit them.
fn marker_legend() -> String {
    format!(
        "> `⚠` marks a row cut short by the per-scenario wall-clock budget \
         (`{}`, default {}s): its percentiles describe the claims it did observe, \
         but it is not a full sample. `‡` marks a row with fewer than {} samples \
         — below the floor the CI gate itself will accept, so treat it as \
         directional only. `n/a` means the scenario collected no post-warmup \
         samples at all.",
        support::SCENARIO_BUDGET_ENV_VAR,
        support::scenario_time_budget().as_secs(),
        MIN_MEANINGFUL_SAMPLES,
    )
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
        "> These rows run at {} concurrent claimers, so they measure the claim \
         path *under contention* — the operational number — not the isolated \
         query cost. The attribution table below deliberately runs below \
         saturation instead.",
        headline.claimers,
    );
    println!("{}", marker_legend());
    println!();
}

fn print_claim_row(report: &ClaimReport, label: &str) {
    println!(
        "| {label}{}{} | {} | {} | {} | {:.0} | {} | {} |",
        truncation_note(report.truncated),
        thin_sample_note(report.stats),
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
        "| gate | seeded rows | claimable | n | p50 ms | p99 ms | max ms | p50 vs | vs what | claims/s |"
    );
    println!("|:--|--:|--:|--:|--:|--:|--:|--:|:--|--:|");

    // Every gate's p50, so a row can be compared against its own comparand
    // rather than always against baseline. `paused_rows` seeds 2x the table
    // depth, and claim latency is superlinear in depth, so comparing it to
    // baseline would charge the anti-join for the extra rows too.
    // Streamed, not buffered: a full sweep takes tens of minutes and an operator
    // watching it should see each row land. Sound because every comparand is
    // ordered before its dependent in `ClaimGate::all()` — pinned by
    // `every_gate_is_ordered_after_its_comparand`.
    let mut p50_by_gate: HashMap<&'static str, f64> = HashMap::new();
    for gate in ClaimGate::all() {
        let scenario = Scenario { gate, ..headline };
        let report = db::run_claim_scenario(db, scenario).await;
        if report.stats.count > 0 {
            p50_by_gate.insert(gate.as_str(), report.stats.p50_ms);
        }
        let comparand = gate.comparand();
        // A delta is only meaningful when BOTH sides are real measurements.
        let (delta, vs) = if gate == comparand {
            ("—".to_string(), String::new())
        } else {
            let base = p50_by_gate.get(comparand.as_str()).copied();
            let cell = match base {
                Some(b) if b > 0.0 && report.stats.count > 0 => {
                    format!("{:+.0}%", (report.stats.p50_ms / b - 1.0) * 100.0)
                }
                _ => "n/a".to_string(),
            };
            (cell, format!("`{}`", comparand.as_str()))
        };
        println!(
            "| `{}`{}{} | {} | {} | {} | {} | {delta} | {vs} | {:.0} |",
            gate.as_str(),
            truncation_note(report.truncated),
            thin_sample_note(report.stats),
            report.seed.seeded_rows,
            report.seed.claimable_rows,
            report.stats.count,
            stats_cells(report.stats),
            report.claims_per_sec(),
        );
    }
    println!();
    println!(
        "> `paused_rows` seeds unclaimable PAUSED-execution ballast *in addition \
         to* the claimable backlog, so it walks a table twice as deep as \
         `baseline`. Because claim latency is strongly superlinear in depth, it \
         is reported against the equal-depth `double_backlog` control — which \
         seeds the same total rows, all claimable — so the delta is the \
         anti-join's cost rather than the extra depth's."
    );
    println!(
        "> `double_backlog` is a control, not a gate: `double_backlog vs baseline` \
         is the cost of doubling table depth on its own."
    );
    println!(
        "> `all_gates` is **not** a strict upper bound: its circuit-breaker \
         tracked set short-circuits the rate-limit `EXISTS` and the debit CTE via \
         `= ANY($5)`, so a deployment with rate limiting and no breaker executes \
         strictly more work than this row shows."
    );
    println!(
        "> Attribution runs at {ATTRIBUTION_CLAIMERS} claimers rather than the \
         headline scenario's {}, so the numbers isolate predicate cost instead of \
         run-queue contention. **`p50 vs` is the attribution statistic**; \
         tail columns at this backlog depth still pick up background stalls \
         (autovacuum, the OS scheduler) and are reported for completeness only.",
        headline_scenario().claimers,
    );
    println!("{}", marker_legend());
    println!();
}

/// Enqueue is the write side of the same hot path: start-storms stress it.
async fn enqueue_section(db: &BenchDb) {
    println!("## Enqueue throughput into a non-empty queue");
    println!();
    println!("| backlog | writers | rows | n | p50 ms | p99 ms | max ms | rows/s |");
    println!("|--:|--:|--:|--:|--:|--:|--:|--:|");
    for backlog in BACKLOG_SWEEP {
        let report: EnqueueReport =
            db::run_enqueue_scenario(db, backlog, ENQUEUE_WRITERS, ENQUEUE_ROWS_PER_WRITER).await;
        println!(
            "| {}{} | {} | {} | {} | {} | {:.0} |",
            // Read the report, not the loop variable: a transcription bug here
            // would silently mislabel which backlog a row describes.
            report.backlog,
            thin_sample_note(report.stats),
            report.writers,
            report.rows,
            report.stats.count,
            stats_cells(report.stats),
            report.rows_per_sec(),
        );
    }
    println!();
    println!(
        "> `rows` counts every row written; `n` is the post-warmup sample count \
         behind the latency columns (the same one-in-{} head trim the claim path \
         uses). `rows/s` divides *all* rows by the *whole* wall clock, warmup \
         included, so it is a conservative floor on sustained throughput rather \
         than a peak.",
        support::warmup_divisor(),
    );
    println!("{}", marker_legend());
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
