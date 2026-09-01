//! End-to-end benchmark suite (issue #941).
//!
//! Four scenarios at three shard counts, producing the numbers published in
//! `docs/benchmarks.md`:
//!
//! | scenario | headline |
//! |:--|:--|
//! | `throughput` | sustained workflows completed/sec for a canonical 3-activity workflow |
//! | `dispatch_latency` | activity schedule -> handler start, p50/p99 |
//! | `signal_roundtrip` | HTTP signal -> the workflow observes it, p50/p99 |
//! | `replay_throughput` | events/sec over the issue #135 10 001-event history |
//!
//! # Running
//!
//! ```text
//! # The documented one command (brings up the compose topology, runs the
//! # whole matrix, tears it down again):
//! ./benchmarks/run.sh
//!
//! # Directly, against an already-running topology (one admin URL per shard):
//! HARVEST_BENCH_SHARD_URLS=postgres://...:55432/postgres,postgres://...:55433/postgres \
//!   cargo bench -p autumn-harvest --features db,testing --bench e2e_bench
//!
//! # Against a single server (every "shard" is a database on it — cheaper, but
//! # the shard sweep is then not a scale-out measurement):
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo bench -p autumn-harvest --features db,testing --bench e2e_bench
//!
//! # Compare this run against the published baselines at the documented
//! # tolerance and print a per-number verdict:
//! HARVEST_BENCH_CHECK=1 ./benchmarks/run.sh
//! ```
//!
//! With none of those and no Docker daemon, the suite prints a skip notice and
//! exits 0 — `cargo bench` on a laptop without Postgres is not a failure.
//!
//! # Why not criterion
//!
//! Every scenario here is destructive and stateful: a drained queue, a consumed
//! signal, a completed execution. Criterion's statistical machinery re-runs the
//! measured closure thousands of times, which for all three database scenarios
//! would end up timing an empty queue. Same reason `benches/claim_bench.rs`
//! (issue #786) is hand-rolled. Replay is the one criterion-shaped scenario and
//! `benches/replay_bench.rs` still benches it that way; this suite reports its
//! throughput from the *same* history builder so the two cannot drift.
//!
//! A partial run is a first-class mode: `HARVEST_BENCH_SCENARIOS` and
//! `HARVEST_BENCH_SHARDS` narrow the matrix, so a reader reproducing one
//! published headline does not have to sit through all twelve cells.
//!
//! # Scope
//!
//! This benchmark **measures**; it does not tune, and it gates nothing. Issue
//! #941 puts CI-gated end-to-end regression budgets explicitly out of scope, and
//! `tests/integration/benchmarks_docs.rs` asserts that no CI manifest row runs
//! any scenario here. The component-level complement — the claim/enqueue path
//! and the only performance gate CI does run — is issue #786's
//! `benches/claim_bench.rs` and `docs/performance.md`.

// The harness is shared with `benches/replay_bench.rs` (the history builder) and
// with the docs-drift guard, so the published numbers, the #135 budget bench and
// the doc can never be produced by three different definitions of the same
// thing. It lives under `tests/` because that is where a shared harness lives in
// this repo (see `claim_bench_support.rs`, issue #786).
#[path = "../tests/integration/claim_bench_support.rs"]
mod claim_bench_support;
#[path = "../tests/integration/e2e_bench_support.rs"]
mod e2e_bench_support;

use e2e_bench_support::{
    BenchScenario, CHECK_ENV_VAR, Metric, PUBLISHED_BASELINES, REPRO_TOLERANCE_PCT,
    SCENARIO_FILTER_ENV_VAR, SHARD_FILTER_ENV_VAR, ReproVerdict, ScenarioReport, baseline_for,
    relative_error_pct, render_matrix, render_value, repro_verdict, selected_scenarios,
    selected_shard_counts, unknown_scenario_ids,
};

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build the benchmark runtime");
    runtime.block_on(run());
}

async fn run() {
    println!("# Harvest end-to-end benchmark run (issue #941)\n");
    print_environment().await;

    let scenario_filter = std::env::var(SCENARIO_FILTER_ENV_VAR).ok();
    let shard_filter = std::env::var(SHARD_FILTER_ENV_VAR).ok();
    let unknown = unknown_scenario_ids(scenario_filter.as_deref());
    if !unknown.is_empty() {
        println!(
            "\n> `{SCENARIO_FILTER_ENV_VAR}` names no such scenario: {}. Known scenarios: {}.\n",
            unknown.join(", "),
            BenchScenario::all()
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let scenarios = selected_scenarios(scenario_filter.as_deref());
    let shard_counts = selected_shard_counts(shard_filter.as_deref());

    let mut reports: Vec<ScenarioReport> = Vec::new();
    for shards in shard_counts {
        for scenario in scenarios.iter().copied() {
            eprintln!("==> {} at {shards} shard(s)", scenario.as_str());
            match run_scenario(scenario, shards).await {
                Ok(report) => reports.push(report),
                Err(reason) => {
                    println!(
                        "\n> **Skipped** `{}` at {shards} shard(s): {}\n",
                        scenario.as_str(),
                        reason
                    );
                }
            }
        }
    }

    if reports.is_empty() {
        println!(
            "\nNo scenario ran. Set `HARVEST_BENCH_SHARD_URLS` (see \
             `benchmarks/docker-compose.yml`) or `HARVEST_TEST_DATABASE_URL`, or start a \
             Docker daemon, and run again.\n"
        );
        return;
    }

    println!("\n## Results\n");
    println!("{}", render_matrix(&reports));

    println!("\n## Notes\n");
    for report in &reports {
        println!("### `{}` at {} shard(s)\n", report.scenario.as_str(), report.shards);
        for note in &report.notes {
            println!("* {note}");
        }
        if report.is_sound() {
            println!("* sound: every published number on this row rests on a measured sample");
        } else {
            for reason in &report.unsound {
                println!("* **not published**: {reason}");
            }
        }
        println!();
    }

    if std::env::var(CHECK_ENV_VAR).is_ok() {
        print_reproduction_check(&reports);
    }

    let unsound: Vec<String> = reports
        .iter()
        .filter(|r| !r.is_sound())
        .map(|r| format!("{}@{}", r.scenario.as_str(), r.shards))
        .collect();
    if unsound.is_empty() {
        println!("\nEvery scenario reported sound.\n");
    } else {
        println!("\n**Unsound scenarios (not publishable): {}**\n", unsound.join(", "));
    }
}

async fn run_scenario(
    scenario: BenchScenario,
    shards: u32,
) -> Result<ScenarioReport, e2e_bench_support::db::SkipReason> {
    match scenario {
        BenchScenario::Throughput => e2e_bench_support::db::run_throughput(shards).await,
        BenchScenario::DispatchLatency => e2e_bench_support::db::run_dispatch_latency(shards).await,
        BenchScenario::SignalRoundtrip => e2e_bench_support::db::run_signal_roundtrip(shards).await,
        BenchScenario::ReplayThroughput => Ok(e2e_bench_support::run_replay_throughput(shards).await),
    }
}

async fn print_environment() {
    println!("## Environment\n");
    println!("| | |");
    println!("|:--|:--|");
    println!("| Logical CPUs | {} |", std::thread::available_parallelism().map_or_else(|_| "unknown".to_owned(), |n| n.to_string()));
    println!("| OS | {} / {} |", std::env::consts::OS, std::env::consts::ARCH);
    println!("| Profile | `bench` (release) |");
    println!("| Harness | `autumn-harvest/tests/integration/e2e_bench_support.rs` |");
    println!(
        "| Workers | {} per shard, {} concurrent workflows, {} concurrent activities |",
        e2e_bench_support::db::WORKERS_PER_SHARD,
        e2e_bench_support::db::MAX_CONCURRENT_WORKFLOWS,
        e2e_bench_support::db::MAX_CONCURRENT_ACTIVITIES,
    );
    println!(
        "| Poll interval | {} ms (LISTEN/NOTIFY wired per shard) |",
        e2e_bench_support::db::POLL_INTERVAL_MS
    );
    println!("| Pool size | {} per shard |", e2e_bench_support::db::POOL_SIZE_PER_SHARD);
    if let Some(version) = e2e_bench_support::db::probe_postgres_version().await {
        println!("| Postgres | {version} |");
    }
    println!();
}

fn print_reproduction_check(reports: &[ScenarioReport]) {
    println!("\n## Reproduction check (±{REPRO_TOLERANCE_PCT:.0}%)\n");
    if PUBLISHED_BASELINES.is_empty() {
        println!("No baselines are published yet, so there is nothing to compare against.\n");
        return;
    }
    println!("| scenario | shards | metric | published | measured | error | verdict |");
    println!("|:--|--:|:--|--:|--:|--:|:--|");
    for report in reports {
        for Metric { key, value } in &report.metrics {
            let Some(published) = baseline_for(report.scenario, report.shards, key) else {
                continue;
            };
            let verdict = value.map_or(ReproVerdict::Outside, |measured| {
                repro_verdict(measured, published, REPRO_TOLERANCE_PCT)
            });
            let error = value
                .and_then(|measured| relative_error_pct(measured, published))
                .map_or_else(|| "n/a".to_owned(), |e| format!("{e:+.1}%"));
            println!(
                "| `{}` | {} | `{key}` | {published:.2} | {} | {error} | {} |",
                report.scenario.as_str(),
                report.shards,
                render_value(*value),
                match verdict {
                    ReproVerdict::Within => "reproduced",
                    ReproVerdict::Outside => "**outside tolerance**",
                }
            );
        }
    }
    println!();
}
