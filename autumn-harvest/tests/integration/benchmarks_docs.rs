//! Docs-drift guard for the end-to-end benchmark suite (issue #941).
//!
//! `docs/benchmarks.md` exists to be *believed*: an evaluating architect sizes a
//! deployment against it and then reproduces it from a fresh clone. That only
//! works while the doc, the harness and the committed topology agree, so this
//! suite pins them together — the `alert_pack_docs` / `chaos_docs` pattern
//! applied to a performance page.
//!
//! It also pins the two scope boundaries issue #941 draws, because both are
//! promises to the reader rather than incidental facts:
//!
//! * **No CI gate.** CI-gated end-to-end regression budgets are explicitly out
//!   of scope, so no manifest row may run a benchmark scenario.
//! * **No duplication of issue #786.** The claim/enqueue microbenchmark and the
//!   only performance gate CI runs stay #786's; this suite cross-references them
//!   instead of re-measuring them.
//!
//! Pure: no database, no async. Runs on every OS.

use std::path::{Path, PathBuf};

use super::e2e_bench_support::{
    BenchScenario, CHECK_ENV_VAR, INFLIGHT_ENV_VAR, MAX_CONCURRENT_ACTIVITIES,
    MAX_CONCURRENT_WORKFLOWS, POOL_SIZE_PER_SHARD, PUBLISHED_BASELINES, PUBLISHED_RESULTS_VERSION,
    REPRO_TOLERANCE_PCT, SCENARIO_FILTER_ENV_VAR, SHARD_COUNTS, SHARD_FILTER_ENV_VAR,
    SHARD_URLS_ENV_VAR, THROUGHPUT_INFLIGHT_PER_SHARD, WORKERS_PER_SHARD, WORKFLOWS_ENV_VAR,
};

fn repo_root() -> PathBuf {
    // `autumn-harvest/` -> repo root.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate directory has a parent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} must exist and be readable: {e}", path.display()))
}

fn benchmarks_doc() -> String {
    read("docs/benchmarks.md")
}

fn results_doc_path() -> String {
    format!("docs/benchmarks/results-v{PUBLISHED_RESULTS_VERSION}.md")
}

#[test]
fn the_published_doc_covers_every_scenario() {
    let doc = benchmarks_doc();
    for scenario in BenchScenario::all() {
        assert!(
            doc.contains(scenario.as_str()),
            "docs/benchmarks.md never mentions the `{}` scenario; issue #941 AC1 publishes \
             all four",
            scenario.as_str()
        );
    }
}

#[test]
fn the_published_doc_covers_every_shard_count() {
    let doc = benchmarks_doc();
    for shards in SHARD_COUNTS {
        assert!(
            doc.contains(&format!("{shards} shard")),
            "docs/benchmarks.md never mentions {shards} shards; issue #941 AC2 publishes every \
             scenario at 1, 2 and 4"
        );
    }
}

#[test]
fn the_documented_command_is_the_committed_runner() {
    let doc = benchmarks_doc();
    assert!(
        doc.contains("./benchmarks/run.sh"),
        "docs/benchmarks.md must document the one command issue #941 AC3 promises"
    );
    let runner = read("benchmarks/run.sh");
    assert!(
        runner.contains("docker-compose.yml") && runner.contains("--bench e2e_bench"),
        "benchmarks/run.sh must bring up the committed topology and run the suite"
    );
    assert!(
        runner.contains("HARVEST_BENCH_SHARD_URLS"),
        "the runner must hand the shard URLs to the harness"
    );
}

#[test]
fn the_committed_topology_has_a_service_per_shard() {
    let compose = read("benchmarks/docker-compose.yml");
    let widest = SHARD_COUNTS.iter().copied().max().expect("a shard matrix");
    for shard in 0..widest {
        assert!(
            compose.contains(&format!("shard-{shard}:")),
            "benchmarks/docker-compose.yml must define a Postgres service for shard {shard}; \
             the widest sweep runs at {widest} shards"
        );
    }
}

#[test]
fn the_doc_states_the_reproduction_tolerance_the_harness_uses() {
    let doc = benchmarks_doc();
    // The signed form, not a bare `15%`: a doc that said "115%" would satisfy a
    // substring check for "15%" while telling the reader something false.
    let stated = format!("\u{b1}{REPRO_TOLERANCE_PCT:.0}%");
    assert!(
        doc.contains(&stated),
        "docs/benchmarks.md must state the {stated} tolerance the harness compares against"
    );
}

#[test]
fn every_documented_environment_variable_is_one_the_harness_reads() {
    // The knob table is the reader's whole interface to a partial run. Renaming
    // a constant without editing the table silently orphans it.
    let doc = benchmarks_doc();
    for var in [
        SHARD_URLS_ENV_VAR,
        CHECK_ENV_VAR,
        SCENARIO_FILTER_ENV_VAR,
        SHARD_FILTER_ENV_VAR,
        INFLIGHT_ENV_VAR,
        WORKFLOWS_ENV_VAR,
    ] {
        assert!(
            doc.contains(var),
            "`{var}` is read by the harness but never documented in docs/benchmarks.md"
        );
    }
}

#[test]
fn the_doc_publishes_the_worker_configuration_the_numbers_were_taken_at() {
    // Issue #941 AC2 asks for "a documented worker/concurrency configuration".
    // Documented means on the published page, not only in the run's own output.
    let doc = benchmarks_doc();
    for (label, value) in [
        ("workers per shard", WORKERS_PER_SHARD),
        ("concurrent workflow tasks", MAX_CONCURRENT_WORKFLOWS),
        ("concurrent activity tasks", MAX_CONCURRENT_ACTIVITIES),
        ("connections per shard pool", POOL_SIZE_PER_SHARD),
        (
            "workflows in flight per shard",
            THROUGHPUT_INFLIGHT_PER_SHARD,
        ),
    ] {
        assert!(
            doc.contains(&value.to_string()),
            "docs/benchmarks.md must state the {label} ({value}) the published numbers were \
             taken at"
        );
    }
}

#[test]
fn the_doc_frames_the_numbers_as_reference_machine_guidance() {
    let doc = benchmarks_doc();
    assert!(
        doc.contains("not an SLO"),
        "issue #941 AC5 asks this page to mirror the honesty of docs/alerts/README.md: the \
         numbers are reference-machine guidance, not a service-level objective"
    );
    assert!(
        doc.contains("Reproduce them on your own hardware"),
        "the page must tell a reader to re-measure rather than design against these figures"
    );
}

#[test]
fn the_doc_cross_references_the_claim_path_microbenchmark() {
    let doc = benchmarks_doc();
    assert!(
        doc.contains("#786"),
        "issue #941 AC5 requires an explicit cross-reference to #786's claim-path \
         microbenchmark as the component-level complement"
    );
    assert!(
        doc.contains("docs/performance.md") || doc.contains("performance.md"),
        "the cross-reference must point at the page #786 publishes"
    );
}

#[test]
fn this_suite_does_not_duplicate_the_claim_path_scenarios() {
    for scenario in BenchScenario::all() {
        let id = scenario.as_str();
        assert!(
            !id.contains("claim") && !id.contains("enqueue"),
            "`{id}` re-measures issue #786's territory; #941 AC5 says no duplication of its \
             claim/enqueue scenarios"
        );
    }
}

#[test]
fn no_ci_manifest_row_runs_the_end_to_end_benchmark() {
    // Issue #941: "CI-gated regression budgets for these end-to-end numbers"
    // is out of scope. A row that ran a scenario would also be a 20-minute,
    // Docker-topology CI job on every PR.
    let manifest = read(".github/ci/integration-suites.txt");
    for line in manifest.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        assert!(
            !line.contains("e2e_bench"),
            "a CI manifest row runs an end-to-end benchmark scenario, which issue #941 puts \
             out of scope:\n  {line}"
        );
    }
}

#[test]
fn the_docs_drift_guard_itself_runs_in_ci() {
    // The guard is only worth having if CI executes it.
    let manifest = read(".github/ci/integration-suites.txt");
    assert!(
        manifest
            .lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .any(|l| l.contains("benchmarks_docs")),
        "add a manifest row for `benchmarks_docs` — a docs guard that never runs is not a guard"
    );
}

#[test]
fn every_published_baseline_appears_on_its_own_row_in_the_versioned_results_file() {
    let results = read(&results_doc_path());
    for baseline in PUBLISHED_BASELINES {
        // The harness renders the results table itself, so match the whole row
        // rather than the number alone: a bare-number check passes when the
        // right value sits on the wrong scenario or the wrong shard count, which
        // is exactly the drift worth catching.
        let row = format!(
            "| `{}` | {} | `{}` | {:.2} |",
            baseline.scenario.as_str(),
            baseline.shards,
            baseline.metric,
            baseline.value,
        );
        assert!(
            results.contains(&row),
            "the published baseline for `{}` at {} shards ({} = {:.2}) has no matching row in \
             {} — the harness's constants and the results file must be the same numbers, on \
             the same rows.\nExpected row: {row}",
            baseline.scenario.as_str(),
            baseline.shards,
            baseline.metric,
            baseline.value,
            results_doc_path(),
        );
    }
}

#[test]
fn every_headline_number_is_also_on_the_index_page() {
    // The changelog and the planning record both claim the doc must contain the
    // published baselines. It does -- docs/benchmarks.md carries a headline
    // table -- and this is what keeps that claim true.
    let doc = benchmarks_doc();
    for baseline in PUBLISHED_BASELINES {
        let rendered = format!("{:.2}", baseline.value);
        // Large counts are rendered with thin spaces for readability on the
        // index page, so compare on the digits alone.
        let digits: String = rendered.chars().filter(char::is_ascii_digit).collect();
        let doc_digits: String = doc.chars().filter(char::is_ascii_digit).collect();
        assert!(
            doc_digits.contains(&digits),
            "the published baseline {rendered} for `{}` at {} shards does not appear in \
             docs/benchmarks.md",
            baseline.scenario.as_str(),
            baseline.shards,
        );
    }
}

#[test]
fn the_results_file_for_this_version_is_linked_from_the_index() {
    let doc = benchmarks_doc();
    assert!(
        doc.contains(&format!("results-v{PUBLISHED_RESULTS_VERSION}.md")),
        "docs/benchmarks.md must link the published release's results file (issue #941 AC4 \
         keeps each release's numbers rather than overwriting them)"
    );
}

#[test]
fn the_results_file_records_the_hardware_it_was_measured_on() {
    let results = read(&results_doc_path());
    for required in ["Postgres", "CPU", "topology"] {
        assert!(
            results.contains(required),
            "the versioned results file must record `{required}`; issue #941 AC4 asks for exact \
             hardware, OS, Postgres version, methodology and configuration"
        );
    }
}
