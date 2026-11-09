//! Docs guard for the open-loop capacity scenario.
//!
//! It pins the published capacity cells to their results file and to the
//! headline in `docs/benchmarks.md`.
//!
//! Pure: no database, no async. Runs on every OS.

use std::path::{Path, PathBuf};

use super::capacity_bench_support::{
    PUBLISHED_CAPACITY, PUBLISHED_CAPACITY_FILE, SUSTAINABLE_METRIC,
};
use super::e2e_bench_support::{BenchScenario, render_value};

/// The reference box, which the results file must name.
const REFERENCE_HARDWARE: &str = "4 logical cpus";

fn read(relative: &str) -> String {
    let root: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate directory has a parent")
        .to_path_buf();
    let path = root.join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} must exist and be readable: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// How the benchmarks page writes a published rate.
fn stated_number(rate: f64) -> String {
    format!("{rate:.1}")
}

#[test]
fn each_arm_has_a_published_cell_at_one_shard() {
    for arm in BenchScenario::capacity() {
        assert!(
            PUBLISHED_CAPACITY.iter().any(|c| {
                BenchScenario::Capacity(c.arm) == arm
                    && c.shards == 1
                    && c.sustainable_per_sec > 0.0
            }),
            "no published 1-shard capacity for `{}`",
            arm.as_str()
        );
    }
}

#[test]
fn the_results_file_publishes_each_cell_as_a_sound_row() {
    let page = read(PUBLISHED_CAPACITY_FILE);
    for cell in PUBLISHED_CAPACITY {
        let row = format!(
            "| `{}` | {} | `{SUSTAINABLE_METRIC}` | {} | yes |",
            BenchScenario::Capacity(cell.arm).as_str(),
            cell.shards,
            render_value(Some(cell.sustainable_per_sec)),
        );
        assert!(
            page.contains(&row),
            "{PUBLISHED_CAPACITY_FILE} must carry the sound row {row}"
        );
    }
}

#[test]
fn the_results_file_records_its_hardware_and_host_layout() {
    let page = read(PUBLISHED_CAPACITY_FILE);
    for needle in [REFERENCE_HARDWARE, "Postgres", "host layout"] {
        assert!(
            page.to_lowercase().contains(&needle.to_lowercase()),
            "{PUBLISHED_CAPACITY_FILE} must record `{needle}`"
        );
    }
}

#[test]
fn the_benchmarks_page_states_each_published_rate() {
    let page = read("docs/benchmarks.md");
    for cell in PUBLISHED_CAPACITY {
        let stated = format!(
            "**{}** `{}`",
            stated_number(cell.sustainable_per_sec),
            BenchScenario::Capacity(cell.arm).as_str()
        );
        assert!(
            page.split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .contains(&stated),
            "docs/benchmarks.md must state {stated}, the published rate"
        );
    }
}
