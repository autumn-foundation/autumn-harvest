//! Docs guard for the open-loop capacity scenario.
//!
//! It pins the published capacity cells to their results file. It also
//! keeps an external capacity figure in `docs/comparison.md` next to a
//! Harvest number from this scenario, with the hardware of each.
//!
//! Pure: no database, no async. Runs on every OS.

use std::path::{Path, PathBuf};

use super::capacity_bench_support::{
    PUBLISHED_CAPACITY, PUBLISHED_CAPACITY_FILE, SUSTAINABLE_METRIC,
};
use super::e2e_bench_support::{BenchScenario, render_value};

/// Text that marks the external capacity figure in `docs/comparison.md`.
const EXTERNAL_FIGURES: [&str; 3] = ["20K", "20,000", "1M workflows"];
/// The external figure's hardware, which must sit next to it.
const EXTERNAL_HARDWARE: &str = "db.m7i.4xlarge";
/// The reference box, which must sit next to the Harvest number.
const REFERENCE_HARDWARE: &str = "4 logical CPUs";

fn read(relative: &str) -> String {
    let root: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate directory has a parent")
        .to_path_buf();
    let path = root.join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} must exist and be readable: {e}", path.display()))
}

/// The blank-line separated blocks of a Markdown page.
fn blocks(page: &str) -> Vec<String> {
    page.split("\n\n").map(str::to_owned).collect()
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
            page.contains(needle),
            "{PUBLISHED_CAPACITY_FILE} must record `{needle}`"
        );
    }
}

#[test]
fn the_comparison_cites_the_external_figure_only_next_to_a_harvest_capacity_number() {
    let page = read("docs/comparison.md");
    let cited: Vec<String> = blocks(&page)
        .into_iter()
        .filter(|b| EXTERNAL_FIGURES.iter().any(|f| b.contains(f)))
        .collect();
    assert!(
        !cited.is_empty(),
        "docs/comparison.md must cite the external capacity figure next to a Harvest number"
    );
    for block in cited {
        for cell in PUBLISHED_CAPACITY {
            let number = render_value(Some(cell.sustainable_per_sec));
            assert!(
                block.contains(&number),
                "a block that cites the external figure must carry the Harvest number \
                 {number}:\n{block}"
            );
        }
        for hardware in [EXTERNAL_HARDWARE, REFERENCE_HARDWARE] {
            assert!(
                block.contains(hardware),
                "a block that cites the external figure must state `{hardware}`:\n{block}"
            );
        }
    }
}
