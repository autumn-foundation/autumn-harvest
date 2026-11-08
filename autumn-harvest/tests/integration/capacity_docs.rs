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

/// Lowercase text that marks the external capacity figure. The figure is
/// 20K workflows/s on one shard, and 1M workflows/s on 50 shards.
const EXTERNAL_FIGURES: [&str; 8] = [
    "20k",
    "20,000",
    "20 000",
    "20000",
    "1m workflows",
    "1,000,000",
    "1 000 000",
    "million",
];
/// The external figure's hardware, which must sit next to it.
const EXTERNAL_HARDWARE: &str = "db.m7i.4xlarge";
/// The reference box, which must sit next to the Harvest number.
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

/// The blocks of a Markdown page: runs of lines between blank lines. A whole
/// table is one block.
fn blocks(page: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    for line in page.lines() {
        if line.trim().is_empty() {
            if !current.is_empty() {
                out.push(current.join("\n"));
                current.clear();
            }
        } else {
            current.push(line);
        }
    }
    if !current.is_empty() {
        out.push(current.join("\n"));
    }
    out
}

/// Whether `text` holds `number` with no digit or point on either side.
fn contains_number(text: &str, number: &str) -> bool {
    let bounded = |c: Option<char>| c.is_none_or(|c| !c.is_ascii_digit() && c != '.');
    text.match_indices(number).any(|(at, _)| {
        let before = text[..at].chars().next_back();
        let after = text[at + number.len()..].chars().next();
        // A sentence-ending point after the number is still a boundary.
        let after_ok = bounded(after)
            || (after == Some('.')
                && !text[at + number.len() + 1..]
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_digit()));
        bounded(before) && after_ok
    })
}

/// Whether a block cites the external figure: it names the engine and one of
/// its figures. A bare "20k" or "million" elsewhere on the page is no
/// citation.
fn is_external_citation(block: &str) -> bool {
    let lower = block.to_lowercase();
    lower.contains("dbos") && EXTERNAL_FIGURES.iter().any(|f| lower.contains(f))
}

/// How the comparison page writes a published rate.
fn comparison_number(rate: f64) -> String {
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
fn the_comparison_cites_the_external_figure_only_next_to_a_harvest_capacity_number() {
    let page = read("docs/comparison.md");
    let cited: Vec<String> = blocks(&page)
        .into_iter()
        .filter(|b| is_external_citation(b))
        .collect();
    assert!(
        !cited.is_empty(),
        "docs/comparison.md must cite the external capacity figure next to a Harvest number"
    );
    for block in cited {
        let lower = block.to_lowercase();
        for cell in PUBLISHED_CAPACITY {
            let number = comparison_number(cell.sustainable_per_sec);
            let arm = format!("`{}`", BenchScenario::Capacity(cell.arm).as_str());
            // The number must sit on the arm's own line, so two swapped
            // rows fail.
            assert!(
                block
                    .lines()
                    .any(|line| line.contains(&arm) && contains_number(line, &number)),
                "a block that cites the external figure must carry the Harvest number {number} \
                 of {arm}:\n{block}"
            );
        }
        for hardware in [EXTERNAL_HARDWARE, REFERENCE_HARDWARE] {
            assert!(
                lower.contains(hardware),
                "a block that cites the external figure must state `{hardware}`:\n{block}"
            );
        }
        let link = PUBLISHED_CAPACITY_FILE
            .strip_prefix("docs/")
            .expect("the capacity results live under docs/");
        assert!(
            block.contains(link),
            "a block that cites the external figure must link {link}:\n{block}"
        );
    }
}

#[test]
fn a_number_matches_only_as_a_whole_number() {
    assert!(contains_number("| 149.0 |", "149.0"));
    assert!(contains_number("ends at 149.0.", "149.0"));
    assert!(!contains_number("149.00", "149.0"));
    assert!(!contains_number("1149.0", "149.0"));
    assert!(!contains_number("10.00", "0.00"));
}

#[test]
fn only_a_block_that_names_the_engine_and_a_figure_is_a_citation() {
    assert!(is_external_citation("DBOS reports 20K workflows/s."));
    assert!(!is_external_citation(
        "Temporal Cloud handles millions of actions."
    ));
    assert!(!is_external_citation("A 20kB payload limit."));
    assert!(!is_external_citation("Choose DBOS if you want TypeScript."));
}

#[test]
fn the_benchmarks_page_states_each_published_rate() {
    let page = read("docs/benchmarks.md");
    for cell in PUBLISHED_CAPACITY {
        let stated = format!(
            "**{}** `{}`",
            comparison_number(cell.sustainable_per_sec),
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

#[test]
fn blocks_split_on_whitespace_only_lines() {
    assert_eq!(blocks("a\nb\n  \nc\n\n\nd"), vec!["a\nb", "c", "d"]);
}
