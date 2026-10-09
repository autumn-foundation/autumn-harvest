//! Docs guard for the assay #11 rerun against 0.7.0 (issue #1972).
//!
//! Issue #1972 asks for four records. This suite pins three of them:
//!
//! * a pre-registration, cited by the report;
//! * a result for every registered tree, arm and depth;
//! * a citation on `docs/comparison.md`, in place of its old disclaimer.
//!
//! `benchmarks_docs.rs` pins the fourth, the 0.7.0 headline.
//!
//! Pure: no database, no async. Runs on every OS.

use std::path::{Path, PathBuf};

/// The pre-registration. It is committed before any run.
const PREREGISTRATION: &str =
    "docs/rnd/2026-10-08-harvest-vs-temporal-0.7.0-depth-sweep-preregistration.md";
/// The report, with the numbers and the verdict.
const REPORT: &str = "docs/assays/0014-harvest-vs-temporal-0.7.0-depth-sweep.md";
/// The report's link target, relative to `docs/`.
const REPORT_LINK: &str = "assays/0014-harvest-vs-temporal-0.7.0-depth-sweep.md";

/// The harvest trees the pre-registration names.
///
/// `0aeb887` is the base of PR #2052. `513b7aa` is its head, with the
/// claim-path fix (#1971). `9f444b7` is the `trunk-dev` head when the sweep
/// ran.
const TREES: [&str; 3] = ["0aeb887", "513b7aa", "9f444b7"];
/// The default mode and the best mode.
const HARVEST_ARMS: [&str; 2] = ["postgres", "redis_pg"];
/// The competitor arm, and the server version it ran.
const TEMPORAL_ARM: &str = "temporal_go";
const TEMPORAL_TREE: &str = "1.25.2";
/// The registered backlog depths.
const DEPTHS: [u32; 4] = [250, 500, 1_000, 2_000];
/// Valid repetitions per cell. The pre-registration asks for three.
const REPS: u32 = 3;
/// `grade.py`'s output, which the report's cells table copies.
const GRADED: &str = "docs/assays/apparatus/0014-harvest-vs-temporal-0.7.0/results/graded.md";
/// The header of the cells table, in the report and in `GRADED`.
const CELLS_HEADER: &str = "| tree | arm | depth | mean workflows/sec | per rep | valid reps |";

/// The sentence the comparison page carried before a rerun existed.
const OLD_DISCLAIMER: &str = "No cell on this page claims a throughput or latency comparison";

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

/// Squash a Markdown text to one line, so a phrase can wrap.
fn flat(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The data rows of the first cells table in `doc`.
///
/// Other tables, such as the signals table, use the same row prefix. Reading
/// only the rows under `CELLS_HEADER` keeps their numbers out.
fn cells_rows(doc: &str) -> Vec<&str> {
    doc.lines()
        .skip_while(|line| line.trim() != CELLS_HEADER)
        .skip(2)
        .take_while(|line| line.trim_start().starts_with('|'))
        .map(str::trim)
        .collect()
}

/// The cells after `tree`, `arm` and `depth` in the cells table.
///
/// A row reads `` | `tree` | `arm` | depth | mean | per rep | valid reps |``.
/// The depth carries no thousands separator.
fn cell_row(doc: &str, tree: &str, arm: &str, depth: u32) -> Option<Vec<String>> {
    let prefix = format!("| `{tree}` | `{arm}` | {depth} |");
    let row = cells_rows(doc)
        .into_iter()
        .find(|line| line.starts_with(&prefix))?;
    let rest = row.strip_prefix(&prefix)?;
    Some(
        rest.split('|')
            .map(|cell| cell.trim().trim_matches('*').to_string())
            .filter(|cell| !cell.is_empty())
            .collect(),
    )
}

fn mean_cell(doc: &str, tree: &str, arm: &str, depth: u32) -> Option<f64> {
    cell_row(doc, tree, arm, depth)?.first()?.parse().ok()
}

fn valid_reps_cell(doc: &str, tree: &str, arm: &str, depth: u32) -> Option<u32> {
    cell_row(doc, tree, arm, depth)?.last()?.parse().ok()
}

#[test]
fn the_preregistration_is_committed_and_cited() {
    let prereg = read(PREREGISTRATION);
    assert!(
        prereg.contains("Committed **before**"),
        "the pre-registration must state that it predates every run"
    );
    for arm in HARVEST_ARMS.iter().chain([&TEMPORAL_ARM]) {
        assert!(
            prereg.contains(&format!("`{arm}`")),
            "the pre-registration must name the `{arm}` arm"
        );
    }
    for tree in TREES {
        assert!(
            prereg.contains(&format!("`{tree}`")),
            "the pre-registration must pin tree `{tree}`"
        );
    }
    let report = read(REPORT);
    let file = Path::new(PREREGISTRATION)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("the pre-registration path has a file name");
    assert!(
        report.contains(file),
        "the report must link the pre-registration it answers"
    );
}

#[test]
fn the_report_publishes_every_registered_cell() {
    let report = read(REPORT);
    let mut missing = Vec::new();
    for depth in DEPTHS {
        let cells = TREES
            .iter()
            .flat_map(|tree| HARVEST_ARMS.iter().map(move |arm| (*tree, *arm)))
            .chain([(TEMPORAL_TREE, TEMPORAL_ARM)]);
        for (tree, arm) in cells {
            let mean = mean_cell(&report, tree, arm, depth);
            let reps = valid_reps_cell(&report, tree, arm, depth);
            let ok = mean.is_some_and(|m| m.is_finite() && m > 0.0) && reps == Some(REPS);
            if !ok {
                missing.push(format!("{tree}/{arm}/{depth}"));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "issue #1972 asks for every depth in both modes; these cells lack a positive \
         mean or {REPS} valid reps in {REPORT}: {missing:?}"
    );
}

#[test]
fn the_report_cells_match_the_grader_output() {
    // The report copies `grade.py`'s table by hand. This keeps the copy true.
    let report = read(REPORT);
    let graded = read(GRADED);
    let (copied, source) = (cells_rows(&report), cells_rows(&graded));
    assert!(!source.is_empty(), "{GRADED} must carry a cells table");
    assert_eq!(
        copied, source,
        "the cells table in {REPORT} must match {GRADED} row for row"
    );
}

#[test]
fn the_ledger_lists_the_rerun() {
    let ledger = read("docs/assays/README.md");
    let row = ledger
        .lines()
        .find(|line| line.starts_with("| 14 |"))
        .expect("the assay ledger must carry row 14");
    let file = Path::new(REPORT)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("the report path has a file name");
    assert!(row.contains(file), "ledger row 14 must link {REPORT}");
}

#[test]
fn comparison_cites_the_rerun_instead_of_disclaiming() {
    let page = read("docs/comparison.md");
    assert!(
        page.contains(REPORT_LINK),
        "docs/comparison.md must cite the rerun ({REPORT_LINK})"
    );
    assert!(
        !flat(&page).contains(OLD_DISCLAIMER),
        "docs/comparison.md still disclaims any comparison; issue #1972 asks it to cite the \
         result instead"
    );
}

#[test]
fn benchmarks_points_at_the_rerun_without_naming_the_engine() {
    // `benchmarks_docs.rs` forbids a competitor name on this page (#1309).
    // The report's file name holds one, so the page names the ledger entry
    // and links the ledger.
    let page = flat(&read("docs/benchmarks.md"));
    assert!(
        page.contains("(assays/README.md)") && page.contains("entries 11 and 14"),
        "docs/benchmarks.md must point at ledger entry 14 through the ledger"
    );
    assert!(
        !page.contains(REPORT_LINK),
        "docs/benchmarks.md must not link {REPORT_LINK}: its name holds the engine"
    );
}

#[test]
fn the_cell_parser_reads_only_the_cells_table() {
    let doc = format!(
        "| tree | arm | depth | claims |\n|:--|:--|--:|--:|\n\
         | `a` | `postgres` | 250 | 1754.67 |\n\n\
         {CELLS_HEADER}\n|:--|:--|--:|--:|:--|--:|\n\
         | `a` | `postgres` | 250 | **12.50** | 12 / 13 | 3 |\n\
         | `a` | `postgres` | 2000 | 3.25 | 3 / 3.5 | 2 |\n\n\
         | `a` | `postgres` | 500 | 9.00 | x | 3 |\n"
    );
    assert_eq!(mean_cell(&doc, "a", "postgres", 250), Some(12.5));
    assert_eq!(valid_reps_cell(&doc, "a", "postgres", 250), Some(3));
    assert_eq!(mean_cell(&doc, "a", "postgres", 2_000), Some(3.25));
    assert_eq!(valid_reps_cell(&doc, "a", "postgres", 2_000), Some(2));
    assert_eq!(mean_cell(&doc, "a", "postgres", 500), None);
    assert_eq!(mean_cell(&doc, "a", "redis_pg", 250), None);
}
