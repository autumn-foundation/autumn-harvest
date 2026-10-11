//! Guards for `docs/rnd/speculative-execution-spike.md` (issue #2011).
//!
//! The report gives measured latency, DST results and a verdict. These
//! guards keep its data true: the sections, the verdict, the cited tests,
//! the model durations and the CI step. They do not judge the reasoning.
//!
//! Pure: no database, no async.

use super::docs_guard_support::{check_stanza, prose_sentences, section, step_stanza};

const REPORT: &str = "docs/rnd/speculative-execution-spike.md";
const MODEL: &str = "autumn-harvest/src/dst/speculate.rs";
const MODEL_TESTS: &str = "autumn-harvest/tests/dst/speculate.rs";
const FILTER: &str = "--test integration speculative_execution_docs::";
const MAX_SENTENCE_WORDS: usize = 25;

/// The sections of the report, in order.
const SECTIONS: &[&str] = &[
    "## Decision summary",
    "## The question",
    "## Method",
    "## Measured latency",
    "## DST results",
    "## Asymmetric logging",
    "## Go / no-go",
    "## Known limits",
    "## Reproduce",
];

/// Pages that must link the report.
const LINKED_FROM: &[&str] = &["docs/testing/simulation.md", "docs/benchmarks.md"];

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate has a parent directory")
        .to_path_buf()
}

fn read(path: &str) -> String {
    std::fs::read_to_string(repo_root().join(path))
        .unwrap_or_else(|error| panic!("issue #2011: could not read {path}: {error}"))
        .replace("\r\n", "\n")
}

#[test]
fn report_has_each_section_in_order() {
    let report = read(REPORT);
    let mut from = 0;
    for heading in SECTIONS {
        let needle = format!("\n{heading}\n");
        let at = report[from..]
            .find(&needle)
            .unwrap_or_else(|| panic!("{REPORT} needs `{heading}` after the sections before it"));
        from += at + needle.len();
    }
}

#[test]
fn report_states_its_status_and_revision() {
    let report = read(REPORT);
    let head: String = report.lines().take(30).collect::<Vec<_>>().join("\n");
    assert!(head.contains("> **Status:"), "{REPORT} needs a status line");
    let revision = section(&report, "**Audited revision:** `", "`")
        .expect("the report names its audited revision");
    assert!(
        revision.len() >= 7 && revision.chars().all(|c| c.is_ascii_hexdigit()),
        "the audited revision must be a commit hash: {revision:?}"
    );
}

/// The word after `**Verdict: ` in `text`, such as `no-go`.
fn verdict(text: &str) -> Option<&str> {
    let rest = &text[text.find("**Verdict: ")? + "**Verdict: ".len()..];
    rest.split(['*', ' '])
        .next()
        .map(|word| word.trim_end_matches('.'))
}

#[test]
fn report_reaches_one_explicit_verdict() {
    let report = read(REPORT);
    let section = section(&report, "\n## Go / no-go\n", "\n## ").expect("a verdict section");
    let summary = section_of(&report, "## Decision summary");
    let stated = verdict(section).expect("the Go / no-go section states `**Verdict: `");
    assert!(
        matches!(stated, "go" | "no-go"),
        "the verdict must be go or no-go: {stated:?}"
    );
    assert_eq!(
        verdict(&summary),
        Some(stated),
        "the decision summary and the Go / no-go section must state one verdict"
    );
}

/// The body of the section under `heading`.
fn section_of(report: &str, heading: &str) -> String {
    section(report, &format!("\n{heading}\n"), "\n## ")
        .unwrap_or_else(|| panic!("{REPORT} needs `{heading}`"))
        .to_string()
}

/// The backticked names in the first cell of each table row of `text`.
fn first_cells(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|line| line.strip_prefix("| `"))
        .filter_map(|rest| rest.split('`').next())
        .map(str::to_string)
        .collect()
}

#[test]
fn every_cited_test_exists() {
    let report = read(REPORT);
    let tests = read(MODEL_TESTS);
    let cited = first_cells(&section_of(&report, "### Cited tests"));
    assert!(
        cited.len() >= 15,
        "the report cites only {} tests",
        cited.len()
    );
    for name in &cited {
        assert!(
            tests.contains(&format!("fn {name}(")),
            "{REPORT} cites `{name}`, but {MODEL_TESTS} has no such test"
        );
    }
}

#[test]
fn the_invariants_table_matches_the_model() {
    let report = read(REPORT);
    let model = read(MODEL);
    let block = section(&model, "    SpecInvariant {\n", "\n    }\n").expect("the invariant list");
    let names: Vec<&str> = block
        .lines()
        .filter_map(|line| line.trim().split_once(" => "))
        .map(|(name, _)| name)
        .collect();
    let table = first_cells(&section_of(&report, "### Invariants"));
    assert_eq!(
        table, names,
        "the invariants table must list SpecInvariant in order"
    );
}

#[test]
fn the_measured_p50_matches_the_model_tests() {
    let tests = read(MODEL_TESTS);
    let value = section(&tests, "const MEASURED_CHAIN_P50_US: u64 = ", ";")
        .expect("the model tests pin the measured p50");
    let micros: u64 = value.replace('_', "").parse().expect("a number");
    let stated = format!("{}.{:02} ms", micros / 1_000, micros % 1_000 / 10);
    assert!(
        read(REPORT).contains(&stated),
        "{REPORT} must state the measured p50 as {stated}"
    );
}

#[test]
fn every_reproduce_command_names_a_real_test() {
    let report = read(REPORT);
    let tests = read(MODEL_TESTS);
    let reproduce = section_of(&report, "## Reproduce");
    for (at, _) in reproduce.match_indices("speculate::") {
        let name: String = reproduce[at + "speculate::".len()..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() {
            assert!(
                tests.contains(&format!("fn {name}(")),
                "the Reproduce section names `{name}`, which is not a test"
            );
        }
    }
}

/// The calibrated durations in the report are the ones the model uses.
#[test]
fn bench_timing_matches_the_model() {
    let model = read(MODEL);
    let body = section(&model, "pub const BENCH: Self = Self {\n", "    };")
        .expect("the model defines Timing::BENCH");
    let report = read(REPORT);
    for line in body.lines().map(str::trim).filter(|line| !line.is_empty()) {
        assert!(
            report.contains(line),
            "{REPORT} must show `{line}` from Timing::BENCH"
        );
    }
}

#[test]
fn the_probe_is_documented() {
    let support = read("autumn-harvest/tests/integration/e2e_bench_support.rs");
    assert!(support.contains("\"HARVEST_BENCH_COMMIT_PROBE\""));
    for page in [REPORT, "docs/benchmarks.md"] {
        assert!(
            read(page).contains("HARVEST_BENCH_COMMIT_PROBE"),
            "{page} must name HARVEST_BENCH_COMMIT_PROBE"
        );
    }
}

#[test]
fn report_is_linked_from_the_corpus() {
    for page in LINKED_FROM {
        assert!(
            read(page).contains("rnd/speculative-execution-spike.md"),
            "{page} must link the report"
        );
    }
}

#[test]
fn a_changelog_fragment_exists() {
    let dir = repo_root().join("docs/changelog.d");
    let found = std::fs::read_dir(&dir)
        .expect("docs/changelog.d exists")
        .filter_map(Result::ok)
        .any(|entry| {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            name.starts_with("issue-2011-") && path.extension().is_some_and(|ext| ext == "md")
        });
    assert!(found, "issue #2011 needs docs/changelog.d/issue-2011-*.md");
}

#[test]
fn prose_sentences_stay_short() {
    let long: Vec<String> = prose_sentences(&read(REPORT))
        .into_iter()
        .filter(|sentence| sentence.split_whitespace().count() > MAX_SENTENCE_WORDS)
        .collect();
    assert!(
        long.is_empty(),
        "{REPORT} has prose sentences over {MAX_SENTENCE_WORDS} words. Split each one:\n{}",
        long.join("\n")
    );
}

/// A docs-only change skips the `test` matrix, so `lint` must run these guards.
#[test]
fn guards_run_on_docs_only_changes() {
    let workflow = read(".github/workflows/ci.yml");
    let lint = workflow
        .find("\n  lint:")
        .expect("ci.yml must define `lint`");
    let test = workflow
        .find("\n  test:")
        .expect("ci.yml must define `test`");
    let stanza = step_stanza(&workflow[lint..test], FILTER).unwrap_or_else(|| {
        panic!(
            "the `lint` job in ci.yml must run these guards. Add this step:\n\
             \x20     - name: Run docs/rnd/speculative-execution-spike.md guards (also on \
             docs-only PRs)\n\
             \x20       run: \"cargo test -p autumn-harvest --no-default-features --features \
             testing {FILTER}\""
        )
    });
    check_stanza(stanza, FILTER).unwrap_or_else(|reason| panic!("{reason}. Stanza:\n{stanza}"));
}
