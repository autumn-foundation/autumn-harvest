//! Guards for the sandboxed code activity docs.
//!
//! `docs/sandbox-activities.md` is the guide, and
//! `DESIGN-sandbox-activities.md` is the design record. These guards read the
//! sources and check that the docs agree with them:
//!
//! * The guide has one failure row per sandbox error type, and each retry
//!   column matches the doc comment of the constant.
//! * The guide names the patched Firecracker minimums that the posture check
//!   enforces, and the kernel arguments that the config sets.
//! * Each acceptance criterion of the design record has evidence, and each
//!   test that the evidence cites exists.
//! * Each test command of the guide runs in CI.
//! * Each prose sentence of both pages has 25 words or fewer.
//!
//! Pure: no database, no async, no feature gate. Runs on every OS.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use super::docs_guard_support::{check_stanza, prose_sentences, section, step_stanza};

const GUIDE: &str = "docs/sandbox-activities.md";
const DESIGN: &str = "DESIGN-sandbox-activities.md";
const FILTER: &str = "--test integration sandbox_activities_docs::";
const MAX_SENTENCE_WORDS: usize = 25;

/// The sources whose test functions the design record can cite.
const TEST_SOURCES: &[&str] = &[
    "autumn-harvest/src/sandbox",
    "autumn-harvest-agent/src/sandbox.rs",
    "autumn-harvest-sandbox-guest/src",
    "autumn-harvest/tests/integration/sandbox_activities_docs.rs",
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

/// Each `.rs` file under `relative`, or the file itself.
fn rust_files(relative: &str) -> Vec<PathBuf> {
    let mut pending = vec![repo_root().join(relative)];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            let entries = std::fs::read_dir(&path)
                .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));
            pending.extend(entries.map(|e| e.expect("readable dir entry").path()));
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            files.push(path);
        }
    }
    files
}

/// The names of the functions in the test sources.
fn function_names() -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for source in TEST_SOURCES {
        for file in rust_files(source) {
            let text = std::fs::read_to_string(&file).expect("readable source");
            for line in text.lines().map(str::trim_start) {
                let line = line.strip_prefix("async ").unwrap_or(line);
                if let Some(rest) = line.strip_prefix("fn ") {
                    names.insert(
                        rest.chars()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect(),
                    );
                }
            }
        }
    }
    names
}

/// The backticked names in `text` that look like test functions: snake case
/// with at least three words.
fn cited_tests(text: &str) -> BTreeSet<String> {
    text.split('`')
        .skip(1)
        .step_by(2)
        .filter(|name| {
            name.matches('_').count() >= 2
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        })
        .map(str::to_owned)
        .collect()
}

/// `(error type, retryable)` for each `ERROR_TYPE_SANDBOX_*` constant, read
/// from the doc comment above it.
fn sandbox_error_types() -> Vec<(String, bool)> {
    let source = read("autumn-harvest/src/failure.rs");
    let lines: Vec<&str> = source.lines().collect();
    let mut types = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let Some(rest) = line.strip_prefix("pub const ERROR_TYPE_SANDBOX_") else {
            continue;
        };
        let value = rest
            .split('"')
            .nth(1)
            .unwrap_or_else(|| panic!("cannot parse `{line}`"))
            .to_owned();
        let doc: String = lines[..index]
            .iter()
            .rev()
            .take_while(|l| l.starts_with("///") || l.starts_with("#["))
            .map(|l| l.to_lowercase())
            .collect::<Vec<_>>()
            .join(" ");
        let retryable = !doc.contains("non-retryable");
        assert!(
            doc.contains("retryable"),
            "the doc comment of {value} must say whether it is retryable"
        );
        types.push((value, retryable));
    }
    types
}

fn assert_short_sentences(page: &str) {
    let text = read(page);
    let long: Vec<String> = prose_sentences(&text)
        .into_iter()
        .filter(|s| s.split_whitespace().count() > MAX_SENTENCE_WORDS)
        .collect();
    assert!(
        long.is_empty(),
        "{page} has sentences over {MAX_SENTENCE_WORDS} words:\n  {}",
        long.join("\n  ")
    );
}

#[test]
fn the_guide_has_a_failure_row_per_error_type() {
    let guide = read(GUIDE);
    let failures = section(&guide, "## Failures", "\n## ").expect("the guide has `## Failures`");
    let types = sandbox_error_types();
    assert_eq!(types.len(), 4, "four sandbox error types: {types:?}");
    for (name, retryable) in types {
        let row = failures
            .lines()
            .find(|line| line.starts_with(&format!("| `{name}` |")))
            .unwrap_or_else(|| panic!("{GUIDE} has no failure row for `{name}`"));
        let retry = row.split('|').nth(2).map(str::trim);
        let expected = if retryable { "Yes" } else { "No" };
        assert_eq!(retry, Some(expected), "the retry column of `{name}`");
    }
}

#[test]
fn the_guide_names_the_enforced_firecracker_minimums() {
    let guide = read(GUIDE);
    let posture = read("autumn-harvest/src/sandbox/firecracker/posture.rs");
    for version in ["1.14.4", "1.15.1"] {
        assert!(guide.contains(version), "{GUIDE} must name {version}");
        assert!(
            posture.contains(version),
            "posture.rs must enforce {version}"
        );
    }
    for cve in ["CVE-2026-5747", "CVE-2026-1386"] {
        assert!(guide.contains(cve), "{GUIDE} must name {cve}");
        assert!(posture.contains(cve), "posture.rs must name {cve}");
    }
}

#[test]
fn the_guide_names_the_kernel_arguments_that_the_config_sets() {
    let guide = read(GUIDE);
    let config = read("autumn-harvest/src/sandbox/firecracker/config.rs");
    for arg in ["reboot=k", "panic=1", "8250.nr_uarts=0"] {
        assert!(guide.contains(arg), "{GUIDE} must name {arg}");
        assert!(
            config.contains(&format!("\"{arg}\"")),
            "config.rs must set {arg}"
        );
    }
}

#[test]
fn each_acceptance_criterion_has_evidence_that_exists() {
    let design = read(DESIGN);
    let criteria = section(&design, "## 1. Acceptance criteria", "\n## ")
        .expect("the design record has `## 1. Acceptance criteria`");
    let evidence = section(&design, "## 3. Evidence", "\n## ")
        .expect("the design record has `## 3. Evidence`");
    let ids: Vec<String> = criteria
        .lines()
        .filter_map(|line| line.strip_prefix("| AC"))
        .map(|rest| {
            rest.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
        })
        .filter(|number| !number.is_empty())
        .map(|number| format!("AC{number}"))
        .collect();
    assert!(ids.len() >= 20, "the record lists the criteria: {ids:?}");
    let functions = function_names();
    for id in ids {
        let row = evidence
            .lines()
            .find(|line| line.starts_with(&format!("| {id} |")))
            .unwrap_or_else(|| panic!("{DESIGN} has no evidence row for {id}"));
        let cited = cited_tests(row);
        let missing: Vec<&String> = cited.difference(&functions).collect();
        assert!(
            missing.is_empty(),
            "{id} cites tests that do not exist: {missing:?}"
        );
    }
}

#[test]
fn each_guide_test_command_runs_in_ci() {
    let guide = read(GUIDE);
    let testing = section(&guide, "## Testing", "\n## ").expect("the guide has `## Testing`");
    let workflow = read(".github/workflows/ci.yml");
    let commands: Vec<&str> = testing
        .lines()
        .filter(|line| line.starts_with("cargo test"))
        .collect();
    assert!(commands.len() >= 3, "the guide lists the test commands");
    for command in commands {
        assert!(
            workflow.contains(command),
            "ci.yml must run the guide command `{command}`"
        );
    }
}

#[test]
fn the_pages_keep_sentences_short() {
    assert_short_sentences(GUIDE);
    assert_short_sentences(DESIGN);
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
        panic!("the `lint` job in ci.yml must run these guards with the filter `{FILTER}`")
    });
    check_stanza(stanza, FILTER).unwrap_or_else(|reason| panic!("{reason}. Stanza:\n{stanza}"));
}

#[test]
fn helpers_parse_their_inputs() {
    assert_eq!(
        cited_tests("see `a_long_test_name`, `Short`, `two_words` and `x.rs`"),
        BTreeSet::from(["a_long_test_name".to_owned()])
    );
    let types = sandbox_error_types();
    assert!(types.contains(&("SandboxDenied".to_owned(), false)));
    assert!(types.contains(&("SandboxUnavailable".to_owned(), true)));
}
