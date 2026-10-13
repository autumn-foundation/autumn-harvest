//! Guards the typed-snapshot spike write-up (issue #2013).
//!
//! `docs/rnd/typed-state-snapshots.md` is a go / no-go document. These tests
//! check its facts, not its argument:
//!
//! - It has the sections and the verdict that the issue asks for.
//! - It cites the gate data, and the counter it cites exists.
//! - Each prototype test that it names exists.

use std::path::{Path, PathBuf};

use autumn_harvest::telemetry::METRIC_WORKFLOW_RESIDENT;

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

fn report() -> String {
    read("docs/rnd/typed-state-snapshots.md")
}

/// The sections that the issue's done-when list asks for.
const REQUIRED_SECTIONS: &[&str] = &[
    "## 1. The question",
    "## 2. The gate: resident hit rate",
    "## 3. Design sketch",
    "## 4. What the prototype shows",
    "## 5. Verdict",
];

#[test]
fn the_report_has_every_required_section() {
    let body = report();
    for heading in REQUIRED_SECTIONS {
        assert!(
            body.lines().any(|line| line == *heading),
            "the report lacks the section {heading:?}"
        );
    }
}

/// The text of the section that starts at `heading`, up to the next `## `.
fn section(body: &str, heading: &str) -> String {
    let start = body
        .find(&format!("\n{heading}\n"))
        .unwrap_or_else(|| panic!("the report lacks {heading:?}"));
    let rest = &body[start + heading.len() + 2..];
    rest.find("\n## ")
        .map_or(rest, |end| &rest[..end])
        .to_owned()
}

#[test]
fn the_report_states_one_verdict() {
    let body = report();
    let verdicts: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix("**Verdict: "))
        .collect();
    assert_eq!(
        verdicts.len(),
        1,
        "the report must state exactly one verdict"
    );
    assert!(
        verdicts[0].starts_with("go") || verdicts[0].starts_with("no-go"),
        "the verdict must start with go or no-go: {}",
        verdicts[0]
    );
}

#[test]
fn the_gate_section_cites_the_counter_and_both_measurements() {
    let gate = section(&report(), "## 2. The gate: resident hit rate");
    for needle in [
        format!("`{METRIC_WORKFLOW_RESIDENT}`"),
        "#2007".to_owned(),
        "e2e bench".to_owned(),
        "agent loop".to_owned(),
    ] {
        assert!(gate.contains(&needle), "section 2 must cite {needle}");
    }
}

#[test]
fn every_test_that_section_4_names_exists() {
    let claims = section(&report(), "## 4. What the prototype shows");
    let spike = read("autumn-harvest/tests/integration/typed_snapshot_spike_tests.rs");
    let named: Vec<&str> = claims
        .split('`')
        .skip(1)
        .step_by(2)
        .filter(|token| {
            token.contains('_')
                && token
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        })
        .collect();
    assert!(
        named.len() >= 7,
        "section 4 must cite each prototype test, found {named:?}"
    );
    for test in named {
        assert!(
            spike.contains(&format!("fn {test}(")),
            "section 4 cites `{test}`, but typed_snapshot_spike_tests.rs has no such test"
        );
    }
}
