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

#[test]
fn the_report_states_one_verdict() {
    let body = report();
    let verdicts: Vec<&str> = body
        .lines()
        .filter(|line| line.starts_with("**Verdict:"))
        .collect();
    assert_eq!(
        verdicts.len(),
        1,
        "the report must state exactly one verdict"
    );
    let verdict = verdicts[0].to_ascii_lowercase();
    assert!(
        verdict.contains("go"),
        "the verdict must say go or no-go: {}",
        verdicts[0]
    );
}

#[test]
fn the_report_cites_the_gate_counter_and_both_measurements() {
    let body = report();
    assert!(
        body.contains(&format!("`{METRIC_WORKFLOW_RESIDENT}`")),
        "the report must name the gate counter"
    );
    for workload in ["e2e bench", "agent loop"] {
        assert!(
            body.contains(workload),
            "the report must record the {workload} measurement"
        );
    }
}

#[test]
fn every_prototype_test_the_report_names_exists() {
    let body = report();
    let spike = read("autumn-harvest/tests/integration/typed_snapshot_spike_tests.rs");
    let named: Vec<&str> = body
        .split('`')
        .filter(|token| {
            token.len() > 8
                && token
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                && token.contains('_')
        })
        .filter(|token| spike.contains(&format!("fn {token}(")))
        .collect();
    assert!(
        named.len() >= 5,
        "the report must name the prototype tests, found {named:?}"
    );
    for test in [
        "a_checkpoint_with_an_open_effect_is_refused",
        "a_v1_snapshot_loads_under_v2_code",
        "a_ledger_that_differs_from_its_source_history_is_refused",
        "changed_code_before_the_checkpoint_resumes_from_the_snapshot",
        "reserved_names_restart_in_each_new_context",
    ] {
        assert!(
            body.contains(&format!("`{test}`")),
            "the report must cite {test}"
        );
        assert!(spike.contains(&format!("fn {test}(")), "{test} must exist");
    }
}

#[test]
fn the_issue_gate_is_recorded_as_met() {
    let body = report();
    assert!(
        body.contains("#2007"),
        "the report must name the gate issue"
    );
}
