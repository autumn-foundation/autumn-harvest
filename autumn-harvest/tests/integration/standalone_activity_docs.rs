//! Docs guard for the standalone-activity decision (issue #1987).
//!
//! Issue #1987 is done when two things exist: a measurement of the one-step
//! workflow overhead, and a recorded decision. These tests pin both. They
//! also check that the decision follows the pre-registered line in
//! `DESIGN-1987.md` §0.6, read from the published table. The §0.4 verdict
//! must stay on record too.
//!
//! Pure: no database, no async. Runs on every OS.

use std::path::{Path, PathBuf};

use super::standalone_activity_support::{
    ADR_DOC, ARM_A, ARM_B, ARM_C, ARM_D, ARMS, ARTIFACT_DIR, Arm, BUILD_LINE, GUIDE_DOC,
    GUIDE_HEADING, PERF_DOC, VERDICT_BUILD, VERDICT_DOCUMENT,
};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate directory has a parent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} must exist and be readable: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// The body of the `##` section that `heading` opens, up to the next `##`.
fn section<'a>(doc: &'a str, heading: &str) -> &'a str {
    let start = doc
        .find(&format!("\n{heading}\n"))
        .unwrap_or_else(|| panic!("the doc has no `{heading}` section"))
        + heading.len()
        + 2;
    let rest = &doc[start..];
    let end = rest.find("\n## ").unwrap_or(rest.len());
    &rest[..end]
}

/// The data rows of the table whose header line starts with `header`.
fn table_rows(doc: &str, header: &str) -> Vec<Vec<String>> {
    let mut lines = doc.lines().skip_while(|l| !l.starts_with(header));
    assert!(
        lines.next().is_some(),
        "the doc has no table that starts with `{header}`"
    );
    lines
        .skip(1)
        .take_while(|l| l.starts_with('|'))
        .map(|l| {
            l.trim_matches('|')
                .split('|')
                .map(|c| c.trim().replace(['*', '`'], ""))
                .collect()
        })
        .collect()
}

fn number(cell: &str) -> f64 {
    cell.trim_end_matches('x')
        .replace(',', "")
        .parse()
        .unwrap_or_else(|e| panic!("`{cell}` is not a number: {e}"))
}

fn row<'a>(rows: &'a [Vec<String>], label: &str) -> &'a [String] {
    rows.iter()
        .find(|r| r[0] == label)
        .unwrap_or_else(|| panic!("the table has no `{label}` row"))
}

/// Per-job rows written and WAL bytes, read from the cost table.
fn cost(doc: &str, arm: Arm) -> (f64, f64) {
    let rows = table_rows(doc, "| Arm | Rows written |");
    let r = row(&rows, arm.label);
    (number(&r[1]), number(&r[2]))
}

/// The verdict at [`BUILD_LINE`] for arm B against `floor`.
fn verdict_against(doc: &str, floor: Arm) -> &'static str {
    let (b_rows, b_wal) = cost(doc, ARM_B);
    let (f_rows, f_wal) = cost(doc, floor);
    if b_rows / f_rows <= BUILD_LINE && b_wal / f_wal <= BUILD_LINE {
        VERDICT_DOCUMENT
    } else {
        VERDICT_BUILD
    }
}

/// The verdict that decides: arm B against the realistic floor (§0.6).
fn the_verdict(doc: &str) -> &'static str {
    verdict_against(doc, ARM_D)
}

#[test]
fn the_perf_page_publishes_the_asserted_structure() {
    let doc = read(PERF_DOC);
    let rows = table_rows(&doc, "| Arm | Shape | Events |");
    for arm in ARMS {
        let r = row(&rows, arm.label);
        let published: Vec<i64> = r[2..5]
            .iter()
            .map(|c| {
                c.parse()
                    .unwrap_or_else(|e| panic!("`{c}` is not a count: {e}"))
            })
            .collect();
        assert_eq!(
            published,
            vec![arm.events, arm.task_rows, arm.claims],
            "arm {} publishes events, task rows and claims that the harness does not assert",
            arm.label
        );
    }
}

#[test]
fn the_ratio_table_matches_the_cost_table() {
    let doc = read(PERF_DOC);
    let ratios = table_rows(&doc, "| Ratio | Rows written |");
    for (arm, floor) in [(ARM_A, ARM_C), (ARM_B, ARM_C), (ARM_B, ARM_D)] {
        let (rows, wal) = cost(&doc, arm);
        let (f_rows, f_wal) = cost(&doc, floor);
        let label = format!("{} / {}", arm.label, floor.label);
        let r = row(&ratios, &label);
        for (published, want) in [(&r[1], rows / f_rows), (&r[2], wal / f_wal)] {
            assert_eq!(
                published,
                &format!("{want:.2}x"),
                "the `{label}` ratio does not match the cost table"
            );
        }
    }
}

#[test]
fn the_cost_table_is_the_captured_one() {
    let doc = read(PERF_DOC);
    let capture = read(&format!("{ARTIFACT_DIR}/capture.txt"));
    let rows = table_rows(&doc, "| Arm | Rows written |");
    for r in &rows {
        let line = format!("| {} |", r.join(" | "));
        assert!(
            capture.contains(&line),
            "the published cost row `{line}` is not in the captured evidence"
        );
    }
    assert_eq!(rows.len(), ARMS.len(), "the cost table has one row per arm");
}

#[test]
fn the_adr_records_the_verdict_that_the_line_gives() {
    let doc = read(PERF_DOC);
    let adr = read(ADR_DOC);
    let verdict = the_verdict(&doc);
    let other = if verdict == VERDICT_DOCUMENT {
        VERDICT_BUILD
    } else {
        VERDICT_DOCUMENT
    };
    let decision = section(&adr, "## Decision");
    assert!(
        decision.contains(verdict),
        "the pre-registered line gives `{verdict}`, but the ADR decision does not say so"
    );
    assert!(
        !decision.contains(other),
        "the ADR decision also states the verdict that the line rejects: `{other}`"
    );
    assert!(
        doc.contains(verdict),
        "the perf page must state the verdict `{verdict}`"
    );
}

#[test]
fn the_first_line_verdict_stays_on_record() {
    let doc = read(PERF_DOC);
    let adr = read(ADR_DOC);
    let first = verdict_against(&doc, ARM_C);
    for (name, text) in [("perf page", &doc), ("ADR", &adr)] {
        assert!(
            text.contains(first),
            "the {name} must keep the §0.4 verdict against arm C on record: `{first}`"
        );
    }
}

#[test]
fn guards_run_on_docs_only_changes() {
    let workflow = read(".github/workflows/ci.yml");
    let invocation = "--test integration standalone_activity_docs::";
    let lint = workflow
        .find("\n  lint:")
        .expect("ci.yml must define a `lint` job");
    let test = workflow
        .find("\n  test:")
        .expect("ci.yml must define a `test` job");
    let at = workflow
        .find(invocation)
        .expect("a ci.yml step must run these guards");
    assert!(
        lint < at && at < test,
        "the guard step must live in the ungated `lint` job: the `test` matrix skips \
         docs-only PRs"
    );
    let stanza_start = workflow[..at]
        .rfind("\n      - name:")
        .expect("the guard runs from a named step");
    assert!(
        !workflow[stanza_start..at].contains("\n        if:"),
        "the guard step must run unconditionally"
    );
}

#[test]
fn the_adr_is_accepted_and_cites_its_evidence() {
    let adr = read(ADR_DOC);
    assert!(
        section(&adr, "## Status").contains("Accepted (issue #1987)"),
        "the ADR status must be `Accepted (issue #1987)`"
    );
    for heading in ["## Context", "## Consequences"] {
        section(&adr, heading);
    }
    let perf_link = Path::new(PERF_DOC)
        .file_name()
        .and_then(|n| n.to_str())
        .expect("the perf page has a file name");
    assert!(
        adr.contains(&format!("(../{perf_link})")),
        "the ADR must link the measurement page"
    );
    assert!(
        adr.contains(&format!("{BUILD_LINE:.1}x")),
        "the ADR must quote the pre-registered line"
    );
}

#[test]
fn the_adr_number_is_not_shared() {
    let dir = repo_root().join("docs/adr");
    let number = Path::new(ADR_DOC)
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.get(..5))
        .expect("the ADR file name starts with its number");
    let holders = std::fs::read_dir(&dir)
        .expect("docs/adr is readable")
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().starts_with(number))
        .count();
    assert_eq!(
        holders, 1,
        "ADR number `{number}` must name exactly one file"
    );
}

#[test]
fn the_guide_shows_the_pattern_and_links_the_decision() {
    let guide = read(GUIDE_DOC);
    let body = section(&guide, GUIDE_HEADING);
    assert!(
        body.contains("```rust"),
        "the guide section must show the pattern in code"
    );
    assert!(
        body.contains("execute_local_activity"),
        "the guide section must name the local-activity fast path"
    );
    assert!(
        body.contains("(../adr/0006-standalone-activity.md)"),
        "the guide section must link the ADR"
    );
}

#[test]
fn the_perf_page_names_its_harness() {
    let doc = read(PERF_DOC);
    for needle in [
        "autumn-harvest/tests/integration/standalone_activity_overhead_perf.rs",
        "zz_capture_standalone_activity_overhead_evidence",
        "DESIGN-1987.md",
    ] {
        assert!(doc.contains(needle), "the perf page must name `{needle}`");
    }
}
