//! Guards the `docs/calendars.md` doctest harness (issue #1783).
//!
//! The harness is `CalendarsDocSnippets` in `calendar.rs`. A docs-only PR
//! skips the `test` matrix, so the `lint` job must run the doctests. These
//! guards fail when the harness or the CI step goes away.

use std::path::{Path, PathBuf};

const FILTER: &str = "--doc calendar::CalendarsDocSnippets";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read_normalized(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// The harness must include the doc, or the CI filter matches zero tests.
///
/// `cargo test` exits 0 when a filter matches nothing. Deleting the harness
/// would therefore pass CI without a failure.
#[test]
fn doctest_harness_includes_calendars_doc() {
    let source = read_normalized(&repo_root().join("autumn-harvest/src/calendar.rs"));
    let include = source
        .find("#[doc = include_str!(\"../../docs/calendars.md\")]")
        .expect("calendar.rs must include docs/calendars.md as a doctest");
    let tail = &source[include..];
    let item = tail.lines().nth(1).unwrap_or_default();
    assert!(
        item.contains("struct CalendarsDocSnippets"),
        "the include must annotate `struct CalendarsDocSnippets`, found: {item}"
    );
    assert!(
        source[..include].trim_end().ends_with("#[cfg(doctest)]"),
        "the harness must be gated on `cfg(doctest)`"
    );
}

/// The CI step must live in the ungated `lint` job, with no `if:`.
#[test]
fn doctest_step_runs_on_docs_only_changes() {
    let workflow = read_normalized(&repo_root().join(".github/workflows/ci.yml"));
    let step_at = workflow
        .find(FILTER)
        .expect("ci.yml must run the calendars.md doctests");
    let line_start = workflow[..step_at].rfind('\n').map_or(0, |i| i + 1);
    assert!(
        workflow[line_start..].trim_start().starts_with("run:"),
        "the doctest invocation must be a step `run:` line"
    );

    let lint_start = workflow
        .find("\n  lint:")
        .expect("ci.yml must define a `lint` job");
    let test_start = workflow
        .find("\n  test:")
        .expect("ci.yml must define a `test` job");
    assert!(
        step_at > lint_start && step_at < test_start,
        "the doctest step must live in the ungated `lint` job"
    );

    let step_name = workflow[..step_at]
        .rfind("\n      - name:")
        .expect("the doctest step must have a name");
    assert!(
        !workflow[step_name..step_at].contains("\n        if:"),
        "the doctest step must not have an `if:` condition"
    );
}
