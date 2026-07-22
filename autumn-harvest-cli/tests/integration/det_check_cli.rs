//! Integration tests for the `harvest det-check` subcommand (issue #778).
//!
//! Black-box tests over the crate's public det-check surface: they build small
//! source trees on disk and drive the pure report/format/gate helpers and
//! `run_det_check` end to end.

use std::path::PathBuf;

use autumn_harvest_cli::{
    DetCheckFormat, det_check_gate, det_check_json, det_check_report_for_paths,
    format_det_findings_text, format_det_suppressions_list, run_det_check,
};

/// Writes a two-file source tree where a workflow reaches a clock-reading
/// first-party helper **in its own module** (same file `a.rs`); `b.rs` is an
/// unrelated clean file. One-hop resolution is same-module only, so the helper
/// must share the workflow's module (issue #778, Codex r5); the second file
/// exercises the multi-file `check_paths` index (dedup / shared scan).
fn transitive_tree() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("a.rs"),
        "\
#[workflow]
async fn cross_wf(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = shared_time();
    Ok(())
}

fn shared_time() -> i64 {
    chrono::Utc::now().timestamp()
}
",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("b.rs"),
        "\
pub fn unrelated() -> i64 {
    0
}
",
    )
    .unwrap();
    dir
}

#[test]
fn transitive_violation_is_reported_in_text_and_json() {
    let dir = transitive_tree();
    let report =
        det_check_report_for_paths(&[dir.path().to_path_buf()]).expect("report should build");

    let text = format_det_findings_text(&report);
    assert!(
        text.contains("DET001"),
        "text output must name the rule: {text}"
    );
    assert!(
        text.contains("in helper `shared_time` reached from workflow `cross_wf`"),
        "text output must name the helper and entry workflow: {text}"
    );

    let json = det_check_json(&report).expect("json should serialize");
    assert!(json.contains("\"rule_id\""), "{json}");
    assert!(json.contains("DET001"), "{json}");
    assert!(json.contains("\"via_helper\": \"shared_time\""), "{json}");

    assert!(
        det_check_gate(&report, false).is_some(),
        "a hard-blocker must trip the gate"
    );
}

#[test]
fn clean_tree_produces_no_findings_and_run_returns_ok() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("clean.rs"),
        "\
#[workflow]
async fn clean_wf(ctx: &WorkflowContext) -> Result<(), String> {
    ctx.timer(\"t\", 1).await?;
    let _ = pure_helper(2);
    Ok(())
}

fn pure_helper(x: i64) -> i64 {
    x * 2
}
",
    )
    .unwrap();

    let report =
        det_check_report_for_paths(&[dir.path().to_path_buf()]).expect("report should build");
    assert!(
        report.findings.is_empty(),
        "clean tree must have no findings"
    );
    assert!(det_check_gate(&report, false).is_none());

    // End-to-end run returns Ok (exit 0).
    let result = run_det_check(
        &[dir.path().to_path_buf()],
        DetCheckFormat::Text,
        false,
        false,
    );
    assert!(
        result.is_ok(),
        "clean tree run must exit Ok, got: {result:?}"
    );
}

#[test]
fn deny_warnings_gates_on_warning_only_tree() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("warn.rs"),
        "\
#[workflow]
async fn warn_wf(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = std::process::id();
    Ok(())
}
",
    )
    .unwrap();

    let paths: Vec<PathBuf> = vec![dir.path().to_path_buf()];

    // Without --deny-warnings a warning-only tree passes.
    assert!(run_det_check(&paths, DetCheckFormat::Text, false, false).is_ok());

    // With --deny-warnings it gates.
    let gated = run_det_check(&paths, DetCheckFormat::Text, true, false);
    assert!(
        gated.is_err(),
        "--deny-warnings must gate on a warning, got: {gated:?}"
    );
}

#[test]
fn list_suppressions_prints_active_suppressions_and_exits_ok() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("sup.rs"),
        "\
#[workflow]
async fn sup_wf(ctx: &WorkflowContext) -> Result<(), String> {
    // harvest-suppress: DET001 \"timestamp comes from the signal payload\"
    let _ = std::time::SystemTime::now();
    Ok(())
}
",
    )
    .unwrap();

    let report =
        det_check_report_for_paths(&[dir.path().to_path_buf()]).expect("report should build");

    // The suppressed violation is not a finding, but is audited.
    assert!(
        report.findings.is_empty(),
        "suppressed violation must not be a finding"
    );
    let listing = format_det_suppressions_list(&report);
    assert!(listing.contains("DET001"), "{listing}");
    assert!(
        listing.contains("timestamp comes from the signal payload"),
        "{listing}"
    );

    // --list-suppressions mode exits Ok.
    let result = run_det_check(
        &[dir.path().to_path_buf()],
        DetCheckFormat::Text,
        false,
        true,
    );
    assert!(
        result.is_ok(),
        "list-suppressions must exit Ok, got: {result:?}"
    );
}

#[test]
fn hard_blocker_run_returns_findings_error() {
    let dir = transitive_tree();
    let result = run_det_check(
        &[dir.path().to_path_buf()],
        DetCheckFormat::Json,
        false,
        false,
    );
    match result {
        Err(err) => assert_eq!(err.exit_code(), 1, "det-check gate must exit 1"),
        Ok(()) => panic!("a hard-blocker tree must return Err"),
    }
}

#[test]
fn missing_path_is_a_read_error() {
    let result =
        det_check_report_for_paths(&[PathBuf::from("/nonexistent/definitely/not/here.rs")]);
    assert!(result.is_err(), "a missing path must surface a read error");
}

// BOUNDARY (issue #778, Codex r4/r5): the changed-files CI pattern
// `det-check f1.rs f2.rs` passes each file as a SEPARATE argument. `check_paths`
// still collects them into one shared index (dedup / robustness stand), but
// one-hop resolution is same-module ONLY — so a helper defined in a DIFFERENT
// file is deliberately NOT resolved (a documented safe false-negative). Never
// resolving cross-module ends the r4/r5 false-positive family; for a CI gate a
// false positive on innocent code is the worse outcome. #386 is body-only and
// also misses these transitive skips; the backstop is runtime replay
// (`WorkflowReplayer` / live `HistoryMatcher`) plus manual review, not #386.
#[test]
fn two_separate_file_args_do_not_resolve_cross_file_transitive() {
    let dir = tempfile::tempdir().expect("tempdir");
    let a = dir.path().join("a.rs");
    let b = dir.path().join("b.rs");
    std::fs::write(
        &a,
        "\
#[workflow]
async fn cross_wf(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = shared_time();
    Ok(())
}
",
    )
    .unwrap();
    std::fs::write(
        &b,
        "\
pub fn shared_time() -> i64 {
    chrono::Utc::now().timestamp()
}
",
    )
    .unwrap();
    let report = det_check_report_for_paths(&[a, b]).expect("report should build");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET001"),
        "a cross-file helper across two file args must NOT be resolved \
         (same-module-only); documented safe false-negative, got: {report:?}"
    );
}

// FIX #3 (P2-2): overlapping arguments (a directory plus a file inside it) must
// not double-count.
#[test]
fn overlapping_dir_and_file_args_do_not_double_count() {
    let dir = transitive_tree();
    let b = dir.path().join("b.rs");
    let report =
        det_check_report_for_paths(&[dir.path().to_path_buf(), b]).expect("report should build");
    assert_eq!(
        report
            .findings
            .iter()
            .filter(|f| f.rule_id == "DET001")
            .count(),
        1,
        "a directory plus a file inside it must yield one finding, got: {report:?}"
    );
}

// Codex P1 (issue #778): a `#[workflow]` declared inside a Rust module must be
// scanned end to end — a module-scoped workflow with a direct hard-blocker must
// trip the gate, not silently pass.
#[test]
fn module_scoped_workflow_direct_violation_trips_the_gate() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("workflows.rs"),
        "\
mod workflows {
    #[workflow]
    pub async fn nested(ctx: &WorkflowContext) -> Result<(), String> {
        let _ = chrono::Utc::now();
        Ok(())
    }
}
",
    )
    .unwrap();
    let report =
        det_check_report_for_paths(&[dir.path().to_path_buf()]).expect("report should build");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET001")
        .unwrap_or_else(|| panic!("module-scoped workflow violation must be caught: {report:?}"));
    assert_eq!(finding.workflow_name.as_deref(), Some("nested"));
    assert!(
        det_check_gate(&report, false).is_some(),
        "a module-scoped hard-blocker must trip the gate: {report:?}"
    );
}
