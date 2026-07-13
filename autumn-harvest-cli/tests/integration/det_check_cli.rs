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

/// Writes a two-file source tree where a workflow in `a.rs` reaches a
/// clock-reading first-party helper defined in `b.rs`.
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
",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("b.rs"),
        "\
pub fn shared_time() -> i64 {
    chrono::Utc::now().timestamp()
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
