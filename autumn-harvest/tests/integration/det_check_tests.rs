//! Red-phase tests for `det_check` — deterministic workflow guardrails.
//!
//! These tests drive the design and must all pass after the green phase.
//! Run with:
//!   cargo test -p autumn-harvest --test `det_check_tests` --no-default-features

use autumn_harvest::det_check::{DetSeverity, check_source};

// ── helpers ────────────────────────────────────────────────────────────────

/// Wrap an expression in a minimal `#[workflow]` function body.
fn wf(body: &str) -> String {
    format!(
        "#[workflow]\nasync fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {{\n    {body}\n    Ok(())\n}}\n"
    )
}

/// Wrap an expression in a `#[activity]` function body (must NOT be flagged).
fn act(body: &str) -> String {
    format!(
        "#[activity(start_to_close = \"30s\")]\nasync fn test_act(_ctx: &ActivityContext) -> Result<(), String> {{\n    {body}\n    Ok(())\n}}\n"
    )
}

// ── DET001: wall-clock time ────────────────────────────────────────────────

#[test]
fn det001_flags_system_time_now() {
    let src = wf("let _t = std::time::SystemTime::now();");
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET001"),
        "expected DET001 finding, got: {report:?}"
    );
}

#[test]
fn det001_flags_instant_now() {
    let src = wf("let _t = std::time::Instant::now();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET001"));
}

#[test]
fn det001_flags_chrono_utc_now() {
    let src = wf("let _t = chrono::Utc::now();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET001"));
}

#[test]
fn det001_flags_chrono_local_now() {
    let src = wf("let _t = chrono::Local::now();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET001"));
}

#[test]
fn det001_is_hard_blocker() {
    let src = wf("let _t = std::time::SystemTime::now();");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET001")
        .unwrap();
    assert!(
        matches!(finding.severity, DetSeverity::Error),
        "DET001 must be Error severity"
    );
}

// ── DET002: randomness ─────────────────────────────────────────────────────

#[test]
fn det002_flags_rand_random() {
    let src = wf("let _n: u64 = rand::random();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET002"));
}

#[test]
fn det002_flags_thread_rng() {
    let src = wf("let _rng = rand::thread_rng();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET002"));
}

#[test]
fn det002_flags_os_rng() {
    let src = wf("let _rng = rand::rngs::OsRng;");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET002"));
}

#[test]
fn det002_is_hard_blocker() {
    let src = wf("let _n: u64 = rand::random();");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET002")
        .unwrap();
    assert!(matches!(finding.severity, DetSeverity::Error));
}

// ── DET003: UUID generation ────────────────────────────────────────────────

#[test]
fn det003_flags_uuid_new_v4() {
    let src = wf("let _id = uuid::Uuid::new_v4();");
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET003"),
        "expected DET003, got: {report:?}"
    );
}

#[test]
fn det003_flags_uuid_new_v7() {
    let src = wf("let _id = Uuid::new_v7(ts);");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET003"));
}

#[test]
fn det003_flags_uuid_now_v7() {
    let src = wf("let _id = Uuid::now_v7();");
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET003"),
        "Uuid::now_v7() uses current system time and must be flagged as DET003"
    );
}

#[test]
fn det003_is_hard_blocker() {
    let src = wf("let _id = uuid::Uuid::new_v4();");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET003")
        .unwrap();
    assert!(matches!(finding.severity, DetSeverity::Error));
}

// ── DET004: environment reads ──────────────────────────────────────────────

#[test]
fn det004_flags_env_var() {
    let src = wf("let _v = std::env::var(\"KEY\").unwrap();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET004"));
}

#[test]
fn det004_flags_env_var_shorthand() {
    let src = wf("let _v = env::var(\"KEY\").unwrap();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET004"));
}

#[test]
fn det004_flags_env_args() {
    let src = wf("let _a = std::env::args().collect::<Vec<_>>();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET004"));
}

#[test]
fn det004_is_hard_blocker() {
    let src = wf("let _v = std::env::var(\"KEY\").unwrap();");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET004")
        .unwrap();
    assert!(matches!(finding.severity, DetSeverity::Error));
}

// ── DET005: process-state reads ────────────────────────────────────────────

#[test]
fn det005_flags_process_id() {
    let src = wf("let _pid = std::process::id();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET005"));
}

#[test]
fn det005_is_warning_not_error() {
    let src = wf("let _pid = std::process::id();");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET005")
        .unwrap();
    assert!(
        matches!(finding.severity, DetSeverity::Warning),
        "DET005 should be Warning severity, not Error"
    );
}

// ── DET006: direct sleep ───────────────────────────────────────────────────

#[test]
fn det006_flags_tokio_sleep() {
    let src = wf("tokio::time::sleep(std::time::Duration::from_secs(1)).await;");
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET006"),
        "expected DET006 finding, got: {report:?}"
    );
}

#[test]
fn det006_flags_thread_sleep() {
    let src = wf("std::thread::sleep(std::time::Duration::from_secs(1));");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET006"));
}

#[test]
fn det006_is_hard_blocker() {
    let src = wf("tokio::time::sleep(std::time::Duration::from_secs(1)).await;");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET006")
        .unwrap();
    assert!(matches!(finding.severity, DetSeverity::Error));
}

// ── DET007: background task spawning ──────────────────────────────────────

#[test]
fn det007_flags_tokio_spawn() {
    let src = wf("tokio::spawn(async { println!(\"oops\"); });");
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET007"),
        "expected DET007 finding, got: {report:?}"
    );
}

#[test]
fn det007_flags_thread_spawn() {
    let src = wf("std::thread::spawn(|| println!(\"oops\"));");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET007"));
}

#[test]
fn det007_flags_spawn_blocking() {
    let src = wf("tokio::task::spawn_blocking(|| heavy_work()).await.unwrap();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET007"));
}

#[test]
fn det007_is_hard_blocker() {
    let src = wf("tokio::spawn(async { });");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET007")
        .unwrap();
    assert!(matches!(finding.severity, DetSeverity::Error));
}

// ── DET008: direct I/O ────────────────────────────────────────────────────

#[test]
fn det008_flags_std_fs_read() {
    let src = wf("let _data = std::fs::read(\"/etc/passwd\").unwrap();");
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET008"),
        "expected DET008 finding, got: {report:?}"
    );
}

#[test]
fn det008_flags_std_fs_write() {
    let src = wf("std::fs::write(\"/tmp/out\", b\"data\").unwrap();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET008"));
}

#[test]
fn det008_flags_file_open() {
    let src = wf("let _f = std::fs::File::open(\"/etc/hosts\").unwrap();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET008"));
}

#[test]
fn det008_flags_reqwest() {
    let src = wf("let _resp = reqwest::get(\"http://example.com\").await.unwrap();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET008"));
}

#[test]
fn det008_flags_tcp_stream() {
    let src = wf("let _conn = std::net::TcpStream::connect(\"127.0.0.1:8080\").unwrap();");
    let report = check_source(&src, "test.rs");
    assert!(report.findings.iter().any(|f| f.rule_id == "DET008"));
}

#[test]
fn det008_is_hard_blocker() {
    let src = wf("let _data = std::fs::read(\"/etc/passwd\").unwrap();");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET008")
        .unwrap();
    assert!(matches!(finding.severity, DetSeverity::Error));
}

// ── clean code produces no findings ───────────────────────────────────────

#[test]
fn clean_workflow_has_no_findings() {
    let src = wf(r#"
    let result = ctx.execute_activity_raw("my_act", serde_json::json!({}), "default").await?;
    ctx.timer("pause", 30).await?;
"#);
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.is_empty(),
        "clean workflow should have no findings, got: {report:?}"
    );
}

#[test]
fn activity_bodies_are_not_flagged() {
    // Direct I/O is fine inside an activity — the checker must NOT flag it
    let src = act("let _data = std::fs::read(\"/tmp/data\").unwrap();");
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.is_empty(),
        "activity bodies must not be flagged, got: {report:?}"
    );
}

#[test]
fn non_annotated_functions_are_not_flagged() {
    let src = "async fn helper() { let _ = std::time::SystemTime::now(); }\n";
    let report = check_source(src, "test.rs");
    assert!(
        report.findings.is_empty(),
        "non-workflow functions must not be flagged, got: {report:?}"
    );
}

#[test]
fn workflow_body_extraction_ignores_braces_inside_string_and_char_literals() {
    let src = r#"
#[workflow]
async fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {
    let _string_marker = "}";
    let _char_marker = '}';
    let _t = std::time::SystemTime::now();
    Ok(())
}
"#;
    let report = check_source(src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET001"),
        "literal braces must not truncate workflow body scanning, got: {report:?}"
    );
}

// ── finding metadata ───────────────────────────────────────────────────────

#[test]
fn finding_carries_workflow_name() {
    let src = wf("let _t = std::time::SystemTime::now();");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET001")
        .unwrap();
    assert_eq!(
        finding.workflow_name.as_deref(),
        Some("test_wf"),
        "finding must carry the workflow function name"
    );
}

#[test]
fn finding_carries_source_location() {
    let src = wf("let _t = std::time::SystemTime::now();");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET001")
        .unwrap();
    let loc = finding
        .location
        .as_ref()
        .expect("finding must have a source location");
    assert_eq!(loc.file, "test.rs");
    assert!(loc.line > 0, "line must be non-zero");
}

#[test]
fn finding_carries_alternative() {
    let src = wf("let _t = std::time::SystemTime::now();");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET001")
        .unwrap();
    assert!(
        !finding.alternative.is_empty(),
        "finding must carry a non-empty alternative suggestion"
    );
}

#[test]
fn finding_carries_message() {
    let src = wf("let _t = std::time::SystemTime::now();");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET001")
        .unwrap();
    assert!(!finding.message.is_empty());
}

// ── report aggregation ─────────────────────────────────────────────────────

#[test]
fn report_has_hard_blockers_when_error_findings_exist() {
    let src = wf("let _t = std::time::SystemTime::now();");
    let report = check_source(&src, "test.rs");
    assert!(report.has_hard_blockers());
}

#[test]
fn report_passes_when_only_warnings_exist() {
    let src = wf("let _pid = std::process::id();");
    let report = check_source(&src, "test.rs");
    // DET005 is a Warning, not an Error, so it must not be a hard blocker
    assert!(
        !report.has_hard_blockers(),
        "warnings alone must not block CI"
    );
}

#[test]
fn report_passes_for_clean_workflow() {
    let src = wf("ctx.timer(\"t\", 10).await?;");
    let report = check_source(&src, "test.rs");
    assert!(!report.has_hard_blockers());
}

// ── suppression mechanism ──────────────────────────────────────────────────

#[test]
fn suppressed_finding_is_not_a_hard_blocker() {
    let src = r#"
#[workflow]
async fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {
    // harvest-suppress: DET001 "recorded in signal payload"
    let _t = std::time::SystemTime::now();
    Ok(())
}
"#;
    let report = check_source(src, "test.rs");
    assert!(
        !report.has_hard_blockers(),
        "suppressed DET001 must not be a hard blocker"
    );
}

#[test]
fn same_line_suppression_is_not_a_hard_blocker() {
    let src = r#"
#[workflow]
async fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {
    let _t = std::time::SystemTime::now(); // harvest-suppress: DET001 "recorded in signal payload"
    Ok(())
}
"#;
    let report = check_source(src, "test.rs");
    assert!(
        !report.has_hard_blockers(),
        "same-line harvest-suppress comment must suppress DET001, got: {report:?}"
    );
    assert_eq!(report.suppressions.len(), 1);
    assert_eq!(report.suppressions[0].rule_id, "DET001");
}

#[test]
fn suppression_requires_reason_string() {
    // Suppression without a quoted reason is not valid and does NOT suppress
    let src = r"
#[workflow]
async fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {
    // harvest-suppress: DET001
    let _t = std::time::SystemTime::now();
    Ok(())
}
";
    let report = check_source(src, "test.rs");
    assert!(
        report.has_hard_blockers(),
        "suppression without a reason must not suppress the finding"
    );
}

#[test]
fn suppression_is_reported_in_output() {
    let src = r#"
#[workflow]
async fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {
    // harvest-suppress: DET001 "recorded in signal payload"
    let _t = std::time::SystemTime::now();
    Ok(())
}
"#;
    let report = check_source(src, "test.rs");
    assert!(
        !report.suppressions.is_empty(),
        "active suppressions must appear in report output"
    );
    assert_eq!(report.suppressions[0].rule_id, "DET001");
    assert!(!report.suppressions[0].reason.is_empty());
}

#[test]
fn suppression_only_applies_to_its_rule_id() {
    // Suppress DET001 but DET003 is also present — DET003 must still block
    let src = r#"
#[workflow]
async fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {
    // harvest-suppress: DET001 "known safe"
    let _t = std::time::SystemTime::now();
    let _id = uuid::Uuid::new_v4();
    Ok(())
}
"#;
    let report = check_source(src, "test.rs");
    assert!(
        report.has_hard_blockers(),
        "DET003 is not suppressed and must still block"
    );
}

// ── multiple violations ────────────────────────────────────────────────────

#[test]
fn multiple_violations_are_all_reported() {
    let src = wf(r"
    let _t = std::time::SystemTime::now();
    let _n: u64 = rand::random();
    let _id = uuid::Uuid::new_v4();
");
    let report = check_source(&src, "test.rs");
    let rule_ids: Vec<&str> = report.findings.iter().map(|f| f.rule_id).collect();
    assert!(rule_ids.contains(&"DET001"), "DET001 must be reported");
    assert!(rule_ids.contains(&"DET002"), "DET002 must be reported");
    assert!(rule_ids.contains(&"DET003"), "DET003 must be reported");
}

// ── examples pass with zero hard blockers ─────────────────────────────────

#[test]
fn quickstart_example_has_no_hard_blockers() {
    let src = include_str!("../../../examples/quickstart/src/main.rs");
    let report = check_source(src, "examples/quickstart/src/main.rs");
    assert!(
        !report.has_hard_blockers(),
        "quickstart example must pass with zero hard blockers, findings: {report:?}"
    );
}

#[test]
fn standalone_runner_workflows_have_no_hard_blockers() {
    let src = include_str!("../../../examples/standalone-runner/src/workflows.rs");
    let report = check_source(src, "examples/standalone-runner/src/workflows.rs");
    assert!(
        !report.has_hard_blockers(),
        "standalone-runner workflows must pass with zero hard blockers, findings: {report:?}"
    );
}

#[test]
fn billing_example_workflows_have_no_hard_blockers() {
    let src = include_str!("../../../examples/billing-autumn-web/src/workflows.rs");
    let report = check_source(src, "examples/billing-autumn-web/src/workflows.rs");
    assert!(
        !report.has_hard_blockers(),
        "billing example workflows must pass with zero hard blockers, findings: {report:?}"
    );
}

// ── single-line workflow body ──────────────────────────────────────────────

#[test]
fn single_line_workflow_body_is_scanned() {
    // Compact single-line form: `fn wf() { stmt }` — body after `{` must be checked.
    let src = "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> { let _ = std::time::SystemTime::now(); Ok(()) }\n";
    let report = check_source(src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET001"),
        "single-line workflow body must be scanned, got: {report:?}"
    );
}

// ── string literal false-positive guard ───────────────────────────────────

#[test]
fn string_literal_not_flagged_as_violation() {
    // A string value that happens to contain a rule pattern must not cause a finding.
    let src = wf(r#"let msg = "std::fs::read is documented here";"#);
    let report = check_source(&src, "test.rs");
    assert!(
        !report.has_hard_blockers(),
        "pattern inside string literal must not cause a hard blocker, got: {report:?}"
    );
}

// ── same-line suppression ──────────────────────────────────────────────────

#[test]
fn same_line_suppression_works() {
    // Suppression comment placed on the SAME line as the violation (trailing comment).
    let src = "#[workflow]\nasync fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _t = std::time::SystemTime::now(); // harvest-suppress: DET001 \"same-line reason\"\n    Ok(())\n}\n";
    let report = check_source(src, "test.rs");
    assert!(
        !report.has_hard_blockers(),
        "same-line suppression must suppress the finding"
    );
    assert!(
        !report.suppressions.is_empty(),
        "same-line suppression must appear in report.suppressions"
    );
}

#[test]
fn inline_suppression_does_not_carry_to_next_line() {
    // A trailing `// harvest-suppress:` on line N must NOT suppress a violation
    // on line N+1 — it is scoped to the same line only.
    let src = wf(
        "let _a = std::time::SystemTime::now(); // harvest-suppress: DET001 \"first\"\n    let _b = std::time::SystemTime::now();",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.has_hard_blockers(),
        "second SystemTime::now() on the next line must still be a hard blocker"
    );
    assert!(
        !report.suppressions.is_empty(),
        "the first line's inline suppression must still be recorded"
    );
}

// ── block comment handling ────────────────────────────────────────────────

#[test]
fn block_comment_brace_does_not_end_body_early() {
    // A `}` inside `/* */` must not terminate workflow body extraction early.
    let src = wf("let x = /* } */ 5;\n    let _ = std::time::SystemTime::now();");
    let report = check_source(&src, "test.rs");
    assert!(
        report.has_hard_blockers(),
        "violation after a block-comment `}}` must still be flagged"
    );
}

#[test]
fn block_comment_pattern_is_not_flagged() {
    // A DET pattern inside `/* */` must not produce a finding.
    let src = wf("let x = /* std::fs::read */ 5;");
    let report = check_source(&src, "test.rs");
    assert!(
        !report.has_hard_blockers(),
        "DET pattern inside block comment must not be flagged"
    );
}

// ── regression fixtures ────────────────────────────────────────────────────
// These fixtures each embed exactly one violation of the target rule.
// They serve as regression tests: if the rule is removed or its ID changes,
// these fail, proving the catalog is complete.

#[test]
fn regression_det001_wall_clock() {
    let report = check_source(
        "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _ = chrono::Utc::now();\n    Ok(())\n}\n",
        "fixtures/det001.rs",
    );
    assert!(report.findings.iter().any(|f| f.rule_id == "DET001"));
}

#[test]
fn regression_det002_randomness() {
    let report = check_source(
        "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _ = rand::random::<u64>();\n    Ok(())\n}\n",
        "fixtures/det002.rs",
    );
    assert!(report.findings.iter().any(|f| f.rule_id == "DET002"));
}

#[test]
fn regression_det003_uuid() {
    let report = check_source(
        "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _ = Uuid::new_v4();\n    Ok(())\n}\n",
        "fixtures/det003.rs",
    );
    assert!(report.findings.iter().any(|f| f.rule_id == "DET003"));
}

#[test]
fn regression_det004_env_read() {
    let report = check_source(
        "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _ = std::env::var(\"X\");\n    Ok(())\n}\n",
        "fixtures/det004.rs",
    );
    assert!(report.findings.iter().any(|f| f.rule_id == "DET004"));
}

#[test]
fn regression_det005_process_read() {
    let report = check_source(
        "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _ = std::process::id();\n    Ok(())\n}\n",
        "fixtures/det005.rs",
    );
    assert!(report.findings.iter().any(|f| f.rule_id == "DET005"));
}

#[test]
fn regression_det006_direct_sleep() {
    let report = check_source(
        "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    tokio::time::sleep(std::time::Duration::from_secs(1)).await;\n    Ok(())\n}\n",
        "fixtures/det006.rs",
    );
    assert!(report.findings.iter().any(|f| f.rule_id == "DET006"));
}

#[test]
fn regression_det007_task_spawn() {
    let report = check_source(
        "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    tokio::spawn(async {});\n    Ok(())\n}\n",
        "fixtures/det007.rs",
    );
    assert!(report.findings.iter().any(|f| f.rule_id == "DET007"));
}

#[test]
fn regression_det008_direct_io() {
    let report = check_source(
        "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _ = std::fs::read(\"/tmp/x\");\n    Ok(())\n}\n",
        "fixtures/det008.rs",
    );
    assert!(report.findings.iter().any(|f| f.rule_id == "DET008"));
}

// ── DET010: HashMap/HashSet iteration order (issue #785) ──────────────────
//
// NOTE on the rule ID: issue #785's text proposed "DET/HVG010" but HVG010 was
// already permanently assigned to SelectMacro (issue #600) and DET009 was the
// current det_check maximum, so this rule ships as DET010 in det_check and
// HVG011 in the guardrail catalog / macro lint.

#[test]
fn det010_flags_hashmap_loop_with_command_as_error() {
    let src = wf(
        "let mut m: HashMap<String, u64> = HashMap::new();\n    for (k, v) in &m {\n        ctx.execute_activity_raw(\"debit\", serde_json::json!(k), \"default\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET010")
        .unwrap_or_else(|| panic!("expected DET010 finding, got: {report:?}"));
    assert!(
        matches!(finding.severity, DetSeverity::Error),
        "command-emitting HashMap loop must be Error, got: {finding:?}"
    );
    assert!(report.has_hard_blockers());
}

#[test]
fn det010_pure_computation_hashmap_loop_is_warning() {
    let src = wf(
        "let mut m: HashMap<String, u64> = HashMap::new();\n    let mut total = 0u64;\n    for (_k, v) in &m {\n        total += v;\n    }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET010")
        .unwrap_or_else(|| panic!("expected DET010 warning finding, got: {report:?}"));
    assert!(
        matches!(finding.severity, DetSeverity::Warning),
        "command-free HashMap loop must be Warning, got: {finding:?}"
    );
    assert!(
        !report.has_hard_blockers(),
        "a command-free loop must not be a hard blocker"
    );
}

#[test]
fn det010_flags_hashset_loop_with_command() {
    let src = wf(
        "let mut s: HashSet<String> = HashSet::new();\n    for item in &s {\n        ctx.spawn_child_workflow_raw(\"child\", serde_json::json!(item)).await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET010")
        .unwrap_or_else(|| panic!("expected DET010 for HashSet, got: {report:?}"));
    assert!(matches!(finding.severity, DetSeverity::Error));
}

#[test]
fn det010_flags_hashset_new_inferred_binding() {
    // Binding hash-typed via the initializer only (no type annotation).
    let src = wf(
        "let mut s = HashSet::new();\n    s.insert(1u64);\n    for item in s.iter() {\n        ctx.timer(\"t\", *item).await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET010"),
        "HashSet::new() initializer must mark the binding, got: {report:?}"
    );
}

#[test]
fn det010_flags_iter_method_forms() {
    for method in ["iter()", "keys()", "values()", "drain()", "into_iter()"] {
        let src = wf(&format!(
            "let mut m: HashMap<String, u64> = HashMap::new();\n    for x in m.{method} {{\n        ctx.execute_activity_raw(\"a\", serde_json::json!(x), \"q\").await?;\n    }}"
        ));
        let report = check_source(&src, "test.rs");
        assert!(
            report.findings.iter().any(|f| f.rule_id == "DET010"),
            "`for x in m.{method}` must be flagged, got: {report:?}"
        );
    }
}

#[test]
fn det010_flags_mut_borrow_form() {
    let src = wf(
        "let mut m: HashMap<String, u64> = HashMap::new();\n    for (_k, v) in &mut m {\n        ctx.side_effect(\"se\", || *v).await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET010"),
        "`for .. in &mut m` must be flagged, got: {report:?}"
    );
}

#[test]
fn det010_flags_collect_turbofish_binding() {
    let src = wf(
        "let m = items.into_iter().collect::<HashMap<String, u64>>();\n    for (k, _v) in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET010"),
        ".collect::<HashMap<..>>() binding must be tracked, got: {report:?}"
    );
}

#[test]
fn det010_shadowing_with_vec_untracks_the_ident() {
    // Last binding wins: re-binding `m` as a Vec must clear the hash mark.
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    let m: Vec<u64> = m.values().copied().collect();\n    for v in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(v), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "a later non-hash binding of the same ident must untrack it, got: {report:?}"
    );
}

#[test]
fn det010_never_flags_ordered_collections() {
    for binding in [
        "let m: BTreeMap<String, u64> = BTreeMap::new();",
        "let m: BTreeSet<String> = BTreeSet::new();",
        "let m: Vec<String> = Vec::new();",
        "let m: IndexMap<String, u64> = IndexMap::new();",
        "let m = [1u64, 2, 3];",
    ] {
        let src = wf(&format!(
            "{binding}\n    for x in &m {{\n        ctx.execute_activity_raw(\"a\", serde_json::json!(x), \"q\").await?;\n    }}"
        ));
        let report = check_source(&src, "test.rs");
        assert!(
            !report.findings.iter().any(|f| f.rule_id == "DET010"),
            "ordered collection `{binding}` must never be flagged, got: {report:?}"
        );
    }
}

#[test]
fn det010_sorted_keys_vec_is_never_flagged() {
    // The recommended remediation itself must pass: collect keys, sort, iterate.
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    let mut keys: Vec<String> = m.keys().cloned().collect();\n    keys.sort();\n    for k in keys {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "sorted-keys Vec iteration must never be flagged, got: {report:?}"
    );
}

#[test]
fn det010_longer_iterator_chain_is_not_flagged() {
    // Chains past a single method call are deliberately out of scope — this is
    // how "already-sorted iterators are never flagged" holds.
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    for k in m.keys().sorted() {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "multi-call iterator chain must not be flagged, got: {report:?}"
    );
}

#[test]
fn det010_function_parameter_map_is_not_flagged() {
    // Only locally `let`-bound hash collections are tracked (explicit
    // syntactic boundary from issue #785) — parameters are never flagged.
    let src = "#[workflow]\nasync fn test_wf(ctx: &WorkflowContext, map: HashMap<String, u64>) -> Result<(), String> {\n    for (k, _v) in &map {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }\n    Ok(())\n}\n";
    let report = check_source(src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "function-parameter map must not be flagged, got: {report:?}"
    );
}

#[test]
fn det010_suppression_is_honored_and_reported() {
    let src = "#[workflow]\nasync fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let m: HashMap<String, u64> = HashMap::new();\n    // harvest-suppress: DET010 \"single entry map; order cannot matter\"\n    for (k, _v) in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }\n    Ok(())\n}\n";
    let report = check_source(src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "suppressed DET010 must not produce a finding, got: {report:?}"
    );
    assert!(
        report
            .suppressions
            .iter()
            .any(|s| s.rule_id == "DET010" && !s.reason.is_empty()),
        "DET010 suppression must be echoed into report.suppressions, got: {report:?}"
    );
}

#[test]
fn det010_activity_bodies_are_never_flagged() {
    let src = act(
        "let m: HashMap<String, u64> = HashMap::new();\n    for (k, _v) in &m {\n        println!(\"{k}\");\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "activity bodies must never be flagged for DET010, got: {report:?}"
    );
}

// ── DET010 review hardening (PR #970) ──────────────────────────────────────

// P1-B: positional binding parser — word-containment false positives.

#[test]
fn det010_vec_of_hashmap_annotation_is_not_flagged() {
    let src = wf(
        "let v: Vec<HashMap<String, u64>> = Vec::new();\n    for m in &v {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(m), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "Vec<HashMap<..>> annotation must not track the binding, got: {report:?}"
    );
}

#[test]
fn det010_option_hashmap_annotation_is_not_flagged() {
    let src = wf(
        "let m: Option<HashMap<String, u64>> = None;\n    for x in &m {\n        let _ = x;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "Option<HashMap<..>> annotation must not track the binding, got: {report:?}"
    );
}

#[test]
fn det010_generic_call_turbofish_is_not_flagged() {
    let src = wf(
        "let ids = load_ids::<HashMap<String, u64>>();\n    for x in &ids {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(x), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "a generic call turbofish mentioning HashMap must not track the binding, got: {report:?}"
    );
}

// P1-C: depth-scoped binding eviction + for-pattern masking.

#[test]
fn det010_inner_block_hash_binding_does_not_leak() {
    let src = wf(
        "let m: Vec<u64> = Vec::new();\n    {\n        let m: HashMap<String, u64> = HashMap::new();\n        let _ = m.len();\n    }\n    for v in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(v), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "an inner-block hash binding must not leak past block exit, got: {report:?}"
    );
}

#[test]
fn det010_for_loop_pattern_masks_tracked_ident() {
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    let lists: Vec<Vec<u64>> = Vec::new();\n    for m in &lists {\n        for x in m {\n            ctx.execute_activity_raw(\"a\", serde_json::json!(x), \"q\").await?;\n        }\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "a for-loop pattern binding must mask the tracked ident for the body extent, got: {report:?}"
    );
}

#[test]
fn det010_for_loop_pattern_mask_is_restored_after_the_loop() {
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    let lists: Vec<Vec<u64>> = Vec::new();\n    for m in &lists {\n        let _ = m;\n    }\n    for (k, _v) in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    let det010: Vec<_> = report
        .findings
        .iter()
        .filter(|f| f.rule_id == "DET010")
        .collect();
    assert_eq!(
        det010.len(),
        1,
        "the outer hash binding must fire again after the masking loop closes, got: {report:?}"
    );
    assert!(matches!(det010[0].severity, DetSeverity::Error));
}

#[test]
fn det010_match_arm_shadow_residual_over_flag_is_documented() {
    // RESIDUAL LIMITATION (documented, PR #970 P1-C): the line-based det_check
    // pass cannot mask match-arm (or closure-param) pattern bindings, so a
    // tracked ident re-bound by `Some(m) =>` is still over-flagged inside the
    // arm. The syn-based macro lint is scope-exact for this shape; suppress
    // with `// harvest-suppress: DET010 "..."` when intended. This test pins
    // the residual behavior so it is explicit, not accidental.
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    match resolve() {\n        Some(m) => {\n            for item in &m {\n                ctx.execute_activity_raw(\"a\", serde_json::json!(item), \"q\").await?;\n            }\n        }\n        None => {}\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET010"),
        "pinned residual: match-arm pattern shadowing is over-flagged by the line-based pass"
    );
}

// P2-B: HVG011 suppression alias (AC5).

#[test]
fn det010_hvg011_alias_suppression_suppresses_and_echoes() {
    let src = "#[workflow]\nasync fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let m: HashMap<String, u64> = HashMap::new();\n    // harvest-suppress: HVG011 \"single entry map; order cannot matter\"\n    for (k, _v) in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }\n    Ok(())\n}\n";
    let report = check_source(src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "HVG011-spelled suppression must suppress the DET010 finding, got: {report:?}"
    );
    assert!(
        report
            .suppressions
            .iter()
            .any(|s| s.rule_id == "HVG011" && !s.reason.is_empty()),
        "the HVG011 alias must be echoed with the id the author wrote, got: {report:?}"
    );
}

// P2-C: fallible collect turbofish.

#[test]
fn det010_collect_result_turbofish_is_tracked() {
    let src = wf(
        "let m = items.into_iter().collect::<Result<HashMap<String, u64>, String>>()?;\n    for (k, _v) in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET010"),
        ".collect::<Result<HashMap<..>, E>>()? must be tracked, got: {report:?}"
    );
}

// P2-D: multi-line `let` statements.

#[test]
fn det010_multiline_let_initializer_is_tracked() {
    let src = wf(
        "let m =\n        items.into_iter().collect::<HashMap<String, u64>>();\n    for (k, _v) in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET010"),
        "a multi-line `let m =` / `..collect::<HashMap<..>>();` binding must be tracked, got: {report:?}"
    );
}

#[test]
fn det010_multiline_let_annotation_is_tracked() {
    let src = wf(
        "let m:\n        HashMap<String, u64> = HashMap::new();\n    for (k, _v) in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET010"),
        "a multi-line `let m:` / `HashMap<..> = ..;` binding must be tracked, got: {report:?}"
    );
}

#[test]
fn det010_multiline_for_header_is_a_documented_miss() {
    // DOCUMENTED LIMITATION (PR #970 P2-D): a `for` header split across lines
    // (`for (k, v) in` / `&m`) is not detected by the line-based pass — the
    // syn-based macro lint catches this shape. Pinned so the miss is explicit.
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    for (k, v) in\n        &m\n    {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET010"),
        "pinned limitation: multi-line for header is not detected by det_check"
    );
}

// P2-F.3: ctx.race() is a command marker.

#[test]
fn det010_race_in_loop_body_is_error() {
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    for (k, _v) in &m {\n        let _ = ctx.race().activity_raw(\"a\", serde_json::json!(k), \"q\").run().await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET010")
        .unwrap_or_else(|| panic!("expected DET010, got: {report:?}"));
    assert!(
        matches!(finding.severity, DetSeverity::Error),
        "ctx.race() in the loop body must make the finding an Error, got: {finding:?}"
    );
}

// P3.2: severity scan starts at the `for` token.

#[test]
fn det010_same_line_preceding_command_does_not_inflate_severity() {
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    ctx.timer(\"t\", 1).await?; for (_k, v) in &m { let _ = v; }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET010")
        .unwrap_or_else(|| panic!("expected DET010, got: {report:?}"));
    assert!(
        matches!(finding.severity, DetSeverity::Warning),
        "a command BEFORE the `for` on the same line must not make an empty loop an Error, got: {finding:?}"
    );
}

#[test]
fn det010_enclosing_same_line_brace_does_not_extend_body_scan() {
    // An enclosing `{` before the `for` must not inflate depth and drag the
    // body scan past the loop's own close into following (outside) lines.
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    if cond { for x in &m { let _ = x; }\n        ctx.execute_activity_raw(\"a\", serde_json::json!(1), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET010")
        .unwrap_or_else(|| panic!("expected DET010, got: {report:?}"));
    assert!(
        matches!(finding.severity, DetSeverity::Warning),
        "a command outside the loop but inside an enclosing same-line block must not inflate severity, got: {finding:?}"
    );
}

// P3.3: test-matrix pins.

#[test]
fn det010_hashmap_from_binding_is_tracked() {
    let src = wf(
        "let m = HashMap::from([(\"a\", 1u64)]);\n    for (k, _v) in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET010"),
        "HashMap::from([..]) binding must be tracked, got: {report:?}"
    );
}

#[test]
fn det010_hashmap_default_binding_is_tracked() {
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::default();\n    for (k, _v) in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET010"),
        "HashMap::default() binding must be tracked, got: {report:?}"
    );
}

#[test]
fn det010_flags_remaining_iter_method_forms() {
    for method in ["iter_mut()", "values_mut()", "into_keys()", "into_values()"] {
        let src = wf(&format!(
            "let mut m: HashMap<String, u64> = HashMap::new();\n    for x in m.{method} {{\n        ctx.execute_activity_raw(\"a\", serde_json::json!(1), \"q\").await?;\n    }}"
        ));
        let report = check_source(&src, "test.rs");
        assert!(
            report.findings.iter().any(|f| f.rule_id == "DET010"),
            "`for x in m.{method}` must be flagged, got: {report:?}"
        );
    }
}

#[test]
fn det010_collect_hashset_turbofish_is_tracked() {
    let src = wf(
        "let s = items.into_iter().collect::<HashSet<String>>();\n    for x in &s {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(x), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET010"),
        ".collect::<HashSet<..>>() binding must be tracked, got: {report:?}"
    );
}

#[test]
fn det010_fan_out_helper_in_loop_body_is_error() {
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    for (_k, v) in &m {\n        let _ = ctx.execute_activity_fan_out(&info(), vec![v]).await;\n    }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET010")
        .unwrap_or_else(|| panic!("expected DET010, got: {report:?}"));
    assert!(matches!(finding.severity, DetSeverity::Error));
}

#[test]
fn det010_side_effect_in_loop_body_is_error() {
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    for (_k, v) in &m {\n        let _ = ctx.side_effect(\"draw\", || *v);\n    }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET010")
        .unwrap_or_else(|| panic!("expected DET010, got: {report:?}"));
    assert!(matches!(finding.severity, DetSeverity::Error));
}

#[test]
fn det010_execute_local_activity_in_loop_body_is_error() {
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    for (k, _v) in &m {\n        ctx.execute_local_activity_raw(\"a\", serde_json::json!(k)).await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET010")
        .unwrap_or_else(|| panic!("expected DET010, got: {report:?}"));
    assert!(matches!(finding.severity, DetSeverity::Error));
}

// P3.5: committed self-scan (AC7). The <5s success metric holds comfortably
// (~100ms observed) but is deliberately not asserted — CI timing is flaky.

#[test]
fn det010_self_scan_of_harvest_src_is_clean() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let report = autumn_harvest::det_check::check_dir(&dir).expect("src dir must be readable");
    let det010: Vec<_> = report
        .findings
        .iter()
        .filter(|f| f.rule_id == "DET010")
        .collect();
    assert!(
        det010.is_empty(),
        "AC7: autumn-harvest/src must have zero DET010 findings, got: {det010:?}"
    );
}

#[test]
fn det010_finding_carries_metadata() {
    let src = wf(
        "let m: HashMap<String, u64> = HashMap::new();\n    for (k, _v) in &m {\n        ctx.execute_activity_raw(\"a\", serde_json::json!(k), \"q\").await?;\n    }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET010")
        .expect("DET010 finding");
    assert_eq!(finding.workflow_name.as_deref(), Some("test_wf"));
    let loc = finding.location.as_ref().expect("location");
    assert_eq!(loc.file, "test.rs");
    assert!(loc.line > 0);
    assert!(!finding.message.is_empty());
    assert!(
        finding.alternative.contains("BTreeMap") || finding.alternative.contains("sort"),
        "alternative must point at BTreeMap or sorted-Vec remediation, got: {}",
        finding.alternative
    );
}

// ── DET011: select! / futures select combinators (issue #799) ─────────────
//
// The det_check twin of guardrail HVG010 (SelectMacro, issue #600). HVG010 is
// the compile-time / catalog id; det_check surfaces the same hazard as DET011
// (DET010 was the prior det_check maximum). Catches BOTH the select MACROS
// (`tokio::select!`, `futures::select!`, `select_biased!`) and the distinctive
// futures combinator FUNCTIONS (`futures::future::select(`, `select_all(`,
// `select_ok(`, `try_select(`). Racing ctx-managed awaitables is always a
// determinism hazard, so DET011 is an Error (no command-aware downgrade).

#[test]
fn det011_flags_tokio_select_macro_as_error() {
    let src = wf(
        "tokio::select! {\n        _ = ctx.timer(\"t\", 60) => {}\n        _ = ctx.wait_for_signal(\"s\") => {}\n    }",
    );
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET011")
        .unwrap_or_else(|| panic!("expected DET011 finding, got: {report:?}"));
    assert!(
        matches!(finding.severity, DetSeverity::Error),
        "select! in a workflow must be an Error, got: {finding:?}"
    );
    assert!(report.has_hard_blockers());
}

#[test]
fn det011_flags_futures_select_macro() {
    let src = wf(
        "futures::select! {\n        a = fut_a.fuse() => {}\n        b = fut_b.fuse() => {}\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET011"),
        "futures::select! must be flagged, got: {report:?}"
    );
}

#[test]
fn det011_flags_select_biased_macro() {
    let src = wf(
        "select_biased! {\n        a = fut_a.fuse() => {}\n        b = fut_b.fuse() => {}\n    }",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET011"),
        "select_biased! must be flagged, got: {report:?}"
    );
}

#[test]
fn det011_select_macro_is_flagged_exactly_once_no_double_count() {
    // A line with a single `tokio::select!` must yield exactly ONE DET011
    // finding — the `select!` and `select_biased!` macro patterns must not both
    // fire on the same line (#980 Codex P2). The engine dedupes per-rule-per-line
    // via `continue 'rules`, so this confirms the boundary guard did not
    // accidentally introduce a second match.
    for macro_call in [
        "tokio::select! { _ = a => {} }",
        "futures::select! { _ = a => {} }",
        "select! { _ = a => {} }",
        "select_biased! { _ = a => {} }",
    ] {
        let src = wf(macro_call);
        let report = check_source(&src, "test.rs");
        assert_eq!(
            report
                .findings
                .iter()
                .filter(|f| f.rule_id == "DET011")
                .count(),
            1,
            "`{macro_call}` must be flagged exactly once, got: {report:?}"
        );
    }
}

#[test]
fn det011_does_not_flag_macros_whose_name_merely_ends_in_select() {
    // An unrelated macro whose name merely ENDS in `select!` / `select_biased!`
    // must NOT be flagged — the identifier-boundary guard excludes a preceding
    // identifier byte, keeping det_check in agreement with the compile-time
    // HVG010 guardrail (which matches macro paths exactly and accepts these)
    // (#980 Codex P2 review).
    for call in [
        "sql_select! { columns }",
        "let _ = my_select!();",
        "let _ = foo_select_biased!();",
    ] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert!(
            !report.findings.iter().any(|f| f.rule_id == "DET011"),
            "`{call}` must NOT be flagged for DET011, got: {report:?}"
        );
    }
}

#[test]
fn det011_flags_qualified_future_select_combinator() {
    let src = wf("let _ = futures::future::select(a, b).await;");
    let report = check_source(&src, "test.rs");
    assert!(
        report.findings.iter().any(|f| f.rule_id == "DET011"),
        "futures::future::select(..) combinator must be flagged, got: {report:?}"
    );
}

#[test]
fn det011_flags_short_form_future_select_combinator() {
    // The `use futures::future; future::select(a, b)` short form must be flagged
    // too, matching the AST macro visitor which recognizes `future::select`
    // (#980 Codex P2). Exactly one DET011 finding is emitted for a qualified
    // call even though the short-form pattern is a suffix of it — the engine
    // emits at most one finding per rule per line.
    let src = wf("let _ = future::select(a, b).await;");
    let report = check_source(&src, "test.rs");
    assert_eq!(
        report
            .findings
            .iter()
            .filter(|f| f.rule_id == "DET011")
            .count(),
        1,
        "short-form `future::select(..)` must be flagged exactly once, got: {report:?}"
    );

    let qualified = wf("let _ = futures::future::select(a, b).await;");
    let qreport = check_source(&qualified, "test.rs");
    assert_eq!(
        qreport
            .findings
            .iter()
            .filter(|f| f.rule_id == "DET011")
            .count(),
        1,
        "qualified `futures::future::select(..)` must be flagged exactly once (no double-count), got: {qreport:?}"
    );
}

#[test]
fn det011_does_not_flag_method_or_suffix_forms_of_future_select() {
    // A `.select()` method call and a module whose name merely ends in `future`
    // must NOT be flagged for the `future::select(` pattern — the call-position
    // boundary guard excludes a preceding `.` or identifier char (#980 Codex P2).
    for call in [
        "let _ = x.select();",
        "let _ = builder.select(cols);",
        "let _ = my_future::select(a, b);",
    ] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert!(
            !report.findings.iter().any(|f| f.rule_id == "DET011"),
            "`{call}` must NOT be flagged, got: {report:?}"
        );
    }
}

#[test]
fn det011_flags_bare_distinctive_combinators() {
    for call in ["select_all(v)", "select_ok(v)", "try_select(a, b)"] {
        let src = wf(&format!("let _ = {call}.await;"));
        let report = check_source(&src, "test.rs");
        assert!(
            report.findings.iter().any(|f| f.rule_id == "DET011"),
            "bare `{call}` must be flagged, got: {report:?}"
        );
    }
}

#[test]
fn det011_does_not_flag_qualified_non_futures_combinator_paths() {
    // A qualified call whose full path is outside the futures allowed set must
    // NOT be flagged — det_check now extracts the FULL path ending at the call
    // and matches it exactly against the same set as the macro lint's
    // `is_select_combinator_path`, so a same-tail-name helper under a different
    // root is rejected exactly as the compile-time HVG010 guardrail rejects it
    // (#980 Codex P2 review).
    for call in [
        "let _ = crate::future::select(a, b).await;",
        "let _ = my_dsl::select_all(v);",
        "let _ = foo::try_select(a, b);",
        "let _ = bar::future::select_ok(v);",
    ] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert!(
            !report.findings.iter().any(|f| f.rule_id == "DET011"),
            "qualified non-futures path `{call}` must NOT be flagged, got: {report:?}"
        );
    }
}

#[test]
fn det011_flags_futures_qualified_and_short_form_combinators() {
    // The `futures`-anchored qualified forms and their `future::…` short forms
    // for every distinctive name must all be flagged (#980 Codex P2). Bare
    // `select(` remains unflagged (deliberately not in the allowed set).
    for call in [
        "let _ = futures::future::select(a, b).await;",
        "let _ = future::select(a, b).await;",
        "let _ = futures::future::select_all(v).await;",
        "let _ = future::select_all(v).await;",
        "let _ = futures::future::select_ok(v).await;",
        "let _ = future::select_ok(v).await;",
        "let _ = futures::future::try_select(a, b).await;",
        "let _ = future::try_select(a, b).await;",
    ] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert_eq!(
            report
                .findings
                .iter()
                .filter(|f| f.rule_id == "DET011")
                .count(),
            1,
            "futures combinator `{call}` must be flagged exactly once, got: {report:?}"
        );
    }
    // Bare `select(...)` is deliberately NOT flagged.
    let src = wf("let _ = select(a, b).await;");
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET011"),
        "bare `select(..)` must NOT be flagged, got: {report:?}"
    );
}

#[test]
fn det011_flags_turbofished_combinator_calls() {
    // A turbofish (and defensive whitespace) between the combinator name and the
    // call `(` must NOT hide the call from det_check: the syn-based HVG010 macro
    // lint strips path arguments before matching and DOES hard-block these
    // forms, so det_check must too, or the text pre-check green-lights code the
    // compile-time guardrail rejects (#980 Codex P2). Each is exactly one
    // DET011 finding.
    for call in [
        "let _ = futures::future::select::<_, _>(a, b).await;",
        "let _ = future::select_all::<Vec<_>>(v).await;",
        "let _ = select_all::<Vec<_>>(v).await;",
        "let _ = try_select::<_,_>(a, b).await;",
        "let _ = select_ok::<Vec<Box<T>>>(v).await;",
        "let _ = future::select ::<_, _> (a, b).await;",
    ] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert_eq!(
            report
                .findings
                .iter()
                .filter(|f| f.rule_id == "DET011")
                .count(),
            1,
            "turbofished combinator `{call}` must be flagged exactly once, got: {report:?}"
        );
    }
}

#[test]
fn det011_does_not_flag_turbofished_non_futures_or_method_forms() {
    // The turbofish tolerance must NOT loosen path precision: a qualified call
    // under a non-futures root, or a method call, stays unflagged even WITH a
    // turbofish (mirrors the macro lint's exact-path matching). Bare `select`
    // has no allowed form (turbofished or not) and stays unflagged.
    for call in [
        "let _ = crate::future::select::<_,_>(a, b).await;",
        "let _ = my_dsl::select_all::<T>(v);",
        "let _ = foo::try_select::<_, _>(a, b);",
        "let _ = bar::future::select_ok::<T>(v);",
        "let _ = x.select_all::<T>();",
        "let _ = q.try_select::<_,_>(a, b);",
        "let _ = select::<_,_>(a, b).await;",
    ] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert!(
            !report.findings.iter().any(|f| f.rule_id == "DET011"),
            "turbofished form `{call}` must NOT be flagged, got: {report:?}"
        );
    }
    // A macro whose name merely ends in `select` is still not a combinator,
    // regardless of any turbofish elsewhere on the line.
    for call in ["sql_select! { foo }", "my_select!(a, b)"] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert!(
            !report.findings.iter().any(|f| f.rule_id == "DET011"),
            "suffix macro `{call}` must NOT be flagged, got: {report:?}"
        );
    }
}

#[test]
fn det011_flags_absolute_futures_combinator_paths() {
    // An absolute path (leading `::`) must be flagged exactly like its relative
    // form: the syn-based HVG010 macro lint's `path_to_string` ignores
    // `leading_colon`, so `::futures::future::select(a, b)` hard-blocks at
    // compile time and det_check must match it too (#980 Codex absolute-path
    // finding). Turbofished absolute forms are covered too. Each is exactly one
    // DET011 finding.
    for call in [
        "let _ = ::futures::future::select(a, b).await;",
        "let _ = ::futures::future::select_all::<Vec<_>>(v).await;",
    ] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert_eq!(
            report
                .findings
                .iter()
                .filter(|f| f.rule_id == "DET011")
                .count(),
            1,
            "absolute-path combinator `{call}` must be flagged exactly once, got: {report:?}"
        );
    }
}

#[test]
fn det011_does_not_flag_absolute_non_futures_combinator_paths() {
    // Stripping the leading `::` must NOT make a non-futures absolute path
    // match: normalizing `::my_dsl::select_all` yields `my_dsl::select_all`,
    // which is not an allowed combinator path, so it stays unflagged (mirrors
    // the macro lint's exact-path matching).
    for call in [
        "let _ = ::my_dsl::select_all(v);",
        "let _ = ::crate::future::select(a, b).await;",
    ] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert!(
            !report.findings.iter().any(|f| f.rule_id == "DET011"),
            "absolute non-futures path `{call}` must NOT be flagged, got: {report:?}"
        );
    }
}

#[test]
fn det011_does_not_flag_method_call_forms_of_combinators() {
    // Method calls (`.select_all()`, `.try_select(..)`) are NOT the futures
    // free-function combinators — they must not be flagged, mirroring the AST
    // visitor's structural exclusion of method-call receivers (#799 P2 review).
    for call in [
        "let _ = x.select_all();",
        "let _ = q.try_select(a, b);",
        "let _ = builder.select_ok(cols);",
    ] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert!(
            !report.findings.iter().any(|f| f.rule_id == "DET011"),
            "method-call form `{call}` must NOT be flagged, got: {report:?}"
        );
    }
    // A free-function call and the qualified plain-select combinator MUST still
    // be flagged.
    for call in [
        "let _ = select_all(v).await;",
        "let _ = futures::future::select(a, b).await;",
    ] {
        let src = wf(call);
        let report = check_source(&src, "test.rs");
        assert!(
            report.findings.iter().any(|f| f.rule_id == "DET011"),
            "free-function combinator `{call}` must be flagged, got: {report:?}"
        );
    }
}

#[test]
fn det011_does_not_flag_unrelated_code() {
    // A clean workflow using the sanctioned alternatives — zero DET011.
    let src = wf(
        "ctx.execute_activity_raw(\"a\", serde_json::json!(1), \"q\").await?;\n    let _ = ctx.race().activity_raw(\"b\", serde_json::json!(2), \"q\");",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET011"),
        "clean workflow must not be flagged, got: {report:?}"
    );
}

#[test]
fn det011_suppression_is_honored_and_reported() {
    let src = "#[workflow]\nasync fn test_wf(ctx: &WorkflowContext) -> Result<(), String> {\n    // harvest-suppress: DET011 \"replay fixture proves this biased select is safe\"\n    let _ = futures::future::select(a, b).await;\n    Ok(())\n}\n";
    let report = check_source(src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET011"),
        "suppressed DET011 must not produce a finding, got: {report:?}"
    );
    assert!(
        report
            .suppressions
            .iter()
            .any(|s| s.rule_id == "DET011" && !s.reason.is_empty()),
        "DET011 suppression must be echoed into report.suppressions, got: {report:?}"
    );
}

#[test]
fn det011_activity_bodies_are_never_flagged() {
    // AC6: select! and the futures combinators inside an #[activity] body are
    // never flagged — activities may race freely.
    let src = act(
        "tokio::select! {\n        _ = std::future::ready(()) => {}\n    }\n    let _ = futures::future::select(a, b).await;",
    );
    let report = check_source(&src, "test.rs");
    assert!(
        !report.findings.iter().any(|f| f.rule_id == "DET011"),
        "activity bodies must never be flagged for DET011, got: {report:?}"
    );
}

#[test]
fn det011_finding_carries_metadata_and_names_race_alternative() {
    let src = wf("let _ = futures::future::select(a, b).await;");
    let report = check_source(&src, "test.rs");
    let finding = report
        .findings
        .iter()
        .find(|f| f.rule_id == "DET011")
        .expect("DET011 finding");
    assert_eq!(finding.workflow_name.as_deref(), Some("test_wf"));
    let loc = finding.location.as_ref().expect("location");
    assert_eq!(loc.file, "test.rs");
    assert!(loc.line > 0);
    assert!(!finding.message.is_empty());
    assert!(
        finding.alternative.contains("ctx.race()"),
        "alternative must point at ctx.race(), got: {}",
        finding.alternative
    );
}

#[test]
fn det011_self_scan_of_harvest_src_is_clean() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let report = autumn_harvest::det_check::check_dir(&dir).expect("src dir must be readable");
    let det011: Vec<_> = report
        .findings
        .iter()
        .filter(|f| f.rule_id == "DET011")
        .collect();
    assert!(
        det011.is_empty(),
        "autumn-harvest/src must have zero DET011 findings, got: {det011:?}"
    );
}
