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
    let src = include_str!("../../examples/quickstart/src/main.rs");
    let report = check_source(src, "examples/quickstart/src/main.rs");
    assert!(
        !report.has_hard_blockers(),
        "quickstart example must pass with zero hard blockers, findings: {report:?}"
    );
}

#[test]
fn standalone_runner_workflows_have_no_hard_blockers() {
    let src = include_str!("../../examples/standalone-runner/src/workflows.rs");
    let report = check_source(src, "examples/standalone-runner/src/workflows.rs");
    assert!(
        !report.has_hard_blockers(),
        "standalone-runner workflows must pass with zero hard blockers, findings: {report:?}"
    );
}

#[test]
fn billing_example_workflows_have_no_hard_blockers() {
    let src = include_str!("../../examples/billing-autumn-web/src/workflows.rs");
    let report = check_source(src, "examples/billing-autumn-web/src/workflows.rs");
    assert!(
        !report.has_hard_blockers(),
        "billing example workflows must pass with zero hard blockers, findings: {report:?}"
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
